import { describe, expect, it } from "@effect/vitest";
import type { ColumnLiveViewEngineHealth } from "@view-server/column-live-view-engine";
import { makeViewServerClient } from "@view-server/client/remote";
import { defineViewServerConfig } from "@view-server/config";
import { makeViewServerRuntimeCore } from "@view-server/runtime-core";
import { Effect, Exit, Fiber, Schema, Stream } from "effect";
import type { ViewServerRuntimeDependencies } from "./internal";
import { makeViewServerRuntimeWithDependencies } from "./internal";
import { makeViewServerRuntime } from "./index";
import { makeViewServerRuntimeTransportHealth } from "./transport-health";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

const HealthJson = Schema.Struct({
  status: Schema.Literal("ready"),
  engine: Schema.Struct({
    topics: Schema.Struct({
      orders: Schema.Struct({
        rowCount: Schema.Number,
      }),
    }),
  }),
});

class RuntimeHealthJsonParseError extends Schema.TaggedErrorClass<RuntimeHealthJsonParseError>()(
  "RuntimeHealthJsonParseError",
  {
    cause: Schema.Unknown,
  },
) {}

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

type OrderRow = typeof Order.Type;

const order = (id: string, price: number): OrderRow => ({
  id,
  price,
});

const fetchHealth = Effect.fn("ViewServerRuntime.test.health.fetch")(function* (url: string) {
  const response = yield* Effect.promise(() => fetch(url));
  const text = yield* Effect.promise(() => response.text());
  const value = yield* Effect.try({
    try: (): unknown => JSON.parse(text),
    catch: (cause) => new RuntimeHealthJsonParseError({ cause }),
  });
  const health = yield* Schema.decodeUnknownEffect(HealthJson)(value);
  return { response, health };
});

describe("@view-server/runtime", () => {
  it.live("starts a websocket runtime with health endpoint and runtime-core mutation client", () =>
    Effect.gen(function* () {
      const runtime = yield* makeViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        rpcPath: "/runtime-rpc",
        healthPath: "/runtime-health",
      });
      const remoteClient = yield* makeViewServerClient(viewServer, { url: runtime.url });
      const subscription = yield* remoteClient.subscribe("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const eventsFiber = yield* subscription.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.sleep("10 millis");
      expect(runtime.liveClient.health.value.transport.activeStreams).toBe(1);

      yield* runtime.client.publish("orders", order("a", 10));

      const events = yield* Fiber.join(eventsFiber);
      expect(runtime.url.endsWith("/runtime-rpc")).toBe(true);
      expect(runtime.healthUrl.endsWith("/runtime-health")).toBe(true);
      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(events[1]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 1,
        operations: [{ type: "insert", key: "a", row: { id: "a", price: 10 }, index: 0 }],
        totalRows: 1,
      });

      const health = yield* fetchHealth(runtime.healthUrl);
      expect(health.response.status).toBe(200);
      expect(health.health.engine.topics.orders.rowCount).toBe(1);

      yield* subscription.close();
      yield* remoteClient.close;
      yield* Effect.sleep("10 millis");
      expect(runtime.liveClient.health.value.transport.activeStreams).toBe(0);
      yield* runtime.close;
    }),
  );

  it.live("supports default paths and queue capacity options", () =>
    Effect.gen(function* () {
      const defaultRuntime = yield* makeViewServerRuntime(viewServer);
      expect(defaultRuntime.url.endsWith("/rpc")).toBe(true);
      expect(defaultRuntime.healthUrl.endsWith("/health")).toBe(true);
      expect("subscribeRuntime" in defaultRuntime.liveClient).toBe(false);
      yield* defaultRuntime.close;

      const configuredRuntime = yield* makeViewServerRuntime(viewServer, {
        websocketPort: 0,
        subscriptionQueueCapacity: 1,
      });
      expect(configuredRuntime.url.endsWith("/rpc")).toBe(true);
      expect(configuredRuntime.healthUrl.endsWith("/health")).toBe(true);
      yield* configuredRuntime.close;
    }),
  );

  it.effect("tracks runtime transport stream health", () =>
    Effect.gen(function* () {
      const transport = makeViewServerRuntimeTransportHealth<typeof viewServer.topics>();
      const engineHealth = {
        status: "ready",
        version: 1,
        topics: {
          orders: {
            status: "ready",
            rowCount: 10,
            liveRowCount: 10,
            deletedRowCount: 0,
            version: 3,
            lastMutationAt: 1,
            mutationsPerSecond: 2,
            rowsPerSecond: 2,
            pendingMutationBatches: 0,
            activeViews: 1,
            activeSubscriptions: 4,
            queuedEvents: 5,
            maxQueueDepth: 6,
            backpressureEvents: 7,
            memoryBytes: 8,
            tombstoneCount: 0,
            compactionPending: false,
          },
        },
        activeSubscriptions: 4,
        queuedEvents: 5,
        maxQueueDepth: 6,
        backpressureEvents: 7,
      } satisfies ColumnLiveViewEngineHealth<typeof viewServer.topics>;

      expect(transport.transportHealth(engineHealth).activeStreams).toBe(0);
      yield* transport.streamOpened;
      yield* transport.streamOpened;
      expect(transport.transportHealth(engineHealth)).toStrictEqual({
        activeClients: 0,
        activeStreams: 2,
        activeSubscriptions: 4,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        queuedMessages: 5,
        queuedBytes: 0,
        droppedClients: 0,
        backpressureEvents: 7,
        reconnects: 0,
        lastError: null,
      });
      yield* transport.streamClosed;
      yield* transport.streamClosed;
      yield* transport.streamClosed;
      expect(transport.transportHealth(engineHealth).activeStreams).toBe(0);
    }),
  );

  it.live("forwards runtime options to the runtime core and websocket server", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<typeof viewServer.topics>;
      let runtimeCoreOptions: Parameters<RuntimeDependencies["makeRuntimeCore"]>[1] | undefined;
      let serverInput: Parameters<RuntimeDependencies["makeServer"]>[1] | undefined;
      let serverOptions: Parameters<RuntimeDependencies["makeServer"]>[2] | undefined;
      const dependencies: RuntimeDependencies = {
        makeRuntimeCore: (config, options) => {
          runtimeCoreOptions = options;
          return makeViewServerRuntimeCore(config, options);
        },
        makeServer: (_config, input, options) => {
          serverInput = input;
          serverOptions = options;
          return Effect.succeed({
            url: "ws://127.0.0.1:0/custom-rpc",
            healthUrl: "http://127.0.0.1:0/custom-health",
            close: Effect.void,
          });
        },
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
        host: "0.0.0.0",
        websocketPort: 1234,
        rpcPath: "/custom-rpc",
        healthPath: "/custom-health",
        subscriptionQueueCapacity: 7,
      });

      expect(runtimeCoreOptions?.subscriptionQueueCapacity).toBe(7);
      expect(runtimeCoreOptions?.transportHealth).toBeTypeOf("function");
      expect(serverInput?.transport?.streamOpened).toBeDefined();
      expect(serverInput?.transport?.streamClosed).toBeDefined();
      expect(serverOptions).toStrictEqual({
        host: "0.0.0.0",
        port: 1234,
        path: "/custom-rpc",
        healthPath: "/custom-health",
      });
      yield* runtime.close;
    }),
  );

  it.live("public live client close closes the websocket server and runtime core", () =>
    Effect.gen(function* () {
      let serverCloseCount = 0;
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        makeRuntimeCore: makeViewServerRuntimeCore,
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            close: Effect.sync(() => {
              serverCloseCount += 1;
            }),
          }),
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer);
      yield* runtime.liveClient.close;
      const health = yield* runtime.client.health();

      expect(serverCloseCount).toBe(1);
      expect(health.status).toBe("stopping");
    }),
  );

  it.live("releases the runtime core when server startup fails before returning a runtime", () =>
    Effect.gen(function* () {
      let closed = false;
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        makeRuntimeCore: (config, options) =>
          makeViewServerRuntimeCore(config, options).pipe(
            Effect.map((runtimeCore) => ({
              ...runtimeCore,
              close: runtimeCore.close.pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    closed = true;
                  }),
                ),
              ),
            })),
          ),
        makeServer: () => Effect.die(new Error("server startup failed")),
      };

      const startupExit = yield* Effect.exit(
        makeViewServerRuntimeWithDependencies(dependencies, viewServer),
      );

      expect(Exit.isFailure(startupExit)).toBe(true);
      expect(closed).toBe(true);
    }),
  );
});
