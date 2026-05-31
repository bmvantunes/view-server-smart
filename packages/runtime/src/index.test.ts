import { describe, expect, it } from "@effect/vitest";
import { makeViewServerClient } from "@view-server/client/remote";
import { defineViewServerConfig } from "@view-server/config";
import { makeInMemoryViewServer } from "@view-server/in-memory";
import { Effect, Exit, Fiber, Schema, Stream } from "effect";
import type { ViewServerRuntimeDependencies } from "./internal";
import { makeViewServerRuntimeWithDependencies } from "./internal";
import { makeViewServerRuntime } from "./index";

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
  it.live("starts a websocket runtime with health endpoint and in-memory mutation client", () =>
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
      yield* runtime.close;
    }),
  );

  it.live("supports default paths and queue capacity options", () =>
    Effect.gen(function* () {
      const defaultRuntime = yield* makeViewServerRuntime(viewServer);
      expect(defaultRuntime.url.endsWith("/rpc")).toBe(true);
      expect(defaultRuntime.healthUrl.endsWith("/health")).toBe(true);
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

  it.live(
    "releases the in-memory runtime when server startup fails before returning a runtime",
    () =>
      Effect.gen(function* () {
        let closed = false;
        const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
          makeInMemory: (config, options) =>
            makeInMemoryViewServer(config, options).pipe(
              Effect.map((inMemory) => ({
                ...inMemory,
                close: inMemory.close.pipe(
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
