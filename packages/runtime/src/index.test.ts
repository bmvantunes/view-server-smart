import { describe, expect, it } from "@effect/vitest";
import type { ColumnLiveViewEngineHealth } from "@view-server/column-live-view-engine";
import { makeViewServerClient } from "@view-server/client/remote";
import { defineViewServerConfig, kafka, type TransportHealth } from "@view-server/config";
import { makeViewServerRuntimeCore } from "@view-server/runtime-core";
import { Config, Deferred, Effect, Exit, Fiber, Schedule, Schema, Stream } from "effect";
import type { ViewServerRuntimeDependencies } from "./internal";
import {
  makeDefaultRuntimeDependencies,
  makeViewServerRuntimeWithDependencies,
  runViewServerRuntimeWithDependencies,
} from "./internal";
import { makeViewServerRuntime, runViewServerRuntime } from "./index";
import { ViewServerKafkaIngressError } from "./kafka-ingress";
import {
  resolveViewServerRuntimeOptions,
  type ResolvedViewServerKafkaRuntimeOptions,
} from "./runtime-options";
import { makeViewServerRuntimeTransportHealth } from "./transport-health";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

const HealthJson = Schema.Struct({
  status: Schema.Literals(["ready", "degraded", "starting", "stopping"]),
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

const nullRecord = <Value>(
  entries: ReadonlyArray<readonly [string, Value]>,
): Record<string, Value> => {
  const record: Record<string, Value> = Object.create(null);
  for (const [key, value] of entries) {
    record[key] = value;
  }
  return record;
};

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

const waitForTransportHealth = Effect.fn("ViewServerRuntime.test.transportHealth.wait")(function* (
  health: () => Effect.Effect<{ readonly transport: TransportHealth }, unknown>,
  expected: {
    readonly activeClients: number;
    readonly activeStreams: number;
  },
) {
  return yield* health().pipe(
    Effect.map((value) => value.transport),
    Effect.repeat({
      schedule: Schedule.addDelay(Schedule.recurs(50), () => Effect.succeed("5 millis")),
      until: (transport) =>
        transport.activeClients === expected.activeClients &&
        transport.activeStreams === expected.activeStreams,
    }),
  );
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
      const connectedTransport = yield* waitForTransportHealth(runtime.client.health, {
        activeClients: 1,
        activeStreams: 1,
      });
      expect(runtime.liveClient.health.value.transport.activeStreams).toBe(1);
      expect(connectedTransport).toStrictEqual({
        activeClients: 1,
        activeStreams: 1,
        activeSubscriptions: 1,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        queuedMessages: 0,
        queuedBytes: 0,
        droppedClients: 0,
        backpressureEvents: 0,
        reconnects: 0,
        lastError: null,
      });

      yield* runtime.client.publish("orders", order("a", 10));

      const events = yield* Fiber.join(eventsFiber);
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
      expect(runtime.url.endsWith("/runtime-rpc")).toBe(true);
      expect(runtime.healthUrl.endsWith("/runtime-health")).toBe(true);
      expect(health.response.status).toBe(200);
      expect(health.health.engine.topics.orders.rowCount).toBe(1);

      yield* subscription.close();
      yield* remoteClient.close;
      const disconnectedTransport = yield* waitForTransportHealth(runtime.client.health, {
        activeClients: 0,
        activeStreams: 0,
      });
      expect(disconnectedTransport).toStrictEqual({
        activeClients: 0,
        activeStreams: 0,
        activeSubscriptions: 0,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        queuedMessages: 0,
        queuedBytes: 0,
        droppedClients: 0,
        backpressureEvents: 0,
        reconnects: 0,
        lastError: null,
      });
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
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: 1,
            activeViews: 1,
            groupedFullEvaluationCount: 0,
            groupedPatchedEvaluationCount: 0,
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
      expect(transport.transportHealth(engineHealth).activeClients).toBe(0);
      yield* transport.clientOpened;
      yield* transport.streamOpened;
      yield* transport.streamOpened;
      expect(transport.transportHealth(engineHealth)).toStrictEqual({
        activeClients: 1,
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
      expect(transport.transportHealth(engineHealth).activeClients).toBe(1);
      yield* transport.clientClosed;
      yield* transport.clientClosed;
      expect(transport.transportHealth(engineHealth).activeClients).toBe(0);
    }),
  );

  it.live("forwards runtime options to the runtime core and websocket server", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<typeof viewServer.topics>;
      let runtimeCoreOptions: Parameters<RuntimeDependencies["makeRuntimeCore"]>[1] | undefined;
      let serverInput: Parameters<RuntimeDependencies["makeServer"]>[1] | undefined;
      let serverOptions: Parameters<RuntimeDependencies["makeServer"]>[2] | undefined;
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
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
        groupedIncrementalAdmissionLimits: {
          maxGroups: 1,
        },
        host: "0.0.0.0",
        websocketPort: 1234,
        rpcPath: "/custom-rpc",
        healthPath: "/custom-health",
        subscriptionQueueCapacity: 7,
      });

      expect({
        runtimeCoreOptions: {
          subscriptionQueueCapacity: runtimeCoreOptions?.subscriptionQueueCapacity,
          groupedIncrementalAdmissionLimits: runtimeCoreOptions?.groupedIncrementalAdmissionLimits,
          transportHealthType: typeof runtimeCoreOptions?.transportHealth,
        },
        serverTransportHooks: {
          clientOpenedType: typeof serverInput?.transport?.clientOpened,
          clientClosedType: typeof serverInput?.transport?.clientClosed,
          streamOpenedType: typeof serverInput?.transport?.streamOpened,
          streamClosedType: typeof serverInput?.transport?.streamClosed,
        },
        serverOptions,
      }).toStrictEqual({
        runtimeCoreOptions: {
          subscriptionQueueCapacity: 7,
          groupedIncrementalAdmissionLimits: {
            maxGroups: 1,
          },
          transportHealthType: "function",
        },
        serverTransportHooks: {
          clientOpenedType: "object",
          clientClosedType: "object",
          streamOpenedType: "object",
          streamClosedType: "object",
        },
        serverOptions: {
          host: "0.0.0.0",
          port: 1234,
          path: "/custom-rpc",
          healthPath: "/custom-health",
        },
      });
      yield* runtime.close;
    }),
  );

  it.live("resolves Kafka runtime options and starts configured ingress", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<typeof viewServer.topics>;
      let kafkaOptions: ResolvedViewServerKafkaRuntimeOptions<typeof viewServer.topics> | undefined;
      const regions = {
        local: Config.succeed("localhost:9092"),
      };
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            close: Effect.void,
          }),
        makeKafkaIngress: (_config, _client, _requestHealthRefresh, options) => {
          kafkaOptions = options;
          return Effect.succeed({
            close: Effect.void,
          });
        },
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
        kafka: {
          consumerGroupId: "view-server-test-runtime",
          regions,
          topics: {
            "orders-source": localKafkaTopic({
              regions: ["local"],
              value: kafka.json(Order),
              key: kafka.stringKey(),
              viewServerTopic: "orders",
              mapping: ({ key, value }) => ({
                id: key,
                price: value.price,
              }),
            }),
          },
        },
      });

      expect({
        consume: kafkaOptions?.consume,
        consumerGroupId: kafkaOptions?.consumerGroupId,
        regions: kafkaOptions?.regions,
        startFrom: kafkaOptions?.startFrom,
        topics: Object.fromEntries(
          Object.entries(kafkaOptions?.topics ?? {}).map(([sourceTopic, topic]) => [
            sourceTopic,
            {
              regions: topic.regions,
              viewServerTopic: topic.viewServerTopic,
            },
          ]),
        ),
      }).toStrictEqual({
        consume: {
          consumerGroupId: "view-server-test-runtime",
          fallbackMode: "earliest",
          mode: "committed",
        },
        consumerGroupId: "view-server-test-runtime",
        regions: nullRecord([["local", "localhost:9092"]]),
        startFrom: {
          committedConsumerGroup: "view-server-test-runtime",
        },
        topics: {
          "orders-source": {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });

      yield* runtime.close;
    }),
  );

  it.effect("resolves explicit Kafka start policies", () =>
    Effect.gen(function* () {
      const regions = {
        local: "localhost:9092",
      };
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
      const topics = {
        "orders-source": localKafkaTopic({
          regions: ["local"],
          value: kafka.json(Order),
          key: kafka.stringKey(),
          viewServerTopic: "orders",
          mapping: ({ key, value }) => ({
            id: key,
            price: value.price,
          }),
        }),
      };

      const earliest = yield* resolveViewServerRuntimeOptions({
        kafka: {
          consumerGroupId: "view-server-earliest",
          regions,
          startFrom: "earliest",
          topics,
        },
      });
      const latest = yield* resolveViewServerRuntimeOptions({
        kafka: {
          consumerGroupId: "view-server-latest",
          regions,
          startFrom: "latest",
          topics,
        },
      });
      const committed = yield* resolveViewServerRuntimeOptions({
        kafka: {
          consumerGroupId: "view-server-default",
          regions,
          startFrom: {
            committedConsumerGroup: "view-server-existing-group",
            fallback: "fail",
          },
          topics,
        },
      });
      const committedWithDefaultFallback = yield* resolveViewServerRuntimeOptions({
        kafka: {
          consumerGroupId: "view-server-default-fallback",
          regions,
          startFrom: {
            committedConsumerGroup: "view-server-existing-default-fallback-group",
          },
          topics,
        },
      });

      expect({
        committed: committed.kafkaOptions?.consume,
        committedWithDefaultFallback: committedWithDefaultFallback.kafkaOptions?.consume,
        earliest: earliest.kafkaOptions?.consume,
        latest: latest.kafkaOptions?.consume,
      }).toStrictEqual({
        committed: {
          consumerGroupId: "view-server-existing-group",
          fallbackMode: "fail",
          mode: "committed",
        },
        committedWithDefaultFallback: {
          consumerGroupId: "view-server-existing-default-fallback-group",
          fallbackMode: "earliest",
          mode: "committed",
        },
        earliest: {
          consumerGroupId: "view-server-earliest",
          fallbackMode: "earliest",
          mode: "earliest",
        },
        latest: {
          consumerGroupId: "view-server-latest",
          fallbackMode: "latest",
          mode: "latest",
        },
      });
    }),
  );

  it.live("preserves dangerous Kafka runtime option keys", () =>
    Effect.gen(function* () {
      type RuntimeDependencies = ViewServerRuntimeDependencies<typeof viewServer.topics>;
      let kafkaOptions: ResolvedViewServerKafkaRuntimeOptions<typeof viewServer.topics> | undefined;
      const regions = nullRecord([["__proto__", Config.succeed("localhost:9092")]]);
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
      const dangerousTopic = localKafkaTopic({
        regions: ["__proto__"],
        value: kafka.json(Order),
        key: kafka.stringKey(),
        viewServerTopic: "orders",
        mapping: ({ key, value }) => ({
          id: key,
          price: value.price,
        }),
      });
      const topics = nullRecord([["__proto__", dangerousTopic]]);
      const dependencies: RuntimeDependencies = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            close: Effect.void,
          }),
        makeKafkaIngress: (_config, _client, _requestHealthRefresh, options) => {
          kafkaOptions = options;
          return Effect.succeed({
            close: Effect.void,
          });
        },
      };

      const runtime = yield* makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
        kafka: {
          consumerGroupId: "view-server-dangerous-key-test-runtime",
          regions,
          topics,
        },
      });

      expect(Object.hasOwn(kafkaOptions?.regions ?? {}, "__proto__")).toBe(true);
      expect(Object.hasOwn(kafkaOptions?.topics ?? {}, "__proto__")).toBe(true);
      expect({
        consumerGroupId: kafkaOptions?.consumerGroupId,
        region: kafkaOptions?.regions["__proto__"],
        topicRegions: kafkaOptions?.topics["__proto__"]?.regions,
        viewServerTopic: kafkaOptions?.topics["__proto__"]?.viewServerTopic,
      }).toStrictEqual({
        consumerGroupId: "view-server-dangerous-key-test-runtime",
        region: "localhost:9092",
        topicRegions: ["__proto__"],
        viewServerTopic: "orders",
      });

      yield* runtime.close;
    }),
  );

  it.live("returns unavailable health when Kafka ingress is degraded", () =>
    Effect.gen(function* () {
      const regions = {
        local: "localhost:9092",
      };
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeKafkaIngress: (_config, _client, _requestHealthRefresh, _options, health) =>
          health.regionDisconnected("local", "lost").pipe(
            Effect.as({
              close: Effect.void,
            }),
          ),
      };

      yield* Effect.acquireUseRelease(
        makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
          kafka: {
            consumerGroupId: "view-server-test-degraded",
            regions,
            topics: {
              "orders-source": localKafkaTopic({
                regions: ["local"],
                value: kafka.json(Order),
                key: kafka.stringKey(),
                viewServerTopic: "orders",
                mapping: ({ key, value }) => ({
                  id: key,
                  price: value.price,
                }),
              }),
            },
          },
        }),
        (runtime) =>
          Effect.gen(function* () {
            const health = yield* fetchHealth(runtime.healthUrl);

            expect(health.response.status).toBe(503);
            expect(health.health.status).toBe("degraded");
            expect(health.health.engine.topics.orders.rowCount).toBe(0);
          }),
        (runtime) => runtime.close,
      );
    }),
  );

  it.live("public live client close closes the websocket server and runtime core", () =>
    Effect.gen(function* () {
      let serverCloseCount = 0;
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
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

  it.live("run helper keeps the runtime alive until the main fiber is interrupted", () =>
    Effect.gen(function* () {
      let serverCloseCount = 0;
      const serverStarted = yield* Deferred.make<void>();
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeRuntimeCore: makeViewServerRuntimeCore,
        makeServer: () =>
          Deferred.succeed(serverStarted, void 0).pipe(
            Effect.as({
              url: "ws://127.0.0.1:0/rpc",
              healthUrl: "http://127.0.0.1:0/health",
              close: Effect.sync(() => {
                serverCloseCount += 1;
              }),
            }),
          ),
      };

      const fiber = yield* runViewServerRuntimeWithDependencies(dependencies, viewServer).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(serverStarted);
      yield* Effect.sleep("10 millis");
      expect(serverCloseCount).toBe(0);

      yield* Fiber.interrupt(fiber);
      expect(serverCloseCount).toBe(1);
    }),
  );

  it.live("public run helper starts a launchable websocket runtime", () =>
    Effect.gen(function* () {
      const fiber = yield* runViewServerRuntime(viewServer, {
        host: "127.0.0.1",
        websocketPort: 0,
      }).pipe(Effect.forkChild({ startImmediately: true }));

      yield* Effect.sleep("20 millis");
      yield* Fiber.interrupt(fiber);
    }),
  );

  it.live("public run helper supports default runtime options", () =>
    Effect.gen(function* () {
      const fiber = yield* runViewServerRuntime(viewServer).pipe(
        Effect.forkChild({ startImmediately: true }),
      );

      yield* Effect.sleep("20 millis");
      yield* Fiber.interrupt(fiber);
    }),
  );

  it.live("releases the runtime core when server startup fails before returning a runtime", () =>
    Effect.gen(function* () {
      let closed = false;
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
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

  it.live("releases server and runtime core when Kafka ingress startup fails", () =>
    Effect.gen(function* () {
      let runtimeCoreClosed = false;
      let serverClosed = false;
      const regions = {
        local: "localhost:9092",
      };
      const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
      const dependencies: ViewServerRuntimeDependencies<typeof viewServer.topics> = {
        ...makeDefaultRuntimeDependencies<typeof viewServer.topics>(),
        makeRuntimeCore: (config, options) =>
          makeViewServerRuntimeCore(config, options).pipe(
            Effect.map((runtimeCore) => ({
              ...runtimeCore,
              close: runtimeCore.close.pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    runtimeCoreClosed = true;
                  }),
                ),
              ),
            })),
          ),
        makeServer: () =>
          Effect.succeed({
            url: "ws://127.0.0.1:0/rpc",
            healthUrl: "http://127.0.0.1:0/health",
            close: Effect.sync(() => {
              serverClosed = true;
            }),
          }),
        makeKafkaIngress: () =>
          Effect.fail(
            new ViewServerKafkaIngressError({
              message: "Kafka ingress startup failed",
              cause: "startup failed",
            }),
          ),
      };

      const startupExit = yield* Effect.exit(
        makeViewServerRuntimeWithDependencies(dependencies, viewServer, {
          kafka: {
            consumerGroupId: "view-server-test-startup-failure",
            regions,
            topics: {
              "orders-source": localKafkaTopic({
                regions: ["local"],
                value: kafka.json(Order),
                key: kafka.stringKey(),
                viewServerTopic: "orders",
                mapping: ({ key, value }) => ({
                  id: key,
                  price: value.price,
                }),
              }),
            },
          },
        }),
      );

      expect(Exit.isFailure(startupExit)).toBe(true);
      expect(serverClosed).toBe(true);
      expect(runtimeCoreClosed).toBe(true);
    }),
  );
});
