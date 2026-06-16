import { NodeHttpServer } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import {
  defineViewServerConfig,
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
} from "@view-server/config";
import {
  Clock,
  Context,
  Effect,
  Fiber,
  Layer,
  ManagedRuntime,
  Queue,
  Schema,
  SchemaGetter,
  Stream,
} from "effect";
import { HttpRouter, HttpServer } from "effect/unstable/http";
import { RpcSerialization, RpcServer } from "effect/unstable/rpc";
import type * as Duration from "effect/Duration";
import * as Http from "node:http";
import {
  ViewServerRpcs,
  ViewServerTrustedWireEventSchema,
  ViewServerWireRowSchema,
  viewServerDecodeHealth,
  type ViewServerRpcError,
  type ViewServerTrustedWireEvent,
  type ViewServerWireEvent,
  type ViewServerWireHealth,
} from "@view-server/protocol";
import { makeViewServerClient } from "./remote";
import { mapViewServerRemoteError } from "./remote-client";
import { makeRemoteHealthState } from "./remote-health";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

const BadJsonField = Schema.String.pipe(
  Schema.encodeTo(Schema.Any, {
    decode: SchemaGetter.transform((value) => (typeof value === "string" ? value : "decoded")),
    encode: SchemaGetter.transform(() => Symbol("not-json")),
  }),
);

const BadJsonRow = Schema.Struct({
  id: BadJsonField,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

const edgeViewServer = defineViewServerConfig({
  topics: {
    badjson: {
      schema: BadJsonRow,
      key: "id",
    },
  },
});

type OrderRow = typeof Order.Type;

const order = (id: string, price: number): OrderRow => ({
  id,
  price,
});

type WireTopicHealth = ViewServerWireHealth["engine"]["topics"][string];

const topicHealth = (rowCount: number, activeSubscriptions: number): WireTopicHealth => ({
  status: "ready",
  rowCount,
  liveRowCount: rowCount,
  deletedRowCount: 0,
  version: rowCount,
  lastMutationAt: null,
  mutationsPerSecond: 0,
  rowsPerSecond: 0,
  pendingMutationBatches: 0,
  activeFallbackGroupedViews: 0,
  activeIncrementalGroupedViews: 0,
  activeViews: activeSubscriptions,
  groupedFullEvaluationCount: 0,
  groupedPatchedEvaluationCount: 0,
  activeSubscriptions,
  queuedEvents: 0,
  maxQueueDepth: 0,
  backpressureEvents: 0,
  memoryBytes: 0,
  tombstoneCount: 0,
  compactionPending: false,
});

const health = (rowCount: number, activeSubscriptions: number): ViewServerWireHealth => ({
  status: "ready",
  version: rowCount,
  uptimeMs: 0,
  engine: {
    topics: {
      orders: topicHealth(rowCount, activeSubscriptions),
    },
  },
  transport: {
    activeClients: 1,
    activeStreams: activeSubscriptions,
    activeSubscriptions,
    messagesPerSecond: 0,
    bytesPerSecond: 0,
    queuedMessages: 0,
    queuedBytes: 0,
    droppedClients: 0,
    backpressureEvents: 0,
    reconnects: 0,
    lastError: null,
  },
});

const kafkaHealth = (): NonNullable<ViewServerWireHealth["kafka"]> => ({
  startFrom: {
    consumerGroupId: "view-server-test",
    fallbackMode: "earliest",
    mode: "committed",
  },
  regions: {
    usa: {
      status: "connected",
      brokers: "localhost:9092",
      lastConnectedAt: 10,
      lastError: null,
    },
  },
  topics: {
    sourceOrders: {
      status: "degraded",
      sourceTopic: "orders-source",
      viewServerTopic: "orders",
      regions: {
        usa: {
          connected: true,
          assignedPartitions: 3,
          messagesPerSecond: 23,
          bytesPerSecond: 33,
          decodedMessagesPerSecond: 20,
          decodeFailuresPerSecond: 1,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 1,
          commitFailuresPerSecond: 1,
          processingFailuresPerSecond: 2,
          lastMessageAt: 60,
          lastCommitAt: 50,
          consumerLagMessages: 9n,
          lagSampledAt: 70,
          committedOffset: "91",
          lastError: "commit failed",
        },
      },
    },
  },
});

const healthSummaryWireRow = (): typeof ViewServerWireRowSchema.Type => ({
  id: "summary",
  status: "ready",
  runtimeStatus: "ready",
  connectionStatus: "connected",
  unhealthyTopics: [],
  updatedAtNanos: "1",
  maxKafkaLag: "0",
});

const healthTopicWireRow = (): typeof ViewServerWireRowSchema.Type => ({
  id: "orders",
  status: "ready",
  rowCount: 0,
  liveRowCount: 0,
  deletedRowCount: 0,
  version: 0,
  lastMutationAt: null,
  mutationsPerSecond: 0,
  rowsPerSecond: 0,
  pendingMutationBatches: 0,
  activeFallbackGroupedViews: 0,
  activeIncrementalGroupedViews: 0,
  activeViews: 0,
  groupedFullEvaluationCount: 0,
  groupedPatchedEvaluationCount: 0,
  activeSubscriptions: 0,
  queuedEvents: 0,
  maxQueueDepth: 0,
  backpressureEvents: 0,
  memoryBytes: 0,
  tombstoneCount: 0,
  compactionPending: false,
  kafkaLag: "0",
  updatedAtNanos: "1",
});

const readLimit = (query: unknown): number | undefined => {
  if (typeof query !== "object" || query === null || !("limit" in query)) {
    return undefined;
  }
  const value = query.limit;
  return typeof value === "number" ? value : undefined;
};

const snapshotEvent = (
  topic: string,
  rows: ReadonlyArray<typeof ViewServerWireRowSchema.Type>,
): ViewServerWireEvent => ({
  type: "snapshot",
  topic,
  queryId: "query-remote",
  version: rows.length,
  keys: rows.map((row) => {
    const id = row["id"];
    const encoded = typeof id === "string" ? id : JSON.stringify(id);
    return encoded === undefined ? "undefined" : encoded;
  }),
  rows,
  totalRows: rows.length,
});

const invalidTrustedEvent = (error: { readonly message: string }): ViewServerRpcError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidRow",
  message: error.message,
});

const trustedEvent = (event: ViewServerWireEvent) =>
  Schema.decodeUnknownEffect(ViewServerTrustedWireEventSchema)(event).pipe(
    Effect.mapError(invalidTrustedEvent),
  );

const makeTestRpcServer = Effect.fn("ViewServerClient.remote.testServer.make")(function* () {
  const path = "/rpc";
  const events = yield* Queue.unbounded<ViewServerTrustedWireEvent>();
  const healthSummaryEvents = yield* Queue.unbounded<ViewServerTrustedWireEvent>();
  const healthTopicEvents = yield* Queue.unbounded<ViewServerTrustedWireEvent>();
  let lastSubscribeQuery: unknown = undefined;
  let rows: ReadonlyArray<typeof ViewServerWireRowSchema.Type> = [];
  let activeSubscriptions = 0;
  let healthRequests = 0;
  let healthOverride: ViewServerWireHealth | undefined = undefined;
  let healthDelay: Duration.Input = "0 millis";
  let healthSummaryRows: ReadonlyArray<typeof ViewServerWireRowSchema.Type> = [
    healthSummaryWireRow(),
  ];
  let healthTopicRows: ReadonlyArray<typeof ViewServerWireRowSchema.Type> = [healthTopicWireRow()];
  let healthSummaryError: ViewServerRpcError | undefined = undefined;
  let healthTopicError: ViewServerRpcError | undefined = undefined;

  const handlers = ViewServerRpcs.toLayer(
    Effect.succeed(
      ViewServerRpcs.of({
        "ViewServer.Health": () =>
          Effect.gen(function* () {
            healthRequests += 1;
            yield* Effect.sleep(healthDelay);
            return healthOverride ?? health(rows.length, activeSubscriptions);
          }),
        "ViewServer.Subscribe": (
          payload,
        ): Stream.Stream<ViewServerTrustedWireEvent, ViewServerRpcError> => {
          lastSubscribeQuery = payload.query;
          if (payload.topic === VIEW_SERVER_HEALTH_SUMMARY_TOPIC) {
            if (healthSummaryError !== undefined) {
              return Stream.fail(healthSummaryError);
            }
            return Stream.unwrap(
              Effect.sync(() => {
                activeSubscriptions += 1;
                return Stream.fromEffect(
                  trustedEvent(snapshotEvent(payload.topic, healthSummaryRows)),
                ).pipe(
                  Stream.concat(Stream.fromQueue(healthSummaryEvents)),
                  Stream.ensuring(
                    Effect.sync(() => {
                      activeSubscriptions -= 1;
                    }),
                  ),
                );
              }),
            );
          }
          if (payload.topic === VIEW_SERVER_HEALTH_TOPIC) {
            if (healthTopicError !== undefined) {
              return Stream.fail(healthTopicError);
            }
            return Stream.unwrap(
              Effect.sync(() => {
                activeSubscriptions += 1;
                return Stream.fromEffect(
                  trustedEvent(snapshotEvent(payload.topic, healthTopicRows)),
                ).pipe(
                  Stream.concat(Stream.fromQueue(healthTopicEvents)),
                  Stream.ensuring(
                    Effect.sync(() => {
                      activeSubscriptions -= 1;
                    }),
                  ),
                );
              }),
            );
          }
          const limit = readLimit(payload.query);
          if (limit === 994) {
            return Stream.fail<ViewServerRpcError>({
              _tag: "ViewServerTransportError",
              code: "SubscriptionClosed",
              message: "subscription closed by server",
            });
          }
          if (limit === 995) {
            return Stream.fail<ViewServerRpcError>({
              _tag: "ViewServerRuntimeError",
              code: "SnapshotStale",
              message: "snapshot stale",
              topic: payload.topic,
            });
          }
          if (limit === 996) {
            return Stream.fail<ViewServerRpcError>({
              _tag: "ViewServerRuntimeError",
              code: "InvalidQuery",
              message: "remote invalid query",
              topic: payload.topic,
            });
          }
          if (limit === 997) {
            return Stream.fail<ViewServerRpcError>({
              _tag: "ViewServerTransportError",
              code: "TransportError",
              message: "transport failure without query",
            });
          }
          if (limit === 998) {
            return Stream.fail<ViewServerRpcError>({
              _tag: "ViewServerTransportError",
              code: "TransportError",
              message: "transport failure",
              queryId: "query-from-server",
            });
          }
          if (limit === 999) {
            return Stream.fail<ViewServerRpcError>({
              _tag: "ViewServerBackpressureError",
              code: "BackpressureExceeded",
              message: "backpressure failure",
            });
          }
          return Stream.unwrap(
            Effect.sync(() => {
              activeSubscriptions += 1;
              return Stream.fromEffect(trustedEvent(snapshotEvent(payload.topic, rows))).pipe(
                Stream.concat(Stream.fromQueue(events)),
                Stream.ensuring(
                  Effect.sync(() => {
                    activeSubscriptions -= 1;
                  }),
                ),
              );
            }),
          );
        },
      }),
    ),
  );
  const protocol = RpcServer.layerProtocolWebsocket({ path }).pipe(Layer.provide(HttpRouter.layer));
  const layer = RpcServer.layer(ViewServerRpcs, {
    disableFatalDefects: true,
  }).pipe(
    Layer.provide(handlers),
    Layer.provideMerge(protocol),
    Layer.provide(
      HttpRouter.serve(protocol, {
        disableListenLog: true,
        disableLogger: true,
      }),
    ),
    Layer.provideMerge(NodeHttpServer.layer(Http.createServer, { port: 0 })),
    Layer.provide(RpcSerialization.layerNdjson),
  );
  const runtime = ManagedRuntime.make(layer);
  const context = yield* runtime.contextEffect;
  const server = Context.get(context, HttpServer.HttpServer);
  const address = server.address;
  if (address._tag !== "TcpAddress") {
    return yield* Effect.die(new Error("Expected a TCP test server address."));
  }
  return {
    activeSubscriptions: () => activeSubscriptions,
    close: runtime.disposeEffect,
    emit: (event: ViewServerWireEvent) =>
      Effect.flatMap(trustedEvent(event), (trusted) => Queue.offer(events, trusted)),
    emitHealthSummary: (event: ViewServerWireEvent) =>
      Effect.flatMap(trustedEvent(event), (trusted) => Queue.offer(healthSummaryEvents, trusted)),
    emitHealthTopic: (event: ViewServerWireEvent) =>
      Effect.flatMap(trustedEvent(event), (trusted) => Queue.offer(healthTopicEvents, trusted)),
    emitInsert: (topic: string, row: typeof ViewServerWireRowSchema.Type) =>
      Effect.gen(function* () {
        rows = [...rows, row];
        const id = row["id"];
        const encodedKey = typeof id === "string" ? id : JSON.stringify(id);
        const key = encodedKey === undefined ? "undefined" : encodedKey;
        const event = yield* trustedEvent({
          type: "delta",
          topic,
          queryId: "query-remote",
          fromVersion: rows.length - 1,
          toVersion: rows.length,
          operations: [
            {
              type: "insert",
              key,
              row,
              index: rows.length - 1,
            },
          ],
          totalRows: rows.length,
        });
        yield* Queue.offer(events, event);
      }),
    healthRequests: () => healthRequests,
    lastSubscribeQuery: () => lastSubscribeQuery,
    setRows: (nextRows: ReadonlyArray<typeof ViewServerWireRowSchema.Type>) => {
      rows = nextRows;
    },
    setHealth: (nextHealth: ViewServerWireHealth) => {
      healthOverride = nextHealth;
    },
    setHealthDelay: (nextDelay: Duration.Input) => {
      healthDelay = nextDelay;
    },
    setHealthSummaryRows: (nextRows: ReadonlyArray<typeof ViewServerWireRowSchema.Type>) => {
      healthSummaryRows = nextRows;
    },
    setHealthTopicRows: (nextRows: ReadonlyArray<typeof ViewServerWireRowSchema.Type>) => {
      healthTopicRows = nextRows;
    },
    failHealthSummary: (error: ViewServerRpcError) => {
      healthSummaryError = error;
    },
    failHealthTopic: (error: ViewServerRpcError) => {
      healthTopicError = error;
    },
    url: `ws://127.0.0.1:${address.port}${path}`,
  };
});

describe("remote ViewServer client", () => {
  it("maps client-side RPC errors into transport errors", () => {
    expect(mapViewServerRemoteError(new Error("socket closed"))).toStrictEqual({
      _tag: "ViewServerTransportError",
      code: "TransportError",
      message: "socket closed",
    });
  });

  it.effect("does not let late pushed health summary events overwrite stopping status", () =>
    Effect.gen(function* () {
      const initialHealth = yield* viewServerDecodeHealth(viewServer, health(0, 0));
      const remoteHealth = makeRemoteHealthState(initialHealth);

      yield* remoteHealth.markStopping;
      yield* remoteHealth.updateHealthSummaryRef({
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "query-remote",
        version: 1,
        keys: ["summary"],
        rows: [
          {
            id: "summary",
            status: "ready",
            runtimeStatus: "ready",
            connectionStatus: "connected",
            unhealthyTopics: [],
            updatedAtNanos: 1n,
            maxKafkaLag: 0n,
          },
        ],
        totalRows: 1,
      });

      expect(remoteHealth.readonlyHealth.value.status).toBe("stopping");
    }),
  );

  it.live("subscribes, receives external live events, and closes over Effect RPC WebSocket", () =>
    Effect.gen(function* () {
      const server = yield* makeTestRpcServer();
      const client = yield* makeViewServerClient(viewServer, {
        url: server.url,
      });
      expect(server.healthRequests()).toBe(1);
      const subscription = yield* client.subscribe("orders", {
        select: ["id", "price"],
        limit: 10,
      });
      expect(server.healthRequests()).toBe(1);
      const eventsFiber = yield* subscription.events.pipe(
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.sleep("10 millis");
      expect(server.healthRequests()).toBe(1);
      expect(client.health.value.engine.topics.orders.activeSubscriptions).toBe(1);
      expect(client.health.value.transport.activeSubscriptions).toBe(1);

      yield* server.emit({
        type: "delta",
        topic: "orders",
        queryId: "query-remote",
        fromVersion: 0,
        toVersion: 1,
        operations: [{ type: "insert", key: "queued", row: { id: "queued", price: 5 }, index: 0 }],
        totalRows: 1,
      });
      yield* Effect.sleep("10 millis");
      expect(server.healthRequests()).toBe(1);

      yield* server.emitInsert("orders", order("a", 10));
      const events = yield* Fiber.join(eventsFiber);
      expect(server.healthRequests()).toBe(1);
      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-remote",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(events[1]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-remote",
        fromVersion: 0,
        toVersion: 1,
        operations: [{ type: "insert", key: "queued", row: { id: "queued", price: 5 }, index: 0 }],
        totalRows: 1,
      });
      expect(events[2]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-remote",
        fromVersion: 0,
        toVersion: 1,
        operations: [{ type: "insert", key: "a", row: { id: "a", price: 10 }, index: 0 }],
        totalRows: 1,
      });

      yield* Effect.sleep("10 millis");
      expect(client.health.value.engine.topics.orders.activeSubscriptions).toBe(0);
      expect(client.health.value.transport.activeSubscriptions).toBe(0);

      yield* client.close;
      expect(client.health.value.status).toBe("stopping");
      yield* server.close;
    }),
  );

  it.live(
    "closes with typed backpressure status when the remote client event buffer overflows",
    () =>
      Effect.gen(function* () {
        const server = yield* makeTestRpcServer();
        const client = yield* makeViewServerClient(viewServer, {
          subscriptionBufferSize: 1,
          url: server.url,
        });
        const subscription = yield* client.subscribe("orders", {
          select: ["id", "price"],
          limit: 10,
        });

        yield* server.emitInsert("orders", order("first", 1));
        yield* server.emitInsert("orders", order("second", 2));
        const events = yield* subscription.events.pipe(Stream.takeRight(1), Stream.runCollect);

        expect(Array.from(events)).toStrictEqual([
          {
            type: "status",
            topic: "orders",
            queryId: "remote",
            status: "closed",
            code: "BackpressureExceeded",
            message: "Remote subscription buffer exceeded capacity with 1 queued event(s).",
          },
        ]);
        yield* Effect.sleep("10 millis");
        expect(server.activeSubscriptions()).toBe(0);

        yield* client.close;
        yield* server.close;
      }),
  );

  it.live("closes with typed backpressure status when the remote client buffer size is zero", () =>
    Effect.gen(function* () {
      const server = yield* makeTestRpcServer();
      const client = yield* makeViewServerClient(viewServer, {
        subscriptionBufferSize: 0,
        url: server.url,
      });
      const subscription = yield* client.subscribe("orders", {
        select: ["id", "price"],
        limit: 10,
      });

      yield* server.emitInsert("orders", order("first", 1));
      yield* server.emitInsert("orders", order("second", 2));
      const events = yield* subscription.events.pipe(Stream.takeRight(1), Stream.runCollect);

      expect(Array.from(events)).toStrictEqual([
        {
          type: "status",
          topic: "orders",
          queryId: "remote",
          status: "closed",
          code: "BackpressureExceeded",
          message: "Remote subscription buffer exceeded capacity with 1 queued event(s).",
        },
      ]);
      yield* Effect.sleep("10 millis");
      expect(server.activeSubscriptions()).toBe(0);

      yield* client.close;
      yield* server.close;
    }),
  );

  it.live(
    "closes with typed backpressure status when the remote client buffer size is not finite",
    () =>
      Effect.gen(function* () {
        const server = yield* makeTestRpcServer();
        const client = yield* makeViewServerClient(viewServer, {
          subscriptionBufferSize: Number.NaN,
          url: server.url,
        });
        const subscription = yield* client.subscribe("orders", {
          select: ["id", "price"],
          limit: 10,
        });

        yield* server.emitInsert("orders", order("first", 1));
        yield* server.emitInsert("orders", order("second", 2));
        const events = yield* subscription.events.pipe(Stream.takeRight(1), Stream.runCollect);

        expect(Array.from(events)).toStrictEqual([
          {
            type: "status",
            topic: "orders",
            queryId: "remote",
            status: "closed",
            code: "BackpressureExceeded",
            message: "Remote subscription buffer exceeded capacity with 1 queued event(s).",
          },
        ]);
        yield* Effect.sleep("10 millis");
        expect(server.activeSubscriptions()).toBe(0);

        yield* client.close;
        yield* server.close;
      }),
  );

  it.live("does not wait for health refresh before delivering the first live event", () =>
    Effect.gen(function* () {
      const server = yield* makeTestRpcServer();
      const client = yield* makeViewServerClient(viewServer, {
        url: server.url,
      });
      server.setHealthDelay("500 millis");
      const subscription = yield* client.subscribe("orders", {
        select: ["id"],
        limit: 10,
      });

      const startedAt = yield* Clock.currentTimeMillis;
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);
      const finishedAt = yield* Clock.currentTimeMillis;

      expect(finishedAt - startedAt).toBeLessThan(250);
      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-remote",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      yield* Effect.sleep("10 millis");
      expect(server.healthRequests()).toBe(1);

      server.setHealthDelay("0 millis");
      yield* subscription.close();
      yield* client.close;
      yield* server.close;
    }),
  );

  it.live("closes the remote subscription scope when stream consumption finalizes", () =>
    Effect.gen(function* () {
      const server = yield* makeTestRpcServer();
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      const subscription = yield* client.subscribe("orders", {
        select: ["id"],
        limit: 10,
      });
      yield* Effect.sleep("10 millis");
      expect(server.activeSubscriptions()).toBe(1);

      yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);
      yield* Effect.sleep("10 millis");

      expect(server.activeSubscriptions()).toBe(0);
      expect(client.health.value.engine.topics.orders.activeSubscriptions).toBe(0);

      yield* client.close;
      yield* server.close;
    }),
  );

  it.live("subscribes to pushed remote health summary", () =>
    Effect.gen(function* () {
      const server = yield* makeTestRpcServer();
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      const subscription = yield* client.subscribeHealthSummary();
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "query-remote",
        version: 1,
        keys: ["summary"],
        rows: [
          {
            id: "summary",
            status: "ready",
            runtimeStatus: "ready",
            connectionStatus: "connected",
            unhealthyTopics: [],
            updatedAtNanos: 1n,
            maxKafkaLag: 0n,
          },
        ],
        totalRows: 1,
      });

      yield* subscription.close();
      yield* client.close;
      yield* server.close;
    }),
  );

  it.live("subscribes to pushed detailed remote health rows", () =>
    Effect.gen(function* () {
      const server = yield* makeTestRpcServer();
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      const subscription = yield* client.subscribeHealth();
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "query-remote",
        version: 1,
        keys: ["orders"],
        rows: [
          {
            id: "orders",
            status: "ready",
            rowCount: 0,
            liveRowCount: 0,
            deletedRowCount: 0,
            version: 0,
            lastMutationAt: null,
            mutationsPerSecond: 0,
            rowsPerSecond: 0,
            pendingMutationBatches: 0,
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: 0,
            activeViews: 0,
            groupedFullEvaluationCount: 0,
            groupedPatchedEvaluationCount: 0,
            activeSubscriptions: 0,
            queuedEvents: 0,
            maxQueueDepth: 0,
            backpressureEvents: 0,
            memoryBytes: 0,
            tombstoneCount: 0,
            compactionPending: false,
            kafkaLag: 0n,
            updatedAtNanos: 1n,
          },
        ],
        totalRows: 1,
      });

      yield* subscription.close();
      yield* client.close;
      yield* server.close;
    }),
  );

  it.live("updates remote health refs from pushed health streams", () =>
    Effect.gen(function* () {
      const server = yield* makeTestRpcServer();
      server.setHealthSummaryRows([
        { ...healthSummaryWireRow(), status: "degraded", runtimeStatus: "degraded" },
      ]);
      server.setHealthTopicRows([{ ...healthTopicWireRow(), status: "stopping" }]);
      server.setHealth({
        ...health(0, 0),
        status: "degraded",
      });
      const client = yield* makeViewServerClient(viewServer, { url: server.url });

      const summarySubscription = yield* client.subscribeHealthSummary();
      const summaryEventsFiber = yield* summarySubscription.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.sleep("10 millis");
      expect(client.health.value.status).toBe("degraded");
      expect(server.healthRequests()).toBe(1);
      const refreshedHealth = health(0, 0);
      const ignoredKafka = {
        startFrom: {
          consumerGroupId: "view-server-test",
          fallbackMode: "earliest",
          mode: "committed",
        },
        regions: {
          usa: {
            status: "connected",
            brokers: "localhost:9092",
            lastConnectedAt: 10,
            lastError: null,
          },
        },
        topics: {
          sourceOrders: {
            status: "ready",
            sourceTopic: "orders-source",
            viewServerTopic: "orders",
            regions: {
              usa: {
                connected: true,
                assignedPartitions: 3,
                messagesPerSecond: 21,
                bytesPerSecond: 33,
                decodedMessagesPerSecond: 20,
                decodeFailuresPerSecond: 1,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                commitFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: 60,
                lastCommitAt: 50,
                consumerLagMessages: 9n,
                lagSampledAt: 70,
                committedOffset: "91",
                lastError: null,
              },
            },
          },
        },
      } satisfies NonNullable<ViewServerWireHealth["kafka"]>;
      server.setHealth({
        ...refreshedHealth,
        version: 42,
        uptimeMs: 1234,
        kafka: ignoredKafka,
        transport: {
          ...refreshedHealth.transport,
          activeClients: 3,
          messagesPerSecond: 44,
          lastError: "previous slow client",
        },
      });
      yield* server.emitHealthSummary({
        type: "delta",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "query-remote",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "update",
            key: "summary",
            row: healthSummaryWireRow(),
            index: 0,
          },
          {
            type: "move",
            key: "summary",
            fromIndex: 0,
            toIndex: 0,
          },
        ],
        totalRows: 1,
      });
      yield* Fiber.join(summaryEventsFiber);
      yield* Effect.sleep("50 millis");
      expect(client.health.value.status).toBe("ready");
      expect(client.health.value.version).toBe(0);
      expect(client.health.value.uptimeMs).toBe(0);
      expect(client.health.value.transport.activeClients).toBe(1);
      expect(client.health.value.transport.messagesPerSecond).toBe(0);
      expect(client.health.value.transport.lastError).toBe(null);
      expect(client.health.value.kafka).toBe(undefined);
      expect(server.healthRequests()).toBe(1);
      yield* summarySubscription.close();

      const healthSubscription = yield* client.subscribeHealth();
      const detailEventsFiber = yield* healthSubscription.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.sleep("10 millis");
      expect(client.health.value.engine.topics.orders.status).toBe("ready");
      yield* server.emitHealthTopic({
        type: "delta",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "query-remote",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "update",
            key: "orders",
            row: { ...healthTopicWireRow(), rowCount: 25 },
            index: 0,
          },
          {
            type: "move",
            key: "orders",
            fromIndex: 0,
            toIndex: 0,
          },
        ],
        totalRows: 1,
      });
      yield* Fiber.join(detailEventsFiber);
      expect(client.health.value.engine.topics.orders.rowCount).toBe(25);
      yield* healthSubscription.close();

      yield* client.close;
      yield* server.close;
    }),
  );

  it.live("decodes Kafka health counters from initial remote health", () =>
    Effect.gen(function* () {
      const server = yield* makeTestRpcServer();
      server.setHealth({
        ...health(0, 0),
        kafka: kafkaHealth(),
      });

      const client = yield* makeViewServerClient(viewServer, { url: server.url });

      expect(client.health.value.kafka).toStrictEqual({
        startFrom: {
          consumerGroupId: "view-server-test",
          fallbackMode: "earliest",
          mode: "committed",
        },
        regions: {
          usa: {
            status: "connected",
            brokers: "localhost:9092",
            lastConnectedAt: 10,
            lastError: null,
          },
        },
        topics: {
          sourceOrders: {
            status: "degraded",
            sourceTopic: "orders-source",
            viewServerTopic: "orders",
            regions: {
              usa: {
                connected: true,
                assignedPartitions: 3,
                messagesPerSecond: 23,
                bytesPerSecond: 33,
                decodedMessagesPerSecond: 20,
                decodeFailuresPerSecond: 1,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 1,
                commitFailuresPerSecond: 1,
                processingFailuresPerSecond: 2,
                lastMessageAt: 60,
                lastCommitAt: 50,
                consumerLagMessages: 9n,
                lagSampledAt: 70,
                committedOffset: "91",
                lastError: "commit failed",
              },
            },
          },
        },
      });

      yield* client.close;
      yield* server.close;
    }),
  );

  it.live("ignores non-snapshot and empty pushed health events for health refs", () =>
    Effect.gen(function* () {
      const summaryServer = yield* makeTestRpcServer();
      summaryServer.failHealthSummary({
        _tag: "ViewServerTransportError",
        code: "TransportError",
        message: "summary stream failed",
      });
      const summaryClient = yield* makeViewServerClient(viewServer, { url: summaryServer.url });
      const summarySubscription = yield* summaryClient.subscribeHealthSummary();
      yield* summarySubscription.events.pipe(Stream.take(1), Stream.runDrain);
      expect(summaryClient.health.value.status).toBe("ready");
      yield* summarySubscription.close();
      yield* summaryClient.close;
      yield* summaryServer.close;

      const detailServer = yield* makeTestRpcServer();
      detailServer.setHealthSummaryRows([]);
      detailServer.failHealthTopic({
        _tag: "ViewServerTransportError",
        code: "TransportError",
        message: "detail stream failed",
      });
      const detailClient = yield* makeViewServerClient(viewServer, { url: detailServer.url });
      const emptySummarySubscription = yield* detailClient.subscribeHealthSummary();
      yield* emptySummarySubscription.events.pipe(Stream.take(1), Stream.runDrain);
      expect(detailClient.health.value.status).toBe("ready");
      yield* emptySummarySubscription.close();
      const detailSubscription = yield* detailClient.subscribeHealth();
      yield* detailSubscription.events.pipe(Stream.take(1), Stream.runDrain);
      expect(detailClient.health.value.engine.topics.orders.status).toBe("ready");
      yield* detailSubscription.close();
      yield* detailClient.close;
      yield* detailServer.close;
    }),
  );

  it.live("client close closes active remote subscription scopes", () =>
    Effect.gen(function* () {
      const server = yield* makeTestRpcServer();
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      const subscription = yield* client.subscribe("orders", {
        select: ["id"],
        limit: 10,
      });
      yield* Effect.sleep("10 millis");
      expect(server.activeSubscriptions()).toBe(1);
      expect(client.health.value.engine.topics.orders.activeSubscriptions).toBe(1);
      expect(client.health.value.transport.activeSubscriptions).toBe(1);

      yield* client.close;
      yield* Effect.sleep("10 millis");

      expect(server.activeSubscriptions()).toBe(0);
      expect(client.health.value.engine.topics.orders.activeSubscriptions).toBe(0);
      expect(client.health.value.transport.activeSubscriptions).toBe(0);

      yield* subscription.close();
      yield* server.close;
    }),
  );

  it.live("updates local live query counters without collapsing active views", () =>
    Effect.gen(function* () {
      const server = yield* makeTestRpcServer();
      server.setHealth({
        ...health(0, 2),
        engine: {
          topics: {
            orders: {
              ...topicHealth(0, 2),
              activeFallbackGroupedViews: 0,
              activeIncrementalGroupedViews: 0,
              activeViews: 7,
              groupedFullEvaluationCount: 0,
              groupedPatchedEvaluationCount: 0,
            },
          },
        },
      });
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      const subscription = yield* client.subscribe("orders", {
        select: ["id"],
        limit: 10,
      });
      yield* Effect.sleep("10 millis");

      expect(client.health.value.engine.topics.orders.activeViews).toBe(7);
      expect(client.health.value.engine.topics.orders.activeSubscriptions).toBe(3);
      expect(client.health.value.transport.activeStreams).toBe(3);
      expect(client.health.value.transport.activeSubscriptions).toBe(3);

      yield* subscription.close();
      expect(client.health.value.engine.topics.orders.activeViews).toBe(7);
      expect(client.health.value.engine.topics.orders.activeSubscriptions).toBe(2);
      expect(client.health.value.transport.activeStreams).toBe(2);
      expect(client.health.value.transport.activeSubscriptions).toBe(2);

      yield* client.close;
      yield* server.close;
    }),
  );

  it.live("encodes query filters and surfaces client-side validation errors", () =>
    Effect.gen(function* () {
      const server = yield* makeTestRpcServer();
      const client = yield* makeViewServerClient(viewServer, { url: server.url });

      const richQuery = yield* client.subscribe("orders", {
        select: ["id", "price"],
        where: {
          id: {
            in: ["a", "b"],
            startsWith: "a",
          },
          price: { gt: 1 },
        },
        orderBy: [{ field: "price", direction: "desc" }],
        offset: 1,
        limit: 10,
      });
      yield* Effect.sleep("10 millis");
      expect(server.lastSubscribeQuery()).toStrictEqual({
        select: ["id", "price"],
        where: {
          id: {
            in: ["a", "b"],
            startsWith: "a",
          },
          price: { gt: 1 },
        },
        orderBy: [{ field: "price", direction: "desc" }],
        offset: 1,
        limit: 10,
      });
      yield* richQuery.close();

      const scalarFilter = yield* client.subscribe("orders", {
        select: ["id"],
        where: {
          price: 10,
        },
        limit: 10,
      });
      yield* Effect.sleep("10 millis");
      expect(server.lastSubscribeQuery()).toStrictEqual({
        select: ["id"],
        where: {
          price: 10,
        },
        limit: 10,
      });
      yield* scalarFilter.close();

      const noLimit = yield* client.subscribe("orders", {
        select: ["id"],
      });
      yield* Effect.sleep("10 millis");
      expect(server.lastSubscribeQuery()).toStrictEqual({
        select: ["id"],
      });
      yield* noLimit.close();

      const invalidTopic = yield* Effect.flip(
        // @ts-expect-error hostile callers can still send unknown topics.
        client.subscribe("missing", {
          select: ["id"],
        }),
      );
      expect(invalidTopic.code).toBe("InvalidTopic");

      const invalidSelect = yield* Effect.flip(
        client.subscribe("orders", {
          // @ts-expect-error hostile callers can still send malformed selected fields.
          select: [1],
        }),
      );
      expect(invalidSelect.code).toBe("InvalidQuery");
      expect(invalidSelect.message).toBe('Expected string, got 1\n  at ["select"][0]');

      const unknownSelect = yield* Effect.flip(
        client.subscribe("orders", {
          // @ts-expect-error hostile callers can still send unknown selected fields.
          select: ["missing"],
        }),
      );
      expect(unknownSelect.code).toBe("InvalidQuery");
      expect(unknownSelect.message).toBe("Query references an unknown field for topic: orders");

      const unknownOrderBy = yield* Effect.flip(
        client.subscribe("orders", {
          // @ts-expect-error invalid query collapse keeps selected fields from being accepted.
          select: ["id"],
          orderBy: [
            {
              // @ts-expect-error hostile callers can still send unknown sort fields.
              field: "missing",
              direction: "asc",
            },
          ],
        }),
      );
      expect(unknownOrderBy.code).toBe("InvalidQuery");
      expect(unknownOrderBy.message).toBe("Query references an unknown field for topic: orders");

      const unknownWhere = yield* Effect.flip(
        client.subscribe("orders", {
          // @ts-expect-error invalid query collapse keeps selected fields from being accepted.
          select: ["id"],
          where: {
            // @ts-expect-error hostile callers can still send unknown filter fields.
            missing: { eq: "x" },
          },
        }),
      );
      expect(unknownWhere.code).toBe("InvalidQuery");
      expect(unknownWhere.message).toBe("Query references an unknown field for topic: orders");

      const invalidFilter = yield* Effect.flip(
        client.subscribe("orders", {
          // @ts-expect-error invalid query collapse keeps selected fields from being accepted.
          select: ["id"],
          where: {
            price: {
              // @ts-expect-error hostile callers can still send malformed filter values.
              gt: "nope",
            },
          },
        }),
      );
      expect(invalidFilter.code).toBe("InvalidQuery");

      const invalidStartsWith = yield* Effect.flip(
        client.subscribe("orders", {
          // @ts-expect-error invalid query collapse keeps selected fields from being accepted.
          select: ["id"],
          where: {
            id: {
              // @ts-expect-error hostile callers can still send non-JSON filter values.
              startsWith: 1n,
            },
          },
        }),
      );
      expect(invalidStartsWith.code).toBe("InvalidQuery");

      const invalidNumericStartsWith = yield* Effect.flip(
        client.subscribe("orders", {
          // @ts-expect-error invalid query collapse keeps selected fields from being accepted.
          select: ["id"],
          where: {
            price: {
              // @ts-expect-error hostile callers can still send string operators to numeric fields.
              startsWith: 1,
            },
          },
        }),
      );
      expect(invalidNumericStartsWith.code).toBe("InvalidQuery");
      expect(invalidNumericStartsWith.message).toBe("Filter price does not support startsWith");

      const invalidUnknownOperator = yield* Effect.flip(
        client.subscribe("orders", {
          select: ["id"],
          where: {
            id: {
              startsWith: "a",
              // @ts-expect-error hostile callers can still send unknown filter operators.
              weird: 1n,
            },
          },
        }),
      );
      expect(invalidUnknownOperator.code).toBe("InvalidQuery");

      const emptySelectQuery: object = {
        select: [],
      };
      const emptySelect = yield* Effect.flip(
        client.subscribe(
          "orders",
          // @ts-expect-error hostile callers can still send empty projections.
          emptySelectQuery,
        ),
      );
      expect(emptySelect.code).toBe("InvalidQuery");
      expect(emptySelect.message).toBe("Query select must include at least one field");

      const invalidOffset = yield* Effect.flip(
        client.subscribe("orders", {
          select: ["id"],
          offset: -1,
        }),
      );
      expect(invalidOffset.code).toBe("InvalidQuery");
      expect(invalidOffset.message).toBe("Query offset must be a non-negative integer");

      const invalidLimit = yield* Effect.flip(
        client.subscribe("orders", {
          select: ["id"],
          limit: -1,
        }),
      );
      expect(invalidLimit.code).toBe("InvalidQuery");
      expect(invalidLimit.message).toBe("Query limit must be a non-negative integer");

      yield* client.close;
      yield* server.close;
    }),
  );

  it.live("rejects non-json schema encodings before RPC", () =>
    Effect.gen(function* () {
      const server = yield* makeTestRpcServer();
      server.setHealth({
        ...health(0, 0),
        engine: {
          topics: {
            badjson: topicHealth(0, 0),
          },
        },
      });
      const client = yield* makeViewServerClient(edgeViewServer, { url: server.url });

      const badFilter = yield* Effect.flip(
        client.subscribe("badjson", {
          select: ["id"],
          where: { id: { eq: "x" } },
        }),
      );
      expect(badFilter.code).toBe("InvalidQuery");
      expect(badFilter.message).toMatch(/Filter id is not JSON-safe/);

      const badStartsWith = yield* Effect.flip(
        client.subscribe("badjson", {
          // @ts-expect-error hostile callers can still send invalid selected fields.
          select: ["id"],
          where: {
            id: {
              // @ts-expect-error hostile callers can still send non-string startsWith filters.
              startsWith: 1,
            },
          },
        }),
      );
      expect(badStartsWith.code).toBe("InvalidQuery");
      expect(badStartsWith.message).toBe("Invalid filter for id: expected string");

      yield* client.close;
      yield* server.close;
    }),
  );

  it.live("returns a transport error and disposes setup when initial health fails", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        makeViewServerClient(viewServer, { url: "ws://127.0.0.1:1/rpc" }),
      );

      expect(error.code).toBe("TransportError");
    }),
  );

  it.live("rejects remote health payloads that omit configured topics", () =>
    Effect.gen(function* () {
      const server = yield* makeTestRpcServer();
      server.setHealth({
        ...health(0, 0),
        engine: {
          topics: {
            badjson: topicHealth(0, 0),
          },
        },
      });

      const error = yield* Effect.flip(makeViewServerClient(viewServer, { url: server.url }));
      expect(error.code).toBe("InvalidRow");
      expect(error.message).toBe("Health payload is missing topic: orders");

      yield* server.close;
    }),
  );

  it.live("rejects remote health payloads with extra unknown topics", () =>
    Effect.gen(function* () {
      const server = yield* makeTestRpcServer();
      server.setHealth({
        ...health(0, 0),
        engine: {
          topics: {
            orders: topicHealth(0, 0),
            missing: topicHealth(0, 0),
          },
        },
      });

      const error = yield* Effect.flip(makeViewServerClient(viewServer, { url: server.url }));
      expect(error.code).toBe("InvalidRow");
      expect(error.message).toBe("Health payload references unknown topic: missing");

      yield* server.close;
    }),
  );

  it.live("maps decoded remote event failures into typed terminal statuses", () =>
    Effect.gen(function* () {
      const server = yield* makeTestRpcServer();
      const client = yield* makeViewServerClient(viewServer, { url: server.url });

      const unknownFieldSubscription = yield* client.subscribe("orders", {
        select: ["id"],
        limit: 10,
      });
      const unknownFieldEventsFiber = yield* unknownFieldSubscription.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.sleep("10 millis");
      yield* server.emit({
        type: "snapshot",
        topic: "orders",
        queryId: "query-remote",
        version: 1,
        keys: ["bad"],
        rows: [{ id: "bad", missing: "x" }],
        totalRows: 1,
      });
      const unknownFieldEvents = yield* Fiber.join(unknownFieldEventsFiber);
      expect(unknownFieldEvents[1]).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "remote",
        status: "error",
        code: "InvalidRow",
        message: "Unexpected row field for topic orders: missing",
      });

      const invalidTypeSubscription = yield* client.subscribe("orders", {
        select: ["id", "price"],
        limit: 10,
      });
      const invalidTypeEventsFiber = yield* invalidTypeSubscription.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.sleep("10 millis");
      yield* server.emit({
        type: "snapshot",
        topic: "orders",
        queryId: "query-remote",
        version: 1,
        keys: ["bad"],
        rows: [{ id: "bad", price: "nope" }],
        totalRows: 1,
      });
      const invalidTypeEvents = yield* Fiber.join(invalidTypeEventsFiber);
      expect(invalidTypeEvents[1]).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "remote",
        status: "error",
        code: "InvalidRow",
        message: 'Invalid field price: Expected "Infinity" | "-Infinity" | "NaN", got "nope"',
      });

      const invalidTopicSubscription = yield* client.subscribe("orders", {
        select: ["id"],
        limit: 10,
      });
      const invalidTopicEventsFiber = yield* invalidTopicSubscription.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.sleep("10 millis");
      yield* server.emit({
        type: "snapshot",
        topic: "missing",
        queryId: "query-remote",
        version: 1,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      const invalidTopicEvents = yield* Fiber.join(invalidTopicEventsFiber);
      expect(invalidTopicEvents[1]).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "remote",
        status: "error",
        code: "InvalidRow",
        message: "Received event for missing while subscribed to orders",
      });

      const missingFieldSubscription = yield* client.subscribe("orders", {
        select: ["id", "price"],
        limit: 10,
      });
      const missingFieldEventsFiber = yield* missingFieldSubscription.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.sleep("10 millis");
      yield* server.emit({
        type: "snapshot",
        topic: "orders",
        queryId: "query-remote",
        version: 1,
        keys: ["bad"],
        rows: [{ id: "bad" }],
        totalRows: 1,
      });
      const missingFieldEvents = yield* Fiber.join(missingFieldEventsFiber);
      expect(missingFieldEvents[1]).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "remote",
        status: "error",
        code: "InvalidRow",
        message: "Missing row field for topic orders: price",
      });

      const statusAndMoveSubscription = yield* client.subscribe("orders", {
        select: ["id"],
        limit: 10,
      });
      const statusAndMoveEventsFiber = yield* statusAndMoveSubscription.events.pipe(
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.sleep("10 millis");
      yield* server.emit({
        type: "status",
        topic: "orders",
        queryId: "query-remote",
        status: "ready",
        code: "Ready",
      });
      yield* server.emit({
        type: "delta",
        topic: "orders",
        queryId: "query-remote",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          { type: "move", key: "a", fromIndex: 1, toIndex: 0 },
          { type: "remove", key: "b" },
        ],
        totalRows: 0,
      });
      const statusAndMoveEvents = yield* Fiber.join(statusAndMoveEventsFiber);
      expect(statusAndMoveEvents[1]).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "query-remote",
        status: "ready",
        code: "Ready",
      });
      expect(statusAndMoveEvents[2]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-remote",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          { type: "move", key: "a", fromIndex: 1, toIndex: 0 },
          { type: "remove", key: "b" },
        ],
        totalRows: 0,
      });

      yield* client.close;
      yield* server.close;
    }),
  );

  it.live("turns remote stream failures into terminal status events", () =>
    Effect.gen(function* () {
      const server = yield* makeTestRpcServer();
      const client = yield* makeViewServerClient(viewServer, { url: server.url });

      const subscriptionClosedSubscription = yield* client.subscribe("orders", {
        select: ["id"],
        limit: 994,
      });
      const subscriptionClosedStatus = yield* subscriptionClosedSubscription.events.pipe(
        Stream.take(1),
        Stream.runCollect,
      );
      expect(subscriptionClosedStatus[0]).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "remote",
        status: "closed",
        code: "SubscriptionClosed",
        message: "subscription closed by server",
      });

      const snapshotStaleSubscription = yield* client.subscribe("orders", {
        select: ["id"],
        limit: 995,
      });
      const snapshotStaleStatus = yield* snapshotStaleSubscription.events.pipe(
        Stream.take(1),
        Stream.runCollect,
      );
      expect(snapshotStaleStatus[0]).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "remote",
        status: "stale",
        code: "SnapshotStale",
        message: "snapshot stale",
      });

      const invalidQuerySubscription = yield* client.subscribe("orders", {
        select: ["id"],
        limit: 996,
      });
      const invalidQueryStatus = yield* invalidQuerySubscription.events.pipe(
        Stream.take(1),
        Stream.runCollect,
      );
      expect(invalidQueryStatus[0]).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "remote",
        status: "error",
        code: "InvalidQuery",
        message: "remote invalid query",
      });

      const transportSubscription = yield* client.subscribe("orders", {
        select: ["id"],
        limit: 997,
      });
      const transportStatus = yield* transportSubscription.events.pipe(
        Stream.take(1),
        Stream.runCollect,
      );
      expect(transportStatus[0]).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "remote",
        status: "error",
        code: "TransportError",
        message: "transport failure without query",
      });

      const identifiedTransportSubscription = yield* client.subscribe("orders", {
        select: ["id"],
        limit: 998,
      });
      const identifiedTransportStatus = yield* identifiedTransportSubscription.events.pipe(
        Stream.take(1),
        Stream.runCollect,
      );
      expect(identifiedTransportStatus[0]).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "query-from-server",
        status: "error",
        code: "TransportError",
        message: "transport failure",
      });

      const backpressureSubscription = yield* client.subscribe("orders", {
        select: ["id"],
        limit: 999,
      });
      const backpressureStatus = yield* backpressureSubscription.events.pipe(
        Stream.take(1),
        Stream.runCollect,
      );
      expect(backpressureStatus[0]).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "remote",
        status: "closed",
        code: "BackpressureExceeded",
        message: "backpressure failure",
      });

      yield* client.close;
      yield* server.close;
    }),
  );
});
