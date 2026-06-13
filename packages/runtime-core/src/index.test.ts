import { describe, expect, it } from "@effect/vitest";
import {
  createColumnLiveViewEngine,
  type ColumnLiveViewEngineHealth,
} from "@view-server/column-live-view-engine";
import { defineViewServerConfig, type ViewServerRuntimeError } from "@view-server/config";
import { Deferred, Effect, Fiber, Queue, Schema, Stream } from "effect";
import { AtomRef } from "effect/unstable/reactivity";
import {
  healthFromEngine,
  makeCoalescedHealthReader,
  makeHealthRefreshScheduler,
  readHealth,
} from "./health";
import { createViewServerRuntimeCore, makeViewServerRuntimeCore } from "./index";
import { makeRuntimeCoreLiveClient } from "./live-client";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

type OrderRow = typeof Order.Type;
type Topics = typeof viewServer.topics;

const order = (id: string, price: number): OrderRow => ({
  id,
  customerId: `customer-${id}`,
  status: "open",
  price,
  region: "usa",
  updatedAt: price,
});

const refreshFailed: ViewServerRuntimeError = {
  _tag: "ViewServerRuntimeError",
  code: "RuntimeUnavailable",
  message: "Health refresh failed.",
};

const engineHealth = (
  status: "ready" | "stopping",
  rowCount: number,
): ColumnLiveViewEngineHealth<Topics> => ({
  status,
  version: status === "ready" ? 1 : 2,
  topics: {
    orders: {
      status: status === "ready" ? "ready" : "degraded",
      rowCount,
      liveRowCount: rowCount,
      deletedRowCount: 0,
      version: status === "ready" ? 1 : 2,
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
    },
  },
  activeSubscriptions: 0,
  queuedEvents: 0,
  maxQueueDepth: 0,
  backpressureEvents: 0,
});

describe("@view-server/runtime-core", () => {
  it.effect("runs the shared runtime core and live client", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const subscription = yield* runtimeCore.liveClient.subscribe("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);
      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });

      yield* runtimeCore.client.publishMany("orders", [order("b", 20), order("a", 10)]);
      yield* runtimeCore.client.publish("orders", order("c", 30));
      yield* runtimeCore.client.patch("orders", "c", { price: 5 });
      yield* runtimeCore.client.delete("orders", "a");

      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      expect(snapshot.rows).toStrictEqual([
        { id: "c", price: 5 },
        { id: "b", price: 20 },
      ]);

      const health = yield* runtimeCore.client.health();
      expect(health.engine.topics.orders.rowCount).toBe(2);
      const refreshedHealth = yield* runtimeCore.refreshHealth;
      expect(refreshedHealth.engine.topics.orders.rowCount).toBe(2);

      yield* subscription.close();
      yield* runtimeCore.client.reset();
      const resetHealth = yield* runtimeCore.client.health();
      expect(resetHealth.engine.topics.orders.rowCount).toBe(0);
      yield* runtimeCore.close;
    }),
  );

  it.effect("supports the synchronous runtime core constructor", () =>
    Effect.gen(function* () {
      const runtimeCore = createViewServerRuntimeCore(viewServer, { subscriptionQueueCapacity: 1 });
      expect("set" in runtimeCore.liveClient.health).toBe(false);
      expect(runtimeCore.liveClient.health.value.status).toBe("ready");

      yield* runtimeCore.client.publish("orders", order("a", 10));
      const health = yield* runtimeCore.client.health();

      expect(health.engine.topics.orders.rowCount).toBe(1);
      expect(health.transport).toStrictEqual({
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
      yield* runtimeCore.close;
    }),
  );

  it.effect("supports the synchronous runtime core constructor defaults", () =>
    Effect.gen(function* () {
      const runtimeCore = createViewServerRuntimeCore(viewServer);

      yield* runtimeCore.client.publish("orders", order("a", 10));
      const health = yield* runtimeCore.client.health();

      expect(health.engine.topics.orders.rowCount).toBe(1);
      yield* runtimeCore.close;
    }),
  );

  it.effect("forwards grouped admission limits to the engine", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        groupedIncrementalAdmissionLimits: {
          maxGroups: 1,
        },
      });
      yield* runtimeCore.client.publishMany("orders", [order("a", 10), order("b", 20)]);
      const subscription = yield* runtimeCore.liveClient.subscribe("orders", {
        groupBy: ["price"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });

      const health = yield* runtimeCore.client.health();
      expect(health.engine.topics.orders.activeFallbackGroupedViews).toBe(1);
      expect(health.engine.topics.orders.activeIncrementalGroupedViews).toBe(0);

      yield* subscription.close();
      yield* runtimeCore.close;
    }),
  );

  it.effect("subscribes through the runtime live-client entrypoint", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      yield* runtimeCore.client.publish("orders", order("a", 10));

      const subscription = yield* runtimeCore.liveClient.subscribeRuntime("orders", {
        select: ["id", "price"],
      });
      const events = yield* subscription.events.pipe(Stream.take(1), Stream.runCollect);

      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["a"],
        rows: [{ id: "a", price: 10 }],
        totalRows: 1,
      });

      yield* subscription.close();
      const health = yield* runtimeCore.client.health();
      expect(health.engine.topics.orders.activeSubscriptions).toBe(0);
      yield* runtimeCore.close;
    }),
  );

  it.effect("releases acquired live subscriptions when initial health refresh fails", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngine({ topics: viewServer.topics });
      const health = AtomRef.make(healthFromEngine(yield* engine.health()));
      const liveClient = yield* makeRuntimeCoreLiveClient(
        engine,
        health,
        Effect.fail(refreshFailed),
      );

      const failedRaw = yield* Effect.flip(
        liveClient.subscribe("orders", {
          select: ["id"],
          limit: 1,
        }),
      );
      const afterRawFailure = yield* engine.health();
      expect(failedRaw).toStrictEqual(refreshFailed);
      expect(afterRawFailure.activeSubscriptions).toBe(0);

      const failedRuntime = yield* Effect.flip(
        liveClient.subscribeRuntime("orders", {
          select: ["id"],
          limit: 1,
        }),
      );
      const afterRuntimeFailure = yield* engine.health();
      expect(failedRuntime).toStrictEqual(refreshFailed);
      expect(afterRuntimeFailure.activeSubscriptions).toBe(0);

      yield* liveClient.close;
    }),
  );

  it.effect("releases pushed health subscriptions when initial health refresh fails", () =>
    Effect.gen(function* () {
      const engine = yield* createColumnLiveViewEngine({ topics: viewServer.topics });
      const health = AtomRef.make(healthFromEngine(yield* engine.health()));
      const liveClient = yield* makeRuntimeCoreLiveClient(
        engine,
        health,
        Effect.fail(refreshFailed),
      );

      const failedSummary = yield* Effect.flip(liveClient.subscribeHealthSummary());
      const failedDetail = yield* Effect.flip(liveClient.subscribeHealth());

      expect(failedSummary).toStrictEqual(refreshFailed);
      expect(failedDetail).toStrictEqual(refreshFailed);
      yield* liveClient.close.pipe(Effect.timeout("1 second"));
    }),
  );

  it.effect("refreshes health after close", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        healthRefreshCadence: "0 millis",
      });
      yield* runtimeCore.client.publish("orders", order("a", 10));

      const ready = yield* runtimeCore.client.health();
      expect(ready.status).toBe("ready");

      yield* runtimeCore.liveClient.close;

      const closed = yield* runtimeCore.client.health();
      expect(closed.status).toBe("stopping");
      expect(closed.engine.topics.orders.activeSubscriptions).toBe(0);
    }),
  );

  it.effect("live client close owns pending runtime health refresh cleanup", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        healthRefreshCadence: "1 minute",
      });

      yield* runtimeCore.client.publish("orders", order("a", 10));
      yield* runtimeCore.liveClient.close;

      const closed = yield* runtimeCore.client.health();
      expect(closed.status).toBe("stopping");
      expect(closed.engine.topics.orders.rowCount).toBe(1);
      yield* runtimeCore.close;
    }),
  );

  it.effect("pushes summary and detailed health snapshots", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        healthRefreshCadence: "0 millis",
      });
      const summary = yield* runtimeCore.liveClient.subscribeHealthSummary();
      const detail = yield* runtimeCore.liveClient.subscribeHealth();

      const summaryFiber = yield* summary.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      const detailFiber = yield* detail.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* runtimeCore.client.publish("orders", order("a", 10));

      const summaryEvents = yield* Fiber.join(summaryFiber);
      const detailEvents = yield* Fiber.join(detailFiber);
      expect(Array.from(summaryEvents)).toStrictEqual([
        {
          type: "snapshot",
          topic: "__view_server_health_summary",
          queryId: "health-summary",
          version: 0,
          keys: ["summary"],
          rows: [
            {
              id: "summary",
              status: "ready",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              unhealthyTopics: [],
              updatedAtNanos: expect.anything(),
              maxKafkaLag: 0n,
            },
          ],
          totalRows: 1,
        },
        {
          type: "snapshot",
          topic: "__view_server_health_summary",
          queryId: "health-summary",
          version: 1,
          keys: ["summary"],
          rows: [
            {
              id: "summary",
              status: "ready",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              unhealthyTopics: [],
              updatedAtNanos: expect.anything(),
              maxKafkaLag: 0n,
            },
          ],
          totalRows: 1,
        },
      ]);
      expect(Array.from(detailEvents)).toStrictEqual([
        {
          type: "snapshot",
          topic: "__view_server_health",
          queryId: "health",
          version: 0,
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
              updatedAtNanos: expect.anything(),
            },
          ],
          totalRows: 1,
        },
        {
          type: "snapshot",
          topic: "__view_server_health",
          queryId: "health",
          version: 1,
          keys: ["orders"],
          rows: [
            {
              id: "orders",
              status: "ready",
              rowCount: 1,
              liveRowCount: 1,
              deletedRowCount: 0,
              version: 1,
              lastMutationAt: expect.anything(),
              mutationsPerSecond: 1,
              rowsPerSecond: 1,
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
              updatedAtNanos: expect.anything(),
            },
          ],
          totalRows: 1,
        },
      ]);

      yield* summary.close();
      yield* summary.close();
      yield* detail.close();
      yield* detail.close();
      yield* runtimeCore.close;
    }),
  );

  it.effect("applies health overlays to pushed health subscriptions", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {
        healthOverlay: (health) => ({
          ...health,
          status: "degraded",
          kafka: {
            startFrom: {
              consumerGroupId: "view-server-test",
              fallbackMode: "earliest",
              mode: "committed",
            },
            regions: {
              local: {
                status: "connected",
                brokers: "localhost:9092",
                lastConnectedAt: 1_000,
                lastError: null,
              },
            },
            topics: {
              sourceOrders: {
                status: "stalled",
                sourceTopic: "orders-source",
                viewServerTopic: "orders",
                regions: {
                  local: {
                    connected: true,
                    assignedPartitions: 1,
                    messagesPerSecond: 0,
                    bytesPerSecond: 0,
                    decodedMessagesPerSecond: 0,
                    decodeFailuresPerSecond: 0,
                    mappingFailuresPerSecond: 0,
                    processingFailuresPerSecond: 0,
                    lastMessageAt: null,
                    lastCommitAt: null,
                    consumerLagMessages: 7n,
                    lagSampledAt: null,
                    committedOffset: "3",
                    lastError: "lagging",
                  },
                },
              },
            },
          },
        }),
      });
      const summary = yield* runtimeCore.liveClient.subscribeHealthSummary();
      const detail = yield* runtimeCore.liveClient.subscribeHealth();

      const summaryEvents = yield* summary.events.pipe(Stream.take(1), Stream.runCollect);
      const detailEvents = yield* detail.events.pipe(Stream.take(1), Stream.runCollect);

      expect(Array.from(summaryEvents)).toStrictEqual([
        {
          type: "snapshot",
          topic: "__view_server_health_summary",
          queryId: "health-summary",
          version: 0,
          keys: ["summary"],
          rows: [
            {
              id: "summary",
              status: "degraded",
              runtimeStatus: "degraded",
              connectionStatus: "connected",
              unhealthyTopics: ["orders"],
              updatedAtNanos: expect.anything(),
              maxKafkaLag: 7n,
            },
          ],
          totalRows: 1,
        },
      ]);
      expect(Array.from(detailEvents)).toStrictEqual([
        {
          type: "snapshot",
          topic: "__view_server_health",
          queryId: "health",
          version: 0,
          keys: ["orders"],
          rows: [
            {
              id: "orders",
              status: "degraded",
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
              kafkaLag: 7n,
              updatedAtNanos: expect.anything(),
            },
          ],
          totalRows: 1,
        },
      ]);

      yield* summary.close();
      yield* detail.close();
      yield* runtimeCore.close;
    }),
  );

  it.effect("closes active pushed health subscriptions when the live client closes", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const summary = yield* runtimeCore.liveClient.subscribeHealthSummary();
      const detail = yield* runtimeCore.liveClient.subscribeHealth();
      const summaryFiber = yield* summary.events.pipe(Stream.runDrain, Effect.forkChild);
      const detailFiber = yield* detail.events.pipe(Stream.runDrain, Effect.forkChild);

      yield* Effect.yieldNow;
      yield* runtimeCore.close;
      yield* Fiber.join(summaryFiber);
      yield* Fiber.join(detailFiber);

      const closed = yield* runtimeCore.client.health();
      expect(closed.status).toBe("stopping");
    }),
  );

  it.effect("rejects pushed health subscriptions after live client close", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});

      yield* runtimeCore.liveClient.close;
      const closedSummary = yield* Effect.flip(runtimeCore.liveClient.subscribeHealthSummary());
      const closedDetail = yield* Effect.flip(runtimeCore.liveClient.subscribeHealth());

      expect(closedSummary).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        message: "Runtime Core is closed.",
      });
      expect(closedDetail).toStrictEqual({
        _tag: "ViewServerRuntimeError",
        code: "RuntimeUnavailable",
        message: "Runtime Core is closed.",
      });
      yield* runtimeCore.close;
    }),
  );

  it.effect("keeps pushed health subscriptions alive after the acquisition fiber completes", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const acquisitionFiber = yield* runtimeCore.liveClient
        .subscribeHealthSummary()
        .pipe(Effect.forkChild);
      const summary = yield* Fiber.join(acquisitionFiber);
      const eventsFiber = yield* summary.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* runtimeCore.client.publish("orders", order("a", 10));
      yield* runtimeCore.client.health();
      const events = yield* Fiber.join(eventsFiber).pipe(
        Effect.timeout("1 second"),
        Effect.ensuring(summary.close().pipe(Effect.orDie, Effect.andThen(runtimeCore.close))),
      );

      expect(Array.from(events)).toStrictEqual([
        {
          type: "snapshot",
          topic: "__view_server_health_summary",
          queryId: "health-summary",
          version: 0,
          keys: ["summary"],
          rows: [
            {
              id: "summary",
              status: "ready",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              unhealthyTopics: [],
              updatedAtNanos: expect.anything(),
              maxKafkaLag: 0n,
            },
          ],
          totalRows: 1,
        },
        {
          type: "snapshot",
          topic: "__view_server_health_summary",
          queryId: "health-summary",
          version: 1,
          keys: ["summary"],
          rows: [
            {
              id: "summary",
              status: "ready",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              unhealthyTopics: [],
              updatedAtNanos: expect.anything(),
              maxKafkaLag: 0n,
            },
          ],
          totalRows: 1,
        },
      ]);
    }),
  );

  it.effect("does not let stale detached health refreshes overwrite stopping health", () =>
    Effect.gen(function* () {
      const firstReadStarted = yield* Deferred.make<void>();
      const releaseFirstRead = yield* Deferred.make<void>();
      const readyHealth = engineHealth("ready", 1);
      const stoppingHealth = engineHealth("stopping", 1);
      const health = AtomRef.make(healthFromEngine(readyHealth));
      let readCount = 0;
      const healthReads = [
        Effect.gen(function* () {
          yield* Deferred.succeed(firstReadStarted, undefined);
          yield* Deferred.await(releaseFirstRead);
          return readyHealth;
        }),
        Effect.succeed(stoppingHealth),
      ];
      const engine = {
        health: () => {
          const nextRead = healthReads[readCount] ?? Effect.succeed(stoppingHealth);
          return Effect.suspend(() =>
            Effect.sync(() => {
              readCount += 1;
            }).pipe(Effect.andThen(nextRead)),
          );
        },
      };

      const staleRefresh = yield* readHealth(engine, health).pipe(Effect.forkDetach);
      yield* Deferred.await(firstReadStarted);

      const closed = yield* readHealth(engine, health);
      expect(closed.status).toBe("stopping");

      yield* Deferred.succeed(releaseFirstRead, undefined);
      yield* Fiber.join(staleRefresh);
      expect(health.value.status).toBe("stopping");
    }),
  );

  it.effect("coalesces concurrent health reads while an active same-epoch read is running", () =>
    Effect.gen(function* () {
      const readStarted = yield* Deferred.make<void>();
      const releaseRead = yield* Deferred.make<void>();
      let readCount = 0;
      const coalescedHealth = makeCoalescedHealthReader(
        () =>
          Effect.gen(function* () {
            readCount += 1;
            yield* Deferred.succeed(readStarted, undefined);
            yield* Deferred.await(releaseRead);
            return healthFromEngine(engineHealth("ready", readCount));
          }),
        () => 0,
      );

      const first = yield* coalescedHealth().pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(readStarted);
      const second = yield* coalescedHealth().pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.succeed(releaseRead, undefined);
      const [firstHealth, secondHealth] = yield* Effect.all(
        [Fiber.join(first), Fiber.join(second)],
        {
          concurrency: 2,
        },
      ).pipe(Effect.timeout("1 second"));

      expect(readCount).toBe(1);
      expect(firstHealth.engine.topics.orders.rowCount).toBe(1);
      expect(secondHealth.engine.topics.orders.rowCount).toBe(1);
      const thirdHealth = yield* coalescedHealth();
      expect(readCount).toBe(2);
      expect(thirdHealth.engine.topics.orders.rowCount).toBe(2);
    }),
  );

  it.effect("clears the active health read after a failed read so the next read retries", () =>
    Effect.gen(function* () {
      let readCount = 0;
      const healthReads = [
        Effect.fail("boom"),
        Effect.succeed(healthFromEngine(engineHealth("ready", 2))),
      ];
      const coalescedHealth = makeCoalescedHealthReader(() =>
        Effect.gen(function* () {
          const nextRead =
            healthReads[readCount] ?? Effect.succeed(healthFromEngine(engineHealth("ready", 3)));
          readCount += 1;
          return yield* nextRead;
        }),
      );

      const failedHealth = yield* Effect.flip(coalescedHealth());
      expect(failedHealth).toBe("boom");
      const recoveredHealth = yield* coalescedHealth();
      expect(readCount).toBe(2);
      expect(recoveredHealth.engine.topics.orders.rowCount).toBe(2);
    }),
  );

  it.effect("does not strand followers when the active health reader is interrupted", () =>
    Effect.gen(function* () {
      const firstReadStarted = yield* Deferred.make<void>();
      const releaseFirstRead = yield* Deferred.make<void>();
      const followerJoinedActiveRead = yield* Deferred.make<void>();
      let epochCheckCount = 0;
      let readCount = 0;
      const coalescedHealth = makeCoalescedHealthReader(
        () =>
          Effect.gen(function* () {
            readCount += 1;
            yield* Deferred.succeed(firstReadStarted, undefined);
            yield* Deferred.await(releaseFirstRead);
            return healthFromEngine(engineHealth("ready", readCount));
          }),
        () => {
          epochCheckCount += 1;
          if (epochCheckCount === 2) {
            Deferred.doneUnsafe(followerJoinedActiveRead, Effect.void);
          }
          return 0;
        },
      );

      const leader = yield* coalescedHealth().pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(firstReadStarted);
      const follower = yield* coalescedHealth().pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(followerJoinedActiveRead).pipe(
        Effect.timeout("1 second"),
        Effect.onError(() => Deferred.succeed(releaseFirstRead, undefined).pipe(Effect.asVoid)),
      );

      const interruptStarted = yield* Deferred.make<void>();
      const interruptLeader = yield* Effect.gen(function* () {
        yield* Deferred.succeed(interruptStarted, undefined);
        yield* Fiber.interrupt(leader);
      }).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(interruptStarted);
      yield* Deferred.succeed(releaseFirstRead, undefined);
      yield* Fiber.join(interruptLeader);
      const followerHealth = yield* Fiber.join(follower);
      const nextHealth = yield* coalescedHealth();
      expect(readCount).toBe(2);
      expect(followerHealth.engine.topics.orders.rowCount).toBe(1);
      expect(nextHealth.engine.topics.orders.rowCount).toBe(2);
    }),
  );

  it.effect("starts a fresh health read after the caller epoch changes", () =>
    Effect.gen(function* () {
      const firstReadStarted = yield* Deferred.make<void>();
      const releaseFirstRead = yield* Deferred.make<void>();
      let epoch = 0;
      let readCount = 0;
      const healthReads = [
        Effect.gen(function* () {
          yield* Deferred.succeed(firstReadStarted, undefined);
          yield* Deferred.await(releaseFirstRead);
          return healthFromEngine(engineHealth("ready", 1));
        }),
        Effect.succeed(healthFromEngine(engineHealth("ready", 2))),
      ];
      const coalescedHealth = makeCoalescedHealthReader(
        () =>
          Effect.gen(function* () {
            const nextRead =
              healthReads[readCount] ?? Effect.succeed(healthFromEngine(engineHealth("ready", 3)));
            readCount += 1;
            return yield* nextRead;
          }),
        () => epoch,
      );

      const staleHealthRead = yield* coalescedHealth().pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(firstReadStarted);
      epoch += 1;
      const freshHealth = yield* coalescedHealth();
      yield* Deferred.succeed(releaseFirstRead, undefined);
      const staleHealth = yield* Fiber.join(staleHealthRead);

      expect(readCount).toBe(2);
      expect(freshHealth.engine.topics.orders.rowCount).toBe(2);
      expect(staleHealth.engine.topics.orders.rowCount).toBe(1);
    }),
  );

  it.effect("does not let obsolete epoch health reads overwrite fresher cached health", () =>
    Effect.gen(function* () {
      const firstReadStarted = yield* Deferred.make<void>();
      const releaseFirstRead = yield* Deferred.make<void>();
      const health = AtomRef.make(healthFromEngine(engineHealth("ready", 0)));
      let epoch = 0;
      let readCount = 0;
      const engineHealthReads = [
        Effect.gen(function* () {
          yield* Deferred.succeed(firstReadStarted, undefined);
          yield* Deferred.await(releaseFirstRead);
          return engineHealth("ready", 1);
        }),
        Effect.succeed(engineHealth("ready", 2)),
      ];
      const engine = {
        health: () => {
          const nextRead = engineHealthReads[readCount] ?? Effect.succeed(engineHealth("ready", 3));
          return Effect.suspend(() =>
            Effect.sync(() => {
              readCount += 1;
            }).pipe(Effect.andThen(nextRead)),
          );
        },
      };
      const coalescedHealth = makeCoalescedHealthReader(
        (readEpoch) => readHealth(engine, health, undefined, undefined, () => epoch === readEpoch),
        () => epoch,
      );

      const obsoleteHealthRead = yield* coalescedHealth().pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(firstReadStarted);
      epoch += 1;
      const freshHealth = yield* coalescedHealth();
      yield* Deferred.succeed(releaseFirstRead, undefined);
      const obsoleteHealth = yield* Fiber.join(obsoleteHealthRead);

      expect(freshHealth.engine.topics.orders.rowCount).toBe(2);
      expect(obsoleteHealth.engine.topics.orders.rowCount).toBe(2);
      expect(health.value.engine.topics.orders.rowCount).toBe(2);
    }),
  );

  it.effect("does not let older scheduled health reads overwrite newer installed health", () =>
    Effect.gen(function* () {
      const scheduledReadStarted = yield* Deferred.make<void>();
      const releaseScheduledRead = yield* Deferred.make<void>();
      const scheduledReadFinished = yield* Deferred.make<void>();
      const health = AtomRef.make(healthFromEngine(engineHealth("ready", 0)));
      let installEpoch = 0;
      let readCount = 0;
      const engineHealthReads = [
        Effect.gen(function* () {
          yield* Deferred.succeed(scheduledReadStarted, undefined);
          yield* Deferred.await(releaseScheduledRead);
          return engineHealth("ready", 1);
        }),
        Effect.succeed(engineHealth("ready", 2)),
      ];
      const engine = {
        health: () => {
          const nextRead = engineHealthReads[readCount] ?? Effect.succeed(engineHealth("ready", 3));
          return Effect.suspend(() =>
            Effect.sync(() => {
              readCount += 1;
            }).pipe(Effect.andThen(nextRead)),
          );
        },
      };
      const scheduler = yield* makeHealthRefreshScheduler(
        Effect.gen(function* () {
          const readInstallEpoch = installEpoch;
          yield* readHealth(
            engine,
            health,
            undefined,
            undefined,
            () => installEpoch === readInstallEpoch,
            () => {
              installEpoch += 1;
            },
          );
          yield* Deferred.succeed(scheduledReadFinished, undefined);
        }),
        "0 millis",
      );

      yield* scheduler.request;
      yield* Deferred.await(scheduledReadStarted).pipe(Effect.timeout("1 second"));
      const freshHealth = yield* readHealth(engine, health, undefined, undefined, undefined, () => {
        installEpoch += 1;
      });
      yield* Deferred.succeed(releaseScheduledRead, undefined);
      yield* Deferred.await(scheduledReadFinished).pipe(Effect.timeout("1 second"));

      expect(freshHealth.engine.topics.orders.rowCount).toBe(2);
      expect(health.value.engine.topics.orders.rowCount).toBe(2);
      yield* scheduler.close;
    }),
  );

  it.effect("lets scheduled health refreshes install and follow up while requests continue", () =>
    Effect.gen(function* () {
      const firstReadStarted = yield* Deferred.make<void>();
      const releaseFirstRead = yield* Deferred.make<void>();
      const secondReadStarted = yield* Deferred.make<void>();
      const releaseSecondRead = yield* Deferred.make<void>();
      const refreshCompleted = yield* Queue.unbounded<void>();
      const health = AtomRef.make(healthFromEngine(engineHealth("ready", 0)));
      let installEpoch = 0;
      let readCount = 0;
      const engineHealthReads = [
        Effect.gen(function* () {
          yield* Deferred.succeed(firstReadStarted, undefined);
          yield* Deferred.await(releaseFirstRead);
          return engineHealth("ready", 1);
        }),
        Effect.gen(function* () {
          yield* Deferred.succeed(secondReadStarted, undefined);
          yield* Deferred.await(releaseSecondRead);
          return engineHealth("ready", 2);
        }),
      ];
      const engine = {
        health: () => {
          const nextRead = engineHealthReads[readCount] ?? Effect.succeed(engineHealth("ready", 3));
          return Effect.suspend(() =>
            Effect.sync(() => {
              readCount += 1;
            }).pipe(Effect.andThen(nextRead)),
          );
        },
      };
      const scheduler = yield* makeHealthRefreshScheduler(
        Effect.gen(function* () {
          const readInstallEpoch = installEpoch;
          yield* readHealth(
            engine,
            health,
            undefined,
            undefined,
            () => installEpoch === readInstallEpoch,
            () => {
              installEpoch += 1;
            },
          );
          yield* Queue.offer(refreshCompleted, undefined);
        }),
        "0 millis",
      );

      yield* scheduler.request;
      yield* Deferred.await(firstReadStarted).pipe(Effect.timeout("1 second"));
      yield* scheduler.request;
      yield* Deferred.succeed(releaseFirstRead, undefined);
      yield* Queue.take(refreshCompleted).pipe(Effect.timeout("1 second"));
      yield* Deferred.await(secondReadStarted).pipe(Effect.timeout("1 second"));
      expect(health.value.engine.topics.orders.rowCount).toBe(1);
      yield* Deferred.succeed(releaseSecondRead, undefined);
      yield* Queue.take(refreshCompleted).pipe(Effect.timeout("1 second"));

      expect(readCount).toBe(2);
      expect(health.value.engine.topics.orders.rowCount).toBe(2);
      yield* scheduler.close;
    }),
  );

  it.effect("maps engine errors into runtime errors", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      yield* runtimeCore.client.publish("orders", order("a", 10));

      const invalidTopic = yield* Effect.flip(
        // @ts-expect-error hostile runtime callers can still send unknown topics.
        runtimeCore.client.publish("missing", order("b", 20)),
      );
      const invalidRow = yield* Effect.flip(
        runtimeCore.client.publish("orders", {
          id: "bad",
          customerId: "customer-bad",
          // @ts-expect-error hostile runtime callers can still send malformed rows.
          status: "unknown",
          price: 20,
          region: "usa",
          updatedAt: 20,
        }),
      );
      const groupedQuery = yield* runtimeCore.client.snapshot("orders", {
        groupBy: ["status"],
        aggregates: { rowCount: { aggFunc: "count" } },
      });
      const invalidQuery = yield* Effect.flip(
        runtimeCore.client.snapshot("orders", {
          // @ts-expect-error hostile runtime callers can still send unknown projected fields.
          select: ["prcie"],
        }),
      );

      yield* runtimeCore.close;
      const runtimeUnavailable = yield* Effect.flip(
        runtimeCore.client.publish("orders", order("closed", 30)),
      );

      expect(invalidTopic.code).toBe("InvalidTopic");
      expect(invalidRow.code).toBe("InvalidRow");
      expect(groupedQuery.rows).toStrictEqual([{ status: "open", rowCount: 1n }]);
      expect(invalidQuery.code).toBe("InvalidQuery");
      expect(runtimeUnavailable.code).toBe("RuntimeUnavailable");
    }),
  );

  it.effect(
    "queues a trailing health scheduler refresh when requested while refresh is pending",
    () =>
      Effect.gen(function* () {
        const firstStarted = yield* Deferred.make<void>();
        const firstFinished = yield* Deferred.make<void>();
        const secondStarted = yield* Deferred.make<void>();
        const secondFinished = yield* Deferred.make<void>();
        let refreshCount = 0;
        const refreshSteps = [
          { started: firstStarted, finished: firstFinished },
          { started: secondStarted, finished: secondFinished },
        ] as const;

        const scheduler = yield* makeHealthRefreshScheduler(
          Effect.gen(function* () {
            const refreshStep = refreshSteps[refreshCount] ?? refreshSteps[1];
            yield* Effect.sync(() => {
              refreshCount += 1;
            });
            yield* Deferred.succeed(refreshStep.started, undefined);
            yield* Deferred.await(refreshStep.finished);
          }),
          "0 millis",
        );

        yield* scheduler.request;
        yield* Deferred.await(firstStarted);

        yield* scheduler.request;
        yield* Deferred.succeed(firstFinished, undefined);
        yield* Deferred.await(secondStarted);

        expect(refreshCount).toBe(2);
        yield* Deferred.succeed(secondFinished, undefined);
        yield* scheduler.close;
      }),
  );

  it.effect("closes a sleeping health scheduler refresh fiber", () =>
    Effect.gen(function* () {
      let refreshCount = 0;
      const scheduler = yield* makeHealthRefreshScheduler(
        Effect.sync(() => {
          refreshCount += 1;
        }),
        "1 minute",
      );

      yield* scheduler.request;
      yield* scheduler.close;

      expect(refreshCount).toBe(0);
      yield* scheduler.close;
    }),
  );

  it.effect("clears active health scheduler state when a refresh interrupts itself", () =>
    Effect.gen(function* () {
      const firstStarted = yield* Deferred.make<void>();
      const secondStarted = yield* Deferred.make<void>();
      const startedSignals = [firstStarted, secondStarted] as const;
      let refreshCount = 0;
      const scheduler = yield* makeHealthRefreshScheduler(
        Effect.gen(function* () {
          const started = startedSignals[refreshCount] ?? secondStarted;
          yield* Effect.sync(() => {
            refreshCount += 1;
          });
          yield* Deferred.succeed(started, undefined);
          return yield* Effect.interrupt;
        }),
        "0 millis",
      );

      yield* scheduler.request;
      yield* Deferred.await(firstStarted);
      yield* Effect.yieldNow;
      yield* scheduler.request;
      yield* Deferred.await(secondStarted).pipe(Effect.timeout("1 second"));

      expect(refreshCount).toBe(2);
      yield* scheduler.close;
    }),
  );

  it.effect("ignores health scheduler refresh requests after close", () =>
    Effect.gen(function* () {
      let refreshCount = 0;
      const scheduler = yield* makeHealthRefreshScheduler(
        Effect.sync(() => {
          refreshCount += 1;
        }),
        "0 millis",
      );

      yield* scheduler.close;
      yield* scheduler.request;

      expect(refreshCount).toBe(0);
    }),
  );
});
