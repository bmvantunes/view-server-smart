import { describe, expect, it } from "@effect/vitest";
import type { ColumnLiveViewEngineHealth } from "@view-server/column-live-view-engine";
import { defineViewServerConfig } from "@view-server/config";
import { Deferred, Effect, Fiber, Schema, Stream } from "effect";
import { AtomRef } from "effect/unstable/reactivity";
import { healthFromEngine, makeHealthRefreshScheduler, readHealth } from "./health";
import { createViewServerRuntimeCore, makeViewServerRuntimeCore } from "./index";

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
      activeViews: 0,
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
              activeViews: 0,
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
              activeViews: 0,
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
        Effect.ensuring(summary.close().pipe(Effect.andThen(runtimeCore.close), Effect.ignore)),
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

        const scheduler = makeHealthRefreshScheduler(
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
      const scheduler = makeHealthRefreshScheduler(
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
      const scheduler = makeHealthRefreshScheduler(
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
      const scheduler = makeHealthRefreshScheduler(
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
