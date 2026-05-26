import { describe, expect, it } from "@effect/vitest";
import type { ColumnLiveViewEngineHealth } from "@view-server/column-live-view-engine";
import { defineViewServerConfig } from "@view-server/config";
import { Deferred, Effect, Fiber, Schema, Stream } from "effect";
import * as AtomRef from "effect/unstable/reactivity/AtomRef";
import { healthFromEngine, makeHealthRefreshScheduler, readHealth } from "./health";
import { createInMemoryViewServer, makeInMemoryViewServer } from "./index";

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

describe("@view-server/in-memory", () => {
  it.effect("runs the framework-neutral runtime and live client", () =>
    Effect.gen(function* () {
      const inMemory = yield* makeInMemoryViewServer(viewServer, {});
      const subscription = yield* inMemory.liveClient.subscribe("orders", {
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

      yield* inMemory.client.publishMany("orders", [order("b", 20), order("a", 10)]);
      yield* inMemory.client.publish("orders", order("c", 30));
      yield* inMemory.client.patch("orders", "c", { price: 5 });
      yield* inMemory.client.delete("orders", "a");

      const snapshot = yield* inMemory.client.snapshot("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      expect(snapshot.rows).toStrictEqual([
        { id: "c", price: 5 },
        { id: "b", price: 20 },
      ]);

      const health = yield* inMemory.client.health();
      expect(health.engine.topics.orders.rowCount).toBe(2);

      yield* subscription.close();
      yield* inMemory.client.reset();
      const resetHealth = yield* inMemory.client.health();
      expect(resetHealth.engine.topics.orders.rowCount).toBe(0);
      yield* inMemory.close;
    }),
  );

  it.effect("supports the synchronous in-memory constructor", () =>
    Effect.gen(function* () {
      const inMemory = createInMemoryViewServer(viewServer, { subscriptionQueueCapacity: 1 });
      expect("set" in inMemory.liveClient.health).toBe(false);
      expect(inMemory.liveClient.health.value.status).toBe("ready");

      yield* inMemory.client.publish("orders", order("a", 10));
      const health = yield* inMemory.client.health();

      expect(health.engine.topics.orders.rowCount).toBe(1);
      yield* inMemory.close;
    }),
  );

  it.effect("supports the synchronous in-memory constructor defaults", () =>
    Effect.gen(function* () {
      const inMemory = createInMemoryViewServer(viewServer);

      yield* inMemory.client.publish("orders", order("a", 10));
      const health = yield* inMemory.client.health();

      expect(health.engine.topics.orders.rowCount).toBe(1);
      yield* inMemory.close;
    }),
  );

  it.effect("refreshes health after close", () =>
    Effect.gen(function* () {
      const inMemory = yield* makeInMemoryViewServer(viewServer, {});
      yield* inMemory.client.publish("orders", order("a", 10));

      const ready = yield* inMemory.client.health();
      expect(ready.status).toBe("ready");

      yield* inMemory.liveClient.close;

      const closed = yield* inMemory.client.health();
      expect(closed.status).toBe("stopping");
      expect(closed.engine.topics.orders.activeSubscriptions).toBe(0);
    }),
  );

  it.effect("pushes summary and detailed health snapshots", () =>
    Effect.gen(function* () {
      const inMemory = yield* makeInMemoryViewServer(viewServer, {});
      const summary = yield* inMemory.liveClient.subscribeHealthSummary();
      const detail = yield* inMemory.liveClient.subscribeHealth();

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

      yield* inMemory.client.publish("orders", order("a", 10));

      const summaryEvents = yield* Fiber.join(summaryFiber);
      const detailEvents = yield* Fiber.join(detailFiber);
      const summarySnapshots = Array.from(summaryEvents).filter(
        (event) => event.type === "snapshot",
      );
      const detailSnapshots = Array.from(detailEvents).filter((event) => event.type === "snapshot");

      expect(summarySnapshots).toHaveLength(2);
      expect(detailSnapshots).toHaveLength(2);
      expect(summarySnapshots[0]).toStrictEqual({
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
            updatedAtNanos: summarySnapshots[0]?.rows[0]?.updatedAtNanos,
            maxKafkaLag: 0n,
          },
        ],
        totalRows: 1,
      });
      expect(summarySnapshots[1]?.rows[0]?.maxKafkaLag).toBe(0n);
      expect(summarySnapshots[1]?.rows[0]?.updatedAtNanos).toBeGreaterThanOrEqual(
        summarySnapshots[0]?.rows[0]?.updatedAtNanos ?? 0n,
      );

      expect(detailSnapshots[0]?.rows[0]).toStrictEqual({
        id: "orders",
        status: "ready",
        rowCount: 0,
        liveRowCount: 0,
        deletedRowCount: 0,
        version: 0,
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
        updatedAtNanos: detailSnapshots[0]?.rows[0]?.updatedAtNanos,
      });
      expect(detailSnapshots[1]?.rows[0]?.rowCount).toBe(1);

      yield* summary.close();
      yield* summary.close();
      yield* detail.close();
      yield* detail.close();
      yield* inMemory.close;
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
      const engine = {
        health: () =>
          Effect.gen(function* () {
            const currentRead = yield* Effect.sync(() => {
              readCount += 1;
              return readCount;
            });
            if (currentRead === 1) {
              yield* Deferred.succeed(firstReadStarted, undefined);
              yield* Deferred.await(releaseFirstRead);
              return readyHealth;
            }
            return stoppingHealth;
          }),
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
      const inMemory = yield* makeInMemoryViewServer(viewServer, {});
      yield* inMemory.client.publish("orders", order("a", 10));

      const invalidTopic = yield* Effect.flip(
        // @ts-expect-error hostile runtime callers can still send unknown topics.
        inMemory.client.publish("missing", order("b", 20)),
      );
      const invalidRow = yield* Effect.flip(
        inMemory.client.publish("orders", {
          id: "bad",
          customerId: "customer-bad",
          // @ts-expect-error hostile runtime callers can still send malformed rows.
          status: "unknown",
          price: 20,
          region: "usa",
          updatedAt: 20,
        }),
      );
      const unsupportedQuery = yield* Effect.flip(
        inMemory.client.snapshot("orders", {
          // @ts-expect-error grouped queries are rejected by the raw in-memory runtime slice.
          groupBy: ["status"],
          // @ts-expect-error grouped queries are rejected by the raw in-memory runtime slice.
          aggregates: { count: { aggFunc: "count" } },
        }),
      );
      const invalidQuery = yield* Effect.flip(
        inMemory.client.snapshot("orders", {
          // @ts-expect-error hostile runtime callers can still send unknown projected fields.
          select: ["prcie"],
        }),
      );

      yield* inMemory.close;
      const runtimeUnavailable = yield* Effect.flip(
        inMemory.client.publish("orders", order("closed", 30)),
      );

      expect(invalidTopic.code).toBe("InvalidTopic");
      expect(invalidRow.code).toBe("InvalidRow");
      expect(unsupportedQuery.code).toBe("UnsupportedQuery");
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

        const requestRefresh = makeHealthRefreshScheduler(
          Effect.gen(function* () {
            const currentRefresh = yield* Effect.sync(() => {
              refreshCount += 1;
              return refreshCount;
            });
            if (currentRefresh === 1) {
              yield* Deferred.succeed(firstStarted, undefined);
              yield* Deferred.await(firstFinished);
              return;
            }
            yield* Deferred.succeed(secondStarted, undefined);
            yield* Deferred.await(secondFinished);
          }),
        );

        yield* requestRefresh;
        yield* Deferred.await(firstStarted);

        yield* requestRefresh;
        yield* Deferred.succeed(firstFinished, undefined);
        yield* Deferred.await(secondStarted);

        expect(refreshCount).toBe(2);
        yield* Deferred.succeed(secondFinished, undefined);
      }),
  );
});
