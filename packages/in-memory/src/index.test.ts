import { assert, describe, it } from "@effect/vitest";
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
      assert.deepStrictEqual(events[0], {
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
      assert.deepStrictEqual(snapshot.rows, [
        { id: "c", price: 5 },
        { id: "b", price: 20 },
      ]);

      const health = yield* inMemory.client.health();
      assert.strictEqual(health.engine.topics.orders.rowCount, 2);

      yield* subscription.close();
      yield* inMemory.client.reset();
      const resetHealth = yield* inMemory.client.health();
      assert.strictEqual(resetHealth.engine.topics.orders.rowCount, 0);
      yield* inMemory.close;
    }),
  );

  it.effect("supports the synchronous in-memory constructor", () =>
    Effect.gen(function* () {
      const inMemory = createInMemoryViewServer(viewServer, { subscriptionQueueCapacity: 1 });
      assert.strictEqual("set" in inMemory.liveClient.health, false);
      assert.strictEqual(inMemory.liveClient.health.value.status, "ready");

      yield* inMemory.client.publish("orders", order("a", 10));
      const health = yield* inMemory.client.health();

      assert.strictEqual(health.engine.topics.orders.rowCount, 1);
      yield* inMemory.close;
    }),
  );

  it.effect("refreshes health after close", () =>
    Effect.gen(function* () {
      const inMemory = yield* makeInMemoryViewServer(viewServer, {});
      yield* inMemory.client.publish("orders", order("a", 10));

      const ready = yield* inMemory.client.health();
      assert.strictEqual(ready.status, "ready");

      yield* inMemory.liveClient.close;

      const closed = yield* inMemory.client.health();
      assert.strictEqual(closed.status, "stopping");
      assert.strictEqual(closed.engine.topics.orders.activeSubscriptions, 0);
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
      assert.strictEqual(closed.status, "stopping");

      yield* Deferred.succeed(releaseFirstRead, undefined);
      yield* Fiber.join(staleRefresh);
      assert.strictEqual(health.value.status, "stopping");
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

      assert.strictEqual(invalidTopic.code, "InvalidTopic");
      assert.strictEqual(invalidRow.code, "InvalidRow");
      assert.strictEqual(unsupportedQuery.code, "UnsupportedQuery");
      assert.strictEqual(invalidQuery.code, "InvalidQuery");
      assert.strictEqual(runtimeUnavailable.code, "RuntimeUnavailable");
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

        assert.strictEqual(refreshCount, 2);
        yield* Deferred.succeed(secondFinished, undefined);
      }),
  );
});
