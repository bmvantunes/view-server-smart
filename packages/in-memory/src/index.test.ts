import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@view-server/config";
import { Effect, Schema, Stream } from "effect";
import { createInMemoryViewServer, makeInMemoryViewServer } from "./index";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

describe("@view-server/in-memory", () => {
  it.effect("adapts the shared runtime core to the public in-memory API", () =>
    Effect.gen(function* () {
      const inMemory = yield* makeInMemoryViewServer(viewServer, {
        subscriptionQueueCapacity: 8,
      });
      const subscription = yield* inMemory.liveClient.subscribe("orders", {
        select: ["id", "price"],
        limit: 10,
      });

      yield* inMemory.client.publish("orders", { id: "order-1", price: 10 });

      const events = yield* subscription.events.pipe(Stream.take(2), Stream.runCollect);
      const health = yield* inMemory.client.health();

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
        operations: [
          {
            type: "insert",
            key: "order-1",
            row: { id: "order-1", price: 10 },
            index: 0,
          },
        ],
        totalRows: 1,
      });
      expect(health.engine.topics.orders.rowCount).toBe(1);

      yield* subscription.close();
      yield* inMemory.close;
    }),
  );

  it.effect("supports the synchronous public in-memory constructor", () =>
    Effect.gen(function* () {
      const inMemory = createInMemoryViewServer(viewServer);
      yield* inMemory.client.publish("orders", { id: "order-1", price: 10 });
      const health = yield* inMemory.client.health();

      expect(health.engine.topics.orders.rowCount).toBe(1);
      expect("subscribeRuntime" in inMemory.liveClient).toBe(false);
      yield* inMemory.close;
    }),
  );

  it.effect("ignores smuggled runtime-core transport health options", () =>
    Effect.gen(function* () {
      const widenedOptions = {
        subscriptionQueueCapacity: 8,
        transportHealth: () => ({
          activeClients: 99,
          activeStreams: 99,
          activeSubscriptions: 99,
          messagesPerSecond: 99,
          bytesPerSecond: 99,
          queuedMessages: 99,
          queuedBytes: 99,
          droppedClients: 99,
          backpressureEvents: 99,
          reconnects: 99,
          lastError: "should not leak",
        }),
      };
      const inMemory = createInMemoryViewServer(viewServer, widenedOptions);
      const health = yield* inMemory.client.health();

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
      yield* inMemory.close;
    }),
  );

  it.effect("live client close owns shared runtime core cleanup", () =>
    Effect.gen(function* () {
      const inMemory = yield* makeInMemoryViewServer(viewServer, {
        healthRefreshCadence: "1 minute",
      });

      yield* inMemory.client.publish("orders", { id: "order-1", price: 10 });
      yield* inMemory.liveClient.close;

      const health = yield* inMemory.client.health();
      expect(health.status).toBe("stopping");
      expect(health.engine.topics.orders.rowCount).toBe(1);
      yield* inMemory.close;
    }),
  );
});
