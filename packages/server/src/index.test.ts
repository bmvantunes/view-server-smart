import { NodeSocket } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import type { ViewServerLiveEvent } from "@view-server/client";
import { makeViewServerClient } from "@view-server/client/remote";
import { defineViewServerConfig } from "@view-server/config";
import { createInMemoryViewServer } from "@view-server/in-memory";
import { ViewServerRpcErrorSchema, ViewServerRpcs } from "@view-server/protocol";
import {
  Context,
  Effect,
  Fiber,
  Layer,
  ManagedRuntime,
  Schema,
  SchemaGetter,
  Stream,
} from "effect";
import { fromStringUnsafe } from "effect/BigDecimal";
import { RpcClient, RpcSerialization } from "effect/unstable/rpc";
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError";
import { makeViewServerWebSocketServer } from "./index";

const Order = Schema.Struct({
  id: Schema.String,
  price: Schema.Number,
});

const Trade = Schema.Struct({
  id: Schema.String,
  quantity: Schema.BigInt,
});

const Quote = Schema.Struct({
  id: Schema.String,
  price: Schema.BigDecimal,
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
    trades: {
      schema: Trade,
      key: "id",
    },
    quotes: {
      schema: Quote,
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
type TradeRow = typeof Trade.Type;
type QuoteRow = typeof Quote.Type;

const order = (id: string, price: number): OrderRow => ({
  id,
  price,
});

const trade = (id: string, quantity: bigint): TradeRow => ({
  id,
  quantity,
});

const quote = (id: string, price: string): QuoteRow => ({
  id,
  price: fromStringUnsafe(price),
});

class RawViewServerRpcClient extends Context.Service<
  RawViewServerRpcClient,
  RpcClient.FromGroup<typeof ViewServerRpcs, RpcClientError>
>()("RawViewServerRpcClient") {}

const makeRawRpcClient = Effect.fn("ViewServerServer.test.rawRpcClient.make")(function* (
  url: string,
) {
  const layer = Layer.effect(RawViewServerRpcClient)(RpcClient.make(ViewServerRpcs)).pipe(
    Layer.provide(RpcClient.layerProtocolSocket()),
    Layer.provide([NodeSocket.layerWebSocket(url), RpcSerialization.layerNdjson]),
  );
  const runtime = ManagedRuntime.make(layer);
  const context = yield* runtime.contextEffect;
  return {
    close: runtime.disposeEffect,
    rpc: Context.get(context, RawViewServerRpcClient),
  };
});

describe("@view-server/server", () => {
  it.live("serves an in-memory runtime through Effect RPC WebSocket", () =>
    Effect.gen(function* () {
      const inMemory = createInMemoryViewServer(viewServer);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      const subscription = yield* client.subscribe("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const eventsFiber = yield* subscription.events.pipe(
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.sleep("10 millis");

      yield* inMemory.client.publish("orders", order("b", 20));
      yield* inMemory.client.publishMany("orders", [order("a", 10)]);

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
        operations: [{ type: "insert", key: "b", row: { id: "b", price: 20 }, index: 0 }],
        totalRows: 1,
      });
      expect(events[2]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [{ type: "insert", key: "a", row: { id: "a", price: 10 }, index: 0 }],
        totalRows: 2,
      });
      expect(client.health.value.engine.topics.orders.rowCount).toBe(2);

      yield* inMemory.client.reset();
      expect((yield* inMemory.client.health()).engine.topics.orders.rowCount).toBe(0);

      yield* Effect.sleep("10 millis");
      const afterClose = yield* inMemory.client.health();
      expect(afterClose.engine.topics.orders.activeSubscriptions).toBe(0);

      yield* client.close;
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("preserves typed server errors for raw RPC clients", () =>
    Effect.gen(function* () {
      const inMemory = createInMemoryViewServer(viewServer);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });
      const raw = yield* makeRawRpcClient(server.url);

      const unknownSubscribeTopic = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "missing",
          query: { select: ["id"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(unknownSubscribeTopic.code).toBe("InvalidTopic");

      const unknownSelect = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["missing"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(unknownSelect.code).toBe("InvalidQuery");

      const unknownWhere = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            where: { missing: { eq: "x" } },
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(unknownWhere.code).toBe("InvalidQuery");

      const unknownOrderBy = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            orderBy: [{ field: "missing", direction: "asc" }],
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(unknownOrderBy.code).toBe("InvalidQuery");

      const emptySelect = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: [] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(emptySelect.code).toBe("InvalidQuery");
      expect(emptySelect.message).toBe("Query select must include at least one field");

      const invalidOffset = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"], offset: -1 },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(invalidOffset.code).toBe("InvalidQuery");
      expect(invalidOffset.message).toBe("Query offset must be a non-negative integer");

      const invalidLimit = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"], limit: 0 },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(invalidLimit.code).toBe("InvalidQuery");
      expect(invalidLimit.message).toBe("Query limit must be a positive integer");

      const extraTopLevelQueryKey = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"], whre: { id: "a" } },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(extraTopLevelQueryKey.code).toBe("InvalidQuery");

      const extraOrderByKey = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            orderBy: [{ field: "id", direction: "asc", aggregate: "total" }],
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(extraOrderByKey.code).toBe("InvalidQuery");

      const invalidFilter = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            where: { price: { gt: "bad" } },
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(invalidFilter.code).toBe("InvalidQuery");

      const invalidStartsWith = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            where: { price: { startsWith: "1" } },
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(invalidStartsWith.code).toBe("InvalidQuery");

      const invalidNumericStartsWith = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            where: { price: { startsWith: 1 } },
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(invalidNumericStartsWith.code).toBe("InvalidQuery");
      expect(invalidNumericStartsWith.message).toBe("Filter price does not support startsWith");

      const unsupportedFilter = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: {
            select: ["id"],
            where: {
              id: {
                startsWith: "a",
                raw: "value",
              },
            },
          },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(unsupportedFilter.code).toBe("InvalidQuery");

      const richFilterEvents = yield* raw.rpc["ViewServer.Subscribe"]({
        topic: "orders",
        query: {
          select: ["id", "price"],
          where: {
            id: {
              in: ["a", "b"],
              startsWith: "a",
            },
            price: 10,
          },
          offset: 0,
        },
      }).pipe(Stream.take(1), Stream.runCollect);
      expect(richFilterEvents[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });

      yield* raw.close;
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("rejects malformed live-client rows during server event encoding", () =>
    Effect.gen(function* () {
      const inMemory = createInMemoryViewServer(viewServer);
      const makeServerForEvent = Effect.fn("ViewServerServer.test.malformedEventServer.make")(
        function* (event: ViewServerLiveEvent<object>) {
          const liveClient = {
            ...inMemory.liveClient,
            subscribe: () =>
              Effect.succeed({
                events: Stream.make(event),
                close: () => Effect.void,
              }),
          };
          const server = yield* makeViewServerWebSocketServer(viewServer, {
            // @ts-expect-error this fake client intentionally violates the row type contract.
            liveClient,
            runtime: inMemory.client,
          });
          const raw = yield* makeRawRpcClient(server.url);
          return { raw, server };
        },
      );
      const makeServerForRow = (row: object) =>
        makeServerForEvent({
          type: "snapshot",
          topic: "orders",
          queryId: "malformed",
          version: 0,
          keys: ["bad"],
          rows: [row],
          totalRows: 1,
        });

      const invalidFieldType = yield* makeServerForRow({ id: 1 });
      const invalidFieldTypeError = yield* Effect.flip(
        invalidFieldType.raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(invalidFieldTypeError.code).toBe("InvalidRow");
      yield* invalidFieldType.raw.close;
      yield* invalidFieldType.server.close;

      const unknownField = yield* makeServerForRow({ id: "ok", missing: "bad" });
      const unknownFieldError = yield* Effect.flip(
        unknownField.raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(unknownFieldError.code).toBe("InvalidRow");
      expect(unknownFieldError.message).toBe("Unexpected row field for topic orders: missing");
      yield* unknownField.raw.close;
      yield* unknownField.server.close;

      const missingField = yield* makeServerForRow({ price: 10 });
      const missingFieldError = yield* Effect.flip(
        missingField.raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(missingFieldError.code).toBe("InvalidRow");
      expect(missingFieldError.message).toBe("Missing row field for topic orders: id");
      yield* missingField.raw.close;
      yield* missingField.server.close;

      const wrongTopic = yield* makeServerForEvent({
        type: "status",
        topic: "trades",
        queryId: "wrong-topic",
        status: "ready",
        code: "Ready",
      });
      const wrongTopicError = yield* Effect.flip(
        wrongTopic.raw.rpc["ViewServer.Subscribe"]({
          topic: "orders",
          query: { select: ["id"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(wrongTopicError.code).toBe("InvalidRow");
      expect(wrongTopicError.message).toBe("Received event for trades while subscribed to orders");
      yield* wrongTopic.raw.close;
      yield* wrongTopic.server.close;

      yield* inMemory.close;
    }),
  );

  it.live("rejects non-json schema encodings during server event encoding", () =>
    Effect.gen(function* () {
      const inMemory = createInMemoryViewServer(edgeViewServer);
      const event: ViewServerLiveEvent<object> = {
        type: "snapshot",
        topic: "badjson",
        queryId: "badjson",
        version: 0,
        keys: ["bad"],
        rows: [{ id: "bad" }],
        totalRows: 1,
      };
      const liveClient = {
        ...inMemory.liveClient,
        subscribe: () =>
          Effect.succeed({
            events: Stream.make(event),
            close: () => Effect.void,
          }),
      };
      const server = yield* makeViewServerWebSocketServer(edgeViewServer, {
        // @ts-expect-error this fake client intentionally emits a row with a non-json encoding.
        liveClient,
        runtime: inMemory.client,
      });
      const raw = yield* makeRawRpcClient(server.url);

      const error = yield* Effect.flip(
        raw.rpc["ViewServer.Subscribe"]({
          topic: "badjson",
          query: { select: ["id"] },
        }).pipe(Stream.runDrain),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)));
      expect(error.code).toBe("InvalidRow");
      expect(error.message).toMatch(/Field id is not JSON-safe/);

      yield* raw.close;
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("round-trips BigInt rows and filters through the RPC NDJSON transport", () =>
    Effect.gen(function* () {
      const inMemory = createInMemoryViewServer(viewServer);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      const subscription = yield* client.subscribe("trades", {
        where: {
          quantity: { gte: 10n },
        },
        select: ["id", "quantity"],
        orderBy: [{ field: "quantity", direction: "asc" }],
        limit: 10,
      });
      const eventsFiber = yield* subscription.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.sleep("10 millis");

      yield* inMemory.client.publish("trades", trade("a", 5n));
      yield* inMemory.client.publish("trades", trade("b", 10n));

      const events = yield* Fiber.join(eventsFiber);
      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "trades",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(events[1]).toStrictEqual({
        type: "delta",
        topic: "trades",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 2,
        operations: [{ type: "insert", key: "b", row: { id: "b", quantity: 10n }, index: 0 }],
        totalRows: 1,
      });

      yield* Effect.sleep("10 millis");
      yield* client.close;
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("round-trips BigDecimal rows and filters through the RPC NDJSON transport", () =>
    Effect.gen(function* () {
      const inMemory = createInMemoryViewServer(viewServer);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      const subscription = yield* client.subscribe("quotes", {
        where: {
          price: { gte: fromStringUnsafe("10.50") },
        },
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

      yield* inMemory.client.publish("quotes", quote("a", "9.99"));
      yield* inMemory.client.publish("quotes", quote("b", "10.50"));

      const events = yield* Fiber.join(eventsFiber);
      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "quotes",
        queryId: "query-0",
        version: 0,
        keys: [],
        rows: [],
        totalRows: 0,
      });
      expect(events[1]).toStrictEqual({
        type: "delta",
        topic: "quotes",
        queryId: "query-0",
        fromVersion: 0,
        toVersion: 2,
        operations: [
          {
            type: "insert",
            key: "b",
            row: { id: "b", price: fromStringUnsafe("10.5") },
            index: 0,
          },
        ],
        totalRows: 1,
      });

      yield* Effect.sleep("10 millis");
      yield* client.close;
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("encodes snapshot rows, move/remove deltas, and close statuses", () =>
    Effect.gen(function* () {
      const inMemory = createInMemoryViewServer(viewServer);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });
      const client = yield* makeViewServerClient(viewServer, { url: server.url });

      yield* inMemory.client.publishMany("orders", [order("a", 10), order("b", 20)]);
      const subscription = yield* client.subscribe("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const eventsFiber = yield* subscription.events.pipe(
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.sleep("10 millis");

      yield* inMemory.client.patch("orders", "a", { price: 30 });
      yield* inMemory.client.delete("orders", "b");

      const events = yield* Fiber.join(eventsFiber);
      expect(events[0]).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["a", "b"],
        rows: [
          { id: "a", price: 10 },
          { id: "b", price: 20 },
        ],
        totalRows: 2,
      });
      expect(events[1]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          { type: "move", key: "b", fromIndex: 1, toIndex: 0 },
          { type: "update", key: "a", row: { id: "a", price: 30 }, index: 1 },
        ],
        totalRows: 2,
      });
      expect(events[2]).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 2,
        toVersion: 3,
        operations: [{ type: "remove", key: "b" }],
        totalRows: 1,
      });

      yield* client.close;
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("encodes subscription closed status when the runtime closes", () =>
    Effect.gen(function* () {
      const inMemory = createInMemoryViewServer(viewServer);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      const subscription = yield* client.subscribe("orders", {
        select: ["id"],
        limit: 10,
      });
      const eventsFiber = yield* subscription.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Effect.sleep("10 millis");

      yield* inMemory.close;

      const events = yield* Fiber.join(eventsFiber);
      expect(events[1]).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "closed",
        code: "SubscriptionClosed",
        message: "Subscription closed because the engine closed.",
      });

      yield* client.close;
      yield* server.close;
    }),
  );

  it.live("serves health from the runtime instead of stale live-client state", () =>
    Effect.gen(function* () {
      const inMemory = createInMemoryViewServer(viewServer);
      const baseHealth = yield* inMemory.client.health();
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: {
          ...inMemory.client,
          health: () =>
            Effect.succeed({
              ...baseHealth,
              engine: {
                topics: {
                  ...baseHealth.engine.topics,
                  orders: {
                    ...baseHealth.engine.topics.orders,
                    rowCount: 123,
                  },
                },
              },
            }),
        },
      });
      const client = yield* makeViewServerClient(viewServer, { url: server.url });

      expect(client.health.value.engine.topics.orders.rowCount).toBe(123);

      yield* client.close;
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("serves custom paths and maps hostile remote inputs", () =>
    Effect.gen(function* () {
      const inMemory = createInMemoryViewServer(viewServer);
      const server = yield* makeViewServerWebSocketServer(
        viewServer,
        {
          liveClient: inMemory.liveClient,
          runtime: inMemory.client,
        },
        { host: "127.0.0.1", path: "/custom-rpc" },
      );
      const client = yield* makeViewServerClient(viewServer, { url: server.url });

      const invalidSubscribeTopic = yield* Effect.flip(
        // @ts-expect-error hostile callers can still send unknown topics over the wire.
        client.subscribe("missing", {
          select: ["id"],
        }),
      );
      expect(invalidSubscribeTopic.code).toBe("InvalidTopic");

      const invalidQuery = yield* Effect.flip(
        client.subscribe("orders", {
          select: [
            // @ts-expect-error hostile callers can still send malformed queries over the wire.
            1,
          ],
        }),
      );
      expect(invalidQuery.code).toBe("InvalidQuery");
      expect(invalidQuery.message).toBe('Expected string, got 1\n  at ["select"][0]');

      const unknownSelect = yield* Effect.flip(
        client.subscribe("orders", {
          select: [
            // @ts-expect-error hostile callers can still send unknown projected fields.
            "missing",
          ],
        }),
      );
      expect(unknownSelect.code).toBe("InvalidQuery");
      expect(unknownSelect.message).toBe("Query references an unknown field for topic: orders");

      const unknownWhere = yield* Effect.flip(
        client.subscribe("orders", {
          select: ["id"],
          where: {
            // @ts-expect-error hostile callers can still send unknown filter fields.
            missing: { eq: "x" },
          },
        }),
      );
      expect(unknownWhere.code).toBe("InvalidQuery");
      expect(unknownWhere.message).toBe("Query references an unknown field for topic: orders");

      const unknownOrderBy = yield* Effect.flip(
        client.subscribe("orders", {
          select: ["id"],
          // @ts-expect-error hostile callers can still send unknown sort fields.
          orderBy: [
            {
              field: "missing",
              direction: "asc",
            },
          ],
        }),
      );
      expect(unknownOrderBy.code).toBe("InvalidQuery");
      expect(unknownOrderBy.message).toBe("Query references an unknown field for topic: orders");

      yield* client.close;
      yield* server.close;
      yield* inMemory.close;
    }),
  );

  it.live("cleans up engine subscribers when the remote websocket disconnects", () =>
    Effect.gen(function* () {
      const inMemory = createInMemoryViewServer(viewServer);
      const server = yield* makeViewServerWebSocketServer(viewServer, {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      });
      const client = yield* makeViewServerClient(viewServer, { url: server.url });
      const subscription = yield* client.subscribe("orders", {
        select: ["id", "price"],
        limit: 10,
      });
      const eventsFiber = yield* subscription.events.pipe(Stream.runDrain, Effect.forkChild);

      yield* Effect.sleep("10 millis");
      const beforeDisconnect = yield* inMemory.client.health();
      expect(beforeDisconnect.engine.topics.orders.activeSubscriptions).toBe(1);

      yield* client.close;
      yield* Effect.sleep("10 millis");

      const afterDisconnect = yield* inMemory.client.health();
      expect(afterDisconnect.engine.topics.orders.activeSubscriptions).toBe(0);

      yield* Fiber.interrupt(eventsFiber);
      yield* server.close;
      yield* inMemory.close;
    }),
  );
});
