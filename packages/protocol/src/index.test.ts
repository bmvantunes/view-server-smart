import { describe, expect, it } from "@effect/vitest";
import {
  defineViewServerConfig,
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
  type ViewServerHealthSummaryRow,
  type ViewServerHealthTopicRow,
} from "@view-server/config";
import { Effect, Schema } from "effect";
import {
  ViewServerBackpressureErrorSchema,
  ViewServerHealthSchema,
  ViewServerRpcErrorSchema,
  ViewServerRpcs,
  ViewServerRuntimeErrorSchema,
  ViewServerSubscribePayloadSchema,
  ViewServerTransportErrorSchema,
  ViewServerWireEventSchema,
  ViewServerWireRawQuerySchema,
  ViewServerWireRowSchema,
  viewServerDecodeHealth,
  viewServerDecodeHealthSummaryEvent,
  viewServerDecodeHealthTopicEvent,
  viewServerDecodeLiveEvent,
  viewServerDecodeRawQuery,
  viewServerDecodeTopic,
  viewServerEncodeHealthSummaryEvent,
  viewServerEncodeHealthTopicEvent,
  viewServerEncodeLiveEvent,
  viewServerEncodeRawQuery,
} from "./index";
import { SchemaGetter } from "effect";

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
    badjson: {
      schema: BadJsonRow,
      key: "id",
    },
  },
});

const topicHealth = {
  status: "ready",
  rowCount: 1,
  liveRowCount: 1,
  deletedRowCount: 0,
  version: 1,
  lastMutationAt: null,
  mutationsPerSecond: 0,
  rowsPerSecond: 0,
  pendingMutationBatches: 0,
  activeViews: 1,
  activeSubscriptions: 1,
  queuedEvents: 0,
  maxQueueDepth: 0,
  backpressureEvents: 0,
  memoryBytes: 0,
  tombstoneCount: 0,
  compactionPending: false,
} as const;

const wireHealth = {
  status: "ready",
  version: 1,
  uptimeMs: 10,
  engine: {
    topics: {
      orders: topicHealth,
      badjson: { ...topicHealth, rowCount: 0, liveRowCount: 0 },
    },
  },
  transport: {
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
  },
} as const;

describe("@view-server/protocol", () => {
  it.effect("decodes the public wire schemas", () =>
    Effect.gen(function* () {
      const row = yield* Schema.decodeUnknownEffect(ViewServerWireRowSchema)({
        id: "a",
        quantity: "10",
      });
      expect(row).toStrictEqual({
        id: "a",
        quantity: "10",
      });

      const query = yield* Schema.decodeUnknownEffect(ViewServerWireRawQuerySchema)({
        select: ["id", "quantity"],
        where: {
          quantity: { gte: "10" },
        },
        orderBy: [{ field: "quantity", direction: "asc" }],
        offset: 0,
        limit: 10,
      });
      expect(query).toStrictEqual({
        select: ["id", "quantity"],
        where: {
          quantity: { gte: "10" },
        },
        orderBy: [{ field: "quantity", direction: "asc" }],
        offset: 0,
        limit: 10,
      });

      const health = yield* Schema.decodeUnknownEffect(ViewServerHealthSchema)({
        status: "ready",
        version: 1,
        uptimeMs: 10,
        engine: {
          topics: {
            orders: {
              status: "ready",
              rowCount: 1,
              liveRowCount: 1,
              deletedRowCount: 0,
              version: 1,
              lastMutationAt: null,
              mutationsPerSecond: 0,
              rowsPerSecond: 0,
              pendingMutationBatches: 0,
              activeViews: 1,
              activeSubscriptions: 1,
              queuedEvents: 0,
              maxQueueDepth: 0,
              backpressureEvents: 0,
              memoryBytes: 0,
              tombstoneCount: 0,
              compactionPending: false,
            },
          },
        },
        transport: {
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
        },
      });
      expect(health.engine.topics["orders"]?.rowCount).toBe(1);

      const snapshot = yield* Schema.decodeUnknownEffect(ViewServerWireEventSchema)({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["a"],
        rows: [row],
        totalRows: 1,
      });
      expect(snapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["a"],
        rows: [row],
        totalRows: 1,
      });

      const delta = yield* Schema.decodeUnknownEffect(ViewServerWireEventSchema)({
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          { type: "insert", key: "a", row, index: 0 },
          { type: "move", key: "a", fromIndex: 1, toIndex: 0 },
          { type: "remove", key: "b" },
        ],
        totalRows: 1,
      });
      expect(delta.type).toBe("delta");

      const ready = yield* Schema.decodeUnknownEffect(ViewServerWireEventSchema)({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "ready",
        code: "Ready",
      });
      expect(ready.type).toBe("status");

      const stale = yield* Schema.decodeUnknownEffect(ViewServerWireEventSchema)({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "stale",
        code: "SnapshotStale",
      });
      expect(stale.type).toBe("status");

      const closed = yield* Schema.decodeUnknownEffect(ViewServerWireEventSchema)({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "closed",
        code: "SubscriptionClosed",
      });
      expect(closed.type).toBe("status");

      const error = yield* Schema.decodeUnknownEffect(ViewServerWireEventSchema)({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "error",
        code: "InvalidQuery",
      });
      expect(error.type).toBe("status");

      const subscribePayload = yield* Schema.decodeUnknownEffect(ViewServerSubscribePayloadSchema)({
        topic: "orders",
        query,
      });
      expect(subscribePayload.topic).toBe("orders");

      const backpressure = yield* Schema.decodeUnknownEffect(ViewServerBackpressureErrorSchema)({
        _tag: "ViewServerBackpressureError",
        code: "BackpressureExceeded",
        message: "queue full",
      });
      expect(backpressure.code).toBe("BackpressureExceeded");

      const runtime = yield* Schema.decodeUnknownEffect(ViewServerRuntimeErrorSchema)({
        _tag: "ViewServerRuntimeError",
        code: "InvalidTopic",
        message: "unknown",
      });
      expect(runtime.code).toBe("InvalidTopic");

      const transport = yield* Schema.decodeUnknownEffect(ViewServerTransportErrorSchema)({
        _tag: "ViewServerTransportError",
        code: "TransportError",
        message: "socket closed",
      });
      expect(transport.code).toBe("TransportError");

      const rpcError = yield* Schema.decodeUnknownEffect(ViewServerRpcErrorSchema)({
        _tag: "ViewServerRuntimeError",
        code: "InvalidQuery",
        message: "bad query",
      });
      expect(rpcError.code).toBe("InvalidQuery");

      expect(typeof ViewServerRpcs).toBe("function");
    }),
  );

  it.effect("encodes and decodes live wire codec operations", () =>
    Effect.gen(function* () {
      const topic = yield* viewServerDecodeTopic(viewServer, "orders");
      expect(topic).toBe("orders");

      const richWireQuery = yield* viewServerEncodeRawQuery(viewServer, "orders", {
        select: ["id", "price"],
        where: {
          id: { in: ["a", "b"], startsWith: "a" },
          price: { gt: 1 },
        },
        orderBy: [{ field: "price", direction: "desc" }],
        offset: 0,
        limit: 10,
      });
      expect(richWireQuery).toStrictEqual({
        select: ["id", "price"],
        where: {
          id: { in: ["a", "b"], startsWith: "a" },
          price: { gt: 1 },
        },
        orderBy: [{ field: "price", direction: "desc" }],
        offset: 0,
        limit: 10,
      });

      const scalarWireQuery = yield* viewServerEncodeRawQuery(viewServer, "orders", {
        select: ["id"],
        where: { price: 10 },
      });
      expect(scalarWireQuery).toStrictEqual({
        select: ["id"],
        where: { price: 10 },
      });
      const minimalWireQuery = yield* viewServerEncodeRawQuery(viewServer, "orders", {
        select: ["id"],
      });
      expect(minimalWireQuery).toStrictEqual({ select: ["id"] });

      const decodedQuery = yield* viewServerDecodeRawQuery(viewServer, "orders", richWireQuery);
      expect(decodedQuery).toStrictEqual(richWireQuery);
      const decodedNoWhere = yield* viewServerDecodeRawQuery(viewServer, "orders", {
        select: ["id"],
      });
      expect(decodedNoWhere).toStrictEqual({ select: ["id"] });
      const decodedScalarWhere = yield* viewServerDecodeRawQuery(viewServer, "orders", {
        select: ["id"],
        where: { price: 10 },
      });
      expect(decodedScalarWhere).toStrictEqual({
        select: ["id"],
        where: { price: 10 },
      });

      const statusEvent = yield* viewServerEncodeLiveEvent(viewServer, "orders", new Set(["id"]), {
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "ready",
        code: "Ready",
      });
      expect(statusEvent).toStrictEqual({
        type: "status",
        topic: "orders",
        queryId: "query-0",
        status: "ready",
        code: "Ready",
      });

      const snapshot = yield* viewServerEncodeLiveEvent(viewServer, "orders", new Set(["id"]), {
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["a"],
        rows: [{ id: "a" }],
        totalRows: 1,
      });
      expect(snapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "query-0",
        version: 1,
        keys: ["a"],
        rows: [{ id: "a" }],
        totalRows: 1,
      });

      const delta = yield* viewServerEncodeLiveEvent(viewServer, "orders", new Set(["id"]), {
        type: "delta",
        topic: "orders",
        queryId: "query-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          { type: "insert", key: "a", row: { id: "a" }, index: 0 },
          { type: "update", key: "b", row: { id: "b" }, index: 1 },
          { type: "move", key: "a", fromIndex: 1, toIndex: 0 },
          { type: "remove", key: "c" },
        ],
        totalRows: 2,
      });
      expect(delta.type).toBe("delta");

      const decodedStatus = yield* viewServerDecodeLiveEvent<
        typeof viewServer.topics,
        "orders",
        typeof Order.Type
      >(viewServer, "orders", new Set(["id"]), statusEvent);
      expect(decodedStatus).toStrictEqual(statusEvent);

      const decodedSnapshot = yield* viewServerDecodeLiveEvent<
        typeof viewServer.topics,
        "orders",
        Pick<typeof Order.Type, "id">
      >(viewServer, "orders", new Set(["id"]), snapshot);
      expect(decodedSnapshot).toStrictEqual(snapshot);

      const decodedDelta = yield* viewServerDecodeLiveEvent<
        typeof viewServer.topics,
        "orders",
        Pick<typeof Order.Type, "id">
      >(viewServer, "orders", new Set(["id"]), delta);
      expect(decodedDelta).toStrictEqual(delta);

      const decodedHealth = yield* viewServerDecodeHealth(viewServer, wireHealth);
      expect(decodedHealth.status).toBe("ready");
    }),
  );

  it.effect("encodes and decodes pushed health wire codec operations", () =>
    Effect.gen(function* () {
      const summaryRow: ViewServerHealthSummaryRow<typeof viewServer.topics> = {
        id: "summary",
        status: "degraded",
        runtimeStatus: "degraded",
        connectionStatus: "connected",
        unhealthyTopics: ["orders"],
        updatedAtNanos: 123n,
        maxKafkaLag: 45n,
      };

      const summaryStatus = yield* viewServerEncodeHealthSummaryEvent({
        type: "status",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "health-summary",
        status: "ready",
        code: "Ready",
      });
      expect(summaryStatus).toStrictEqual({
        type: "status",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "health-summary",
        status: "ready",
        code: "Ready",
      });

      const decodedSummaryStatus =
        yield* viewServerDecodeHealthSummaryEvent<typeof viewServer.topics>(summaryStatus);
      expect(decodedSummaryStatus).toStrictEqual(summaryStatus);

      const summarySnapshot = yield* viewServerEncodeHealthSummaryEvent({
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "health-summary",
        version: 1,
        keys: ["summary"],
        rows: [summaryRow],
        totalRows: 1,
      });
      expect(summarySnapshot).toStrictEqual({
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "health-summary",
        version: 1,
        keys: ["summary"],
        rows: [
          {
            id: "summary",
            status: "degraded",
            runtimeStatus: "degraded",
            connectionStatus: "connected",
            unhealthyTopics: ["orders"],
            updatedAtNanos: "123",
            maxKafkaLag: "45",
          },
        ],
        totalRows: 1,
      });

      const decodedSummary =
        yield* viewServerDecodeHealthSummaryEvent<typeof viewServer.topics>(summarySnapshot);
      expect(decodedSummary).toStrictEqual({
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "health-summary",
        version: 1,
        keys: ["summary"],
        rows: [summaryRow],
        totalRows: 1,
      });

      const healthTopicRow: ViewServerHealthTopicRow<"orders"> = {
        id: "orders",
        status: "ready",
        rowCount: 10,
        liveRowCount: 9,
        deletedRowCount: 1,
        version: 10,
        mutationsPerSecond: 2,
        rowsPerSecond: 3,
        pendingMutationBatches: 0,
        activeViews: 1,
        activeSubscriptions: 2,
        queuedEvents: 3,
        maxQueueDepth: 4,
        backpressureEvents: 5,
        memoryBytes: 6,
        tombstoneCount: 1,
        compactionPending: false,
        kafkaLag: 7n,
        updatedAtNanos: 456n,
      };

      const topicSnapshot = yield* viewServerEncodeHealthTopicEvent({
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        version: 1,
        keys: ["orders"],
        rows: [healthTopicRow],
        totalRows: 1,
      });
      expect(topicSnapshot).toStrictEqual({
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        version: 1,
        keys: ["orders"],
        rows: [
          {
            id: "orders",
            status: "ready",
            rowCount: 10,
            liveRowCount: 9,
            deletedRowCount: 1,
            version: 10,
            mutationsPerSecond: 2,
            rowsPerSecond: 3,
            pendingMutationBatches: 0,
            activeViews: 1,
            activeSubscriptions: 2,
            queuedEvents: 3,
            maxQueueDepth: 4,
            backpressureEvents: 5,
            memoryBytes: 6,
            tombstoneCount: 1,
            compactionPending: false,
            kafkaLag: "7",
            updatedAtNanos: "456",
          },
        ],
        totalRows: 1,
      });

      const topicDelta = yield* viewServerEncodeHealthTopicEvent({
        type: "delta",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          { type: "insert", key: "orders", row: healthTopicRow, index: 0 },
          { type: "update", key: "orders", row: { ...healthTopicRow, rowCount: 11 }, index: 0 },
          { type: "move", key: "orders", fromIndex: 1, toIndex: 0 },
          { type: "remove", key: "badjson" },
        ],
        totalRows: 1,
      });
      expect(topicDelta).toStrictEqual({
        type: "delta",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "insert",
            key: "orders",
            row: { ...healthTopicRow, kafkaLag: "7", updatedAtNanos: "456" },
            index: 0,
          },
          {
            type: "update",
            key: "orders",
            row: { ...healthTopicRow, rowCount: 11, kafkaLag: "7", updatedAtNanos: "456" },
            index: 0,
          },
          { type: "move", key: "orders", fromIndex: 1, toIndex: 0 },
          { type: "remove", key: "badjson" },
        ],
        totalRows: 1,
      });

      const decodedTopicSnapshot =
        yield* viewServerDecodeHealthTopicEvent<typeof viewServer.topics>(topicSnapshot);
      expect(decodedTopicSnapshot).toStrictEqual({
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        version: 1,
        keys: ["orders"],
        rows: [healthTopicRow],
        totalRows: 1,
      });

      const decodedTopicDelta =
        yield* viewServerDecodeHealthTopicEvent<typeof viewServer.topics>(topicDelta);
      expect(decodedTopicDelta).toStrictEqual({
        type: "delta",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          { type: "insert", key: "orders", row: healthTopicRow, index: 0 },
          { type: "update", key: "orders", row: { ...healthTopicRow, rowCount: 11 }, index: 0 },
          { type: "move", key: "orders", fromIndex: 1, toIndex: 0 },
          { type: "remove", key: "badjson" },
        ],
        totalRows: 1,
      });
    }),
  );

  it.effect("rejects invalid live wire codec inputs", () =>
    Effect.gen(function* () {
      const missingTopic = yield* Effect.flip(viewServerDecodeTopic(viewServer, "missing"));
      expect(missingTopic.code).toBe("InvalidTopic");

      const invalidEncodeTopic = yield* Effect.flip(
        // @ts-expect-error hostile callers can still encode unknown topics.
        viewServerEncodeRawQuery(viewServer, "missing", { select: ["id"] }),
      );
      expect(invalidEncodeTopic.code).toBe("InvalidTopic");

      const queryCases = [
        [{ select: [] }, "Query select must include at least one field"],
        [{ select: ["id"], offset: -1 }, "Query offset must be a non-negative integer"],
        [{ select: ["id"], limit: 0 }, "Query limit must be a positive integer"],
        [{ select: ["missing"] }, "Query references an unknown field for topic: orders"],
        [
          { select: ["id"], where: { missing: "x" } },
          "Query references an unknown field for topic: orders",
        ],
        [
          { select: ["id"], orderBy: [{ field: "missing", direction: "asc" }] },
          "Query references an unknown field for topic: orders",
        ],
      ] as const;
      for (const [query, message] of queryCases) {
        const encodeError = yield* Effect.flip(
          viewServerEncodeRawQuery(viewServer, "orders", query),
        );
        expect(encodeError.code).toBe("InvalidQuery");
        expect(encodeError.message).toBe(message);

        const decodeError = yield* Effect.flip(
          viewServerDecodeRawQuery(viewServer, "orders", query),
        );
        expect(decodeError.code).toBe("InvalidQuery");
        expect(decodeError.message).toBe(message);
      }

      const extraKey = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", { select: ["id"], whre: {} }),
      );
      expect(extraKey.code).toBe("InvalidQuery");

      const decodeExtraKey = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", { select: ["id"], whre: {} }),
      );
      expect(decodeExtraKey.code).toBe("InvalidQuery");

      const invalidFilter = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: { price: { gt: "nope" } },
        }),
      );
      expect(invalidFilter.code).toBe("InvalidQuery");

      const invalidEncodeStartsWith = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: { price: { startsWith: 1 } },
        }),
      );
      expect(invalidEncodeStartsWith.message).toBe("Filter price does not support startsWith");

      const invalidStringStartsWith = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: { id: { startsWith: 1 } },
        }),
      );
      expect(invalidStringStartsWith.message).toMatch(/Invalid filter for id/);

      const invalidDecodeStartsWith = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: { price: { startsWith: 1 } },
        }),
      );
      expect(invalidDecodeStartsWith.message).toBe("Filter price does not support startsWith");

      const invalidDecodedStringStartsWith = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: { id: { startsWith: 1 } },
        }),
      );
      expect(invalidDecodedStringStartsWith.message).toMatch(/Invalid filter for id/);

      const nonJsonFilter = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "badjson", {
          select: ["id"],
          where: { id: { eq: "x" } },
        }),
      );
      expect(nonJsonFilter.message).toMatch(/Filter id is not JSON-safe/);

      const badStartsWith = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "badjson", {
          select: ["id"],
          where: { id: { startsWith: 1 } },
        }),
      );
      expect(badStartsWith.message).toMatch(/Invalid startsWith filter for id/);

      const badDecodedField = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: { price: { gt: "nope" } },
        }),
      );
      expect(badDecodedField.code).toBe("InvalidQuery");

      const wrongEncodeTopic = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", new Set(["id"]), {
          type: "status",
          topic: "badjson",
          queryId: "query-0",
          status: "ready",
          code: "Ready",
        }),
      );
      expect(wrongEncodeTopic.code).toBe("InvalidRow");

      const wrongDecodeTopic = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", typeof Order.Type>(
          viewServer,
          "orders",
          new Set(["id"]),
          {
            type: "status",
            topic: "badjson",
            queryId: "query-0",
            status: "ready",
            code: "Ready",
          },
        ),
      );
      expect(wrongDecodeTopic.code).toBe("InvalidRow");

      const missingField = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", new Set(["id"]), {
          type: "snapshot",
          topic: "orders",
          queryId: "query-0",
          version: 1,
          keys: ["a"],
          rows: [{ price: 10 }],
          totalRows: 1,
        }),
      );
      expect(missingField.message).toBe("Missing row field for topic orders: id");

      const extraEncodeField = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", new Set(["id"]), {
          type: "snapshot",
          topic: "orders",
          queryId: "query-0",
          version: 1,
          keys: ["a"],
          rows: [{ id: "a", price: 10 }],
          totalRows: 1,
        }),
      );
      expect(extraEncodeField.message).toBe("Unexpected row field for topic orders: price");

      const invalidEncodeFieldType = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", new Set(["price"]), {
          type: "snapshot",
          topic: "orders",
          queryId: "query-0",
          version: 1,
          keys: ["a"],
          rows: [{ price: "nope" }],
          totalRows: 1,
        }),
      );
      expect(invalidEncodeFieldType.code).toBe("InvalidRow");

      const extraField = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", typeof Order.Type>(
          viewServer,
          "orders",
          new Set(["id"]),
          {
            type: "snapshot",
            topic: "orders",
            queryId: "query-0",
            version: 1,
            keys: ["a"],
            rows: [{ id: "a", price: 10 }],
            totalRows: 1,
          },
        ),
      );
      expect(extraField.message).toBe("Unexpected row field for topic orders: price");

      const missingDecodeField = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", typeof Order.Type>(
          viewServer,
          "orders",
          new Set(["price"]),
          {
            type: "snapshot",
            topic: "orders",
            queryId: "query-0",
            version: 1,
            keys: ["a"],
            rows: [{ id: "a" }],
            totalRows: 1,
          },
        ),
      );
      expect(missingDecodeField.message).toBe("Missing row field for topic orders: price");

      const invalidFieldType = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", typeof Order.Type>(
          viewServer,
          "orders",
          new Set(["price"]),
          {
            type: "snapshot",
            topic: "orders",
            queryId: "query-0",
            version: 1,
            keys: ["a"],
            rows: [{ price: "nope" }],
            totalRows: 1,
          },
        ),
      );
      expect(invalidFieldType.code).toBe("InvalidRow");

      const nonJsonRow = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "badjson", new Set(["id"]), {
          type: "snapshot",
          topic: "badjson",
          queryId: "query-0",
          version: 1,
          keys: ["a"],
          rows: [{ id: "a" }],
          totalRows: 1,
        }),
      );
      expect(nonJsonRow.message).toMatch(/Field id is not JSON-safe/);

      const missingHealthTopic = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          engine: { topics: { orders: topicHealth } },
        }),
      );
      expect(missingHealthTopic.message).toBe("Health payload is missing topic: badjson");

      const wrongSummaryEncodeTopic = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent({
          type: "status",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-summary",
          status: "ready",
          code: "Ready",
        }),
      );
      expect(wrongSummaryEncodeTopic.message).toBe(
        "Received event for __view_server_health while subscribed to __view_server_health_summary",
      );

      const wrongTopicDecodeTopic = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent<typeof viewServer.topics>({
          type: "status",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-detail",
          status: "ready",
          code: "Ready",
        }),
      );
      expect(wrongTopicDecodeTopic.message).toBe(
        "Received event for __view_server_health_summary while subscribed to __view_server_health",
      );

      const invalidHealthSummaryRow = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent<typeof viewServer.topics>({
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          version: 1,
          keys: ["summary"],
          rows: [
            {
              id: "summary",
              status: "ready",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              unhealthyTopics: ["orders"],
              updatedAtNanos: "1",
              maxKafkaLag: 1,
            },
          ],
          totalRows: 1,
        }),
      );
      expect(invalidHealthSummaryRow.message).toMatch(/Invalid system row/);

      const invalidHealthSummaryEncodeRow = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent({
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          version: 1,
          keys: ["summary"],
          rows: [
            {
              id: "summary",
              status: "ready",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              unhealthyTopics: ["orders"],
              updatedAtNanos: 1n,
              // @ts-expect-error hostile callers can pass invalid system row values.
              maxKafkaLag: 1,
            },
          ],
          totalRows: 1,
        }),
      );
      expect(invalidHealthSummaryEncodeRow.message).toMatch(/Invalid system row/);
    }),
  );
});
