import { describe, expect, it } from "@effect/vitest";
import {
  defineViewServerConfig,
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
  type ViewServerHealthSummaryRow,
  type ViewServerHealthTopicRow,
} from "@view-server/config";
import { Effect, Schema } from "effect";
import * as BigDecimal from "effect/BigDecimal";
import {
  ViewServerBackpressureErrorSchema,
  ViewServerHealthSchema,
  ViewServerRpcErrorSchema,
  ViewServerRpcs,
  ViewServerRuntimeErrorSchema,
  ViewServerSubscribePayloadSchema,
  ViewServerTransportErrorSchema,
  ViewServerTrustedWireEventSchema,
  ViewServerWireEventSchema,
  ViewServerWireGroupedQuerySchema,
  ViewServerWireRawQuerySchema,
  ViewServerWireRowSchema,
  viewServerDecodeGroupedQuery,
  viewServerDecodeHealth,
  viewServerDecodeHealthQuery,
  viewServerDecodeHealthSummaryEvent,
  viewServerDecodeHealthTopicEvent,
  viewServerDecodeLiveEvent,
  viewServerDecodeLiveQuery,
  viewServerDecodeRawQuery,
  viewServerDecodeTrustedLiveEvent,
  viewServerDecodeTopic,
  viewServerEncodeGroupedQuery,
  viewServerEncodeHealthSummaryEvent,
  viewServerEncodeHealthTopicEvent,
  viewServerEncodeLiveEvent,
  viewServerEncodeLiveQuery,
  viewServerEncodeRawQuery,
} from "./index";
import { SchemaGetter } from "effect";

const Order = Schema.Struct({
  id: Schema.String,
  status: Schema.Literals(["open", "closed"]),
  price: Schema.Number,
  quantity: Schema.BigInt,
  decimalPrice: Schema.BigDecimal,
  optionalPrice: Schema.Union([Schema.Number, Schema.Undefined]),
  optionalQuantity: Schema.Union([Schema.BigInt, Schema.Undefined]),
  unset: Schema.Undefined,
  metadata: Schema.Struct({
    _viewServerScalar: Schema.String,
    value: Schema.String,
  }),
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

const formatDecodedDecimal = (value: unknown): string =>
  BigDecimal.isBigDecimal(value) ? BigDecimal.format(value) : String(value);

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
  activeFallbackGroupedViews: 0,
  activeIncrementalGroupedViews: 1,
  activeViews: 1,
  groupedFullEvaluationCount: 0,
  groupedPatchedEvaluationCount: 0,
  activeSubscriptions: 1,
  queuedEvents: 0,
  maxQueueDepth: 0,
  backpressureEvents: 0,
  memoryBytes: 0,
  tombstoneCount: 0,
  compactionPending: false,
} as const;

const kafkaStartFromHealth = {
  consumerGroupId: "view-server-test",
  fallbackMode: "latest",
  mode: "latest",
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

      const groupedQuery = yield* Schema.decodeUnknownEffect(ViewServerWireGroupedQuerySchema)({
        groupBy: ["id"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          totalPrice: { aggFunc: "sum", field: "price" },
        },
        where: {
          price: { gte: 10 },
        },
        orderBy: [{ aggregate: "totalPrice", direction: "desc" }],
        offset: 0,
        limit: 10,
      });
      expect(groupedQuery).toStrictEqual({
        groupBy: ["id"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          totalPrice: { aggFunc: "sum", field: "price" },
        },
        where: {
          price: { gte: 10 },
        },
        orderBy: [{ aggregate: "totalPrice", direction: "desc" }],
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
              status: "degraded",
              rowCount: 1,
              liveRowCount: 1,
              deletedRowCount: 0,
              version: 1,
              lastMutationAt: null,
              mutationsPerSecond: 0,
              rowsPerSecond: 0,
              pendingMutationBatches: 0,
              activeFallbackGroupedViews: 0,
              activeIncrementalGroupedViews: 0,
              activeViews: 1,
              groupedFullEvaluationCount: 0,
              groupedPatchedEvaluationCount: 0,
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

      const largeLag = 9_007_199_254_740_993n;
      const lagHealth = yield* Schema.decodeUnknownEffect(ViewServerHealthSchema)({
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
              activeFallbackGroupedViews: 0,
              activeIncrementalGroupedViews: 0,
              activeViews: 1,
              groupedFullEvaluationCount: 0,
              groupedPatchedEvaluationCount: 0,
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
        kafka: {
          startFrom: kafkaStartFromHealth,
          regions: {},
          topics: {
            orders: {
              status: "ready",
              sourceTopic: "orders-source",
              viewServerTopic: "orders",
              regions: {
                usa: {
                  connected: true,
                  assignedPartitions: 1,
                  messagesPerSecond: 2,
                  bytesPerSecond: 2,
                  decodedMessagesPerSecond: 0,
                  decodeFailuresPerSecond: 0,
                  mappingFailuresPerSecond: 0,
                  publishFailuresPerSecond: 1,
                  commitFailuresPerSecond: 1,
                  processingFailuresPerSecond: 2,
                  lastMessageAt: 123,
                  lastCommitAt: null,
                  consumerLagMessages: largeLag.toString(),
                  lagSampledAt: null,
                  committedOffset: null,
                  lastError: "commit failed",
                },
              },
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
      expect(lagHealth.kafka?.topics["orders"]?.regions["usa"]?.consumerLagMessages).toBe(largeLag);
      expect(lagHealth.kafka?.topics["orders"]?.regions["usa"]?.processingFailuresPerSecond).toBe(
        2,
      );
      expect(lagHealth.kafka?.topics["orders"]?.regions["usa"]?.publishFailuresPerSecond).toBe(1);
      expect(lagHealth.kafka?.topics["orders"]?.regions["usa"]?.commitFailuresPerSecond).toBe(1);
      const encodedLagHealth = yield* Schema.encodeUnknownEffect(ViewServerHealthSchema)(lagHealth);
      expect(lagHealth.kafka?.startFrom).toStrictEqual(kafkaStartFromHealth);
      expect(encodedLagHealth.kafka?.topics["orders"]?.regions["usa"]?.consumerLagMessages).toBe(
        largeLag.toString(),
      );
      expect(encodedLagHealth.kafka?.startFrom).toStrictEqual(kafkaStartFromHealth);
      const impossibleKafkaStartFromHealth = yield* Effect.flip(
        Schema.decodeUnknownEffect(ViewServerHealthSchema)({
          ...wireHealth,
          kafka: {
            startFrom: {
              consumerGroupId: "view-server-invalid-latest-fail",
              fallbackMode: "fail",
              mode: "latest",
            },
            regions: {},
            topics: {},
          },
        }),
      );
      expect(String(impossibleKafkaStartFromHealth)).toContain("fallbackMode");

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
      const healthQuery = yield* viewServerDecodeHealthQuery(VIEW_SERVER_HEALTH_TOPIC, {
        select: ["id"],
      });
      expect(healthQuery).toStrictEqual({ select: ["id"] });
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

      const idQuery = { select: ["id"] };
      const statusEvent = yield* viewServerEncodeLiveEvent(viewServer, "orders", idQuery, {
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

      const malformedStatusEncode = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", idQuery, {
          type: "status",
          topic: "orders",
          queryId: "query-0",
          status: "ready",
          // @ts-expect-error ready status events can only use the Ready code.
          code: "InvalidRow",
        }),
      );
      expect(malformedStatusEncode.message).toMatch(/Invalid event/);

      const snapshot = yield* viewServerEncodeLiveEvent(viewServer, "orders", idQuery, {
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

      const delta = yield* viewServerEncodeLiveEvent(viewServer, "orders", idQuery, {
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
      >(viewServer, "orders", idQuery, statusEvent);
      expect(decodedStatus).toStrictEqual(statusEvent);

      const malformedStatusDecode = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", typeof Order.Type>(
          viewServer,
          "orders",
          idQuery,
          {
            type: "status",
            topic: "orders",
            queryId: "query-0",
            status: "ready",
            // @ts-expect-error hostile wire status can use an invalid ready code.
            code: "InvalidRow",
          },
        ),
      );
      expect(malformedStatusDecode.message).toMatch(/Invalid event/);

      const decodedSnapshot = yield* viewServerDecodeLiveEvent<
        typeof viewServer.topics,
        "orders",
        Pick<typeof Order.Type, "id">
      >(viewServer, "orders", idQuery, snapshot);
      expect(decodedSnapshot).toStrictEqual(snapshot);

      const decodedDelta = yield* viewServerDecodeLiveEvent<
        typeof viewServer.topics,
        "orders",
        Pick<typeof Order.Type, "id">
      >(viewServer, "orders", idQuery, delta);
      expect(decodedDelta).toStrictEqual(delta);

      const trustedSnapshot = yield* Schema.decodeUnknownEffect(ViewServerTrustedWireEventSchema)(
        snapshot,
      );
      const decodedTrustedSnapshot = yield* viewServerDecodeTrustedLiveEvent<
        typeof viewServer.topics,
        "orders",
        Pick<typeof Order.Type, "id">
      >(viewServer, "orders", idQuery, trustedSnapshot);
      expect(decodedTrustedSnapshot).toStrictEqual(snapshot);

      const decodedHealth = yield* viewServerDecodeHealth(viewServer, wireHealth);
      expect(decodedHealth.status).toBe("ready");
    }),
  );

  it.effect("encodes and decodes grouped query and grouped live event operations", () =>
    Effect.gen(function* () {
      const groupedQuery = {
        groupBy: ["id"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          totalPrice: { aggFunc: "sum", field: "price" },
          averagePrice: { aggFunc: "avg", field: "price" },
          minPrice: { aggFunc: "min", field: "price" },
          maxPrice: { aggFunc: "max", field: "price" },
          distinctPrice: { aggFunc: "countDistinct", field: "price" },
        },
        where: {
          id: { startsWith: "a" },
          price: { in: [10, 11], gte: 10 },
        },
        orderBy: [
          { field: "id", direction: "asc" },
          { aggregate: "totalPrice", direction: "desc" },
        ],
        offset: 0,
        limit: 10,
      };

      const encodedGrouped = yield* viewServerEncodeGroupedQuery(
        viewServer,
        "orders",
        groupedQuery,
      );
      expect(encodedGrouped).toStrictEqual(groupedQuery);

      const minimalGroupedQuery = {
        groupBy: ["id"],
        aggregates: {
          rowCount: { aggFunc: "count" },
        },
      };
      const encodedMinimalGrouped = yield* viewServerEncodeGroupedQuery(
        viewServer,
        "orders",
        minimalGroupedQuery,
      );
      expect(encodedMinimalGrouped).toStrictEqual(minimalGroupedQuery);

      const encodedDecimalGrouped = yield* viewServerEncodeGroupedQuery(viewServer, "orders", {
        groupBy: ["id"],
        aggregates: {
          totalDecimalPrice: { aggFunc: "sum", field: "decimalPrice" },
        },
      });
      expect(encodedDecimalGrouped).toStrictEqual({
        groupBy: ["id"],
        aggregates: {
          totalDecimalPrice: { aggFunc: "sum", field: "decimalPrice" },
        },
      });

      const optionalGroupedQuery = {
        groupBy: ["id"],
        aggregates: {
          totalOptionalPrice: { aggFunc: "sum", field: "optionalPrice" },
          totalOptionalQuantity: { aggFunc: "sum", field: "optionalQuantity" },
        },
      };
      const optionalGroupedEncodeError = yield* Effect.flip(
        viewServerEncodeGroupedQuery(viewServer, "orders", optionalGroupedQuery),
      );
      expect(optionalGroupedEncodeError.code).toBe("InvalidQuery");
      expect(optionalGroupedEncodeError.message).toBe(
        "Grouped aggregate totalOptionalPrice must reference a numeric field",
      );
      const optionalGroupedDecodeError = yield* Effect.flip(
        viewServerDecodeGroupedQuery(viewServer, "orders", optionalGroupedQuery),
      );
      expect(optionalGroupedDecodeError.code).toBe("InvalidQuery");
      expect(optionalGroupedDecodeError.message).toBe(
        "Grouped aggregate totalOptionalPrice must reference a numeric field",
      );

      const optionalBigIntGroupedQuery = {
        groupBy: ["id"],
        aggregates: {
          totalOptionalQuantity: { aggFunc: "sum", field: "optionalQuantity" },
        },
      };
      const optionalBigIntGroupedError = yield* Effect.flip(
        viewServerEncodeGroupedQuery(viewServer, "orders", optionalBigIntGroupedQuery),
      );
      expect(optionalBigIntGroupedError.code).toBe("InvalidQuery");
      expect(optionalBigIntGroupedError.message).toBe(
        "Grouped aggregate totalOptionalQuantity must reference a numeric field",
      );

      const decodedGrouped = yield* viewServerDecodeGroupedQuery(
        viewServer,
        "orders",
        encodedGrouped,
      );
      expect(decodedGrouped).toStrictEqual(groupedQuery);

      const decodedMinimalGrouped = yield* viewServerDecodeGroupedQuery(
        viewServer,
        "orders",
        encodedMinimalGrouped,
      );
      expect(decodedMinimalGrouped).toStrictEqual(minimalGroupedQuery);

      const decodedOptionalBigIntGroupedError = yield* Effect.flip(
        viewServerDecodeGroupedQuery(viewServer, "orders", optionalBigIntGroupedQuery),
      );
      expect(decodedOptionalBigIntGroupedError.code).toBe("InvalidQuery");
      expect(decodedOptionalBigIntGroupedError.message).toBe(
        "Grouped aggregate totalOptionalQuantity must reference a numeric field",
      );

      const encodedLiveGrouped = yield* viewServerEncodeLiveQuery(
        viewServer,
        "orders",
        groupedQuery,
      );
      expect(encodedLiveGrouped).toStrictEqual(groupedQuery);

      const encodedLiveRaw = yield* viewServerEncodeLiveQuery(viewServer, "orders", {
        select: ["id"],
      });
      expect(encodedLiveRaw).toStrictEqual({ select: ["id"] });

      const decodedLiveGrouped = yield* viewServerDecodeLiveQuery(
        viewServer,
        "orders",
        encodedLiveGrouped,
      );
      expect(decodedLiveGrouped).toStrictEqual(groupedQuery);

      const decodedLiveRaw = yield* viewServerDecodeLiveQuery(viewServer, "orders", encodedLiveRaw);
      expect(decodedLiveRaw).toStrictEqual({ select: ["id"] });

      const invalidOptionalLiveQuery = yield* Effect.flip(
        viewServerEncodeLiveQuery(viewServer, "orders", optionalGroupedQuery),
      );
      expect(invalidOptionalLiveQuery.code).toBe("InvalidQuery");

      const groupedRow = {
        id: "a",
        rowCount: 2n,
        totalPrice: BigDecimal.fromStringUnsafe("21"),
        averagePrice: BigDecimal.fromStringUnsafe("10.5"),
        minPrice: 10,
        maxPrice: 11,
        distinctPrice: 2n,
      };

      const groupedSnapshot = yield* viewServerEncodeLiveEvent(
        viewServer,
        "orders",
        encodedGrouped,
        {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-0",
          version: 1,
          keys: ["a"],
          rows: [groupedRow],
          totalRows: 1,
        },
      );
      expect(groupedSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "grouped-0",
        version: 1,
        keys: ["a"],
        rows: [
          {
            id: "a",
            rowCount: { _viewServerAggregate: "bigint", value: "2" },
            totalPrice: { _viewServerAggregate: "bigdecimal", value: "21" },
            averagePrice: { _viewServerAggregate: "bigdecimal", value: "10.5" },
            minPrice: { _viewServerAggregate: "json", value: 10 },
            maxPrice: { _viewServerAggregate: "json", value: 11 },
            distinctPrice: { _viewServerAggregate: "bigint", value: "2" },
          },
        ],
        totalRows: 1,
      });

      const decodedGroupedSnapshot = yield* viewServerDecodeLiveEvent<
        typeof viewServer.topics,
        "orders",
        typeof groupedRow
      >(viewServer, "orders", encodedGrouped, groupedSnapshot);
      const decodedGroupedSnapshotRows =
        decodedGroupedSnapshot.type === "snapshot"
          ? decodedGroupedSnapshot.rows.map((row) => ({
              ...row,
              totalPrice: formatDecodedDecimal(row.totalPrice),
              averagePrice: formatDecodedDecimal(row.averagePrice),
            }))
          : [];
      expect({
        ...decodedGroupedSnapshot,
        rows: decodedGroupedSnapshotRows,
      }).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "grouped-0",
        version: 1,
        keys: ["a"],
        rows: [{ ...groupedRow, totalPrice: "21", averagePrice: "10.5" }],
        totalRows: 1,
      });

      const invalidMinSnapshot = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", encodedGrouped, {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-undefined",
          version: 1,
          keys: ["a"],
          rows: [{ ...groupedRow, minPrice: undefined }],
          totalRows: 1,
        }),
      );
      expect(invalidMinSnapshot.message).toMatch(/Invalid field minPrice/);

      const optionalMinQuery = yield* viewServerEncodeGroupedQuery(viewServer, "orders", {
        groupBy: ["id"],
        aggregates: {
          minUnset: { aggFunc: "min", field: "unset" },
        },
      });
      const optionalMinSnapshot = yield* viewServerEncodeLiveEvent(
        viewServer,
        "orders",
        optionalMinQuery,
        {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-unset-min",
          version: 1,
          keys: ["a"],
          rows: [{ id: "a", minUnset: undefined }],
          totalRows: 1,
        },
      );
      expect(optionalMinSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "grouped-unset-min",
        version: 1,
        keys: ["a"],
        rows: [
          {
            id: "a",
            minUnset: { _viewServerAggregate: "json", value: null },
          },
        ],
        totalRows: 1,
      });
      const decodedOptionalMinSnapshot = yield* viewServerDecodeLiveEvent<
        typeof viewServer.topics,
        "orders",
        { readonly id: string; readonly minUnset: undefined }
      >(viewServer, "orders", optionalMinQuery, optionalMinSnapshot);
      expect(decodedOptionalMinSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "grouped-unset-min",
        version: 1,
        keys: ["a"],
        rows: [{ id: "a", minUnset: undefined }],
        totalRows: 1,
      });

      const objectAggregateQuery = yield* viewServerEncodeGroupedQuery(viewServer, "orders", {
        groupBy: ["id"],
        aggregates: {
          firstMetadata: { aggFunc: "min", field: "metadata" },
        },
      });
      const objectAggregateSnapshot = yield* viewServerEncodeLiveEvent(
        viewServer,
        "orders",
        objectAggregateQuery,
        {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-object",
          version: 1,
          keys: ["a"],
          rows: [
            {
              id: "a",
              firstMetadata: {
                _viewServerScalar: "bigint",
                value: "not-protocol",
              },
            },
          ],
          totalRows: 1,
        },
      );
      expect(objectAggregateSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "grouped-object",
        version: 1,
        keys: ["a"],
        rows: [
          {
            id: "a",
            firstMetadata: {
              _viewServerAggregate: "json",
              value: {
                _viewServerScalar: "bigint",
                value: "not-protocol",
              },
            },
          },
        ],
        totalRows: 1,
      });
      const decodedObjectAggregateSnapshot = yield* viewServerDecodeLiveEvent<
        typeof viewServer.topics,
        "orders",
        {
          readonly id: string;
          readonly firstMetadata: { readonly _viewServerScalar: string; readonly value: string };
        }
      >(viewServer, "orders", objectAggregateQuery, objectAggregateSnapshot);
      expect(decodedObjectAggregateSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "grouped-object",
        version: 1,
        keys: ["a"],
        rows: [
          {
            id: "a",
            firstMetadata: {
              _viewServerScalar: "bigint",
              value: "not-protocol",
            },
          },
        ],
        totalRows: 1,
      });

      const bigIntSumQuery = yield* viewServerEncodeGroupedQuery(viewServer, "orders", {
        groupBy: ["id"],
        aggregates: {
          totalQuantity: { aggFunc: "sum", field: "quantity" },
        },
      });
      const bigIntSumSnapshot = yield* viewServerEncodeLiveEvent(
        viewServer,
        "orders",
        bigIntSumQuery,
        {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-bigint-sum",
          version: 1,
          keys: ["a"],
          rows: [{ id: "a", totalQuantity: 3n }],
          totalRows: 1,
        },
      );
      expect(bigIntSumSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "grouped-bigint-sum",
        version: 1,
        keys: ["a"],
        rows: [
          {
            id: "a",
            totalQuantity: { _viewServerAggregate: "bigint", value: "3" },
          },
        ],
        totalRows: 1,
      });
      const decodedBigIntSumSnapshot = yield* viewServerDecodeLiveEvent<
        typeof viewServer.topics,
        "orders",
        {
          readonly id: string;
          readonly totalQuantity: bigint;
        }
      >(viewServer, "orders", bigIntSumQuery, bigIntSumSnapshot);
      expect(decodedBigIntSumSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "orders",
        queryId: "grouped-bigint-sum",
        version: 1,
        keys: ["a"],
        rows: [{ id: "a", totalQuantity: 3n }],
        totalRows: 1,
      });

      const groupedDelta = yield* viewServerEncodeLiveEvent(viewServer, "orders", encodedGrouped, {
        type: "delta",
        topic: "orders",
        queryId: "grouped-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          { type: "insert", key: "a", row: groupedRow, index: 0 },
          {
            type: "update",
            key: "b",
            row: { ...groupedRow, id: "b", rowCount: 3n },
            index: 1,
          },
          { type: "move", key: "a", fromIndex: 1, toIndex: 0 },
          { type: "remove", key: "c" },
        ],
        totalRows: 2,
      });

      const decodedGroupedDelta = yield* viewServerDecodeLiveEvent<
        typeof viewServer.topics,
        "orders",
        typeof groupedRow
      >(viewServer, "orders", encodedGrouped, groupedDelta);
      const decodedGroupedDeltaOperations =
        decodedGroupedDelta.type === "delta"
          ? decodedGroupedDelta.operations.map((operation) =>
              operation.type === "insert" || operation.type === "update"
                ? {
                    ...operation,
                    row: {
                      ...operation.row,
                      totalPrice: formatDecodedDecimal(operation.row.totalPrice),
                      averagePrice: formatDecodedDecimal(operation.row.averagePrice),
                    },
                  }
                : operation,
            )
          : [];
      expect({
        ...decodedGroupedDelta,
        operations: decodedGroupedDeltaOperations,
      }).toStrictEqual({
        type: "delta",
        topic: "orders",
        queryId: "grouped-0",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "insert",
            key: "a",
            row: { ...groupedRow, totalPrice: "21", averagePrice: "10.5" },
            index: 0,
          },
          {
            type: "update",
            key: "b",
            row: { ...groupedRow, id: "b", rowCount: 3n, totalPrice: "21", averagePrice: "10.5" },
            index: 1,
          },
          { type: "move", key: "a", fromIndex: 1, toIndex: 0 },
          { type: "remove", key: "c" },
        ],
        totalRows: 2,
      });
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

      const summaryStatus = yield* viewServerEncodeHealthSummaryEvent(viewServer, {
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

      const decodedSummaryStatus = yield* viewServerDecodeHealthSummaryEvent(
        viewServer,
        summaryStatus,
      );
      expect(decodedSummaryStatus).toStrictEqual(summaryStatus);

      const summarySnapshot = yield* viewServerEncodeHealthSummaryEvent(viewServer, {
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

      const decodedSummary = yield* viewServerDecodeHealthSummaryEvent(viewServer, summarySnapshot);
      expect(decodedSummary).toStrictEqual({
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "health-summary",
        version: 1,
        keys: ["summary"],
        rows: [summaryRow],
        totalRows: 1,
      });

      const disconnectedSummary = yield* viewServerDecodeHealthSummaryEvent(viewServer, {
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "health-summary",
        version: 1,
        keys: ["summary"],
        rows: [
          {
            id: "summary",
            status: "disconnected",
            runtimeStatus: "ready",
            connectionStatus: "disconnected",
            unhealthyTopics: [],
            updatedAtNanos: "1",
            maxKafkaLag: "0",
          },
        ],
        totalRows: 1,
      });
      expect(disconnectedSummary.type).toBe("snapshot");

      const summaryDelta = yield* viewServerEncodeHealthSummaryEvent(viewServer, {
        type: "delta",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "health-summary",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "update",
            key: "summary",
            row: { ...summaryRow, unhealthyTopics: [] },
            index: 0,
          },
          { type: "move", key: "summary", fromIndex: 0, toIndex: 0 },
        ],
        totalRows: 1,
      });
      const decodedSummaryDelta = yield* viewServerDecodeHealthSummaryEvent(
        viewServer,
        summaryDelta,
      );
      expect(decodedSummaryDelta).toStrictEqual({
        type: "delta",
        topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        queryId: "health-summary",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "update",
            key: "summary",
            row: { ...summaryRow, unhealthyTopics: [] },
            index: 0,
          },
          { type: "move", key: "summary", fromIndex: 0, toIndex: 0 },
        ],
        totalRows: 1,
      });

      const healthTopicRow: ViewServerHealthTopicRow<"orders"> = {
        id: "orders",
        status: "ready",
        rowCount: 10,
        liveRowCount: 9,
        deletedRowCount: 1,
        version: 10,
        lastMutationAt: null,
        mutationsPerSecond: 2,
        rowsPerSecond: 3,
        pendingMutationBatches: 0,
        activeFallbackGroupedViews: 0,
        activeIncrementalGroupedViews: 0,
        activeViews: 1,
        groupedFullEvaluationCount: 0,
        groupedPatchedEvaluationCount: 0,
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
      const badJsonHealthTopicRow: ViewServerHealthTopicRow<"badjson"> = {
        ...healthTopicRow,
        id: "badjson",
        rowCount: 0,
        liveRowCount: 0,
        version: 0,
      };

      const topicSnapshot = yield* viewServerEncodeHealthTopicEvent(viewServer, {
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        version: 1,
        keys: ["orders", "badjson"],
        rows: [healthTopicRow, badJsonHealthTopicRow],
        totalRows: 2,
      });
      expect(topicSnapshot).toStrictEqual({
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        version: 1,
        keys: ["orders", "badjson"],
        rows: [
          {
            id: "orders",
            status: "ready",
            rowCount: 10,
            liveRowCount: 9,
            deletedRowCount: 1,
            version: 10,
            lastMutationAt: null,
            mutationsPerSecond: 2,
            rowsPerSecond: 3,
            pendingMutationBatches: 0,
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: 0,
            activeViews: 1,
            groupedFullEvaluationCount: 0,
            groupedPatchedEvaluationCount: 0,
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
          {
            ...badJsonHealthTopicRow,
            kafkaLag: "7",
            updatedAtNanos: "456",
          },
        ],
        totalRows: 2,
      });

      const topicDelta = yield* viewServerEncodeHealthTopicEvent(viewServer, {
        type: "delta",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          { type: "update", key: "orders", row: { ...healthTopicRow, rowCount: 11 }, index: 0 },
          { type: "move", key: "orders", fromIndex: 1, toIndex: 0 },
        ],
        totalRows: 2,
      });
      expect(topicDelta).toStrictEqual({
        type: "delta",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          {
            type: "update",
            key: "orders",
            row: { ...healthTopicRow, rowCount: 11, kafkaLag: "7", updatedAtNanos: "456" },
            index: 0,
          },
          { type: "move", key: "orders", fromIndex: 1, toIndex: 0 },
        ],
        totalRows: 2,
      });

      const decodedTopicSnapshot = yield* viewServerDecodeHealthTopicEvent(
        viewServer,
        topicSnapshot,
      );
      expect(decodedTopicSnapshot).toStrictEqual({
        type: "snapshot",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        version: 1,
        keys: ["orders", "badjson"],
        rows: [healthTopicRow, badJsonHealthTopicRow],
        totalRows: 2,
      });

      const decodedTopicDelta = yield* viewServerDecodeHealthTopicEvent(viewServer, topicDelta);
      expect(decodedTopicDelta).toStrictEqual({
        type: "delta",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        fromVersion: 1,
        toVersion: 2,
        operations: [
          { type: "update", key: "orders", row: { ...healthTopicRow, rowCount: 11 }, index: 0 },
          { type: "move", key: "orders", fromIndex: 1, toIndex: 0 },
        ],
        totalRows: 2,
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
        [
          { select: ["id"], offset: Number.MAX_SAFE_INTEGER + 1 },
          "Query offset must be a non-negative integer",
        ],
        [{ select: ["id"], limit: -1 }, "Query limit must be a non-negative integer"],
        [
          { select: ["id"], limit: Number.MAX_SAFE_INTEGER + 1 },
          "Query limit must be a non-negative integer",
        ],
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

      const malformedGroupedEncode = yield* Effect.flip(
        viewServerEncodeGroupedQuery(viewServer, "orders", {
          groupBy: ["id"],
          aggregates: { rowCount: { aggFunc: "count" } },
          typo: true,
        }),
      );
      expect(malformedGroupedEncode.code).toBe("InvalidQuery");

      const malformedGroupedDecode = yield* Effect.flip(
        viewServerDecodeGroupedQuery(viewServer, "orders", {
          groupBy: ["id"],
          aggregates: { rowCount: { aggFunc: "count" } },
          typo: true,
        }),
      );
      expect(malformedGroupedDecode.code).toBe("InvalidQuery");

      const groupedQueryCases = [
        [
          { groupBy: [], aggregates: { rowCount: { aggFunc: "count" } } },
          "Grouped query groupBy must include at least one field",
        ],
        [
          { groupBy: ["id"], aggregates: {} },
          "Grouped query aggregates must include at least one aggregate",
        ],
        [
          { groupBy: ["missing"], aggregates: { rowCount: { aggFunc: "count" } } },
          "Query references an unknown field for topic: orders",
        ],
        [
          { groupBy: ["id"], aggregates: { id: { aggFunc: "count" } } },
          "Aggregate alias collides with groupBy field: id",
        ],
        [
          { groupBy: ["id"], aggregates: { constructor: { aggFunc: "count" } } },
          "Grouped aggregate alias is not allowed: constructor",
        ],
        [
          { groupBy: ["id"], aggregates: { total: { aggFunc: "sum", field: "missing" } } },
          "Query references an unknown field for topic: orders",
        ],
        [
          { groupBy: ["id"], aggregates: { total: { aggFunc: "sum", field: "id" } } },
          "Grouped aggregate total must reference a numeric field",
        ],
        [
          {
            groupBy: ["id"],
            aggregates: { rowCount: { aggFunc: "count" } },
            where: { missing: "x" },
          },
          "Query references an unknown field for topic: orders",
        ],
        [
          {
            groupBy: ["id"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: [{ field: "price", direction: "asc" }],
          },
          "Grouped orderBy field is not in groupBy: price",
        ],
        [
          {
            groupBy: ["id"],
            aggregates: { rowCount: { aggFunc: "count" } },
            orderBy: [{ aggregate: "missing", direction: "asc" }],
          },
          "Grouped orderBy aggregate is not defined: missing",
        ],
        [
          { groupBy: ["id"], aggregates: { rowCount: { aggFunc: "count" } }, offset: -1 },
          "Query offset must be a non-negative integer",
        ],
        [
          {
            groupBy: ["id"],
            aggregates: { rowCount: { aggFunc: "count" } },
            offset: Number.MAX_SAFE_INTEGER + 1,
          },
          "Query offset must be a non-negative integer",
        ],
        [
          { groupBy: ["id"], aggregates: { rowCount: { aggFunc: "count" } }, limit: -1 },
          "Query limit must be a non-negative integer",
        ],
        [
          {
            groupBy: ["id"],
            aggregates: { rowCount: { aggFunc: "count" } },
            limit: Number.MAX_SAFE_INTEGER + 1,
          },
          "Query limit must be a non-negative integer",
        ],
      ] as const;
      for (const [query, message] of groupedQueryCases) {
        const encodeError = yield* Effect.flip(
          viewServerEncodeGroupedQuery(viewServer, "orders", query),
        );
        expect(encodeError.code).toBe("InvalidQuery");
        expect(encodeError.message).toBe(message);

        const decodeError = yield* Effect.flip(
          viewServerDecodeGroupedQuery(viewServer, "orders", query),
        );
        expect(decodeError.code).toBe("InvalidQuery");
        expect(decodeError.message).toBe(message);
      }

      const malformedSchemaConfig = {
        topics: {
          broken: {
            schema: {
              fields: {
                id: { ast: "not-a-schema-ast" },
              },
            },
            key: "id",
          },
        },
      };
      const malformedSchemaNumericField = yield* Effect.flip(
        // @ts-expect-error hostile config can have malformed field schemas.
        viewServerEncodeGroupedQuery(malformedSchemaConfig, "broken", {
          groupBy: ["id"],
          aggregates: { total: { aggFunc: "sum", field: "id" } },
        }),
      );
      expect(malformedSchemaNumericField.message).toBe(
        "Grouped aggregate total must reference a numeric field",
      );

      const malformedPrimitiveSchemaConfig = {
        topics: {
          broken: {
            schema: {
              fields: {
                id: "not-a-schema",
              },
            },
            key: "id",
          },
        },
      };
      const primitiveSchemaNumericField = yield* Effect.flip(
        // @ts-expect-error hostile config can have primitive field schemas.
        viewServerEncodeGroupedQuery(malformedPrimitiveSchemaConfig, "broken", {
          groupBy: ["id"],
          aggregates: { total: { aggFunc: "sum", field: "id" } },
        }),
      );
      expect(primitiveSchemaNumericField.message).toBe(
        "Grouped aggregate total must reference a numeric field",
      );

      const invalidGroupedEncodeTopic = yield* Effect.flip(
        // @ts-expect-error hostile callers can still encode unknown topics.
        viewServerEncodeGroupedQuery(viewServer, "missing", {
          groupBy: ["id"],
          aggregates: { rowCount: { aggFunc: "count" } },
        }),
      );
      expect(invalidGroupedEncodeTopic.code).toBe("InvalidTopic");

      const liveGroupedDecodeError = yield* Effect.flip(
        viewServerDecodeLiveQuery(viewServer, "orders", {
          groupBy: ["id"],
          aggregates: { rowCount: { aggFunc: "count" } },
          orderBy: [{ aggregate: "missing", direction: "asc" }],
        }),
      );
      expect(liveGroupedDecodeError.message).toBe(
        "Grouped orderBy aggregate is not defined: missing",
      );

      const invalidFilter = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: { price: { gt: "nope" } },
        }),
      );
      expect(invalidFilter.code).toBe("InvalidQuery");
      expect(invalidFilter.message).toBe('Invalid filter for price: Expected number, got "nope"');

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
      expect(invalidStringStartsWith.message).toBe("Invalid filter for id: expected string");

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
      expect(invalidDecodedStringStartsWith.message).toBe("Invalid filter for id: expected string");

      const trimmedViewServer = defineViewServerConfig({
        topics: {
          trimmed: {
            schema: Schema.Struct({
              id: Schema.Trim,
            }),
            key: "id",
          },
        },
      });
      const encodedTrimmedStartsWith = yield* viewServerEncodeRawQuery(
        trimmedViewServer,
        "trimmed",
        {
          select: ["id"],
          where: { id: { startsWith: "  abc  " } },
        },
      );
      expect(encodedTrimmedStartsWith).toStrictEqual({
        select: ["id"],
        where: { id: { startsWith: "  abc  " } },
      });

      const decodedTrimmedStartsWith = yield* viewServerDecodeRawQuery(
        trimmedViewServer,
        "trimmed",
        {
          select: ["id"],
          where: { id: { startsWith: "  abc  " } },
        },
      );
      expect(decodedTrimmedStartsWith).toStrictEqual({
        select: ["id"],
        where: { id: { startsWith: "  abc  " } },
      });

      const badJsonStartsWith = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "badjson", {
          select: ["id"],
          where: { id: { startsWith: 1 } },
        }),
      );
      expect(badJsonStartsWith.message).toBe("Invalid filter for id: expected string");

      const refinedStringViewServer = defineViewServerConfig({
        topics: {
          refined: {
            schema: Schema.Struct({
              id: Schema.String.check(Schema.isMinLength(2)),
            }),
            key: "id",
          },
        },
      });
      const encodedRefinedStartsWith = yield* viewServerEncodeRawQuery(
        refinedStringViewServer,
        "refined",
        {
          select: ["id"],
          where: { id: { startsWith: "x" } },
        },
      );
      expect(encodedRefinedStartsWith).toStrictEqual({
        select: ["id"],
        where: { id: { startsWith: "x" } },
      });

      const decodedRefinedStartsWith = yield* viewServerDecodeRawQuery(
        refinedStringViewServer,
        "refined",
        {
          select: ["id"],
          where: { id: { startsWith: "x" } },
        },
      );
      expect(decodedRefinedStartsWith).toStrictEqual({
        select: ["id"],
        where: { id: { startsWith: "x" } },
      });

      const encodedLiteralStartsWith = yield* viewServerEncodeRawQuery(viewServer, "orders", {
        select: ["status"],
        where: { status: { startsWith: "op" } },
      });
      expect(encodedLiteralStartsWith).toStrictEqual({
        select: ["status"],
        where: { status: { startsWith: "op" } },
      });

      const structuredEncodeStartsWith = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: {
            metadata: {
              startsWith: {
                _viewServerScalar: "kind",
                value: "x",
              },
            },
          },
        }),
      );
      expect(structuredEncodeStartsWith.message).toBe(
        "Filter metadata does not support startsWith",
      );

      const structuredDecodeStartsWith = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: {
            metadata: {
              startsWith: {
                _viewServerScalar: "kind",
                value: "x",
              },
            },
          },
        }),
      );
      expect(structuredDecodeStartsWith.message).toBe(
        "Filter metadata does not support startsWith",
      );

      const structuredEncodeRange = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: {
            metadata: {
              gt: {
                _viewServerScalar: "kind",
                value: "x",
              },
            },
          },
        }),
      );
      expect(structuredEncodeRange.message).toBe(
        "Filter metadata does not support range operators",
      );

      const structuredDecodeRange = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: {
            metadata: {
              gt: {
                _viewServerScalar: "kind",
                value: "x",
              },
            },
          },
        }),
      );
      expect(structuredDecodeRange.message).toBe(
        "Filter metadata does not support range operators",
      );

      const invalidRangeOperator = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: { id: { gt: "a" } },
        }),
      );
      expect(invalidRangeOperator.message).toBe("Filter id does not support range operators");

      const nonJsonFilter = yield* Effect.flip(
        viewServerEncodeRawQuery(viewServer, "badjson", {
          select: ["id"],
          where: { id: { eq: "x" } },
        }),
      );
      expect(nonJsonFilter.message).toBe(
        "Filter id is not JSON-safe: Expected JSON value, got Symbol(not-json)",
      );

      const badDecodedField = yield* Effect.flip(
        viewServerDecodeRawQuery(viewServer, "orders", {
          select: ["id"],
          where: { price: { gt: "nope" } },
        }),
      );
      expect(badDecodedField.code).toBe("InvalidQuery");

      const idQuery = { select: ["id"] };
      const priceQuery = { select: ["price"] };
      const wrongEncodeTopic = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", idQuery, {
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
          idQuery,
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
        viewServerEncodeLiveEvent(viewServer, "orders", idQuery, {
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
        viewServerEncodeLiveEvent(viewServer, "orders", idQuery, {
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
        viewServerEncodeLiveEvent(viewServer, "orders", priceQuery, {
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
      expect(invalidEncodeFieldType.message).toBe(
        'Invalid field price: Expected number, got "nope"',
      );

      const extraField = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", typeof Order.Type>(
          viewServer,
          "orders",
          idQuery,
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
          priceQuery,
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
          priceQuery,
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
      expect(invalidFieldType.message).toBe(
        'Invalid field price: Expected "Infinity" | "-Infinity" | "NaN", got "nope"',
      );

      const nonJsonRow = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "badjson", idQuery, {
          type: "snapshot",
          topic: "badjson",
          queryId: "query-0",
          version: 1,
          keys: ["a"],
          rows: [{ id: "a" }],
          totalRows: 1,
        }),
      );
      expect(nonJsonRow.message).toBe(
        "Field id is not JSON-safe: Expected JSON value, got Symbol(not-json)",
      );

      const BadJsonAggregateRow = Schema.Struct({
        id: Schema.String,
        value: BadJsonField,
      });
      const badJsonAggregateViewServer = defineViewServerConfig({
        topics: {
          badAggregate: {
            schema: BadJsonAggregateRow,
            key: "id",
          },
        },
      });
      const badJsonAggregateQuery = yield* viewServerEncodeGroupedQuery(
        badJsonAggregateViewServer,
        "badAggregate",
        {
          groupBy: ["id"],
          aggregates: {
            badValue: { aggFunc: "min", field: "value" },
          },
        },
      );
      const nonJsonAggregate = yield* Effect.flip(
        viewServerEncodeLiveEvent(
          badJsonAggregateViewServer,
          "badAggregate",
          badJsonAggregateQuery,
          {
            type: "snapshot",
            topic: "badAggregate",
            queryId: "grouped-bad-aggregate",
            version: 1,
            keys: ["a"],
            rows: [{ id: "a", badValue: "not-json-safe" }],
            totalRows: 1,
          },
        ),
      );
      expect(nonJsonAggregate.code).toBe("InvalidRow");
      expect(nonJsonAggregate.message).toBe(
        "Field badValue is not JSON-safe: Expected JSON value, got Symbol(not-json)",
      );

      const groupedQuery = yield* viewServerEncodeGroupedQuery(viewServer, "orders", {
        groupBy: ["id"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          averagePrice: { aggFunc: "avg", field: "price" },
        },
      });

      const missingGroupedField = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", groupedQuery, {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-0",
          version: 1,
          keys: ["a"],
          rows: [{ rowCount: 1n, averagePrice: BigDecimal.fromStringUnsafe("1.5") }],
          totalRows: 1,
        }),
      );
      expect(missingGroupedField.message).toBe("Missing grouped row field for topic orders: id");

      const missingGroupedAggregate = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", groupedQuery, {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-0",
          version: 1,
          keys: ["a"],
          rows: [{ id: "a", averagePrice: BigDecimal.fromStringUnsafe("1.5") }],
          totalRows: 1,
        }),
      );
      expect(missingGroupedAggregate.message).toBe(
        "Missing grouped aggregate for topic orders: rowCount",
      );

      const missingGroupedAggregateDefinition = yield* Effect.flip(
        viewServerEncodeLiveEvent(
          viewServer,
          "orders",
          {
            groupBy: ["id"],
            aggregates: {
              // @ts-expect-error hostile query payload can omit an aggregate definition value.
              missing: undefined,
            },
          },
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [{ id: "a", missing: 1n }],
            totalRows: 1,
          },
        ),
      );
      expect(missingGroupedAggregateDefinition.message).toBe(
        "Missing grouped aggregate definition for topic orders: missing",
      );

      const unexpectedGroupedField = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", groupedQuery, {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-0",
          version: 1,
          keys: ["a"],
          rows: [
            {
              id: "a",
              rowCount: 1n,
              averagePrice: BigDecimal.fromStringUnsafe("1.5"),
              extra: true,
            },
          ],
          totalRows: 1,
        }),
      );
      expect(unexpectedGroupedField.message).toBe(
        "Unexpected grouped row field for topic orders: extra",
      );

      const nonJsonGroupedAggregate = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", groupedQuery, {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-0",
          version: 1,
          keys: ["a"],
          rows: [
            {
              id: "a",
              rowCount: Symbol("bad"),
              averagePrice: BigDecimal.fromStringUnsafe("1.5"),
            },
          ],
          totalRows: 1,
        }),
      );
      expect(nonJsonGroupedAggregate.message).toBe("Aggregate rowCount must be a bigint.");

      const invalidBigDecimalGroupedAggregate = yield* Effect.flip(
        viewServerEncodeLiveEvent(viewServer, "orders", groupedQuery, {
          type: "snapshot",
          topic: "orders",
          queryId: "grouped-0",
          version: 1,
          keys: ["a"],
          rows: [
            {
              id: "a",
              rowCount: 1n,
              averagePrice: 1,
            },
          ],
          totalRows: 1,
        }),
      );
      expect(invalidBigDecimalGroupedAggregate.message).toBe(
        "Aggregate averagePrice must be a BigDecimal.",
      );

      const invalidGroupedField = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: 10,
                rowCount: { _viewServerAggregate: "bigint", value: "1" },
                averagePrice: { _viewServerAggregate: "bigdecimal", value: "1.5" },
              },
            ],
            totalRows: 1,
          },
        ),
      );
      expect(invalidGroupedField.message).toBe("Invalid field id: Expected string, got 10");

      const missingDecodedGroupedField = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                rowCount: { _viewServerAggregate: "bigint", value: "1" },
                averagePrice: { _viewServerAggregate: "bigdecimal", value: "1.5" },
              },
            ],
            totalRows: 1,
          },
        ),
      );
      expect(missingDecodedGroupedField.message).toBe(
        "Missing grouped row field for topic orders: id",
      );

      const missingDecodedGroupedAggregate = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                averagePrice: { _viewServerAggregate: "bigdecimal", value: "1.5" },
              },
            ],
            totalRows: 1,
          },
        ),
      );
      expect(missingDecodedGroupedAggregate.message).toBe(
        "Missing grouped aggregate for topic orders: rowCount",
      );

      const missingDecodedGroupedAggregateDefinition = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          {
            groupBy: ["id"],
            aggregates: {
              // @ts-expect-error hostile query payload can omit an aggregate definition value.
              missing: undefined,
            },
          },
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                missing: { _viewServerAggregate: "bigint", value: "1" },
              },
            ],
            totalRows: 1,
          },
        ),
      );
      expect(missingDecodedGroupedAggregateDefinition.message).toBe(
        "Missing grouped aggregate definition for topic orders: missing",
      );

      const unexpectedDecodedGroupedField = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                rowCount: { _viewServerAggregate: "bigint", value: "1" },
                averagePrice: { _viewServerAggregate: "bigdecimal", value: "1.5" },
                extra: true,
              },
            ],
            totalRows: 1,
          },
        ),
      );
      expect(unexpectedDecodedGroupedField.message).toBe(
        "Unexpected grouped row field for topic orders: extra",
      );

      const nonJsonDecodedGroupedAggregate = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                rowCount: { _viewServerAggregate: "nope", value: "bad" },
                averagePrice: { _viewServerAggregate: "bigdecimal", value: "1.5" },
              },
            ],
            totalRows: 1,
          },
        ),
      );
      expect(nonJsonDecodedGroupedAggregate.message).toBe(
        "Aggregate rowCount must be a View Server aggregate envelope.",
      );

      const numericBigIntEnvelope = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                rowCount: { _viewServerAggregate: "bigint", value: 1 },
                averagePrice: { _viewServerAggregate: "bigdecimal", value: "1.5" },
              },
            ],
            totalRows: 1,
          },
        ),
      );
      expect(numericBigIntEnvelope.message).toBe(
        "Aggregate rowCount must be a View Server aggregate envelope.",
      );

      const invalidGroupedBigInt = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                rowCount: { _viewServerAggregate: "bigint", value: "not-a-bigint" },
                averagePrice: { _viewServerAggregate: "bigdecimal", value: "1.5" },
              },
            ],
            totalRows: 1,
          },
        ),
      );
      expect(invalidGroupedBigInt.message).toBe("Aggregate rowCount must be a bigint envelope.");

      const invalidGroupedBigDecimal = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                rowCount: { _viewServerAggregate: "bigint", value: "1" },
                averagePrice: { _viewServerAggregate: "bigdecimal", value: "nope" },
              },
            ],
            totalRows: 1,
          },
        ),
      );
      expect(invalidGroupedBigDecimal.message).toMatch(/Invalid aggregate averagePrice/);

      const numericBigDecimalEnvelope = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                rowCount: { _viewServerAggregate: "bigint", value: "1" },
                averagePrice: { _viewServerAggregate: "bigdecimal", value: 1 },
              },
            ],
            totalRows: 1,
          },
        ),
      );
      expect(numericBigDecimalEnvelope.message).toBe(
        "Aggregate averagePrice must be a View Server aggregate envelope.",
      );

      const wrongGroupedBigDecimalEnvelope = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-0",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                rowCount: { _viewServerAggregate: "bigint", value: "1" },
                averagePrice: { _viewServerAggregate: "bigint", value: "1" },
              },
            ],
            totalRows: 1,
          },
        ),
      );
      expect(wrongGroupedBigDecimalEnvelope.message).toBe(
        "Aggregate averagePrice must be a BigDecimal envelope.",
      );

      const groupedMinQuery = yield* viewServerEncodeGroupedQuery(viewServer, "orders", {
        groupBy: ["id"],
        aggregates: {
          minPrice: { aggFunc: "min", field: "price" },
        },
      });
      const wrongJsonEnvelope = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedMinQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-min",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                minPrice: { _viewServerAggregate: "bigint", value: "1" },
              },
            ],
            totalRows: 1,
          },
        ),
      );
      expect(wrongJsonEnvelope.message).toBe(
        "Aggregate minPrice must be a JSON aggregate envelope.",
      );

      const invalidJsonAggregateValue = yield* Effect.flip(
        viewServerDecodeLiveEvent<typeof viewServer.topics, "orders", object>(
          viewServer,
          "orders",
          groupedMinQuery,
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-min",
            version: 1,
            keys: ["a"],
            rows: [
              {
                id: "a",
                minPrice: { _viewServerAggregate: "json", value: "not-a-number" },
              },
            ],
            totalRows: 1,
          },
        ),
      );
      expect(invalidJsonAggregateValue.message).toBe(
        'Invalid field minPrice: Expected "Infinity" | "-Infinity" | "NaN", got "not-a-number"',
      );

      const missingAggregateSourceField = yield* Effect.flip(
        viewServerEncodeLiveEvent(
          viewServer,
          "orders",
          {
            groupBy: ["id"],
            aggregates: {
              badPrice: { aggFunc: "min", field: "missing" },
            },
          },
          {
            type: "snapshot",
            topic: "orders",
            queryId: "grouped-missing-field",
            version: 1,
            keys: ["a"],
            rows: [{ id: "a", badPrice: 1 }],
            totalRows: 1,
          },
        ),
      );
      expect(missingAggregateSourceField.message).toBe(
        "Aggregate references unknown field for topic orders: missing",
      );

      const malformedEventSchemaConfig = {
        topics: {
          broken: {
            schema: {
              fields: {
                group: Schema.String,
                value: "not-a-schema",
              },
            },
            key: "group",
          },
        },
      };
      const malformedEventSchemaSnapshot = yield* viewServerEncodeLiveEvent(
        // @ts-expect-error hostile config can have malformed aggregate field schemas.
        malformedEventSchemaConfig,
        "broken",
        {
          groupBy: ["group"],
          aggregates: {
            totalValue: { aggFunc: "sum", field: "value" },
          },
        },
        {
          type: "snapshot",
          topic: "broken",
          queryId: "grouped-malformed-schema",
          version: 1,
          keys: ["a"],
          rows: [
            {
              group: "a",
              totalValue: BigDecimal.fromStringUnsafe("1"),
            },
          ],
          totalRows: 1,
        },
      );
      expect(malformedEventSchemaSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "broken",
        queryId: "grouped-malformed-schema",
        version: 1,
        keys: ["a"],
        rows: [
          {
            group: "a",
            totalValue: { _viewServerAggregate: "bigdecimal", value: "1" },
          },
        ],
        totalRows: 1,
      });

      const malformedEventAstSchemaConfig = {
        topics: {
          broken: {
            schema: {
              fields: {
                group: Schema.String,
                value: { ast: "not-a-schema-ast" },
              },
            },
            key: "group",
          },
        },
      };
      const malformedEventAstSchemaSnapshot = yield* viewServerEncodeLiveEvent(
        // @ts-expect-error hostile config can have malformed aggregate field schema ASTs.
        malformedEventAstSchemaConfig,
        "broken",
        {
          groupBy: ["group"],
          aggregates: {
            totalValue: { aggFunc: "sum", field: "value" },
          },
        },
        {
          type: "snapshot",
          topic: "broken",
          queryId: "grouped-malformed-schema-ast",
          version: 1,
          keys: ["a"],
          rows: [
            {
              group: "a",
              totalValue: BigDecimal.fromStringUnsafe("1"),
            },
          ],
          totalRows: 1,
        },
      );
      expect(malformedEventAstSchemaSnapshot).toStrictEqual({
        type: "snapshot",
        topic: "broken",
        queryId: "grouped-malformed-schema-ast",
        version: 1,
        keys: ["a"],
        rows: [
          {
            group: "a",
            totalValue: { _viewServerAggregate: "bigdecimal", value: "1" },
          },
        ],
        totalRows: 1,
      });

      const missingHealthTopic = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          engine: { topics: { orders: topicHealth } },
        }),
      );
      expect(missingHealthTopic.message).toBe("Health payload is missing topic: badjson");

      const extraHealthTopic = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          engine: { topics: { ...wireHealth.engine.topics, missing: topicHealth } },
        }),
      );
      expect(extraHealthTopic.message).toBe("Health payload references unknown topic: missing");

      const reservedExtraHealthTopic = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          engine: { topics: { ...wireHealth.engine.topics, __view_server_health: topicHealth } },
        }),
      );
      expect(reservedExtraHealthTopic.message).toBe(
        "Health payload references unknown topic: __view_server_health",
      );

      const healthWithExtras = {
        ...wireHealth,
        extraRoot: "drop-me",
        engine: {
          topics: {
            orders: { ...topicHealth, extraTopic: "drop-me" },
            badjson: { ...topicHealth, rowCount: 0, liveRowCount: 0, extraTopic: "drop-me" },
          },
        },
        transport: { ...wireHealth.transport, extraTransport: "drop-me" },
      };
      const normalizedHealth = yield* viewServerDecodeHealth(viewServer, healthWithExtras);
      expect(Object.hasOwn(normalizedHealth, "extraRoot")).toBe(false);
      expect(Object.hasOwn(normalizedHealth.transport, "extraTransport")).toBe(false);
      expect(Object.hasOwn(normalizedHealth.engine.topics["orders"], "extraTopic")).toBe(false);

      const malformedHealthStatus = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          // @ts-expect-error hostile runtime adapters can return malformed health status.
          status: "broken",
        }),
      );
      expect(malformedHealthStatus.message).toMatch(/Invalid health payload/);

      const malformedHealthTransport = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          transport: {
            ...wireHealth.transport,
            // @ts-expect-error hostile runtime adapters can return malformed health counters.
            activeClients: "1",
          },
        }),
      );
      expect(malformedHealthTransport.message).toMatch(/Invalid health payload/);

      const validKafkaViewServerTopic = yield* viewServerDecodeHealth(viewServer, {
        ...wireHealth,
        kafka: {
          startFrom: kafkaStartFromHealth,
          regions: {},
          topics: {
            ordersSource: {
              status: "ready",
              sourceTopic: "orders-source",
              viewServerTopic: "orders",
              regions: {},
            },
          },
        },
      });
      expect(validKafkaViewServerTopic.kafka?.topics["ordersSource"]?.viewServerTopic).toBe(
        "orders",
      );

      const unknownKafkaViewServerTopic = yield* Effect.flip(
        viewServerDecodeHealth(viewServer, {
          ...wireHealth,
          kafka: {
            startFrom: kafkaStartFromHealth,
            regions: {},
            topics: {
              ordersSource: {
                status: "ready",
                sourceTopic: "orders-source",
                viewServerTopic: "missing",
                regions: {},
              },
            },
          },
        }),
      );
      expect(unknownKafkaViewServerTopic.message).toBe(
        "Health payload references unknown topic: missing",
      );

      const wrongSummaryEncodeTopic = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent(viewServer, {
          type: "status",
          // @ts-expect-error hostile callers can pass the wrong system topic.
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-summary",
          status: "ready",
          code: "Ready",
        }),
      );
      expect(wrongSummaryEncodeTopic.message).toBe(
        "Received event for __view_server_health while subscribed to __view_server_health_summary",
      );

      const malformedSummaryStatus = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent(viewServer, {
          type: "status",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          status: "ready",
          // @ts-expect-error ready summary status can only use the Ready code.
          code: "InvalidRow",
        }),
      );
      expect(malformedSummaryStatus.message).toMatch(/Invalid event/);

      const malformedDecodedSummaryStatus = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "status",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          status: "ready",
          // @ts-expect-error hostile wire status can use an invalid ready code.
          code: "InvalidRow",
        }),
      );
      expect(malformedDecodedSummaryStatus.message).toMatch(/Invalid system event/);

      const wrongTopicDecodeTopic = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
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

      const validTopicStatus = yield* viewServerDecodeHealthTopicEvent(viewServer, {
        type: "status",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        status: "ready",
        code: "Ready",
      });
      expect(validTopicStatus).toStrictEqual({
        type: "status",
        topic: VIEW_SERVER_HEALTH_TOPIC,
        queryId: "health-detail",
        status: "ready",
        code: "Ready",
      });

      const malformedTopicStatus = yield* Effect.flip(
        viewServerEncodeHealthTopicEvent(viewServer, {
          type: "status",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          status: "ready",
          // @ts-expect-error ready detail status can only use the Ready code.
          code: "InvalidRow",
        }),
      );
      expect(malformedTopicStatus.message).toMatch(/Invalid event/);

      const malformedDecodedTopicStatus = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "status",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          status: "ready",
          // @ts-expect-error hostile wire status can use an invalid ready code.
          code: "InvalidRow",
        }),
      );
      expect(malformedDecodedTopicStatus.message).toMatch(/Invalid system event/);

      const invalidHealthSummaryRow = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
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
              updatedAtNanos: 1,
              maxKafkaLag: "0",
            },
          ],
          totalRows: 1,
        }),
      );
      expect(invalidHealthSummaryRow.message).toMatch(/Invalid system row/);

      const missingSummaryTopics = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          version: 1,
          keys: ["summary"],
          rows: [{ id: "summary", updatedAtNanos: 1 }],
          totalRows: 1,
        }),
      );
      expect(missingSummaryTopics.message).toMatch(/Invalid system row/);

      const missingDeltaSummaryTopics = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          fromVersion: 1,
          toVersion: 2,
          operations: [{ type: "update", key: "summary", row: { id: "summary" }, index: 0 }],
          totalRows: 1,
        }),
      );
      expect(missingDeltaSummaryTopics.message).toMatch(/Invalid system row/);

      const unknownSummaryTopic = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
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
              unhealthyTopics: ["missing"],
              updatedAtNanos: "1",
              maxKafkaLag: "0",
            },
          ],
          totalRows: 1,
        }),
      );
      expect(unknownSummaryTopic.message).toBe("Health payload references unknown topic: missing");

      const wrongSummaryKey = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          version: 1,
          keys: ["not-summary"],
          rows: [
            {
              id: "summary",
              status: "ready",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              unhealthyTopics: [],
              updatedAtNanos: "1",
              maxKafkaLag: "0",
            },
          ],
          totalRows: 1,
        }),
      );
      expect(wrongSummaryKey.message).toBe("Health summary keys must be exactly: summary");

      const wrongSummaryRowCount = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          version: 1,
          keys: ["summary"],
          rows: [],
          totalRows: 0,
        }),
      );
      expect(wrongSummaryRowCount.message).toBe("Health summary must contain exactly one row");

      const wrongSummaryTotalRows = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
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
              unhealthyTopics: [],
              updatedAtNanos: "1",
              maxKafkaLag: "0",
            },
          ],
          totalRows: 2,
        }),
      );
      expect(wrongSummaryTotalRows.message).toBe("Health summary must contain exactly one row");

      const inconsistentSummaryStatus = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          version: 1,
          keys: ["summary"],
          rows: [
            {
              id: "summary",
              status: "ready",
              runtimeStatus: "degraded",
              connectionStatus: "connected",
              unhealthyTopics: ["orders"],
              updatedAtNanos: "1",
              maxKafkaLag: "0",
            },
          ],
          totalRows: 1,
        }),
      );
      expect(inconsistentSummaryStatus.message).toBe(
        "Health summary status does not match runtime/connection status: ready != degraded",
      );

      const connectedSummaryStatus = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          version: 1,
          keys: ["summary"],
          rows: [
            {
              id: "summary",
              status: "connected",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              unhealthyTopics: [],
              updatedAtNanos: "1",
              maxKafkaLag: "0",
            },
          ],
          totalRows: 1,
        }),
      );
      expect(connectedSummaryStatus.message).toMatch(/Invalid system row/);

      const wrongSummaryDeltaKey = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "remove",
              key: "not-summary",
            },
          ],
          totalRows: 1,
        }),
      );
      expect(wrongSummaryDeltaKey.message).toBe("Health summary delta key must be: summary");

      const mismatchedSummaryDeltaRow = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "update",
              key: "summary",
              row: {
                id: "not-summary",
                status: "ready",
                runtimeStatus: "ready",
                connectionStatus: "connected",
                unhealthyTopics: [],
                updatedAtNanos: "1",
                maxKafkaLag: "0",
              },
              index: 0,
            },
          ],
          totalRows: 1,
        }),
      );
      expect(mismatchedSummaryDeltaRow.message).toBe(
        "Health summary delta key does not match row id: summary != not-summary",
      );

      const unknownDetailTopic = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          version: 1,
          keys: ["orders", "badjson", "missing"],
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
              kafkaLag: "0",
              updatedAtNanos: "1",
            },
            {
              id: "badjson",
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
            },
            {
              id: "missing",
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
              kafkaLag: 0,
              updatedAtNanos: 1,
            },
          ],
          totalRows: 2,
        }),
      );
      expect(unknownDetailTopic.message).toBe("Health payload references unknown topic: missing");

      const partialDetailTopicSnapshot = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
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
              kafkaLag: "0",
              updatedAtNanos: "1",
            },
          ],
          totalRows: 2,
        }),
      );
      expect(partialDetailTopicSnapshot.message).toBe(
        "Health topic snapshot keys is missing topic: badjson",
      );

      const wrongDetailTopicSnapshotTotalRows = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          version: 1,
          keys: ["orders", "badjson"],
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
              kafkaLag: "0",
              updatedAtNanos: "1",
            },
            {
              id: "badjson",
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
            },
          ],
          totalRows: 999,
        }),
      );
      expect(wrongDetailTopicSnapshotTotalRows.message).toBe(
        "Health topic snapshot totalRows must equal configured topic count: 999 != 2",
      );

      const duplicateDetailTopicKeys = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          version: 1,
          keys: ["orders", "orders"],
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
              kafkaLag: "0",
              updatedAtNanos: "1",
            },
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
              kafkaLag: "0",
              updatedAtNanos: "1",
            },
          ],
          totalRows: 2,
        }),
      );
      expect(duplicateDetailTopicKeys.message).toBe(
        "Health topic snapshot keys contains duplicate topic: orders",
      );

      const mismatchedDetailTopicKey = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          version: 1,
          keys: ["orders", "badjson"],
          rows: [
            {
              id: "badjson",
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
            },
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
              kafkaLag: "0",
              updatedAtNanos: "1",
            },
          ],
          totalRows: 2,
        }),
      );
      expect(mismatchedDetailTopicKey.message).toBe(
        "Health topic snapshot key does not match row id: orders != badjson",
      );

      const nonStringDetailTopic = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          version: 1,
          keys: ["orders", "badjson"],
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
              kafkaLag: "0",
              updatedAtNanos: "1",
            },
            {
              id: "badjson",
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
            },
            {
              id: 1,
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
            },
          ],
          totalRows: 2,
        }),
      );
      expect(nonStringDetailTopic.message).toMatch(/Invalid system row/);

      const nonStringDeltaDetailTopic = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "update",
              key: "orders",
              row: {
                id: 1,
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
                kafkaLag: 0,
                updatedAtNanos: 1,
              },
              index: 0,
            },
          ],
          totalRows: 2,
        }),
      );
      expect(nonStringDeltaDetailTopic.message).toMatch(/Invalid system row/);

      const mismatchedDeltaDetailTopic = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "update",
              key: "orders",
              row: {
                id: "badjson",
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
              },
              index: 0,
            },
          ],
          totalRows: 2,
        }),
      );
      expect(mismatchedDeltaDetailTopic.message).toBe(
        "Health topic delta key does not match row id: orders != badjson",
      );

      const wrongDetailTopicDeltaTotalRows = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          fromVersion: 1,
          toVersion: 2,
          operations: [{ type: "remove", key: "orders" }],
          totalRows: 1,
        }),
      );
      expect(wrongDetailTopicDeltaTotalRows.message).toBe(
        "Health topic delta totalRows must equal configured topic count: 1 != 2",
      );

      const malformedHealthQuery = yield* Effect.flip(
        viewServerDecodeHealthQuery(VIEW_SERVER_HEALTH_TOPIC, { select: ["rowCount"] }),
      );
      expect(malformedHealthQuery.message).toBe("Health query select must be exactly: id");

      const extraHealthQueryKey = yield* Effect.flip(
        viewServerDecodeHealthQuery(VIEW_SERVER_HEALTH_TOPIC, {
          select: ["id"],
          limit: 1,
        }),
      );
      expect(extraHealthQueryKey.code).toBe("InvalidQuery");

      const invalidHealthSummaryEncodeRow = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent(viewServer, {
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
              // @ts-expect-error hostile callers can pass invalid system row values.
              updatedAtNanos: "1",
              // @ts-expect-error hostile callers can pass invalid system row values.
              maxKafkaLag: 1,
            },
          ],
          totalRows: 1,
        }),
      );
      expect(invalidHealthSummaryEncodeRow.message).toMatch(/Invalid system row/);

      const missingHealthSummaryEncodeTopics = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          version: 1,
          keys: ["summary"],
          rows: [
            // @ts-expect-error hostile callers can omit required system row values.
            {
              id: "summary",
              status: "ready",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              updatedAtNanos: 1n,
              maxKafkaLag: 0n,
            },
          ],
          totalRows: 1,
        }),
      );
      expect(missingHealthSummaryEncodeTopics.message).toMatch(/Invalid system row/);

      const invalidHealthSummaryEncodeKey = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          version: 1,
          // @ts-expect-error hostile callers can pass invalid summary snapshot keys.
          keys: ["not-summary"],
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
        }),
      );
      expect(invalidHealthSummaryEncodeKey.message).toBe(
        "Health summary keys must be exactly: summary",
      );

      const unknownHealthSummaryEncodeTopic = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent(viewServer, {
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
              // @ts-expect-error hostile callers can pass unknown unhealthy topics.
              unhealthyTopics: ["missing"],
              updatedAtNanos: 1n,
              maxKafkaLag: 0n,
            },
          ],
          totalRows: 1,
        }),
      );
      expect(unknownHealthSummaryEncodeTopic.message).toBe(
        "Health payload references unknown topic: missing",
      );

      const invalidHealthSummaryEncodeRowId = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          version: 1,
          keys: ["summary"],
          rows: [
            {
              // @ts-expect-error hostile callers can pass invalid system row ids.
              id: "not-summary",
              status: "ready",
              runtimeStatus: "ready",
              connectionStatus: "connected",
              unhealthyTopics: [],
              updatedAtNanos: 1n,
              maxKafkaLag: 0n,
            },
          ],
          totalRows: 1,
        }),
      );
      expect(invalidHealthSummaryEncodeRowId.message).toMatch(/Invalid system row/);

      const invalidHealthSummaryEncodeVersion = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          // @ts-expect-error hostile callers can pass malformed snapshot metadata.
          version: "not-a-number",
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
        }),
      );
      expect(invalidHealthSummaryEncodeVersion.message).toMatch(/Invalid event/);

      const invalidHealthSummaryEncodeDeltaKey = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "move",
              // @ts-expect-error hostile callers can pass invalid summary delta keys.
              key: "not-summary",
              fromIndex: 0,
              toIndex: 0,
            },
          ],
          totalRows: 1,
        }),
      );
      expect(invalidHealthSummaryEncodeDeltaKey.message).toBe(
        "Health summary delta key must be: summary",
      );

      const mismatchedHealthSummaryEncodeDeltaRow = yield* Effect.flip(
        viewServerEncodeHealthSummaryEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "update",
              key: "summary",
              row: {
                // @ts-expect-error hostile callers can pass invalid system row ids.
                id: "not-summary",
                status: "ready",
                runtimeStatus: "ready",
                connectionStatus: "connected",
                unhealthyTopics: [],
                updatedAtNanos: 1n,
                maxKafkaLag: 0n,
              },
              index: 0,
            },
          ],
          totalRows: 1,
        }),
      );
      expect(mismatchedHealthSummaryEncodeDeltaRow.message).toMatch(/Invalid system row/);

      const invalidHealthSummaryDecodeRemove = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          fromVersion: 1,
          toVersion: 2,
          operations: [{ type: "remove", key: "summary" }],
          totalRows: 1,
        }),
      );
      expect(invalidHealthSummaryDecodeRemove.message).toBe(
        "Health summary delta cannot remove summary",
      );

      const invalidHealthSummaryDecodeInsert = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "insert",
              key: "summary",
              row: {
                id: "summary",
                status: "ready",
                runtimeStatus: "ready",
                connectionStatus: "connected",
                unhealthyTopics: [],
                updatedAtNanos: "1",
                maxKafkaLag: "0",
              },
              index: 0,
            },
          ],
          totalRows: 1,
        }),
      );
      expect(invalidHealthSummaryDecodeInsert.message).toBe(
        "Health summary delta cannot insert summary",
      );

      const invalidHealthSummaryDecodeIndex = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          fromVersion: 1,
          toVersion: 2,
          operations: [{ type: "move", key: "summary", fromIndex: 0, toIndex: -1 }],
          totalRows: 1,
        }),
      );
      expect(invalidHealthSummaryDecodeIndex.message).toBe(
        "Health summary move to index must be 0: -1",
      );

      const invalidHealthSummaryDecodeDeltaTopic = yield* Effect.flip(
        viewServerDecodeHealthSummaryEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          queryId: "health-summary",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "update",
              key: "summary",
              row: {
                id: "summary",
                status: "degraded",
                runtimeStatus: "degraded",
                connectionStatus: "connected",
                unhealthyTopics: ["missing"],
                updatedAtNanos: "1",
                maxKafkaLag: "0",
              },
              index: 0,
            },
          ],
          totalRows: 1,
        }),
      );
      expect(invalidHealthSummaryDecodeDeltaTopic.message).toBe(
        "Health payload references unknown topic: missing",
      );

      const encodedHealthTopicRow: ViewServerHealthTopicRow<"orders"> = {
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
      };

      const partialHealthTopicEncodeSnapshot = yield* Effect.flip(
        viewServerEncodeHealthTopicEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          version: 1,
          keys: ["orders"],
          rows: [encodedHealthTopicRow],
          totalRows: 2,
        }),
      );
      expect(partialHealthTopicEncodeSnapshot.message).toBe(
        "Health topic snapshot keys is missing topic: badjson",
      );

      const invalidHealthTopicEncodeVersion = yield* Effect.flip(
        viewServerEncodeHealthTopicEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          // @ts-expect-error hostile callers can pass malformed snapshot metadata.
          version: "not-a-number",
          keys: ["orders", "badjson"],
          rows: [encodedHealthTopicRow, { ...encodedHealthTopicRow, id: "badjson" }],
          totalRows: 2,
        }),
      );
      expect(invalidHealthTopicEncodeVersion.message).toMatch(/Invalid event/);

      const mismatchedHealthTopicEncodeSnapshot = yield* Effect.flip(
        viewServerEncodeHealthTopicEvent(viewServer, {
          type: "snapshot",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          version: 1,
          keys: ["badjson", "orders"],
          rows: [encodedHealthTopicRow, { ...encodedHealthTopicRow, id: "badjson" }],
          totalRows: 2,
        }),
      );
      expect(mismatchedHealthTopicEncodeSnapshot.message).toBe(
        "Health topic snapshot key does not match row id: badjson != orders",
      );

      const mismatchedHealthTopicEncodeDelta = yield* Effect.flip(
        viewServerEncodeHealthTopicEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "update",
              key: "orders",
              row: {
                ...encodedHealthTopicRow,
                // @ts-expect-error hostile callers can pass a valid but mismatched topic row id.
                id: "badjson",
              },
              index: 0,
            },
          ],
          totalRows: 2,
        }),
      );
      expect(mismatchedHealthTopicEncodeDelta.message).toBe(
        "Health topic delta key does not match row id: orders != badjson",
      );

      const invalidHealthTopicDecodeRemove = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          fromVersion: 1,
          toVersion: 2,
          operations: [{ type: "remove", key: "orders" }],
          totalRows: 2,
        }),
      );
      expect(invalidHealthTopicDecodeRemove.message).toBe(
        "Health topic delta cannot remove configured topic: orders",
      );

      const invalidHealthTopicDecodeInsert = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          fromVersion: 1,
          toVersion: 2,
          operations: [
            {
              type: "insert",
              key: "orders",
              row: {
                ...encodedHealthTopicRow,
                kafkaLag: "0",
                updatedAtNanos: "1",
              },
              index: 0,
            },
          ],
          totalRows: 2,
        }),
      );
      expect(invalidHealthTopicDecodeInsert.message).toBe(
        "Health topic delta cannot insert configured topic: orders",
      );

      const invalidHealthTopicDecodeIndex = yield* Effect.flip(
        viewServerDecodeHealthTopicEvent(viewServer, {
          type: "delta",
          topic: VIEW_SERVER_HEALTH_TOPIC,
          queryId: "health-detail",
          fromVersion: 1,
          toVersion: 2,
          operations: [{ type: "move", key: "orders", fromIndex: 99, toIndex: 0 }],
          totalRows: 2,
        }),
      );
      expect(invalidHealthTopicDecodeIndex.message).toBe(
        "Health topic move from index must be within configured topic count: 99",
      );
    }),
  );
});
