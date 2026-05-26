import type {
  DeltaEvent,
  ExactRawQuery,
  FieldKey,
  OrderBy,
  RowSchema,
  SnapshotEvent,
  StatusEvent,
  TopicDefinitions,
  TopicRow,
  TopicRuntimeHealth,
  TransportHealth,
  ViewServerBackpressureError,
  ViewServerHealth,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
  ViewServerRuntimeError,
  ViewServerTransportError,
  Where,
} from "@view-server/config";
import { VIEW_SERVER_HEALTH_SUMMARY_TOPIC, VIEW_SERVER_HEALTH_TOPIC } from "@view-server/config";
import { Effect, Schema } from "effect";
import { Rpc, RpcGroup } from "effect/unstable/rpc";

type ViewServerProtocolEvent<Row> = SnapshotEvent<Row> | DeltaEvent<Row> | StatusEvent;

const StringOrNull = Schema.NullOr(Schema.String);
const NumberOrNull = Schema.NullOr(Schema.Number);
const BigIntString = Schema.BigIntFromString;

export const ViewServerBackpressureErrorSchema: Schema.Codec<ViewServerBackpressureError> =
  Schema.TaggedStruct("ViewServerBackpressureError", {
    code: Schema.Literal("BackpressureExceeded"),
    message: Schema.String,
    topic: Schema.optionalKey(Schema.String),
    queryId: Schema.optionalKey(Schema.String),
    queuedEvents: Schema.optionalKey(Schema.Number),
    maxQueueDepth: Schema.optionalKey(Schema.Number),
  });

export const ViewServerRuntimeErrorSchema: Schema.Codec<ViewServerRuntimeError> = Schema.Union([
  ViewServerBackpressureErrorSchema,
  Schema.TaggedStruct("ViewServerRuntimeError", {
    code: Schema.Literals([
      "InvalidTopic",
      "InvalidRow",
      "InvalidQuery",
      "UnsupportedQuery",
      "SnapshotStale",
      "RuntimeUnavailable",
      "RuntimeResetFailed",
    ]),
    message: Schema.String,
    topic: Schema.optionalKey(Schema.String),
  }),
]);

export const ViewServerTransportErrorSchema: Schema.Codec<ViewServerTransportError> = Schema.Union([
  ViewServerBackpressureErrorSchema,
  Schema.TaggedStruct("ViewServerTransportError", {
    code: Schema.Literals(["TransportError", "SubscriptionClosed"]),
    message: Schema.String,
    topic: Schema.optionalKey(Schema.String),
    queryId: Schema.optionalKey(Schema.String),
  }),
]);

export const ViewServerRpcErrorSchema = Schema.Union([
  ViewServerRuntimeErrorSchema,
  ViewServerTransportErrorSchema,
]);

const TopicRuntimeHealthSchema: Schema.Codec<TopicRuntimeHealth> = Schema.Struct({
  status: Schema.Literals(["ready", "degraded", "starting"]),
  rowCount: Schema.Number,
  liveRowCount: Schema.Number,
  deletedRowCount: Schema.Number,
  version: Schema.Number,
  lastMutationAt: NumberOrNull,
  mutationsPerSecond: Schema.Number,
  rowsPerSecond: Schema.Number,
  pendingMutationBatches: Schema.Number,
  activeViews: Schema.Number,
  activeSubscriptions: Schema.Number,
  queuedEvents: Schema.Number,
  maxQueueDepth: Schema.Number,
  backpressureEvents: Schema.Number,
  memoryBytes: Schema.Number,
  tombstoneCount: Schema.Number,
  compactionPending: Schema.Boolean,
});

const TransportHealthSchema: Schema.Codec<TransportHealth> = Schema.Struct({
  activeClients: Schema.Number,
  activeStreams: Schema.Number,
  activeSubscriptions: Schema.Number,
  messagesPerSecond: Schema.Number,
  bytesPerSecond: Schema.Number,
  queuedMessages: Schema.Number,
  queuedBytes: Schema.Number,
  droppedClients: Schema.Number,
  backpressureEvents: Schema.Number,
  reconnects: Schema.Number,
  lastError: StringOrNull,
});

export const ViewServerHealthSchema: Schema.Codec<ViewServerHealth<Record<string, object>>> =
  Schema.Struct({
    status: Schema.Literals(["ready", "degraded", "starting", "stopping"]),
    version: Schema.Number,
    uptimeMs: Schema.Number,
    engine: Schema.Struct({
      topics: Schema.Record(Schema.String, TopicRuntimeHealthSchema),
    }),
    kafka: Schema.optionalKey(
      Schema.Struct({
        regions: Schema.Record(
          Schema.String,
          Schema.Struct({
            status: Schema.Literals(["connected", "disconnected", "degraded", "starting"]),
            brokers: Schema.String,
            lastConnectedAt: NumberOrNull,
            lastError: StringOrNull,
          }),
        ),
        topics: Schema.Record(
          Schema.String,
          Schema.Struct({
            status: Schema.Literals(["ready", "degraded", "starting", "stalled"]),
            sourceTopic: Schema.String,
            viewServerTopic: Schema.String,
            regions: Schema.Record(
              Schema.String,
              Schema.Struct({
                connected: Schema.Boolean,
                assignedPartitions: Schema.Number,
                messagesPerSecond: Schema.Number,
                bytesPerSecond: Schema.Number,
                decodedMessagesPerSecond: Schema.Number,
                decodeFailuresPerSecond: Schema.Number,
                lastMessageAt: NumberOrNull,
                lastCommitAt: NumberOrNull,
                consumerLagMessages: NumberOrNull,
                consumerLagMs: NumberOrNull,
                lagSampledAt: NumberOrNull,
                highWatermarkOffset: StringOrNull,
                committedOffset: StringOrNull,
                lastError: StringOrNull,
              }),
            ),
          }),
        ),
      }),
    ),
    transport: TransportHealthSchema,
  });

export type ViewServerWireHealth = typeof ViewServerHealthSchema.Type;

export const ViewServerWireRowSchema: Schema.Codec<Schema.JsonObject> = Schema.Record(
  Schema.String,
  Schema.Json,
);
export type ViewServerWireRow = typeof ViewServerWireRowSchema.Type;

const SnapshotEventSchema = Schema.Struct({
  type: Schema.Literal("snapshot"),
  topic: Schema.String,
  queryId: Schema.String,
  version: Schema.Number,
  keys: Schema.Array(Schema.String),
  rows: Schema.Array(ViewServerWireRowSchema),
  totalRows: Schema.Number,
});

const DeltaOperationSchema = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("insert"),
    key: Schema.String,
    row: ViewServerWireRowSchema,
    index: Schema.Number,
  }),
  Schema.Struct({
    type: Schema.Literal("update"),
    key: Schema.String,
    row: ViewServerWireRowSchema,
    index: Schema.Number,
  }),
  Schema.Struct({
    type: Schema.Literal("move"),
    key: Schema.String,
    fromIndex: Schema.Number,
    toIndex: Schema.Number,
  }),
  Schema.Struct({
    type: Schema.Literal("remove"),
    key: Schema.String,
  }),
]);

const DeltaEventSchema = Schema.Struct({
  type: Schema.Literal("delta"),
  topic: Schema.String,
  queryId: Schema.String,
  fromVersion: Schema.Number,
  toVersion: Schema.Number,
  operations: Schema.Array(DeltaOperationSchema),
  totalRows: Schema.Number,
});

const StatusEventSchema: Schema.Codec<StatusEvent> = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("status"),
    topic: Schema.String,
    queryId: Schema.String,
    status: Schema.Literal("ready"),
    code: Schema.Literal("Ready"),
    message: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("status"),
    topic: Schema.String,
    queryId: Schema.String,
    status: Schema.Literal("stale"),
    code: Schema.Literals(["SnapshotStale", "BackpressureExceeded"]),
    message: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("status"),
    topic: Schema.String,
    queryId: Schema.String,
    status: Schema.Literal("closed"),
    code: Schema.Literals(["SubscriptionClosed", "BackpressureExceeded"]),
    message: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("status"),
    topic: Schema.String,
    queryId: Schema.String,
    status: Schema.Literal("error"),
    code: Schema.Literals([
      "TransportError",
      "BackpressureExceeded",
      "InvalidTopic",
      "InvalidRow",
      "InvalidQuery",
      "UnsupportedQuery",
      "RuntimeUnavailable",
      "RuntimeResetFailed",
    ]),
    message: Schema.optionalKey(Schema.String),
  }),
]);

export const ViewServerWireEventSchema = Schema.Union([
  SnapshotEventSchema,
  DeltaEventSchema,
  StatusEventSchema,
]);

export type ViewServerWireEvent = typeof ViewServerWireEventSchema.Type;

export const ViewServerHealthSummaryRowSchema: Schema.Codec<
  ViewServerHealthSummaryRow,
  unknown,
  never,
  never
> = Schema.Struct({
  id: Schema.Literal("summary"),
  status: Schema.Literals([
    "ready",
    "degraded",
    "starting",
    "stopping",
    "connecting",
    "connected",
    "disconnected",
  ]),
  runtimeStatus: Schema.Literals(["ready", "degraded", "starting", "stopping"]),
  connectionStatus: Schema.Literals(["connecting", "connected", "disconnected"]),
  unhealthyTopics: Schema.Array(Schema.String),
  updatedAtNanos: BigIntString,
  maxKafkaLag: BigIntString,
});

export const ViewServerHealthTopicRowSchema: Schema.Codec<
  ViewServerHealthTopicRow,
  unknown,
  never,
  never
> = Schema.Struct({
  id: Schema.String,
  status: Schema.Literals(["ready", "degraded", "starting", "stopping"]),
  rowCount: Schema.Number,
  liveRowCount: Schema.Number,
  deletedRowCount: Schema.Number,
  version: Schema.Number,
  mutationsPerSecond: Schema.Number,
  rowsPerSecond: Schema.Number,
  pendingMutationBatches: Schema.Number,
  activeViews: Schema.Number,
  activeSubscriptions: Schema.Number,
  queuedEvents: Schema.Number,
  maxQueueDepth: Schema.Number,
  backpressureEvents: Schema.Number,
  memoryBytes: Schema.Number,
  tombstoneCount: Schema.Number,
  compactionPending: Schema.Boolean,
  kafkaLag: BigIntString,
  updatedAtNanos: BigIntString,
});

export const ViewServerWireRawQuerySchema = Schema.Struct({
  select: Schema.Array(Schema.String),
  where: Schema.optionalKey(Schema.Record(Schema.String, Schema.Json)),
  orderBy: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        field: Schema.String,
        direction: Schema.Literals(["asc", "desc"]),
      }),
    ),
  ),
  offset: Schema.optionalKey(Schema.Number),
  limit: Schema.optionalKey(Schema.Number),
});

export type ViewServerWireRawQuery = typeof ViewServerWireRawQuerySchema.Type;

export const ViewServerSubscribePayloadSchema = Schema.Struct({
  topic: Schema.String,
  // Keep this loose so excess query keys survive RPC decoding and can be rejected by strict query validation.
  query: Schema.Record(Schema.String, Schema.Unknown),
});

export const ViewServerRpcs = RpcGroup.make(
  Rpc.make("ViewServer.Health", {
    success: ViewServerHealthSchema,
    error: ViewServerRpcErrorSchema,
  }),
  Rpc.make("ViewServer.Subscribe", {
    payload: ViewServerSubscribePayloadSchema,
    success: ViewServerWireEventSchema,
    error: ViewServerRpcErrorSchema,
    stream: true,
  }),
);

export type ViewServerRpcError =
  | typeof ViewServerRuntimeErrorSchema.Type
  | typeof ViewServerTransportErrorSchema.Type;

type JsonFieldSchema = Schema.Codec<unknown, unknown, never, never>;

const LooseWireRawQuerySchema = Schema.Struct({
  select: Schema.Array(Schema.String),
  where: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
  orderBy: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        field: Schema.String,
        direction: Schema.Literals(["asc", "desc"]),
      }),
    ),
  ),
  offset: Schema.optionalKey(Schema.Number),
  limit: Schema.optionalKey(Schema.Number),
});

type LooseWireRawQuery = typeof LooseWireRawQuerySchema.Type;

type TrustedRawQuery<Row> = {
  readonly select: ReadonlyArray<FieldKey<Row>>;
  readonly where?: Where<Row>;
  readonly orderBy?: ReadonlyArray<OrderBy<Row>>;
  readonly offset?: number;
  readonly limit?: number;
};

export type ViewServerValidatedRawQuery<Row> = TrustedRawQuery<Row> &
  ExactRawQuery<Row, TrustedRawQuery<Row>>;

const filterOperatorKeys = new Set(["eq", "neq", "in", "gt", "gte", "lt", "lte", "startsWith"]);

const strictParseOptions = {
  onExcessProperty: "error",
} as const;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isFilterObject = (value: unknown): value is Record<string, unknown> =>
  isRecord(value) &&
  Object.keys(value).length > 0 &&
  Object.keys(value).every((key) => filterOperatorKeys.has(key));

const invalidTopic = (topic: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidTopic",
  message: `Unknown topic: ${topic}`,
  topic,
});

const invalidQuery = (topic: string, message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidQuery",
  message,
  topic,
});

const invalidRow = (topic: string, message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidRow",
  message,
  topic,
});

const hasTopic = <Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  topic: string,
): topic is Extract<keyof Topics, string> => Object.hasOwn(config.topics, topic);

const getFieldSchema = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  topic: Extract<keyof Topics, string>,
  field: string,
): JsonFieldSchema | undefined => {
  return config.topics[topic]!.schema.fields[field];
};

const hasOnlyKnownFields = (schema: RowSchema, fields: Iterable<string>): boolean =>
  Array.from(fields).every((field) => schema.fields[field] !== undefined);

const isRawQueryForTopic = (schema: RowSchema, query: LooseWireRawQuery): boolean => {
  if (!hasOnlyKnownFields(schema, query.select)) {
    return false;
  }
  if (query.where !== undefined && !hasOnlyKnownFields(schema, Object.keys(query.where))) {
    return false;
  }
  if (
    query.orderBy !== undefined &&
    !hasOnlyKnownFields(
      schema,
      query.orderBy.map((entry) => entry.field),
    )
  ) {
    return false;
  }
  return true;
};

export const viewServerDecodeTopic = Effect.fn("ViewServerProtocol.topic.decode")(function* <
  const Topics extends TopicDefinitions,
>(config: { readonly topics: Topics }, topic: string) {
  if (hasTopic(config, topic)) {
    return topic;
  }
  return yield* Effect.fail(invalidTopic(topic));
});

const encodeJsonFieldValue = Effect.fn("ViewServerProtocol.field.encode")(function* (
  topic: string,
  field: string,
  schema: JsonFieldSchema,
  value: unknown,
) {
  const encoded = yield* Schema.encodeUnknownEffect(Schema.toCodecJson(schema))(value).pipe(
    Effect.mapError((error) =>
      invalidQuery(topic, `Invalid filter for ${field}: ${error.message}`),
    ),
  );
  return yield* Schema.decodeUnknownEffect(Schema.Json)(encoded).pipe(
    Effect.mapError((error) =>
      invalidQuery(topic, `Filter ${field} is not JSON-safe: ${error.message}`),
    ),
  );
});

const decodeJsonFieldValue = Effect.fn("ViewServerProtocol.field.decode")(function* (
  topic: string,
  field: string,
  schema: JsonFieldSchema,
  value: unknown,
) {
  return yield* Schema.decodeUnknownEffect(Schema.toCodecJson(schema))(value).pipe(
    Effect.mapError((error) =>
      invalidQuery(topic, `Invalid filter for ${field}: ${error.message}`),
    ),
  );
});

const decodeRowFieldValue = Effect.fn("ViewServerProtocol.row.field.decode")(function* (
  topic: string,
  field: string,
  schema: JsonFieldSchema,
  value: unknown,
) {
  return yield* Schema.decodeUnknownEffect(Schema.toCodecJson(schema))(value).pipe(
    Effect.mapError((error) => invalidRow(topic, `Invalid field ${field}: ${error.message}`)),
  );
});

const encodeStringFilterValue = Effect.fn("ViewServerProtocol.filter.string.encode")(function* (
  topic: string,
  field: string,
  schema: JsonFieldSchema,
  value: unknown,
) {
  const decoded = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(schema))(value).pipe(
    Effect.mapError((error) =>
      invalidQuery(topic, `Invalid filter for ${field}: ${error.message}`),
    ),
  );
  if (typeof decoded !== "string") {
    return yield* Effect.fail(invalidQuery(topic, `Filter ${field} does not support startsWith`));
  }
  return yield* Schema.decodeUnknownEffect(Schema.String)(value).pipe(
    Effect.mapError((error) =>
      invalidQuery(topic, `Invalid startsWith filter for ${field}: ${error.message}`),
    ),
  );
});

const decodeStringFilterValue = Effect.fn("ViewServerProtocol.filter.string.decode")(function* (
  topic: string,
  field: string,
  schema: JsonFieldSchema,
  value: unknown,
) {
  const decoded = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(schema))(value).pipe(
    Effect.mapError((error) =>
      invalidQuery(topic, `Invalid filter for ${field}: ${error.message}`),
    ),
  );
  if (typeof decoded !== "string") {
    return yield* Effect.fail(invalidQuery(topic, `Filter ${field} does not support startsWith`));
  }
  return decoded;
});

const encodeOperatorFilterValue = Effect.fn("ViewServerProtocol.filter.operator.encode")(function* (
  topic: string,
  field: string,
  schema: JsonFieldSchema,
  value: Record<string, unknown>,
) {
  const output: Record<string, Schema.Json> = {};
  for (const [operator, operatorValue] of Object.entries(value)) {
    if (operator === "in" && Array.isArray(operatorValue)) {
      output[operator] = yield* Effect.forEach(operatorValue, (entry) =>
        encodeJsonFieldValue(topic, field, schema, entry),
      );
    } else if (operator === "startsWith") {
      output[operator] = yield* encodeStringFilterValue(topic, field, schema, operatorValue);
    } else {
      output[operator] = yield* encodeJsonFieldValue(topic, field, schema, operatorValue);
    }
  }
  return output;
});

const decodeOperatorFilterValue = Effect.fn("ViewServerProtocol.filter.operator.decode")(function* (
  topic: string,
  field: string,
  schema: JsonFieldSchema,
  value: Record<string, unknown>,
) {
  const output: Record<string, unknown> = {};
  for (const [operator, operatorValue] of Object.entries(value)) {
    if (operator === "in" && Array.isArray(operatorValue)) {
      output[operator] = yield* Effect.forEach(operatorValue, (entry) =>
        decodeJsonFieldValue(topic, field, schema, entry),
      );
    } else if (operator === "startsWith") {
      output[operator] = yield* decodeStringFilterValue(topic, field, schema, operatorValue);
    } else {
      output[operator] = yield* decodeJsonFieldValue(topic, field, schema, operatorValue);
    }
  }
  return output;
});

const encodeFilterValue = Effect.fn("ViewServerProtocol.filter.encode")(function* (
  topic: string,
  field: string,
  schema: JsonFieldSchema,
  value: unknown,
) {
  if (!isFilterObject(value)) {
    return yield* encodeJsonFieldValue(topic, field, schema, value);
  }
  return yield* Effect.matchEffect(encodeOperatorFilterValue(topic, field, schema, value), {
    onFailure: (operatorError) =>
      Effect.matchEffect(encodeJsonFieldValue(topic, field, schema, value), {
        onFailure: () => Effect.fail(operatorError),
        onSuccess: Effect.succeed,
      }),
    onSuccess: Effect.succeed,
  });
});

const decodeFilterValue = Effect.fn("ViewServerProtocol.filter.decode")(function* (
  topic: string,
  field: string,
  schema: JsonFieldSchema,
  value: unknown,
) {
  if (!isFilterObject(value)) {
    return yield* decodeJsonFieldValue(topic, field, schema, value);
  }
  return yield* Effect.matchEffect(decodeOperatorFilterValue(topic, field, schema, value), {
    onFailure: (operatorError) =>
      Effect.matchEffect(decodeJsonFieldValue(topic, field, schema, value), {
        onFailure: () => Effect.fail(operatorError),
        onSuccess: Effect.succeed,
      }),
    onSuccess: Effect.succeed,
  });
});

const encodeWhere = Effect.fn("ViewServerProtocol.query.where.encode")(function* <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(config: { readonly topics: Topics }, topic: Topic, where: Record<string, unknown> | undefined) {
  if (where === undefined) {
    return undefined;
  }
  const output: Record<string, Schema.Json> = {};
  for (const [field, value] of Object.entries(where)) {
    const fieldSchema = getFieldSchema(config, topic, field);
    if (fieldSchema === undefined) {
      return yield* Effect.fail(
        invalidQuery(topic, `Query references an unknown field for topic: ${topic}`),
      );
    }
    output[field] = yield* encodeFilterValue(topic, field, fieldSchema, value);
  }
  return output;
});

const decodeWhere = Effect.fn("ViewServerProtocol.query.where.decode")(function* (
  topic: string,
  schema: RowSchema,
  where: Record<string, unknown> | undefined,
) {
  if (where === undefined) {
    return undefined;
  }
  const output: Record<string, unknown> = {};
  for (const [field, value] of Object.entries(where)) {
    const fieldSchema = schema.fields[field]!;
    output[field] = yield* decodeFilterValue(topic, field, fieldSchema, value);
  }
  return output;
});

export const viewServerEncodeRawQuery = Effect.fn("ViewServerProtocol.query.encode")(function* <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(config: { readonly topics: Topics }, topic: Topic, query: unknown) {
  if (!hasTopic(config, topic)) {
    return yield* Effect.fail(invalidTopic(topic));
  }
  const decoded = yield* Schema.decodeUnknownEffect(LooseWireRawQuerySchema)(
    query,
    strictParseOptions,
  ).pipe(Effect.mapError((error) => invalidQuery(topic, error.message)));
  if (decoded.select.length === 0) {
    return yield* Effect.fail(invalidQuery(topic, "Query select must include at least one field"));
  }
  if (decoded.offset !== undefined && (!Number.isInteger(decoded.offset) || decoded.offset < 0)) {
    return yield* Effect.fail(invalidQuery(topic, "Query offset must be a non-negative integer"));
  }
  if (decoded.limit !== undefined && (!Number.isInteger(decoded.limit) || decoded.limit <= 0)) {
    return yield* Effect.fail(invalidQuery(topic, "Query limit must be a positive integer"));
  }
  for (const field of decoded.select) {
    if (getFieldSchema(config, topic, field) === undefined) {
      return yield* Effect.fail(
        invalidQuery(topic, `Query references an unknown field for topic: ${topic}`),
      );
    }
  }
  if (decoded.orderBy !== undefined) {
    for (const entry of decoded.orderBy) {
      if (getFieldSchema(config, topic, entry.field) === undefined) {
        return yield* Effect.fail(
          invalidQuery(topic, `Query references an unknown field for topic: ${topic}`),
        );
      }
    }
  }
  const where = yield* encodeWhere(config, topic, decoded.where);
  const wireQuery: ViewServerWireRawQuery = {
    select: decoded.select,
    ...(where === undefined ? {} : { where }),
    ...(decoded.orderBy === undefined ? {} : { orderBy: decoded.orderBy }),
    ...(decoded.offset === undefined ? {} : { offset: decoded.offset }),
    ...(decoded.limit === undefined ? {} : { limit: decoded.limit }),
  };
  return wireQuery;
});

function validatedRawQuery<Row>(query: LooseWireRawQuery): ViewServerValidatedRawQuery<Row>;
function validatedRawQuery(query: LooseWireRawQuery): LooseWireRawQuery {
  return query;
}

export const viewServerDecodeRawQuery = Effect.fn("ViewServerProtocol.query.decode")(function* <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(config: { readonly topics: Topics }, topic: Topic, query: unknown) {
  const decoded = yield* Schema.decodeUnknownEffect(LooseWireRawQuerySchema)(
    query,
    strictParseOptions,
  ).pipe(Effect.mapError((error) => invalidQuery(topic, error.message)));
  if (decoded.select.length === 0) {
    return yield* Effect.fail(invalidQuery(topic, "Query select must include at least one field"));
  }
  if (decoded.offset !== undefined && (!Number.isInteger(decoded.offset) || decoded.offset < 0)) {
    return yield* Effect.fail(invalidQuery(topic, "Query offset must be a non-negative integer"));
  }
  if (decoded.limit !== undefined && (!Number.isInteger(decoded.limit) || decoded.limit <= 0)) {
    return yield* Effect.fail(invalidQuery(topic, "Query limit must be a positive integer"));
  }
  const topicSchema = config.topics[topic]!.schema;
  if (isRawQueryForTopic(topicSchema, decoded)) {
    const where = yield* decodeWhere(topic, topicSchema, decoded.where);
    return validatedRawQuery<TopicRow<Topics, Topic>>({
      select: decoded.select,
      ...(where === undefined ? {} : { where }),
      ...(decoded.orderBy === undefined ? {} : { orderBy: decoded.orderBy }),
      ...(decoded.offset === undefined ? {} : { offset: decoded.offset }),
      ...(decoded.limit === undefined ? {} : { limit: decoded.limit }),
    });
  }
  return yield* Effect.fail(
    invalidQuery(topic, `Query references an unknown field for topic: ${topic}`),
  );
});

const encodeEventJsonFieldValue = Effect.fn("ViewServerProtocol.event.field.encode")(function* (
  topic: string,
  field: string,
  schema: JsonFieldSchema,
  value: unknown,
) {
  const encoded = yield* Schema.encodeUnknownEffect(Schema.toCodecJson(schema))(value).pipe(
    Effect.mapError((error) => invalidRow(topic, `Invalid field ${field}: ${error.message}`)),
  );
  return yield* Schema.decodeUnknownEffect(Schema.Json)(encoded).pipe(
    Effect.mapError((error) =>
      invalidRow(topic, `Field ${field} is not JSON-safe: ${error.message}`),
    ),
  );
});

const encodeProjectedRow = Effect.fn("ViewServerProtocol.row.project.encode")(function* <
  const Topics extends TopicDefinitions,
>(
  config: { readonly topics: Topics },
  topic: Extract<keyof Topics, string>,
  selectedFields: ReadonlySet<string>,
  row: object,
) {
  const topicSchema = config.topics[topic]!.schema;
  const output: Record<string, Schema.Json> = {};
  for (const field of selectedFields) {
    if (!Object.hasOwn(row, field)) {
      return yield* Effect.fail(
        invalidRow(topic, `Missing row field for topic ${topic}: ${field}`),
      );
    }
  }
  for (const [field, value] of Object.entries(row)) {
    if (!selectedFields.has(field)) {
      return yield* Effect.fail(
        invalidRow(topic, `Unexpected row field for topic ${topic}: ${field}`),
      );
    }
    const fieldSchema = topicSchema.fields[field]!;
    output[field] = yield* encodeEventJsonFieldValue(topic, field, fieldSchema, value);
  }
  return output;
});

const decodeProjectedRow = Effect.fn("ViewServerProtocol.row.project.decode")(function* <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  config: { readonly topics: Topics },
  topic: Topic,
  selectedFields: ReadonlySet<string>,
  row: ViewServerWireRow,
) {
  const output: Record<string, unknown> = {};
  for (const field of selectedFields) {
    if (!Object.hasOwn(row, field)) {
      return yield* Effect.fail(
        invalidRow(topic, `Missing row field for topic ${topic}: ${field}`),
      );
    }
  }
  for (const [field, value] of Object.entries(row)) {
    if (!selectedFields.has(field)) {
      return yield* Effect.fail(
        invalidRow(topic, `Unexpected row field for topic ${topic}: ${field}`),
      );
    }
    const fieldSchema = config.topics[topic]!.schema.fields[field]!;
    output[field] = yield* decodeRowFieldValue(topic, field, fieldSchema, value);
  }
  return output;
});

const encodeStatusEvent = (event: StatusEvent): ViewServerWireEvent => event;

export const viewServerEncodeLiveEvent = Effect.fn("ViewServerProtocol.event.encode")(function* <
  const Topics extends TopicDefinitions,
>(
  config: { readonly topics: Topics },
  expectedTopic: Extract<keyof Topics, string>,
  selectedFields: ReadonlySet<string>,
  event: ViewServerProtocolEvent<object>,
) {
  if (event.topic !== expectedTopic) {
    return yield* Effect.fail(
      invalidRow(
        expectedTopic,
        `Received event for ${event.topic} while subscribed to ${expectedTopic}`,
      ),
    );
  }
  if (event.type === "status") {
    return encodeStatusEvent(event);
  }
  if (event.type === "snapshot") {
    const rows = yield* Effect.forEach(event.rows, (row) =>
      encodeProjectedRow(config, expectedTopic, selectedFields, row),
    );
    return {
      ...event,
      rows,
    };
  }
  type WireDeltaOperation = Extract<
    ViewServerWireEvent,
    { readonly type: "delta" }
  >["operations"][number];
  const operations: Array<WireDeltaOperation> = [];
  for (const operation of event.operations) {
    if (operation.type === "insert" || operation.type === "update") {
      const row = yield* encodeProjectedRow(config, expectedTopic, selectedFields, operation.row);
      operations.push({
        ...operation,
        row,
      });
    } else {
      operations.push(operation);
    }
  }
  return {
    ...event,
    operations,
  };
});

function typedLiveEvent<Row>(
  event: ViewServerProtocolEvent<Record<string, unknown>>,
): ViewServerProtocolEvent<Row>;
function typedLiveEvent(
  event: ViewServerProtocolEvent<Record<string, unknown>>,
): ViewServerProtocolEvent<Record<string, unknown>> {
  return event;
}

export const viewServerDecodeLiveEvent = Effect.fn("ViewServerProtocol.event.decode")(function* <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Row,
>(
  config: { readonly topics: Topics },
  expectedTopic: Topic,
  selectedFields: ReadonlySet<string>,
  event: ViewServerWireEvent,
) {
  if (event.topic !== expectedTopic) {
    return yield* Effect.fail(
      invalidRow(
        expectedTopic,
        `Received event for ${event.topic} while subscribed to ${expectedTopic}`,
      ),
    );
  }
  if (event.type === "status") {
    return typedLiveEvent<Row>(event);
  }
  if (event.type === "snapshot") {
    const rows = yield* Effect.forEach(event.rows, (row) =>
      decodeProjectedRow(config, expectedTopic, selectedFields, row),
    );
    return typedLiveEvent<Row>({
      ...event,
      rows,
    });
  }
  type DecodedDeltaOperation = Extract<
    ViewServerProtocolEvent<Record<string, unknown>>,
    { readonly type: "delta" }
  >["operations"][number];
  const operations: Array<DecodedDeltaOperation> = [];
  for (const operation of event.operations) {
    if (operation.type === "insert" || operation.type === "update") {
      const row = yield* decodeProjectedRow(config, expectedTopic, selectedFields, operation.row);
      operations.push({
        ...operation,
        row,
      });
    } else {
      operations.push(operation);
    }
  }
  return typedLiveEvent<Row>({
    ...event,
    operations,
  });
});

const encodeSystemRow = Effect.fn("ViewServerProtocol.system.row.encode")(function* <Row>(
  topic: string,
  schema: Schema.Codec<Row, unknown, never, never>,
  row: Row,
) {
  const encoded = yield* Schema.encodeUnknownEffect(Schema.toCodecJson(schema))(row).pipe(
    Effect.mapError((error) => invalidRow(topic, `Invalid system row: ${error.message}`)),
  );
  return yield* Schema.decodeUnknownEffect(ViewServerWireRowSchema)(encoded).pipe(Effect.orDie);
});

const decodeSystemRow = Effect.fn("ViewServerProtocol.system.row.decode")(function* <Row>(
  topic: string,
  schema: Schema.Codec<Row, unknown, never, never>,
  row: ViewServerWireRow,
) {
  return yield* Schema.decodeUnknownEffect(Schema.toCodecJson(schema))(row).pipe(
    Effect.mapError((error) => invalidRow(topic, `Invalid system row: ${error.message}`)),
  );
});

const encodeSystemLiveEvent = Effect.fn("ViewServerProtocol.system.event.encode")(function* <Row>(
  expectedTopic: string,
  schema: Schema.Codec<Row, unknown, never, never>,
  event: ViewServerProtocolEvent<Row>,
) {
  if (event.topic !== expectedTopic) {
    return yield* Effect.fail(
      invalidRow(
        expectedTopic,
        `Received event for ${event.topic} while subscribed to ${expectedTopic}`,
      ),
    );
  }
  if (event.type === "status") {
    return encodeStatusEvent(event);
  }
  if (event.type === "snapshot") {
    const rows = yield* Effect.forEach(event.rows, (row) =>
      encodeSystemRow(expectedTopic, schema, row),
    );
    return {
      ...event,
      rows,
    };
  }
  type WireDeltaOperation = Extract<
    ViewServerWireEvent,
    { readonly type: "delta" }
  >["operations"][number];
  const operations: Array<WireDeltaOperation> = [];
  for (const operation of event.operations) {
    if (operation.type === "insert" || operation.type === "update") {
      const row = yield* encodeSystemRow(expectedTopic, schema, operation.row);
      operations.push({
        ...operation,
        row,
      });
    } else {
      operations.push(operation);
    }
  }
  return {
    ...event,
    operations,
  };
});

const decodeSystemLiveEvent = Effect.fn("ViewServerProtocol.system.event.decode")(function* <Row>(
  expectedTopic: string,
  schema: Schema.Codec<Row, unknown, never, never>,
  event: ViewServerWireEvent,
) {
  if (event.topic !== expectedTopic) {
    return yield* Effect.fail(
      invalidRow(
        expectedTopic,
        `Received event for ${event.topic} while subscribed to ${expectedTopic}`,
      ),
    );
  }
  if (event.type === "status") {
    return event;
  }
  if (event.type === "snapshot") {
    const rows = yield* Effect.forEach(event.rows, (row) =>
      decodeSystemRow(expectedTopic, schema, row),
    );
    return {
      ...event,
      rows,
    };
  }
  type DecodedDeltaOperation = Extract<
    ViewServerProtocolEvent<Row>,
    { readonly type: "delta" }
  >["operations"][number];
  const operations: Array<DecodedDeltaOperation> = [];
  for (const operation of event.operations) {
    if (operation.type === "insert" || operation.type === "update") {
      const row = yield* decodeSystemRow(expectedTopic, schema, operation.row);
      operations.push({
        ...operation,
        row,
      });
    } else {
      operations.push(operation);
    }
  }
  return {
    ...event,
    operations,
  };
});

function typedHealthSummaryEvent<Topics extends TopicDefinitions>(
  event: ViewServerProtocolEvent<ViewServerHealthSummaryRow>,
): ViewServerProtocolEvent<ViewServerHealthSummaryRow<Topics>>;
function typedHealthSummaryEvent(
  event: ViewServerProtocolEvent<ViewServerHealthSummaryRow>,
): ViewServerProtocolEvent<ViewServerHealthSummaryRow> {
  return event;
}

function typedHealthTopicEvent<Topics extends TopicDefinitions>(
  event: ViewServerProtocolEvent<ViewServerHealthTopicRow>,
): ViewServerProtocolEvent<ViewServerHealthTopicRow<Extract<keyof Topics, string>>>;
function typedHealthTopicEvent(
  event: ViewServerProtocolEvent<ViewServerHealthTopicRow>,
): ViewServerProtocolEvent<ViewServerHealthTopicRow> {
  return event;
}

export const viewServerEncodeHealthSummaryEvent = Effect.fn(
  "ViewServerProtocol.healthSummary.event.encode",
)(function* (event: ViewServerProtocolEvent<ViewServerHealthSummaryRow>) {
  return yield* encodeSystemLiveEvent(
    VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
    ViewServerHealthSummaryRowSchema,
    event,
  );
});

export const viewServerDecodeHealthSummaryEvent = Effect.fn(
  "ViewServerProtocol.healthSummary.event.decode",
)(function* <const Topics extends TopicDefinitions>(event: ViewServerWireEvent) {
  const decoded = yield* decodeSystemLiveEvent(
    VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
    ViewServerHealthSummaryRowSchema,
    event,
  );
  return typedHealthSummaryEvent<Topics>(decoded);
});

export const viewServerEncodeHealthTopicEvent = Effect.fn(
  "ViewServerProtocol.healthTopic.event.encode",
)(function* (event: ViewServerProtocolEvent<ViewServerHealthTopicRow>) {
  return yield* encodeSystemLiveEvent(
    VIEW_SERVER_HEALTH_TOPIC,
    ViewServerHealthTopicRowSchema,
    event,
  );
});

export const viewServerDecodeHealthTopicEvent = Effect.fn(
  "ViewServerProtocol.healthTopic.event.decode",
)(function* <const Topics extends TopicDefinitions>(event: ViewServerWireEvent) {
  const decoded = yield* decodeSystemLiveEvent(
    VIEW_SERVER_HEALTH_TOPIC,
    ViewServerHealthTopicRowSchema,
    event,
  );
  return typedHealthTopicEvent<Topics>(decoded);
});

function typedHealth<Topics extends TopicDefinitions>(
  health: ViewServerWireHealth,
): ViewServerHealth<Topics>;
function typedHealth(health: ViewServerWireHealth): ViewServerWireHealth {
  return health;
}

export const viewServerDecodeHealth = Effect.fn("ViewServerProtocol.health.decode")(function* <
  const Topics extends TopicDefinitions,
>(config: { readonly topics: Topics }, health: ViewServerWireHealth) {
  for (const topic of Object.keys(config.topics)) {
    if (!Object.hasOwn(health.engine.topics, topic)) {
      return yield* Effect.fail(invalidRow(topic, `Health payload is missing topic: ${topic}`));
    }
  }
  return typedHealth<Topics>(health);
});
