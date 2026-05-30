import type {
  ExactRawQuery,
  FieldKey,
  OrderBy,
  RowSchema,
  TopicDefinitions,
  TopicRow,
  ViewServerRuntimeError,
  Where,
} from "@view-server/config";
import { Effect, Schema } from "effect";

type JsonFieldSchema = Schema.Codec<unknown, unknown, never, never>;

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

export const ViewServerHealthQuerySchema = Schema.Struct({
  select: Schema.Array(Schema.String),
});

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

export const viewServerDecodeHealthQuery = Effect.fn("ViewServerProtocol.healthQuery.decode")(
  function* (topic: string, query: unknown) {
    const decoded = yield* Schema.decodeUnknownEffect(ViewServerHealthQuerySchema)(
      query,
      strictParseOptions,
    ).pipe(Effect.mapError((error) => invalidQuery(topic, error.message)));
    if (decoded.select.length !== 1 || decoded.select[0] !== "id") {
      return yield* Effect.fail(invalidQuery(topic, "Health query select must be exactly: id"));
    }
    return decoded;
  },
);

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
