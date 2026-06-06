import type {
  Aggregates,
  FieldKey,
  GroupedOrderBy,
  OrderBy,
  RowSchema,
  TopicDefinitions,
  TopicRow,
  ViewServerRuntimeError,
  Where,
} from "@view-server/config";
import { viewServerSchemaFieldMetadata } from "@view-server/config";
import { Effect, Schema } from "effect";
import {
  decodeFilterValue,
  encodeFilterValue,
  type JsonFieldSchema,
} from "./protocol-field-filter-codec";
import {
  LooseWireGroupedQuerySchema,
  type LooseWireGroupedQuery,
  LooseWireRawQuerySchema,
  type LooseWireRawQuery,
  ViewServerHealthQuerySchema,
  type ViewServerWireGroupedQuery,
  type ViewServerWireRawQuery,
} from "./protocol-query-schema";

type TrustedRawQuery<Row> = {
  readonly select: ReadonlyArray<FieldKey<Row>>;
  readonly where?: Where<Row>;
  readonly orderBy?: ReadonlyArray<OrderBy<Row>>;
  readonly offset?: number;
  readonly limit?: number;
};

export type ViewServerValidatedRawQuery<Row> = TrustedRawQuery<Row>;

type TrustedGroupedQuery<Row> = {
  readonly groupBy: readonly [FieldKey<Row>, ...Array<FieldKey<Row>>];
  readonly aggregates: Aggregates<Row>;
  readonly where?: Where<Row>;
  readonly orderBy?: ReadonlyArray<GroupedOrderBy<Row>>;
  readonly offset?: number;
  readonly limit?: number;
};

export type ViewServerValidatedGroupedQuery<Row> = TrustedGroupedQuery<Row>;

export type ViewServerValidatedLiveQuery<Row> =
  | ViewServerValidatedRawQuery<Row>
  | ViewServerValidatedGroupedQuery<Row>;

const dangerousRecordKeys = new Set(["__proto__", "prototype", "constructor"]);

const strictParseOptions = {
  onExcessProperty: "error",
} as const;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isNumericFieldSchema = (schema: JsonFieldSchema | undefined): boolean => {
  return schema !== undefined && viewServerSchemaFieldMetadata(schema).isNumeric;
};

const isGroupedQueryInput = (query: unknown): query is { readonly groupBy: unknown } =>
  isRecord(query) && Object.hasOwn(query, "groupBy");

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

const validateWindow = Effect.fn("ViewServerProtocol.query.window.validate")(function* (
  topic: string,
  offset: number | undefined,
  limit: number | undefined,
) {
  if (offset !== undefined && (!Number.isSafeInteger(offset) || offset < 0)) {
    return yield* Effect.fail(invalidQuery(topic, "Query offset must be a non-negative integer"));
  }
  if (limit !== undefined && (!Number.isSafeInteger(limit) || limit < 0)) {
    return yield* Effect.fail(invalidQuery(topic, "Query limit must be a non-negative integer"));
  }
});

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
  yield* validateWindow(topic, decoded.offset, decoded.limit);
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

const validateGroupedQuery = Effect.fn("ViewServerProtocol.groupedQuery.validate")(function* (
  topic: string,
  schema: RowSchema,
  decoded: LooseWireGroupedQuery,
) {
  if (decoded.groupBy.length === 0) {
    return yield* Effect.fail(
      invalidQuery(topic, "Grouped query groupBy must include at least one field"),
    );
  }
  const aggregateAliases = Object.keys(decoded.aggregates);
  if (aggregateAliases.length === 0) {
    return yield* Effect.fail(
      invalidQuery(topic, "Grouped query aggregates must include at least one aggregate"),
    );
  }
  for (const groupField of decoded.groupBy) {
    if (schema.fields[groupField] === undefined) {
      return yield* Effect.fail(
        invalidQuery(topic, `Query references an unknown field for topic: ${topic}`),
      );
    }
  }
  for (const [alias, aggregate] of Object.entries(decoded.aggregates)) {
    if (dangerousRecordKeys.has(alias)) {
      return yield* Effect.fail(
        invalidQuery(topic, `Grouped aggregate alias is not allowed: ${alias}`),
      );
    }
    if (decoded.groupBy.includes(alias)) {
      return yield* Effect.fail(
        invalidQuery(topic, `Aggregate alias collides with groupBy field: ${alias}`),
      );
    }
    if (aggregate.aggFunc !== "count" && schema.fields[aggregate.field] === undefined) {
      return yield* Effect.fail(
        invalidQuery(topic, `Query references an unknown field for topic: ${topic}`),
      );
    }
    if (
      (aggregate.aggFunc === "sum" || aggregate.aggFunc === "avg") &&
      !isNumericFieldSchema(schema.fields[aggregate.field])
    ) {
      return yield* Effect.fail(
        invalidQuery(topic, `Grouped aggregate ${alias} must reference a numeric field`),
      );
    }
  }
  if (decoded.where !== undefined && !hasOnlyKnownFields(schema, Object.keys(decoded.where))) {
    return yield* Effect.fail(
      invalidQuery(topic, `Query references an unknown field for topic: ${topic}`),
    );
  }
  if (decoded.orderBy !== undefined) {
    for (const entry of decoded.orderBy) {
      if ("field" in entry && !decoded.groupBy.includes(entry.field)) {
        return yield* Effect.fail(
          invalidQuery(topic, `Grouped orderBy field is not in groupBy: ${entry.field}`),
        );
      }
      if ("aggregate" in entry && !Object.hasOwn(decoded.aggregates, entry.aggregate)) {
        return yield* Effect.fail(
          invalidQuery(topic, `Grouped orderBy aggregate is not defined: ${entry.aggregate}`),
        );
      }
    }
  }
  yield* validateWindow(topic, decoded.offset, decoded.limit);
});

export const viewServerEncodeGroupedQuery = Effect.fn("ViewServerProtocol.groupedQuery.encode")(
  function* <const Topics extends TopicDefinitions, Topic extends Extract<keyof Topics, string>>(
    config: { readonly topics: Topics },
    topic: Topic,
    query: unknown,
  ) {
    if (!hasTopic(config, topic)) {
      return yield* Effect.fail(invalidTopic(topic));
    }
    const decoded = yield* Schema.decodeUnknownEffect(LooseWireGroupedQuerySchema)(
      query,
      strictParseOptions,
    ).pipe(Effect.mapError((error) => invalidQuery(topic, error.message)));
    const topicSchema = config.topics[topic]!.schema;
    yield* validateGroupedQuery(topic, topicSchema, decoded);
    const where = yield* encodeWhere(config, topic, decoded.where);
    const wireQuery: ViewServerWireGroupedQuery = {
      groupBy: decoded.groupBy,
      aggregates: decoded.aggregates,
      ...(where === undefined ? {} : { where }),
      ...(decoded.orderBy === undefined ? {} : { orderBy: decoded.orderBy }),
      ...(decoded.offset === undefined ? {} : { offset: decoded.offset }),
      ...(decoded.limit === undefined ? {} : { limit: decoded.limit }),
    };
    return wireQuery;
  },
);

export const viewServerEncodeLiveQuery = Effect.fn("ViewServerProtocol.liveQuery.encode")(
  function* <const Topics extends TopicDefinitions, Topic extends Extract<keyof Topics, string>>(
    config: { readonly topics: Topics },
    topic: Topic,
    query: unknown,
  ) {
    if (isGroupedQueryInput(query)) {
      return yield* viewServerEncodeGroupedQuery(config, topic, query);
    }
    return yield* viewServerEncodeRawQuery(config, topic, query);
  },
);

function validatedRawQuery<Row>(query: LooseWireRawQuery): ViewServerValidatedRawQuery<Row>;
function validatedRawQuery(query: LooseWireRawQuery) {
  return query;
}

export const viewServerDecodeRawQuery: <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  config: { readonly topics: Topics },
  topic: Topic,
  query: unknown,
) => Effect.Effect<ViewServerValidatedRawQuery<TopicRow<Topics, Topic>>, ViewServerRuntimeError> =
  Effect.fn("ViewServerProtocol.query.decode")(function* <
    const Topics extends TopicDefinitions,
    Topic extends Extract<keyof Topics, string>,
  >(config: { readonly topics: Topics }, topic: Topic, query: unknown) {
    const decodedTopic = yield* viewServerDecodeTopic(config, topic);
    const decoded = yield* Schema.decodeUnknownEffect(LooseWireRawQuerySchema)(
      query,
      strictParseOptions,
    ).pipe(Effect.mapError((error) => invalidQuery(topic, error.message)));
    if (decoded.select.length === 0) {
      return yield* Effect.fail(
        invalidQuery(topic, "Query select must include at least one field"),
      );
    }
    yield* validateWindow(topic, decoded.offset, decoded.limit);
    const topicSchema = config.topics[decodedTopic]!.schema;
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

function validatedGroupedQuery<Row>(
  query: LooseWireGroupedQuery,
): ViewServerValidatedGroupedQuery<Row>;
function validatedGroupedQuery(query: LooseWireGroupedQuery) {
  return query;
}

export const viewServerDecodeGroupedQuery: <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  config: { readonly topics: Topics },
  topic: Topic,
  query: unknown,
) => Effect.Effect<
  ViewServerValidatedGroupedQuery<TopicRow<Topics, Topic>>,
  ViewServerRuntimeError
> = Effect.fn("ViewServerProtocol.groupedQuery.decode")(function* <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(config: { readonly topics: Topics }, topic: Topic, query: unknown) {
  const decodedTopic = yield* viewServerDecodeTopic(config, topic);
  const decoded = yield* Schema.decodeUnknownEffect(LooseWireGroupedQuerySchema)(
    query,
    strictParseOptions,
  ).pipe(Effect.mapError((error) => invalidQuery(topic, error.message)));
  const topicSchema = config.topics[decodedTopic]!.schema;
  yield* validateGroupedQuery(topic, topicSchema, decoded);
  const where = yield* decodeWhere(topic, topicSchema, decoded.where);
  return validatedGroupedQuery<TopicRow<Topics, Topic>>({
    groupBy: decoded.groupBy,
    aggregates: decoded.aggregates,
    ...(where === undefined ? {} : { where }),
    ...(decoded.orderBy === undefined ? {} : { orderBy: decoded.orderBy }),
    ...(decoded.offset === undefined ? {} : { offset: decoded.offset }),
    ...(decoded.limit === undefined ? {} : { limit: decoded.limit }),
  });
});

export const viewServerDecodeLiveQuery: <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  config: { readonly topics: Topics },
  topic: Topic,
  query: unknown,
) => Effect.Effect<ViewServerValidatedLiveQuery<TopicRow<Topics, Topic>>, ViewServerRuntimeError> =
  Effect.fn("ViewServerProtocol.liveQuery.decode")(function* <
    const Topics extends TopicDefinitions,
    Topic extends Extract<keyof Topics, string>,
  >(config: { readonly topics: Topics }, topic: Topic, query: unknown) {
    if (isGroupedQueryInput(query)) {
      return yield* viewServerDecodeGroupedQuery(config, topic, query);
    }
    return yield* viewServerDecodeRawQuery(config, topic, query);
  });
