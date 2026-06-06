import type {
  Aggregates,
  FieldKey,
  GroupedOrderBy,
  RowSchema,
  TopicDefinitions,
  TopicRow,
  ViewServerRuntimeError,
  Where,
} from "@view-server/config";
import { viewServerSchemaFieldMetadata } from "@view-server/config";
import { Effect, Schema } from "effect";
import type { JsonFieldSchema } from "./protocol-field-filter-codec";
import {
  decodeWhere,
  encodeWhere,
  hasOnlyKnownFields,
  hasTopic,
  invalidQuery,
  invalidTopic,
  strictParseOptions,
  validateWindow,
  viewServerDecodeTopic,
} from "./protocol-query-common";
import {
  LooseWireGroupedQuerySchema,
  type LooseWireGroupedQuery,
  type ViewServerWireGroupedQuery,
} from "./protocol-query-schema";

type TrustedGroupedQuery<Row> = {
  readonly groupBy: readonly [FieldKey<Row>, ...Array<FieldKey<Row>>];
  readonly aggregates: Aggregates<Row>;
  readonly where?: Where<Row>;
  readonly orderBy?: ReadonlyArray<GroupedOrderBy<Row>>;
  readonly offset?: number;
  readonly limit?: number;
};

export type ViewServerValidatedGroupedQuery<Row> = TrustedGroupedQuery<Row>;

const dangerousRecordKeys = new Set(["__proto__", "prototype", "constructor"]);

const isNumericFieldSchema = (schema: JsonFieldSchema | undefined): boolean => {
  return schema !== undefined && viewServerSchemaFieldMetadata(schema).isNumeric;
};

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
