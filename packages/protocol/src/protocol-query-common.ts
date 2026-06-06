import type { RowSchema, TopicDefinitions, ViewServerRuntimeError } from "@view-server/config";
import { Effect, Schema } from "effect";
import { decodeFilterValue, encodeFilterValue } from "./protocol-field-filter-codec";
import { ViewServerHealthQuerySchema } from "./protocol-query-schema";

export const strictParseOptions = {
  onExcessProperty: "error",
} as const;

export const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const isGroupedQueryInput = (query: unknown): query is { readonly groupBy: unknown } =>
  isRecord(query) && Object.hasOwn(query, "groupBy");

export const invalidTopic = (topic: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidTopic",
  message: `Unknown topic: ${topic}`,
  topic,
});

export const invalidQuery = (topic: string, message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidQuery",
  message,
  topic,
});

export const hasTopic = <Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  topic: string,
): topic is Extract<keyof Topics, string> => Object.hasOwn(config.topics, topic);

export const getFieldSchema = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  topic: Extract<keyof Topics, string>,
  field: string,
) => {
  return config.topics[topic]!.schema.fields[field];
};

export const hasOnlyKnownFields = (schema: RowSchema, fields: Iterable<string>): boolean =>
  Array.from(fields).every((field) => schema.fields[field] !== undefined);

export const validateWindow = Effect.fn("ViewServerProtocol.query.window.validate")(function* (
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

export const encodeWhere = Effect.fn("ViewServerProtocol.query.where.encode")(function* <
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

export const decodeWhere = Effect.fn("ViewServerProtocol.query.where.decode")(function* (
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
