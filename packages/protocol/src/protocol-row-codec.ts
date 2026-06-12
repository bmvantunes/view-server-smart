import type { TopicDefinitions, ViewServerRuntimeError } from "@view-server/config";
import { Effect, Schema } from "effect";
import { decodeAggregateValue, encodeAggregateValue } from "./protocol-aggregate-row-codec";
import { ViewServerWireRowSchema, type ViewServerWireRow } from "./protocol-event-schema";
import {
  decodeJsonFieldValue,
  encodeJsonFieldValue,
  type JsonFieldSchema,
} from "./protocol-json-field-codec";
import type { ViewServerEventGroupedQuery, ViewServerEventQuery } from "./protocol-query-schema";

const invalidRow = (topic: string, message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidRow",
  message,
  topic,
});

export const isViewServerEventGroupedQuery = (
  query: ViewServerEventQuery,
): query is ViewServerEventGroupedQuery => "groupBy" in query;

const encodeEventJsonFieldValue = Effect.fn("ViewServerProtocol.event.field.encode")(function* (
  topic: string,
  field: string,
  schema: JsonFieldSchema,
  value: unknown,
) {
  return yield* encodeJsonFieldValue(schema, value, {
    invalid: (message) => invalidRow(topic, `Invalid field ${field}: ${message}`),
    notJsonSafe: (message) => invalidRow(topic, `Field ${field} is not JSON-safe: ${message}`),
  });
});

export const encodeProjectedRow = Effect.fn("ViewServerProtocol.row.project.encode")(function* <
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

export const decodeProjectedRow = Effect.fn("ViewServerProtocol.row.project.decode")(function* <
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
    output[field] = yield* decodeJsonFieldValue(fieldSchema, value, {
      invalid: (message) => invalidRow(topic, `Invalid field ${field}: ${message}`),
    });
  }
  return output;
});

export const encodeGroupedRow = Effect.fn("ViewServerProtocol.row.grouped.encode")(function* <
  const Topics extends TopicDefinitions,
>(
  config: { readonly topics: Topics },
  topic: Extract<keyof Topics, string>,
  query: ViewServerEventGroupedQuery,
  row: object,
) {
  const topicSchema = config.topics[topic]!.schema;
  const groupFields = new Set<string>(query.groupBy);
  const aggregateAliases = new Set<string>(Object.keys(query.aggregates));
  const output: Record<string, Schema.Json> = {};
  for (const field of groupFields) {
    if (!Object.hasOwn(row, field)) {
      return yield* Effect.fail(
        invalidRow(topic, `Missing grouped row field for topic ${topic}: ${field}`),
      );
    }
  }
  for (const alias of aggregateAliases) {
    if (!Object.hasOwn(row, alias)) {
      return yield* Effect.fail(
        invalidRow(topic, `Missing grouped aggregate for topic ${topic}: ${alias}`),
      );
    }
  }
  for (const [field, value] of Object.entries(row)) {
    if (groupFields.has(field)) {
      const fieldSchema = topicSchema.fields[field]!;
      output[field] = yield* encodeEventJsonFieldValue(topic, field, fieldSchema, value);
    } else if (aggregateAliases.has(field)) {
      const aggregate = query.aggregates[field];
      if (aggregate === undefined) {
        return yield* Effect.fail(
          invalidRow(topic, `Missing grouped aggregate definition for topic ${topic}: ${field}`),
        );
      }
      output[field] = yield* encodeAggregateValue(config, topic, field, aggregate, value);
    } else {
      return yield* Effect.fail(
        invalidRow(topic, `Unexpected grouped row field for topic ${topic}: ${field}`),
      );
    }
  }
  return output;
});

export const decodeGroupedRow = Effect.fn("ViewServerProtocol.row.grouped.decode")(function* <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  config: { readonly topics: Topics },
  topic: Topic,
  query: ViewServerEventGroupedQuery,
  row: ViewServerWireRow,
) {
  const topicSchema = config.topics[topic]!.schema;
  const groupFields = new Set<string>(query.groupBy);
  const aggregateAliases = new Set<string>(Object.keys(query.aggregates));
  const output: Record<string, unknown> = {};
  for (const field of groupFields) {
    if (!Object.hasOwn(row, field)) {
      return yield* Effect.fail(
        invalidRow(topic, `Missing grouped row field for topic ${topic}: ${field}`),
      );
    }
  }
  for (const alias of aggregateAliases) {
    if (!Object.hasOwn(row, alias)) {
      return yield* Effect.fail(
        invalidRow(topic, `Missing grouped aggregate for topic ${topic}: ${alias}`),
      );
    }
  }
  for (const [field, value] of Object.entries(row)) {
    if (groupFields.has(field)) {
      const fieldSchema = topicSchema.fields[field]!;
      output[field] = yield* decodeJsonFieldValue(fieldSchema, value, {
        invalid: (message) => invalidRow(topic, `Invalid field ${field}: ${message}`),
      });
    } else if (aggregateAliases.has(field)) {
      const aggregate = query.aggregates[field];
      if (aggregate === undefined) {
        return yield* Effect.fail(
          invalidRow(topic, `Missing grouped aggregate definition for topic ${topic}: ${field}`),
        );
      }
      output[field] = yield* decodeAggregateValue(config, topic, field, aggregate, value);
    } else {
      return yield* Effect.fail(
        invalidRow(topic, `Unexpected grouped row field for topic ${topic}: ${field}`),
      );
    }
  }
  return output;
});

export const encodeSystemRow = Effect.fn("ViewServerProtocol.system.row.encode")(function* <Row>(
  topic: string,
  schema: Schema.Codec<Row, unknown, never, never>,
  row: Row,
) {
  const encoded = yield* Schema.encodeUnknownEffect(Schema.toCodecJson(schema))(row).pipe(
    Effect.mapError((error) => invalidRow(topic, `Invalid system row: ${error.message}`)),
  );
  return yield* Schema.decodeUnknownEffect(ViewServerWireRowSchema)(encoded).pipe(Effect.orDie);
});

export const decodeSystemRow = Effect.fn("ViewServerProtocol.system.row.decode")(function* <Row>(
  topic: string,
  schema: Schema.Codec<Row, unknown, never, never>,
  row: ViewServerWireRow,
) {
  return yield* Schema.decodeUnknownEffect(Schema.toCodecJson(schema))(row).pipe(
    Effect.mapError((error) => invalidRow(topic, `Invalid system row: ${error.message}`)),
  );
});
