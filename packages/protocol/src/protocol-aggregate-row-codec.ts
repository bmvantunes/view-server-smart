import type { TopicDefinitions, ViewServerRuntimeError } from "@view-server/config";
import { viewServerSchemaFieldMetadata } from "@view-server/config";
import { Effect, Schema } from "effect";
import * as BigDecimal from "effect/BigDecimal";
import type { ViewServerWireAggregate } from "./protocol-query-schema";

const invalidRow = (topic: string, message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidRow",
  message,
  topic,
});

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isBigIntFieldSchema = (schema: Schema.Codec<unknown>): boolean =>
  viewServerSchemaFieldMetadata(schema).sumResultKind === "bigint";

const bigintPattern = /^-?\d+$/;

type AggregateEnvelope =
  | {
      readonly _viewServerAggregate: "bigint";
      readonly value: string;
    }
  | {
      readonly _viewServerAggregate: "bigdecimal";
      readonly value: string;
    }
  | {
      readonly _viewServerAggregate: "json";
      readonly value: Schema.Json;
    };

const isAggregateEnvelope = (value: unknown): value is AggregateEnvelope =>
  isRecord(value) &&
  ((value["_viewServerAggregate"] === "bigint" && typeof value["value"] === "string") ||
    (value["_viewServerAggregate"] === "bigdecimal" && typeof value["value"] === "string") ||
    (value["_viewServerAggregate"] === "json" &&
      Schema.decodeUnknownOption(Schema.Json)(value["value"])._tag === "Some"));

const encodeJsonAggregateEnvelope = (value: Schema.Json): AggregateEnvelope => ({
  _viewServerAggregate: "json",
  value,
});

const encodeBigIntAggregateEnvelope = (value: bigint): AggregateEnvelope => ({
  _viewServerAggregate: "bigint",
  value: value.toString(),
});

const encodeBigDecimalAggregateEnvelope = (value: BigDecimal.BigDecimal): AggregateEnvelope => ({
  _viewServerAggregate: "bigdecimal",
  value: BigDecimal.format(value),
});

const decodeAggregateEnvelope = Effect.fn("ViewServerProtocol.row.aggregate.envelope.decode")(
  function* (topic: string, field: string, value: unknown) {
    if (!isAggregateEnvelope(value)) {
      return yield* Effect.fail(
        invalidRow(topic, `Aggregate ${field} must be a View Server aggregate envelope.`),
      );
    }
    return value;
  },
);

const encodeAggregateJsonFieldValue = Effect.fn(
  "ViewServerProtocol.row.aggregate.jsonField.encode",
)(function* (topic: string, field: string, schema: Schema.Codec<unknown>, value: unknown) {
  const encoded = yield* Schema.encodeUnknownEffect(Schema.toCodecJson(schema))(value).pipe(
    Effect.mapError((error) => invalidRow(topic, `Invalid field ${field}: ${error.message}`)),
  );
  return yield* Schema.decodeUnknownEffect(Schema.Json)(encoded).pipe(
    Effect.mapError((error) =>
      invalidRow(topic, `Field ${field} is not JSON-safe: ${error.message}`),
    ),
  );
});

const encodeBigIntAggregateValue = Effect.fn("ViewServerProtocol.row.aggregate.bigint.encode")(
  function* (topic: string, field: string, value: unknown) {
    if (typeof value !== "bigint") {
      return yield* Effect.fail(invalidRow(topic, `Aggregate ${field} must be a bigint.`));
    }
    return encodeBigIntAggregateEnvelope(value);
  },
);

const decodeBigIntAggregateValue = Effect.fn("ViewServerProtocol.row.aggregate.bigint.decode")(
  function* (topic: string, field: string, value: unknown) {
    const envelope = yield* decodeAggregateEnvelope(topic, field, value);
    if (envelope._viewServerAggregate !== "bigint" || !bigintPattern.test(envelope.value)) {
      return yield* Effect.fail(invalidRow(topic, `Aggregate ${field} must be a bigint envelope.`));
    }
    return BigInt(envelope.value);
  },
);

const encodeBigDecimalAggregateValue = Effect.fn(
  "ViewServerProtocol.row.aggregate.bigDecimal.encode",
)(function* (topic: string, field: string, value: unknown) {
  if (!BigDecimal.isBigDecimal(value)) {
    return yield* Effect.fail(invalidRow(topic, `Aggregate ${field} must be a BigDecimal.`));
  }
  return encodeBigDecimalAggregateEnvelope(value);
});

const decodeBigDecimalAggregateValue = Effect.fn(
  "ViewServerProtocol.row.aggregate.bigDecimal.decode",
)(function* (topic: string, field: string, value: unknown) {
  const envelope = yield* decodeAggregateEnvelope(topic, field, value);
  if (envelope._viewServerAggregate !== "bigdecimal") {
    return yield* Effect.fail(
      invalidRow(topic, `Aggregate ${field} must be a BigDecimal envelope.`),
    );
  }
  return yield* Schema.decodeUnknownEffect(Schema.toCodecJson(Schema.BigDecimal))(
    envelope.value,
  ).pipe(
    Effect.mapError((error) => invalidRow(topic, `Invalid aggregate ${field}: ${error.message}`)),
  );
});

const encodeJsonAggregateValue = Effect.fn("ViewServerProtocol.row.aggregate.json.encode")(
  function* (topic: string, field: string, schema: Schema.Codec<unknown>, value: unknown) {
    const encoded = yield* encodeAggregateJsonFieldValue(topic, field, schema, value);
    return encodeJsonAggregateEnvelope(encoded);
  },
);

const decodeJsonAggregateValue = Effect.fn("ViewServerProtocol.row.aggregate.json.decode")(
  function* (topic: string, field: string, schema: Schema.Codec<unknown>, value: unknown) {
    const envelope = yield* decodeAggregateEnvelope(topic, field, value);
    if (envelope._viewServerAggregate !== "json") {
      return yield* Effect.fail(
        invalidRow(topic, `Aggregate ${field} must be a JSON aggregate envelope.`),
      );
    }
    return yield* Schema.decodeUnknownEffect(Schema.toCodecJson(schema))(envelope.value).pipe(
      Effect.mapError((error) => invalidRow(topic, `Invalid field ${field}: ${error.message}`)),
    );
  },
);

const aggregateFieldSchema = Effect.fn("ViewServerProtocol.row.aggregate.fieldSchema")(function* <
  const Topics extends TopicDefinitions,
>(config: { readonly topics: Topics }, topic: Extract<keyof Topics, string>, field: string) {
  const fieldSchema = config.topics[topic]!.schema.fields[field];
  if (fieldSchema === undefined) {
    return yield* Effect.fail(
      invalidRow(topic, `Aggregate references unknown field for topic ${topic}: ${field}`),
    );
  }
  return fieldSchema;
});

export const encodeAggregateValue = Effect.fn("ViewServerProtocol.row.aggregate.encode")(function* <
  const Topics extends TopicDefinitions,
>(
  config: { readonly topics: Topics },
  topic: Extract<keyof Topics, string>,
  field: string,
  aggregate: ViewServerWireAggregate,
  value: unknown,
) {
  if (aggregate.aggFunc === "count" || aggregate.aggFunc === "countDistinct") {
    return yield* encodeBigIntAggregateValue(topic, field, value);
  }
  const fieldSchema = yield* aggregateFieldSchema(config, topic, aggregate.field);
  if (aggregate.aggFunc === "avg") {
    return yield* encodeBigDecimalAggregateValue(topic, field, value);
  }
  if (aggregate.aggFunc === "sum") {
    if (isBigIntFieldSchema(fieldSchema)) {
      return yield* encodeBigIntAggregateValue(topic, field, value);
    }
    return yield* encodeBigDecimalAggregateValue(topic, field, value);
  }
  return yield* encodeJsonAggregateValue(topic, field, fieldSchema, value);
});

export const decodeAggregateValue = Effect.fn("ViewServerProtocol.row.aggregate.decode")(function* <
  const Topics extends TopicDefinitions,
>(
  config: { readonly topics: Topics },
  topic: Extract<keyof Topics, string>,
  field: string,
  aggregate: ViewServerWireAggregate,
  value: unknown,
) {
  if (aggregate.aggFunc === "count" || aggregate.aggFunc === "countDistinct") {
    return yield* decodeBigIntAggregateValue(topic, field, value);
  }
  const fieldSchema = yield* aggregateFieldSchema(config, topic, aggregate.field);
  if (aggregate.aggFunc === "avg") {
    return yield* decodeBigDecimalAggregateValue(topic, field, value);
  }
  if (aggregate.aggFunc === "sum") {
    if (isBigIntFieldSchema(fieldSchema)) {
      return yield* decodeBigIntAggregateValue(topic, field, value);
    }
    return yield* decodeBigDecimalAggregateValue(topic, field, value);
  }
  return yield* decodeJsonAggregateValue(topic, field, fieldSchema, value);
});
