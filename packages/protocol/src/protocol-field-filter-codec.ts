import { viewServerSchemaFieldMetadata, type ViewServerRuntimeError } from "@view-server/config";
import { Effect, Schema } from "effect";
import {
  decodeJsonFieldValue,
  encodeJsonFieldValue,
  type JsonFieldSchema,
} from "./protocol-json-field-codec";

export const filterOperatorKeys = new Set([
  "eq",
  "neq",
  "in",
  "gt",
  "gte",
  "lt",
  "lte",
  "startsWith",
]);

const rangeFilterOperatorKeys = new Set(["gt", "gte", "lt", "lte"]);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const isFilterObject = (value: unknown): value is Record<string, unknown> =>
  isRecord(value) &&
  Object.keys(value).length > 0 &&
  Object.keys(value).every((key) => filterOperatorKeys.has(key));

const invalidQuery = (topic: string, message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidQuery",
  message,
  topic,
});

const encodeFilterJsonFieldValue = Effect.fn("ViewServerProtocol.field.encode")(function* (
  topic: string,
  field: string,
  schema: JsonFieldSchema,
  value: unknown,
) {
  return yield* encodeJsonFieldValue(schema, value, {
    invalid: (message) => invalidQuery(topic, `Invalid filter for ${field}: ${message}`),
    notJsonSafe: (message) => invalidQuery(topic, `Filter ${field} is not JSON-safe: ${message}`),
  });
});

const decodeFilterJsonFieldValue = Effect.fn("ViewServerProtocol.field.decode")(function* (
  topic: string,
  field: string,
  schema: JsonFieldSchema,
  value: unknown,
) {
  return yield* decodeJsonFieldValue(schema, value, {
    invalid: (message) => invalidQuery(topic, `Invalid filter for ${field}: ${message}`),
  });
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

const validateOperatorFilterValue = Effect.fn("ViewServerProtocol.filter.operator.validate")(
  function* (
    topic: string,
    field: string,
    schema: JsonFieldSchema,
    value: Record<string, unknown>,
  ) {
    const metadata = viewServerSchemaFieldMetadata(schema);
    if (metadata.isStructured) {
      return;
    }
    const keys = Object.keys(value);
    if (keys.includes("startsWith") && !metadata.isString) {
      return yield* Effect.fail(invalidQuery(topic, `Filter ${field} does not support startsWith`));
    }
    if (keys.some((key) => rangeFilterOperatorKeys.has(key)) && !metadata.isNumeric) {
      return yield* Effect.fail(
        invalidQuery(topic, `Filter ${field} does not support range operators`),
      );
    }
  },
);

const encodeOperatorFilterValue = Effect.fn("ViewServerProtocol.filter.operator.encode")(function* (
  topic: string,
  field: string,
  schema: JsonFieldSchema,
  value: Record<string, unknown>,
) {
  yield* validateOperatorFilterValue(topic, field, schema, value);
  const output: Record<string, Schema.Json> = {};
  for (const [operator, operatorValue] of Object.entries(value)) {
    if (operator === "in" && Array.isArray(operatorValue)) {
      output[operator] = yield* Effect.forEach(operatorValue, (entry) =>
        encodeFilterJsonFieldValue(topic, field, schema, entry),
      );
    } else if (operator === "startsWith") {
      output[operator] = yield* encodeStringFilterValue(topic, field, schema, operatorValue);
    } else {
      output[operator] = yield* encodeFilterJsonFieldValue(topic, field, schema, operatorValue);
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
  yield* validateOperatorFilterValue(topic, field, schema, value);
  const output: Record<string, unknown> = {};
  for (const [operator, operatorValue] of Object.entries(value)) {
    if (operator === "in" && Array.isArray(operatorValue)) {
      output[operator] = yield* Effect.forEach(operatorValue, (entry) =>
        decodeFilterJsonFieldValue(topic, field, schema, entry),
      );
    } else if (operator === "startsWith") {
      output[operator] = yield* decodeStringFilterValue(topic, field, schema, operatorValue);
    } else {
      output[operator] = yield* decodeFilterJsonFieldValue(topic, field, schema, operatorValue);
    }
  }
  return output;
});

export const encodeFilterValue = Effect.fn("ViewServerProtocol.filter.encode")(function* (
  topic: string,
  field: string,
  schema: JsonFieldSchema,
  value: unknown,
) {
  if (!isFilterObject(value)) {
    return yield* encodeFilterJsonFieldValue(topic, field, schema, value);
  }
  return yield* Effect.matchEffect(encodeOperatorFilterValue(topic, field, schema, value), {
    onFailure: (operatorError) =>
      Effect.matchEffect(encodeFilterJsonFieldValue(topic, field, schema, value), {
        onFailure: () => Effect.fail(operatorError),
        onSuccess: Effect.succeed,
      }),
    onSuccess: Effect.succeed,
  });
});

export const decodeFilterValue = Effect.fn("ViewServerProtocol.filter.decode")(function* (
  topic: string,
  field: string,
  schema: JsonFieldSchema,
  value: unknown,
) {
  if (!isFilterObject(value)) {
    return yield* decodeFilterJsonFieldValue(topic, field, schema, value);
  }
  return yield* Effect.matchEffect(decodeOperatorFilterValue(topic, field, schema, value), {
    onFailure: (operatorError) =>
      Effect.matchEffect(decodeFilterJsonFieldValue(topic, field, schema, value), {
        onFailure: () => Effect.fail(operatorError),
        onSuccess: Effect.succeed,
      }),
    onSuccess: Effect.succeed,
  });
});
