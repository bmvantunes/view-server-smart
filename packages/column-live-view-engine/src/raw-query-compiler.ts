import { viewServerSchemaFieldMetadata, type FieldKey, type OrderBy } from "@view-server/config";
import { Effect, Schema } from "effect";
import { format as formatBigDecimal, isBigDecimal, normalize, Order } from "effect/BigDecimal";
import {
  cloneRecord,
  cloneUnknown,
  fieldValue,
  isPlainRecord,
  isRecord,
  valuesEqual,
} from "./row-values";
import type { StoredRowOf } from "./query-result";

type RowObject = object;
const compiledRawQueryBrand: unique symbol = Symbol("CompiledRawQuery");

export class InvalidQueryError extends Schema.TaggedErrorClass<InvalidQueryError>()(
  "InvalidQueryError",
  {
    topic: Schema.String,
    message: Schema.String,
  },
) {}

export type RuntimeRawQuery = {
  readonly select: ReadonlyArray<string>;
  readonly where?: Record<string, unknown>;
  readonly orderBy?: ReadonlyArray<{
    readonly field: string;
    readonly direction: "asc" | "desc";
  }>;
  readonly offset?: number;
  readonly limit?: number;
};

type SchemaWithFields = Schema.Decoder<object> & {
  readonly fields: Record<string, unknown>;
};

export type RawQueryCompilerMetadata = {
  readonly fieldNames: ReadonlySet<string>;
  readonly fieldMetadata: ReadonlyMap<string, ReturnType<typeof viewServerSchemaFieldMetadata>>;
  readonly structuredFieldNames: ReadonlySet<string>;
  readonly structuredObjectFieldNames: ReadonlySet<string>;
  readonly stringFieldNames: ReadonlySet<string>;
  readonly numericFieldNames: ReadonlySet<string>;
  readonly bigintFieldNames: ReadonlySet<string>;
  readonly bigDecimalFieldNames: ReadonlySet<string>;
};

export type CompiledRawQuery<Row extends RowObject, ResultRow extends RowObject> = {
  readonly [compiledRawQueryBrand]: true;
  readonly query: RuntimeRawQuery;
  readonly matches: (row: Row) => boolean;
  readonly compare: (left: StoredRowOf<Row>, right: StoredRowOf<Row>) => number;
  readonly project: (row: Row) => ResultRow;
  readonly offset: number;
  readonly limit: number | undefined;
};

type FilterObject = {
  readonly eq?: unknown;
  readonly neq?: unknown;
  readonly in?: ReadonlyArray<unknown>;
  readonly gt?: unknown;
  readonly gte?: unknown;
  readonly lt?: unknown;
  readonly lte?: unknown;
  readonly startsWith?: string;
};

const rawQueryKeys = new Set(["where", "orderBy", "offset", "limit", "select"]);
const filterOperatorKeys = new Set(["eq", "neq", "in", "gt", "gte", "lt", "lte", "startsWith"]);
const rangeFilterOperatorKeys = new Set(["gt", "gte", "lt", "lte"]);

const isDenseArray = (value: ReadonlyArray<unknown>): boolean => {
  for (let index = 0; index < value.length; index += 1) {
    if (!(index in value)) {
      return false;
    }
  }
  return true;
};

const isValidWindowNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0;

const isQueryValueSafe = (value: unknown, active: WeakSet<object> = new WeakSet()): boolean => {
  if (
    value === null ||
    value === undefined ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean" ||
    isBigDecimal(value)
  ) {
    return true;
  }
  if (Array.isArray(value)) {
    if (active.has(value)) {
      return false;
    }
    active.add(value);
    const safe = isDenseArray(value) && value.every((entry) => isQueryValueSafe(entry, active));
    active.delete(value);
    return safe;
  }
  if (isPlainRecord(value)) {
    if (active.has(value)) {
      return false;
    }
    active.add(value);
    const safe = Object.values(value).every((entry) => isQueryValueSafe(entry, active));
    active.delete(value);
    return safe;
  }
  return false;
};

const isSchemaWithFields = (schema: Schema.Decoder<object>): schema is SchemaWithFields =>
  "fields" in schema && isRecord(schema.fields);

const schemaFieldNames = (schema: Schema.Decoder<object>): ReadonlySet<string> =>
  isSchemaWithFields(schema) ? new Set(Object.keys(schema.fields)) : new Set();

const schemaFieldMetadata = (
  schema: Schema.Decoder<object>,
): ReadonlyMap<string, ReturnType<typeof viewServerSchemaFieldMetadata>> => {
  if (!isSchemaWithFields(schema)) {
    return new Map();
  }

  const fields = new Map<string, ReturnType<typeof viewServerSchemaFieldMetadata>>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    fields.set(field, viewServerSchemaFieldMetadata(fieldSchema));
  }
  return fields;
};

const schemaNumericFieldNames = (schema: Schema.Decoder<object>): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    if (!viewServerSchemaFieldMetadata(fieldSchema).isNumeric) {
      continue;
    }
    fields.add(field);
  }
  return fields;
};

const schemaBigintFieldNames = (schema: Schema.Decoder<object>): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    if (viewServerSchemaFieldMetadata(fieldSchema).isPureBigInt) {
      fields.add(field);
    }
  }
  return fields;
};

const schemaBigDecimalFieldNames = (schema: Schema.Decoder<object>): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    if (viewServerSchemaFieldMetadata(fieldSchema).sumResultKind === "bigDecimal") {
      fields.add(field);
    }
  }
  return fields;
};

const schemaStringFieldNames = (schema: Schema.Decoder<object>): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    if (viewServerSchemaFieldMetadata(fieldSchema).isString) {
      fields.add(field);
    }
  }
  return fields;
};

const schemaStructuredFieldNames = (schema: Schema.Decoder<object>): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    if (viewServerSchemaFieldMetadata(fieldSchema).isStructured) {
      fields.add(field);
    }
  }
  return fields;
};

const schemaStructuredObjectFieldNames = (schema: Schema.Decoder<object>): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    if (viewServerSchemaFieldMetadata(fieldSchema).isStructuredObject) {
      fields.add(field);
    }
  }
  return fields;
};

export const rawQueryCompilerMetadata = (
  schema: Schema.Decoder<object>,
): RawQueryCompilerMetadata => ({
  fieldNames: schemaFieldNames(schema),
  fieldMetadata: schemaFieldMetadata(schema),
  structuredFieldNames: schemaStructuredFieldNames(schema),
  structuredObjectFieldNames: schemaStructuredObjectFieldNames(schema),
  stringFieldNames: schemaStringFieldNames(schema),
  numericFieldNames: schemaNumericFieldNames(schema),
  bigintFieldNames: schemaBigintFieldNames(schema),
  bigDecimalFieldNames: schemaBigDecimalFieldNames(schema),
});

const decodeRawQuery = Effect.fn("ColumnLiveViewEngine.rawQuery.decode")((
  topic: string,
  metadata: RawQueryCompilerMetadata,
  query: unknown,
): Effect.Effect<RuntimeRawQuery, InvalidQueryError> => {
  if (query === undefined) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query select must be a non-empty array of strings.",
    });
  }
  if (!isPlainRecord(query)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query must be a plain object.",
    });
  }
  for (const key of Object.keys(query)) {
    if (!rawQueryKeys.has(key)) {
      return InvalidQueryError.make({
        topic,
        message: `Raw query contains unsupported key: ${key}.`,
      });
    }
  }

  const where = query["where"];
  if (where !== undefined && !isPlainRecord(where)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query where must be a plain object.",
    });
  }
  if (where !== undefined) {
    for (const field of Object.keys(where)) {
      if (!metadata.fieldNames.has(field)) {
        return InvalidQueryError.make({
          topic,
          message: `Raw query where contains unknown field: ${field}.`,
        });
      }
      if (!isQueryValueSafe(where[field])) {
        return InvalidQueryError.make({
          topic,
          message: `Raw query where field ${field} contains unsupported query value.`,
        });
      }
    }
  }

  const orderBy = query["orderBy"];
  if (orderBy !== undefined && !Array.isArray(orderBy)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query orderBy must be an array.",
    });
  }

  const select = query["select"];
  if (!Array.isArray(select)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query select must be a non-empty array of strings.",
    });
  }
  if (select.length === 0 || !isDenseArray(select)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query select must be a non-empty array of strings.",
    });
  }
  const selectedFields: Array<string> = [];
  for (const field of select) {
    if (typeof field !== "string") {
      return InvalidQueryError.make({
        topic,
        message: "Raw query select must be a non-empty array of strings.",
      });
    }
    if (!metadata.fieldNames.has(field)) {
      return InvalidQueryError.make({
        topic,
        message: `Raw query select contains unknown field: ${field}.`,
      });
    }
    selectedFields.push(field);
  }

  const offset = query["offset"];
  if (offset !== undefined && !isValidWindowNumber(offset)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query offset must be a non-negative safe integer.",
    });
  }

  const limit = query["limit"];
  if (limit !== undefined && !isValidWindowNumber(limit)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query limit must be a non-negative safe integer.",
    });
  }

  const decoded: {
    select: Array<string>;
    where?: Record<string, unknown>;
    orderBy?: Array<{ readonly field: string; readonly direction: "asc" | "desc" }>;
    offset?: number;
    limit?: number;
  } = {
    select: selectedFields,
  };

  if (where !== undefined) {
    let clonedWhere: Record<string, unknown>;
    try {
      clonedWhere = cloneRecord(where);
    } catch (cause) {
      return InvalidQueryError.make({
        topic,
        message: `Raw query where could not be cloned: ${String(cause)}`,
      });
    }
    decoded.where = clonedWhere;
  }
  if (offset !== undefined) {
    decoded.offset = offset;
  }
  if (limit !== undefined) {
    decoded.limit = limit;
  }
  const clonedOrderBy: Array<{ readonly field: string; readonly direction: "asc" | "desc" }> = [];
  if (Array.isArray(orderBy)) {
    for (const entry of orderBy) {
      if (!isPlainRecord(entry)) {
        return InvalidQueryError.make({
          topic,
          message: "Raw query orderBy entries must be plain objects.",
        });
      }
      for (const key of Object.keys(entry)) {
        if (key !== "field" && key !== "direction") {
          return InvalidQueryError.make({
            topic,
            message: `Raw query orderBy contains unsupported key: ${key}.`,
          });
        }
      }
      const field = entry["field"];
      if (typeof field !== "string") {
        return InvalidQueryError.make({
          topic,
          message: "Raw query orderBy field must be a string.",
        });
      }
      if (!metadata.fieldNames.has(field)) {
        return InvalidQueryError.make({
          topic,
          message: `Raw query orderBy contains unknown field: ${field}.`,
        });
      }
      const direction = entry["direction"];
      if (direction !== "asc" && direction !== "desc") {
        return InvalidQueryError.make({
          topic,
          message: "Raw query orderBy direction must be asc or desc.",
        });
      }
      clonedOrderBy.push({
        field,
        direction,
      });
    }
  }
  if (clonedOrderBy.length > 0) {
    decoded.orderBy = clonedOrderBy;
  }

  return Effect.succeed(decoded);
});

const validateRuntimeQuery = Effect.fn("ColumnLiveViewEngine.rawQuery.validate")(function* (
  topic: string,
  metadata: RawQueryCompilerMetadata,
  query: RuntimeRawQuery,
) {
  if (query.where === undefined) {
    return;
  }

  for (const [field, filter] of Object.entries(query.where)) {
    if (!isPlainRecord(filter) || isBigDecimal(filter)) {
      continue;
    }
    const keys = Object.keys(filter);
    const operatorKeyCount = keys.filter((key) => filterOperatorKeys.has(key)).length;
    if (metadata.structuredObjectFieldNames.has(field)) {
      continue;
    }
    if (operatorKeyCount > 0 && operatorKeyCount !== keys.length) {
      return yield* InvalidQueryError.make({
        topic,
        message: `Raw query where field ${field} contains unsupported filter operator.`,
      });
    }
    if (operatorKeyCount > 0) {
      if (keys.includes("startsWith") && !metadata.stringFieldNames.has(field)) {
        return yield* InvalidQueryError.make({
          topic,
          message: `Raw query where field ${field} does not support startsWith.`,
        });
      }
      if (
        keys.some((key) => rangeFilterOperatorKeys.has(key)) &&
        !metadata.numericFieldNames.has(field)
      ) {
        return yield* InvalidQueryError.make({
          topic,
          message: `Raw query where field ${field} does not support range operators.`,
        });
      }
    }
    if (operatorKeyCount === 0) {
      return yield* InvalidQueryError.make({
        topic,
        message: `Raw query where field ${field} contains unsupported filter operator.`,
      });
    }
  }
});

type StableQueryObjectEntry = readonly [string, StableQueryValueToken];

type StableQueryValueToken =
  | readonly ["null"]
  | readonly ["undefined"]
  | readonly ["boolean", boolean]
  | readonly ["number", string]
  | readonly ["string", string]
  | readonly ["bigint", string]
  | readonly ["bigDecimal", string]
  | readonly ["unsupported", string]
  | readonly ["cycle"]
  | readonly ["array", ReadonlyArray<StableQueryValueToken>]
  | readonly ["map", ReadonlyArray<readonly [StableQueryValueToken, StableQueryValueToken]>]
  | readonly ["object", ReadonlyArray<StableQueryObjectEntry>]
  | readonly ["set", ReadonlyArray<StableQueryValueToken>]
  | readonly ["nonPlainObject", string];

const compareStableQueryValueToken = (
  left: StableQueryValueToken,
  right: StableQueryValueToken,
): number => {
  const leftString = JSON.stringify(left);
  const rightString = JSON.stringify(right);
  return Number(leftString > rightString) - Number(leftString < rightString);
};

const stableQueryValueToken = (value: unknown, active: WeakSet<object>): StableQueryValueToken => {
  if (isBigDecimal(value)) {
    return ["bigDecimal", formatBigDecimal(normalize(value))];
  }
  if (value === null) {
    return ["null"];
  }
  if (typeof value === "bigint") {
    return ["bigint", value.toString()];
  }
  if (typeof value === "symbol") {
    return ["unsupported", `symbol:${value.description ?? ""}`];
  }
  if (typeof value === "function") {
    return ["unsupported", `function:${value.name}`];
  }
  if (Array.isArray(value)) {
    if (active.has(value)) {
      return ["cycle"];
    }
    active.add(value);
    const token: StableQueryValueToken = [
      "array",
      value.map((entry) => stableQueryValueToken(entry, active)),
    ];
    active.delete(value);
    return token;
  }
  if (value instanceof Map) {
    if (active.has(value)) {
      return ["cycle"];
    }
    active.add(value);
    const entries = Array.from(
      value.entries(),
      ([key, entryValue]) =>
        [stableQueryValueToken(key, active), stableQueryValueToken(entryValue, active)] as const,
    ).toSorted((left, right) => {
      const keyComparison = compareStableQueryValueToken(left[0], right[0]);
      return keyComparison === 0 ? compareStableQueryValueToken(left[1], right[1]) : keyComparison;
    });
    active.delete(value);
    return ["map", entries];
  }
  if (value instanceof Set) {
    if (active.has(value)) {
      return ["cycle"];
    }
    active.add(value);
    const values = Array.from(value.values(), (entry) =>
      stableQueryValueToken(entry, active),
    ).toSorted(compareStableQueryValueToken);
    active.delete(value);
    return ["set", values];
  }
  if (isPlainRecord(value)) {
    if (active.has(value)) {
      return ["cycle"];
    }
    active.add(value);
    const entries: Array<StableQueryObjectEntry> = Object.keys(value)
      .toSorted()
      .map((key) => [key, stableQueryValueToken(value[key], active)]);
    active.delete(value);
    return ["object", entries];
  }
  if (typeof value === "object" && value !== null) {
    return ["nonPlainObject", Object.prototype.toString.call(value)];
  }
  if (typeof value === "number") {
    return ["number", Object.is(value, -0) ? "-0" : String(value)];
  }
  if (typeof value === "string") {
    return ["string", value];
  }
  if (typeof value === "boolean") {
    return ["boolean", value];
  }
  return ["undefined"];
};

export const stableQueryValueString = (value: unknown): string =>
  JSON.stringify(stableQueryValueToken(value, new WeakSet()));

const valueRank = (value: unknown): number => {
  if (value == null) {
    return 0;
  }
  if (typeof value === "boolean") {
    return 1;
  }
  if (typeof value === "number" || typeof value === "bigint" || isBigDecimal(value)) {
    return 2;
  }
  if (typeof value === "string") {
    return 3;
  }
  if (Array.isArray(value)) {
    return 4;
  }
  return 5;
};

const compareFilterValue = (left: unknown, right: unknown): number | undefined => {
  if (typeof left === "number" && typeof right === "number") {
    if (!Number.isFinite(left) || !Number.isFinite(right)) {
      return undefined;
    }
    return left === right ? 0 : left < right ? -1 : 1;
  }
  if (typeof left === "string" && typeof right === "string") {
    return left === right ? 0 : left < right ? -1 : 1;
  }
  if (typeof left === "bigint" && typeof right === "bigint") {
    return left === right ? 0 : left < right ? -1 : 1;
  }
  if (isBigDecimal(left) && isBigDecimal(right)) {
    return Order(left, right);
  }
  return undefined;
};

const compareByStableString = (left: unknown, right: unknown): number => {
  const leftString = stableQueryValueString(left);
  const rightString = stableQueryValueString(right);
  return Number(leftString > rightString) - Number(leftString < rightString);
};

export const compareQueryValue = (left: unknown, right: unknown): number | undefined => {
  const leftRank = valueRank(left);
  const rightRank = valueRank(right);
  if (leftRank !== rightRank) {
    return Number(leftRank > rightRank) - Number(leftRank < rightRank);
  }
  if (typeof left === "boolean" && typeof right === "boolean") {
    return left === right ? 0 : left ? 1 : -1;
  }
  const filterComparison = compareFilterValue(left, right);
  if (filterComparison !== undefined) {
    return filterComparison;
  }
  return compareByStableString(left, right);
};

const isEqualityComparable = (left: unknown, right: unknown): boolean => {
  if (isBigDecimal(left) || isBigDecimal(right)) {
    return isBigDecimal(left) && isBigDecimal(right);
  }
  if (typeof left === "number" || typeof right === "number") {
    return typeof left === "number" && typeof right === "number" && Number.isFinite(right);
  }
  return typeof left === typeof right;
};

const isOperatorFilterObject = (filter: Record<string, unknown>): filter is FilterObject => {
  const keys = Object.keys(filter);
  return keys.length > 0 && keys.every((key) => filterOperatorKeys.has(key));
};

const includesValue = (values: ReadonlyArray<unknown>, value: unknown): boolean => {
  for (const candidate of values) {
    if (valuesEqual(value, candidate)) {
      return true;
    }
  }
  return false;
};

const matchesFilter = (value: unknown, filter: unknown): boolean => {
  if (filter === undefined) {
    return false;
  }
  if (!isPlainRecord(filter) || isBigDecimal(filter)) {
    return valuesEqual(value, filter);
  }
  const valueIsStructured = (isPlainRecord(value) && !isBigDecimal(value)) || Array.isArray(value);
  if (valueIsStructured) {
    if (valuesEqual(value, filter)) {
      return true;
    }
    const oneOf = filter["in"];
    if (oneOf !== undefined) {
      if (
        !Array.isArray(oneOf) ||
        !isDenseArray(oneOf) ||
        oneOf.some((candidate) => candidate === undefined)
      ) {
        return false;
      }
      if (!includesValue(oneOf, value)) {
        return false;
      }
    }
    const eq = filter["eq"];
    if (eq !== undefined && !valuesEqual(value, eq)) {
      return false;
    }
    const notEqual = filter["neq"];
    if (notEqual !== undefined && valuesEqual(value, notEqual)) {
      return false;
    }
    return eq !== undefined || oneOf !== undefined || notEqual !== undefined;
  }
  if (!isOperatorFilterObject(filter)) {
    return valuesEqual(value, filter);
  }

  if (
    ("eq" in filter && filter.eq === undefined) ||
    ("neq" in filter && filter.neq === undefined) ||
    ("in" in filter && filter.in === undefined) ||
    ("gt" in filter && filter.gt === undefined) ||
    ("gte" in filter && filter.gte === undefined) ||
    ("lt" in filter && filter.lt === undefined) ||
    ("lte" in filter && filter.lte === undefined) ||
    ("startsWith" in filter && filter.startsWith === undefined)
  ) {
    return false;
  }

  if (filter.eq !== undefined && !valuesEqual(value, filter.eq)) {
    return false;
  }
  if (filter.neq !== undefined) {
    if (!isEqualityComparable(value, filter.neq) || valuesEqual(value, filter.neq)) {
      return false;
    }
  }
  if (filter.in !== undefined) {
    if (
      !Array.isArray(filter.in) ||
      !isDenseArray(filter.in) ||
      filter.in.some((candidate) => candidate === undefined) ||
      !includesValue(filter.in, value)
    ) {
      return false;
    }
  }
  if (filter.startsWith !== undefined) {
    if (
      typeof filter.startsWith !== "string" ||
      typeof value !== "string" ||
      !value.startsWith(filter.startsWith)
    ) {
      return false;
    }
  }

  if (filter.gt !== undefined) {
    const comparison = compareFilterValue(value, filter.gt);
    if (comparison === undefined || comparison <= 0) {
      return false;
    }
  }
  if (filter.gte !== undefined) {
    const comparison = compareFilterValue(value, filter.gte);
    if (comparison === undefined || comparison < 0) {
      return false;
    }
  }
  if (filter.lt !== undefined) {
    const comparison = compareFilterValue(value, filter.lt);
    if (comparison === undefined || comparison >= 0) {
      return false;
    }
  }
  if (filter.lte !== undefined) {
    const comparison = compareFilterValue(value, filter.lte);
    if (comparison === undefined || comparison > 0) {
      return false;
    }
  }

  return true;
};

const compileMatches = <Row extends RowObject>(
  where: RuntimeRawQuery["where"],
): ((row: Row) => boolean) => {
  if (where === undefined) {
    return () => true;
  }

  const filters = Object.entries(where);
  return (row) => {
    for (const [field, filter] of filters) {
      if (!matchesFilter(fieldValue(row, field), filter)) {
        return false;
      }
    }
    return true;
  };
};

const compareRows = <Row extends RowObject>(
  left: StoredRowOf<Row>,
  right: StoredRowOf<Row>,
  orderBy: ReadonlyArray<OrderBy<Record<string, unknown>>>,
): number => {
  for (const order of orderBy) {
    const comparison = compareQueryValue(
      fieldValue(left.row, order.field),
      fieldValue(right.row, order.field),
    );
    if (comparison !== undefined && comparison !== 0) {
      return order.direction === "asc" ? comparison : -comparison;
    }
  }
  return Number(left.key > right.key) - Number(left.key < right.key);
};

const projectRow = (
  row: RowObject,
  select: ReadonlyArray<FieldKey<Record<string, unknown>>>,
): RowObject => {
  const projected: Record<string, unknown> = {};
  for (const field of select) {
    projected[field] = cloneUnknown(fieldValue(row, field));
  }
  return projected;
};

function projectCompiledRow<ResultRow extends RowObject>(
  row: RowObject,
  select: ReadonlyArray<FieldKey<Record<string, unknown>>>,
): ResultRow;
function projectCompiledRow(
  row: RowObject,
  select: ReadonlyArray<FieldKey<Record<string, unknown>>>,
): RowObject {
  return projectRow(row, select);
}

const compileProjection = <Row extends RowObject, ResultRow extends RowObject>(
  select: ReadonlyArray<string>,
): ((row: Row) => ResultRow) => {
  const selectedFields = [...select];
  return (row) => projectCompiledRow(row, selectedFields);
};

const compileRawQuery = <Row extends RowObject, ResultRow extends RowObject>(
  query: RuntimeRawQuery,
): CompiledRawQuery<Row, ResultRow> => {
  const orderBy = query.orderBy ?? [];
  return {
    [compiledRawQueryBrand]: true,
    query,
    matches: compileMatches(query.where),
    compare: (left, right) => compareRows(left, right, orderBy),
    project: compileProjection(query.select),
    offset: query.offset ?? 0,
    limit: query.limit,
  };
};

export const prepareRawQuery = Effect.fn("ColumnLiveViewEngine.rawQuery.prepare")(function* <
  Row extends RowObject,
  ResultRow extends RowObject,
>(topic: string, metadata: RawQueryCompilerMetadata, query: unknown) {
  const decoded = yield* decodeRawQuery(topic, metadata, query);
  yield* validateRuntimeQuery(topic, metadata, decoded);
  return compileRawQuery<Row, ResultRow>(decoded);
});
