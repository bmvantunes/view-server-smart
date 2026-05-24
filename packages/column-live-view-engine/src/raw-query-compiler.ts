import type { FieldKey, OrderBy } from "@view-server/config";
import { Effect, Schema } from "effect";
import { isBigDecimal, Order } from "effect/BigDecimal";
import {
  cloneRecord,
  cloneRow,
  cloneUnknown,
  fieldValue,
  isPlainRecord,
  isRecord,
  valuesEqual,
} from "./row-values";

type RowObject = object;

export class InvalidQueryError extends Schema.TaggedErrorClass<InvalidQueryError>()(
  "InvalidQueryError",
  {
    topic: Schema.String,
    message: Schema.String,
  },
) {}

export type RuntimeRawQuery = {
  readonly where?: object;
  readonly orderBy?: ReadonlyArray<{
    readonly field: string;
    readonly direction: "asc" | "desc";
  }>;
  readonly offset?: number;
  readonly limit?: number;
  readonly fields?: ReadonlyArray<string>;
};

type SchemaWithFields = Schema.Decoder<object> & {
  readonly fields: Record<string, unknown>;
};

export type RawQueryCompilerMetadata = {
  readonly fieldNames: ReadonlySet<string>;
  readonly structuredFieldNames: ReadonlySet<string>;
};

export type StoredRowOf<Row extends RowObject> = {
  readonly key: string;
  readonly row: Row;
};

export type QueryEvaluation<ResultRow extends RowObject> = {
  readonly rows: ReadonlyArray<ResultRow>;
  readonly keys: ReadonlyArray<string>;
  readonly window: ReadonlyArray<StoredRowOf<ResultRow>>;
  readonly totalRows: number;
  readonly version: number;
};

export type CompiledRawQuery<Row extends RowObject, ResultRow extends RowObject> = {
  readonly matches: (row: Row) => boolean;
  readonly compare: (left: StoredRowOf<Row>, right: StoredRowOf<Row>) => number;
  readonly project: (row: Row) => ResultRow;
  readonly offset: number;
  readonly limit: number | undefined;
};

export type RawQueryRowStore<Row extends RowObject> = {
  readonly rows: ReadonlyMap<string, Row>;
  readonly version: number;
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

const rawQueryKeys = new Set(["where", "orderBy", "offset", "limit", "fields"]);
const filterOperatorKeys = new Set(["eq", "neq", "in", "gt", "gte", "lt", "lte", "startsWith"]);

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

const isSchemaWithFields = (schema: Schema.Decoder<object>): schema is SchemaWithFields =>
  "fields" in schema && isRecord(schema.fields);

const schemaFieldNames = (schema: Schema.Decoder<object>): ReadonlySet<string> =>
  isSchemaWithFields(schema) ? new Set(Object.keys(schema.fields)) : new Set();

const schemaStructuredFieldNames = (schema: Schema.Decoder<object>): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    const tag = Object(Object(fieldSchema)["ast"])["_tag"];
    if (tag === "Objects" || tag === "Arrays" || tag === "ObjectKeyword") {
      fields.add(field);
    }
  }
  return fields;
};

export const rawQueryCompilerMetadata = (
  schema: Schema.Decoder<object>,
): RawQueryCompilerMetadata => ({
  fieldNames: schemaFieldNames(schema),
  structuredFieldNames: schemaStructuredFieldNames(schema),
});

const decodeRawQuery = (
  topic: string,
  metadata: RawQueryCompilerMetadata,
  query: unknown,
): Effect.Effect<RuntimeRawQuery, InvalidQueryError> => {
  if (query === undefined) {
    return Effect.succeed({});
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
    }
  }

  const orderBy = query["orderBy"];
  if (orderBy !== undefined && !Array.isArray(orderBy)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query orderBy must be an array.",
    });
  }

  const fields = query["fields"];
  if (
    fields !== undefined &&
    (!Array.isArray(fields) || !fields.every((field) => typeof field === "string"))
  ) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query fields must be an array of strings.",
    });
  }
  if (Array.isArray(fields)) {
    for (const field of fields) {
      if (!metadata.fieldNames.has(field)) {
        return InvalidQueryError.make({
          topic,
          message: `Raw query fields contains unknown field: ${field}.`,
        });
      }
    }
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
    where?: object;
    orderBy?: Array<{ readonly field: string; readonly direction: "asc" | "desc" }>;
    offset?: number;
    limit?: number;
    fields?: Array<string>;
  } = {};

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
  if (Array.isArray(fields)) {
    decoded.fields = [...fields];
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
};

const validateRuntimeQuery = (
  topic: string,
  metadata: RawQueryCompilerMetadata,
  query: RuntimeRawQuery,
): Effect.Effect<void, InvalidQueryError> =>
  Effect.gen(function* () {
    if (query.where === undefined) {
      return;
    }

    for (const [field, filter] of Object.entries(query.where)) {
      if (!isPlainRecord(filter) || isBigDecimal(filter)) {
        continue;
      }
      const keys = Object.keys(filter);
      const operatorKeyCount = keys.filter((key) => filterOperatorKeys.has(key)).length;
      if (operatorKeyCount > 0 && operatorKeyCount !== keys.length) {
        return yield* InvalidQueryError.make({
          topic,
          message: `Raw query where field ${field} contains unsupported filter operator.`,
        });
      }
      if (operatorKeyCount === 0 && !metadata.structuredFieldNames.has(field)) {
        return yield* InvalidQueryError.make({
          topic,
          message: `Raw query where field ${field} contains unsupported filter operator.`,
        });
      }
    }
  });

const stableValueString = (value: unknown): string => {
  if (Array.isArray(value)) {
    return `[${value.map(stableValueString).join(",")}]`;
  }
  if (isPlainRecord(value)) {
    return `{${Object.keys(value)
      .toSorted()
      .map((key) => `${key}:${stableValueString(value[key])}`)
      .join(",")}}`;
  }
  return `${typeof value}:${
    JSON.stringify(value, (_key, entry: unknown) =>
      typeof entry === "bigint" ? entry.toString() : entry,
    ) ?? ""
  }`;
};

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
  const leftString = stableValueString(left);
  const rightString = stableValueString(right);
  return Number(leftString > rightString) - Number(leftString < rightString);
};

const compareValue = (left: unknown, right: unknown): number | undefined => {
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
  /* v8 ignore next -- runtime validation rejects scalar object filters before evaluation. */
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
    const comparison = compareValue(
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
  fields: ReadonlyArray<FieldKey<Record<string, unknown>>> | undefined,
): RowObject => {
  if (fields === undefined) {
    return cloneRow(row);
  }

  const projected: Record<string, unknown> = {};
  for (const field of fields) {
    projected[field] = cloneUnknown(fieldValue(row, field));
  }
  return projected;
};

const projectCompiledRow = <ResultRow extends RowObject>(
  row: RowObject,
  fields: ReadonlyArray<FieldKey<Record<string, unknown>>> | undefined,
): ResultRow => projectRow(row, fields) as ResultRow;

const compileProjection = <Row extends RowObject, ResultRow extends RowObject>(
  fields: RuntimeRawQuery["fields"],
): ((row: Row) => ResultRow) => {
  if (fields === undefined) {
    return (row) => projectCompiledRow(row, undefined);
  }

  const selectedFields = [...fields];
  return (row) => projectCompiledRow(row, selectedFields);
};

const compileRawQuery = <Row extends RowObject, ResultRow extends RowObject>(
  query: RuntimeRawQuery,
): CompiledRawQuery<Row, ResultRow> => {
  const orderBy = query.orderBy ?? [];
  return {
    matches: compileMatches(query.where),
    compare: (left, right) => compareRows(left, right, orderBy),
    project: compileProjection(query.fields),
    offset: query.offset ?? 0,
    limit: query.limit,
  };
};

export const prepareRawQuery = <Row extends RowObject, ResultRow extends RowObject>(
  topic: string,
  metadata: RawQueryCompilerMetadata,
  query: unknown,
): Effect.Effect<CompiledRawQuery<Row, ResultRow>, InvalidQueryError> =>
  Effect.gen(function* () {
    const decoded = yield* decodeRawQuery(topic, metadata, query);
    yield* validateRuntimeQuery(topic, metadata, decoded);
    return compileRawQuery<Row, ResultRow>(decoded);
  });

export const evaluateCompiledRawQuery = <Row extends RowObject, ResultRow extends RowObject>(
  store: RawQueryRowStore<Row>,
  compiled: CompiledRawQuery<Row, ResultRow>,
): QueryEvaluation<ResultRow> => {
  const filtered = Array.from(store.rows, ([key, row]) => ({ key, row })).filter((entry) =>
    compiled.matches(entry.row),
  );
  const ordered = filtered.toSorted(compiled.compare);
  const offset = compiled.offset;
  const windowed = ordered.slice(
    offset,
    compiled.limit === undefined ? undefined : offset + compiled.limit,
  );
  const window = windowed.map((entry) => ({
    key: entry.key,
    row: compiled.project(entry.row),
  }));

  return {
    rows: window.map((entry) => entry.row),
    keys: window.map((entry) => entry.key),
    window,
    totalRows: filtered.length,
    version: store.version,
  };
};
