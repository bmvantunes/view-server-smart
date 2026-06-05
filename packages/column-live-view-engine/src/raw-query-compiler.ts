import type { FieldKey, OrderBy } from "@view-server/config";
import { Effect, Schema } from "effect";
import { isBigDecimal } from "effect/BigDecimal";
import { compareFilterValue, compareQueryValue, stableQueryValueString } from "./query-value";
import {
  rawQueryCompilerMetadata,
  type RangeValueKind,
  type RawQueryCompilerMetadata,
} from "./raw-query-metadata";
import {
  cloneRecord,
  cloneUnknown,
  fieldValue,
  isPlainRecord,
  scalarEqualityKey,
  type ScalarEqualityKeyValue,
  valuesEqual,
} from "./row-values";
import type { TopicRawOrderByPlan, TopicRawPredicatePlan, TopicRowEntry } from "./row-scan";

type RowObject = object;
const compiledRawQueryBrand: unique symbol = Symbol("CompiledRawQuery");

export { rawQueryCompilerMetadata };
export { compareQueryValue, stableQueryValueString };
export type { RawQueryCompilerMetadata };

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

export type CompiledRawQuery<Row extends RowObject, ResultRow extends RowObject> = {
  readonly [compiledRawQueryBrand]: true;
  readonly query: RuntimeRawQuery;
  readonly predicate: CompiledRawPredicate<Row>;
  readonly ordering: CompiledRawOrdering<Row>;
  readonly projection: CompiledRawProjection<Row, ResultRow>;
  readonly window: CompiledRawWindow;
};

export type CompiledRawPredicate<Row extends RowObject> = {
  readonly plan: TopicRawPredicatePlan;
  readonly matches: (row: Row) => boolean;
};

export type CompiledRawOrdering<Row extends RowObject> = {
  readonly plan: ReadonlyArray<TopicRawOrderByPlan>;
  readonly compare: (left: TopicRowEntry<Row>, right: TopicRowEntry<Row>) => number;
};

export type CompiledRawProjection<Row extends RowObject, ResultRow extends RowObject> = {
  readonly project: (row: Row) => ResultRow;
};

export type CompiledRawWindow = {
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

const rangeValueKind = (value: unknown): RangeValueKind | undefined => {
  if (typeof value === "number" && Number.isFinite(value)) {
    return "number";
  }
  if (typeof value === "bigint") {
    return "bigint";
  }
  if (isBigDecimal(value)) {
    return "bigDecimal";
  }
  return undefined;
};

const isScalarPlanValue = (value: unknown): value is ScalarEqualityKeyValue =>
  value === null ||
  typeof value === "string" ||
  typeof value === "boolean" ||
  typeof value === "bigint" ||
  isBigDecimal(value) ||
  (typeof value === "number" && Number.isFinite(value));

export const isRangePlanValue = (
  field: string,
  value: unknown,
  metadata: RawQueryCompilerMetadata,
): boolean => {
  const kind = rangeValueKind(value);
  const fieldKinds = metadata.rangeValueKinds.get(field);
  return kind !== undefined && fieldKinds?.size === 1 && fieldKinds.has(kind);
};

const isEqualityPlanValue = (value: unknown): value is ScalarEqualityKeyValue =>
  isScalarPlanValue(value);

const isNotEqualPlanValue = (
  field: string,
  value: unknown,
  metadata: RawQueryCompilerMetadata,
): boolean => {
  if (!isEqualityPlanValue(value)) {
    return false;
  }
  if (metadata.numericFieldNames.has(field)) {
    return isRangePlanValue(field, value, metadata);
  }
  if (metadata.stringFieldNames.has(field)) {
    return typeof value === "string";
  }
  return false;
};

const isInPlanValues = (value: unknown): value is ReadonlyArray<ScalarEqualityKeyValue> =>
  Array.isArray(value) &&
  isDenseArray(value) &&
  value.every((candidate) => isEqualityPlanValue(candidate));

const scalarEqualityKeys = (values: ReadonlyArray<ScalarEqualityKeyValue>): ReadonlySet<string> => {
  const keys = new Set<string>();
  for (const value of values) {
    keys.add(scalarEqualityKey(value));
  }
  return keys;
};

type PredicateFieldPlan = {
  readonly filters: TopicRawPredicatePlan["filters"];
  readonly callbackRequired: boolean;
};

type CompiledRawPredicateClause = {
  readonly field: string;
  readonly matches: (value: unknown) => boolean;
};

type CompiledRawPredicateParts = {
  readonly clauses: ReadonlyArray<CompiledRawPredicateClause>;
  readonly plan: TopicRawPredicatePlan;
};

const predicateFilterPlans = (
  field: string,
  filter: unknown,
  metadata: RawQueryCompilerMetadata,
): PredicateFieldPlan => {
  if (metadata.structuredFieldNames.has(field) || filter === undefined) {
    return {
      filters: [],
      callbackRequired: true,
    };
  }
  if (!isPlainRecord(filter) || isBigDecimal(filter)) {
    if (!isScalarPlanValue(filter)) {
      return {
        filters: [],
        callbackRequired: true,
      };
    }
    return {
      filters: [
        {
          field,
          operator: "eq",
          value: filter,
        },
      ],
      callbackRequired: false,
    };
  }

  const operatorKeys = Object.keys(filter).filter((key) => filterOperatorKeys.has(key));
  let callbackRequired = operatorKeys.length === 0;
  const plans: Array<TopicRawPredicatePlan["filters"][number]> = [];
  if ("eq" in filter) {
    if (isEqualityPlanValue(filter["eq"])) {
      plans.push({
        field,
        operator: "eq",
        value: filter["eq"],
      });
    } else {
      callbackRequired = true;
    }
  }
  if ("neq" in filter) {
    if (isNotEqualPlanValue(field, filter["neq"], metadata)) {
      plans.push({
        field,
        operator: "neq",
        value: filter["neq"],
      });
    } else {
      callbackRequired = true;
    }
  }
  if ("in" in filter) {
    if (isInPlanValues(filter["in"])) {
      const values = [...filter["in"]];
      plans.push({
        field,
        operator: "in",
        values,
        valueKeys: scalarEqualityKeys(values),
      });
    } else {
      callbackRequired = true;
    }
  }
  if ("gt" in filter) {
    if (isRangePlanValue(field, filter["gt"], metadata)) {
      plans.push({
        field,
        operator: "gt",
        value: filter["gt"],
      });
    } else {
      callbackRequired = true;
    }
  }
  if ("gte" in filter) {
    if (isRangePlanValue(field, filter["gte"], metadata)) {
      plans.push({
        field,
        operator: "gte",
        value: filter["gte"],
      });
    } else {
      callbackRequired = true;
    }
  }
  if ("lt" in filter) {
    if (isRangePlanValue(field, filter["lt"], metadata)) {
      plans.push({
        field,
        operator: "lt",
        value: filter["lt"],
      });
    } else {
      callbackRequired = true;
    }
  }
  if ("lte" in filter) {
    if (isRangePlanValue(field, filter["lte"], metadata)) {
      plans.push({
        field,
        operator: "lte",
        value: filter["lte"],
      });
    } else {
      callbackRequired = true;
    }
  }
  if ("startsWith" in filter) {
    if (typeof filter["startsWith"] === "string") {
      plans.push({
        field,
        operator: "startsWith",
        value: filter["startsWith"],
      });
    } else {
      callbackRequired = true;
    }
  }
  return {
    filters: plans,
    callbackRequired,
  };
};

const isStructuredQueryValue = (value: unknown): boolean =>
  (isPlainRecord(value) && !isBigDecimal(value)) || Array.isArray(value);

const compileStructuredFilterMatcher = (
  filter: Readonly<Record<string, unknown>>,
): ((value: unknown) => boolean) => {
  const oneOf = filter["in"];
  const oneOfMatcher =
    oneOf === undefined
      ? undefined
      : Array.isArray(oneOf) &&
          isDenseArray(oneOf) &&
          !oneOf.some((candidate) => candidate === undefined)
        ? (value: unknown) => includesValue(oneOf, value)
        : () => false;
  const eq = filter["eq"];
  const neq = filter["neq"];

  return (value) => {
    if (valuesEqual(value, filter)) {
      return true;
    }
    if (oneOfMatcher !== undefined && !oneOfMatcher(value)) {
      return false;
    }
    if (eq !== undefined && !valuesEqual(value, eq)) {
      return false;
    }
    if (neq !== undefined && valuesEqual(value, neq)) {
      return false;
    }
    return eq !== undefined || oneOfMatcher !== undefined || neq !== undefined;
  };
};

const compileScalarOperatorFilterMatcher = (
  filter: Readonly<Record<string, unknown>>,
): ((value: unknown) => boolean) => {
  if (
    ("eq" in filter && filter["eq"] === undefined) ||
    ("neq" in filter && filter["neq"] === undefined) ||
    ("in" in filter && filter["in"] === undefined) ||
    ("gt" in filter && filter["gt"] === undefined) ||
    ("gte" in filter && filter["gte"] === undefined) ||
    ("lt" in filter && filter["lt"] === undefined) ||
    ("lte" in filter && filter["lte"] === undefined) ||
    ("startsWith" in filter && filter["startsWith"] === undefined)
  ) {
    return () => false;
  }

  const eq = filter["eq"];
  const neq = filter["neq"];
  const oneOf = filter["in"];
  const startsWith = filter["startsWith"];
  const gt = filter["gt"];
  const gte = filter["gte"];
  const lt = filter["lt"];
  const lte = filter["lte"];
  const oneOfMatcher =
    oneOf === undefined
      ? undefined
      : Array.isArray(oneOf) &&
          isDenseArray(oneOf) &&
          !oneOf.some((candidate) => candidate === undefined)
        ? (value: unknown) => includesValue(oneOf, value)
        : () => false;

  return (value) => {
    if (eq !== undefined && !valuesEqual(value, eq)) {
      return false;
    }
    if (neq !== undefined) {
      if (!isEqualityComparable(value, neq) || valuesEqual(value, neq)) {
        return false;
      }
    }
    if (oneOfMatcher !== undefined && !oneOfMatcher(value)) {
      return false;
    }
    if (startsWith !== undefined) {
      if (
        typeof startsWith !== "string" ||
        typeof value !== "string" ||
        !value.startsWith(startsWith)
      ) {
        return false;
      }
    }

    if (gt !== undefined) {
      const comparison = compareFilterValue(value, gt);
      if (comparison === undefined || comparison <= 0) {
        return false;
      }
    }
    if (gte !== undefined) {
      const comparison = compareFilterValue(value, gte);
      if (comparison === undefined || comparison < 0) {
        return false;
      }
    }
    if (lt !== undefined) {
      const comparison = compareFilterValue(value, lt);
      if (comparison === undefined || comparison >= 0) {
        return false;
      }
    }
    if (lte !== undefined) {
      const comparison = compareFilterValue(value, lte);
      if (comparison === undefined || comparison > 0) {
        return false;
      }
    }

    return true;
  };
};

const compileFilterMatcher = (filter: unknown): ((value: unknown) => boolean) => {
  if (filter === undefined) {
    return () => false;
  }
  if (!isPlainRecord(filter) || isBigDecimal(filter)) {
    return (value) => valuesEqual(value, filter);
  }

  const structuredMatcher = compileStructuredFilterMatcher(filter);
  if (!isOperatorFilterObject(filter)) {
    return (value) =>
      isStructuredQueryValue(value) ? structuredMatcher(value) : valuesEqual(value, filter);
  }

  const scalarMatcher = compileScalarOperatorFilterMatcher(filter);
  return (value) =>
    isStructuredQueryValue(value) ? structuredMatcher(value) : scalarMatcher(value);
};

const compilePredicateParts = (
  metadata: RawQueryCompilerMetadata,
  where: RuntimeRawQuery["where"],
): CompiledRawPredicateParts => {
  if (where === undefined) {
    return {
      clauses: [],
      plan: {
        filters: [],
        callbackRequired: false,
        callbackSkippable: true,
      },
    };
  }

  const filters: Array<TopicRawPredicatePlan["filters"][number]> = [];
  const clauses: Array<CompiledRawPredicateClause> = [];
  let callbackRequired = false;
  for (const [field, filter] of Object.entries(where)) {
    const fieldPlan = predicateFilterPlans(field, filter, metadata);
    filters.push(...fieldPlan.filters);
    callbackRequired ||= fieldPlan.callbackRequired;
    clauses.push({
      field,
      matches: compileFilterMatcher(filter),
    });
  }
  return {
    clauses,
    plan: {
      filters,
      callbackRequired,
      callbackSkippable: !callbackRequired,
    },
  };
};

const compileMatches = <Row extends RowObject>(
  metadata: RawQueryCompilerMetadata,
  where: RuntimeRawQuery["where"],
): CompiledRawPredicate<Row> => {
  const parts = compilePredicateParts(metadata, where);
  if (parts.clauses.length === 0) {
    return {
      plan: parts.plan,
      matches: () => true,
    };
  }

  return {
    plan: parts.plan,
    matches: (row) => {
      for (const clause of parts.clauses) {
        if (!clause.matches(fieldValue(row, clause.field))) {
          return false;
        }
      }
      return true;
    },
  };
};

const compareRows = <Row extends RowObject>(
  left: TopicRowEntry<Row>,
  right: TopicRowEntry<Row>,
  orderBy: ReadonlyArray<OrderBy<Record<string, unknown>>>,
): number => {
  for (const order of orderBy) {
    const comparison = compareQueryValue(
      fieldValue(left.row, order.field),
      fieldValue(right.row, order.field),
    );
    if (comparison !== 0) {
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
): CompiledRawProjection<Row, ResultRow> => {
  const selectedFields = [...select];
  return {
    project: (row) => projectCompiledRow(row, selectedFields),
  };
};

const compileOrdering = <Row extends RowObject>(
  orderBy: ReadonlyArray<OrderBy<Record<string, unknown>>>,
): CompiledRawOrdering<Row> => ({
  plan: [...orderBy],
  compare: (left, right) => compareRows(left, right, orderBy),
});

const compileWindow = (query: RuntimeRawQuery): CompiledRawWindow => ({
  offset: query.offset ?? 0,
  limit: query.limit,
});

const compileRawQueryParts = <Row extends RowObject, ResultRow extends RowObject>(
  metadata: RawQueryCompilerMetadata,
  query: RuntimeRawQuery,
): Pick<CompiledRawQuery<Row, ResultRow>, "predicate" | "ordering" | "projection" | "window"> => {
  const orderBy = query.orderBy ?? [];
  return {
    predicate: compileMatches(metadata, query.where),
    ordering: compileOrdering(orderBy),
    projection: compileProjection(query.select),
    window: compileWindow(query),
  };
};

const compileRawQuery = <Row extends RowObject, ResultRow extends RowObject>(
  metadata: RawQueryCompilerMetadata,
  query: RuntimeRawQuery,
): CompiledRawQuery<Row, ResultRow> => {
  const parts = compileRawQueryParts<Row, ResultRow>(metadata, query);
  return {
    [compiledRawQueryBrand]: true,
    query,
    predicate: parts.predicate,
    ordering: parts.ordering,
    projection: parts.projection,
    window: parts.window,
  };
};

export const prepareRawQuery = Effect.fn("ColumnLiveViewEngine.rawQuery.prepare")(function* <
  Row extends RowObject,
  ResultRow extends RowObject,
>(topic: string, metadata: RawQueryCompilerMetadata, query: unknown) {
  const decoded = yield* decodeRawQuery(topic, metadata, query);
  yield* validateRuntimeQuery(topic, metadata, decoded);
  return compileRawQuery<Row, ResultRow>(metadata, decoded);
});
