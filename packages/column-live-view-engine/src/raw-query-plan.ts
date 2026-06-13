import { isBigDecimal, Order as orderBigDecimal } from "effect/BigDecimal";
import { compareQueryValue, stableQueryValueString } from "./query-value";
import { compileRawPredicate, type CompiledRawPredicate } from "./raw-predicate-compiler";
import type { RuntimeRawQuery } from "./raw-query-decoder";
import type { RawQueryCompilerMetadata } from "./raw-query-metadata";
import type { TopicRawOrderByPlan, TopicRawWindowScanPlan } from "./raw-window-scan";
import type { TopicRowEntry } from "./row-scan";
import { cloneUnknown, fieldValue } from "./row-values";

type RowObject = object;

type QueryCacheToken = readonly ["raw", string, string];
type QueryWindowCacheToken = readonly ["window", string, string];

export type RawQueryPlanWindow = {
  readonly cacheKey: string;
  readonly offset: number;
  readonly limit: number | undefined;
};

export type RawQueryPlan<Row extends RowObject, ResultRow extends RowObject> = {
  readonly queryCacheKey: string;
  readonly selectedFields: ReadonlyArray<string>;
  readonly predicate: CompiledRawPredicate<Row>;
  readonly orderBy: ReadonlyArray<TopicRawOrderByPlan>;
  readonly storageOrderBy: ReadonlyArray<TopicRawOrderByPlan>;
  readonly compare: (left: TopicRowEntry<Row>, right: TopicRowEntry<Row>) => number;
  readonly project: (row: Row) => ResultRow;
  readonly window: RawQueryPlanWindow;
};

type RawRowOrderColumn<Row extends RowObject> = {
  readonly compareRows: (left: Row, right: Row) => number;
  readonly direction: "asc" | "desc";
};

const rawQueryShapeCacheKey = (query: RuntimeRawQuery): string => {
  const orderBy: ReadonlyArray<readonly [string, "asc" | "desc"]> =
    query.orderBy === undefined ? [] : query.orderBy.map((entry) => [entry.field, entry.direction]);
  const token: QueryCacheToken = [
    "raw",
    query.where === undefined ? "" : stableQueryValueString(query.where),
    stableQueryValueString(orderBy),
  ];
  return JSON.stringify(token);
};

const rawQueryWindowCacheKey = (offset: number, limit: number | undefined): string => {
  const token: QueryWindowCacheToken = [
    "window",
    stableQueryValueString(offset),
    stableQueryValueString(limit ?? null),
  ];
  return JSON.stringify(token);
};

export const rawQueryPlanWindow = (
  offset: number,
  limit: number | undefined,
): RawQueryPlanWindow => ({
  cacheKey: rawQueryWindowCacheKey(offset, limit),
  offset,
  limit,
});

const rawQueryPlanWindowFromQuery = (query: RuntimeRawQuery): RawQueryPlanWindow =>
  rawQueryPlanWindow(query.offset ?? 0, query.limit);

const compareRowFieldValues = <Row extends RowObject>(
  left: Row,
  right: Row,
  field: string,
): number => compareQueryValue(fieldValue(left, field), fieldValue(right, field));

const compareStringRowFieldValues = <Row extends RowObject>(
  left: Row,
  right: Row,
  field: string,
): number => {
  const leftValue = fieldValue(left, field);
  const rightValue = fieldValue(right, field);
  if (typeof leftValue === "string" && typeof rightValue === "string") {
    return Number(leftValue > rightValue) - Number(leftValue < rightValue);
  }
  return compareQueryValue(leftValue, rightValue);
};

const compareNumberRowFieldValues = <Row extends RowObject>(
  left: Row,
  right: Row,
  field: string,
): number => {
  const leftValue = fieldValue(left, field);
  const rightValue = fieldValue(right, field);
  if (
    typeof leftValue === "number" &&
    typeof rightValue === "number" &&
    Number.isFinite(leftValue) &&
    Number.isFinite(rightValue)
  ) {
    return leftValue === rightValue ? 0 : leftValue < rightValue ? -1 : 1;
  }
  return compareQueryValue(leftValue, rightValue);
};

const compareBigintRowFieldValues = <Row extends RowObject>(
  left: Row,
  right: Row,
  field: string,
): number => {
  const leftValue = fieldValue(left, field);
  const rightValue = fieldValue(right, field);
  if (typeof leftValue === "bigint" && typeof rightValue === "bigint") {
    return leftValue === rightValue ? 0 : leftValue < rightValue ? -1 : 1;
  }
  return compareQueryValue(leftValue, rightValue);
};

const compareBigDecimalRowFieldValues = <Row extends RowObject>(
  left: Row,
  right: Row,
  field: string,
): number => {
  const leftValue = fieldValue(left, field);
  const rightValue = fieldValue(right, field);
  if (isBigDecimal(leftValue) && isBigDecimal(rightValue)) {
    return orderBigDecimal(leftValue, rightValue);
  }
  return compareQueryValue(leftValue, rightValue);
};

const rawRowOrderColumnComparator = <Row extends RowObject>(
  metadata: RawQueryCompilerMetadata,
  field: string,
): ((left: Row, right: Row) => number) => {
  if (metadata.stringFieldNames.has(field)) {
    return (left, right) => compareStringRowFieldValues(left, right, field);
  }
  if (metadata.numberFieldNames.has(field)) {
    return (left, right) => compareNumberRowFieldValues(left, right, field);
  }
  if (metadata.bigintFieldNames.has(field)) {
    return (left, right) => compareBigintRowFieldValues(left, right, field);
  }
  if (metadata.bigDecimalFieldNames.has(field)) {
    return (left, right) => compareBigDecimalRowFieldValues(left, right, field);
  }
  return (left, right) => compareRowFieldValues(left, right, field);
};

const compiledRawRowOrder = <Row extends RowObject>(
  metadata: RawQueryCompilerMetadata,
  orderBy: ReadonlyArray<TopicRawOrderByPlan>,
): ReadonlyArray<RawRowOrderColumn<Row>> =>
  orderBy.map((order) => ({
    compareRows: rawRowOrderColumnComparator<Row>(metadata, order.field),
    direction: order.direction,
  }));

const compareRows = <Row extends RowObject>(
  left: TopicRowEntry<Row>,
  right: TopicRowEntry<Row>,
  orderBy: ReadonlyArray<RawRowOrderColumn<Row>>,
): number => {
  for (const order of orderBy) {
    const comparison = order.compareRows(left.row, right.row);
    if (comparison !== 0) {
      return order.direction === "asc" ? comparison : -comparison;
    }
  }
  return Number(left.key > right.key) - Number(left.key < right.key);
};

const projectRow = (row: RowObject, select: ReadonlyArray<string>): RowObject => {
  const projected: Record<string, unknown> = {};
  for (const field of select) {
    projected[field] = cloneUnknown(fieldValue(row, field));
  }
  return projected;
};

function projectCompiledRow<ResultRow extends RowObject>(
  row: RowObject,
  select: ReadonlyArray<string>,
): ResultRow;
function projectCompiledRow(row: RowObject, select: ReadonlyArray<string>): RowObject {
  return projectRow(row, select);
}

export const makeRawQueryPlan = <Row extends RowObject, ResultRow extends RowObject>(
  metadata: RawQueryCompilerMetadata,
  query: RuntimeRawQuery,
): RawQueryPlan<Row, ResultRow> => {
  const orderBy = query.orderBy ?? [];
  const rowOrderBy = compiledRawRowOrder<Row>(metadata, orderBy);
  const selectedFields = [...query.select];
  const predicate = compileRawPredicate<Row>(metadata, query.where);
  return {
    queryCacheKey: rawQueryShapeCacheKey(query),
    selectedFields,
    predicate,
    orderBy,
    storageOrderBy: orderBy,
    compare: (left, right) => compareRows(left, right, rowOrderBy),
    project: (row) => projectCompiledRow(row, selectedFields),
    window: rawQueryPlanWindowFromQuery(query),
  };
};

export const rawQueryWindowScanPlan = <Row extends RowObject, ResultRow extends RowObject>(
  plan: RawQueryPlan<Row, ResultRow>,
  window: RawQueryPlanWindow,
): TopicRawWindowScanPlan<Row> => ({
  predicate: plan.predicate.plan,
  orderBy: plan.orderBy,
  storageOrderBy: plan.storageOrderBy,
  matches: plan.predicate.matches,
  compare: plan.compare,
  offset: window.offset,
  limit: window.limit,
});
