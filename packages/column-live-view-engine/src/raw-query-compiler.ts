import type { FieldKey, OrderBy } from "@view-server/config";
import { Effect } from "effect";
import { compareQueryValue, stableQueryValueString } from "./query-value";
import {
  compileRawPredicate,
  isRangePlanValue,
  type CompiledRawPredicate,
} from "./raw-predicate-compiler";
import {
  decodeRawQuery,
  InvalidQueryError,
  type RuntimeRawQuery,
  validateRuntimeQuery,
} from "./raw-query-decoder";
import { rawQueryCompilerMetadata, type RawQueryCompilerMetadata } from "./raw-query-metadata";
import { cloneUnknown, fieldValue } from "./row-values";
import type { TopicRawOrderByPlan, TopicRowEntry } from "./row-scan";

type RowObject = object;
const compiledRawQueryBrand: unique symbol = Symbol("CompiledRawQuery");

export { rawQueryCompilerMetadata };
export { compareQueryValue, stableQueryValueString };
export { isRangePlanValue };
export { InvalidQueryError };
export type { RawQueryCompilerMetadata, RuntimeRawQuery };

export type CompiledRawQuery<Row extends RowObject, ResultRow extends RowObject> = {
  readonly [compiledRawQueryBrand]: true;
  readonly query: RuntimeRawQuery;
  readonly predicate: CompiledRawPredicate<Row>;
  readonly ordering: CompiledRawOrdering<Row>;
  readonly projection: CompiledRawProjection<Row, ResultRow>;
  readonly window: CompiledRawWindow;
};

export type { CompiledRawPredicate };

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
    predicate: compileRawPredicate(metadata, query.where),
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
