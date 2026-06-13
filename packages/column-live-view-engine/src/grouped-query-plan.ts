import {
  aggregateStateCompareValue,
  type GroupState,
  type RuntimeGroupedAggregate,
} from "./grouped-aggregate-state";
import { stableQueryValueString } from "./query-value";
import type { StoredRowOf } from "./query-result";
import { fieldValue } from "./row-values";

type RowObject = object;

export type RuntimeGroupedOrderBy =
  | {
      readonly field: string;
      readonly direction: "asc" | "desc";
    }
  | {
      readonly aggregate: string;
      readonly direction: "asc" | "desc";
    };

export type GroupedQueryPlanInput = {
  readonly groupBy: ReadonlyArray<string>;
  readonly aggregates: Readonly<Record<string, RuntimeGroupedAggregate>>;
  readonly where?: Record<string, unknown>;
  readonly orderBy?: ReadonlyArray<RuntimeGroupedOrderBy>;
  readonly offset?: number;
  readonly limit?: number;
};

export type CompiledGroupedOrderBy = {
  readonly direction: "asc" | "desc";
  readonly groupValue: (group: GroupState) => unknown;
  readonly rowValue: (entry: StoredRowOf<RowObject>) => unknown;
};

export type GroupedQueryPlan<Row extends RowObject> = {
  readonly query: GroupedQueryPlanInput;
  readonly cacheKey: string;
  readonly groupBy: ReadonlyArray<string>;
  readonly aggregates: Readonly<Record<string, RuntimeGroupedAggregate>>;
  readonly orderBy: ReadonlyArray<RuntimeGroupedOrderBy>;
  readonly compiledOrderBy: ReadonlyArray<CompiledGroupedOrderBy>;
  readonly offset: number;
  readonly limit: number | undefined;
  readonly zeroLimit: boolean;
  readonly groupKey: (row: Row) => string;
};

const groupedQueryPlanCacheKey = (query: GroupedQueryPlanInput): string =>
  stableQueryValueString([
    "grouped",
    query.groupBy,
    Object.entries(query.aggregates).toSorted(
      ([left], [right]) => Number(left > right) - Number(left < right),
    ),
    query.where === undefined ? [] : stableQueryValueString(query.where),
    query.orderBy ?? [],
    query.offset ?? null,
    query.limit ?? null,
  ]);

const groupedQueryPlanGroupKey = <Row extends RowObject>(
  groupBy: ReadonlyArray<string>,
  row: Row,
): string => stableQueryValueString(groupBy.map((field) => [field, fieldValue(row, field)]));

const groupedFieldOrderColumn = (
  field: string,
  direction: "asc" | "desc",
): CompiledGroupedOrderBy => ({
  direction,
  groupValue: (group) => fieldValue(group.row, field),
  rowValue: (entry) => fieldValue(entry.row, field),
});

const groupedAggregateOrderColumn = (
  aggregate: string,
  direction: "asc" | "desc",
): CompiledGroupedOrderBy => ({
  direction,
  groupValue: (group) => aggregateStateCompareValue(group.aggregates[aggregate]!),
  rowValue: (entry) => fieldValue(entry.row, aggregate),
});

const compileGroupedOrderBy = (
  orderBy: ReadonlyArray<RuntimeGroupedOrderBy>,
): ReadonlyArray<CompiledGroupedOrderBy> =>
  orderBy.map((order) =>
    "field" in order
      ? groupedFieldOrderColumn(order.field, order.direction)
      : groupedAggregateOrderColumn(order.aggregate, order.direction),
  );

export const makeGroupedQueryPlan = <Row extends RowObject>(
  query: GroupedQueryPlanInput,
): GroupedQueryPlan<Row> => {
  const groupBy = [...query.groupBy];
  const orderBy = query.orderBy === undefined ? [] : [...query.orderBy];
  return {
    query,
    cacheKey: groupedQueryPlanCacheKey(query),
    groupBy,
    aggregates: query.aggregates,
    orderBy,
    compiledOrderBy: compileGroupedOrderBy(orderBy),
    offset: query.offset ?? 0,
    limit: query.limit,
    zeroLimit: query.limit === 0,
    groupKey: (row) => groupedQueryPlanGroupKey(groupBy, row),
  };
};
