import {
  aggregateStateCompareValue,
  type GroupedAggregatePlan,
  type GroupState,
  type RuntimeGroupedAggregate,
} from "./grouped-aggregate-state";
import { stableQueryValueString } from "./query-value";
import type { StoredRowOf } from "./query-result";
import { trustedFieldValue } from "./row-values";

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

type CompiledGroupedKeyField<Row extends RowObject> = {
  readonly field: string;
  readonly value: (row: Row) => unknown;
};

export type GroupedQueryPlan<Row extends RowObject> = {
  readonly query: GroupedQueryPlanInput;
  readonly cacheKey: string;
  readonly groupBy: ReadonlyArray<string>;
  readonly aggregates: Readonly<Record<string, RuntimeGroupedAggregate>>;
  readonly aggregatePlans: ReadonlyArray<GroupedAggregatePlan>;
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

const stableScalarGroupedKeyValueTokenString = (value: unknown): string | undefined => {
  if (value === null) {
    return `["null"]`;
  }
  if (typeof value === "bigint") {
    return `["bigint",${JSON.stringify(value.toString())}]`;
  }
  if (typeof value === "number") {
    return `["number",${JSON.stringify(Object.is(value, -0) ? "-0" : String(value))}]`;
  }
  if (typeof value === "string") {
    return `["string",${JSON.stringify(value)}]`;
  }
  if (typeof value === "boolean") {
    return `["boolean",${value ? "true" : "false"}]`;
  }
  if (value === undefined) {
    return `["undefined"]`;
  }
  return undefined;
};

const stableScalarGroupedKeyFieldTokenString = (
  field: string,
  value: unknown,
): string | undefined => {
  const valueToken = stableScalarGroupedKeyValueTokenString(value);
  if (valueToken === undefined) {
    return undefined;
  }
  return `["array",[["string",${JSON.stringify(field)}],${valueToken}]]`;
};

const groupedQueryPlanGroupKey = <Row extends RowObject>(
  keyFields: ReadonlyArray<CompiledGroupedKeyField<Row>>,
  row: Row,
): string => {
  const tokens: Array<string> = [];
  const values: Array<readonly [string, unknown]> = [];
  for (const keyField of keyFields) {
    const value = keyField.value(row);
    values.push([keyField.field, value]);
    const token = stableScalarGroupedKeyFieldTokenString(keyField.field, value);
    if (token === undefined) {
      for (const fallbackKeyField of keyFields.slice(values.length)) {
        values.push([fallbackKeyField.field, fallbackKeyField.value(row)]);
      }
      return stableQueryValueString(values);
    }
    tokens.push(token);
  }
  return `["array",[${tokens.join(",")}]]`;
};

const compileGroupedKeyFields = <Row extends RowObject>(
  groupBy: ReadonlyArray<string>,
): ReadonlyArray<CompiledGroupedKeyField<Row>> =>
  groupBy.map((field) => ({
    field,
    value: (row) => trustedFieldValue(row, field),
  }));

const compileGroupedAggregates = (
  aggregates: Readonly<Record<string, RuntimeGroupedAggregate>>,
): ReadonlyArray<GroupedAggregatePlan> =>
  Object.entries(aggregates).map(([alias, aggregate]) => ({
    alias,
    aggregate,
  }));

const groupedFieldOrderColumn = (
  field: string,
  direction: "asc" | "desc",
): CompiledGroupedOrderBy => ({
  direction,
  groupValue: (group) => trustedFieldValue(group.row, field),
  rowValue: (entry) => trustedFieldValue(entry.row, field),
});

const groupedAggregateOrderColumn = (
  aggregate: string,
  direction: "asc" | "desc",
): CompiledGroupedOrderBy => ({
  direction,
  groupValue: (group) => aggregateStateCompareValue(group.aggregates[aggregate]!),
  rowValue: (entry) => trustedFieldValue(entry.row, aggregate),
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
  const keyFields = compileGroupedKeyFields<Row>(groupBy);
  const orderBy = query.orderBy === undefined ? [] : [...query.orderBy];
  return {
    query,
    cacheKey: groupedQueryPlanCacheKey(query),
    groupBy,
    aggregates: query.aggregates,
    aggregatePlans: compileGroupedAggregates(query.aggregates),
    orderBy,
    compiledOrderBy: compileGroupedOrderBy(orderBy),
    offset: query.offset ?? 0,
    limit: query.limit,
    zeroLimit: query.limit === 0,
    groupKey: (row) => groupedQueryPlanGroupKey(keyFields, row),
  };
};
