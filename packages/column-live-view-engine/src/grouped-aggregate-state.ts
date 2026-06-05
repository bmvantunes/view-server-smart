import {
  divideUnsafe,
  fromBigInt,
  fromNumberUnsafe,
  isBigDecimal,
  sum as sumBigDecimal,
  type BigDecimal,
} from "effect/BigDecimal";
import { compareQueryValue, stableQueryValueString } from "./raw-query-compiler";
import type { StoredRowOf } from "./query-result";
import { cloneUnknown, fieldValue } from "./row-values";

type RowObject = object;

export type RuntimeGroupedAggregate =
  | {
      readonly aggFunc: "count";
    }
  | {
      readonly aggFunc: "countDistinct" | "min" | "max" | "avg";
      readonly field: string;
    }
  | {
      readonly aggFunc: "sum";
      readonly field: string;
      readonly resultKind: "bigint" | "bigDecimal";
    };

type AggregateState =
  | {
      readonly aggFunc: "count";
      count: bigint;
    }
  | {
      readonly aggFunc: "countDistinct";
      readonly values: Set<string>;
      count: bigint;
    }
  | {
      readonly aggFunc: "sum";
      readonly resultKind: "bigint";
      bigintTotal: bigint;
    }
  | {
      readonly aggFunc: "sum";
      readonly resultKind: "bigDecimal";
      decimalTotal: BigDecimal;
    }
  | {
      readonly aggFunc: "avg";
      count: bigint;
      total: BigDecimal;
    }
  | {
      readonly aggFunc: "min" | "max";
      value: unknown;
      hasValue: boolean;
    };

export type GroupState = {
  readonly key: string;
  readonly row: Record<string, unknown>;
  readonly aggregates: Record<string, AggregateState>;
};

export type MaterializedIncrementalGroupState = GroupState & {
  readonly members: Map<string, RowObject>;
};

const bigintToDecimal = (value: bigint): BigDecimal => fromBigInt(value);

const numberToDecimal = (value: number): BigDecimal => fromNumberUnsafe(value);

const runtimeValueToDecimal = (value: unknown): BigDecimal | undefined => {
  if (isBigDecimal(value)) {
    return value;
  }
  if (typeof value === "bigint") {
    return bigintToDecimal(value);
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return numberToDecimal(value);
  }
  return undefined;
};

const emptyAggregateState = (aggregate: RuntimeGroupedAggregate): AggregateState => {
  if (aggregate.aggFunc === "count") {
    return {
      aggFunc: "count",
      count: 0n,
    };
  }
  if (aggregate.aggFunc === "countDistinct") {
    return {
      aggFunc: "countDistinct",
      values: new Set<string>(),
      count: 0n,
    };
  }
  if (aggregate.aggFunc === "sum") {
    return aggregate.resultKind === "bigint"
      ? {
          aggFunc: "sum",
          resultKind: "bigint",
          bigintTotal: 0n,
        }
      : {
          aggFunc: "sum",
          resultKind: "bigDecimal",
          decimalTotal: fromBigInt(0n),
        };
  }
  if (aggregate.aggFunc === "avg") {
    return {
      aggFunc: "avg",
      count: 0n,
      total: fromBigInt(0n),
    };
  }
  return {
    aggFunc: aggregate.aggFunc,
    value: undefined,
    hasValue: false,
  };
};

const aggregateFieldValue = (row: RowObject, aggregate: RuntimeGroupedAggregate): unknown =>
  "field" in aggregate ? fieldValue(row, aggregate.field) : undefined;

export const updateAggregateState = (
  state: AggregateState,
  aggregate: RuntimeGroupedAggregate,
  row: RowObject,
): void => {
  const value = aggregateFieldValue(row, aggregate);
  if (state.aggFunc === "count") {
    state.count += 1n;
    return;
  }
  if (state.aggFunc === "countDistinct") {
    const sizeBefore = state.values.size;
    state.values.add(stableQueryValueString(value));
    if (state.values.size !== sizeBefore) {
      state.count += 1n;
    }
    return;
  }
  if (state.aggFunc === "sum") {
    if (state.resultKind === "bigint") {
      if (typeof value === "bigint") {
        state.bigintTotal += value;
      }
      return;
    }
    const decimal = runtimeValueToDecimal(value);
    if (decimal !== undefined) {
      state.decimalTotal = sumBigDecimal(state.decimalTotal, decimal);
    }
    return;
  }
  if (state.aggFunc === "avg") {
    const decimal = runtimeValueToDecimal(value);
    if (decimal !== undefined) {
      state.count += 1n;
      state.total = sumBigDecimal(state.total, decimal);
    }
    return;
  }
  if (!state.hasValue) {
    state.value = cloneUnknown(value);
    state.hasValue = true;
    return;
  }
  const comparison = compareQueryValue(value, state.value);
  if (comparison === 0) {
    return;
  }
  if ((state.aggFunc === "min" && comparison < 0) || (state.aggFunc === "max" && comparison > 0)) {
    state.value = cloneUnknown(value);
  }
};

const aggregateStateValue = (state: AggregateState): unknown => {
  if (state.aggFunc === "count") {
    return state.count;
  }
  if (state.aggFunc === "countDistinct") {
    return state.count;
  }
  if (state.aggFunc === "sum") {
    return state.resultKind === "bigint" ? state.bigintTotal : state.decimalTotal;
  }
  if (state.aggFunc === "avg") {
    return state.count === 0n ? fromBigInt(0n) : divideUnsafe(state.total, fromBigInt(state.count));
  }
  return cloneUnknown(state.value);
};

export const aggregateStateCompareValue = (state: AggregateState): unknown => {
  if (state.aggFunc === "count") {
    return state.count;
  }
  if (state.aggFunc === "countDistinct") {
    return state.count;
  }
  if (state.aggFunc === "sum") {
    return state.resultKind === "bigint" ? state.bigintTotal : state.decimalTotal;
  }
  if (state.aggFunc === "avg") {
    return state.count === 0n ? fromBigInt(0n) : divideUnsafe(state.total, fromBigInt(state.count));
  }
  return state.value;
};

export const newGroupState = (
  key: string,
  groupBy: ReadonlyArray<string>,
  aggregates: Readonly<Record<string, RuntimeGroupedAggregate>>,
  row: RowObject,
): GroupState => {
  const resultRow: Record<string, unknown> = {};
  for (const field of groupBy) {
    resultRow[field] = cloneUnknown(fieldValue(row, field));
  }
  const aggregateStates: Record<string, AggregateState> = {};
  for (const [alias, aggregate] of Object.entries(aggregates)) {
    aggregateStates[alias] = emptyAggregateState(aggregate);
  }
  return {
    key,
    row: resultRow,
    aggregates: aggregateStates,
  };
};

export const newIncrementalGroupState = (
  key: string,
  groupBy: ReadonlyArray<string>,
  aggregates: Readonly<Record<string, RuntimeGroupedAggregate>>,
  row: RowObject,
): MaterializedIncrementalGroupState => ({
  ...newGroupState(key, groupBy, aggregates, row),
  members: new Map(),
});

const resetAggregateStates = (
  group: GroupState,
  aggregates: Readonly<Record<string, RuntimeGroupedAggregate>>,
): void => {
  for (const key of Object.keys(group.aggregates)) {
    delete group.aggregates[key];
  }
  for (const [alias, aggregate] of Object.entries(aggregates)) {
    group.aggregates[alias] = emptyAggregateState(aggregate);
  }
};

export const recomputeIncrementalGroupState = (
  group: MaterializedIncrementalGroupState,
  aggregates: Readonly<Record<string, RuntimeGroupedAggregate>>,
): void => {
  resetAggregateStates(group, aggregates);
  for (const row of group.members.values()) {
    for (const [alias, aggregate] of Object.entries(aggregates)) {
      updateAggregateState(group.aggregates[alias]!, aggregate, row);
    }
  }
};

export const finalizeGroup = (group: GroupState): StoredRowOf<RowObject> => {
  const row: Record<string, unknown> = { ...group.row };
  for (const [alias, state] of Object.entries(group.aggregates)) {
    row[alias] = aggregateStateValue(state);
  }
  return {
    key: group.key,
    row,
  };
};
