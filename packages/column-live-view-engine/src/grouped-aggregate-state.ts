import {
  divideUnsafe,
  fromBigInt,
  fromNumberUnsafe,
  isBigDecimal,
  subtract as subtractBigDecimal,
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

export type GroupedAggregatePlan = {
  readonly alias: string;
  readonly aggregate: RuntimeGroupedAggregate;
};

type CountAggregateState = {
  readonly aggFunc: "count";
  count: bigint;
};

type CountDistinctAggregateState = {
  readonly aggFunc: "countDistinct";
  readonly values: Map<string, number>;
  count: bigint;
};

type BigIntSumAggregateState = {
  readonly aggFunc: "sum";
  readonly resultKind: "bigint";
  bigintTotal: bigint;
};

type BigDecimalSumAggregateState = {
  readonly aggFunc: "sum";
  readonly resultKind: "bigDecimal";
  decimalTotal: BigDecimal;
};

type AverageAggregateState = {
  readonly aggFunc: "avg";
  count: bigint;
  total: BigDecimal;
};

type MinMaxAggregateValueState = {
  count: number;
  value: unknown;
};

type BaseMinMaxAggregateState = {
  readonly aggFunc: "min" | "max";
  value: unknown;
  hasValue: boolean;
};

export type RetainedMinMaxAggregateState = BaseMinMaxAggregateState & {
  readonly values: Map<string, MinMaxAggregateValueState>;
};

type NonMinMaxAggregateState =
  | CountAggregateState
  | CountDistinctAggregateState
  | BigIntSumAggregateState
  | BigDecimalSumAggregateState
  | AverageAggregateState;

type AggregateState =
  | NonMinMaxAggregateState
  | BaseMinMaxAggregateState
  | RetainedMinMaxAggregateState;

export type ReversibleAggregateState = NonMinMaxAggregateState | RetainedMinMaxAggregateState;

export type GroupState = {
  readonly key: string;
  readonly row: Record<string, unknown>;
  readonly aggregates: Record<string, AggregateState>;
};

export type MaterializedIncrementalGroupState = Omit<GroupState, "aggregates"> & {
  readonly aggregates: Record<string, ReversibleAggregateState>;
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
      values: new Map<string, number>(),
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

const emptyReversibleAggregateState = (
  aggregate: RuntimeGroupedAggregate,
): ReversibleAggregateState => {
  if (aggregate.aggFunc === "count") {
    return {
      aggFunc: "count",
      count: 0n,
    };
  }
  if (aggregate.aggFunc === "countDistinct") {
    return {
      aggFunc: "countDistinct",
      values: new Map<string, number>(),
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
    values: new Map<string, MinMaxAggregateValueState>(),
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
    const key = stableQueryValueString(value);
    const count = state.values.get(key);
    if (count === undefined) {
      state.values.set(key, 1);
      state.count += 1n;
    } else {
      state.values.set(key, count + 1);
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
  if ("values" in state) {
    const values = state.values;
    const key = stableQueryValueString(value);
    const entry = values.get(key);
    if (entry === undefined) {
      values.set(key, {
        count: 1,
        value: cloneUnknown(value),
      });
    } else {
      entry.count += 1;
    }
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

const recomputeMinMaxAggregateState = (state: RetainedMinMaxAggregateState): void => {
  let nextValue: unknown;
  let hasValue = false;
  for (const entry of state.values.values()) {
    if (!hasValue) {
      nextValue = cloneUnknown(entry.value);
      hasValue = true;
      continue;
    }
    const comparison = compareQueryValue(entry.value, nextValue);
    const isBetterValue = state.aggFunc === "min" ? comparison < 0 : comparison > 0;
    if (isBetterValue) {
      nextValue = cloneUnknown(entry.value);
    }
  }
  state.value = nextValue;
  state.hasValue = hasValue;
};

export const removeAggregateState = (
  state: ReversibleAggregateState,
  aggregate: RuntimeGroupedAggregate,
  row: RowObject,
): RetainedMinMaxAggregateState | undefined => {
  const value = aggregateFieldValue(row, aggregate);
  if (state.aggFunc === "count") {
    state.count -= 1n;
    return undefined;
  }
  if (state.aggFunc === "countDistinct") {
    const key = stableQueryValueString(value);
    const count = state.values.get(key)!;
    if (count === 1) {
      state.values.delete(key);
      state.count -= 1n;
    } else {
      state.values.set(key, count - 1);
    }
    return undefined;
  }
  if (state.aggFunc === "sum") {
    if (state.resultKind === "bigint") {
      if (typeof value === "bigint") {
        state.bigintTotal -= value;
      }
      return undefined;
    }
    const decimal = runtimeValueToDecimal(value);
    if (decimal !== undefined) {
      state.decimalTotal = subtractBigDecimal(state.decimalTotal, decimal);
    }
    return undefined;
  }
  if (state.aggFunc === "avg") {
    const decimal = runtimeValueToDecimal(value);
    if (decimal !== undefined) {
      state.count -= 1n;
      state.total = subtractBigDecimal(state.total, decimal);
    }
    return undefined;
  }
  const key = stableQueryValueString(value);
  const values = state.values;
  const entry = values.get(key)!;
  if (entry.count > 1) {
    entry.count -= 1;
    return undefined;
  }
  values.delete(key);
  if (state.hasValue && stableQueryValueString(state.value) === key) {
    return state;
  }
  return undefined;
};

export const recomputeRetainedMinMaxAggregateState = (
  state: RetainedMinMaxAggregateState,
): void => {
  recomputeMinMaxAggregateState(state);
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
  aggregates: ReadonlyArray<GroupedAggregatePlan>,
  row: RowObject,
): GroupState => {
  const resultRow: Record<string, unknown> = {};
  for (const field of groupBy) {
    resultRow[field] = cloneUnknown(fieldValue(row, field));
  }
  const aggregateStates: Record<string, AggregateState> = {};
  for (const { alias, aggregate } of aggregates) {
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
  aggregates: ReadonlyArray<GroupedAggregatePlan>,
  row: RowObject,
): MaterializedIncrementalGroupState => {
  const resultRow: Record<string, unknown> = {};
  for (const field of groupBy) {
    resultRow[field] = cloneUnknown(fieldValue(row, field));
  }
  const aggregateStates: Record<string, ReversibleAggregateState> = {};
  for (const { alias, aggregate } of aggregates) {
    aggregateStates[alias] = emptyReversibleAggregateState(aggregate);
  }
  return {
    key,
    row: resultRow,
    aggregates: aggregateStates,
    members: new Map(),
  };
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
