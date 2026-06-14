import { compareQueryValue } from "./query-value";
import type { RawQueryCompilerMetadata } from "./raw-query-metadata";
import type { TopicRawOrderByPlan, TopicRawWindowScanPlan } from "./raw-window-scan";
import type { TopicRowEntry } from "./row-scan";
import { columnValue, type TopicColumnValues } from "./topic-column-vector";
import { Order as orderBigDecimal } from "effect/BigDecimal";
import {
  distinctOrderedEqualityValues,
  equalityValueSatisfiesRangeBounds,
  noOrderedRangeBounds,
  orderedEqualityValuesForField,
  orderedRangeBoundsForField,
  orderedSlotBoundIndex,
  orderedSlotIndexInsertionPoint,
  orderedSlotIndexKey,
  orderedWindowSpansInIndexOrder,
  predicateFiltersAreOrderedIndexAdmissible,
  rangeBoundsAreEmpty,
  type OrderedRangeBounds,
  type OrderedRawWindow,
  type OrderedRawWindowSpan,
  type OrderedSlotIndex,
  type RawStorageOrderColumn,
} from "./topic-ordered-window";

export type RawOrderedWindowIndexState = {
  readonly columns: ReadonlyMap<string, TopicColumnValues>;
  readonly orderedSlotIndexes: Map<string, OrderedSlotIndex>;
  readonly rawQueryMetadata: RawQueryCompilerMetadata;
  readonly slots: ReadonlyArray<TopicRowEntry<object>>;
};

export const insertSlotIntoRawWindowIndexes = (
  state: RawOrderedWindowIndexState,
  slot: number,
): void => {
  for (const index of state.orderedSlotIndexes.values()) {
    insertSlotIntoRawWindowIndex(state, index, slot);
  }
};

export const insertSlotIntoRawWindowIndex = (
  state: Pick<RawOrderedWindowIndexState, "slots">,
  index: OrderedSlotIndex,
  slot: number,
): void => {
  const insertAt = orderedSlotIndexInsertionPoint(index.slots, slot, (left, right) =>
    compareSlotsByStorageOrder(state, left, right, index.orderColumns),
  );
  index.slots.splice(insertAt, 0, slot);
};

export const removeSlotFromRawWindowIndexes = (
  state: RawOrderedWindowIndexState,
  slot: number,
): void => {
  for (const index of state.orderedSlotIndexes.values()) {
    removeSlotFromRawWindowIndex(index, slot);
  }
};

export const removeSlotFromRawWindowIndex = (index: OrderedSlotIndex, slot: number): void => {
  const slotIndex = index.slots.indexOf(slot);
  if (slotIndex >= 0) {
    index.slots.splice(slotIndex, 1);
  }
};

export const rawWindowOrderedWindow = (
  state: RawOrderedWindowIndexState,
  plan: TopicRawWindowScanPlan<object>,
): OrderedRawWindow | undefined => {
  const storageOrderBy = plan.storageOrderBy;
  if (
    plan.limit === undefined ||
    !Number.isSafeInteger(plan.limit) ||
    plan.limit <= 0 ||
    storageOrderBy === undefined ||
    storageOrderBy.length !== 1
  ) {
    return undefined;
  }
  const orderField = storageOrderBy[0]!.field;
  if (plan.predicate.callbackSkippable !== true) {
    return undefined;
  }
  if (!predicateFiltersAreOrderedIndexAdmissible(plan.predicate.filters, orderField)) {
    return undefined;
  }
  if (!storageOrderByFieldsExist(state, storageOrderBy)) {
    return undefined;
  }

  const rangeBounds = orderedRangeBoundsForField(
    plan.predicate.filters,
    orderField,
    state.rawQueryMetadata,
  );
  if (rangeBounds !== undefined && rangeBoundsAreEmpty(rangeBounds)) {
    return {
      candidateExcludedField: orderField,
      limit: plan.limit,
      slots: [],
      spans: [],
    };
  }
  const seekBounds = rangeBounds ?? noOrderedRangeBounds;
  const equalityValues = orderedEqualityValuesForField(
    plan.predicate.filters,
    orderField,
    state.rawQueryMetadata,
  );
  const index = rawWindowOrderedSlotIndex(state, storageOrderBy);

  return {
    candidateExcludedField: orderField,
    limit: plan.limit,
    slots: index.slots,
    spans: rawWindowOrderedSpans(state, index, seekBounds, equalityValues),
  };
};

export const rawWindowOrderedSlotIndex = (
  state: RawOrderedWindowIndexState,
  storageOrderBy: ReadonlyArray<TopicRawOrderByPlan>,
): OrderedSlotIndex => {
  const indexKey = orderedSlotIndexKey(storageOrderBy);
  const existing = state.orderedSlotIndexes.get(indexKey);
  if (existing !== undefined) {
    return existing;
  }
  const orderColumns = compiledRawStorageOrder(state, storageOrderBy);
  if (orderColumns === undefined) {
    return {
      orderBy: storageOrderBy,
      orderColumns: [],
      slots: [],
    };
  }
  const slots = Array.from({ length: state.slots.length }, (_value, slot) => slot);
  slots.sort((left, right) => compareSlotsByStorageOrder(state, left, right, orderColumns));
  const index: OrderedSlotIndex = {
    orderBy: storageOrderBy,
    orderColumns,
    slots,
  };
  state.orderedSlotIndexes.set(indexKey, index);
  return index;
};

export const existingRawWindowOrderedSlotIndex = (
  state: RawOrderedWindowIndexState,
  storageOrderBy: ReadonlyArray<TopicRawOrderByPlan>,
): OrderedSlotIndex | undefined => {
  return state.orderedSlotIndexes.get(orderedSlotIndexKey(storageOrderBy));
};

export const rawWindowOrderedSpans = (
  state: RawOrderedWindowIndexState,
  index: OrderedSlotIndex,
  rangeBounds: OrderedRangeBounds,
  equalityValues: ReadonlyArray<unknown> | undefined,
): ReadonlyArray<OrderedRawWindowSpan> => {
  return orderedSlotIndexSpans(state, index, rangeBounds, equalityValues);
};

export const rawWindowSlotComparator = (
  state: RawOrderedWindowIndexState,
  plan: TopicRawWindowScanPlan<object>,
): ((left: number, right: number) => number) | undefined => {
  const storageOrderBy = plan.storageOrderBy;
  if (storageOrderBy === undefined) {
    return undefined;
  }
  const orderColumns = compiledRawStorageOrder(state, storageOrderBy);
  if (orderColumns === undefined) {
    return undefined;
  }

  return (left, right) => {
    return compareSlotsByStorageOrder(state, left, right, orderColumns);
  };
};

export const compiledRawStorageOrder = (
  state: Pick<RawOrderedWindowIndexState, "columns">,
  storageOrderBy: ReadonlyArray<TopicRawOrderByPlan>,
): ReadonlyArray<RawStorageOrderColumn> | undefined => {
  const orderColumns: Array<RawStorageOrderColumn> = [];
  for (const order of storageOrderBy) {
    const column = state.columns.get(order.field);
    if (column === undefined) {
      return undefined;
    }
    orderColumns.push({
      compareSlots: rawStorageOrderColumnComparator(column),
      direction: order.direction,
    });
  }
  return orderColumns;
};

export const compareSlotsByStorageOrder = (
  state: Pick<RawOrderedWindowIndexState, "slots">,
  left: number,
  right: number,
  storageOrderBy: ReadonlyArray<RawStorageOrderColumn>,
): number => {
  for (const order of storageOrderBy) {
    const comparison = order.compareSlots(left, right);
    if (comparison !== 0) {
      return order.direction === "asc" ? comparison : -comparison;
    }
  }
  const leftKey = state.slots[left]!.key;
  const rightKey = state.slots[right]!.key;
  return Number(leftKey > rightKey) - Number(leftKey < rightKey);
};

const rawStorageOrderColumnComparator = (
  column: TopicColumnValues,
): ((left: number, right: number) => number) => {
  if (column.kind === "string") {
    return (left, right) => compareStringColumnSlots(column, left, right);
  }
  if (column.kind === "number") {
    return (left, right) => compareNumberColumnSlots(column, left, right);
  }
  if (column.kind === "bigint") {
    return (left, right) => compareBigIntColumnSlots(column, left, right);
  }
  if (column.kind === "bigDecimal") {
    return (left, right) => compareBigDecimalColumnSlots(column, left, right);
  }
  return (left, right) => compareQueryValue(columnValue(column, left), columnValue(column, right));
};

const compareStringColumnSlots = (
  column: TopicColumnValues & { readonly kind: "string" },
  left: number,
  right: number,
): number => {
  const leftValue = column.stringAt(left);
  const rightValue = column.stringAt(right);
  if (leftValue !== undefined && rightValue !== undefined) {
    return Number(leftValue > rightValue) - Number(leftValue < rightValue);
  }
  return compareQueryValue(columnValue(column, left), columnValue(column, right));
};

const compareNumberColumnSlots = (
  column: TopicColumnValues & { readonly kind: "number" },
  left: number,
  right: number,
): number => {
  const leftValue = column.numberAt(left);
  const rightValue = column.numberAt(right);
  if (
    leftValue !== undefined &&
    rightValue !== undefined &&
    Number.isFinite(leftValue) &&
    Number.isFinite(rightValue)
  ) {
    return leftValue === rightValue ? 0 : leftValue < rightValue ? -1 : 1;
  }
  return compareQueryValue(columnValue(column, left), columnValue(column, right));
};

const compareBigIntColumnSlots = (
  column: TopicColumnValues & { readonly kind: "bigint" },
  left: number,
  right: number,
): number => {
  const leftValue = column.bigintAt(left);
  const rightValue = column.bigintAt(right);
  if (leftValue !== undefined && rightValue !== undefined) {
    return leftValue === rightValue ? 0 : leftValue < rightValue ? -1 : 1;
  }
  return compareQueryValue(columnValue(column, left), columnValue(column, right));
};

const compareBigDecimalColumnSlots = (
  column: TopicColumnValues & { readonly kind: "bigDecimal" },
  left: number,
  right: number,
): number => {
  const leftValue = column.bigDecimalAt(left);
  const rightValue = column.bigDecimalAt(right);
  if (leftValue !== undefined && rightValue !== undefined) {
    return orderBigDecimal(leftValue, rightValue);
  }
  return compareQueryValue(columnValue(column, left), columnValue(column, right));
};

const storageOrderByFieldsExist = (
  state: RawOrderedWindowIndexState,
  storageOrderBy: ReadonlyArray<TopicRawOrderByPlan>,
): boolean => {
  for (const order of storageOrderBy) {
    if (!state.columns.has(order.field)) {
      return false;
    }
  }
  return true;
};

const orderedSlotIndexSpans = (
  state: RawOrderedWindowIndexState,
  index: OrderedSlotIndex,
  rangeBounds: OrderedRangeBounds,
  equalityValues: ReadonlyArray<unknown> | undefined,
): ReadonlyArray<OrderedRawWindowSpan> => {
  if (equalityValues === undefined) {
    return [orderedSlotIndexBounds(state, index, rangeBounds)];
  }
  const seekValues = distinctOrderedEqualityValues(equalityValues).filter((value) =>
    equalityValueSatisfiesRangeBounds(value, rangeBounds),
  );
  const spans = seekValues.map((value) =>
    orderedSlotIndexBounds(state, index, {
      lower: {
        exclusive: false,
        value,
      },
      upper: {
        exclusive: false,
        value,
      },
    }),
  );
  return orderedWindowSpansInIndexOrder(spans);
};

const orderedSlotIndexBounds = (
  state: RawOrderedWindowIndexState,
  index: OrderedSlotIndex,
  rangeBounds: OrderedRangeBounds,
): OrderedRawWindowSpan => {
  const order = index.orderBy[0]!;
  const column = state.columns.get(order.field)!;
  if (order.direction === "asc") {
    const startIndex =
      rangeBounds.lower === undefined
        ? 0
        : orderedSlotBoundIndex(
            index.slots,
            column,
            rangeBounds.lower.value,
            rangeBounds.lower.exclusive
              ? (comparison) => comparison > 0
              : (comparison) => comparison >= 0,
          );
    const endIndex =
      rangeBounds.upper === undefined
        ? index.slots.length
        : orderedSlotBoundIndex(
            index.slots,
            column,
            rangeBounds.upper.value,
            rangeBounds.upper.exclusive
              ? (comparison) => comparison >= 0
              : (comparison) => comparison > 0,
          );
    return {
      endIndex: Math.max(startIndex, endIndex),
      startIndex,
    };
  }
  const startIndex =
    rangeBounds.upper === undefined
      ? 0
      : orderedSlotBoundIndex(
          index.slots,
          column,
          rangeBounds.upper.value,
          rangeBounds.upper.exclusive
            ? (comparison) => comparison < 0
            : (comparison) => comparison <= 0,
        );
  const endIndex =
    rangeBounds.lower === undefined
      ? index.slots.length
      : orderedSlotBoundIndex(
          index.slots,
          column,
          rangeBounds.lower.value,
          rangeBounds.lower.exclusive
            ? (comparison) => comparison <= 0
            : (comparison) => comparison < 0,
        );
  return {
    endIndex: Math.max(startIndex, endIndex),
    startIndex,
  };
};
