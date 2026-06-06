import { compareQueryValue } from "./query-value";
import type { RawQueryCompilerMetadata } from "./raw-query-metadata";
import type { TopicRawOrderByPlan, TopicRawWindowScanPlan } from "./raw-window-scan";
import type { TopicRowEntry } from "./row-scan";
import { columnValue, type TopicColumnValues } from "./topic-column-vector";
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
    const insertAt = orderedSlotIndexInsertionPoint(index.slots, slot, (left, right) =>
      compareSlotsByStorageOrder(state, left, right, index.orderBy),
    );
    index.slots.splice(insertAt, 0, slot);
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
  const slots = Array.from({ length: state.slots.length }, (_value, slot) => slot);
  slots.sort((left, right) => compareSlotsByStorageOrder(state, left, right, storageOrderBy));
  const index: OrderedSlotIndex = {
    orderBy: storageOrderBy,
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
  if (!storageOrderByFieldsExist(state, storageOrderBy)) {
    return undefined;
  }

  return (left, right) => {
    return compareSlotsByStorageOrder(state, left, right, storageOrderBy);
  };
};

const compareSlotsByStorageOrder = (
  state: RawOrderedWindowIndexState,
  left: number,
  right: number,
  storageOrderBy: ReadonlyArray<TopicRawOrderByPlan>,
): number => {
  for (const order of storageOrderBy) {
    const column = state.columns.get(order.field)!;
    const comparison = compareQueryValue(columnValue(column, left), columnValue(column, right));
    if (comparison !== 0) {
      return order.direction === "asc" ? comparison : -comparison;
    }
  }
  const leftKey = state.slots[left]!.key;
  const rightKey = state.slots[right]!.key;
  return Number(leftKey > rightKey) - Number(leftKey < rightKey);
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
