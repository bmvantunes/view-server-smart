import { compareQueryValue, type RawQueryCompilerMetadata } from "./raw-query-compiler";
import type {
  TopicRawOrderByPlan,
  TopicRawWindowScanPlan,
  TopicRawWindowScanResult,
} from "./raw-window-scan";
import type { TopicRowEntry } from "./row-scan";
import {
  selectedPredicateCandidateFilter,
  type PredicateCandidateFilter,
} from "./topic-predicate-candidate-filter";
import {
  distinctOrderedEqualityValues,
  equalityValueSatisfiesRangeBounds,
  noOrderedRangeBounds,
  orderedEqualityValuesForField,
  orderedRangeBoundsForField,
  orderedRawWindowSlotCount,
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
import { slotMatchesRawPredicatePlan } from "./topic-slot-predicate";

type ColumnValues = ReadonlyArray<unknown>;
type RowObject = object;

export type TopicRawWindowScanState = {
  readonly columns: ReadonlyMap<string, ColumnValues>;
  readonly orderedSlotIndexes: Map<string, OrderedSlotIndex>;
  readonly rawQueryMetadata: RawQueryCompilerMetadata;
  readonly slots: ReadonlyArray<TopicRowEntry<object>>;
};

const maxBoundedRawWindowEnd = 1_024;

export const scanTopicRawWindow = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
): TopicRawWindowScanResult<object> => {
  const orderedWindow = rawWindowOrderedWindow(state, plan);
  if (orderedWindow !== undefined) {
    const candidateFilter =
      plan.predicate.callbackSkippable === true &&
      orderedRawWindowSlotCount(orderedWindow) * 2 > state.slots.length
        ? selectedPredicateCandidateFilter(
            plan.predicate.filters,
            state.columns,
            state.slots.length,
            orderedWindow.candidateExcludedField,
          )
        : undefined;
    return scanRawWindowOrderedSlots(state, plan, orderedWindow, candidateFilter);
  }

  const compareSlots =
    rawWindowSlotComparator(state, plan) ??
    ((left, right) => plan.compare(state.slots[left]!, state.slots[right]!));
  return scanRawWindowSlots(state, plan, compareSlots);
};

export const insertSlotIntoRawWindowIndexes = (
  state: TopicRawWindowScanState,
  slot: number,
): void => {
  for (const index of state.orderedSlotIndexes.values()) {
    const insertAt = orderedSlotIndexInsertionPoint(index.slots, slot, (left, right) =>
      compareSlotsByStorageOrder(state, left, right, index.orderBy),
    );
    index.slots.splice(insertAt, 0, slot);
  }
};

const scanRawWindowOrderedSlots = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
  orderedWindow: OrderedRawWindow,
  candidateFilter: PredicateCandidateFilter | undefined,
): TopicRawWindowScanResult<object> => {
  let totalRows = 0;
  const windowSlots: Array<number> = [];
  const windowEnd = plan.offset + orderedWindow.limit;
  for (const span of orderedWindow.spans) {
    for (let slotIndex = span.startIndex; slotIndex < span.endIndex; slotIndex += 1) {
      const slot = orderedWindow.slots[slotIndex]!;
      if (candidateFilter !== undefined && !candidateFilter.matches(candidateFilter.column[slot])) {
        continue;
      }
      const entry = state.slots[slot]!;
      if (!slotMatchesRawPredicatePlan(slot, plan, entry.row, state.columns)) {
        continue;
      }
      const matchIndex = totalRows;
      totalRows += 1;
      if (matchIndex >= plan.offset && matchIndex < windowEnd) {
        windowSlots.push(slot);
      }
    }
  }
  return rawWindowScanResult(state, windowSlots, totalRows);
};

const scanRawWindowSlots = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
  compareSlots: (left: number, right: number) => number,
): TopicRawWindowScanResult<object> => {
  const candidateFilter =
    plan.predicate.callbackSkippable === true
      ? selectedPredicateCandidateFilter(plan.predicate.filters, state.columns, state.slots.length)
      : undefined;
  const boundedWindowEnd = boundedRawWindowEnd(plan);
  if (boundedWindowEnd !== undefined) {
    return scanRawWindowBoundedSlots(state, plan, compareSlots, boundedWindowEnd, candidateFilter);
  }

  let totalRows = 0;
  const filteredSlots: Array<number> = [];
  for (let slot = 0; slot < state.slots.length; slot += 1) {
    if (candidateFilter !== undefined && !candidateFilter.matches(candidateFilter.column[slot])) {
      continue;
    }
    const entry = state.slots[slot]!;
    if (!slotMatchesRawPredicatePlan(slot, plan, entry.row, state.columns)) {
      continue;
    }
    totalRows += 1;
    if (plan.limit !== 0) {
      filteredSlots.push(slot);
    }
  }
  filteredSlots.sort(compareSlots);
  const windowSlots = filteredSlots.slice(
    plan.offset,
    plan.limit === undefined ? undefined : plan.offset + plan.limit,
  );
  return rawWindowScanResult(state, windowSlots, totalRows);
};

const scanRawWindowBoundedSlots = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
  compareSlots: (left: number, right: number) => number,
  windowEnd: number,
  candidateFilter: PredicateCandidateFilter | undefined,
): TopicRawWindowScanResult<object> => {
  let totalRows = 0;
  const windowSlots: Array<number> = [];
  for (let slot = 0; slot < state.slots.length; slot += 1) {
    if (candidateFilter !== undefined && !candidateFilter.matches(candidateFilter.column[slot])) {
      continue;
    }
    const entry = state.slots[slot]!;
    if (!slotMatchesRawPredicatePlan(slot, plan, entry.row, state.columns)) {
      continue;
    }
    totalRows += 1;
    if (windowSlots.length < windowEnd) {
      const insertAt = orderedSlotIndexInsertionPoint(windowSlots, slot, compareSlots);
      windowSlots.splice(insertAt, 0, slot);
      continue;
    }
    const worstSlot = windowSlots[windowSlots.length - 1]!;
    if (compareSlots(slot, worstSlot) < 0) {
      const insertAt = orderedSlotIndexInsertionPoint(windowSlots, slot, compareSlots);
      windowSlots.splice(insertAt, 0, slot);
      windowSlots.pop();
    }
  }
  return rawWindowScanResult(state, windowSlots.slice(plan.offset), totalRows);
};

const rawWindowOrderedWindow = (
  state: TopicRawWindowScanState,
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
  const indexKey = orderedSlotIndexKey(storageOrderBy);
  const existing = state.orderedSlotIndexes.get(indexKey);
  if (existing !== undefined) {
    return {
      candidateExcludedField: orderField,
      limit: plan.limit,
      slots: existing.slots,
      spans: orderedSlotIndexSpans(state, existing, seekBounds, equalityValues),
    };
  }

  const slots = Array.from({ length: state.slots.length }, (_value, slot) => slot);
  slots.sort((left, right) => compareSlotsByStorageOrder(state, left, right, storageOrderBy));
  state.orderedSlotIndexes.set(indexKey, {
    orderBy: storageOrderBy,
    slots,
  });
  return {
    candidateExcludedField: orderField,
    limit: plan.limit,
    slots,
    spans: orderedSlotIndexSpans(
      state,
      {
        orderBy: storageOrderBy,
        slots,
      },
      seekBounds,
      equalityValues,
    ),
  };
};

const rawWindowSlotComparator = (
  state: TopicRawWindowScanState,
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
  state: TopicRawWindowScanState,
  left: number,
  right: number,
  storageOrderBy: ReadonlyArray<TopicRawOrderByPlan>,
): number => {
  for (const order of storageOrderBy) {
    const column = state.columns.get(order.field)!;
    const comparison = compareQueryValue(column[left], column[right]);
    if (comparison !== 0) {
      return order.direction === "asc" ? comparison : -comparison;
    }
  }
  const leftKey = state.slots[left]!.key;
  const rightKey = state.slots[right]!.key;
  return Number(leftKey > rightKey) - Number(leftKey < rightKey);
};

const storageOrderByFieldsExist = (
  state: TopicRawWindowScanState,
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
  state: TopicRawWindowScanState,
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
  state: TopicRawWindowScanState,
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

const rawWindowScanResult = (
  state: TopicRawWindowScanState,
  windowSlots: ReadonlyArray<number>,
  totalRows: number,
): TopicRawWindowScanResult<RowObject> => {
  const window = windowSlots.map((slot) => state.slots[slot]!);
  return {
    keys: window.map((entry) => entry.key),
    window,
    totalRows,
  };
};

const boundedRawWindowEnd = (plan: TopicRawWindowScanPlan<object>): number | undefined => {
  if (plan.limit === undefined || plan.limit <= 0) {
    return undefined;
  }
  if (!Number.isSafeInteger(plan.offset) || plan.offset < 0 || !Number.isSafeInteger(plan.limit)) {
    return undefined;
  }
  const windowEnd = plan.offset + plan.limit;
  if (!Number.isSafeInteger(windowEnd) || windowEnd > maxBoundedRawWindowEnd) {
    return undefined;
  }
  return windowEnd;
};
