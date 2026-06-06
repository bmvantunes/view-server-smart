import type { RawQueryCompilerMetadata } from "./raw-query-metadata";
import type { TopicRawWindowScanPlan, TopicRawWindowScanResult } from "./raw-window-scan";
import type { TopicRowEntry } from "./row-scan";
import {
  selectedPredicateCandidateFilter,
  type PredicateCandidateFilter,
} from "./topic-predicate-candidate-filter";
import {
  insertSlotIntoRawWindowIndexes,
  rawWindowOrderedWindow,
  rawWindowSlotComparator,
} from "./topic-raw-ordered-window-index";
import {
  orderedRawWindowSlotCount,
  orderedSlotIndexInsertionPoint,
  type OrderedRawWindow,
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

export { insertSlotIntoRawWindowIndexes };

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
