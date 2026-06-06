import type { RawQueryCompilerMetadata } from "./raw-query-metadata";
import type { TopicRawWindowScanPlan, TopicRawWindowScanResult } from "./raw-window-scan";
import type { TopicRowEntry } from "./row-scan";
import type { TopicColumnValues } from "./topic-column-vector";
import {
  selectedPredicateCandidateSlots,
  type PredicateCandidateSlotIndexState,
  type PredicateCandidateSlots,
} from "./topic-predicate-candidate-index";
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
import { rawPredicateSlotMatcher } from "./topic-slot-predicate";

type RowObject = object;

export type TopicRawWindowScanState = {
  readonly columns: ReadonlyMap<string, TopicColumnValues>;
  readonly orderedSlotIndexes: Map<string, OrderedSlotIndex>;
  readonly rawQueryMetadata: RawQueryCompilerMetadata;
  readonly scalarPredicateIndexes: PredicateCandidateSlotIndexState["scalarPredicateIndexes"];
  readonly slots: ReadonlyArray<TopicRowEntry<object>>;
};

const maxBoundedRawWindowEnd = 1_024;
const maxMaterializedPredicateCandidateSlots = 100_000;
const materializedPredicateCandidateSlotBudget = maxMaterializedPredicateCandidateSlots + 1;

export const scanTopicRawWindow = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
): TopicRawWindowScanResult<object> => {
  const matchesSlot = rawPredicateMatchesSlot(state, plan);
  if (plan.limit === 0) {
    return countRawWindowSlots(state, plan, matchesSlot);
  }

  const orderedWindow = rawWindowOrderedWindow(state, plan);
  if (orderedWindow !== undefined) {
    const orderedSlotCount = orderedRawWindowSlotCount(orderedWindow);
    const candidateSlots = selectedPredicateCandidateSlots(state, plan.predicate.filters, {
      allowScalarIndexBuild: false,
      exactRangeCandidates: plan.predicate.callbackSkippable === true,
      excludedField: orderedWindow.candidateExcludedField,
      maxSlotCount: Math.min(orderedSlotCount, materializedPredicateCandidateSlotBudget),
    });
    if (candidateSlots !== undefined && candidateSlots.slots.length < orderedSlotCount) {
      const compareSlots = rawWindowSlotComparator(state, plan)!;
      return scanRawWindowCandidateSlots(state, plan, compareSlots, matchesSlot, candidateSlots);
    }
    return scanRawWindowOrderedSlots(state, plan, matchesSlot, orderedWindow);
  }

  const compareSlots =
    rawWindowSlotComparator(state, plan) ??
    ((left, right) => plan.compare(state.slots[left]!, state.slots[right]!));
  const candidateSlots = selectedPredicateCandidateSlots(state, plan.predicate.filters, {
    allowScalarIndexBuild: true,
    exactRangeCandidates: plan.predicate.callbackSkippable === true,
    maxSlotCount: Math.min(state.slots.length, materializedPredicateCandidateSlotBudget),
  });
  if (candidateSlots !== undefined) {
    return scanRawWindowCandidateSlots(state, plan, compareSlots, matchesSlot, candidateSlots);
  }
  return scanRawWindowSlots(state, plan, compareSlots, matchesSlot);
};

export { insertSlotIntoRawWindowIndexes };

const countRawWindowSlots = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
  matchesSlot: (slot: number) => boolean,
): TopicRawWindowScanResult<object> => {
  const candidateSlots = selectedPredicateCandidateSlots(state, plan.predicate.filters, {
    allowScalarIndexBuild: true,
    exactRangeCandidates: plan.predicate.callbackSkippable === true,
    maxSlotCount: Math.min(state.slots.length, materializedPredicateCandidateSlotBudget),
  });
  if (candidateSlots !== undefined) {
    return countRawWindowCandidateSlots(state, matchesSlot, candidateSlots);
  }

  let totalRows = 0;
  for (let slot = 0; slot < state.slots.length; slot += 1) {
    if (matchesSlot(slot)) {
      totalRows += 1;
    }
  }
  return rawWindowScanResult(state, [], totalRows);
};

const countRawWindowCandidateSlots = (
  state: TopicRawWindowScanState,
  matchesSlot: (slot: number) => boolean,
  candidateSlots: PredicateCandidateSlots,
): TopicRawWindowScanResult<object> => {
  let totalRows = 0;
  for (const slot of candidateSlots.slots) {
    if (matchesSlot(slot)) {
      totalRows += 1;
    }
  }
  return rawWindowScanResult(state, [], totalRows);
};

const scanRawWindowOrderedSlots = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
  matchesSlot: (slot: number) => boolean,
  orderedWindow: OrderedRawWindow,
): TopicRawWindowScanResult<object> => {
  let totalRows = 0;
  const windowSlots: Array<number> = [];
  const windowEnd = plan.offset + orderedWindow.limit;
  for (const span of orderedWindow.spans) {
    for (let slotIndex = span.startIndex; slotIndex < span.endIndex; slotIndex += 1) {
      const slot = orderedWindow.slots[slotIndex]!;
      if (!matchesSlot(slot)) {
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

const scanRawWindowCandidateSlots = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
  compareSlots: (left: number, right: number) => number,
  matchesSlot: (slot: number) => boolean,
  candidateSlots: PredicateCandidateSlots,
): TopicRawWindowScanResult<object> => {
  const boundedWindowEnd = boundedRawWindowEnd(plan);
  if (boundedWindowEnd !== undefined) {
    return scanRawWindowBoundedSlotCandidates(
      state,
      plan,
      compareSlots,
      matchesSlot,
      boundedWindowEnd,
      candidateSlots,
    );
  }

  let totalRows = 0;
  const filteredSlots: Array<number> = [];
  for (const slot of candidateSlots.slots) {
    if (!matchesSlot(slot)) {
      continue;
    }
    totalRows += 1;
    filteredSlots.push(slot);
  }
  filteredSlots.sort(compareSlots);
  const windowSlots = filteredSlots.slice(
    plan.offset,
    plan.limit === undefined ? undefined : plan.offset + plan.limit,
  );
  return rawWindowScanResult(state, windowSlots, totalRows);
};

const scanRawWindowSlots = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
  compareSlots: (left: number, right: number) => number,
  matchesSlot: (slot: number) => boolean,
): TopicRawWindowScanResult<object> => {
  const boundedWindowEnd = boundedRawWindowEnd(plan);
  if (boundedWindowEnd !== undefined) {
    return scanRawWindowBoundedSlots(state, plan, compareSlots, matchesSlot, boundedWindowEnd);
  }

  let totalRows = 0;
  const filteredSlots: Array<number> = [];
  for (let slot = 0; slot < state.slots.length; slot += 1) {
    if (!matchesSlot(slot)) {
      continue;
    }
    totalRows += 1;
    filteredSlots.push(slot);
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
  matchesSlot: (slot: number) => boolean,
  windowEnd: number,
): TopicRawWindowScanResult<object> => {
  let totalRows = 0;
  const windowSlots: Array<number> = [];
  for (let slot = 0; slot < state.slots.length; slot += 1) {
    if (!matchesSlot(slot)) {
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

const scanRawWindowBoundedSlotCandidates = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
  compareSlots: (left: number, right: number) => number,
  matchesSlot: (slot: number) => boolean,
  windowEnd: number,
  candidateSlots: PredicateCandidateSlots,
): TopicRawWindowScanResult<object> => {
  let totalRows = 0;
  const windowSlots: Array<number> = [];
  for (const slot of candidateSlots.slots) {
    if (!matchesSlot(slot)) {
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

const rawPredicateMatchesSlot = (
  state: TopicRawWindowScanState,
  plan: TopicRawWindowScanPlan<object>,
): ((slot: number) => boolean) => {
  const matcher = rawPredicateSlotMatcher(plan, state.columns);
  if (matcher.kind === "slot") {
    return matcher.matchesSlot;
  }
  return (slot) => matcher.matchesEntry(slot, state.slots[slot]!);
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
