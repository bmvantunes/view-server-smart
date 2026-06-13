import type { TopicRawPredicateFilterPlan } from "./raw-predicate-plan";
import type { RawOrderedWindowIndexState } from "./topic-raw-ordered-window-index";
import {
  existingRawWindowOrderedSlotIndex,
  rawWindowOrderedSpans,
} from "./topic-raw-ordered-window-index";
import {
  orderedRawWindowSpanSlotCount,
  orderedRangeBoundsForField,
  rangeBoundsAreEmpty,
} from "./topic-ordered-window";
import { scalarEqualityKey } from "./row-values";
import { columnScalarEqualityKey, type TopicColumnValues } from "./topic-column-vector";

type RangePredicateFilter = TopicRawPredicateFilterPlan & {
  readonly operator: "gt" | "gte" | "lt" | "lte";
};

type ScalarPredicateFieldIndex = {
  readonly buckets: Map<string, Set<number>>;
  readonly indexedKeys: Set<string>;
};

export const maxRetainedScalarPredicateBucketSlots = 100_000;

export type ScalarPredicateIndexes = Map<string, ScalarPredicateFieldIndex>;

export type PredicateCandidateSlotIndexState = RawOrderedWindowIndexState & {
  readonly scalarPredicateIndexes: ScalarPredicateIndexes;
};

export type PredicateCandidateSlots = {
  readonly slots: ReadonlyArray<number>;
};

type PredicateCandidateSelectionOptions = {
  readonly allowScalarIndexBuild: boolean;
  readonly exactRangeCandidates: boolean;
  readonly excludedField?: string;
  readonly maxSlotCount?: number;
};

export const createScalarPredicateIndexes = (): ScalarPredicateIndexes => new Map();

export const addSlotToScalarPredicateIndexes = (
  indexes: ScalarPredicateIndexes,
  columns: ReadonlyMap<string, TopicColumnValues>,
  slot: number,
): void => {
  for (const [field, index] of indexes) {
    addSlotToScalarPredicateIndex(index, columns.get(field), slot);
    pruneEmptyScalarPredicateIndex(indexes, field, index);
  }
};

export const removeSlotFromScalarPredicateIndexes = (
  indexes: ScalarPredicateIndexes,
  columns: ReadonlyMap<string, TopicColumnValues>,
  slot: number,
): void => {
  for (const [field, index] of indexes) {
    removeSlotFromScalarPredicateIndex(index, columns.get(field), slot);
    pruneEmptyScalarPredicateIndex(indexes, field, index);
  }
};

export const selectedPredicateCandidateSlots = (
  state: PredicateCandidateSlotIndexState,
  filters: ReadonlyArray<TopicRawPredicateFilterPlan>,
  options: PredicateCandidateSelectionOptions,
): PredicateCandidateSlots | undefined => {
  let selected: PredicateCandidateSlots | undefined;
  const maxSlotCount = options.maxSlotCount ?? state.slots.length;
  for (const filter of filters) {
    if (filter.field === options.excludedField) {
      continue;
    }
    const candidate = predicateCandidateSlots(state, filter, filters, options);
    if (candidate === undefined) {
      continue;
    }
    if (candidate.slots.length >= maxSlotCount) {
      continue;
    }
    if (selected === undefined || candidate.slots.length < selected.slots.length) {
      selected = candidate;
    }
  }
  if (selected === undefined || selected.slots.length >= state.slots.length) {
    return undefined;
  }
  return selected;
};

const predicateCandidateSlots = (
  state: PredicateCandidateSlotIndexState,
  filter: TopicRawPredicateFilterPlan,
  filters: ReadonlyArray<TopicRawPredicateFilterPlan>,
  options: PredicateCandidateSelectionOptions,
): PredicateCandidateSlots | undefined => {
  if (filter.operator === "eq") {
    const key = scalarEqualityKey(filter.value);
    if (key === undefined) {
      return undefined;
    }
    return scalarEqualityCandidateSlots(
      state,
      filter.field,
      [key],
      options.allowScalarIndexBuild,
      options.maxSlotCount,
    );
  }
  if (filter.operator === "in") {
    const valueKeys =
      filter.valueKeys === undefined ? scalarEqualityKeys(filter.values) : [...filter.valueKeys];
    if (valueKeys === undefined) {
      return undefined;
    }
    return scalarEqualityCandidateSlots(
      state,
      filter.field,
      valueKeys,
      options.allowScalarIndexBuild,
      options.maxSlotCount,
    );
  }
  if (options.exactRangeCandidates && isRangePredicateFilter(filter)) {
    return rangeCandidateSlots(state, filter.field, filters, options.maxSlotCount);
  }
  return undefined;
};

const scalarEqualityCandidateSlots = (
  state: PredicateCandidateSlotIndexState,
  field: string,
  valueKeys: ReadonlyArray<string>,
  allowIndexBuild: boolean,
  maxSlotCount: number | undefined,
): PredicateCandidateSlots | undefined => {
  if (!state.columns.has(field)) {
    return undefined;
  }
  const index = scalarPredicateIndexForField(state, field, allowIndexBuild);
  if (index === undefined) {
    return undefined;
  }
  const column = state.columns.get(field)!;
  const slots = unionScalarPredicateSlots(index, column, valueKeys, allowIndexBuild, maxSlotCount);
  pruneEmptyScalarPredicateIndex(state.scalarPredicateIndexes, field, index);
  if (slots === undefined) {
    return undefined;
  }
  return { slots: stableCandidateSlotOrder(slots) };
};

const scalarEqualityKeys = (values: ReadonlyArray<unknown>): ReadonlyArray<string> | undefined => {
  const keys: Array<string> = [];
  const seen = new Set<string>();
  for (const value of values) {
    const key = scalarEqualityKey(value);
    if (key === undefined) {
      return undefined;
    }
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    keys.push(key);
  }
  return keys;
};

const scalarPredicateIndexForField = (
  state: PredicateCandidateSlotIndexState,
  field: string,
  allowIndexBuild: boolean,
): ScalarPredicateFieldIndex | undefined => {
  const existing = state.scalarPredicateIndexes.get(field);
  if (existing !== undefined) {
    return existing;
  }
  if (!allowIndexBuild) {
    return undefined;
  }
  const index: ScalarPredicateFieldIndex = {
    buckets: new Map(),
    indexedKeys: new Set(),
  };
  state.scalarPredicateIndexes.set(field, index);
  return index;
};

const unionScalarPredicateSlots = (
  index: ScalarPredicateFieldIndex,
  column: TopicColumnValues,
  valueKeys: ReadonlyArray<string>,
  allowBucketBuild: boolean,
  maxSlotCount: number | undefined,
): ReadonlyArray<number> | undefined => {
  if (valueKeys.length === 0) {
    return [];
  }
  if (valueKeys.length === 1) {
    const bucket = ensureScalarPredicateBucket(
      index,
      column,
      valueKeys[0]!,
      allowBucketBuild,
      maxSlotCount,
    );
    if (bucket === undefined) {
      return undefined;
    }
    if (maxSlotCount !== undefined && bucket.size >= maxSlotCount) {
      return undefined;
    }
    return [...bucket];
  }
  const slots = new Set<number>();
  const missingKeys: Array<string> = [];
  for (const key of valueKeys) {
    if (!index.indexedKeys.has(key)) {
      missingKeys.push(key);
      continue;
    }
    const bucket = index.buckets.get(key)!;
    if (scalarPredicateBucketIsOverRetainedBudget(bucket)) {
      evictScalarPredicateBucket(index, key);
      return undefined;
    }
    for (const slot of bucket) {
      slots.add(slot);
      if (maxSlotCount !== undefined && slots.size >= maxSlotCount) {
        return undefined;
      }
    }
  }
  if (missingKeys.length === 0) {
    return [...slots];
  }
  if (!allowBucketBuild) {
    return undefined;
  }
  const missingBuckets = new Map<string, Set<number>>();
  for (const key of missingKeys) {
    missingBuckets.set(key, new Set());
  }
  for (let slot = 0; slot < column.length; slot += 1) {
    const key = columnScalarEqualityKey(column, slot);
    if (key === undefined) {
      continue;
    }
    const bucket = missingBuckets.get(key);
    if (bucket === undefined) {
      continue;
    }
    bucket.add(slot);
    if (scalarPredicateBucketIsOverRetainedBudget(bucket)) {
      return undefined;
    }
    slots.add(slot);
    if (maxSlotCount !== undefined && slots.size >= maxSlotCount) {
      return undefined;
    }
  }
  for (const [key, bucket] of missingBuckets) {
    if (bucket.size === 0) {
      continue;
    }
    index.indexedKeys.add(key);
    index.buckets.set(key, bucket);
  }
  return [...slots];
};

const ensureScalarPredicateBucket = (
  index: ScalarPredicateFieldIndex,
  column: TopicColumnValues,
  valueKey: string,
  allowBucketBuild: boolean,
  maxSlotCount: number | undefined,
): Set<number> | undefined => {
  if (index.indexedKeys.has(valueKey)) {
    const bucket = index.buckets.get(valueKey)!;
    if (scalarPredicateBucketIsOverRetainedBudget(bucket)) {
      evictScalarPredicateBucket(index, valueKey);
      return undefined;
    }
    return bucket;
  }
  if (!allowBucketBuild) {
    return undefined;
  }

  const bucket = new Set<number>();
  for (let slot = 0; slot < column.length; slot += 1) {
    if (columnScalarEqualityKey(column, slot) !== valueKey) {
      continue;
    }
    bucket.add(slot);
    if (scalarPredicateBucketIsOverRetainedBudget(bucket)) {
      return undefined;
    }
    if (maxSlotCount !== undefined && bucket.size >= maxSlotCount) {
      return undefined;
    }
  }
  if (bucket.size > 0) {
    index.indexedKeys.add(valueKey);
    index.buckets.set(valueKey, bucket);
  }
  return bucket;
};

const rangeCandidateSlots = (
  state: PredicateCandidateSlotIndexState,
  field: string,
  filters: ReadonlyArray<TopicRawPredicateFilterPlan>,
  maxSlotCount: number | undefined,
): PredicateCandidateSlots | undefined => {
  if (!state.columns.has(field)) {
    return undefined;
  }
  const bounds = orderedRangeBoundsForField(filters, field, state.rawQueryMetadata);
  if (bounds === undefined) {
    return undefined;
  }
  if (rangeBoundsAreEmpty(bounds)) {
    return { slots: [] };
  }
  const index = existingRawWindowOrderedSlotIndex(state, [{ field, direction: "asc" }]);
  if (index === undefined) {
    return undefined;
  }
  const spans = rawWindowOrderedSpans(state, index, bounds, undefined);
  const spanSlotCount = orderedRawWindowSpanSlotCount(spans);
  if (spanSlotCount >= state.slots.length) {
    return undefined;
  }
  if (maxSlotCount !== undefined && spanSlotCount >= maxSlotCount) {
    return undefined;
  }
  return {
    slots: stableCandidateSlotOrder(slotsFromOrderedSpans(index.slots, spans)),
  };
};

const stableCandidateSlotOrder = (slots: ReadonlyArray<number>): ReadonlyArray<number> =>
  slots.toSorted((left, right) => left - right);

const slotsFromOrderedSpans = (
  orderedSlots: ReadonlyArray<number>,
  spans: ReturnType<typeof rawWindowOrderedSpans>,
): ReadonlyArray<number> => {
  const slots: Array<number> = [];
  for (const span of spans) {
    for (let index = span.startIndex; index < span.endIndex; index += 1) {
      slots.push(orderedSlots[index]!);
    }
  }
  return slots;
};

const addSlotToScalarPredicateIndex = (
  index: ScalarPredicateFieldIndex,
  column: TopicColumnValues | undefined,
  slot: number,
): void => {
  if (column === undefined) {
    return;
  }
  const key = columnScalarEqualityKey(column, slot);
  if (key === undefined || !index.indexedKeys.has(key)) {
    return;
  }
  const bucket = index.buckets.get(key)!;
  bucket.add(slot);
  if (scalarPredicateBucketIsOverRetainedBudget(bucket)) {
    evictScalarPredicateBucket(index, key);
  }
};

const removeSlotFromScalarPredicateIndex = (
  index: ScalarPredicateFieldIndex,
  column: TopicColumnValues | undefined,
  slot: number,
): void => {
  if (column === undefined) {
    return;
  }
  const key = columnScalarEqualityKey(column, slot);
  if (key === undefined || !index.indexedKeys.has(key)) {
    return;
  }
  index.buckets.get(key)!.delete(slot);
};

const scalarPredicateBucketIsOverRetainedBudget = (bucket: Set<number>): boolean =>
  bucket.size > maxRetainedScalarPredicateBucketSlots;

const evictScalarPredicateBucket = (index: ScalarPredicateFieldIndex, key: string): void => {
  index.indexedKeys.delete(key);
  index.buckets.delete(key);
};

const pruneEmptyScalarPredicateIndex = (
  indexes: ScalarPredicateIndexes,
  field: string,
  index: ScalarPredicateFieldIndex,
): void => {
  if (index.indexedKeys.size === 0) {
    indexes.delete(field);
  }
};

const isRangePredicateFilter = (
  filter: TopicRawPredicateFilterPlan,
): filter is RangePredicateFilter =>
  filter.operator === "gt" ||
  filter.operator === "gte" ||
  filter.operator === "lt" ||
  filter.operator === "lte";
