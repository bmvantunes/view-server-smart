import type { TopicRawPredicateFilterPlan } from "./row-scan";
import { scalarEqualityKey } from "./row-values";
import {
  compareExactRangeColumnValue,
  isComparableRangeValue,
  rangeComparisonMatches,
} from "./topic-range-value";

type ColumnValues = ReadonlyArray<unknown>;

type TopicRawRangePredicateFilterPlan = TopicRawPredicateFilterPlan & {
  readonly operator: "gt" | "gte" | "lt" | "lte";
  readonly value: unknown;
};

type PredicateCandidateSample = {
  readonly sampleMatches: number;
  readonly sampleSize: number;
};

export type PredicateCandidateFilter = {
  readonly column: ColumnValues;
  readonly matches: (value: unknown) => boolean;
  readonly sampleMatches: number;
};

const maxPredicateCandidateSampleSlots = 4_096;
const maxTransientPredicateCandidateSlots = 65_536;

export const selectedPredicateCandidateFilter = (
  filters: ReadonlyArray<TopicRawPredicateFilterPlan>,
  columns: ReadonlyMap<string, ColumnValues>,
  rowCount: number,
  excludedField?: string,
): PredicateCandidateFilter | undefined => {
  let selectedFilter: PredicateCandidateFilter | undefined;
  for (const filter of filters) {
    if (filter.field === excludedField) {
      continue;
    }
    const candidateFilter = predicateCandidateFilter(filter, columns, rowCount);
    if (candidateFilter === undefined) {
      continue;
    }
    if (
      selectedFilter === undefined ||
      candidateFilter.sampleMatches < selectedFilter.sampleMatches
    ) {
      selectedFilter = candidateFilter;
    }
  }
  if (selectedFilter === undefined) {
    return undefined;
  }
  return selectedFilter;
};

const predicateCandidateFilter = (
  filter: TopicRawPredicateFilterPlan,
  columns: ReadonlyMap<string, ColumnValues>,
  rowCount: number,
): PredicateCandidateFilter | undefined => {
  if (filter.operator === "eq") {
    const key = scalarEqualityKey(filter.value);
    if (key === undefined) {
      return undefined;
    }
    const column = columns.get(filter.field);
    if (column === undefined) {
      return undefined;
    }
    return scalarColumnCandidateFilter(column, new Set([key]), rowCount);
  }
  if (filter.operator === "in" && filter.valueKeys !== undefined) {
    const column = columns.get(filter.field);
    if (column === undefined) {
      return undefined;
    }
    return scalarColumnCandidateFilter(column, filter.valueKeys, rowCount);
  }
  if (!isRangeFilterPlan(filter) || !isComparableRangeValue(filter.value)) {
    return undefined;
  }
  const column = columns.get(filter.field);
  if (column === undefined) {
    return undefined;
  }
  return rangeColumnCandidateFilter(column, filter.operator, filter.value, rowCount);
};

const scalarColumnCandidateFilter = (
  column: ColumnValues,
  valueKeys: ReadonlySet<string>,
  rowCount: number,
): PredicateCandidateFilter | undefined => {
  const matches = (value: unknown) => {
    const key = scalarEqualityKey(value);
    return key !== undefined && valueKeys.has(key);
  };
  return predicateColumnCandidateFilter(column, matches, rowCount);
};

const rangeColumnCandidateFilter = (
  column: ColumnValues,
  operator: TopicRawRangePredicateFilterPlan["operator"],
  value: unknown,
  rowCount: number,
): PredicateCandidateFilter | undefined => {
  const matches = (columnValue: unknown) => {
    const comparison = compareExactRangeColumnValue(columnValue, value);
    return comparison !== undefined && rangeComparisonMatches(operator, comparison);
  };
  return predicateColumnCandidateFilter(column, matches, rowCount);
};

const predicateColumnCandidateFilter = (
  column: ColumnValues,
  matches: (value: unknown) => boolean,
  rowCount: number,
): PredicateCandidateFilter | undefined => {
  const sample = predicateColumnCandidateSample(column, matches, rowCount);
  if (sample === undefined) {
    return undefined;
  }
  if (sample.sampleMatches === 0 && sample.sampleSize < rowCount) {
    return undefined;
  }
  return {
    column,
    matches,
    sampleMatches: sample.sampleMatches,
  };
};

const predicateColumnCandidateSample = (
  column: ColumnValues,
  matches: (value: unknown) => boolean,
  rowCount: number,
): PredicateCandidateSample | undefined => {
  const sampleSize = Math.min(rowCount, maxPredicateCandidateSampleSlots);
  const maxMatches = predicateCandidateSampleMatchLimit(sampleSize);
  let matchCount = 0;
  for (let sampleIndex = 0; sampleIndex < sampleSize; sampleIndex += 1) {
    const slot = predicateCandidateSampleSlot(rowCount, sampleSize, sampleIndex);
    if (!matches(column[slot])) {
      continue;
    }
    matchCount += 1;
    if (matchCount > maxMatches) {
      return undefined;
    }
  }
  if (predicateCandidateSampleExceedsSlotLimit(matchCount, sampleSize, rowCount)) {
    return undefined;
  }
  return {
    sampleMatches: matchCount,
    sampleSize,
  };
};

const predicateCandidateSlotLimit = (rowCount: number): number =>
  Math.min(maxTransientPredicateCandidateSlots, Math.floor(rowCount / 2));

const predicateCandidateSampleMatchLimit = (sampleSize: number): number =>
  Math.max(8, Math.floor(sampleSize / 8));

const predicateCandidateSampleSlot = (
  rowCount: number,
  sampleSize: number,
  sampleIndex: number,
): number => {
  if (sampleSize <= 1) {
    return 0;
  }
  return Math.round((sampleIndex * (rowCount - 1)) / (sampleSize - 1));
};

const predicateCandidateSampleExceedsSlotLimit = (
  sampleMatches: number,
  sampleSize: number,
  rowCount: number,
): boolean => {
  return (
    sampleMatches > 0 &&
    sampleSize > 0 &&
    sampleMatches * rowCount > predicateCandidateSlotLimit(rowCount) * sampleSize
  );
};

const isRangeFilterPlan = (
  filter: TopicRawPredicateFilterPlan,
): filter is TopicRawRangePredicateFilterPlan =>
  filter.operator === "gt" ||
  filter.operator === "gte" ||
  filter.operator === "lt" ||
  filter.operator === "lte";
