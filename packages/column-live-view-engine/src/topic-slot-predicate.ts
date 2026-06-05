import type { TopicRawPredicateFilterPlan } from "./raw-predicate-plan";
import type { TopicRawWindowScanPlan } from "./raw-window-scan";
import { scalarEqualityKey, valuesEqual } from "./row-values";
import {
  columnValueDoesNotEqual,
  compareExactRangeColumnValue,
  compareRangeColumnValue,
  isComparableRangeValue,
} from "./topic-range-value";

type RowObject = object;
type ColumnValues = ReadonlyArray<unknown>;

export const slotMatchesRawPredicatePlan = <Row extends RowObject>(
  slot: number,
  plan: TopicRawWindowScanPlan<Row>,
  row: Row,
  columns: ReadonlyMap<string, ColumnValues>,
): boolean => {
  const exact = plan.predicate.callbackSkippable === true;
  if (!slotMayMatchFilters(slot, plan.predicate.filters, columns, exact)) {
    return false;
  }
  return exact || plan.matches(row);
};

const slotMayMatchFilters = (
  slot: number,
  filters: ReadonlyArray<TopicRawPredicateFilterPlan>,
  columns: ReadonlyMap<string, ColumnValues>,
  exact: boolean,
): boolean => {
  for (const filter of filters) {
    if (!slotMayMatchFilter(slot, filter, columns, exact)) {
      return false;
    }
  }
  return true;
};

const slotMayMatchFilter = (
  slot: number,
  filter: TopicRawPredicateFilterPlan,
  columns: ReadonlyMap<string, ColumnValues>,
  exact: boolean,
): boolean => {
  const column = columns.get(filter.field);
  if (column === undefined) {
    return true;
  }
  const value = column[slot];

  if (filter.operator === "eq") {
    return valuesEqual(value, filter.value);
  }
  if (filter.operator === "neq") {
    if (exact) {
      return columnValueDoesNotEqual(value, filter.value);
    }
    return !valuesEqual(value, filter.value);
  }
  if (filter.operator === "in") {
    if (filter.valueKeys !== undefined) {
      const key = scalarEqualityKey(value);
      return key !== undefined && filter.valueKeys.has(key);
    }
    return filter.values.some((candidate) => valuesEqual(value, candidate));
  }
  if (filter.operator === "startsWith") {
    if (typeof filter.value !== "string") {
      return true;
    }
    return typeof value === "string" && value.startsWith(filter.value);
  }

  if (exact && !isComparableRangeValue(filter.value)) {
    return true;
  }
  if (exact) {
    const exactComparison = compareExactRangeColumnValue(value, filter.value);
    if (exactComparison === undefined) {
      return false;
    }
    if (filter.operator === "gt") {
      return exactComparison > 0;
    }
    if (filter.operator === "gte") {
      return exactComparison >= 0;
    }
    if (filter.operator === "lt") {
      return exactComparison < 0;
    }
    return exactComparison <= 0;
  }

  const comparison = compareRangeColumnValue(value, filter.value);
  if (comparison === undefined) {
    return true;
  }
  if (filter.operator === "gt") {
    return comparison > 0;
  }
  if (filter.operator === "gte") {
    return comparison >= 0;
  }
  if (filter.operator === "lt") {
    return comparison < 0;
  }
  return comparison <= 0;
};
