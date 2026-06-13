import type { TopicRawPredicateFilterPlan } from "./raw-predicate-plan";
import { valuesEqual } from "./row-values";
import {
  columnValueDoesNotEqual,
  compareExactRangeColumnValue,
  compareRangeColumnValue,
  isComparableRangeValue,
} from "./topic-range-value";
import {
  columnScalarEqualityKey,
  columnValue,
  type TopicColumnValues,
} from "./topic-column-vector";
import { equals as bigDecimalEquals, isBigDecimal } from "effect/BigDecimal";

export type SlotFilterMatcher = (slot: number) => boolean;
type RangePredicateFilter = TopicRawPredicateFilterPlan & {
  readonly operator: "gt" | "gte" | "lt" | "lte";
  readonly value: unknown;
};

export const rawPredicateSlotFilterMatcher = (
  filters: ReadonlyArray<TopicRawPredicateFilterPlan>,
  columns: ReadonlyMap<string, TopicColumnValues>,
  exact: boolean,
): SlotFilterMatcher => {
  const filterMatchers = slotFilterMatchers(filters, columns, exact);
  return (slot) => {
    for (const matcher of filterMatchers) {
      if (!matcher(slot)) {
        return false;
      }
    }
    return true;
  };
};

const slotFilterMatchers = (
  filters: ReadonlyArray<TopicRawPredicateFilterPlan>,
  columns: ReadonlyMap<string, TopicColumnValues>,
  exact: boolean,
): ReadonlyArray<SlotFilterMatcher> => {
  const matchers: Array<SlotFilterMatcher> = [];
  for (const filter of filters) {
    matchers.push(slotFilterMatcher(filter, columns, exact));
  }
  return matchers;
};

const slotFilterMatcher = (
  filter: TopicRawPredicateFilterPlan,
  columns: ReadonlyMap<string, TopicColumnValues>,
  exact: boolean,
): SlotFilterMatcher => {
  const column = columns.get(filter.field);
  if (column === undefined) {
    return () => true;
  }

  switch (filter.operator) {
    case "eq": {
      if (column.kind === "string" && typeof filter.value === "string") {
        return (slot) => column.stringAt(slot) === filter.value;
      }
      if (column.kind === "number" && typeof filter.value === "number") {
        return (slot) => Object.is(column.numberAt(slot), filter.value);
      }
      if (column.kind === "bigint" && typeof filter.value === "bigint") {
        return (slot) => column.bigintAt(slot) === filter.value;
      }
      if (column.kind === "bigDecimal" && isBigDecimal(filter.value)) {
        const expected = filter.value;
        return (slot) => {
          const value = column.bigDecimalAt(slot);
          return value !== undefined && bigDecimalEquals(value, expected);
        };
      }
      return (slot) => valuesEqual(columnValue(column, slot), filter.value);
    }
    case "neq": {
      if (exact) {
        if (column.kind === "string" && typeof filter.value === "string") {
          return (slot) => {
            const value = column.stringAt(slot);
            return value !== undefined && value !== filter.value;
          };
        }
        if (column.kind === "number" && typeof filter.value === "number") {
          return (slot) => {
            const value = column.numberAt(slot);
            return value !== undefined && !Object.is(value, filter.value);
          };
        }
        if (column.kind === "bigint" && typeof filter.value === "bigint") {
          return (slot) => {
            const value = column.bigintAt(slot);
            return value !== undefined && value !== filter.value;
          };
        }
        if (column.kind === "bigDecimal" && isBigDecimal(filter.value)) {
          const expected = filter.value;
          return (slot) => {
            const value = column.bigDecimalAt(slot);
            return value !== undefined && !bigDecimalEquals(value, expected);
          };
        }
        return (slot) => columnValueDoesNotEqual(columnValue(column, slot), filter.value);
      }
      return (slot) => !valuesEqual(columnValue(column, slot), filter.value);
    }
    case "in": {
      if (filter.valueKeys !== undefined) {
        const valueKeys = filter.valueKeys;
        return (slot) => {
          const key = columnScalarEqualityKey(column, slot);
          return key !== undefined && valueKeys.has(key);
        };
      }
      return (slot) => {
        const value = columnValue(column, slot);
        return filter.values.some((candidate) => valuesEqual(value, candidate));
      };
    }
    case "startsWith": {
      if (typeof filter.value !== "string") {
        return () => true;
      }
      const prefix = filter.value;
      if (column.kind === "string") {
        return (slot) => column.stringAt(slot)?.startsWith(prefix) === true;
      }
      return (slot) => {
        const value = columnValue(column, slot);
        return typeof value === "string" && value.startsWith(prefix);
      };
    }
    case "gt":
    case "gte":
    case "lt":
    case "lte": {
      const rangeFilter: RangePredicateFilter = {
        field: filter.field,
        operator: filter.operator,
        value: filter.value,
      };
      return rangeSlotFilterMatcher(column, rangeFilter, exact);
    }
  }
};

const rangeSlotFilterMatcher = (
  column: TopicColumnValues,
  filter: RangePredicateFilter,
  exact: boolean,
): SlotFilterMatcher => {
  if (exact && !isComparableRangeValue(filter.value)) {
    return () => true;
  }
  const numberRangeMatcher = numberColumnRangeMatcher(column, filter, exact);
  if (numberRangeMatcher !== undefined) {
    return numberRangeMatcher;
  }
  const bigintRangeMatcher = bigintColumnRangeMatcher(column, filter, exact);
  if (bigintRangeMatcher !== undefined) {
    return bigintRangeMatcher;
  }
  const bigDecimalRangeMatcher = bigDecimalColumnRangeMatcher(column, filter, exact);
  if (bigDecimalRangeMatcher !== undefined) {
    return bigDecimalRangeMatcher;
  }

  if (exact) {
    return (slot) => {
      const exactComparison = compareExactRangeColumnValue(columnValue(column, slot), filter.value);
      if (exactComparison === undefined) {
        return false;
      }
      return rangeComparisonMatches(filter.operator, exactComparison);
    };
  }

  return (slot) => {
    const comparison = compareRangeColumnValue(columnValue(column, slot), filter.value);
    return comparison === undefined || rangeComparisonMatches(filter.operator, comparison);
  };
};

const bigintColumnRangeMatcher = (
  column: TopicColumnValues,
  filter: RangePredicateFilter,
  exact: boolean,
): SlotFilterMatcher | undefined => {
  if (column.kind !== "bigint" || typeof filter.value !== "bigint") {
    return undefined;
  }
  const expected = filter.value;
  return rangeColumnMatcher(
    exact,
    (slot) => {
      const value = column.bigintAt(slot);
      if (value === undefined) {
        return undefined;
      }
      return value === expected ? 0 : value < expected ? -1 : 1;
    },
    filter.operator,
  );
};

const bigDecimalColumnRangeMatcher = (
  column: TopicColumnValues,
  filter: RangePredicateFilter,
  exact: boolean,
): SlotFilterMatcher | undefined => {
  if (column.kind !== "bigDecimal" || !isBigDecimal(filter.value)) {
    return undefined;
  }
  const expected = filter.value;
  return rangeColumnMatcher(
    exact,
    (slot) => {
      const value = column.bigDecimalAt(slot);
      if (value === undefined) {
        return undefined;
      }
      return compareExactRangeColumnValue(value, expected);
    },
    filter.operator,
  );
};

const numberColumnRangeMatcher = (
  column: TopicColumnValues,
  filter: RangePredicateFilter,
  exact: boolean,
): SlotFilterMatcher | undefined => {
  if (
    column.kind !== "number" ||
    typeof filter.value !== "number" ||
    !Number.isFinite(filter.value)
  ) {
    return undefined;
  }
  const expected = filter.value;
  if (filter.operator === "gt") {
    return exact
      ? (slot) => {
          const value = column.numberAt(slot);
          return value !== undefined && Number.isFinite(value) && value > expected;
        }
      : (slot) => {
          const value = column.numberAt(slot);
          return value === undefined || !Number.isFinite(value) || value > expected;
        };
  }
  if (filter.operator === "gte") {
    return exact
      ? (slot) => {
          const value = column.numberAt(slot);
          return value !== undefined && Number.isFinite(value) && value >= expected;
        }
      : (slot) => {
          const value = column.numberAt(slot);
          return value === undefined || !Number.isFinite(value) || value >= expected;
        };
  }
  if (filter.operator === "lt") {
    return exact
      ? (slot) => {
          const value = column.numberAt(slot);
          return value !== undefined && Number.isFinite(value) && value < expected;
        }
      : (slot) => {
          const value = column.numberAt(slot);
          return value === undefined || !Number.isFinite(value) || value < expected;
        };
  }
  return exact
    ? (slot) => {
        const value = column.numberAt(slot);
        return value !== undefined && Number.isFinite(value) && value <= expected;
      }
    : (slot) => {
        const value = column.numberAt(slot);
        return value === undefined || !Number.isFinite(value) || value <= expected;
      };
};

const rangeColumnMatcher = (
  exact: boolean,
  compareSlot: (slot: number) => number | undefined,
  operator: TopicRawPredicateFilterPlan["operator"],
): SlotFilterMatcher =>
  exact
    ? (slot) => {
        const comparison = compareSlot(slot);
        return comparison !== undefined && rangeComparisonMatches(operator, comparison);
      }
    : (slot) => {
        const comparison = compareSlot(slot);
        return comparison === undefined || rangeComparisonMatches(operator, comparison);
      };

const rangeComparisonMatches = (
  operator: TopicRawPredicateFilterPlan["operator"],
  comparison: number,
): boolean => {
  if (operator === "gt") {
    return comparison > 0;
  }
  if (operator === "gte") {
    return comparison >= 0;
  }
  if (operator === "lt") {
    return comparison < 0;
  }
  return comparison <= 0;
};
