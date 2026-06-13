import type { TopicRawPredicateFilterPlan } from "./raw-predicate-plan";
import type { TopicRawOrderByPlan } from "./raw-window-scan";
import {
  compareQueryValue,
  isRangePlanValue,
  type RawQueryCompilerMetadata,
} from "./raw-query-compiler";
import { scalarEqualityKey } from "./row-values";
import { columnValue, type TopicColumnValues } from "./topic-column-vector";
import { Order as orderBigDecimal, isBigDecimal } from "effect/BigDecimal";

export type RawStorageOrderColumn = {
  readonly compareSlots: (left: number, right: number) => number;
  readonly direction: "asc" | "desc";
};

export type OrderedSlotIndex = {
  readonly orderColumns: ReadonlyArray<RawStorageOrderColumn>;
  readonly orderBy: ReadonlyArray<TopicRawOrderByPlan>;
  readonly slots: Array<number>;
};

export type OrderedRawWindowSpan = {
  readonly endIndex: number;
  readonly startIndex: number;
};

export type OrderedRawWindow = {
  readonly candidateExcludedField: string;
  readonly limit: number;
  readonly slots: ReadonlyArray<number>;
  readonly spans: ReadonlyArray<OrderedRawWindowSpan>;
};

export type OrderedRangeBound = {
  readonly exclusive: boolean;
  readonly value: unknown;
};

export type OrderedRangeBounds = {
  readonly lower: OrderedRangeBound | undefined;
  readonly upper: OrderedRangeBound | undefined;
};

type TopicRawRangePredicateFilterPlan = TopicRawPredicateFilterPlan & {
  readonly operator: "gt" | "gte" | "lt" | "lte";
  readonly value: unknown;
};

type TopicRawEqualityPredicateFilterPlan = TopicRawPredicateFilterPlan & {
  readonly operator: "eq";
  readonly value: unknown;
};

type TopicRawInPredicateFilterPlan = TopicRawPredicateFilterPlan & {
  readonly operator: "in";
  readonly values: ReadonlyArray<unknown>;
};

export const noOrderedRangeBounds: OrderedRangeBounds = {
  lower: undefined,
  upper: undefined,
};

export const orderedSlotIndexKey = (orderBy: ReadonlyArray<TopicRawOrderByPlan>): string => {
  const order = orderBy[0]!;
  return `${order.field.length}:${order.field}:${order.direction}`;
};

export const orderedRawWindowSlotCount = (window: OrderedRawWindow): number => {
  return orderedRawWindowSpanSlotCount(window.spans);
};

export const orderedRawWindowSpanSlotCount = (
  spans: ReadonlyArray<OrderedRawWindowSpan>,
): number => {
  let count = 0;
  for (const span of spans) {
    count += span.endIndex - span.startIndex;
  }
  return count;
};

export const orderedSlotIndexInsertionPoint = (
  slots: ReadonlyArray<number>,
  slot: number,
  compareSlots: (left: number, right: number) => number,
): number => {
  let low = 0;
  let high = slots.length;
  while (low < high) {
    const middle = Math.floor((low + high) / 2);
    if (compareSlots(slots[middle]!, slot) <= 0) {
      low = middle + 1;
    } else {
      high = middle;
    }
  }
  return low;
};

export const predicateFiltersAreOrderedIndexAdmissible = (
  filters: ReadonlyArray<TopicRawPredicateFilterPlan>,
  orderField: string,
): boolean => {
  for (const filter of filters) {
    if (filter.operator === "eq" || filter.operator === "in") {
      continue;
    }
    if (isRangeFilterPlan(filter) && filter.field === orderField) {
      continue;
    }
    return false;
  }
  return true;
};

export const orderedEqualityValuesForField = (
  filters: ReadonlyArray<TopicRawPredicateFilterPlan>,
  field: string,
  metadata: RawQueryCompilerMetadata,
): ReadonlyArray<unknown> | undefined => {
  let values: ReadonlyArray<unknown> = [];
  let hasEqualityFilter = false;
  let hasSafeEqualityFilter = false;
  let hasUnsafeEqualityFilter = false;
  let hasEmptyInFilter = false;
  for (const filter of filters) {
    if (filter.field !== field || !isEqualityFilterPlan(filter)) {
      continue;
    }
    hasEqualityFilter = true;
    const nextValues: Array<unknown> = [];
    if (filter.operator === "eq") {
      if (isEqualitySeekPlanValue(field, filter.value, metadata)) {
        nextValues.push(filter.value);
      } else {
        hasUnsafeEqualityFilter = true;
      }
    } else {
      const seekValues = orderedInSeekValues(filter, field, metadata);
      if (seekValues === undefined) {
        hasUnsafeEqualityFilter = true;
      } else if (seekValues.length === 0) {
        hasEmptyInFilter = true;
      } else {
        nextValues.push(...seekValues);
      }
    }
    if (nextValues.length > 0) {
      if (!hasSafeEqualityFilter) {
        values = nextValues;
        hasSafeEqualityFilter = true;
      } else {
        values = intersectOrderedEqualityValues(values, nextValues);
      }
    }
  }
  if (hasEmptyInFilter) {
    return [];
  }
  if (hasUnsafeEqualityFilter || !hasEqualityFilter) {
    return undefined;
  }
  return values;
};

export const orderedRangeBoundsForField = (
  filters: ReadonlyArray<TopicRawPredicateFilterPlan>,
  field: string,
  metadata: RawQueryCompilerMetadata,
): OrderedRangeBounds | undefined => {
  let lower: OrderedRangeBound | undefined;
  let upper: OrderedRangeBound | undefined;
  for (const filter of filters) {
    if (filter.field !== field || !isRangeFilterPlan(filter)) {
      continue;
    }
    if (!isRangePlanValue(field, filter.value, metadata)) {
      return undefined;
    }
    switch (filter.operator) {
      case "gt": {
        lower = strongerLowerBound(lower, {
          exclusive: true,
          value: filter.value,
        });
        break;
      }
      case "gte": {
        lower = strongerLowerBound(lower, {
          exclusive: false,
          value: filter.value,
        });
        break;
      }
      case "lt": {
        upper = strongerUpperBound(upper, {
          exclusive: true,
          value: filter.value,
        });
        break;
      }
      case "lte": {
        upper = strongerUpperBound(upper, {
          exclusive: false,
          value: filter.value,
        });
        break;
      }
    }
  }
  return {
    lower,
    upper,
  };
};

export const rangeBoundsAreEmpty = (bounds: OrderedRangeBounds): boolean => {
  if (bounds.lower === undefined || bounds.upper === undefined) {
    return false;
  }
  const comparison = compareOrderedRangeValue(bounds.lower.value, bounds.upper.value);
  if (comparison > 0) {
    return true;
  }
  return comparison === 0 && (bounds.lower.exclusive || bounds.upper.exclusive);
};

export const orderedSlotBoundIndex = (
  slots: ReadonlyArray<number>,
  column: TopicColumnValues,
  value: unknown,
  predicate: (comparison: number) => boolean,
): number => {
  let low = 0;
  let high = slots.length;
  while (low < high) {
    const middle = Math.floor((low + high) / 2);
    const comparison = compareOrderedSlotRangeValue(column, slots[middle]!, value);
    if (predicate(comparison)) {
      high = middle;
    } else {
      low = middle + 1;
    }
  }
  return low;
};

const compareOrderedSlotRangeValue = (
  column: TopicColumnValues,
  slot: number,
  value: unknown,
): number => {
  if (column.kind === "string" && typeof value === "string") {
    const slotValue = column.stringAt(slot);
    if (slotValue !== undefined) {
      return slotValue === value ? 0 : slotValue < value ? -1 : 1;
    }
  }
  if (column.kind === "number" && typeof value === "number") {
    const slotValue = column.numberAt(slot);
    if (slotValue !== undefined && Number.isFinite(slotValue) && Number.isFinite(value)) {
      return slotValue === value ? 0 : slotValue < value ? -1 : 1;
    }
  }
  if (column.kind === "bigint" && typeof value === "bigint") {
    const slotValue = column.bigintAt(slot);
    if (slotValue !== undefined) {
      return slotValue === value ? 0 : slotValue < value ? -1 : 1;
    }
  }
  if (column.kind === "bigDecimal" && isBigDecimal(value)) {
    const slotValue = column.bigDecimalAt(slot);
    if (slotValue !== undefined) {
      return orderBigDecimal(slotValue, value);
    }
  }
  return compareOrderedRangeValue(columnValue(column, slot), value);
};

export const orderedWindowSpansInIndexOrder = (
  spans: ReadonlyArray<OrderedRawWindowSpan>,
): ReadonlyArray<OrderedRawWindowSpan> => {
  return spans
    .filter((span) => span.startIndex < span.endIndex)
    .toSorted((left, right) => left.startIndex - right.startIndex);
};

export const equalityValueSatisfiesRangeBounds = (
  value: unknown,
  rangeBounds: OrderedRangeBounds,
): boolean => {
  if (rangeBounds.lower !== undefined) {
    const comparison = compareOrderedRangeValue(value, rangeBounds.lower.value);
    if (comparison < 0 || (comparison === 0 && rangeBounds.lower.exclusive)) {
      return false;
    }
  }
  if (rangeBounds.upper !== undefined) {
    const comparison = compareOrderedRangeValue(value, rangeBounds.upper.value);
    if (comparison > 0 || (comparison === 0 && rangeBounds.upper.exclusive)) {
      return false;
    }
  }
  return true;
};

const isEqualitySeekPlanValue = (
  field: string,
  value: unknown,
  metadata: RawQueryCompilerMetadata,
): boolean => {
  if (metadata.stringFieldNames.has(field)) {
    return typeof value === "string";
  }
  return isRangePlanValue(field, value, metadata);
};

const orderedInSeekValues = (
  filter: TopicRawInPredicateFilterPlan,
  field: string,
  metadata: RawQueryCompilerMetadata,
): ReadonlyArray<unknown> | undefined => {
  const values: Array<unknown> = [];
  const valueKeys = new Set<string>();
  for (const value of filter.values) {
    if (!isEqualitySeekPlanValue(field, value, metadata)) {
      return undefined;
    }
    values.push(value);
    valueKeys.add(scalarEqualityKey(value)!);
  }
  if (filter.valueKeys === undefined) {
    return values;
  }
  if (valueKeys.size !== filter.valueKeys.size) {
    return undefined;
  }
  for (const key of filter.valueKeys) {
    if (!valueKeys.has(key)) {
      return undefined;
    }
  }
  return values;
};

const strongerLowerBound = (
  current: OrderedRangeBound | undefined,
  candidate: OrderedRangeBound,
): OrderedRangeBound => {
  if (current === undefined) {
    return candidate;
  }
  const comparison = compareOrderedRangeValue(candidate.value, current.value);
  if (comparison > 0) {
    return candidate;
  }
  if (comparison === 0 && candidate.exclusive && !current.exclusive) {
    return candidate;
  }
  return current;
};

const strongerUpperBound = (
  current: OrderedRangeBound | undefined,
  candidate: OrderedRangeBound,
): OrderedRangeBound => {
  if (current === undefined) {
    return candidate;
  }
  const comparison = compareOrderedRangeValue(candidate.value, current.value);
  if (comparison < 0) {
    return candidate;
  }
  if (comparison === 0 && candidate.exclusive && !current.exclusive) {
    return candidate;
  }
  return current;
};

export const distinctOrderedEqualityValues = (
  values: ReadonlyArray<unknown>,
): ReadonlyArray<unknown> => {
  const sorted = values.toSorted(compareOrderedRangeValue);
  const distinct: Array<unknown> = [];
  for (const value of sorted) {
    const previous = distinct.at(-1);
    if (previous === undefined || compareOrderedRangeValue(previous, value) !== 0) {
      distinct.push(value);
    }
  }
  return distinct;
};

const intersectOrderedEqualityValues = (
  leftValues: ReadonlyArray<unknown>,
  rightValues: ReadonlyArray<unknown>,
): ReadonlyArray<unknown> => {
  const left = distinctOrderedEqualityValues(leftValues);
  const right = distinctOrderedEqualityValues(rightValues);
  const intersection: Array<unknown> = [];
  let rightIndex = 0;
  for (const leftValue of left) {
    while (
      rightIndex < right.length &&
      compareOrderedRangeValue(right[rightIndex]!, leftValue) < 0
    ) {
      rightIndex += 1;
    }
    if (
      rightIndex < right.length &&
      compareOrderedRangeValue(right[rightIndex]!, leftValue) === 0
    ) {
      intersection.push(leftValue);
    }
  }
  return intersection;
};

const compareOrderedRangeValue = (left: unknown, right: unknown): number =>
  compareQueryValue(left, right);

const isEqualityFilterPlan = (
  filter: TopicRawPredicateFilterPlan,
): filter is TopicRawEqualityPredicateFilterPlan | TopicRawInPredicateFilterPlan => {
  if (filter.operator === "eq" || filter.operator === "in") {
    return true;
  }
  return false;
};

const isRangeFilterPlan = (
  filter: TopicRawPredicateFilterPlan,
): filter is TopicRawRangePredicateFilterPlan => {
  if (
    filter.operator === "gt" ||
    filter.operator === "gte" ||
    filter.operator === "lt" ||
    filter.operator === "lte"
  ) {
    return true;
  }
  return false;
};
