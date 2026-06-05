import { valuesEqual } from "./row-values";
import { isBigDecimal, Order as orderBigDecimal } from "effect/BigDecimal";

export const isComparableRangeValue = (value: unknown): boolean =>
  (typeof value === "number" && Number.isFinite(value)) ||
  typeof value === "bigint" ||
  isBigDecimal(value);

export const compareExactRangeColumnValue = (left: unknown, right: unknown): number | undefined => {
  if (typeof left === "number" && typeof right === "number") {
    if (!Number.isFinite(left) || !Number.isFinite(right)) {
      return undefined;
    }
    return left === right ? 0 : left < right ? -1 : 1;
  }
  if (typeof left === "bigint" && typeof right === "bigint") {
    if (left === right) {
      return 0;
    }
    return left < right ? -1 : 1;
  }
  if (isBigDecimal(left) && isBigDecimal(right)) {
    return orderBigDecimal(left, right);
  }
  return undefined;
};

export const rangeComparisonMatches = (
  operator: "gt" | "gte" | "lt" | "lte",
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

export const columnValueDoesNotEqual = (value: unknown, notEqual: unknown): boolean => {
  return equalityComparableValues(value, notEqual) && !valuesEqual(value, notEqual);
};

export const compareRangeColumnValue = (left: unknown, right: unknown): number | undefined => {
  if (typeof left === "number" && typeof right === "number") {
    if (!Number.isFinite(left) || !Number.isFinite(right)) {
      return undefined;
    }
    return left - right;
  }
  if (typeof left === "bigint" && typeof right === "bigint") {
    if (left === right) {
      return 0;
    }
    return left < right ? -1 : 1;
  }
  if (isBigDecimal(left) && isBigDecimal(right)) {
    return orderBigDecimal(left, right);
  }
  return undefined;
};

const equalityComparableValues = (left: unknown, right: unknown): boolean => {
  if (isBigDecimal(left) || isBigDecimal(right)) {
    return isBigDecimal(left) && isBigDecimal(right);
  }
  if (typeof left === "number" || typeof right === "number") {
    return typeof left === "number" && typeof right === "number" && Number.isFinite(right);
  }
  return typeof left === typeof right;
};
