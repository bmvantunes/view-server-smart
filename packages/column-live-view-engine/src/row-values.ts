import { equals, isBigDecimal } from "effect/BigDecimal";

type RowObject = object;

export const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const isPlainRecord = (value: unknown): value is Record<string, unknown> => {
  if (!isRecord(value) || isBigDecimal(value)) {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype;
};

export const cloneUnknown = (value: unknown): unknown => {
  if (isBigDecimal(value)) {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map(cloneUnknown);
  }
  if (isPlainRecord(value)) {
    return cloneRecord(value);
  }
  if (typeof value !== "object" || value === null) {
    return value;
  }
  return structuredClone(value);
};

export const cloneRecord = (value: Record<string, unknown>): Record<string, unknown> => {
  const cloned: Record<string, unknown> = {};
  for (const [key, entry] of Object.entries(value)) {
    cloned[key] = cloneUnknown(entry);
  }
  return cloned;
};

export const cloneRow = <Row extends RowObject>(row: Row): Row => {
  const cloned: Record<string, unknown> = {};
  for (const [key, entry] of Object.entries(row)) {
    cloned[key] = cloneUnknown(entry);
  }
  return cloned as Row;
};

export const fieldValue = (row: RowObject, field: string): unknown => {
  for (const [key, value] of Object.entries(row)) {
    if (key === field) {
      return value;
    }
  }
  return undefined;
};

export const valuesEqual = (left: unknown, right: unknown): boolean => {
  if (isBigDecimal(left) && isBigDecimal(right)) {
    return equals(left, right);
  }
  if (Array.isArray(left) && Array.isArray(right)) {
    return (
      left.length === right.length && left.every((entry, index) => valuesEqual(entry, right[index]))
    );
  }
  if (isPlainRecord(left) && isPlainRecord(right)) {
    const leftEntries = Object.entries(left);
    const rightKeys = new Set(Object.keys(right));
    return (
      leftEntries.length === rightKeys.size &&
      leftEntries.every(([key, entry]) => rightKeys.has(key) && valuesEqual(entry, right[key]))
    );
  }
  return Object.is(left, right);
};

export const rowsEqual = <Row extends RowObject>(left: Row, right: Row): boolean => {
  const leftEntries = Object.entries(left);
  const rightEntries = Object.entries(right);
  if (leftEntries.length !== rightEntries.length) {
    return false;
  }
  for (const [key, value] of leftEntries) {
    if (!valuesEqual(value, fieldValue(right, key))) {
      return false;
    }
  }
  return true;
};
