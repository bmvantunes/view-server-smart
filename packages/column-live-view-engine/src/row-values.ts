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
  for (const key in value) {
    if (Object.hasOwn(value, key)) {
      cloned[key] = cloneUnknown(value[key]);
    }
  }
  return cloned;
};

export function cloneRow<Row extends RowObject>(row: Row): Row;
export function cloneRow(row: RowObject): RowObject {
  const cloned: Record<string, unknown> = {};
  for (const key in row) {
    if (Object.hasOwn(row, key)) {
      cloned[key] = cloneUnknown(Reflect.get(row, key));
    }
  }
  return cloned;
}

export const fieldValue = (row: RowObject, field: string): unknown => {
  if (!Object.hasOwn(row, field)) {
    return undefined;
  }
  return Reflect.get(row, field);
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
    const rightKeys = new Set(Object.keys(right));
    for (const key of Object.keys(left)) {
      if (!rightKeys.delete(key)) {
        return false;
      }
      if (!valuesEqual(left[key], right[key])) {
        return false;
      }
    }
    return rightKeys.size === 0;
  }
  return Object.is(left, right);
};

export const rowsEqual = <Row extends RowObject>(left: Row, right: Row): boolean => {
  const rightKeys = new Set(Object.keys(right));
  const leftKeys = Object.keys(left);
  if (leftKeys.length !== rightKeys.size) {
    return false;
  }
  for (const key of leftKeys) {
    if (!rightKeys.delete(key) || !valuesEqual(Reflect.get(left, key), fieldValue(right, key))) {
      return false;
    }
  }
  return true;
};
