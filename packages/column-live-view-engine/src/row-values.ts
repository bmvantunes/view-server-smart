import {
  equals,
  format as formatBigDecimal,
  isBigDecimal,
  normalize,
  type BigDecimal,
} from "effect/BigDecimal";

type RowObject = object;

export type ScalarEqualityKeyValue = null | string | boolean | bigint | number | BigDecimal;

export const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const isPlainRecord = (value: unknown): value is Record<string, unknown> => {
  if (!isRecord(value) || isBigDecimal(value)) {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype;
};

const setClonedField = (record: Record<string, unknown>, field: string, value: unknown): void => {
  Object.defineProperty(record, field, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
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
      setClonedField(cloned, key, cloneUnknown(value[key]));
    }
  }
  return cloned;
};

export function cloneRow<Row extends RowObject>(row: Row): Row;
export function cloneRow(row: RowObject): RowObject {
  const cloned: Record<string, unknown> = {};
  for (const key in row) {
    if (Object.hasOwn(row, key)) {
      setClonedField(cloned, key, cloneUnknown(Reflect.get(row, key)));
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

// Use only after the engine has decoded and shadowed schema fields as own properties.
export const trustedFieldValue = (row: RowObject, field: string): unknown =>
  Reflect.get(row, field);

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

export function scalarEqualityKey(value: ScalarEqualityKeyValue): string;
export function scalarEqualityKey(value: unknown): string | undefined;
export function scalarEqualityKey(value: unknown): string | undefined {
  if (value === null) {
    return "null";
  }
  if (typeof value === "string") {
    return `string:${value.length}:${value}`;
  }
  if (typeof value === "boolean") {
    return `boolean:${value ? "true" : "false"}`;
  }
  if (typeof value === "bigint") {
    return `bigint:${value.toString()}`;
  }
  if (typeof value === "number") {
    return `number:${Object.is(value, -0) ? "-0" : value.toString()}`;
  }
  if (isBigDecimal(value)) {
    return `bigDecimal:${formatBigDecimal(normalize(value))}`;
  }
  return undefined;
}

export const rowsEqual = <Row extends RowObject>(left: Row, right: Row): boolean => {
  if (Object.is(left, right)) {
    return true;
  }
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
