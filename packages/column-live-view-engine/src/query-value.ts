import { format as formatBigDecimal, isBigDecimal, normalize, Order } from "effect/BigDecimal";
import { isPlainRecord } from "./row-values";

type StableQueryObjectEntry = readonly [string, StableQueryValueToken];

type StableQueryValueToken =
  | readonly ["null"]
  | readonly ["undefined"]
  | readonly ["boolean", boolean]
  | readonly ["number", string]
  | readonly ["string", string]
  | readonly ["bigint", string]
  | readonly ["bigDecimal", string]
  | readonly ["unsupported", string]
  | readonly ["cycle"]
  | readonly ["array", ReadonlyArray<StableQueryValueToken>]
  | readonly ["map", ReadonlyArray<readonly [StableQueryValueToken, StableQueryValueToken]>]
  | readonly ["object", ReadonlyArray<StableQueryObjectEntry>]
  | readonly ["set", ReadonlyArray<StableQueryValueToken>]
  | readonly ["nonPlainObject", string];

const compareStableQueryValueToken = (
  left: StableQueryValueToken,
  right: StableQueryValueToken,
): number => {
  const leftString = JSON.stringify(left);
  const rightString = JSON.stringify(right);
  return Number(leftString > rightString) - Number(leftString < rightString);
};

const stableQueryValueToken = (value: unknown, active: WeakSet<object>): StableQueryValueToken => {
  if (isBigDecimal(value)) {
    return ["bigDecimal", formatBigDecimal(normalize(value))];
  }
  if (value === null) {
    return ["null"];
  }
  if (typeof value === "bigint") {
    return ["bigint", value.toString()];
  }
  if (typeof value === "symbol") {
    return ["unsupported", `symbol:${value.description ?? ""}`];
  }
  if (typeof value === "function") {
    return ["unsupported", `function:${value.name}`];
  }
  if (Array.isArray(value)) {
    if (active.has(value)) {
      return ["cycle"];
    }
    active.add(value);
    const token: StableQueryValueToken = [
      "array",
      value.map((entry) => stableQueryValueToken(entry, active)),
    ];
    active.delete(value);
    return token;
  }
  if (value instanceof Map) {
    if (active.has(value)) {
      return ["cycle"];
    }
    active.add(value);
    const entries = Array.from(
      value.entries(),
      ([key, entryValue]) =>
        [stableQueryValueToken(key, active), stableQueryValueToken(entryValue, active)] as const,
    ).toSorted((left, right) => {
      const keyComparison = compareStableQueryValueToken(left[0], right[0]);
      return keyComparison === 0 ? compareStableQueryValueToken(left[1], right[1]) : keyComparison;
    });
    active.delete(value);
    return ["map", entries];
  }
  if (value instanceof Set) {
    if (active.has(value)) {
      return ["cycle"];
    }
    active.add(value);
    const values = Array.from(value.values(), (entry) =>
      stableQueryValueToken(entry, active),
    ).toSorted(compareStableQueryValueToken);
    active.delete(value);
    return ["set", values];
  }
  if (isPlainRecord(value)) {
    if (active.has(value)) {
      return ["cycle"];
    }
    active.add(value);
    const entries: Array<StableQueryObjectEntry> = Object.keys(value)
      .toSorted()
      .map((key) => [key, stableQueryValueToken(value[key], active)]);
    active.delete(value);
    return ["object", entries];
  }
  if (typeof value === "object" && value !== null) {
    return ["nonPlainObject", Object.prototype.toString.call(value)];
  }
  if (typeof value === "number") {
    return ["number", Object.is(value, -0) ? "-0" : String(value)];
  }
  if (typeof value === "string") {
    return ["string", value];
  }
  if (typeof value === "boolean") {
    return ["boolean", value];
  }
  return ["undefined"];
};

export const stableQueryValueString = (value: unknown): string =>
  JSON.stringify(stableQueryValueToken(value, new WeakSet()));

const valueRank = (value: unknown): number => {
  if (value == null) {
    return 0;
  }
  if (typeof value === "boolean") {
    return 1;
  }
  if (typeof value === "number" || typeof value === "bigint" || isBigDecimal(value)) {
    return 2;
  }
  if (typeof value === "string") {
    return 3;
  }
  if (Array.isArray(value)) {
    return 4;
  }
  return 5;
};

export const compareFilterValue = (left: unknown, right: unknown): number | undefined => {
  if (typeof left === "number" && typeof right === "number") {
    if (!Number.isFinite(left) || !Number.isFinite(right)) {
      return undefined;
    }
    return left === right ? 0 : left < right ? -1 : 1;
  }
  if (typeof left === "string" && typeof right === "string") {
    return left === right ? 0 : left < right ? -1 : 1;
  }
  if (typeof left === "bigint" && typeof right === "bigint") {
    return left === right ? 0 : left < right ? -1 : 1;
  }
  if (isBigDecimal(left) && isBigDecimal(right)) {
    return Order(left, right);
  }
  return undefined;
};

const compareByStableString = (left: unknown, right: unknown): number => {
  const leftString = stableQueryValueString(left);
  const rightString = stableQueryValueString(right);
  return Number(leftString > rightString) - Number(leftString < rightString);
};

export const compareQueryValue = (left: unknown, right: unknown): number => {
  const leftRank = valueRank(left);
  const rightRank = valueRank(right);
  if (leftRank !== rightRank) {
    return Number(leftRank > rightRank) - Number(leftRank < rightRank);
  }
  if (typeof left === "boolean" && typeof right === "boolean") {
    return left === right ? 0 : left ? 1 : -1;
  }
  const filterComparison = compareFilterValue(left, right);
  if (filterComparison !== undefined) {
    return filterComparison;
  }
  return compareByStableString(left, right);
};
