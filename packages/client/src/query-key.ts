import { format, isBigDecimal, normalize } from "effect/BigDecimal";

const objectIdentities = new WeakMap<object, number>();
const symbolIdentities = new Map<symbol, number>();
let nextObjectIdentity = 0;
let nextSymbolIdentity = 0;

type StableObjectEntry = readonly [string, StableQueryToken];
type StableMapEntry = readonly [StableQueryToken, StableQueryToken];

type StableQueryToken =
  | readonly ["null"]
  | readonly ["undefined"]
  | readonly ["boolean", boolean]
  | readonly ["number", string]
  | readonly ["string", string]
  | readonly ["bigint", string]
  | readonly ["bigDecimal", string]
  | readonly ["symbol", number]
  | readonly ["function", string, number]
  | readonly ["cycle"]
  | readonly ["array", ReadonlyArray<StableQueryToken>]
  | readonly ["object", ReadonlyArray<StableObjectEntry>]
  | readonly ["map", ReadonlyArray<StableMapEntry>]
  | readonly ["set", ReadonlyArray<StableQueryToken>]
  | readonly ["nonPlainObject", string, number];

const stableObjectIdentity = (value: object): number => {
  const identity = objectIdentities.get(value);
  if (identity !== undefined) {
    return identity;
  }
  nextObjectIdentity += 1;
  objectIdentities.set(value, nextObjectIdentity);
  return nextObjectIdentity;
};

const stableSymbolIdentity = (value: symbol): number => {
  const identity = symbolIdentities.get(value);
  if (identity !== undefined) {
    return identity;
  }
  nextSymbolIdentity += 1;
  symbolIdentities.set(value, nextSymbolIdentity);
  return nextSymbolIdentity;
};

const isPlainObject = (value: object): boolean => {
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
};

const stableNumberValue = (value: number): string => {
  if (Object.is(value, -0)) {
    return "-0";
  }
  return String(value);
};

const stableFunctionName = (value: { readonly name: string }): string =>
  value.name === "" ? "anonymous" : value.name;

const stableObjectName = (value: object): string => {
  const constructor = value.constructor;
  return typeof constructor === "function" && constructor.name !== "" ? constructor.name : "Object";
};

const stableTokenSortKey = (value: StableQueryToken): string => JSON.stringify(value);

const stableQueryValue = (value: unknown, active: WeakSet<object>): StableQueryToken => {
  if (value === null) {
    return ["null"];
  }
  if (value === undefined) {
    return ["undefined"];
  }
  if (typeof value === "boolean") {
    return ["boolean", value];
  }
  if (typeof value === "number") {
    return ["number", stableNumberValue(value)];
  }
  if (typeof value === "string") {
    return ["string", value];
  }
  if (typeof value === "bigint") {
    return ["bigint", value.toString()];
  }
  if (isBigDecimal(value)) {
    return ["bigDecimal", format(normalize(value))];
  }
  if (typeof value === "symbol") {
    return ["symbol", stableSymbolIdentity(value)];
  }
  if (typeof value === "function") {
    return ["function", stableFunctionName(value), stableObjectIdentity(value)];
  }
  if (Array.isArray(value)) {
    if (active.has(value)) {
      return ["cycle"];
    }
    active.add(value);
    try {
      return ["array", value.map((entry) => stableQueryValue(entry, active))];
    } finally {
      active.delete(value);
    }
  }
  if (active.has(value)) {
    return ["cycle"];
  }
  if (value instanceof Map) {
    active.add(value);
    try {
      const entries: Array<StableMapEntry> = [];
      for (const [key, entry] of value.entries()) {
        entries.push([stableQueryValue(key, active), stableQueryValue(entry, active)]);
      }
      return [
        "map",
        entries.sort((left, right) =>
          stableTokenSortKey(left[0]).localeCompare(stableTokenSortKey(right[0])),
        ),
      ];
    } finally {
      active.delete(value);
    }
  }
  if (value instanceof Set) {
    active.add(value);
    try {
      const entries: Array<StableQueryToken> = [];
      for (const entry of value.values()) {
        entries.push(stableQueryValue(entry, active));
      }
      return [
        "set",
        entries.sort((left, right) =>
          stableTokenSortKey(left).localeCompare(stableTokenSortKey(right)),
        ),
      ];
    } finally {
      active.delete(value);
    }
  }
  if (!isPlainObject(value)) {
    return ["nonPlainObject", stableObjectName(value), stableObjectIdentity(value)];
  }
  active.add(value);
  try {
    return [
      "object",
      Object.entries(value)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, entry]) => [key, stableQueryValue(entry, active)]),
    ];
  } finally {
    active.delete(value);
  }
};

export const stableQueryKey = (query: object): string =>
  JSON.stringify(stableQueryValue(query, new WeakSet<object>()));
