import { isBigDecimal } from "effect/BigDecimal";
import { filterOperatorKeys, isDenseArray } from "./raw-query-decoder";
import type { RangeValueKind, RawQueryCompilerMetadata } from "./raw-query-metadata";
import { isPlainRecord, scalarEqualityKey, type ScalarEqualityKeyValue } from "./row-values";

export type TopicRawPredicateFilterPlan =
  | {
      readonly field: string;
      readonly operator: "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "startsWith";
      readonly value: unknown;
    }
  | {
      readonly field: string;
      readonly operator: "in";
      readonly values: ReadonlyArray<unknown>;
      readonly valueKeys?: ReadonlySet<string>;
    };

export type TopicRawPredicatePlan = {
  /**
   * Safe scalar hints that storage can use to narrow a raw scan.
   * `matches` remains the correctness guard unless an adapter implements a
   * proven equivalent for every emitted hint.
   */
  readonly filters: ReadonlyArray<TopicRawPredicateFilterPlan>;
  /**
   * True when the compiler intentionally omitted part of the predicate from
   * `filters`, for example structured fields or malformed runtime filters.
   */
  readonly callbackRequired: boolean;
  /**
   * True when the compiler proved that `filters` fully represent `matches`.
   * Hand-written plans omit this and stay guarded by the row callback.
   */
  readonly callbackSkippable?: boolean;
};

export type PredicateFieldPlan = {
  readonly filters: TopicRawPredicatePlan["filters"];
  readonly callbackRequired: boolean;
};

type FilterObject = {
  readonly eq?: unknown;
  readonly neq?: unknown;
  readonly in?: ReadonlyArray<unknown>;
  readonly gt?: unknown;
  readonly gte?: unknown;
  readonly lt?: unknown;
  readonly lte?: unknown;
  readonly startsWith?: string;
};

const rangeValueKind = (value: unknown): RangeValueKind | undefined => {
  if (typeof value === "number" && Number.isFinite(value)) {
    return "number";
  }
  if (typeof value === "bigint") {
    return "bigint";
  }
  if (isBigDecimal(value)) {
    return "bigDecimal";
  }
  return undefined;
};

const isScalarPlanValue = (value: unknown): value is ScalarEqualityKeyValue =>
  value === null ||
  typeof value === "string" ||
  typeof value === "boolean" ||
  typeof value === "bigint" ||
  isBigDecimal(value) ||
  (typeof value === "number" && Number.isFinite(value));

export const isRangePlanValue = (
  field: string,
  value: unknown,
  metadata: RawQueryCompilerMetadata,
): boolean => {
  const kind = rangeValueKind(value);
  const fieldKinds = metadata.rangeValueKinds.get(field);
  return kind !== undefined && fieldKinds?.size === 1 && fieldKinds.has(kind);
};

const isEqualityPlanValue = (value: unknown): value is ScalarEqualityKeyValue =>
  isScalarPlanValue(value);

const isNotEqualPlanValue = (
  field: string,
  value: unknown,
  metadata: RawQueryCompilerMetadata,
): boolean => {
  if (!isEqualityPlanValue(value)) {
    return false;
  }
  if (metadata.numericFieldNames.has(field)) {
    return isRangePlanValue(field, value, metadata);
  }
  if (metadata.stringFieldNames.has(field)) {
    return typeof value === "string";
  }
  return false;
};

const isInPlanValues = (value: unknown): value is ReadonlyArray<ScalarEqualityKeyValue> =>
  Array.isArray(value) &&
  isDenseArray(value) &&
  value.every((candidate) => isEqualityPlanValue(candidate));

const scalarEqualityKeys = (values: ReadonlyArray<ScalarEqualityKeyValue>): ReadonlySet<string> => {
  const keys = new Set<string>();
  for (const value of values) {
    keys.add(scalarEqualityKey(value));
  }
  return keys;
};

export const isOperatorFilterObject = (filter: Record<string, unknown>): filter is FilterObject => {
  const keys = Object.keys(filter);
  return keys.length > 0 && keys.every((key) => filterOperatorKeys.has(key));
};

export const predicateFilterPlans = (
  field: string,
  filter: unknown,
  metadata: RawQueryCompilerMetadata,
): PredicateFieldPlan => {
  if (metadata.structuredFieldNames.has(field) || filter === undefined) {
    return {
      filters: [],
      callbackRequired: true,
    };
  }
  if (!isPlainRecord(filter) || isBigDecimal(filter)) {
    if (!isScalarPlanValue(filter)) {
      return {
        filters: [],
        callbackRequired: true,
      };
    }
    return {
      filters: [
        {
          field,
          operator: "eq",
          value: filter,
        },
      ],
      callbackRequired: false,
    };
  }

  const operatorKeys = Object.keys(filter).filter((key) => filterOperatorKeys.has(key));
  let callbackRequired = operatorKeys.length === 0;
  const plans: Array<TopicRawPredicatePlan["filters"][number]> = [];
  if ("eq" in filter) {
    if (isEqualityPlanValue(filter["eq"])) {
      plans.push({
        field,
        operator: "eq",
        value: filter["eq"],
      });
    } else {
      callbackRequired = true;
    }
  }
  if ("neq" in filter) {
    if (isNotEqualPlanValue(field, filter["neq"], metadata)) {
      plans.push({
        field,
        operator: "neq",
        value: filter["neq"],
      });
    } else {
      callbackRequired = true;
    }
  }
  if ("in" in filter) {
    if (isInPlanValues(filter["in"])) {
      const values = [...filter["in"]];
      plans.push({
        field,
        operator: "in",
        values,
        valueKeys: scalarEqualityKeys(values),
      });
    } else {
      callbackRequired = true;
    }
  }
  if ("gt" in filter) {
    if (isRangePlanValue(field, filter["gt"], metadata)) {
      plans.push({
        field,
        operator: "gt",
        value: filter["gt"],
      });
    } else {
      callbackRequired = true;
    }
  }
  if ("gte" in filter) {
    if (isRangePlanValue(field, filter["gte"], metadata)) {
      plans.push({
        field,
        operator: "gte",
        value: filter["gte"],
      });
    } else {
      callbackRequired = true;
    }
  }
  if ("lt" in filter) {
    if (isRangePlanValue(field, filter["lt"], metadata)) {
      plans.push({
        field,
        operator: "lt",
        value: filter["lt"],
      });
    } else {
      callbackRequired = true;
    }
  }
  if ("lte" in filter) {
    if (isRangePlanValue(field, filter["lte"], metadata)) {
      plans.push({
        field,
        operator: "lte",
        value: filter["lte"],
      });
    } else {
      callbackRequired = true;
    }
  }
  if ("startsWith" in filter) {
    if (typeof filter["startsWith"] === "string") {
      plans.push({
        field,
        operator: "startsWith",
        value: filter["startsWith"],
      });
    } else {
      callbackRequired = true;
    }
  }
  return {
    filters: plans,
    callbackRequired,
  };
};
