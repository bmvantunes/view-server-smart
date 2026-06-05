import { isBigDecimal } from "effect/BigDecimal";
import { compareFilterValue } from "./query-value";
import { filterOperatorKeys, isDenseArray, type RuntimeRawQuery } from "./raw-query-decoder";
import type { RangeValueKind, RawQueryCompilerMetadata } from "./raw-query-metadata";
import {
  fieldValue,
  isPlainRecord,
  scalarEqualityKey,
  type ScalarEqualityKeyValue,
  valuesEqual,
} from "./row-values";
import type { TopicRawPredicatePlan } from "./row-scan";

type RowObject = object;

export type CompiledRawPredicate<Row extends RowObject> = {
  readonly plan: TopicRawPredicatePlan;
  readonly matches: (row: Row) => boolean;
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

const isEqualityComparable = (left: unknown, right: unknown): boolean => {
  if (isBigDecimal(left) || isBigDecimal(right)) {
    return isBigDecimal(left) && isBigDecimal(right);
  }
  if (typeof left === "number" || typeof right === "number") {
    return typeof left === "number" && typeof right === "number" && Number.isFinite(right);
  }
  return typeof left === typeof right;
};

const isOperatorFilterObject = (filter: Record<string, unknown>): filter is FilterObject => {
  const keys = Object.keys(filter);
  return keys.length > 0 && keys.every((key) => filterOperatorKeys.has(key));
};

const includesValue = (values: ReadonlyArray<unknown>, value: unknown): boolean => {
  for (const candidate of values) {
    if (valuesEqual(value, candidate)) {
      return true;
    }
  }
  return false;
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

type PredicateFieldPlan = {
  readonly filters: TopicRawPredicatePlan["filters"];
  readonly callbackRequired: boolean;
};

type CompiledRawPredicateClause = {
  readonly field: string;
  readonly matches: (value: unknown) => boolean;
};

type CompiledRawPredicateParts = {
  readonly clauses: ReadonlyArray<CompiledRawPredicateClause>;
  readonly plan: TopicRawPredicatePlan;
};

const predicateFilterPlans = (
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

const isStructuredQueryValue = (value: unknown): boolean =>
  (isPlainRecord(value) && !isBigDecimal(value)) || Array.isArray(value);

const compileStructuredFilterMatcher = (
  filter: Readonly<Record<string, unknown>>,
): ((value: unknown) => boolean) => {
  const oneOf = filter["in"];
  const oneOfMatcher =
    oneOf === undefined
      ? undefined
      : Array.isArray(oneOf) &&
          isDenseArray(oneOf) &&
          !oneOf.some((candidate) => candidate === undefined)
        ? (value: unknown) => includesValue(oneOf, value)
        : () => false;
  const eq = filter["eq"];
  const neq = filter["neq"];

  return (value) => {
    if (valuesEqual(value, filter)) {
      return true;
    }
    if (oneOfMatcher !== undefined && !oneOfMatcher(value)) {
      return false;
    }
    if (eq !== undefined && !valuesEqual(value, eq)) {
      return false;
    }
    if (neq !== undefined && valuesEqual(value, neq)) {
      return false;
    }
    return eq !== undefined || oneOfMatcher !== undefined || neq !== undefined;
  };
};

const compileScalarOperatorFilterMatcher = (
  filter: Readonly<Record<string, unknown>>,
): ((value: unknown) => boolean) => {
  if (
    ("eq" in filter && filter["eq"] === undefined) ||
    ("neq" in filter && filter["neq"] === undefined) ||
    ("in" in filter && filter["in"] === undefined) ||
    ("gt" in filter && filter["gt"] === undefined) ||
    ("gte" in filter && filter["gte"] === undefined) ||
    ("lt" in filter && filter["lt"] === undefined) ||
    ("lte" in filter && filter["lte"] === undefined) ||
    ("startsWith" in filter && filter["startsWith"] === undefined)
  ) {
    return () => false;
  }

  const eq = filter["eq"];
  const neq = filter["neq"];
  const oneOf = filter["in"];
  const startsWith = filter["startsWith"];
  const gt = filter["gt"];
  const gte = filter["gte"];
  const lt = filter["lt"];
  const lte = filter["lte"];
  const oneOfMatcher =
    oneOf === undefined
      ? undefined
      : Array.isArray(oneOf) &&
          isDenseArray(oneOf) &&
          !oneOf.some((candidate) => candidate === undefined)
        ? (value: unknown) => includesValue(oneOf, value)
        : () => false;

  return (value) => {
    if (eq !== undefined && !valuesEqual(value, eq)) {
      return false;
    }
    if (neq !== undefined) {
      if (!isEqualityComparable(value, neq) || valuesEqual(value, neq)) {
        return false;
      }
    }
    if (oneOfMatcher !== undefined && !oneOfMatcher(value)) {
      return false;
    }
    if (startsWith !== undefined) {
      if (
        typeof startsWith !== "string" ||
        typeof value !== "string" ||
        !value.startsWith(startsWith)
      ) {
        return false;
      }
    }

    if (gt !== undefined) {
      const comparison = compareFilterValue(value, gt);
      if (comparison === undefined || comparison <= 0) {
        return false;
      }
    }
    if (gte !== undefined) {
      const comparison = compareFilterValue(value, gte);
      if (comparison === undefined || comparison < 0) {
        return false;
      }
    }
    if (lt !== undefined) {
      const comparison = compareFilterValue(value, lt);
      if (comparison === undefined || comparison >= 0) {
        return false;
      }
    }
    if (lte !== undefined) {
      const comparison = compareFilterValue(value, lte);
      if (comparison === undefined || comparison > 0) {
        return false;
      }
    }

    return true;
  };
};

const compileFilterMatcher = (filter: unknown): ((value: unknown) => boolean) => {
  if (filter === undefined) {
    return () => false;
  }
  if (!isPlainRecord(filter) || isBigDecimal(filter)) {
    return (value) => valuesEqual(value, filter);
  }

  const structuredMatcher = compileStructuredFilterMatcher(filter);
  if (!isOperatorFilterObject(filter)) {
    return (value) =>
      isStructuredQueryValue(value) ? structuredMatcher(value) : valuesEqual(value, filter);
  }

  const scalarMatcher = compileScalarOperatorFilterMatcher(filter);
  return (value) =>
    isStructuredQueryValue(value) ? structuredMatcher(value) : scalarMatcher(value);
};

const compilePredicateParts = (
  metadata: RawQueryCompilerMetadata,
  where: RuntimeRawQuery["where"],
): CompiledRawPredicateParts => {
  if (where === undefined) {
    return {
      clauses: [],
      plan: {
        filters: [],
        callbackRequired: false,
        callbackSkippable: true,
      },
    };
  }

  const filters: Array<TopicRawPredicatePlan["filters"][number]> = [];
  const clauses: Array<CompiledRawPredicateClause> = [];
  let callbackRequired = false;
  for (const [field, filter] of Object.entries(where)) {
    const fieldPlan = predicateFilterPlans(field, filter, metadata);
    filters.push(...fieldPlan.filters);
    callbackRequired ||= fieldPlan.callbackRequired;
    clauses.push({
      field,
      matches: compileFilterMatcher(filter),
    });
  }
  return {
    clauses,
    plan: {
      filters,
      callbackRequired,
      callbackSkippable: !callbackRequired,
    },
  };
};

export const compileRawPredicate = <Row extends RowObject>(
  metadata: RawQueryCompilerMetadata,
  where: RuntimeRawQuery["where"],
): CompiledRawPredicate<Row> => {
  const parts = compilePredicateParts(metadata, where);
  if (parts.clauses.length === 0) {
    return {
      plan: parts.plan,
      matches: () => true,
    };
  }

  return {
    plan: parts.plan,
    matches: (row) => {
      for (const clause of parts.clauses) {
        if (!clause.matches(fieldValue(row, clause.field))) {
          return false;
        }
      }
      return true;
    },
  };
};
