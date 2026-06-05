import { isBigDecimal } from "effect/BigDecimal";
import { compareFilterValue } from "./query-value";
import { isDenseArray, type RuntimeRawQuery } from "./raw-query-decoder";
import {
  isOperatorFilterObject,
  predicateFilterPlans,
  type TopicRawPredicatePlan,
} from "./raw-predicate-plan";
import type { RawQueryCompilerMetadata } from "./raw-query-metadata";
import { fieldValue, isPlainRecord, valuesEqual } from "./row-values";

type RowObject = object;

export type CompiledRawPredicate<Row extends RowObject> = {
  readonly plan: TopicRawPredicatePlan;
  readonly matches: (row: Row) => boolean;
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

const includesValue = (values: ReadonlyArray<unknown>, value: unknown): boolean => {
  for (const candidate of values) {
    if (valuesEqual(value, candidate)) {
      return true;
    }
  }
  return false;
};

type CompiledRawPredicateClause = {
  readonly field: string;
  readonly matches: (value: unknown) => boolean;
};

type CompiledRawPredicateParts = {
  readonly clauses: ReadonlyArray<CompiledRawPredicateClause>;
  readonly plan: TopicRawPredicatePlan;
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
