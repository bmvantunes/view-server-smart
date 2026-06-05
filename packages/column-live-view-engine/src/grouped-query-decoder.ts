import { Effect } from "effect";
import type { RuntimeGroupedAggregate } from "./grouped-aggregate-state";
import type { GroupedQueryPlanInput, RuntimeGroupedOrderBy } from "./grouped-query-plan";
import { InvalidQueryError, isDenseArray } from "./raw-query-decoder";
import type { RawQueryCompilerMetadata } from "./raw-query-metadata";
import { isPlainRecord } from "./row-values";

export type RuntimeGroupedQuery = GroupedQueryPlanInput;

const groupedQueryKeys = new Set([
  "groupBy",
  "aggregates",
  "select",
  "where",
  "orderBy",
  "offset",
  "limit",
]);
const dangerousRecordKeys = new Set(["__proto__", "prototype", "constructor"]);

const isValidWindowNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0;

export const decodeGroupedQuery = Effect.fn("ColumnLiveViewEngine.groupedQuery.decode")((
  topic: string,
  metadata: RawQueryCompilerMetadata,
  query: unknown,
): Effect.Effect<RuntimeGroupedQuery, InvalidQueryError> => {
  if (!isPlainRecord(query)) {
    return InvalidQueryError.make({
      topic,
      message: "Grouped query must be a plain object.",
    });
  }
  for (const key of Object.keys(query)) {
    if (!groupedQueryKeys.has(key)) {
      return InvalidQueryError.make({
        topic,
        message: `Grouped query contains unsupported key: ${key}.`,
      });
    }
  }
  if (Object.hasOwn(query, "select")) {
    return InvalidQueryError.make({
      topic,
      message: "Grouped query must not include select.",
    });
  }

  const groupBy = query["groupBy"];
  if (!Array.isArray(groupBy) || groupBy.length === 0 || !isDenseArray(groupBy)) {
    return InvalidQueryError.make({
      topic,
      message: "Grouped query groupBy must be a non-empty array of strings.",
    });
  }
  const decodedGroupBy: Array<string> = [];
  for (const field of groupBy) {
    if (typeof field !== "string") {
      return InvalidQueryError.make({
        topic,
        message: "Grouped query groupBy must be a non-empty array of strings.",
      });
    }
    if (!metadata.fieldNames.has(field)) {
      return InvalidQueryError.make({
        topic,
        message: `Grouped query groupBy contains unknown field: ${field}.`,
      });
    }
    decodedGroupBy.push(field);
  }

  const aggregates = query["aggregates"];
  if (!isPlainRecord(aggregates) || Object.keys(aggregates).length === 0) {
    return InvalidQueryError.make({
      topic,
      message: "Grouped query aggregates must be a non-empty plain object.",
    });
  }
  const aggregateAliases = new Set(Object.keys(aggregates));
  for (const field of decodedGroupBy) {
    if (aggregateAliases.has(field)) {
      return InvalidQueryError.make({
        topic,
        message: `Grouped query aggregate alias collides with groupBy field: ${field}.`,
      });
    }
  }
  const decodedAggregates: Record<string, RuntimeGroupedAggregate> = {};
  for (const [alias, aggregate] of Object.entries(aggregates)) {
    if (dangerousRecordKeys.has(alias)) {
      return InvalidQueryError.make({
        topic,
        message: `Grouped query aggregate alias is not allowed: ${alias}.`,
      });
    }
    if (!isPlainRecord(aggregate)) {
      return InvalidQueryError.make({
        topic,
        message: `Grouped query aggregate ${alias} must be a plain object.`,
      });
    }
    const aggregateKeys = Object.keys(aggregate);
    const aggFunc = aggregate["aggFunc"];
    if (
      aggFunc !== "count" &&
      aggFunc !== "countDistinct" &&
      aggFunc !== "sum" &&
      aggFunc !== "min" &&
      aggFunc !== "max" &&
      aggFunc !== "avg"
    ) {
      return InvalidQueryError.make({
        topic,
        message: `Grouped query aggregate ${alias} has an unsupported aggFunc.`,
      });
    }
    if (aggFunc === "count") {
      if (aggregateKeys.some((key) => key !== "aggFunc")) {
        return InvalidQueryError.make({
          topic,
          message: `Grouped query count aggregate ${alias} must not include a field.`,
        });
      }
      decodedAggregates[alias] = { aggFunc };
      continue;
    }
    for (const key of aggregateKeys) {
      if (key !== "aggFunc" && key !== "field") {
        return InvalidQueryError.make({
          topic,
          message: `Grouped query aggregate ${alias} contains unsupported key: ${key}.`,
        });
      }
    }
    const field = aggregate["field"];
    if (typeof field !== "string") {
      return InvalidQueryError.make({
        topic,
        message: `Grouped query aggregate ${alias} field must be a string.`,
      });
    }
    if (!metadata.fieldNames.has(field)) {
      return InvalidQueryError.make({
        topic,
        message: `Grouped query aggregate ${alias} contains unknown field: ${field}.`,
      });
    }
    if ((aggFunc === "sum" || aggFunc === "avg") && !metadata.numericFieldNames.has(field)) {
      return InvalidQueryError.make({
        topic,
        message: `Grouped query aggregate ${alias} must reference a numeric field.`,
      });
    }
    if (aggFunc === "sum") {
      const resultKind = metadata.fieldMetadata.get(field)?.sumResultKind;
      if (resultKind === undefined) {
        return InvalidQueryError.make({
          topic,
          message: `Grouped query aggregate ${alias} must reference a numeric field.`,
        });
      }
      decodedAggregates[alias] = {
        aggFunc,
        field,
        resultKind,
      };
    } else {
      decodedAggregates[alias] = {
        aggFunc,
        field,
      };
    }
  }

  const where = query["where"];
  if (where !== undefined && !isPlainRecord(where)) {
    return InvalidQueryError.make({
      topic,
      message: "Grouped query where must be a plain object.",
    });
  }

  const orderBy = query["orderBy"];
  if (orderBy !== undefined && !Array.isArray(orderBy)) {
    return InvalidQueryError.make({
      topic,
      message: "Grouped query orderBy must be an array.",
    });
  }
  const decodedOrderBy: Array<RuntimeGroupedOrderBy> = [];
  if (Array.isArray(orderBy)) {
    for (const entry of orderBy) {
      if (!isPlainRecord(entry)) {
        return InvalidQueryError.make({
          topic,
          message: "Grouped query orderBy entries must be plain objects.",
        });
      }
      for (const key of Object.keys(entry)) {
        if (key !== "field" && key !== "aggregate" && key !== "direction") {
          return InvalidQueryError.make({
            topic,
            message: `Grouped query orderBy contains unsupported key: ${key}.`,
          });
        }
      }
      const direction = entry["direction"];
      if (direction !== "asc" && direction !== "desc") {
        return InvalidQueryError.make({
          topic,
          message: "Grouped query orderBy direction must be asc or desc.",
        });
      }
      const hasField = Object.hasOwn(entry, "field");
      const hasAggregate = Object.hasOwn(entry, "aggregate");
      if (hasField === hasAggregate) {
        return InvalidQueryError.make({
          topic,
          message: "Grouped query orderBy entries must choose field or aggregate.",
        });
      }
      if (hasField) {
        const field = entry["field"];
        if (typeof field !== "string" || !decodedGroupBy.includes(field)) {
          return InvalidQueryError.make({
            topic,
            message: "Grouped query orderBy field must be present in groupBy.",
          });
        }
        decodedOrderBy.push({ field, direction });
      } else {
        const aggregate = entry["aggregate"];
        if (typeof aggregate !== "string" || !aggregateAliases.has(aggregate)) {
          return InvalidQueryError.make({
            topic,
            message: "Grouped query orderBy aggregate must reference an aggregate alias.",
          });
        }
        decodedOrderBy.push({ aggregate, direction });
      }
    }
  }

  const offset = query["offset"];
  if (offset !== undefined && !isValidWindowNumber(offset)) {
    return InvalidQueryError.make({
      topic,
      message: "Grouped query offset must be a non-negative safe integer.",
    });
  }

  const limit = query["limit"];
  if (limit !== undefined && !isValidWindowNumber(limit)) {
    return InvalidQueryError.make({
      topic,
      message: "Grouped query limit must be a non-negative safe integer.",
    });
  }

  return Effect.succeed({
    groupBy: decodedGroupBy,
    aggregates: decodedAggregates,
    ...(where === undefined ? {} : { where }),
    ...(decodedOrderBy.length === 0 ? {} : { orderBy: decodedOrderBy }),
    ...(offset === undefined ? {} : { offset }),
    ...(limit === undefined ? {} : { limit }),
  });
});
