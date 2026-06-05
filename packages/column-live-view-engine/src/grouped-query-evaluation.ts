import { type GroupState, newGroupState, updateAggregateState } from "./grouped-aggregate-state";
import type { RuntimeGroupedQuery } from "./grouped-query-decoder";
import { emptyGroupedEvaluation, groupedEvaluationFromGroups } from "./grouped-window-evaluation";
import { stableQueryValueString } from "./query-value";
import { fieldValue } from "./row-values";
import type { QueryEvaluation } from "./query-result";
import type { TopicRowScan } from "./row-scan";

type RowObject = object;

export const groupedCacheKey = (query: RuntimeGroupedQuery): string =>
  stableQueryValueString([
    "grouped",
    query.groupBy,
    Object.entries(query.aggregates).toSorted(
      ([left], [right]) => Number(left > right) - Number(left < right),
    ),
    query.where === undefined ? [] : stableQueryValueString(query.where),
    query.orderBy ?? [],
    query.offset ?? null,
    query.limit ?? null,
  ]);

export const groupKey = (groupBy: ReadonlyArray<string>, row: RowObject): string =>
  stableQueryValueString(groupBy.map((field) => [field, fieldValue(row, field)]));

export function typedGroupedEvaluation<ResultRow extends RowObject>(
  evaluation: QueryEvaluation<RowObject>,
): QueryEvaluation<ResultRow>;
export function typedGroupedEvaluation(
  evaluation: QueryEvaluation<RowObject>,
): QueryEvaluation<RowObject> {
  return evaluation;
}

const evaluateZeroLimitGroupedRows = <Row extends RowObject>(
  store: TopicRowScan<Row>,
  query: RuntimeGroupedQuery,
  matches: (row: Row) => boolean,
): QueryEvaluation<RowObject> => {
  const groupKeys = new Set<string>();
  store.scanRows((_key, row) => {
    if (matches(row)) {
      groupKeys.add(groupKey(query.groupBy, row));
    }
  });
  return emptyGroupedEvaluation(groupKeys.size, store.version());
};

export const evaluateGroupedRows = <Row extends RowObject>(
  store: TopicRowScan<Row>,
  query: RuntimeGroupedQuery,
  matches: (row: Row) => boolean,
): QueryEvaluation<RowObject> => {
  if (query.limit === 0) {
    return evaluateZeroLimitGroupedRows(store, query, matches);
  }
  const groups = new Map<string, GroupState>();
  store.scanRows((_key, row) => {
    if (!matches(row)) {
      return;
    }
    const key = groupKey(query.groupBy, row);
    let group = groups.get(key);
    if (group === undefined) {
      group = newGroupState(key, query.groupBy, query.aggregates, row);
      groups.set(key, group);
    }
    for (const [alias, aggregate] of Object.entries(query.aggregates)) {
      updateAggregateState(group.aggregates[alias]!, aggregate, row);
    }
  });
  return groupedEvaluationFromGroups(groups.values(), query, store.version());
};
