import { type GroupState, newGroupState, updateAggregateState } from "./grouped-aggregate-state";
import type { GroupedQueryPlan } from "./grouped-query-plan";
import { emptyGroupedEvaluation, groupedEvaluationFromGroups } from "./grouped-window-evaluation";
import type { QueryEvaluation } from "./query-result";
import type { TopicRowScan } from "./row-scan";

type RowObject = object;

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
  plan: GroupedQueryPlan<Row>,
  matches: (row: Row) => boolean,
): QueryEvaluation<RowObject> => {
  const groupKeys = new Set<string>();
  store.scanRows((_key, row) => {
    if (matches(row)) {
      groupKeys.add(plan.groupKey(row));
    }
  });
  return emptyGroupedEvaluation(groupKeys.size, store.version());
};

export const evaluateGroupedRows = <Row extends RowObject>(
  store: TopicRowScan<Row>,
  plan: GroupedQueryPlan<Row>,
  matches: (row: Row) => boolean,
): QueryEvaluation<RowObject> => {
  if (plan.zeroLimit) {
    return evaluateZeroLimitGroupedRows(store, plan, matches);
  }
  const groups = new Map<string, GroupState>();
  store.scanRows((_key, row) => {
    if (!matches(row)) {
      return;
    }
    const key = plan.groupKey(row);
    let group = groups.get(key);
    if (group === undefined) {
      group = newGroupState(key, plan.groupBy, plan.aggregatePlans, row);
      groups.set(key, group);
    }
    for (const { alias, aggregate } of plan.aggregatePlans) {
      updateAggregateState(group.aggregates[alias]!, aggregate, row);
    }
  });
  return groupedEvaluationFromGroups(groups.values(), plan, store.version());
};
