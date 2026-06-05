import {
  type MaterializedIncrementalGroupState,
  newIncrementalGroupState,
  recomputeIncrementalGroupState,
  updateAggregateState,
} from "./grouped-aggregate-state";
import { typedGroupedEvaluation } from "./grouped-query-evaluation";
import type { GroupedQueryPlan } from "./grouped-query-plan";
import {
  emptyGroupedEvaluation,
  groupedEvaluationFromEntries,
  groupedEvaluationFromGroups,
} from "./grouped-window-evaluation";
import type { CompiledGroupedQuery } from "./grouped-query-compiler";
import type { QueryEvaluation } from "./query-result";
import type { TopicRowChangeBatch, TopicRowScan } from "./row-scan";

type RowObject = object;

type CountOnlyIncrementalGroupState = {
  readonly key: string;
  count: number;
};

export type IncrementalGroupedQueryExecution<ResultRow extends RowObject> = {
  readonly incremental: boolean;
  readonly latest: () => QueryEvaluation<ResultRow>;
};

const maxIncrementalGroupedMembers = 65_536;
const maxIncrementalGroupedMembersPerGroup = 4_096;
const maxIncrementalGroupedGroups = 8_192;

const newZeroLimitIncrementalGroupState = (key: string): CountOnlyIncrementalGroupState => ({
  key,
  count: 0,
});

type IncrementalGroupedQueryState =
  | {
      readonly mode: "materialized";
      readonly groups: Map<string, MaterializedIncrementalGroupState>;
      evaluation: QueryEvaluation<RowObject>;
      memberCount: number;
      version: number;
    }
  | {
      readonly mode: "countOnly";
      readonly groups: Map<string, CountOnlyIncrementalGroupState>;
      evaluation: QueryEvaluation<RowObject>;
      version: number;
    };

type IncrementalGroupedQueryBuildState =
  | {
      readonly admitted: false;
    }
  | {
      readonly admitted: true;
      readonly state: IncrementalGroupedQueryState;
    };

const evaluateIncrementalGroupedQuery = <Row extends RowObject>(
  state: Extract<IncrementalGroupedQueryState, { readonly mode: "materialized" }>,
  plan: GroupedQueryPlan<Row>,
  version: number,
): QueryEvaluation<RowObject> => groupedEvaluationFromGroups(state.groups.values(), plan, version);

const clearIncrementalGroupedQueryState = <Row extends RowObject>(
  state: IncrementalGroupedQueryState,
  plan: GroupedQueryPlan<Row>,
  version: number,
): void => {
  state.groups.clear();
  state.evaluation =
    state.mode === "countOnly"
      ? emptyGroupedEvaluation(0, version)
      : groupedEvaluationFromEntries([], plan, version);
  if (state.mode === "materialized") {
    state.memberCount = 0;
  }
  state.version = version;
};

const buildCountOnlyIncrementalGroupedQueryState = <Row extends RowObject>(
  store: TopicRowScan<Row>,
  plan: GroupedQueryPlan<Row>,
  matches: (row: Row) => boolean,
): IncrementalGroupedQueryBuildState => {
  const groups = new Map<string, CountOnlyIncrementalGroupState>();
  let admitted = true;
  store.scanRows((_key, row) => {
    if (!admitted) {
      return undefined;
    }
    if (!matches(row)) {
      return undefined;
    }
    const groupedKey = plan.groupKey(row);
    let group = groups.get(groupedKey);
    if (group === undefined) {
      group = newZeroLimitIncrementalGroupState(groupedKey);
      groups.set(groupedKey, group);
    }
    group.count += 1;
    if (groups.size > maxIncrementalGroupedGroups) {
      groups.clear();
      admitted = false;
      return false;
    }
    return undefined;
  });
  if (!admitted) {
    return {
      admitted: false,
    };
  }
  const version = store.version();
  return {
    admitted: true,
    state: {
      mode: "countOnly",
      groups,
      evaluation: emptyGroupedEvaluation(groups.size, version),
      version,
    },
  };
};

const buildMaterializedIncrementalGroupedQueryState = <Row extends RowObject>(
  store: TopicRowScan<Row>,
  plan: GroupedQueryPlan<Row>,
  matches: (row: Row) => boolean,
): IncrementalGroupedQueryBuildState => {
  const groups = new Map<string, MaterializedIncrementalGroupState>();
  let memberCount = 0;
  let admitted = true;
  store.scanRows((key, row) => {
    if (!admitted) {
      return undefined;
    }
    if (!matches(row)) {
      return undefined;
    }
    const groupedKey = plan.groupKey(row);
    let group = groups.get(groupedKey);
    if (group === undefined) {
      group = newIncrementalGroupState(groupedKey, plan.groupBy, plan.aggregates, row);
      groups.set(groupedKey, group);
    }
    group.members.set(key, row);
    memberCount += 1;
    if (
      memberCount > maxIncrementalGroupedMembers ||
      group.members.size > maxIncrementalGroupedMembersPerGroup ||
      groups.size > maxIncrementalGroupedGroups
    ) {
      groups.clear();
      admitted = false;
      return false;
    }
    for (const [alias, aggregate] of Object.entries(plan.aggregates)) {
      updateAggregateState(group.aggregates[alias]!, aggregate, row);
    }
    return undefined;
  });
  if (!admitted) {
    return {
      admitted: false,
    };
  }
  const version = store.version();
  const state: IncrementalGroupedQueryState = {
    mode: "materialized",
    groups,
    evaluation: groupedEvaluationFromGroups(groups.values(), plan, version),
    memberCount,
    version,
  };
  return {
    admitted: true,
    state,
  };
};

const buildIncrementalGroupedQueryState = <Row extends RowObject>(
  store: TopicRowScan<Row>,
  plan: GroupedQueryPlan<Row>,
  matches: (row: Row) => boolean,
): IncrementalGroupedQueryBuildState =>
  plan.zeroLimit
    ? buildCountOnlyIncrementalGroupedQueryState(store, plan, matches)
    : buildMaterializedIncrementalGroupedQueryState(store, plan, matches);

const removeMaterializedIncrementalGroupedMember = <Row extends RowObject>(
  groups: Map<string, MaterializedIncrementalGroupState>,
  plan: GroupedQueryPlan<Row>,
  matches: (row: Row) => boolean,
  key: string,
  row: Row,
  dirtyGroups: Set<MaterializedIncrementalGroupState>,
): boolean => {
  if (!matches(row)) {
    return false;
  }
  const groupedKey = plan.groupKey(row);
  const group = groups.get(groupedKey);
  if (group === undefined) {
    return false;
  }
  const removed = group.members.delete(key);
  if (!removed) {
    return false;
  }
  if (group.members.size === 0) {
    groups.delete(groupedKey);
    dirtyGroups.delete(group);
    return true;
  }
  dirtyGroups.add(group);
  return true;
};

const removeCountOnlyIncrementalGroupedMember = <Row extends RowObject>(
  groups: Map<string, CountOnlyIncrementalGroupState>,
  plan: GroupedQueryPlan<Row>,
  matches: (row: Row) => boolean,
  row: Row,
): boolean => {
  if (!matches(row)) {
    return false;
  }
  const groupedKey = plan.groupKey(row);
  const group = groups.get(groupedKey);
  if (group === undefined) {
    return false;
  }
  group.count -= 1;
  if (group.count === 0) {
    groups.delete(groupedKey);
  }
  return true;
};

type UpsertIncrementalGroupedMemberResult = {
  readonly groupSize: number;
  readonly inserted: boolean;
};

const upsertMaterializedIncrementalGroupedMember = <Row extends RowObject>(
  groups: Map<string, MaterializedIncrementalGroupState>,
  plan: GroupedQueryPlan<Row>,
  matches: (row: Row) => boolean,
  key: string,
  row: Row,
  dirtyGroups: Set<MaterializedIncrementalGroupState>,
): UpsertIncrementalGroupedMemberResult | undefined => {
  if (!matches(row)) {
    return undefined;
  }
  const groupedKey = plan.groupKey(row);
  let group = groups.get(groupedKey);
  if (group === undefined) {
    group = newIncrementalGroupState(groupedKey, plan.groupBy, plan.aggregates, row);
    groups.set(groupedKey, group);
  }
  const inserted = !group.members.has(key);
  group.members.set(key, row);
  dirtyGroups.add(group);
  return {
    groupSize: group.members.size,
    inserted,
  };
};

const upsertCountOnlyIncrementalGroupedMember = <Row extends RowObject>(
  groups: Map<string, CountOnlyIncrementalGroupState>,
  plan: GroupedQueryPlan<Row>,
  matches: (row: Row) => boolean,
  row: Row,
): boolean => {
  if (!matches(row)) {
    return false;
  }
  const groupedKey = plan.groupKey(row);
  let group = groups.get(groupedKey);
  if (group === undefined) {
    group = newZeroLimitIncrementalGroupState(groupedKey);
    groups.set(groupedKey, group);
  }
  group.count += 1;
  return true;
};

const applyMaterializedIncrementalGroupedQueryBatch = <Row extends RowObject>(
  state: Extract<IncrementalGroupedQueryState, { readonly mode: "materialized" }>,
  plan: GroupedQueryPlan<Row>,
  matches: (row: Row) => boolean,
  batch: TopicRowChangeBatch<Row>,
  dirtyGroups: Set<MaterializedIncrementalGroupState>,
): boolean => {
  for (const change of batch.changes) {
    if (change.previous !== undefined) {
      const removed = removeMaterializedIncrementalGroupedMember(
        state.groups,
        plan,
        matches,
        change.key,
        change.previous,
        dirtyGroups,
      );
      if (removed) {
        state.memberCount -= 1;
      }
    }
    if (change.next !== undefined) {
      const upserted = upsertMaterializedIncrementalGroupedMember(
        state.groups,
        plan,
        matches,
        change.key,
        change.next,
        dirtyGroups,
      );
      if (upserted === undefined) {
        continue;
      }
      if (upserted.groupSize > maxIncrementalGroupedMembersPerGroup) {
        return false;
      }
      if (state.groups.size > maxIncrementalGroupedGroups) {
        return false;
      }
      if (upserted.inserted) {
        state.memberCount += 1;
        if (state.memberCount > maxIncrementalGroupedMembers) {
          return false;
        }
      }
    }
  }
  return true;
};

const applyCountOnlyIncrementalGroupedQueryBatch = <Row extends RowObject>(
  state: Extract<IncrementalGroupedQueryState, { readonly mode: "countOnly" }>,
  plan: GroupedQueryPlan<Row>,
  matches: (row: Row) => boolean,
  batch: TopicRowChangeBatch<Row>,
): boolean => {
  for (const change of batch.changes) {
    if (change.previous !== undefined) {
      removeCountOnlyIncrementalGroupedMember(state.groups, plan, matches, change.previous);
    }
    if (change.next !== undefined) {
      const inserted = upsertCountOnlyIncrementalGroupedMember(
        state.groups,
        plan,
        matches,
        change.next,
      );
      if (!inserted) {
        continue;
      }
      if (state.groups.size > maxIncrementalGroupedGroups) {
        return false;
      }
    }
  }
  return true;
};

const applyMaterializedIncrementalGroupedQueryBatches = <Row extends RowObject>(
  state: Extract<IncrementalGroupedQueryState, { readonly mode: "materialized" }>,
  plan: GroupedQueryPlan<Row>,
  matches: (row: Row) => boolean,
  batches: ReadonlyArray<TopicRowChangeBatch<Row>>,
): boolean => {
  const dirtyGroups = new Set<MaterializedIncrementalGroupState>();
  for (const batch of batches) {
    if (!applyMaterializedIncrementalGroupedQueryBatch(state, plan, matches, batch, dirtyGroups)) {
      state.groups.clear();
      state.memberCount = 0;
      return false;
    }
    state.version = batch.version;
  }
  for (const group of dirtyGroups) {
    recomputeIncrementalGroupState(group, plan.aggregates);
  }
  state.evaluation = evaluateIncrementalGroupedQuery(state, plan, state.version);
  return true;
};

const applyCountOnlyIncrementalGroupedQueryBatches = <Row extends RowObject>(
  state: Extract<IncrementalGroupedQueryState, { readonly mode: "countOnly" }>,
  plan: GroupedQueryPlan<Row>,
  matches: (row: Row) => boolean,
  batches: ReadonlyArray<TopicRowChangeBatch<Row>>,
): boolean => {
  for (const batch of batches) {
    if (!applyCountOnlyIncrementalGroupedQueryBatch(state, plan, matches, batch)) {
      state.groups.clear();
      return false;
    }
    state.version = batch.version;
  }
  state.evaluation = emptyGroupedEvaluation(state.groups.size, state.version);
  return true;
};

const applyIncrementalGroupedQueryBatches = <Row extends RowObject>(
  state: IncrementalGroupedQueryState,
  plan: GroupedQueryPlan<Row>,
  matches: (row: Row) => boolean,
  batches: ReadonlyArray<TopicRowChangeBatch<Row>>,
): boolean => {
  if (state.mode === "countOnly") {
    return applyCountOnlyIncrementalGroupedQueryBatches(state, plan, matches, batches);
  }
  return applyMaterializedIncrementalGroupedQueryBatches(state, plan, matches, batches);
};

const makeFallbackGroupedQueryExecution = <Row extends RowObject, ResultRow extends RowObject>(
  store: TopicRowScan<Row>,
  compiled: CompiledGroupedQuery<Row, ResultRow>,
): IncrementalGroupedQueryExecution<ResultRow> => {
  let snapshot = {
    evaluation: compiled.evaluate(store),
    version: store.version(),
  };
  return {
    incremental: false,
    latest: () => {
      const storeVersion = store.version();
      if (snapshot.version !== storeVersion) {
        snapshot = {
          evaluation: compiled.evaluate(store),
          version: storeVersion,
        };
      }
      return snapshot.evaluation;
    },
  };
};

export const makeIncrementalGroupedQueryExecution = <
  Row extends RowObject,
  ResultRow extends RowObject,
>(
  store: TopicRowScan<Row>,
  compiled: CompiledGroupedQuery<Row, ResultRow>,
  releaseRetainedChanges: () => void,
): IncrementalGroupedQueryExecution<ResultRow> => {
  let build = buildIncrementalGroupedQueryState(store, compiled.plan, compiled.matches);
  if (!build.admitted) {
    return makeFallbackGroupedQueryExecution(store, compiled);
  }
  let state = build.state;
  let fallback: IncrementalGroupedQueryExecution<ResultRow> | undefined;
  const activateFallback = (): IncrementalGroupedQueryExecution<ResultRow> => {
    clearIncrementalGroupedQueryState(state, compiled.plan, store.version());
    const nextFallback = makeFallbackGroupedQueryExecution(store, compiled);
    fallback = nextFallback;
    releaseRetainedChanges();
    return nextFallback;
  };
  return {
    get incremental() {
      return fallback === undefined;
    },
    latest: () => {
      if (fallback !== undefined) {
        return fallback.latest();
      }
      const storeVersion = store.version();
      if (state.version === storeVersion) {
        return typedGroupedEvaluation<ResultRow>(state.evaluation);
      }
      const batches = store.changesSince(state.version);
      if (batches === undefined) {
        build = buildIncrementalGroupedQueryState(store, compiled.plan, compiled.matches);
        if (!build.admitted) {
          return activateFallback().latest();
        }
        state = build.state;
        return typedGroupedEvaluation<ResultRow>(state.evaluation);
      }
      if (!applyIncrementalGroupedQueryBatches(state, compiled.plan, compiled.matches, batches)) {
        return activateFallback().latest();
      }
      return typedGroupedEvaluation<ResultRow>(state.evaluation);
    },
  };
};
