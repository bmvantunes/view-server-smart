import { Effect, Option } from "effect";
import type { SnapshotEvent, DeltaEvent } from "@view-server/config";
import type { ActiveQueryStoreState, LiveQueryExecution } from "./active-query";
import { deltaEvent, deltaOperations, snapshotEvent } from "./query-result";
import type { QueryEvaluation } from "./query-result";
import type { GroupedIncrementalExecutionDiagnosticCounts } from "./grouped-incremental-execution";

type RowObject = object;

type ActiveMaterializedQueryExecution = {
  readonly diagnostics: () => GroupedIncrementalExecutionDiagnosticCounts;
  readonly incremental: boolean;
  readonly latest: () => QueryEvaluation<object>;
};

export type MaterializedQueryExecution<ResultRow extends RowObject> = {
  readonly diagnostics: () => GroupedIncrementalExecutionDiagnosticCounts;
  readonly incremental: boolean;
  readonly latest: () => QueryEvaluation<ResultRow>;
};

export type MaterializedQueryExecutionSlot = {
  readonly execution: ActiveMaterializedQueryExecution;
  readonly releaseRetainedChanges: () => void;
  refs: number;
};

export type MaterializedQueryExecutionModeCounts = {
  readonly activeFallback: number;
  readonly activeIncremental: number;
  readonly activeTotal: number;
  readonly groupedFullEvaluationCount: number;
  readonly groupedPatchedEvaluationCount: number;
};

const getActiveMaterializedQueryMap = (
  store: ActiveQueryStoreState,
): Map<string, MaterializedQueryExecutionSlot> => {
  return store.activeQueries.materialized;
};

const leaseMaterializedQueryExecution = <ResultRow extends RowObject>(
  store: ActiveQueryStoreState,
  execution: ActiveMaterializedQueryExecution,
): LiveQueryExecution<ResultRow> => {
  const latestEvaluation = () => typedQueryEvaluation<ResultRow>(execution.latest());

  return {
    initial: (queryId): SnapshotEvent<ResultRow> =>
      snapshotEvent(store, queryId, latestEvaluation()),
    createCursor: () => ({
      evaluation: latestEvaluation(),
    }),
    next: (queryId, cursor): Effect.Effect<Option.Option<DeltaEvent<ResultRow>>> =>
      Effect.sync(() => {
        const previous = cursor.evaluation;
        const next = latestEvaluation();
        const operations = deltaOperations(previous, next);
        if (operations.length === 0 && previous.totalRows === next.totalRows) {
          return Option.none();
        }
        cursor.evaluation = next;
        return Option.some(deltaEvent(store, queryId, previous.version, next, operations));
      }),
  };
};

function typedQueryEvaluation<ResultRow extends RowObject>(
  evaluation: QueryEvaluation<object>,
): QueryEvaluation<ResultRow>;
function typedQueryEvaluation(evaluation: QueryEvaluation<object>): QueryEvaluation<object> {
  return evaluation;
}

export const acquireMaterializedQueryExecution = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.materialized.acquire",
)(function <ResultRow extends RowObject>(
  store: ActiveQueryStoreState,
  cacheKey: string,
  makeExecution: (releaseRetainedChanges: () => void) => MaterializedQueryExecution<ResultRow>,
) {
  return Effect.sync(() => {
    const map = getActiveMaterializedQueryMap(store);
    const existing = map.get(cacheKey);
    if (existing !== undefined) {
      const entry = existing;
      entry.refs += 1;
      return leaseMaterializedQueryExecution<ResultRow>(store, entry.execution);
    }

    let retainedChanges = false;
    const releaseRetainedChanges = () => {
      if (!retainedChanges) {
        return;
      }
      retainedChanges = false;
      store.releaseChanges();
    };
    const execution = makeExecution(releaseRetainedChanges);
    if (execution.incremental) {
      retainedChanges = true;
      store.retainChanges();
    }
    map.set(cacheKey, {
      execution,
      releaseRetainedChanges,
      refs: 1,
    });
    return leaseMaterializedQueryExecution<ResultRow>(store, execution);
  });
});

export const releaseMaterializedQueryExecution = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.materialized.release",
)((store: ActiveQueryStoreState, cacheKey: string) =>
  Effect.sync(() => {
    const map = getActiveMaterializedQueryMap(store);
    const existing = map.get(cacheKey);
    if (existing === undefined) {
      return undefined;
    }
    const entry = existing;
    if (entry.refs > 1) {
      entry.refs -= 1;
      return undefined;
    }
    entry.releaseRetainedChanges();
    map.delete(cacheKey);
    return undefined;
  }),
);

export const clearMaterializedQueryExecutions = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.materialized.clearStore",
)((store: ActiveQueryStoreState) =>
  Effect.sync(() => {
    const map = getActiveMaterializedQueryMap(store);
    for (const entry of map.values()) {
      entry.releaseRetainedChanges();
    }
    map.clear();
  }),
);

export const activeMaterializedQueryExecutionCount = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.materialized.countStore",
)((store: ActiveQueryStoreState) => Effect.sync(() => getActiveMaterializedQueryMap(store).size));

export const activeMaterializedQueryExecutionModeCounts = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.materialized.countModes",
)((store: ActiveQueryStoreState) =>
  Effect.sync(() => {
    let activeFallback = 0;
    let activeIncremental = 0;
    let groupedFullEvaluationCount = 0;
    let groupedPatchedEvaluationCount = 0;

    for (const entry of getActiveMaterializedQueryMap(store).values()) {
      if (entry.execution.incremental) {
        activeIncremental += 1;
      } else {
        activeFallback += 1;
      }
      const diagnostics = entry.execution.diagnostics();
      groupedFullEvaluationCount += diagnostics.fullEvaluationCount;
      groupedPatchedEvaluationCount += diagnostics.patchedEvaluationCount;
    }

    return {
      activeFallback,
      activeIncremental,
      activeTotal: activeFallback + activeIncremental,
      groupedFullEvaluationCount,
      groupedPatchedEvaluationCount,
    } satisfies MaterializedQueryExecutionModeCounts;
  }),
);
