import { Effect, Option } from "effect";
import type { DeltaEvent, SnapshotEvent } from "@view-server/config";
import type { QueryEvaluation } from "./query-result";
import type { TopicRawWindowScan } from "./raw-window-scan";
import type { TopicRowScan } from "./row-scan";
import {
  activeRawQueryExecutionCount,
  clearRawQueryExecutions,
  type RawQueryExecutionSlot,
} from "./active-raw-query";
import type { MaterializedQueryExecutionSlot } from "./active-materialized-query";
import {
  activeMaterializedQueryExecutionCount,
  activeMaterializedQueryExecutionModeCounts,
  clearMaterializedQueryExecutions,
} from "./active-materialized-query";
export {
  acquireRawQueryExecution,
  evaluateRawQuery,
  releaseRawQueryExecution,
} from "./active-raw-query";
export {
  acquireMaterializedQueryExecution,
  releaseMaterializedQueryExecution,
} from "./active-materialized-query";
export type { MaterializedQueryExecution } from "./active-materialized-query";

type RowObject = object;

export type ActiveQueryStoreState = TopicRowScan<object> &
  TopicRawWindowScan<object> & {
    readonly activeQueries: ActiveQueryRegistry;
    readonly releaseChanges: () => void;
    readonly retainChanges: () => void;
    readonly topic: string;
  };

export type LiveQueryExecutionCursor<ResultRow extends RowObject> = {
  evaluation: QueryEvaluation<ResultRow>;
};

type RawQueryExecutionUpdate<ResultRow extends RowObject> = Effect.Effect<
  Option.Option<DeltaEvent<ResultRow>>,
  never,
  never
>;

export type LiveQueryExecution<ResultRow extends RowObject> = {
  readonly initial: (queryId: string) => SnapshotEvent<ResultRow>;
  readonly createCursor: () => LiveQueryExecutionCursor<ResultRow>;
  readonly next: (
    queryId: string,
    cursor: LiveQueryExecutionCursor<ResultRow>,
  ) => RawQueryExecutionUpdate<ResultRow>;
};

export type RawQueryExecution<ResultRow extends RowObject> = LiveQueryExecution<ResultRow>;

export type ActiveQueryRegistry = {
  readonly raw: Map<string, RawQueryExecutionSlot>;
  readonly materialized: Map<string, MaterializedQueryExecutionSlot>;
};

export type ActiveQueryExecutionCounts = {
  readonly activeFallbackGroupedViews: number;
  readonly activeIncrementalGroupedViews: number;
  readonly activeViews: number;
  readonly groupedFullEvaluationCount: number;
  readonly groupedPatchedEvaluationCount: number;
};

export const createActiveQueryRegistry = (): ActiveQueryRegistry => ({
  raw: new Map(),
  materialized: new Map(),
});

export const clearStoreRawQueryExecutions = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.clearStore",
)((store: ActiveQueryStoreState) =>
  Effect.uninterruptible(
    Effect.gen(function* () {
      yield* clearRawQueryExecutions(store);
      yield* clearMaterializedQueryExecutions(store);
    }),
  ),
);

export const activeStoreRawQueryExecutionCount = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.countStore",
)((store: ActiveQueryStoreState) =>
  Effect.gen(function* () {
    const rawCount = yield* activeRawQueryExecutionCount(store);
    const materializedCount = yield* activeMaterializedQueryExecutionCount(store);
    return rawCount + materializedCount;
  }),
);

export const activeStoreQueryExecutionCounts = Effect.fn(
  "ColumnLiveViewEngine.activeQuery.countStoreModes",
)((store: ActiveQueryStoreState) =>
  Effect.gen(function* () {
    const rawCount = yield* activeRawQueryExecutionCount(store);
    const materializedCounts = yield* activeMaterializedQueryExecutionModeCounts(store);
    return {
      activeFallbackGroupedViews: materializedCounts.activeFallback,
      activeIncrementalGroupedViews: materializedCounts.activeIncremental,
      activeViews: rawCount + materializedCounts.activeTotal,
      groupedFullEvaluationCount: materializedCounts.groupedFullEvaluationCount,
      groupedPatchedEvaluationCount: materializedCounts.groupedPatchedEvaluationCount,
    } satisfies ActiveQueryExecutionCounts;
  }),
);
