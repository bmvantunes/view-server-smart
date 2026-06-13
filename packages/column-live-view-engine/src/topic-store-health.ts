import { Clock, Effect } from "effect";
import type { TopicRuntimeHealth } from "@view-server/config";
import type { ActiveQueryExecutionCounts } from "./active-query";
import {
  collectTopicStoreActiveQueryCounts,
  TopicStore,
  topicStoreHealthSource,
  type TopicStoreHealthSource,
} from "./topic-store-state";

export type TopicStoreHealthView = {
  readonly topic: string;
  readonly status: "ready" | "degraded";
  readonly rowCount: number;
  readonly liveRowCount: number;
  readonly deletedRowCount: number;
  readonly version: number;
  readonly lastMutationAt: number | null;
  readonly mutationsPerSecond: number;
  readonly rowsPerSecond: number;
  readonly pendingMutationBatches: number;
  readonly activeFallbackGroupedViews: number;
  readonly activeIncrementalGroupedViews: number;
  readonly activeViews: number;
  readonly groupedFullEvaluationCount: number;
  readonly groupedPatchedEvaluationCount: number;
  readonly activeSubscriptions: number;
  readonly queuedEvents: number;
  readonly maxQueueDepth: number;
  readonly backpressureEvents: number;
  readonly memoryBytes: number;
  readonly tombstoneCount: number;
  readonly compactionPending: boolean;
};

export type TopicStoreHealthState = {
  readonly activeQueries: ActiveQueryExecutionCounts;
} & TopicStoreHealthSource;

export const collectTopicStoreHealthView = Effect.fn(
  "ColumnLiveViewEngine.topicStore.healthView.collect",
)(function* (state: TopicStoreHealthState, closed: boolean) {
  const totals = state.healthLedger.snapshot(yield* Clock.currentTimeMillis);
  let queuedEvents = 0;

  for (const subscriber of state.subscribers) {
    const currentQueuedEvents = yield* subscriber.queuedEvents;
    queuedEvents += currentQueuedEvents;
  }

  const activeSubscriptions = state.subscribers.size;
  const status: TopicRuntimeHealth["status"] = closed ? "degraded" : "ready";
  const lastMutationAt = totals.lastMutationAt;
  const rowsPerSecond = totals.rowsPerSecond;
  const health: TopicStoreHealthView = {
    topic: state.topic,
    status,
    rowCount: totals.rowCount,
    liveRowCount: totals.rowCount,
    deletedRowCount: 0,
    version: totals.version,
    lastMutationAt,
    mutationsPerSecond: totals.mutationsPerSecond,
    rowsPerSecond,
    pendingMutationBatches: totals.pendingMutationBatches,
    activeFallbackGroupedViews: state.activeQueries.activeFallbackGroupedViews,
    activeIncrementalGroupedViews: state.activeQueries.activeIncrementalGroupedViews,
    activeViews: state.activeQueries.activeViews,
    groupedFullEvaluationCount: state.activeQueries.groupedFullEvaluationCount,
    groupedPatchedEvaluationCount: state.activeQueries.groupedPatchedEvaluationCount,
    activeSubscriptions,
    queuedEvents,
    maxQueueDepth: totals.maxQueueDepth,
    backpressureEvents: totals.backpressureEvents,
    memoryBytes: 0,
    tombstoneCount: 0,
    compactionPending: false,
  };
  return health;
});

export const collectTopicStoreHealth = Effect.fn("ColumnLiveViewEngine.topicStore.health")(
  function* (store: TopicStore, closed: boolean) {
    const activeQueries = yield* collectTopicStoreActiveQueryCounts(store);
    return yield* collectTopicStoreHealthView(
      {
        ...topicStoreHealthSource(store),
        activeQueries,
      },
      closed,
    );
  },
);
