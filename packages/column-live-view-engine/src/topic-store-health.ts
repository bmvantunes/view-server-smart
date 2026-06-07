import { Clock, Effect } from "effect";
import type { TopicRuntimeHealth } from "@view-server/config";
import { activeStoreQueryExecutionCounts, type ActiveQueryExecutionCounts } from "./active-query";
import type { createTopicHealthLedger } from "./topic-health-ledger";
import { TopicStore, topicStoreReadModel, topicStoreState } from "./topic-store-state";
import type { LiveTopicSubscriber } from "./topic-subscriber";

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
  readonly healthLedger: ReturnType<typeof createTopicHealthLedger>;
  readonly subscribers: ReadonlySet<LiveTopicSubscriber>;
  readonly topic: string;
};

const topicStoreHealthState = (
  store: TopicStore,
  activeQueries: ActiveQueryExecutionCounts,
): TopicStoreHealthState => {
  const state = topicStoreState(store);
  return {
    activeQueries,
    healthLedger: state.healthLedger,
    subscribers: state.subscribers,
    topic: store.topic,
  };
};

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
    const activeQueries = yield* activeStoreQueryExecutionCounts(topicStoreReadModel(store));
    return yield* collectTopicStoreHealthView(topicStoreHealthState(store, activeQueries), closed);
  },
);
