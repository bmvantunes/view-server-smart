import { Effect } from "effect";
import type { TopicRuntimeHealth } from "@view-server/config";
import { collectTopicStoreHealth } from "./topic-store";
import type { TopicStore } from "./topic-store";

export type ColumnLiveViewTopicHealth = TopicRuntimeHealth;

export type ColumnLiveViewEngineHealth<Topics extends object = Record<string, object>> = {
  readonly status: "ready" | "stopping";
  readonly version: number;
  readonly topics: {
    readonly [Topic in Extract<keyof Topics, string>]: ColumnLiveViewTopicHealth;
  };
  readonly activeSubscriptions: number;
  readonly queuedEvents: number;
  readonly maxQueueDepth: number;
  readonly backpressureEvents: number;
};

export type EngineHealthRuntimeState = {
  readonly version: () => number;
  readonly closed: () => boolean;
};

function exactTopicHealth<Topics extends object>(
  topics: Record<string, ColumnLiveViewTopicHealth>,
): ColumnLiveViewEngineHealth<Topics>["topics"];
function exactTopicHealth(
  topics: Record<string, ColumnLiveViewTopicHealth>,
): Record<string, ColumnLiveViewTopicHealth> {
  return topics;
}

export const collectColumnLiveViewEngineHealth = Effect.fn("ColumnLiveViewEngine.health.collect")(
  function* <Topics extends object>(
    stores: ReadonlyMap<Extract<keyof Topics, string>, TopicStore>,
    engineState: EngineHealthRuntimeState,
  ) {
    const topics: Record<string, ColumnLiveViewTopicHealth> = {};
    let activeSubscriptions = 0;
    let queuedEvents = 0;
    let maxQueueDepth = 0;
    let backpressureEvents = 0;
    const closed = engineState.closed();
    const engineVersion = engineState.version();

    for (const [topic, store] of stores) {
      const topicHealth = yield* collectTopicStoreHealth(store, closed);
      activeSubscriptions += topicHealth.activeSubscriptions;
      const topicQueuedEvents = topicHealth.queuedEvents;
      const topicMaxQueueDepth = topicHealth.maxQueueDepth;
      const topicBackpressureEvents = topicHealth.backpressureEvents;

      queuedEvents += topicQueuedEvents;
      maxQueueDepth = Math.max(maxQueueDepth, topicMaxQueueDepth);
      backpressureEvents += topicBackpressureEvents;
      topics[topic] = {
        status: topicHealth.status,
        rowCount: topicHealth.rowCount,
        liveRowCount: topicHealth.liveRowCount,
        deletedRowCount: topicHealth.deletedRowCount,
        version: topicHealth.version,
        lastMutationAt: topicHealth.lastMutationAt,
        mutationsPerSecond: topicHealth.mutationsPerSecond,
        rowsPerSecond: topicHealth.rowsPerSecond,
        pendingMutationBatches: topicHealth.pendingMutationBatches,
        activeFallbackGroupedViews: topicHealth.activeFallbackGroupedViews,
        activeIncrementalGroupedViews: topicHealth.activeIncrementalGroupedViews,
        activeViews: topicHealth.activeViews,
        groupedFullEvaluationCount: topicHealth.groupedFullEvaluationCount,
        groupedPatchedEvaluationCount: topicHealth.groupedPatchedEvaluationCount,
        activeSubscriptions: topicHealth.activeSubscriptions,
        queuedEvents: topicQueuedEvents,
        maxQueueDepth: topicMaxQueueDepth,
        backpressureEvents: topicBackpressureEvents,
        memoryBytes: topicHealth.memoryBytes,
        tombstoneCount: topicHealth.tombstoneCount,
        compactionPending: topicHealth.compactionPending,
      };
    }

    return {
      status: closed ? "stopping" : "ready",
      version: engineVersion,
      topics: exactTopicHealth<Topics>(topics),
      activeSubscriptions,
      queuedEvents,
      maxQueueDepth,
      backpressureEvents,
    } satisfies ColumnLiveViewEngineHealth;
  },
);
