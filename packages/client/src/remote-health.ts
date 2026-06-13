import type {
  TopicDefinitions,
  TopicRuntimeHealth,
  ViewServerHealth,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
} from "@view-server/config";
import { Effect } from "effect";
import { AtomRef } from "effect/unstable/reactivity";
import type { ViewServerLiveEvent } from "./live-client";

function typedHealthTopics<Topics extends TopicDefinitions>(
  topics: Record<string, TopicRuntimeHealth>,
): ViewServerHealth<Topics>["engine"]["topics"];
function typedHealthTopics(
  topics: Record<string, TopicRuntimeHealth>,
): Record<string, TopicRuntimeHealth> {
  return topics;
}

const topicHealthFromRow = (
  existing: TopicRuntimeHealth,
  row: ViewServerHealthTopicRow,
): TopicRuntimeHealth => ({
  status: row.status === "stopping" ? existing.status : row.status,
  rowCount: row.rowCount,
  liveRowCount: row.liveRowCount,
  deletedRowCount: row.deletedRowCount,
  version: row.version,
  lastMutationAt: row.lastMutationAt,
  mutationsPerSecond: row.mutationsPerSecond,
  rowsPerSecond: row.rowsPerSecond,
  pendingMutationBatches: row.pendingMutationBatches,
  activeFallbackGroupedViews: row.activeFallbackGroupedViews,
  activeIncrementalGroupedViews: row.activeIncrementalGroupedViews,
  activeViews: row.activeViews,
  groupedFullEvaluationCount: row.groupedFullEvaluationCount,
  groupedPatchedEvaluationCount: row.groupedPatchedEvaluationCount,
  activeSubscriptions: row.activeSubscriptions,
  queuedEvents: row.queuedEvents,
  maxQueueDepth: row.maxQueueDepth,
  backpressureEvents: row.backpressureEvents,
  memoryBytes: row.memoryBytes,
  tombstoneCount: row.tombstoneCount,
  compactionPending: row.compactionPending,
});

export type RemoteHealthState<Topics extends TopicDefinitions> = {
  readonly readonlyHealth: AtomRef.ReadonlyRef<ViewServerHealth<Topics>>;
  readonly markStopping: Effect.Effect<void>;
  readonly updateHealthSummaryRef: (
    event: ViewServerLiveEvent<ViewServerHealthSummaryRow<Topics>>,
  ) => Effect.Effect<void>;
  readonly updateHealthTopicRef: (
    event: ViewServerLiveEvent<ViewServerHealthTopicRow<Extract<keyof Topics, string>>>,
  ) => Effect.Effect<void>;
  readonly updateSubscriptionCount: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    delta: 1 | -1,
  ) => Effect.Effect<void>;
};

export const makeRemoteHealthState = <const Topics extends TopicDefinitions>(
  initialHealth: ViewServerHealth<Topics>,
): RemoteHealthState<Topics> => {
  const health = AtomRef.make(initialHealth);
  const updateHealthSummaryRef = (event: ViewServerLiveEvent<ViewServerHealthSummaryRow<Topics>>) =>
    Effect.sync(() => {
      const applySummaryRow = (row: ViewServerHealthSummaryRow<Topics>) => {
        health.update((current) => ({
          ...current,
          status: current.status === "stopping" ? "stopping" : row.runtimeStatus,
        }));
      };
      if (event.type === "snapshot") {
        for (const row of event.rows) {
          applySummaryRow(row);
        }
      }
      if (event.type === "delta") {
        for (const operation of event.operations) {
          if (operation.type === "insert" || operation.type === "update") {
            applySummaryRow(operation.row);
          }
        }
      }
    });

  const updateHealthTopicRef = (
    event: ViewServerLiveEvent<ViewServerHealthTopicRow<Extract<keyof Topics, string>>>,
  ) =>
    Effect.sync(() => {
      if (event.type === "snapshot") {
        health.update((current) => {
          const topics: Record<string, TopicRuntimeHealth> = { ...current.engine.topics };
          for (const row of event.rows) {
            topics[row.id] = topicHealthFromRow(current.engine.topics[row.id], row);
          }
          return {
            ...current,
            engine: {
              topics: typedHealthTopics<Topics>(topics),
            },
          };
        });
      }
      if (event.type === "delta") {
        health.update((current) => {
          const topics: Record<string, TopicRuntimeHealth> = { ...current.engine.topics };
          for (const operation of event.operations) {
            if (operation.type === "insert" || operation.type === "update") {
              topics[operation.key] = topicHealthFromRow(
                current.engine.topics[operation.row.id],
                operation.row,
              );
            }
          }
          return {
            ...current,
            engine: {
              topics: typedHealthTopics<Topics>(topics),
            },
          };
        });
      }
    });

  const updateLiveTopicHealth = <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    update: (current: TopicRuntimeHealth) => TopicRuntimeHealth,
  ) =>
    Effect.sync(() => {
      health.update((current) => {
        const topics: Record<string, TopicRuntimeHealth> = {
          ...current.engine.topics,
          [topic]: update(current.engine.topics[topic]),
        };
        return {
          ...current,
          engine: {
            topics: typedHealthTopics<Topics>(topics),
          },
        };
      });
    });

  const updateSubscriptionCount = <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    delta: 1 | -1,
  ) =>
    Effect.gen(function* () {
      yield* updateLiveTopicHealth(topic, (current) => {
        const activeSubscriptions = Math.max(0, current.activeSubscriptions + delta);
        return {
          ...current,
          activeSubscriptions,
        };
      });
      yield* Effect.sync(() => {
        health.update((current) => ({
          ...current,
          transport: {
            ...current.transport,
            activeStreams: Math.max(0, current.transport.activeStreams + delta),
            activeSubscriptions: Math.max(0, current.transport.activeSubscriptions + delta),
          },
        }));
      });
    });

  const markStopping = Effect.sync(() => {
    health.update((current) => ({
      ...current,
      status: "stopping",
    }));
  });

  return {
    readonlyHealth: health.map((value) => value),
    markStopping,
    updateHealthSummaryRef,
    updateHealthTopicRef,
    updateSubscriptionCount,
  };
};
