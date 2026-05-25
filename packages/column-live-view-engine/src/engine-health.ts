import type { LiveTopicSubscriber } from "./live-subscription";
import { Effect } from "effect";
import type { TopicRuntimeHealth } from "@view-server/config";

type RowObject = object;

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

export type HealthTopicStoreState<Row extends RowObject> = {
  readonly rows: ReadonlyMap<string, Row>;
  readonly subscribers: ReadonlySet<LiveTopicSubscriber<Row>>;
  readonly version: number;
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
  function* <Topics extends object, Row extends RowObject>(
    stores: ReadonlyMap<Extract<keyof Topics, string>, HealthTopicStoreState<Row>>,
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
      activeSubscriptions += store.subscribers.size;
      let topicQueuedEvents = 0;
      let topicMaxQueueDepth = store.maxQueueDepth;
      let topicBackpressureEvents = store.backpressureEvents;

      for (const subscriber of store.subscribers) {
        const subscriberQueueDepth = yield* subscriber.queuedEvents;
        topicQueuedEvents += subscriberQueueDepth;
        topicMaxQueueDepth = Math.max(topicMaxQueueDepth, subscriber.maxQueueDepth);
        topicBackpressureEvents += subscriber.backpressureEvents;
      }

      queuedEvents += topicQueuedEvents;
      maxQueueDepth = Math.max(maxQueueDepth, topicMaxQueueDepth);
      backpressureEvents += topicBackpressureEvents;
      topics[topic] = {
        status: closed ? "degraded" : "ready",
        rowCount: store.rows.size,
        liveRowCount: store.rows.size,
        deletedRowCount: 0,
        version: store.version,
        lastMutationAt: null,
        mutationsPerSecond: 0,
        rowsPerSecond: 0,
        pendingMutationBatches: 0,
        activeViews: store.subscribers.size,
        activeSubscriptions: store.subscribers.size,
        queuedEvents: topicQueuedEvents,
        maxQueueDepth: topicMaxQueueDepth,
        backpressureEvents: topicBackpressureEvents,
        memoryBytes: 0,
        tombstoneCount: 0,
        compactionPending: false,
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
