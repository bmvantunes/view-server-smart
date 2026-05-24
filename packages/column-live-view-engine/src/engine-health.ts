import type { LiveTopicSubscriber } from "./live-subscription.js";
import { Effect } from "effect";

type RowObject = object;

export type ColumnLiveViewTopicHealth = {
  readonly status: "ready" | "degraded";
  readonly rowCount: number;
  readonly version: number;
  readonly activeSubscriptions: number;
  readonly queuedEvents: number;
  readonly maxQueueDepth: number;
  readonly backpressureEvents: number;
};

export type ColumnLiveViewEngineHealth<Topics extends object = Record<string, object>> = {
  readonly status: "ready" | "stopping";
  readonly version: number;
  readonly topics: Readonly<Record<Extract<keyof Topics, string>, ColumnLiveViewTopicHealth>>;
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

export const collectColumnLiveViewEngineHealth = Effect.fn("ColumnLiveViewEngine.health.collect")(
  function* <Topics extends object, Row extends RowObject>(
    stores: ReadonlyMap<Extract<keyof Topics, string>, HealthTopicStoreState<Row>>,
    engineState: EngineHealthRuntimeState,
  ) {
    const topics = {} as Record<Extract<keyof Topics, string>, ColumnLiveViewTopicHealth>;
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
        version: store.version,
        activeSubscriptions: store.subscribers.size,
        queuedEvents: topicQueuedEvents,
        maxQueueDepth: topicMaxQueueDepth,
        backpressureEvents: topicBackpressureEvents,
      };
    }

    return {
      status: closed ? "stopping" : "ready",
      version: engineVersion,
      topics,
      activeSubscriptions,
      queuedEvents,
      maxQueueDepth,
      backpressureEvents,
    } satisfies ColumnLiveViewEngineHealth<Topics>;
  },
);
