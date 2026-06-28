import { Effect, Schema, Semaphore } from "effect";
import {
  activeStoreQueryExecutionCounts,
  clearStoreRawQueryExecutions,
  type ActiveQueryExecutionCounts,
  type ActiveQueryStoreState,
} from "./active-query";
import { TopicRowStorage } from "./topic-row-storage";
import { createTopicHealthLedger } from "./topic-health-ledger";
import type { TopicStoreMutationState } from "./topic-store-mutation";
import type { RawQueryCompilerMetadata } from "./raw-query-compiler";
import type { LiveTopicSubscriber } from "./topic-subscriber";

const topicStoreSubscriptionPermitBrand: unique symbol = Symbol("TopicStoreSubscriptionPermit");

export type TopicStoreSubscriptionPermit = {
  readonly [topicStoreSubscriptionPermitBrand]: true;
  readonly store: TopicStore;
};

export type TopicStoreState = TopicStoreMutationState;

const topicStoreStates = new WeakMap<TopicStore, TopicStoreState>();

export type TopicStoreHealthSource = {
  readonly healthLedger: ReturnType<typeof createTopicHealthLedger>;
  readonly subscribers: ReadonlySet<LiveTopicSubscriber>;
  readonly topic: string;
};

export class TopicStore {
  declare private readonly topicStoreBrand: void;

  constructor(
    readonly topic: string,
    schema: Schema.Codec<object, unknown, never, unknown>,
    keyField: string,
    onCommit: () => void,
  ) {
    const storage = new TopicRowStorage(topic, schema, keyField);
    const subscribers = new Set<LiveTopicSubscriber>();
    const state: TopicStoreState = {
      storage,
      subscribers,
      mutationSemaphore: Semaphore.makeUnsafe(1),
      notificationSemaphore: Semaphore.makeUnsafe(1),
      healthLedger: createTopicHealthLedger(),
      onCommit,
    };
    topicStoreStates.set(this, state);
  }
}

export const topicStoreState = (store: TopicStore): TopicStoreState => {
  return topicStoreStates.get(store)!;
};

export const makeTopicStoreSubscriptionPermit = (
  store: TopicStore,
): TopicStoreSubscriptionPermit => ({
  [topicStoreSubscriptionPermitBrand]: true,
  store,
});

export const openTopicStoreSubscriber = (
  permit: TopicStoreSubscriptionPermit,
  subscriber: LiveTopicSubscriber,
): void => {
  const state = topicStoreState(permit.store);
  state.healthLedger.openSubscription(subscriber);
  state.subscribers.add(subscriber);
};

export const closeTopicStoreSubscriber = (
  store: TopicStore,
  subscriber: LiveTopicSubscriber,
): void => {
  const state = topicStoreState(store);
  state.healthLedger.closeSubscription(subscriber);
  state.subscribers.delete(subscriber);
};

export const updateTopicStoreSubscriberQueueDepth = (
  store: TopicStore,
  subscriber: LiveTopicSubscriber,
  queueDepth: number,
): void => {
  topicStoreState(store).healthLedger.updateQueueDepth(subscriber, queueDepth);
  subscriber.maxQueueDepth = Math.max(subscriber.maxQueueDepth, queueDepth);
};

export const markTopicStoreSubscriberBackpressure = (
  store: TopicStore,
  subscriber: LiveTopicSubscriber,
): void => {
  topicStoreState(store).healthLedger.markBackpressure(subscriber);
  subscriber.backpressureEvents += 1;
};

export const topicStoreHealthSource = (store: TopicStore): TopicStoreHealthSource => {
  const state = topicStoreState(store);
  return {
    healthLedger: state.healthLedger,
    subscribers: state.subscribers,
    topic: store.topic,
  };
};

export const topicStoreRawQueryMetadata = (store: TopicStore): RawQueryCompilerMetadata =>
  topicStoreState(store).storage.rawQueryMetadata;

export const topicStoreReadModel = (store: TopicStore): ActiveQueryStoreState =>
  topicStoreState(store).storage.readModel;

export const clearTopicStoreQueryExecutions = Effect.fn(
  "ColumnLiveViewEngine.topicStore.queryExecutions.clear",
)((store: TopicStore) => clearStoreRawQueryExecutions(topicStoreReadModel(store)));

export const collectTopicStoreActiveQueryCounts = Effect.fn(
  "ColumnLiveViewEngine.topicStore.queryExecutions.count",
)(
  (store: TopicStore): Effect.Effect<ActiveQueryExecutionCounts> =>
    activeStoreQueryExecutionCounts(topicStoreReadModel(store)),
);
