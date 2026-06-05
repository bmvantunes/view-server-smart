import { Schema, Semaphore } from "effect";
import type { ActiveQueryStoreState } from "./active-query";
import { ColumnarTopicStore } from "./columnar-topic-store";
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

export class TopicStore {
  declare private readonly topicStoreBrand: void;

  constructor(
    readonly topic: string,
    schema: Schema.Decoder<object>,
    keyField: string,
    onCommit: () => void,
  ) {
    const storage = new ColumnarTopicStore(topic, schema, keyField);
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

export const topicStoreRawQueryMetadata = (store: TopicStore): RawQueryCompilerMetadata =>
  topicStoreState(store).storage.rawQueryMetadata;

export const topicStoreReadModel = (store: TopicStore): ActiveQueryStoreState =>
  topicStoreState(store).storage.readModel;
