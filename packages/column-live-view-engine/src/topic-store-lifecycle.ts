import type { StatusEvent } from "@view-server/config";
import { Effect } from "effect";
import {
  withTopicStoreStateTransition,
  type TopicStoreMutationState,
} from "./topic-store-mutation";
import { clearTopicStoreQueryExecutions, type TopicStore } from "./topic-store-state";
import type { LiveTopicSubscriber } from "./topic-subscriber";

const resetStatusEvent = (store: TopicStore, subscriber: LiveTopicSubscriber): StatusEvent => ({
  type: "status",
  topic: store.topic,
  queryId: subscriber.queryId,
  status: "closed",
  code: "SubscriptionClosed",
  message: "Subscription closed because the engine reset.",
});

const engineClosedStatusEvent = (
  store: TopicStore,
  subscriber: LiveTopicSubscriber,
): StatusEvent => ({
  type: "status",
  topic: store.topic,
  queryId: subscriber.queryId,
  status: "closed",
  code: "SubscriptionClosed",
  message: "Subscription closed because the engine closed.",
});

const drainTopicStoreSubscribersForReset = (
  state: TopicStoreMutationState,
): ReadonlyArray<LiveTopicSubscriber> => {
  state.storage.clear();
  const closingSubscribers = [...state.subscribers];
  for (const subscriber of closingSubscribers) {
    subscriber.closed = true;
  }
  state.subscribers.clear();
  state.healthLedger.reset();
  return closingSubscribers;
};

const drainTopicStoreSubscribersForClose = (
  state: TopicStoreMutationState,
): ReadonlyArray<LiveTopicSubscriber> => {
  const closingSubscribers = [...state.subscribers];
  for (const subscriber of closingSubscribers) {
    subscriber.closed = true;
    state.healthLedger.closeSubscription(subscriber);
  }
  state.subscribers.clear();
  return closingSubscribers;
};

export const resetTopicStore = Effect.fn("ColumnLiveViewEngine.topicStore.reset")(function* (
  store: TopicStore,
) {
  yield* withTopicStoreStateTransition(store, (state) =>
    Effect.gen(function* () {
      const subscribers = yield* Effect.sync(() => {
        return drainTopicStoreSubscribersForReset(state);
      });
      yield* clearTopicStoreQueryExecutions(store);
      for (const subscriber of subscribers) {
        yield* subscriber.closeWithStatus(resetStatusEvent(store, subscriber));
      }
    }),
  );
});

export const closeTopicStoreSubscriptions = Effect.fn(
  "ColumnLiveViewEngine.topicStore.closeSubscriptions",
)(function* (store: TopicStore) {
  yield* withTopicStoreStateTransition(store, (state) =>
    Effect.gen(function* () {
      const subscribers = yield* Effect.sync(() => {
        return drainTopicStoreSubscribersForClose(state);
      });
      yield* clearTopicStoreQueryExecutions(store);
      for (const subscriber of subscribers) {
        yield* subscriber.closeWithStatus(engineClosedStatusEvent(store, subscriber));
      }
    }),
  );
});
