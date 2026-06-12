import { Effect } from "effect";
import {
  acquireSubscriptionHandoff,
  type MarkAcquiredSubscription,
  type SubscriptionHandoffOptions,
} from "./subscription-handoff";
import type { LiveTopicSubscriber } from "./topic-subscriber";
import { withTopicStoreNotification, withTopicStoreTransaction } from "./topic-store-mutation";
import {
  closeTopicStoreSubscriber,
  markTopicStoreSubscriberBackpressure,
  makeTopicStoreSubscriptionPermit,
  openTopicStoreSubscriber,
  type TopicStore,
  type TopicStoreSubscriptionPermit,
  updateTopicStoreSubscriberQueueDepth,
} from "./topic-store-state";

export function acquireTopicStoreSubscription<
  Subscription extends { readonly close: () => Effect.Effect<void, never> },
  Error,
  Requirements,
>(
  store: TopicStore,
  acquire: (
    permit: TopicStoreSubscriptionPermit,
    markAcquired: MarkAcquiredSubscription<Subscription>,
  ) => Effect.Effect<Subscription, Error, Requirements>,
  options: SubscriptionHandoffOptions = {},
): Effect.Effect<Subscription, Error, Requirements> {
  return acquireSubscriptionHandoff(
    (markAcquired: (subscription: Subscription) => Effect.Effect<void>) =>
      withTopicStoreTransaction(
        store,
        acquire(makeTopicStoreSubscriptionPermit(store), markAcquired),
      ),
    options,
  );
}

export const registerTopicStoreSubscription = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.add",
)(function (permit: TopicStoreSubscriptionPermit, subscriber: LiveTopicSubscriber) {
  return Effect.sync(() => {
    openTopicStoreSubscriber(permit, subscriber);
  });
});

const unregisterTopicStoreSubscription = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.remove",
)(function (store: TopicStore, subscriber: LiveTopicSubscriber) {
  return Effect.sync(() => {
    closeTopicStoreSubscriber(store, subscriber);
  });
});

export const trackTopicStoreSubscriptionQueueDepth = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.queueDepth",
)((store: TopicStore, subscriber: LiveTopicSubscriber, queueDepth: number) =>
  Effect.sync(() => {
    updateTopicStoreSubscriberQueueDepth(store, subscriber, queueDepth);
  }),
);

const reportTopicStoreSubscriptionBackpressure = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.backpressure",
)((store: TopicStore, subscriber: LiveTopicSubscriber) =>
  Effect.sync(() => {
    markTopicStoreSubscriberBackpressure(store, subscriber);
  }),
);

export const closeTopicStoreSubscription = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.close",
)(function* (store: TopicStore, subscriber: LiveTopicSubscriber, finalize: Effect.Effect<void>) {
  yield* withTopicStoreNotification(
    store,
    withTopicStoreTransaction(
      store,
      Effect.gen(function* () {
        if (subscriber.closed) {
          return;
        }
        subscriber.closed = true;
        yield* unregisterTopicStoreSubscription(store, subscriber);
        yield* finalize;
      }),
    ),
  );
});

export const closeBackpressuredTopicStoreSubscription = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.closeBackpressured",
)(function* (store: TopicStore, subscriber: LiveTopicSubscriber, finalize: Effect.Effect<void>) {
  yield* withTopicStoreTransaction(
    store,
    Effect.gen(function* () {
      if (subscriber.closed) {
        return;
      }
      subscriber.closed = true;
      yield* reportTopicStoreSubscriptionBackpressure(store, subscriber);
      yield* unregisterTopicStoreSubscription(store, subscriber);
      yield* finalize;
    }),
  );
});
