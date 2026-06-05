import { Effect } from "effect";
import type { StatusEvent } from "@view-server/config";
import {
  acquireMaterializedQueryExecution,
  acquireRawQueryExecution,
  activeStoreRawQueryExecutionCount,
  clearStoreRawQueryExecutions,
  evaluateRawQuery,
  releaseMaterializedQueryExecution,
  releaseRawQueryExecution,
} from "./active-query";
import {
  evaluateCompiledGroupedQuery,
  prepareGroupedQuery,
  type CompiledGroupedQuery,
} from "./grouped-query-compiler";
import { makeIncrementalGroupedQueryExecution } from "./grouped-incremental-execution";
import {
  runTopicStoreMutationTransaction,
  withTopicStoreNotification,
  withTopicStoreTransaction,
} from "./topic-store-mutation";
import { prepareRawQuery, type CompiledRawQuery } from "./raw-query-compiler";
import type { QueryEvaluation } from "./query-result";
import type { InvalidRowErrorFactory } from "./topic-row-preparation";
import {
  acquireSubscriptionHandoff,
  type MarkAcquiredSubscription,
  type SubscriptionHandoffOptions,
} from "./subscription-handoff";
import type { LiveTopicSubscriber } from "./topic-subscriber";
import { collectTopicStoreHealthView, type TopicStoreHealthState } from "./topic-store-health";
import {
  makeTopicStoreSubscriptionPermit,
  TopicStore,
  topicStoreRawQueryMetadata,
  topicStoreReadModel,
  topicStoreState,
  type TopicStoreState,
  type TopicStoreSubscriptionPermit,
} from "./topic-store-state";

type RowObject = object;

export { TopicStore, topicStoreRawQueryMetadata, topicStoreReadModel };
export type { TopicStoreSubscriptionPermit } from "./topic-store-state";

export const prepareTopicStoreRawQuery = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.raw.prepare",
)(function* <ResultRow extends RowObject>(store: TopicStore, query: unknown) {
  return yield* prepareRawQuery<object, ResultRow>(
    store.topic,
    topicStoreState(store).storage.rawQueryMetadata,
    query,
  );
});

export const prepareTopicStoreGroupedQuery = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.grouped.prepare",
)(function* <ResultRow extends RowObject>(store: TopicStore, query: unknown) {
  return yield* prepareGroupedQuery<object, ResultRow>(
    store.topic,
    topicStoreState(store).storage.rawQueryMetadata,
    query,
  );
});

export const evaluateTopicStoreRawQuery = <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledRawQuery<object, ResultRow>,
): QueryEvaluation<ResultRow> =>
  evaluateRawQuery(topicStoreState(store).storage.readModel, compiled);

export const evaluateTopicStoreGroupedQuery = <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledGroupedQuery<object, ResultRow>,
): QueryEvaluation<ResultRow> =>
  evaluateCompiledGroupedQuery(topicStoreState(store).storage.readModel, compiled);

export const acquireTopicStoreRawQueryExecution = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.raw.acquire",
)(function* <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledRawQuery<object, ResultRow>,
) {
  return yield* acquireRawQueryExecution(topicStoreState(store).storage.readModel, compiled);
});

export const releaseTopicStoreRawQueryExecution = <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledRawQuery<object, ResultRow>,
): Effect.Effect<void> =>
  releaseRawQueryExecution(topicStoreState(store).storage.readModel, compiled);

export const acquireTopicStoreMaterializedQueryExecution = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.materialized.acquire",
)(function* <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledGroupedQuery<object, ResultRow>,
) {
  const readModel = topicStoreState(store).storage.readModel;
  return yield* acquireMaterializedQueryExecution(
    readModel,
    compiled.cacheKey,
    (releaseRetainedChanges) =>
      makeIncrementalGroupedQueryExecution(readModel, compiled, releaseRetainedChanges),
  );
});

export const releaseTopicStoreMaterializedQueryExecution = <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledGroupedQuery<object, ResultRow>,
): Effect.Effect<void> =>
  releaseMaterializedQueryExecution(topicStoreState(store).storage.readModel, compiled.cacheKey);

const topicStoreHealthState = (store: TopicStore, activeViews: number): TopicStoreHealthState => {
  const state = topicStoreState(store);
  return {
    activeViews,
    healthLedger: state.healthLedger,
    subscribers: state.subscribers,
    topic: store.topic,
  };
};

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
        topicStoreState(store),
        acquire(makeTopicStoreSubscriptionPermit(store), markAcquired),
      ),
    options,
  );
}

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

export const collectTopicStoreHealth = Effect.fn("ColumnLiveViewEngine.topicStore.health")(
  function* (store: TopicStore, closed: boolean) {
    const activeViews = yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store));
    return yield* collectTopicStoreHealthView(topicStoreHealthState(store, activeViews), closed);
  },
);

export const registerTopicStoreSubscription = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.add",
)(function (permit: TopicStoreSubscriptionPermit, subscriber: LiveTopicSubscriber) {
  return Effect.sync(() => {
    const state = topicStoreState(permit.store);
    state.healthLedger.openSubscription(subscriber);
    state.subscribers.add(subscriber);
  });
});

const unregisterTopicStoreSubscription = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.remove",
)(function (store: TopicStore, subscriber: LiveTopicSubscriber) {
  return Effect.sync(() => {
    const state = topicStoreState(store);
    state.healthLedger.closeSubscription(subscriber);
    state.subscribers.delete(subscriber);
  });
});

const drainTopicStoreSubscribersForReset = (
  state: TopicStoreState,
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

function drainTopicStoreSubscribersForClose(
  state: TopicStoreState,
): ReadonlyArray<LiveTopicSubscriber> {
  const closingSubscribers = Array.from(state.subscribers);
  for (const subscriber of closingSubscribers) {
    subscriber.closed = true;
    state.healthLedger.closeSubscription(subscriber);
  }
  state.subscribers.clear();
  return closingSubscribers;
}

export const trackTopicStoreSubscriptionQueueDepth = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.queueDepth",
)((store: TopicStore, subscriber: LiveTopicSubscriber, queueDepth: number) =>
  Effect.sync(() => {
    topicStoreState(store).healthLedger.updateQueueDepth(subscriber, queueDepth);
    subscriber.maxQueueDepth = Math.max(subscriber.maxQueueDepth, queueDepth);
  }),
);

const reportTopicStoreSubscriptionBackpressure = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.backpressure",
)((store: TopicStore, subscriber: LiveTopicSubscriber) =>
  Effect.sync(() => {
    topicStoreState(store).healthLedger.markBackpressure(subscriber);
    subscriber.backpressureEvents += 1;
  }),
);

export const closeTopicStoreSubscription = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.close",
)(function* (store: TopicStore, subscriber: LiveTopicSubscriber, finalize: Effect.Effect<void>) {
  const state = topicStoreState(store);
  yield* withTopicStoreNotification(
    state,
    withTopicStoreTransaction(
      state,
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
  const state = topicStoreState(store);
  yield* withTopicStoreTransaction(
    state,
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

export const resetTopicStore = Effect.fn("ColumnLiveViewEngine.topicStore.reset")(function* (
  store: TopicStore,
) {
  const state = topicStoreState(store);
  yield* withTopicStoreNotification(
    state,
    withTopicStoreTransaction(
      state,
      Effect.gen(function* () {
        const subscribers = yield* Effect.sync(() => {
          return drainTopicStoreSubscribersForReset(topicStoreState(store));
        });
        yield* clearStoreRawQueryExecutions(topicStoreReadModel(store));
        for (const subscriber of subscribers) {
          yield* subscriber.closeWithStatus(resetStatusEvent(store, subscriber));
        }
      }),
    ),
  );
});

export const closeTopicStoreSubscriptions = Effect.fn(
  "ColumnLiveViewEngine.topicStore.closeSubscriptions",
)(function* (store: TopicStore) {
  const state = topicStoreState(store);
  yield* withTopicStoreNotification(
    state,
    withTopicStoreTransaction(
      state,
      Effect.gen(function* () {
        const subscribers = yield* Effect.sync(() => {
          return drainTopicStoreSubscribersForClose(topicStoreState(store));
        });
        yield* clearStoreRawQueryExecutions(topicStoreReadModel(store));
        for (const subscriber of subscribers) {
          yield* subscriber.closeWithStatus(engineClosedStatusEvent(store, subscriber));
        }
      }),
    ),
  );
});

export const publishTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.publish")(function* <
  Error,
  Row extends RowObject,
>(store: TopicStore, row: Row, invalidRow: InvalidRowErrorFactory<Error>) {
  const prepared = yield* topicStoreState(store).storage.prepareRow(row, invalidRow);
  yield* runTopicStoreMutationTransaction(topicStoreState(store), store, (mutation) =>
    Effect.sync(() => {
      return mutation.publishPrepared(prepared);
    }),
  );
});

export const publishTopicStoreRows = Effect.fn("ColumnLiveViewEngine.topicStore.publishMany")(
  function* <Error, Row extends RowObject>(
    store: TopicStore,
    rows: ReadonlyArray<Row>,
    invalidRow: InvalidRowErrorFactory<Error>,
  ) {
    const preparedRows = yield* topicStoreState(store).storage.prepareRows(rows, invalidRow);
    yield* runTopicStoreMutationTransaction(topicStoreState(store), store, (mutation) =>
      Effect.sync(() => {
        return mutation.publishPreparedMany(preparedRows);
      }),
    );
  },
);

export const patchTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.patch")(function* <
  Patch extends Partial<RowObject>,
  Error,
>(store: TopicStore, key: string, patch: Patch, invalidRow: InvalidRowErrorFactory<Error>) {
  yield* runTopicStoreMutationTransaction(topicStoreState(store), store, (mutation) =>
    mutation.patch(key, patch, invalidRow),
  );
});

export const deleteTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.delete")(function* (
  store: TopicStore,
  key: string,
) {
  yield* runTopicStoreMutationTransaction(topicStoreState(store), store, (mutation) =>
    Effect.sync(() => {
      return mutation.delete(key);
    }),
  );
});
