import { Effect } from "effect";
import {
  acquireMaterializedQueryExecution,
  acquireRawQueryExecution,
  activeStoreRawQueryExecutionCount,
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
import { prepareRawQuery, type CompiledRawQuery } from "./raw-query-compiler";
import type { QueryEvaluation } from "./query-result";
import { collectTopicStoreHealthView, type TopicStoreHealthState } from "./topic-store-health";
import {
  TopicStore,
  topicStoreRawQueryMetadata,
  topicStoreReadModel,
  topicStoreState,
} from "./topic-store-state";

type RowObject = object;

export { TopicStore, topicStoreRawQueryMetadata, topicStoreReadModel };
export {
  deleteTopicStoreRow,
  patchTopicStoreRow,
  publishTopicStoreRow,
  publishTopicStoreRows,
} from "./topic-store-mutation";
export { closeTopicStoreSubscriptions, resetTopicStore } from "./topic-store-lifecycle";
export {
  acquireTopicStoreSubscription,
  closeBackpressuredTopicStoreSubscription,
  closeTopicStoreSubscription,
  registerTopicStoreSubscription,
  trackTopicStoreSubscriptionQueueDepth,
} from "./topic-store-subscription";
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

export const collectTopicStoreHealth = Effect.fn("ColumnLiveViewEngine.topicStore.health")(
  function* (store: TopicStore, closed: boolean) {
    const activeViews = yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store));
    return yield* collectTopicStoreHealthView(topicStoreHealthState(store, activeViews), closed);
  },
);
