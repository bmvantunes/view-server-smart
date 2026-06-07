import { Effect } from "effect";
import {
  acquireMaterializedQueryExecution,
  acquireRawQueryExecution,
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
import type { GroupedIncrementalAdmissionLimits } from "./grouped-incremental-admission";
import { prepareRawQuery, type CompiledRawQuery } from "./raw-query-compiler";
import type { QueryEvaluation } from "./query-result";
import {
  topicStoreRawQueryMetadata,
  topicStoreReadModel,
  type TopicStore,
} from "./topic-store-state";

type RowObject = object;

export const prepareTopicStoreRawQuery = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.raw.prepare",
)(function* <ResultRow extends RowObject>(store: TopicStore, query: unknown) {
  return yield* prepareRawQuery<object, ResultRow>(
    store.topic,
    topicStoreRawQueryMetadata(store),
    query,
  );
});

export const prepareTopicStoreGroupedQuery = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.grouped.prepare",
)(function* <ResultRow extends RowObject>(store: TopicStore, query: unknown) {
  return yield* prepareGroupedQuery<object, ResultRow>(
    store.topic,
    topicStoreRawQueryMetadata(store),
    query,
  );
});

export const evaluateTopicStoreRawQuery = <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledRawQuery<object, ResultRow>,
): QueryEvaluation<ResultRow> => evaluateRawQuery(topicStoreReadModel(store), compiled);

export const evaluateTopicStoreGroupedQuery = <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledGroupedQuery<object, ResultRow>,
): QueryEvaluation<ResultRow> => evaluateCompiledGroupedQuery(topicStoreReadModel(store), compiled);

export const acquireTopicStoreRawQueryExecution = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.raw.acquire",
)(function* <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledRawQuery<object, ResultRow>,
) {
  return yield* acquireRawQueryExecution(topicStoreReadModel(store), compiled);
});

export const releaseTopicStoreRawQueryExecution = <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledRawQuery<object, ResultRow>,
): Effect.Effect<void> => releaseRawQueryExecution(topicStoreReadModel(store), compiled);

export const acquireTopicStoreMaterializedQueryExecution = Effect.fn(
  "ColumnLiveViewEngine.topicStore.query.materialized.acquire",
)(function* <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledGroupedQuery<object, ResultRow>,
  groupedIncrementalAdmissionLimits: GroupedIncrementalAdmissionLimits,
) {
  const readModel = topicStoreReadModel(store);
  return yield* acquireMaterializedQueryExecution(
    readModel,
    compiled.cacheKey,
    (releaseRetainedChanges) =>
      makeIncrementalGroupedQueryExecution(
        readModel,
        compiled,
        releaseRetainedChanges,
        groupedIncrementalAdmissionLimits,
      ),
  );
});

export const releaseTopicStoreMaterializedQueryExecution = <ResultRow extends RowObject>(
  store: TopicStore,
  compiled: CompiledGroupedQuery<object, ResultRow>,
): Effect.Effect<void> =>
  releaseMaterializedQueryExecution(topicStoreReadModel(store), compiled.cacheKey);
