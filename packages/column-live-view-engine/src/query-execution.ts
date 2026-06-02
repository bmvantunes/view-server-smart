import { Effect } from "effect";
import { makeLiveSubscription } from "./live-subscription";
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
import { prepareRawQuery, type CompiledRawQuery } from "./raw-query-compiler";
import { liveQueryResult, type QueryEvaluation } from "./query-result";
import {
  topicStoreRawQueryMetadata,
  topicStoreReadModel,
  type TopicStore,
  type TopicStoreSubscriptionPermit,
} from "./topic-store";

type RowObject = object;

export type ExecutableQuery<ResultRow extends RowObject> =
  | {
      readonly kind: "raw";
      readonly compiled: CompiledRawQuery<object, ResultRow>;
    }
  | {
      readonly kind: "grouped";
      readonly compiled: CompiledGroupedQuery<object, ResultRow>;
    };

export const isGroupedQuery = (query: unknown): boolean =>
  typeof query === "object" &&
  query !== null &&
  !Array.isArray(query) &&
  ("groupBy" in query || "aggregates" in query);

export const prepareExecutableQuery = Effect.fn("ColumnLiveViewEngine.queryExecution.prepare")(
  function* <ResultRow extends RowObject>(topic: string, store: TopicStore, query: unknown) {
    if (isGroupedQuery(query)) {
      const compiled = yield* prepareGroupedQuery<object, ResultRow>(
        topic,
        topicStoreRawQueryMetadata(store),
        query,
      );
      return {
        kind: "grouped",
        compiled,
      } satisfies ExecutableQuery<ResultRow>;
    }
    const compiled = yield* prepareRawQuery<object, ResultRow>(
      topic,
      topicStoreRawQueryMetadata(store),
      query,
    );
    return {
      kind: "raw",
      compiled,
    } satisfies ExecutableQuery<ResultRow>;
  },
);

export const evaluateExecutableQuery = <ResultRow extends RowObject>(
  store: TopicStore,
  executable: ExecutableQuery<ResultRow>,
): QueryEvaluation<ResultRow> =>
  executable.kind === "raw"
    ? evaluateRawQuery(topicStoreReadModel(store), executable.compiled)
    : evaluateCompiledGroupedQuery(topicStoreReadModel(store), executable.compiled);

export const snapshotExecutableQuery = Effect.fn("ColumnLiveViewEngine.queryExecution.snapshot")(
  function* <ResultRow extends RowObject>(topic: string, store: TopicStore, query: unknown) {
    const executable = yield* prepareExecutableQuery<ResultRow>(topic, store, query);
    return liveQueryResult(evaluateExecutableQuery(store, executable));
  },
);

export const subscribeExecutableQuery = Effect.fn("ColumnLiveViewEngine.queryExecution.subscribe")(
  function* <ResultRow extends RowObject>(
    query: unknown,
    input: {
      readonly permit: TopicStoreSubscriptionPermit;
      readonly queryId: string;
      readonly queueCapacity: number;
    },
  ) {
    const { store } = input.permit;
    const { topic } = store;
    const executable = yield* prepareExecutableQuery<ResultRow>(topic, store, query);
    const storeReadModel = topicStoreReadModel(store);
    if (executable.kind === "raw") {
      const execution = yield* acquireRawQueryExecution(storeReadModel, executable.compiled);
      return yield* makeLiveSubscription({
        permit: input.permit,
        queryId: input.queryId,
        execution,
        queueCapacity: input.queueCapacity,
        release: releaseRawQueryExecution(storeReadModel, executable.compiled),
      });
    }

    const execution = yield* acquireMaterializedQueryExecution(
      storeReadModel,
      executable.compiled.cacheKey,
      () => evaluateCompiledGroupedQuery(storeReadModel, executable.compiled),
    );
    return yield* makeLiveSubscription({
      permit: input.permit,
      queryId: input.queryId,
      execution,
      queueCapacity: input.queueCapacity,
      release: releaseMaterializedQueryExecution(storeReadModel, executable.compiled.cacheKey),
    });
  },
);
