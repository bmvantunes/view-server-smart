import { Effect } from "effect";
import { UnsupportedQueryError } from "./engine-errors";
import { makeLiveSubscription } from "./live-subscription";
import { acquireRawQueryExecution, releaseRawQueryExecution } from "./active-query";
import {
  evaluateCompiledRawQuery,
  prepareRawQuery,
  type CompiledRawQuery,
} from "./raw-query-compiler";
import { liveQueryResult, type QueryEvaluation } from "./query-result";
import { topicStoreRawQueryMetadata, topicStoreReadModel, type TopicStore } from "./topic-store";

type RowObject = object;

export type ExecutableQuery<ResultRow extends RowObject> = {
  readonly kind: "raw";
  readonly compiled: CompiledRawQuery<object, ResultRow>;
};

export const isGroupedQuery = (query: unknown): boolean =>
  typeof query === "object" &&
  query !== null &&
  !Array.isArray(query) &&
  ("groupBy" in query || "aggregates" in query);

const unsupportedGroupedQuery = (topic: string) =>
  new UnsupportedQueryError({
    topic,
    message: "Grouped aggregate queries are not implemented in this slice.",
  });

export const prepareExecutableQuery = Effect.fn("ColumnLiveViewEngine.queryExecution.prepare")(
  function* <ResultRow extends RowObject>(topic: string, store: TopicStore, query: unknown) {
    if (isGroupedQuery(query)) {
      return yield* unsupportedGroupedQuery(topic);
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
  evaluateCompiledRawQuery(topicStoreReadModel(store), executable.compiled);

export const snapshotExecutableQuery = Effect.fn("ColumnLiveViewEngine.queryExecution.snapshot")(
  function* <ResultRow extends RowObject>(topic: string, store: TopicStore, query: unknown) {
    const executable = yield* prepareExecutableQuery<ResultRow>(topic, store, query);
    return liveQueryResult(evaluateExecutableQuery(store, executable));
  },
);

export const subscribeExecutableQuery = Effect.fn("ColumnLiveViewEngine.queryExecution.subscribe")(
  function* <ResultRow extends RowObject>(
    topic: string,
    store: TopicStore,
    query: unknown,
    input: {
      readonly queryId: string;
      readonly queueCapacity: number;
    },
  ) {
    const executable = yield* prepareExecutableQuery<ResultRow>(topic, store, query);
    const storeReadModel = topicStoreReadModel(store);
    const execution = yield* acquireRawQueryExecution(storeReadModel, executable.compiled);
    return yield* makeLiveSubscription({
      store,
      queryId: input.queryId,
      execution,
      queueCapacity: input.queueCapacity,
      release: releaseRawQueryExecution(storeReadModel, executable.compiled),
    });
  },
);
