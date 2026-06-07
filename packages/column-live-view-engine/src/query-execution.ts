import { Effect } from "effect";
import { makeLiveSubscription } from "./live-subscription";
import type { CompiledGroupedQuery } from "./grouped-query-compiler";
import type { GroupedIncrementalAdmissionLimits } from "./grouped-incremental-admission";
import type { CompiledRawQuery } from "./raw-query-compiler";
import { liveQueryResult, type QueryEvaluation } from "./query-result";
import {
  acquireTopicStoreMaterializedQueryExecution,
  acquireTopicStoreRawQueryExecution,
  evaluateTopicStoreGroupedQuery,
  evaluateTopicStoreRawQuery,
  prepareTopicStoreGroupedQuery,
  prepareTopicStoreRawQuery,
  releaseTopicStoreMaterializedQueryExecution,
  releaseTopicStoreRawQueryExecution,
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
  function* <ResultRow extends RowObject>(store: TopicStore, query: unknown) {
    if (isGroupedQuery(query)) {
      const compiled = yield* prepareTopicStoreGroupedQuery<ResultRow>(store, query);
      return {
        kind: "grouped",
        compiled,
      } satisfies ExecutableQuery<ResultRow>;
    }
    const compiled = yield* prepareTopicStoreRawQuery<ResultRow>(store, query);
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
    ? evaluateTopicStoreRawQuery(store, executable.compiled)
    : evaluateTopicStoreGroupedQuery(store, executable.compiled);

export const snapshotExecutableQuery = Effect.fn("ColumnLiveViewEngine.queryExecution.snapshot")(
  function* <ResultRow extends RowObject>(store: TopicStore, query: unknown) {
    const executable = yield* prepareExecutableQuery<ResultRow>(store, query);
    return liveQueryResult(evaluateExecutableQuery(store, executable));
  },
);

export const subscribeExecutableQuery = Effect.fn("ColumnLiveViewEngine.queryExecution.subscribe")(
  function* <ResultRow extends RowObject>(
    query: unknown,
    input: {
      readonly groupedIncrementalAdmissionLimits: GroupedIncrementalAdmissionLimits;
      readonly permit: TopicStoreSubscriptionPermit;
      readonly queryId: string;
      readonly queueCapacity: number;
    },
  ) {
    const { store } = input.permit;
    const executable = yield* prepareExecutableQuery<ResultRow>(store, query);
    if (executable.kind === "raw") {
      const execution = yield* acquireTopicStoreRawQueryExecution(store, executable.compiled);
      return yield* makeLiveSubscription({
        permit: input.permit,
        queryId: input.queryId,
        execution,
        queueCapacity: input.queueCapacity,
        release: releaseTopicStoreRawQueryExecution(store, executable.compiled),
      });
    }

    const execution = yield* acquireTopicStoreMaterializedQueryExecution(
      store,
      executable.compiled,
      input.groupedIncrementalAdmissionLimits,
    );
    return yield* makeLiveSubscription({
      permit: input.permit,
      queryId: input.queryId,
      execution,
      queueCapacity: input.queueCapacity,
      release: releaseTopicStoreMaterializedQueryExecution(store, executable.compiled),
    });
  },
);
