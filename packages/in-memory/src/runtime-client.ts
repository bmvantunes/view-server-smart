import type {
  ColumnLiveViewEngine,
  DecodableTopicDefinitions,
} from "@view-server/column-live-view-engine";
import type {
  ExactLiveQueryInput,
  GroupedQuery,
  LiveQueryRow,
  LiveQueryResult,
  RawQuery,
  TopicRow,
  ViewServerHealth,
  ViewServerInMemoryRuntime,
  ViewServerRuntimeError,
} from "@view-server/config";
import { Effect } from "effect";
import type { AtomRef } from "effect/unstable/reactivity";
import { makeHealthRefreshScheduler, readHealth, refreshHealth } from "./health";
import { engineErrorToRuntimeError } from "./runtime-error";

export const makeInMemoryRuntimeClient = Effect.fn("ViewServerInMemory.runtime.make")(<
  const Topics extends DecodableTopicDefinitions,
>(
  engine: ColumnLiveViewEngine<Topics>,
  health: AtomRef.AtomRef<ViewServerHealth<Topics>>,
): Effect.Effect<ViewServerInMemoryRuntime<Topics>> => {
  const requestHealthRefresh = makeHealthRefreshScheduler(refreshHealth(engine, health));
  const snapshot = <
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInput<TopicRow<Topics, Topic>, Query>,
  ): Effect.Effect<
    LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError
  > => engine.snapshot<Topic, Query>(topic, query).pipe(Effect.mapError(engineErrorToRuntimeError));
  return Effect.succeed<ViewServerInMemoryRuntime<Topics>>({
    publish: (topic, row) =>
      engine.publish(topic, row).pipe(
        Effect.tap(() => requestHealthRefresh),
        Effect.mapError(engineErrorToRuntimeError),
      ),
    publishMany: (topic, rows) =>
      engine.publishMany(topic, rows).pipe(
        Effect.tap(() => requestHealthRefresh),
        Effect.mapError(engineErrorToRuntimeError),
      ),
    patch: (topic, key, patch) =>
      engine.patch(topic, key, patch).pipe(
        Effect.tap(() => requestHealthRefresh),
        Effect.mapError(engineErrorToRuntimeError),
      ),
    delete: (topic, key) =>
      engine.delete(topic, key).pipe(
        Effect.tap(() => requestHealthRefresh),
        Effect.mapError(engineErrorToRuntimeError),
      ),
    snapshot,
    health: () => readHealth(engine, health).pipe(Effect.mapError(engineErrorToRuntimeError)),
    reset: () =>
      engine.reset().pipe(
        Effect.tap(() => requestHealthRefresh),
        Effect.mapError(engineErrorToRuntimeError),
      ),
  });
});
