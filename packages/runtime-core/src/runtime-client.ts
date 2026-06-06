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
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
} from "@view-server/config";
import { Effect } from "effect";
import type { AtomRef } from "effect/unstable/reactivity";
import {
  makeHealthRefreshScheduler,
  readHealth,
  refreshHealth,
  type RuntimeCoreTransportHealth,
} from "./health";
import type * as Duration from "effect/Duration";
import { engineErrorToRuntimeError } from "./runtime-error";

export type RuntimeCoreClientInstance<Topics extends DecodableTopicDefinitions> = {
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly close: Effect.Effect<void>;
};

export const makeRuntimeCoreClient = Effect.fn("ViewServerRuntimeCore.client.make")(<
  const Topics extends DecodableTopicDefinitions,
>(
  engine: ColumnLiveViewEngine<Topics>,
  health: AtomRef.AtomRef<ViewServerHealth<Topics>>,
  transportHealth: RuntimeCoreTransportHealth<Topics>,
  healthRefreshCadence?: Duration.Input,
): Effect.Effect<RuntimeCoreClientInstance<Topics>> => {
  const healthRefreshScheduler = makeHealthRefreshScheduler(
    refreshHealth(engine, health, transportHealth),
    healthRefreshCadence,
  );
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
  return Effect.succeed<RuntimeCoreClientInstance<Topics>>({
    client: {
      publish: (topic, row) =>
        engine.publish(topic, row).pipe(
          Effect.tap(() => healthRefreshScheduler.request),
          Effect.mapError(engineErrorToRuntimeError),
        ),
      publishMany: (topic, rows) =>
        engine.publishMany(topic, rows).pipe(
          Effect.tap(() => healthRefreshScheduler.request),
          Effect.mapError(engineErrorToRuntimeError),
        ),
      patch: (topic, key, patch) =>
        engine.patch(topic, key, patch).pipe(
          Effect.tap(() => healthRefreshScheduler.request),
          Effect.mapError(engineErrorToRuntimeError),
        ),
      delete: (topic, key) =>
        engine.delete(topic, key).pipe(
          Effect.tap(() => healthRefreshScheduler.request),
          Effect.mapError(engineErrorToRuntimeError),
        ),
      snapshot,
      health: () =>
        readHealth(engine, health, transportHealth).pipe(
          Effect.mapError(engineErrorToRuntimeError),
        ),
      reset: () =>
        engine.reset().pipe(
          Effect.tap(() => healthRefreshScheduler.request),
          Effect.mapError(engineErrorToRuntimeError),
        ),
    },
    close: healthRefreshScheduler.close,
  });
});
