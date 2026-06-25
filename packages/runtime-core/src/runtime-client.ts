import type {
  ColumnLiveViewEngine,
  DecodableTopicDefinitions,
} from "@view-server/column-live-view-engine";
import type {
  ExactLiveQueryInputForTopic,
  GroupedQuery,
  LiveQueryRow,
  LiveQueryResult,
  RawQuery,
  TopicRow,
  ViewServerConfig,
  ViewServerHealth,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
} from "@view-server/config";
import { validateLiveQuerySourceRoute } from "@view-server/config";
import { Effect } from "effect";
import type { AtomRef } from "effect/unstable/reactivity";
import {
  makeHealthRefreshScheduler,
  makeCoalescedHealthReader,
  readHealth,
  type RuntimeCoreHealthOverlay,
  type RuntimeCoreTransportHealth,
} from "./health";
import type * as Duration from "effect/Duration";
import { engineErrorToRuntimeError, invalidRuntimeQueryError } from "./runtime-error";

export type RuntimeCoreClientInstance<Topics extends DecodableTopicDefinitions> = {
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly close: Effect.Effect<void>;
  readonly requestHealthRefresh: Effect.Effect<void>;
  readonly refreshHealth: Effect.Effect<ViewServerHealth<Topics>, ViewServerRuntimeError>;
};

export const makeRuntimeCoreClient = Effect.fn("ViewServerRuntimeCore.client.make")(
  <const Topics extends DecodableTopicDefinitions>(
    config: ViewServerConfig<Topics>,
    engine: ColumnLiveViewEngine<Topics>,
    health: AtomRef.AtomRef<ViewServerHealth<Topics>>,
    transportHealth: RuntimeCoreTransportHealth<Topics>,
    healthOverlay?: RuntimeCoreHealthOverlay<Topics>,
    healthRefreshCadence?: Duration.Input,
  ): Effect.Effect<RuntimeCoreClientInstance<Topics>> =>
    Effect.gen(function* () {
      let healthReadEpoch = 0;
      let healthInstallEpoch = 0;
      const bumpHealthReadEpoch = Effect.sync(() => {
        healthReadEpoch += 1;
      });
      const readRuntimeHealth = (epoch: number, installMode: "strict" | "scheduled") => {
        const installEpoch = healthInstallEpoch;
        return readHealth(
          engine,
          health,
          transportHealth,
          healthOverlay,
          () =>
            healthInstallEpoch === installEpoch &&
            (installMode === "scheduled" || healthReadEpoch === epoch),
          () => {
            healthInstallEpoch += 1;
          },
        );
      };
      const healthReader = makeCoalescedHealthReader(
        (epoch) => readRuntimeHealth(epoch, "strict"),
        () => healthReadEpoch,
      );
      const scheduledHealthReader = makeCoalescedHealthReader(
        (epoch) => readRuntimeHealth(epoch, "scheduled"),
        () => healthReadEpoch,
      );
      const scheduledHealthRefresh = Effect.fn(
        "ViewServerRuntimeCore.client.healthRefresh.scheduled",
      )(function* () {
        yield* scheduledHealthReader();
      });
      const healthRefreshScheduler = yield* makeHealthRefreshScheduler(
        scheduledHealthRefresh(),
        healthRefreshCadence,
      );
      const requestHealthRefresh = Effect.fn("ViewServerRuntimeCore.client.healthRefresh.request")(
        function* () {
          yield* Effect.uninterruptible(
            bumpHealthReadEpoch.pipe(Effect.andThen(healthRefreshScheduler.request)),
          );
        },
      );
      const refreshHealthNow = Effect.fn("ViewServerRuntimeCore.client.healthRefresh.now")(
        function* () {
          return yield* Effect.uninterruptible(
            bumpHealthReadEpoch.pipe(Effect.andThen(healthReader())),
          );
        },
      );
      const snapshot = <
        Topic extends Extract<keyof Topics, string>,
        const Query extends
          | RawQuery<TopicRow<Topics, Topic>>
          | GroupedQuery<TopicRow<Topics, Topic>>,
      >(
        topic: Topic,
        query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
      ): Effect.Effect<
        LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
        ViewServerRuntimeError
      > =>
        Effect.suspend(() => {
          const routeError = validateLiveQuerySourceRoute(config.topics, topic, query);
          if (routeError !== undefined) {
            return Effect.fail(invalidRuntimeQueryError(topic, routeError));
          }
          return engine
            .snapshot<Topic, Query>(topic, query)
            .pipe(Effect.mapError(engineErrorToRuntimeError));
        });
      return {
        client: {
          publish: (topic, row) =>
            Effect.uninterruptible(
              engine.publish(topic, row).pipe(Effect.tap(() => requestHealthRefresh())),
            ).pipe(Effect.mapError(engineErrorToRuntimeError)),
          publishMany: (topic, rows) =>
            Effect.uninterruptible(
              engine.publishMany(topic, rows).pipe(Effect.tap(() => requestHealthRefresh())),
            ).pipe(Effect.mapError(engineErrorToRuntimeError)),
          patch: (topic, key, patch) =>
            Effect.uninterruptible(
              engine.patch(topic, key, patch).pipe(Effect.tap(() => requestHealthRefresh())),
            ).pipe(Effect.mapError(engineErrorToRuntimeError)),
          delete: (topic, key) =>
            Effect.uninterruptible(
              engine.delete(topic, key).pipe(Effect.tap(() => requestHealthRefresh())),
            ).pipe(Effect.mapError(engineErrorToRuntimeError)),
          snapshot,
          health: () => healthReader(),
          reset: () =>
            Effect.uninterruptible(
              engine.reset().pipe(Effect.tap(() => requestHealthRefresh())),
            ).pipe(Effect.mapError(engineErrorToRuntimeError)),
        },
        close: healthRefreshScheduler.close,
        requestHealthRefresh: requestHealthRefresh(),
        refreshHealth: refreshHealthNow(),
      } satisfies RuntimeCoreClientInstance<Topics>;
    }),
);
