import type { ViewServerLiveClient, ViewServerRuntimeLiveClient } from "@view-server/client";
import type { RuntimeRegions, ViewServerConfig, ViewServerHealth } from "@view-server/config";
import { ignoreLoggedTypedFailuresPreserveNonTypedFailures } from "@view-server/effect-utils";
import type { ViewServerRuntimeCoreOptionsFor } from "@view-server/runtime-core";
import { Config, Effect, Exit, Layer } from "effect";
import type { HttpServerError } from "effect/unstable/http";
import {
  makeDefaultRuntimeDependencies,
  type ViewServerRuntimeDependencies,
} from "./runtime-dependencies";
import type { ViewServerKafkaIngressError } from "./kafka-ingress";
import { resolveViewServerRuntimeOptions } from "./runtime-options";
import type {
  ViewServerRuntime,
  ViewServerRuntimeOptionsInput,
  ViewServerRuntimeOptions,
  ViewServerRuntimeTopicDefinitions,
} from "./runtime-types";
import { makeViewServerRuntimeTransportHealth } from "./transport-health";

export { makeDefaultRuntimeDependencies };
export type {
  ViewServerRuntime,
  ViewServerRuntimeDependencies,
  ViewServerRuntimeOptionsInput,
  ViewServerRuntimeOptions,
  ViewServerRuntimeTopicDefinitions,
};

const toPublicLiveClient = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  liveClient: ViewServerRuntimeLiveClient<Topics>,
  close: Effect.Effect<void>,
): ViewServerLiveClient<Topics> => ({
  close,
  health: liveClient.health,
  subscribe: liveClient.subscribe,
  subscribeHealth: liveClient.subscribeHealth,
  subscribeHealthSummary: liveClient.subscribeHealthSummary,
});

const ignoreRuntimeHealthRefreshFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring runtime health refresh failure.",
);

type RuntimeCoreOptionsBuilder<Topics extends ViewServerRuntimeTopicDefinitions> = {
  groupedIncrementalAdmissionLimits?: NonNullable<
    ViewServerRuntimeCoreOptionsFor<Topics>["groupedIncrementalAdmissionLimits"]
  >;
  subscriptionQueueCapacity?: NonNullable<
    ViewServerRuntimeCoreOptionsFor<Topics>["subscriptionQueueCapacity"]
  >;
  transportHealth: NonNullable<ViewServerRuntimeCoreOptionsFor<Topics>["transportHealth"]>;
  healthOverlay?: NonNullable<ViewServerRuntimeCoreOptionsFor<Topics>["healthOverlay"]>;
};

export const makeViewServerRuntimeWithDependencies: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Options extends ViewServerRuntimeOptions<Topics> = ViewServerRuntimeOptions<Topics>,
>(
  dependencies: ViewServerRuntimeDependencies<Topics>,
  config: ViewServerConfig<Topics>,
  options?: ViewServerRuntimeOptionsInput<Topics, Options>,
) => Effect.Effect<
  ViewServerRuntime<Topics>,
  HttpServerError.ServeError | Config.ConfigError | ViewServerKafkaIngressError
> = Effect.fn("ViewServerRuntime.makeWithDependencies")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Options extends ViewServerRuntimeOptions<Topics>,
>(
  dependencies: ViewServerRuntimeDependencies<Topics>,
  config: ViewServerConfig<Topics>,
  options?: ViewServerRuntimeOptionsInput<Topics, Options>,
) {
  const { runtimeCoreOptions, serverOptions, kafkaOptions } =
    options === undefined
      ? yield* resolveViewServerRuntimeOptions<Topics, RuntimeRegions>({})
      : yield* resolveViewServerRuntimeOptions(options);
  const transportHealth = makeViewServerRuntimeTransportHealth<Topics>();
  const kafkaHealth =
    kafkaOptions === undefined
      ? undefined
      : dependencies.makeKafkaHealthLedger(config, kafkaOptions);
  const runtimeCoreInput: RuntimeCoreOptionsBuilder<Topics> = {
    transportHealth: transportHealth.transportHealth,
  };
  if (runtimeCoreOptions.groupedIncrementalAdmissionLimits !== undefined) {
    runtimeCoreInput.groupedIncrementalAdmissionLimits =
      runtimeCoreOptions.groupedIncrementalAdmissionLimits;
  }
  if (runtimeCoreOptions.subscriptionQueueCapacity !== undefined) {
    runtimeCoreInput.subscriptionQueueCapacity = runtimeCoreOptions.subscriptionQueueCapacity;
  }
  if (kafkaHealth !== undefined) {
    runtimeCoreInput.healthOverlay = (
      health: ViewServerHealth<Topics>,
      nowMillis: number,
    ): ViewServerHealth<Topics> => kafkaHealth.healthOverlay(health, nowMillis);
  }
  const runtimeCore = yield* dependencies.makeRuntimeCore(config, runtimeCoreInput);
  const refreshTransportHealth = ignoreRuntimeHealthRefreshFailure(runtimeCore.refreshHealth);
  const server = yield* dependencies
    .makeServer(
      config,
      {
        liveClient: runtimeCore.liveClient,
        runtime: runtimeCore.client,
        transport: {
          clientOpened: transportHealth.clientOpened.pipe(Effect.andThen(refreshTransportHealth)),
          clientClosed: transportHealth.clientClosed.pipe(Effect.andThen(refreshTransportHealth)),
          streamOpened: transportHealth.streamOpened.pipe(Effect.andThen(refreshTransportHealth)),
          streamClosed: transportHealth.streamClosed.pipe(Effect.andThen(refreshTransportHealth)),
        },
      },
      serverOptions,
    )
    .pipe(Effect.onExit((exit) => (Exit.isFailure(exit) ? runtimeCore.close : Effect.void)));
  const kafkaIngress =
    kafkaOptions === undefined || kafkaHealth === undefined
      ? undefined
      : yield* dependencies
          .makeKafkaIngress(
            config,
            runtimeCore.client,
            runtimeCore.requestHealthRefresh,
            kafkaOptions,
            kafkaHealth,
          )
          .pipe(
            Effect.onExit((exit) =>
              Exit.isFailure(exit)
                ? server.close.pipe(Effect.ensuring(runtimeCore.close))
                : Effect.void,
            ),
          );
  const close = (kafkaIngress?.close ?? Effect.void).pipe(
    Effect.ensuring(server.close),
    Effect.ensuring(runtimeCore.close),
  );
  const publicLiveClient = toPublicLiveClient(runtimeCore.liveClient, close);
  return {
    url: server.url,
    healthUrl: server.healthUrl,
    client: runtimeCore.client,
    liveClient: publicLiveClient,
    health: runtimeCore.client.health,
    close,
  };
});

const logRuntimeStarted = Effect.fn("ViewServerRuntime.logStarted")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(runtime: ViewServerRuntime<Topics>) {
  yield* Effect.logInfo(`View Server WebSocket listening at ${runtime.url}`);
  yield* Effect.logInfo(`View Server health endpoint listening at ${runtime.healthUrl}`);
});

const makeViewServerRuntimeLaunchLayer = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Options extends ViewServerRuntimeOptions<Topics>,
>(
  dependencies: ViewServerRuntimeDependencies<Topics>,
  config: ViewServerConfig<Topics>,
  options?: ViewServerRuntimeOptionsInput<Topics, Options>,
) =>
  Layer.effectDiscard(
    Effect.acquireRelease(
      (options === undefined
        ? makeViewServerRuntimeWithDependencies(dependencies, config)
        : makeViewServerRuntimeWithDependencies(dependencies, config, options)
      ).pipe(Effect.tap(logRuntimeStarted)),
      (runtime) => runtime.close,
    ),
  );

export const runViewServerRuntimeWithDependencies: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Options extends ViewServerRuntimeOptions<Topics> = ViewServerRuntimeOptions<Topics>,
>(
  dependencies: ViewServerRuntimeDependencies<Topics>,
  config: ViewServerConfig<Topics>,
  options?: ViewServerRuntimeOptionsInput<Topics, Options>,
) => Effect.Effect<
  never,
  HttpServerError.ServeError | Config.ConfigError | ViewServerKafkaIngressError
> = Effect.fn("ViewServerRuntime.runWithDependencies")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Options extends ViewServerRuntimeOptions<Topics>,
>(
  dependencies: ViewServerRuntimeDependencies<Topics>,
  config: ViewServerConfig<Topics>,
  options?: ViewServerRuntimeOptionsInput<Topics, Options>,
) {
  return yield* makeViewServerRuntimeLaunchLayer(dependencies, config, options).pipe(Layer.launch);
});
