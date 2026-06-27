import type { ViewServerLiveClient, ViewServerRuntimeLiveClient } from "@view-server/client";
import type {
  GrpcRuntimeClients,
  RuntimeRegions,
  ViewServerConfig,
  ViewServerHealth,
  ViewServerRuntimeError,
} from "@view-server/config";
import { ignoreLoggedTypedFailuresPreserveNonTypedFailures } from "@view-server/effect-utils";
import type { ViewServerRuntimeCoreOptionsFor } from "@view-server/runtime-core";
import { Config, Effect, Exit, Layer } from "effect";
import type { HttpServerError } from "effect/unstable/http";
import {
  makeDefaultRuntimeDependencies,
  type ViewServerRuntimeDependencies,
} from "./runtime-dependencies";
import type { ViewServerKafkaIngressError } from "./kafka-ingress";
import type { ViewServerGrpcIngressError } from "./grpc-ingress";
import { makeViewServerGrpcLeaseManager } from "./grpc-lease-manager";
import {
  resolveViewServerRuntimeOptions,
  validateGrpcSourceFeeds,
  type ResolvedViewServerRuntimeOptions,
} from "./runtime-options";
import type {
  ViewServerRuntime,
  ViewServerRuntimeOptionsInput,
  ViewServerRuntimeOptions,
  ViewServerGrpcRuntimeOptions,
  ViewServerRuntimeTopicDefinitions,
} from "./runtime-types";
import { makeViewServerRuntimeTransportHealth } from "./transport-health";

export { makeDefaultRuntimeDependencies };
export type {
  ViewServerRuntime,
  ViewServerRuntimeDependencies,
  ViewServerRuntimeOptionsInput,
  ViewServerRuntimeOptions,
  ViewServerGrpcRuntimeOptions,
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

type ViewServerRuntimeFactoryError =
  | HttpServerError.ServeError
  | Config.ConfigError
  | ViewServerRuntimeError
  | ViewServerKafkaIngressError
  | ViewServerGrpcIngressError;

type MakeViewServerRuntimeWithDependencies = {
  <const Topics extends ViewServerRuntimeTopicDefinitions>(
    dependencies: ViewServerRuntimeDependencies<Topics>,
    config: ViewServerConfig<Topics>,
  ): Effect.Effect<ViewServerRuntime<Topics>, ViewServerRuntimeFactoryError>;
  <const Topics extends ViewServerRuntimeTopicDefinitions, const Options extends object>(
    dependencies: ViewServerRuntimeDependencies<Topics>,
    config: ViewServerConfig<Topics>,
    options: Options & ViewServerRuntimeOptionsInput<Topics, Options>,
  ): Effect.Effect<ViewServerRuntime<Topics>, ViewServerRuntimeFactoryError>;
};

export const makeViewServerRuntimeWithDependencies: MakeViewServerRuntimeWithDependencies =
  Effect.fn("ViewServerRuntime.makeWithDependencies")(function* <
    const Topics extends ViewServerRuntimeTopicDefinitions,
    const Options extends object,
  >(
    dependencies: ViewServerRuntimeDependencies<Topics>,
    config: ViewServerConfig<Topics>,
    options?: ViewServerRuntimeOptionsInput<Topics, Options>,
  ) {
    if (options === undefined) {
      const resolvedOptions = yield* resolveViewServerRuntimeOptions<
        Topics,
        RuntimeRegions,
        GrpcRuntimeClients
      >({});
      return yield* makeViewServerRuntimeFromResolvedOptions(dependencies, config, resolvedOptions);
    }
    const resolvedOptions = yield* resolveViewServerRuntimeOptions(options);
    return yield* makeViewServerRuntimeFromResolvedOptions(dependencies, config, resolvedOptions);
  });

const makeViewServerRuntimeFromResolvedOptions = Effect.fn(
  "ViewServerRuntime.makeFromResolvedOptions",
)(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
>(
  dependencies: ViewServerRuntimeDependencies<Topics>,
  config: ViewServerConfig<Topics>,
  resolvedOptions: ResolvedViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
) {
  const kafkaOptions = resolvedOptions.kafkaOptions;
  const grpcOptions = resolvedOptions.grpcOptions;
  yield* validateGrpcSourceFeeds(config, grpcOptions);
  const transportHealth = makeViewServerRuntimeTransportHealth<Topics>();
  const kafkaHealth =
    kafkaOptions === undefined
      ? undefined
      : dependencies.makeKafkaHealthLedger(config, kafkaOptions);
  const grpcHealth =
    grpcOptions === undefined ? undefined : dependencies.makeGrpcHealthLedger(config, grpcOptions);
  const runtimeCoreInput: RuntimeCoreOptionsBuilder<Topics> = {
    transportHealth: transportHealth.transportHealth,
  };
  if (resolvedOptions.runtimeCoreOptions.groupedIncrementalAdmissionLimits !== undefined) {
    runtimeCoreInput.groupedIncrementalAdmissionLimits =
      resolvedOptions.runtimeCoreOptions.groupedIncrementalAdmissionLimits;
  }
  if (resolvedOptions.runtimeCoreOptions.subscriptionQueueCapacity !== undefined) {
    runtimeCoreInput.subscriptionQueueCapacity =
      resolvedOptions.runtimeCoreOptions.subscriptionQueueCapacity;
  }
  if (kafkaHealth !== undefined || grpcHealth !== undefined) {
    runtimeCoreInput.healthOverlay = (
      health: ViewServerHealth<Topics>,
      nowMillis: number,
    ): ViewServerHealth<Topics> => {
      const kafkaOverlayed =
        kafkaHealth === undefined ? health : kafkaHealth.healthOverlay(health, nowMillis);
      return grpcHealth === undefined
        ? kafkaOverlayed
        : grpcHealth.healthOverlay(kafkaOverlayed, nowMillis);
    };
  }
  const runtimeCore = yield* dependencies.makeRuntimeCore(config, runtimeCoreInput);
  const refreshTransportHealth = ignoreRuntimeHealthRefreshFailure(runtimeCore.refreshHealth);
  const grpcLeaseManager =
    grpcOptions === undefined || grpcHealth === undefined
      ? undefined
      : yield* makeViewServerGrpcLeaseManager(
          config,
          runtimeCore.internalClient,
          runtimeCore.liveClient,
          runtimeCore.internalLiveClient,
          runtimeCore.requestHealthRefresh,
          grpcOptions,
          grpcHealth,
        );
  const runtimeLiveClient = grpcLeaseManager?.liveClient ?? runtimeCore.liveClient;
  const runtimeClient = grpcLeaseManager?.client ?? runtimeCore.client;
  const closeGrpcLeaseManager =
    grpcLeaseManager === undefined ? Effect.void : grpcLeaseManager.close;
  const server = yield* dependencies
    .makeServer(
      config,
      {
        liveClient: runtimeLiveClient,
        runtime: runtimeClient,
        transport: {
          clientOpened: transportHealth.clientOpened.pipe(Effect.andThen(refreshTransportHealth)),
          clientClosed: transportHealth.clientClosed.pipe(Effect.andThen(refreshTransportHealth)),
          streamOpened: transportHealth.streamOpened.pipe(Effect.andThen(refreshTransportHealth)),
          streamClosed: transportHealth.streamClosed.pipe(Effect.andThen(refreshTransportHealth)),
        },
      },
      resolvedOptions.serverOptions,
    )
    .pipe(
      Effect.onExit((exit) =>
        Exit.isFailure(exit)
          ? closeGrpcLeaseManager.pipe(Effect.ensuring(runtimeCore.close))
          : Effect.void,
      ),
    );
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
                ? server.close.pipe(
                    Effect.ensuring(closeGrpcLeaseManager),
                    Effect.ensuring(runtimeCore.close),
                  )
                : Effect.void,
            ),
          );
  const grpcIngress =
    grpcOptions === undefined || grpcHealth === undefined
      ? undefined
      : yield* dependencies
          .makeGrpcIngress(
            config,
            runtimeCore.client,
            runtimeCore.requestHealthRefresh,
            grpcOptions,
            grpcHealth,
          )
          .pipe(
            Effect.onExit((exit) =>
              Exit.isFailure(exit)
                ? (kafkaIngress?.close ?? Effect.void).pipe(
                    Effect.ensuring(closeGrpcLeaseManager),
                    Effect.ensuring(server.close),
                    Effect.ensuring(runtimeCore.close),
                  )
                : Effect.void,
            ),
          );
  const close = (grpcIngress?.close ?? Effect.void).pipe(
    Effect.ensuring(closeGrpcLeaseManager),
    Effect.ensuring(kafkaIngress?.close ?? Effect.void),
    Effect.ensuring(server.close),
    Effect.ensuring(runtimeCore.close),
  );
  const publicLiveClient = toPublicLiveClient(runtimeLiveClient, close);
  return {
    url: server.url,
    healthUrl: server.healthUrl,
    client: runtimeClient,
    liveClient: publicLiveClient,
    health: runtimeClient.health,
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
  const Options extends object,
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
  const Options extends object = ViewServerRuntimeOptions<Topics>,
>(
  dependencies: ViewServerRuntimeDependencies<Topics>,
  config: ViewServerConfig<Topics>,
  options?: ViewServerRuntimeOptionsInput<Topics, Options>,
) => Effect.Effect<never, ViewServerRuntimeFactoryError> = Effect.fn(
  "ViewServerRuntime.runWithDependencies",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions, const Options extends object>(
  dependencies: ViewServerRuntimeDependencies<Topics>,
  config: ViewServerConfig<Topics>,
  options?: ViewServerRuntimeOptionsInput<Topics, Options>,
) {
  return yield* makeViewServerRuntimeLaunchLayer(dependencies, config, options).pipe(Layer.launch);
});
