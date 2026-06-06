import type { ViewServerLiveClient, ViewServerRuntimeLiveClient } from "@view-server/client";
import type { ViewServerConfig } from "@view-server/config";
import { Effect, Exit, Layer } from "effect";
import type { HttpServerError } from "effect/unstable/http";
import {
  makeDefaultRuntimeDependencies,
  type ViewServerRuntimeDependencies,
} from "./runtime-dependencies";
import { resolveViewServerRuntimeOptions } from "./runtime-options";
import type {
  ViewServerRuntime,
  ViewServerRuntimeOptions,
  ViewServerRuntimeTopicDefinitions,
} from "./runtime-types";
import { makeViewServerRuntimeTransportHealth } from "./transport-health";

export { makeDefaultRuntimeDependencies };
export type {
  ViewServerRuntime,
  ViewServerRuntimeDependencies,
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

export const makeViewServerRuntimeWithDependencies: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  dependencies: ViewServerRuntimeDependencies<Topics>,
  config: ViewServerConfig<Topics>,
  options?: ViewServerRuntimeOptions,
) => Effect.Effect<ViewServerRuntime<Topics>, HttpServerError.ServeError> = Effect.fn(
  "ViewServerRuntime.makeWithDependencies",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  dependencies: ViewServerRuntimeDependencies<Topics>,
  config: ViewServerConfig<Topics>,
  options: ViewServerRuntimeOptions = {},
) {
  const { runtimeCoreOptions, serverOptions } = resolveViewServerRuntimeOptions(options);
  const transportHealth = makeViewServerRuntimeTransportHealth<Topics>();
  const runtimeCore = yield* dependencies.makeRuntimeCore(config, {
    ...runtimeCoreOptions,
    transportHealth: transportHealth.transportHealth,
  });
  const refreshTransportHealth = runtimeCore.client.health().pipe(Effect.ignore);
  const server = yield* dependencies
    .makeServer(
      config,
      {
        liveClient: runtimeCore.liveClient,
        runtime: runtimeCore.client,
        transport: {
          streamOpened: transportHealth.streamOpened.pipe(Effect.andThen(refreshTransportHealth)),
          streamClosed: transportHealth.streamClosed.pipe(Effect.andThen(refreshTransportHealth)),
        },
      },
      serverOptions,
    )
    .pipe(Effect.onExit((exit) => (Exit.isFailure(exit) ? runtimeCore.close : Effect.void)));
  const close = server.close.pipe(Effect.ensuring(runtimeCore.close));
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

const makeViewServerRuntimeLaunchLayer = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  dependencies: ViewServerRuntimeDependencies<Topics>,
  config: ViewServerConfig<Topics>,
  options: ViewServerRuntimeOptions,
) =>
  Layer.effectDiscard(
    Effect.acquireRelease(
      makeViewServerRuntimeWithDependencies(dependencies, config, options).pipe(
        Effect.tap(logRuntimeStarted),
      ),
      (runtime) => runtime.close,
    ),
  );

export const runViewServerRuntimeWithDependencies: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  dependencies: ViewServerRuntimeDependencies<Topics>,
  config: ViewServerConfig<Topics>,
  options?: ViewServerRuntimeOptions,
) => Effect.Effect<never, HttpServerError.ServeError> = Effect.fn(
  "ViewServerRuntime.runWithDependencies",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  dependencies: ViewServerRuntimeDependencies<Topics>,
  config: ViewServerConfig<Topics>,
  options: ViewServerRuntimeOptions = {},
) {
  return yield* makeViewServerRuntimeLaunchLayer(dependencies, config, options).pipe(Layer.launch);
});
