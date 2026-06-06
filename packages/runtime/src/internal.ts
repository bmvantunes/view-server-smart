import type { ViewServerLiveClient, ViewServerRuntimeLiveClient } from "@view-server/client";
import type { ViewServerConfig } from "@view-server/config";
import { Effect, Exit } from "effect";
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
