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

export { makeDefaultRuntimeDependencies };
export type {
  ViewServerRuntime,
  ViewServerRuntimeDependencies,
  ViewServerRuntimeOptions,
  ViewServerRuntimeTopicDefinitions,
};

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
  const { inMemoryOptions, serverOptions } = resolveViewServerRuntimeOptions(options);
  const inMemory = yield* dependencies.makeInMemory(config, inMemoryOptions);
  const server = yield* dependencies
    .makeServer(
      config,
      {
        liveClient: inMemory.liveClient,
        runtime: inMemory.client,
      },
      serverOptions,
    )
    .pipe(Effect.onExit((exit) => (Exit.isFailure(exit) ? inMemory.close : Effect.void)));
  return {
    url: server.url,
    healthUrl: server.healthUrl,
    client: inMemory.client,
    liveClient: inMemory.liveClient,
    health: inMemory.client.health,
    close: server.close.pipe(Effect.ensuring(inMemory.close)),
  };
});
