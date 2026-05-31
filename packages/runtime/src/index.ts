import type { ViewServerConfig } from "@view-server/config";
import { Effect } from "effect";
import type { HttpServerError } from "effect/unstable/http";
import {
  makeDefaultRuntimeDependencies,
  makeViewServerRuntimeWithDependencies,
  type ViewServerRuntime,
  type ViewServerRuntimeOptions,
  type ViewServerRuntimeTopicDefinitions,
} from "./internal";

export type { ViewServerRuntime, ViewServerRuntimeOptions };

export const makeViewServerRuntime: <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  options?: ViewServerRuntimeOptions,
) => Effect.Effect<ViewServerRuntime<Topics>, HttpServerError.ServeError> = Effect.fn(
  "ViewServerRuntime.make",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  options: ViewServerRuntimeOptions = {},
) {
  return yield* makeViewServerRuntimeWithDependencies(
    makeDefaultRuntimeDependencies<Topics>(),
    config,
    options,
  );
});

export const createViewServerRuntime = makeViewServerRuntime;
