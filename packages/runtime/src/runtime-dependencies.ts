import type { ViewServerConfig } from "@view-server/config";
import {
  makeViewServerRuntimeCore,
  type ViewServerRuntimeCoreInstance,
  type ViewServerRuntimeCoreOptionsFor,
} from "@view-server/runtime-core";
import {
  makeViewServerWebSocketServer,
  type ViewServerWebSocketServer,
  type ViewServerWebSocketServerInput,
  type ViewServerWebSocketServerOptions,
} from "@view-server/server";
import type { Effect } from "effect";
import type { HttpServerError } from "effect/unstable/http";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export type ViewServerRuntimeDependencies<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly makeRuntimeCore: (
    config: ViewServerConfig<Topics>,
    options: ViewServerRuntimeCoreOptionsFor<Topics>,
  ) => Effect.Effect<ViewServerRuntimeCoreInstance<Topics>>;
  readonly makeServer: (
    config: ViewServerConfig<Topics>,
    input: ViewServerWebSocketServerInput<Topics>,
    options: ViewServerWebSocketServerOptions,
  ) => Effect.Effect<ViewServerWebSocketServer, HttpServerError.ServeError>;
};

export const makeDefaultRuntimeDependencies = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(): ViewServerRuntimeDependencies<Topics> => ({
  makeRuntimeCore: makeViewServerRuntimeCore,
  makeServer: makeViewServerWebSocketServer,
});
