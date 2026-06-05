import type { ViewServerConfig } from "@view-server/config";
import {
  makeInMemoryViewServer,
  type ViewServerInMemoryInstance,
  type ViewServerInMemoryOptions,
} from "@view-server/in-memory";
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
  readonly makeInMemory: (
    config: ViewServerConfig<Topics>,
    options: ViewServerInMemoryOptions,
  ) => Effect.Effect<ViewServerInMemoryInstance<Topics>>;
  readonly makeServer: (
    config: ViewServerConfig<Topics>,
    input: ViewServerWebSocketServerInput<Topics>,
    options: ViewServerWebSocketServerOptions,
  ) => Effect.Effect<ViewServerWebSocketServer, HttpServerError.ServeError>;
};

export const makeDefaultRuntimeDependencies = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(): ViewServerRuntimeDependencies<Topics> => ({
  makeInMemory: makeInMemoryViewServer,
  makeServer: makeViewServerWebSocketServer,
});
