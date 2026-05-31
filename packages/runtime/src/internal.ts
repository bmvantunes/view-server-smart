import type { ViewServerLiveClient } from "@view-server/client";
import type {
  RowSchema,
  TopicDefinitions,
  ViewServerConfig,
  ViewServerHealth,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
} from "@view-server/config";
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
import { Effect, Exit, type Schema } from "effect";
import type { HttpServerError } from "effect/unstable/http";

export type ViewServerRuntimeTopicDefinitions = TopicDefinitions &
  Record<
    string,
    {
      readonly schema: RowSchema & Schema.Decoder<object>;
      readonly key: string;
    }
  >;

type RuntimeHttpPath = `/${string}`;

export type ViewServerRuntimeOptions = {
  readonly host?: string;
  readonly websocketPort?: number;
  readonly rpcPath?: RuntimeHttpPath;
  readonly healthPath?: RuntimeHttpPath;
  readonly subscriptionQueueCapacity?: number;
};

export type ViewServerRuntime<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly url: string;
  readonly healthUrl: string;
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly liveClient: ViewServerLiveClient<Topics>;
  readonly health: () => Effect.Effect<ViewServerHealth<Topics>, ViewServerRuntimeError>;
  readonly close: Effect.Effect<void>;
};

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
  const inMemoryOptions =
    options.subscriptionQueueCapacity === undefined
      ? {}
      : { subscriptionQueueCapacity: options.subscriptionQueueCapacity };
  const serverOptions = {
    ...(options.host === undefined ? {} : { host: options.host }),
    ...(options.websocketPort === undefined ? {} : { port: options.websocketPort }),
    ...(options.rpcPath === undefined ? {} : { path: options.rpcPath }),
    ...(options.healthPath === undefined ? {} : { healthPath: options.healthPath }),
  };
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
