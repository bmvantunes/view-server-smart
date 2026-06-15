import { NodeHttpServer } from "@effect/platform-node";
import type { TopicDefinitions, ViewServerConfig } from "@view-server/config";
import { ViewServerRpcs } from "@view-server/protocol";
import { Context, Effect, Exit, Layer, ManagedRuntime, Scope } from "effect";
import { HttpRouter, HttpServer, HttpServerError, HttpServerRequest } from "effect/unstable/http";
import { RpcSerialization, RpcServer } from "effect/unstable/rpc";
import * as Http from "node:http";
import { makeViewServerHealthRoute } from "./health-route";
import { makeViewServerRpcHandlers } from "./rpc-handlers";
import type {
  Jsonify,
  ViewServerHealthHttpJson,
  ViewServerWebSocketServer,
  ViewServerWebSocketServerInput,
  ViewServerWebSocketServerOptions,
} from "./server-types";
import {
  closeTrackedSockets,
  makeTrackedUpgradeRequest,
  type ActiveSocketClosers,
} from "./websocket-tracking";

const makeTrackedWebSocketProtocol = Effect.fn("ViewServerServer.websocket.protocol.make")(
  function* <const Topics extends TopicDefinitions>(
    path: `/${string}`,
    input: ViewServerWebSocketServerInput<Topics>,
    activeSocketClosers: ActiveSocketClosers,
  ) {
    const router = yield* HttpRouter.HttpRouter;
    const clientOpened = input.transport?.clientOpened ?? Effect.void;
    const clientClosed = input.transport?.clientClosed ?? Effect.void;
    const { httpEffect, protocol } = yield* RpcServer.makeProtocolWithHttpEffectWebsocket;
    const trackedHttpEffect = Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      return yield* httpEffect.pipe(
        Effect.provideService(
          HttpServerRequest.HttpServerRequest,
          makeTrackedUpgradeRequest(request, clientOpened, clientClosed, activeSocketClosers),
        ),
      );
    });
    yield* router.add("GET", path, trackedHttpEffect);
    return protocol;
  },
);

const makeTrackedWebSocketProtocolLayer = <const Topics extends TopicDefinitions>(
  path: `/${string}`,
  input: ViewServerWebSocketServerInput<Topics>,
  activeSocketClosers: ActiveSocketClosers,
) =>
  Layer.effect(RpcServer.Protocol)(makeTrackedWebSocketProtocol(path, input, activeSocketClosers));

export type {
  Jsonify,
  ViewServerHealthHttpJson,
  ViewServerWebSocketServer,
  ViewServerWebSocketServerInput,
  ViewServerWebSocketServerOptions,
};

export const makeViewServerWebSocketServer: <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerWebSocketServerInput<Topics>,
  options?: ViewServerWebSocketServerOptions,
) => Effect.Effect<ViewServerWebSocketServer, HttpServerError.ServeError> = Effect.fn(
  "ViewServerServer.websocket.make",
)(function* <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerWebSocketServerInput<Topics>,
  options: ViewServerWebSocketServerOptions = {},
) {
  const path = options.path ?? "/rpc";
  const healthPath = options.healthPath ?? "/health";
  const handlerScope = yield* Scope.make("parallel");
  const activeSocketClosers: ActiveSocketClosers = new Set();
  const protocol = makeTrackedWebSocketProtocolLayer(path, input, activeSocketClosers).pipe(
    Layer.provide(HttpRouter.layer),
  );
  const handlers = ViewServerRpcs.toLayer(makeViewServerRpcHandlers(config, input, handlerScope));
  const healthRoute = makeViewServerHealthRoute(config, input, healthPath);
  const httpApp = Layer.merge(protocol, healthRoute);
  const rpcLayer = RpcServer.layer(ViewServerRpcs, {
    disableFatalDefects: true,
  }).pipe(
    Layer.provide(handlers),
    Layer.provideMerge(protocol),
    Layer.provide(
      HttpRouter.serve(httpApp, {
        disableListenLog: true,
        disableLogger: true,
      }),
    ),
    Layer.provideMerge(
      NodeHttpServer.layer(Http.createServer, {
        host: options.host,
        port: options.port ?? 0,
      }),
    ),
    Layer.provide(RpcSerialization.layerNdjson),
  );
  const managedRuntime = ManagedRuntime.make(rpcLayer);
  const context = yield* managedRuntime.contextEffect.pipe(
    Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(handlerScope, exit) : Effect.void)),
  );
  const server = Context.get(context, HttpServer.HttpServer);
  const httpUrl = HttpServer.formatAddress(server.address);
  const serverUrl = httpUrl.startsWith("http://0.0.0.0:")
    ? `ws://127.0.0.1:${httpUrl.slice("http://0.0.0.0:".length)}`
    : httpUrl.replace("http://", "ws://");
  const publicHttpUrl = serverUrl.replace("ws://", "http://");
  return {
    url: `${serverUrl}${path}`,
    healthUrl: `${publicHttpUrl}${healthPath}`,
    close: Scope.close(handlerScope, Exit.void).pipe(
      Effect.andThen(closeTrackedSockets(activeSocketClosers)),
      Effect.andThen(managedRuntime.disposeEffect),
    ),
  };
});

export const createViewServerWebSocketServer = makeViewServerWebSocketServer;
