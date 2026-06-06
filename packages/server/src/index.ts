import { NodeHttpServer } from "@effect/platform-node";
import type { TopicDefinitions, ViewServerConfig } from "@view-server/config";
import { ViewServerRpcs } from "@view-server/protocol";
import { Context, Effect, Layer, ManagedRuntime } from "effect";
import { HttpRouter, HttpServer, HttpServerError, HttpServerRequest } from "effect/unstable/http";
import { RpcSerialization, RpcServer } from "effect/unstable/rpc";
import * as Socket from "effect/unstable/socket/Socket";
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

const makeTrackedSocket = (
  socket: Socket.Socket,
  clientOpened: Effect.Effect<void>,
  clientClosed: Effect.Effect<void>,
): Socket.Socket =>
  new Proxy(socket, {
    get(target, property, receiver) {
      if (property === "runRaw") {
        const runRaw: Socket.Socket["runRaw"] = (handler, options) => {
          let closeWhenOpened = Effect.void;
          const onOpen = Effect.sync(() => {
            closeWhenOpened = clientClosed;
          }).pipe(Effect.andThen(clientOpened), Effect.andThen(options?.onOpen ?? Effect.void));
          const close = Effect.sync(() => closeWhenOpened).pipe(
            Effect.flatMap((closeEffect) => closeEffect),
          );
          return target
            .runRaw(handler, {
              ...options,
              onOpen,
            })
            .pipe(Effect.ensuring(close));
        };
        return runRaw;
      }
      return Reflect.get(target, property, receiver);
    },
  });

const makeTrackedUpgradeRequest = (
  request: HttpServerRequest.HttpServerRequest,
  clientOpened: Effect.Effect<void>,
  clientClosed: Effect.Effect<void>,
): HttpServerRequest.HttpServerRequest =>
  new Proxy(request, {
    get(target, property, receiver) {
      if (property === "upgrade") {
        return target.upgrade.pipe(
          Effect.map((socket) => makeTrackedSocket(socket, clientOpened, clientClosed)),
        );
      }
      return Reflect.get(target, property, receiver);
    },
  });

const makeTrackedWebSocketProtocol = Effect.fn("ViewServerServer.websocket.protocol.make")(
  function* <const Topics extends TopicDefinitions>(
    path: `/${string}`,
    input: ViewServerWebSocketServerInput<Topics>,
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
          makeTrackedUpgradeRequest(request, clientOpened, clientClosed),
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
) => Layer.effect(RpcServer.Protocol)(makeTrackedWebSocketProtocol(path, input));

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
  const protocol = makeTrackedWebSocketProtocolLayer(path, input).pipe(
    Layer.provide(HttpRouter.layer),
  );
  const handlers = ViewServerRpcs.toLayer(makeViewServerRpcHandlers(config, input));
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
  const context = yield* managedRuntime.contextEffect;
  const server = Context.get(context, HttpServer.HttpServer);
  const httpUrl = HttpServer.formatAddress(server.address);
  const serverUrl = httpUrl.startsWith("http://0.0.0.0:")
    ? `ws://127.0.0.1:${httpUrl.slice("http://0.0.0.0:".length)}`
    : httpUrl.replace("http://", "ws://");
  const publicHttpUrl = serverUrl.replace("ws://", "http://");
  return {
    url: `${serverUrl}${path}`,
    healthUrl: `${publicHttpUrl}${healthPath}`,
    close: managedRuntime.disposeEffect,
  };
});

export const createViewServerWebSocketServer = makeViewServerWebSocketServer;
