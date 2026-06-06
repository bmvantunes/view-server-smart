import { NodeHttpServer } from "@effect/platform-node";
import type { TopicDefinitions, ViewServerConfig } from "@view-server/config";
import { ViewServerRpcs } from "@view-server/protocol";
import { Context, Effect, Layer, ManagedRuntime } from "effect";
import { HttpRouter, HttpServer, HttpServerError } from "effect/unstable/http";
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
  const protocol = RpcServer.layerProtocolWebsocket({ path }).pipe(Layer.provide(HttpRouter.layer));
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
