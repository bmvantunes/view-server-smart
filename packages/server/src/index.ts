import { NodeHttpServer } from "@effect/platform-node";
import type {
  TopicDefinitions,
  ViewServerConfig,
  ViewServerRuntimeClient,
} from "@view-server/config";
import type { ViewServerLiveClient } from "@view-server/client";
import {
  ViewServerRpcs,
  viewServerDecodeRawQuery,
  viewServerDecodeTopic,
  viewServerEncodeLiveEvent,
} from "@view-server/protocol";
import { Context, Effect, Layer, ManagedRuntime, Stream } from "effect";
import { HttpRouter, HttpServer, HttpServerError } from "effect/unstable/http";
import { RpcSerialization, RpcServer } from "effect/unstable/rpc";
import * as Http from "node:http";

type ViewServerServerRuntime<Topics extends TopicDefinitions> = Pick<
  ViewServerRuntimeClient<Topics>,
  "health"
>;

export type ViewServerWebSocketServerInput<Topics extends TopicDefinitions> = {
  readonly liveClient: ViewServerLiveClient<Topics>;
  readonly runtime: ViewServerServerRuntime<Topics>;
};

export type ViewServerWebSocketServerOptions = {
  readonly host?: string;
  readonly port?: number;
  readonly path?: HttpRouter.PathInput;
};

export type ViewServerWebSocketServer = {
  readonly url: string;
  readonly close: Effect.Effect<void>;
};

const makeHandlers = <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerWebSocketServerInput<Topics>,
) =>
  ViewServerRpcs.of({
    "ViewServer.Health": () => input.runtime.health(),
    "ViewServer.Subscribe": (payload) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const topic = yield* viewServerDecodeTopic(config, payload.topic);
          const query = yield* viewServerDecodeRawQuery(config, topic, payload.query);
          const subscription = yield* input.liveClient.subscribe(topic, query);
          const selectedFields = new Set<string>(query.select);
          return subscription.events.pipe(
            Stream.mapEffect((event) =>
              viewServerEncodeLiveEvent(config, topic, selectedFields, event),
            ),
            Stream.ensuring(subscription.close().pipe(Effect.ignore)),
          );
        }),
      ),
  });

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
  const protocol = RpcServer.layerProtocolWebsocket({ path }).pipe(Layer.provide(HttpRouter.layer));
  const handlers = ViewServerRpcs.toLayer(makeHandlers(config, input));
  const rpcLayer = RpcServer.layer(ViewServerRpcs, {
    disableFatalDefects: true,
  }).pipe(
    Layer.provide(handlers),
    Layer.provideMerge(protocol),
    Layer.provide(
      HttpRouter.serve(protocol, {
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
  return {
    url: `${serverUrl}${path}`,
    close: managedRuntime.disposeEffect,
  };
});

export const createViewServerWebSocketServer = makeViewServerWebSocketServer;
