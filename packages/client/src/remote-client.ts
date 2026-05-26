import { BrowserSocket } from "@effect/platform-browser";
import type {
  ExactRawQuery,
  LiveQueryRow,
  StatusEvent,
  TopicDefinitions,
  TopicRow,
  ValidateLiveQuery,
  ViewServerConfig,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@view-server/config";
import {
  ViewServerRpcs,
  viewServerDecodeHealth,
  viewServerDecodeLiveEvent,
  viewServerEncodeRawQuery,
  type ViewServerRpcError,
  type ViewServerWireHealth,
  type ViewServerWireRawQuery,
} from "@view-server/protocol";
import { Cause, Context, Effect, Exit, Layer, ManagedRuntime, Queue, Scope, Stream } from "effect";
import type * as Duration from "effect/Duration";
import * as AtomRef from "effect/unstable/reactivity/AtomRef";
import { RpcClient, RpcSerialization } from "effect/unstable/rpc";
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError";
import type {
  ViewServerLiveClient,
  ViewServerLiveEvent,
  ViewServerLiveSubscription,
} from "./live-client";

export type ViewServerRemoteClientError = ViewServerRuntimeError | ViewServerTransportError;

export type ViewServerClientOptions = {
  readonly url: string;
  readonly subscriptionBufferSize?: number;
  readonly healthPollInterval?: Duration.Input | false;
};

export type ViewServerRemoteClient<Topics extends TopicDefinitions> = ViewServerLiveClient<Topics>;

class ViewServerRpcClient extends Context.Service<
  ViewServerRpcClient,
  RpcClient.FromGroup<typeof ViewServerRpcs, RpcClientError>
>()("ViewServerRpcClient") {}

const rpcClientLayer = (url: string) =>
  Layer.effect(ViewServerRpcClient)(RpcClient.make(ViewServerRpcs)).pipe(
    Layer.provide(RpcClient.layerProtocolSocket()),
    Layer.provide([BrowserSocket.layerWebSocket(url), RpcSerialization.layerNdjson]),
  );

const transportError = (error: Error): ViewServerTransportError => ({
  _tag: "ViewServerTransportError",
  code: "TransportError",
  message: error.message,
});

export const mapViewServerRemoteError = (
  error: ViewServerRpcError | Error,
): ViewServerRemoteClientError => {
  if (error instanceof Error) {
    return transportError(error);
  }
  return error;
};

const subscriptionFailureStatus = (
  topic: string,
  error: ViewServerRemoteClientError,
): StatusEvent => {
  if (error.code === "BackpressureExceeded" || error.code === "SubscriptionClosed") {
    return {
      type: "status",
      topic,
      queryId: "remote",
      status: "closed",
      code: error.code,
      message: error.message,
    };
  }
  if (error.code === "SnapshotStale") {
    return {
      type: "status",
      topic,
      queryId: "remote",
      status: "stale",
      code: "SnapshotStale",
      message: error.message,
    };
  }
  return {
    type: "status",
    topic,
    queryId: "queryId" in error && error.queryId !== undefined ? error.queryId : "remote",
    status: "error",
    code: error.code,
    message: error.message,
  };
};

export const makeViewServerClient: <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  options: ViewServerClientOptions,
) => Effect.Effect<ViewServerRemoteClient<Topics>, ViewServerRemoteClientError> = Effect.fn(
  "ViewServerClient.remote.make",
)(function* <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  options: ViewServerClientOptions,
) {
  const managedRuntime = ManagedRuntime.make(rpcClientLayer(options.url));
  const cleanupOnConstructionFailure = <Value, Error, Services>(
    effect: Effect.Effect<Value, Error, Services>,
  ): Effect.Effect<Value, Error, Services> =>
    effect.pipe(Effect.onError(() => managedRuntime.disposeEffect));

  const context = yield* cleanupOnConstructionFailure(managedRuntime.contextEffect);
  const rpc = Context.get(context, ViewServerRpcClient);

  const healthRpc = (): Effect.Effect<ViewServerWireHealth, ViewServerRemoteClientError> =>
    rpc["ViewServer.Health"](undefined).pipe(Effect.mapError(mapViewServerRemoteError));

  const subscribeRpc = <Row>(
    topic: Extract<keyof Topics, string>,
    query: ViewServerWireRawQuery,
  ): Stream.Stream<ViewServerLiveEvent<Row>, ViewServerRemoteClientError> =>
    rpc["ViewServer.Subscribe"](
      {
        topic,
        query,
      },
      {
        streamBufferSize: options.subscriptionBufferSize ?? 1_024,
      },
    ).pipe(
      Stream.mapError(mapViewServerRemoteError),
      Stream.mapEffect((event) =>
        viewServerDecodeLiveEvent<Topics, typeof topic, Row>(
          config,
          topic,
          new Set(query.select),
          event,
        ),
      ),
    );

  const initialHealth = yield* cleanupOnConstructionFailure(
    healthRpc().pipe(Effect.flatMap((next) => viewServerDecodeHealth(config, next))),
  );
  const health = AtomRef.make(initialHealth);
  const subscriptionBufferSize = options.subscriptionBufferSize ?? 1_024;
  const clientScope = yield* Scope.make("parallel");

  const updateHealth = Effect.fn("ViewServerClient.remote.health.update")(function* (
    next: typeof initialHealth,
  ) {
    yield* Effect.sync(() => {
      health.update(() => next);
    });
  });

  const refreshHealth = healthRpc().pipe(
    Effect.flatMap((next) => viewServerDecodeHealth(config, next)),
    Effect.flatMap(updateHealth),
  );

  const pollHealth = Effect.fn("ViewServerClient.remote.health.poll")(function* (
    interval: Duration.Input,
  ) {
    while (true) {
      yield* Effect.sleep(interval);
      yield* refreshHealth.pipe(Effect.ignore);
    }
  });

  const healthPollInterval = options.healthPollInterval ?? "1 second";
  if (healthPollInterval !== false) {
    yield* pollHealth(healthPollInterval).pipe(
      Effect.ignore,
      Effect.forkIn(clientScope, { startImmediately: true }),
      Effect.ignore,
    );
  }

  const close = Scope.close(clientScope, Exit.void).pipe(
    Effect.andThen(managedRuntime.disposeEffect),
    Effect.andThen(
      Effect.sync(() => {
        health.update((current) => ({
          ...current,
          status: "stopping",
        }));
      }),
    ),
  );

  const subscribe = Effect.fn("ViewServerClient.remote.subscribe")(function* <
    Topic extends Extract<keyof Topics, string>,
    const Query extends { readonly select: ReadonlyArray<unknown> },
  >(
    topic: Topic,
    query: Query & ExactRawQuery<TopicRow<Topics, Topic>, Query> & ValidateLiveQuery<Query>,
  ) {
    type Row = LiveQueryRow<TopicRow<Topics, Topic>, Query>;
    const wireQuery = yield* viewServerEncodeRawQuery(config, topic, query);
    const scope = yield* Scope.fork(clientScope, "parallel");
    const stream = subscribeRpc<Row>(topic, wireQuery).pipe(
      Stream.catch((error) => Stream.make(subscriptionFailureStatus(topic, error))),
    );
    const queue = yield* Queue.bounded<ViewServerLiveEvent<Row>, Cause.Done>(
      subscriptionBufferSize,
    );
    yield* Stream.runIntoQueue(stream, queue).pipe(
      Effect.forkIn(scope, { startImmediately: true }),
      Effect.ignore,
    );
    yield* refreshHealth.pipe(Effect.ignore);
    const closeSubscription = Scope.close(scope, Exit.void).pipe(
      Effect.andThen(refreshHealth),
      Effect.ignore,
    );
    return {
      events: Stream.fromQueue(queue).pipe(Stream.ensuring(closeSubscription)),
      close: () => closeSubscription,
    } satisfies ViewServerLiveSubscription<Row>;
  });

  return {
    subscribe,
    health: health.map((value) => value),
    close,
  };
});

export const createViewServerClient = makeViewServerClient;
