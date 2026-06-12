import { BrowserSocket } from "@effect/platform-browser";
import type {
  ExactGroupedQuery,
  ExactLiveQuery,
  ExactRawQuery,
  GroupedQuery,
  GroupedResult,
  LiveQueryRow,
  PickRawFields,
  RawQuery,
  TopicDefinitions,
  TopicRow,
  ValidateLiveQuery,
  ViewServerConfig,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@view-server/config";
import { VIEW_SERVER_HEALTH_SUMMARY_TOPIC, VIEW_SERVER_HEALTH_TOPIC } from "@view-server/config";
import { runAllFinalizers } from "@view-server/effect-utils";
import {
  ViewServerRpcs,
  viewServerDecodeHealth,
  viewServerDecodeHealthQuery,
  viewServerDecodeHealthSummaryEvent,
  viewServerDecodeHealthTopicEvent,
  viewServerDecodeTrustedLiveEvent,
  viewServerEncodeLiveQuery,
  type ViewServerRpcError,
  type ViewServerTrustedWireEvent,
  type ViewServerWireHealth,
  type ViewServerWireLiveQuery,
} from "@view-server/protocol";
import { Context, Effect, Exit, Layer, ManagedRuntime, Scope, Stream } from "effect";
import { RpcClient, RpcSerialization } from "effect/unstable/rpc";
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError";
import type {
  ViewServerLiveClient,
  ViewServerLiveEvent,
  ViewServerLiveSubscription,
  ViewServerStatusEvent,
} from "./live-client";
import { makeRemoteHealthState } from "./remote-health";
import { makeRemoteSubscription } from "./remote-subscription";

export type ViewServerRemoteClientError = ViewServerRuntimeError | ViewServerTransportError;

export type ViewServerClientOptions = {
  readonly url: string;
  readonly subscriptionBufferSize?: number;
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

const subscriptionFailureStatus = <Topic extends string>(
  topic: Topic,
  error: ViewServerRemoteClientError,
): ViewServerStatusEvent<Topic> => {
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

  const subscribeRpc = <Row, Topic extends string = string, Key extends string = string>(
    topic: Topic,
    query: ViewServerWireLiveQuery,
    decodeEvent: (
      event: ViewServerTrustedWireEvent,
    ) => Effect.Effect<ViewServerLiveEvent<Row, Topic, Key>, ViewServerRuntimeError>,
  ): Stream.Stream<ViewServerLiveEvent<Row, Topic, Key>, ViewServerRemoteClientError> =>
    rpc["ViewServer.Subscribe"](
      {
        topic,
        query,
      },
      {
        streamBufferSize: options.subscriptionBufferSize ?? 1_024,
      },
    ).pipe(Stream.mapError(mapViewServerRemoteError), Stream.mapEffect(decodeEvent));

  const initialHealth = yield* cleanupOnConstructionFailure(
    healthRpc().pipe(Effect.flatMap((next) => viewServerDecodeHealth(config, next))),
  );
  const remoteHealth = makeRemoteHealthState(initialHealth);
  const subscriptionBufferSize = options.subscriptionBufferSize ?? 1_024;
  const clientScope = yield* Scope.make("parallel");

  const close = runAllFinalizers([
    Scope.close(clientScope, Exit.void),
    managedRuntime.disposeEffect,
    remoteHealth.markStopping,
  ]);

  const streamToSubscription = <Row, Topic extends string = string, Key extends string = string>(
    topic: Topic,
    source: Stream.Stream<ViewServerLiveEvent<Row, Topic, Key>, ViewServerRemoteClientError>,
    lifecycle: {
      readonly onOpen: Effect.Effect<void>;
      readonly onClose: Effect.Effect<void>;
    } = {
      onOpen: Effect.void,
      onClose: Effect.void,
    },
  ) =>
    makeRemoteSubscription<Row, ViewServerRemoteClientError, Topic, Key>({
      clientScope,
      failureStatus: subscriptionFailureStatus,
      lifecycle,
      source,
      subscriptionBufferSize,
      topic,
    });

  const subscribeLive = Effect.fn("ViewServerClient.remote.subscribe")(function* <
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(topic: Topic, query: Query) {
    type Row = LiveQueryRow<TopicRow<Topics, Topic>, Query>;
    const wireQuery = yield* viewServerEncodeLiveQuery(config, topic, query);
    const stream = subscribeRpc<Row>(topic, wireQuery, (event) =>
      viewServerDecodeTrustedLiveEvent<Topics, Topic, Row>(config, topic, wireQuery, event),
    );
    return yield* streamToSubscription(topic, stream, {
      onOpen: remoteHealth.updateSubscriptionCount(topic, 1),
      onClose: remoteHealth.updateSubscriptionCount(topic, -1),
    });
  });

  function subscribe<
    Topic extends Extract<keyof Topics, string>,
    const Query extends GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: Query &
      ExactGroupedQuery<TopicRow<Topics, Topic>, NoInfer<Query>> &
      ValidateLiveQuery<NoInfer<Query>>,
  ): Effect.Effect<
    ViewServerLiveSubscription<GroupedResult<TopicRow<Topics, Topic>, Query>>,
    ViewServerRemoteClientError
  >;
  function subscribe<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: Query &
      ExactRawQuery<TopicRow<Topics, Topic>, NoInfer<Query>> &
      ValidateLiveQuery<NoInfer<Query>>,
  ): Effect.Effect<
    ViewServerLiveSubscription<PickRawFields<TopicRow<Topics, Topic>, Query>>,
    ViewServerRemoteClientError
  >;
  function subscribe<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: Query &
      ExactLiveQuery<TopicRow<Topics, Topic>, NoInfer<Query>> &
      ValidateLiveQuery<NoInfer<Query>>,
  ): Effect.Effect<
    ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRemoteClientError
  > {
    return subscribeLive(topic, query);
  }

  const subscribeHealthSummary = Effect.fn("ViewServerClient.remote.healthSummary.subscribe")(
    function* () {
      type Row = ViewServerHealthSummaryRow<Topics>;
      yield* viewServerDecodeHealthQuery(VIEW_SERVER_HEALTH_SUMMARY_TOPIC, { select: ["id"] });
      const stream = subscribeRpc<Row, typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC, "summary">(
        VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        { select: ["id"] },
        (event) => viewServerDecodeHealthSummaryEvent<Topics>(config, event),
      );
      const subscription = yield* streamToSubscription<
        Row,
        typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        "summary"
      >(VIEW_SERVER_HEALTH_SUMMARY_TOPIC, stream);
      const events = subscription.events.pipe(Stream.tap(remoteHealth.updateHealthSummaryRef));
      return {
        events,
        close: subscription.close,
      };
    },
  );

  const subscribeHealth = Effect.fn("ViewServerClient.remote.health.subscribe")(function* () {
    type Row = ViewServerHealthTopicRow<Extract<keyof Topics, string>>;
    yield* viewServerDecodeHealthQuery(VIEW_SERVER_HEALTH_TOPIC, { select: ["id"] });
    const stream = subscribeRpc<
      Row,
      typeof VIEW_SERVER_HEALTH_TOPIC,
      Extract<keyof Topics, string>
    >(VIEW_SERVER_HEALTH_TOPIC, { select: ["id"] }, (event) =>
      viewServerDecodeHealthTopicEvent<Topics>(config, event),
    );
    const subscription = yield* streamToSubscription<
      Row,
      typeof VIEW_SERVER_HEALTH_TOPIC,
      Extract<keyof Topics, string>
    >(VIEW_SERVER_HEALTH_TOPIC, stream);
    const events = subscription.events.pipe(Stream.tap(remoteHealth.updateHealthTopicRef));
    return {
      events,
      close: subscription.close,
    };
  });

  return {
    subscribe,
    subscribeHealthSummary,
    subscribeHealth,
    health: remoteHealth.readonlyHealth,
    close,
  };
});

export const createViewServerClient = makeViewServerClient;
