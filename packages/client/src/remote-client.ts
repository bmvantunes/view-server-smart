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
  StatusEvent,
  TopicRuntimeHealth,
  TopicDefinitions,
  TopicRow,
  ValidateLiveQuery,
  ViewServerConfig,
  ViewServerHealth,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@view-server/config";
import { VIEW_SERVER_HEALTH_SUMMARY_TOPIC, VIEW_SERVER_HEALTH_TOPIC } from "@view-server/config";
import {
  ViewServerRpcs,
  viewServerDecodeHealth,
  viewServerDecodeHealthQuery,
  viewServerDecodeHealthSummaryEvent,
  viewServerDecodeHealthTopicEvent,
  viewServerDecodeLiveEvent,
  viewServerEncodeLiveQuery,
  type ViewServerRpcError,
  type ViewServerWireEvent,
  type ViewServerWireHealth,
  type ViewServerWireLiveQuery,
} from "@view-server/protocol";
import { Context, Effect, Exit, Layer, ManagedRuntime, Scope, Stream } from "effect";
import { AtomRef } from "effect/unstable/reactivity";
import { RpcClient, RpcSerialization } from "effect/unstable/rpc";
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError";
import type {
  ViewServerLiveClient,
  ViewServerLiveEvent,
  ViewServerLiveSubscription,
} from "./live-client";
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

function typedHealthTopics<Topics extends TopicDefinitions>(
  topics: Record<string, TopicRuntimeHealth>,
): ViewServerHealth<Topics>["engine"]["topics"];
function typedHealthTopics(
  topics: Record<string, TopicRuntimeHealth>,
): Record<string, TopicRuntimeHealth> {
  return topics;
}

const topicHealthFromRow = (
  existing: TopicRuntimeHealth,
  row: ViewServerHealthTopicRow,
): TopicRuntimeHealth => ({
  status: row.status === "stopping" ? existing.status : row.status,
  rowCount: row.rowCount,
  liveRowCount: row.liveRowCount,
  deletedRowCount: row.deletedRowCount,
  version: row.version,
  lastMutationAt: row.lastMutationAt,
  mutationsPerSecond: row.mutationsPerSecond,
  rowsPerSecond: row.rowsPerSecond,
  pendingMutationBatches: row.pendingMutationBatches,
  activeViews: row.activeViews,
  activeSubscriptions: row.activeSubscriptions,
  queuedEvents: row.queuedEvents,
  maxQueueDepth: row.maxQueueDepth,
  backpressureEvents: row.backpressureEvents,
  memoryBytes: row.memoryBytes,
  tombstoneCount: row.tombstoneCount,
  compactionPending: row.compactionPending,
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
    topic: string,
    query: ViewServerWireLiveQuery,
    decodeEvent: (
      event: ViewServerWireEvent,
    ) => Effect.Effect<ViewServerLiveEvent<Row>, ViewServerRuntimeError>,
  ): Stream.Stream<ViewServerLiveEvent<Row>, ViewServerRemoteClientError> =>
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
  const health = AtomRef.make(initialHealth);
  const subscriptionBufferSize = options.subscriptionBufferSize ?? 1_024;
  const clientScope = yield* Scope.make("parallel");

  const updateHealthSummaryRef = (event: ViewServerLiveEvent<ViewServerHealthSummaryRow<Topics>>) =>
    Effect.sync(() => {
      const applySummaryRow = (row: ViewServerHealthSummaryRow<Topics>) => {
        health.update((current) => ({
          ...current,
          status: row.runtimeStatus,
        }));
      };
      if (event.type === "snapshot") {
        for (const row of event.rows) {
          applySummaryRow(row);
        }
      }
      if (event.type === "delta") {
        for (const operation of event.operations) {
          if (operation.type === "insert" || operation.type === "update") {
            applySummaryRow(operation.row);
          }
        }
      }
    });

  const updateHealthTopicRef = (
    event: ViewServerLiveEvent<ViewServerHealthTopicRow<Extract<keyof Topics, string>>>,
  ) =>
    Effect.sync(() => {
      if (event.type === "snapshot") {
        health.update((current) => {
          const topics: Record<string, TopicRuntimeHealth> = { ...current.engine.topics };
          for (const row of event.rows) {
            topics[row.id] = topicHealthFromRow(current.engine.topics[row.id], row);
          }
          return {
            ...current,
            engine: {
              topics: typedHealthTopics<Topics>(topics),
            },
          };
        });
      }
      if (event.type === "delta") {
        health.update((current) => {
          const topics: Record<string, TopicRuntimeHealth> = { ...current.engine.topics };
          for (const operation of event.operations) {
            if (operation.type === "insert" || operation.type === "update") {
              topics[operation.key] = topicHealthFromRow(
                current.engine.topics[operation.row.id],
                operation.row,
              );
            }
          }
          return {
            ...current,
            engine: {
              topics: typedHealthTopics<Topics>(topics),
            },
          };
        });
      }
    });

  const updateLiveTopicHealth = <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    update: (current: TopicRuntimeHealth) => TopicRuntimeHealth,
  ) =>
    Effect.sync(() => {
      health.update((current) => {
        const topics: Record<string, TopicRuntimeHealth> = {
          ...current.engine.topics,
          [topic]: update(current.engine.topics[topic]),
        };
        return {
          ...current,
          engine: {
            topics: typedHealthTopics<Topics>(topics),
          },
        };
      });
    });

  const updateSubscriptionCount = <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    delta: 1 | -1,
  ) =>
    Effect.gen(function* () {
      yield* updateLiveTopicHealth(topic, (current) => {
        const activeSubscriptions = Math.max(0, current.activeSubscriptions + delta);
        return {
          ...current,
          activeSubscriptions,
        };
      });
      yield* Effect.sync(() => {
        health.update((current) => ({
          ...current,
          transport: {
            ...current.transport,
            activeStreams: Math.max(0, current.transport.activeStreams + delta),
            activeSubscriptions: Math.max(0, current.transport.activeSubscriptions + delta),
          },
        }));
      });
    });

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

  const streamToSubscription = <Row>(
    topic: string,
    source: Stream.Stream<ViewServerLiveEvent<Row>, ViewServerRemoteClientError>,
    lifecycle: {
      readonly onOpen: Effect.Effect<void>;
      readonly onClose: Effect.Effect<void>;
    } = {
      onOpen: Effect.void,
      onClose: Effect.void,
    },
  ) =>
    makeRemoteSubscription({
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
      viewServerDecodeLiveEvent<Topics, Topic, Row>(config, topic, wireQuery, event),
    );
    return yield* streamToSubscription(topic, stream, {
      onOpen: updateSubscriptionCount(topic, 1),
      onClose: updateSubscriptionCount(topic, -1),
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
      const stream = subscribeRpc<Row>(
        VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        { select: ["id"] },
        (event) => viewServerDecodeHealthSummaryEvent(config, event),
      );
      const subscription = yield* streamToSubscription(VIEW_SERVER_HEALTH_SUMMARY_TOPIC, stream);
      const events = subscription.events.pipe(Stream.tap(updateHealthSummaryRef));
      return {
        events,
        close: subscription.close,
      };
    },
  );

  const subscribeHealth = Effect.fn("ViewServerClient.remote.health.subscribe")(function* () {
    type Row = ViewServerHealthTopicRow<Extract<keyof Topics, string>>;
    yield* viewServerDecodeHealthQuery(VIEW_SERVER_HEALTH_TOPIC, { select: ["id"] });
    const stream = subscribeRpc<Row>(VIEW_SERVER_HEALTH_TOPIC, { select: ["id"] }, (event) =>
      viewServerDecodeHealthTopicEvent(config, event),
    );
    const subscription = yield* streamToSubscription(VIEW_SERVER_HEALTH_TOPIC, stream);
    const events = subscription.events.pipe(Stream.tap(updateHealthTopicRef));
    return {
      events,
      close: subscription.close,
    };
  });

  return {
    subscribe,
    subscribeHealthSummary,
    subscribeHealth,
    health: health.map((value) => value),
    close,
  };
});

export const createViewServerClient = makeViewServerClient;
