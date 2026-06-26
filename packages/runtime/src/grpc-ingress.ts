import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import {
  type GrpcClientValue,
  type GrpcConnectClientDefinition,
  type GrpcFeedAcquireInput,
  type GrpcMaterializedFeedDefinition,
  type GrpcMaterializedTopic,
  type GrpcMethodRequest,
  type GrpcMethodValue,
  type GrpcRuntimeClients,
  type GrpcServerStreamingMethodName,
  type TopicRow,
  type ViewServerConfig,
  type ViewServerRuntimeClient,
} from "@view-server/config";
import { ignoreLoggedTypedFailuresPreserveNonTypedFailures } from "@view-server/effect-utils";
import { Cause, Clock, Effect, Exit, Option, Schema, Scope, Stream } from "effect";
import type { ViewServerGrpcHealthLedger } from "./grpc-health";
import type { ResolvedViewServerGrpcRuntimeOptions } from "./runtime-options";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export class ViewServerGrpcIngressError extends Schema.TaggedErrorClass<ViewServerGrpcIngressError>()(
  "ViewServerGrpcIngressError",
  {
    message: Schema.String,
    cause: Schema.Unknown,
    feedName: Schema.optionalKey(Schema.String),
    topic: Schema.optionalKey(Schema.String),
  },
) {}

export type ViewServerGrpcIngress = {
  readonly close: Effect.Effect<void>;
};

export type ViewServerGrpcClientFactory = <
  const ClientDefinition extends GrpcConnectClientDefinition,
>(
  definition: ClientDefinition,
  baseUrl: string,
) => GrpcClientValue<ClientDefinition>;

type ViewServerGrpcHealthRefreshRequest = Effect.Effect<void>;

const grpcMessageBatchSize = 256;
const grpcMessageBatchFlushInterval = "2 millis";

const materializedFeedSession = {
  id: null,
  forwardedHeaders: {},
  systemHeaders: {},
};

const ignoreGrpcFeedReleaseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring gRPC feed release failure.",
);
const ignoreGrpcHealthRefreshFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring gRPC health refresh failure.",
);

export function makeDefaultGrpcClient<const ClientDefinition extends GrpcConnectClientDefinition>(
  definition: ClientDefinition,
  baseUrl: string,
): GrpcClientValue<ClientDefinition>;
export function makeDefaultGrpcClient(definition: GrpcConnectClientDefinition, baseUrl: string) {
  return createClient(definition.service, createGrpcTransport({ baseUrl }));
}

const grpcIngressError = (input: {
  readonly message: string;
  readonly cause: unknown;
  readonly feedName: string;
  readonly topic: string;
}) =>
  new ViewServerGrpcIngressError({
    message: input.message,
    cause: input.cause,
    feedName: input.feedName,
    topic: input.topic,
  });

const hasMessage = (value: unknown): value is { readonly message: string } =>
  typeof value === "object" &&
  value !== null &&
  "message" in value &&
  typeof value.message === "string";

const messageFromUnknown = (value: unknown): string => {
  if (value instanceof Error) {
    return value.message;
  }
  if (hasMessage(value)) {
    return value.message;
  }
  return String(value);
};

const grpcIngressErrorMessage = (error: ViewServerGrpcIngressError): string =>
  `${error.message}: ${messageFromUnknown(error.cause)}`;

const feedFailureMessage = (feedName: string, cause: Cause.Cause<unknown>): string => {
  const error = Cause.findErrorOption(cause);
  if (Option.isSome(error) && error.value instanceof ViewServerGrpcIngressError) {
    return `gRPC feed ${feedName} failed: ${grpcIngressErrorMessage(error.value)}`;
  }
  return `gRPC feed ${feedName} failed: ${Cause.pretty(cause)}`;
};

const unexpectedFeedCompletionMessage = (feedName: string): string =>
  `gRPC feed ${feedName} completed unexpectedly.`;

const markMaterializedFeedDegraded = Effect.fn("ViewServerRuntime.grpc.materialized.degraded")(
  function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
    requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
    health: ViewServerGrpcHealthLedger<Topics>,
    feedName: string,
    clientName: string,
    message: string,
  ) {
    yield* health.feedDegraded(feedName, message);
    yield* health.clientDegraded(clientName, message);
    yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
  },
);

const handleMaterializedFeedExit = Effect.fn("ViewServerRuntime.grpc.materialized.exit")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
  health: ViewServerGrpcHealthLedger<Topics>,
  feedName: string,
  clientName: string,
  exit: Exit.Exit<void, ViewServerGrpcIngressError>,
) {
  if (Exit.isSuccess(exit)) {
    yield* markMaterializedFeedDegraded(
      requestHealthRefresh,
      health,
      feedName,
      clientName,
      unexpectedFeedCompletionMessage(feedName),
    );
    return;
  }
  if (Cause.hasInterruptsOnly(exit.cause)) {
    yield* health.feedStopping(feedName);
    yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
    return;
  }
  yield* markMaterializedFeedDegraded(
    requestHealthRefresh,
    health,
    feedName,
    clientName,
    feedFailureMessage(feedName, exit.cause),
  );
});

const mapMaterializedValue = Effect.fn("ViewServerRuntime.grpc.materialized.map")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
  const Topic extends GrpcMaterializedTopic<Topics>,
  const ClientName extends Extract<keyof Clients, string>,
  const MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
>(
  config: ViewServerConfig<Topics>,
  feed: GrpcMaterializedFeedDefinition<Topics, Clients, Topic, ClientName, MethodName>,
  feedName: string,
  value: GrpcMethodValue<Clients[ClientName], MethodName>,
) {
  const topicDefinition = config.topics[feed.topic];
  return yield* Effect.try({
    try: (): TopicRow<Topics, Topic> =>
      feed.map({
        value,
        route: undefined,
        schema: topicDefinition.schema,
      }),
    catch: (cause) =>
      grpcIngressError({
        message: `gRPC feed mapping failed for ${feedName}`,
        cause,
        feedName,
        topic: feed.topic,
      }),
  });
});

const publishMaterializedBatch = Effect.fn("ViewServerRuntime.grpc.materialized.publishBatch")(
  function* <
    const Topics extends ViewServerRuntimeTopicDefinitions,
    const Clients extends GrpcRuntimeClients,
    const Topic extends GrpcMaterializedTopic<Topics>,
    const ClientName extends Extract<keyof Clients, string>,
    const MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
  >(
    config: ViewServerConfig<Topics>,
    client: ViewServerRuntimeClient<Topics>,
    requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
    health: ViewServerGrpcHealthLedger<Topics>,
    feed: GrpcMaterializedFeedDefinition<Topics, Clients, Topic, ClientName, MethodName>,
    feedName: string,
    values: ReadonlyArray<GrpcMethodValue<Clients[ClientName], MethodName>>,
  ) {
    const rows = yield* Effect.forEach(values, (value) =>
      mapMaterializedValue(config, feed, feedName, value).pipe(
        Effect.tapError((error) =>
          Clock.currentTimeMillis.pipe(
            Effect.flatMap((nowMillis) =>
              health.mappingFailed(feedName, {
                message: error.message,
                nowMillis,
              }),
            ),
          ),
        ),
      ),
    );
    yield* client.publishMany(feed.topic, rows).pipe(
      Effect.tapError((error) =>
        Clock.currentTimeMillis.pipe(
          Effect.flatMap((nowMillis) =>
            health.publishFailed(feedName, {
              message: error.message,
              nowMillis,
            }),
          ),
        ),
      ),
      Effect.mapError((cause) =>
        grpcIngressError({
          message: `gRPC feed publish failed for ${feedName}`,
          cause,
          feedName,
          topic: feed.topic,
        }),
      ),
    );
    const nowMillis = yield* Clock.currentTimeMillis;
    yield* health.rowsPublished(feedName, {
      messages: values.length,
      rows: rows.length,
      nowMillis,
    });
    yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
  },
);

const materializedAcquireInput = <
  const Clients extends GrpcRuntimeClients,
  const ClientName extends Extract<keyof Clients, string>,
  const MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
>(
  grpcClient: GrpcClientValue<Clients[ClientName]>,
  request: GrpcMethodRequest<Clients[ClientName], MethodName>,
): GrpcFeedAcquireInput<
  GrpcClientValue<Clients[ClientName]>,
  GrpcMethodRequest<Clients[ClientName], MethodName>,
  undefined
> => ({
  client: grpcClient,
  request,
  route: undefined,
  session: materializedFeedSession,
});

const startMaterializedFeed = Effect.fn("ViewServerRuntime.grpc.materialized.start")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
  const Topic extends GrpcMaterializedTopic<Topics>,
  const ClientName extends Extract<keyof Clients, string>,
  const MethodName extends GrpcServerStreamingMethodName<Clients[ClientName]>,
>(
  scope: Scope.Scope,
  config: ViewServerConfig<Topics>,
  runtimeClient: ViewServerRuntimeClient<Topics>,
  requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
  options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
  health: ViewServerGrpcHealthLedger<Topics>,
  makeClient: ViewServerGrpcClientFactory,
  feedName: string,
  feed: GrpcMaterializedFeedDefinition<Topics, Clients, Topic, ClientName, MethodName>,
) {
  const clientDefinition = options.clients[feed.client];
  if (clientDefinition === undefined) {
    return yield* grpcIngressError({
      message: `gRPC feed ${feedName} references missing client: ${feed.client}`,
      cause: feed.client,
      feedName,
      topic: feed.topic,
    });
  }
  const baseUrl = options.clientBaseUrls[feed.client];
  if (baseUrl === undefined) {
    return yield* grpcIngressError({
      message: `gRPC feed ${feedName} references unresolved client URL: ${feed.client}`,
      cause: feed.client,
      feedName,
      topic: feed.topic,
    });
  }
  const grpcClient = yield* Effect.try({
    try: () => makeClient(clientDefinition, baseUrl),
    catch: (cause) =>
      grpcIngressError({
        message: `gRPC client creation failed for ${feedName}`,
        cause,
        feedName,
        topic: feed.topic,
      }),
  });
  const request = yield* Effect.try({
    try: () => feed.request(),
    catch: (cause) =>
      grpcIngressError({
        message: `gRPC feed request creation failed for ${feedName}`,
        cause,
        feedName,
        topic: feed.topic,
      }),
  });
  const input = materializedAcquireInput(grpcClient, request);
  const releaseFeed = feed.release;
  const startedAt = yield* Clock.currentTimeMillis;
  yield* health.clientConnected(feed.client, startedAt);
  yield* health.feedReady(feedName);
  yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
  const runFeed = Effect.scoped(
    Effect.gen(function* () {
      const stream = yield* Effect.acquireRelease(
        Effect.try({
          try: () => feed.acquire(input),
          catch: (cause) =>
            grpcIngressError({
              message: `gRPC feed acquire failed for ${feedName}`,
              cause,
              feedName,
              topic: feed.topic,
            }),
        }),
        () =>
          releaseFeed === undefined
            ? Effect.void
            : ignoreGrpcFeedReleaseFailure(
                Effect.try({
                  try: () => releaseFeed(input),
                  catch: (cause) =>
                    grpcIngressError({
                      message: `gRPC feed release failed for ${feedName}`,
                      cause,
                      feedName,
                      topic: feed.topic,
                    }),
                }).pipe(Effect.flatMap((release) => release)),
              ),
      );
      yield* stream.pipe(
        Stream.mapError((cause) =>
          grpcIngressError({
            message: `gRPC feed stream failed for ${feedName}`,
            cause,
            feedName,
            topic: feed.topic,
          }),
        ),
        Stream.groupedWithin(grpcMessageBatchSize, grpcMessageBatchFlushInterval),
        Stream.runForEach((values) =>
          publishMaterializedBatch(
            config,
            runtimeClient,
            requestHealthRefresh,
            health,
            feed,
            feedName,
            values,
          ),
        ),
      );
    }),
  ).pipe(
    Effect.exit,
    Effect.flatMap((exit) =>
      handleMaterializedFeedExit(requestHealthRefresh, health, feedName, feed.client, exit),
    ),
  );
  yield* runFeed.pipe(Effect.forkIn(scope, { startImmediately: true }));
});

export const makeViewServerGrpcIngress: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics>,
  client: ViewServerRuntimeClient<Topics>,
  requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
  options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
  health: ViewServerGrpcHealthLedger<Topics>,
  makeClient?: ViewServerGrpcClientFactory,
) => Effect.Effect<ViewServerGrpcIngress, ViewServerGrpcIngressError> = Effect.fn(
  "ViewServerRuntime.grpc.makeIngress",
)(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics>,
  client: ViewServerRuntimeClient<Topics>,
  requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
  options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
  health: ViewServerGrpcHealthLedger<Topics>,
  makeClient: ViewServerGrpcClientFactory = makeDefaultGrpcClient,
) {
  const scope = yield* Scope.make("parallel");
  const feedNames = Object.entries(options.feeds)
    .filter(([, feed]) => feed.lifecycle === "materialized")
    .map(([feedName]) => feedName);
  return yield* Effect.gen(function* () {
    for (const [feedName, feed] of Object.entries(options.feeds)) {
      if (feed.lifecycle === "materialized") {
        yield* startMaterializedFeed(
          scope,
          config,
          client,
          requestHealthRefresh,
          options,
          health,
          makeClient,
          feedName,
          feed,
        );
      }
    }
    return {
      close: Effect.gen(function* () {
        yield* Effect.forEach(feedNames, (feedName) => health.feedStopping(feedName), {
          discard: true,
        });
        yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
        yield* Scope.close(scope, Exit.void);
      }),
    };
  }).pipe(Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void)));
});
