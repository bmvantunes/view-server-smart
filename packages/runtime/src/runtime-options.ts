import type { ViewServerRuntimeCoreOptionsFor } from "@effect-view-server/runtime-core";
import type { ViewServerWebSocketServerOptions } from "@effect-view-server/server";
import type {
  GrpcRuntimeClients,
  KafkaStartFromHealth,
  RuntimeRegions,
  RuntimeValue,
  ViewServerConfig,
  ViewServerKafkaStartFrom,
} from "@effect-view-server/config";
import {
  makeKafkaRuntimeTopicsForConfig,
  type KafkaRuntimeSourceTopicDefinition,
} from "@effect-view-server/config/internal";
import type { Duration } from "effect";
import { Config, Duration as EffectDuration, Effect, Option } from "effect";
import type {
  ViewServerKafkaRuntimeOptions,
  ViewServerGrpcRuntimeOptions,
  ViewServerRuntimeOptions,
  ViewServerRuntimeTopicDefinitions,
} from "./runtime-types";
import { ViewServerGrpcIngressError } from "./grpc-ingress";
import { messageFromUnknown, ViewServerKafkaIngressError } from "./kafka-ingress";

export type ResolvedViewServerRuntimeOptions<
  Topics extends ViewServerRuntimeTopicDefinitions = ViewServerRuntimeTopicDefinitions,
  Regions extends RuntimeRegions = RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
> = {
  readonly auth?: ViewServerRuntimeOptions<Topics, Regions, GrpcClients>["auth"];
  readonly runtimeCoreOptions: ViewServerRuntimeCoreOptionsFor<Topics>;
  readonly serverOptions: ViewServerWebSocketServerOptions;
  readonly tcpPublishOptions?: {
    readonly host?: string;
    readonly maxConnections?: number;
    readonly port: number;
  };
  readonly kafkaOptions?: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>;
  readonly grpcOptions?: ResolvedViewServerGrpcRuntimeOptions<Topics, GrpcClients>;
};

export type ResolvedViewServerKafkaRuntimeOptions<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Regions extends RuntimeRegions = RuntimeRegions,
> = {
  readonly consumerGroupId: string;
  readonly startFrom: ViewServerKafkaStartFrom;
  readonly consume: KafkaStartFromHealth;
  readonly regions: Record<string, string>;
  readonly topics: Record<string, KafkaRuntimeSourceTopicDefinition<Topics, Regions>>;
};

export type ResolvedViewServerGrpcRuntimeOptions<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Clients extends GrpcRuntimeClients = GrpcRuntimeClients,
> = {
  readonly clients: Clients;
  readonly clientBaseUrls: Record<string, string>;
  readonly feeds: ViewServerGrpcRuntimeOptions<Topics, Clients>["feeds"];
  readonly materializedReconnect: {
    readonly maxReconnects: number;
    readonly delay: Duration.Input;
  };
};

const resolveRuntimeValue = <A>(value: RuntimeValue<A>): Effect.Effect<A, Config.ConfigError> =>
  Config.isConfig(value) ? value : Effect.succeed(value);

const defaultKafkaStartFrom = (consumerGroupId: string): ViewServerKafkaStartFrom => ({
  committedConsumerGroup: consumerGroupId,
});

const defaultGrpcMaterializedReconnect = {
  delay: "1 second",
  maxReconnects: 60,
} satisfies ResolvedViewServerGrpcRuntimeOptions<ViewServerRuntimeTopicDefinitions>["materializedReconnect"];

const validateGrpcMaterializedMaxReconnects = (
  maxReconnects: number,
): Effect.Effect<number, ViewServerGrpcIngressError> => {
  if (Number.isSafeInteger(maxReconnects) && maxReconnects >= 0) {
    return Effect.succeed(maxReconnects);
  }
  return Effect.fail(
    new ViewServerGrpcIngressError({
      message: "gRPC materialized reconnect maxReconnects must be a finite non-negative integer.",
      cause: maxReconnects,
      phase: "configuration",
    }),
  );
};

const validateGrpcMaterializedReconnectDelay = (
  delay: Duration.Input,
): Effect.Effect<Duration.Input, ViewServerGrpcIngressError> => {
  const duration = EffectDuration.fromInput(delay);
  if (Option.isSome(duration) && EffectDuration.isFinite(duration.value)) {
    const millis = EffectDuration.toMillis(duration.value);
    if (Number.isFinite(millis) && millis > 0) {
      return Effect.succeed(delay);
    }
  }
  return Effect.fail(
    new ViewServerGrpcIngressError({
      message: "gRPC materialized reconnect delay must be finite and positive.",
      cause: delay,
      phase: "configuration",
    }),
  );
};

const normalizeKafkaConsumePolicy = (
  consumerGroupId: string,
  startFrom: ViewServerKafkaStartFrom,
): ResolvedViewServerKafkaRuntimeOptions<ViewServerRuntimeTopicDefinitions>["consume"] => {
  if (startFrom === "earliest") {
    return {
      consumerGroupId,
      fallbackMode: "earliest",
      mode: "earliest",
    };
  }
  if (startFrom === "latest") {
    return {
      consumerGroupId,
      fallbackMode: "latest",
      mode: "latest",
    };
  }
  return {
    consumerGroupId: startFrom.committedConsumerGroup,
    fallbackMode: startFrom.fallback ?? "earliest",
    mode: "committed",
  };
};

const kafkaSourcesFromConfig = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  config: ViewServerConfig<Topics, Regions>,
): Effect.Effect<
  Record<string, KafkaRuntimeSourceTopicDefinition<Topics, Regions>>,
  ViewServerKafkaIngressError
> =>
  Effect.gen(function* () {
    const runtimeTopics = yield* Effect.try({
      try: () => makeKafkaRuntimeTopicsForConfig<Topics, Regions>(config),
      catch: (cause) =>
        new ViewServerKafkaIngressError({
          message: `Invalid topic-owned Kafka source configuration: ${messageFromUnknown(cause)}`,
          cause,
        }),
    });
    const topics: Record<
      string,
      KafkaRuntimeSourceTopicDefinition<Topics, Regions>
    > = Object.create(null);
    for (const runtimeTopic of runtimeTopics) {
      if (topics[runtimeTopic.topic] !== undefined) {
        return yield* new ViewServerKafkaIngressError({
          message: `Kafka source topic is configured more than once: ${runtimeTopic.topic}`,
          cause: runtimeTopic.topic,
          sourceTopic: runtimeTopic.topic,
        });
      }
      topics[runtimeTopic.topic] = runtimeTopic;
    }
    return topics;
  });

const emptyKafkaRuntimeTopics = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(): Record<string, KafkaRuntimeSourceTopicDefinition<Topics, Regions>> => Object.create(null);

const requireKafkaRuntimeOptionsForConfigSources = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  config: ViewServerConfig<Topics, Regions> | undefined,
): Effect.Effect<void, ViewServerKafkaIngressError> =>
  Effect.gen(function* () {
    const configuredTopics =
      config === undefined
        ? emptyKafkaRuntimeTopics<Topics, Regions>()
        : yield* kafkaSourcesFromConfig(config);
    if (Object.keys(configuredTopics).length > 0) {
      return yield* new ViewServerKafkaIngressError({
        message:
          "Kafka sources are configured, but runtime options.kafka.consumerGroupId was not provided.",
        cause: "missing-kafka-consumer-group",
      });
    }
  });

const validateKafkaTopicRegions = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  topics: Record<string, KafkaRuntimeSourceTopicDefinition<Topics, Regions>>,
  regions: Record<string, string>,
): Effect.Effect<void, ViewServerKafkaIngressError> =>
  Effect.gen(function* () {
    for (const [sourceTopic, topic] of Object.entries(topics)) {
      for (const region of topic.regions) {
        if (regions[region] === undefined) {
          return yield* new ViewServerKafkaIngressError({
            message: `Kafka source topic ${sourceTopic} references unknown Kafka region: ${region}`,
            cause: {
              region,
              sourceTopic,
            },
            region,
            sourceTopic,
          });
        }
      }
    }
  });

const validateKafkaConsumerGroupId = (
  consumerGroupId: unknown,
): Effect.Effect<string, ViewServerKafkaIngressError> =>
  Effect.gen(function* () {
    if (typeof consumerGroupId !== "string" || consumerGroupId.length === 0) {
      return yield* new ViewServerKafkaIngressError({
        message:
          "Kafka sources are configured, but runtime options.kafka.consumerGroupId was not provided.",
        cause: "missing-kafka-consumer-group",
      });
    }
    return consumerGroupId;
  });

const resolveKafkaOptions: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  config: ViewServerConfig<Topics, Regions> | undefined,
  options: ViewServerKafkaRuntimeOptions<Topics, Regions>,
) => Effect.Effect<
  ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
  Config.ConfigError | ViewServerKafkaIngressError
> = Effect.fn("ViewServerRuntime.options.kafka.resolve")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  config: ViewServerConfig<Topics, Regions> | undefined,
  options: ViewServerKafkaRuntimeOptions<Topics, Regions>,
) {
  const consumerGroupId = yield* validateKafkaConsumerGroupId(options.consumerGroupId);
  const configuredRegions = options.regions ?? config?.kafka;
  const entries = yield* Effect.forEach(
    Object.entries(configuredRegions ?? {}),
    ([region, value]) =>
      resolveRuntimeValue(value).pipe(Effect.map((bootstrap) => [region, bootstrap] as const)),
  );
  const regions: Record<string, string> = Object.create(null);
  for (const [region, bootstrap] of entries) {
    regions[region] = bootstrap;
  }
  const configuredTopics =
    config === undefined
      ? emptyKafkaRuntimeTopics<Topics, Regions>()
      : yield* kafkaSourcesFromConfig(config);
  if (options.topics !== undefined && Object.keys(configuredTopics).length > 0) {
    return yield* new ViewServerKafkaIngressError({
      message:
        "Kafka runtime topics cannot be provided when config topics declare kafkaSource; use topic-owned kafkaSource definitions or legacy runtime kafka.topics, not both.",
      cause: "mixed-kafka-source-configuration",
    });
  }
  const topics = options.topics ?? configuredTopics;
  if (Object.keys(topics).length > 0 && Object.keys(regions).length === 0) {
    return yield* new ViewServerKafkaIngressError({
      message:
        "Kafka sources are configured, but no Kafka regions were provided on config.kafka or runtime options.kafka.regions.",
      cause: "missing-kafka-regions",
    });
  }
  yield* validateKafkaTopicRegions(topics, regions);
  const startFrom = options.startFrom ?? defaultKafkaStartFrom(consumerGroupId);
  return {
    consumerGroupId,
    consume: normalizeKafkaConsumePolicy(consumerGroupId, startFrom),
    regions,
    startFrom,
    topics,
  };
});

const resolveGrpcOptions: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, RuntimeRegions, Clients> | undefined,
  options: ViewServerGrpcRuntimeOptions<Topics, Clients>,
) => Effect.Effect<
  ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
  Config.ConfigError | ViewServerGrpcIngressError
> = Effect.fn("ViewServerRuntime.options.grpc.resolve")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, RuntimeRegions, Clients> | undefined,
  options: ViewServerGrpcRuntimeOptions<Topics, Clients>,
) {
  const clients = options.clients ?? config?.grpc?.clients;
  if (clients === undefined) {
    return yield* new ViewServerGrpcIngressError({
      message:
        "gRPC feeds are configured, but no gRPC clients were provided on config.grpc.clients or runtime options.grpc.clients.",
      cause: "missing-grpc-clients",
      phase: "configuration",
    });
  }
  const entries = yield* Effect.forEach(Object.entries(clients), ([clientName, client]) =>
    resolveRuntimeValue(client.baseUrl).pipe(
      Effect.map(
        (baseUrl) =>
          [
            clientName,
            {
              _tag: client._tag,
              baseUrl,
              protocol: client.protocol,
              service: client.service,
            },
          ] as const,
      ),
    ),
  );
  const clientBaseUrls: Record<string, string> = Object.create(null);
  for (const [clientName, client] of entries) {
    clientBaseUrls[clientName] = client.baseUrl;
  }
  const materializedReconnectDelay = yield* validateGrpcMaterializedReconnectDelay(
    options.materializedReconnect?.delay ?? defaultGrpcMaterializedReconnect.delay,
  );
  const materializedReconnectMaxReconnects = yield* validateGrpcMaterializedMaxReconnects(
    options.materializedReconnect?.maxReconnects ?? defaultGrpcMaterializedReconnect.maxReconnects,
  );
  const feedTopics = new Map<string, string>();
  for (const [feedName, feed] of Object.entries(options.feeds)) {
    const previousFeedName = feedTopics.get(feed.topic);
    if (previousFeedName !== undefined) {
      return yield* new ViewServerGrpcIngressError({
        message: `gRPC feed ${feedName} conflicts with ${previousFeedName}; View Server topic ${feed.topic} already has a gRPC feed owner.`,
        cause: feed.topic,
        feedName,
        topic: feed.topic,
      });
    }
    feedTopics.set(feed.topic, feedName);
  }
  return {
    clients,
    clientBaseUrls,
    feeds: options.feeds,
    materializedReconnect: {
      delay: materializedReconnectDelay,
      maxReconnects: materializedReconnectMaxReconnects,
    },
  };
});

type SourceOwnershipKafkaOptions = {
  readonly topics: Readonly<Record<string, { readonly viewServerTopic: string }>>;
};

type SourceOwnershipGrpcOptions = {
  readonly feeds: Readonly<Record<string, { readonly topic: string }>>;
};

export const validateSourceOwnership: (
  kafkaOptions: SourceOwnershipKafkaOptions | undefined,
  grpcOptions: SourceOwnershipGrpcOptions | undefined,
) => Effect.Effect<void, ViewServerGrpcIngressError> = Effect.fn(
  "ViewServerRuntime.options.sourceOwnership.validate",
)(function* (
  kafkaOptions: SourceOwnershipKafkaOptions | undefined,
  grpcOptions: SourceOwnershipGrpcOptions | undefined,
) {
  if (kafkaOptions === undefined || grpcOptions === undefined) {
    return;
  }
  const grpcFeedByTopic = new Map<string, string>();
  for (const [feedName, feed] of Object.entries(grpcOptions.feeds)) {
    grpcFeedByTopic.set(feed.topic, feedName);
  }
  for (const [sourceTopic, kafkaTopic] of Object.entries(kafkaOptions.topics)) {
    const grpcFeedName = grpcFeedByTopic.get(kafkaTopic.viewServerTopic);
    if (grpcFeedName !== undefined) {
      return yield* new ViewServerGrpcIngressError({
        message: `View Server topic ${kafkaTopic.viewServerTopic} cannot be owned by both Kafka source ${sourceTopic} and gRPC feed ${grpcFeedName}.`,
        cause: kafkaTopic.viewServerTopic,
        feedName: grpcFeedName,
        topic: kafkaTopic.viewServerTopic,
      });
    }
  }
});

type GrpcTopicSourceMetadata =
  | {
      readonly _tag: "absent";
    }
  | {
      readonly _tag: "invalid";
      readonly cause: unknown;
    }
  | {
      readonly _tag: "valid";
      readonly lifecycle: "materialized";
    }
  | {
      readonly _tag: "valid";
      readonly lifecycle: "leased";
      readonly routeBy: ReadonlyArray<string>;
    };

const hasDefinedOwnProperty = (value: object, key: string): boolean =>
  Object.prototype.hasOwnProperty.call(value, key) && Reflect.get(value, key) !== undefined;

const hasOnlyOwnStringKeys = (value: object, allowedKeys: ReadonlyArray<string>): boolean =>
  Object.getOwnPropertyNames(value).every((key) => allowedKeys.includes(key));

const grpcTopicSourceFromUnknown = (source: unknown): GrpcTopicSourceMetadata => {
  if (typeof source !== "object" || source === null) {
    return { _tag: "invalid", cause: source };
  }
  if (!hasOnlyOwnStringKeys(source, ["_tag", "kind", "lifecycle", "routeBy"])) {
    return { _tag: "invalid", cause: source };
  }
  if (Reflect.get(source, "kind") !== "grpc") {
    return { _tag: "invalid", cause: source };
  }
  const lifecycle = Reflect.get(source, "lifecycle");
  if (lifecycle !== "leased" && lifecycle !== "materialized") {
    return { _tag: "invalid", cause: source };
  }
  const sourceTag = Reflect.get(source, "_tag");
  if (lifecycle === "materialized") {
    if (sourceTag !== "GrpcMaterializedTopicSource") {
      return { _tag: "invalid", cause: source };
    }
    return { _tag: "valid", lifecycle };
  }
  if (sourceTag !== "GrpcLeasedTopicSource") {
    return { _tag: "invalid", cause: source };
  }
  const routeBy = Reflect.get(source, "routeBy");
  if (!Array.isArray(routeBy) || !routeBy.every((field) => typeof field === "string")) {
    return { _tag: "invalid", cause: source };
  }
  return { _tag: "valid", lifecycle, routeBy };
};

const grpcTopicSourceMetadata = (topicDefinition: unknown): GrpcTopicSourceMetadata => {
  if (typeof topicDefinition !== "object" || topicDefinition === null) {
    return { _tag: "absent" };
  }
  if (hasDefinedOwnProperty(topicDefinition, "grpcSource")) {
    return grpcTopicSourceFromUnknown(Reflect.get(topicDefinition, "grpcSource"));
  }
  if (hasDefinedOwnProperty(topicDefinition, "source")) {
    return grpcTopicSourceFromUnknown(Reflect.get(topicDefinition, "source"));
  }
  return { _tag: "absent" };
};

const grpcFeedLeasedRouteBy = (feed: unknown): ReadonlyArray<string> | undefined => {
  const routeBy = Reflect.get(Object(feed), "routeBy");
  return Array.isArray(routeBy) && routeBy.every((field) => typeof field === "string")
    ? routeBy
    : undefined;
};

const sameRouteBy = (left: ReadonlyArray<string>, right: ReadonlyArray<string>): boolean =>
  left.length === right.length && left.every((field, index) => field === right[index]);

export const validateGrpcSourceFeeds: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics>,
  grpcOptions: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients> | undefined,
) => Effect.Effect<void, ViewServerGrpcIngressError> = Effect.fn(
  "ViewServerRuntime.options.grpcSourceFeeds.validate",
)(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics>,
  grpcOptions: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients> | undefined,
) {
  const feedEntries = Object.entries(grpcOptions?.feeds ?? {});
  for (const [topic, topicDefinition] of Object.entries(config.topics)) {
    const sourceMetadata = grpcTopicSourceMetadata(topicDefinition);
    if (sourceMetadata._tag === "absent") {
      continue;
    }
    if (sourceMetadata._tag === "invalid") {
      return yield* new ViewServerGrpcIngressError({
        message: `View Server topic ${topic} declares invalid gRPC source metadata.`,
        cause: sourceMetadata.cause,
        feedName: topic,
        topic,
        phase: "configuration",
      });
    }
    const lifecycle = sourceMetadata.lifecycle;
    const matchingFeeds = feedEntries.filter(([_feedName, feed]) => feed.topic === topic);
    if (matchingFeeds.length === 0) {
      return yield* new ViewServerGrpcIngressError({
        message: `View Server topic ${topic} declares gRPC ${lifecycle} source but no matching gRPC feed was configured.`,
        cause: topic,
        feedName: topic,
        topic,
      });
    }
    const mismatchedFeed = matchingFeeds.find(([_feedName, feed]) => feed.lifecycle !== lifecycle);
    if (mismatchedFeed !== undefined) {
      const [feedName, feed] = mismatchedFeed;
      return yield* new ViewServerGrpcIngressError({
        message: `gRPC feed ${feedName} lifecycle ${feed.lifecycle} does not match View Server topic ${topic} source lifecycle ${lifecycle}.`,
        cause: feed.lifecycle,
        feedName,
        topic,
      });
    }
    if (lifecycle === "leased") {
      const sourceRouteBy = sourceMetadata.routeBy;
      const routeMismatch = matchingFeeds.find(([_feedName, feed]) => {
        const feedRouteBy = grpcFeedLeasedRouteBy(feed);
        return feedRouteBy === undefined || !sameRouteBy(sourceRouteBy, feedRouteBy);
      });
      if (routeMismatch !== undefined) {
        const [feedName, feed] = routeMismatch;
        const feedRouteBy = grpcFeedLeasedRouteBy(feed) ?? [];
        return yield* new ViewServerGrpcIngressError({
          message: `gRPC leased feed ${feedName} routeBy ${feedRouteBy.join(", ")} does not match View Server topic ${topic} source routeBy ${sourceRouteBy.join(", ")}.`,
          cause: feedRouteBy,
          feedName,
          topic,
        });
      }
    }
  }
  for (const [feedName, feed] of feedEntries) {
    const topicDefinition = config.topics[feed.topic];
    if (topicDefinition === undefined) {
      return yield* new ViewServerGrpcIngressError({
        message: `gRPC feed ${feedName} references unknown View Server topic ${feed.topic}.`,
        cause: feed.topic,
        feedName,
        topic: feed.topic,
      });
    }
    const sourceMetadata = grpcTopicSourceMetadata(topicDefinition);
    if (sourceMetadata._tag === "absent") {
      return yield* new ViewServerGrpcIngressError({
        message: `gRPC feed ${feedName} targets View Server topic ${feed.topic}, but that topic does not declare a gRPC source.`,
        cause: feed.topic,
        feedName,
        topic: feed.topic,
      });
    }
  }
});

const resolveViewServerRuntimeOptionsWithConfig: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients> | undefined,
  options: ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
) => Effect.Effect<
  ResolvedViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  Config.ConfigError | ViewServerGrpcIngressError | ViewServerKafkaIngressError
> = Effect.fn("ViewServerRuntime.options.resolve")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients> | undefined,
  options: ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
) {
  const runtimeCoreOptions = {
    ...(options.groupedIncrementalAdmissionLimits === undefined
      ? {}
      : { groupedIncrementalAdmissionLimits: options.groupedIncrementalAdmissionLimits }),
    ...(options.subscriptionQueueCapacity === undefined
      ? {}
      : { subscriptionQueueCapacity: options.subscriptionQueueCapacity }),
  };
  const serverOptions = {
    ...(options.host === undefined ? {} : { host: options.host }),
    ...(options.websocketPort === undefined ? {} : { port: options.websocketPort }),
    ...(options.rpcPath === undefined ? {} : { path: options.rpcPath }),
    ...(options.healthPath === undefined ? {} : { healthPath: options.healthPath }),
    ...(options.metricsPath === undefined ? {} : { metricsPath: options.metricsPath }),
  };
  const tcpPublishOptions =
    options.tcpPublishPort === undefined
      ? undefined
      : {
          ...(options.tcpPublishHost === undefined ? {} : { host: options.tcpPublishHost }),
          ...(options.tcpPublishMaxConnections === undefined
            ? {}
            : { maxConnections: options.tcpPublishMaxConnections }),
          port: options.tcpPublishPort,
        };
  const kafkaOptions =
    options.kafka === undefined
      ? (yield* requireKafkaRuntimeOptionsForConfigSources(config), undefined)
      : yield* resolveKafkaOptions(config, options.kafka);
  const grpcOptions =
    options.grpc === undefined ? undefined : yield* resolveGrpcOptions(config, options.grpc);
  yield* validateSourceOwnership(kafkaOptions, grpcOptions);
  return {
    ...(options.auth === undefined ? {} : { auth: options.auth }),
    runtimeCoreOptions,
    serverOptions,
    ...(tcpPublishOptions === undefined ? {} : { tcpPublishOptions }),
    ...(kafkaOptions === undefined ? {} : { kafkaOptions }),
    ...(grpcOptions === undefined ? {} : { grpcOptions }),
  };
});

export function resolveViewServerRuntimeOptions<
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
>(
  options: ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
): Effect.Effect<
  ResolvedViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  Config.ConfigError | ViewServerGrpcIngressError | ViewServerKafkaIngressError
>;
export function resolveViewServerRuntimeOptions<
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
): Effect.Effect<
  ResolvedViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  Config.ConfigError | ViewServerGrpcIngressError | ViewServerKafkaIngressError
>;
export function resolveViewServerRuntimeOptions<
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  options: ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
): Effect.Effect<
  ResolvedViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  Config.ConfigError | ViewServerGrpcIngressError | ViewServerKafkaIngressError
>;
export function resolveViewServerRuntimeOptions<
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
>(
  configOrOptions:
    | ViewServerConfig<Topics, Regions, GrpcClients>
    | ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  maybeOptions?: ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
): Effect.Effect<
  ResolvedViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
  Config.ConfigError | ViewServerGrpcIngressError | ViewServerKafkaIngressError
> {
  if ("defineRuntimeOptions" in configOrOptions) {
    return resolveViewServerRuntimeOptionsWithConfig(configOrOptions, maybeOptions ?? {});
  }
  return resolveViewServerRuntimeOptionsWithConfig(undefined, configOrOptions);
}
