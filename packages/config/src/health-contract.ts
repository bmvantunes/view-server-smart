import type { LiveQueryResult } from "./topic-contract";

export type RuntimeStatus = "ready" | "degraded" | "starting" | "stopping";
export type TopicHealthStatus = "ready" | "degraded" | "starting";
export type KafkaRegionStatus = "connected" | "disconnected" | "degraded" | "starting";
export type KafkaTopicStatus = "ready" | "degraded" | "starting" | "stalled";
export type GrpcClientStatus = "connected" | "disconnected" | "degraded" | "starting";
export type GrpcFeedStatus = "starting" | "ready" | "degraded" | "stopping";
export type GrpcFeedLifecycle = "materialized" | "leased";
export type ViewServerHealthConnectionStatus = "connecting" | "connected" | "disconnected";
export type ViewServerHealthStatus =
  | RuntimeStatus
  | Exclude<ViewServerHealthConnectionStatus, "connected">;

export const VIEW_SERVER_HEALTH_SUMMARY_TOPIC = "__view_server_health_summary";
export const VIEW_SERVER_HEALTH_TOPIC = "__view_server_health";

export type ViewServerSystemTopicName =
  | typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC
  | typeof VIEW_SERVER_HEALTH_TOPIC;

export const viewServerReservedTopicNames: ReadonlyArray<ViewServerSystemTopicName> = [
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
];

export const viewServerTopicNameIsReserved = (topic: string): topic is ViewServerSystemTopicName =>
  viewServerReservedTopicNames.some((reservedTopic) => reservedTopic === topic);

export type TopicRuntimeHealth = {
  readonly status: TopicHealthStatus;
  readonly rowCount: number;
  readonly liveRowCount: number;
  readonly deletedRowCount: number;
  readonly version: number;
  readonly lastMutationAt: number | null;
  readonly mutationsPerSecond: number;
  readonly rowsPerSecond: number;
  readonly pendingMutationBatches: number;
  readonly activeFallbackGroupedViews: number;
  readonly activeIncrementalGroupedViews: number;
  readonly activeViews: number;
  readonly groupedFullEvaluationCount: number;
  readonly groupedPatchedEvaluationCount: number;
  readonly activeSubscriptions: number;
  readonly queuedEvents: number;
  readonly maxQueueDepth: number;
  readonly backpressureEvents: number;
  readonly memoryBytes: number;
  readonly tombstoneCount: number;
  readonly compactionPending: boolean;
};

export type KafkaRegionHealth = {
  readonly status: KafkaRegionStatus;
  readonly brokers: string;
  readonly lastConnectedAt: number | null;
  readonly lastError: string | null;
};

export type KafkaTopicRegionHealth = {
  readonly connected: boolean;
  readonly assignedPartitions: number;
  readonly messagesPerSecond: number;
  readonly bytesPerSecond: number;
  readonly decodedMessagesPerSecond: number;
  readonly decodeFailuresPerSecond: number;
  readonly mappingFailuresPerSecond: number;
  readonly publishFailuresPerSecond: number;
  readonly commitFailuresPerSecond: number;
  readonly processingFailuresPerSecond: number;
  readonly lastMessageAt: number | null;
  readonly lastCommitAt: number | null;
  readonly consumerLagMessages: bigint | null;
  readonly lagSampledAt: number | null;
  readonly committedOffset: string | null;
  readonly lastError: string | null;
};

export type KafkaTopicHealth = {
  readonly status: KafkaTopicStatus;
  readonly sourceTopic: string;
  readonly viewServerTopic: string;
  readonly regions: Record<string, KafkaTopicRegionHealth>;
};

export type KafkaStartFromHealth =
  | {
      readonly consumerGroupId: string;
      readonly mode: "earliest";
      readonly fallbackMode: "earliest";
    }
  | {
      readonly consumerGroupId: string;
      readonly mode: "latest";
      readonly fallbackMode: "latest";
    }
  | {
      readonly consumerGroupId: string;
      readonly mode: "committed";
      readonly fallbackMode: "earliest" | "latest" | "fail";
    };

export type GrpcClientHealth = {
  readonly status: GrpcClientStatus;
  readonly baseUrl: string;
  readonly activeFeeds: number;
  readonly lastConnectedAt: number | null;
  readonly lastError: string | null;
};

export type GrpcFeedHealth<Topic extends string = string> = {
  readonly status: GrpcFeedStatus;
  readonly lifecycle: GrpcFeedLifecycle;
  readonly feedName: string;
  readonly feedKey: string;
  readonly topic: Topic;
  readonly subscriberCount: number;
  readonly rowCount: number;
  readonly messagesPerSecond: number;
  readonly rowsPerSecond: number;
  readonly decodeFailuresPerSecond: number;
  readonly mappingFailuresPerSecond: number;
  readonly publishFailuresPerSecond: number;
  readonly reconnects: number;
  readonly lastMessageAt: number | null;
  readonly lastError: string | null;
};

export type GrpcTopicFeedsHealth<Topic extends string = string> = {
  readonly materialized: Record<string, GrpcFeedHealth<Topic>>;
  readonly leased: Record<string, GrpcFeedHealth<Topic>>;
};

export type GrpcRuntimeHealth<Topics extends object = Record<string, object>> = {
  readonly clients: Record<string, GrpcClientHealth>;
  readonly feeds: {
    readonly [Topic in Extract<keyof Topics, string>]?: GrpcTopicFeedsHealth<Topic>;
  } & Record<string, GrpcTopicFeedsHealth<string>>;
};

export type TransportHealth = {
  readonly activeClients: number;
  readonly activeStreams: number;
  readonly activeSubscriptions: number;
  readonly messagesPerSecond: number;
  readonly bytesPerSecond: number;
  readonly queuedMessages: number;
  readonly queuedBytes: number;
  readonly droppedClients: number;
  readonly backpressureEvents: number;
  readonly reconnects: number;
  readonly lastError: string | null;
};

export type ViewServerHealth<Topics extends object = Record<string, object>> = {
  readonly status: RuntimeStatus;
  readonly version: number;
  readonly uptimeMs: number;
  readonly engine: {
    readonly topics: {
      readonly [Topic in Extract<keyof Topics, string>]: TopicRuntimeHealth;
    };
  };
  readonly kafka?: {
    readonly startFrom: KafkaStartFromHealth;
    readonly regions: Record<string, KafkaRegionHealth>;
    readonly topics: Record<string, KafkaTopicHealth>;
  };
  readonly grpc?: GrpcRuntimeHealth<Topics>;
  readonly transport: TransportHealth;
};

export type ViewServerHealthSummary<Topics extends object = Record<string, object>> = {
  readonly status: ViewServerHealthStatus;
  readonly runtimeStatus: RuntimeStatus;
  readonly connectionStatus: ViewServerHealthConnectionStatus;
  readonly unhealthyTopics: ReadonlyArray<Extract<keyof Topics, string>>;
  readonly updatedAtNanos: bigint;
  readonly maxKafkaLag: bigint | null;
};

export type ViewServerHealthSummaryRow<Topics extends object = Record<string, object>> =
  ViewServerHealthSummary<Topics> & {
    readonly id: "summary";
  };

export type ViewServerHealthTopicRow<Topic extends string = string> = {
  readonly id: Topic;
  readonly status: TopicHealthStatus | "stopping";
  readonly rowCount: number;
  readonly liveRowCount: number;
  readonly deletedRowCount: number;
  readonly version: number;
  readonly lastMutationAt: number | null;
  readonly mutationsPerSecond: number;
  readonly rowsPerSecond: number;
  readonly pendingMutationBatches: number;
  readonly activeFallbackGroupedViews: number;
  readonly activeIncrementalGroupedViews: number;
  readonly activeViews: number;
  readonly groupedFullEvaluationCount: number;
  readonly groupedPatchedEvaluationCount: number;
  readonly activeSubscriptions: number;
  readonly queuedEvents: number;
  readonly maxQueueDepth: number;
  readonly backpressureEvents: number;
  readonly memoryBytes: number;
  readonly tombstoneCount: number;
  readonly compactionPending: boolean;
  readonly kafkaLag: bigint | null;
  readonly updatedAtNanos: bigint;
};

export type ViewServerHealthDetails<Topic extends string = string> = Omit<
  LiveQueryResult<ViewServerHealthTopicRow<Topic>>,
  "status"
> & {
  readonly runtimeStatus: RuntimeStatus;
  readonly connectionStatus: ViewServerHealthConnectionStatus;
  readonly status: ViewServerHealthStatus;
};

function typedTopicNames<Topics extends object>(
  topics: ReadonlyArray<string>,
): ReadonlyArray<Extract<keyof Topics, string>>;
function typedTopicNames(topics: ReadonlyArray<string>): ReadonlyArray<string> {
  return topics;
}

function typedHealthTopicRows<Topic extends string>(
  rows: ReadonlyArray<ViewServerHealthTopicRow<string>>,
): ReadonlyArray<ViewServerHealthTopicRow<Topic>>;
function typedHealthTopicRows(
  rows: ReadonlyArray<ViewServerHealthTopicRow<string>>,
): ReadonlyArray<ViewServerHealthTopicRow<string>> {
  return rows;
}

const topicIsUnhealthy = (topic: TopicRuntimeHealth): boolean => topic.status !== "ready";

const kafkaTopicStatusToTopicStatus = (status: KafkaTopicStatus): TopicHealthStatus => {
  if (status === "ready") {
    return "ready";
  }
  if (status === "starting") {
    return "starting";
  }
  return "degraded";
};

const mergeTopicHealthStatus = (
  engineStatus: TopicHealthStatus,
  kafkaStatus: TopicHealthStatus | undefined,
): TopicHealthStatus => {
  if (engineStatus === "degraded" || kafkaStatus === "degraded") {
    return "degraded";
  }
  if (engineStatus === "starting" || kafkaStatus === "starting") {
    return "starting";
  }
  return "ready";
};

const kafkaTopicStatusForViewTopic = (
  health: Pick<ViewServerHealth, "kafka">,
  viewServerTopic: string,
): TopicHealthStatus | undefined => {
  let status: TopicHealthStatus | undefined = undefined;
  for (const kafkaTopic of Object.values(health.kafka?.topics ?? {})) {
    if (kafkaTopic.viewServerTopic === viewServerTopic) {
      status = mergeTopicHealthStatus(
        status ?? "ready",
        kafkaTopicStatusToTopicStatus(kafkaTopic.status),
      );
    }
  }
  return status;
};

const grpcFeedStatusToTopicStatus = (status: GrpcFeedStatus): TopicHealthStatus => {
  if (status === "ready") {
    return "ready";
  }
  if (status === "starting") {
    return "starting";
  }
  return "degraded";
};

const grpcTopicStatusForViewTopic = <Topics extends object>(
  health: Pick<ViewServerHealth<Topics>, "grpc">,
  viewServerTopic: string,
): TopicHealthStatus | undefined => {
  const feeds = health.grpc?.feeds[viewServerTopic];
  if (feeds === undefined) {
    return undefined;
  }
  let status: TopicHealthStatus = "ready";
  for (const feed of Object.values(feeds.materialized)) {
    status = mergeTopicHealthStatus(status, grpcFeedStatusToTopicStatus(feed.status));
  }
  for (const feed of Object.values(feeds.leased)) {
    status = mergeTopicHealthStatus(status, grpcFeedStatusToTopicStatus(feed.status));
  }
  return status;
};

const grpcTopicIsUnhealthy = <Topics extends object>(
  health: Pick<ViewServerHealth<Topics>, "grpc">,
  viewServerTopic: string,
): boolean => {
  const status = grpcTopicStatusForViewTopic(health, viewServerTopic);
  return status !== undefined && status !== "ready";
};

const kafkaTopicIsUnhealthy = (
  health: Pick<ViewServerHealth, "kafka">,
  viewServerTopic: string,
): boolean => {
  const status = kafkaTopicStatusForViewTopic(health, viewServerTopic);
  return status !== undefined && status !== "ready";
};

const kafkaRegionLag = (region: KafkaTopicRegionHealth): bigint | null =>
  region.consumerLagMessages;

type KafkaLagState = {
  readonly hasKafkaSource: boolean;
  readonly hasUnknownLag: boolean;
  readonly maxLag: bigint | null;
};

const emptyKafkaLagState = (): KafkaLagState => ({
  hasKafkaSource: false,
  hasUnknownLag: false,
  maxLag: null,
});

const addKafkaRegionLag = (state: KafkaLagState, region: KafkaTopicRegionHealth): KafkaLagState => {
  const lag = kafkaRegionLag(region);
  if (lag === null) {
    return {
      hasKafkaSource: true,
      hasUnknownLag: true,
      maxLag: state.maxLag,
    };
  }
  return {
    hasKafkaSource: true,
    hasUnknownLag: state.hasUnknownLag,
    maxLag: state.maxLag === null || lag > state.maxLag ? lag : state.maxLag,
  };
};

const mergeKafkaLagState = (left: KafkaLagState, right: KafkaLagState): KafkaLagState => ({
  hasKafkaSource: left.hasKafkaSource || right.hasKafkaSource,
  hasUnknownLag: left.hasUnknownLag || right.hasUnknownLag,
  maxLag:
    left.maxLag === null
      ? right.maxLag
      : right.maxLag === null || left.maxLag > right.maxLag
        ? left.maxLag
        : right.maxLag,
});

const kafkaLagStateToPublicLag = (state: KafkaLagState): bigint | null => {
  if (!state.hasKafkaSource || state.hasUnknownLag) {
    return null;
  }
  return state.maxLag;
};

const kafkaLagStateForViewTopic = (
  health: Pick<ViewServerHealth, "kafka">,
  viewServerTopic: string,
): KafkaLagState => {
  let state = emptyKafkaLagState();
  for (const kafkaTopic of Object.values(health.kafka?.topics ?? {})) {
    if (kafkaTopic.viewServerTopic === viewServerTopic) {
      for (const region of Object.values(kafkaTopic.regions)) {
        state = addKafkaRegionLag(state, region);
      }
    }
  }
  return state;
};

const maxKafkaLagForViewTopic = (
  health: Pick<ViewServerHealth, "kafka">,
  viewServerTopic: string,
): bigint | null => kafkaLagStateToPublicLag(kafkaLagStateForViewTopic(health, viewServerTopic));

const maxKafkaLagForHealth = (
  health: Pick<ViewServerHealth, "engine" | "kafka">,
): bigint | null => {
  let state = emptyKafkaLagState();
  for (const topic of Object.keys(health.engine.topics)) {
    state = mergeKafkaLagState(state, kafkaLagStateForViewTopic(health, topic));
  }
  return kafkaLagStateToPublicLag(state);
};

export const viewServerHealthSummaryFromHealth = <Topics extends object>(
  health: ViewServerHealth<Topics>,
  updatedAtNanos: bigint,
): ViewServerHealthSummary<Topics> => {
  const topicHealthByName: Readonly<Record<string, TopicRuntimeHealth>> = health.engine.topics;
  const unhealthyTopics = Object.entries(topicHealthByName)
    .filter(
      ([topicName, topic]) =>
        topicIsUnhealthy(topic) ||
        kafkaTopicIsUnhealthy(health, topicName) ||
        grpcTopicIsUnhealthy(health, topicName),
    )
    .map(([topic]) => topic);
  return {
    status: health.status,
    runtimeStatus: health.status,
    connectionStatus: "connected",
    unhealthyTopics: typedTopicNames<Topics>(unhealthyTopics),
    updatedAtNanos,
    maxKafkaLag: maxKafkaLagForHealth(health),
  };
};

export const viewServerHealthSummaryRowFromHealth = <Topics extends object>(
  health: ViewServerHealth<Topics>,
  updatedAtNanos: bigint,
): ViewServerHealthSummaryRow<Topics> => ({
  id: "summary",
  ...viewServerHealthSummaryFromHealth(health, updatedAtNanos),
});

export const viewServerHealthTopicRowsFromHealth = <Topics extends object>(
  health: ViewServerHealth<Topics>,
  updatedAtNanos: bigint,
): ReadonlyArray<ViewServerHealthTopicRow<Extract<keyof Topics, string>>> => {
  const topicHealthByName: Readonly<Record<string, TopicRuntimeHealth>> = health.engine.topics;
  const rows: Array<ViewServerHealthTopicRow<string>> = [];
  for (const [id, topic] of Object.entries(topicHealthByName)) {
    const effectiveStatus = mergeTopicHealthStatus(
      mergeTopicHealthStatus(topic.status, kafkaTopicStatusForViewTopic(health, id)),
      grpcTopicStatusForViewTopic(health, id),
    );
    const status: TopicHealthStatus | "stopping" =
      health.status === "stopping" ? "stopping" : effectiveStatus;
    rows.push({
      id,
      status,
      rowCount: topic.rowCount,
      liveRowCount: topic.liveRowCount,
      deletedRowCount: topic.deletedRowCount,
      version: topic.version,
      lastMutationAt: topic.lastMutationAt,
      mutationsPerSecond: topic.mutationsPerSecond,
      rowsPerSecond: topic.rowsPerSecond,
      pendingMutationBatches: topic.pendingMutationBatches,
      activeFallbackGroupedViews: topic.activeFallbackGroupedViews,
      activeIncrementalGroupedViews: topic.activeIncrementalGroupedViews,
      activeViews: topic.activeViews,
      groupedFullEvaluationCount: topic.groupedFullEvaluationCount,
      groupedPatchedEvaluationCount: topic.groupedPatchedEvaluationCount,
      activeSubscriptions: topic.activeSubscriptions,
      queuedEvents: topic.queuedEvents,
      maxQueueDepth: topic.maxQueueDepth,
      backpressureEvents: topic.backpressureEvents,
      memoryBytes: topic.memoryBytes,
      tombstoneCount: topic.tombstoneCount,
      compactionPending: topic.compactionPending,
      kafkaLag: maxKafkaLagForViewTopic(health, id),
      updatedAtNanos,
    });
  }
  return typedHealthTopicRows<Extract<keyof Topics, string>>(rows);
};
