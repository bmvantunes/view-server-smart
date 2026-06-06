import type { LiveQueryResult } from "./topic-contract";

export type RuntimeStatus = "ready" | "degraded" | "starting" | "stopping";
export type TopicHealthStatus = "ready" | "degraded" | "starting";
export type KafkaRegionStatus = "connected" | "disconnected" | "degraded" | "starting";
export type KafkaTopicStatus = "ready" | "degraded" | "starting" | "stalled";
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
  readonly activeViews: number;
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
  readonly lastMessageAt: number | null;
  readonly lastCommitAt: number | null;
  readonly consumerLagMessages: bigint | null;
  readonly consumerLagMs: number | null;
  readonly lagSampledAt: number | null;
  readonly highWatermarkOffset: string | null;
  readonly committedOffset: string | null;
  readonly lastError: string | null;
};

export type KafkaTopicHealth = {
  readonly status: KafkaTopicStatus;
  readonly sourceTopic: string;
  readonly viewServerTopic: string;
  readonly regions: Record<string, KafkaTopicRegionHealth>;
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
    readonly regions: Record<string, KafkaRegionHealth>;
    readonly topics: Record<string, KafkaTopicHealth>;
  };
  readonly transport: TransportHealth;
};

export type ViewServerHealthSummary<Topics extends object = Record<string, object>> = {
  readonly status: ViewServerHealthStatus;
  readonly runtimeStatus: RuntimeStatus;
  readonly connectionStatus: ViewServerHealthConnectionStatus;
  readonly unhealthyTopics: ReadonlyArray<Extract<keyof Topics, string>>;
  readonly updatedAtNanos: bigint;
  readonly maxKafkaLag: bigint;
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
  readonly activeViews: number;
  readonly activeSubscriptions: number;
  readonly queuedEvents: number;
  readonly maxQueueDepth: number;
  readonly backpressureEvents: number;
  readonly memoryBytes: number;
  readonly tombstoneCount: number;
  readonly compactionPending: boolean;
  readonly kafkaLag: bigint;
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

const kafkaRegionLag = (region: KafkaTopicRegionHealth): bigint =>
  region.consumerLagMessages === null ? 0n : region.consumerLagMessages;

const maxKafkaLagForViewTopic = (
  health: Pick<ViewServerHealth, "kafka">,
  viewServerTopic: string,
): bigint => {
  let maxLag = 0n;
  for (const kafkaTopic of Object.values(health.kafka?.topics ?? {})) {
    if (kafkaTopic.viewServerTopic === viewServerTopic) {
      for (const region of Object.values(kafkaTopic.regions)) {
        const lag = kafkaRegionLag(region);
        if (lag > maxLag) {
          maxLag = lag;
        }
      }
    }
  }
  return maxLag;
};

export const viewServerHealthSummaryFromHealth = <Topics extends object>(
  health: ViewServerHealth<Topics>,
  updatedAtNanos: bigint,
): ViewServerHealthSummary<Topics> => {
  const topicHealthByName: Readonly<Record<string, TopicRuntimeHealth>> = health.engine.topics;
  const unhealthyTopics = Object.entries(topicHealthByName)
    .filter(([, topic]) => topicIsUnhealthy(topic))
    .map(([topic]) => topic);
  let maxKafkaLag = 0n;
  for (const topic of Object.keys(health.engine.topics)) {
    const lag = maxKafkaLagForViewTopic(health, topic);
    if (lag > maxKafkaLag) {
      maxKafkaLag = lag;
    }
  }
  return {
    status: health.status,
    runtimeStatus: health.status,
    connectionStatus: "connected",
    unhealthyTopics: typedTopicNames<Topics>(unhealthyTopics),
    updatedAtNanos,
    maxKafkaLag,
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
    const status: TopicHealthStatus | "stopping" =
      health.status === "stopping" ? "stopping" : topic.status;
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
      activeViews: topic.activeViews,
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
