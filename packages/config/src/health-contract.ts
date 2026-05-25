export type RuntimeStatus = "ready" | "degraded" | "starting" | "stopping";
export type TopicHealthStatus = "ready" | "degraded" | "starting";
export type KafkaRegionStatus = "connected" | "disconnected" | "degraded" | "starting";
export type KafkaTopicStatus = "ready" | "degraded" | "starting" | "stalled";

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
  readonly consumerLagMessages: number | null;
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
