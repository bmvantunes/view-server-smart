import type {
  KafkaStartFromHealth,
  TopicRuntimeHealth,
  TransportHealth,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
} from "@view-server/config";
import { Schema } from "effect";

const StringOrNull = Schema.NullOr(Schema.String);
const NumberOrNull = Schema.NullOr(Schema.Number);
const BigIntString = Schema.BigIntFromString;

const TopicRuntimeHealthSchema: Schema.Codec<TopicRuntimeHealth> = Schema.Struct({
  status: Schema.Literals(["ready", "degraded", "starting"]),
  rowCount: Schema.Number,
  liveRowCount: Schema.Number,
  deletedRowCount: Schema.Number,
  version: Schema.Number,
  lastMutationAt: NumberOrNull,
  mutationsPerSecond: Schema.Number,
  rowsPerSecond: Schema.Number,
  pendingMutationBatches: Schema.Number,
  activeFallbackGroupedViews: Schema.Number,
  activeIncrementalGroupedViews: Schema.Number,
  activeViews: Schema.Number,
  groupedFullEvaluationCount: Schema.Number,
  groupedPatchedEvaluationCount: Schema.Number,
  activeSubscriptions: Schema.Number,
  queuedEvents: Schema.Number,
  maxQueueDepth: Schema.Number,
  backpressureEvents: Schema.Number,
  memoryBytes: Schema.Number,
  tombstoneCount: Schema.Number,
  compactionPending: Schema.Boolean,
});

const TransportHealthSchema: Schema.Codec<TransportHealth> = Schema.Struct({
  activeClients: Schema.Number,
  activeStreams: Schema.Number,
  activeSubscriptions: Schema.Number,
  messagesPerSecond: Schema.Number,
  bytesPerSecond: Schema.Number,
  queuedMessages: Schema.Number,
  queuedBytes: Schema.Number,
  droppedClients: Schema.Number,
  backpressureEvents: Schema.Number,
  reconnects: Schema.Number,
  lastError: StringOrNull,
});

const KafkaStartFromHealthSchema: Schema.Codec<KafkaStartFromHealth> = Schema.Union([
  Schema.Struct({
    consumerGroupId: Schema.String,
    mode: Schema.Literal("earliest"),
    fallbackMode: Schema.Literal("earliest"),
  }),
  Schema.Struct({
    consumerGroupId: Schema.String,
    mode: Schema.Literal("latest"),
    fallbackMode: Schema.Literal("latest"),
  }),
  Schema.Struct({
    consumerGroupId: Schema.String,
    mode: Schema.Literal("committed"),
    fallbackMode: Schema.Literals(["earliest", "latest", "fail"]),
  }),
]);

export const ViewServerHealthSchema = Schema.Struct({
  status: Schema.Literals(["ready", "degraded", "starting", "stopping"]),
  version: Schema.Number,
  uptimeMs: Schema.Number,
  engine: Schema.Struct({
    topics: Schema.Record(Schema.String, TopicRuntimeHealthSchema),
  }),
  kafka: Schema.optionalKey(
    Schema.Struct({
      startFrom: KafkaStartFromHealthSchema,
      regions: Schema.Record(
        Schema.String,
        Schema.Struct({
          status: Schema.Literals(["connected", "disconnected", "degraded", "starting"]),
          brokers: Schema.String,
          lastConnectedAt: NumberOrNull,
          lastError: StringOrNull,
        }),
      ),
      topics: Schema.Record(
        Schema.String,
        Schema.Struct({
          status: Schema.Literals(["ready", "degraded", "starting", "stalled"]),
          sourceTopic: Schema.String,
          viewServerTopic: Schema.String,
          regions: Schema.Record(
            Schema.String,
            Schema.Struct({
              connected: Schema.Boolean,
              assignedPartitions: Schema.Number,
              messagesPerSecond: Schema.Number,
              bytesPerSecond: Schema.Number,
              decodedMessagesPerSecond: Schema.Number,
              decodeFailuresPerSecond: Schema.Number,
              mappingFailuresPerSecond: Schema.Number,
              publishFailuresPerSecond: Schema.Number,
              commitFailuresPerSecond: Schema.Number,
              processingFailuresPerSecond: Schema.Number,
              lastMessageAt: NumberOrNull,
              lastCommitAt: NumberOrNull,
              consumerLagMessages: Schema.NullOr(BigIntString),
              lagSampledAt: NumberOrNull,
              committedOffset: StringOrNull,
              lastError: StringOrNull,
            }),
          ),
        }),
      ),
    }),
  ),
  transport: TransportHealthSchema,
});

export type ViewServerWireHealth = typeof ViewServerHealthSchema.Type;

export const ViewServerHealthSummaryRowSchema: Schema.Codec<
  ViewServerHealthSummaryRow,
  unknown,
  never,
  never
> = Schema.Struct({
  id: Schema.Literal("summary"),
  status: Schema.Literals([
    "ready",
    "degraded",
    "starting",
    "stopping",
    "connecting",
    "disconnected",
  ]),
  runtimeStatus: Schema.Literals(["ready", "degraded", "starting", "stopping"]),
  connectionStatus: Schema.Literals(["connecting", "connected", "disconnected"]),
  unhealthyTopics: Schema.Array(Schema.String),
  updatedAtNanos: BigIntString,
  maxKafkaLag: BigIntString,
});

export const ViewServerHealthTopicRowSchema: Schema.Codec<
  ViewServerHealthTopicRow,
  unknown,
  never,
  never
> = Schema.Struct({
  id: Schema.String,
  status: Schema.Literals(["ready", "degraded", "starting", "stopping"]),
  rowCount: Schema.Number,
  liveRowCount: Schema.Number,
  deletedRowCount: Schema.Number,
  version: Schema.Number,
  lastMutationAt: NumberOrNull,
  mutationsPerSecond: Schema.Number,
  rowsPerSecond: Schema.Number,
  pendingMutationBatches: Schema.Number,
  activeFallbackGroupedViews: Schema.Number,
  activeIncrementalGroupedViews: Schema.Number,
  activeViews: Schema.Number,
  groupedFullEvaluationCount: Schema.Number,
  groupedPatchedEvaluationCount: Schema.Number,
  activeSubscriptions: Schema.Number,
  queuedEvents: Schema.Number,
  maxQueueDepth: Schema.Number,
  backpressureEvents: Schema.Number,
  memoryBytes: Schema.Number,
  tombstoneCount: Schema.Number,
  compactionPending: Schema.Boolean,
  kafkaLag: BigIntString,
  updatedAtNanos: BigIntString,
});
