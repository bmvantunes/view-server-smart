import type { TopicDefinitions, ViewServerConfig, ViewServerHealth } from "@view-server/config";
import { viewServerDecodeHealth } from "@view-server/protocol";
import { Effect } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { validateViewServerHttpRequest, viewServerAuthErrorResponse } from "./auth";
import type { ViewServerWebSocketServerInput } from "./server-types";

const metricContentType = "text/plain; version=0.0.4; charset=utf-8";

const escapeLabelValue = (value: string): string =>
  value.replaceAll("\\", "\\\\").replaceAll("\n", "\\n").replaceAll('"', '\\"');

const labelsText = (labels: Readonly<Record<string, string>>): string => {
  const entries = Object.entries(labels);
  if (entries.length === 0) {
    return "";
  }
  return `{${entries.map(([key, value]) => `${key}="${escapeLabelValue(value)}"`).join(",")}}`;
};

const metricLine = (
  name: string,
  value: number | bigint,
  labels: Readonly<Record<string, string>> = {},
): string => `${name}${labelsText(labels)} ${value.toString()}`;

const nullableNumberMetricLine = (
  name: string,
  value: number | bigint | null,
  labels: Readonly<Record<string, string>>,
): string | undefined => (value === null ? undefined : metricLine(name, value, labels));

const booleanMetric = (value: boolean): number => (value ? 1 : 0);

const statusMetric = (status: string, expected: string): number =>
  booleanMetric(status === expected);

const compactLines = (lines: ReadonlyArray<string | undefined>): string =>
  `${lines.filter((line) => line !== undefined).join("\n")}\n`;

type GrpcFeedMetricTotals = {
  readonly rowCount: number;
  readonly subscriberCount: number;
  readonly messagesPerSecond: number;
  readonly rowsPerSecond: number;
  readonly decodeFailuresPerSecond: number;
  readonly mappingFailuresPerSecond: number;
  readonly publishFailuresPerSecond: number;
  readonly reconnects: number;
};

const emptyGrpcFeedMetricTotals = (): GrpcFeedMetricTotals => ({
  rowCount: 0,
  subscriberCount: 0,
  messagesPerSecond: 0,
  rowsPerSecond: 0,
  decodeFailuresPerSecond: 0,
  mappingFailuresPerSecond: 0,
  publishFailuresPerSecond: 0,
  reconnects: 0,
});

const addGrpcFeedMetricTotals = (
  current: GrpcFeedMetricTotals,
  feed: GrpcFeedMetricTotals,
): GrpcFeedMetricTotals => ({
  rowCount: current.rowCount + feed.rowCount,
  subscriberCount: current.subscriberCount + feed.subscriberCount,
  messagesPerSecond: current.messagesPerSecond + feed.messagesPerSecond,
  rowsPerSecond: current.rowsPerSecond + feed.rowsPerSecond,
  decodeFailuresPerSecond: current.decodeFailuresPerSecond + feed.decodeFailuresPerSecond,
  mappingFailuresPerSecond: current.mappingFailuresPerSecond + feed.mappingFailuresPerSecond,
  publishFailuresPerSecond: current.publishFailuresPerSecond + feed.publishFailuresPerSecond,
  reconnects: current.reconnects + feed.reconnects,
});

const pushGrpcFeedMetrics = (
  lines: Array<string | undefined>,
  labels: Readonly<Record<string, string>>,
  feed: GrpcFeedMetricTotals,
): void => {
  lines.push(
    metricLine("view_server_grpc_feed_rows", feed.rowCount, labels),
    metricLine("view_server_grpc_feed_subscribers", feed.subscriberCount, labels),
    metricLine("view_server_grpc_feed_messages_per_second", feed.messagesPerSecond, labels),
    metricLine("view_server_grpc_feed_rows_per_second", feed.rowsPerSecond, labels),
    metricLine(
      "view_server_grpc_feed_decode_failures_per_second",
      feed.decodeFailuresPerSecond,
      labels,
    ),
    metricLine(
      "view_server_grpc_feed_mapping_failures_per_second",
      feed.mappingFailuresPerSecond,
      labels,
    ),
    metricLine(
      "view_server_grpc_feed_publish_failures_per_second",
      feed.publishFailuresPerSecond,
      labels,
    ),
    metricLine("view_server_grpc_feed_reconnects", feed.reconnects, labels),
  );
};

const viewServerHealthMetrics = <const Topics extends TopicDefinitions>(
  health: ViewServerHealth<Topics>,
): string => {
  const lines: Array<string | undefined> = [
    "# HELP view_server_runtime_status Runtime status as one-hot labels.",
    "# TYPE view_server_runtime_status gauge",
    metricLine("view_server_runtime_status", statusMetric(health.status, "ready"), {
      status: "ready",
    }),
    metricLine("view_server_runtime_status", statusMetric(health.status, "starting"), {
      status: "starting",
    }),
    metricLine("view_server_runtime_status", statusMetric(health.status, "degraded"), {
      status: "degraded",
    }),
    metricLine("view_server_runtime_status", statusMetric(health.status, "stopping"), {
      status: "stopping",
    }),
    "# HELP view_server_runtime_version Runtime health version.",
    "# TYPE view_server_runtime_version gauge",
    metricLine("view_server_runtime_version", health.version),
    "# HELP view_server_runtime_uptime_millis Runtime uptime in milliseconds.",
    "# TYPE view_server_runtime_uptime_millis gauge",
    metricLine("view_server_runtime_uptime_millis", health.uptimeMs),
    "# HELP view_server_transport_active_clients Active transport clients.",
    "# TYPE view_server_transport_active_clients gauge",
    metricLine("view_server_transport_active_clients", health.transport.activeClients),
    "# HELP view_server_transport_active_streams Active transport streams.",
    "# TYPE view_server_transport_active_streams gauge",
    metricLine("view_server_transport_active_streams", health.transport.activeStreams),
    "# HELP view_server_transport_active_subscriptions Active transport subscriptions.",
    "# TYPE view_server_transport_active_subscriptions gauge",
    metricLine("view_server_transport_active_subscriptions", health.transport.activeSubscriptions),
    "# HELP view_server_transport_queued_messages Queued transport messages.",
    "# TYPE view_server_transport_queued_messages gauge",
    metricLine("view_server_transport_queued_messages", health.transport.queuedMessages),
    "# HELP view_server_transport_queued_bytes Queued transport bytes.",
    "# TYPE view_server_transport_queued_bytes gauge",
    metricLine("view_server_transport_queued_bytes", health.transport.queuedBytes),
    "# HELP view_server_transport_messages_per_second Transport messages per second.",
    "# TYPE view_server_transport_messages_per_second gauge",
    metricLine("view_server_transport_messages_per_second", health.transport.messagesPerSecond),
    "# HELP view_server_transport_bytes_per_second Transport bytes per second.",
    "# TYPE view_server_transport_bytes_per_second gauge",
    metricLine("view_server_transport_bytes_per_second", health.transport.bytesPerSecond),
    "# HELP view_server_transport_dropped_clients Dropped transport clients.",
    "# TYPE view_server_transport_dropped_clients counter",
    metricLine("view_server_transport_dropped_clients", health.transport.droppedClients),
    "# HELP view_server_transport_backpressure_events Transport backpressure events.",
    "# TYPE view_server_transport_backpressure_events gauge",
    metricLine("view_server_transport_backpressure_events", health.transport.backpressureEvents),
    "# HELP view_server_transport_reconnects Transport reconnects.",
    "# TYPE view_server_transport_reconnects counter",
    metricLine("view_server_transport_reconnects", health.transport.reconnects),
    "# HELP view_server_engine_topic_rows Engine topic row counts.",
    "# TYPE view_server_engine_topic_rows gauge",
    "# HELP view_server_engine_topic_version Engine topic version.",
    "# TYPE view_server_engine_topic_version gauge",
    "# HELP view_server_engine_topic_pending_mutation_batches Pending mutation batches by topic.",
    "# TYPE view_server_engine_topic_pending_mutation_batches gauge",
    "# HELP view_server_engine_topic_active_views Active raw views by topic.",
    "# TYPE view_server_engine_topic_active_views gauge",
    "# HELP view_server_engine_topic_active_grouped_views Active grouped views by topic and mode.",
    "# TYPE view_server_engine_topic_active_grouped_views gauge",
    "# HELP view_server_engine_topic_grouped_evaluations Active grouped evaluation count by topic and mode.",
    "# TYPE view_server_engine_topic_grouped_evaluations gauge",
    "# HELP view_server_engine_topic_active_subscriptions Active subscriptions by topic.",
    "# TYPE view_server_engine_topic_active_subscriptions gauge",
    "# HELP view_server_engine_topic_queued_events Queued events by topic.",
    "# TYPE view_server_engine_topic_queued_events gauge",
    "# HELP view_server_engine_topic_max_queue_depth Maximum queue depth by topic.",
    "# TYPE view_server_engine_topic_max_queue_depth gauge",
    "# HELP view_server_engine_topic_backpressure_events Backpressure events by topic.",
    "# TYPE view_server_engine_topic_backpressure_events gauge",
    "# HELP view_server_engine_topic_memory_bytes Estimated memory bytes by topic.",
    "# TYPE view_server_engine_topic_memory_bytes gauge",
    "# HELP view_server_engine_topic_tombstones Tombstone count by topic.",
    "# TYPE view_server_engine_topic_tombstones gauge",
    "# HELP view_server_engine_topic_compaction_pending Topic compaction pending flag.",
    "# TYPE view_server_engine_topic_compaction_pending gauge",
    "# HELP view_server_engine_topic_mutations_per_second Mutations per second by topic.",
    "# TYPE view_server_engine_topic_mutations_per_second gauge",
    "# HELP view_server_engine_topic_rows_per_second Rows per second by topic.",
    "# TYPE view_server_engine_topic_rows_per_second gauge",
    "# HELP view_server_kafka_region_connected Kafka region connection state.",
    "# TYPE view_server_kafka_region_connected gauge",
    "# HELP view_server_kafka_messages_per_second Kafka messages per second.",
    "# TYPE view_server_kafka_messages_per_second gauge",
    "# HELP view_server_kafka_bytes_per_second Kafka bytes per second.",
    "# TYPE view_server_kafka_bytes_per_second gauge",
    "# HELP view_server_kafka_decoded_messages_per_second Kafka decoded messages per second.",
    "# TYPE view_server_kafka_decoded_messages_per_second gauge",
    "# HELP view_server_kafka_decode_failures_per_second Kafka decode failures per second.",
    "# TYPE view_server_kafka_decode_failures_per_second gauge",
    "# HELP view_server_kafka_mapping_failures_per_second Kafka mapping failures per second.",
    "# TYPE view_server_kafka_mapping_failures_per_second gauge",
    "# HELP view_server_kafka_publish_failures_per_second Kafka publish failures per second.",
    "# TYPE view_server_kafka_publish_failures_per_second gauge",
    "# HELP view_server_kafka_commit_failures_per_second Kafka commit failures per second.",
    "# TYPE view_server_kafka_commit_failures_per_second gauge",
    "# HELP view_server_kafka_processing_failures_per_second Kafka processing failures per second.",
    "# TYPE view_server_kafka_processing_failures_per_second gauge",
    "# HELP view_server_kafka_consumer_lag_messages Kafka consumer lag messages.",
    "# TYPE view_server_kafka_consumer_lag_messages gauge",
    "# HELP view_server_grpc_client_connected gRPC client connection state.",
    "# TYPE view_server_grpc_client_connected gauge",
    "# HELP view_server_grpc_client_active_feeds gRPC client active feed count.",
    "# TYPE view_server_grpc_client_active_feeds gauge",
    "# HELP view_server_grpc_feed_rows gRPC feed retained rows.",
    "# TYPE view_server_grpc_feed_rows gauge",
    "# HELP view_server_grpc_feed_subscribers gRPC feed subscribers.",
    "# TYPE view_server_grpc_feed_subscribers gauge",
    "# HELP view_server_grpc_feed_messages_per_second gRPC feed messages per second.",
    "# TYPE view_server_grpc_feed_messages_per_second gauge",
    "# HELP view_server_grpc_feed_rows_per_second gRPC feed rows per second.",
    "# TYPE view_server_grpc_feed_rows_per_second gauge",
    "# HELP view_server_grpc_feed_decode_failures_per_second gRPC feed decode failures per second.",
    "# TYPE view_server_grpc_feed_decode_failures_per_second gauge",
    "# HELP view_server_grpc_feed_mapping_failures_per_second gRPC feed mapping failures per second.",
    "# TYPE view_server_grpc_feed_mapping_failures_per_second gauge",
    "# HELP view_server_grpc_feed_publish_failures_per_second gRPC feed publish failures per second.",
    "# TYPE view_server_grpc_feed_publish_failures_per_second gauge",
    "# HELP view_server_grpc_feed_reconnects gRPC feed reconnects.",
    "# TYPE view_server_grpc_feed_reconnects gauge",
  ];

  for (const [topicName, topic] of Object.entries(health.engine.topics)) {
    const labels = { topic: topicName };
    lines.push(
      metricLine("view_server_engine_topic_rows", topic.rowCount, {
        ...labels,
        state: "total",
      }),
      metricLine("view_server_engine_topic_rows", topic.liveRowCount, {
        ...labels,
        state: "live",
      }),
      metricLine("view_server_engine_topic_rows", topic.deletedRowCount, {
        ...labels,
        state: "deleted",
      }),
      metricLine("view_server_engine_topic_version", topic.version, labels),
      metricLine(
        "view_server_engine_topic_pending_mutation_batches",
        topic.pendingMutationBatches,
        labels,
      ),
      metricLine("view_server_engine_topic_active_views", topic.activeViews, labels),
      metricLine(
        "view_server_engine_topic_active_grouped_views",
        topic.activeFallbackGroupedViews,
        {
          ...labels,
          mode: "fallback",
        },
      ),
      metricLine(
        "view_server_engine_topic_active_grouped_views",
        topic.activeIncrementalGroupedViews,
        {
          ...labels,
          mode: "incremental",
        },
      ),
      metricLine("view_server_engine_topic_grouped_evaluations", topic.groupedFullEvaluationCount, {
        ...labels,
        mode: "full",
      }),
      metricLine(
        "view_server_engine_topic_grouped_evaluations",
        topic.groupedPatchedEvaluationCount,
        {
          ...labels,
          mode: "patched",
        },
      ),
      metricLine(
        "view_server_engine_topic_active_subscriptions",
        topic.activeSubscriptions,
        labels,
      ),
      metricLine("view_server_engine_topic_queued_events", topic.queuedEvents, labels),
      metricLine("view_server_engine_topic_max_queue_depth", topic.maxQueueDepth, labels),
      metricLine("view_server_engine_topic_backpressure_events", topic.backpressureEvents, labels),
      metricLine("view_server_engine_topic_memory_bytes", topic.memoryBytes, labels),
      metricLine("view_server_engine_topic_tombstones", topic.tombstoneCount, labels),
      metricLine(
        "view_server_engine_topic_compaction_pending",
        booleanMetric(topic.compactionPending),
        labels,
      ),
      metricLine("view_server_engine_topic_mutations_per_second", topic.mutationsPerSecond, labels),
      metricLine("view_server_engine_topic_rows_per_second", topic.rowsPerSecond, labels),
    );
  }

  const kafka = health.kafka;
  if (kafka !== undefined) {
    for (const sourceTopic of Object.values(kafka.topics)) {
      for (const [regionName, region] of Object.entries(sourceTopic.regions)) {
        const labels = {
          region: regionName,
          sourceTopic: sourceTopic.sourceTopic,
          viewServerTopic: sourceTopic.viewServerTopic,
        };
        lines.push(
          metricLine("view_server_kafka_region_connected", booleanMetric(region.connected), labels),
          metricLine("view_server_kafka_messages_per_second", region.messagesPerSecond, labels),
          metricLine("view_server_kafka_bytes_per_second", region.bytesPerSecond, labels),
          metricLine(
            "view_server_kafka_decoded_messages_per_second",
            region.decodedMessagesPerSecond,
            labels,
          ),
          metricLine(
            "view_server_kafka_decode_failures_per_second",
            region.decodeFailuresPerSecond,
            labels,
          ),
          metricLine(
            "view_server_kafka_mapping_failures_per_second",
            region.mappingFailuresPerSecond,
            labels,
          ),
          metricLine(
            "view_server_kafka_publish_failures_per_second",
            region.publishFailuresPerSecond,
            labels,
          ),
          metricLine(
            "view_server_kafka_commit_failures_per_second",
            region.commitFailuresPerSecond,
            labels,
          ),
          metricLine(
            "view_server_kafka_processing_failures_per_second",
            region.processingFailuresPerSecond,
            labels,
          ),
          nullableNumberMetricLine(
            "view_server_kafka_consumer_lag_messages",
            region.consumerLagMessages,
            labels,
          ),
        );
      }
    }
  }

  const grpc = health.grpc;
  if (grpc !== undefined) {
    for (const [clientName, client] of Object.entries(grpc.clients)) {
      const labels = {
        client: clientName,
        baseUrl: client.baseUrl,
      };
      lines.push(
        metricLine(
          "view_server_grpc_client_connected",
          booleanMetric(client.status === "connected"),
          labels,
        ),
        metricLine("view_server_grpc_client_active_feeds", client.activeFeeds, labels),
      );
    }
    for (const [topicName, topicFeeds] of Object.entries(grpc.feeds)) {
      for (const [feedName, feed] of Object.entries(topicFeeds.materialized)) {
        const labels = {
          lifecycle: "materialized",
          topic: topicName,
          feed: feedName,
        };
        pushGrpcFeedMetrics(lines, labels, feed);
      }
      const leasedFeedTotals = new Map<string, GrpcFeedMetricTotals>();
      for (const feed of Object.values(topicFeeds.leased)) {
        const current = leasedFeedTotals.get(feed.feedName) ?? emptyGrpcFeedMetricTotals();
        leasedFeedTotals.set(feed.feedName, addGrpcFeedMetricTotals(current, feed));
      }
      for (const [feedName, feed] of leasedFeedTotals) {
        const labels = {
          lifecycle: "leased",
          topic: topicName,
          feed: feedName,
        };
        pushGrpcFeedMetrics(lines, labels, feed);
      }
    }
  }

  return compactLines(lines);
};

const metricsResponse = (status: number, body: string): HttpServerResponse.HttpServerResponse =>
  HttpServerResponse.text(body, {
    status,
    contentType: metricContentType,
  });

export const makeViewServerMetricsRoute = <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerWebSocketServerInput<Topics>,
  path: `/${string}`,
) =>
  HttpRouter.add(
    "GET",
    path,
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      return yield* validateViewServerHttpRequest(input.auth, request).pipe(
        Effect.matchEffect({
          onFailure: (error) => Effect.succeed(viewServerAuthErrorResponse(error)),
          onSuccess: () =>
            Effect.gen(function* () {
              const health = yield* input.runtime.health();
              return yield* viewServerDecodeHealth(config, health);
            }).pipe(
              Effect.map((health) => metricsResponse(200, viewServerHealthMetrics(health))),
              Effect.catchCause(() =>
                Effect.succeed(
                  metricsResponse(200, compactLines([metricLine("view_server_metrics_error", 1)])),
                ),
              ),
            ),
        }),
      );
    }),
  );
