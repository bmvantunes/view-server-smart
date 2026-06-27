import type {
  GrpcFeedHealth,
  TopicDefinitions,
  ViewServerConfig,
  ViewServerHealth,
  ViewServerRuntimeError,
} from "@view-server/config";
import { Effect, Schema } from "effect";
import {
  configuredTopicNames,
  hasConfiguredTopic,
  invalidHealthRow,
} from "./protocol-health-common";
import { ViewServerHealthSchema, type ViewServerWireHealth } from "./protocol-health-schema";

export {
  viewServerDecodeHealthSummaryEvent,
  viewServerEncodeHealthSummaryEvent,
} from "./protocol-health-summary-codec";
export {
  ViewServerHealthSchema,
  ViewServerHealthSummaryRowSchema,
  ViewServerHealthTopicRowSchema,
} from "./protocol-health-schema";
export type { ViewServerWireHealth } from "./protocol-health-schema";
export {
  viewServerDecodeHealthTopicEvent,
  viewServerEncodeHealthTopicEvent,
} from "./protocol-health-topic-codec";

const invalidHealthPayload = (error: { readonly message: string }): ViewServerRuntimeError =>
  invalidHealthRow("__view_server_health", `Invalid health payload: ${error.message}`);

function typedHealth<Topics extends TopicDefinitions>(
  health: ViewServerWireHealth,
): ViewServerHealth<Topics>;
function typedHealth(health: ViewServerWireHealth): ViewServerWireHealth {
  return health;
}

const validateGrpcFeedHealth = Effect.fn("ViewServerProtocol.health.grpcFeed.validate")(function* (
  topicName: string,
  expectedLifecycle: GrpcFeedHealth["lifecycle"],
  feed: GrpcFeedHealth,
) {
  if (feed.topic !== topicName) {
    return yield* Effect.fail(
      invalidHealthRow(
        topicName,
        `Health payload feed topic does not match feed group: ${feed.topic} != ${topicName}`,
      ),
    );
  }
  if (feed.lifecycle !== expectedLifecycle) {
    return yield* Effect.fail(
      invalidHealthRow(
        topicName,
        `Health payload ${expectedLifecycle} feed has ${feed.lifecycle} lifecycle: ${feed.feedName}`,
      ),
    );
  }
});

export const viewServerDecodeHealth = Effect.fn("ViewServerProtocol.health.decode")(function* <
  const Topics extends TopicDefinitions,
>(config: ViewServerConfig<Topics>, health: ViewServerWireHealth) {
  const encodedHealth = yield* Schema.encodeUnknownEffect(ViewServerHealthSchema)(health).pipe(
    Effect.mapError(invalidHealthPayload),
  );
  const normalizedHealth = yield* Schema.decodeUnknownEffect(ViewServerHealthSchema)(
    encodedHealth,
  ).pipe(Effect.mapError(invalidHealthPayload));
  const configuredTopics = configuredTopicNames(config);
  const healthTopics = Object.keys(normalizedHealth.engine.topics);
  for (const topic of configuredTopics) {
    if (!Object.hasOwn(normalizedHealth.engine.topics, topic)) {
      return yield* Effect.fail(
        invalidHealthRow(topic, `Health payload is missing topic: ${topic}`),
      );
    }
  }
  for (const topic of healthTopics) {
    if (!hasConfiguredTopic(config, topic)) {
      return yield* Effect.fail(
        invalidHealthRow(topic, `Health payload references unknown topic: ${topic}`),
      );
    }
  }
  for (const kafkaTopic of Object.values(normalizedHealth.kafka?.topics ?? {})) {
    if (!hasConfiguredTopic(config, kafkaTopic.viewServerTopic)) {
      return yield* Effect.fail(
        invalidHealthRow(
          kafkaTopic.viewServerTopic,
          `Health payload references unknown topic: ${kafkaTopic.viewServerTopic}`,
        ),
      );
    }
  }
  for (const [topicName, feeds] of Object.entries(normalizedHealth.grpc?.feeds ?? {})) {
    if (!hasConfiguredTopic(config, topicName)) {
      return yield* Effect.fail(
        invalidHealthRow(topicName, `Health payload references unknown topic: ${topicName}`),
      );
    }
    for (const feed of Object.values(feeds.materialized)) {
      yield* validateGrpcFeedHealth(topicName, "materialized", feed);
    }
    for (const feed of Object.values(feeds.leased)) {
      yield* validateGrpcFeedHealth(topicName, "leased", feed);
    }
  }
  return typedHealth<Topics>(normalizedHealth);
});
