import { VIEW_SERVER_HEALTH_SUMMARY_TOPIC, VIEW_SERVER_HEALTH_TOPIC } from "@view-server/config";
import type { TopicDefinitions, ViewServerConfig } from "@view-server/config";
import {
  ViewServerRpcs,
  viewServerDecodeHealth,
  viewServerDecodeHealthQuery,
  viewServerDecodeLiveQuery,
  viewServerDecodeTopic,
  viewServerEncodeHealthSummaryEvent,
  viewServerEncodeHealthTopicEvent,
  viewServerEncodeLiveEvent,
} from "@view-server/protocol";
import { Effect, Stream } from "effect";
import type { ViewServerWebSocketServerInput } from "./server-types";

export const makeViewServerRpcHandlers = <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerWebSocketServerInput<Topics>,
) => {
  return ViewServerRpcs.of({
    "ViewServer.Health": () =>
      Effect.gen(function* () {
        const health = yield* input.runtime.health();
        return yield* viewServerDecodeHealth(config, health);
      }),
    "ViewServer.Subscribe": (payload) =>
      Stream.unwrap(
        Effect.gen(function* () {
          if (payload.topic === VIEW_SERVER_HEALTH_SUMMARY_TOPIC) {
            yield* viewServerDecodeHealthQuery(payload.topic, payload.query);
            const subscription = yield* input.liveClient.subscribeHealthSummary();
            return subscription.events.pipe(
              Stream.mapEffect((event) =>
                viewServerEncodeHealthSummaryEvent<Topics>(config, event),
              ),
              Stream.ensuring(subscription.close().pipe(Effect.ignore)),
            );
          }
          if (payload.topic === VIEW_SERVER_HEALTH_TOPIC) {
            yield* viewServerDecodeHealthQuery(payload.topic, payload.query);
            const subscription = yield* input.liveClient.subscribeHealth();
            return subscription.events.pipe(
              Stream.mapEffect((event) => viewServerEncodeHealthTopicEvent<Topics>(config, event)),
              Stream.ensuring(subscription.close().pipe(Effect.ignore)),
            );
          }
          const topic = yield* viewServerDecodeTopic(config, payload.topic);
          const query = yield* viewServerDecodeLiveQuery<Topics, typeof topic>(
            config,
            topic,
            payload.query,
          );
          const subscription = yield* input.liveClient.subscribeRuntime(topic, query);
          return subscription.events.pipe(
            Stream.mapEffect((event) => viewServerEncodeLiveEvent(config, topic, query, event)),
            Stream.ensuring(subscription.close().pipe(Effect.ignore)),
          );
        }),
      ),
  });
};
