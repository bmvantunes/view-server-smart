import { VIEW_SERVER_HEALTH_SUMMARY_TOPIC, VIEW_SERVER_HEALTH_TOPIC } from "@view-server/config";
import { ignoreLoggedTypedFailuresPreserveNonTypedFailures } from "@view-server/effect-utils";
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
import { Effect, Exit, Stream } from "effect";
import type { ViewServerWebSocketServerInput } from "./server-types";

const ignoreSubscriptionCloseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring RPC subscription close failure.",
);

export const makeViewServerRpcHandlers = <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerWebSocketServerInput<Topics>,
) => {
  const streamOpened = input.transport?.streamOpened ?? Effect.void;
  const streamClosed = input.transport?.streamClosed ?? Effect.void;
  const withTransportLifecycle = <A, E, R>(
    stream: Effect.Effect<Stream.Stream<A, E, R>, E, R>,
  ): Stream.Stream<A, E, R> =>
    Stream.unwrap(
      streamOpened.pipe(
        Effect.andThen(stream),
        Effect.onExit((exit) => (Exit.isFailure(exit) ? streamClosed : Effect.void)),
        Effect.map((openedStream) => openedStream.pipe(Stream.ensuring(streamClosed))),
      ),
    );

  return ViewServerRpcs.of({
    "ViewServer.Health": () =>
      Effect.gen(function* () {
        const health = yield* input.runtime.health();
        return yield* viewServerDecodeHealth(config, health);
      }),
    "ViewServer.Subscribe": (payload) =>
      withTransportLifecycle(
        Effect.gen(function* () {
          if (payload.topic === VIEW_SERVER_HEALTH_SUMMARY_TOPIC) {
            yield* viewServerDecodeHealthQuery(payload.topic, payload.query);
            const subscription = yield* input.liveClient.subscribeHealthSummary();
            return subscription.events.pipe(
              Stream.mapEffect((event) =>
                viewServerEncodeHealthSummaryEvent<Topics>(config, event),
              ),
              Stream.ensuring(subscription.close().pipe(ignoreSubscriptionCloseFailure)),
            );
          }
          if (payload.topic === VIEW_SERVER_HEALTH_TOPIC) {
            yield* viewServerDecodeHealthQuery(payload.topic, payload.query);
            const subscription = yield* input.liveClient.subscribeHealth();
            return subscription.events.pipe(
              Stream.mapEffect((event) => viewServerEncodeHealthTopicEvent<Topics>(config, event)),
              Stream.ensuring(subscription.close().pipe(ignoreSubscriptionCloseFailure)),
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
            Stream.ensuring(subscription.close().pipe(ignoreSubscriptionCloseFailure)),
          );
        }),
      ),
  });
};
