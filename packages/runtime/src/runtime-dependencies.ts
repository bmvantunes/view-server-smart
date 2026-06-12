import type { ViewServerConfig, ViewServerRuntimeClient } from "@view-server/config";
import {
  makeViewServerRuntimeCore,
  type ViewServerRuntimeCoreInstance,
  type ViewServerRuntimeCoreOptionsFor,
} from "@view-server/runtime-core";
import {
  makeViewServerWebSocketServer,
  type ViewServerWebSocketServer,
  type ViewServerWebSocketServerInput,
  type ViewServerWebSocketServerOptions,
} from "@view-server/server";
import type { Effect } from "effect";
import type { HttpServerError } from "effect/unstable/http";
import { makeViewServerKafkaHealthLedger, type ViewServerKafkaHealthLedger } from "./kafka-health";
import {
  makeViewServerKafkaIngress,
  type ViewServerKafkaIngress,
  type ViewServerKafkaIngressError,
} from "./kafka-ingress";
import type { ResolvedViewServerKafkaRuntimeOptions } from "./runtime-options";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export type ViewServerRuntimeDependencies<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly makeRuntimeCore: (
    config: ViewServerConfig<Topics>,
    options: ViewServerRuntimeCoreOptionsFor<Topics>,
  ) => Effect.Effect<ViewServerRuntimeCoreInstance<Topics>>;
  readonly makeServer: (
    config: ViewServerConfig<Topics>,
    input: ViewServerWebSocketServerInput<Topics>,
    options: ViewServerWebSocketServerOptions,
  ) => Effect.Effect<ViewServerWebSocketServer, HttpServerError.ServeError>;
  readonly makeKafkaHealthLedger: (
    config: ViewServerConfig<Topics>,
    options: ResolvedViewServerKafkaRuntimeOptions<Topics>,
  ) => ViewServerKafkaHealthLedger<Topics>;
  readonly makeKafkaIngress: (
    config: ViewServerConfig<Topics>,
    client: ViewServerRuntimeClient<Topics>,
    requestHealthRefresh: Effect.Effect<void>,
    options: ResolvedViewServerKafkaRuntimeOptions<Topics>,
    health: ViewServerKafkaHealthLedger<Topics>,
  ) => Effect.Effect<ViewServerKafkaIngress, ViewServerKafkaIngressError>;
};

export const makeDefaultRuntimeDependencies = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(): ViewServerRuntimeDependencies<Topics> => ({
  makeRuntimeCore: makeViewServerRuntimeCore,
  makeServer: makeViewServerWebSocketServer,
  makeKafkaHealthLedger: (_config, options) =>
    makeViewServerKafkaHealthLedger({
      regions: options.regions,
      topics: Object.fromEntries(
        Object.entries(options.topics).map(([sourceTopic, topic]) => [
          sourceTopic,
          {
            regions: topic.regions,
            viewServerTopic: topic.viewServerTopic,
          },
        ]),
      ),
    }),
  makeKafkaIngress: makeViewServerKafkaIngress,
});
