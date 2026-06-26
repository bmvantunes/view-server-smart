import type {
  GrpcRuntimeClients,
  RuntimeRegions,
  ViewServerConfig,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
} from "@view-server/config";
import { type ViewServerRuntimeCoreOptionsFor } from "@view-server/runtime-core";
import {
  makeViewServerRuntimeCoreInternal,
  type ViewServerRuntimeCoreInternalInstance,
} from "@view-server/runtime-core/internal";
import {
  makeViewServerWebSocketServer,
  type ViewServerWebSocketServer,
  type ViewServerWebSocketServerInput,
  type ViewServerWebSocketServerOptions,
} from "@view-server/server";
import type { Effect } from "effect";
import type { HttpServerError } from "effect/unstable/http";
import { makeViewServerKafkaHealthLedger, type ViewServerKafkaHealthLedger } from "./kafka-health";
import { makeViewServerGrpcHealthLedger, type ViewServerGrpcHealthLedger } from "./grpc-health";
import {
  makeViewServerGrpcIngress,
  type ViewServerGrpcIngress,
  type ViewServerGrpcIngressError,
} from "./grpc-ingress";
import {
  makeViewServerKafkaIngress,
  type ViewServerKafkaIngress,
  type ViewServerKafkaIngressError,
} from "./kafka-ingress";
import type {
  ResolvedViewServerGrpcRuntimeOptions,
  ResolvedViewServerKafkaRuntimeOptions,
} from "./runtime-options";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export type ViewServerRuntimeDependencies<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly makeRuntimeCore: (
    config: ViewServerConfig<Topics>,
    options: ViewServerRuntimeCoreOptionsFor<Topics>,
  ) => Effect.Effect<ViewServerRuntimeCoreInternalInstance<Topics>, ViewServerRuntimeError>;
  readonly makeServer: (
    config: ViewServerConfig<Topics>,
    input: ViewServerWebSocketServerInput<Topics>,
    options: ViewServerWebSocketServerOptions,
  ) => Effect.Effect<ViewServerWebSocketServer, HttpServerError.ServeError>;
  readonly makeKafkaHealthLedger: <const Regions extends RuntimeRegions>(
    config: ViewServerConfig<Topics>,
    options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
  ) => ViewServerKafkaHealthLedger<Topics>;
  readonly makeGrpcHealthLedger: <const Clients extends GrpcRuntimeClients>(
    config: ViewServerConfig<Topics>,
    options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
  ) => ViewServerGrpcHealthLedger<Topics>;
  readonly makeKafkaIngress: <const Regions extends RuntimeRegions>(
    config: ViewServerConfig<Topics>,
    client: ViewServerRuntimeClient<Topics>,
    requestHealthRefresh: Effect.Effect<void>,
    options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
    health: ViewServerKafkaHealthLedger<Topics>,
  ) => Effect.Effect<ViewServerKafkaIngress, ViewServerKafkaIngressError>;
  readonly makeGrpcIngress: <const Clients extends GrpcRuntimeClients>(
    config: ViewServerConfig<Topics>,
    client: ViewServerRuntimeClient<Topics>,
    requestHealthRefresh: Effect.Effect<void>,
    options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
    health: ViewServerGrpcHealthLedger<Topics>,
  ) => Effect.Effect<ViewServerGrpcIngress, ViewServerGrpcIngressError>;
};

export const makeDefaultRuntimeDependencies = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(): ViewServerRuntimeDependencies<Topics> => ({
  makeRuntimeCore: makeViewServerRuntimeCoreInternal,
  makeServer: makeViewServerWebSocketServer,
  makeKafkaHealthLedger: (_config, options) =>
    makeViewServerKafkaHealthLedger({
      startFrom: options.consume,
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
  makeGrpcHealthLedger: (_config, options) =>
    makeViewServerGrpcHealthLedger({
      clients: options.clientBaseUrls,
      feeds: Object.fromEntries(
        Object.entries(options.feeds)
          .filter(([, feed]) => feed.lifecycle === "materialized")
          .map(([feedName, feed]) => [
            feedName,
            {
              client: feed.client,
              lifecycle: feed.lifecycle,
              topic: feed.topic,
            },
          ]),
      ),
    }),
  makeKafkaIngress: makeViewServerKafkaIngress,
  makeGrpcIngress: makeViewServerGrpcIngress,
});
