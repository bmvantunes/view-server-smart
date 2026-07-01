import type {
  GrpcRuntimeClients,
  RuntimeRegions,
  ViewServerConfig,
  ViewServerRuntimeError,
} from "@effect-view-server/config";
import { Config, Effect } from "effect";
import type { HttpServerError } from "effect/unstable/http";
import type { ViewServerGrpcIngressError } from "./grpc-ingress";
import type { ViewServerKafkaIngressError } from "./kafka-ingress";
import type { ViewServerTcpPublishIngressError } from "./tcp-publish-ingress";
import {
  makeDefaultRuntimeDependencies,
  makeViewServerRuntimeWithDependencies,
  runViewServerRuntimeWithDependencies,
  type ViewServerRuntime,
  type ViewServerRuntimeOptionsInput,
  type ViewServerRuntimeOptions,
  type ViewServerRuntimeOptionsArgs,
  type ViewServerGrpcRuntimeOptions,
  type ViewServerRuntimeTopicDefinitions,
} from "./internal";

export type {
  ViewServerRuntime,
  ViewServerRuntimeOptions,
  ViewServerRuntimeOptionsInput,
  ViewServerRuntimeOptionsArgs,
  ViewServerGrpcRuntimeOptions,
};
export type { ViewServerKafkaIngressError };
export type { ViewServerGrpcIngressError };
export type { ViewServerTcpPublishIngressError } from "./tcp-publish-ingress";

const makeViewServerRuntimeEffect = Effect.fn("ViewServerRuntime.make")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
  const Options extends object,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  options?: ViewServerRuntimeOptionsInput<Topics, Regions, GrpcClients, Options>,
) {
  if (options === undefined) {
    return yield* makeViewServerRuntimeWithDependencies(
      makeDefaultRuntimeDependencies<Topics>(),
      config,
    );
  }
  return yield* makeViewServerRuntimeWithDependencies(
    makeDefaultRuntimeDependencies<Topics>(),
    config,
    options,
  );
});

export function makeViewServerRuntime<
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions = RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
  const Options extends object = ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  ...args: ViewServerRuntimeOptionsArgs<
    NoInfer<Topics>,
    NoInfer<Regions>,
    NoInfer<GrpcClients>,
    Options
  >
): Effect.Effect<
  ViewServerRuntime<Topics>,
  | HttpServerError.ServeError
  | Config.ConfigError
  | ViewServerRuntimeError
  | ViewServerKafkaIngressError
  | ViewServerGrpcIngressError
  | ViewServerTcpPublishIngressError
> {
  const options = args[0];
  return makeViewServerRuntimeEffect(config, options);
}

export const createViewServerRuntime = makeViewServerRuntime;

const runViewServerRuntimeEffect = Effect.fn("ViewServerRuntime.run")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients,
  const Options extends object,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  options?: ViewServerRuntimeOptionsInput<Topics, Regions, GrpcClients, Options>,
) {
  if (options === undefined) {
    return yield* runViewServerRuntimeWithDependencies(
      makeDefaultRuntimeDependencies<Topics>(),
      config,
    );
  }
  return yield* runViewServerRuntimeWithDependencies(
    makeDefaultRuntimeDependencies<Topics>(),
    config,
    options,
  );
});

export function runViewServerRuntime<
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions = RuntimeRegions,
  const GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
  const Options extends object = ViewServerRuntimeOptions<Topics, Regions, GrpcClients>,
>(
  config: ViewServerConfig<Topics, Regions, GrpcClients>,
  ...args: ViewServerRuntimeOptionsArgs<
    NoInfer<Topics>,
    NoInfer<Regions>,
    NoInfer<GrpcClients>,
    Options
  >
): Effect.Effect<
  never,
  | HttpServerError.ServeError
  | Config.ConfigError
  | ViewServerRuntimeError
  | ViewServerKafkaIngressError
  | ViewServerGrpcIngressError
  | ViewServerTcpPublishIngressError
> {
  const options = args[0];
  return runViewServerRuntimeEffect(config, options);
}
