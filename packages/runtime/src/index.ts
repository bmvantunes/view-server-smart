import type { ViewServerConfig, ViewServerRuntimeError } from "@view-server/config";
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
  type ViewServerGrpcRuntimeOptions,
  type ViewServerRuntimeTopicDefinitions,
} from "./internal";

export type {
  ViewServerRuntime,
  ViewServerRuntimeOptions,
  ViewServerRuntimeOptionsInput,
  ViewServerGrpcRuntimeOptions,
};
export type { ViewServerKafkaIngressError };
export type { ViewServerGrpcIngressError };
export type { ViewServerTcpPublishIngressError } from "./tcp-publish-ingress";

export const makeViewServerRuntime: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Options extends object = ViewServerRuntimeOptions<Topics>,
>(
  config: ViewServerConfig<Topics>,
  options?: ViewServerRuntimeOptionsInput<Topics, Options>,
) => Effect.Effect<
  ViewServerRuntime<Topics>,
  | HttpServerError.ServeError
  | Config.ConfigError
  | ViewServerRuntimeError
  | ViewServerKafkaIngressError
  | ViewServerGrpcIngressError
  | ViewServerTcpPublishIngressError
> = Effect.fn("ViewServerRuntime.make")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Options extends object,
>(config: ViewServerConfig<Topics>, options?: ViewServerRuntimeOptionsInput<Topics, Options>) {
  return yield* options === undefined
    ? makeViewServerRuntimeWithDependencies(makeDefaultRuntimeDependencies<Topics>(), config)
    : makeViewServerRuntimeWithDependencies(
        makeDefaultRuntimeDependencies<Topics>(),
        config,
        options,
      );
});

export const createViewServerRuntime = makeViewServerRuntime;

export const runViewServerRuntime: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Options extends object = ViewServerRuntimeOptions<Topics>,
>(
  config: ViewServerConfig<Topics>,
  options?: ViewServerRuntimeOptionsInput<Topics, Options>,
) => Effect.Effect<
  never,
  | HttpServerError.ServeError
  | Config.ConfigError
  | ViewServerRuntimeError
  | ViewServerKafkaIngressError
  | ViewServerGrpcIngressError
  | ViewServerTcpPublishIngressError
> = Effect.fn("ViewServerRuntime.run")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Options extends object,
>(config: ViewServerConfig<Topics>, options?: ViewServerRuntimeOptionsInput<Topics, Options>) {
  return yield* options === undefined
    ? runViewServerRuntimeWithDependencies(makeDefaultRuntimeDependencies<Topics>(), config)
    : runViewServerRuntimeWithDependencies(
        makeDefaultRuntimeDependencies<Topics>(),
        config,
        options,
      );
});
