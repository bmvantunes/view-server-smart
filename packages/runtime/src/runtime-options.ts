import type { ViewServerRuntimeCoreOptions } from "@view-server/runtime-core";
import type { ViewServerWebSocketServerOptions } from "@view-server/server";
import type { ViewServerRuntimeOptions } from "./runtime-types";

export type ResolvedViewServerRuntimeOptions = {
  readonly runtimeCoreOptions: ViewServerRuntimeCoreOptions;
  readonly serverOptions: ViewServerWebSocketServerOptions;
};

export const resolveViewServerRuntimeOptions = (
  options: ViewServerRuntimeOptions,
): ResolvedViewServerRuntimeOptions => {
  const runtimeCoreOptions =
    options.subscriptionQueueCapacity === undefined
      ? {}
      : { subscriptionQueueCapacity: options.subscriptionQueueCapacity };
  const serverOptions = {
    ...(options.host === undefined ? {} : { host: options.host }),
    ...(options.websocketPort === undefined ? {} : { port: options.websocketPort }),
    ...(options.rpcPath === undefined ? {} : { path: options.rpcPath }),
    ...(options.healthPath === undefined ? {} : { healthPath: options.healthPath }),
  };
  return {
    runtimeCoreOptions,
    serverOptions,
  };
};
