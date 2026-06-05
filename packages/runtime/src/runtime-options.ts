import type { ViewServerInMemoryOptions } from "@view-server/in-memory";
import type { ViewServerWebSocketServerOptions } from "@view-server/server";
import type { ViewServerRuntimeOptions } from "./runtime-types";

export type ResolvedViewServerRuntimeOptions = {
  readonly inMemoryOptions: ViewServerInMemoryOptions;
  readonly serverOptions: ViewServerWebSocketServerOptions;
};

export const resolveViewServerRuntimeOptions = (
  options: ViewServerRuntimeOptions,
): ResolvedViewServerRuntimeOptions => {
  const inMemoryOptions =
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
    inMemoryOptions,
    serverOptions,
  };
};
