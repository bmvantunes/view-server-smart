import type { ViewServerConfig } from "@view-server/config";
import type { ViewServerRuntimeError } from "@view-server/config";
import {
  createViewServerRuntimeCore,
  makeViewServerRuntimeCore,
  type DecodableTopicDefinitions,
  type ViewServerRuntimeCoreInstance,
  type ViewServerRuntimeCoreOptionsFor,
  type ViewServerRuntimeCorePublicClient,
  type ViewServerRuntimeCorePublicLiveClient,
} from "@view-server/runtime-core";
import { Effect } from "effect";

export type { DecodableTopicDefinitions } from "@view-server/runtime-core";

export type ViewServerInMemoryInstance<Topics extends DecodableTopicDefinitions> = {
  readonly client: ViewServerRuntimeCorePublicClient<Topics>;
  readonly liveClient: ViewServerRuntimeCorePublicLiveClient<Topics>;
  readonly close: Effect.Effect<void>;
};

export type ViewServerInMemoryOptions<
  Topics extends DecodableTopicDefinitions = DecodableTopicDefinitions,
> = Omit<ViewServerRuntimeCoreOptionsFor<Topics>, "transportHealth">;

const toRuntimeCoreOptions = <const Topics extends DecodableTopicDefinitions>(
  input: ViewServerInMemoryOptions<Topics>,
): ViewServerRuntimeCoreOptionsFor<Topics> => ({
  ...(input.groupedIncrementalAdmissionLimits === undefined
    ? {}
    : { groupedIncrementalAdmissionLimits: input.groupedIncrementalAdmissionLimits }),
  ...(input.subscriptionQueueCapacity === undefined
    ? {}
    : { subscriptionQueueCapacity: input.subscriptionQueueCapacity }),
  ...(input.healthRefreshCadence === undefined
    ? {}
    : { healthRefreshCadence: input.healthRefreshCadence }),
});

const toInMemoryInstance = <const Topics extends DecodableTopicDefinitions>(
  runtimeCore: ViewServerRuntimeCoreInstance<Topics>,
): ViewServerInMemoryInstance<Topics> => {
  return {
    client: runtimeCore.client,
    close: runtimeCore.close,
    liveClient: runtimeCore.liveClient,
  };
};

export const makeInMemoryViewServer: <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerInMemoryOptions<Topics>,
) => Effect.Effect<ViewServerInMemoryInstance<Topics>, ViewServerRuntimeError> = Effect.fn(
  "ViewServerInMemory.make",
)(
  <const Topics extends DecodableTopicDefinitions>(
    config: ViewServerConfig<Topics>,
    input: ViewServerInMemoryOptions<Topics>,
  ) =>
    makeViewServerRuntimeCore(config, toRuntimeCoreOptions(input)).pipe(
      Effect.map(toInMemoryInstance),
    ),
);

export const createInMemoryViewServer = <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  options: ViewServerInMemoryOptions<Topics> = {},
): ViewServerInMemoryInstance<Topics> =>
  toInMemoryInstance(createViewServerRuntimeCore(config, toRuntimeCoreOptions(options)));
