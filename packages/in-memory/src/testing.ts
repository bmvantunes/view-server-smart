import type { ViewServerRuntimeLiveClient } from "@view-server/client";
import type { ViewServerConfig, ViewServerRuntimeError } from "@view-server/config";
import {
  makeViewServerRuntimeCoreInternal,
  type ViewServerRuntimeCoreInternalInstance,
  type ViewServerRuntimeCoreInternalOptionsFor,
} from "@view-server/runtime-core/internal";
import { Effect } from "effect";
import type {
  DecodableTopicDefinitions,
  ViewServerInMemoryInstance,
  ViewServerInMemoryOptions,
} from "./index";

export type ViewServerInMemoryTestingInstance<Topics extends DecodableTopicDefinitions> = {
  readonly client: ViewServerInMemoryInstance<Topics>["client"];
  readonly liveClient: ViewServerRuntimeLiveClient<Topics>;
  readonly close: Effect.Effect<void>;
};

const toRuntimeCoreInternalOptions = <const Topics extends DecodableTopicDefinitions>(
  input: ViewServerInMemoryOptions<Topics>,
): ViewServerRuntimeCoreInternalOptionsFor<Topics> => ({
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

const toInMemoryTestingInstance = <const Topics extends DecodableTopicDefinitions>(
  runtimeCore: ViewServerRuntimeCoreInternalInstance<Topics>,
): ViewServerInMemoryTestingInstance<Topics> => ({
  client: runtimeCore.publicClient,
  close: runtimeCore.close,
  liveClient: {
    close: runtimeCore.liveClient.close,
    health: runtimeCore.liveClient.health,
    subscribe: runtimeCore.internalLiveClient.subscribeInternal,
    subscribeRuntime: runtimeCore.internalLiveClient.subscribeRuntimeInternal,
    subscribeHealth: runtimeCore.liveClient.subscribeHealth,
    subscribeHealthSummary: runtimeCore.liveClient.subscribeHealthSummary,
  },
});

export const makeInMemoryViewServerTesting: <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerInMemoryOptions<Topics>,
) => Effect.Effect<ViewServerInMemoryTestingInstance<Topics>, ViewServerRuntimeError> = Effect.fn(
  "ViewServerInMemory.testing.make",
)(
  <const Topics extends DecodableTopicDefinitions>(
    config: ViewServerConfig<Topics>,
    input: ViewServerInMemoryOptions<Topics>,
  ) =>
    makeViewServerRuntimeCoreInternal(config, toRuntimeCoreInternalOptions(input)).pipe(
      Effect.map(toInMemoryTestingInstance),
    ),
);

export const createInMemoryViewServerTesting = <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  options: ViewServerInMemoryOptions<Topics> = {},
): ViewServerInMemoryTestingInstance<Topics> =>
  Effect.runSync(makeInMemoryViewServerTesting(config, options));
