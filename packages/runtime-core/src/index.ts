import {
  createColumnLiveViewEngine,
  type DecodableTopicDefinitions,
  type GroupedIncrementalAdmissionLimits,
} from "@view-server/column-live-view-engine";
import type { ViewServerRuntimeLiveClient } from "@view-server/client";
import type {
  ViewServerConfig,
  ViewServerHealth,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
} from "@view-server/config";
import { runAllFinalizers } from "@view-server/effect-utils";
import { Clock, Effect } from "effect";
import { AtomRef } from "effect/unstable/reactivity";
import {
  defaultRuntimeCoreTransportHealth,
  healthFromEngine,
  type RuntimeCoreHealthOverlay,
  type RuntimeCoreTransportHealth,
} from "./health";
import type * as Duration from "effect/Duration";
import { makeRuntimeCoreLiveClient } from "./live-client";
import { makeRuntimeCoreClient } from "./runtime-client";

export type { DecodableTopicDefinitions } from "@view-server/column-live-view-engine";
export type { GroupedIncrementalAdmissionLimits } from "@view-server/column-live-view-engine";
export type { RuntimeCoreTransportHealth } from "./health";
export type { RuntimeCoreHealthOverlay } from "./health";

export type ViewServerRuntimeCoreInstance<Topics extends DecodableTopicDefinitions> = {
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly liveClient: ViewServerRuntimeLiveClient<Topics>;
  readonly close: Effect.Effect<void>;
  readonly requestHealthRefresh: Effect.Effect<void>;
  readonly refreshHealth: Effect.Effect<ViewServerHealth<Topics>, ViewServerRuntimeError>;
};

export type ViewServerRuntimeCoreOptions = {
  readonly groupedIncrementalAdmissionLimits?: Partial<GroupedIncrementalAdmissionLimits>;
  readonly subscriptionQueueCapacity?: number;
  readonly transportHealth?: RuntimeCoreTransportHealth<DecodableTopicDefinitions>;
  readonly healthOverlay?: RuntimeCoreHealthOverlay<DecodableTopicDefinitions>;
  readonly healthRefreshCadence?: Duration.Input;
};

export type ViewServerRuntimeCoreOptionsFor<Topics extends DecodableTopicDefinitions> = {
  readonly groupedIncrementalAdmissionLimits?: Partial<GroupedIncrementalAdmissionLimits>;
  readonly subscriptionQueueCapacity?: number;
  readonly transportHealth?: RuntimeCoreTransportHealth<Topics>;
  readonly healthOverlay?: RuntimeCoreHealthOverlay<Topics>;
  readonly healthRefreshCadence?: Duration.Input;
};

export const makeViewServerRuntimeCore: <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerRuntimeCoreOptionsFor<Topics>,
) => Effect.Effect<ViewServerRuntimeCoreInstance<Topics>> = Effect.fn("ViewServerRuntimeCore.make")(
  function* <const Topics extends DecodableTopicDefinitions>(
    config: ViewServerConfig<Topics>,
    input: ViewServerRuntimeCoreOptionsFor<Topics>,
  ) {
    const transportHealth = input.transportHealth ?? defaultRuntimeCoreTransportHealth;
    const healthOverlay = input.healthOverlay;
    const engineConfig = {
      ...(input.groupedIncrementalAdmissionLimits === undefined
        ? {}
        : { groupedIncrementalAdmissionLimits: input.groupedIncrementalAdmissionLimits }),
      ...(input.subscriptionQueueCapacity === undefined
        ? {}
        : { subscriptionQueueCapacity: input.subscriptionQueueCapacity }),
      topics: config.topics,
    };
    const engine = yield* createColumnLiveViewEngine<Topics>(engineConfig);
    const engineHealth = yield* engine.health();
    const nowMillis = yield* Clock.currentTimeMillis;
    const health: AtomRef.AtomRef<ViewServerHealth<Topics>> = AtomRef.make(
      healthFromEngine(engineHealth, transportHealth, healthOverlay, nowMillis),
    );
    const runtimeClient = yield* makeRuntimeCoreClient<Topics>(
      engine,
      health,
      transportHealth,
      healthOverlay,
      input.healthRefreshCadence,
    );
    const liveClient = yield* makeRuntimeCoreLiveClient<Topics>(
      engine,
      health,
      runtimeClient.refreshHealth,
    );
    const close = Effect.uninterruptible(runAllFinalizers([runtimeClient.close, liveClient.close]));
    return {
      client: runtimeClient.client,
      liveClient: {
        ...liveClient,
        close,
      },
      close,
      requestHealthRefresh: runtimeClient.requestHealthRefresh,
      refreshHealth: runtimeClient.refreshHealth,
    };
  },
);

export const createViewServerRuntimeCore = <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  options: ViewServerRuntimeCoreOptionsFor<Topics> = {},
): ViewServerRuntimeCoreInstance<Topics> =>
  Effect.runSync(makeViewServerRuntimeCore(config, options));
