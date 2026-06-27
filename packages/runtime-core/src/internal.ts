import {
  type DecodableTopicDefinitions,
  type GroupedIncrementalAdmissionLimits,
} from "@view-server/column-live-view-engine";
import { createColumnLiveViewEngineInternal } from "@view-server/column-live-view-engine/internal";
import type {
  ViewServerConfig,
  ViewServerHealth,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
} from "@view-server/config";
import type { ViewServerRuntimeLiveClient } from "@view-server/client";
import { runAllFinalizers } from "@view-server/effect-utils";
import { Clock, Effect } from "effect";
import type * as Duration from "effect/Duration";
import { AtomRef } from "effect/unstable/reactivity";
import {
  defaultRuntimeCoreTransportHealth,
  healthFromEngine,
  type RuntimeCoreHealthOverlay,
  type RuntimeCoreTransportHealth,
} from "./health";
import {
  makeRuntimeCoreLiveClient,
  type ViewServerRuntimeCoreInternalLiveClient,
} from "./live-client";
import type {
  ViewServerRuntimeCorePublicClient,
  ViewServerRuntimeCorePublicLiveClient,
} from "./public-client";
import { makeRuntimeCoreClient } from "./runtime-client";
import type { ViewServerRuntimeCoreInternalClient } from "./runtime-client";
import type { ViewServerRuntimeCoreInstance } from "./index";

export type { ViewServerRuntimeCoreInternalLiveClient } from "./live-client";
export type { ViewServerRuntimeCoreInternalClient } from "./runtime-client";

export type ViewServerRuntimeCoreInternalInstance<Topics extends DecodableTopicDefinitions> = Omit<
  ViewServerRuntimeCoreInstance<Topics>,
  "client" | "liveClient"
> & {
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly internalClient: ViewServerRuntimeCoreInternalClient<Topics>;
  readonly publicClient: ViewServerRuntimeCorePublicClient<Topics>;
  readonly liveClient: ViewServerRuntimeLiveClient<Topics>;
  readonly internalLiveClient: ViewServerRuntimeCoreInternalLiveClient<Topics>;
  readonly publicLiveClient: ViewServerRuntimeCorePublicLiveClient<Topics>;
};

export type ViewServerRuntimeCoreInternalOptionsFor<Topics extends DecodableTopicDefinitions> = {
  readonly groupedIncrementalAdmissionLimits?: Partial<GroupedIncrementalAdmissionLimits>;
  readonly subscriptionQueueCapacity?: number;
  readonly transportHealth?: RuntimeCoreTransportHealth<Topics>;
  readonly healthOverlay?: RuntimeCoreHealthOverlay<Topics>;
  readonly healthRefreshCadence?: Duration.Input;
};

export const makeViewServerRuntimeCoreInternal: <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerRuntimeCoreInternalOptionsFor<Topics>,
) => Effect.Effect<ViewServerRuntimeCoreInternalInstance<Topics>, ViewServerRuntimeError> =
  Effect.fn("ViewServerRuntimeCore.internal.make")(function* <
    const Topics extends DecodableTopicDefinitions,
  >(config: ViewServerConfig<Topics>, input: ViewServerRuntimeCoreInternalOptionsFor<Topics>) {
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
    const engine = yield* createColumnLiveViewEngineInternal<Topics>(engineConfig);
    const engineHealth = yield* engine.health();
    const nowMillis = yield* Clock.currentTimeMillis;
    const health: AtomRef.AtomRef<ViewServerHealth<Topics>> = AtomRef.make(
      healthFromEngine(engineHealth, transportHealth, healthOverlay, nowMillis),
    );
    const runtimeClient = yield* makeRuntimeCoreClient<Topics>(
      config,
      engine,
      health,
      transportHealth,
      healthOverlay,
      input.healthRefreshCadence,
    );
    const liveClient = yield* makeRuntimeCoreLiveClient<Topics>(
      config,
      engine,
      health,
      runtimeClient.refreshHealth,
    );
    const close = Effect.uninterruptible(runAllFinalizers([runtimeClient.close, liveClient.close]));
    const publicLiveClient: ViewServerRuntimeCorePublicLiveClient<Topics> = {
      close,
      health: liveClient.health,
      subscribe: liveClient.subscribe,
      subscribeHealth: liveClient.subscribeHealth,
      subscribeHealthSummary: liveClient.subscribeHealthSummary,
    };
    return {
      client: runtimeClient.client,
      internalClient: runtimeClient.internalClient,
      publicClient: runtimeClient.client,
      liveClient: {
        ...liveClient,
        close,
      },
      serverLiveClient: {
        ...liveClient,
        close,
      },
      internalLiveClient: liveClient,
      publicLiveClient,
      close,
      requestHealthRefresh: runtimeClient.requestHealthRefresh,
      refreshHealth: runtimeClient.refreshHealth,
    };
  });
