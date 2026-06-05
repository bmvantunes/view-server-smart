import {
  createColumnLiveViewEngine,
  type DecodableTopicDefinitions,
} from "@view-server/column-live-view-engine";
import type { ViewServerRuntimeLiveClient } from "@view-server/client";
import type {
  ViewServerConfig,
  ViewServerHealth,
  ViewServerInMemoryRuntime,
} from "@view-server/config";
import { Effect } from "effect";
import { AtomRef } from "effect/unstable/reactivity";
import { healthFromEngine } from "./health";
import { makeInMemoryLiveClient } from "./live-client";
import { makeInMemoryRuntimeClient } from "./runtime-client";

export type { DecodableTopicDefinitions } from "@view-server/column-live-view-engine";

export type ViewServerInMemoryInstance<Topics extends DecodableTopicDefinitions> = {
  readonly client: ViewServerInMemoryRuntime<Topics>;
  readonly liveClient: ViewServerRuntimeLiveClient<Topics>;
  readonly close: Effect.Effect<void>;
};

export type ViewServerInMemoryOptions = {
  readonly subscriptionQueueCapacity?: number;
};

export const makeInMemoryViewServer: <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ViewServerInMemoryOptions,
) => Effect.Effect<ViewServerInMemoryInstance<Topics>> = Effect.fn("ViewServerInMemory.make")(
  function* <const Topics extends DecodableTopicDefinitions>(
    config: ViewServerConfig<Topics>,
    input: ViewServerInMemoryOptions,
  ) {
    const engineConfig =
      input.subscriptionQueueCapacity === undefined
        ? { topics: config.topics }
        : {
            topics: config.topics,
            subscriptionQueueCapacity: input.subscriptionQueueCapacity,
          };
    const engine = yield* createColumnLiveViewEngine<Topics>(engineConfig);
    const engineHealth = yield* engine.health();
    const health: AtomRef.AtomRef<ViewServerHealth<Topics>> = AtomRef.make(
      healthFromEngine(engineHealth),
    );
    const client = yield* makeInMemoryRuntimeClient(engine, health);
    const liveClient = yield* makeInMemoryLiveClient(engine, health);
    return {
      client,
      liveClient,
      close: liveClient.close,
    };
  },
);

export const createInMemoryViewServer = <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  options: ViewServerInMemoryOptions = {},
): ViewServerInMemoryInstance<Topics> => Effect.runSync(makeInMemoryViewServer(config, options));
