import type {
  ColumnLiveViewEngine,
  ColumnLiveViewEngineHealth,
  DecodableTopicDefinitions,
} from "@view-server/column-live-view-engine";
import type { ViewServerHealth } from "@view-server/config";
import { Effect } from "effect";
import type * as AtomRef from "effect/unstable/reactivity/AtomRef";

type EngineHealthReader<Topics extends DecodableTopicDefinitions> = {
  readonly health: () => Effect.Effect<ColumnLiveViewEngineHealth<Topics>, never>;
};

export const healthFromEngine = <Topics extends DecodableTopicDefinitions>(
  engineHealth: ColumnLiveViewEngineHealth<Topics>,
): ViewServerHealth<Topics> => {
  return {
    status: engineHealth.status,
    version: engineHealth.version,
    uptimeMs: 0,
    engine: { topics: engineHealth.topics },
    transport: {
      activeClients: 1,
      activeStreams: engineHealth.activeSubscriptions,
      activeSubscriptions: engineHealth.activeSubscriptions,
      messagesPerSecond: 0,
      bytesPerSecond: 0,
      queuedMessages: engineHealth.queuedEvents,
      queuedBytes: 0,
      droppedClients: 0,
      backpressureEvents: engineHealth.backpressureEvents,
      reconnects: 0,
      lastError: null,
    },
  };
};

const nextHealthValue = <Topics extends DecodableTopicDefinitions>(
  current: ViewServerHealth<Topics>,
  next: ViewServerHealth<Topics>,
): ViewServerHealth<Topics> => {
  if (current.status === "stopping" && next.status !== "stopping") {
    return current;
  }
  return next;
};

export const readHealth = Effect.fn("ViewServerInMemory.health.read")(function* <
  const Topics extends DecodableTopicDefinitions,
>(engine: EngineHealthReader<Topics>, health: AtomRef.AtomRef<ViewServerHealth<Topics>>) {
  const value = healthFromEngine(yield* engine.health());
  yield* Effect.sync(() => {
    health.update((current) => nextHealthValue(current, value));
  });
  return health.value;
});

export const refreshHealth = Effect.fn("ViewServerInMemory.health.refresh")(function* <
  const Topics extends DecodableTopicDefinitions,
>(engine: ColumnLiveViewEngine<Topics>, health: AtomRef.AtomRef<ViewServerHealth<Topics>>) {
  yield* readHealth(engine, health);
});

export const makeHealthRefreshScheduler = (refresh: Effect.Effect<void>) => {
  let running = false;
  let pending = false;

  const drainRefreshes = Effect.fn("ViewServerInMemory.healthRefreshScheduler.drain")(function* () {
    let shouldRefresh = true;
    while (shouldRefresh) {
      pending = false;
      yield* refresh;
      shouldRefresh = pending;
    }
  });

  const requestRefresh = Effect.fn("ViewServerInMemory.healthRefreshScheduler.request")(
    function* () {
      if (running) {
        pending = true;
        return;
      }
      running = true;
      yield* drainRefreshes().pipe(
        Effect.ensuring(
          Effect.sync(() => {
            running = false;
          }),
        ),
        Effect.forkDetach({ startImmediately: true }),
      );
    },
  );

  return requestRefresh();
};
