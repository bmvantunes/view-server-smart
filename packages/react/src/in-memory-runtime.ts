import {
  createColumnLiveViewEngine,
  InvalidQueryError,
  InvalidRowError,
  InvalidTopicError,
  UnsupportedQueryError,
  type ColumnLiveViewEngine,
  type ColumnLiveViewEngineError,
  type ColumnLiveViewEngineHealth,
  type DecodableTopicDefinitions,
} from "@view-server/column-live-view-engine";
import type {
  ViewServerConfig,
  ViewServerHealth,
  ViewServerInMemoryRuntime,
  ViewServerRuntimeError,
} from "@view-server/config";
import { Effect } from "effect";
import * as AtomRef from "effect/unstable/reactivity/AtomRef";
import type { ViewServerReactClient } from "./react-client";

export type InMemoryViewServerState<Topics extends DecodableTopicDefinitions> = {
  readonly reactClient: ViewServerReactClient<Topics>;
  readonly runtime: ViewServerInMemoryRuntime<Topics>;
};

export type ProviderInput = {
  readonly subscriptionQueueCapacity?: number;
};

const engineErrorToRuntimeError = (error: ColumnLiveViewEngineError): ViewServerRuntimeError => {
  if (error instanceof InvalidTopicError) {
    return {
      _tag: "ViewServerRuntimeError",
      code: "InvalidTopic",
      message: error.message,
      topic: error.topic,
    };
  }
  if (error instanceof InvalidRowError) {
    return {
      _tag: "ViewServerRuntimeError",
      code: "InvalidRow",
      message: error.message,
      topic: error.topic,
    };
  }
  if (error instanceof InvalidQueryError) {
    return {
      _tag: "ViewServerRuntimeError",
      code: "SnapshotStale",
      message: error.message,
      topic: error.topic,
    };
  }
  if (error instanceof UnsupportedQueryError) {
    return {
      _tag: "ViewServerRuntimeError",
      code: "SnapshotStale",
      message: error.message,
      topic: error.topic,
    };
  }
  return {
    _tag: "ViewServerRuntimeError",
    code: "RuntimeUnavailable",
    message: error.message,
  };
};

const healthFromEngine = <Topics extends DecodableTopicDefinitions>(
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

export const readHealth = <Topics extends DecodableTopicDefinitions>(
  engine: ColumnLiveViewEngine<Topics>,
  health: AtomRef.AtomRef<ViewServerHealth<Topics>>,
) =>
  engine.health().pipe(
    Effect.map(healthFromEngine),
    Effect.tap((value) => Effect.sync(() => health.set(value))),
  );

export const refreshHealth = <Topics extends DecodableTopicDefinitions>(
  engine: ColumnLiveViewEngine<Topics>,
  health: AtomRef.AtomRef<ViewServerHealth<Topics>>,
) => readHealth(engine, health).pipe(Effect.asVoid);

export const makeHealthRefreshScheduler = (refresh: Effect.Effect<void>) => {
  let running = false;
  let pending = false;

  const drainRefreshes = Effect.fn("ViewServerReact.healthRefreshScheduler.drain")(function* () {
    let shouldRefresh = true;
    while (shouldRefresh) {
      pending = false;
      yield* refresh;
      shouldRefresh = pending;
    }
  });

  return Effect.gen(function* () {
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
  });
};

const makeRuntime = <Topics extends DecodableTopicDefinitions>(
  engine: ColumnLiveViewEngine<Topics>,
  health: AtomRef.AtomRef<ViewServerHealth<Topics>>,
): ViewServerInMemoryRuntime<Topics> => {
  const requestHealthRefresh = makeHealthRefreshScheduler(refreshHealth(engine, health));
  return {
    publish: (topic, row) =>
      engine.publish(topic, row).pipe(
        Effect.tap(() => requestHealthRefresh),
        Effect.mapError(engineErrorToRuntimeError),
      ),
    publishMany: (topic, rows) =>
      engine.publishMany(topic, rows).pipe(
        Effect.tap(() => requestHealthRefresh),
        Effect.mapError(engineErrorToRuntimeError),
      ),
    patch: (topic, key, patch) =>
      engine.patch(topic, key, patch).pipe(
        Effect.tap(() => requestHealthRefresh),
        Effect.mapError(engineErrorToRuntimeError),
      ),
    delete: (topic, key) =>
      engine.delete(topic, key).pipe(
        Effect.tap(() => requestHealthRefresh),
        Effect.mapError(engineErrorToRuntimeError),
      ),
    snapshot: (topic, query) =>
      engine.snapshot(topic, query).pipe(Effect.mapError(engineErrorToRuntimeError)),
    health: () => readHealth(engine, health).pipe(Effect.mapError(engineErrorToRuntimeError)),
    reset: () =>
      engine.reset().pipe(
        Effect.tap(() => requestHealthRefresh),
        Effect.mapError(engineErrorToRuntimeError),
      ),
  };
};

const makeReactClient = <Topics extends DecodableTopicDefinitions>(
  engine: ColumnLiveViewEngine<Topics>,
  health: AtomRef.AtomRef<ViewServerHealth<Topics>>,
): ViewServerReactClient<Topics> => ({
  subscribe: (topic, query) =>
    engine.subscribe(topic, query).pipe(
      Effect.map((subscription) => ({
        events: subscription.events,
        close: () => subscription.close().pipe(Effect.andThen(refreshHealth(engine, health))),
      })),
      Effect.tap(() => refreshHealth(engine, health)),
      Effect.mapError(engineErrorToRuntimeError),
    ),
  health,
  close: engine.close(),
});

export const makeProviderState = <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  input: ProviderInput,
): Effect.Effect<InMemoryViewServerState<Topics>> =>
  Effect.gen(function* () {
    const engineConfig =
      input.subscriptionQueueCapacity === undefined
        ? { topics: config.topics }
        : {
            topics: config.topics,
            subscriptionQueueCapacity: input.subscriptionQueueCapacity,
          };
    const engine = yield* createColumnLiveViewEngine<Topics>(engineConfig);
    const engineHealth = yield* engine.health();
    const health = AtomRef.make(healthFromEngine(engineHealth));
    const runtime = makeRuntime(engine, health);
    const reactClient = makeReactClient(engine, health);
    return yield* Effect.succeed({
      reactClient,
      runtime,
    });
  });
