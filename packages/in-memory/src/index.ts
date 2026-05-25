import {
  createColumnLiveViewEngine,
  InvalidQueryError,
  InvalidRowError,
  InvalidTopicError,
  UnsupportedQueryError,
  type ColumnLiveViewEngine,
  type ColumnLiveViewEngineError,
  type DecodableTopicDefinitions,
} from "@view-server/column-live-view-engine";
import type { ViewServerLiveClient } from "@view-server/client";
import type {
  ViewServerConfig,
  ViewServerHealth,
  ViewServerInMemoryRuntime,
  ViewServerRuntimeError,
} from "@view-server/config";
import { Effect } from "effect";
import * as AtomRef from "effect/unstable/reactivity/AtomRef";
import { healthFromEngine, makeHealthRefreshScheduler, readHealth, refreshHealth } from "./health";

export type { DecodableTopicDefinitions } from "@view-server/column-live-view-engine";

export type ViewServerInMemoryInstance<Topics extends DecodableTopicDefinitions> = {
  readonly client: ViewServerInMemoryRuntime<Topics>;
  readonly liveClient: ViewServerLiveClient<Topics>;
  readonly close: Effect.Effect<void>;
};

export type ViewServerInMemoryOptions = {
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
      code: "InvalidQuery",
      message: error.message,
      topic: error.topic,
    };
  }
  if (error instanceof UnsupportedQueryError) {
    return {
      _tag: "ViewServerRuntimeError",
      code: "UnsupportedQuery",
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

const makeRuntime = Effect.fn("ViewServerInMemory.runtime.make")(<
  const Topics extends DecodableTopicDefinitions,
>(
  engine: ColumnLiveViewEngine<Topics>,
  health: AtomRef.AtomRef<ViewServerHealth<Topics>>,
): Effect.Effect<ViewServerInMemoryRuntime<Topics>> => {
  const requestHealthRefresh = makeHealthRefreshScheduler(refreshHealth(engine, health));
  return Effect.succeed<ViewServerInMemoryRuntime<Topics>>({
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
  });
});

const makeLiveClient = Effect.fn("ViewServerInMemory.liveClient.make")(<
  const Topics extends DecodableTopicDefinitions,
>(
  engine: ColumnLiveViewEngine<Topics>,
  health: AtomRef.AtomRef<ViewServerHealth<Topics>>,
): Effect.Effect<ViewServerLiveClient<Topics>> => {
  const close = engine.close().pipe(Effect.andThen(refreshHealth(engine, health)));
  const readonlyHealth = health.map((value) => value);
  return Effect.succeed<ViewServerLiveClient<Topics>>({
    subscribe: (topic, query) =>
      engine.subscribe(topic, query).pipe(
        Effect.map((subscription) => ({
          events: subscription.events,
          close: () => subscription.close().pipe(Effect.andThen(refreshHealth(engine, health))),
        })),
        Effect.tap(() => refreshHealth(engine, health)),
        Effect.mapError(engineErrorToRuntimeError),
      ),
    health: readonlyHealth,
    close,
  });
});

export const makeInMemoryViewServer = Effect.fn("ViewServerInMemory.make")(function* <
  const Topics extends DecodableTopicDefinitions,
>(
  config: ViewServerConfig<Topics>,
  input: ViewServerInMemoryOptions,
): Effect.fn.Return<ViewServerInMemoryInstance<Topics>> {
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
  const client = yield* makeRuntime(engine, health);
  const liveClient = yield* makeLiveClient(engine, health);
  return {
    client,
    liveClient,
    close: liveClient.close,
  };
});

export const createInMemoryViewServer = <const Topics extends DecodableTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  options: ViewServerInMemoryOptions = {},
): ViewServerInMemoryInstance<Topics> => Effect.runSync(makeInMemoryViewServer(config, options));
