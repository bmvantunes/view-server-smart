import {
  createColumnLiveViewEngine,
  InvalidQueryError,
  InvalidRowError,
  InvalidTopicError,
  type ColumnLiveViewEngine,
  type ColumnLiveViewEngineError,
  type DecodableTopicDefinitions,
} from "@view-server/column-live-view-engine";
import type {
  ViewServerLiveEvent,
  ViewServerLiveSubscription,
  ViewServerRuntimeLiveClient,
} from "@view-server/client";
import type {
  ExactLiveQueryInput,
  GroupedQuery,
  LiveQueryRow,
  LiveQueryResult,
  RawQuery,
  TopicRow,
  ViewServerConfig,
  ViewServerHealth,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
  ViewServerInMemoryRuntime,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@view-server/config";
import {
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
  viewServerHealthSummaryRowFromHealth,
  viewServerHealthTopicRowsFromHealth,
} from "@view-server/config";
import { Cause, Clock, Effect, Queue, Stream } from "effect";
import { AtomRef } from "effect/unstable/reactivity";
import { healthFromEngine, makeHealthRefreshScheduler, readHealth, refreshHealth } from "./health";

export type { DecodableTopicDefinitions } from "@view-server/column-live-view-engine";

export type ViewServerInMemoryInstance<Topics extends DecodableTopicDefinitions> = {
  readonly client: ViewServerInMemoryRuntime<Topics>;
  readonly liveClient: ViewServerRuntimeLiveClient<Topics>;
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
  const snapshot = <
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInput<TopicRow<Topics, Topic>, Query>,
  ): Effect.Effect<
    LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError
  > => engine.snapshot<Topic, Query>(topic, query).pipe(Effect.mapError(engineErrorToRuntimeError));
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
    snapshot,
    health: () => readHealth(engine, health).pipe(Effect.mapError(engineErrorToRuntimeError)),
    reset: () =>
      engine.reset().pipe(
        Effect.tap(() => requestHealthRefresh),
        Effect.mapError(engineErrorToRuntimeError),
      ),
  });
});

const makeLiveClient = Effect.fn("ViewServerInMemory.liveClient.make")(
  <const Topics extends DecodableTopicDefinitions>(
    engine: ColumnLiveViewEngine<Topics>,
    health: AtomRef.AtomRef<ViewServerHealth<Topics>>,
  ): Effect.Effect<ViewServerRuntimeLiveClient<Topics>> =>
    Effect.sync<ViewServerRuntimeLiveClient<Topics>>(() => {
      function subscribe<
        Topic extends Extract<keyof Topics, string>,
        const Query extends
          | RawQuery<TopicRow<Topics, Topic>>
          | GroupedQuery<TopicRow<Topics, Topic>>,
      >(
        topic: Topic,
        query: ExactLiveQueryInput<TopicRow<Topics, Topic>, Query>,
      ): Effect.Effect<
        ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
        ViewServerRuntimeError | ViewServerTransportError
      >;
      function subscribe<
        Topic extends Extract<keyof Topics, string>,
        const Query extends
          | RawQuery<TopicRow<Topics, Topic>>
          | GroupedQuery<TopicRow<Topics, Topic>>,
      >(
        topic: Topic,
        query: ExactLiveQueryInput<TopicRow<Topics, Topic>, Query>,
      ): Effect.Effect<
        ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
        ViewServerRuntimeError | ViewServerTransportError
      > {
        return engine.subscribe<Topic, Query>(topic, query).pipe(
          Effect.map((subscription) => ({
            events: subscription.events,
            close: () => subscription.close().pipe(Effect.andThen(refreshHealth(engine, health))),
          })),
          Effect.tap(() => refreshHealth(engine, health)),
          Effect.mapError(engineErrorToRuntimeError),
        );
      }
      const subscribeRuntime: ViewServerRuntimeLiveClient<Topics>["subscribeRuntime"] = (
        topic,
        query,
      ) =>
        engine.subscribeRuntime(topic, query).pipe(
          Effect.map((subscription) => ({
            events: subscription.events,
            close: () => subscription.close().pipe(Effect.andThen(refreshHealth(engine, health))),
          })),
          Effect.tap(() => refreshHealth(engine, health)),
          Effect.mapError(engineErrorToRuntimeError),
        );
      const activeHealthSubscriptions = new Set<{ close: Effect.Effect<void> }>();
      const closeActiveHealthSubscriptions = Effect.suspend(() =>
        Effect.forEach(
          Array.from(activeHealthSubscriptions),
          (subscription) => subscription.close,
          {
            discard: true,
          },
        ),
      ).pipe(Effect.ignore);
      const close = closeActiveHealthSubscriptions.pipe(
        Effect.andThen(engine.close()),
        Effect.andThen(refreshHealth(engine, health)),
      );
      const readonlyHealth = health.map((value) => value);
      const makeHealthSubscription = Effect.fn("ViewServerInMemory.health.subscribe")(function* <
        Row extends { readonly id: string },
      >(
        topic: typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC | typeof VIEW_SERVER_HEALTH_TOPIC,
        queryId: string,
        rowsFromHealth: (
          nextHealth: ViewServerHealth<Topics>,
          updatedAtNanos: bigint,
        ) => ReadonlyArray<Row>,
      ) {
        const queue = yield* Queue.bounded<ViewServerLiveEvent<Row>, Cause.Done>(64);
        const updates = yield* Queue.sliding<ViewServerHealth<Topics>, Cause.Done>(1);
        const subscription = { close: Effect.void };
        let closed = false;
        const offerSnapshot = Effect.fn("ViewServerInMemory.health.snapshot.offer")(function* (
          nextHealth: ViewServerHealth<Topics>,
        ) {
          const updatedAtNanos = yield* Clock.currentTimeNanos;
          const rows = rowsFromHealth(nextHealth, updatedAtNanos);
          yield* Queue.offer(queue, {
            type: "snapshot",
            topic,
            queryId,
            version: nextHealth.version,
            keys: rows.map((row) => row.id),
            rows,
            totalRows: rows.length,
          });
        });
        const unsubscribe = health.subscribe((nextHealth) => {
          Queue.offerUnsafe(updates, nextHealth);
        });
        const latestHealth = yield* readHealth(engine, health).pipe(
          Effect.mapError(engineErrorToRuntimeError),
        );
        yield* offerSnapshot(latestHealth);
        yield* Stream.fromQueue(updates).pipe(
          Stream.runForEach(offerSnapshot),
          Effect.forkChild({ startImmediately: true }),
        );
        const releaseSubscription = Effect.gen(function* () {
          const shouldClose = yield* Effect.sync(() => {
            if (closed) {
              return false;
            }
            closed = true;
            unsubscribe();
            activeHealthSubscriptions.delete(subscription);
            return true;
          });
          if (shouldClose) {
            yield* Queue.end(updates).pipe(Effect.ignore);
            yield* Queue.end(queue);
          }
        });
        yield* Effect.sync(() => {
          subscription.close = releaseSubscription;
          activeHealthSubscriptions.add(subscription);
        });
        return {
          events: Stream.fromQueue(queue).pipe(Stream.ensuring(subscription.close)),
          close: () => subscription.close,
        };
      });
      return {
        subscribe,
        subscribeRuntime,
        subscribeHealthSummary: () =>
          makeHealthSubscription<ViewServerHealthSummaryRow<Topics>>(
            VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
            "health-summary",
            (nextHealth, updatedAtNanos) => [
              viewServerHealthSummaryRowFromHealth(nextHealth, updatedAtNanos),
            ],
          ),
        subscribeHealth: () =>
          makeHealthSubscription<ViewServerHealthTopicRow<Extract<keyof Topics, string>>>(
            VIEW_SERVER_HEALTH_TOPIC,
            "health",
            viewServerHealthTopicRowsFromHealth,
          ),
        health: readonlyHealth,
        close,
      };
    }),
);

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
    const health = AtomRef.make(healthFromEngine(engineHealth));
    const client = yield* makeRuntime(engine, health);
    const liveClient = yield* makeLiveClient(engine, health);
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
