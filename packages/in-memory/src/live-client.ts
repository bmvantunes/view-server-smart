import type {
  ColumnLiveViewEngine,
  DecodableTopicDefinitions,
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
  RawQuery,
  TopicRow,
  ViewServerHealth,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
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
import type { AtomRef } from "effect/unstable/reactivity";
import { readHealth, refreshHealth } from "./health";
import { engineErrorToRuntimeError } from "./runtime-error";

export const makeInMemoryLiveClient = Effect.fn("ViewServerInMemory.liveClient.make")(
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
