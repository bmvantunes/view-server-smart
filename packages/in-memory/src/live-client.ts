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
        Topic extends typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC | typeof VIEW_SERVER_HEALTH_TOPIC,
        Key extends string,
        Row extends { readonly id: Key },
      >(
        snapshotFromHealth: (
          nextHealth: ViewServerHealth<Topics>,
          updatedAtNanos: bigint,
        ) => Extract<ViewServerLiveEvent<Row, Topic, Key>, { readonly type: "snapshot" }>,
      ) {
        const queue = yield* Queue.bounded<ViewServerLiveEvent<Row, Topic, Key>, Cause.Done>(64);
        const updates = yield* Queue.sliding<ViewServerHealth<Topics>, Cause.Done>(1);
        const subscription = { close: Effect.void };
        let closed = false;
        const offerSnapshot = Effect.fn("ViewServerInMemory.health.snapshot.offer")(function* (
          nextHealth: ViewServerHealth<Topics>,
        ) {
          const updatedAtNanos = yield* Clock.currentTimeNanos;
          yield* Queue.offer(queue, snapshotFromHealth(nextHealth, updatedAtNanos));
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
          makeHealthSubscription<
            typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
            "summary",
            ViewServerHealthSummaryRow<Topics>
          >((nextHealth, updatedAtNanos) => ({
            type: "snapshot",
            topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
            queryId: "health-summary",
            version: nextHealth.version,
            keys: ["summary"],
            rows: [viewServerHealthSummaryRowFromHealth(nextHealth, updatedAtNanos)],
            totalRows: 1,
          })),
        subscribeHealth: () =>
          makeHealthSubscription<
            typeof VIEW_SERVER_HEALTH_TOPIC,
            Extract<keyof Topics, string>,
            ViewServerHealthTopicRow<Extract<keyof Topics, string>>
          >((nextHealth, updatedAtNanos) => {
            const rows = viewServerHealthTopicRowsFromHealth(nextHealth, updatedAtNanos);
            return {
              type: "snapshot",
              topic: VIEW_SERVER_HEALTH_TOPIC,
              queryId: "health",
              version: nextHealth.version,
              keys: rows.map((row) => row.id),
              rows,
              totalRows: rows.length,
            };
          }),
        health: readonlyHealth,
        close,
      };
    }),
);
