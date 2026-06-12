import type {
  ColumnLiveViewEngine,
  DecodableTopicDefinitions,
} from "@view-server/column-live-view-engine";
import type {
  ViewServerLiveEvent,
  ViewServerLiveSubscription,
  ViewServerRuntimeLiveClient,
} from "@view-server/client";
import {
  ignoreLoggedTypedFailuresPreserveNonTypedFailures,
  runAllFinalizers,
} from "@view-server/effect-utils";
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
import { Cause, Clock, Effect, Fiber, Queue, Semaphore, Stream } from "effect";
import type { AtomRef } from "effect/unstable/reactivity";
import { engineErrorToRuntimeError } from "./runtime-error";

const runtimeClosedError: ViewServerRuntimeError = {
  _tag: "ViewServerRuntimeError",
  code: "RuntimeUnavailable",
  message: "Runtime Core is closed.",
};

const ignoreHealthSubscriptionCloseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring runtime health subscription close failure.",
);

const ignoreLiveSubscriptionCloseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring runtime live subscription close failure.",
);

const ignoreRuntimeHealthRefreshFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring runtime health refresh failure.",
);

export const makeRuntimeCoreLiveClient = Effect.fn("ViewServerRuntimeCore.liveClient.make")(
  <const Topics extends DecodableTopicDefinitions>(
    engine: ColumnLiveViewEngine<Topics>,
    health: AtomRef.AtomRef<ViewServerHealth<Topics>>,
    refreshHealth: Effect.Effect<ViewServerHealth<Topics>, ViewServerRuntimeError>,
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
        const closeRefresh = ignoreRuntimeHealthRefreshFailure(refreshHealth);
        return engine.subscribe<Topic, Query>(topic, query).pipe(
          Effect.mapError(engineErrorToRuntimeError),
          Effect.flatMap((subscription) =>
            refreshHealth.pipe(
              Effect.as({
                events: subscription.events,
                close: () => subscription.close().pipe(Effect.andThen(closeRefresh)),
              }),
              Effect.onError(() => subscription.close().pipe(ignoreLiveSubscriptionCloseFailure)),
            ),
          ),
        );
      }
      const subscribeRuntime: ViewServerRuntimeLiveClient<Topics>["subscribeRuntime"] = (
        topic,
        query,
      ) => {
        const closeRefresh = ignoreRuntimeHealthRefreshFailure(refreshHealth);
        return engine.subscribeRuntime(topic, query).pipe(
          Effect.mapError(engineErrorToRuntimeError),
          Effect.flatMap((subscription) =>
            refreshHealth.pipe(
              Effect.as({
                events: subscription.events,
                close: () => subscription.close().pipe(Effect.andThen(closeRefresh)),
              }),
              Effect.onError(() => subscription.close().pipe(ignoreLiveSubscriptionCloseFailure)),
            ),
          ),
        );
      };
      const activeHealthSubscriptions = new Set<{ close: Effect.Effect<void> }>();
      const healthSubscriptionLock = Semaphore.makeUnsafe(1);
      let healthSubscriptionsClosed = false;
      const closeActiveHealthSubscriptions = Effect.suspend(() =>
        healthSubscriptionLock
          .withPermit(
            Effect.sync(() => {
              healthSubscriptionsClosed = true;
              const subscriptions = Array.from(activeHealthSubscriptions);
              activeHealthSubscriptions.clear();
              return subscriptions;
            }),
          )
          .pipe(
            Effect.andThen((subscriptions) =>
              runAllFinalizers(subscriptions.map((subscription) => subscription.close)),
            ),
          ),
      ).pipe(ignoreHealthSubscriptionCloseFailure);
      const close = runAllFinalizers([
        closeActiveHealthSubscriptions,
        engine.close(),
        ignoreRuntimeHealthRefreshFailure(refreshHealth),
      ]);
      const readonlyHealth = health.map((value) => value);
      const makeHealthSubscription = Effect.fn("ViewServerRuntimeCore.health.subscribe")(function* <
        Topic extends typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC | typeof VIEW_SERVER_HEALTH_TOPIC,
        Key extends string,
        Row extends { readonly id: Key },
      >(
        snapshotFromHealth: (
          nextHealth: ViewServerHealth<Topics>,
          updatedAtNanos: bigint,
        ) => Extract<ViewServerLiveEvent<Row, Topic, Key>, { readonly type: "snapshot" }>,
      ) {
        return yield* Effect.uninterruptible(
          Effect.gen(function* () {
            const queue = yield* Queue.bounded<ViewServerLiveEvent<Row, Topic, Key>, Cause.Done>(
              64,
            );
            const updates = yield* Queue.sliding<ViewServerHealth<Topics>, Cause.Done>(1);
            const subscription = { close: Effect.void };
            let subscriptionClosed = false;
            const offerSnapshot = Effect.fn("ViewServerRuntimeCore.health.snapshot.offer")(
              function* (nextHealth: ViewServerHealth<Topics>) {
                const updatedAtNanos = yield* Clock.currentTimeNanos;
                yield* Queue.offer(queue, snapshotFromHealth(nextHealth, updatedAtNanos));
              },
            );
            const pumpFiber = yield* Stream.fromQueue(updates).pipe(
              Stream.runForEach(offerSnapshot),
              Effect.forkDetach({ startImmediately: true }),
            );
            const unsubscribe = health.subscribe((nextHealth) => {
              Queue.offerUnsafe(updates, nextHealth);
            });
            const releaseSubscription = Effect.gen(function* () {
              const shouldClose = yield* healthSubscriptionLock.withPermit(
                Effect.sync(() => {
                  if (subscriptionClosed) {
                    return false;
                  }
                  subscriptionClosed = true;
                  unsubscribe();
                  activeHealthSubscriptions.delete(subscription);
                  return true;
                }),
              );
              if (shouldClose) {
                yield* Queue.end(updates);
                yield* Fiber.interrupt(pumpFiber).pipe(Effect.asVoid);
                yield* Queue.end(queue);
              }
            });
            const registered = yield* healthSubscriptionLock.withPermit(
              Effect.sync(() => {
                subscription.close = releaseSubscription;
                if (healthSubscriptionsClosed) {
                  return false;
                }
                activeHealthSubscriptions.add(subscription);
                return true;
              }),
            );
            if (!registered) {
              yield* releaseSubscription;
              return yield* Effect.fail(runtimeClosedError);
            }
            const latestHealth = yield* refreshHealth.pipe(
              Effect.onError(() => releaseSubscription),
            );
            yield* offerSnapshot(latestHealth);
            return {
              events: Stream.fromQueue(queue).pipe(Stream.ensuring(subscription.close)),
              close: () => subscription.close,
            };
          }),
        );
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
