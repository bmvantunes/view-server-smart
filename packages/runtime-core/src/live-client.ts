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
  ExactLiveQueryInputForTopic,
  GroupedQuery,
  LiveQueryRow,
  RawQuery,
  TopicRow,
  ViewServerConfig,
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
import { Cause, Clock, Effect, Exit, Queue, Scope, Semaphore, Stream } from "effect";
import type { AtomRef } from "effect/unstable/reactivity";
import { engineErrorToRuntimeError, leasedRuntimeAccessError } from "./runtime-error";
import { grpcLeasedSourceTopics } from "./topic-source";

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

export type ViewServerRuntimeCoreEngineQueryInput<
  Topics extends DecodableTopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Query,
> = ExactLiveQueryInput<TopicRow<Topics, Topic>, Query>;

export type ViewServerRuntimeCoreInternalLiveClient<Topics extends DecodableTopicDefinitions> = {
  readonly subscribeInternal: <
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  ) => Effect.Effect<
    ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  readonly subscribeRuntimeInternal: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    query: RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  ) => Effect.Effect<
    ViewServerLiveSubscription<object>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
};

const runtimeHealthBackpressureStatus = <Topic extends string>(
  topic: Topic,
  queryId: string,
  queuedEvents: number,
) =>
  ({
    type: "status",
    topic,
    queryId,
    status: "closed",
    code: "BackpressureExceeded",
    message: `Runtime health subscription closed because its event queue exceeded capacity with ${queuedEvents} queued event(s).`,
  }) satisfies ViewServerLiveEvent<never, Topic, never>;

export const makeRuntimeCoreLiveClient = Effect.fn("ViewServerRuntimeCore.liveClient.make")(
  <const Topics extends DecodableTopicDefinitions>(
    config: ViewServerConfig<Topics>,
    engine: ColumnLiveViewEngine<Topics>,
    health: AtomRef.AtomRef<ViewServerHealth<Topics>>,
    refreshHealth: Effect.Effect<ViewServerHealth<Topics>, ViewServerRuntimeError>,
  ): Effect.Effect<
    ViewServerRuntimeLiveClient<Topics> & ViewServerRuntimeCoreInternalLiveClient<Topics>
  > =>
    Effect.sync<
      ViewServerRuntimeLiveClient<Topics> & ViewServerRuntimeCoreInternalLiveClient<Topics>
    >(() => {
      const leasedTopics = grpcLeasedSourceTopics(config);
      function subscribeInternal<
        Topic extends Extract<keyof Topics, string>,
        const Query extends
          | RawQuery<TopicRow<Topics, Topic>>
          | GroupedQuery<TopicRow<Topics, Topic>>,
      >(
        topic: Topic,
        query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
      ): Effect.Effect<
        ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
        ViewServerRuntimeError | ViewServerTransportError
      >;
      function subscribeInternal<
        Topic extends Extract<keyof Topics, string>,
        const Query extends
          | RawQuery<TopicRow<Topics, Topic>>
          | GroupedQuery<TopicRow<Topics, Topic>>,
      >(
        topic: Topic,
        query: ViewServerRuntimeCoreEngineQueryInput<Topics, Topic, Query>,
      ): Effect.Effect<
        ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
        ViewServerRuntimeError | ViewServerTransportError
      > {
        return Effect.suspend(() => {
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
        });
      }
      const subscribeRuntimeInternal: ViewServerRuntimeCoreInternalLiveClient<Topics>["subscribeRuntimeInternal"] =
        (topic, query) =>
          Effect.suspend(() => {
            const closeRefresh = ignoreRuntimeHealthRefreshFailure(refreshHealth);
            return engine.subscribeRuntime(topic, query).pipe(
              Effect.mapError(engineErrorToRuntimeError),
              Effect.flatMap((subscription) =>
                refreshHealth.pipe(
                  Effect.as({
                    events: subscription.events,
                    close: () => subscription.close().pipe(Effect.andThen(closeRefresh)),
                  }),
                  Effect.onError(() =>
                    subscription.close().pipe(ignoreLiveSubscriptionCloseFailure),
                  ),
                ),
              ),
            );
          });
      function subscribe<
        Topic extends Extract<keyof Topics, string>,
        const Query extends
          | RawQuery<TopicRow<Topics, Topic>>
          | GroupedQuery<TopicRow<Topics, Topic>>,
      >(
        topic: Topic,
        query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
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
        query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
      ): Effect.Effect<
        ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
        ViewServerRuntimeError | ViewServerTransportError
      > {
        return Effect.suspend(() => {
          if (leasedTopics.has(topic)) {
            return Effect.fail(leasedRuntimeAccessError(topic));
          }
          return subscribeInternal<Topic, Query>(topic, query);
        });
      }
      const subscribeRuntime: ViewServerRuntimeLiveClient<Topics>["subscribeRuntime"] = (
        topic,
        query,
      ) => {
        return Effect.suspend(() => {
          if (leasedTopics.has(topic)) {
            return Effect.fail(leasedRuntimeAccessError(topic));
          }
          return subscribeRuntimeInternal(topic, query);
        });
      };
      type ActiveHealthSubscription = {
        close: Effect.Effect<void>;
        claimClosed: () => boolean;
        finishClosed: Effect.Effect<void>;
      };
      const activeHealthSubscriptions = new Set<ActiveHealthSubscription>();
      const healthSubscriptionLock = Semaphore.makeUnsafe(1);
      let healthSubscriptionsClosed = false;
      const closeActiveHealthSubscriptions = Effect.suspend(() =>
        healthSubscriptionLock
          .withPermit(
            Effect.sync(() => {
              healthSubscriptionsClosed = true;
              const subscriptions = Array.from(activeHealthSubscriptions);
              const claimedSubscriptions = subscriptions.filter((subscription) =>
                subscription.claimClosed(),
              );
              activeHealthSubscriptions.clear();
              return claimedSubscriptions;
            }),
          )
          .pipe(
            Effect.andThen((subscriptions) =>
              runAllFinalizers(subscriptions.map((subscription) => subscription.finishClosed)),
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
        topic: Topic,
        queryId: string,
        snapshotFromHealth: (
          nextHealth: ViewServerHealth<Topics>,
          updatedAtNanos: bigint,
        ) => Extract<ViewServerLiveEvent<Row, Topic, Key>, { readonly type: "snapshot" }>,
      ) {
        return yield* Effect.uninterruptible(
          Effect.gen(function* () {
            const queue = yield* Queue.dropping<ViewServerLiveEvent<Row, Topic, Key>, Cause.Done>(
              64,
            );
            const updates = yield* Queue.sliding<ViewServerHealth<Topics>, Cause.Done>(1);
            const subscriptionScope = yield* Scope.make("parallel");
            const closeSubscriptionScope = Scope.close(subscriptionScope, Exit.void);
            let subscriptionClosed = false;
            let unsubscribe: () => void;
            const claimClosed = () => {
              if (subscriptionClosed) {
                return false;
              }
              subscriptionClosed = true;
              unsubscribe();
              return true;
            };
            const subscription: ActiveHealthSubscription = {
              close: Effect.void,
              claimClosed,
              finishClosed: Effect.void,
            };
            type HealthSubscriptionCloseReason =
              | {
                  readonly _tag: "normal";
                }
              | {
                  readonly _tag: "backpressure";
                  readonly queuedEvents: number;
                };
            type HealthSnapshotOfferResult =
              | {
                  readonly _tag: "offered";
                }
              | {
                  readonly _tag: "closed";
                }
              | {
                  readonly _tag: "backpressure";
                  readonly queuedEvents: number;
                };
            const releaseSubscriptionResources = Effect.fn(
              "ViewServerRuntimeCore.health.subscription.releaseResources",
            )(function* () {
              yield* Queue.end(updates);
              yield* closeSubscriptionScope;
            });
            const finishClosedSubscription = Effect.fn(
              "ViewServerRuntimeCore.health.subscription.finishClosed",
            )(function* (reason: HealthSubscriptionCloseReason) {
              if (reason._tag === "backpressure") {
                yield* Queue.clear(queue);
                yield* Queue.offer(
                  queue,
                  runtimeHealthBackpressureStatus(topic, queryId, reason.queuedEvents),
                );
              }
              yield* Queue.end(queue);
              yield* releaseSubscriptionResources();
            });
            const closeSubscription = Effect.fn("ViewServerRuntimeCore.health.subscription.close")(
              function* (reason: HealthSubscriptionCloseReason) {
                const shouldClose = yield* healthSubscriptionLock.withPermit(
                  Effect.sync(() => {
                    const claimed = subscription.claimClosed();
                    if (claimed) {
                      activeHealthSubscriptions.delete(subscription);
                    }
                    return claimed;
                  }),
                );
                if (shouldClose) {
                  yield* finishClosedSubscription(reason);
                }
              },
            );
            const releaseSubscriptionAndEndQueue = () => closeSubscription({ _tag: "normal" });
            subscription.finishClosed = finishClosedSubscription({ _tag: "normal" });
            const offerSnapshot = Effect.fn("ViewServerRuntimeCore.health.snapshot.offer")(
              function* (nextHealth: ViewServerHealth<Topics>) {
                const updatedAtNanos = yield* Clock.currentTimeNanos;
                const snapshot = snapshotFromHealth(nextHealth, updatedAtNanos);
                const offerResult: HealthSnapshotOfferResult =
                  yield* healthSubscriptionLock.withPermit(
                    Effect.gen(function* () {
                      if (subscriptionClosed || healthSubscriptionsClosed) {
                        const closed: HealthSnapshotOfferResult = { _tag: "closed" };
                        return closed;
                      }
                      const offered = yield* Queue.offer(queue, snapshot);
                      if (offered) {
                        const offeredResult: HealthSnapshotOfferResult = { _tag: "offered" };
                        return offeredResult;
                      }
                      const queuedEvents = yield* Queue.size(queue);
                      subscriptionClosed = true;
                      unsubscribe();
                      activeHealthSubscriptions.delete(subscription);
                      const backpressure: HealthSnapshotOfferResult = {
                        _tag: "backpressure",
                        queuedEvents,
                      };
                      return backpressure;
                    }),
                  );
                if (offerResult._tag === "closed") {
                  return yield* Effect.fail(runtimeClosedError);
                }
                if (offerResult._tag === "backpressure") {
                  yield* finishClosedSubscription(offerResult);
                  return yield* Effect.interrupt;
                }
              },
            );
            unsubscribe = health.subscribe((nextHealth) => {
              Queue.offerUnsafe(updates, nextHealth);
            });
            const registered = yield* healthSubscriptionLock.withPermit(
              Effect.sync(() => {
                subscription.close = releaseSubscriptionAndEndQueue();
                if (healthSubscriptionsClosed) {
                  return false;
                }
                activeHealthSubscriptions.add(subscription);
                return true;
              }),
            );
            if (!registered) {
              yield* releaseSubscriptionAndEndQueue();
              return yield* Effect.fail(runtimeClosedError);
            }
            const latestHealth = yield* refreshHealth.pipe(
              Effect.onError(() => releaseSubscriptionAndEndQueue()),
            );
            yield* offerSnapshot(latestHealth);
            yield* Stream.fromQueue(updates).pipe(
              Stream.runForEach(offerSnapshot),
              Effect.forkIn(subscriptionScope, { startImmediately: true }),
            );
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
        subscribeInternal,
        subscribeRuntimeInternal,
        subscribeHealthSummary: () =>
          makeHealthSubscription<
            typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
            "summary",
            ViewServerHealthSummaryRow<Topics>
          >(VIEW_SERVER_HEALTH_SUMMARY_TOPIC, "health-summary", (nextHealth, updatedAtNanos) => ({
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
          >(VIEW_SERVER_HEALTH_TOPIC, "health", (nextHealth, updatedAtNanos) => {
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
