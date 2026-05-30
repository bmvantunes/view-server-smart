import type { DeltaEvent, SnapshotEvent, StatusEvent } from "@view-server/config";
import { Cause, Effect, Option, Queue, Stream } from "effect";
import type { RawQueryExecution } from "./active-query";
import type { TopicStore } from "./topic-store";
import {
  registerTopicStoreSubscription,
  reportTopicStoreSubscriptionBackpressure,
  trackTopicStoreSubscriptionQueueDepth,
  unregisterTopicStoreSubscription,
} from "./topic-store";

type RowObject = object;

type LiveSubscriptionEvent<Row extends RowObject> =
  | SnapshotEvent<Row>
  | DeltaEvent<Row>
  | StatusEvent;

export type LiveTopicSubscriber = {
  readonly topic: string;
  readonly queryId: string;
  readonly notify: (store: TopicStore) => Effect.Effect<void>;
  readonly queuedEvents: Effect.Effect<number>;
  readonly end: Effect.Effect<void>;
  readonly closeWithStatus: (event: StatusEvent) => Effect.Effect<void>;
  maxQueueDepth: number;
  backpressureEvents: number;
  closed: boolean;
};

type MakeLiveSubscriptionOptions<ResultRow extends RowObject> = {
  readonly store: TopicStore;
  readonly queryId: string;
  readonly queueCapacity: number;
  readonly execution: RawQueryExecution<ResultRow>;
  readonly release: Effect.Effect<void>;
};

export type LiveSubscription<ResultRow extends RowObject> = {
  readonly events: Stream.Stream<LiveSubscriptionEvent<ResultRow>>;
  readonly close: () => Effect.Effect<void, never>;
};

const backpressureStatusEvent = (
  store: { readonly topic: string },
  subscriber: { readonly queryId: string },
): StatusEvent => ({
  type: "status",
  topic: store.topic,
  queryId: subscriber.queryId,
  status: "closed",
  code: "BackpressureExceeded",
  message: "Subscription closed because its event queue exceeded capacity.",
});

const closeForBackpressure = Effect.fn(
  "ColumnLiveViewEngine.liveSubscription.closeForBackpressure",
)(function* <ResultRow extends RowObject>(
  store: TopicStore,
  subscriber: LiveTopicSubscriber,
  queue: Queue.Queue<LiveSubscriptionEvent<ResultRow>, Cause.Done>,
  release: Effect.Effect<void>,
) {
  subscriber.closed = true;
  yield* reportTopicStoreSubscriptionBackpressure(store, subscriber);
  yield* unregisterTopicStoreSubscription(store, subscriber);
  yield* release;
  yield* Queue.takeAll(queue).pipe(Effect.ignore);
  yield* Queue.offer(queue, backpressureStatusEvent(store, subscriber)).pipe(Effect.ignore);
  yield* Queue.end(queue);
});

export const makeLiveSubscription = Effect.fn("ColumnLiveViewEngine.liveSubscription.make")(
  function* <ResultRow extends RowObject>(options: MakeLiveSubscriptionOptions<ResultRow>) {
    const { execution, queryId, queueCapacity, store } = options;
    const { release } = options;
    const queue = yield* Queue.dropping<LiveSubscriptionEvent<ResultRow>, Cause.Done>(
      queueCapacity,
    );
    const cursor = execution.createCursor();

    const releaseExecution = release;

    const subscriber: LiveTopicSubscriber = {
      topic: store.topic,
      queryId,
      notify: () =>
        Effect.gen(function* () {
          const nextEvent = yield* execution.next(queryId, cursor);
          if (Option.isNone(nextEvent)) {
            return;
          }
          const offered = yield* Queue.offer(queue, nextEvent.value);
          if (!offered) {
            yield* closeForBackpressure(store, subscriber, queue, releaseExecution);
            return;
          }

          const queueDepth = yield* Queue.size(queue);
          yield* trackTopicStoreSubscriptionQueueDepth(store, subscriber, queueDepth);
        }),
      queuedEvents: Queue.size(queue),
      end: Queue.end(queue),
      closeWithStatus: (event) =>
        Effect.gen(function* () {
          subscriber.closed = true;
          let drained = yield* Queue.poll(queue);
          while (Option.isSome(drained)) {
            drained = yield* Queue.poll(queue);
          }
          yield* releaseExecution;
          yield* Queue.offer(queue, event).pipe(Effect.ignore);
          yield* Queue.end(queue);
        }),
      maxQueueDepth: 0,
      backpressureEvents: 0,
      closed: false,
    };

    yield* registerTopicStoreSubscription(store, subscriber);
    yield* Queue.offer(queue, execution.initial(queryId));
    yield* trackTopicStoreSubscriptionQueueDepth(store, subscriber, yield* Queue.size(queue));

    const close = Effect.fn("ColumnLiveViewEngine.liveSubscription.close")(function* () {
      yield* store.mutationSemaphore.withPermits(1)(
        Effect.gen(function* () {
          if (!subscriber.closed) {
            subscriber.closed = true;
            yield* unregisterTopicStoreSubscription(store, subscriber);
            yield* releaseExecution;
            yield* subscriber.end;
          }
        }),
      );
    });

    return {
      events: Stream.fromQueue(queue).pipe(Stream.ensuring(close())),
      close,
    };
  },
);
