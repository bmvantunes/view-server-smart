import type { DeltaEvent, DeltaOperation, SnapshotEvent, StatusEvent } from "@view-server/config";
import { Cause, Effect, Queue, Stream } from "effect";
import {
  evaluateCompiledRawQuery,
  type CompiledRawQuery,
  type QueryEvaluation,
} from "./raw-query-compiler.js";
import { cloneRow, rowsEqual } from "./row-values.js";

type RowObject = object;

type LiveSubscriptionEvent<Row extends RowObject> =
  | SnapshotEvent<Row>
  | DeltaEvent<Row>
  | StatusEvent;

export type LiveTopicStoreState<Row extends RowObject> = {
  readonly topic: string;
  readonly rows: ReadonlyMap<string, Row>;
  readonly version: number;
  readonly subscribers: Set<LiveTopicSubscriber<Row>>;
  maxQueueDepth: number;
  backpressureEvents: number;
};

export type LiveTopicSubscriber<Row extends RowObject> = {
  readonly topic: string;
  readonly queryId: string;
  readonly notify: (store: LiveTopicStoreState<Row>) => Effect.Effect<void>;
  readonly queuedEvents: Effect.Effect<number>;
  readonly end: Effect.Effect<void>;
  maxQueueDepth: number;
  backpressureEvents: number;
  closed: boolean;
};

type MakeLiveSubscriptionOptions<StoreRow extends RowObject, ResultRow extends RowObject> = {
  readonly store: LiveTopicStoreState<StoreRow>;
  readonly queryId: string;
  readonly compiled: CompiledRawQuery<StoreRow, ResultRow>;
  readonly queueCapacity: number;
};

export type LiveSubscription<ResultRow extends RowObject> = {
  readonly events: Stream.Stream<LiveSubscriptionEvent<ResultRow>>;
  readonly close: () => Effect.Effect<void, never>;
};

const snapshotEvent = <Row extends RowObject>(
  store: { readonly topic: string },
  queryId: string,
  evaluation: QueryEvaluation<Row>,
): SnapshotEvent<Row> => ({
  type: "snapshot",
  topic: store.topic,
  queryId,
  version: evaluation.version,
  keys: [...evaluation.keys],
  rows: evaluation.rows.map(cloneRow),
  totalRows: evaluation.totalRows,
});

const deltaOperations = <Row extends RowObject>(
  previous: QueryEvaluation<Row>,
  next: QueryEvaluation<Row>,
): ReadonlyArray<DeltaOperation<Row>> => {
  const operations: Array<DeltaOperation<Row>> = [];
  const nextKeys = new Set(next.keys);
  const currentKeys = [...previous.keys];
  const currentRows = [...previous.rows];

  for (const key of previous.keys) {
    if (!nextKeys.has(key)) {
      const index = currentKeys.indexOf(key);
      currentKeys.splice(index, 1);
      currentRows.splice(index, 1);
      operations.push({
        type: "remove",
        key,
      });
    }
  }

  for (const [index, { key, row }] of next.window.entries()) {
    const currentIndex = currentKeys.indexOf(key);
    if (currentIndex < 0) {
      currentKeys.splice(index, 0, key);
      currentRows.splice(index, 0, row);
      operations.push({
        type: "insert",
        key,
        row,
        index,
      });
      continue;
    }

    if (currentIndex !== index) {
      const currentRow = currentRows[currentIndex]!;
      currentKeys.splice(currentIndex, 1);
      currentRows.splice(currentIndex, 1);
      currentKeys.splice(index, 0, key);
      currentRows.splice(index, 0, currentRow);
      operations.push({
        type: "move",
        key,
        fromIndex: currentIndex,
        toIndex: index,
      });
    }

    const currentRow = currentRows[index];
    if (currentRow === undefined || !rowsEqual(currentRow, row)) {
      currentRows[index] = row;
      operations.push({
        type: "update",
        key,
        row,
        index,
      });
    }
  }

  return operations;
};

const cloneDeltaOperations = <Row extends RowObject>(
  operations: ReadonlyArray<DeltaOperation<Row>>,
): ReadonlyArray<DeltaOperation<Row>> =>
  operations.map((operation) => {
    if (operation.type === "insert" || operation.type === "update") {
      return {
        ...operation,
        row: cloneRow(operation.row),
      };
    }
    return operation;
  });

const deltaEvent = <Row extends RowObject>(
  store: { readonly topic: string },
  queryId: string,
  fromVersion: number,
  next: QueryEvaluation<Row>,
  operations: ReadonlyArray<DeltaOperation<Row>>,
): DeltaEvent<Row> => ({
  type: "delta",
  topic: store.topic,
  queryId,
  fromVersion,
  toVersion: next.version,
  operations: cloneDeltaOperations(operations),
  totalRows: next.totalRows,
});

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

const updateQueueDepth = <Row extends RowObject>(
  store: LiveTopicStoreState<Row>,
  subscriber: LiveTopicSubscriber<Row>,
  queueDepth: number,
): void => {
  subscriber.maxQueueDepth = Math.max(subscriber.maxQueueDepth, queueDepth);
  store.maxQueueDepth = Math.max(store.maxQueueDepth, subscriber.maxQueueDepth);
};

const closeForBackpressure = Effect.fn(
  "ColumnLiveViewEngine.liveSubscription.closeForBackpressure",
)(function* <Row extends RowObject, ResultRow extends RowObject>(
  store: LiveTopicStoreState<Row>,
  subscriber: LiveTopicSubscriber<Row>,
  queue: Queue.Queue<LiveSubscriptionEvent<ResultRow>, Cause.Done>,
) {
  subscriber.backpressureEvents += 1;
  store.backpressureEvents += 1;
  subscriber.closed = true;
  store.subscribers.delete(subscriber);
  yield* Queue.takeAll(queue).pipe(Effect.ignore);
  yield* Queue.offer(queue, backpressureStatusEvent(store, subscriber)).pipe(Effect.ignore);
  yield* Queue.end(queue);
});

export const makeLiveSubscription = Effect.fn("ColumnLiveViewEngine.liveSubscription.make")(
  function* <StoreRow extends RowObject, ResultRow extends RowObject>(
    options: MakeLiveSubscriptionOptions<StoreRow, ResultRow>,
  ) {
    const { compiled, queryId, queueCapacity, store } = options;
    const queue = yield* Queue.dropping<LiveSubscriptionEvent<ResultRow>, Cause.Done>(
      queueCapacity,
    );
    let evaluation = evaluateCompiledRawQuery(store, compiled);

    const subscriber: LiveTopicSubscriber<StoreRow> = {
      topic: store.topic,
      queryId,
      notify: (currentStore) =>
        Effect.gen(function* () {
          const previous = evaluation;
          const next = evaluateCompiledRawQuery(currentStore, compiled);
          const operations = deltaOperations(previous, next);
          if (operations.length === 0 && previous.totalRows === next.totalRows) {
            return;
          }

          const offered = yield* Queue.offer(
            queue,
            deltaEvent(currentStore, queryId, previous.version, next, operations),
          );
          if (!offered) {
            yield* closeForBackpressure(currentStore, subscriber, queue);
            return;
          }

          const queueDepth = yield* Queue.size(queue);
          updateQueueDepth(currentStore, subscriber, queueDepth);
          evaluation = next;
        }),
      queuedEvents: Queue.size(queue),
      end: Queue.end(queue),
      maxQueueDepth: 0,
      backpressureEvents: 0,
      closed: false,
    };

    store.subscribers.add(subscriber);
    yield* Queue.offer(queue, snapshotEvent(store, queryId, evaluation));
    updateQueueDepth(store, subscriber, yield* Queue.size(queue));

    const close = Effect.fn("ColumnLiveViewEngine.liveSubscription.close")(function* () {
      if (!subscriber.closed) {
        subscriber.closed = true;
        store.subscribers.delete(subscriber);
        yield* subscriber.end;
      }
    });

    return {
      events: Stream.fromQueue(queue).pipe(Stream.ensuring(close())),
      close,
    };
  },
);
