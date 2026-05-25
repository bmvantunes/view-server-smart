import { Effect, Schema, Semaphore } from "effect";
import type { StatusEvent } from "@view-server/config";
import type { HealthTopicStoreState } from "./engine-health";
import type { LiveTopicStoreState, LiveTopicSubscriber } from "./live-subscription";
import { rawQueryCompilerMetadata, type RawQueryCompilerMetadata } from "./raw-query-compiler";
import { cloneRow, fieldValue } from "./row-values";

type RowObject = object;

export type InvalidRowErrorFactory<Error> = (topic: string, message: string) => Error;

export class TopicStore<Row extends RowObject>
  implements HealthTopicStoreState<Row>, LiveTopicStoreState<Row>
{
  readonly rows = new Map<string, Row>();
  readonly subscribers = new Set<LiveTopicSubscriber<Row>>();
  readonly mutationSemaphore = Semaphore.makeUnsafe(1);
  readonly rawQueryMetadata: RawQueryCompilerMetadata;
  version = 0;
  maxQueueDepth = 0;
  backpressureEvents = 0;

  constructor(
    readonly topic: string,
    readonly schema: Schema.Decoder<object>,
    readonly keyField: string,
    readonly onCommit: () => void,
  ) {
    this.rawQueryMetadata = rawQueryCompilerMetadata(schema);
  }
}

const resetStatusEvent = <Row extends RowObject>(
  store: TopicStore<Row>,
  subscriber: LiveTopicSubscriber<Row>,
): StatusEvent => ({
  type: "status",
  topic: store.topic,
  queryId: subscriber.queryId,
  status: "closed",
  code: "SubscriptionClosed",
  message: "Subscription closed because the engine reset.",
});

const commitTopicStore = Effect.fn("ColumnLiveViewEngine.topicStore.commit")(function* <
  Row extends RowObject,
>(store: TopicStore<Row>) {
  store.version += 1;
  store.onCommit();
  for (const subscriber of store.subscribers) {
    yield* subscriber.notify(store);
  }
});

export const resetTopicStore = Effect.fn("ColumnLiveViewEngine.topicStore.reset")(function* <
  Row extends RowObject,
>(store: TopicStore<Row>) {
  store.rows.clear();
  store.version = 0;
  for (const subscriber of store.subscribers) {
    subscriber.closed = true;
    yield* subscriber.closeWithStatus(resetStatusEvent(store, subscriber));
  }
  store.subscribers.clear();
  store.maxQueueDepth = 0;
  store.backpressureEvents = 0;
});

export const closeTopicStoreSubscriptions = Effect.fn(
  "ColumnLiveViewEngine.topicStore.closeSubscriptions",
)(function* <Row extends RowObject>(store: TopicStore<Row>) {
  for (const subscriber of store.subscribers) {
    subscriber.closed = true;
    yield* subscriber.end;
  }
  store.subscribers.clear();
});

const decodeRow = Effect.fn("ColumnLiveViewEngine.topicStore.decodeRow")(function* <Error>(
  store: TopicStore<object>,
  row: RowObject,
  invalidRow: InvalidRowErrorFactory<Error>,
) {
  return yield* Effect.try({
    try: () => {
      const decoded = Schema.decodeUnknownSync(store.schema)(row);
      return cloneRow(decoded);
    },
    catch: (cause) => invalidRow(store.topic, String(cause)),
  });
});

const rowKey = Effect.fn("ColumnLiveViewEngine.topicStore.rowKey")(function* <Error>(
  store: TopicStore<object>,
  row: RowObject,
  invalidRow: InvalidRowErrorFactory<Error>,
) {
  const key = fieldValue(row, store.keyField);
  if (typeof key !== "string") {
    return yield* Effect.fail(
      invalidRow(store.topic, `Key field ${store.keyField} must decode to a string.`),
    );
  }
  return key;
});

export const publishTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.publish")(function* <
  Error,
>(store: TopicStore<object>, row: RowObject, invalidRow: InvalidRowErrorFactory<Error>) {
  const decoded = yield* decodeRow(store, row, invalidRow);
  const key = yield* rowKey(store, decoded, invalidRow);
  yield* store.mutationSemaphore.withPermits(1)(
    Effect.gen(function* () {
      store.rows.set(key, decoded);
      yield* commitTopicStore(store);
    }),
  );
});

export const publishTopicStoreRows = Effect.fn("ColumnLiveViewEngine.topicStore.publishMany")(
  function* <Error>(
    store: TopicStore<object>,
    rows: ReadonlyArray<RowObject>,
    invalidRow: InvalidRowErrorFactory<Error>,
  ) {
    const decodedRows = yield* Effect.forEach(rows, (row) => decodeRow(store, row, invalidRow));
    const keyedRows = yield* Effect.forEach(decodedRows, (row) =>
      Effect.gen(function* () {
        const key = yield* rowKey(store, row, invalidRow);
        return { key, row };
      }),
    );
    yield* store.mutationSemaphore.withPermits(1)(
      Effect.gen(function* () {
        for (const { key, row } of keyedRows) {
          store.rows.set(key, row);
        }
        yield* commitTopicStore(store);
      }),
    );
  },
);

export const patchTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.patch")(function* <
  Patch extends Partial<RowObject>,
  Error,
>(store: TopicStore<object>, key: string, patch: Patch, invalidRow: InvalidRowErrorFactory<Error>) {
  yield* store.mutationSemaphore.withPermits(1)(
    Effect.gen(function* () {
      const current = store.rows.get(key);
      if (current === undefined) {
        return yield* Effect.fail(invalidRow(store.topic, `Cannot patch missing key: ${key}`));
      }
      const decoded = yield* decodeRow(store, { ...current, ...patch }, invalidRow);
      const decodedKey = yield* rowKey(store, decoded, invalidRow);
      if (decodedKey !== key) {
        return yield* Effect.fail(invalidRow(store.topic, "Patch must not change the row key."));
      }
      store.rows.set(key, decoded);
      yield* commitTopicStore(store);
    }),
  );
});

export const deleteTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.delete")(function* <
  Row extends RowObject,
>(store: TopicStore<Row>, key: string) {
  yield* store.mutationSemaphore.withPermits(1)(
    Effect.gen(function* () {
      store.rows.delete(key);
      yield* commitTopicStore(store);
    }),
  );
});
