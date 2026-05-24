import { Effect, Schema, Semaphore } from "effect";
import type { HealthTopicStoreState } from "./engine-health";
import type { LiveTopicStoreState, LiveTopicSubscriber } from "./live-subscription";
import { rawQueryCompilerMetadata, type RawQueryCompilerMetadata } from "./raw-query-compiler";
import { cloneRow, fieldValue } from "./row-values";

type RowObject = object;

export type InvalidRowErrorFactory<Error> = (topic: string, message: string) => Error;

export type CommitTopicStore = <Row extends RowObject>(
  store: TopicStore<Row>,
) => Effect.Effect<void>;

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
  ) {
    this.rawQueryMetadata = rawQueryCompilerMetadata(schema);
  }
}

export const notifyTopicStoreSubscribers = Effect.fn("ColumnLiveViewEngine.topicStore.notify")(
  function* <Row extends RowObject>(store: TopicStore<Row>) {
    for (const subscriber of store.subscribers) {
      yield* subscriber.notify(store);
    }
  },
);

export const resetTopicStore = Effect.fn("ColumnLiveViewEngine.topicStore.reset")(function* <
  Row extends RowObject,
>(store: TopicStore<Row>) {
  for (const subscriber of store.subscribers) {
    subscriber.closed = true;
    yield* subscriber.end;
  }
  store.subscribers.clear();
  store.rows.clear();
  store.version = 0;
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

const safeCloneRow = Effect.fn("ColumnLiveViewEngine.topicStore.safeCloneRow")(function* <
  Row extends RowObject,
  Error,
>(store: TopicStore<Row>, row: Row, invalidRow: InvalidRowErrorFactory<Error>) {
  return yield* Effect.try({
    try: () => cloneRow(row),
    catch: (cause) => invalidRow(store.topic, String(cause)),
  });
});

const decodeRow = Effect.fn("ColumnLiveViewEngine.topicStore.decodeRow")(function* <
  Row extends RowObject,
  Error,
>(store: TopicStore<Row>, row: RowObject, invalidRow: InvalidRowErrorFactory<Error>) {
  return yield* Effect.try({
    try: () => Schema.decodeUnknownSync(store.schema)(row) as Row,
    catch: (cause) => invalidRow(store.topic, String(cause)),
  });
});

const rowKey = Effect.fn("ColumnLiveViewEngine.topicStore.rowKey")(function* <
  Row extends RowObject,
  Error,
>(store: TopicStore<Row>, row: Row, invalidRow: InvalidRowErrorFactory<Error>) {
  const key = fieldValue(row, store.keyField);
  if (typeof key !== "string") {
    return yield* Effect.fail(
      invalidRow(store.topic, `Key field ${store.keyField} must decode to a string.`),
    );
  }
  return key;
});

export const publishTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.publish")(function* <
  Row extends RowObject,
  Error,
>(
  store: TopicStore<Row>,
  row: Row,
  invalidRow: InvalidRowErrorFactory<Error>,
  commit: CommitTopicStore,
) {
  const decoded = yield* decodeRow(store, row, invalidRow);
  const key = yield* rowKey(store, decoded, invalidRow);
  const cloned = yield* safeCloneRow(store, decoded, invalidRow);
  yield* store.mutationSemaphore.withPermits(1)(
    Effect.gen(function* () {
      store.rows.set(key, cloned);
      yield* commit(store);
    }),
  );
});

export const publishTopicStoreRows = Effect.fn("ColumnLiveViewEngine.topicStore.publishMany")(
  function* <Row extends RowObject, Error>(
    store: TopicStore<Row>,
    rows: ReadonlyArray<Row>,
    invalidRow: InvalidRowErrorFactory<Error>,
    commit: CommitTopicStore,
  ) {
    const decodedRows = yield* Effect.forEach(rows, (row) => decodeRow(store, row, invalidRow));
    const keyedRows = yield* Effect.forEach(decodedRows, (row) =>
      Effect.gen(function* () {
        const key = yield* rowKey(store, row, invalidRow);
        const cloned = yield* safeCloneRow(store, row, invalidRow);
        return { key, row: cloned };
      }),
    );
    yield* store.mutationSemaphore.withPermits(1)(
      Effect.gen(function* () {
        for (const { key, row } of keyedRows) {
          store.rows.set(key, row);
        }
        yield* commit(store);
      }),
    );
  },
);

export const patchTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.patch")(function* <
  Row extends RowObject,
  Patch extends Partial<Row>,
  Error,
>(
  store: TopicStore<Row>,
  key: string,
  patch: Patch,
  invalidRow: InvalidRowErrorFactory<Error>,
  commit: CommitTopicStore,
) {
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
      const cloned = yield* safeCloneRow(store, decoded, invalidRow);
      store.rows.set(key, cloned);
      yield* commit(store);
    }),
  );
});

export const deleteTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.delete")(function* <
  Row extends RowObject,
>(store: TopicStore<Row>, key: string, commit: CommitTopicStore) {
  yield* store.mutationSemaphore.withPermits(1)(
    Effect.gen(function* () {
      store.rows.delete(key);
      yield* commit(store);
    }),
  );
});
