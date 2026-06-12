import { Clock, Effect, Semaphore } from "effect";
import type { ColumnarTopicStore } from "./columnar-topic-store";
import type { InvalidRowErrorFactory, PreparedTopicRow } from "./topic-row-preparation";
import type { createTopicHealthLedger } from "./topic-health-ledger";
import type { LiveTopicSubscriber } from "./topic-subscriber";
import { topicStoreState, type TopicStore } from "./topic-store-state";

type RowObject = object;

export type TopicStoreMutationState = {
  readonly storage: ColumnarTopicStore;
  readonly subscribers: Set<LiveTopicSubscriber>;
  readonly mutationSemaphore: Semaphore.Semaphore;
  readonly notificationSemaphore: Semaphore.Semaphore;
  readonly healthLedger: ReturnType<typeof createTopicHealthLedger>;
  readonly onCommit: () => void;
};

export type TopicStoreMutationContext = {
  readonly publishPrepared: (prepared: PreparedTopicRow) => number;
  readonly publishPreparedMany: (preparedRows: ReadonlyArray<PreparedTopicRow>) => number;
  readonly patch: <Patch extends Partial<RowObject>, Error>(
    key: string,
    patch: Patch,
    invalidRow: InvalidRowErrorFactory<Error>,
  ) => Effect.Effect<number, Error>;
  readonly delete: (key: string) => number;
};

export const withTopicStoreTransaction = Effect.fn("ColumnLiveViewEngine.topicStore.transaction")(
  function* <Success, Error, Requirements>(
    state: TopicStoreMutationState,
    effect: Effect.Effect<Success, Error, Requirements>,
  ) {
    return yield* state.mutationSemaphore.withPermits(1)(Effect.uninterruptible(effect));
  },
);

export const withTopicStoreNotification = Effect.fn("ColumnLiveViewEngine.topicStore.notification")(
  function* <Success, Error, Requirements>(
    state: TopicStoreMutationState,
    effect: Effect.Effect<Success, Error, Requirements>,
  ) {
    return yield* state.notificationSemaphore.withPermits(1)(Effect.uninterruptible(effect));
  },
);

const commitTopicStoreState = (
  state: TopicStoreMutationState,
): ReadonlyArray<LiveTopicSubscriber> => {
  state.storage.advanceVersion();
  state.onCommit();
  return [...state.subscribers];
};

const recordTopicStoreMutation = (
  state: TopicStoreMutationState,
  rowsChanged: number,
  occurredAt: number,
): ReadonlyArray<LiveTopicSubscriber> => {
  if (rowsChanged <= 0) {
    return [];
  }
  const subscribersToNotify = commitTopicStoreState(state);
  state.healthLedger.recordMutation({
    version: state.storage.version,
    rowCount: state.storage.rowCount,
    rowsChanged,
    occurredAt,
  });
  return subscribersToNotify;
};

const topicStoreMutationContext = (state: TopicStoreMutationState): TopicStoreMutationContext => ({
  publishPrepared: (prepared) => {
    state.storage.setPrepared(prepared);
    return 1;
  },
  publishPreparedMany: (preparedRows) => {
    state.storage.setPreparedMany(preparedRows);
    return preparedRows.length;
  },
  patch: (key, patch, invalidRow) =>
    Effect.gen(function* () {
      const prepared = yield* state.storage.preparePatch(key, patch, invalidRow);
      state.storage.setPrepared(prepared);
      return 1;
    }),
  delete: (key) => state.storage.delete(key),
});

const withTopicStoreMutationBatch = Effect.fn("ColumnLiveViewEngine.topicStore.mutationBatch")(
  function* <Success, Error, Requirements>(
    state: TopicStoreMutationState,
    effect: Effect.Effect<Success, Error, Requirements>,
  ) {
    const ledger = state.healthLedger;
    return yield* Effect.acquireUseRelease(
      Effect.sync(() => {
        ledger.beginMutationBatch();
      }),
      () => effect,
      () =>
        Effect.sync(() => {
          ledger.endMutationBatch();
        }),
    );
  },
);

const notifyTopicStoreSubscribers = Effect.fn("ColumnLiveViewEngine.topicStore.notify")(function* (
  state: TopicStoreMutationState,
  store: TopicStore,
  subscribers: ReadonlyArray<LiveTopicSubscriber>,
) {
  yield* withTopicStoreNotification(
    state,
    Effect.gen(function* () {
      for (const subscriber of subscribers) {
        if (!subscriber.closed) {
          yield* subscriber.notify(store);
        }
      }
    }),
  );
});

export const runTopicStoreMutationTransaction = Effect.fn(
  "ColumnLiveViewEngine.topicStore.mutationTransaction",
)(function* <Error, Requirements>(
  state: TopicStoreMutationState,
  store: TopicStore,
  mutate: (mutation: TopicStoreMutationContext) => Effect.Effect<number, Error, Requirements>,
) {
  yield* withTopicStoreMutationBatch(
    state,
    Effect.gen(function* () {
      const subscribers = yield* withTopicStoreTransaction(
        state,
        Effect.gen(function* () {
          const rowsChanged = yield* mutate(topicStoreMutationContext(state));
          const occurredAt = yield* Clock.currentTimeMillis;
          return recordTopicStoreMutation(state, rowsChanged, occurredAt);
        }),
      );
      yield* notifyTopicStoreSubscribers(state, store, subscribers);
    }),
  );
});

export const publishTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.publish")(function* <
  Error,
  Row extends RowObject,
>(store: TopicStore, row: Row, invalidRow: InvalidRowErrorFactory<Error>) {
  const state = topicStoreState(store);
  const prepared = yield* state.storage.prepareRow(row, invalidRow);
  yield* runTopicStoreMutationTransaction(state, store, (mutation) =>
    Effect.sync(() => {
      return mutation.publishPrepared(prepared);
    }),
  );
});

export const publishTopicStoreRows = Effect.fn("ColumnLiveViewEngine.topicStore.publishMany")(
  function* <Error, Row extends RowObject>(
    store: TopicStore,
    rows: ReadonlyArray<Row>,
    invalidRow: InvalidRowErrorFactory<Error>,
  ) {
    const state = topicStoreState(store);
    const preparedRows = yield* state.storage.prepareRows(rows, invalidRow);
    yield* runTopicStoreMutationTransaction(state, store, (mutation) =>
      Effect.sync(() => {
        return mutation.publishPreparedMany(preparedRows);
      }),
    );
  },
);

export const patchTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.patch")(function* <
  Patch extends Partial<RowObject>,
  Error,
>(store: TopicStore, key: string, patch: Patch, invalidRow: InvalidRowErrorFactory<Error>) {
  yield* runTopicStoreMutationTransaction(topicStoreState(store), store, (mutation) =>
    mutation.patch(key, patch, invalidRow),
  );
});

export const deleteTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.delete")(function* (
  store: TopicStore,
  key: string,
) {
  yield* runTopicStoreMutationTransaction(topicStoreState(store), store, (mutation) =>
    Effect.sync(() => {
      return mutation.delete(key);
    }),
  );
});
