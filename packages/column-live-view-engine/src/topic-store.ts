import { Effect, Schema, Semaphore } from "effect";
import type { StatusEvent, TopicRuntimeHealth } from "@view-server/config";
import { activeStoreRawQueryExecutionCount, clearStoreRawQueryExecutions } from "./active-query";
import { createTopicHealthLedger } from "./topic-health-ledger";
import { rawQueryCompilerMetadata, type RawQueryCompilerMetadata } from "./raw-query-compiler";
import { cloneRow, fieldValue, isPlainRecord } from "./row-values";
import type { LiveTopicSubscriber } from "./live-subscription";

type RowObject = object;

type InvalidRowErrorFactory<Error> = (topic: string, message: string) => Error;

export type TopicStoreHealthView = {
  readonly topic: string;
  readonly status: "ready" | "degraded";
  readonly rowCount: number;
  readonly liveRowCount: number;
  readonly deletedRowCount: number;
  readonly version: number;
  readonly lastMutationAt: number | null;
  readonly mutationsPerSecond: number;
  readonly rowsPerSecond: number;
  readonly pendingMutationBatches: number;
  readonly activeViews: number;
  readonly activeSubscriptions: number;
  readonly queuedEvents: number;
  readonly maxQueueDepth: number;
  readonly backpressureEvents: number;
  readonly memoryBytes: number;
  readonly tombstoneCount: number;
  readonly compactionPending: boolean;
};

export class TopicStore {
  readonly rows = new Map<string, object>();
  readonly subscribers = new Set<LiveTopicSubscriber>();
  readonly mutationSemaphore = Semaphore.makeUnsafe(1);
  readonly rawQueryMetadata: RawQueryCompilerMetadata;
  readonly healthLedger = createTopicHealthLedger();
  version = 0;

  constructor(
    readonly topic: string,
    readonly schema: Schema.Decoder<object>,
    readonly keyField: string,
    readonly onCommit: () => void,
  ) {
    this.rawQueryMetadata = rawQueryCompilerMetadata(schema);
  }
}

const resetStatusEvent = (store: TopicStore, subscriber: LiveTopicSubscriber): StatusEvent => ({
  type: "status",
  topic: store.topic,
  queryId: subscriber.queryId,
  status: "closed",
  code: "SubscriptionClosed",
  message: "Subscription closed because the engine reset.",
});

const engineClosedStatusEvent = (
  store: TopicStore,
  subscriber: LiveTopicSubscriber,
): StatusEvent => ({
  type: "status",
  topic: store.topic,
  queryId: subscriber.queryId,
  status: "closed",
  code: "SubscriptionClosed",
  message: "Subscription closed because the engine closed.",
});

const commitTopicStore = Effect.fn("ColumnLiveViewEngine.topicStore.commit")(function* (
  store: TopicStore,
) {
  store.version += 1;
  store.onCommit();
  for (const subscriber of store.subscribers) {
    yield* subscriber.notify(store);
  }
});

export const collectTopicStoreHealth = Effect.fn("ColumnLiveViewEngine.topicStore.health")(
  function* (store: TopicStore, closed: boolean) {
    const totals = store.healthLedger.snapshot();
    let queuedEvents = 0;

    for (const subscriber of store.subscribers) {
      const currentQueuedEvents = yield* subscriber.queuedEvents;
      queuedEvents += currentQueuedEvents;
    }

    const activeSubscriptions = store.subscribers.size;
    const activeViews = yield* activeStoreRawQueryExecutionCount(store);
    const rowCount = store.rows.size;
    const status: TopicRuntimeHealth["status"] = closed ? "degraded" : "ready";

    return {
      topic: store.topic,
      status,
      rowCount,
      liveRowCount: rowCount,
      deletedRowCount: 0,
      version: store.version,
      lastMutationAt: null,
      mutationsPerSecond: 0,
      rowsPerSecond: 0,
      pendingMutationBatches: 0,
      activeViews,
      activeSubscriptions,
      queuedEvents,
      maxQueueDepth: totals.maxQueueDepth,
      backpressureEvents: totals.backpressureEvents,
      memoryBytes: 0,
      tombstoneCount: 0,
      compactionPending: false,
    } satisfies TopicStoreHealthView;
  },
);

export const registerTopicStoreSubscription = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.add",
)((store: TopicStore, subscriber: LiveTopicSubscriber) =>
  Effect.sync(() => {
    store.healthLedger.openSubscription(subscriber);
    store.subscribers.add(subscriber);
  }),
);

export const unregisterTopicStoreSubscription = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.remove",
)((store: TopicStore, subscriber: LiveTopicSubscriber) =>
  Effect.sync(() => {
    store.healthLedger.closeSubscription(subscriber);
    store.subscribers.delete(subscriber);
  }),
);

export const trackTopicStoreSubscriptionQueueDepth = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.queueDepth",
)((store: TopicStore, subscriber: LiveTopicSubscriber, queueDepth: number) =>
  Effect.sync(() => {
    store.healthLedger.updateQueueDepth(subscriber, queueDepth);
    subscriber.maxQueueDepth = Math.max(subscriber.maxQueueDepth, queueDepth);
  }),
);

export const reportTopicStoreSubscriptionBackpressure = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.backpressure",
)((store: TopicStore, subscriber: LiveTopicSubscriber) =>
  Effect.sync(() => {
    store.healthLedger.markBackpressure(subscriber);
    subscriber.backpressureEvents += 1;
  }),
);

export const resetTopicStore = Effect.fn("ColumnLiveViewEngine.topicStore.reset")(function* (
  store: TopicStore,
) {
  yield* store.mutationSemaphore.withPermits(1)(
    Effect.gen(function* () {
      store.rows.clear();
      store.version = 0;
      for (const subscriber of store.subscribers) {
        subscriber.closed = true;
        yield* subscriber.closeWithStatus(resetStatusEvent(store, subscriber));
      }
      store.subscribers.clear();
      store.healthLedger.reset();
      yield* clearStoreRawQueryExecutions(store);
    }),
  );
});

export const closeTopicStoreSubscriptions = Effect.fn(
  "ColumnLiveViewEngine.topicStore.closeSubscriptions",
)(function* (store: TopicStore) {
  yield* store.mutationSemaphore.withPermits(1)(
    Effect.gen(function* () {
      for (const subscriber of store.subscribers) {
        subscriber.closed = true;
        yield* subscriber.closeWithStatus(engineClosedStatusEvent(store, subscriber));
        store.healthLedger.closeSubscription(subscriber);
      }
      store.subscribers.clear();
      yield* clearStoreRawQueryExecutions(store);
    }),
  );
});

const decodeRow = Effect.fn("ColumnLiveViewEngine.topicStore.decodeRow")(function* <Error>(
  store: TopicStore,
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

const rowKey = Effect.fn("ColumnLiveViewEngine.topicStore.rowKey")(function* <
  Error,
  Row extends RowObject,
>(store: TopicStore, row: Row, invalidRow: InvalidRowErrorFactory<Error>) {
  const key = fieldValue(row, store.keyField);
  if (typeof key !== "string") {
    return yield* Effect.fail(
      invalidRow(store.topic, `Key field ${store.keyField} must decode to a string.`),
    );
  }
  return key;
});

const validatePatchKeys = Effect.fn("ColumnLiveViewEngine.topicStore.patchKeys.validate")(
  function* <Error>(store: TopicStore, patch: unknown, invalidRow: InvalidRowErrorFactory<Error>) {
    if (!isPlainRecord(patch)) {
      return yield* Effect.fail(invalidRow(store.topic, "Patch must be a plain object."));
    }
    for (const key of Reflect.ownKeys(patch)) {
      if (typeof key !== "string" || !store.rawQueryMetadata.fieldNames.has(key)) {
        return yield* Effect.fail(
          invalidRow(store.topic, `Patch contains unknown field: ${String(key)}.`),
        );
      }
    }
  },
);

export const publishTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.publish")(function* <
  Error,
  Row extends RowObject,
>(store: TopicStore, row: Row, invalidRow: InvalidRowErrorFactory<Error>) {
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
  function* <Error, Row extends RowObject>(
    store: TopicStore,
    rows: ReadonlyArray<Row>,
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
>(store: TopicStore, key: string, patch: Patch, invalidRow: InvalidRowErrorFactory<Error>) {
  yield* store.mutationSemaphore.withPermits(1)(
    Effect.gen(function* () {
      yield* validatePatchKeys(store, patch, invalidRow);
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

export const deleteTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.delete")(function* (
  store: TopicStore,
  key: string,
) {
  yield* store.mutationSemaphore.withPermits(1)(
    Effect.gen(function* () {
      store.rows.delete(key);
      yield* commitTopicStore(store);
    }),
  );
});
