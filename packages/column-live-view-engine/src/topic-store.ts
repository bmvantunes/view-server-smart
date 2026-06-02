import { Clock, Effect, Schema, Semaphore } from "effect";
import type { StatusEvent, TopicRuntimeHealth } from "@view-server/config";
import {
  activeStoreRawQueryExecutionCount,
  clearStoreRawQueryExecutions,
  type ActiveQueryStoreState,
} from "./active-query";
import { createTopicHealthLedger } from "./topic-health-ledger";
import { rawQueryCompilerMetadata, type RawQueryCompilerMetadata } from "./raw-query-compiler";
import { cloneRow, fieldValue, isPlainRecord } from "./row-values";
import {
  acquireSubscriptionHandoff,
  type MarkAcquiredSubscription,
  type SubscriptionHandoffOptions,
} from "./subscription-handoff";
import type { LiveTopicSubscriber } from "./topic-subscriber";

type RowObject = object;

type InvalidRowErrorFactory<Error> = (topic: string, message: string) => Error;
const topicStoreSubscriptionPermitBrand: unique symbol = Symbol("TopicStoreSubscriptionPermit");

export type TopicStoreSubscriptionPermit = {
  readonly [topicStoreSubscriptionPermitBrand]: true;
  readonly store: TopicStore;
};

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

type TopicStoreState = {
  readonly rows: Map<string, object>;
  readonly subscribers: Set<LiveTopicSubscriber>;
  readonly mutationSemaphore: Semaphore.Semaphore;
  readonly notificationSemaphore: Semaphore.Semaphore;
  readonly rawQueryMetadata: RawQueryCompilerMetadata;
  readonly healthLedger: ReturnType<typeof createTopicHealthLedger>;
  readonly schema: Schema.Decoder<object>;
  readonly keyField: string;
  readonly onCommit: () => void;
  readonly readModel: ActiveQueryStoreState;
  version: number;
};

const topicStoreStates = new WeakMap<TopicStore, TopicStoreState>();

export class TopicStore {
  declare private readonly topicStoreBrand: void;

  constructor(
    readonly topic: string,
    schema: Schema.Decoder<object>,
    keyField: string,
    onCommit: () => void,
  ) {
    const rows = new Map<string, object>();
    const subscribers = new Set<LiveTopicSubscriber>();
    let version = 0;
    const state: TopicStoreState = {
      rows,
      subscribers,
      mutationSemaphore: Semaphore.makeUnsafe(1),
      notificationSemaphore: Semaphore.makeUnsafe(1),
      rawQueryMetadata: rawQueryCompilerMetadata(schema),
      healthLedger: createTopicHealthLedger(),
      schema,
      keyField,
      onCommit,
      readModel: {
        identity: this,
        topic,
        rows: () => rows,
        version: () => version,
      },
      get version() {
        return version;
      },
      set version(nextVersion: number) {
        version = nextVersion;
      },
    };
    topicStoreStates.set(this, state);
  }
}

const topicStoreState = (store: TopicStore): TopicStoreState => {
  return topicStoreStates.get(store)!;
};

export const topicStoreRawQueryMetadata = (store: TopicStore): RawQueryCompilerMetadata =>
  topicStoreState(store).rawQueryMetadata;

export const topicStoreReadModel = (store: TopicStore): ActiveQueryStoreState =>
  topicStoreState(store).readModel;

const withTopicStoreTransaction = Effect.fn("ColumnLiveViewEngine.topicStore.transaction")(
  function* <Success, Error, Requirements>(
    store: TopicStore,
    effect: Effect.Effect<Success, Error, Requirements>,
  ) {
    return yield* topicStoreState(store).mutationSemaphore.withPermits(1)(
      Effect.uninterruptible(effect),
    );
  },
);

const withTopicStoreNotification = Effect.fn("ColumnLiveViewEngine.topicStore.notification")(
  function* <Success, Error, Requirements>(
    store: TopicStore,
    effect: Effect.Effect<Success, Error, Requirements>,
  ) {
    return yield* topicStoreState(store).notificationSemaphore.withPermits(1)(
      Effect.uninterruptible(effect),
    );
  },
);

export function acquireTopicStoreSubscription<
  Subscription extends { readonly close: () => Effect.Effect<void, never> },
  Error,
  Requirements,
>(
  store: TopicStore,
  acquire: (
    permit: TopicStoreSubscriptionPermit,
    markAcquired: MarkAcquiredSubscription<Subscription>,
  ) => Effect.Effect<Subscription, Error, Requirements>,
  options: SubscriptionHandoffOptions = {},
): Effect.Effect<Subscription, Error, Requirements> {
  return acquireSubscriptionHandoff(
    (markAcquired: (subscription: Subscription) => Effect.Effect<void>) =>
      withTopicStoreTransaction(
        store,
        acquire(
          {
            [topicStoreSubscriptionPermitBrand]: true,
            store,
          },
          markAcquired,
        ),
      ),
    options,
  );
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

const commitTopicStoreState = (state: TopicStoreState): ReadonlyArray<LiveTopicSubscriber> => {
  state.version += 1;
  state.onCommit();
  return [...state.subscribers];
};

const notifyTopicStoreSubscribers = Effect.fn("ColumnLiveViewEngine.topicStore.notify")(function* (
  store: TopicStore,
  subscribers: ReadonlyArray<LiveTopicSubscriber>,
) {
  yield* withTopicStoreNotification(
    store,
    Effect.gen(function* () {
      for (const subscriber of subscribers) {
        if (!subscriber.closed) {
          yield* subscriber.notify(store);
        }
      }
    }),
  );
});

const commitTopicStoreMutation = Effect.fn("ColumnLiveViewEngine.topicStore.commitMutation")(
  function* (store: TopicStore, mutate: (state: TopicStoreState) => number) {
    const occurredAt = yield* Clock.currentTimeMillis;
    return yield* Effect.sync(() => {
      const state = topicStoreState(store);
      const rowsChanged = mutate(state);
      const subscribersToNotify = commitTopicStoreState(state);
      state.healthLedger.recordMutation({
        version: state.version,
        rowCount: state.rows.size,
        rowsChanged,
        occurredAt,
      });
      return subscribersToNotify;
    });
  },
);

const commitAndNotifyTopicStoreMutation = Effect.fn(
  "ColumnLiveViewEngine.topicStore.commitAndNotifyMutation",
)(function* (store: TopicStore, mutate: (state: TopicStoreState) => number) {
  yield* withTopicStoreMutationBatch(
    store,
    Effect.gen(function* () {
      const subscribers = yield* withTopicStoreTransaction(
        store,
        commitTopicStoreMutation(store, mutate),
      );
      yield* notifyTopicStoreSubscribers(store, subscribers);
    }),
  );
});

const withTopicStoreMutationBatch = Effect.fn("ColumnLiveViewEngine.topicStore.mutationBatch")(
  function* <Success, Error, Requirements>(
    store: TopicStore,
    effect: Effect.Effect<Success, Error, Requirements>,
  ) {
    const ledger = topicStoreState(store).healthLedger;
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

export const collectTopicStoreHealth = Effect.fn("ColumnLiveViewEngine.topicStore.health")(
  function* (store: TopicStore, closed: boolean) {
    const state = topicStoreState(store);
    const totals = state.healthLedger.snapshot(yield* Clock.currentTimeMillis);
    let queuedEvents = 0;

    for (const subscriber of state.subscribers) {
      const currentQueuedEvents = yield* subscriber.queuedEvents;
      queuedEvents += currentQueuedEvents;
    }

    const activeSubscriptions = state.subscribers.size;
    const activeViews = yield* activeStoreRawQueryExecutionCount(topicStoreReadModel(store));
    const status: TopicRuntimeHealth["status"] = closed ? "degraded" : "ready";

    return {
      topic: store.topic,
      status,
      rowCount: totals.rowCount,
      liveRowCount: totals.rowCount,
      deletedRowCount: 0,
      version: totals.version,
      lastMutationAt: totals.lastMutationAt,
      mutationsPerSecond: totals.mutationsPerSecond,
      rowsPerSecond: totals.rowsPerSecond,
      pendingMutationBatches: totals.pendingMutationBatches,
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
)((permit: TopicStoreSubscriptionPermit, subscriber: LiveTopicSubscriber) =>
  Effect.sync(() => {
    const state = topicStoreState(permit.store);
    state.healthLedger.openSubscription(subscriber);
    state.subscribers.add(subscriber);
  }),
);

const unregisterTopicStoreSubscription = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.remove",
)((store: TopicStore, subscriber: LiveTopicSubscriber) =>
  Effect.sync(() => {
    const state = topicStoreState(store);
    state.healthLedger.closeSubscription(subscriber);
    state.subscribers.delete(subscriber);
  }),
);

export const trackTopicStoreSubscriptionQueueDepth = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.queueDepth",
)((store: TopicStore, subscriber: LiveTopicSubscriber, queueDepth: number) =>
  Effect.sync(() => {
    topicStoreState(store).healthLedger.updateQueueDepth(subscriber, queueDepth);
    subscriber.maxQueueDepth = Math.max(subscriber.maxQueueDepth, queueDepth);
  }),
);

const reportTopicStoreSubscriptionBackpressure = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.backpressure",
)((store: TopicStore, subscriber: LiveTopicSubscriber) =>
  Effect.sync(() => {
    topicStoreState(store).healthLedger.markBackpressure(subscriber);
    subscriber.backpressureEvents += 1;
  }),
);

export const closeTopicStoreSubscription = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.close",
)(function* (store: TopicStore, subscriber: LiveTopicSubscriber, finalize: Effect.Effect<void>) {
  yield* withTopicStoreNotification(
    store,
    withTopicStoreTransaction(
      store,
      Effect.gen(function* () {
        if (subscriber.closed) {
          return;
        }
        subscriber.closed = true;
        yield* unregisterTopicStoreSubscription(store, subscriber);
        yield* finalize;
      }),
    ),
  );
});

export const closeBackpressuredTopicStoreSubscription = Effect.fn(
  "ColumnLiveViewEngine.topicStore.subscribe.closeBackpressured",
)(function* (store: TopicStore, subscriber: LiveTopicSubscriber, finalize: Effect.Effect<void>) {
  yield* withTopicStoreTransaction(
    store,
    Effect.gen(function* () {
      if (subscriber.closed) {
        return;
      }
      subscriber.closed = true;
      yield* reportTopicStoreSubscriptionBackpressure(store, subscriber);
      yield* unregisterTopicStoreSubscription(store, subscriber);
      yield* finalize;
    }),
  );
});

export const resetTopicStore = Effect.fn("ColumnLiveViewEngine.topicStore.reset")(function* (
  store: TopicStore,
) {
  yield* withTopicStoreNotification(
    store,
    withTopicStoreTransaction(
      store,
      Effect.gen(function* () {
        const subscribers = yield* Effect.sync(() => {
          const state = topicStoreState(store);
          state.rows.clear();
          state.version = 0;
          const closingSubscribers = [...state.subscribers];
          for (const subscriber of closingSubscribers) {
            subscriber.closed = true;
          }
          state.subscribers.clear();
          state.healthLedger.reset();
          return closingSubscribers;
        });
        yield* clearStoreRawQueryExecutions(topicStoreReadModel(store));
        for (const subscriber of subscribers) {
          yield* subscriber.closeWithStatus(resetStatusEvent(store, subscriber));
        }
      }),
    ),
  );
});

export const closeTopicStoreSubscriptions = Effect.fn(
  "ColumnLiveViewEngine.topicStore.closeSubscriptions",
)(function* (store: TopicStore) {
  yield* withTopicStoreNotification(
    store,
    withTopicStoreTransaction(
      store,
      Effect.gen(function* () {
        const subscribers = yield* Effect.sync(() => {
          const state = topicStoreState(store);
          const closingSubscribers = [...state.subscribers];
          for (const subscriber of closingSubscribers) {
            subscriber.closed = true;
            state.healthLedger.closeSubscription(subscriber);
          }
          state.subscribers.clear();
          return closingSubscribers;
        });
        yield* clearStoreRawQueryExecutions(topicStoreReadModel(store));
        for (const subscriber of subscribers) {
          yield* subscriber.closeWithStatus(engineClosedStatusEvent(store, subscriber));
        }
      }),
    ),
  );
});

const decodeRow = Effect.fn("ColumnLiveViewEngine.topicStore.decodeRow")(function* <Error>(
  store: TopicStore,
  row: RowObject,
  invalidRow: InvalidRowErrorFactory<Error>,
) {
  return yield* Effect.try({
    try: () => {
      const decoded = Schema.decodeUnknownSync(topicStoreState(store).schema)(row);
      return cloneRow(decoded);
    },
    catch: (cause) => invalidRow(store.topic, String(cause)),
  });
});

const rowKey = Effect.fn("ColumnLiveViewEngine.topicStore.rowKey")(function* <
  Error,
  Row extends RowObject,
>(store: TopicStore, row: Row, invalidRow: InvalidRowErrorFactory<Error>) {
  const keyField = topicStoreState(store).keyField;
  const key = fieldValue(row, keyField);
  if (typeof key !== "string") {
    return yield* Effect.fail(
      invalidRow(store.topic, `Key field ${keyField} must decode to a string.`),
    );
  }
  return key;
});

const validatePatchKeys = Effect.fn("ColumnLiveViewEngine.topicStore.patchKeys.validate")(
  function* <Error>(store: TopicStore, patch: unknown, invalidRow: InvalidRowErrorFactory<Error>) {
    if (!isPlainRecord(patch)) {
      return yield* Effect.fail(invalidRow(store.topic, "Patch must be a plain object."));
    }
    const metadata = topicStoreRawQueryMetadata(store);
    for (const key of Reflect.ownKeys(patch)) {
      if (typeof key !== "string" || !metadata.fieldNames.has(key)) {
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
  yield* commitAndNotifyTopicStoreMutation(store, (state) => {
    state.rows.set(key, decoded);
    return 1;
  });
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
    yield* commitAndNotifyTopicStoreMutation(store, (state) => {
      for (const { key, row } of keyedRows) {
        state.rows.set(key, row);
      }
      return keyedRows.length;
    });
  },
);

export const patchTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.patch")(function* <
  Patch extends Partial<RowObject>,
  Error,
>(store: TopicStore, key: string, patch: Patch, invalidRow: InvalidRowErrorFactory<Error>) {
  yield* withTopicStoreMutationBatch(
    store,
    Effect.gen(function* () {
      const subscribers = yield* withTopicStoreTransaction(
        store,
        Effect.gen(function* () {
          const state = topicStoreState(store);
          yield* validatePatchKeys(store, patch, invalidRow);
          const current = state.rows.get(key);
          if (current === undefined) {
            return yield* Effect.fail(invalidRow(store.topic, `Cannot patch missing key: ${key}`));
          }
          const decoded = yield* decodeRow(store, { ...current, ...patch }, invalidRow);
          const decodedKey = yield* rowKey(store, decoded, invalidRow);
          if (decodedKey !== key) {
            return yield* Effect.fail(
              invalidRow(store.topic, "Patch must not change the row key."),
            );
          }
          return yield* commitTopicStoreMutation(store, (currentState) => {
            currentState.rows.set(key, decoded);
            return 1;
          });
        }),
      );
      yield* notifyTopicStoreSubscribers(store, subscribers);
    }),
  );
});

export const deleteTopicStoreRow = Effect.fn("ColumnLiveViewEngine.topicStore.delete")(function* (
  store: TopicStore,
  key: string,
) {
  yield* commitAndNotifyTopicStoreMutation(store, (state) => {
    return state.rows.delete(key) ? 1 : 0;
  });
});
