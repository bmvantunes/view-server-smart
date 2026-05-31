import type { ExactPatch, ExactRawQuery, LiveQueryRow, TopicRow } from "@view-server/config";
import { Effect } from "effect";
import type {
  ColumnLiveViewEngine,
  ColumnLiveViewEngineConfig,
  DecodableTopicDefinitions,
} from "./engine-contract";
import { EngineClosedError, InvalidRowError, InvalidTopicError } from "./engine-errors";
import { acquireLiveSubscriptionHandoff, type LiveSubscription } from "./live-subscription";
import { collectColumnLiveViewEngineHealth } from "./engine-health";
import { snapshotExecutableQuery, subscribeExecutableQuery } from "./query-execution";
import {
  closeTopicStoreSubscriptions,
  deleteTopicStoreRow,
  patchTopicStoreRow,
  publishTopicStoreRow,
  publishTopicStoreRows,
  resetTopicStore,
  TopicStore,
  withTopicStoreMutation,
} from "./topic-store";

export { InvalidQueryError } from "./raw-query-compiler";
export {
  EngineClosedError,
  InvalidRowError,
  InvalidTopicError,
  UnsupportedQueryError,
} from "./engine-errors";
export type { ColumnLiveViewEngineError } from "./engine-errors";
export type {
  ColumnLiveViewEngine,
  ColumnLiveViewEngineConfig,
  ColumnLiveViewEngineEvent,
  ColumnLiveViewSubscription,
  DecodableTopicDefinitions,
} from "./engine-contract";
export type { ColumnLiveViewEngineHealth, ColumnLiveViewTopicHealth } from "./engine-health";

const defaultSubscriptionQueueCapacity = 1_024;

const invalidRow = (topic: string, message: string) =>
  new InvalidRowError({
    topic,
    message,
  });

class InMemoryColumnLiveViewEngine<
  Topics extends DecodableTopicDefinitions,
> implements ColumnLiveViewEngine<Topics> {
  private readonly stores = new Map<Extract<keyof Topics, string>, TopicStore>();
  private readonly subscriptionQueueCapacity: number;
  private engineVersion = 0;
  private nextQueryId = 0;
  private closed = false;

  constructor(config: ColumnLiveViewEngineConfig<Topics>) {
    const configuredCapacity = config.subscriptionQueueCapacity ?? defaultSubscriptionQueueCapacity;
    this.subscriptionQueueCapacity =
      Number.isSafeInteger(configuredCapacity) && configuredCapacity > 0
        ? configuredCapacity
        : defaultSubscriptionQueueCapacity;
    for (const topic in config.topics) {
      if (!Object.hasOwn(config.topics, topic)) {
        continue;
      }
      const definition = config.topics[topic];
      this.stores.set(
        topic,
        new TopicStore(topic, definition.schema, definition.key, () => {
          this.engineVersion += 1;
        }),
      );
    }
  }

  private readonly getStore = Effect.fn("ColumnLiveViewEngine.store.get")(
    { self: this },
    function* <Topic extends Extract<keyof Topics, string>>(
      this: InMemoryColumnLiveViewEngine<Topics>,
      topic: Topic,
    ) {
      const store = this.stores.get(topic);
      if (store === undefined) {
        return yield* InvalidTopicError.make({
          topic,
          message: `Unknown topic: ${topic}`,
        });
      }
      return store;
    },
  );

  private readonly ensureOpen = Effect.fn("ColumnLiveViewEngine.open.ensure")(
    { self: this },
    function* (this: InMemoryColumnLiveViewEngine<Topics>) {
      if (this.closed) {
        return yield* EngineClosedError.make({
          message: "ColumnLiveViewEngine is closed.",
        });
      }
    },
  );

  readonly publish: ColumnLiveViewEngine<Topics>["publish"] = Effect.fn(
    "ColumnLiveViewEngine.publish",
  )({ self: this }, function* <
    Topic extends Extract<keyof Topics, string>,
  >(this: InMemoryColumnLiveViewEngine<Topics>, topic: Topic, row: TopicRow<Topics, Topic>) {
    yield* this.ensureOpen();
    const store = yield* this.getStore(topic);
    yield* publishTopicStoreRow(store, row, invalidRow);
  });

  readonly publishMany: ColumnLiveViewEngine<Topics>["publishMany"] = Effect.fn(
    "ColumnLiveViewEngine.publishMany",
  )({ self: this }, function* <
    Topic extends Extract<keyof Topics, string>,
  >(this: InMemoryColumnLiveViewEngine<Topics>, topic: Topic, rows: ReadonlyArray<TopicRow<Topics, Topic>>) {
    yield* this.ensureOpen();
    const store = yield* this.getStore(topic);
    yield* publishTopicStoreRows(store, rows, invalidRow);
  });

  readonly patch: ColumnLiveViewEngine<Topics>["patch"] = Effect.fn("ColumnLiveViewEngine.patch")(
    { self: this },
    function* <
      Topic extends Extract<keyof Topics, string>,
      const Patch extends Partial<TopicRow<Topics, Topic>>,
    >(
      this: InMemoryColumnLiveViewEngine<Topics>,
      topic: Topic,
      key: string,
      patch: ExactPatch<TopicRow<Topics, Topic>, Patch>,
    ) {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      yield* patchTopicStoreRow(store, key, patch, invalidRow);
    },
  );

  readonly delete: ColumnLiveViewEngine<Topics>["delete"] = Effect.fn(
    "ColumnLiveViewEngine.delete",
  )({ self: this }, function* <
    Topic extends Extract<keyof Topics, string>,
  >(this: InMemoryColumnLiveViewEngine<Topics>, topic: Topic, key: string) {
    yield* this.ensureOpen();
    const store = yield* this.getStore(topic);
    yield* deleteTopicStoreRow(store, key);
  });

  readonly snapshot: ColumnLiveViewEngine<Topics>["snapshot"] = Effect.fn(
    "ColumnLiveViewEngine.snapshot",
  )({ self: this }, function* <
    Topic extends Extract<keyof Topics, string>,
    const Query extends { readonly select: ReadonlyArray<unknown> },
  >(this: InMemoryColumnLiveViewEngine<Topics>, topic: Topic, query: Query & ExactRawQuery<TopicRow<Topics, Topic>, Query>) {
    yield* this.ensureOpen();
    const store = yield* this.getStore(topic);
    return yield* snapshotExecutableQuery<
      LiveQueryRow<TopicRow<Topics, typeof topic>, typeof query>
    >(topic, store, query);
  });

  readonly subscribe: ColumnLiveViewEngine<Topics>["subscribe"] = Effect.fn(
    "ColumnLiveViewEngine.subscribe",
  )({ self: this }, function* <
    Topic extends Extract<keyof Topics, string>,
    const Query extends { readonly select: ReadonlyArray<unknown> },
  >(this: InMemoryColumnLiveViewEngine<Topics>, topic: Topic, query: Query & ExactRawQuery<TopicRow<Topics, Topic>, Query>) {
    yield* this.ensureOpen();
    const store = yield* this.getStore(topic);
    type ResultRow = LiveQueryRow<TopicRow<Topics, typeof topic>, typeof query>;
    const acquireSubscription = (
      markAcquired: (subscription: LiveSubscription<ResultRow>) => Effect.Effect<void>,
    ) =>
      withTopicStoreMutation(
        store,
        Effect.gen({ self: this }, function* () {
          yield* this.ensureOpen();
          const queryId = `query-${this.nextQueryId}`;
          this.nextQueryId += 1;
          const acquiredSubscription = yield* subscribeExecutableQuery<ResultRow>(
            topic,
            store,
            query,
            {
              queryId,
              queueCapacity: this.subscriptionQueueCapacity,
            },
          );
          yield* markAcquired(acquiredSubscription);
          return acquiredSubscription;
        }),
      );
    const subscription = yield* acquireLiveSubscriptionHandoff(acquireSubscription);

    return {
      events: subscription.events,
      close: subscription.close,
    };
  });

  readonly health: ColumnLiveViewEngine<Topics>["health"] = Effect.fn(
    "ColumnLiveViewEngine.health",
  )({ self: this }, function* (this: InMemoryColumnLiveViewEngine<Topics>) {
    return yield* collectColumnLiveViewEngineHealth<Topics>(this.stores, {
      version: () => this.engineVersion,
      closed: () => this.closed,
    });
  });

  readonly reset: ColumnLiveViewEngine<Topics>["reset"] = Effect.fn("ColumnLiveViewEngine.reset")(
    { self: this },
    function* (this: InMemoryColumnLiveViewEngine<Topics>) {
      yield* this.ensureOpen();
      yield* Effect.uninterruptible(
        Effect.gen({ self: this }, function* () {
          for (const store of this.stores.values()) {
            yield* resetTopicStore(store);
          }
          this.engineVersion = 0;
        }),
      );
    },
  );

  readonly close: ColumnLiveViewEngine<Topics>["close"] = Effect.fn("ColumnLiveViewEngine.close")(
    { self: this },
    function* (this: InMemoryColumnLiveViewEngine<Topics>) {
      yield* Effect.uninterruptible(
        Effect.gen({ self: this }, function* () {
          this.closed = true;
          for (const store of this.stores.values()) {
            yield* closeTopicStoreSubscriptions(store);
          }
        }),
      );
    },
  );
}

export const createColumnLiveViewEngine = Effect.fn("ColumnLiveViewEngine.make")(
  <const Topics extends DecodableTopicDefinitions>(
    config: ColumnLiveViewEngineConfig<Topics>,
  ): Effect.Effect<ColumnLiveViewEngine<Topics>> =>
    Effect.sync(() => new InMemoryColumnLiveViewEngine(config)),
);
