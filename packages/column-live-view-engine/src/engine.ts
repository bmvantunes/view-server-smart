import type {
  ExactGroupedQuery,
  ExactLiveQuery,
  ExactPatch,
  ExactRawQuery,
  GroupedQuery,
  GroupedResult,
  LiveQueryRow,
  LiveQueryResult,
  PickRawFields,
  RawQuery,
  TopicRow,
  ValidateLiveQuery,
} from "@view-server/config";
import { Effect } from "effect";
import type {
  ColumnLiveViewEngine,
  ColumnLiveViewEngineConfig,
  ColumnLiveViewEngineInternal,
  ColumnLiveViewSubscription,
  DecodableTopicDefinitions,
} from "./engine-contract";
import {
  EngineClosedError,
  InvalidRowError,
  InvalidTopicError,
  type ColumnLiveViewEngineError,
} from "./engine-errors";
import { collectColumnLiveViewEngineHealth } from "./engine-health";
import {
  groupedIncrementalAdmissionLimitsFromConfig,
  type GroupedIncrementalAdmissionLimits,
} from "./grouped-incremental-admission";
import type { LiveSubscription } from "./live-subscription";
import { snapshotExecutableQuery, subscribeExecutableQuery } from "./query-execution";
import {
  acquireTopicStoreSubscription,
  closeTopicStoreSubscriptions,
  deleteTopicStoreRow,
  patchTopicStoreRow,
  publishTopicStoreRow,
  publishTopicStoreRows,
  publishTopicStoreRowsWithStorageKeys,
  resetTopicStore,
  TopicStore,
} from "./topic-store";

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
  private readonly groupedIncrementalAdmissionLimits: GroupedIncrementalAdmissionLimits;
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
    this.groupedIncrementalAdmissionLimits = groupedIncrementalAdmissionLimitsFromConfig(
      config.groupedIncrementalAdmissionLimits,
    );
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

  readonly publishManyWithStorageKeys: ColumnLiveViewEngineInternal<Topics>["publishManyWithStorageKeys"] =
    Effect.fn("ColumnLiveViewEngine.publishManyWithStorageKeys")({ self: this }, function* <
      Topic extends Extract<keyof Topics, string>,
    >(this: InMemoryColumnLiveViewEngine<Topics>, topic: Topic, rows: Parameters<ColumnLiveViewEngineInternal<Topics>["publishManyWithStorageKeys"]>[1]) {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      yield* publishTopicStoreRowsWithStorageKeys(store, rows, invalidRow);
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

  private readonly snapshotQuery = <
    Topic extends Extract<keyof Topics, string>,
    ResultRow extends object,
  >(
    topic: Topic,
    query: unknown,
  ) =>
    Effect.fn("ColumnLiveViewEngine.snapshot")(
      { self: this },
      function* (this: InMemoryColumnLiveViewEngine<Topics>) {
        yield* this.ensureOpen();
        const store = yield* this.getStore(topic);
        return yield* snapshotExecutableQuery<ResultRow>(store, query);
      },
    )();

  snapshot<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: Query &
      ExactLiveQuery<TopicRow<Topics, Topic>, NoInfer<Query>> &
      ValidateLiveQuery<NoInfer<Query>>,
  ): Effect.Effect<
    LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  snapshot<
    Topic extends Extract<keyof Topics, string>,
    const Query extends GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: Query &
      ExactGroupedQuery<TopicRow<Topics, Topic>, NoInfer<Query>> &
      ValidateLiveQuery<NoInfer<Query>>,
  ): Effect.Effect<
    LiveQueryResult<GroupedResult<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  snapshot<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: Query &
      ExactRawQuery<TopicRow<Topics, Topic>, NoInfer<Query>> &
      ValidateLiveQuery<NoInfer<Query>>,
  ): Effect.Effect<
    LiveQueryResult<PickRawFields<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  snapshot<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: Query &
      ExactLiveQuery<TopicRow<Topics, Topic>, NoInfer<Query>> &
      ValidateLiveQuery<NoInfer<Query>>,
  ): Effect.Effect<
    LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  > {
    return this.snapshotQuery(topic, query);
  }

  private readonly subscribeQuery = <
    Topic extends Extract<keyof Topics, string>,
    ResultRow extends object,
  >(
    topic: Topic,
    query: unknown,
  ) =>
    Effect.fn("ColumnLiveViewEngine.subscribe")(
      { self: this },
      function* (this: InMemoryColumnLiveViewEngine<Topics>) {
        yield* this.ensureOpen();
        const store = yield* this.getStore(topic);
        const subscription = yield* acquireTopicStoreSubscription(
          store,
          (
            permit,
            markAcquired: (subscription: LiveSubscription<ResultRow>) => Effect.Effect<void>,
          ): Effect.Effect<LiveSubscription<ResultRow>, ColumnLiveViewEngineError> =>
            Effect.gen({ self: this }, function* () {
              yield* this.ensureOpen();
              const queryId = `query-${this.nextQueryId}`;
              this.nextQueryId += 1;
              const acquiredSubscription = yield* subscribeExecutableQuery<ResultRow>(query, {
                groupedIncrementalAdmissionLimits: this.groupedIncrementalAdmissionLimits,
                permit,
                queryId,
                queueCapacity: this.subscriptionQueueCapacity,
              });
              yield* markAcquired(acquiredSubscription);
              return acquiredSubscription;
            }),
        );

        return {
          events: subscription.events,
          close: subscription.close,
        };
      },
    )();

  subscribe<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: Query &
      ExactLiveQuery<TopicRow<Topics, Topic>, NoInfer<Query>> &
      ValidateLiveQuery<NoInfer<Query>>,
  ): Effect.Effect<
    ColumnLiveViewSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  subscribe<
    Topic extends Extract<keyof Topics, string>,
    const Query extends GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: Query &
      ExactGroupedQuery<TopicRow<Topics, Topic>, NoInfer<Query>> &
      ValidateLiveQuery<NoInfer<Query>>,
  ): Effect.Effect<
    ColumnLiveViewSubscription<GroupedResult<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  subscribe<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: Query &
      ExactRawQuery<TopicRow<Topics, Topic>, NoInfer<Query>> &
      ValidateLiveQuery<NoInfer<Query>>,
  ): Effect.Effect<
    ColumnLiveViewSubscription<PickRawFields<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  subscribe<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: Query &
      ExactLiveQuery<TopicRow<Topics, Topic>, NoInfer<Query>> &
      ValidateLiveQuery<NoInfer<Query>>,
  ): Effect.Effect<
    ColumnLiveViewSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  > {
    return this.subscribeQuery(topic, query);
  }

  readonly subscribeRuntime: ColumnLiveViewEngine<Topics>["subscribeRuntime"] = (topic, query) =>
    this.subscribeQuery(topic, query);

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

const publicColumnLiveViewEngine = <Topics extends DecodableTopicDefinitions>(
  engine: InMemoryColumnLiveViewEngine<Topics>,
): ColumnLiveViewEngine<Topics> => {
  Reflect.deleteProperty(engine, "publishManyWithStorageKeys");
  return engine;
};

export const createColumnLiveViewEngine = Effect.fn("ColumnLiveViewEngine.make")(
  <const Topics extends DecodableTopicDefinitions>(
    config: ColumnLiveViewEngineConfig<Topics>,
  ): Effect.Effect<ColumnLiveViewEngine<Topics>> =>
    Effect.sync(() => publicColumnLiveViewEngine(new InMemoryColumnLiveViewEngine(config))),
);

export const createColumnLiveViewEngineInternal = Effect.fn("ColumnLiveViewEngine.internal.make")(
  <const Topics extends DecodableTopicDefinitions>(
    config: ColumnLiveViewEngineConfig<Topics>,
  ): Effect.Effect<ColumnLiveViewEngineInternal<Topics>> =>
    Effect.sync(() => new InMemoryColumnLiveViewEngine(config)),
);
