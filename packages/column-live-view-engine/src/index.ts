import type { LiveQueryRow, LiveQueryResult, TopicRow } from "@view-server/config";
import { Effect } from "effect";
import type {
  AnyTopicRow,
  ColumnLiveViewEngine,
  ColumnLiveViewEngineConfig,
  DecodableTopicDefinitions,
} from "./engine-contract";
import {
  EngineClosedError,
  InvalidRowError,
  InvalidTopicError,
  UnsupportedQueryError,
} from "./engine-errors";
import { collectColumnLiveViewEngineHealth } from "./engine-health";
import { makeLiveSubscription } from "./live-subscription";
import {
  evaluateCompiledRawQuery,
  prepareRawQuery,
  type QueryEvaluation,
} from "./raw-query-compiler";
import {
  closeTopicStoreSubscriptions,
  deleteTopicStoreRow,
  notifyTopicStoreSubscribers,
  patchTopicStoreRow,
  publishTopicStoreRow,
  publishTopicStoreRows,
  resetTopicStore,
  TopicStore,
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

type RowObject = object;
const defaultSubscriptionQueueCapacity = 1_024;

const isGroupedQuery = (query: unknown): boolean =>
  typeof query === "object" &&
  query !== null &&
  !Array.isArray(query) &&
  ("groupBy" in query || "aggregates" in query);

const liveQueryResult = <Row extends RowObject>(
  evaluation: QueryEvaluation<Row>,
): LiveQueryResult<Row> => ({
  rows: evaluation.rows,
  totalRows: evaluation.totalRows,
  version: evaluation.version,
});

const invalidRow = (topic: string, message: string) =>
  new InvalidRowError({
    topic,
    message,
  });

class InMemoryColumnLiveViewEngine<
  Topics extends DecodableTopicDefinitions,
> implements ColumnLiveViewEngine<Topics> {
  private readonly stores = new Map<
    Extract<keyof Topics, string>,
    TopicStore<AnyTopicRow<Topics>>
  >();
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
        new TopicStore<AnyTopicRow<Topics>>(topic, definition.schema, definition.key),
      );
    }
  }

  private getStore<Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
  ): Effect.Effect<TopicStore<TopicRow<Topics, Topic>>, InvalidTopicError> {
    return Effect.gen({ self: this }, function* () {
      const store = this.stores.get(topic);
      if (store === undefined) {
        return yield* InvalidTopicError.make({
          topic,
          message: `Unknown topic: ${topic}`,
        });
      }
      return store as TopicStore<TopicRow<Topics, Topic>>;
    });
  }

  private ensureOpen(): Effect.Effect<void, EngineClosedError> {
    return Effect.gen({ self: this }, function* () {
      if (this.closed) {
        return yield* EngineClosedError.make({
          message: "ColumnLiveViewEngine is closed.",
        });
      }
    });
  }

  private commit<Row extends RowObject>(store: TopicStore<Row>): Effect.Effect<void> {
    return Effect.gen({ self: this }, function* () {
      store.version += 1;
      this.engineVersion += 1;
      yield* notifyTopicStoreSubscribers(store);
    });
  }

  readonly publish: ColumnLiveViewEngine<Topics>["publish"] = (topic, row) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      yield* publishTopicStoreRow(store, row, invalidRow, (committedStore) =>
        this.commit(committedStore),
      );
    });
  };

  readonly publishMany: ColumnLiveViewEngine<Topics>["publishMany"] = (topic, rows) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      yield* publishTopicStoreRows(store, rows, invalidRow, (committedStore) =>
        this.commit(committedStore),
      );
    });
  };

  readonly patch: ColumnLiveViewEngine<Topics>["patch"] = (topic, key, patch) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      yield* patchTopicStoreRow(store, key, patch, invalidRow, (committedStore) =>
        this.commit(committedStore),
      );
    });
  };

  readonly delete: ColumnLiveViewEngine<Topics>["delete"] = (topic, key) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      yield* deleteTopicStoreRow(store, key, (committedStore) => this.commit(committedStore));
    });
  };

  readonly snapshot: ColumnLiveViewEngine<Topics>["snapshot"] = (topic, query) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      if (isGroupedQuery(query)) {
        return yield* UnsupportedQueryError.make({
          topic,
          message: "Grouped aggregate queries are not implemented in this slice.",
        });
      }
      const store = yield* this.getStore(topic);
      type ResultRow = LiveQueryRow<TopicRow<Topics, typeof topic>, typeof query>;
      const compiled = yield* prepareRawQuery<TopicRow<Topics, typeof topic>, ResultRow>(
        topic,
        store.rawQueryMetadata,
        query,
      );
      return liveQueryResult(evaluateCompiledRawQuery(store, compiled));
    });
  };

  readonly subscribe: ColumnLiveViewEngine<Topics>["subscribe"] = (topic, query) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      if (isGroupedQuery(query)) {
        return yield* UnsupportedQueryError.make({
          topic,
          message: "Grouped aggregate queries are not implemented in this slice.",
        });
      }
      const store = yield* this.getStore(topic);
      const queryId = `query-${this.nextQueryId}`;
      this.nextQueryId += 1;
      type StoreRow = TopicRow<Topics, typeof topic>;
      type ResultRow = LiveQueryRow<StoreRow, typeof query>;
      const compiled = yield* prepareRawQuery<StoreRow, ResultRow>(
        topic,
        store.rawQueryMetadata,
        query,
      );
      const subscription = yield* makeLiveSubscription({
        store,
        queryId,
        compiled,
        queueCapacity: this.subscriptionQueueCapacity,
      });

      return {
        events: subscription.events,
        close: subscription.close,
      };
    });
  };

  readonly health: ColumnLiveViewEngine<Topics>["health"] = () => {
    return collectColumnLiveViewEngineHealth<Topics, AnyTopicRow<Topics>>(this.stores, {
      version: () => this.engineVersion,
      closed: () => this.closed,
    });
  };

  readonly reset: ColumnLiveViewEngine<Topics>["reset"] = () => {
    return Effect.gen({ self: this }, function* () {
      for (const store of this.stores.values()) {
        yield* resetTopicStore(store);
      }
      this.engineVersion = 0;
    });
  };

  readonly close: ColumnLiveViewEngine<Topics>["close"] = () => {
    return Effect.gen({ self: this }, function* () {
      if (!this.closed) {
        this.closed = true;
        for (const store of this.stores.values()) {
          yield* closeTopicStoreSubscriptions(store);
        }
      }
    });
  };
}

export const createColumnLiveViewEngine = <const Topics extends DecodableTopicDefinitions>(
  config: ColumnLiveViewEngineConfig<Topics>,
): Effect.Effect<ColumnLiveViewEngine<Topics>> =>
  Effect.sync(() => new InMemoryColumnLiveViewEngine(config));
