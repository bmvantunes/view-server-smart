import type { LiveQueryRow, TopicRow } from "@view-server/config";
import { Effect } from "effect";
import type {
  ColumnLiveViewEngine,
  ColumnLiveViewEngineConfig,
  DecodableTopicDefinitions,
} from "./engine-contract";
import { EngineClosedError, InvalidRowError, InvalidTopicError } from "./engine-errors";
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
  private readonly stores = new Map<Extract<keyof Topics, string>, TopicStore<object>>();
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
        new TopicStore<object>(topic, definition.schema, definition.key, () => {
          this.engineVersion += 1;
        }),
      );
    }
  }

  private getStore<Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
  ): Effect.Effect<TopicStore<object>, InvalidTopicError> {
    return Effect.gen({ self: this }, function* () {
      const store = this.stores.get(topic);
      if (store === undefined) {
        return yield* InvalidTopicError.make({
          topic,
          message: `Unknown topic: ${topic}`,
        });
      }
      return store;
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

  readonly publish: ColumnLiveViewEngine<Topics>["publish"] = (topic, row) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      yield* publishTopicStoreRow(store, row, invalidRow);
    });
  };

  readonly publishMany: ColumnLiveViewEngine<Topics>["publishMany"] = (topic, rows) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      yield* publishTopicStoreRows(store, rows, invalidRow);
    });
  };

  readonly patch: ColumnLiveViewEngine<Topics>["patch"] = (topic, key, patch) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      yield* patchTopicStoreRow(store, key, patch, invalidRow);
    });
  };

  readonly delete: ColumnLiveViewEngine<Topics>["delete"] = (topic, key) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      yield* deleteTopicStoreRow(store, key);
    });
  };

  readonly snapshot: ColumnLiveViewEngine<Topics>["snapshot"] = (topic, query) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      return yield* snapshotExecutableQuery<
        object,
        LiveQueryRow<TopicRow<Topics, typeof topic>, typeof query>
      >(topic, store, query);
    });
  };

  readonly subscribe: ColumnLiveViewEngine<Topics>["subscribe"] = (topic, query) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      const queryId = `query-${this.nextQueryId}`;
      this.nextQueryId += 1;
      const subscription = yield* subscribeExecutableQuery<
        object,
        LiveQueryRow<TopicRow<Topics, typeof topic>, typeof query>
      >(topic, store, query, { queryId, queueCapacity: this.subscriptionQueueCapacity });

      return {
        events: subscription.events,
        close: subscription.close,
      };
    });
  };

  readonly health: ColumnLiveViewEngine<Topics>["health"] = () => {
    return collectColumnLiveViewEngineHealth<Topics, object>(this.stores, {
      version: () => this.engineVersion,
      closed: () => this.closed,
    });
  };

  readonly reset: ColumnLiveViewEngine<Topics>["reset"] = () => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
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
