import type {
  DeltaEvent,
  FieldKey,
  LiveQueryRow,
  LiveQueryResult,
  OrderBy,
  RawQuery,
  RowFromSchema,
  RowSchema,
  SnapshotEvent,
  StatusEvent,
  StringFieldKey,
  TopicRow,
} from "@view-server/config";
import { Effect, Schema, Semaphore, Stream } from "effect";
import {
  collectColumnLiveViewEngineHealth,
  type ColumnLiveViewEngineHealth,
  type HealthTopicStoreState,
} from "./engine-health.js";
import { makeLiveSubscription, type LiveTopicSubscriber } from "./live-subscription.js";
import {
  evaluateCompiledRawQuery,
  InvalidQueryError,
  prepareRawQuery,
  rawQueryCompilerMetadata,
  type QueryEvaluation,
  type RawQueryCompilerMetadata,
} from "./raw-query-compiler.js";
import { cloneRow, fieldValue } from "./row-values.js";

export { InvalidQueryError } from "./raw-query-compiler.js";
export type { ColumnLiveViewEngineHealth, ColumnLiveViewTopicHealth } from "./engine-health.js";

export type DecodableTopicDefinitions = Record<
  string,
  {
    readonly schema: RowSchema & Schema.Decoder<object>;
    readonly key: string;
  }
>;

type ValidateEngineTopics<Topics extends DecodableTopicDefinitions> = {
  readonly [Topic in keyof Topics]: Topics[Topic] extends {
    readonly schema: infer S extends RowSchema & Schema.Decoder<object>;
    readonly key: infer Key extends string;
  }
    ? {
        readonly schema: S;
        readonly key: Key & StringFieldKey<RowFromSchema<S>>;
      }
    : never;
};

export type ColumnLiveViewEngineConfig<Topics extends DecodableTopicDefinitions> = {
  readonly topics: Topics & ValidateEngineTopics<Topics>;
  readonly subscriptionQueueCapacity?: number;
};

export class InvalidTopicError extends Schema.TaggedErrorClass<InvalidTopicError>()(
  "InvalidTopicError",
  {
    topic: Schema.String,
    message: Schema.String,
  },
) {}

export class InvalidRowError extends Schema.TaggedErrorClass<InvalidRowError>()("InvalidRowError", {
  topic: Schema.String,
  message: Schema.String,
}) {}

export class UnsupportedQueryError extends Schema.TaggedErrorClass<UnsupportedQueryError>()(
  "UnsupportedQueryError",
  {
    topic: Schema.String,
    message: Schema.String,
  },
) {}

export class EngineClosedError extends Schema.TaggedErrorClass<EngineClosedError>()(
  "EngineClosedError",
  {
    message: Schema.String,
  },
) {}

export type ColumnLiveViewEngineError =
  | InvalidTopicError
  | InvalidRowError
  | UnsupportedQueryError
  | InvalidQueryError
  | EngineClosedError;

export type ColumnLiveViewEngineEvent<Row> = SnapshotEvent<Row> | DeltaEvent<Row> | StatusEvent;

export type ColumnLiveViewSubscription<Row> = {
  readonly events: Stream.Stream<ColumnLiveViewEngineEvent<Row>>;
  readonly close: () => Effect.Effect<void, never>;
};

type AnyTopicRow<Topics extends DecodableTopicDefinitions> = TopicRow<
  Topics,
  Extract<keyof Topics, string>
>;

type RejectExtraKeys<Candidate, Shape> = {
  readonly [Key in Exclude<keyof Candidate, keyof Shape>]: never;
};

type IsUnion<Value, Candidate = Value> = Value extends unknown
  ? [Candidate] extends [Value]
    ? false
    : true
  : false;

type TupleHasUnionElement<Tuple extends ReadonlyArray<unknown>> = Tuple extends readonly [
  infer Head,
  ...infer Tail,
]
  ? IsUnion<Head> extends true
    ? true
    : TupleHasUnionElement<Tail>
  : false;

type ExactRawQuery<Row, Query> = Query &
  RejectExtraKeys<Query, RawQuery<Row>> & {
    readonly groupBy?: never;
    readonly aggregates?: never;
  } & ExactWhere<Row, Query> &
  ExactOrderBy<Row, Query> &
  RejectDynamicRawFields<Row, Query>;

type RejectDynamicRawFields<Row, Query> = "fields" extends keyof Query
  ? Query extends { readonly fields?: infer Fields }
    ? NonNullable<Fields> extends ReadonlyArray<unknown>
      ? undefined extends Query["fields"]
        ? {
            readonly fields: never;
          }
        : IsUnion<NonNullable<Fields>> extends true
          ? {
              readonly fields: never;
            }
          : number extends NonNullable<Fields>["length"]
            ? {
                readonly fields: never;
              }
            : TupleHasUnionElement<NonNullable<Fields>> extends true
              ? {
                  readonly fields: never;
                }
              : NonNullable<Fields>[number] extends FieldKey<Row>
                ? unknown
                : {
                    readonly fields: never;
                  }
      : unknown
    : unknown
  : unknown;

type ExactWhere<Row, Query> = Query extends {
  readonly where: infer Where;
}
  ? {
      readonly where: Where &
        RejectExtraKeys<Where, { readonly [Field in FieldKey<Row>]?: unknown }> & {
          readonly [Field in Extract<keyof Where, FieldKey<Row>>]: ExactFilter<
            Row[Field],
            Where[Field]
          >;
        };
    }
  : unknown;

type ExactFilter<Value, Filter> = Value extends object
  ? unknown
  : ExactOperatorFilter<Value, Filter>;

type ExactOperatorFilter<Value, Filter> = Filter extends object
  ? Filter extends ReadonlyArray<unknown>
    ? unknown
    : Filter & RejectExtraKeys<Filter, FieldFilterShape<Value>>
  : unknown;

type FieldFilterShape<Value> = Value extends string
  ? {
      readonly eq?: Value;
      readonly neq?: Value;
      readonly in?: ReadonlyArray<Value>;
      readonly startsWith?: string;
    }
  : {
      readonly eq?: Value;
      readonly neq?: Value;
      readonly in?: ReadonlyArray<Value>;
      readonly gt?: Value;
      readonly gte?: Value;
      readonly lt?: Value;
      readonly lte?: Value;
    };

type ExactOrderBy<Row, Query> = Query extends {
  readonly orderBy: ReadonlyArray<infer Entry>;
}
  ? {
      readonly orderBy: ReadonlyArray<Entry & RejectExtraKeys<Entry, OrderBy<Row>>>;
    }
  : unknown;

type ExactPatch<Row, Patch> = Patch & RejectExtraKeys<Patch, Partial<Row>>;

export type ColumnLiveViewEngine<Topics extends DecodableTopicDefinitions> = {
  readonly publish: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    row: TopicRow<Topics, Topic>,
  ) => Effect.Effect<void, ColumnLiveViewEngineError>;
  readonly publishMany: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    rows: ReadonlyArray<TopicRow<Topics, Topic>>,
  ) => Effect.Effect<void, ColumnLiveViewEngineError>;
  readonly patch: <
    Topic extends Extract<keyof Topics, string>,
    const Patch extends Partial<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    key: string,
    patch: ExactPatch<TopicRow<Topics, Topic>, Patch>,
  ) => Effect.Effect<void, ColumnLiveViewEngineError>;
  readonly delete: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    key: string,
  ) => Effect.Effect<void, ColumnLiveViewEngineError>;
  readonly snapshot: <
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactRawQuery<TopicRow<Topics, Topic>, Query>,
  ) => Effect.Effect<
    LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  readonly subscribe: <
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactRawQuery<TopicRow<Topics, Topic>, Query>,
  ) => Effect.Effect<
    ColumnLiveViewSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  readonly health: () => Effect.Effect<ColumnLiveViewEngineHealth<Topics>, never>;
  readonly reset: () => Effect.Effect<void, never>;
  readonly close: () => Effect.Effect<void, never>;
};

type RowObject = object;
const defaultSubscriptionQueueCapacity = 1_024;

class TopicStore<Row extends RowObject> implements HealthTopicStoreState<Row> {
  readonly rows = new Map<string, Row>();
  readonly subscribers = new Set<LiveTopicSubscriber<Row>>();
  readonly mutationSemaphore = Semaphore.makeUnsafe(1);
  version = 0;
  maxQueueDepth = 0;
  backpressureEvents = 0;

  constructor(
    readonly topic: string,
    readonly schema: Schema.Decoder<object>,
    readonly keyField: string,
    readonly rawQueryMetadata: RawQueryCompilerMetadata,
  ) {}
}

const isGroupedQuery = (query: unknown): boolean =>
  typeof query === "object" &&
  query !== null &&
  !Array.isArray(query) &&
  ("groupBy" in query || "aggregates" in query);

const safeCloneRow = Effect.fn("ColumnLiveViewEngine.safeCloneRow")(function* <
  Row extends RowObject,
>(store: TopicStore<Row>, row: Row) {
  return yield* Effect.try({
    try: () => cloneRow(row),
    catch: (cause) =>
      InvalidRowError.make({
        topic: store.topic,
        message: String(cause),
      }),
  });
});

const liveQueryResult = <Row extends RowObject>(
  evaluation: QueryEvaluation<Row>,
): LiveQueryResult<Row> => ({
  rows: evaluation.rows,
  totalRows: evaluation.totalRows,
  version: evaluation.version,
});

const decodeRow = Effect.fn("ColumnLiveViewEngine.decodeRow")(function* <Row extends RowObject>(
  store: TopicStore<Row>,
  row: RowObject,
) {
  return yield* Effect.try({
    try: () => Schema.decodeUnknownSync(store.schema)(row) as Row,
    catch: (cause) =>
      InvalidRowError.make({
        topic: store.topic,
        message: String(cause),
      }),
  });
});

const rowKey = Effect.fn("ColumnLiveViewEngine.rowKey")(function* <Row extends RowObject>(
  store: TopicStore<Row>,
  row: Row,
) {
  const key = fieldValue(row, store.keyField);
  if (typeof key !== "string") {
    return yield* InvalidRowError.make({
      topic: store.topic,
      message: `Key field ${store.keyField} must decode to a string.`,
    });
  }
  return key;
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
      const definition = config.topics[topic];
      this.stores.set(
        topic,
        new TopicStore<AnyTopicRow<Topics>>(
          topic,
          definition.schema,
          definition.key,
          rawQueryCompilerMetadata(definition.schema),
        ),
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

  private notifySubscribers<Row extends RowObject>(store: TopicStore<Row>): Effect.Effect<void> {
    return Effect.gen(function* () {
      for (const subscriber of store.subscribers) {
        yield* subscriber.notify(store);
      }
    });
  }

  private commit<Row extends RowObject>(store: TopicStore<Row>): Effect.Effect<void> {
    return Effect.gen({ self: this }, function* () {
      store.version += 1;
      this.engineVersion += 1;
      yield* this.notifySubscribers(store);
    });
  }

  readonly publish: ColumnLiveViewEngine<Topics>["publish"] = (topic, row) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      const decoded = yield* decodeRow(store, row);
      const key = yield* rowKey(store, decoded);
      const cloned = yield* safeCloneRow(store, decoded);
      yield* store.mutationSemaphore.withPermits(1)(
        Effect.gen({ self: this }, function* () {
          store.rows.set(key, cloned);
          yield* this.commit(store);
        }),
      );
    });
  };

  readonly publishMany: ColumnLiveViewEngine<Topics>["publishMany"] = (topic, rows) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      const decodedRows = yield* Effect.forEach(rows, (row) => decodeRow(store, row));
      const keyedRows = yield* Effect.forEach(decodedRows, (row) =>
        Effect.gen(function* () {
          const key = yield* rowKey(store, row);
          const cloned = yield* safeCloneRow(store, row);
          return { key, row: cloned };
        }),
      );
      yield* store.mutationSemaphore.withPermits(1)(
        Effect.gen({ self: this }, function* () {
          for (const { key, row } of keyedRows) {
            store.rows.set(key, row);
          }
          yield* this.commit(store);
        }),
      );
    });
  };

  readonly patch: ColumnLiveViewEngine<Topics>["patch"] = (topic, key, patch) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      yield* store.mutationSemaphore.withPermits(1)(
        Effect.gen({ self: this }, function* () {
          const current = store.rows.get(key);
          if (current === undefined) {
            return yield* InvalidRowError.make({
              topic,
              message: `Cannot patch missing key: ${key}`,
            });
          }
          const decoded = yield* decodeRow(store, { ...current, ...patch });
          const decodedKey = yield* rowKey(store, decoded);
          if (decodedKey !== key) {
            return yield* InvalidRowError.make({
              topic,
              message: "Patch must not change the row key.",
            });
          }
          const cloned = yield* safeCloneRow(store, decoded);
          store.rows.set(key, cloned);
          yield* this.commit(store);
        }),
      );
    });
  };

  readonly delete: ColumnLiveViewEngine<Topics>["delete"] = (topic, key) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      yield* store.mutationSemaphore.withPermits(1)(
        Effect.gen({ self: this }, function* () {
          store.rows.delete(key);
          yield* this.commit(store);
        }),
      );
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
        for (const subscriber of store.subscribers) {
          subscriber.closed = true;
          yield* subscriber.end;
        }
        store.subscribers.clear();
        store.rows.clear();
        store.version = 0;
        store.maxQueueDepth = 0;
        store.backpressureEvents = 0;
      }
      this.engineVersion = 0;
    });
  };

  readonly close: ColumnLiveViewEngine<Topics>["close"] = () => {
    return Effect.gen({ self: this }, function* () {
      if (!this.closed) {
        this.closed = true;
        for (const store of this.stores.values()) {
          for (const subscriber of store.subscribers) {
            subscriber.closed = true;
            yield* subscriber.end;
          }
          store.subscribers.clear();
        }
      }
    });
  };
}

export const createColumnLiveViewEngine = <const Topics extends DecodableTopicDefinitions>(
  config: ColumnLiveViewEngineConfig<Topics>,
): Effect.Effect<ColumnLiveViewEngine<Topics>> =>
  Effect.sync(() => new InMemoryColumnLiveViewEngine(config));
