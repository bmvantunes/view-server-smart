import type {
  DeltaEvent,
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
  RowFromSchema,
  RowSchema,
  SnapshotEvent,
  StatusEvent,
  StringFieldKey,
  TopicRow,
  ValidateLiveQuery,
} from "@view-server/config";
import type { Effect, Schema, Stream } from "effect";
import type { ColumnLiveViewEngineHealth } from "./engine-health";
import type { ColumnLiveViewEngineError, EngineClosedError } from "./engine-errors";
import type { GroupedIncrementalAdmissionLimits } from "./grouped-incremental-admission";

export type DecodableTopicDefinitions = Record<
  string,
  {
    readonly schema: RowSchema & Schema.Codec<object, unknown, never, unknown>;
    readonly key: string;
  }
>;

type ValidateEngineTopics<Topics extends DecodableTopicDefinitions> = {
  readonly [Topic in keyof Topics]: Topics[Topic] extends {
    readonly schema: infer S extends RowSchema & Schema.Codec<object, unknown, never, unknown>;
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
  readonly groupedIncrementalAdmissionLimits?: Partial<GroupedIncrementalAdmissionLimits>;
  readonly subscriptionQueueCapacity?: number;
};

export type ColumnLiveViewEngineEvent<Row> = SnapshotEvent<Row> | DeltaEvent<Row> | StatusEvent;

export type ColumnLiveViewSubscription<Row> = {
  readonly events: Stream.Stream<ColumnLiveViewEngineEvent<Row>>;
  readonly close: () => Effect.Effect<void, never>;
};

type EngineSnapshot<Topics extends DecodableTopicDefinitions> = {
  <
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
  <
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
  <
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
};

type EngineSubscribe<Topics extends DecodableTopicDefinitions> = {
  <
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
  <
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
  <
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
};

export type AnyTopicRow<Topics extends DecodableTopicDefinitions> = TopicRow<
  Topics,
  Extract<keyof Topics, string>
>;

type TopicRowWithStorageKey<Row extends object> = {
  readonly storageKey: string;
  readonly row: Row;
};

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
  readonly snapshot: EngineSnapshot<Topics>;
  readonly subscribe: EngineSubscribe<Topics>;
  readonly subscribeRuntime: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    query: unknown,
  ) => Effect.Effect<ColumnLiveViewSubscription<object>, ColumnLiveViewEngineError>;
  readonly health: () => Effect.Effect<ColumnLiveViewEngineHealth<Topics>, never>;
  readonly reset: () => Effect.Effect<void, EngineClosedError>;
  readonly close: () => Effect.Effect<void, never>;
};

export type ColumnLiveViewEngineInternal<Topics extends DecodableTopicDefinitions> =
  ColumnLiveViewEngine<Topics> & {
    readonly publishManyWithStorageKeys: <Topic extends Extract<keyof Topics, string>>(
      topic: Topic,
      rows: ReadonlyArray<TopicRowWithStorageKey<TopicRow<Topics, Topic>>>,
    ) => Effect.Effect<void, ColumnLiveViewEngineError>;
  };
