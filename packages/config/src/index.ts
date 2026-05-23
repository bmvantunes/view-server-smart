import type { DescMessage, MessageShape } from "@bufbuild/protobuf";
import type { Clock, Config, Effect, Schema } from "effect";
import type * as BigDecimal from "effect/BigDecimal";

export type TopicName = string;
export type SortDirection = "asc" | "desc";
export type AggregateKind = "count" | "countDistinct" | "sum" | "min" | "max" | "avg";
export type RuntimeStatus = "ready" | "degraded" | "starting" | "stopping";
export type TopicHealthStatus = "ready" | "degraded" | "starting";
export type KafkaRegionStatus = "connected" | "disconnected" | "degraded" | "starting";
export type KafkaTopicStatus = "ready" | "degraded" | "starting" | "stalled";

export type SchemaType<S> = Schema.Schema.Type<S>;
export type RowSchema = Schema.Schema<object>;
export type RowFromSchema<S extends RowSchema> = SchemaType<S>;

export type StringFieldKey<Row> = Extract<
  {
    readonly [Key in keyof Row]-?: Row[Key] extends string ? Key : never;
  }[keyof Row],
  string
>;

export type NumericFieldKey<Row> = Extract<
  {
    readonly [Key in keyof Row]-?: Row[Key] extends number | bigint | BigDecimal.BigDecimal
      ? Key
      : never;
  }[keyof Row],
  string
>;

export type FieldKey<Row> = Extract<keyof Row, string>;

export type TopicDefinition<S extends RowSchema, Key extends string> = {
  readonly schema: S;
  readonly key: Key;
};

export type TopicDefinitions = Record<string, TopicDefinition<RowSchema, string>>;

export type TopicSchema<Topics, Topic extends keyof Topics> = Topics[Topic] extends {
  readonly schema: infer S extends RowSchema;
}
  ? S
  : never;

export type TopicRow<Topics, Topic extends keyof Topics> = RowFromSchema<
  TopicSchema<Topics, Topic>
>;

export type EqualityFilter<Value> =
  | Value
  | {
      readonly eq?: Value;
      readonly neq?: Value;
      readonly in?: ReadonlyArray<Value>;
    };

export type RangeFilter<Value> =
  | Value
  | {
      readonly eq?: Value;
      readonly neq?: Value;
      readonly in?: ReadonlyArray<Value>;
      readonly gt?: Value;
      readonly gte?: Value;
      readonly lt?: Value;
      readonly lte?: Value;
    };

export type StringFilter<Value extends string> =
  | Value
  | {
      readonly eq?: Value;
      readonly neq?: Value;
      readonly in?: ReadonlyArray<Value>;
      readonly startsWith?: string;
    };

export type FieldFilter<Value> = Value extends string
  ? StringFilter<Value>
  : Value extends number | bigint | BigDecimal.BigDecimal
    ? RangeFilter<Value>
    : EqualityFilter<Value>;

export type Where<Row> = {
  readonly [Field in FieldKey<Row>]?: FieldFilter<Row[Field]>;
};

export type OrderBy<Row> = {
  readonly field: FieldKey<Row>;
  readonly direction: SortDirection;
};

export type RawQuery<Row> = {
  readonly where?: Where<Row>;
  readonly orderBy?: ReadonlyArray<OrderBy<Row>>;
  readonly offset?: number;
  readonly limit?: number;
  readonly fields?: ReadonlyArray<FieldKey<Row>>;
};

export type CountAggregate<Alias extends string = string> = {
  readonly type: "count";
  readonly as: Alias;
};

export type CountDistinctAggregate<Row, Alias extends string = string> = {
  readonly type: "countDistinct";
  readonly field: FieldKey<Row>;
  readonly as: Alias;
};

export type SumAggregate<Row, Alias extends string = string> = {
  readonly type: "sum";
  readonly field: NumericFieldKey<Row>;
  readonly as: Alias;
};

export type AverageAggregate<Row, Alias extends string = string> = {
  readonly type: "avg";
  readonly field: NumericFieldKey<Row>;
  readonly as: Alias;
};

export type ComparableAggregate<Row, Alias extends string = string> = {
  readonly type: "min" | "max";
  readonly field: FieldKey<Row>;
  readonly as: Alias;
};

export type Aggregate<Row, Alias extends string = string> =
  | CountAggregate<Alias>
  | CountDistinctAggregate<Row, Alias>
  | SumAggregate<Row, Alias>
  | AverageAggregate<Row, Alias>
  | ComparableAggregate<Row, Alias>;

export type GroupedQuery<Row> = {
  readonly groupBy: ReadonlyArray<FieldKey<Row>>;
  readonly aggregates: ReadonlyArray<Aggregate<Row>>;
  readonly where?: Where<Row>;
  readonly offset?: number;
  readonly limit?: number;
};

export type LiveQuery<Row> = RawQuery<Row> | GroupedQuery<Row>;

type PickRawFields<Row, Query> = Query extends { readonly fields: ReadonlyArray<infer Field> }
  ? Pick<Row, Extract<Field, keyof Row>>
  : Row;

type AggregateResultValue<Row, Agg> = Agg extends { readonly type: "count" | "countDistinct" }
  ? bigint
  : Agg extends { readonly type: "sum"; readonly field: infer Field }
    ? Field extends keyof Row
      ? Row[Field] extends bigint
        ? bigint
        : BigDecimal.BigDecimal
      : never
    : Agg extends { readonly type: "avg" }
      ? BigDecimal.BigDecimal
      : Agg extends { readonly type: "min" | "max"; readonly field: infer Field }
        ? Field extends keyof Row
          ? Row[Field]
          : never
        : never;

type AggregateResultObject<Row, Agg> = Agg extends { readonly as: infer Alias extends string }
  ? {
      readonly [Key in Alias]: AggregateResultValue<Row, Agg>;
    }
  : object;

type UnionToIntersection<Union> = (Union extends unknown ? (value: Union) => void : never) extends (
  value: infer Intersection,
) => void
  ? Intersection
  : never;

type Simplify<T> = { readonly [Key in keyof T]: T[Key] };

type GroupedResult<Row, Query> = Query extends {
  readonly groupBy: ReadonlyArray<infer GroupField>;
  readonly aggregates: ReadonlyArray<infer Agg>;
}
  ? Simplify<
      Pick<Row, Extract<GroupField, keyof Row>> &
        UnionToIntersection<AggregateResultObject<Row, Agg>>
    >
  : never;

export type LiveQueryRow<Row, Query> =
  Query extends GroupedQuery<Row> ? GroupedResult<Row, Query> : PickRawFields<Row, Query>;

export type LiveQueryResult<Row> = {
  readonly rows: ReadonlyArray<Row>;
  readonly totalRows?: number;
  readonly version: number;
};

type RejectBroadAggregateAliases<Query> = Query extends {
  readonly aggregates: ReadonlyArray<infer Agg>;
}
  ? Agg extends { readonly as: infer Alias extends string }
    ? string extends Alias
      ? { readonly aggregates: never }
      : unknown
    : unknown
  : unknown;

type AggregateAliases<Query> = Query extends {
  readonly aggregates: ReadonlyArray<infer Agg>;
}
  ? Agg extends { readonly as: infer Alias extends string }
    ? Alias
    : never
  : never;

type GroupedFields<Query> = Query extends {
  readonly groupBy: ReadonlyArray<infer Field>;
}
  ? Extract<Field, string>
  : never;

type RejectAggregateAliasCollisions<Query> =
  Extract<AggregateAliases<Query>, GroupedFields<Query>> extends never
    ? unknown
    : { readonly aggregates: never };

type ValidateLiveQuery<Query> = RejectBroadAggregateAliases<Query> &
  RejectAggregateAliasCollisions<Query>;

export type UseLiveQuery<Topics extends object> = <
  Topic extends Extract<keyof Topics, string>,
  const Query extends LiveQuery<TopicRow<Topics, Topic>>,
>(
  topic: Topic,
  query: Query & ValidateLiveQuery<Query>,
) => LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>;

export type ViewServerProviderOptions = {
  readonly url: string;
};

export type ViewServerBackpressureError = {
  readonly _tag: "ViewServerBackpressureError";
  readonly code: "BackpressureExceeded";
  readonly message: string;
  readonly topic?: string;
  readonly queryId?: string;
  readonly queuedEvents?: number;
  readonly maxQueueDepth?: number;
};

export type ViewServerRuntimeError =
  | ViewServerBackpressureError
  | {
      readonly _tag: "ViewServerRuntimeError";
      readonly code:
        | "InvalidTopic"
        | "InvalidRow"
        | "SnapshotStale"
        | "RuntimeUnavailable"
        | "RuntimeResetFailed";
      readonly message: string;
      readonly topic?: string;
    };

export type ViewServerTransportError =
  | ViewServerBackpressureError
  | {
      readonly _tag: "ViewServerTransportError";
      readonly code: "TransportError" | "SubscriptionClosed";
      readonly message: string;
      readonly topic?: string;
      readonly queryId?: string;
    };

export type ViewServerInMemoryRuntime<Topics extends object> = {
  readonly publish: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    row: TopicRow<Topics, Topic>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly publishMany: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    rows: ReadonlyArray<TopicRow<Topics, Topic>>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly patch: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    key: string,
    patch: Partial<TopicRow<Topics, Topic>>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly delete: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    key: string,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly snapshot: <
    Topic extends Extract<keyof Topics, string>,
    const Query extends LiveQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: Query & ValidateLiveQuery<Query>,
  ) => Effect.Effect<
    LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError
  >;
  readonly health: () => Effect.Effect<ViewServerHealth<Topics>, ViewServerRuntimeError>;
  readonly reset: () => Effect.Effect<void, ViewServerRuntimeError>;
};

export type ViewServerInMemoryProviderOptions<Topics extends object> = {
  readonly seed?: {
    readonly [Topic in keyof Topics]?: ReadonlyArray<TopicRow<Topics, Topic>>;
  };
  readonly runtime?: ViewServerInMemoryRuntime<Topics>;
  readonly onRuntime?: (runtime: ViewServerInMemoryRuntime<Topics>) => void;
  readonly clock?: Clock.Clock;
};

export type ReactHookContracts<Topics extends object> = {
  readonly useLiveQuery: UseLiveQuery<Topics>;
  readonly useViewServerHealth: () => ViewServerHealth<Topics>;
  readonly useViewServerTestRuntime: () => ViewServerInMemoryRuntime<Topics>;
};

export type RuntimeValue<A> = A | Config.Config<A>;
export type RuntimeRegions = Record<string, RuntimeValue<string>>;
export type NonEmptyReadonlyArray<A> = readonly [A, ...ReadonlyArray<A>];

type RejectExtraKeys<Candidate, Shape> = {
  readonly [Key in Exclude<keyof Candidate, keyof Shape>]: never;
};

type ExactObject<Candidate, Shape> = Candidate & RejectExtraKeys<Candidate, Shape>;

type ExactMappingReturn<Input, Row, Mapping extends (input: Input) => Row> = Mapping &
  ((input: Input) => ExactObject<ReturnType<Mapping>, Row>);

const ProtoCodecTypeId = Symbol("@view-server/config/ProtoCodec");

export type ProtoCodec<T> = {
  readonly [ProtoCodecTypeId]: ReadonlyArray<T>;
};

export const defineProto = <T>(): ProtoCodec<T> => ({
  [ProtoCodecTypeId]: [],
});

export type ProtobufEsGeneratedMessageDescriptor<T extends object> = {
  readonly typeName: string;
  readonly fields?: unknown;
  readonly field?: Record<string, unknown>;
  readonly _viewServerProtoType: (value: T) => T;
};

export type ProtoType<Proto> =
  Proto extends ProtoCodec<infer T>
    ? T
    : Proto extends DescMessage
      ? MessageShape<Proto>
      : Proto extends ProtobufEsGeneratedMessageDescriptor<infer T>
        ? T
        : never;

type SupportedProto<Proto> = [ProtoType<Proto>] extends [never] ? never : Proto;

export type KafkaMessageMetadata<Region extends string = string> = {
  readonly sourceTopic: string;
  readonly sourceRegion: Region;
  readonly partition: number;
  readonly offset: string;
  readonly timestamp: number | null;
  readonly headers: Readonly<
    Record<string, string | Uint8Array | ReadonlyArray<string | Uint8Array>>
  >;
};

export type KafkaMappingInput<
  Topics extends object,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  ProtoValue,
  ProtoKey,
> = {
  readonly key: [ProtoKey] extends [undefined] ? string : ProtoType<ProtoKey>;
  readonly value: ProtoType<ProtoValue>;
  readonly region: Region;
  readonly schema: TopicSchema<Topics, ViewTopic>;
  readonly metadata: KafkaMessageMetadata<Region>;
};

type KafkaTopicWithoutProtoKey<
  Topics extends object,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  ProtoValue,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ProtoValue, undefined>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ProtoValue, undefined>,
  ) => TopicRow<Topics, ViewTopic>,
> = {
  readonly regions: TopicRegions;
  readonly protoValue: SupportedProto<ProtoValue>;
  readonly viewServerTopic: ViewTopic;
  readonly mapping: ExactMappingReturn<
    KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ProtoValue, undefined>,
    TopicRow<Topics, ViewTopic>,
    Mapping
  >;
};

type KafkaTopicWithProtoKey<
  Topics extends object,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  ProtoValue,
  ProtoKey,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ProtoValue, ProtoKey>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ProtoValue, ProtoKey>,
  ) => TopicRow<Topics, ViewTopic>,
> = {
  readonly regions: TopicRegions;
  readonly protoValue: SupportedProto<ProtoValue>;
  readonly protoKey: SupportedProto<ProtoKey>;
  readonly viewServerTopic: ViewTopic;
  readonly mapping: ExactMappingReturn<
    KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ProtoValue, ProtoKey>,
    TopicRow<Topics, ViewTopic>,
    Mapping
  >;
};

export type KafkaTopicDefinition<
  Topics extends object,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string> = Extract<keyof Topics, string>,
  ProtoValue = unknown,
  ProtoKey = unknown,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>> =
    NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  MappingWithoutProtoKey extends (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ProtoValue, undefined>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ProtoValue, undefined>,
  ) => TopicRow<Topics, ViewTopic>,
  MappingWithProtoKey extends (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ProtoValue, ProtoKey>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ProtoValue, ProtoKey>,
  ) => TopicRow<Topics, ViewTopic>,
> =
  | KafkaTopicWithoutProtoKey<
      Topics,
      Regions,
      ViewTopic,
      ProtoValue,
      TopicRegions,
      MappingWithoutProtoKey
    >
  | KafkaTopicWithProtoKey<
      Topics,
      Regions,
      ViewTopic,
      ProtoValue,
      ProtoKey,
      TopicRegions,
      MappingWithProtoKey
    >;

type ValidateKafkaTopic<
  Topics extends object,
  Regions extends RuntimeRegions,
  Candidate,
> = Candidate extends {
  readonly regions: infer TopicRegions extends NonEmptyReadonlyArray<
    Extract<keyof Regions, string>
  >;
  readonly protoValue: infer ProtoValue;
  readonly protoKey: infer ProtoKey;
  readonly viewServerTopic: infer ViewTopic extends Extract<keyof Topics, string>;
  readonly mapping: infer Mapping;
}
  ? Mapping extends (
      input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ProtoValue, ProtoKey>,
    ) => TopicRow<Topics, ViewTopic>
    ? KafkaTopicWithProtoKey<
        Topics,
        Regions,
        ViewTopic,
        ProtoValue,
        ProtoKey,
        TopicRegions,
        Mapping
      >
    : never
  : Candidate extends {
        readonly regions: infer TopicRegions extends NonEmptyReadonlyArray<
          Extract<keyof Regions, string>
        >;
        readonly protoValue: infer ProtoValue;
        readonly viewServerTopic: infer ViewTopic extends Extract<keyof Topics, string>;
        readonly mapping: infer Mapping;
      }
    ? Mapping extends (
        input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ProtoValue, undefined>,
      ) => TopicRow<Topics, ViewTopic>
      ? KafkaTopicWithoutProtoKey<Topics, Regions, ViewTopic, ProtoValue, TopicRegions, Mapping>
      : never
    : never;

type ValidateKafkaTopics<
  Topics extends object,
  Regions extends RuntimeRegions,
  KafkaTopics extends Record<string, object>,
> = {
  readonly [SourceTopic in keyof KafkaTopics]: ValidateKafkaTopic<
    Topics,
    Regions,
    KafkaTopics[SourceTopic]
  >;
};

export type RuntimeOptions<
  Topics extends object,
  Regions extends RuntimeRegions,
  KafkaTopics extends Record<string, object>,
> = {
  readonly websocketPort: RuntimeValue<number>;
  readonly tcpPublishPort: RuntimeValue<number>;
  readonly kafka: {
    readonly regions: Regions;
    readonly topics: ValidateKafkaTopics<Topics, Regions, KafkaTopics>;
  };
};

export type RuntimeOptionsCandidate = {
  readonly websocketPort: RuntimeValue<number>;
  readonly tcpPublishPort: RuntimeValue<number>;
  readonly kafka: {
    readonly regions: RuntimeRegions;
    readonly topics: Record<string, object>;
  };
};

export type ValidateRuntimeOptions<
  Topics extends object,
  Options,
> = Options extends RuntimeOptionsCandidate
  ? RuntimeOptions<Topics, Options["kafka"]["regions"], Options["kafka"]["topics"]>
  : never;

export type RuntimeOptionsDefinition<Topics extends object, Options> = ValidateRuntimeOptions<
  Topics,
  Options
>;

type RejectExtraRuntimeKafkaKeys<Options, Shape> = Options extends {
  readonly kafka: infer CandidateKafka;
}
  ? Shape extends {
      readonly kafka: infer RuntimeKafka;
    }
    ? {
        readonly kafka: CandidateKafka & RejectExtraKeys<CandidateKafka, RuntimeKafka>;
      }
    : unknown
  : unknown;

type ExactRuntimeOptions<Topics extends object, Options> = Options &
  ValidateRuntimeOptions<Topics, Options> &
  RejectExtraKeys<Options, ValidateRuntimeOptions<Topics, Options>> &
  RejectExtraRuntimeKafkaKeys<Options, ValidateRuntimeOptions<Topics, Options>>;

export type RuntimeEnvironmentConfig = {
  readonly websocketPort: Config.Config<number>;
  readonly tcpPublishPort: Config.Config<number>;
};

export type TopicRuntimeHealth = {
  readonly status: TopicHealthStatus;
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
  readonly memoryBytes: number;
  readonly tombstoneCount: number;
  readonly compactionPending: boolean;
};

export type KafkaRegionHealth = {
  readonly status: KafkaRegionStatus;
  readonly brokers: string;
  readonly lastConnectedAt: number | null;
  readonly lastError: string | null;
};

export type KafkaTopicRegionHealth = {
  readonly connected: boolean;
  readonly assignedPartitions: number;
  readonly messagesPerSecond: number;
  readonly bytesPerSecond: number;
  readonly decodedMessagesPerSecond: number;
  readonly decodeFailuresPerSecond: number;
  readonly lastMessageAt: number | null;
  readonly lastCommitAt: number | null;
  readonly consumerLagMessages: number | null;
  readonly consumerLagMs: number | null;
  readonly lagSampledAt: number | null;
  readonly highWatermarkOffset: string | null;
  readonly committedOffset: string | null;
  readonly lastError: string | null;
};

export type KafkaTopicHealth = {
  readonly status: KafkaTopicStatus;
  readonly sourceTopic: string;
  readonly viewServerTopic: string;
  readonly regions: Record<string, KafkaTopicRegionHealth>;
};

export type TransportHealth = {
  readonly activeClients: number;
  readonly activeStreams: number;
  readonly activeSubscriptions: number;
  readonly messagesPerSecond: number;
  readonly bytesPerSecond: number;
  readonly queuedMessages: number;
  readonly queuedBytes: number;
  readonly droppedClients: number;
  readonly backpressureEvents: number;
  readonly reconnects: number;
  readonly lastError: string | null;
};

export type ViewServerHealth<Topics extends object = Record<string, object>> = {
  readonly status: RuntimeStatus;
  readonly version: number;
  readonly uptimeMs: number;
  readonly engine: {
    readonly topics: {
      readonly [Topic in Extract<keyof Topics, string>]: TopicRuntimeHealth;
    };
  };
  readonly kafka?: {
    readonly regions: Record<string, KafkaRegionHealth>;
    readonly topics: Record<string, KafkaTopicHealth>;
  };
  readonly transport: TransportHealth;
};

export type SnapshotEvent<Row> = {
  readonly type: "snapshot";
  readonly topic: string;
  readonly queryId: string;
  readonly version: number;
  readonly keys: ReadonlyArray<string>;
  readonly rows: ReadonlyArray<Row>;
  readonly totalRows?: number;
};

export type DeltaOperation<Row> =
  | {
      readonly type: "insert";
      readonly key: string;
      readonly row: Row;
      readonly index: number;
    }
  | {
      readonly type: "update";
      readonly key: string;
      readonly row: Row;
      readonly index: number;
    }
  | {
      readonly type: "move";
      readonly key: string;
      readonly fromIndex: number;
      readonly toIndex: number;
    }
  | {
      readonly type: "remove";
      readonly key: string;
    };

export type DeltaEvent<Row> = {
  readonly type: "delta";
  readonly topic: string;
  readonly queryId: string;
  readonly fromVersion: number;
  readonly toVersion: number;
  readonly operations: ReadonlyArray<DeltaOperation<Row>>;
  readonly totalRows?: number;
};

export type StatusEventCode =
  | "Ready"
  | "SnapshotStale"
  | "SubscriptionClosed"
  | "TransportError"
  | "BackpressureExceeded";

export type StatusEvent =
  | {
      readonly type: "status";
      readonly topic: string;
      readonly queryId: string;
      readonly status: "ready";
      readonly code: "Ready";
      readonly message?: string;
    }
  | {
      readonly type: "status";
      readonly topic: string;
      readonly queryId: string;
      readonly status: "stale";
      readonly code: "SnapshotStale" | "BackpressureExceeded";
      readonly message?: string;
    }
  | {
      readonly type: "status";
      readonly topic: string;
      readonly queryId: string;
      readonly status: "closed";
      readonly code: "SubscriptionClosed" | "BackpressureExceeded";
      readonly message?: string;
    }
  | {
      readonly type: "status";
      readonly topic: string;
      readonly queryId: string;
      readonly status: "error";
      readonly code: "TransportError" | "BackpressureExceeded";
      readonly message?: string;
    };

export type LiveSubscription<Row> = {
  readonly events: AsyncIterable<SnapshotEvent<Row> | DeltaEvent<Row> | StatusEvent>;
  readonly close: () => Effect.Effect<void, ViewServerTransportError>;
};

export type LiveTransportAdapter = {
  readonly subscribe: <Row>(
    topic: string,
    query: LiveQuery<Row>,
  ) => Effect.Effect<LiveSubscription<Row>, ViewServerTransportError>;
};

export type ViewServerConfig<Topics extends object> = {
  readonly topics: Topics;
  readonly defineRuntimeOptions: <const Options extends RuntimeOptionsCandidate>(
    options: ExactRuntimeOptions<Topics, Options>,
  ) => RuntimeOptionsDefinition<Topics, Options>;
  readonly kafkaTopic: KafkaTopicHelper<Topics>;
};

type ValidateTopicDefinitions<Topics extends object> = {
  readonly [Topic in keyof Topics]: Topics[Topic] extends {
    readonly schema: infer S extends RowSchema;
    readonly key: infer Key extends string;
  }
    ? TopicDefinition<S, Key & StringFieldKey<RowFromSchema<S>>>
    : never;
};

export type DefineViewServerConfigInput<Topics extends object> = {
  readonly topics: Topics & ValidateTopicDefinitions<Topics>;
};

export type KafkaTopicHelper<Topics extends object> = <const Regions extends RuntimeRegions>() => {
  <
    const TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
    ProtoValue,
    ProtoKey,
    const ViewTopic extends Extract<keyof Topics, string>,
    Mapping extends (
      input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ProtoValue, ProtoKey>,
    ) => TopicRow<Topics, ViewTopic>,
  >(
    topic: KafkaTopicWithProtoKey<
      Topics,
      Regions,
      ViewTopic,
      ProtoValue,
      ProtoKey,
      TopicRegions,
      Mapping
    >,
  ): KafkaTopicWithProtoKey<
    Topics,
    Regions,
    ViewTopic,
    ProtoValue,
    ProtoKey,
    TopicRegions,
    Mapping
  >;
  <
    const TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
    ProtoValue,
    const ViewTopic extends Extract<keyof Topics, string>,
    Mapping extends (
      input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ProtoValue, undefined>,
    ) => TopicRow<Topics, ViewTopic>,
  >(
    topic: KafkaTopicWithoutProtoKey<Topics, Regions, ViewTopic, ProtoValue, TopicRegions, Mapping>,
  ): KafkaTopicWithoutProtoKey<Topics, Regions, ViewTopic, ProtoValue, TopicRegions, Mapping>;
};

export const defineKafkaTopic = <Topics extends object>(): KafkaTopicHelper<Topics> => {
  function forRegions<const Regions extends RuntimeRegions>() {
    function topicHelper<
      const TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
      ProtoValue,
      ProtoKey,
      const ViewTopic extends Extract<keyof Topics, string>,
      Mapping extends (
        input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ProtoValue, ProtoKey>,
      ) => TopicRow<Topics, ViewTopic>,
    >(
      topic: KafkaTopicWithProtoKey<
        Topics,
        Regions,
        ViewTopic,
        ProtoValue,
        ProtoKey,
        TopicRegions,
        Mapping
      >,
    ): KafkaTopicWithProtoKey<
      Topics,
      Regions,
      ViewTopic,
      ProtoValue,
      ProtoKey,
      TopicRegions,
      Mapping
    >;
    function topicHelper<
      const TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
      ProtoValue,
      const ViewTopic extends Extract<keyof Topics, string>,
      Mapping extends (
        input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ProtoValue, undefined>,
      ) => TopicRow<Topics, ViewTopic>,
    >(
      topic: KafkaTopicWithoutProtoKey<
        Topics,
        Regions,
        ViewTopic,
        ProtoValue,
        TopicRegions,
        Mapping
      >,
    ): KafkaTopicWithoutProtoKey<Topics, Regions, ViewTopic, ProtoValue, TopicRegions, Mapping>;
    function topicHelper<
      const TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
      ProtoValue,
      ProtoKey,
      const ViewTopic extends Extract<keyof Topics, string>,
    >(topic: KafkaTopicDefinition<Topics, Regions, ViewTopic, ProtoValue, ProtoKey, TopicRegions>) {
      return topic;
    }

    return topicHelper;
  }

  return forRegions;
};

export const defineViewServerConfig = <
  const Topics extends Record<
    string,
    {
      readonly schema: RowSchema;
      readonly key: string;
    }
  >,
>(
  input: DefineViewServerConfigInput<Topics>,
): ViewServerConfig<Topics> => ({
  topics: input.topics,
  defineRuntimeOptions: (options) => options,
  kafkaTopic: defineKafkaTopic<Topics>(),
});
