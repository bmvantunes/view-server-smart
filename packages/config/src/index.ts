import {
  defineKafkaTopic,
  type ExactRuntimeOptions,
  type KafkaTopicHelper,
  type RuntimeOptionsCandidate,
  type RuntimeOptionsDefinition,
} from "./kafka-contract";
import type { RowFromSchema, RowSchema, StringFieldKey, TopicDefinition } from "./topic-contract";

export type {
  Aggregate,
  AggregateKind,
  AverageAggregate,
  ComparableAggregate,
  CountAggregate,
  CountDistinctAggregate,
  EqualityFilter,
  FieldFilter,
  FieldKey,
  GroupedQuery,
  LiveQuery,
  LiveQueryResult,
  LiveQueryRow,
  NumericFieldKey,
  OrderBy,
  RangeFilter,
  RawQuery,
  RowFromSchema,
  RowSchema,
  SchemaType,
  SortDirection,
  StringFieldKey,
  StringFilter,
  SumAggregate,
  TopicDefinition,
  TopicDefinitions,
  TopicName,
  TopicRow,
  TopicSchema,
  UseLiveQuery,
  ValidateLiveQuery,
  Where,
} from "./topic-contract";
export type {
  KafkaRegionHealth,
  KafkaRegionStatus,
  KafkaTopicHealth,
  KafkaTopicRegionHealth,
  KafkaTopicStatus,
  RuntimeStatus,
  TopicHealthStatus,
  TopicRuntimeHealth,
  TransportHealth,
  ViewServerHealth,
} from "./health-contract";
export type {
  ReactHookContracts,
  ViewServerBackpressureError,
  ViewServerInMemoryProviderOptions,
  ViewServerInMemoryRuntime,
  ViewServerProviderOptions,
  RuntimeEnvironmentConfig,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "./runtime-contract";
export type {
  DeltaEvent,
  DeltaOperation,
  LiveSubscription,
  LiveTransportAdapter,
  SnapshotEvent,
  StatusEvent,
  StatusEventCode,
} from "./live-protocol";
export { defineKafkaTopic, defineProto } from "./kafka-contract";
export type {
  ExactRuntimeOptions,
  KafkaMappingInput,
  KafkaMessageMetadata,
  KafkaTopicDefinition,
  KafkaTopicHelper,
  NonEmptyReadonlyArray,
  ProtoCodec,
  ProtoType,
  ProtobufEsGeneratedMessageDescriptor,
  RuntimeOptions,
  RuntimeOptionsCandidate,
  RuntimeOptionsDefinition,
  RuntimeRegions,
  RuntimeValue,
  ValidateRuntimeOptions,
} from "./kafka-contract";

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
