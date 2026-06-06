import { type ViewServerSystemTopicName, viewServerTopicNameIsReserved } from "./health-contract";
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
  AggregateOrderByField,
  Aggregates,
  AggregateKind,
  AverageAggregate,
  ComparableAggregate,
  CountAggregate,
  CountDistinctAggregate,
  EqualityFilter,
  ExactGroupedQuery,
  ExactLiveQuery,
  ExactLiveQueryInput,
  ExactPatch,
  ExactRawQuery,
  FieldFilter,
  FieldKey,
  GroupedOrderBy,
  GroupedQuery,
  GroupedResult,
  LiveQuery,
  LiveQueryResult,
  LiveQueryRow,
  NumericFieldKey,
  OrderBy,
  OrderByField,
  PickRawFields,
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
  ViewServerHealthConnectionStatus,
  ViewServerHealthDetails,
  ViewServerHealthStatus,
  ViewServerHealthSummary,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
} from "./health-contract";
export {
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
  viewServerReservedTopicNames,
  viewServerTopicNameIsReserved,
  viewServerHealthSummaryFromHealth,
  viewServerHealthSummaryRowFromHealth,
  viewServerHealthTopicRowsFromHealth,
} from "./health-contract";
export type { ViewServerSystemTopicName } from "./health-contract";
export type {
  ViewServerBackpressureError,
  RuntimeEnvironmentConfig,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "./runtime-contract";
export {
  viewServerSchemaFieldMetadata,
  type ViewServerSchemaFieldMetadata,
} from "./schema-field-metadata";
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
  readonly topics: Topics & ValidateTopicDefinitions<Topics>;
  readonly defineRuntimeOptions: <const Options extends RuntimeOptionsCandidate>(
    options: ExactRuntimeOptions<Topics, Options>,
  ) => RuntimeOptionsDefinition<Topics, Options>;
  readonly kafkaTopic: KafkaTopicHelper<Topics>;
};

type ValidateTopicDefinitions<Topics extends object> = {
  readonly [Topic in keyof Topics]: Topic extends ViewServerSystemTopicName
    ? never
    : Topics[Topic] extends {
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
): ViewServerConfig<Topics> => {
  for (const topic of Object.keys(input.topics)) {
    if (viewServerTopicNameIsReserved(topic)) {
      throw new Error(`View Server topic name is reserved for system health streams: ${topic}`);
    }
    const schema = input.topics[topic]!.schema;
    for (const field of Object.keys(schema.fields)) {
      if (field === "__proto__" || field === "prototype" || field === "constructor") {
        throw new Error(`View Server topic ${topic} uses a reserved row field name: ${field}`);
      }
    }
  }
  return {
    topics: input.topics,
    defineRuntimeOptions: (options) => options,
    kafkaTopic: defineKafkaTopic<Topics>(),
  };
};
