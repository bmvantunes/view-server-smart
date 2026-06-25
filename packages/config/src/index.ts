import { type ViewServerSystemTopicName, viewServerTopicNameIsReserved } from "./health-contract";
import {
  defineKafkaTopic,
  type ExactRuntimeOptions,
  type KafkaTopicHelper,
  type RuntimeOptionsCandidate,
  type RuntimeOptionsDefinition,
} from "./kafka-contract";
import type {
  GrpcFeedHelper,
  GrpcLeasedTopicSource,
  GrpcRuntimeClients,
  GrpcTopicSource,
} from "./grpc-contract";
import { defineGrpcFeed } from "./grpc-contract";
import type { TopicSourceDefinition } from "./source-contract";
import type {
  FieldKey,
  RowFromSchema,
  RowSchema,
  StringFieldKey,
  TopicDefinition,
  TopicDefinitions,
} from "./topic-contract";
import { viewServerUnsupportedRuntimeFieldDomain } from "./schema-field-metadata";

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
  KafkaStartFromHealth,
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
  viewServerUnsupportedRuntimeFieldDomain,
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
export {
  decodeKafkaCodec,
  decodeKafkaTopicMessage,
  defineKafkaTopic,
  kafka,
  kafkaErrorIsMapping,
} from "./kafka-contract";
export { grpc } from "./grpc-contract";
export type {
  GrpcConnectClientDefinition,
  GrpcClientValue,
  GrpcFeedAcquireInput,
  GrpcFeedHelper,
  GrpcFeedMapInput,
  GrpcFeedReleaseInput,
  GrpcFeedSession,
  GrpcHelper,
  GrpcLeasedTopicSource,
  GrpcMaterializedTopicSource,
  GrpcMethodRequest,
  GrpcMethodValue,
  GrpcRuntimeClients,
  GrpcRuntimeValue,
  GrpcServerStreamingMethodName,
  GrpcTopicSource,
  GrpcTopicSourceLifecycle,
} from "./grpc-contract";
export { defineGrpcFeed } from "./grpc-contract";
export type {
  ExactLeasedRouteQuery,
  ExactLiveQueryInputForTopic,
  TopicRouteBy,
} from "./source-query-contract";
export { validateLiveQuerySourceRoute } from "./source-query-contract";
export type { TopicSourceDefinition } from "./source-contract";
export type {
  KafkaBytesCodec,
  KafkaDecodedTopicMessage,
  ExactRuntimeOptions,
  KafkaCodec,
  KafkaCodecDecodeInput,
  KafkaCodecError,
  KafkaCodecType,
  KafkaCustomCodec,
  KafkaDecodeError,
  KafkaJsonCodec,
  KafkaMappingError,
  KafkaMappingInput,
  KafkaMessageMetadata,
  KafkaProtobufCodec,
  KafkaSourceCodec,
  KafkaStringCodec,
  KafkaRuntimeTopicDefinition,
  KafkaProtobufType,
  KafkaTopicDefinition,
  KafkaTopicHelper,
  NonEmptyReadonlyArray,
  RuntimeOptions,
  RuntimeOptionsCandidate,
  RuntimeOptionsDefinition,
  RuntimeRegions,
  RuntimeValue,
  ValidateRuntimeOptions,
  ViewServerKafkaCommittedStartFrom,
  ViewServerKafkaStartFrom,
} from "./kafka-contract";

type ViewServerConfigTopicShape = Record<
  string,
  {
    readonly schema: RowSchema;
    readonly key: string;
    readonly source?: TopicSourceDefinition;
  }
>;

export type ViewServerConfig<Topics extends TopicDefinitions> = {
  readonly topics: Topics & ValidateTopicDefinitions<Topics>;
  readonly defineRuntimeOptions: <const Options extends RuntimeOptionsCandidate>(
    options: ExactRuntimeOptions<Topics, Options>,
  ) => RuntimeOptionsDefinition<Topics, Options>;
  readonly kafkaTopic: KafkaTopicHelper<Topics>;
  readonly grpcFeed: <const Clients extends GrpcRuntimeClients>() => GrpcFeedHelper<
    Topics,
    Clients
  >;
};

type ValidateTopicDefinitions<Topics extends TopicDefinitions> = {
  readonly [Topic in keyof Topics]: Topic extends ViewServerSystemTopicName
    ? never
    : Topics[Topic] extends {
          readonly schema: infer S extends RowSchema;
          readonly key: infer Key extends string;
        }
      ? Topics[Topic] extends { readonly source: infer Source }
        ? TopicDefinition<
            S,
            Key & StringFieldKey<RowFromSchema<S>>,
            ValidateTopicSource<RowFromSchema<S>, Source>
          >
        : TopicDefinition<S, Key & StringFieldKey<RowFromSchema<S>>>
      : never;
};

type ValidateGrpcLeasedRouteBy<Row, Source> =
  Source extends GrpcLeasedTopicSource<infer RouteBy>
    ? GrpcLeasedTopicSource<{
        readonly [Index in keyof RouteBy]: RouteBy[Index] extends FieldKey<Row>
          ? RouteBy[Index]
          : never;
      }>
    : Source;

type ValidateTopicSource<Row, Source> = Source extends GrpcTopicSource
  ? ValidateGrpcLeasedRouteBy<Row, Source>
  : Source extends undefined
    ? undefined
    : never;

export type DefineViewServerConfigInput<Topics extends ViewServerConfigTopicShape> = {
  readonly topics: Topics & ValidateTopicDefinitions<Topics>;
};

export const defineViewServerConfig = <
  const Topics extends Record<
    string,
    {
      readonly schema: RowSchema;
      readonly key: string;
      readonly source?: TopicSourceDefinition;
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
      const unsupportedRuntimeDomain = viewServerUnsupportedRuntimeFieldDomain(
        schema.fields[field],
      );
      if (unsupportedRuntimeDomain !== undefined) {
        throw new Error(
          `View Server topic ${topic} field ${field} uses unsupported runtime domain: ${unsupportedRuntimeDomain}`,
        );
      }
    }
  }
  return {
    topics: input.topics,
    defineRuntimeOptions: (options) => options,
    kafkaTopic: defineKafkaTopic<Topics>(input.topics),
    grpcFeed: <const Clients extends GrpcRuntimeClients>() => defineGrpcFeed<Topics, Clients>(),
  };
};
