import type { DescMessage, MessageShape } from "@bufbuild/protobuf";
import type { Config } from "effect";
import type { TopicRow, TopicSchema } from "./topic-contract";

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

export type ExactRuntimeOptions<Topics extends object, Options> = Options &
  ValidateRuntimeOptions<Topics, Options> &
  RejectExtraKeys<Options, ValidateRuntimeOptions<Topics, Options>> &
  RejectExtraRuntimeKafkaKeys<Options, ValidateRuntimeOptions<Topics, Options>>;

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
