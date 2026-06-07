import type { DescMessage, MessageShape } from "@bufbuild/protobuf";
import type { Config, Effect } from "effect";
import type { RowFromSchema, RowSchema, TopicRow, TopicSchema } from "./topic-contract";

export type RuntimeValue<A> = A | Config.Config<A>;
export type RuntimeRegions = Record<string, RuntimeValue<string>>;
export type NonEmptyReadonlyArray<A> = readonly [A, ...ReadonlyArray<A>];

type RejectExtraKeys<Candidate, Shape> = {
  readonly [Key in Exclude<keyof Candidate, keyof Shape>]: never;
};

type IsAny<A> = 0 extends 1 & A ? true : false;

type IsUnknown<A> =
  IsAny<A> extends true
    ? false
    : unknown extends A
      ? [A] extends [unknown]
        ? true
        : false
      : false;

type RejectAnyCodecValue<A> =
  IsAny<A> extends true ? { readonly __viewServerKafkaCodecValueCannotBeAny: never } : unknown;

type RejectAnyCodecError<E> =
  IsAny<E> extends true ? { readonly __viewServerKafkaCodecErrorCannotBeAny: never } : unknown;

type ExactObject<Candidate, Shape> = Candidate & RejectExtraKeys<Candidate, Shape>;

type ExactMappingReturn<Input, Row, Mapping extends (input: Input) => Row> = Mapping &
  ((input: Input) => ExactObject<ReturnType<Mapping>, Row>);

const KafkaCodecValueTypeId = Symbol("@view-server/config/KafkaCodecValue");
const KafkaCodecErrorTypeId = Symbol("@view-server/config/KafkaCodecError");
const KafkaTopicDefinitionTypeId = Symbol("@view-server/config/KafkaTopicDefinition");

export type KafkaDecodeError = {
  readonly _tag: "KafkaDecodeError";
  readonly message: string;
  readonly cause?: unknown;
};

export type KafkaCodec<A, E = never> = {
  readonly [KafkaCodecValueTypeId]: ReadonlyArray<A>;
  readonly [KafkaCodecErrorTypeId]: ReadonlyArray<E>;
  readonly format: string;
};

export type KafkaCodecType<Codec> = Codec extends KafkaCodec<infer A, infer _E> ? A : never;
export type KafkaCodecError<Codec> = Codec extends KafkaCodec<infer _A, infer E> ? E : never;

export type KafkaCodecDecodeInput = {
  readonly bytes: Uint8Array;
  readonly metadata: KafkaMessageMetadata;
};

export type KafkaProtobufType<Proto> = Proto extends DescMessage ? MessageShape<Proto> : never;

type SupportedKafkaProtobufInput<Proto> =
  IsAny<Proto> extends true ? never : [KafkaProtobufType<Proto>] extends [never] ? never : Proto;

type SupportedKafkaCodec<Codec> =
  IsAny<Codec> extends true
    ? never
    : IsAny<KafkaCodecType<Codec>> extends true
      ? never
      : IsAny<KafkaCodecError<Codec>> extends true
        ? never
        : IsUnknown<KafkaCodecError<Codec>> extends true
          ? never
          : [KafkaCodecType<Codec>] extends [never]
            ? never
            : Codec;

type SupportedKafkaJsonSchema<SourceSchema extends RowSchema> =
  IsAny<SourceSchema> extends true ? never : SourceSchema;

type KafkaTopicDefinitionMarker = {
  readonly [KafkaTopicDefinitionTypeId]: true;
};

const makeKafkaCodec = <A, E, Format extends string>(
  format: Format,
): KafkaCodec<A, E> & {
  readonly format: Format;
} => ({
  [KafkaCodecValueTypeId]: [],
  [KafkaCodecErrorTypeId]: [],
  format,
});

export const kafka = {
  bytes: (): KafkaCodec<Uint8Array> & { readonly format: "bytes" } =>
    makeKafkaCodec<Uint8Array, never, "bytes">("bytes"),
  string: (): KafkaCodec<string> & { readonly format: "string" } =>
    makeKafkaCodec<string, never, "string">("string"),
  stringKey: (): KafkaCodec<string> & { readonly format: "string" } =>
    makeKafkaCodec<string, never, "string">("string"),
  json: <const SourceSchema extends RowSchema>(
    schema: SupportedKafkaJsonSchema<SourceSchema>,
  ): KafkaCodec<RowFromSchema<SourceSchema>, KafkaDecodeError> & {
    readonly format: "json";
    readonly schema: SourceSchema;
  } => ({
    ...makeKafkaCodec<RowFromSchema<SourceSchema>, KafkaDecodeError, "json">("json"),
    schema,
  }),
  protobuf: <const Proto>(
    descriptor: SupportedKafkaProtobufInput<Proto>,
  ): KafkaCodec<KafkaProtobufType<Proto>, KafkaDecodeError> & {
    readonly format: "protobuf";
    readonly descriptor: SupportedKafkaProtobufInput<Proto>;
  } => ({
    ...makeKafkaCodec<KafkaProtobufType<Proto>, KafkaDecodeError, "protobuf">("protobuf"),
    descriptor,
  }),
  codec: <A, E>(
    definition: {
      readonly name: string;
      readonly decode: (input: KafkaCodecDecodeInput) => Effect.Effect<A, E, never>;
    } & RejectAnyCodecValue<NoInfer<A>> &
      RejectAnyCodecError<NoInfer<E>>,
  ): KafkaCodec<A, E> & {
    readonly format: "custom";
    readonly name: string;
    readonly decode: (input: KafkaCodecDecodeInput) => Effect.Effect<A, E, never>;
  } => ({
    ...makeKafkaCodec<A, E, "custom">("custom"),
    name: definition.name,
    decode: definition.decode,
  }),
};

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
  ValueCodec,
  KeyCodec,
> = {
  readonly key: [KeyCodec] extends [undefined] ? string : KafkaCodecType<KeyCodec>;
  readonly value: KafkaCodecType<ValueCodec>;
  readonly region: Region;
  readonly schema: TopicSchema<Topics, ViewTopic>;
  readonly metadata: KafkaMessageMetadata<Region>;
};

type KafkaTopicWithoutKeyInput<
  Topics extends object,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  ValueCodec,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, undefined>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, undefined>,
  ) => TopicRow<Topics, ViewTopic>,
> = {
  readonly regions: TopicRegions;
  readonly value: SupportedKafkaCodec<ValueCodec>;
  readonly viewServerTopic: ViewTopic;
  readonly mapping: ExactMappingReturn<
    KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, undefined>,
    TopicRow<Topics, ViewTopic>,
    Mapping
  >;
};

type KafkaTopicWithoutKey<
  Topics extends object,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  ValueCodec,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, undefined>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, undefined>,
  ) => TopicRow<Topics, ViewTopic>,
> = KafkaTopicWithoutKeyInput<Topics, Regions, ViewTopic, ValueCodec, TopicRegions, Mapping> &
  KafkaTopicDefinitionMarker;

type KafkaTopicWithKeyInput<
  Topics extends object,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  ValueCodec,
  KeyCodec,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => TopicRow<Topics, ViewTopic>,
> = {
  readonly regions: TopicRegions;
  readonly value: SupportedKafkaCodec<ValueCodec>;
  readonly key: SupportedKafkaCodec<KeyCodec>;
  readonly viewServerTopic: ViewTopic;
  readonly mapping: ExactMappingReturn<
    KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
    TopicRow<Topics, ViewTopic>,
    Mapping
  >;
};

type KafkaTopicWithKey<
  Topics extends object,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  ValueCodec,
  KeyCodec,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => TopicRow<Topics, ViewTopic>,
> = KafkaTopicWithKeyInput<
  Topics,
  Regions,
  ViewTopic,
  ValueCodec,
  KeyCodec,
  TopicRegions,
  Mapping
> &
  KafkaTopicDefinitionMarker;

export type KafkaTopicDefinition<
  Topics extends object,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string> = Extract<keyof Topics, string>,
  ValueCodec = unknown,
  KeyCodec = unknown,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>> =
    NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  MappingWithoutKey extends (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, undefined>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, undefined>,
  ) => TopicRow<Topics, ViewTopic>,
  MappingWithKey extends (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => TopicRow<Topics, ViewTopic>,
> =
  | KafkaTopicWithoutKey<Topics, Regions, ViewTopic, ValueCodec, TopicRegions, MappingWithoutKey>
  | KafkaTopicWithKey<
      Topics,
      Regions,
      ViewTopic,
      ValueCodec,
      KeyCodec,
      TopicRegions,
      MappingWithKey
    >;

type KafkaTopicDefinitionInput<
  Topics extends object,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string> = Extract<keyof Topics, string>,
  ValueCodec = unknown,
  KeyCodec = unknown,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>> =
    NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  MappingWithoutKey extends (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, undefined>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, undefined>,
  ) => TopicRow<Topics, ViewTopic>,
  MappingWithKey extends (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => TopicRow<Topics, ViewTopic>,
> =
  | KafkaTopicWithoutKeyInput<
      Topics,
      Regions,
      ViewTopic,
      ValueCodec,
      TopicRegions,
      MappingWithoutKey
    >
  | KafkaTopicWithKeyInput<
      Topics,
      Regions,
      ViewTopic,
      ValueCodec,
      KeyCodec,
      TopicRegions,
      MappingWithKey
    >;

type ValidateKafkaTopic<
  Topics extends object,
  Regions extends RuntimeRegions,
  Candidate,
> = Candidate extends KafkaTopicDefinitionMarker & {
  readonly regions: infer TopicRegions extends NonEmptyReadonlyArray<
    Extract<keyof Regions, string>
  >;
  readonly value: infer ValueCodec;
  readonly key: infer KeyCodec;
  readonly viewServerTopic: infer ViewTopic extends Extract<keyof Topics, string>;
}
  ? Candidate extends {
      readonly mapping: infer Mapping extends (
        input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
      ) => TopicRow<Topics, ViewTopic>;
    }
    ? Candidate extends KafkaTopicWithKey<
        Topics,
        Regions,
        ViewTopic,
        ValueCodec,
        KeyCodec,
        TopicRegions,
        Mapping
      >
      ? Candidate
      : never
    : never
  : Candidate extends KafkaTopicDefinitionMarker & {
        readonly regions: infer TopicRegions extends NonEmptyReadonlyArray<
          Extract<keyof Regions, string>
        >;
        readonly value: infer ValueCodec;
        readonly viewServerTopic: infer ViewTopic extends Extract<keyof Topics, string>;
      }
    ? Candidate extends {
        readonly mapping: infer Mapping extends (
          input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, undefined>,
        ) => TopicRow<Topics, ViewTopic>;
      }
      ? Candidate extends KafkaTopicWithoutKey<
          Topics,
          Regions,
          ViewTopic,
          ValueCodec,
          TopicRegions,
          Mapping
        >
        ? Candidate
        : never
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
    ValueCodec,
    KeyCodec,
    const ViewTopic extends Extract<keyof Topics, string>,
    Mapping extends (
      input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
    ) => TopicRow<Topics, ViewTopic>,
  >(
    topic: KafkaTopicWithKeyInput<
      Topics,
      Regions,
      ViewTopic,
      ValueCodec,
      KeyCodec,
      TopicRegions,
      Mapping
    >,
  ): KafkaTopicWithKey<Topics, Regions, ViewTopic, ValueCodec, KeyCodec, TopicRegions, Mapping>;
  <
    const TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
    ValueCodec,
    const ViewTopic extends Extract<keyof Topics, string>,
    Mapping extends (
      input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, undefined>,
    ) => TopicRow<Topics, ViewTopic>,
  >(
    topic: KafkaTopicWithoutKeyInput<Topics, Regions, ViewTopic, ValueCodec, TopicRegions, Mapping>,
  ): KafkaTopicWithoutKey<Topics, Regions, ViewTopic, ValueCodec, TopicRegions, Mapping>;
};

export const defineKafkaTopic = <Topics extends object>(): KafkaTopicHelper<Topics> => {
  function forRegions<const Regions extends RuntimeRegions>() {
    function topicHelper<
      const TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
      ValueCodec,
      KeyCodec,
      const ViewTopic extends Extract<keyof Topics, string>,
      Mapping extends (
        input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
      ) => TopicRow<Topics, ViewTopic>,
    >(
      topic: KafkaTopicWithKeyInput<
        Topics,
        Regions,
        ViewTopic,
        ValueCodec,
        KeyCodec,
        TopicRegions,
        Mapping
      >,
    ): KafkaTopicWithKey<Topics, Regions, ViewTopic, ValueCodec, KeyCodec, TopicRegions, Mapping>;
    function topicHelper<
      const TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
      ValueCodec,
      const ViewTopic extends Extract<keyof Topics, string>,
      Mapping extends (
        input: KafkaMappingInput<Topics, ViewTopic, TopicRegions[number], ValueCodec, undefined>,
      ) => TopicRow<Topics, ViewTopic>,
    >(
      topic: KafkaTopicWithoutKeyInput<
        Topics,
        Regions,
        ViewTopic,
        ValueCodec,
        TopicRegions,
        Mapping
      >,
    ): KafkaTopicWithoutKey<Topics, Regions, ViewTopic, ValueCodec, TopicRegions, Mapping>;
    function topicHelper<
      const TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
      ValueCodec,
      KeyCodec,
      const ViewTopic extends Extract<keyof Topics, string>,
    >(
      topic: KafkaTopicDefinitionInput<
        Topics,
        Regions,
        ViewTopic,
        ValueCodec,
        KeyCodec,
        TopicRegions
      >,
    ) {
      return {
        ...topic,
        [KafkaTopicDefinitionTypeId]: true,
      };
    }

    return topicHelper;
  }

  return forRegions;
};
