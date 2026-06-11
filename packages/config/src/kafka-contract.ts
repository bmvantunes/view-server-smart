import { fromBinary } from "@bufbuild/protobuf";
import type { DescMessage, MessageShape } from "@bufbuild/protobuf";
import { Effect, Schema } from "effect";
import type { Config } from "effect";
import type { RowSchema, TopicRow } from "./topic-contract";

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
const KafkaCodecDecodeTypeId = Symbol("@view-server/config/KafkaCodecDecode");
const KafkaTopicDefinitionTypeId = Symbol("@view-server/config/KafkaTopicDefinition");
const KafkaTopicDecodeTypeId = Symbol("@view-server/config/KafkaTopicDecode");
const KafkaTopicSchemaTypeId = Symbol("@view-server/config/KafkaTopicSchema");

export type KafkaDecodeError = {
  readonly _tag: "KafkaDecodeError";
  readonly message: string;
  readonly cause?: unknown;
};

const KafkaMappingErrorTypeId: unique symbol = Symbol("@view-server/config/KafkaMappingError");

export type KafkaMappingError = {
  readonly _tag: "KafkaMappingError";
  readonly [KafkaMappingErrorTypeId]: typeof KafkaMappingErrorTypeId;
  readonly message: string;
  readonly cause?: unknown;
};

export const kafkaErrorIsMapping = (error: unknown): error is KafkaMappingError =>
  typeof error === "object" &&
  error !== null &&
  Object.hasOwn(error, KafkaMappingErrorTypeId) &&
  Reflect.get(error, KafkaMappingErrorTypeId) === KafkaMappingErrorTypeId;

export type KafkaCodec<A, E = never> = {
  readonly [KafkaCodecValueTypeId]: ReadonlyArray<A>;
  readonly [KafkaCodecErrorTypeId]: ReadonlyArray<E>;
  readonly [KafkaCodecDecodeTypeId]: (input: KafkaCodecDecodeInput) => Effect.Effect<A, E>;
  readonly format: string;
};

export type KafkaBytesCodec = KafkaCodec<Uint8Array> & { readonly format: "bytes" };
export type KafkaStringCodec = KafkaCodec<string> & { readonly format: "string" };
export type KafkaJsonCodec<SourceSchema extends RowSchema = RowSchema> = KafkaCodec<
  SourceSchema["Type"],
  KafkaDecodeError
> & {
  readonly format: "json";
  readonly schema: SourceSchema;
};
export type KafkaProtobufCodec<Proto extends DescMessage = DescMessage> = KafkaCodec<
  MessageShape<Proto>,
  KafkaDecodeError
> & {
  readonly format: "protobuf";
  readonly descriptor: Proto;
};
export type KafkaCustomCodec<A = unknown, E = unknown> = KafkaCodec<A, E> & {
  readonly format: "custom";
  readonly name: string;
  readonly decode: (input: KafkaCodecDecodeInput) => Effect.Effect<A, E, never>;
};

export type KafkaSourceCodec =
  | KafkaBytesCodec
  | KafkaStringCodec
  | KafkaJsonCodec
  | KafkaProtobufCodec
  | KafkaCustomCodec;

export type KafkaCodecType<Codec> = Codec extends KafkaCodec<infer A, infer _E> ? A : never;
export type KafkaCodecError<Codec> = Codec extends KafkaCodec<infer _A, infer E> ? E : never;

export type KafkaCodecDecodeInput = {
  readonly bytes: Uint8Array;
  readonly metadata: KafkaMessageMetadata;
};

export type KafkaProtobufType<Proto extends DescMessage> = MessageShape<Proto>;

type SupportedKafkaProtobufInput<Proto> = IsAny<Proto> extends true ? never : unknown;

type SupportedKafkaCodec<Codec extends KafkaCodec<unknown, unknown>> =
  IsAny<Codec> extends true
    ? never
    : IsAny<KafkaCodecType<Codec>> extends true
      ? never
      : IsAny<KafkaCodecError<Codec>> extends true
        ? never
        : IsUnknown<KafkaCodecError<Codec>> extends true
          ? never
          : Codec & KafkaCodec<KafkaCodecType<Codec>, KafkaCodecError<Codec>>;

type SupportedKafkaJsonSchema<SourceSchema extends RowSchema> =
  IsAny<SourceSchema> extends true ? never : unknown;

type KafkaTopicDefinitionMarker = {
  readonly [KafkaTopicDefinitionTypeId]: true;
};

type KafkaTopicDecodeInput<
  Topics extends KafkaTopicSchemaRegistry,
  _ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
> = {
  readonly keyBytes: Uint8Array;
  readonly valueBytes: Uint8Array;
  readonly region: Region;
  readonly metadata: KafkaMessageMetadata<Region>;
};

type KafkaTopicDecoder<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  E,
> = {
  readonly [KafkaTopicDecodeTypeId]: {
    bivarianceHack(
      input: KafkaTopicDecodeInput<Topics, ViewTopic, Region>,
    ): Effect.Effect<KafkaDecodedTopicMessage<Topics, ViewTopic>, E>;
  }["bivarianceHack"];
};

type KafkaTopicSchemaMarker<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
> = {
  readonly [KafkaTopicSchemaTypeId]: KafkaTopicSchemaValue<Topics, ViewTopic>;
};

type KafkaTopicSchemaValue<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
> = Topics[ViewTopic]["schema"];

type KafkaTopicSchemaRegistry = Record<
  string,
  {
    readonly schema: RowSchema;
  }
>;

const schemaForKafkaTopic = <
  const ViewTopic extends string,
  SchemaValue extends RowSchema,
  Topics extends {
    readonly [Topic in ViewTopic]: {
      readonly schema: SchemaValue;
    };
  },
>(
  topics: Topics,
  viewTopic: ViewTopic,
): SchemaValue => topics[viewTopic].schema;

const utf8Decoder = new TextDecoder();

const kafkaDecodeError = (message: string, cause: unknown): KafkaDecodeError => ({
  _tag: "KafkaDecodeError",
  message,
  cause,
});

const kafkaMappingError = (message: string, cause: unknown): KafkaMappingError => ({
  _tag: "KafkaMappingError",
  [KafkaMappingErrorTypeId]: KafkaMappingErrorTypeId,
  message,
  cause,
});

const makeKafkaCodec = <A, E, Format extends string>(
  format: Format,
  decode: (input: KafkaCodecDecodeInput) => Effect.Effect<A, E>,
): KafkaCodec<A, E> & {
  readonly format: Format;
} => ({
  [KafkaCodecValueTypeId]: [],
  [KafkaCodecErrorTypeId]: [],
  [KafkaCodecDecodeTypeId]: decode,
  format,
});

export const kafka = {
  bytes: (): KafkaBytesCodec =>
    makeKafkaCodec<Uint8Array, never, "bytes">("bytes", (input) => Effect.succeed(input.bytes)),
  string: (): KafkaStringCodec =>
    makeKafkaCodec<string, never, "string">("string", (input) =>
      Effect.succeed(utf8Decoder.decode(input.bytes)),
    ),
  stringKey: (): KafkaStringCodec =>
    makeKafkaCodec<string, never, "string">("string", (input) =>
      Effect.succeed(utf8Decoder.decode(input.bytes)),
    ),
  json: <const SourceSchema extends RowSchema>(
    schema: SourceSchema & SupportedKafkaJsonSchema<SourceSchema>,
  ): KafkaJsonCodec<SourceSchema> => ({
    ...makeKafkaCodec<SourceSchema["Type"], KafkaDecodeError, "json">("json", (input) =>
      Effect.gen(function* () {
        const rowSchema: SourceSchema = schema;
        const decodedJson = yield* Effect.try({
          try: (): unknown => JSON.parse(utf8Decoder.decode(input.bytes)),
          catch: (cause) => kafkaDecodeError("Failed to parse Kafka JSON payload", cause),
        });
        return yield* Schema.decodeUnknownEffect(rowSchema)(decodedJson).pipe(
          Effect.mapError((cause) =>
            kafkaDecodeError("Failed to decode Kafka JSON payload", cause),
          ),
        );
      }),
    ),
    schema,
  }),
  protobuf: <const Proto extends DescMessage>(
    descriptor: Proto & SupportedKafkaProtobufInput<Proto>,
  ): KafkaProtobufCodec<Proto> => ({
    ...makeKafkaCodec<MessageShape<Proto>, KafkaDecodeError, "protobuf">("protobuf", (input) => {
      const messageDescriptor: Proto = descriptor;
      return Effect.try({
        try: () => fromBinary(messageDescriptor, input.bytes),
        catch: (cause) => kafkaDecodeError("Failed to decode Kafka protobuf payload", cause),
      });
    }),
    descriptor,
  }),
  codec: <A, E>(
    definition: {
      readonly name: string;
      readonly decode: (input: KafkaCodecDecodeInput) => Effect.Effect<A, E, never>;
    } & RejectAnyCodecValue<NoInfer<A>> &
      RejectAnyCodecError<NoInfer<E>>,
  ): KafkaCustomCodec<A, E> => ({
    ...makeKafkaCodec<A, E, "custom">("custom", definition.decode),
    name: definition.name,
    decode: definition.decode,
  }),
};

export const decodeKafkaCodec: <A, E>(
  codec: KafkaCodec<A, E>,
  input: KafkaCodecDecodeInput,
) => Effect.Effect<A, E> = Effect.fn("ViewServerConfig.kafka.codec.decode")(function* <A, E>(
  codec: KafkaCodec<A, E>,
  input: KafkaCodecDecodeInput,
) {
  return yield* codec[KafkaCodecDecodeTypeId](input);
});

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
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  ValueCodec,
  KeyCodec,
> = {
  readonly key: [KeyCodec] extends [KafkaCodec<unknown, unknown>]
    ? KafkaCodecType<KeyCodec>
    : string;
  readonly value: KafkaCodecType<ValueCodec>;
  readonly region: Region;
  readonly schema: KafkaTopicSchemaValue<Topics, ViewTopic>;
  readonly metadata: KafkaMessageMetadata<Region>;
};

type KafkaMappingInputWithoutKey<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  ValueCodec extends KafkaCodec<unknown, unknown>,
> = {
  readonly key: string;
  readonly value: KafkaCodecType<ValueCodec>;
  readonly region: Region;
  readonly schema: KafkaTopicSchemaValue<Topics, ViewTopic>;
  readonly metadata: KafkaMessageMetadata<Region>;
};

type KafkaMappingInputWithKey<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
> = {
  readonly key: KafkaCodecType<KeyCodec>;
  readonly value: KafkaCodecType<ValueCodec>;
  readonly region: Region;
  readonly schema: KafkaTopicSchemaValue<Topics, ViewTopic>;
  readonly metadata: KafkaMessageMetadata<Region>;
};

export type KafkaDecodedTopicMessage<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
> = {
  readonly viewServerTopic: ViewTopic;
  readonly row: TopicRow<Topics, ViewTopic>;
};

type KafkaTopicWithoutKeyInput<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => TopicRow<Topics, ViewTopic>,
> = {
  readonly regions: TopicRegions;
  readonly value: SupportedKafkaCodec<ValueCodec>;
  readonly viewServerTopic: ViewTopic;
  readonly mapping: ExactMappingReturn<
    KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
    TopicRow<Topics, ViewTopic>,
    Mapping
  >;
};

type KafkaTopicWithoutKey<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => TopicRow<Topics, ViewTopic>,
> = KafkaTopicWithoutKeyInput<Topics, Regions, ViewTopic, ValueCodec, TopicRegions, Mapping> &
  KafkaTopicDefinitionMarker &
  KafkaTopicSchemaMarker<Topics, ViewTopic> &
  KafkaTopicDecoder<
    Topics,
    ViewTopic,
    TopicRegions[number],
    KafkaCodecError<ValueCodec> | KafkaDecodeError | KafkaMappingError
  >;

type KafkaTopicWithKeyInput<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => TopicRow<Topics, ViewTopic>,
> = {
  readonly regions: TopicRegions;
  readonly value: SupportedKafkaCodec<ValueCodec>;
  readonly key: SupportedKafkaCodec<KeyCodec>;
  readonly viewServerTopic: ViewTopic;
  readonly mapping: ExactMappingReturn<
    KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
    TopicRow<Topics, ViewTopic>,
    Mapping
  >;
};

type KafkaTopicWithKey<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
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
  KafkaTopicDefinitionMarker &
  KafkaTopicSchemaMarker<Topics, ViewTopic> &
  KafkaTopicDecoder<
    Topics,
    ViewTopic,
    TopicRegions[number],
    KafkaCodecError<ValueCodec> | KafkaCodecError<KeyCodec> | KafkaDecodeError | KafkaMappingError
  >;

export type KafkaTopicDefinition<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string> = Extract<keyof Topics, string>,
  ValueCodec extends KafkaCodec<unknown, unknown> = KafkaCodec<unknown, unknown>,
  KeyCodec = undefined,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>> =
    NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  MappingWithoutKey extends (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => TopicRow<Topics, ViewTopic>,
  MappingWithKey extends (
    input: KafkaMappingInputWithKey<
      Topics,
      ViewTopic,
      TopicRegions[number],
      ValueCodec,
      Extract<KeyCodec, KafkaCodec<unknown, unknown>>
    >,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithKey<
      Topics,
      ViewTopic,
      TopicRegions[number],
      ValueCodec,
      Extract<KeyCodec, KafkaCodec<unknown, unknown>>
    >,
  ) => TopicRow<Topics, ViewTopic>,
> =
  | KafkaTopicWithoutKey<Topics, Regions, ViewTopic, ValueCodec, TopicRegions, MappingWithoutKey>
  | (KeyCodec extends KafkaCodec<unknown, unknown>
      ? MappingWithKey extends (
          input: KafkaMappingInputWithKey<
            Topics,
            ViewTopic,
            TopicRegions[number],
            ValueCodec,
            KeyCodec
          >,
        ) => TopicRow<Topics, ViewTopic>
        ? KafkaTopicWithKey<
            Topics,
            Regions,
            ViewTopic,
            ValueCodec,
            KeyCodec,
            TopicRegions,
            MappingWithKey
          >
        : never
      : never);

export type KafkaRuntimeTopicDefinition<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>> =
    NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
> = KafkaTopicDefinitionMarker &
  KafkaTopicSchemaMarker<Topics, Extract<keyof Topics, string>> &
  KafkaTopicDecoder<Topics, Extract<keyof Topics, string>, TopicRegions[number], unknown> & {
    readonly regions: TopicRegions;
    readonly viewServerTopic: Extract<keyof Topics, string>;
  };

const decodeKafkaStringKey = (input: KafkaCodecDecodeInput): string =>
  utf8Decoder.decode(input.bytes);

const mapKafkaPayload = <A>(map: () => A): Effect.Effect<A, KafkaMappingError> =>
  Effect.try({
    try: map,
    catch: (cause) => kafkaMappingError("Failed to map Kafka payload", cause),
  });

export const decodeKafkaTopicMessage = Effect.fn("ViewServerConfig.kafka.topic.decodeMessage")(
  function* <
    Topics extends KafkaTopicSchemaRegistry,
    Regions extends RuntimeRegions,
    TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  >(
    topic: KafkaRuntimeTopicDefinition<Topics, Regions, TopicRegions>,
    input: {
      readonly keyBytes: Uint8Array;
      readonly valueBytes: Uint8Array;
      readonly region: TopicRegions[number];
      readonly metadata: KafkaMessageMetadata<TopicRegions[number]>;
    },
  ) {
    return yield* topic[KafkaTopicDecodeTypeId](input);
  },
);

type KafkaTopicDefinitionInput<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string> = Extract<keyof Topics, string>,
  ValueCodec extends KafkaCodec<unknown, unknown> = KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown> = KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>> =
    NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  MappingWithoutKey extends (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => TopicRow<Topics, ViewTopic>,
  MappingWithKey extends (
    input: KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
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
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  Candidate,
> = Candidate extends KafkaTopicDefinitionMarker & {
  readonly regions: infer TopicRegions extends NonEmptyReadonlyArray<
    Extract<keyof Regions, string>
  >;
  readonly value: infer ValueCodec extends KafkaCodec<unknown, unknown>;
  readonly key: infer KeyCodec extends KafkaCodec<unknown, unknown>;
  readonly viewServerTopic: infer ViewTopic extends Extract<keyof Topics, string>;
}
  ? Candidate extends {
      readonly mapping: infer Mapping extends (
        input: KafkaMappingInputWithKey<
          Topics,
          ViewTopic,
          TopicRegions[number],
          ValueCodec,
          KeyCodec
        >,
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
        readonly value: infer ValueCodec extends KafkaCodec<unknown, unknown>;
        readonly viewServerTopic: infer ViewTopic extends Extract<keyof Topics, string>;
      }
    ? Candidate extends {
        readonly mapping: infer Mapping extends (
          input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
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
  Topics extends KafkaTopicSchemaRegistry,
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
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  KafkaTopics extends Record<string, object>,
> = {
  readonly websocketPort: RuntimeValue<number>;
  readonly kafka: {
    readonly consumerGroupId: string;
    readonly regions: Regions;
    readonly topics: ValidateKafkaTopics<Topics, Regions, KafkaTopics>;
  };
};

export type RuntimeOptionsCandidate = {
  readonly websocketPort: RuntimeValue<number>;
  readonly kafka: {
    readonly consumerGroupId: string;
    readonly regions: RuntimeRegions;
    readonly topics: Record<string, object>;
  };
};

export type ValidateRuntimeOptions<
  Topics extends KafkaTopicSchemaRegistry,
  Options,
> = Options extends RuntimeOptionsCandidate
  ? RuntimeOptions<Topics, Options["kafka"]["regions"], Options["kafka"]["topics"]>
  : never;

export type RuntimeOptionsDefinition<
  Topics extends KafkaTopicSchemaRegistry,
  Options,
> = ValidateRuntimeOptions<Topics, Options>;

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

export type ExactRuntimeOptions<Topics extends KafkaTopicSchemaRegistry, Options> = Options &
  ValidateRuntimeOptions<Topics, Options> &
  RejectExtraKeys<Options, ValidateRuntimeOptions<Topics, Options>> &
  RejectExtraRuntimeKafkaKeys<Options, ValidateRuntimeOptions<Topics, Options>>;

export type KafkaTopicHelper<Topics extends KafkaTopicSchemaRegistry> = <
  const Regions extends RuntimeRegions,
>() => {
  <
    const TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
    ValueCodec extends KafkaCodec<unknown, unknown>,
    KeyCodec extends KafkaCodec<unknown, unknown>,
    const ViewTopic extends Extract<keyof Topics, string>,
    Mapping extends (
      input: KafkaMappingInputWithKey<
        Topics,
        ViewTopic,
        TopicRegions[number],
        ValueCodec,
        KeyCodec
      >,
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
    ValueCodec extends KafkaCodec<unknown, unknown>,
    const ViewTopic extends Extract<keyof Topics, string>,
    Mapping extends (
      input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
    ) => TopicRow<Topics, ViewTopic>,
  >(
    topic: KafkaTopicWithoutKeyInput<Topics, Regions, ViewTopic, ValueCodec, TopicRegions, Mapping>,
  ): KafkaTopicWithoutKey<Topics, Regions, ViewTopic, ValueCodec, TopicRegions, Mapping>;
};

const makeKafkaTopicWithKey = <
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
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
  schema: KafkaTopicSchemaValue<Topics, ViewTopic>,
): KafkaTopicWithKey<Topics, Regions, ViewTopic, ValueCodec, KeyCodec, TopicRegions, Mapping> => ({
  ...topic,
  [KafkaTopicDefinitionTypeId]: true,
  [KafkaTopicSchemaTypeId]: schema,
  [KafkaTopicDecodeTypeId]: (input) =>
    Effect.gen(function* () {
      const value = yield* decodeKafkaCodec(topic.value, {
        bytes: input.valueBytes,
        metadata: input.metadata,
      });
      const key = yield* decodeKafkaCodec(topic.key, {
        bytes: input.keyBytes,
        metadata: input.metadata,
      });
      const row = yield* mapKafkaPayload(() =>
        topic.mapping({
          key,
          value,
          region: input.region,
          schema,
          metadata: input.metadata,
        }),
      );
      return {
        viewServerTopic: topic.viewServerTopic,
        row,
      };
    }),
});

const makeKafkaTopicWithoutKey = <
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => TopicRow<Topics, ViewTopic>,
>(
  topic: KafkaTopicWithoutKeyInput<Topics, Regions, ViewTopic, ValueCodec, TopicRegions, Mapping>,
  schema: KafkaTopicSchemaValue<Topics, ViewTopic>,
): KafkaTopicWithoutKey<Topics, Regions, ViewTopic, ValueCodec, TopicRegions, Mapping> => ({
  ...topic,
  [KafkaTopicDefinitionTypeId]: true,
  [KafkaTopicSchemaTypeId]: schema,
  [KafkaTopicDecodeTypeId]: (input) =>
    Effect.gen(function* () {
      const value = yield* decodeKafkaCodec(topic.value, {
        bytes: input.valueBytes,
        metadata: input.metadata,
      });
      const key = decodeKafkaStringKey({
        bytes: input.keyBytes,
        metadata: input.metadata,
      });
      const row = yield* mapKafkaPayload(() =>
        topic.mapping({
          key,
          value,
          region: input.region,
          schema,
          metadata: input.metadata,
        }),
      );
      return {
        viewServerTopic: topic.viewServerTopic,
        row,
      };
    }),
});

export const defineKafkaTopic = <Topics extends KafkaTopicSchemaRegistry>(
  topics: Topics,
): KafkaTopicHelper<Topics> => {
  function forRegions<const Regions extends RuntimeRegions>() {
    function topicHelper<
      const TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
      ValueCodec extends KafkaCodec<unknown, unknown>,
      KeyCodec extends KafkaCodec<unknown, unknown>,
      const ViewTopic extends Extract<keyof Topics, string>,
      Mapping extends (
        input: KafkaMappingInputWithKey<
          Topics,
          ViewTopic,
          TopicRegions[number],
          ValueCodec,
          KeyCodec
        >,
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
      ValueCodec extends KafkaCodec<unknown, unknown>,
      const ViewTopic extends Extract<keyof Topics, string>,
      Mapping extends (
        input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
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
      ValueCodec extends KafkaCodec<unknown, unknown>,
      KeyCodec extends KafkaCodec<unknown, unknown>,
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
      if ("key" in topic) {
        return makeKafkaTopicWithKey(
          topic,
          schemaForKafkaTopic<ViewTopic, Topics[ViewTopic]["schema"], Topics>(
            topics,
            topic.viewServerTopic,
          ),
        );
      }
      return makeKafkaTopicWithoutKey(
        topic,
        schemaForKafkaTopic<ViewTopic, Topics[ViewTopic]["schema"], Topics>(
          topics,
          topic.viewServerTopic,
        ),
      );
    }

    return topicHelper;
  }

  return forRegions;
};
