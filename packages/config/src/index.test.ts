import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import { create, toBinary } from "@bufbuild/protobuf";
import { fileDesc, messageDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import type { GenMessage, GenService } from "@bufbuild/protobuf/codegenv2";
import type { Message } from "@bufbuild/protobuf";
import { FieldDescriptorProto_Type, FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import * as BigDecimal from "effect/BigDecimal";
import * as HashMap from "effect/HashMap";
import * as HashSet from "effect/HashSet";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import {
  Config,
  Duration,
  Effect,
  Exit,
  Schema,
  SchemaGetter,
  SchemaTransformation,
  Stream,
} from "effect";
import {
  decodeKafkaCodec,
  decodeKafkaTopicMessage,
  defineKafkaTopic,
  defineGrpcFeed,
  defineViewServerConfig,
  grpc,
  kafka,
  kafkaErrorIsMapping,
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
  viewServerReservedTopicNames,
  viewServerSchemaFieldMetadata,
  viewServerUnsupportedRuntimeFieldDomain,
  viewServerTopicNameIsReserved,
  viewServerHealthSummaryFromHealth,
  viewServerHealthSummaryRowFromHealth,
  viewServerHealthTopicRowsFromHealth,
  validateLiveQuerySourceRoute,
  type GrpcClientValue,
  type GrpcFeedDefinition,
  type GrpcTopicFeedsHealth,
  type KafkaCodec,
  type KafkaMappingInput,
  type KafkaMessageMetadata,
  type KafkaStartFromHealth,
  type KafkaCodecError,
  type KafkaCodecType,
  type KafkaDecodeError,
  type KafkaTopicHealth,
  type KafkaTopicRegionHealth,
  type KafkaTopicDefinition,
  type ExactGroupedQuery,
  type ExactLiveQueryInputForTopic,
  type ExactRawQuery,
  type GroupedQuery,
  type LiveQueryResult,
  type LiveQueryRow,
  type LiveSubscription,
  type LiveTransportAdapter,
  type RawQuery,
  type RowSchema,
  type SnapshotEvent,
  type StatusEvent,
  type TopicRuntimeHealth,
  type TopicRouteBy,
  type TopicRow,
  type ValidateLiveQuery,
  type ViewServerBackpressureError,
  type ViewServerHealth,
  type ViewServerHealthDetails,
  type ViewServerHealthSummary,
  type ViewServerHealthSummaryRow,
  type ViewServerHealthTopicRow,
  type ViewServerRuntimeClient,
  type ViewServerRuntimeError,
  type ViewServerTransportError,
} from "./index";
import { runtimeConfig, runtimeEnvironmentConfig } from "./runtime";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  quantity: Schema.Number,
  price: Schema.Number,
  region: Schema.String,
});

const Position = Schema.Struct({
  id: Schema.String,
  accountId: Schema.String,
  symbol: Schema.String,
  active: Schema.Boolean,
  quantity: Schema.BigInt,
  optionalQuantity: Schema.Union([Schema.BigInt, Schema.Undefined]),
  price: Schema.BigDecimal,
  notional: Schema.Number,
  optionalNotional: Schema.Union([Schema.Number, Schema.Undefined]),
});

const AmbiguousJsonNestedPosition = Schema.Struct({
  quantity: Schema.BigInt,
  price: Schema.BigDecimal,
  stringOrBigInt: Schema.Union([Schema.String, Schema.BigInt]),
  stringOrBytes: Schema.Union([Schema.String, Schema.Uint8Array]),
  stringOrNumber: Schema.Union([Schema.String, Schema.Number]),
});

const AmbiguousJsonNativeJsonPosition = Schema.Struct({
  id: Schema.String,
  jsonPayload: Schema.Json,
  mutableJsonPayload: Schema.MutableJson,
});

class AmbiguousJsonClassPosition extends Schema.Class<AmbiguousJsonClassPosition>(
  "AmbiguousJsonClassPosition",
)({
  quantity: Schema.BigInt,
  stringOrBigInt: Schema.Union([Schema.String, Schema.BigInt]),
}) {}

class AmbiguousJsonEmptyClassPosition extends Schema.Class<AmbiguousJsonEmptyClassPosition>(
  "AmbiguousJsonEmptyClassPosition",
)({}) {}

class AmbiguousJsonStringOnlyClassPosition extends Schema.Class<AmbiguousJsonStringOnlyClassPosition>(
  "AmbiguousJsonStringOnlyClassPosition",
)({
  id: Schema.String,
}) {}

class InvalidAmbiguousJsonSuspendedRecordKeyClassPosition extends Schema.Class<InvalidAmbiguousJsonSuspendedRecordKeyClassPosition>(
  "InvalidAmbiguousJsonSuspendedRecordKeyClassPosition",
)({
  id: Schema.String,
  suspendedRecord: Schema.Record(
    Schema.String,
    Schema.suspend(() => Schema.BigInt),
  ),
}) {}

const SuspendedAmbiguousJsonNestedPosition = Schema.suspend(() => AmbiguousJsonNestedPosition);

const AmbiguousJsonSuspendedEmptyObjectPosition = Schema.Struct({
  id: Schema.String,
  empty: Schema.suspend(() => Schema.Struct({})),
});

const AmbiguousJsonSuspendedEmptyObjectTransformPosition = Schema.Struct({
  id: Schema.String,
  empty: Schema.suspend(() =>
    Schema.String.pipe(
      Schema.decodeTo(Schema.Struct({}), {
        decode: SchemaGetter.transform(() => ({})),
        encode: SchemaGetter.transform(() => "empty"),
      }),
    ),
  ),
});

const AmbiguousJsonSuspendedOptionEmptyObjectTransformPosition = Schema.Struct({
  id: Schema.String,
  maybeEmpty: Schema.suspend(() =>
    Schema.String.pipe(
      Schema.decodeTo(Schema.Option(Schema.Struct({})), {
        decode: SchemaGetter.transform(() => Option.some({})),
        encode: SchemaGetter.transform(() => "empty"),
      }),
    ),
  ),
});

const AmbiguousJsonSuspendedClassPosition = Schema.Struct({
  id: Schema.String,
  nested: Schema.suspend(() => AmbiguousJsonClassPosition),
});

const AmbiguousJsonSuspendedEmptyClassPosition = Schema.Struct({
  id: Schema.String,
  nested: Schema.suspend(() => AmbiguousJsonEmptyClassPosition),
});

const AmbiguousJsonSuspendedOptionPosition = Schema.Struct({
  id: Schema.String,
  optionQuantity: Schema.suspend(() => Schema.Option(Schema.BigInt)),
});

const AmbiguousJsonEncodedKeyNestedPosition = Schema.Struct({
  quantity: Schema.BigInt,
  quantityFromString: Schema.BigIntFromString,
}).pipe(Schema.encodeKeys({ quantity: "qty", quantityFromString: "qty_from_string" }));

const AmbiguousJsonComposedObjectTransformPosition = Schema.Struct({
  quantity: Schema.BigInt,
}).pipe(
  Schema.decodeTo(Schema.Struct({ quantity: Schema.String }), {
    decode: SchemaGetter.transform((value) => ({ quantity: String(value.quantity) })),
    encode: SchemaGetter.transform((value) => ({ quantity: BigInt(value.quantity) })),
  }),
  Schema.decodeTo(Schema.Struct({ quantity: Schema.Number }), {
    decode: SchemaGetter.transform((value) => ({ quantity: Number(value.quantity) })),
    encode: SchemaGetter.transform((value) => ({ quantity: String(value.quantity) })),
  }),
);

const AmbiguousJsonEncodedKeyObjectTransformPosition = Schema.Struct({
  quantity: Schema.BigInt,
}).pipe(
  Schema.encodeKeys({ quantity: "qty" }),
  Schema.decodeTo(Schema.Struct({ quantityText: Schema.String }), {
    decode: SchemaGetter.transform((value) => ({ quantityText: String(value.quantity) })),
    encode: SchemaGetter.transform((value) => ({ quantity: BigInt(value.quantityText) })),
  }),
);

const AmbiguousJsonArrayTransformPosition = Schema.Array(Schema.String).pipe(
  Schema.decodeTo(Schema.Array(Schema.BigInt), {
    decode: SchemaGetter.transform((values) => values.map((value) => BigInt(value.trim()))),
    encode: SchemaGetter.transform((values) => values.map((value) => String(value))),
  }),
);

const AmbiguousJsonEncodedScalarInputPosition = Schema.BigInt.pipe(
  Schema.decodeTo(Schema.String, {
    decode: SchemaGetter.transform((value) => String(value)),
    encode: SchemaGetter.transform((value) => BigInt(value)),
  }),
);

const AmbiguousJsonEncodedArrayInputPosition = Schema.Array(Schema.BigInt).pipe(
  Schema.decodeTo(Schema.Array(Schema.String), {
    decode: SchemaGetter.transform((values) => values.map((value) => String(value))),
    encode: SchemaGetter.transform((values) => values.map((value) => BigInt(value))),
  }),
);

const AmbiguousJsonTransformUnionPosition = Schema.Union([
  AmbiguousJsonEncodedScalarInputPosition,
  Schema.Number,
]);

const AmbiguousJsonUnionPosition = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("primary"),
    quantity: Schema.BigInt,
    stringOrBigInt: Schema.Union([Schema.String, Schema.BigInt]),
  }),
  Schema.Struct({
    kind: Schema.Literal("secondary"),
    quantity: Schema.BigInt,
  }),
]);

const AmbiguousJsonOneOfPosition = Schema.Union(
  [
    Schema.Struct({
      quantity: Schema.BigInt,
    }),
    Schema.Struct({
      quantity: Schema.BigInt,
      note: Schema.String,
    }),
  ],
  { mode: "oneOf" },
);

const AmbiguousJsonUntaggedStructuredUnionPosition = Schema.Union([
  Schema.Struct({
    quantity: Schema.BigInt,
    stringOrBigInt: Schema.String,
  }),
  Schema.Struct({
    quantity: Schema.BigInt,
    stringOrBigInt: Schema.BigInt,
  }),
]);

const AmbiguousJsonSentinelUnionPosition = Schema.Union([
  Schema.Struct({
    id: Schema.BigInt,
  }),
  Schema.Struct({
    kind: Schema.Literal("b"),
    id: Schema.BigDecimal,
  }),
]);

const AmbiguousJsonOptionalSentinelUnionPosition = Schema.Union([
  Schema.Struct({
    id: Schema.BigInt,
  }),
  Schema.Struct({
    kind: Schema.optionalKey(Schema.Literal("b")),
    id: Schema.BigDecimal,
  }),
]);

const AmbiguousJsonPosition = Schema.Struct({
  id: Schema.String,
  quantity: Schema.BigInt,
  quantityFromString: Schema.BigIntFromString,
  stringOrBigInt: Schema.Union([Schema.String, Schema.BigInt]),
  stringOrBytes: Schema.Union([Schema.String, Schema.Uint8Array]),
  stringOrNumber: Schema.Union([Schema.String, Schema.Number]),
  bigDecimalOrBigInt: Schema.Union([Schema.BigDecimal, Schema.BigInt]),
  nested: AmbiguousJsonNestedPosition,
  nestedRows: Schema.Array(AmbiguousJsonNestedPosition),
  nestedUnion: AmbiguousJsonUnionPosition,
  structuredOrString: Schema.Union([Schema.String, AmbiguousJsonNestedPosition]),
  scalarOrStructured: Schema.Union([Schema.BigInt, AmbiguousJsonNestedPosition]),
  validOneOf: AmbiguousJsonOneOfPosition,
  classOrString: Schema.Union([AmbiguousJsonClassPosition, Schema.String]),
  untaggedStructuredUnion: AmbiguousJsonUntaggedStructuredUnionPosition,
  sentinelUnion: AmbiguousJsonSentinelUnionPosition,
  optionalSentinelUnion: AmbiguousJsonOptionalSentinelUnionPosition,
  suspendedNested: SuspendedAmbiguousJsonNestedPosition,
  classNested: AmbiguousJsonClassPosition,
  optionNested: Schema.Option(AmbiguousJsonNestedPosition),
  optionFromNullNested: Schema.OptionFromNullOr(AmbiguousJsonNestedPosition),
  optionFromNullishNested: Schema.OptionFromNullishOr(AmbiguousJsonNestedPosition),
  encodedNested: AmbiguousJsonEncodedKeyNestedPosition,
  composedObjectTransform: AmbiguousJsonComposedObjectTransformPosition,
  encodedKeyObjectTransform: AmbiguousJsonEncodedKeyObjectTransformPosition,
  arrayTransform: AmbiguousJsonArrayTransformPosition,
  encodedScalarInput: AmbiguousJsonEncodedScalarInputPosition,
  encodedArrayInput: AmbiguousJsonEncodedArrayInputPosition,
  transformUnion: AmbiguousJsonTransformUnionPosition,
  amountsByAccount: Schema.Record(Schema.String, Schema.BigInt),
  pricesByAccount: Schema.Record(Schema.String, Schema.BigDecimal),
  amountsByNumericAccount: Schema.Record(Schema.Number, Schema.BigInt),
  amountsByPrefixedAccount: Schema.Record(
    Schema.TemplateLiteral(["account-", Schema.Number]),
    Schema.BigInt,
  ),
  amountsByUnionKeyAccount: Schema.Record(
    Schema.Union([
      Schema.Number,
      Schema.TemplateLiteral([
        "account-",
        Schema.Union([Schema.Literal("open"), Schema.Literal("closed")]),
      ]),
    ]),
    Schema.BigInt,
  ),
  amountsByNestedTemplateAccount: Schema.Record(
    Schema.TemplateLiteral([
      "book-",
      Schema.TemplateLiteral([
        "account-",
        Schema.Union([Schema.Literal("open"), Schema.Literal("closed")]),
      ]),
    ]),
    Schema.BigInt,
  ),
  amountsByLiteralAccount: Schema.Record(Schema.Literal("account1"), Schema.BigInt),
  amountsBySymbolAccount: Schema.Record(Schema.Symbol, Schema.BigInt),
});

const AmbiguousJsonOptionPosition = Schema.Struct({
  id: Schema.String,
  optionNested: Schema.Option(AmbiguousJsonNestedPosition),
  optionQuantityFromString: Schema.Option(Schema.BigIntFromString),
});

const AmbiguousJsonRepeatedRecordKeyPosition = Schema.Struct({
  id: Schema.String,
  amountsByRepeatedAccount: Schema.Record(
    Schema.Union([Schema.String, Schema.String]),
    Schema.BigInt,
  ),
});

const AmbiguousJsonTransformedRecordKey = Schema.String.pipe(
  Schema.decodeTo(Schema.String, {
    decode: SchemaGetter.transform((value) => value.trim()),
    encode: SchemaGetter.transform((value) => value),
  }),
);

const AmbiguousJsonTransformedRecordKeyPosition = Schema.Struct({
  id: Schema.String,
  amountsByTransformedAccount: Schema.Record(
    AmbiguousJsonTransformedRecordKey,
    Schema.BigIntFromString,
  ),
});

const AmbiguousJsonDeclaredQuantity = Schema.declare<
  typeof Schema.BigIntFromString.Type,
  typeof Schema.BigIntFromString.Encoded
>((value): value is typeof Schema.BigIntFromString.Type => typeof value === "bigint", {
  toCodecJson: () =>
    Schema.link<typeof Schema.BigIntFromString.Type>()(
      Schema.BigIntFromString,
      SchemaTransformation.passthrough(),
    ),
});

const AmbiguousJsonParametricDeclaredString = Schema.declareConstructor<string>()(
  [Schema.String],
  () => (value) => Effect.succeed(String(value)),
  {
    toCodecJson: () => Schema.link<string>()(Schema.String, SchemaTransformation.passthrough()),
  },
);

const AmbiguousJsonDeclaredCodecPosition = Schema.Struct({
  id: Schema.String,
  declaredQuantity: AmbiguousJsonDeclaredQuantity,
});

const AmbiguousJsonSuspendedScalarCodecPosition = Schema.Struct({
  id: Schema.String,
  scalarCodecs: Schema.suspend(() =>
    Schema.Struct({
      declaredQuantity: AmbiguousJsonDeclaredQuantity,
      declaredText: AmbiguousJsonParametricDeclaredString,
      quantityFromString: Schema.BigIntFromString,
    }),
  ),
});

const AmbiguousJsonDeclaredRepeatedStringUnionEncoded = Schema.Union([
  Schema.String,
  Schema.String,
]);

const AmbiguousJsonDeclaredRepeatedStringUnion = Schema.declare<
  typeof AmbiguousJsonDeclaredRepeatedStringUnionEncoded.Type,
  typeof AmbiguousJsonDeclaredRepeatedStringUnionEncoded.Encoded
>(
  (value): value is typeof AmbiguousJsonDeclaredRepeatedStringUnionEncoded.Type => {
    return typeof value === "string";
  },
  {
    toCodecJson: () =>
      Schema.link<typeof AmbiguousJsonDeclaredRepeatedStringUnionEncoded.Type>()(
        AmbiguousJsonDeclaredRepeatedStringUnionEncoded,
        SchemaTransformation.passthrough(),
      ),
  },
);

const AmbiguousJsonDeclaredStringArrayEncoded = Schema.Array(Schema.String);

const AmbiguousJsonDeclaredStringArray = Schema.declare<
  typeof AmbiguousJsonDeclaredStringArrayEncoded.Type,
  typeof AmbiguousJsonDeclaredStringArrayEncoded.Encoded
>(Array.isArray, {
  toCodecJson: () =>
    Schema.link<typeof AmbiguousJsonDeclaredStringArrayEncoded.Type>()(
      AmbiguousJsonDeclaredStringArrayEncoded,
      SchemaTransformation.passthrough(),
    ),
});

const AmbiguousJsonDeclaredScalarToObjectTarget = Schema.String.pipe(
  Schema.decodeTo(Schema.Struct({ id: Schema.String }), {
    decode: SchemaGetter.transform((value) => ({ id: value })),
    encode: SchemaGetter.transform((value) => value.id),
  }),
);

const AmbiguousJsonDeclaredScalarToObject = Schema.declare<
  typeof AmbiguousJsonDeclaredScalarToObjectTarget.Type,
  typeof AmbiguousJsonDeclaredScalarToObjectTarget.Encoded
>((_value): _value is typeof AmbiguousJsonDeclaredScalarToObjectTarget.Type => true, {
  toCodecJson: () =>
    Schema.link<typeof AmbiguousJsonDeclaredScalarToObjectTarget.Type>()(
      AmbiguousJsonDeclaredScalarToObjectTarget,
      SchemaTransformation.passthrough(),
    ),
});

const AmbiguousJsonDeclaredScalarToObjectKeywordTarget = Schema.String.pipe(
  Schema.decodeTo(Schema.ObjectKeyword, {
    decode: SchemaGetter.transform((value) => ({ id: value })),
    encode: SchemaGetter.transform((value) => String(Reflect.get(value, "id"))),
  }),
);

const AmbiguousJsonDeclaredScalarToObjectKeyword = Schema.declare<
  typeof AmbiguousJsonDeclaredScalarToObjectKeywordTarget.Type,
  typeof AmbiguousJsonDeclaredScalarToObjectKeywordTarget.Encoded
>((_value): _value is typeof AmbiguousJsonDeclaredScalarToObjectKeywordTarget.Type => true, {
  toCodecJson: () =>
    Schema.link<typeof AmbiguousJsonDeclaredScalarToObjectKeywordTarget.Type>()(
      AmbiguousJsonDeclaredScalarToObjectKeywordTarget,
      SchemaTransformation.passthrough(),
    ),
});

const AmbiguousJsonDeclaredScalarToUnknownTarget = Schema.String.pipe(
  Schema.decodeTo(Schema.Unknown, {
    decode: SchemaGetter.transform((value) => ({ id: value })),
    encode: SchemaGetter.transform((value) => String(Reflect.get(Object(value), "id"))),
  }),
);

const AmbiguousJsonDeclaredScalarToUnknown = Schema.declare<
  typeof AmbiguousJsonDeclaredScalarToUnknownTarget.Type,
  typeof AmbiguousJsonDeclaredScalarToUnknownTarget.Encoded
>((_value): _value is typeof AmbiguousJsonDeclaredScalarToUnknownTarget.Type => true, {
  toCodecJson: () =>
    Schema.link<typeof AmbiguousJsonDeclaredScalarToUnknownTarget.Type>()(
      AmbiguousJsonDeclaredScalarToUnknownTarget,
      SchemaTransformation.passthrough(),
    ),
});

const AmbiguousJsonDeclaredTemplateTarget = Schema.TemplateLiteral(["template-", Schema.String]);

const AmbiguousJsonDeclaredTemplate = Schema.declare<
  typeof AmbiguousJsonDeclaredTemplateTarget.Type,
  typeof AmbiguousJsonDeclaredTemplateTarget.Encoded
>((_value): _value is typeof AmbiguousJsonDeclaredTemplateTarget.Type => true, {
  toCodecJson: () =>
    Schema.link<typeof AmbiguousJsonDeclaredTemplateTarget.Type>()(
      AmbiguousJsonDeclaredTemplateTarget,
      SchemaTransformation.passthrough(),
    ),
});

const AmbiguousJsonInnerDeclaredString = Schema.declareConstructor<string>()(
  [Schema.String],
  () => (value) => Effect.succeed(String(value)),
);

const AmbiguousJsonDeclaredNestedDeclaration = Schema.declare<
  typeof AmbiguousJsonInnerDeclaredString.Type,
  typeof AmbiguousJsonInnerDeclaredString.Encoded
>((_value): _value is typeof AmbiguousJsonInnerDeclaredString.Type => true, {
  toCodecJson: () =>
    Schema.link<typeof AmbiguousJsonInnerDeclaredString.Type>()(
      AmbiguousJsonInnerDeclaredString,
      SchemaTransformation.passthrough(),
    ),
});

const AmbiguousJsonDeclaredScalarCollectionsPosition = Schema.Struct({
  id: Schema.String,
  declaredText: AmbiguousJsonDeclaredRepeatedStringUnion,
  declaredTags: AmbiguousJsonDeclaredStringArray,
  declaredObject: AmbiguousJsonDeclaredScalarToObject,
  declaredObjectKeyword: AmbiguousJsonDeclaredScalarToObjectKeyword,
  declaredUnknown: AmbiguousJsonDeclaredScalarToUnknown,
  declaredTemplate: AmbiguousJsonDeclaredTemplate,
});

const AmbiguousJsonDeclaredNestedDeclarationPosition = Schema.Struct({
  id: Schema.String,
  declaredNested: AmbiguousJsonDeclaredNestedDeclaration,
});

const AmbiguousJsonOpaqueDeclaredString = Schema.declare<string>(
  (value): value is string => typeof value === "string",
);

const AmbiguousJsonDeclaredOpaqueDeclaration = Schema.declare<
  typeof AmbiguousJsonOpaqueDeclaredString.Type,
  typeof AmbiguousJsonOpaqueDeclaredString.Encoded
>((_value): _value is typeof AmbiguousJsonOpaqueDeclaredString.Type => true, {
  toCodecJson: () =>
    Schema.link<typeof AmbiguousJsonOpaqueDeclaredString.Type>()(
      AmbiguousJsonOpaqueDeclaredString,
      SchemaTransformation.passthrough(),
    ),
});

const AmbiguousJsonDeclaredOpaqueDeclarationPosition = Schema.Struct({
  id: Schema.String,
  declaredOpaque: AmbiguousJsonDeclaredOpaqueDeclaration,
});

const AmbiguousJsonDurationPosition = Schema.Struct({
  id: Schema.String,
  latency: Schema.Duration,
});

const AmbiguousJsonAnnotatedDurationPosition = Schema.Struct({
  id: Schema.String,
  latency: Schema.Duration.annotate({ title: "latency" }),
});

const AmbiguousJsonErrorObjectsPosition = Schema.Struct({
  id: Schema.String,
  error: Schema.Error().annotate({ title: "error" }),
  errorWithStack: Schema.Error({ includeStack: true }),
  errorWithoutCause: Schema.Error({ excludeCause: true }),
  errorWithStackWithoutCause: Schema.Error({ includeStack: true, excludeCause: true }),
  pattern: Schema.RegExp,
});

const AmbiguousJsonFilePosition = Schema.Struct({
  id: Schema.String,
  attachment: Schema.File,
});

const AmbiguousJsonFormDataPosition = Schema.Struct({
  id: Schema.String,
  formData: Schema.FormData,
});

const AmbiguousJsonEffectCollectionsPosition = Schema.Struct({
  id: Schema.String,
  readonlySet: Schema.ReadonlySet(AmbiguousJsonNestedPosition),
  hashSet: Schema.HashSet(AmbiguousJsonNestedPosition),
  chunk: Schema.Chunk(AmbiguousJsonNestedPosition),
});

const AmbiguousJsonRedactedPosition = Schema.Struct({
  id: Schema.String,
  secret: Schema.Redacted(Schema.String),
});

const AmbiguousJsonRedactedObjectPosition = Schema.Struct({
  id: Schema.String,
  redactedDuration: Schema.Redacted(Schema.Duration),
  redactedNested: Schema.Redacted(AmbiguousJsonNestedPosition),
});

const AmbiguousJsonSuspendedStringMapKeyPosition = Schema.Struct({
  id: Schema.String,
  readonlyMap: Schema.ReadonlyMap(
    Schema.suspend(() => Schema.String),
    Schema.BigInt,
  ),
  hashMap: Schema.HashMap(
    Schema.suspend(() => Schema.String),
    Schema.BigInt,
  ),
});

const AmbiguousJsonSuspendedTupleWithRestPosition = Schema.Struct({
  id: Schema.String,
  tuple: Schema.suspend(() =>
    Schema.TupleWithRest(Schema.Tuple([AmbiguousJsonParametricDeclaredString]), [Schema.String]),
  ),
});

const AmbiguousJsonTaggedUnionPosition = Schema.Struct({
  id: Schema.String,
  tagged: Schema.TaggedUnion({
    quantity: {
      amount: Schema.BigInt,
    },
    note: {
      text: Schema.String,
    },
  }),
});

const InvalidAmbiguousJsonArrayPosition = Schema.Struct({
  id: Schema.String,
  nestedRows: Schema.Array(AmbiguousJsonNestedPosition),
});

const InvalidAmbiguousJsonArrayTransformPosition = Schema.Struct({
  id: Schema.String,
  arrayTransform: AmbiguousJsonArrayTransformPosition,
});

const InvalidAmbiguousJsonEncodedArrayInputPosition = Schema.Struct({
  id: Schema.String,
  encodedArrayInput: AmbiguousJsonEncodedArrayInputPosition,
});

const InvalidAmbiguousJsonNestedPosition = Schema.Struct({
  id: Schema.String,
  nested: AmbiguousJsonNestedPosition,
});

const InvalidAmbiguousJsonTuplePosition = Schema.Struct({
  id: Schema.String,
  tuple: Schema.Tuple([Schema.BigInt]),
});

const InvalidAmbiguousJsonUnionPosition = Schema.Struct({
  id: Schema.String,
  nestedUnion: AmbiguousJsonUnionPosition,
});

const InvalidAmbiguousJsonUnionWithInvalidRecordPosition = Schema.Struct({
  id: Schema.String,
  unionWithInvalidRecord: Schema.Union([
    Schema.Record(Schema.Literal(1), Schema.BigInt),
    Schema.Struct({
      kind: Schema.Literal("ok"),
      value: Schema.String,
    }),
  ]),
});

const InvalidAmbiguousJsonRecordPosition = Schema.Struct({
  id: Schema.String,
  amountsByAccount: Schema.Record(Schema.String, Schema.BigInt),
});

const InvalidAmbiguousJsonNumericLiteralRecordPosition = Schema.Struct({
  id: Schema.String,
  amountsByNumericLiteralAccount: Schema.Record(Schema.Literal(1), Schema.BigInt),
});

const InvalidAmbiguousJsonDeclaredRecordEncoded = Schema.Record(Schema.Literal(1), Schema.BigInt);

const InvalidAmbiguousJsonDeclaredRecord = Schema.declare<
  typeof InvalidAmbiguousJsonDeclaredRecordEncoded.Type,
  typeof InvalidAmbiguousJsonDeclaredRecordEncoded.Encoded
>(
  (value): value is typeof InvalidAmbiguousJsonDeclaredRecordEncoded.Type => {
    return typeof value === "object" && value !== null;
  },
  {
    toCodecJson: () =>
      Schema.link<typeof InvalidAmbiguousJsonDeclaredRecordEncoded.Type>()(
        InvalidAmbiguousJsonDeclaredRecordEncoded,
        SchemaTransformation.passthrough(),
      ),
  },
);

const InvalidAmbiguousJsonDeclaredRecordPosition = Schema.Struct({
  id: Schema.String,
  declaredRecord: InvalidAmbiguousJsonDeclaredRecord,
});

const InvalidAmbiguousJsonDeclaredSuspendedRecordKeyEncoded = Schema.Record(
  Schema.String,
  Schema.suspend(() => Schema.BigIntFromString),
);

const InvalidAmbiguousJsonDeclaredSuspendedRecordKey = Schema.declare<
  typeof InvalidAmbiguousJsonDeclaredSuspendedRecordKeyEncoded.Type,
  typeof InvalidAmbiguousJsonDeclaredSuspendedRecordKeyEncoded.Encoded
>(
  (value): value is typeof InvalidAmbiguousJsonDeclaredSuspendedRecordKeyEncoded.Type =>
    typeof value === "object" && value !== null,
  {
    toCodecJson: () =>
      Schema.link<typeof InvalidAmbiguousJsonDeclaredSuspendedRecordKeyEncoded.Type>()(
        InvalidAmbiguousJsonDeclaredSuspendedRecordKeyEncoded,
        SchemaTransformation.passthrough(),
      ),
  },
);

const InvalidAmbiguousJsonDeclaredSuspendedRecordKeyPosition = Schema.Struct({
  id: Schema.String,
  declaredRecord: InvalidAmbiguousJsonDeclaredSuspendedRecordKey,
});

const InvalidAmbiguousJsonDeclaredSuspendedRecordKeyTarget = Schema.declare<
  typeof InvalidAmbiguousJsonDeclaredSuspendedRecordKeyEncoded.Type,
  typeof InvalidAmbiguousJsonDeclaredSuspendedRecordKeyEncoded.Encoded
>(
  (value): value is typeof InvalidAmbiguousJsonDeclaredSuspendedRecordKeyEncoded.Type =>
    typeof value === "object" && value !== null,
  {
    toCodecJson: () =>
      Schema.link<typeof InvalidAmbiguousJsonDeclaredSuspendedRecordKeyEncoded.Type>()(
        Schema.suspend(() => InvalidAmbiguousJsonDeclaredSuspendedRecordKeyEncoded),
        SchemaTransformation.passthrough(),
      ),
  },
);

const InvalidAmbiguousJsonDeclaredSuspendedRecordKeyTargetPosition = Schema.Struct({
  id: Schema.String,
  declaredRecord: InvalidAmbiguousJsonDeclaredSuspendedRecordKeyTarget,
});

const InvalidAmbiguousJsonDeclaredNestedSuspendedRecordKeyEncoded = Schema.Struct({
  nested: Schema.Record(
    Schema.String,
    Schema.suspend(() => Schema.BigIntFromString),
  ),
});

const InvalidAmbiguousJsonDeclaredNestedSuspendedRecordKey = Schema.declare<
  typeof InvalidAmbiguousJsonDeclaredNestedSuspendedRecordKeyEncoded.Type,
  typeof InvalidAmbiguousJsonDeclaredNestedSuspendedRecordKeyEncoded.Encoded
>(
  (value): value is typeof InvalidAmbiguousJsonDeclaredNestedSuspendedRecordKeyEncoded.Type =>
    typeof value === "object" && value !== null,
  {
    toCodecJson: () =>
      Schema.link<typeof InvalidAmbiguousJsonDeclaredNestedSuspendedRecordKeyEncoded.Type>()(
        InvalidAmbiguousJsonDeclaredNestedSuspendedRecordKeyEncoded,
        SchemaTransformation.passthrough(),
      ),
  },
);

const InvalidAmbiguousJsonDeclaredNestedSuspendedRecordKeyPosition = Schema.Struct({
  id: Schema.String,
  declaredRecord: InvalidAmbiguousJsonDeclaredNestedSuspendedRecordKey,
});

const InvalidAmbiguousJsonDeclaredPartialRecordKeyEncoded = Schema.Record(
  Schema.String,
  Schema.suspend(() => Schema.BigIntFromString),
);

const InvalidAmbiguousJsonDeclaredPartialRecordKey = Schema.declare<
  typeof InvalidAmbiguousJsonDeclaredPartialRecordKeyEncoded.Type,
  typeof InvalidAmbiguousJsonDeclaredPartialRecordKeyEncoded.Encoded
>(
  (value): value is typeof InvalidAmbiguousJsonDeclaredPartialRecordKeyEncoded.Type =>
    typeof value === "object" && value !== null,
  {
    toCodecJson: () =>
      Schema.link<typeof InvalidAmbiguousJsonDeclaredPartialRecordKeyEncoded.Type>()(
        InvalidAmbiguousJsonDeclaredPartialRecordKeyEncoded,
        SchemaTransformation.passthrough(),
      ),
  },
);

const InvalidAmbiguousJsonDeclaredPartialRecordKeyPosition = Schema.Struct({
  id: Schema.String,
  declaredRecord: InvalidAmbiguousJsonDeclaredPartialRecordKey,
});

const InvalidAmbiguousJsonDeclaredSpoofedTypeConstructorEncoded = Schema.Record(
  Schema.String,
  Schema.suspend(() => Schema.BigIntFromString),
);

const InvalidAmbiguousJsonDeclaredSpoofedTypeConstructor = Schema.declare<
  typeof InvalidAmbiguousJsonDeclaredSpoofedTypeConstructorEncoded.Type,
  typeof InvalidAmbiguousJsonDeclaredSpoofedTypeConstructorEncoded.Encoded
>(
  (value): value is typeof InvalidAmbiguousJsonDeclaredSpoofedTypeConstructorEncoded.Type =>
    typeof value === "object" && value !== null,
  {
    typeConstructor: { _tag: "effect/Duration" },
    toCodecJson: () =>
      Schema.link<typeof InvalidAmbiguousJsonDeclaredSpoofedTypeConstructorEncoded.Type>()(
        InvalidAmbiguousJsonDeclaredSpoofedTypeConstructorEncoded,
        SchemaTransformation.passthrough(),
      ),
  },
);

const InvalidAmbiguousJsonDeclaredSpoofedTypeConstructorPosition = Schema.Struct({
  id: Schema.String,
  declaredRecord: InvalidAmbiguousJsonDeclaredSpoofedTypeConstructor,
});

const InvalidAmbiguousJsonDeclaredSpoofedOptionWithValue = Schema.declare<
  typeof InvalidAmbiguousJsonDeclaredSpoofedTypeConstructorEncoded.Type,
  typeof InvalidAmbiguousJsonDeclaredSpoofedTypeConstructorEncoded.Encoded
>(
  (value): value is typeof InvalidAmbiguousJsonDeclaredSpoofedTypeConstructorEncoded.Type =>
    typeof value === "object" && value !== null,
  {
    typeConstructor: { _tag: "effect/Option" },
    toCodecJson: () =>
      Schema.link<typeof InvalidAmbiguousJsonDeclaredSpoofedTypeConstructorEncoded.Type>()(
        InvalidAmbiguousJsonDeclaredSpoofedTypeConstructorEncoded,
        SchemaTransformation.passthrough(),
      ),
  },
);

Object.defineProperty(InvalidAmbiguousJsonDeclaredSpoofedOptionWithValue, "value", {
  configurable: true,
  enumerable: true,
  value: Schema.String,
});

const InvalidAmbiguousJsonDeclaredSpoofedOptionWithValuePosition = Schema.Struct({
  id: Schema.String,
  declaredRecord: InvalidAmbiguousJsonDeclaredSpoofedOptionWithValue,
});

const InvalidAmbiguousJsonOptionAnnotationSource = Object(
  Schema.Option(Schema.String).ast.annotations,
);

const InvalidAmbiguousJsonDeclaredSpoofedOptionBuiltInToCodec = Schema.declareConstructor<object>()(
  [Schema.String],
  () => (value) => Effect.succeed(Object(value)),
  {
    typeConstructor: Reflect.get(InvalidAmbiguousJsonOptionAnnotationSource, "typeConstructor"),
    toCodec: Reflect.get(InvalidAmbiguousJsonOptionAnnotationSource, "toCodec"),
  },
);

Object.defineProperty(InvalidAmbiguousJsonDeclaredSpoofedOptionBuiltInToCodec, "value", {
  configurable: true,
  enumerable: true,
  value: Schema.String,
});

const InvalidAmbiguousJsonDeclaredSpoofedOptionBuiltInToCodecPosition = Schema.Struct({
  id: Schema.String,
  declaredRecord: InvalidAmbiguousJsonDeclaredSpoofedOptionBuiltInToCodec,
});

const InvalidAmbiguousJsonDeclaredDurationTarget = Schema.declare<
  typeof Schema.Duration.Type,
  typeof Schema.Duration.Encoded
>((_value): _value is typeof Schema.Duration.Type => true, {
  toCodecJson: () =>
    Schema.link<typeof Schema.Duration.Type>()(Schema.Duration, SchemaTransformation.passthrough()),
});

const InvalidAmbiguousJsonDeclaredDurationTargetPosition = Schema.Struct({
  id: Schema.String,
  declaredDuration: InvalidAmbiguousJsonDeclaredDurationTarget,
});

const InvalidAmbiguousJsonDeclaredOptionRecordTargetEncoded = Schema.Option(
  Schema.Record(Schema.String, Schema.BigInt),
);

const InvalidAmbiguousJsonDeclaredOptionRecordTarget = Schema.declare<
  typeof InvalidAmbiguousJsonDeclaredOptionRecordTargetEncoded.Type,
  typeof InvalidAmbiguousJsonDeclaredOptionRecordTargetEncoded.Encoded
>((_value): _value is typeof InvalidAmbiguousJsonDeclaredOptionRecordTargetEncoded.Type => true, {
  toCodecJson: () =>
    Schema.link<typeof InvalidAmbiguousJsonDeclaredOptionRecordTargetEncoded.Type>()(
      InvalidAmbiguousJsonDeclaredOptionRecordTargetEncoded,
      SchemaTransformation.passthrough(),
    ),
});

const InvalidAmbiguousJsonDeclaredOptionRecordTargetPosition = Schema.Struct({
  id: Schema.String,
  declaredOptionRecord: InvalidAmbiguousJsonDeclaredOptionRecordTarget,
});

const InvalidAmbiguousJsonAnnotatedDurationOverridePosition = Schema.Struct({
  id: Schema.String,
  latency: Schema.Duration.annotate({
    toCodecJson: () =>
      Schema.link<typeof Schema.Duration.Type>()(
        Schema.Struct({ value: Schema.String }),
        SchemaTransformation.transform({
          decode: () => Duration.millis(0),
          encode: () => ({ value: "0" }),
        }),
      ),
  }),
});

const InvalidAmbiguousJsonAnnotatedOptionOverridePosition = Schema.Struct({
  id: Schema.String,
  maybe: Schema.Option(Schema.String).annotate({
    toCodecJson: () =>
      Schema.link<Option.Option<string>>()(
        Schema.Struct({ value: Schema.String }),
        SchemaTransformation.transform({
          decode: ({ value }) => Option.some(value),
          encode: (value) => ({ value: Option.getOrElse(value, () => "") }),
        }),
      ),
  }),
});

const InvalidAmbiguousJsonAnnotatedReadonlyMapOverridePosition = Schema.Struct({
  id: Schema.String,
  map: Schema.ReadonlyMap(Schema.String, Schema.String).annotate({
    toCodecJson: () =>
      Schema.link<ReadonlyMap<string, string>>()(
        Schema.Struct({ value: Schema.String }),
        SchemaTransformation.transform({
          decode: (): ReadonlyMap<string, string> => new Map<string, string>(),
          encode: () => ({ value: "" }),
        }),
      ),
  }),
});

const InvalidAmbiguousJsonAnnotatedOptionToCodecOverridePosition = Schema.Struct({
  id: Schema.String,
  maybe: Schema.Option(Schema.String).annotate({
    toCodec: () =>
      Schema.link<Option.Option<string>>()(
        Schema.Struct({ value: Schema.String }),
        SchemaTransformation.transform({
          decode: ({ value }) => Option.some(value),
          encode: (value) => ({ value: Option.getOrElse(value, () => "") }),
        }),
      ),
  }),
});

const InvalidAmbiguousJsonAnnotatedOptionToCodecSpoofedSourcePosition = (() => {
  const spoofedToCodec = () =>
    Schema.link<Option.Option<string>>()(
      Schema.Struct({ value: Schema.String }),
      SchemaTransformation.transform({
        decode: ({ value }) => Option.some(value),
        encode: (value) => ({ value: Option.getOrElse(value, () => "") }),
      }),
    );
  Object.defineProperty(spoofedToCodec, "toString", {
    value: () =>
      String(Reflect.get(Object(Schema.Option(Schema.String).ast.annotations), "toCodec")),
  });
  return Schema.Struct({
    id: Schema.String,
    maybe: Schema.Option(Schema.String).annotate({
      toCodec: spoofedToCodec,
    }),
  });
})();

const InvalidAmbiguousJsonAnnotatedReadonlyMapToCodecOverridePosition = Schema.Struct({
  id: Schema.String,
  map: Schema.ReadonlyMap(Schema.String, Schema.String).annotate({
    toCodec: () =>
      Schema.link<ReadonlyMap<string, string>>()(
        Schema.Struct({ value: Schema.String }),
        SchemaTransformation.transform({
          decode: (): ReadonlyMap<string, string> => new Map<string, string>(),
          encode: () => ({ value: "" }),
        }),
      ),
  }),
});

const InvalidAmbiguousJsonDeclaredToCodecRecordEncoded = Schema.Record(
  Schema.String,
  Schema.suspend(() => Schema.BigIntFromString),
);

const InvalidAmbiguousJsonDeclaredToCodecRecord = Schema.declare<
  typeof InvalidAmbiguousJsonDeclaredToCodecRecordEncoded.Type,
  typeof InvalidAmbiguousJsonDeclaredToCodecRecordEncoded.Encoded
>(
  (value): value is typeof InvalidAmbiguousJsonDeclaredToCodecRecordEncoded.Type =>
    typeof value === "object" && value !== null,
  {
    toCodec: () =>
      Schema.link<typeof InvalidAmbiguousJsonDeclaredToCodecRecordEncoded.Type>()(
        InvalidAmbiguousJsonDeclaredToCodecRecordEncoded,
        SchemaTransformation.passthrough(),
      ),
  },
);

const InvalidAmbiguousJsonDeclaredToCodecRecordPosition = Schema.Struct({
  id: Schema.String,
  declaredRecord: InvalidAmbiguousJsonDeclaredToCodecRecord,
});

const UnsupportedAmbiguousJsonDeclaredEmptyStruct = Schema.declare<{}>(
  (value): value is {} => typeof value === "object" && value !== null,
  {
    toCodecJson: () => Schema.link<{}>()(Schema.Struct({}), SchemaTransformation.passthrough()),
  },
);

const UnsupportedAmbiguousJsonDeclaredEmptyStructPosition = Schema.Struct({
  id: Schema.String,
  declaredEmpty: UnsupportedAmbiguousJsonDeclaredEmptyStruct,
});

const UnsupportedAmbiguousJsonDeclaredSuspendedEmptyStruct = Schema.declare<{}>(
  (value): value is {} => typeof value === "object" && value !== null,
  {
    toCodecJson: () =>
      Schema.link<{}>()(
        Schema.suspend(() => Schema.Struct({})),
        SchemaTransformation.passthrough(),
      ),
  },
);

const UnsupportedAmbiguousJsonDeclaredSuspendedEmptyStructPosition = Schema.Struct({
  id: Schema.String,
  declaredEmpty: UnsupportedAmbiguousJsonDeclaredSuspendedEmptyStruct,
});

const UnsupportedAmbiguousJsonSuspendedDeclaredEmptyStructPosition = Schema.Struct({
  id: Schema.String,
  declaredEmpty: Schema.suspend(() => UnsupportedAmbiguousJsonDeclaredEmptyStruct),
});

const UnsupportedAmbiguousJsonSuspendedStructDeclaredEmptyStructPosition = Schema.Struct({
  id: Schema.String,
  wrapper: Schema.suspend(() =>
    Schema.Struct({
      nested: UnsupportedAmbiguousJsonDeclaredEmptyStruct,
    }),
  ),
});

const UnsupportedAmbiguousJsonSuspendedArrayDeclaredEmptyStructPosition = Schema.Struct({
  id: Schema.String,
  wrapper: Schema.suspend(() => Schema.Array(UnsupportedAmbiguousJsonDeclaredEmptyStruct)),
});

const UnsupportedAmbiguousJsonSuspendedUnionDeclaredEmptyStructPosition = Schema.Struct({
  id: Schema.String,
  wrapper: Schema.suspend(() =>
    Schema.Union([Schema.String, UnsupportedAmbiguousJsonDeclaredEmptyStruct]),
  ),
});

const UnsupportedAmbiguousJsonSuspendedRecordDeclaredEmptyStructPosition = Schema.Struct({
  id: Schema.String,
  wrapper: Schema.suspend(() =>
    Schema.Record(
      Schema.TemplateLiteral(["nested-", Schema.String]),
      UnsupportedAmbiguousJsonDeclaredEmptyStruct,
    ),
  ),
});

const UnsupportedAmbiguousJsonDeclaredClassAnnotationSpoof = Schema.declare<{}>(
  (value): value is {} => typeof value === "object" && value !== null,
  {
    "~effect/Schema/Class": true,
    toCodecJson: () => Schema.link<{}>()(Schema.Struct({}), SchemaTransformation.passthrough()),
  },
);

const UnsupportedAmbiguousJsonSuspendedClassAnnotationSpoofPosition = Schema.Struct({
  id: Schema.String,
  wrapper: Schema.suspend(() =>
    Schema.Struct({
      nested: UnsupportedAmbiguousJsonDeclaredClassAnnotationSpoof,
    }),
  ),
});

const unsupportedAmbiguousJsonEmptyObjectStringTransformation = SchemaTransformation.transform<
  {},
  string
>({
  decode: () => ({}),
  encode: () => "spoof",
});

const UnsupportedAmbiguousJsonDeclaredClassAnnotationStringParameterSpoof =
  Schema.declareConstructor<{}>()([Schema.String], () => (value) => Effect.succeed(Object(value)), {
    "~effect/Schema/Class": () =>
      Schema.link<{}>()(Schema.String, unsupportedAmbiguousJsonEmptyObjectStringTransformation),
    toCodec: () =>
      Schema.link<{}>()(Schema.String, unsupportedAmbiguousJsonEmptyObjectStringTransformation),
    toCodecJson: () => Schema.link<{}>()(Schema.Struct({}), SchemaTransformation.passthrough()),
  });

const UnsupportedAmbiguousJsonSuspendedClassAnnotationStringParameterSpoofPosition = Schema.Struct({
  id: Schema.String,
  wrapper: Schema.suspend(() =>
    Schema.Struct({
      nested: UnsupportedAmbiguousJsonDeclaredClassAnnotationStringParameterSpoof,
    }),
  ),
});

const UnsupportedAmbiguousJsonDeclaredFieldsSpoof = Schema.declare<{}>(
  (value): value is {} => typeof value === "object" && value !== null,
  {
    toCodecJson: () => Schema.link<{}>()(Schema.Struct({}), SchemaTransformation.passthrough()),
  },
);

Object.defineProperty(UnsupportedAmbiguousJsonDeclaredFieldsSpoof, "fields", {
  configurable: true,
  enumerable: true,
  value: {},
});

const UnsupportedAmbiguousJsonDeclaredFieldsSpoofPosition = Schema.Struct({
  id: Schema.String,
  declaredEmpty: UnsupportedAmbiguousJsonDeclaredFieldsSpoof,
});

const UnsupportedAmbiguousJsonDeclaredTransformFromEmptyStructEncoded = Schema.Struct({}).pipe(
  Schema.decodeTo(Schema.String, {
    decode: SchemaGetter.transform(() => "decoded"),
    encode: SchemaGetter.transform(() => ({})),
  }),
);

const UnsupportedAmbiguousJsonDeclaredTransformFromEmptyStruct = Schema.declare<
  typeof UnsupportedAmbiguousJsonDeclaredTransformFromEmptyStructEncoded.Type,
  typeof UnsupportedAmbiguousJsonDeclaredTransformFromEmptyStructEncoded.Encoded
>(
  (value): value is typeof UnsupportedAmbiguousJsonDeclaredTransformFromEmptyStructEncoded.Type =>
    typeof value === "string",
  {
    toCodecJson: () =>
      Schema.link<typeof UnsupportedAmbiguousJsonDeclaredTransformFromEmptyStructEncoded.Type>()(
        UnsupportedAmbiguousJsonDeclaredTransformFromEmptyStructEncoded,
        SchemaTransformation.passthrough(),
      ),
  },
);

const UnsupportedAmbiguousJsonDeclaredTransformFromEmptyStructPosition = Schema.Struct({
  id: Schema.String,
  declaredTransformed: UnsupportedAmbiguousJsonDeclaredTransformFromEmptyStruct,
});

const UnsupportedAmbiguousJsonDeclaredObjectTupleEncoded = Schema.Tuple([Schema.Struct({})]);

const UnsupportedAmbiguousJsonDeclaredObjectTuple = Schema.declare<
  typeof UnsupportedAmbiguousJsonDeclaredObjectTupleEncoded.Type,
  typeof UnsupportedAmbiguousJsonDeclaredObjectTupleEncoded.Encoded
>(
  (value): value is typeof UnsupportedAmbiguousJsonDeclaredObjectTupleEncoded.Type =>
    Array.isArray(value) && value.length === 1 && typeof value[0] === "object" && value[0] !== null,
  {
    toCodecJson: () =>
      Schema.link<typeof UnsupportedAmbiguousJsonDeclaredObjectTupleEncoded.Type>()(
        UnsupportedAmbiguousJsonDeclaredObjectTupleEncoded,
        SchemaTransformation.passthrough(),
      ),
  },
);

const UnsupportedAmbiguousJsonDeclaredObjectTuplePosition = Schema.Struct({
  id: Schema.String,
  declaredTuple: UnsupportedAmbiguousJsonDeclaredObjectTuple,
});

const UnsupportedAmbiguousJsonDeclaredObjectKeyword = Schema.declare<
  typeof Schema.ObjectKeyword.Type,
  typeof Schema.ObjectKeyword.Encoded
>((_value): _value is typeof Schema.ObjectKeyword.Type => true, {
  toCodecJson: () =>
    Schema.link<typeof Schema.ObjectKeyword.Type>()(
      Schema.ObjectKeyword,
      SchemaTransformation.passthrough(),
    ),
});

const UnsupportedAmbiguousJsonDeclaredObjectKeywordPosition = Schema.Struct({
  id: Schema.String,
  declaredObjectKeyword: UnsupportedAmbiguousJsonDeclaredObjectKeyword,
});

const UnsupportedAmbiguousJsonDeclaredJson = Schema.declare<
  typeof Schema.Json.Type,
  typeof Schema.Json.Encoded
>((_value): _value is typeof Schema.Json.Type => true, {
  toCodecJson: () =>
    Schema.link<typeof Schema.Json.Type>()(Schema.Json, SchemaTransformation.passthrough()),
});

const UnsupportedAmbiguousJsonDeclaredJsonPosition = Schema.Struct({
  id: Schema.String,
  declaredJson: UnsupportedAmbiguousJsonDeclaredJson,
});

const UnsupportedAmbiguousJsonDeclaredMutableJson = Schema.declare<
  typeof Schema.MutableJson.Type,
  typeof Schema.MutableJson.Encoded
>((_value): _value is typeof Schema.MutableJson.Type => true, {
  toCodecJson: () =>
    Schema.link<typeof Schema.MutableJson.Type>()(
      Schema.MutableJson,
      SchemaTransformation.passthrough(),
    ),
});

const UnsupportedAmbiguousJsonDeclaredMutableJsonPosition = Schema.Struct({
  id: Schema.String,
  declaredMutableJson: UnsupportedAmbiguousJsonDeclaredMutableJson,
});

const UnsupportedAmbiguousJsonDeclaredUnknown = Schema.declare<
  typeof Schema.Unknown.Type,
  typeof Schema.Unknown.Encoded
>((_value): _value is typeof Schema.Unknown.Type => true, {
  toCodecJson: () =>
    Schema.link<typeof Schema.Unknown.Type>()(Schema.Unknown, SchemaTransformation.passthrough()),
});

const UnsupportedAmbiguousJsonDeclaredUnknownPosition = Schema.Struct({
  id: Schema.String,
  declaredUnknown: UnsupportedAmbiguousJsonDeclaredUnknown,
});

const UnsupportedAmbiguousJsonDeclaredAny = Schema.declare<
  typeof Schema.Any.Type,
  typeof Schema.Any.Encoded
>((_value): _value is typeof Schema.Any.Type => true, {
  toCodecJson: () =>
    Schema.link<typeof Schema.Any.Type>()(Schema.Any, SchemaTransformation.passthrough()),
});

const UnsupportedAmbiguousJsonDeclaredAnyPosition = Schema.Struct({
  id: Schema.String,
  declaredAny: UnsupportedAmbiguousJsonDeclaredAny,
});

const InvalidAmbiguousJsonDeclaredRecordValue = Schema.declare<bigint>(
  (value): value is bigint => typeof value === "bigint",
);

const InvalidAmbiguousJsonDeclaredRecordValuePosition = Schema.Struct({
  id: Schema.String,
  declaredRecordValue: Schema.Record(Schema.String, InvalidAmbiguousJsonDeclaredRecordValue),
});

const InvalidAmbiguousJsonSuspendedStructDeclaredRecordValuePosition = Schema.Struct({
  id: Schema.String,
  wrapper: Schema.suspend(() =>
    Schema.Struct({
      declaredRecordValue: Schema.Record(Schema.String, InvalidAmbiguousJsonDeclaredRecordValue),
    }),
  ),
});

const InvalidAmbiguousJsonSuspendedRecordPosition = Schema.Struct({
  id: Schema.String,
  suspendedRecord: Schema.suspend(() => Schema.Record(Schema.Literal(1), Schema.BigInt)),
});

const InvalidAmbiguousJsonSuspendedStringRecordKey = Schema.Record(
  Schema.String,
  Schema.suspend(() => Schema.BigInt),
);

class AmbiguousJsonDecodedOnlyBadRecordKeyClassPosition extends Schema.Class<AmbiguousJsonDecodedOnlyBadRecordKeyClassPosition>(
  "AmbiguousJsonDecodedOnlyBadRecordKeyClassPosition",
)({
  id: Schema.String,
  decodedRecord: InvalidAmbiguousJsonSuspendedStringRecordKey,
}) {}

const AmbiguousJsonScalarToBadRecordKeyClassTarget = Schema.String.pipe(
  Schema.decodeTo(AmbiguousJsonDecodedOnlyBadRecordKeyClassPosition, {
    decode: SchemaGetter.transform(
      (id) =>
        new AmbiguousJsonDecodedOnlyBadRecordKeyClassPosition({
          id,
          decodedRecord: {
            account1: 1n,
          },
        }),
    ),
    encode: SchemaGetter.transform((value) => value.id),
  }),
);

const AmbiguousJsonSuspendedScalarToBadRecordKeyClassPosition = Schema.Struct({
  id: Schema.String,
  nested: Schema.suspend(() => AmbiguousJsonScalarToBadRecordKeyClassTarget),
});

const AmbiguousJsonDeclaredScalarToBadRecordKeyClass = Schema.declare<
  typeof AmbiguousJsonScalarToBadRecordKeyClassTarget.Type,
  typeof AmbiguousJsonScalarToBadRecordKeyClassTarget.Encoded
>((_value): _value is typeof AmbiguousJsonScalarToBadRecordKeyClassTarget.Type => true, {
  toCodecJson: () =>
    Schema.link<typeof AmbiguousJsonScalarToBadRecordKeyClassTarget.Type>()(
      AmbiguousJsonScalarToBadRecordKeyClassTarget,
      SchemaTransformation.passthrough(),
    ),
});

const AmbiguousJsonDeclaredScalarToBadRecordKeyClassPosition = Schema.Struct({
  id: Schema.String,
  declaredClass: AmbiguousJsonDeclaredScalarToBadRecordKeyClass,
});

const AmbiguousJsonDecodedSideSuspendedRecordKeyPosition = Schema.Struct({
  id: Schema.String,
  decodedRecord: Schema.String.pipe(
    Schema.decodeTo(InvalidAmbiguousJsonSuspendedStringRecordKey, {
      decode: SchemaGetter.transform(() => ({
        account1: 9007199254741041n,
      })),
      encode: SchemaGetter.transform(() => "encoded"),
    }),
  ),
});

const InvalidAmbiguousJsonDeclaredClassTarget = Schema.declare<
  typeof AmbiguousJsonClassPosition.Type,
  typeof AmbiguousJsonClassPosition.Encoded
>((_value): _value is typeof AmbiguousJsonClassPosition.Type => true, {
  toCodecJson: () =>
    Schema.link<typeof AmbiguousJsonClassPosition.Type>()(
      AmbiguousJsonClassPosition,
      SchemaTransformation.passthrough(),
    ),
});

const InvalidAmbiguousJsonDeclaredClassTargetPosition = Schema.Struct({
  id: Schema.String,
  declaredClass: InvalidAmbiguousJsonDeclaredClassTarget,
});

const InvalidAmbiguousJsonDeclaredStringOnlyClassTarget = Schema.declare<
  typeof AmbiguousJsonStringOnlyClassPosition.Type,
  typeof AmbiguousJsonStringOnlyClassPosition.Encoded
>((_value): _value is typeof AmbiguousJsonStringOnlyClassPosition.Type => true, {
  toCodecJson: () =>
    Schema.link<typeof AmbiguousJsonStringOnlyClassPosition.Type>()(
      AmbiguousJsonStringOnlyClassPosition,
      SchemaTransformation.passthrough(),
    ),
});

const InvalidAmbiguousJsonDeclaredStringOnlyClassTargetPosition = Schema.Struct({
  id: Schema.String,
  declaredClass: InvalidAmbiguousJsonDeclaredStringOnlyClassTarget,
});

const InvalidAmbiguousJsonCustomJsonCodecClassJson = Schema.Struct({
  id: Schema.String,
  suspendedRecord: Schema.Record(
    Schema.String,
    Schema.suspend(() => Schema.String),
  ),
});

class InvalidAmbiguousJsonCustomJsonCodecClassPosition extends Schema.Class<InvalidAmbiguousJsonCustomJsonCodecClassPosition>(
  "InvalidAmbiguousJsonCustomJsonCodecClassPosition",
)(
  {
    id: Schema.String,
  },
  {
    toCodecJson: () =>
      Schema.link<object>()(InvalidAmbiguousJsonCustomJsonCodecClassJson, {
        decode: SchemaGetter.transform((value) => ({
          id: value.id,
        })),
        encode: SchemaGetter.transform(() => ({
          id: "encoded",
          suspendedRecord: {
            account1: "1",
          },
        })),
      }),
  },
) {}

const InvalidAmbiguousJsonOneOfPosition = Schema.Struct({
  id: Schema.String,
  nestedOneOf: AmbiguousJsonOneOfPosition,
});

const InvalidAmbiguousJsonBroadOneOfPosition = Schema.Struct({
  id: Schema.String,
  broadOneOf: Schema.Union(
    [
      Schema.Struct({
        id: Schema.BigInt,
      }),
      Schema.Struct({
        kind: Schema.Literal("b"),
        id: Schema.BigDecimal,
      }),
    ],
    { mode: "oneOf" },
  ),
});

const InvalidAmbiguousJsonScalarOneOfPosition = Schema.Struct({
  id: Schema.String,
  scalarOneOf: Schema.Union([Schema.String, Schema.BigInt], { mode: "oneOf" }),
});

const OrderWithExtraSourceField = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
  ze: Schema.Boolean,
});

declare const decimal: (value: string) => BigDecimal.BigDecimal;

type OrdersValueMessage = Message<"viewserver.test.OrderValue"> & {
  readonly customerId: string;
  readonly status: "open" | "closed" | "cancelled";
  readonly price: number;
  readonly updatedAt: number;
};

type OrdersKeyMessage = Message<"viewserver.test.OrderKey"> & {
  readonly orderId: string;
};

type TradesValueMessage = Message<"viewserver.test.TradeValue"> & {
  readonly symbol: string;
  readonly quantity: number;
  readonly price: number;
};

type CustomKafkaCodecError = {
  readonly _tag: "CustomKafkaCodecError";
  readonly message: string;
};

const base64FromBytes = (bytes: Uint8Array) =>
  globalThis.btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(""));

const textEncoder = new TextEncoder();

const kafkaJsonBytes = Effect.fn("ViewServerConfig.test.kafka.json.bytes")(function* <
  const SourceSchema extends RowSchema,
>(schema: SourceSchema, value: SourceSchema["Type"]) {
  const encoded = yield* Schema.encodeUnknownEffect(Schema.toCodecJson(schema))(value);
  return textEncoder.encode(JSON.stringify(encoded));
});

const kafkaTestMetadata = <const Region extends "usa" | "london">(
  region: Region,
): KafkaMessageMetadata<Region> => ({
  sourceTopic: "orders-source",
  sourceRegion: region,
  partition: 0,
  offset: "1",
  timestamp: null,
  headers: {},
});

const testProtoFile = fileDesc(
  base64FromBytes(
    toBinary(
      FileDescriptorProtoSchema,
      create(FileDescriptorProtoSchema, {
        name: "viewserver/test.proto",
        package: "viewserver.test",
        syntax: "proto3",
        messageType: [
          {
            name: "OrderValue",
            field: [
              { name: "customer_id", number: 1, type: FieldDescriptorProto_Type.STRING },
              { name: "status", number: 2, type: FieldDescriptorProto_Type.STRING },
              { name: "price", number: 3, type: FieldDescriptorProto_Type.DOUBLE },
              { name: "updated_at", number: 4, type: FieldDescriptorProto_Type.DOUBLE },
            ],
          },
          {
            name: "OrderKey",
            field: [{ name: "order_id", number: 1, type: FieldDescriptorProto_Type.STRING }],
          },
          {
            name: "TradeValue",
            field: [
              { name: "symbol", number: 1, type: FieldDescriptorProto_Type.STRING },
              { name: "quantity", number: 2, type: FieldDescriptorProto_Type.DOUBLE },
              { name: "price", number: 3, type: FieldDescriptorProto_Type.DOUBLE },
            ],
          },
        ],
        service: [
          {
            name: "OrdersService",
            method: [
              {
                name: "StreamOrders",
                inputType: ".viewserver.test.OrderKey",
                outputType: ".viewserver.test.OrderValue",
                serverStreaming: true,
              },
              {
                name: "StreamTrades",
                inputType: ".viewserver.test.OrderKey",
                outputType: ".viewserver.test.TradeValue",
                serverStreaming: true,
              },
              {
                name: "GetOrder",
                inputType: ".viewserver.test.OrderKey",
                outputType: ".viewserver.test.OrderValue",
              },
            ],
          },
        ],
      }),
    ),
  ),
);

const ordersValueSchema = messageDesc<OrdersValueMessage>(testProtoFile, 0);
const ordersKeySchema = messageDesc<OrdersKeyMessage>(testProtoFile, 1);
const tradesValueSchema = messageDesc<TradesValueMessage>(testProtoFile, 2);
const ordersService = serviceDesc<{
  readonly streamOrders: {
    readonly input: typeof ordersKeySchema;
    readonly output: typeof ordersValueSchema;
    readonly methodKind: "server_streaming";
  };
  readonly streamTrades: {
    readonly input: typeof ordersKeySchema;
    readonly output: typeof tradesValueSchema;
    readonly methodKind: "server_streaming";
  };
  readonly getOrder: {
    readonly input: typeof ordersKeySchema;
    readonly output: typeof ordersValueSchema;
    readonly methodKind: "unary";
  };
}>(testProtoFile, 0);
const tradesOnlyService = serviceDesc<{
  readonly streamTrades: {
    readonly input: typeof ordersKeySchema;
    readonly output: typeof tradesValueSchema;
    readonly methodKind: "server_streaming";
  };
}>(testProtoFile, 0);

declare const generatedOrdersValueSchema: GenMessage<
  Message<"viewserver.test.OrderValue"> & {
    readonly customerId: string;
    readonly status: "open" | "closed" | "cancelled";
    readonly price: number;
    readonly updatedAt: number;
  }
>;
declare const generatedOrdersKeySchema: GenMessage<
  Message<"viewserver.test.OrderKey"> & {
    readonly orderId: string;
  }
>;

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
    trades: {
      schema: Trade,
      key: "id",
    },
    positions: {
      schema: Position,
      key: "id",
    },
  },
});

const runtimeTopicHealth = (
  status: TopicRuntimeHealth["status"],
  rowCount: number,
): TopicRuntimeHealth => ({
  status,
  rowCount,
  liveRowCount: rowCount,
  deletedRowCount: 0,
  version: rowCount,
  lastMutationAt: null,
  mutationsPerSecond: rowCount,
  rowsPerSecond: rowCount,
  pendingMutationBatches: 0,
  activeFallbackGroupedViews: 0,
  activeIncrementalGroupedViews: 0,
  activeViews: 0,
  groupedFullEvaluationCount: 0,
  groupedPatchedEvaluationCount: 0,
  activeSubscriptions: 0,
  queuedEvents: 0,
  maxQueueDepth: 0,
  backpressureEvents: 0,
  memoryBytes: 0,
  tombstoneCount: 0,
  compactionPending: false,
});

const kafkaStartFromHealth = {
  consumerGroupId: "view-server-test",
  fallbackMode: "earliest",
  mode: "committed",
} as const;

const kafkaLatestStartFromHealth = {
  consumerGroupId: "view-server-latest",
  fallbackMode: "latest",
  mode: "latest",
} satisfies KafkaStartFromHealth;

const kafkaEarliestStartFromHealth = {
  consumerGroupId: "view-server-earliest",
  fallbackMode: "earliest",
  mode: "earliest",
} satisfies KafkaStartFromHealth;

const kafkaCommittedFailStartFromHealth = {
  consumerGroupId: "view-server-committed",
  fallbackMode: "fail",
  mode: "committed",
} satisfies KafkaStartFromHealth;

describe("Kafka health start policy", () => {
  it("types only normalized start policy combinations", () => {
    expectTypeOf<{
      readonly consumerGroupId: "view-server-invalid-latest-fail";
      readonly fallbackMode: "fail";
      readonly mode: "latest";
    }>().not.toMatchTypeOf<KafkaStartFromHealth>();
    expect(kafkaLatestStartFromHealth).toStrictEqual({
      consumerGroupId: "view-server-latest",
      fallbackMode: "latest",
      mode: "latest",
    });
    expect(kafkaEarliestStartFromHealth).toStrictEqual({
      consumerGroupId: "view-server-earliest",
      fallbackMode: "earliest",
      mode: "earliest",
    });
    expect(kafkaCommittedFailStartFromHealth).toStrictEqual({
      consumerGroupId: "view-server-committed",
      fallbackMode: "fail",
      mode: "committed",
    });
  });
});

type LiveQueryCall<Topics extends object> = {
  <
    Topic extends Extract<keyof Topics, string>,
    const Query extends GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  ): LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>;
  <
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  ): LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>;
};

const kafkaRegions = {
  usa: runtimeConfig.kafkaBootstrapServers("VIEW_SERVER_KAFKA_USA_BOOTSTRAP_SERVERS"),
  london: runtimeConfig.kafkaBootstrapServers("VIEW_SERVER_KAFKA_LONDON_BOOTSTRAP_SERVERS"),
};

const kafkaTopic = viewServer.kafkaTopic<typeof kafkaRegions>();
const ordersValueKafkaCodec = kafka.protobuf(ordersValueSchema);
const ordersKeyKafkaCodec = kafka.protobuf(ordersKeySchema);
const tradesValueKafkaCodec = kafka.protobuf(tradesValueSchema);

describe("defineViewServerConfig", () => {
  it("types gRPC leased topic route metadata", () => {
    const grpcViewServer = defineViewServerConfig({
      topics: {
        orders: {
          schema: Order,
          key: "id",
          source: grpc.leased({
            routeBy: ["region", "status"],
          }),
        },
        trades: {
          schema: Trade,
          key: "id",
          source: grpc.materialized(),
        },
        positions: {
          schema: Position,
          key: "id",
        },
      },
    });
    expectTypeOf<TopicRouteBy<typeof grpcViewServer.topics, "orders">>().toEqualTypeOf<
      "region" | "status"
    >();
    expectTypeOf<TopicRouteBy<typeof grpcViewServer.topics, "trades">>().toEqualTypeOf<never>();

    defineViewServerConfig({
      topics: {
        orders: {
          schema: Order,
          key: "id",
          // @ts-expect-error routeBy fields must exist on the target topic row.
          source: grpc.leased({
            routeBy: ["strategyId"],
          }),
        },
      },
    });
  });

  it("requires exact equality predicates for leased gRPC route fields", () => {
    const grpcViewServer = defineViewServerConfig({
      topics: {
        orders: {
          schema: Order,
          key: "id",
          source: grpc.leased({
            routeBy: ["region", "status"],
          }),
        },
      },
    });
    const grpcRouteValidationViewServer = defineViewServerConfig({
      topics: {
        orders: {
          schema: Order,
          key: "id",
          source: grpc.leased({
            routeBy: ["region", "status"],
          }),
        },
        trades: {
          schema: Trade,
          key: "id",
          source: grpc.materialized(),
        },
        positions: {
          schema: Position,
          key: "id",
        },
      },
    });
    const assertGrpcRouteQueryTypes = (
      useLiveQuery: LiveQueryCall<typeof grpcViewServer.topics>,
    ) => {
      const validRouteQuery = useLiveQuery("orders", {
        where: {
          region: { eq: "usa" },
          status: { eq: "open" },
          price: { gte: 10 },
        },
        orderBy: [{ field: "updatedAt", direction: "desc" }],
        select: ["id", "price", "updatedAt"],
        limit: 50,
      });

      expectTypeOf(validRouteQuery).toEqualTypeOf<
        LiveQueryResult<{
          readonly id: string;
          readonly price: number;
          readonly updatedAt: number;
        }>
      >();

      const missingRouteFieldQuery = {
        where: {
          region: { eq: "usa" },
        },
        select: ["id"],
      } satisfies {
        readonly where: {
          readonly region: {
            readonly eq: "usa";
          };
        };
        readonly select: readonly ["id"];
      };
      // @ts-expect-error leased gRPC topics require every routeBy field.
      useLiveQuery("orders", missingRouteFieldQuery);

      const routeInOperatorQuery = {
        where: {
          region: { eq: "usa" },
          status: { in: ["open", "closed"] },
        },
        select: ["id"],
      } satisfies {
        readonly where: {
          readonly region: {
            readonly eq: "usa";
          };
          readonly status: {
            readonly in: readonly ["open", "closed"];
          };
        };
        readonly select: readonly ["id"];
      };
      // @ts-expect-error leased gRPC route filters must be exact eq predicates.
      useLiveQuery("orders", routeInOperatorQuery);

      const routeShorthandQuery = {
        where: {
          region: "usa",
          status: { eq: "open" },
        },
        select: ["id"],
      } satisfies {
        readonly where: {
          readonly region: "usa";
          readonly status: {
            readonly eq: "open";
          };
        };
        readonly select: readonly ["id"];
      };
      // @ts-expect-error leased gRPC route filters must not use shorthand equality.
      useLiveQuery("orders", routeShorthandQuery);

      const routeExtraOperatorQuery = {
        where: {
          region: {
            eq: "usa",
            neq: "london",
          },
          status: { eq: "open" },
        },
        select: ["id"],
      } satisfies {
        readonly where: {
          readonly region: {
            readonly eq: "usa";
            readonly neq: "london";
          };
          readonly status: {
            readonly eq: "open";
          };
        };
        readonly select: readonly ["id"];
      };
      // @ts-expect-error leased gRPC route filters must not include extra operators.
      useLiveQuery("orders", routeExtraOperatorQuery);
    };
    expectTypeOf(assertGrpcRouteQueryTypes).toBeFunction();
    expect(validateLiveQuerySourceRoute(grpcViewServer.topics, "missing", {})).toBeUndefined();
    expect(
      validateLiveQuerySourceRoute(grpcRouteValidationViewServer.topics, "positions", {}),
    ).toBeUndefined();
    expect(
      validateLiveQuerySourceRoute(grpcRouteValidationViewServer.topics, "trades", {}),
    ).toBeUndefined();
    expect(validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", null)).toBe(
      "Leased topic orders requires a query object.",
    );
    expect(validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", {})).toBe(
      "Leased topic orders requires exact equality filters for route fields: region, status.",
    );
    expect(
      validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", {
        where: {
          region: { eq: "usa" },
        },
      }),
    ).toBe("Leased topic orders route field status must use an exact eq filter.");
    expect(
      validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", {
        where: {
          region: { eq: "usa" },
          status: { eq: "open", neq: "closed" },
        },
      }),
    ).toBe("Leased topic orders route field status must use an exact eq filter.");
    expect(
      validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", {
        where: {
          region: { eq: "usa" },
          status: { neq: "closed" },
        },
      }),
    ).toBe("Leased topic orders route field status must use an exact eq filter.");
    expect(
      validateLiveQuerySourceRoute(grpcViewServer.topics, "orders", {
        where: {
          region: { eq: "usa" },
          status: { eq: "open" },
        },
      }),
    ).toBeUndefined();
    expect(
      validateLiveQuerySourceRoute(
        {
          malformed: {
            schema: Order,
            key: "id",
            source: { kind: "grpc", lifecycle: "leased", routeBy: [] },
          },
        },
        "malformed",
        {},
      ),
    ).toBe("Leased topic malformed has invalid route metadata.");
    expect(
      validateLiveQuerySourceRoute(
        {
          malformed: {
            schema: Order,
            key: "id",
            source: { kind: "grpc", lifecycle: "leased", routeBy: ["region", 1] },
          },
        },
        "malformed",
        {},
      ),
    ).toBe("Leased topic malformed has invalid route metadata.");
  });

  it("types gRPC clients and feed mapping contracts", () => {
    const grpcViewServer = defineViewServerConfig({
      topics: {
        orders: {
          schema: Order,
          key: "id",
          source: grpc.leased({
            routeBy: ["region", "status"],
          }),
        },
        trades: {
          schema: Trade,
          key: "id",
          source: grpc.materialized(),
        },
        positions: {
          schema: Position,
          key: "id",
        },
      },
    });
    const clients = {
      orders: grpc.connectClient({
        service: ordersService,
        baseUrl: "https://orders.example.test",
      }),
      trades: grpc.connectClient({
        service: tradesOnlyService,
        baseUrl: "https://trades.example.test",
      }),
    };
    const standaloneFeed = defineGrpcFeed<typeof grpcViewServer.topics, typeof clients>();
    const feed = grpcViewServer.grpcFeed<typeof clients>();
    expectTypeOf(standaloneFeed.leasedFeed).toBeFunction();
    const leasedOrders = feed.leasedFeed({
      topic: "orders",
      client: "orders",
      method: "streamOrders",
      routeBy: ["region", "status"],
      request: ({ region, status }) => {
        expectTypeOf(region).toEqualTypeOf<string>();
        expectTypeOf(status).toEqualTypeOf<"open" | "closed" | "cancelled">();
        return { orderId: `${region}:${status}` };
      },
      acquire: ({ client, request, route, session }) => {
        expectTypeOf(client).toEqualTypeOf<GrpcClientValue<(typeof clients)["orders"]>>();
        expectTypeOf(client.streamOrders).toBeFunction();
        expectTypeOf(request.orderId).toEqualTypeOf<string | undefined>();
        expectTypeOf(route).toEqualTypeOf<{
          readonly region: string;
          readonly status: "open" | "closed" | "cancelled";
        }>();
        expectTypeOf(session.forwardedHeaders).toEqualTypeOf<Readonly<Record<string, string>>>();
        return Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
          status: "open",
          price: 10,
          updatedAt: 1,
        });
      },
      release: ({ client, request, route, session }) => {
        expectTypeOf(client).toEqualTypeOf<GrpcClientValue<(typeof clients)["orders"]>>();
        expectTypeOf(request.orderId).toEqualTypeOf<string | undefined>();
        expectTypeOf(route).toEqualTypeOf<{
          readonly region: string;
          readonly status: "open" | "closed" | "cancelled";
        }>();
        expectTypeOf(session.systemHeaders).toEqualTypeOf<Readonly<Record<string, string>>>();
        return Effect.void;
      },
      map: ({ value, route, schema }) => {
        expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
        expectTypeOf(route.region).toEqualTypeOf<string>();
        expectTypeOf(schema).toEqualTypeOf<typeof Order>();
        return {
          id: `${route.region}:${value.customerId}`,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region: route.region,
          updatedAt: value.updatedAt,
        };
      },
    });
    const materializedTrades = feed.materializedFeed({
      topic: "trades",
      client: "orders",
      method: "streamTrades",
      request: () => ({ orderId: "all-trades" }),
      acquire: ({ client, route }) => {
        expectTypeOf(client).toEqualTypeOf<GrpcClientValue<(typeof clients)["orders"]>>();
        expectTypeOf(route).toEqualTypeOf<undefined>();
        return Stream.make({
          $typeName: "viewserver.test.TradeValue",
          symbol: "AAPL",
          quantity: 1,
          price: 10,
        });
      },
      release: ({ request, route }) => {
        expectTypeOf(request.orderId).toEqualTypeOf<string | undefined>();
        expectTypeOf(route).toEqualTypeOf<undefined>();
        return Effect.void;
      },
      map: ({ value }) => ({
        id: value.symbol,
        symbol: value.symbol,
        quantity: value.quantity,
        price: value.price,
        region: "usa",
      }),
    });

    expect(leasedOrders.lifecycle).toBe("leased");
    expect(materializedTrades.lifecycle).toBe("materialized");
    expectTypeOf(ordersService).toEqualTypeOf<
      GenService<{
        readonly streamOrders: {
          readonly input: typeof ordersKeySchema;
          readonly output: typeof ordersValueSchema;
          readonly methodKind: "server_streaming";
        };
        readonly streamTrades: {
          readonly input: typeof ordersKeySchema;
          readonly output: typeof tradesValueSchema;
          readonly methodKind: "server_streaming";
        };
        readonly getOrder: {
          readonly input: typeof ordersKeySchema;
          readonly output: typeof ordersValueSchema;
          readonly methodKind: "unary";
        };
      }>
    >();

    const openOrderStatus = "open" as const;

    const invalidManualRuntimeFeedDefinition: typeof leasedOrders = {
      _tag: "GrpcLeasedFeedDefinition",
      lifecycle: "leased",
      topic: "orders",
      client: "orders",
      method: "streamOrders",
      routeBy: ["region", "status"],
      request: ({ region, status }) => ({ orderId: `${region}:${status}` }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
          status: "open",
          price: 10,
          updatedAt: 1,
        }),
      // @ts-expect-error runtime feed definitions require helper-branded maps, so manual objects cannot bypass map exactness.
      map: () => ({
        id: "order-1",
        customerId: "customer-1",
        status: openOrderStatus,
        price: 10,
        region: "usa",
        updatedAt: 1,
        ze: true,
      }),
    };
    expectTypeOf(invalidManualRuntimeFeedDefinition).not.toBeNever();

    const invalidSpreadRuntimeFeedDefinition: typeof leasedOrders = {
      ...leasedOrders,
      // @ts-expect-error spread feeds cannot replace helper-branded exact maps with a broader function.
      map: () => ({
        id: "order-1",
        customerId: "customer-1",
        status: openOrderStatus,
        price: 10,
        region: "usa",
        updatedAt: 1,
        ze: true,
      }),
    };
    expectTypeOf(invalidSpreadRuntimeFeedDefinition).not.toBeNever();

    // @ts-expect-error spread-mutated materialized feeds must preserve client/method/request/acquire/map correlation.
    const invalidMaterializedFeedClientMutation: GrpcFeedDefinition<
      typeof grpcViewServer.topics,
      typeof clients
    > = {
      ...materializedTrades,
      client: "trades",
    };
    expectTypeOf(invalidMaterializedFeedClientMutation).not.toBeNever();

    // @ts-expect-error spread-mutated leased feeds must preserve method/request/acquire/map correlation.
    const invalidLeasedFeedMethodMutation: GrpcFeedDefinition<
      typeof grpcViewServer.topics,
      typeof clients
    > = {
      ...leasedOrders,
      method: "streamTrades",
    };
    expectTypeOf(invalidLeasedFeedMethodMutation).not.toBeNever();

    feed.materializedFeed({
      topic: "trades",
      client: "trades",
      // @ts-expect-error gRPC feed methods must belong to the selected client.
      method: "streamOrders",
      request: () => ({ orderId: "all-trades" }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.TradeValue",
          symbol: "AAPL",
          quantity: 1,
          price: 10,
        }),
      map: ({ value }) => ({
        id: value.symbol,
        symbol: value.symbol,
        quantity: value.quantity,
        price: value.price,
        region: "usa",
      }),
    });

    feed.materializedFeed({
      // @ts-expect-error materialized feeds can only target topics declared with grpc.materialized().
      topic: "orders",
      client: "orders",
      method: "streamTrades",
      request: () => ({ orderId: "all-trades" }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.TradeValue",
          symbol: "AAPL",
          quantity: 1,
          price: 10,
        }),
      map: ({ value }) => ({
        id: value.symbol,
        symbol: value.symbol,
        quantity: value.quantity,
        price: value.price,
        region: "usa",
      }),
    });

    feed.materializedFeed({
      // @ts-expect-error materialized feeds can only target topics explicitly marked as gRPC materialized.
      topic: "positions",
      client: "orders",
      method: "streamTrades",
      request: () => ({ orderId: "positions" }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.TradeValue",
          symbol: "AAPL",
          quantity: 1,
          price: 10,
        }),
      map: ({ value }) => ({
        id: value.symbol,
        symbol: value.symbol,
        quantity: value.quantity,
        price: value.price,
        region: "usa",
      }),
    });

    feed.leasedFeed({
      topic: "orders",
      client: "orders",
      // @ts-expect-error gRPC feeds must use server-streaming methods.
      method: "getOrder",
      routeBy: ["region", "status"],
      request: ({ region, status }) => ({ orderId: `${region}:${status}` }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
          status: "open",
          price: 10,
          updatedAt: 1,
        }),
      map: () => ({
        id: "order-1",
        customerId: "customer-1",
        status: openOrderStatus,
        price: 10,
        region: "usa",
        updatedAt: 1,
      }),
    });

    feed.leasedFeed({
      topic: "orders",
      client: "orders",
      method: "streamOrders",
      // @ts-expect-error leased feed routeBy must match the configured topic route tuple.
      routeBy: ["region"],
      request: ({ region }) => ({ orderId: region }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
          status: "open",
          price: 10,
          updatedAt: 1,
        }),
      map: ({ value }) => ({
        id: value.customerId,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: "usa",
        updatedAt: value.updatedAt,
      }),
    });

    feed.leasedFeed({
      topic: "orders",
      client: "orders",
      method: "streamOrders",
      routeBy: ["region", "status"],
      request: ({ region, status }) => ({ orderId: `${region}:${status}` }),
      // @ts-expect-error leased feed acquire callbacks must accept every configured route value.
      acquire: (input: {
        readonly route: {
          readonly region: "usa";
          readonly status: "open";
        };
      }) =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: input.route.region,
          status: input.route.status,
          price: 10,
          updatedAt: 1,
        }),
      map: ({ value }) => ({
        id: value.customerId,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: "usa",
        updatedAt: value.updatedAt,
      }),
    });

    feed.leasedFeed({
      topic: "orders",
      client: "orders",
      method: "streamOrders",
      routeBy: ["region", "status"],
      request: ({ region, status }) => ({ orderId: `${region}:${status}` }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
          status: "open",
          price: 10,
          updatedAt: 1,
        }),
      // @ts-expect-error leased feed release callbacks must accept every configured route value.
      release: (input: {
        readonly route: {
          readonly region: "usa";
          readonly status: "open";
        };
      }) => Effect.logDebug(input.route.region),
      map: ({ value }) => ({
        id: value.customerId,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: "usa",
        updatedAt: value.updatedAt,
      }),
    });

    feed.leasedFeed({
      topic: "orders",
      client: "orders",
      method: "streamOrders",
      routeBy: ["region", "status"],
      request: ({ region, status }) => ({ orderId: `${region}:${status}` }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
          status: "open",
          price: 10,
          updatedAt: 1,
        }),
      // @ts-expect-error gRPC feed mappings must not return fields outside the topic schema.
      map: ({ value }) => ({
        id: value.customerId,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: "usa",
        updatedAt: value.updatedAt,
        ze: true,
      }),
    });

    feed.leasedFeed({
      topic: "orders",
      client: "orders",
      method: "streamOrders",
      routeBy: ["region", "status"],
      request: ({ region, status }) => ({ orderId: `${region}:${status}` }),
      acquire: () =>
        Stream.make({
          $typeName: "viewserver.test.OrderValue",
          customerId: "customer-1",
          status: "open",
          price: 10,
          updatedAt: 1,
        }),
      // @ts-expect-error gRPC feed mappings must return every topic field.
      map: ({ value }) => ({
        id: value.customerId,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: "usa",
      }),
    });

    const grpcOwnedKafkaTopic = grpcViewServer.kafkaTopic<typeof kafkaRegions>();
    expect(() =>
      grpcOwnedKafkaTopic({
        regions: ["usa"],
        value: tradesValueKafkaCodec,
        key: kafka.stringKey(),
        // @ts-expect-error Kafka runtime topics cannot publish into gRPC-owned leased topics.
        viewServerTopic: "orders",
        mapping: ({ key, value }) => ({
          id: key,
          accountId: "account-1",
          symbol: value.symbol,
          active: true,
          quantity: BigInt(value.quantity),
          optionalQuantity: undefined,
          price: BigDecimal.fromStringUnsafe(String(value.price)),
          notional: value.price * value.quantity,
          optionalNotional: undefined,
        }),
      }),
    ).toThrow("Kafka source cannot publish into gRPC-owned View Server topic: orders");
    expect(() =>
      grpcOwnedKafkaTopic({
        regions: ["usa"],
        value: tradesValueKafkaCodec,
        key: kafka.stringKey(),
        // @ts-expect-error Kafka runtime topics cannot publish into gRPC-owned materialized topics.
        viewServerTopic: "trades",
        mapping: ({ key, value }) => ({
          id: key,
          accountId: "account-1",
          symbol: value.symbol,
          active: true,
          quantity: BigInt(value.quantity),
          optionalQuantity: undefined,
          price: BigDecimal.fromStringUnsafe(String(value.price)),
          notional: value.price * value.quantity,
          optionalNotional: undefined,
        }),
      }),
    ).toThrow("Kafka source cannot publish into gRPC-owned View Server topic: trades");
    expect(() =>
      grpcOwnedKafkaTopic({
        regions: ["usa"],
        value: tradesValueKafkaCodec,
        key: kafka.stringKey(),
        // @ts-expect-error hostile JS callers can still target missing View Server topics.
        viewServerTopic: "missing",
        mapping: ({ key, value }) => ({
          id: key,
          accountId: "account-1",
          symbol: value.symbol,
          active: true,
          quantity: BigInt(value.quantity),
          optionalQuantity: undefined,
          price: BigDecimal.fromStringUnsafe(String(value.price)),
          notional: value.price * value.quantity,
          optionalNotional: undefined,
        }),
      }),
    ).toThrow();
    const malformedSourceKafkaTopic = defineKafkaTopic({
      malformed: {
        schema: Position,
        source: { kind: 1 },
      },
    })<typeof kafkaRegions>();
    const malformedSourceTopic = malformedSourceKafkaTopic({
      regions: ["usa"],
      value: tradesValueKafkaCodec,
      key: kafka.stringKey(),
      viewServerTopic: "malformed",
      mapping: ({ key, value }) => ({
        id: key,
        accountId: "account-1",
        symbol: value.symbol,
        active: true,
        quantity: BigInt(value.quantity),
        optionalQuantity: undefined,
        price: BigDecimal.fromStringUnsafe(String(value.price)),
        notional: value.price * value.quantity,
        optionalNotional: undefined,
      }),
    });
    expect(malformedSourceTopic.viewServerTopic).toBe("malformed");
    const kafkaPositionTopic = grpcOwnedKafkaTopic({
      regions: ["usa"],
      value: tradesValueKafkaCodec,
      key: kafka.stringKey(),
      viewServerTopic: "positions",
      mapping: ({ key, value }) => ({
        id: key,
        accountId: "account-1",
        symbol: value.symbol,
        active: true,
        quantity: BigInt(value.quantity),
        optionalQuantity: undefined,
        price: BigDecimal.fromStringUnsafe(String(value.price)),
        notional: value.price * value.quantity,
        optionalNotional: undefined,
      }),
    });
    expect(kafkaPositionTopic.viewServerTopic).toBe("positions");
  });

  it("derives schema field metadata for query validation", () => {
    expect(viewServerSchemaFieldMetadata(Schema.Number)).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigDecimal",
    });
    expect(viewServerSchemaFieldMetadata(Schema.BigInt)).toStrictEqual({
      isNumeric: true,
      isPureBigInt: true,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigint",
    });
    expect(viewServerSchemaFieldMetadata(Schema.BigDecimal)).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigDecimal",
    });
    expect(
      viewServerSchemaFieldMetadata(Schema.Union([Schema.BigInt, Schema.BigInt])),
    ).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigint",
    });
    expect(
      viewServerSchemaFieldMetadata(Schema.Union([Schema.BigInt, Schema.Number])),
    ).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigDecimal",
    });
    expect(viewServerSchemaFieldMetadata(Schema.Literal(1))).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigDecimal",
    });
    expect(viewServerSchemaFieldMetadata(Schema.Literals([1, 2]))).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigDecimal",
    });
    expect(viewServerSchemaFieldMetadata(Schema.Literal(1n))).toStrictEqual({
      isNumeric: true,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
      sumResultKind: "bigint",
    });
    expect(
      viewServerSchemaFieldMetadata(Schema.Union([Schema.Number, Schema.Undefined])),
    ).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(
      viewServerSchemaFieldMetadata(Schema.Union([Schema.BigInt, Schema.Undefined])),
    ).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata(Schema.Undefined)).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata(Schema.Union([Schema.Undefined]))).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata(Schema.Union([]))).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata(undefined)).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata("not-a-schema")).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata({ ast: "not-an-effect-ast" })).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(viewServerSchemaFieldMetadata(Schema.Literals(["open", "closed"]))).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: true,
      isStructured: false,
      isStructuredObject: false,
    });
    expect(
      viewServerSchemaFieldMetadata(
        Schema.Union([
          Schema.Struct({ id: Schema.String }),
          Schema.Struct({ id: Schema.String, name: Schema.String }),
        ]),
      ),
    ).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: true,
      isStructuredObject: true,
    });
    expect(viewServerSchemaFieldMetadata(Schema.Struct({ id: Schema.String }))).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: true,
      isStructuredObject: true,
    });
    expect(viewServerSchemaFieldMetadata(Schema.Array(Schema.String))).toStrictEqual({
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: true,
      isStructuredObject: false,
    });
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Date)).toBe("Date");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.DateTimeUtc)).toBe("DateTimeUtc");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.DateTimeUtcFromString)).toBe(
      "DateTimeUtc",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.DateTimeZoned)).toBe("DateTimeZoned");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.DateTimeZonedFromString)).toBe(
      "DateTimeZoned",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Duration)).toBe("Duration");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Error())).toBe("Error");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Error({ includeStack: true }))).toBe(
      "ErrorWithStack",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Error({ excludeCause: true }))).toBe(
      "ErrorWithoutCause",
    );
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Error({ includeStack: true, excludeCause: true }),
      ),
    ).toBe("ErrorWithStackWithoutCause");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Symbol)).toBe("Symbol");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.TimeZone)).toBe("TimeZone");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.TimeZoneFromString)).toBe("TimeZone");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.TimeZoneNamed)).toBe("TimeZoneNamed");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.TimeZoneNamedFromString)).toBe(
      "TimeZoneNamed",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.TimeZoneOffset)).toBe("TimeZoneOffset");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Trim)).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Uint8Array)).toBe("Uint8Array");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Uint8ArrayFromBase64)).toBe("Uint8Array");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Uint8ArrayFromBase64Url)).toBe(
      "Uint8Array",
    );
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Uint8ArrayFromHex)).toBe("Uint8Array");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.suspend(() => Schema.Date))).toBe("Date");
    const recursiveSuspend: Schema.Schema<string> = Schema.suspend(() => recursiveSuspend);
    expect(viewServerUnsupportedRuntimeFieldDomain(recursiveSuspend)).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.String.pipe(
          Schema.decodeTo(Schema.Duration, {
            decode: SchemaGetter.transform(() => Duration.millis(1)),
            encode: SchemaGetter.transform(() => "1"),
          }),
        ),
      ),
    ).toBe("Duration");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.String.pipe(
          Schema.encodeTo(Schema.Duration, {
            decode: SchemaGetter.transform(() => "1"),
            encode: SchemaGetter.transform(() => Duration.millis(1)),
          }),
        ),
      ),
    ).toBe("Duration");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Array(Schema.RegExp))).toBe("RegExp");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Tuple([Schema.RegExp]))).toBe("RegExp");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.TupleWithRest(Schema.Tuple([Schema.String]), [Schema.Date]),
      ),
    ).toBe("Date");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Record(Schema.Symbol, Schema.String)),
    ).toBe("Symbol");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Record(Schema.String, Schema.String)),
    ).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Record(Schema.String, Schema.URL))).toBe(
      "URL",
    );
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Union([Schema.BigInt, Schema.Number])),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Union([Schema.BigDecimal, Schema.Number])),
    ).toBe("mixed numeric domain: bigDecimal, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.Literal(1), Schema.Literal(2n)]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(Schema.Union([Schema.String, Schema.File])),
    ).toBe("File");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.String.pipe(
          Schema.decodeTo(Schema.Union([Schema.Number, Schema.BigInt]), {
            decode: SchemaGetter.transform(() => 1n),
            encode: SchemaGetter.transform(() => "1"),
          }),
        ),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.String.pipe(
          Schema.encodeTo(Schema.Union([Schema.Number, Schema.BigInt]), {
            decode: SchemaGetter.transform(() => "1"),
            encode: SchemaGetter.transform(() => 1n),
          }),
        ),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Struct({
          nested: Schema.Struct({
            href: Schema.URL,
          }),
        }),
      ),
    ).toBe("URL");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Option(Schema.Date))).toBe("Date");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Option(Schema.Union([Schema.Number, Schema.BigInt])),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Option(Schema.String))).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Redacted(Schema.Duration))).toBe(
      "Duration",
    );
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Struct({
          nested: Schema.Struct({
            amount: Schema.Union([Schema.Number, Schema.BigInt]),
          }),
        }),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Record(Schema.String, Schema.Union([Schema.Number, Schema.BigInt])),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Tuple([Schema.Union([Schema.Number, Schema.BigInt])]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.TupleWithRest(Schema.Tuple([Schema.String]), [
          Schema.Union([Schema.Number, Schema.BigInt]),
        ]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.String,
          Schema.Struct({
            amount: Schema.Union([Schema.Number, Schema.BigInt]),
          }),
        ]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.Array(Schema.Number), Schema.Array(Schema.BigInt)]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.Tuple([Schema.Number]), Schema.Tuple([Schema.BigInt])]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.Array(Schema.Number), Schema.Tuple([Schema.BigInt])]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.Struct({
            amount: Schema.Number,
          }),
          Schema.Struct({
            amount: Schema.BigInt,
          }),
        ]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.Struct({
            payload: Schema.Array(Schema.Number),
          }),
          Schema.Struct({
            payload: Schema.Tuple([Schema.BigInt]),
          }),
        ]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.Record(Schema.String, Schema.Number),
          Schema.Struct({
            amount: Schema.BigInt,
          }),
        ]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.Struct({
            amount: Schema.Number,
          }),
          Schema.Record(Schema.String, Schema.BigInt),
        ]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.Tuple([Schema.Number]), Schema.Array(Schema.BigInt)]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.TupleWithRest(Schema.Tuple([Schema.String]), [Schema.Number]),
          Schema.Tuple([Schema.BigInt]),
        ]),
      ),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.TupleWithRest(Schema.Tuple([Schema.String]), [Schema.Number]),
          Schema.Tuple([Schema.String, Schema.BigInt]),
        ]),
      ),
    ).toBe("mixed numeric domain: bigint, number");
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.Number, Schema.Array(Schema.BigInt)]),
      ),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([Schema.Tuple([Schema.Number]), Schema.Record(Schema.String, Schema.BigInt)]),
      ),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.Struct({
            amount: Schema.Number,
          }),
          Schema.Tuple([Schema.BigInt]),
        ]),
      ),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.Struct({
            amount: Schema.Number,
          }),
          Schema.Array(Schema.BigInt),
        ]),
      ),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Union([
          Schema.Struct({
            price: Schema.Number,
          }),
          Schema.Struct({
            quantity: Schema.BigInt,
          }),
        ]),
      ),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.Struct({
          quantity: Schema.BigInt,
          price: Schema.Number,
        }),
      ),
    ).toBe(undefined);
    expect(
      viewServerUnsupportedRuntimeFieldDomain(
        Schema.TupleWithRest(Schema.Tuple([Schema.String]), [Schema.String]),
      ),
    ).toBe(undefined);
    expect(viewServerUnsupportedRuntimeFieldDomain(Schema.Struct({ id: Schema.String }))).toBe(
      undefined,
    );
    expect(viewServerUnsupportedRuntimeFieldDomain({})).toBe(undefined);
  });

  it("rejects topic fields with unsupported runtime value domains", () => {
    expect(() =>
      defineViewServerConfig({
        topics: {
          dated: {
            schema: Schema.Struct({
              id: Schema.String,
              createdAt: Schema.Date,
            }),
            key: "id",
          },
        },
      }),
    ).toThrow("View Server topic dated field createdAt uses unsupported runtime domain: Date");
    expect(() =>
      defineViewServerConfig({
        topics: {
          nested: {
            schema: Schema.Struct({
              id: Schema.String,
              metadata: Schema.Struct({
                latency: Schema.Duration,
              }),
            }),
            key: "id",
          },
        },
      }),
    ).toThrow("View Server topic nested field metadata uses unsupported runtime domain: Duration");
    expect(() =>
      defineViewServerConfig({
        topics: {
          mixedNumeric: {
            schema: Schema.Struct({
              id: Schema.String,
              amount: Schema.Union([Schema.BigInt, Schema.Number]),
            }),
            key: "id",
          },
        },
      }),
    ).toThrow(
      "View Server topic mixedNumeric field amount uses unsupported runtime domain: mixed numeric domain: bigint, number",
    );
    expect(() =>
      defineViewServerConfig({
        topics: {
          nestedMixedNumeric: {
            schema: Schema.Struct({
              id: Schema.String,
              payload: Schema.Struct({
                amount: Schema.Union([Schema.BigInt, Schema.Number]),
              }),
            }),
            key: "id",
          },
        },
      }),
    ).toThrow(
      "View Server topic nestedMixedNumeric field payload uses unsupported runtime domain: mixed numeric domain: bigint, number",
    );
  });

  it("defines topics and pure runtime option contracts without starting a runtime", () => {
    const runtimeOptions = viewServer.defineRuntimeOptions({
      websocketPort: runtimeEnvironmentConfig.websocketPort,
      kafka: {
        consumerGroupId: "view-server-config-test",
        regions: kafkaRegions,
        topics: {
          orders: kafkaTopic({
            regions: ["usa", "london"],
            value: kafka.protobuf(ordersValueSchema),
            key: kafka.protobuf(ordersKeySchema),
            viewServerTopic: "orders",
            mapping: ({ key, value, region }) => {
              expectTypeOf(key).toEqualTypeOf<OrdersKeyMessage>();
              expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
              expectTypeOf(region).toEqualTypeOf<"usa" | "london">();
              return {
                id: key.orderId,
                customerId: value.customerId,
                status: value.status,
                price: value.price,
                region,
                updatedAt: value.updatedAt,
              };
            },
          }),
          trades: kafkaTopic({
            regions: ["usa"],
            value: kafka.protobuf(tradesValueSchema),
            viewServerTopic: "trades",
            mapping: ({ key, value, region }) => {
              expectTypeOf(key).toEqualTypeOf<string>();
              expectTypeOf(value).toEqualTypeOf<TradesValueMessage>();
              expectTypeOf(region).toEqualTypeOf<"usa">();
              return {
                id: key,
                symbol: value.symbol,
                quantity: value.quantity,
                price: value.price,
                region,
              };
            },
          }),
        },
      },
    });

    expect(runtimeOptions.kafka.regions["usa"]).toBe(kafkaRegions.usa);
    expect(viewServer.topics.orders.key).toBe("id");
    expect(runtimeOptions.websocketPort).toBe(runtimeEnvironmentConfig.websocketPort);
    expect(runtimeOptions.kafka.consumerGroupId).toBe("view-server-config-test");
    expect(Config.isConfig(runtimeConfig.port("VIEW_SERVER_WEBSOCKET_PORT"))).toBe(true);
  });

  it.effect("defines typed Kafka source codecs", () =>
    Effect.gen(function* () {
      const bytesCodec = kafka.bytes();
      const stringCodec = kafka.string();
      const stringKeyCodec = kafka.stringKey();
      const jsonCodec = kafka.json(Order);
      const jsonPositionCodec = kafka.json(Position);
      const protobufCodec = kafka.protobuf(ordersValueSchema);
      const customCodec = kafka.codec({
        name: "custom-order-value",
        decode: ({ bytes }): Effect.Effect<{ readonly byteLength: number }, never> =>
          Effect.succeed({
            byteLength: bytes.byteLength,
          }),
      });
      const customErrorCodec = kafka.codec({
        name: "custom-order-value-with-error",
        decode: (): Effect.Effect<{ readonly id: string }, CustomKafkaCodecError> =>
          Effect.fail({
            _tag: "CustomKafkaCodecError",
            message: "decode failed",
          }),
      });

      expect(bytesCodec.format).toBe("bytes");
      expect(stringCodec.format).toBe("string");
      expect(stringKeyCodec.format).toBe("string");
      expect(jsonCodec.schema).toBe(Order);
      expect(protobufCodec.descriptor).toBe(ordersValueSchema);
      expect(customCodec.name).toBe("custom-order-value");
      expect(
        yield* decodeKafkaCodec(bytesCodec, {
          bytes: new Uint8Array([1, 2, 3]),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual(new Uint8Array([1, 2, 3]));
      expect(
        yield* decodeKafkaCodec(stringCodec, {
          bytes: textEncoder.encode("order-value"),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toBe("order-value");
      expect(
        yield* decodeKafkaCodec(stringKeyCodec, {
          bytes: textEncoder.encode("order-key"),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toBe("order-key");
      expect(
        yield* decodeKafkaCodec(jsonCodec, {
          bytes: yield* kafkaJsonBytes(Order, {
            id: "order-1",
            customerId: "customer-1",
            status: "open",
            price: 42,
            region: "usa",
            updatedAt: 1,
          }),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual({
        id: "order-1",
        customerId: "customer-1",
        status: "open",
        price: 42,
        region: "usa",
        updatedAt: 1,
      });
      const decodedJsonPosition = yield* decodeKafkaCodec(jsonPositionCodec, {
        bytes: yield* kafkaJsonBytes(Position, {
          id: "position-1",
          accountId: "account-1",
          symbol: "AAPL",
          active: true,
          quantity: 9007199254740993n,
          optionalQuantity: 9007199254740995n,
          price: BigDecimal.fromStringUnsafe("1234567890.123456789"),
          notional: 10,
          optionalNotional: 20,
        }),
        metadata: kafkaTestMetadata("usa"),
      });
      expect(BigDecimal.isBigDecimal(decodedJsonPosition.price)).toBe(true);
      expect({
        ...decodedJsonPosition,
        price: BigDecimal.format(decodedJsonPosition.price),
      }).toStrictEqual({
        id: "position-1",
        accountId: "account-1",
        symbol: "AAPL",
        active: true,
        quantity: 9007199254740993n,
        optionalQuantity: 9007199254740995n,
        price: "1234567890.123456789",
        notional: 10,
        optionalNotional: 20,
      });
      const decodedAmbiguousJsonPosition = yield* decodeKafkaCodec(
        kafka.json(AmbiguousJsonPosition),
        {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-2",
              quantity: "9007199254740997",
              quantityFromString: "9007199254740998",
              stringOrBigInt: "9007199254740999",
              stringOrBytes: "AQID",
              stringOrNumber: "NaN",
              bigDecimalOrBigInt: "9007199254741000",
              nested: {
                quantity: "9007199254741001",
                price: "987654321.123456789",
                stringOrBigInt: "9007199254741003",
                stringOrBytes: "AQID",
                stringOrNumber: "NaN",
              },
              nestedRows: [
                {
                  quantity: "9007199254741005",
                  price: "123.456",
                  stringOrBigInt: "9007199254741007",
                  stringOrBytes: "AQID",
                  stringOrNumber: "NaN",
                },
              ],
              nestedUnion: {
                kind: "primary",
                quantity: "9007199254741009",
                stringOrBigInt: "9007199254741011",
              },
              structuredOrString: {
                quantity: "9007199254741012",
                price: "222.333",
                stringOrBigInt: "9007199254741012",
                stringOrBytes: "AQID",
                stringOrNumber: "NaN",
              },
              scalarOrStructured: "9007199254741012",
              validOneOf: {
                quantity: "9007199254741012",
              },
              classOrString: {
                quantity: "9007199254741012",
                stringOrBigInt: "9007199254741012",
              },
              untaggedStructuredUnion: {
                quantity: "9007199254741012",
                stringOrBigInt: "9007199254741012",
              },
              sentinelUnion: {
                kind: "b",
                id: "123.456",
              },
              optionalSentinelUnion: {
                kind: "b",
                id: "123",
              },
              suspendedNested: {
                quantity: "9007199254741012",
                price: "333.444",
                stringOrBigInt: "9007199254741012",
                stringOrBytes: "AQID",
                stringOrNumber: "NaN",
              },
              classNested: {
                quantity: "9007199254741012",
                stringOrBigInt: "9007199254741012",
              },
              optionNested: {
                _tag: "Some",
                value: {
                  quantity: "9007199254741012",
                  price: "444.555",
                  stringOrBigInt: "9007199254741012",
                  stringOrBytes: "AQID",
                  stringOrNumber: "NaN",
                },
              },
              optionFromNullNested: {
                quantity: "9007199254741012",
                price: "555.666",
                stringOrBigInt: "9007199254741012",
                stringOrBytes: "AQID",
                stringOrNumber: "NaN",
              },
              optionFromNullishNested: null,
              encodedNested: {
                qty: "9007199254741012",
                qty_from_string: "9007199254741013",
              },
              composedObjectTransform: {
                quantity: "123",
              },
              encodedKeyObjectTransform: {
                qty: "9007199254741014",
              },
              arrayTransform: [" 9007199254741014 "],
              encodedScalarInput: "9007199254741015",
              encodedArrayInput: ["9007199254741015"],
              transformUnion: "9007199254741015",
              amountsByAccount: {
                account1: "9007199254741013",
              },
              pricesByAccount: {
                account1: "77.123456789",
              },
              amountsByNumericAccount: {
                1: "9007199254741015",
                ignored: "not-a-bigint",
              },
              amountsByPrefixedAccount: {
                "account-1": "9007199254741016",
                ignored: "not-a-bigint",
              },
              amountsByUnionKeyAccount: {
                2: "9007199254741017",
                "account-open": "9007199254741018",
                ignored: "not-a-bigint",
              },
              amountsByNestedTemplateAccount: {
                "book-account-closed": "9007199254741019",
                ignored: "not-a-bigint",
              },
              amountsByLiteralAccount: {
                account1: "9007199254741020",
              },
              amountsBySymbolAccount: {
                "Symbol(account1)": "9007199254741021",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        },
      );
      const decodedStructuredOrString = yield* Schema.decodeUnknownEffect(
        AmbiguousJsonNestedPosition,
      )(decodedAmbiguousJsonPosition.structuredOrString);
      expect({
        ...decodedAmbiguousJsonPosition,
        nested: {
          ...decodedAmbiguousJsonPosition.nested,
          price: BigDecimal.format(decodedAmbiguousJsonPosition.nested.price),
        },
        nestedRows: decodedAmbiguousJsonPosition.nestedRows.map((row) => ({
          ...row,
          price: BigDecimal.format(row.price),
        })),
        structuredOrString: {
          ...decodedStructuredOrString,
          price: BigDecimal.format(decodedStructuredOrString.price),
        },
        suspendedNested: {
          ...decodedAmbiguousJsonPosition.suspendedNested,
          price: BigDecimal.format(decodedAmbiguousJsonPosition.suspendedNested.price),
        },
      }).toStrictEqual({
        id: "position-2",
        quantity: 9007199254740997n,
        quantityFromString: 9007199254740998n,
        stringOrBigInt: 9007199254740999n,
        stringOrBytes: "AQID",
        stringOrNumber: "NaN",
        bigDecimalOrBigInt: 9007199254741000n,
        nested: {
          quantity: 9007199254741001n,
          price: "987654321.123456789",
          stringOrBigInt: 9007199254741003n,
          stringOrBytes: "AQID",
          stringOrNumber: "NaN",
        },
        nestedRows: [
          {
            quantity: 9007199254741005n,
            price: "123.456",
            stringOrBigInt: 9007199254741007n,
            stringOrBytes: "AQID",
            stringOrNumber: "NaN",
          },
        ],
        nestedUnion: {
          kind: "primary",
          quantity: 9007199254741009n,
          stringOrBigInt: 9007199254741011n,
        },
        structuredOrString: {
          quantity: 9007199254741012n,
          price: "222.333",
          stringOrBigInt: 9007199254741012n,
          stringOrBytes: "AQID",
          stringOrNumber: "NaN",
        },
        scalarOrStructured: 9007199254741012n,
        validOneOf: {
          quantity: 9007199254741012n,
        },
        classOrString: new AmbiguousJsonClassPosition({
          quantity: 9007199254741012n,
          stringOrBigInt: 9007199254741012n,
        }),
        untaggedStructuredUnion: {
          quantity: 9007199254741012n,
          stringOrBigInt: "9007199254741012",
        },
        sentinelUnion: {
          kind: "b",
          id: BigDecimal.fromStringUnsafe("123.456"),
        },
        optionalSentinelUnion: {
          id: 123n,
        },
        suspendedNested: {
          quantity: 9007199254741012n,
          price: "333.444",
          stringOrBigInt: 9007199254741012n,
          stringOrBytes: "AQID",
          stringOrNumber: "NaN",
        },
        classNested: new AmbiguousJsonClassPosition({
          quantity: 9007199254741012n,
          stringOrBigInt: 9007199254741012n,
        }),
        optionNested: Option.some({
          quantity: 9007199254741012n,
          price: BigDecimal.fromStringUnsafe("444.555"),
          stringOrBigInt: 9007199254741012n,
          stringOrBytes: "AQID",
          stringOrNumber: "NaN",
        }),
        optionFromNullNested: Option.some({
          quantity: 9007199254741012n,
          price: BigDecimal.fromStringUnsafe("555.666"),
          stringOrBigInt: 9007199254741012n,
          stringOrBytes: "AQID",
          stringOrNumber: "NaN",
        }),
        optionFromNullishNested: Option.none(),
        encodedNested: {
          quantity: 9007199254741012n,
          quantityFromString: 9007199254741013n,
        },
        composedObjectTransform: {
          quantity: 123,
        },
        encodedKeyObjectTransform: {
          quantityText: "9007199254741014",
        },
        arrayTransform: [9007199254741014n],
        encodedScalarInput: "9007199254741015",
        encodedArrayInput: ["9007199254741015"],
        transformUnion: "9007199254741015",
        amountsByAccount: {
          account1: 9007199254741013n,
        },
        pricesByAccount: {
          account1: BigDecimal.fromStringUnsafe("77.123456789"),
        },
        amountsByNumericAccount: {
          1: 9007199254741015n,
        },
        amountsByPrefixedAccount: {
          "account-1": 9007199254741016n,
        },
        amountsByUnionKeyAccount: {
          2: 9007199254741017n,
          "account-open": 9007199254741018n,
        },
        amountsByNestedTemplateAccount: {
          "book-account-closed": 9007199254741019n,
        },
        amountsByLiteralAccount: {
          account1: 9007199254741020n,
        },
        amountsBySymbolAccount: {
          [Symbol.for("account1")]: 9007199254741021n,
        },
      });
      expect(
        yield* decodeKafkaCodec(kafka.json(AmbiguousJsonOptionPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-option-none",
              optionNested: {
                _tag: "None",
              },
              optionQuantityFromString: {
                _tag: "Some",
                value: "9007199254741022",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual({
        id: "position-option-none",
        optionNested: Option.none(),
        optionQuantityFromString: Option.some(9007199254741022n),
      });
      expect(
        yield* decodeKafkaCodec(kafka.json(AmbiguousJsonSuspendedOptionPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-suspended-option",
              optionQuantity: {
                _tag: "Some",
                value: "9007199254741023",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual({
        id: "position-suspended-option",
        optionQuantity: Option.some(9007199254741023n),
      });
      expect(
        yield* decodeKafkaCodec(kafka.json(AmbiguousJsonRepeatedRecordKeyPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-repeated-record-key",
              amountsByRepeatedAccount: {
                account1: "9007199254741024",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual({
        id: "position-repeated-record-key",
        amountsByRepeatedAccount: {
          account1: 9007199254741024n,
        },
      });
      expect(
        yield* decodeKafkaCodec(kafka.json(AmbiguousJsonTransformedRecordKeyPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-transformed-record-key",
              amountsByTransformedAccount: {
                " account1 ": "9007199254741025",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual({
        id: "position-transformed-record-key",
        amountsByTransformedAccount: {
          account1: 9007199254741025n,
        },
      });
      expect(
        yield* decodeKafkaCodec(kafka.json(AmbiguousJsonDeclaredCodecPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-declared-codec",
              declaredQuantity: "9007199254741025",
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual({
        id: "position-declared-codec",
        declaredQuantity: 9007199254741025n,
      });
      expect(
        yield* decodeKafkaCodec(kafka.json(AmbiguousJsonNativeJsonPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-native-json",
              jsonPayload: {
                tags: ["json", "direct"],
                quantity: 42,
              },
              mutableJsonPayload: {
                tags: ["mutable-json", "direct"],
                quantity: 43,
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual({
        id: "position-native-json",
        jsonPayload: {
          tags: ["json", "direct"],
          quantity: 42,
        },
        mutableJsonPayload: {
          tags: ["mutable-json", "direct"],
          quantity: 43,
        },
      });
      expect(
        yield* decodeKafkaCodec(kafka.json(AmbiguousJsonDeclaredScalarCollectionsPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-declared-scalar-collections",
              declaredText: "ready",
              declaredTags: ["fast", "typed"],
              declaredObject: "decoded-object",
              declaredObjectKeyword: "decoded-object-keyword",
              declaredUnknown: "decoded-unknown",
              declaredTemplate: "template-value",
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual({
        id: "position-declared-scalar-collections",
        declaredText: "ready",
        declaredTags: ["fast", "typed"],
        declaredObject: {
          id: "decoded-object",
        },
        declaredObjectKeyword: {
          id: "decoded-object-keyword",
        },
        declaredUnknown: {
          id: "decoded-unknown",
        },
        declaredTemplate: "template-value",
      });
      const decodedDurationPosition = yield* decodeKafkaCodec(
        kafka.json(AmbiguousJsonDurationPosition),
        {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-duration",
              latency: {
                _tag: "Millis",
                value: 42,
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        },
      );
      expect({
        id: decodedDurationPosition.id,
        latencyMillis: Duration.toMillis(decodedDurationPosition.latency),
      }).toStrictEqual({
        id: "position-duration",
        latencyMillis: 42,
      });
      const decodedAnnotatedDurationPosition = yield* decodeKafkaCodec(
        kafka.json(AmbiguousJsonAnnotatedDurationPosition),
        {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-annotated-duration",
              latency: {
                _tag: "Millis",
                value: 84,
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        },
      );
      expect({
        id: decodedAnnotatedDurationPosition.id,
        latencyMillis: Duration.toMillis(decodedAnnotatedDurationPosition.latency),
      }).toStrictEqual({
        id: "position-annotated-duration",
        latencyMillis: 84,
      });
      const decodedErrorObjectsPosition = yield* decodeKafkaCodec(
        kafka.json(AmbiguousJsonErrorObjectsPosition),
        {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-error-objects",
              error: {
                message: "plain error",
                name: "Error",
              },
              errorWithStack: {
                message: "stacked error",
                name: "RangeError",
                stack: "RangeError: stacked error",
              },
              errorWithoutCause: {
                message: "cause-free error",
                name: "TypeError",
              },
              errorWithStackWithoutCause: {
                message: "stacked cause-free error",
                name: "SyntaxError",
                stack: "SyntaxError: stacked cause-free error",
              },
              pattern: {
                source: "orders-[0-9]+",
                flags: "i",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        },
      );
      expect({
        id: decodedErrorObjectsPosition.id,
        errorMessage: decodedErrorObjectsPosition.error.message,
        errorName: decodedErrorObjectsPosition.error.name,
        errorWithStackMessage: decodedErrorObjectsPosition.errorWithStack.message,
        errorWithStackName: decodedErrorObjectsPosition.errorWithStack.name,
        errorWithStack: decodedErrorObjectsPosition.errorWithStack.stack,
        errorWithoutCauseMessage: decodedErrorObjectsPosition.errorWithoutCause.message,
        errorWithoutCauseName: decodedErrorObjectsPosition.errorWithoutCause.name,
        errorWithStackWithoutCauseMessage:
          decodedErrorObjectsPosition.errorWithStackWithoutCause.message,
        errorWithStackWithoutCauseName: decodedErrorObjectsPosition.errorWithStackWithoutCause.name,
        errorWithStackWithoutCause: decodedErrorObjectsPosition.errorWithStackWithoutCause.stack,
        patternSource: decodedErrorObjectsPosition.pattern.source,
        patternFlags: decodedErrorObjectsPosition.pattern.flags,
      }).toStrictEqual({
        id: "position-error-objects",
        errorMessage: "plain error",
        errorName: "Error",
        errorWithStackMessage: "stacked error",
        errorWithStackName: "RangeError",
        errorWithStack: "RangeError: stacked error",
        errorWithoutCauseMessage: "cause-free error",
        errorWithoutCauseName: "TypeError",
        errorWithStackWithoutCauseMessage: "stacked cause-free error",
        errorWithStackWithoutCauseName: "SyntaxError",
        errorWithStackWithoutCause: "SyntaxError: stacked cause-free error",
        patternSource: "orders-[0-9]+",
        patternFlags: "i",
      });
      const decodedFilePosition = yield* decodeKafkaCodec(kafka.json(AmbiguousJsonFilePosition), {
        bytes: textEncoder.encode(
          JSON.stringify({
            id: "position-file",
            attachment: {
              data: base64FromBytes(new Uint8Array([1, 2, 3])),
              type: "application/octet-stream",
              name: "payload.bin",
              lastModified: 42,
            },
          }),
        ),
        metadata: kafkaTestMetadata("usa"),
      });
      expect({
        id: decodedFilePosition.id,
        attachmentName: decodedFilePosition.attachment.name,
        attachmentType: decodedFilePosition.attachment.type,
        attachmentSize: decodedFilePosition.attachment.size,
        attachmentLastModified: decodedFilePosition.attachment.lastModified,
      }).toStrictEqual({
        id: "position-file",
        attachmentName: "payload.bin",
        attachmentType: "application/octet-stream",
        attachmentSize: 3,
        attachmentLastModified: 42,
      });
      const decodedFormDataPosition = yield* decodeKafkaCodec(
        kafka.json(AmbiguousJsonFormDataPosition),
        {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-form-data",
              formData: [
                ["note", { _tag: "String", value: "ready" }],
                [
                  "attachment",
                  {
                    _tag: "File",
                    value: {
                      data: base64FromBytes(new Uint8Array([4, 5])),
                      type: "application/octet-stream",
                      name: "form.bin",
                      lastModified: 24,
                    },
                  },
                ],
              ],
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        },
      );
      expect(decodedFormDataPosition.id).toBe("position-form-data");
      expect(decodedFormDataPosition.formData.get("note")).toBe("ready");
      expect(decodedFormDataPosition.formData.get("attachment")).toBeInstanceOf(File);
      expect(decodedFormDataPosition.formData.get("attachment")).toHaveProperty("name", "form.bin");
      const decodedEffectCollectionsPosition = yield* decodeKafkaCodec(
        kafka.json(AmbiguousJsonEffectCollectionsPosition),
        {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-effect-collections",
              readonlySet: [
                {
                  quantity: "9007199254741051",
                  price: "10.5",
                  stringOrBigInt: "9007199254741052",
                  stringOrBytes: "AQID",
                  stringOrNumber: 11,
                },
              ],
              hashSet: [
                {
                  quantity: "9007199254741053",
                  price: "11.5",
                  stringOrBigInt: "9007199254741054",
                  stringOrBytes: "BAU=",
                  stringOrNumber: 12,
                },
              ],
              chunk: [
                {
                  quantity: "9007199254741055",
                  price: "12.5",
                  stringOrBigInt: "9007199254741056",
                  stringOrBytes: "Bgc=",
                  stringOrNumber: 13,
                },
              ],
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        },
      );
      expect({
        id: decodedEffectCollectionsPosition.id,
        readonlySetSize: decodedEffectCollectionsPosition.readonlySet.size,
        hashSetSize: HashSet.size(decodedEffectCollectionsPosition.hashSet),
        chunkSize: Array.from(decodedEffectCollectionsPosition.chunk).length,
      }).toStrictEqual({
        id: "position-effect-collections",
        readonlySetSize: 1,
        hashSetSize: 1,
        chunkSize: 1,
      });
      const decodedRedactedPosition = yield* decodeKafkaCodec(
        kafka.json(AmbiguousJsonRedactedPosition),
        {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-redacted",
              secret: "classified",
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        },
      );
      expect({
        id: decodedRedactedPosition.id,
        secret: Redacted.value(decodedRedactedPosition.secret),
      }).toStrictEqual({
        id: "position-redacted",
        secret: "classified",
      });
      const decodedRedactedObjectPosition = yield* decodeKafkaCodec(
        kafka.json(AmbiguousJsonRedactedObjectPosition),
        {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-redacted-object",
              redactedDuration: {
                _tag: "Millis",
                value: 7,
              },
              redactedNested: {
                quantity: "9007199254741057",
                price: "13.5",
                stringOrBigInt: "9007199254741058",
                stringOrBytes: "CAk=",
                stringOrNumber: 14,
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        },
      );
      expect({
        id: decodedRedactedObjectPosition.id,
        durationMillis: Duration.toMillis(
          Redacted.value(decodedRedactedObjectPosition.redactedDuration),
        ),
        nestedQuantity: Redacted.value(decodedRedactedObjectPosition.redactedNested).quantity,
        nestedPrice: BigDecimal.format(
          Redacted.value(decodedRedactedObjectPosition.redactedNested).price,
        ),
      }).toStrictEqual({
        id: "position-redacted-object",
        durationMillis: 7,
        nestedQuantity: 9007199254741057n,
        nestedPrice: "13.5",
      });
      const decodedSuspendedStringMapKey = yield* decodeKafkaCodec(
        kafka.json(AmbiguousJsonSuspendedStringMapKeyPosition),
        {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-suspended-string-map-key",
              readonlyMap: [["account1", "9007199254741025"]],
              hashMap: [["account2", "9007199254741026"]],
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        },
      );
      expect({
        id: decodedSuspendedStringMapKey.id,
        readonlyMap: decodedSuspendedStringMapKey.readonlyMap,
        hashMapEntries: HashMap.toEntries(decodedSuspendedStringMapKey.hashMap),
      }).toStrictEqual({
        id: "position-suspended-string-map-key",
        readonlyMap: new Map([["account1", 9007199254741025n]]),
        hashMapEntries: [["account2", 9007199254741026n]],
      });
      expect(
        yield* decodeKafkaCodec(kafka.json(AmbiguousJsonTaggedUnionPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-tagged-union",
              tagged: {
                _tag: "quantity",
                amount: "9007199254741026",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual({
        id: "position-tagged-union",
        tagged: {
          _tag: "quantity",
          amount: 9007199254741026n,
        },
      });
      expect(
        yield* decodeKafkaCodec(kafka.json(AmbiguousJsonSuspendedClassPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-suspended-class",
              nested: {
                quantity: "9007199254741022",
                stringOrBigInt: "9007199254741023",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual({
        id: "position-suspended-class",
        nested: new AmbiguousJsonClassPosition({
          quantity: 9007199254741022n,
          stringOrBigInt: 9007199254741023n,
        }),
      });
      expect(
        yield* decodeKafkaCodec(kafka.json(AmbiguousJsonSuspendedEmptyObjectTransformPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-suspended-empty-object-transform",
              empty: "wire-empty",
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual({
        id: "position-suspended-empty-object-transform",
        empty: {},
      });
      expect(
        yield* decodeKafkaCodec(
          kafka.json(AmbiguousJsonSuspendedOptionEmptyObjectTransformPosition),
          {
            bytes: textEncoder.encode(
              JSON.stringify({
                id: "position-suspended-option-empty-object-transform",
                maybeEmpty: "wire-empty",
              }),
            ),
            metadata: kafkaTestMetadata("usa"),
          },
        ),
      ).toStrictEqual({
        id: "position-suspended-option-empty-object-transform",
        maybeEmpty: Option.some({}),
      });
      expect(
        yield* decodeKafkaCodec(kafka.json(AmbiguousJsonSuspendedEmptyClassPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-suspended-empty-class",
              nested: {},
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual({
        id: "position-suspended-empty-class",
        nested: new AmbiguousJsonEmptyClassPosition({}),
      });
      expect(
        yield* decodeKafkaCodec(kafka.json(AmbiguousJsonDecodedSideSuspendedRecordKeyPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-decoded-side-suspended-record-key",
              decodedRecord: "wire-value",
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual({
        id: "position-decoded-side-suspended-record-key",
        decodedRecord: {
          account1: 9007199254741041n,
        },
      });
      expect(
        yield* decodeKafkaCodec(
          kafka.json(AmbiguousJsonSuspendedScalarToBadRecordKeyClassPosition),
          {
            bytes: textEncoder.encode(
              JSON.stringify({
                id: "position-suspended-scalar-to-bad-record-key-class",
                nested: "nested-class",
              }),
            ),
            metadata: kafkaTestMetadata("usa"),
          },
        ),
      ).toStrictEqual({
        id: "position-suspended-scalar-to-bad-record-key-class",
        nested: new AmbiguousJsonDecodedOnlyBadRecordKeyClassPosition({
          id: "nested-class",
          decodedRecord: {
            account1: 1n,
          },
        }),
      });
      expect(
        yield* decodeKafkaCodec(
          kafka.json(AmbiguousJsonDeclaredScalarToBadRecordKeyClassPosition),
          {
            bytes: textEncoder.encode(
              JSON.stringify({
                id: "position-declared-scalar-to-bad-record-key-class",
                declaredClass: "declared-class",
              }),
            ),
            metadata: kafkaTestMetadata("usa"),
          },
        ),
      ).toStrictEqual({
        id: "position-declared-scalar-to-bad-record-key-class",
        declaredClass: new AmbiguousJsonDecodedOnlyBadRecordKeyClassPosition({
          id: "declared-class",
          decodedRecord: {
            account1: 1n,
          },
        }),
      });
      expect(
        yield* decodeKafkaCodec(protobufCodec, {
          bytes: toBinary(
            ordersValueSchema,
            create(ordersValueSchema, {
              customerId: "customer-1",
              status: "open",
              price: 42,
              updatedAt: 1,
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual(
        create(ordersValueSchema, {
          customerId: "customer-1",
          status: "open",
          price: 42,
          updatedAt: 1,
        }),
      );
      expect(
        yield* customCodec.decode({
          bytes: new Uint8Array([1, 2, 3]),
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual({
        byteLength: 3,
      });
      const jsonParseFailure = yield* Effect.exit(
        decodeKafkaCodec(jsonCodec, {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const validSuspendedTupleWithRestMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(AmbiguousJsonSuspendedTupleWithRestPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const validSuspendedScalarCodecMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(AmbiguousJsonSuspendedScalarCodecPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const validNestedDeclarationMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(AmbiguousJsonDeclaredNestedDeclarationPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidOpaqueDeclarationMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(AmbiguousJsonDeclaredOpaqueDeclarationPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidSuspendedEmptyObjectJsonSchemaError = yield* decodeKafkaCodec(
        kafka.json(AmbiguousJsonSuspendedEmptyObjectPosition),
        {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-suspended-empty-object",
              empty: {},
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const jsonSchemaFailure = yield* Effect.exit(
        decodeKafkaCodec(jsonCodec, {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "order-1",
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const primitiveJsonSchemaFailure = yield* Effect.exit(
        decodeKafkaCodec(jsonCodec, {
          bytes: textEncoder.encode(JSON.stringify("order-1")),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const invalidArrayJsonSchemaFailure = yield* Effect.exit(
        decodeKafkaCodec(kafka.json(InvalidAmbiguousJsonArrayPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-3",
              nestedRows: "not-an-array",
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const invalidArrayTransformJsonSchemaFailure = yield* Effect.exit(
        decodeKafkaCodec(kafka.json(InvalidAmbiguousJsonArrayTransformPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-array-transform",
              arrayTransform: [123],
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const invalidEncodedArrayInputJsonSchemaFailure = yield* Effect.exit(
        decodeKafkaCodec(kafka.json(InvalidAmbiguousJsonEncodedArrayInputPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-encoded-array-input",
              encodedArrayInput: [123],
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const invalidNestedJsonSchemaFailure = yield* Effect.exit(
        decodeKafkaCodec(kafka.json(InvalidAmbiguousJsonNestedPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-4",
              nested: {
                quantity: "9007199254741013",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const invalidNestedShapeJsonSchemaFailure = yield* Effect.exit(
        decodeKafkaCodec(kafka.json(InvalidAmbiguousJsonNestedPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-5",
              nested: "not-an-object",
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const invalidTupleJsonSchemaFailure = yield* Effect.exit(
        decodeKafkaCodec(kafka.json(InvalidAmbiguousJsonTuplePosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-6",
              tuple: ["9007199254741015", "extra"],
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const invalidUnionJsonSchemaFailure = yield* Effect.exit(
        decodeKafkaCodec(kafka.json(InvalidAmbiguousJsonUnionPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-7",
              nestedUnion: {
                kind: "unknown",
                quantity: "9007199254741017",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const invalidUnionWithInvalidRecordJsonSchemaFailure = yield* Effect.exit(
        decodeKafkaCodec(kafka.json(InvalidAmbiguousJsonUnionWithInvalidRecordPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-invalid-union-record",
              unionWithInvalidRecord: {
                kind: "ok",
                value: "would otherwise match the second branch",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const invalidRecordJsonSchemaFailure = yield* Effect.exit(
        decodeKafkaCodec(kafka.json(InvalidAmbiguousJsonRecordPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-8",
              amountsByAccount: {
                account1: "not-a-bigint",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const invalidNumericLiteralRecordJsonSchemaFailure = yield* Effect.exit(
        decodeKafkaCodec(kafka.json(InvalidAmbiguousJsonNumericLiteralRecordPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-numeric-literal-record",
              amountsByNumericLiteralAccount: {
                "1": "9007199254741025",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const invalidDeclaredRecordJsonSchemaFailure = yield* Effect.exit(
        decodeKafkaCodec(kafka.json(InvalidAmbiguousJsonDeclaredRecordPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-declared-record",
              declaredRecord: {
                "1": "9007199254741027",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const invalidDeclaredRecordJsonSchemaError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonDeclaredRecordPosition),
        {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-declared-record-error",
              declaredRecord: {
                "1": "9007199254741027",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidDeclaredRecordMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonDeclaredRecordPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidDeclaredSuspendedRecordKeyMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonDeclaredSuspendedRecordKeyPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidDeclaredSuspendedRecordKeyTargetMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonDeclaredSuspendedRecordKeyTargetPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidDeclaredNestedSuspendedRecordKeyMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonDeclaredNestedSuspendedRecordKeyPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidDeclaredPartialRecordKeyMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonDeclaredPartialRecordKeyPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidDeclaredSpoofedTypeConstructorMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonDeclaredSpoofedTypeConstructorPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidDeclaredSpoofedOptionWithValueMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonDeclaredSpoofedOptionWithValuePosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidDeclaredSpoofedOptionBuiltInToCodecMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonDeclaredSpoofedOptionBuiltInToCodecPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidDeclaredDurationTargetMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonDeclaredDurationTargetPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidDeclaredOptionRecordTargetMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonDeclaredOptionRecordTargetPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidAnnotatedDurationOverrideMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonAnnotatedDurationOverridePosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidAnnotatedOptionOverrideMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonAnnotatedOptionOverridePosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidAnnotatedReadonlyMapOverrideMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonAnnotatedReadonlyMapOverridePosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidAnnotatedOptionToCodecOverrideMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonAnnotatedOptionToCodecOverridePosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidAnnotatedOptionToCodecSpoofedSourceMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonAnnotatedOptionToCodecSpoofedSourcePosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidAnnotatedReadonlyMapToCodecOverrideMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonAnnotatedReadonlyMapToCodecOverridePosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidDeclaredToCodecRecordMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonDeclaredToCodecRecordPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const unsupportedDeclaredEmptyStructMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(UnsupportedAmbiguousJsonDeclaredEmptyStructPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const unsupportedDeclaredSuspendedEmptyStructMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(UnsupportedAmbiguousJsonDeclaredSuspendedEmptyStructPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const unsupportedSuspendedDeclaredEmptyStructMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(UnsupportedAmbiguousJsonSuspendedDeclaredEmptyStructPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const unsupportedSuspendedStructDeclaredEmptyStructMalformedJsonError =
        yield* decodeKafkaCodec(
          kafka.json(UnsupportedAmbiguousJsonSuspendedStructDeclaredEmptyStructPosition),
          {
            bytes: textEncoder.encode("{"),
            metadata: kafkaTestMetadata("usa"),
          },
        ).pipe(Effect.flip);
      const unsupportedSuspendedArrayDeclaredEmptyStructMalformedJsonError =
        yield* decodeKafkaCodec(
          kafka.json(UnsupportedAmbiguousJsonSuspendedArrayDeclaredEmptyStructPosition),
          {
            bytes: textEncoder.encode("{"),
            metadata: kafkaTestMetadata("usa"),
          },
        ).pipe(Effect.flip);
      const unsupportedSuspendedUnionDeclaredEmptyStructMalformedJsonError =
        yield* decodeKafkaCodec(
          kafka.json(UnsupportedAmbiguousJsonSuspendedUnionDeclaredEmptyStructPosition),
          {
            bytes: textEncoder.encode("{"),
            metadata: kafkaTestMetadata("usa"),
          },
        ).pipe(Effect.flip);
      const unsupportedSuspendedRecordDeclaredEmptyStructMalformedJsonError =
        yield* decodeKafkaCodec(
          kafka.json(UnsupportedAmbiguousJsonSuspendedRecordDeclaredEmptyStructPosition),
          {
            bytes: textEncoder.encode("{"),
            metadata: kafkaTestMetadata("usa"),
          },
        ).pipe(Effect.flip);
      const unsupportedSuspendedClassAnnotationSpoofMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(UnsupportedAmbiguousJsonSuspendedClassAnnotationSpoofPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const unsupportedSuspendedClassAnnotationStringParameterSpoofMalformedJsonError =
        yield* decodeKafkaCodec(
          kafka.json(UnsupportedAmbiguousJsonSuspendedClassAnnotationStringParameterSpoofPosition),
          {
            bytes: textEncoder.encode("{"),
            metadata: kafkaTestMetadata("usa"),
          },
        ).pipe(Effect.flip);
      const unsupportedDeclaredFieldsSpoofMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(UnsupportedAmbiguousJsonDeclaredFieldsSpoofPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const unsupportedDeclaredTransformFromEmptyStructMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(UnsupportedAmbiguousJsonDeclaredTransformFromEmptyStructPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const unsupportedDeclaredObjectTupleMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(UnsupportedAmbiguousJsonDeclaredObjectTuplePosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const unsupportedDeclaredObjectKeywordMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(UnsupportedAmbiguousJsonDeclaredObjectKeywordPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const unsupportedDeclaredJsonMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(UnsupportedAmbiguousJsonDeclaredJsonPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const unsupportedDeclaredMutableJsonMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(UnsupportedAmbiguousJsonDeclaredMutableJsonPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const unsupportedDeclaredUnknownMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(UnsupportedAmbiguousJsonDeclaredUnknownPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const unsupportedDeclaredAnyMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(UnsupportedAmbiguousJsonDeclaredAnyPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidDeclaredRecordValueMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonDeclaredRecordValuePosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidSuspendedStructDeclaredRecordValueMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonSuspendedStructDeclaredRecordValuePosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidSuspendedRecordJsonSchemaError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonSuspendedRecordPosition),
        {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-suspended-record-error",
              suspendedRecord: {
                "1": "9007199254741029",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidSuspendedRecordMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonSuspendedRecordPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidSuspendedRecordKeyClassMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonSuspendedRecordKeyClassPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidDeclaredClassTargetMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonDeclaredClassTargetPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidDeclaredStringOnlyClassTargetMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonDeclaredStringOnlyClassTargetPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidCustomJsonCodecClassMalformedJsonError = yield* decodeKafkaCodec(
        kafka.json(InvalidAmbiguousJsonCustomJsonCodecClassPosition),
        {
          bytes: textEncoder.encode("{"),
          metadata: kafkaTestMetadata("usa"),
        },
      ).pipe(Effect.flip);
      const invalidOneOfJsonSchemaFailure = yield* Effect.exit(
        decodeKafkaCodec(kafka.json(InvalidAmbiguousJsonOneOfPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-9",
              nestedOneOf: {
                quantity: "9007199254741019",
                note: "matches both oneOf members",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const invalidBroadOneOfJsonSchemaFailure = yield* Effect.exit(
        decodeKafkaCodec(kafka.json(InvalidAmbiguousJsonBroadOneOfPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-10",
              broadOneOf: {
                kind: "b",
                id: "123",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const invalidScalarOneOfJsonSchemaFailure = yield* Effect.exit(
        decodeKafkaCodec(kafka.json(InvalidAmbiguousJsonScalarOneOfPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-11",
              scalarOneOf: "9007199254741023",
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const invalidOptionPrimitiveJsonSchemaFailure = yield* Effect.exit(
        decodeKafkaCodec(kafka.json(AmbiguousJsonOptionPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-option-primitive",
              optionNested: "not-an-option",
              optionQuantityFromString: {
                _tag: "None",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const invalidOptionTagJsonSchemaFailure = yield* Effect.exit(
        decodeKafkaCodec(kafka.json(AmbiguousJsonOptionPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-option-tag",
              optionNested: {
                _tag: "Unknown",
              },
              optionQuantityFromString: {
                _tag: "None",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const invalidOptionMissingValueJsonSchemaFailure = yield* Effect.exit(
        decodeKafkaCodec(kafka.json(AmbiguousJsonOptionPosition), {
          bytes: textEncoder.encode(
            JSON.stringify({
              id: "position-option-missing-value",
              optionNested: {
                _tag: "Some",
              },
              optionQuantityFromString: {
                _tag: "None",
              },
            }),
          ),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const protobufFailure = yield* Effect.exit(
        decodeKafkaCodec(protobufCodec, {
          bytes: new Uint8Array([255]),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      const customFailure = yield* Effect.exit(
        decodeKafkaCodec(customErrorCodec, {
          bytes: new Uint8Array([1]),
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      expect(Exit.isFailure(jsonParseFailure)).toBe(true);
      expect(validSuspendedTupleWithRestMalformedJsonError.message).toBe(
        "Failed to parse Kafka JSON payload",
      );
      expect(validSuspendedScalarCodecMalformedJsonError.message).toBe(
        "Failed to parse Kafka JSON payload",
      );
      expect(validNestedDeclarationMalformedJsonError.message).toBe(
        "Failed to parse Kafka JSON payload",
      );
      expect(invalidOpaqueDeclarationMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidSuspendedEmptyObjectJsonSchemaError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(Exit.isFailure(jsonSchemaFailure)).toBe(true);
      expect(Exit.isFailure(primitiveJsonSchemaFailure)).toBe(true);
      expect(Exit.isFailure(invalidArrayJsonSchemaFailure)).toBe(true);
      expect(Exit.isFailure(invalidArrayTransformJsonSchemaFailure)).toBe(true);
      expect(Exit.isFailure(invalidEncodedArrayInputJsonSchemaFailure)).toBe(true);
      expect(Exit.isFailure(invalidNestedJsonSchemaFailure)).toBe(true);
      expect(Exit.isFailure(invalidNestedShapeJsonSchemaFailure)).toBe(true);
      expect(Exit.isFailure(invalidTupleJsonSchemaFailure)).toBe(true);
      expect(Exit.isFailure(invalidUnionJsonSchemaFailure)).toBe(true);
      expect(Exit.isFailure(invalidUnionWithInvalidRecordJsonSchemaFailure)).toBe(true);
      expect(Exit.isFailure(invalidRecordJsonSchemaFailure)).toBe(true);
      expect(Exit.isFailure(invalidNumericLiteralRecordJsonSchemaFailure)).toBe(true);
      expect(Exit.isFailure(invalidDeclaredRecordJsonSchemaFailure)).toBe(true);
      expect(invalidDeclaredRecordJsonSchemaError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidDeclaredRecordMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidDeclaredSuspendedRecordKeyMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidDeclaredSuspendedRecordKeyTargetMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidDeclaredNestedSuspendedRecordKeyMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidDeclaredPartialRecordKeyMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidDeclaredSpoofedTypeConstructorMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidDeclaredSpoofedOptionWithValueMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidDeclaredSpoofedOptionBuiltInToCodecMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidDeclaredDurationTargetMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidDeclaredOptionRecordTargetMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidAnnotatedDurationOverrideMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidAnnotatedOptionOverrideMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidAnnotatedReadonlyMapOverrideMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidAnnotatedOptionToCodecOverrideMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidAnnotatedOptionToCodecSpoofedSourceMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidAnnotatedReadonlyMapToCodecOverrideMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidDeclaredToCodecRecordMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(unsupportedDeclaredEmptyStructMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(unsupportedDeclaredSuspendedEmptyStructMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(unsupportedSuspendedDeclaredEmptyStructMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(unsupportedSuspendedStructDeclaredEmptyStructMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(unsupportedSuspendedArrayDeclaredEmptyStructMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(unsupportedSuspendedUnionDeclaredEmptyStructMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(unsupportedSuspendedRecordDeclaredEmptyStructMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(unsupportedSuspendedClassAnnotationSpoofMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(
        unsupportedSuspendedClassAnnotationStringParameterSpoofMalformedJsonError.message,
      ).toBe("Kafka JSON schema is not JSON-compatible");
      expect(unsupportedDeclaredFieldsSpoofMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(unsupportedDeclaredTransformFromEmptyStructMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(unsupportedDeclaredObjectTupleMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(unsupportedDeclaredObjectKeywordMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(unsupportedDeclaredJsonMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(unsupportedDeclaredMutableJsonMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(unsupportedDeclaredUnknownMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(unsupportedDeclaredAnyMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidDeclaredRecordValueMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidSuspendedStructDeclaredRecordValueMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidSuspendedRecordJsonSchemaError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidSuspendedRecordMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidSuspendedRecordKeyClassMalformedJsonError.message).toBe(
        "Failed to parse Kafka JSON payload",
      );
      expect(invalidDeclaredClassTargetMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidDeclaredStringOnlyClassTargetMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(invalidCustomJsonCodecClassMalformedJsonError.message).toBe(
        "Kafka JSON schema is not JSON-compatible",
      );
      expect(Exit.isFailure(invalidOneOfJsonSchemaFailure)).toBe(true);
      expect(Exit.isFailure(invalidBroadOneOfJsonSchemaFailure)).toBe(true);
      expect(Exit.isFailure(invalidScalarOneOfJsonSchemaFailure)).toBe(true);
      expect(Exit.isFailure(invalidOptionPrimitiveJsonSchemaFailure)).toBe(true);
      expect(Exit.isFailure(invalidOptionTagJsonSchemaFailure)).toBe(true);
      expect(Exit.isFailure(invalidOptionMissingValueJsonSchemaFailure)).toBe(true);
      expect(Exit.isFailure(protobufFailure)).toBe(true);
      expect(Exit.isFailure(customFailure)).toBe(true);

      expectTypeOf<KafkaCodecType<typeof bytesCodec>>().toEqualTypeOf<Uint8Array>();
      expectTypeOf<KafkaCodecType<typeof stringCodec>>().toEqualTypeOf<string>();
      expectTypeOf<KafkaCodecType<typeof stringKeyCodec>>().toEqualTypeOf<string>();
      expectTypeOf<KafkaCodecType<typeof jsonCodec>>().toEqualTypeOf<typeof Order.Type>();
      expectTypeOf<KafkaCodecType<typeof protobufCodec>>().toEqualTypeOf<OrdersValueMessage>();
      expectTypeOf<KafkaCodecType<typeof customCodec>>().toEqualTypeOf<{
        readonly byteLength: number;
      }>();
      expectTypeOf<KafkaCodecError<typeof jsonCodec>>().toEqualTypeOf<KafkaDecodeError>();
      expectTypeOf<KafkaCodecError<typeof protobufCodec>>().toEqualTypeOf<KafkaDecodeError>();
      expectTypeOf<KafkaCodecError<typeof customCodec>>().toEqualTypeOf<never>();
      expectTypeOf<
        KafkaCodecError<typeof customErrorCodec>
      >().toEqualTypeOf<CustomKafkaCodecError>();
    }),
  );

  it("does not expose executable React or runtime placeholders from config", () => {
    expect(Object.keys(viewServer)).toStrictEqual([
      "topics",
      "defineRuntimeOptions",
      "kafkaTopic",
      "grpcFeed",
    ]);
  });
});

describe("public type surface", () => {
  it("exposes health and transport contracts", () => {
    const snapshot: SnapshotEvent<{ readonly id: string }> = {
      type: "snapshot",
      topic: "orders",
      queryId: "query-1",
      version: 1,
      keys: ["order-1"],
      rows: [{ id: "order-1" }],
      totalRows: 1,
    };

    const metadata: KafkaMessageMetadata<"usa"> = {
      sourceTopic: "orders",
      sourceRegion: "usa",
      partition: 0,
      offset: "1",
      timestamp: null,
      headers: {},
    };

    const topicHealth: TopicRuntimeHealth = {
      status: "ready",
      rowCount: 1,
      liveRowCount: 1,
      deletedRowCount: 0,
      version: 1,
      lastMutationAt: null,
      mutationsPerSecond: 0,
      rowsPerSecond: 0,
      pendingMutationBatches: 0,
      activeFallbackGroupedViews: 0,
      activeIncrementalGroupedViews: 0,
      activeViews: 0,
      groupedFullEvaluationCount: 0,
      groupedPatchedEvaluationCount: 0,
      activeSubscriptions: 0,
      queuedEvents: 0,
      maxQueueDepth: 0,
      backpressureEvents: 0,
      memoryBytes: 0,
      tombstoneCount: 0,
      compactionPending: false,
    };

    const health: ViewServerHealth<typeof viewServer.topics> = {
      status: "ready",
      version: 1,
      uptimeMs: 100,
      engine: {
        topics: {
          orders: topicHealth,
          trades: topicHealth,
          positions: topicHealth,
        },
      },
      transport: {
        activeClients: 0,
        activeStreams: 0,
        activeSubscriptions: 0,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        queuedMessages: 0,
        queuedBytes: 0,
        droppedClients: 0,
        backpressureEvents: 0,
        reconnects: 0,
        lastError: null,
      },
    };

    const backpressure: StatusEvent = {
      type: "status",
      topic: "orders",
      queryId: "query-1",
      status: "error",
      code: "BackpressureExceeded",
      message: "client queue exceeded configured limits",
    };

    expect(snapshot.rows[0]).toStrictEqual({
      id: "order-1",
    });
    expect(metadata.sourceRegion).toBe("usa");
    expect(health.engine.topics["orders"].rowCount).toBe(1);
    expect(backpressure).toStrictEqual({
      type: "status",
      topic: "orders",
      queryId: "query-1",
      status: "error",
      code: "BackpressureExceeded",
      message: "client queue exceeded configured limits",
    });
    expectTypeOf<LiveTransportAdapter>().toHaveProperty("subscribe");
    expectTypeOf<Effect.Success<ReturnType<LiveTransportAdapter["subscribe"]>>>().toEqualTypeOf<
      LiveSubscription<unknown>
    >();
    expectTypeOf<
      Effect.Error<ReturnType<LiveTransportAdapter["subscribe"]>>
    >().toEqualTypeOf<ViewServerTransportError>();
  });

  it("derives pushed health summary and detailed rows from runtime health", () => {
    const health: ViewServerHealth<typeof viewServer.topics> = {
      status: "degraded",
      version: 7,
      uptimeMs: 100,
      engine: {
        topics: {
          orders: runtimeTopicHealth("ready", 10),
          trades: runtimeTopicHealth("degraded", 20),
          positions: runtimeTopicHealth("starting", 30),
        },
      },
      kafka: {
        startFrom: kafkaStartFromHealth,
        regions: {
          usa: {
            status: "connected",
            brokers: "localhost:9092",
            lastConnectedAt: null,
            lastError: null,
          },
        },
        topics: {
          sourceOrders: {
            status: "degraded",
            sourceTopic: "orders-source",
            viewServerTopic: "orders",
            regions: {
              usa: {
                connected: true,
                assignedPartitions: 1,
                messagesPerSecond: 10,
                bytesPerSecond: 100,
                decodedMessagesPerSecond: 10,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                commitFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: null,
                lastCommitAt: null,
                consumerLagMessages: 5n,
                lagSampledAt: null,
                committedOffset: "5",
                lastError: "decode failed",
              },
              london: {
                connected: true,
                assignedPartitions: 1,
                messagesPerSecond: 0,
                bytesPerSecond: 0,
                decodedMessagesPerSecond: 0,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                commitFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: null,
                lastCommitAt: null,
                consumerLagMessages: 3n,
                lagSampledAt: null,
                committedOffset: null,
                lastError: null,
              },
            },
          },
          sourceOrdersUnknownLag: {
            status: "ready",
            sourceTopic: "orders-source-unknown-lag",
            viewServerTopic: "orders",
            regions: {
              usa: {
                connected: true,
                assignedPartitions: 1,
                messagesPerSecond: 0,
                bytesPerSecond: 0,
                decodedMessagesPerSecond: 0,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                commitFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: null,
                lastCommitAt: null,
                consumerLagMessages: null,
                lagSampledAt: null,
                committedOffset: null,
                lastError: null,
              },
            },
          },
          sourceTrades: {
            status: "stalled",
            sourceTopic: "trades-source",
            viewServerTopic: "trades",
            regions: {
              usa: {
                connected: false,
                assignedPartitions: 1,
                messagesPerSecond: 0,
                bytesPerSecond: 0,
                decodedMessagesPerSecond: 0,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                commitFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: null,
                lastCommitAt: null,
                consumerLagMessages: 11n,
                lagSampledAt: null,
                committedOffset: "9",
                lastError: "stalled",
              },
            },
          },
        },
      },
      transport: {
        activeClients: 0,
        activeStreams: 0,
        activeSubscriptions: 0,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        queuedMessages: 0,
        queuedBytes: 0,
        droppedClients: 0,
        backpressureEvents: 0,
        reconnects: 0,
        lastError: null,
      },
    };

    const summary = viewServerHealthSummaryFromHealth(health, 123n);
    const summaryRow = viewServerHealthSummaryRowFromHealth(health, 123n);
    const rows = viewServerHealthTopicRowsFromHealth(health, 123n);
    const healthWithoutKafka: ViewServerHealth<typeof viewServer.topics> = {
      status: health.status,
      version: health.version,
      uptimeMs: health.uptimeMs,
      engine: health.engine,
      transport: health.transport,
    };
    const grpcOnlyHealth: ViewServerHealth<typeof viewServer.topics> = {
      status: "degraded",
      version: 9,
      uptimeMs: 300,
      engine: {
        topics: {
          orders: runtimeTopicHealth("ready", 10),
          trades: runtimeTopicHealth("ready", 20),
          positions: runtimeTopicHealth("ready", 30),
        },
      },
      grpc: {
        clients: {
          ordersClient: {
            status: "connected",
            baseUrl: "http://localhost:8080",
            activeFeeds: 3,
            lastConnectedAt: null,
            lastError: null,
          },
        },
        feeds: {
          orders: {
            materialized: {
              ordersFeed: {
                status: "ready",
                lifecycle: "materialized",
                feedName: "ordersFeed",
                feedKey: "ordersFeed",
                topic: "orders",
                subscriberCount: 0,
                rowCount: 10,
                messagesPerSecond: 1,
                rowsPerSecond: 1,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                reconnects: 0,
                lastMessageAt: null,
                lastError: null,
              },
            },
            leased: {},
          },
          trades: {
            materialized: {},
            leased: {
              tradesFeed: {
                status: "starting",
                lifecycle: "leased",
                feedName: "tradesFeed",
                feedKey: "tradesFeed:region=usa",
                topic: "trades",
                subscriberCount: 1,
                rowCount: 0,
                messagesPerSecond: 0,
                rowsPerSecond: 0,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                reconnects: 0,
                lastMessageAt: null,
                lastError: null,
              },
            },
          },
          positions: {
            materialized: {
              positionsFeed: {
                status: "degraded",
                lifecycle: "materialized",
                feedName: "positionsFeed",
                feedKey: "positionsFeed",
                topic: "positions",
                subscriberCount: 0,
                rowCount: 0,
                messagesPerSecond: 0,
                rowsPerSecond: 0,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 1,
                publishFailuresPerSecond: 0,
                reconnects: 1,
                lastMessageAt: null,
                lastError: "mapping failed",
              },
            },
            leased: {},
          },
        },
      },
      transport: health.transport,
    };
    const kafkaStartingHealth: ViewServerHealth<typeof viewServer.topics> = {
      status: "starting",
      version: 8,
      uptimeMs: 200,
      engine: {
        topics: {
          orders: runtimeTopicHealth("ready", 10),
          trades: runtimeTopicHealth("ready", 20),
          positions: runtimeTopicHealth("ready", 30),
        },
      },
      kafka: {
        startFrom: kafkaStartFromHealth,
        regions: {
          usa: {
            status: "connected",
            brokers: "localhost:9092",
            lastConnectedAt: null,
            lastError: null,
          },
        },
        topics: {
          sourceOrdersReady: {
            status: "ready",
            sourceTopic: "orders-source",
            viewServerTopic: "orders",
            regions: {
              usa: {
                connected: true,
                assignedPartitions: 1,
                messagesPerSecond: 0,
                bytesPerSecond: 0,
                decodedMessagesPerSecond: 0,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                commitFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: null,
                lastCommitAt: null,
                consumerLagMessages: null,
                lagSampledAt: null,
                committedOffset: null,
                lastError: null,
              },
            },
          },
          sourceTradesStarting: {
            status: "starting",
            sourceTopic: "trades-source",
            viewServerTopic: "trades",
            regions: {
              usa: {
                connected: false,
                assignedPartitions: 0,
                messagesPerSecond: 0,
                bytesPerSecond: 0,
                decodedMessagesPerSecond: 0,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                commitFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: null,
                lastCommitAt: null,
                consumerLagMessages: null,
                lagSampledAt: null,
                committedOffset: null,
                lastError: null,
              },
            },
          },
        },
      },
      transport: health.transport,
    };
    const orphanKafkaHealth: ViewServerHealth<typeof viewServer.topics> = {
      ...healthWithoutKafka,
      kafka: {
        startFrom: kafkaStartFromHealth,
        regions: {
          usa: {
            status: "connected",
            brokers: "localhost:9092",
            lastConnectedAt: null,
            lastError: null,
          },
        },
        topics: {
          orphanSource: {
            status: "ready",
            sourceTopic: "orphan-source",
            viewServerTopic: "orphan",
            regions: {
              usa: {
                connected: true,
                assignedPartitions: 1,
                messagesPerSecond: 0,
                bytesPerSecond: 0,
                decodedMessagesPerSecond: 0,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                publishFailuresPerSecond: 0,
                commitFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: null,
                lastCommitAt: null,
                consumerLagMessages: 99n,
                lagSampledAt: null,
                committedOffset: null,
                lastError: null,
              },
            },
          },
        },
      },
    };
    const stoppingRows = viewServerHealthTopicRowsFromHealth(
      {
        ...health,
        status: "stopping",
      },
      456n,
    );

    expect(summary).toStrictEqual({
      status: "degraded",
      runtimeStatus: "degraded",
      connectionStatus: "connected",
      unhealthyTopics: ["orders", "trades", "positions"],
      updatedAtNanos: 123n,
      maxKafkaLag: null,
    });
    expect(summaryRow).toStrictEqual({
      id: "summary",
      status: "degraded",
      runtimeStatus: "degraded",
      connectionStatus: "connected",
      unhealthyTopics: ["orders", "trades", "positions"],
      updatedAtNanos: 123n,
      maxKafkaLag: null,
    });
    expect(rows).toStrictEqual([
      {
        id: "orders",
        status: "degraded",
        rowCount: 10,
        liveRowCount: 10,
        deletedRowCount: 0,
        version: 10,
        lastMutationAt: null,
        mutationsPerSecond: 10,
        rowsPerSecond: 10,
        pendingMutationBatches: 0,
        activeFallbackGroupedViews: 0,
        activeIncrementalGroupedViews: 0,
        activeViews: 0,
        groupedFullEvaluationCount: 0,
        groupedPatchedEvaluationCount: 0,
        activeSubscriptions: 0,
        queuedEvents: 0,
        maxQueueDepth: 0,
        backpressureEvents: 0,
        memoryBytes: 0,
        tombstoneCount: 0,
        compactionPending: false,
        kafkaLag: null,
        updatedAtNanos: 123n,
      },
      {
        id: "trades",
        status: "degraded",
        rowCount: 20,
        liveRowCount: 20,
        deletedRowCount: 0,
        version: 20,
        lastMutationAt: null,
        mutationsPerSecond: 20,
        rowsPerSecond: 20,
        pendingMutationBatches: 0,
        activeFallbackGroupedViews: 0,
        activeIncrementalGroupedViews: 0,
        activeViews: 0,
        groupedFullEvaluationCount: 0,
        groupedPatchedEvaluationCount: 0,
        activeSubscriptions: 0,
        queuedEvents: 0,
        maxQueueDepth: 0,
        backpressureEvents: 0,
        memoryBytes: 0,
        tombstoneCount: 0,
        compactionPending: false,
        kafkaLag: 11n,
        updatedAtNanos: 123n,
      },
      {
        id: "positions",
        status: "starting",
        rowCount: 30,
        liveRowCount: 30,
        deletedRowCount: 0,
        version: 30,
        lastMutationAt: null,
        mutationsPerSecond: 30,
        rowsPerSecond: 30,
        pendingMutationBatches: 0,
        activeFallbackGroupedViews: 0,
        activeIncrementalGroupedViews: 0,
        activeViews: 0,
        groupedFullEvaluationCount: 0,
        groupedPatchedEvaluationCount: 0,
        activeSubscriptions: 0,
        queuedEvents: 0,
        maxQueueDepth: 0,
        backpressureEvents: 0,
        memoryBytes: 0,
        tombstoneCount: 0,
        compactionPending: false,
        kafkaLag: null,
        updatedAtNanos: 123n,
      },
    ]);
    expect(viewServerHealthSummaryFromHealth(healthWithoutKafka, 123n).maxKafkaLag).toBe(null);
    expect(viewServerHealthSummaryFromHealth(orphanKafkaHealth, 123n).maxKafkaLag).toBe(null);
    expect(viewServerHealthTopicRowsFromHealth(orphanKafkaHealth, 123n)).toStrictEqual(
      viewServerHealthTopicRowsFromHealth(healthWithoutKafka, 123n),
    );
    expect(
      viewServerHealthTopicRowsFromHealth(healthWithoutKafka, 123n).map((row) => [
        row.id,
        row.status,
      ]),
    ).toStrictEqual([
      ["orders", "ready"],
      ["trades", "degraded"],
      ["positions", "starting"],
    ]);
    expect(
      viewServerHealthTopicRowsFromHealth(kafkaStartingHealth, 123n).map((row) => [
        row.id,
        row.status,
      ]),
    ).toStrictEqual([
      ["orders", "ready"],
      ["trades", "starting"],
      ["positions", "ready"],
    ]);
    expect(viewServerHealthSummaryFromHealth(kafkaStartingHealth, 123n)).toStrictEqual({
      status: "starting",
      runtimeStatus: "starting",
      connectionStatus: "connected",
      unhealthyTopics: ["trades"],
      updatedAtNanos: 123n,
      maxKafkaLag: null,
    });
    expect(viewServerHealthSummaryFromHealth(grpcOnlyHealth, 123n)).toStrictEqual({
      status: "degraded",
      runtimeStatus: "degraded",
      connectionStatus: "connected",
      unhealthyTopics: ["trades", "positions"],
      updatedAtNanos: 123n,
      maxKafkaLag: null,
    });
    expect(
      viewServerHealthTopicRowsFromHealth(grpcOnlyHealth, 123n).map((row) => [row.id, row.status]),
    ).toStrictEqual([
      ["orders", "ready"],
      ["trades", "starting"],
      ["positions", "degraded"],
    ]);
    expect(stoppingRows.map((row) => row.status)).toStrictEqual([
      "stopping",
      "stopping",
      "stopping",
    ]);
    expect({
      summary: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
      detailed: VIEW_SERVER_HEALTH_TOPIC,
      detailedIsReserved: viewServerTopicNameIsReserved(VIEW_SERVER_HEALTH_TOPIC),
      ordersIsReserved: viewServerTopicNameIsReserved("orders"),
      all: viewServerReservedTopicNames,
    }).toStrictEqual({
      summary: "__view_server_health_summary",
      detailed: "__view_server_health",
      detailedIsReserved: true,
      ordersIsReserved: false,
      all: [VIEW_SERVER_HEALTH_SUMMARY_TOPIC, VIEW_SERVER_HEALTH_TOPIC],
    });
    expectTypeOf(summary).toEqualTypeOf<ViewServerHealthSummary<typeof viewServer.topics>>();
    expectTypeOf(summary.maxKafkaLag).toEqualTypeOf<bigint | null>();
    expectTypeOf(summaryRow).toEqualTypeOf<ViewServerHealthSummaryRow<typeof viewServer.topics>>();
    expectTypeOf(summaryRow.maxKafkaLag).toEqualTypeOf<bigint | null>();
    expectTypeOf(rows[0]).toEqualTypeOf<
      ViewServerHealthTopicRow<"orders" | "trades" | "positions"> | undefined
    >();
    expectTypeOf(rows[0]?.kafkaLag).toEqualTypeOf<bigint | null | undefined>();
    expectTypeOf(grpcOnlyHealth.grpc?.feeds.orders).toEqualTypeOf<
      GrpcTopicFeedsHealth<"orders"> | undefined
    >();
    expectTypeOf(grpcOnlyHealth.grpc?.feeds.trades).toEqualTypeOf<
      GrpcTopicFeedsHealth<"trades"> | undefined
    >();
    expectTypeOf(grpcOnlyHealth.grpc?.feeds.positions).toEqualTypeOf<
      GrpcTopicFeedsHealth<"positions"> | undefined
    >();
    expectTypeOf<ViewServerHealthDetails<"orders">["status"]>().toEqualTypeOf<
      "ready" | "degraded" | "starting" | "stopping" | "connecting" | "disconnected"
    >();
  });

  it("derives Kafka lag only from Kafka sources mapped to engine topics", () => {
    const baseHealth: ViewServerHealth<typeof viewServer.topics> = {
      status: "ready",
      version: 1,
      uptimeMs: 100,
      engine: {
        topics: {
          orders: runtimeTopicHealth("ready", 10),
          trades: runtimeTopicHealth("ready", 20),
          positions: runtimeTopicHealth("ready", 30),
        },
      },
      transport: {
        activeClients: 0,
        activeStreams: 0,
        activeSubscriptions: 0,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        queuedMessages: 0,
        queuedBytes: 0,
        droppedClients: 0,
        backpressureEvents: 0,
        reconnects: 0,
        lastError: null,
      },
    };
    const regionHealth = (consumerLagMessages: bigint | null): KafkaTopicRegionHealth => ({
      connected: true,
      assignedPartitions: 1,
      messagesPerSecond: 0,
      bytesPerSecond: 0,
      decodedMessagesPerSecond: 0,
      decodeFailuresPerSecond: 0,
      mappingFailuresPerSecond: 0,
      publishFailuresPerSecond: 0,
      commitFailuresPerSecond: 0,
      processingFailuresPerSecond: 0,
      lastMessageAt: null,
      lastCommitAt: null,
      consumerLagMessages,
      lagSampledAt: null,
      committedOffset: null,
      lastError: null,
    });
    const healthWithKafkaTopics = (
      topics: NonNullable<ViewServerHealth<typeof viewServer.topics>["kafka"]>["topics"],
    ): ViewServerHealth<typeof viewServer.topics> => ({
      ...baseHealth,
      kafka: {
        startFrom: kafkaStartFromHealth,
        regions: {
          usa: {
            status: "connected",
            brokers: "localhost:9092",
            lastConnectedAt: null,
            lastError: null,
          },
        },
        topics,
      },
    });
    const topicHealth = (
      viewServerTopic: string,
      consumerLagMessages: bigint | null,
    ): KafkaTopicHealth => ({
      status: "ready",
      sourceTopic: `${viewServerTopic}-source-${String(consumerLagMessages)}`,
      viewServerTopic,
      regions: {
        usa: regionHealth(consumerLagMessages),
      },
    });
    const lagSummary = (health: ViewServerHealth<typeof viewServer.topics>) => ({
      maxKafkaLag: viewServerHealthSummaryFromHealth(health, 123n).maxKafkaLag,
      topicLags: viewServerHealthTopicRowsFromHealth(health, 123n).map((row) => [
        row.id,
        row.kafkaLag,
      ]),
    });

    expect(lagSummary(baseHealth)).toStrictEqual({
      maxKafkaLag: null,
      topicLags: [
        ["orders", null],
        ["trades", null],
        ["positions", null],
      ],
    });
    expect(
      lagSummary(
        healthWithKafkaTopics({
          ordersZero: topicHealth("orders", 0n),
        }),
      ),
    ).toStrictEqual({
      maxKafkaLag: 0n,
      topicLags: [
        ["orders", 0n],
        ["trades", null],
        ["positions", null],
      ],
    });
    expect(
      lagSummary(
        healthWithKafkaTopics({
          ordersLow: topicHealth("orders", 2n),
          ordersHigh: topicHealth("orders", 8n),
          trades: topicHealth("trades", 5n),
        }),
      ),
    ).toStrictEqual({
      maxKafkaLag: 8n,
      topicLags: [
        ["orders", 8n],
        ["trades", 5n],
        ["positions", null],
      ],
    });
    expect(
      lagSummary(
        healthWithKafkaTopics({
          ordersKnown: topicHealth("orders", 0n),
          ordersUnknown: topicHealth("orders", null),
          trades: topicHealth("trades", 5n),
        }),
      ),
    ).toStrictEqual({
      maxKafkaLag: null,
      topicLags: [
        ["orders", null],
        ["trades", 5n],
        ["positions", null],
      ],
    });
    expect(
      lagSummary(
        healthWithKafkaTopics({
          orders: topicHealth("orders", 4n),
          orphanKnown: topicHealth("orphan", 99n),
          orphanUnknown: topicHealth("orphan", null),
        }),
      ),
    ).toStrictEqual({
      maxKafkaLag: 4n,
      topicLags: [
        ["orders", 4n],
        ["trades", null],
        ["positions", null],
      ],
    });
  });

  it("derives query result rows from select and grouped aggregates", () => {
    const assertQueryTypes = (useLiveQuery: LiveQueryCall<typeof viewServer.topics>) => {
      const selectedRawResult = useLiveQuery("orders", {
        select: ["id", "customerId", "status", "price", "region", "updatedAt"],
        where: {
          status: { eq: "open" },
        },
      });

      expectTypeOf(selectedRawResult).toEqualTypeOf<{
        readonly rows: ReadonlyArray<{
          readonly id: string;
          readonly customerId: string;
          readonly status: "open" | "closed" | "cancelled";
          readonly price: number;
          readonly region: string;
          readonly updatedAt: number;
        }>;
        readonly totalRows: number;
        readonly version: number;
        readonly status: "loading" | "ready" | "stale" | "closed" | "error";
        readonly statusCode?:
          | "Ready"
          | "SnapshotStale"
          | "SubscriptionClosed"
          | "TransportError"
          | "BackpressureExceeded"
          | "InvalidTopic"
          | "InvalidRow"
          | "InvalidQuery"
          | "UnsupportedQuery"
          | "RuntimeUnavailable"
          | "RuntimeResetFailed"
          | undefined;
        readonly message?: string | undefined;
      }>();

      const selectedResult = useLiveQuery("orders", {
        select: ["customerId", "status", "updatedAt"],
        where: {
          customerId: { startsWith: "customer-" },
          status: "open",
          updatedAt: { gte: 1, lte: 10 },
        },
      });

      expectTypeOf(selectedResult).toEqualTypeOf<{
        readonly rows: ReadonlyArray<{
          readonly customerId: string;
          readonly status: "open" | "closed" | "cancelled";
          readonly updatedAt: number;
        }>;
        readonly totalRows: number;
        readonly version: number;
        readonly status: "loading" | "ready" | "stale" | "closed" | "error";
        readonly statusCode?:
          | "Ready"
          | "SnapshotStale"
          | "SubscriptionClosed"
          | "TransportError"
          | "BackpressureExceeded"
          | "InvalidTopic"
          | "InvalidRow"
          | "InvalidQuery"
          | "UnsupportedQuery"
          | "RuntimeUnavailable"
          | "RuntimeResetFailed"
          | undefined;
        readonly message?: string | undefined;
      }>();

      const rawRows = useLiveQuery("orders", {
        select: ["id", "price"],
        where: {
          status: "open",
        },
        orderBy: [{ field: "price", direction: "desc" }],
        limit: 50,
      }).rows;

      const groupedRows = useLiveQuery("orders", {
        groupBy: ["status"],
        aggregates: {
          count: { aggFunc: "count" },
          totalPrice: { aggFunc: "sum", field: "price" },
          averageUpdatedAt: { aggFunc: "avg", field: "updatedAt" },
          firstStatus: { aggFunc: "min", field: "status" },
        },
        where: {
          region: "london",
        },
        orderBy: [
          { aggregate: "totalPrice", direction: "desc" },
          { field: "status", direction: "asc" },
        ],
      }).rows;

      expectTypeOf(rawRows).toEqualTypeOf<
        ReadonlyArray<{ readonly id: string; readonly price: number }>
      >();
      type GroupedRow = (typeof groupedRows)[number];
      expectTypeOf<GroupedRow>().toEqualTypeOf<{
        readonly status: "open" | "closed" | "cancelled";
        readonly count: bigint;
        readonly totalPrice: BigDecimal.BigDecimal;
        readonly averageUpdatedAt: BigDecimal.BigDecimal;
        readonly firstStatus: "open" | "closed" | "cancelled";
      }>();

      const singleAggregateResult = useLiveQuery("orders", {
        groupBy: ["region"],
        aggregates: { uniqueCustomers: { aggFunc: "countDistinct", field: "customerId" } },
      });

      expectTypeOf(singleAggregateResult).toEqualTypeOf<{
        readonly rows: ReadonlyArray<{
          readonly region: string;
          readonly uniqueCustomers: bigint;
        }>;
        readonly totalRows: number;
        readonly version: number;
        readonly status: "loading" | "ready" | "stale" | "closed" | "error";
        readonly statusCode?:
          | "Ready"
          | "SnapshotStale"
          | "SubscriptionClosed"
          | "TransportError"
          | "BackpressureExceeded"
          | "InvalidTopic"
          | "InvalidRow"
          | "InvalidQuery"
          | "UnsupportedQuery"
          | "RuntimeUnavailable"
          | "RuntimeResetFailed"
          | undefined;
        readonly message?: string | undefined;
      }>();

      const positionRows = useLiveQuery("positions", {
        select: ["id", "price", "quantity"],
        where: {
          accountId: { startsWith: "acct-" },
          active: true,
          quantity: { gte: 1n, lte: 100n },
          price: { gt: decimal("10.00") },
          notional: { lt: 1_000_000 },
        },
        orderBy: [
          { field: "price", direction: "desc" },
          { field: "quantity", direction: "asc" },
        ],
      }).rows;

      expectTypeOf(positionRows).toEqualTypeOf<
        ReadonlyArray<{
          readonly id: string;
          readonly price: BigDecimal.BigDecimal;
          readonly quantity: bigint;
        }>
      >();

      const groupedPositionRows = useLiveQuery("positions", {
        groupBy: ["accountId", "active"],
        aggregates: {
          rowCount: { aggFunc: "count" },
          symbolCount: { aggFunc: "countDistinct", field: "symbol" },
          totalQuantity: { aggFunc: "sum", field: "quantity" },
          totalPrice: { aggFunc: "sum", field: "price" },
          totalNotional: { aggFunc: "sum", field: "notional" },
          averagePrice: { aggFunc: "avg", field: "price" },
          firstAccountId: { aggFunc: "min", field: "accountId" },
          maxQuantity: { aggFunc: "max", field: "quantity" },
        },
        orderBy: [
          { aggregate: "totalQuantity", direction: "desc" },
          { field: "accountId", direction: "asc" },
        ],
      }).rows;

      expectTypeOf<(typeof groupedPositionRows)[number]>().toEqualTypeOf<{
        readonly accountId: string;
        readonly active: boolean;
        readonly rowCount: bigint;
        readonly symbolCount: bigint;
        readonly totalQuantity: bigint;
        readonly totalPrice: BigDecimal.BigDecimal;
        readonly totalNotional: BigDecimal.BigDecimal;
        readonly averagePrice: BigDecimal.BigDecimal;
        readonly firstAccountId: string;
        readonly maxQuantity: bigint;
      }>();

      const optionalNumericSumQuery = {
        groupBy: ["accountId"],
        aggregates: {
          totalOptionalQuantity: { aggFunc: "sum", field: "optionalQuantity" },
        },
      } satisfies {
        readonly groupBy: readonly ["accountId"];
        readonly aggregates: {
          readonly totalOptionalQuantity: {
            readonly aggFunc: "sum";
            readonly field: "optionalQuantity";
          };
        };
      };
      // @ts-expect-error optional numeric fields cannot be summed without an explicit non-null mapping.
      useLiveQuery("positions", optionalNumericSumQuery);

      const optionalNumberSumQuery = {
        groupBy: ["accountId"],
        aggregates: {
          totalOptionalNotional: { aggFunc: "sum", field: "optionalNotional" },
        },
      } satisfies {
        readonly groupBy: readonly ["accountId"];
        readonly aggregates: {
          readonly totalOptionalNotional: {
            readonly aggFunc: "sum";
            readonly field: "optionalNotional";
          };
        };
      };
      // @ts-expect-error optional numeric fields cannot be summed without an explicit non-null mapping.
      useLiveQuery("positions", optionalNumberSumQuery);

      const dynamicAggregateAlias: string = "dynamicTotal";
      const dynamicAggregateQuery = {
        groupBy: ["status"],
        aggregates: {
          [dynamicAggregateAlias]: { aggFunc: "sum", field: "price" },
        },
      } satisfies {
        readonly groupBy: readonly ["status"];
        readonly aggregates: {
          readonly [key: string]: {
            readonly aggFunc: "sum";
            readonly field: "price";
          };
        };
      };
      // @ts-expect-error aggregate aliases must be literal object keys.
      const _invalidDynamicAggregateAlias: ExactGroupedQuery<
        typeof Order.Type,
        typeof dynamicAggregateQuery
      > &
        ValidateLiveQuery<typeof dynamicAggregateQuery> = dynamicAggregateQuery;

      void _invalidDynamicAggregateAlias;
    };

    expect(assertQueryTypes).toBeTypeOf("function");
  });

  it.effect("infers and decodes Kafka mapping callback parameters through the topic helper", () =>
    Effect.gen(function* () {
      const topic = kafkaTopic({
        regions: ["usa", "london"],
        value: kafka.protobuf(ordersValueSchema),
        viewServerTopic: "orders",
        mapping: ({ key, value, region }) => {
          expectTypeOf(key).toEqualTypeOf<string>();
          expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
          expectTypeOf(region).toEqualTypeOf<"usa" | "london">();
          return {
            id: key,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region,
            updatedAt: value.updatedAt,
          };
        },
      });

      expect(topic.viewServerTopic).toBe("orders");
      expect(
        yield* decodeKafkaTopicMessage(topic, {
          keyBytes: textEncoder.encode("order-1"),
          valueBytes: toBinary(
            ordersValueSchema,
            create(ordersValueSchema, {
              customerId: "customer-1",
              status: "open",
              price: 42,
              updatedAt: 1,
            }),
          ),
          region: "london",
          metadata: kafkaTestMetadata("london"),
        }),
      ).toStrictEqual({
        viewServerTopic: "orders",
        row: {
          id: "order-1",
          customerId: "customer-1",
          status: "open",
          price: 42,
          region: "london",
          updatedAt: 1,
        },
      });
      const keyedTopic = kafkaTopic({
        regions: ["usa"],
        value: kafka.protobuf(ordersValueSchema),
        key: kafka.protobuf(ordersKeySchema),
        viewServerTopic: "orders",
        mapping: ({ key, value, region }) => {
          expectTypeOf(key).toEqualTypeOf<OrdersKeyMessage>();
          expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
          expectTypeOf(region).toEqualTypeOf<"usa">();
          return {
            id: key.orderId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region,
            updatedAt: value.updatedAt,
          };
        },
      });

      expect(keyedTopic.key.descriptor).toBe(ordersKeySchema);
      const invalidKeyedTopicRegion = decodeKafkaTopicMessage(keyedTopic, {
        keyBytes: toBinary(
          ordersKeySchema,
          create(ordersKeySchema, {
            orderId: "order-keyed-london",
          }),
        ),
        valueBytes: toBinary(
          ordersValueSchema,
          create(ordersValueSchema, {
            customerId: "customer-keyed-london",
            status: "closed",
            price: 84,
            updatedAt: 2,
          }),
        ),
        // @ts-expect-error keyed topic is configured only for usa.
        region: "london",
        metadata: kafkaTestMetadata("usa"),
      });
      expect(
        yield* decodeKafkaTopicMessage(keyedTopic, {
          keyBytes: toBinary(
            ordersKeySchema,
            create(ordersKeySchema, {
              orderId: "order-keyed-1",
            }),
          ),
          valueBytes: toBinary(
            ordersValueSchema,
            create(ordersValueSchema, {
              customerId: "customer-keyed-1",
              status: "closed",
              price: 84,
              updatedAt: 2,
            }),
          ),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
        }),
      ).toStrictEqual({
        viewServerTopic: "orders",
        row: {
          id: "order-keyed-1",
          customerId: "customer-keyed-1",
          status: "closed",
          price: 84,
          region: "usa",
          updatedAt: 2,
        },
      });

      const stringKeyedTopic = kafkaTopic({
        regions: ["usa"],
        value: kafka.protobuf(ordersValueSchema),
        key: kafka.stringKey(),
        viewServerTopic: "orders",
        mapping: ({ key, value, region }) => {
          expectTypeOf(key).toEqualTypeOf<string>();
          expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
          expectTypeOf(region).toEqualTypeOf<"usa">();
          return {
            id: key,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region,
            updatedAt: value.updatedAt,
          };
        },
      });
      expect(stringKeyedTopic.key.format).toBe("string");

      expectTypeOf(invalidKeyedTopicRegion).not.toBeAny();

      const jsonPositionTopic = kafkaTopic({
        regions: ["usa"],
        value: kafka.json(Position),
        viewServerTopic: "positions",
        mapping: ({ key, value, region }) => {
          expectTypeOf(key).toEqualTypeOf<string>();
          expectTypeOf(value).toEqualTypeOf<typeof Position.Type>();
          expectTypeOf(region).toEqualTypeOf<"usa">();
          return {
            ...value,
            id: key,
          };
        },
      });
      const decodedJsonPosition = yield* decodeKafkaTopicMessage(jsonPositionTopic, {
        keyBytes: textEncoder.encode("position-json-1"),
        valueBytes: textEncoder.encode(
          JSON.stringify({
            id: "ignored-source-id",
            accountId: "account-json-1",
            symbol: "AAPL",
            active: true,
            quantity: "9007199254740993",
            optionalQuantity: "9007199254740995",
            price: "1234567890.123456789",
            notional: 10,
            optionalNotional: 20,
          }),
        ),
        region: "usa",
        metadata: kafkaTestMetadata("usa"),
      });
      expect(BigDecimal.isBigDecimal(decodedJsonPosition.row.price)).toBe(true);
      const decodedJsonPositionPrice = yield* Schema.decodeUnknownEffect(Schema.BigDecimal)(
        decodedJsonPosition.row.price,
      );
      expect({
        ...decodedJsonPosition,
        row: {
          ...decodedJsonPosition.row,
          price: BigDecimal.format(decodedJsonPositionPrice),
        },
      }).toStrictEqual({
        viewServerTopic: "positions",
        row: {
          id: "position-json-1",
          accountId: "account-json-1",
          symbol: "AAPL",
          active: true,
          quantity: 9007199254740993n,
          optionalQuantity: 9007199254740995n,
          price: "1234567890.123456789",
          notional: 10,
          optionalNotional: 20,
        },
      });

      const throwingTopic = kafkaTopic({
        regions: ["usa"],
        value: kafka.protobuf(ordersValueSchema),
        viewServerTopic: "orders",
        mapping: () => {
          throw new Error("mapper failed");
        },
      });
      const mappingFailure = yield* Effect.flip(
        decodeKafkaTopicMessage(throwingTopic, {
          keyBytes: textEncoder.encode("order-throws"),
          valueBytes: toBinary(
            ordersValueSchema,
            create(ordersValueSchema, {
              customerId: "customer-throws",
              status: "open",
              price: 1,
              updatedAt: 1,
            }),
          ),
          region: "usa",
          metadata: kafkaTestMetadata("usa"),
        }),
      );
      expect({
        mappingFailure: kafkaErrorIsMapping(mappingFailure),
        forgedMappingFailure: kafkaErrorIsMapping({
          _tag: "KafkaMappingError",
          message: "forged",
        }),
      }).toStrictEqual({
        mappingFailure: true,
        forgedMappingFailure: false,
      });
    }),
  );

  it("supports json and custom Kafka source codecs without weakening mapping exactness", () => {
    const jsonTopic = kafkaTopic({
      regions: ["usa"],
      value: kafka.json(Order),
      viewServerTopic: "orders",
      mapping: ({ key, value, region }) => {
        expectTypeOf(key).toEqualTypeOf<string>();
        expectTypeOf(value).toEqualTypeOf<typeof Order.Type>();
        expectTypeOf(region).toEqualTypeOf<"usa">();
        return value;
      },
    });

    const customTopic = kafkaTopic({
      regions: ["london"],
      value: kafka.codec({
        name: "trade-json-lines",
        decode: (): Effect.Effect<
          {
            readonly tradeId: string;
            readonly symbol: string;
            readonly quantity: number;
            readonly price: number;
          },
          never
        > =>
          Effect.succeed({
            tradeId: "trade-1",
            symbol: "AAPL",
            quantity: 10,
            price: 42,
          }),
      }),
      viewServerTopic: "trades",
      mapping: ({ key, value, region }) => {
        expectTypeOf(key).toEqualTypeOf<string>();
        expectTypeOf(value).toEqualTypeOf<{
          readonly tradeId: string;
          readonly symbol: string;
          readonly quantity: number;
          readonly price: number;
        }>();
        expectTypeOf(region).toEqualTypeOf<"london">();
        return {
          id: value.tradeId,
          symbol: value.symbol,
          quantity: value.quantity,
          price: value.price,
          region,
        };
      },
    });

    expect(jsonTopic.value.format).toBe("json");
    expect(customTopic.value.format).toBe("custom");
  });

  it("keeps real Protobuf-ES v2 generated schema inference typechecked", () => {
    expect(assertGeneratedSchemaContracts).toBeTypeOf("function");
  });
});

const assertGeneratedSchemaContracts = () => {
  const keyedTopic = kafkaTopic({
    regions: ["usa", "london"],
    value: kafka.protobuf(generatedOrdersValueSchema),
    key: kafka.protobuf(generatedOrdersKeySchema),
    viewServerTopic: "orders",
    mapping: ({ key, value, region }) => {
      expectTypeOf(key).toEqualTypeOf<
        Message<"viewserver.test.OrderKey"> & { readonly orderId: string }
      >();
      expectTypeOf(value).toEqualTypeOf<
        Message<"viewserver.test.OrderValue"> & {
          readonly customerId: string;
          readonly status: "open" | "closed" | "cancelled";
          readonly price: number;
          readonly updatedAt: number;
        }
      >();
      expectTypeOf(region).toEqualTypeOf<"usa" | "london">();
      return {
        id: key.orderId,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region,
        updatedAt: value.updatedAt,
      };
    },
  });

  expectTypeOf<KafkaCodecType<typeof keyedTopic.key>>().toEqualTypeOf<
    Message<"viewserver.test.OrderKey"> & {
      readonly orderId: string;
    }
  >();

  kafkaTopic({
    regions: ["usa", "london"],
    value: kafka.protobuf(generatedOrdersValueSchema),
    viewServerTopic: "orders",
    mapping: ({ key, value, region }) => {
      expectTypeOf(key).toEqualTypeOf<string>();
      expectTypeOf(value).toEqualTypeOf<
        Message<"viewserver.test.OrderValue"> & {
          readonly customerId: string;
          readonly status: "open" | "closed" | "cancelled";
          readonly price: number;
          readonly updatedAt: number;
        }
      >();
      expectTypeOf(region).toEqualTypeOf<"usa" | "london">();
      return {
        id: key,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region,
        updatedAt: value.updatedAt,
      };
    },
  });
};

const assertCompileTimeContracts = () => {
  const localKafkaRegions = {
    usa: "broker-a:9092",
  };
  const localKafkaTopic = viewServer.kafkaTopic<typeof localKafkaRegions>();
  const londonKafkaRegions = {
    london: "broker-b:9092",
  };
  const londonKafkaTopic = viewServer.kafkaTopic<typeof londonKafkaRegions>()({
    regions: ["london"],
    value: kafka.protobuf(ordersValueSchema),
    viewServerTopic: "orders",
    mapping: ({ key, value, region }) => ({
      id: key,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region,
      updatedAt: value.updatedAt,
    }),
  });
  const validLocalOrdersTopic = localKafkaTopic({
    regions: ["usa"],
    value: kafka.protobuf(ordersValueSchema),
    viewServerTopic: "orders",
    mapping: ({ key, value, region }) => ({
      id: key,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region,
      updatedAt: value.updatedAt,
    }),
  });
  const validKeyedLocalOrdersTopic = localKafkaTopic({
    regions: ["usa"],
    value: kafka.protobuf(ordersValueSchema),
    key: kafka.protobuf(ordersKeySchema),
    viewServerTopic: "orders",
    mapping: ({ key, value, region }) => ({
      id: key.orderId,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region,
      updatedAt: value.updatedAt,
    }),
  });
  const spreadValueMismatchTopic = {
    ...validLocalOrdersTopic,
    value: kafka.string(),
  };
  const spreadKeyMismatchTopic = {
    ...validKeyedLocalOrdersTopic,
    key: kafka.stringKey(),
  };
  const spreadMappingMismatchTopic = {
    ...validLocalOrdersTopic,
    mapping: (): typeof Trade.Type => ({
      id: "trade-1",
      symbol: "AAPL",
      quantity: 1,
      price: 42,
      region: "usa",
    }),
  };
  const spreadTargetMismatchTopic = {
    ...validLocalOrdersTopic,
    viewServerTopic: "trades",
  };
  type UnsafeJsonParseResult = ReturnType<typeof JSON.parse>;
  const unsafeValueCodec: KafkaCodec<UnsafeJsonParseResult> = kafka.bytes();
  const unsafeErrorCodec: KafkaCodec<string, UnsafeJsonParseResult> = kafka.string();
  const unknownErrorCodec: KafkaCodec<string, unknown> = kafka.string();

  // @ts-expect-error protobuf descriptors cannot be inferred from any
  kafka.protobuf(JSON.parse("{}"));

  // @ts-expect-error json schemas cannot be inferred from any
  kafka.json(JSON.parse("{}"));

  // @ts-expect-error custom Kafka codec values cannot infer any
  kafka.codec({
    name: "unsafe-effect-json-parse",
    decode: () => Effect.succeed(JSON.parse("{}")),
  });

  // @ts-expect-error custom Kafka codec errors cannot infer any
  kafka.codec({
    name: "unsafe-effect-json-parse-error",
    decode: () => Effect.fail(JSON.parse("{}")),
  });

  localKafkaTopic({
    regions: ["usa"],
    // @ts-expect-error Kafka value codecs cannot be inferred from any
    value: JSON.parse("{}"),
    viewServerTopic: "orders",
    mapping: (): typeof Order.Type => ({
      id: "order-1",
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    }),
  });

  expectTypeOf<
    KafkaTopicDefinition<
      typeof viewServer.topics,
      typeof localKafkaRegions,
      "orders",
      typeof ordersValueKafkaCodec,
      any,
      readonly ["usa"]
    >
  >().toEqualTypeOf<never>();

  localKafkaTopic({
    regions: ["usa"],
    // @ts-expect-error Kafka value codecs cannot be widened to KafkaCodec<any>
    value: unsafeValueCodec,
    viewServerTopic: "orders",
    mapping: (): typeof Order.Type => ({
      id: "order-1",
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    }),
  });

  localKafkaTopic({
    regions: ["usa"],
    // @ts-expect-error Kafka codec error channels cannot be widened to any
    value: unsafeErrorCodec,
    viewServerTopic: "orders",
    mapping: (): typeof Order.Type => ({
      id: "order-1",
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    }),
  });

  localKafkaTopic({
    regions: ["usa"],
    // @ts-expect-error Kafka codec error channels cannot be widened to unknown
    value: unknownErrorCodec,
    viewServerTopic: "orders",
    mapping: (): typeof Order.Type => ({
      id: "order-1",
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    }),
  });

  expectTypeOf<
    KafkaMappingInput<
      typeof viewServer.topics,
      "orders",
      "usa" | "london",
      typeof ordersValueKafkaCodec,
      typeof ordersKeyKafkaCodec
    >["key"]
  >().toEqualTypeOf<OrdersKeyMessage>();
  expectTypeOf<
    KafkaMappingInput<
      typeof viewServer.topics,
      "orders",
      "usa" | "london",
      typeof ordersValueKafkaCodec,
      typeof ordersKeyKafkaCodec
    >["value"]
  >().toEqualTypeOf<OrdersValueMessage>();
  expectTypeOf<
    KafkaMappingInput<
      typeof viewServer.topics,
      "orders",
      "usa" | "london",
      typeof ordersValueKafkaCodec,
      typeof ordersKeyKafkaCodec
    >["region"]
  >().toEqualTypeOf<"usa" | "london">();
  expectTypeOf<
    KafkaMappingInput<
      typeof viewServer.topics,
      "orders",
      "usa" | "london",
      typeof ordersValueKafkaCodec,
      typeof ordersKeyKafkaCodec
    >["schema"]
  >().toEqualTypeOf<typeof Order>();
  expectTypeOf<
    KafkaMappingInput<
      typeof viewServer.topics,
      "orders",
      "usa" | "london",
      typeof ordersValueKafkaCodec,
      typeof ordersKeyKafkaCodec
    >["metadata"]["sourceRegion"]
  >().toEqualTypeOf<"usa" | "london">();
  expectTypeOf<
    KafkaMappingInput<
      typeof viewServer.topics,
      "trades",
      "usa",
      typeof tradesValueKafkaCodec,
      undefined
    >["key"]
  >().toEqualTypeOf<string>();
  expectTypeOf<
    KafkaMappingInput<
      typeof viewServer.topics,
      "trades",
      "usa",
      typeof tradesValueKafkaCodec,
      undefined
    >["value"]
  >().toEqualTypeOf<TradesValueMessage>();
  expectTypeOf<
    KafkaMappingInput<
      typeof viewServer.topics,
      "trades",
      "usa",
      typeof tradesValueKafkaCodec,
      undefined
    >["region"]
  >().toEqualTypeOf<"usa">();
  expectTypeOf<
    KafkaMappingInput<
      typeof viewServer.topics,
      "trades",
      "usa",
      typeof tradesValueKafkaCodec,
      undefined
    >["schema"]
  >().toEqualTypeOf<typeof Trade>();

  const assertRuntimeContracts = (runtime: ViewServerRuntimeClient<typeof viewServer.topics>) => {
    const publishEffect = runtime.publish("orders", {
      id: "order-1",
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      updatedAt: 1,
    });
    const snapshotEffect = runtime.snapshot("orders", {
      select: ["id"],
      where: {
        status: "open",
      },
    });
    const patchEffect = runtime.patch("orders", "order-1", {
      price: 43,
      status: "closed",
    });

    expectTypeOf<Effect.Error<typeof publishEffect>>().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<Effect.Error<typeof snapshotEffect>>().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<Effect.Error<typeof patchEffect>>().toEqualTypeOf<ViewServerRuntimeError>();

    const invalidPublishWrongField = runtime.publish("orders", {
      id: "order-1",
      customerId: "customer-1",
      status: "open",
      price: 42,
      region: "usa",
      // @ts-expect-error publish rows must match the topic schema
      updatedAt: "not-a-number",
    });

    const invalidPublishMissingField = runtime.publish("trades", {
      id: "trade-1",
      symbol: "AAPL",
      quantity: 1,
      price: 42,
      // @ts-expect-error publish rows must include all required topic fields
      updatedAt: 1,
    });

    const invalidPublishTopic = runtime.publish(
      // @ts-expect-error runtime publish topics are constrained to configured topics
      "customers",
      {
        id: "customer-1",
      },
    );

    const invalidPatchField = runtime.patch("orders", "order-1", {
      // @ts-expect-error patch fields must belong to the selected topic row
      missing: true,
    });

    const invalidPatchValue = runtime.patch("orders", "order-1", {
      // @ts-expect-error patch field values must match the selected topic row
      price: "not-a-number",
    });

    const invalidSnapshotTopic = runtime.snapshot(
      // @ts-expect-error snapshot topics are constrained to configured topics
      "customers",
      {},
    );

    const invalidSnapshotFilter = runtime.snapshot("orders", {
      // @ts-expect-error invalid query collapse keeps selected fields from being accepted
      select: ["id"],
      where: {
        // @ts-expect-error snapshot filters must use values from the selected topic row
        price: "not-a-number",
      },
    });
    expectTypeOf<
      Effect.Error<typeof invalidPublishWrongField>
    >().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<
      Effect.Error<typeof invalidPublishMissingField>
    >().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<
      Effect.Error<typeof invalidPublishTopic>
    >().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<Effect.Error<typeof invalidPatchField>>().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<Effect.Error<typeof invalidPatchValue>>().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<
      Effect.Error<typeof invalidSnapshotTopic>
    >().toEqualTypeOf<ViewServerRuntimeError>();
    expectTypeOf<
      Effect.Error<typeof invalidSnapshotFilter>
    >().toEqualTypeOf<ViewServerRuntimeError>();
  };

  expectTypeOf(assertRuntimeContracts).toBeFunction();
  expectTypeOf<ViewServerBackpressureError>().toMatchTypeOf<ViewServerRuntimeError>();

  defineViewServerConfig({
    topics: {
      invalid: {
        schema: Order,
        // @ts-expect-error topic keys must be string fields from the Effect Schema row type
        key: "missing",
      },
    },
  });

  defineViewServerConfig({
    topics: {
      loose: {
        // @ts-expect-error topic schemas must expose concrete fields for query typing and wire validation
        schema: Schema.Record(Schema.String, Schema.String),
        // @ts-expect-error non-field schemas cannot provide a valid string row key
        key: "id",
      },
    },
  });

  defineViewServerConfig({
    topics: {
      // @ts-expect-error system health topic names are reserved
      __view_server_health: {
        schema: Order,
        key: "id",
      },
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    // @ts-expect-error runtime options reject unknown top-level fields
    extraRuntimeField: true,
    kafka: {
      consumerGroupId: "view-server-type-test",
      regions: {
        usa: "broker-a:9092",
      },
      topics: {},
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    // @ts-expect-error runtime options must include Kafka topic definitions
    kafka: {
      consumerGroupId: "view-server-type-test",
      regions: {
        usa: "broker-a:9092",
      },
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "view-server-type-test",
      startFrom: "latest",
      regions: {
        usa: "broker-a:9092",
      },
      topics: {},
      // @ts-expect-error runtime kafka options reject unknown fields
      extraKafkaField: true,
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "view-server-type-test",
      regions: localKafkaRegions,
      topics: {
        // @ts-expect-error Kafka source topics must be created with viewServer.kafkaTopic
        orders: {
          regions: ["usa"],
          value: kafka.protobuf(ordersValueSchema),
          viewServerTopic: "orders",
          mapping: () => ({
            id: "order-1",
            customerId: "customer-1",
            status: "open",
            price: 42,
            region: "usa",
            updatedAt: 1,
          }),
        },
      },
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "view-server-type-test",
      regions: localKafkaRegions,
      topics: {
        // @ts-expect-error spread-mutated Kafka topic values must still match mapping input types
        orders: spreadValueMismatchTopic,
      },
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "view-server-type-test",
      regions: localKafkaRegions,
      topics: {
        // @ts-expect-error spread-mutated Kafka topic keys must still match mapping input types
        orders: spreadKeyMismatchTopic,
      },
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "view-server-type-test",
      regions: localKafkaRegions,
      topics: {
        // @ts-expect-error spread-mutated Kafka mappings must still return the target topic row
        orders: spreadMappingMismatchTopic,
      },
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "view-server-type-test",
      regions: localKafkaRegions,
      topics: {
        // @ts-expect-error spread-mutated Kafka target topics must still match the mapping row
        orders: spreadTargetMismatchTopic,
      },
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "view-server-type-test",
      regions: localKafkaRegions,
      topics: {
        // @ts-expect-error Kafka topic helper regions must match runtime kafka.regions
        orders: londonKafkaTopic,
      },
    },
  });

  // @ts-expect-error Kafka topic regions are constrained to kafka.regions keys
  localKafkaTopic({ regions: ["USA"] });

  // @ts-expect-error Kafka topic regions must be non-empty
  localKafkaTopic({ regions: [] });

  // @ts-expect-error Kafka mappings must target a configured View Server topic
  localKafkaTopic({ viewServerTopic: "customers" });

  const invalidExtraKafkaTopicField: KafkaTopicDefinition<
    typeof viewServer.topics,
    typeof localKafkaRegions,
    "orders",
    typeof ordersValueKafkaCodec,
    undefined,
    readonly ["usa"]
  > = {
    regions: ["usa"],
    value: kafka.protobuf(ordersValueSchema),
    viewServerTopic: "orders",
    // @ts-expect-error Kafka topic definitions reject unknown topic contract fields
    extraTopicField: true,
    mapping: ({ key, value, region }) => ({
      id: key,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region,
      updatedAt: value.updatedAt,
    }),
  };

  expectTypeOf(invalidExtraKafkaTopicField.viewServerTopic).toEqualTypeOf<"orders">();

  localKafkaTopic({
    regions: ["usa"],
    value: kafka.protobuf(ordersValueSchema),
    // @ts-expect-error unsupported Kafka key codecs must fail instead of inferring unknown
    key: {},
    viewServerTopic: "orders",
    mapping: ({ value, region }) => ({
      id: "order-1",
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region,
      updatedAt: value.updatedAt,
    }),
  });

  localKafkaTopic({
    regions: ["usa"],
    value: kafka.json(OrderWithExtraSourceField),
    viewServerTopic: "orders",
    // @ts-expect-error returning source JSON value directly rejects fields outside the target row
    mapping: ({ value }) => value,
  });

  localKafkaTopic({
    regions: ["usa"],
    value: kafka.protobuf(ordersValueSchema),
    key: kafka.stringKey(),
    viewServerTopic: "orders",
    // @ts-expect-error unannotated mapping returns must match the target View Server topic row
    mapping: ({ key, value, region }) => ({
      id: key,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region,
    }),
  });

  localKafkaTopic({
    regions: ["usa"],
    value: kafka.protobuf(ordersValueSchema),
    key: kafka.stringKey(),
    viewServerTopic: "orders",
    // @ts-expect-error unannotated mapping returns reject extra fields outside the target row
    mapping: ({ key, value, region }) => ({
      id: key,
      customerId: value.customerId,
      status: value.status,
      price: value.price,
      region,
      updatedAt: value.updatedAt,
      ze: true,
    }),
  });

  localKafkaTopic({
    regions: ["usa"],
    value: kafka.protobuf(ordersValueSchema),
    key: kafka.protobuf(ordersKeySchema),
    viewServerTopic: "orders",
    mapping: ({ key, value, schema, metadata }) => {
      expectTypeOf(key).toEqualTypeOf<OrdersKeyMessage>();
      expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
      expectTypeOf(schema).toEqualTypeOf<typeof Order>();
      expectTypeOf(metadata.sourceRegion).toEqualTypeOf<"usa">();
      expectTypeOf(metadata.headers).toEqualTypeOf<
        Readonly<Record<string, string | Uint8Array | ReadonlyArray<string | Uint8Array>>>
      >();
      return {
        id: key.orderId,
        customerId: value.customerId,
        status: value.status,
        price: value.price,
        region: metadata.sourceRegion,
        updatedAt: value.updatedAt,
      };
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "view-server-type-test",
      startFrom: {
        committedConsumerGroup: "view-server-existing-group",
        fallback: "fail",
      },
      regions: {
        usa: "broker-a:9092",
      },
      topics: {},
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "view-server-type-test",
      // @ts-expect-error committed Kafka start config requires committedConsumerGroup.
      startFrom: {
        fallback: "earliest",
      },
      regions: {
        usa: "broker-a:9092",
      },
      topics: {},
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "view-server-type-test",
      // @ts-expect-error runtime Kafka startFrom only accepts earliest, latest, or committed group config.
      startFrom: "middle",
      regions: {
        usa: "broker-a:9092",
      },
      topics: {},
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "view-server-type-test",
      startFrom: {
        committedConsumerGroup: "view-server-existing-group",
        // @ts-expect-error committed Kafka start fallback must be earliest, latest, or fail.
        fallback: "middle",
      },
      regions: {
        usa: "broker-a:9092",
      },
      topics: {},
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "view-server-type-test",
      startFrom: {
        committedConsumerGroup: "view-server-existing-group",
        // @ts-expect-error committed Kafka start config rejects unknown keys.
        committedConsumerGroupId: "view-server-typo",
      },
      regions: {
        usa: "broker-a:9092",
      },
      topics: {
        orders: localKafkaTopic({
          regions: ["usa"],
          value: kafka.protobuf(ordersValueSchema),
          key: kafka.stringKey(),
          viewServerTopic: "orders",
          // @ts-expect-error mapping return must match the target View Server topic row type
          mapping: ({ key, value, region }) => ({
            id: key,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region,
          }),
        }),
      },
    },
  });

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "view-server-type-test",
      regions: {
        usa: "broker-a:9092",
      },
      topics: {
        orders: localKafkaTopic({
          regions: ["usa"],
          value: kafka.protobuf(ordersValueSchema),
          key: kafka.stringKey(),
          viewServerTopic: "orders",
          // @ts-expect-error raw runtime topic mappings must return the target topic row
          mapping: ({ key, value, region }) => ({
            id: key,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region,
          }),
        }),
      },
    },
  });

  const assertLiveQueryContracts = (useLiveQuery: LiveQueryCall<typeof viewServer.topics>) => {
    // @ts-expect-error raw queries must explicitly select projected fields.
    useLiveQuery("orders", {
      where: { status: "open" },
    });

    const unknownWhereFieldQuery = {
      select: ["id"],
      where: {
        missing: "open",
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly missing: "open" };
    };
    // @ts-expect-error raw queries reject fields not present on the selected topic.
    useLiveQuery("orders", unknownWhereFieldQuery);

    const wrongFilterValueQuery = {
      select: ["id"],
      where: {
        price: "not-a-number",
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly price: "not-a-number" };
    };
    // @ts-expect-error filter values must match the selected field type.
    useLiveQuery("orders", wrongFilterValueQuery);

    const stringRangeFilterQuery = {
      select: ["id"],
      where: {
        status: { gte: "open" },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly status: { readonly gte: "open" } };
    };
    // @ts-expect-error string filters do not accept range operators.
    useLiveQuery("orders", stringRangeFilterQuery);

    const invalidStatusInFilter = {
      select: ["id"],
      where: {
        status: { in: ["open", "pending"] },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: {
        readonly status: {
          readonly in: readonly ["open", "pending"];
        };
      };
    };
    // @ts-expect-error filter arrays must contain selected field values
    const _invalidStatusInFilter: RawQuery<typeof Order.Type> &
      ExactRawQuery<typeof Order.Type, typeof invalidStatusInFilter> = invalidStatusInFilter;

    const numericStartsWithFilterQuery = {
      select: ["id"],
      where: {
        price: { startsWith: "1" },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly price: { readonly startsWith: "1" } };
    };
    // @ts-expect-error number filters do not accept string-only operators.
    useLiveQuery("orders", numericStartsWithFilterQuery);

    const booleanRangeFilterQuery = {
      select: ["id"],
      where: {
        active: { gte: true },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly active: { readonly gte: true } };
    };
    // @ts-expect-error boolean filters do not accept range operators.
    useLiveQuery("positions", booleanRangeFilterQuery);

    const booleanStartsWithFilterQuery = {
      select: ["id"],
      where: {
        active: { startsWith: "t" },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly active: { readonly startsWith: "t" } };
    };
    // @ts-expect-error boolean filters do not accept string-only operators.
    useLiveQuery("positions", booleanStartsWithFilterQuery);

    const bigDecimalStringFilterQuery = {
      select: ["id"],
      where: {
        price: { gte: "10.00" },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly price: { readonly gte: "10.00" } };
    };
    // @ts-expect-error BigDecimal filters require BigDecimal values, not strings.
    useLiveQuery("positions", bigDecimalStringFilterQuery);

    const bigDecimalStartsWithFilterQuery = {
      select: ["id"],
      where: {
        price: { startsWith: "10" },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly price: { readonly startsWith: "10" } };
    };
    // @ts-expect-error BigDecimal filters do not accept string-only operators.
    useLiveQuery("positions", bigDecimalStartsWithFilterQuery);

    const bigintNumberFilterQuery = {
      select: ["id"],
      where: {
        quantity: { gte: 1 },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly quantity: { readonly gte: 1 } };
    };
    // @ts-expect-error bigint filters require bigint values, not numbers.
    useLiveQuery("positions", bigintNumberFilterQuery);

    const optionalNumericEqualityRows = useLiveQuery("positions", {
      select: ["id"],
      where: {
        optionalQuantity: { eq: 1n },
        optionalNotional: 100,
      },
    }).rows;
    expectTypeOf(optionalNumericEqualityRows).toEqualTypeOf<
      ReadonlyArray<{ readonly id: string }>
    >();

    const optionalBigintUndefinedFilterQuery = {
      select: ["id"],
      where: {
        optionalQuantity: undefined,
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly optionalQuantity: undefined };
    };
    // @ts-expect-error optional filters reject present undefined values.
    useLiveQuery("positions", optionalBigintUndefinedFilterQuery);

    const optionalBigintUnionFilterQuery = (optionalQuantity: bigint | undefined) =>
      ({
        select: ["id"],
        where: {
          optionalQuantity,
        },
      }) satisfies {
        readonly select: readonly ["id"];
        readonly where: { readonly optionalQuantity: bigint | undefined };
      };
    // @ts-expect-error optional filters reject unions that can contain undefined.
    useLiveQuery("positions", optionalBigintUnionFilterQuery(1n));

    const optionalBigintUndefinedEqualityFilterQuery = {
      select: ["id"],
      where: {
        optionalQuantity: { eq: undefined },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly optionalQuantity: { readonly eq: undefined } };
    };
    // @ts-expect-error optional equality filters reject present undefined values.
    useLiveQuery("positions", optionalBigintUndefinedEqualityFilterQuery);

    const optionalBigintUnionEqualityFilterQuery = (eq: bigint | undefined) =>
      ({
        select: ["id"],
        where: {
          optionalQuantity: { eq },
        },
      }) satisfies {
        readonly select: readonly ["id"];
        readonly where: { readonly optionalQuantity: { readonly eq: bigint | undefined } };
      };
    // @ts-expect-error optional equality filters reject unions that can contain undefined.
    useLiveQuery("positions", optionalBigintUnionEqualityFilterQuery(1n));

    const optionalBigintRangeFilterQuery = {
      select: ["id"],
      where: {
        optionalQuantity: { gte: 1n },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly optionalQuantity: { readonly gte: 1n } };
    };
    // @ts-expect-error optional numeric fields only support equality filters.
    useLiveQuery("positions", optionalBigintRangeFilterQuery);

    const optionalBigintEqualityWithRangeFilterQuery = {
      select: ["id"],
      where: {
        optionalQuantity: { eq: 1n, gte: 1n },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly optionalQuantity: { readonly eq: 1n; readonly gte: 1n } };
    };
    // @ts-expect-error optional numeric exact filters reject range operators even when equality is present.
    useLiveQuery("positions", optionalBigintEqualityWithRangeFilterQuery);

    const optionalNumberRangeFilterQuery = {
      select: ["id"],
      where: {
        optionalNotional: { lte: 100 },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly optionalNotional: { readonly lte: 100 } };
    };
    // @ts-expect-error optional numeric fields only support equality filters.
    useLiveQuery("positions", optionalNumberRangeFilterQuery);

    const optionalNumberEqualityWithRangeFilterQuery = {
      select: ["id"],
      where: {
        optionalNotional: { eq: 100, lte: 100 },
      },
    } satisfies {
      readonly select: readonly ["id"];
      readonly where: { readonly optionalNotional: { readonly eq: 100; readonly lte: 100 } };
    };
    // @ts-expect-error optional numeric exact filters reject range operators even when equality is present.
    useLiveQuery("positions", optionalNumberEqualityWithRangeFilterQuery);

    const unknownOrderByFieldQuery = {
      select: ["id"],
      orderBy: [{ field: "missing", direction: "asc" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly orderBy: readonly [{ readonly field: "missing"; readonly direction: "asc" }];
    };
    // @ts-expect-error orderBy fields are constrained to the selected topic row.
    useLiveQuery("orders", unknownOrderByFieldQuery);

    const invalidOrderByDirectionQuery = {
      select: ["id"],
      orderBy: [{ field: "price", direction: "ascending" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly orderBy: readonly [{ readonly field: "price"; readonly direction: "ascending" }];
    };
    // @ts-expect-error sort direction is constrained to asc or desc.
    useLiveQuery("orders", invalidOrderByDirectionQuery);

    const rawAggregateOrderByQuery = {
      select: ["id"],
      orderBy: [{ aggregate: "totalPrice", direction: "desc" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly orderBy: readonly [{ readonly aggregate: "totalPrice"; readonly direction: "desc" }];
    };
    // @ts-expect-error raw orderBy cannot reference aggregate aliases.
    useLiveQuery("orders", rawAggregateOrderByQuery);

    const invalidSelectedFields = {
      select: ["id", "missing"],
    } satisfies {
      readonly select: readonly ["id", "missing"];
    };
    // @ts-expect-error projected fields are constrained to the selected topic row
    useLiveQuery("orders", invalidSelectedFields);

    const invalidGroupByField = {
      groupBy: ["missing"],
      aggregates: { count: { aggFunc: "count" } },
    } satisfies {
      readonly groupBy: readonly ["missing"];
      readonly aggregates: {
        readonly count: {
          readonly aggFunc: "count";
        };
      };
    };
    // @ts-expect-error grouped queries reject groupBy fields not present on the topic row
    const _invalidGroupByField: ExactGroupedQuery<typeof Order.Type, typeof invalidGroupByField> =
      invalidGroupByField;

    const invalidGroupedSelect = {
      groupBy: ["status"],
      select: ["id"],
      aggregates: { count: { aggFunc: "count" } },
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly select: readonly ["id"];
      readonly aggregates: {
        readonly count: {
          readonly aggFunc: "count";
        };
      };
    };
    // @ts-expect-error grouped queries cannot select raw fields.
    const _invalidGroupedSelect: ExactGroupedQuery<typeof Order.Type, typeof invalidGroupedSelect> =
      invalidGroupedSelect;

    const invalidAggregateAliasCollision = {
      groupBy: ["status"],
      aggregates: { status: { aggFunc: "count" } },
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly status: {
          readonly aggFunc: "count";
        };
      };
    };
    // @ts-expect-error aggregate aliases cannot collide with groupBy fields
    const _invalidAggregateAliasCollision: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidAggregateAliasCollision
    > &
      ValidateLiveQuery<typeof invalidAggregateAliasCollision> = invalidAggregateAliasCollision;

    const invalidEmptyAggregates = {
      groupBy: ["status"],
      aggregates: {},
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {};
    };
    // @ts-expect-error grouped queries require at least one aggregate alias.
    const _invalidEmptyAggregates: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidEmptyAggregates
    > &
      ValidateLiveQuery<typeof invalidEmptyAggregates> = invalidEmptyAggregates;

    const invalidDangerousAggregateAlias = {
      groupBy: ["status"],
      aggregates: { constructor: { aggFunc: "count" } },
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly constructor: {
          readonly aggFunc: "count";
        };
      };
    };
    // @ts-expect-error grouped aggregate aliases must not use dangerous object keys.
    const _invalidDangerousAggregateAlias: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidDangerousAggregateAlias
    > &
      ValidateLiveQuery<typeof invalidDangerousAggregateAlias> = invalidDangerousAggregateAlias;

    const invalidGroupedOrderByRawField = {
      groupBy: ["status"],
      aggregates: { count: { aggFunc: "count" } },
      orderBy: [{ field: "price", direction: "desc" }],
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly count: {
          readonly aggFunc: "count";
        };
      };
      readonly orderBy: readonly [
        {
          readonly field: "price";
          readonly direction: "desc";
        },
      ];
    };
    // @ts-expect-error grouped orderBy only accepts groupBy fields or aggregate aliases.
    const _invalidGroupedOrderByRawField: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidGroupedOrderByRawField
    > = invalidGroupedOrderByRawField;

    const invalidGroupedOrderByDirection = {
      groupBy: ["status"],
      aggregates: { count: { aggFunc: "count" } },
      orderBy: [{ aggregate: "count", direction: "descending" }],
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly count: {
          readonly aggFunc: "count";
        };
      };
      readonly orderBy: readonly [
        {
          readonly aggregate: "count";
          readonly direction: "descending";
        },
      ];
    };
    // @ts-expect-error grouped orderBy direction is constrained to asc or desc.
    const _invalidGroupedOrderByDirection: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidGroupedOrderByDirection
    > = invalidGroupedOrderByDirection;

    const invalidGroupedOrderByAggregate = {
      groupBy: ["status"],
      aggregates: { count: { aggFunc: "count" } },
      orderBy: [{ aggregate: "totalPrice", direction: "desc" }],
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly count: {
          readonly aggFunc: "count";
        };
      };
      readonly orderBy: readonly [
        {
          readonly aggregate: "totalPrice";
          readonly direction: "desc";
        },
      ];
    };
    // @ts-expect-error grouped orderBy aggregate aliases must exist in aggregates.
    const _invalidGroupedOrderByAggregate: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidGroupedOrderByAggregate
    > = invalidGroupedOrderByAggregate;

    const invalidGroupedOrderByFieldKey = {
      groupBy: ["status"],
      aggregates: { count: { aggFunc: "count" } },
      orderBy: [{ orderByField: "status", direction: "asc" }],
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly count: {
          readonly aggFunc: "count";
        };
      };
      readonly orderBy: readonly [
        {
          readonly orderByField: "status";
          readonly direction: "asc";
        },
      ];
    };
    // @ts-expect-error grouped orderBy group fields use field, not orderByField.
    const _invalidGroupedOrderByFieldKey: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidGroupedOrderByFieldKey
    > = invalidGroupedOrderByFieldKey;

    const invalidGroupedOrderByAggregateKey = {
      groupBy: ["status"],
      aggregates: { count: { aggFunc: "count" } },
      orderBy: [{ field: "count", direction: "desc" }],
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly count: {
          readonly aggFunc: "count";
        };
      };
      readonly orderBy: readonly [
        {
          readonly field: "count";
          readonly direction: "desc";
        },
      ];
    };
    // @ts-expect-error grouped orderBy aggregate aliases use aggregate, not field.
    const _invalidGroupedOrderByAggregateKey: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidGroupedOrderByAggregateKey
    > = invalidGroupedOrderByAggregateKey;

    const invalidGroupedOrderByBothFieldAndAggregate = {
      groupBy: ["status"],
      aggregates: { count: { aggFunc: "count" } },
      orderBy: [{ field: "status", aggregate: "count", direction: "desc" }],
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly count: {
          readonly aggFunc: "count";
        };
      };
      readonly orderBy: readonly [
        {
          readonly field: "status";
          readonly aggregate: "count";
          readonly direction: "desc";
        },
      ];
    };
    // @ts-expect-error grouped orderBy entries must choose field or aggregate, not both.
    const _invalidGroupedOrderByBothFieldAndAggregate: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidGroupedOrderByBothFieldAndAggregate
    > = invalidGroupedOrderByBothFieldAndAggregate;

    const rawOrderByFieldAndAggregateQuery = {
      select: ["id"],
      orderBy: [{ field: "price", aggregate: "totalPrice", direction: "desc" }],
    } satisfies {
      readonly select: readonly ["id"];
      readonly orderBy: readonly [
        {
          readonly field: "price";
          readonly aggregate: "totalPrice";
          readonly direction: "desc";
        },
      ];
    };
    // @ts-expect-error raw orderBy entries cannot also include aggregate.
    useLiveQuery("orders", rawOrderByFieldAndAggregateQuery);

    const invalidOrderSumField = {
      groupBy: ["status"],
      aggregates: {
        badTotal: { aggFunc: "sum", field: "status" },
      },
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly badTotal: {
          readonly aggFunc: "sum";
          readonly field: "status";
        };
      };
    };
    // @ts-expect-error sum and avg aggregate fields must be numeric
    const _invalidOrderSumField: ExactGroupedQuery<typeof Order.Type, typeof invalidOrderSumField> =
      invalidOrderSumField;

    const invalidAggregateExtraKey = {
      groupBy: ["status"],
      aggregates: {
        totalPrice: { aggFunc: "sum", field: "price", typo: true },
      },
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly totalPrice: {
          readonly aggFunc: "sum";
          readonly field: "price";
          readonly typo: true;
        };
      };
    };
    // @ts-expect-error aggregate definitions reject extra keys through variables.
    const _invalidAggregateExtraKey: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidAggregateExtraKey
    > = invalidAggregateExtraKey;

    const invalidCountAggregateField = {
      groupBy: ["status"],
      aggregates: {
        rowCount: { aggFunc: "count", field: "price" },
      },
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly rowCount: {
          readonly aggFunc: "count";
          readonly field: "price";
        };
      };
    };
    // @ts-expect-error count aggregate definitions must not include a field.
    const _invalidCountAggregateField: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidCountAggregateField
    > = invalidCountAggregateField;

    const invalidPositionSumField = {
      groupBy: ["accountId"],
      aggregates: {
        badSymbolTotal: { aggFunc: "sum", field: "symbol" },
      },
    } satisfies {
      readonly groupBy: readonly ["accountId"];
      readonly aggregates: {
        readonly badSymbolTotal: {
          readonly aggFunc: "sum";
          readonly field: "symbol";
        };
      };
    };
    // @ts-expect-error sum aggregate fields must be numeric, bigint, or BigDecimal
    const _invalidPositionSumField: ExactGroupedQuery<
      typeof Position.Type,
      typeof invalidPositionSumField
    > = invalidPositionSumField;

    const invalidOrderAverageField = {
      groupBy: ["status"],
      aggregates: {
        badAverage: { aggFunc: "avg", field: "status" },
      },
    } satisfies {
      readonly groupBy: readonly ["status"];
      readonly aggregates: {
        readonly badAverage: {
          readonly aggFunc: "avg";
          readonly field: "status";
        };
      };
    };
    // @ts-expect-error avg aggregate fields must be numeric
    const _invalidOrderAverageField: ExactGroupedQuery<
      typeof Order.Type,
      typeof invalidOrderAverageField
    > = invalidOrderAverageField;

    const invalidPositionAverageField = {
      groupBy: ["accountId"],
      aggregates: {
        badSymbolAverage: { aggFunc: "avg", field: "symbol" },
      },
    } satisfies {
      readonly groupBy: readonly ["accountId"];
      readonly aggregates: {
        readonly badSymbolAverage: {
          readonly aggFunc: "avg";
          readonly field: "symbol";
        };
      };
    };
    // @ts-expect-error avg aggregate fields must be numeric, bigint, or BigDecimal
    const _invalidPositionAverageField: ExactGroupedQuery<
      typeof Position.Type,
      typeof invalidPositionAverageField
    > = invalidPositionAverageField;
  };

  expectTypeOf(assertLiveQueryContracts).toBeFunction();

  viewServer.defineRuntimeOptions({
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "view-server-type-test",
      regions: {
        usa: "broker-a:9092",
      },
      topics: {
        orders: localKafkaTopic({
          regions: ["usa"],
          value: kafka.protobuf(ordersValueSchema),
          key: kafka.protobuf(ordersKeySchema),
          viewServerTopic: "orders",
          mapping: ({ key, value, region }) => {
            expectTypeOf(key).toEqualTypeOf<OrdersKeyMessage>();
            expectTypeOf(value).toEqualTypeOf<OrdersValueMessage>();
            expectTypeOf(region).toEqualTypeOf<"usa">();
            return {
              id: key.orderId,
              customerId: value.customerId,
              status: value.status,
              price: value.price,
              region,
              updatedAt: value.updatedAt,
            };
          },
        }),
        trades: localKafkaTopic({
          regions: ["usa"],
          value: kafka.protobuf(tradesValueSchema),
          viewServerTopic: "trades",
          mapping: ({ key, value, region }) => {
            expectTypeOf(key).toEqualTypeOf<string>();
            expectTypeOf(value).toEqualTypeOf<TradesValueMessage>();
            expectTypeOf(region).toEqualTypeOf<"usa">();
            return {
              id: key,
              symbol: value.symbol,
              quantity: value.quantity,
              price: value.price,
              region,
            };
          },
        }),
      },
    },
  });

  // @ts-expect-error unsupported Kafka value codecs must fail instead of inferring unknown
  localKafkaTopic({ value: {} });

  // @ts-expect-error $typeName-only objects are message instances, not generated schemas/codecs
  localKafkaTopic({ value: { $typeName: "viewserver.test.OrderValue" } });

  // @ts-expect-error arbitrary decoder shapes are not accepted as Kafka codecs
  localKafkaTopic({ value: { fromBinary: (_bytes: Uint8Array) => ({}) } });

  // @ts-expect-error row Effect schemas are not Kafka codecs unless wrapped with kafka.json
  localKafkaTopic({ value: Order });
};

describe("compile-time contract assertions", () => {
  it("keeps negative type tests typechecked without executing placeholders", () => {
    expect(assertCompileTimeContracts).toBeTypeOf("function");
  });
});

describe("reserved system topic validation", () => {
  it("rejects reserved health topic names at runtime", () => {
    const reservedTopicName: string = VIEW_SERVER_HEALTH_SUMMARY_TOPIC;
    expect(() =>
      defineViewServerConfig({
        topics: {
          [reservedTopicName]: {
            schema: Order,
            key: "id",
          },
        },
      }),
    ).toThrow("View Server topic name is reserved for system health streams");
  });

  it("rejects reserved row field names at runtime", () => {
    const reservedFieldName = "__proto__";
    const BadRow = Schema.Struct({
      id: Schema.String,
      [reservedFieldName]: Schema.String,
    });

    expect(() =>
      defineViewServerConfig({
        topics: {
          badRows: {
            schema: BadRow,
            key: "id",
          },
        },
      }),
    ).toThrow("uses a reserved row field name: __proto__");
  });
});
