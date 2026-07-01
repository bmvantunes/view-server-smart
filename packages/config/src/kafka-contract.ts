import { fromBinary } from "@bufbuild/protobuf";
import type { DescMessage, MessageShape } from "@bufbuild/protobuf";
import { Effect, Schema, SchemaAST } from "effect";
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

const KafkaCodecValueTypeId = Symbol("@effect-view-server/config/KafkaCodecValue");
const KafkaCodecErrorTypeId = Symbol("@effect-view-server/config/KafkaCodecError");
const KafkaCodecDecodeTypeId = Symbol("@effect-view-server/config/KafkaCodecDecode");
const KafkaTopicDefinitionTypeId = Symbol("@effect-view-server/config/KafkaTopicDefinition");
const KafkaTopicDecodeTypeId = Symbol("@effect-view-server/config/KafkaTopicDecode");
const KafkaRuntimeTopicSourceTypeId = Symbol("@effect-view-server/config/KafkaRuntimeTopicSource");
const KafkaTopicSchemaTypeId = Symbol("@effect-view-server/config/KafkaTopicSchema");
const EffectSchemaClassAnnotationKey = "~effect/Schema/Class";

export type KafkaDecodeError = {
  readonly _tag: "KafkaDecodeError";
  readonly message: string;
  readonly cause?: unknown;
};

const KafkaMappingErrorTypeId: unique symbol = Symbol(
  "@effect-view-server/config/KafkaMappingError",
);

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

type KafkaTopicSourceDecodeInput<Region extends string> = {
  readonly keyBytes: Uint8Array;
  readonly valueBytes: Uint8Array;
  readonly region: Region;
  readonly metadata: KafkaMessageMetadata<Region>;
  readonly rowKeyField: string;
  readonly schema: RowSchema;
  readonly viewServerTopic: string;
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

type KafkaTopicSourceDecoder<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  E,
> = {
  readonly [KafkaTopicDecodeTypeId]: {
    bivarianceHack(input: KafkaTopicSourceDecodeInput<Region>): Effect.Effect<
      {
        readonly row: TopicRow<Topics, ViewTopic>;
        readonly rowKey: string;
        readonly viewServerTopic: string;
      },
      E
    >;
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

type KafkaWritableViewTopic<Topics extends KafkaTopicSchemaRegistry> = Extract<
  {
    readonly [Topic in keyof Topics]: Topics[Topic] extends { readonly kafkaSource: object }
      ? never
      : Topics[Topic] extends {
            readonly grpcSource: { readonly kind: "grpc" };
          }
        ? never
        : Topics[Topic] extends { readonly source: { readonly kind: "grpc" } }
          ? never
          : Topic;
  }[keyof Topics],
  string
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

const viewTopicSourceKind = <Topics extends KafkaTopicSchemaRegistry>(
  topics: Topics,
  viewTopic: Extract<keyof Topics, string>,
): string | undefined => {
  const topicDefinition: unknown = topics[viewTopic];
  if (!isInspectableObject(topicDefinition)) {
    return undefined;
  }
  if (
    Object.prototype.hasOwnProperty.call(topicDefinition, "kafkaSource") &&
    Reflect.get(topicDefinition, "kafkaSource") !== undefined
  ) {
    return "kafka";
  }
  if (
    Object.prototype.hasOwnProperty.call(topicDefinition, "grpcSource") &&
    Reflect.get(topicDefinition, "grpcSource") !== undefined
  ) {
    return "grpc";
  }
  const source: unknown = Reflect.get(topicDefinition, "source");
  if (!isInspectableObject(source)) {
    return undefined;
  }
  const kind: unknown = Reflect.get(source, "kind");
  return typeof kind === "string" ? kind : undefined;
};

const validateKafkaViewTopicOwnership = <Topics extends KafkaTopicSchemaRegistry>(
  topics: Topics,
  viewTopic: Extract<keyof Topics, string>,
) => {
  const sourceKind = viewTopicSourceKind(topics, viewTopic);
  if (sourceKind === "grpc") {
    throw new Error(`Kafka source cannot publish into gRPC-owned View Server topic: ${viewTopic}`);
  }
  if (sourceKind === "kafka") {
    throw new Error(`Kafka source cannot publish into Kafka-owned View Server topic: ${viewTopic}`);
  }
};

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

const isInspectableObject = (value: unknown): value is object =>
  (typeof value === "object" && value !== null) || typeof value === "function";

const isKafkaCodec = (value: unknown): value is KafkaCodec<unknown, unknown> =>
  isInspectableObject(value) &&
  typeof Reflect.get(value, "format") === "string" &&
  typeof Reflect.get(value, KafkaCodecDecodeTypeId) === "function";

const isSupportedJsonObjectRecordKeyAst = (ast: SchemaAST.AST): boolean => {
  if (SchemaAST.isLiteral(ast)) {
    return typeof ast.literal === "string" || typeof ast.literal === "symbol";
  }
  if (SchemaAST.isUniqueSymbol(ast) || SchemaAST.isTemplateLiteral(ast)) {
    return true;
  }
  if (SchemaAST.isUnion(ast)) {
    return (
      ast.types.length > 0 && ast.types.every((member) => isSupportedJsonObjectRecordKeyAst(member))
    );
  }
  return ast._tag === "String" || ast._tag === "Number" || ast._tag === "Symbol";
};

const isSupportedJsonObjectRecordKeySourceSchema = (schema: object): boolean => {
  const from = Reflect.get(schema, "from");
  if (isInspectableObject(from)) {
    return isSupportedJsonObjectRecordKeySourceSchema(from);
  }
  const members = Reflect.get(schema, "members");
  if (Array.isArray(members)) {
    return (
      members.length > 0 &&
      members.every(
        (member) =>
          isInspectableObject(member) && isSupportedJsonObjectRecordKeySourceSchema(member),
      )
    );
  }
  return isSupportedJsonObjectRecordKeyAst(Reflect.get(schema, "ast"));
};

const isSupportedJsonObjectRecordKeyDecodedSchema = (schema: object): boolean => {
  const to = Reflect.get(schema, "to");
  if (isInspectableObject(to)) {
    return isSupportedJsonObjectRecordKeyDecodedSchema(to);
  }
  const members = Reflect.get(schema, "members");
  if (Array.isArray(members)) {
    return (
      members.length > 0 &&
      members.every(
        (member) => isInspectableObject(member) && isSupportedJsonObjectRecordKeySchema(member),
      )
    );
  }
  return isSupportedJsonObjectRecordKeyAst(Reflect.get(schema, "ast"));
};

const isSupportedJsonObjectRecordKeySchema = (schema: object): boolean =>
  isSupportedJsonObjectRecordKeyAst(Reflect.get(schema, "ast")) &&
  isSupportedJsonObjectRecordKeySourceSchema(schema) &&
  isSupportedJsonObjectRecordKeyDecodedSchema(schema);

const isJsonLikeDeclarationAst = (ast: SchemaAST.AST): boolean =>
  SchemaAST.isDeclaration(ast) &&
  ast.typeParameters.length === 0 &&
  ast.encoding !== undefined &&
  ast.encoding.some((link) => SchemaAST.isUnknown(link.to));

const isObjectLikeJsonCodecTargetAst = (ast: SchemaAST.AST): boolean =>
  SchemaAST.isObjects(ast) ||
  SchemaAST.isObjectKeyword(ast) ||
  SchemaAST.isUnknown(ast) ||
  SchemaAST.isAny(ast) ||
  isJsonLikeDeclarationAst(ast);

const validateCustomDeclarationJsonCodecTargetAst = (
  ast: SchemaAST.AST,
  seen: Set<SchemaAST.AST>,
  requiresConcreteWireShape = false,
): boolean => {
  if (seen.has(ast)) {
    return false;
  }
  seen.add(ast);
  if (SchemaAST.isSuspend(ast)) {
    return validateCustomDeclarationJsonCodecTargetAst(
      ast.thunk(),
      seen,
      requiresConcreteWireShape,
    );
  }
  if (
    SchemaAST.isDeclaration(ast) &&
    ast.typeParameters.length === 0 &&
    typeof declarationJsonLink(ast) !== "function"
  ) {
    throw new Error("Declaration schemas must define Kafka JSON codecs");
  }
  if (ast.encoding !== undefined) {
    const classTarget =
      SchemaAST.isDeclaration(ast) && isSchemaClassDeclarationAst(ast)
        ? ast.typeParameters[0]
        : undefined;
    let hasJsonWireLink = false;
    for (const link of ast.encoding) {
      if (link.to !== classTarget) {
        hasJsonWireLink =
          validateCustomDeclarationJsonCodecTargetAst(
            link.to,
            seen,
            isObjectLikeJsonCodecTargetAst(ast),
          ) || hasJsonWireLink;
      }
    }
    if (classTarget !== undefined && !hasJsonWireLink && SchemaAST.isObjects(classTarget)) {
      validateCustomDeclarationJsonCodecTargetAst(classTarget, seen);
    }
    if (isObjectLikeJsonCodecTargetAst(ast) && !hasJsonWireLink) {
      throw new Error("Declaration JSON codecs must not produce object-like codecs");
    }
    return hasJsonWireLink;
  }
  if (isObjectLikeJsonCodecTargetAst(ast)) {
    throw new Error("Declaration JSON codecs must not produce object-like codecs");
  }
  if (SchemaAST.isDeclaration(ast) && requiresConcreteWireShape) {
    return false;
  }
  if (SchemaAST.isArrays(ast)) {
    for (const element of ast.elements) {
      validateCustomDeclarationJsonCodecTargetAst(element, seen);
    }
    for (const rest of ast.rest) {
      validateCustomDeclarationJsonCodecTargetAst(rest, seen);
    }
    return true;
  }
  if (SchemaAST.isUnion(ast)) {
    for (const member of ast.types) {
      validateCustomDeclarationJsonCodecTargetAst(member, seen);
    }
    return true;
  }
  if (SchemaAST.isTemplateLiteral(ast)) {
    for (const part of ast.parts) {
      validateCustomDeclarationJsonCodecTargetAst(part, seen);
    }
  }
  return true;
};

const declarationJsonLink = (ast: SchemaAST.AST): unknown =>
  Reflect.get(Object(Reflect.get(ast, "annotations")), "toCodecJson") ??
  Reflect.get(Object(Reflect.get(ast, "annotations")), "toCodec");

const effectDeclarationDescriptor = (schema: { readonly ast: SchemaAST.AST }) => ({
  run: Reflect.get(schema.ast, "run"),
  link: declarationJsonLink(schema.ast),
});

const knownEffectJsonDeclarations = [
  Schema.BigDecimal,
  Schema.Date,
  Schema.Duration,
  Schema.Error(),
  Schema.Error({ includeStack: true }),
  Schema.Error({ excludeCause: true }),
  Schema.Error({ includeStack: true, excludeCause: true }),
  Schema.File,
  Schema.FormData,
  Schema.Json,
  Schema.MutableJson,
  Schema.RegExp,
  Schema.URL,
  Schema.URLSearchParams,
].map(effectDeclarationDescriptor) satisfies ReadonlyArray<{
  readonly run: unknown;
  readonly link: unknown;
}>;

const parametricDeclarationKey = (tag: string, shape: string): string => `${tag}\n${shape}`;

const functionSource = (fn: unknown): string => Function.prototype.toString.call(fn);

// Effect v4 beta does not expose stable constructor identities for parametric declarations.
// Kafka topic schemas are trusted application config; this guard catches accidental/custom
// declaration drift while preserving real Effect JSON codecs such as Option and ReadonlyMap.
const declarationLinkSource = (schema: { readonly ast: SchemaAST.Declaration }): string =>
  functionSource(declarationJsonLink(schema.ast));

const declarationParserSource = (schema: { readonly ast: SchemaAST.Declaration }): string =>
  functionSource(schema.ast.run(schema.ast.typeParameters));

type ParametricDeclarationDescriptor = {
  readonly annotationKey: "toCodec" | "toCodecJson";
  readonly parameterKeys: ReadonlyArray<string>;
  readonly parameterPaths: ReadonlyArray<string>;
  readonly parserSources: ReadonlyArray<string>;
  readonly sources: ReadonlyArray<string>;
};

const parametricDeclarationDescriptor = (
  schema: { readonly ast: SchemaAST.Declaration },
  annotationKey: "toCodec" | "toCodecJson",
  parameterKeys: ReadonlyArray<string>,
  parameterPaths: ReadonlyArray<string> = parameterKeys,
): ParametricDeclarationDescriptor => ({
  annotationKey,
  parameterKeys,
  parameterPaths,
  parserSources: [declarationParserSource(schema)],
  sources: [declarationLinkSource(schema)],
});

const knownEffectParametricDeclarationSources = new Map<string, ParametricDeclarationDescriptor>([
  [
    parametricDeclarationKey("ReadonlyMap", "key,value,ast,rebuild,makeEffect,make,makeOption"),
    parametricDeclarationDescriptor(Schema.ReadonlyMap(Schema.String, Schema.String), "toCodec", [
      "key",
      "value",
    ]),
  ],
  [
    parametricDeclarationKey("ReadonlySet", "value,ast,rebuild,makeEffect,make,makeOption"),
    parametricDeclarationDescriptor(Schema.ReadonlySet(Schema.String), "toCodec", ["value"]),
  ],
  [
    parametricDeclarationKey("effect/Cause", "error,defect,ast,rebuild,makeEffect,make,makeOption"),
    parametricDeclarationDescriptor(Schema.Cause(Schema.String, Schema.String), "toCodec", [
      "error",
      "defect",
    ]),
  ],
  [
    parametricDeclarationKey(
      "effect/Cause/Failure",
      "error,defect,ast,rebuild,makeEffect,make,makeOption",
    ),
    parametricDeclarationDescriptor(Schema.CauseReason(Schema.String, Schema.String), "toCodec", [
      "error",
      "defect",
    ]),
  ],
  [
    parametricDeclarationKey("effect/Chunk", "value,ast,rebuild,makeEffect,make,makeOption"),
    parametricDeclarationDescriptor(Schema.Chunk(Schema.String), "toCodec", ["value"]),
  ],
  [
    parametricDeclarationKey(
      "effect/Exit",
      "value,error,defect,ast,rebuild,makeEffect,make,makeOption",
    ),
    parametricDeclarationDescriptor(
      Schema.Exit(Schema.String, Schema.String, Schema.String),
      "toCodec",
      ["value", "error", "defect"],
    ),
  ],
  [
    parametricDeclarationKey("effect/HashMap", "key,value,ast,rebuild,makeEffect,make,makeOption"),
    parametricDeclarationDescriptor(Schema.HashMap(Schema.String, Schema.String), "toCodec", [
      "key",
      "value",
    ]),
  ],
  [
    parametricDeclarationKey("effect/HashSet", "value,ast,rebuild,makeEffect,make,makeOption"),
    parametricDeclarationDescriptor(Schema.HashSet(Schema.String), "toCodec", ["value"]),
  ],
  [
    parametricDeclarationKey("effect/Option", "value,ast,rebuild,makeEffect,make,makeOption"),
    parametricDeclarationDescriptor(Schema.Option(Schema.String), "toCodec", ["value"]),
  ],
  [
    parametricDeclarationKey("effect/Option", "from,to,ast,rebuild,makeEffect,make,makeOption"),
    parametricDeclarationDescriptor(
      Schema.OptionFromNullOr(Schema.String),
      "toCodec",
      ["from", "to"],
      ["to.value"],
    ),
  ],
  [
    parametricDeclarationKey("effect/Redacted", "value,ast,rebuild,makeEffect,make,makeOption"),
    parametricDeclarationDescriptor(Schema.Redacted(Schema.String), "toCodecJson", ["value"]),
  ],
  [
    parametricDeclarationKey(
      "effect/Result",
      "success,failure,ast,rebuild,makeEffect,make,makeOption",
    ),
    parametricDeclarationDescriptor(Schema.Result(Schema.String, Schema.String), "toCodec", [
      "success",
      "failure",
    ]),
  ],
]);

const knownEffectParametricDeclarationAstDescriptors = Array.from(
  knownEffectParametricDeclarationSources,
  ([key, descriptor]) => ({
    parameterKeys: descriptor.parameterKeys,
    tag: key.slice(0, key.indexOf("\n")),
  }),
);

const schemaParametersMatchDeclarationAst = (
  schema: object,
  ast: SchemaAST.Declaration,
  parameterPaths: ReadonlyArray<string>,
): boolean =>
  parameterPaths.length === ast.typeParameters.length &&
  parameterPaths.every((path, index) => {
    const parameterSchema = path
      .split(".")
      .reduce<unknown>((current, key) => Reflect.get(Object(current), key), schema);
    return (
      isInspectableObject(parameterSchema) &&
      Reflect.get(parameterSchema, "ast") === ast.typeParameters[index]
    );
  });

const isKnownEffectParametricJsonDeclarationSchema = (
  schema: object,
  ast: SchemaAST.Declaration,
  getLink: unknown,
): boolean => {
  const annotations = Object(ast.annotations);
  const typeConstructor = Reflect.get(annotations, "typeConstructor");
  if (!isInspectableObject(typeConstructor)) {
    return false;
  }
  const expectedSources = knownEffectParametricDeclarationSources.get(
    parametricDeclarationKey(
      String(Reflect.get(typeConstructor, "_tag")),
      Object.keys(schema).join(","),
    ),
  );
  return (
    expectedSources !== undefined &&
    schemaParametersMatchDeclarationAst(schema, ast, expectedSources.parameterPaths) &&
    (expectedSources.annotationKey === "toCodecJson" ||
      Reflect.get(annotations, "toCodecJson") === undefined) &&
    getLink === Reflect.get(annotations, expectedSources.annotationKey) &&
    expectedSources.parserSources.includes(functionSource(ast.run(ast.typeParameters))) &&
    expectedSources.sources.includes(functionSource(getLink))
  );
};

const isKnownEffectParametricJsonDeclarationAst = (
  ast: SchemaAST.Declaration,
  getLink: unknown,
): boolean => {
  const annotations = Object(ast.annotations);
  const typeConstructor = Reflect.get(annotations, "typeConstructor");
  if (!isInspectableObject(typeConstructor)) {
    return false;
  }
  const typeConstructorTag = String(Reflect.get(typeConstructor, "_tag"));
  return knownEffectParametricDeclarationAstDescriptors.some(({ parameterKeys, tag }) => {
    if (tag !== typeConstructorTag || parameterKeys.length !== ast.typeParameters.length) {
      return false;
    }
    const syntheticSchema = Schema.make(
      ast,
      Object.fromEntries(
        ast.typeParameters.map((typeParameter, index) => [
          String(parameterKeys[index]),
          Schema.make(typeParameter),
        ]),
      ),
    );
    return isKnownEffectParametricJsonDeclarationSchema(syntheticSchema, ast, getLink);
  });
};

const isKnownEffectJsonDeclarationSchema = (
  schema: object,
  ast: SchemaAST.Declaration,
  getLink: unknown,
): boolean =>
  knownEffectJsonDeclarations.some(
    (declaration) => declaration.run === ast.run && declaration.link === getLink,
  ) || isKnownEffectParametricJsonDeclarationSchema(schema, ast, getLink);

const isKnownEffectJsonDeclarationAst = (ast: SchemaAST.Declaration, getLink: unknown): boolean =>
  knownEffectJsonDeclarations.some(
    (declaration) => declaration.run === ast.run && declaration.link === getLink,
  ) || isKnownEffectParametricJsonDeclarationAst(ast, getLink);

const isSchemaClassDeclarationAst = (ast: SchemaAST.Declaration): boolean => {
  const classLink = Reflect.get(Object(ast.annotations), EffectSchemaClassAnnotationKey);
  const toCodecLink = Reflect.get(Object(ast.annotations), "toCodec");
  if (
    typeof classLink !== "function" ||
    typeof toCodecLink !== "function" ||
    ast.typeParameters.length !== 1
  ) {
    return false;
  }
  const classTarget = ast.typeParameters[0];
  if (classTarget === undefined || !SchemaAST.isObjects(classTarget)) {
    return false;
  }
  const classLinkResult = classLink([classTarget]);
  const toCodecLinkResult = toCodecLink([Schema.make(classTarget)]);
  return (
    isInspectableObject(classLinkResult) &&
    isInspectableObject(toCodecLinkResult) &&
    Reflect.get(classLinkResult, "to") === classTarget &&
    Reflect.get(toCodecLinkResult, "to") === classTarget
  );
};

const isDefaultSchemaClassDeclarationAst = (ast: SchemaAST.Declaration): boolean =>
  Reflect.get(Object(ast.annotations), "toCodecJson") === undefined &&
  isSchemaClassDeclarationAst(ast);

const rejectSuspendedRecordKeySchemas = (schema: unknown): void => {
  const customDeclarationJsonCodecTargetAstFromAst = (ast: SchemaAST.Declaration): unknown => {
    const getLink = declarationJsonLink(ast);
    if (
      typeof getLink !== "function" ||
      isKnownEffectJsonDeclarationAst(ast, getLink) ||
      isDefaultSchemaClassDeclarationAst(ast)
    ) {
      return undefined;
    }
    const link = getLink(
      ast.typeParameters.map((typeParameter) => Schema.make(SchemaAST.toEncoded(typeParameter))),
    );
    return Schema.toCodecJson(Schema.make(link.to)).ast;
  };
  const visitAst = (root: SchemaAST.AST): void => {
    const seen = new Set<SchemaAST.AST>();
    const visitCurrentAst = (current: SchemaAST.AST): void => {
      if (seen.has(current)) {
        return;
      }
      seen.add(current);
      if (SchemaAST.isSuspend(current)) {
        visitCurrentAst(current.thunk());
        return;
      }
      if (!SchemaAST.isDeclaration(current) && current.encoding !== undefined) {
        for (const link of current.encoding) {
          visitCurrentAst(link.to);
        }
        return;
      }
      if (SchemaAST.isDeclaration(current)) {
        if (
          current.typeParameters.length === 0 &&
          typeof declarationJsonLink(current) !== "function"
        ) {
          throw new Error("Declaration schemas must define Kafka JSON codecs");
        }
        const isSchemaClass = isSchemaClassDeclarationAst(current);
        const classTarget = isSchemaClass ? current.typeParameters[0] : undefined;
        const customJsonCodecTargetAst = customDeclarationJsonCodecTargetAstFromAst(current);
        if (SchemaAST.isAST(customJsonCodecTargetAst)) {
          validateCustomDeclarationJsonCodecTargetAst(customJsonCodecTargetAst, new Set());
        }
        let inspectedDeclarationEncoding = false;
        if (current.encoding !== undefined) {
          for (const link of current.encoding) {
            if (!isSchemaClass || link.to !== classTarget) {
              visitCurrentAst(link.to);
              inspectedDeclarationEncoding = true;
            }
          }
        }
        if (inspectedDeclarationEncoding) {
          return;
        }
        if (isSchemaClass) {
          if (
            classTarget !== undefined &&
            SchemaAST.isObjects(classTarget) &&
            classTarget.propertySignatures.length === 0 &&
            classTarget.indexSignatures.length === 0
          ) {
            return;
          }
        }
        for (const typeParameter of current.typeParameters) {
          visitCurrentAst(typeParameter);
        }
        return;
      }
      if (SchemaAST.isArrays(current)) {
        for (const element of current.elements) {
          visitCurrentAst(element);
        }
        for (const rest of current.rest) {
          visitCurrentAst(rest);
        }
        return;
      }
      if (SchemaAST.isObjects(current)) {
        // Effect erases unsupported record-key schemas inside Suspend to Objects without
        // stable source metadata. Reject the ambiguous empty shape instead of silently
        // accepting a schema whose JSON codec can skip record-value decoding.
        // Non-empty erased record-key unions are indistinguishable from ordinary suspended
        // structs at this boundary; Kafka schemas are trusted config and must avoid those.
        if (current.propertySignatures.length === 0 && current.indexSignatures.length === 0) {
          throw new Error("Suspended empty object schemas are not supported by Kafka JSON codecs");
        }
        for (const property of current.propertySignatures) {
          visitCurrentAst(property.type);
        }
        for (const index of current.indexSignatures) {
          visitCurrentAst(index.parameter);
          visitCurrentAst(index.type);
        }
        return;
      }
      if (SchemaAST.isUnion(current)) {
        for (const member of current.types) {
          visitCurrentAst(member);
        }
        return;
      }
      if (SchemaAST.isTemplateLiteral(current)) {
        for (const part of current.parts) {
          visitCurrentAst(part);
        }
      }
    };
    visitCurrentAst(root);
  };
  const customDeclarationJsonCodecTargetAst = (current: object): unknown => {
    const ast = Reflect.get(current, "ast");
    if (!SchemaAST.isAST(ast) || !SchemaAST.isDeclaration(ast)) {
      return undefined;
    }
    const getLink = declarationJsonLink(ast);
    if (
      typeof getLink !== "function" ||
      isKnownEffectJsonDeclarationSchema(current, ast, getLink) ||
      isDefaultSchemaClassDeclarationAst(ast)
    ) {
      return undefined;
    }
    const link = getLink(
      ast.typeParameters.map((typeParameter) => Schema.make(SchemaAST.toEncoded(typeParameter))),
    );
    return Schema.toCodecJson(Schema.make(link.to)).ast;
  };
  const visitChild = (current: object, key: string): void => {
    visit(Reflect.get(current, key));
  };
  const visitChildren = (current: object, key: string): void => {
    const children = Reflect.get(current, key);
    if (Array.isArray(children)) {
      for (const child of children) {
        visit(child);
      }
    }
  };
  const visitRecordChildren = (current: object, key: string): void => {
    const children = Reflect.get(current, key);
    if (isInspectableObject(children)) {
      for (const child of Object.values(children)) {
        visit(child);
      }
    }
  };
  const visit = (current: unknown): void => {
    if (!isInspectableObject(current)) {
      return;
    }
    const customJsonCodecTargetAst = customDeclarationJsonCodecTargetAst(current);
    if (SchemaAST.isAST(customJsonCodecTargetAst)) {
      validateCustomDeclarationJsonCodecTargetAst(customJsonCodecTargetAst, new Set());
    }
    const key = Reflect.get(current, "key");
    const ast = Reflect.get(current, "ast");
    if (SchemaAST.isAST(ast) && SchemaAST.isSuspend(ast)) {
      visitAst(ast);
    }
    if (SchemaAST.isAST(ast) && SchemaAST.isDeclaration(ast)) {
      if (ast.typeParameters.length === 0 && typeof declarationJsonLink(ast) !== "function") {
        throw new Error("Declaration schemas must define Kafka JSON codecs");
      }
    }
    if (
      SchemaAST.isAST(ast) &&
      SchemaAST.isObjects(ast) &&
      Object.hasOwn(current, "key") &&
      (!isInspectableObject(key) || !isSupportedJsonObjectRecordKeySchema(key))
    ) {
      throw new Error("Unsupported record key schemas are not supported by Kafka JSON codecs");
    }
    visitChild(current, "key");
    visitChild(current, "value");
    visitChild(current, "schema");
    visitChild(current, "from");
    visitChild(current, "success");
    visitChild(current, "failure");
    visitChild(current, "error");
    visitChild(current, "defect");
    visitChildren(current, "members");
    visitChildren(current, "elements");
    visitChildren(current, "rest");
    visitChildren(current, "records");
    visitRecordChildren(current, "cases");
    const fields = Reflect.get(current, "fields");
    if (isInspectableObject(fields)) {
      for (const fieldSchema of Object.values(fields)) {
        visit(fieldSchema);
      }
    }
  };
  visit(schema);
};

const forceSuspendedJsonCodecBranches = (ast: SchemaAST.AST): void => {
  const seen = new Set<SchemaAST.AST>();
  const visit = (current: SchemaAST.AST): SchemaAST.AST => {
    if (seen.has(current)) {
      return current;
    }
    seen.add(current);
    if (current.encoding !== undefined) {
      for (const link of current.encoding) {
        visit(link.to);
      }
    }
    if (SchemaAST.isSuspend(current)) {
      visit(current.thunk());
      return current;
    }
    if (SchemaAST.isDeclaration(current)) {
      for (const typeParameter of current.typeParameters) {
        visit(typeParameter);
      }
      return current;
    }
    if (SchemaAST.isArrays(current)) {
      for (const element of current.elements) {
        visit(element);
      }
      for (const rest of current.rest) {
        visit(rest);
      }
      return current;
    }
    if (SchemaAST.isObjects(current)) {
      for (const property of current.propertySignatures) {
        visit(property.type);
      }
      for (const index of current.indexSignatures) {
        visit(index.parameter);
        visit(index.type);
      }
      return current;
    }
    if (SchemaAST.isUnion(current)) {
      for (const member of current.types) {
        visit(member);
      }
      return current;
    }
    if (SchemaAST.isTemplateLiteral(current)) {
      for (const part of current.parts) {
        visit(part);
      }
      return current;
    }
    return current;
  };
  visit(ast);
};

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

type KafkaTopicSourceHelperMapWithoutKey<
  TopicRegions extends NonEmptyReadonlyArray<string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
> = (input: KafkaTopicSourceHelperMapInputWithoutKey<TopicRegions[number], ValueCodec>) => object;

type KafkaTopicSourceHelperMapWithKey<
  TopicRegions extends NonEmptyReadonlyArray<string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
> = (
  input: KafkaTopicSourceHelperMapInputWithKey<TopicRegions[number], ValueCodec, KeyCodec>,
) => object;

type KafkaTopicSourceHelperInputWithoutKey<
  SourceTopic extends string,
  TopicRegions extends NonEmptyReadonlyArray<string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  Mapping extends KafkaTopicSourceHelperMapWithoutKey<TopicRegions, ValueCodec> =
    KafkaTopicSourceHelperMapWithoutKey<TopicRegions, ValueCodec>,
> = {
  readonly topic: SourceTopic;
  readonly regions: TopicRegions;
  readonly value: SupportedKafkaCodec<ValueCodec>;
  readonly map: Mapping;
};

type KafkaTopicSourceHelperInputWithKey<
  SourceTopic extends string,
  TopicRegions extends NonEmptyReadonlyArray<string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  Mapping extends KafkaTopicSourceHelperMapWithKey<TopicRegions, ValueCodec, KeyCodec> =
    KafkaTopicSourceHelperMapWithKey<TopicRegions, ValueCodec, KeyCodec>,
> = {
  readonly topic: SourceTopic;
  readonly regions: TopicRegions;
  readonly value: SupportedKafkaCodec<ValueCodec>;
  readonly key: SupportedKafkaCodec<KeyCodec>;
  readonly map: Mapping;
};

function defineKafkaTopicSource<
  const SourceTopic extends string,
  const TopicRegions extends NonEmptyReadonlyArray<string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  Mapping extends (
    input: KafkaTopicSourceHelperMapInputWithKey<TopicRegions[number], ValueCodec, KeyCodec>,
  ) => object,
>(
  topic: KafkaTopicSourceHelperInputWithKey<
    SourceTopic,
    TopicRegions,
    ValueCodec,
    KeyCodec,
    Mapping
  >,
): KafkaTopicSourceHelperInputWithKey<SourceTopic, TopicRegions, ValueCodec, KeyCodec, Mapping>;
function defineKafkaTopicSource<
  const SourceTopic extends string,
  const TopicRegions extends NonEmptyReadonlyArray<string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  Mapping extends KafkaTopicSourceHelperMapWithoutKey<TopicRegions, ValueCodec> =
    KafkaTopicSourceHelperMapWithoutKey<TopicRegions, ValueCodec>,
>(
  topic: KafkaTopicSourceHelperInputWithoutKey<SourceTopic, TopicRegions, ValueCodec, Mapping>,
): KafkaTopicSourceHelperInputWithoutKey<SourceTopic, TopicRegions, ValueCodec, Mapping>;
function defineKafkaTopicSource(topic: object): object {
  return topic;
}

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
  ): KafkaJsonCodec<SourceSchema> => {
    const jsonDecoder = (() => {
      try {
        rejectSuspendedRecordKeySchemas(schema);
        forceSuspendedJsonCodecBranches(schema.ast);
        const jsonCodec = Schema.toCodecJson(schema);
        rejectSuspendedRecordKeySchemas(jsonCodec);
        forceSuspendedJsonCodecBranches(jsonCodec.ast);
        const decodeJsonRow = Schema.decodeUnknownEffect(jsonCodec);
        return {
          _tag: "valid",
          decodeJsonRow,
        } as const;
      } catch (cause) {
        return {
          _tag: "invalid",
          error: kafkaDecodeError("Kafka JSON schema is not JSON-compatible", cause),
        } as const;
      }
    })();
    return {
      ...makeKafkaCodec<SourceSchema["Type"], KafkaDecodeError, "json">("json", (input) =>
        Effect.gen(function* () {
          if (jsonDecoder._tag === "invalid") {
            return yield* Effect.fail(jsonDecoder.error);
          }
          const decodedJson = yield* Effect.try({
            try: (): unknown => JSON.parse(utf8Decoder.decode(input.bytes)),
            catch: (cause) => kafkaDecodeError("Failed to parse Kafka JSON payload", cause),
          });
          return yield* jsonDecoder
            .decodeJsonRow(decodedJson)
            .pipe(
              Effect.mapError((cause) =>
                kafkaDecodeError("Failed to decode Kafka JSON payload", cause),
              ),
            );
        }),
      ),
      schema,
    };
  },
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
  source: defineKafkaTopicSource,
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

export type KafkaTopicSourceMapInput<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  ValueCodec,
  KeyCodec,
> = [KeyCodec] extends [undefined]
  ? KafkaTopicSourceMapInputWithoutKey<
      Topics,
      ViewTopic,
      Region,
      ValueCodec & KafkaCodec<unknown, unknown>
    >
  : KafkaTopicSourceMapInputWithKey<
      Topics,
      ViewTopic,
      Region,
      ValueCodec & KafkaCodec<unknown, unknown>,
      KeyCodec & KafkaCodec<unknown, unknown>
    >;

type KafkaTopicSourceMapInputWithoutKey<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  ValueCodec extends KafkaCodec<unknown, unknown>,
> = {
  readonly key: string;
  readonly value: KafkaCodecType<ValueCodec>;
  readonly region: Region;
  readonly rowKey: string;
  readonly schema: KafkaTopicSchemaValue<Topics, ViewTopic>;
  readonly metadata: KafkaMessageMetadata<Region>;
};

type KafkaTopicSourceMapInputWithKey<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
> = {
  readonly key: KafkaCodecType<KeyCodec>;
  readonly value: KafkaCodecType<ValueCodec>;
  readonly region: Region;
  readonly rowKey: string;
  readonly schema: KafkaTopicSchemaValue<Topics, ViewTopic>;
  readonly metadata: KafkaMessageMetadata<Region>;
};

type KafkaTopicSourceHelperMapInputWithoutKey<
  Region extends string,
  ValueCodec extends KafkaCodec<unknown, unknown>,
> = {
  readonly key: string;
  readonly value: KafkaCodecType<ValueCodec>;
  readonly region: Region;
  readonly rowKey: string;
  readonly metadata: KafkaMessageMetadata<Region>;
};

type KafkaTopicSourceHelperMapInputWithKey<
  Region extends string,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
> = {
  readonly key: KafkaCodecType<KeyCodec>;
  readonly value: KafkaCodecType<ValueCodec>;
  readonly region: Region;
  readonly rowKey: string;
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

type KafkaTopicSourceInputWithoutKey<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaTopicSourceMapInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaTopicSourceMapInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => TopicRow<Topics, ViewTopic>,
  SourceTopic extends string = string,
> = {
  readonly topic: SourceTopic;
  readonly regions: TopicRegions;
  readonly value: SupportedKafkaCodec<ValueCodec>;
  readonly map: ExactMappingReturn<
    KafkaTopicSourceMapInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
    TopicRow<Topics, ViewTopic>,
    Mapping
  >;
};

type KafkaTopicSourceInputWithKey<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaTopicSourceMapInputWithKey<
      Topics,
      ViewTopic,
      TopicRegions[number],
      ValueCodec,
      KeyCodec
    >,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaTopicSourceMapInputWithKey<
      Topics,
      ViewTopic,
      TopicRegions[number],
      ValueCodec,
      KeyCodec
    >,
  ) => TopicRow<Topics, ViewTopic>,
  SourceTopic extends string = string,
> = {
  readonly topic: SourceTopic;
  readonly regions: TopicRegions;
  readonly value: SupportedKafkaCodec<ValueCodec>;
  readonly key: SupportedKafkaCodec<KeyCodec>;
  readonly map: ExactMappingReturn<
    KafkaTopicSourceMapInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
    TopicRow<Topics, ViewTopic>,
    Mapping
  >;
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
  ViewTopic extends KafkaWritableViewTopic<Topics>,
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
  ViewTopic extends KafkaWritableViewTopic<Topics>,
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
  ViewTopic extends KafkaWritableViewTopic<Topics>,
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
  ViewTopic extends KafkaWritableViewTopic<Topics>,
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
  ViewTopic extends KafkaWritableViewTopic<Topics> = KafkaWritableViewTopic<Topics>,
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

export type KafkaTopicSourceDefinition<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string> = Extract<keyof Topics, string>,
  ValueCodec extends KafkaCodec<unknown, unknown> = KafkaCodec<unknown, unknown>,
  KeyCodec = undefined,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>> =
    NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  MappingWithoutKey extends (
    input: KafkaTopicSourceMapInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaTopicSourceMapInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => TopicRow<Topics, ViewTopic>,
  MappingWithKey extends (
    input: KafkaTopicSourceMapInputWithKey<
      Topics,
      ViewTopic,
      TopicRegions[number],
      ValueCodec,
      Extract<KeyCodec, KafkaCodec<unknown, unknown>>
    >,
  ) => TopicRow<Topics, ViewTopic> = (
    input: KafkaTopicSourceMapInputWithKey<
      Topics,
      ViewTopic,
      TopicRegions[number],
      ValueCodec,
      Extract<KeyCodec, KafkaCodec<unknown, unknown>>
    >,
  ) => TopicRow<Topics, ViewTopic>,
  SourceTopic extends string = string,
> =
  | KafkaTopicSourceInputWithoutKey<
      Topics,
      Regions,
      ViewTopic,
      ValueCodec,
      TopicRegions,
      MappingWithoutKey,
      SourceTopic
    >
  | (KeyCodec extends KafkaCodec<unknown, unknown>
      ? MappingWithKey extends (
          input: KafkaTopicSourceMapInputWithKey<
            Topics,
            ViewTopic,
            TopicRegions[number],
            ValueCodec,
            KeyCodec
          >,
        ) => TopicRow<Topics, ViewTopic>
        ? KafkaTopicSourceInputWithKey<
            Topics,
            Regions,
            ViewTopic,
            ValueCodec,
            KeyCodec,
            TopicRegions,
            MappingWithKey,
            SourceTopic
          >
        : never
      : never);

export type KafkaRuntimeTopicSourceDefinition<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string> = Extract<keyof Topics, string>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>> =
    NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
> = KafkaTopicSourceDecoder<Topics, ViewTopic, TopicRegions[number], unknown> & {
  readonly [KafkaRuntimeTopicSourceTypeId]: true;
  readonly regions: TopicRegions;
  readonly topic: string;
  readonly viewServerTopic: ViewTopic;
};

export type KafkaRuntimeSourceTopicDefinition<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>> =
    NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
> =
  | KafkaRuntimeTopicDefinition<Topics, Regions, TopicRegions>
  | KafkaRuntimeTopicSourceDefinition<Topics, Regions, Extract<keyof Topics, string>, TopicRegions>;

export type KafkaRuntimeTopicDefinition<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>> =
    NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
> = KafkaTopicDefinitionMarker &
  KafkaTopicSchemaMarker<Topics, KafkaWritableViewTopic<Topics>> &
  KafkaTopicDecoder<Topics, KafkaWritableViewTopic<Topics>, TopicRegions[number], unknown> & {
    readonly regions: TopicRegions;
    readonly viewServerTopic: KafkaWritableViewTopic<Topics>;
  };

const decodeKafkaStringKey = (input: KafkaCodecDecodeInput): string =>
  utf8Decoder.decode(input.bytes);

const mapKafkaPayload = <A>(map: () => A): Effect.Effect<A, KafkaMappingError> =>
  Effect.try({
    try: map,
    catch: (cause) => kafkaMappingError("Failed to map Kafka payload", cause),
  });

const rowKeyFromDecodedKafkaKey = (
  key: unknown,
  rowKeyField: string,
): Effect.Effect<string, KafkaMappingError> => {
  if (typeof key === "string") {
    return Effect.succeed(key);
  }
  if (typeof key === "object" && key !== null) {
    const hasConfiguredField = Object.hasOwn(key, rowKeyField);
    const candidate = Reflect.get(key, rowKeyField);
    if (typeof candidate === "string") {
      return Effect.succeed(candidate);
    }
    if (hasConfiguredField) {
      return Effect.fail(
        kafkaMappingError(`Kafka key cannot derive View Server row key field: ${rowKeyField}`, key),
      );
    }
    const stringValues = Object.entries(key)
      .filter(([field]) => !field.startsWith("$"))
      .map(([_field, value]) => value)
      .filter((value) => typeof value === "string");
    const [onlyStringValue] = stringValues;
    if (stringValues.length === 1 && onlyStringValue !== undefined) {
      return Effect.succeed(onlyStringValue);
    }
  }
  return Effect.fail(
    kafkaMappingError(`Kafka key cannot derive View Server row key field: ${rowKeyField}`, key),
  );
};

type AnyKafkaRuntimeTopic = KafkaTopicDefinitionMarker &
  KafkaTopicDecoder<KafkaTopicSchemaRegistry, string, string, unknown> & {
    readonly regions: NonEmptyReadonlyArray<string>;
    readonly viewServerTopic: string;
  };

type AnyKafkaRuntimeSourceTopic = KafkaTopicSourceDecoder<
  KafkaTopicSchemaRegistry,
  string,
  string,
  unknown
> & {
  readonly regions: NonEmptyReadonlyArray<string>;
  readonly topic: string;
  readonly viewServerTopic: string;
};

const decodeKafkaTopicMessageEffect: (
  topic: AnyKafkaRuntimeTopic | AnyKafkaRuntimeSourceTopic,
  input:
    | KafkaTopicDecodeInput<KafkaTopicSchemaRegistry, string, string>
    | KafkaTopicSourceDecodeInput<string>,
) => Effect.Effect<
  {
    readonly row: object;
    readonly viewServerTopic: string;
  },
  unknown
> = Effect.fn("ViewServerConfig.kafka.topic.decodeMessage")(function* (
  topic: AnyKafkaRuntimeTopic | AnyKafkaRuntimeSourceTopic,
  input:
    | KafkaTopicDecodeInput<KafkaTopicSchemaRegistry, string, string>
    | KafkaTopicSourceDecodeInput<string>,
) {
  if (isKafkaRuntimeTopicDefinition(topic)) {
    return yield* topic[KafkaTopicDecodeTypeId]({
      keyBytes: input.keyBytes,
      valueBytes: input.valueBytes,
      region: input.region,
      metadata: input.metadata,
    });
  }
  if (!("schema" in input)) {
    return yield* Effect.fail(
      kafkaMappingError("Topic-owned Kafka source decode is missing topic metadata", {
        rowKeyField: undefined,
        schema: undefined,
        viewServerTopic: undefined,
      }),
    );
  }
  if (
    input.schema === undefined ||
    input.rowKeyField === undefined ||
    input.viewServerTopic === undefined
  ) {
    return yield* Effect.fail(
      kafkaMappingError("Topic-owned Kafka source decode is missing topic metadata", {
        rowKeyField: input.rowKeyField,
        schema: input.schema,
        viewServerTopic: input.viewServerTopic,
      }),
    );
  }
  const decoded = yield* topic[KafkaTopicDecodeTypeId]({
    keyBytes: input.keyBytes,
    valueBytes: input.valueBytes,
    region: input.region,
    metadata: input.metadata,
    rowKeyField: input.rowKeyField,
    schema: input.schema,
    viewServerTopic: input.viewServerTopic,
  });
  return {
    viewServerTopic: decoded.viewServerTopic,
    row: decoded.row,
  };
});

const isKafkaRuntimeTopicDefinition = (
  topic: AnyKafkaRuntimeTopic | AnyKafkaRuntimeSourceTopic,
): topic is AnyKafkaRuntimeTopic => !isKafkaRuntimeTopicSourceDefinition(topic);

type DecodedTopicTopics<Topic> =
  Topic extends KafkaTopicDecoder<infer Topics, infer _ViewTopic, infer _Region, infer _Error>
    ? Topics
    : never;

type DecodedTopicViewTopic<Topic> =
  Topic extends KafkaTopicDecoder<infer _Topics, infer ViewTopic, infer _Region, infer _Error>
    ? ViewTopic
    : never;

type DecodedTopicRegion<Topic> = Topic extends {
  readonly regions: NonEmptyReadonlyArray<infer Region extends string>;
}
  ? Region
  : never;

type DecodedSourceTopicTopics<Topic> =
  Topic extends KafkaTopicSourceDecoder<infer Topics, infer _ViewTopic, infer _Region, infer _Error>
    ? Topics
    : never;

type DecodedSourceTopicViewTopic<Topic> =
  Topic extends KafkaTopicSourceDecoder<infer _Topics, infer ViewTopic, infer _Region, infer _Error>
    ? ViewTopic
    : never;

type DecodedTopicInput<Topic> =
  DecodedTopicViewTopic<Topic> extends Extract<keyof DecodedTopicTopics<Topic>, string>
    ? KafkaTopicDecodeInput<
        DecodedTopicTopics<Topic>,
        DecodedTopicViewTopic<Topic>,
        DecodedTopicRegion<Topic>
      >
    : never;

type DecodedTopicMessage<Topic> =
  DecodedTopicViewTopic<Topic> extends Extract<keyof DecodedTopicTopics<Topic>, string>
    ? KafkaDecodedTopicMessage<DecodedTopicTopics<Topic>, DecodedTopicViewTopic<Topic>>
    : never;

type DecodedSourceTopicMessage<Topic> =
  DecodedSourceTopicViewTopic<Topic> extends Extract<keyof DecodedSourceTopicTopics<Topic>, string>
    ? KafkaDecodedTopicMessage<DecodedSourceTopicTopics<Topic>, DecodedSourceTopicViewTopic<Topic>>
    : never;

export function decodeKafkaTopicMessage<Topic extends AnyKafkaRuntimeSourceTopic>(
  topic: Topic,
  input: KafkaTopicSourceDecodeInput<DecodedTopicRegion<Topic>>,
): Effect.Effect<DecodedSourceTopicMessage<Topic>, unknown>;
export function decodeKafkaTopicMessage<Topic extends AnyKafkaRuntimeTopic>(
  topic: Topic,
  input: DecodedTopicInput<Topic>,
): Effect.Effect<DecodedTopicMessage<Topic>, unknown>;
export function decodeKafkaTopicMessage(
  topic: AnyKafkaRuntimeTopic | AnyKafkaRuntimeSourceTopic,
  input:
    | KafkaTopicDecodeInput<KafkaTopicSchemaRegistry, string, string>
    | KafkaTopicSourceDecodeInput<string>,
): Effect.Effect<
  {
    readonly row: object;
    readonly viewServerTopic: string;
  },
  unknown
> {
  return decodeKafkaTopicMessageEffect(topic, input);
}

type KafkaTopicDefinitionInput<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends KafkaWritableViewTopic<Topics> = KafkaWritableViewTopic<Topics>,
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
> = "topic" extends keyof Candidate
  ? never
  : Candidate extends KafkaTopicDefinitionMarker & {
        readonly regions: infer TopicRegions extends NonEmptyReadonlyArray<
          Extract<keyof Regions, string>
        >;
        readonly value: infer ValueCodec extends KafkaCodec<unknown, unknown>;
        readonly key: infer KeyCodec extends KafkaCodec<unknown, unknown>;
        readonly viewServerTopic: infer ViewTopic extends KafkaWritableViewTopic<Topics>;
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
    : "key" extends keyof Candidate
      ? never
      : Candidate extends KafkaTopicDefinitionMarker & {
            readonly regions: infer TopicRegions extends NonEmptyReadonlyArray<
              Extract<keyof Regions, string>
            >;
            readonly value: infer ValueCodec extends KafkaCodec<unknown, unknown>;
            readonly viewServerTopic: infer ViewTopic extends KafkaWritableViewTopic<Topics>;
          }
        ? Candidate extends {
            readonly mapping: infer Mapping extends (
              input: KafkaMappingInputWithoutKey<
                Topics,
                ViewTopic,
                TopicRegions[number],
                ValueCodec
              >,
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

export type ValidateKafkaTopicSource<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  Candidate,
> = Candidate extends {
  readonly topic: string;
  readonly regions: infer TopicRegions extends NonEmptyReadonlyArray<
    Extract<keyof Regions, string>
  >;
  readonly value: infer ValueCodec extends KafkaCodec<unknown, unknown>;
  readonly key: infer KeyCodec extends KafkaCodec<unknown, unknown>;
}
  ? Candidate extends {
      readonly map: infer Mapping extends (
        input: KafkaTopicSourceMapInputWithKey<
          Topics,
          ViewTopic,
          TopicRegions[number],
          ValueCodec,
          KeyCodec
        >,
      ) => TopicRow<Topics, ViewTopic>;
    }
    ? Candidate extends KafkaTopicSourceInputWithKey<
        Topics,
        Regions,
        ViewTopic,
        ValueCodec,
        KeyCodec,
        TopicRegions,
        Mapping
      >
      ? Candidate &
          RejectExtraKeys<
            Candidate,
            KafkaTopicSourceInputWithKey<
              Topics,
              Regions,
              ViewTopic,
              ValueCodec,
              KeyCodec,
              TopicRegions,
              Mapping
            >
          >
      : never
    : never
  : "key" extends keyof Candidate
    ? never
    : Candidate extends {
          readonly topic: string;
          readonly regions: infer TopicRegions extends NonEmptyReadonlyArray<
            Extract<keyof Regions, string>
          >;
          readonly value: infer ValueCodec extends KafkaCodec<unknown, unknown>;
        }
      ? Candidate extends {
          readonly map: infer Mapping extends (
            input: KafkaTopicSourceMapInputWithoutKey<
              Topics,
              ViewTopic,
              TopicRegions[number],
              ValueCodec
            >,
          ) => TopicRow<Topics, ViewTopic>;
        }
        ? Candidate extends KafkaTopicSourceInputWithoutKey<
            Topics,
            Regions,
            ViewTopic,
            ValueCodec,
            TopicRegions,
            Mapping
          >
          ? Candidate &
              RejectExtraKeys<
                Candidate,
                KafkaTopicSourceInputWithoutKey<
                  Topics,
                  Regions,
                  ViewTopic,
                  ValueCodec,
                  TopicRegions,
                  Mapping
                >
              >
          : never
        : never
      : never;

export type ViewServerKafkaCommittedStartFrom = {
  readonly committedConsumerGroup: string;
  readonly fallback?: "earliest" | "latest" | "fail";
};

export type ViewServerKafkaStartFrom = "earliest" | "latest" | ViewServerKafkaCommittedStartFrom;

export type RuntimeOptions<
  Topics extends KafkaTopicSchemaRegistry,
  ConfigRegions extends RuntimeRegions,
  Options,
> = {
  readonly [Key in Extract<keyof Options, "websocketPort">]: RuntimeValue<number>;
} & (Options extends { readonly kafka: infer CandidateKafka }
  ? {
      readonly kafka: RuntimeKafkaOptions<Topics, ConfigRegions, CandidateKafka>;
    }
  : {
      readonly kafka?: undefined;
    });

type RuntimeKafkaRegions<
  ConfigRegions extends RuntimeRegions,
  CandidateKafka,
> = CandidateKafka extends {
  readonly regions: infer Regions extends RuntimeRegions;
}
  ? Regions
  : ConfigRegions;

type RuntimeKafkaOptions<
  Topics extends KafkaTopicSchemaRegistry,
  ConfigRegions extends RuntimeRegions,
  CandidateKafka,
> = CandidateKafka extends {
  readonly consumerGroupId: string;
}
  ? {
      readonly consumerGroupId: string;
      readonly startFrom?: ViewServerKafkaStartFrom;
    } & (CandidateKafka extends { readonly regions: infer Regions extends RuntimeRegions }
      ? { readonly regions: Regions }
      : { readonly regions?: undefined }) &
      (CandidateKafka extends { readonly topics: infer KafkaTopics extends Record<string, object> }
        ? {
            readonly topics: ValidateKafkaTopics<
              Topics,
              RuntimeKafkaRegions<ConfigRegions, CandidateKafka>,
              KafkaTopics
            >;
          }
        : { readonly topics?: undefined })
  : never;

export type RuntimeOptionsCandidate = {
  readonly websocketPort?: RuntimeValue<number>;
  readonly kafka?: {
    readonly consumerGroupId: string;
    readonly startFrom?: ViewServerKafkaStartFrom;
    readonly regions?: RuntimeRegions;
    readonly topics?: Record<string, object>;
  };
};

export type ValidateRuntimeOptions<
  Topics extends KafkaTopicSchemaRegistry,
  ConfigRegions extends RuntimeRegions,
  Options,
> = Options extends RuntimeOptionsCandidate
  ? RuntimeOptions<Topics, ConfigRegions, Options>
  : never;

export type RuntimeOptionsDefinition<
  Topics extends KafkaTopicSchemaRegistry,
  ConfigRegions extends RuntimeRegions,
  Options,
> = ValidateRuntimeOptions<Topics, ConfigRegions, Options>;

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

type RejectExtraRuntimeKafkaStartFromKeys<Options> = Options extends {
  readonly kafka: {
    readonly startFrom: infer CandidateStartFrom;
  };
}
  ? CandidateStartFrom extends object
    ? {
        readonly kafka: {
          readonly startFrom: CandidateStartFrom &
            RejectExtraKeys<CandidateStartFrom, ViewServerKafkaCommittedStartFrom>;
        };
      }
    : unknown
  : unknown;

type TopicOwnedKafkaSourceTopic<Topics extends object> = Extract<
  {
    readonly [Topic in keyof Topics]: Topics[Topic] extends {
      readonly kafkaSource: object;
    }
      ? Topic
      : never;
  }[keyof Topics],
  string
>;

type TopicOwnedKafkaSourceRegion<Topics extends object> = Extract<
  {
    readonly [Topic in keyof Topics]: Topics[Topic] extends {
      readonly kafkaSource: {
        readonly regions: infer Regions extends ReadonlyArray<string>;
      };
    }
      ? Regions[number]
      : never;
  }[keyof Topics],
  string
>;

type RuntimeRegionsAreBroad<Regions extends RuntimeRegions> = string extends keyof Regions
  ? true
  : false;

type RuntimeKafkaExplicitTopicRegionsConstraint<
  ConfigRegions extends RuntimeRegions,
  Options,
> = Options extends {
  readonly kafka: {
    readonly topics: Record<string, object>;
  };
}
  ? Options extends {
      readonly kafka: {
        readonly regions: RuntimeRegions;
      };
    }
    ? unknown
    : RuntimeRegionsAreBroad<ConfigRegions> extends true
      ? {
          readonly kafka: {
            readonly regions: never;
          };
        }
      : unknown
  : unknown;

type RuntimeKafkaSourceRegionConstraint<
  Topics extends object,
  ConfigRegions extends RuntimeRegions,
  Options,
> = [TopicOwnedKafkaSourceRegion<Topics>] extends [never]
  ? unknown
  : Options extends {
        readonly kafka: {
          readonly regions: infer Regions extends RuntimeRegions;
        };
      }
    ? RuntimeRegionsAreBroad<Regions> extends true
      ? {
          readonly kafka: {
            readonly regions: never;
          };
        }
      : Exclude<TopicOwnedKafkaSourceRegion<Topics>, keyof Regions> extends never
        ? unknown
        : {
            readonly kafka: {
              readonly regions: never;
            };
          }
    : RuntimeRegionsAreBroad<ConfigRegions> extends true
      ? {
          readonly kafka: {
            readonly regions: never;
          };
        }
      : unknown;

type RuntimeKafkaSourceOwnershipConstraint<Topics extends object, Options> = [
  TopicOwnedKafkaSourceTopic<Topics>,
] extends [never]
  ? unknown
  : Options extends {
        readonly kafka: infer CandidateKafka;
      }
    ? {
        readonly kafka: CandidateKafka & {
          readonly consumerGroupId: string;
          readonly topics?: never;
        };
      }
    : {
        readonly kafka: never;
      };

export type ExactRuntimeOptions<
  Topics extends KafkaTopicSchemaRegistry,
  ConfigRegions extends RuntimeRegions,
  Options,
> = Options &
  ValidateRuntimeOptions<Topics, ConfigRegions, Options> &
  RejectExtraKeys<Options, ValidateRuntimeOptions<Topics, ConfigRegions, Options>> &
  RejectExtraRuntimeKafkaKeys<Options, ValidateRuntimeOptions<Topics, ConfigRegions, Options>> &
  RejectExtraRuntimeKafkaStartFromKeys<Options> &
  RuntimeKafkaExplicitTopicRegionsConstraint<ConfigRegions, Options> &
  RuntimeKafkaSourceOwnershipConstraint<Topics, Options> &
  RuntimeKafkaSourceRegionConstraint<Topics, ConfigRegions, Options>;

export type KafkaTopicHelper<Topics extends KafkaTopicSchemaRegistry> = <
  const Regions extends RuntimeRegions,
>() => {
  <
    const TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
    ValueCodec extends KafkaCodec<unknown, unknown>,
    KeyCodec extends KafkaCodec<unknown, unknown>,
    const ViewTopic extends KafkaWritableViewTopic<Topics>,
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
    const ViewTopic extends KafkaWritableViewTopic<Topics>,
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
  ViewTopic extends KafkaWritableViewTopic<Topics>,
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
  ViewTopic extends KafkaWritableViewTopic<Topics>,
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

const makeKafkaRuntimeTopicSourceWithKey = <
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaTopicSourceMapInputWithKey<
      Topics,
      ViewTopic,
      TopicRegions[number],
      ValueCodec,
      KeyCodec
    >,
  ) => TopicRow<Topics, ViewTopic>,
>(
  viewServerTopic: ViewTopic,
  topic: KafkaTopicSourceInputWithKey<
    Topics,
    Regions,
    ViewTopic,
    ValueCodec,
    KeyCodec,
    TopicRegions,
    Mapping
  >,
): KafkaRuntimeTopicSourceDefinition<Topics, Regions, ViewTopic, TopicRegions> => ({
  ...topic,
  [KafkaRuntimeTopicSourceTypeId]: true,
  viewServerTopic,
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
      const rowKey = yield* rowKeyFromDecodedKafkaKey(key, input.rowKeyField);
      const row = yield* mapKafkaPayload(() =>
        topic.map({
          key,
          value,
          region: input.region,
          rowKey,
          schema: input.schema,
          metadata: input.metadata,
        }),
      );
      return {
        row,
        rowKey,
        viewServerTopic,
      };
    }),
});

const makeKafkaRuntimeTopicSourceWithoutKey = <
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaTopicSourceMapInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => TopicRow<Topics, ViewTopic>,
>(
  viewServerTopic: ViewTopic,
  topic: KafkaTopicSourceInputWithoutKey<
    Topics,
    Regions,
    ViewTopic,
    ValueCodec,
    TopicRegions,
    Mapping
  >,
): KafkaRuntimeTopicSourceDefinition<Topics, Regions, ViewTopic, TopicRegions> => ({
  ...topic,
  [KafkaRuntimeTopicSourceTypeId]: true,
  viewServerTopic,
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
      const rowKey = yield* rowKeyFromDecodedKafkaKey(key, input.rowKeyField);
      const row = yield* mapKafkaPayload(() =>
        topic.map({
          key,
          value,
          region: input.region,
          rowKey,
          schema: input.schema,
          metadata: input.metadata,
        }),
      );
      return {
        row,
        rowKey,
        viewServerTopic,
      };
    }),
});

const makeKafkaRuntimeTopicSource = <
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
>(
  viewServerTopic: ViewTopic,
  topic: KafkaTopicSourceDefinition<Topics, Regions, ViewTopic, ValueCodec, KeyCodec, TopicRegions>,
): KafkaRuntimeTopicSourceDefinition<Topics, Regions, ViewTopic, TopicRegions> => {
  if ("key" in topic) {
    return makeKafkaRuntimeTopicSourceWithKey(viewServerTopic, topic);
  }
  return makeKafkaRuntimeTopicSourceWithoutKey(viewServerTopic, topic);
};

type KafkaSourceTopicRegistry<
  Topics extends KafkaTopicSchemaRegistry,
  _Regions extends RuntimeRegions,
> = {
  readonly [Topic in keyof Topics]: {
    readonly kafkaSource?: object | undefined;
  };
};

export const makeKafkaRuntimeTopicSources = <
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
>(
  topics: KafkaSourceTopicRegistry<Topics, Regions>,
): ReadonlyArray<
  KafkaRuntimeTopicSourceDefinition<Topics, Regions, Extract<keyof Topics, string>>
> => {
  const runtimeTopics: Array<
    KafkaRuntimeTopicSourceDefinition<Topics, Regions, Extract<keyof Topics, string>>
  > = [];
  for (const viewServerTopic in topics) {
    const kafkaSource = topics[viewServerTopic].kafkaSource;
    if (kafkaSource === undefined) {
      continue;
    }
    if (!isKafkaTopicSourceDefinition<Topics, Regions, typeof viewServerTopic>(kafkaSource)) {
      throw new Error(`View Server topic ${viewServerTopic} has an invalid Kafka source.`);
    }
    runtimeTopics.push(makeKafkaRuntimeTopicSource(viewServerTopic, kafkaSource));
  }
  return runtimeTopics;
};

export const isKafkaTopicSourceDefinition = <
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends Extract<keyof Topics, string>,
>(
  topic: unknown,
): topic is KafkaTopicSourceDefinition<Topics, Regions, ViewTopic> => {
  if (!isInspectableObject(topic)) {
    return false;
  }
  const ownKeys = Object.getOwnPropertyNames(topic);
  const allowedKeys =
    "key" in topic
      ? ["topic", "regions", "value", "key", "map"]
      : ["topic", "regions", "value", "map"];
  if (!ownKeys.every((key) => allowedKeys.includes(key))) {
    return false;
  }
  const sourceTopic = Reflect.get(topic, "topic");
  const regions = Reflect.get(topic, "regions");
  const value = Reflect.get(topic, "value");
  const key = Reflect.get(topic, "key");
  const hasOwnKey = Object.prototype.hasOwnProperty.call(topic, "key");
  const map = Reflect.get(topic, "map");
  return (
    typeof sourceTopic === "string" &&
    Array.isArray(regions) &&
    regions.length > 0 &&
    regions.every((region) => typeof region === "string") &&
    isKafkaCodec(value) &&
    ((key === undefined && !hasOwnKey) || isKafkaCodec(key)) &&
    typeof map === "function"
  );
};

export const isKafkaRuntimeTopicSourceDefinition = (
  topic: unknown,
): topic is KafkaRuntimeTopicSourceDefinition<KafkaTopicSchemaRegistry, RuntimeRegions, string> => {
  if (typeof topic !== "object" || topic === null) {
    return false;
  }
  return Reflect.get(topic, KafkaRuntimeTopicSourceTypeId) === true;
};

export const defineKafkaTopic = <Topics extends KafkaTopicSchemaRegistry>(
  topics: Topics,
): KafkaTopicHelper<Topics> => {
  function forRegions<const Regions extends RuntimeRegions>() {
    const kafkaTopicDefinitionHasKey = <
      const TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
      ValueCodec extends KafkaCodec<unknown, unknown>,
      KeyCodec extends KafkaCodec<unknown, unknown>,
      const ViewTopic extends KafkaWritableViewTopic<Topics>,
    >(
      topic: KafkaTopicDefinitionInput<
        Topics,
        Regions,
        ViewTopic,
        ValueCodec,
        KeyCodec,
        TopicRegions
      >,
    ): topic is KafkaTopicWithKeyInput<
      Topics,
      Regions,
      ViewTopic,
      ValueCodec,
      KeyCodec,
      TopicRegions
    > => isKafkaCodec(Reflect.get(topic, "key"));

    function topicHelper<
      const TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
      ValueCodec extends KafkaCodec<unknown, unknown>,
      KeyCodec extends KafkaCodec<unknown, unknown>,
      const ViewTopic extends KafkaWritableViewTopic<Topics>,
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
      const ViewTopic extends KafkaWritableViewTopic<Topics>,
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
      const ViewTopic extends KafkaWritableViewTopic<Topics>,
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
      validateKafkaViewTopicOwnership(topics, topic.viewServerTopic);
      if (kafkaTopicDefinitionHasKey(topic)) {
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
