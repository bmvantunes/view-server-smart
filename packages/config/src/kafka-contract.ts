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

const KafkaCodecValueTypeId: unique symbol = Symbol("@effect-view-server/config/KafkaCodecValue");
const KafkaCodecErrorTypeId: unique symbol = Symbol("@effect-view-server/config/KafkaCodecError");
const KafkaCodecDecodeTypeId: unique symbol = Symbol("@effect-view-server/config/KafkaCodecDecode");
const KafkaTopicDefinitionTypeId: unique symbol = Symbol(
  "@effect-view-server/config/KafkaTopicDefinition",
);
const KafkaTopicDecodeTypeId: unique symbol = Symbol("@effect-view-server/config/KafkaTopicDecode");
const KafkaTopicRowKeyDecodeTypeId: unique symbol = Symbol(
  "@effect-view-server/config/KafkaTopicRowKeyDecode",
);
const KafkaTopicSchemaTypeId: unique symbol = Symbol("@effect-view-server/config/KafkaTopicSchema");
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

type KafkaTopicRowKeyDecodeInput<
  Topics extends KafkaTopicSchemaRegistry,
  _ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
> = {
  readonly keyBytes: Uint8Array;
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

type KafkaTopicRowKeyDecoder<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  E,
> = {
  readonly [KafkaTopicRowKeyDecodeTypeId]: {
    bivarianceHack(
      input: KafkaTopicRowKeyDecodeInput<Topics, ViewTopic, Region>,
    ): Effect.Effect<string | null, E>;
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

type KafkaTopicRow<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
> = TopicRow<Topics, ViewTopic>;

type KafkaTopicSchemaRegistry = Record<
  string,
  {
    readonly schema: RowSchema;
    readonly key: string;
    readonly source?: unknown;
  }
>;

type KafkaWritableViewTopic<Topics extends KafkaTopicSchemaRegistry> = Extract<
  {
    readonly [Topic in keyof Topics]: "source" extends keyof Topics[Topic]
      ? Topics[Topic] extends { readonly source: infer Source }
        ? Source extends { readonly kind: "grpc" }
          ? never
          : Topic
        : Topic
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

const keyFieldForKafkaTopic = <
  const ViewTopic extends string,
  Key extends string,
  Topics extends {
    readonly [Topic in ViewTopic]: {
      readonly key: Key;
    };
  },
>(
  topics: Topics,
  viewTopic: ViewTopic,
): Key => topics[viewTopic].key;

const viewTopicSourceKind = <Topics extends KafkaTopicSchemaRegistry>(
  topics: Topics,
  viewTopic: Extract<keyof Topics, string>,
): string | undefined => {
  const topicDefinition: unknown = topics[viewTopic];
  if (!isInspectableObject(topicDefinition)) {
    return undefined;
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
  if (viewTopicSourceKind(topics, viewTopic) === "grpc") {
    throw new Error(`Kafka source cannot publish into gRPC-owned View Server topic: ${viewTopic}`);
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

export type KafkaSafeRowKeyInput<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  KeyCodec,
> = {
  readonly key: [KeyCodec] extends [KafkaCodec<unknown, unknown>]
    ? KafkaCodecType<KeyCodec>
    : string;
  readonly region: Region;
  readonly schema: KafkaTopicSchemaValue<Topics, ViewTopic>;
  readonly metadata: KafkaMessageMetadata<Region>;
};

export type KafkaUnsafeRowKeyInput<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  ValueCodec,
  KeyCodec,
> = KafkaSafeRowKeyInput<Topics, ViewTopic, Region, KeyCodec> & {
  readonly value: KafkaCodecType<ValueCodec>;
};

type KafkaMappedTopicRow<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
> = KafkaTopicRow<Topics, ViewTopic>;

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

type KafkaSafeRowKeyInputWithoutKey<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
> = {
  readonly key: string;
  readonly region: Region;
  readonly schema: KafkaTopicSchemaValue<Topics, ViewTopic>;
  readonly metadata: KafkaMessageMetadata<Region>;
};

type KafkaUnsafeRowKeyInputWithoutKey<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  ValueCodec extends KafkaCodec<unknown, unknown>,
> = KafkaSafeRowKeyInputWithoutKey<Topics, ViewTopic, Region> & {
  readonly value: KafkaCodecType<ValueCodec>;
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

type KafkaSafeRowKeyInputWithKey<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  KeyCodec extends KafkaCodec<unknown, unknown>,
> = {
  readonly key: KafkaCodecType<KeyCodec>;
  readonly region: Region;
  readonly schema: KafkaTopicSchemaValue<Topics, ViewTopic>;
  readonly metadata: KafkaMessageMetadata<Region>;
};

type KafkaUnsafeRowKeyInputWithKey<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
> = KafkaSafeRowKeyInputWithKey<Topics, ViewTopic, Region, KeyCodec> & {
  readonly value: KafkaCodecType<ValueCodec>;
};

type KafkaSafeRowKeyDefinitionWithoutKey<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
> = {
  readonly getSafeRowKey: (
    input: KafkaSafeRowKeyInputWithoutKey<Topics, ViewTopic, Region>,
  ) => string;
  readonly getUnsafeRowKey?: never;
};

type KafkaUnsafeRowKeyDefinitionWithoutKey<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  ValueCodec extends KafkaCodec<unknown, unknown>,
> = {
  readonly getSafeRowKey?: never;
  readonly getUnsafeRowKey: (
    input: KafkaUnsafeRowKeyInputWithoutKey<Topics, ViewTopic, Region, ValueCodec>,
  ) => string;
};

type KafkaSafeRowKeyDefinitionWithKey<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  KeyCodec extends KafkaCodec<unknown, unknown>,
> = {
  readonly getSafeRowKey: (
    input: KafkaSafeRowKeyInputWithKey<Topics, ViewTopic, Region, KeyCodec>,
  ) => string;
  readonly getUnsafeRowKey?: never;
};

type KafkaUnsafeRowKeyDefinitionWithKey<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
  Region extends string,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
> = {
  readonly getSafeRowKey?: never;
  readonly getUnsafeRowKey: (
    input: KafkaUnsafeRowKeyInputWithKey<Topics, ViewTopic, Region, ValueCodec, KeyCodec>,
  ) => string;
};

export type KafkaDecodedTopicMessage<
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
> = {
  readonly viewServerTopic: ViewTopic;
  readonly rowKey: string;
  readonly row: KafkaTopicRow<Topics, ViewTopic>;
};

type KafkaTopicWithoutKeyBaseInput<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends KafkaWritableViewTopic<Topics>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic>,
> = {
  readonly regions: TopicRegions;
  readonly value: SupportedKafkaCodec<ValueCodec>;
  readonly key?: never;
  readonly viewServerTopic: ViewTopic;
  readonly mapping: ExactMappingReturn<
    KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
    KafkaMappedTopicRow<Topics, ViewTopic>,
    Mapping
  >;
};

type KafkaTopicWithoutKeySafeInput<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends KafkaWritableViewTopic<Topics>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic>,
> = KafkaTopicWithoutKeyBaseInput<Topics, Regions, ViewTopic, ValueCodec, TopicRegions, Mapping> &
  KafkaSafeRowKeyDefinitionWithoutKey<Topics, ViewTopic, TopicRegions[number]>;

type KafkaTopicWithoutKeyUnsafeInput<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends KafkaWritableViewTopic<Topics>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic>,
> = KafkaTopicWithoutKeyBaseInput<Topics, Regions, ViewTopic, ValueCodec, TopicRegions, Mapping> &
  KafkaUnsafeRowKeyDefinitionWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>;

type KafkaTopicWithoutKeyInput<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends KafkaWritableViewTopic<Topics>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic>,
> =
  | KafkaTopicWithoutKeySafeInput<Topics, Regions, ViewTopic, ValueCodec, TopicRegions, Mapping>
  | KafkaTopicWithoutKeyUnsafeInput<Topics, Regions, ViewTopic, ValueCodec, TopicRegions, Mapping>;

type KafkaTopicWithoutKey<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends KafkaWritableViewTopic<Topics>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic>,
> = KafkaTopicWithoutKeyInput<Topics, Regions, ViewTopic, ValueCodec, TopicRegions, Mapping> &
  KafkaTopicDefinitionMarker &
  KafkaTopicSchemaMarker<Topics, ViewTopic> &
  KafkaTopicRowKeyDecoder<
    Topics,
    ViewTopic,
    TopicRegions[number],
    KafkaDecodeError | KafkaMappingError
  > &
  KafkaTopicDecoder<
    Topics,
    ViewTopic,
    TopicRegions[number],
    KafkaCodecError<ValueCodec> | KafkaDecodeError | KafkaMappingError
  >;

type KafkaTopicWithKeyBaseInput<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends KafkaWritableViewTopic<Topics>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic>,
> = {
  readonly regions: TopicRegions;
  readonly value: SupportedKafkaCodec<ValueCodec>;
  readonly key: SupportedKafkaCodec<KeyCodec>;
  readonly viewServerTopic: ViewTopic;
  readonly mapping: ExactMappingReturn<
    KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
    KafkaMappedTopicRow<Topics, ViewTopic>,
    Mapping
  >;
};

type KafkaTopicWithKeySafeInput<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends KafkaWritableViewTopic<Topics>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic>,
> = KafkaTopicWithKeyBaseInput<
  Topics,
  Regions,
  ViewTopic,
  ValueCodec,
  KeyCodec,
  TopicRegions,
  Mapping
> &
  KafkaSafeRowKeyDefinitionWithKey<Topics, ViewTopic, TopicRegions[number], KeyCodec>;

type KafkaTopicWithKeyUnsafeInput<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends KafkaWritableViewTopic<Topics>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic>,
> = KafkaTopicWithKeyBaseInput<
  Topics,
  Regions,
  ViewTopic,
  ValueCodec,
  KeyCodec,
  TopicRegions,
  Mapping
> &
  KafkaUnsafeRowKeyDefinitionWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>;

type KafkaTopicWithKeyInput<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends KafkaWritableViewTopic<Topics>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic>,
> =
  | KafkaTopicWithKeySafeInput<
      Topics,
      Regions,
      ViewTopic,
      ValueCodec,
      KeyCodec,
      TopicRegions,
      Mapping
    >
  | KafkaTopicWithKeyUnsafeInput<
      Topics,
      Regions,
      ViewTopic,
      ValueCodec,
      KeyCodec,
      TopicRegions,
      Mapping
    >;

type KafkaTopicWithKey<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends KafkaWritableViewTopic<Topics>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  Mapping extends (
    input: KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic>,
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
  KafkaTopicRowKeyDecoder<
    Topics,
    ViewTopic,
    TopicRegions[number],
    KafkaCodecError<KeyCodec> | KafkaDecodeError | KafkaMappingError
  > &
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
  ) => KafkaMappedTopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic>,
  MappingWithKey extends (
    input: KafkaMappingInputWithKey<
      Topics,
      ViewTopic,
      TopicRegions[number],
      ValueCodec,
      Extract<KeyCodec, KafkaCodec<unknown, unknown>>
    >,
  ) => KafkaMappedTopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithKey<
      Topics,
      ViewTopic,
      TopicRegions[number],
      ValueCodec,
      Extract<KeyCodec, KafkaCodec<unknown, unknown>>
    >,
  ) => KafkaMappedTopicRow<Topics, ViewTopic>,
> =
  IsAny<ValueCodec> extends true
    ? never
    : IsAny<KeyCodec> extends true
      ? never
      :
          | KafkaTopicWithoutKey<
              Topics,
              Regions,
              ViewTopic,
              ValueCodec,
              TopicRegions,
              MappingWithoutKey
            >
          | (KeyCodec extends KafkaCodec<unknown, unknown>
              ? MappingWithKey extends (
                  input: KafkaMappingInputWithKey<
                    Topics,
                    ViewTopic,
                    TopicRegions[number],
                    ValueCodec,
                    KeyCodec
                  >,
                ) => KafkaMappedTopicRow<Topics, ViewTopic>
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
  KafkaTopicSchemaMarker<Topics, KafkaWritableViewTopic<Topics>> &
  KafkaTopicRowKeyDecoder<Topics, KafkaWritableViewTopic<Topics>, TopicRegions[number], unknown> &
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

const mapKafkaRowKey = <A>(resolve: () => A): Effect.Effect<A, KafkaMappingError> =>
  Effect.try({
    try: resolve,
    catch: (cause) => kafkaMappingError("Failed to resolve Kafka row key", cause),
  });

const completeKafkaMappedRow = <
  Topics extends KafkaTopicSchemaRegistry,
  ViewTopic extends Extract<keyof Topics, string>,
>(
  schema: KafkaTopicSchemaValue<Topics, ViewTopic>,
  keyField: Topics[ViewTopic]["key"],
  rowKey: string,
  mappedRow: KafkaMappedTopicRow<Topics, ViewTopic>,
): Effect.Effect<KafkaTopicRow<Topics, ViewTopic>, KafkaMappingError> =>
  Effect.try({
    try: () =>
      Schema.decodeUnknownSync(schema)({
        ...mappedRow,
        [keyField]: rowKey,
      }),
    catch: (cause) => kafkaMappingError("Kafka mapped row failed topic schema", cause),
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

export const decodeKafkaTopicRowKey = Effect.fn("ViewServerConfig.kafka.topic.decodeRowKey")(
  function* <
    Topics extends KafkaTopicSchemaRegistry,
    Regions extends RuntimeRegions,
    TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  >(
    topic: KafkaRuntimeTopicDefinition<Topics, Regions, TopicRegions>,
    input: {
      readonly keyBytes: Uint8Array;
      readonly region: TopicRegions[number];
      readonly metadata: KafkaMessageMetadata<TopicRegions[number]>;
    },
  ) {
    return yield* topic[KafkaTopicRowKeyDecodeTypeId](input);
  },
);

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
  ) => KafkaMappedTopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic>,
  MappingWithKey extends (
    input: KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic> = (
    input: KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic>,
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

const kafkaTopicInputHasKey = <
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  ViewTopic extends KafkaWritableViewTopic<Topics>,
  ValueCodec extends KafkaCodec<unknown, unknown>,
  KeyCodec extends KafkaCodec<unknown, unknown>,
  TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
  MappingWithoutKey extends (
    input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic>,
  MappingWithKey extends (
    input: KafkaMappingInputWithKey<Topics, ViewTopic, TopicRegions[number], ValueCodec, KeyCodec>,
  ) => KafkaMappedTopicRow<Topics, ViewTopic>,
>(
  topic: KafkaTopicDefinitionInput<
    Topics,
    Regions,
    ViewTopic,
    ValueCodec,
    KeyCodec,
    TopicRegions,
    MappingWithoutKey,
    MappingWithKey
  >,
): topic is KafkaTopicWithKeyInput<
  Topics,
  Regions,
  ViewTopic,
  ValueCodec,
  KeyCodec,
  TopicRegions,
  MappingWithKey
> => "key" in topic;

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
      ) => KafkaMappedTopicRow<Topics, ViewTopic>;
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
        readonly viewServerTopic: infer ViewTopic extends KafkaWritableViewTopic<Topics>;
      }
    ? Candidate extends {
        readonly mapping: infer Mapping extends (
          input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
        ) => KafkaMappedTopicRow<Topics, ViewTopic>;
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

export type ViewServerKafkaCommittedStartFrom = {
  readonly committedConsumerGroup: string;
  readonly fallback?: "earliest" | "latest" | "fail";
};

export type ViewServerKafkaStartFrom = "earliest" | "latest" | ViewServerKafkaCommittedStartFrom;

export type RuntimeOptions<
  Topics extends KafkaTopicSchemaRegistry,
  Regions extends RuntimeRegions,
  KafkaTopics extends Record<string, object>,
> = {
  readonly websocketPort: RuntimeValue<number>;
  readonly kafka: {
    readonly consumerGroupId: string;
    readonly startFrom?: ViewServerKafkaStartFrom;
    readonly regions: Regions;
    readonly topics: ValidateKafkaTopics<Topics, Regions, KafkaTopics>;
  };
};

export type RuntimeOptionsCandidate = {
  readonly websocketPort: RuntimeValue<number>;
  readonly kafka: {
    readonly consumerGroupId: string;
    readonly startFrom?: ViewServerKafkaStartFrom;
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

export type ExactRuntimeOptions<Topics extends KafkaTopicSchemaRegistry, Options> = Options &
  ValidateRuntimeOptions<Topics, Options> &
  RejectExtraKeys<Options, ValidateRuntimeOptions<Topics, Options>> &
  RejectExtraRuntimeKafkaKeys<Options, ValidateRuntimeOptions<Topics, Options>> &
  RejectExtraRuntimeKafkaStartFromKeys<Options>;

export type KafkaTopicHelper<Topics extends KafkaTopicSchemaRegistry> = <
  const Regions extends RuntimeRegions,
>() => {
  <
    const TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
    ValueCodec extends KafkaCodec<unknown, unknown>,
    const ViewTopic extends KafkaWritableViewTopic<Topics>,
    Mapping extends (
      input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
    ) => KafkaMappedTopicRow<Topics, ViewTopic>,
  >(
    topic: KafkaTopicWithoutKeySafeInput<
      Topics,
      Regions,
      ViewTopic,
      ValueCodec,
      TopicRegions,
      Mapping
    >,
  ): KafkaTopicWithoutKey<Topics, Regions, ViewTopic, ValueCodec, TopicRegions, Mapping>;
  <
    const TopicRegions extends NonEmptyReadonlyArray<Extract<keyof Regions, string>>,
    ValueCodec extends KafkaCodec<unknown, unknown>,
    const ViewTopic extends KafkaWritableViewTopic<Topics>,
    Mapping extends (
      input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
    ) => KafkaMappedTopicRow<Topics, ViewTopic>,
  >(
    topic: KafkaTopicWithoutKeyUnsafeInput<
      Topics,
      Regions,
      ViewTopic,
      ValueCodec,
      TopicRegions,
      Mapping
    >,
  ): KafkaTopicWithoutKey<Topics, Regions, ViewTopic, ValueCodec, TopicRegions, Mapping>;
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
    ) => KafkaMappedTopicRow<Topics, ViewTopic>,
  >(
    topic: KafkaTopicWithKeySafeInput<
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
    ) => KafkaMappedTopicRow<Topics, ViewTopic>,
  >(
    topic: KafkaTopicWithKeyUnsafeInput<
      Topics,
      Regions,
      ViewTopic,
      ValueCodec,
      KeyCodec,
      TopicRegions,
      Mapping
    >,
  ): KafkaTopicWithKey<Topics, Regions, ViewTopic, ValueCodec, KeyCodec, TopicRegions, Mapping>;
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
  ) => KafkaMappedTopicRow<Topics, ViewTopic>,
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
  keyField: Topics[ViewTopic]["key"],
) => ({
  ...topic,
  [KafkaTopicDefinitionTypeId]: true as const,
  [KafkaTopicSchemaTypeId]: schema,
  [KafkaTopicRowKeyDecodeTypeId]: (
    input: KafkaTopicRowKeyDecodeInput<Topics, ViewTopic, TopicRegions[number]>,
  ) =>
    Effect.gen(function* () {
      const key = yield* decodeKafkaCodec(topic.key, {
        bytes: input.keyBytes,
        metadata: input.metadata,
      });
      if (topic.getSafeRowKey === undefined) {
        return null;
      }
      return yield* mapKafkaRowKey(() =>
        topic.getSafeRowKey({
          key,
          region: input.region,
          schema,
          metadata: input.metadata,
        }),
      );
    }),
  [KafkaTopicDecodeTypeId]: (
    input: KafkaTopicDecodeInput<Topics, ViewTopic, TopicRegions[number]>,
  ) =>
    Effect.gen(function* () {
      const value = yield* decodeKafkaCodec(topic.value, {
        bytes: input.valueBytes,
        metadata: input.metadata,
      });
      const key = yield* decodeKafkaCodec(topic.key, {
        bytes: input.keyBytes,
        metadata: input.metadata,
      });
      const rowKey = yield* mapKafkaRowKey(() =>
        topic.getSafeRowKey === undefined
          ? topic.getUnsafeRowKey({
              key,
              value,
              region: input.region,
              schema,
              metadata: input.metadata,
            })
          : topic.getSafeRowKey({
              key,
              region: input.region,
              schema,
              metadata: input.metadata,
            }),
      );
      const mappedRow = yield* mapKafkaPayload(() =>
        topic.mapping({
          key,
          value,
          region: input.region,
          schema,
          metadata: input.metadata,
        }),
      );
      const row = yield* completeKafkaMappedRow(schema, keyField, rowKey, mappedRow);
      return {
        viewServerTopic: topic.viewServerTopic,
        rowKey,
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
  ) => KafkaMappedTopicRow<Topics, ViewTopic>,
>(
  topic: KafkaTopicWithoutKeyInput<Topics, Regions, ViewTopic, ValueCodec, TopicRegions, Mapping>,
  schema: KafkaTopicSchemaValue<Topics, ViewTopic>,
  keyField: Topics[ViewTopic]["key"],
): KafkaTopicWithoutKey<Topics, Regions, ViewTopic, ValueCodec, TopicRegions, Mapping> => ({
  ...topic,
  [KafkaTopicDefinitionTypeId]: true as const,
  [KafkaTopicSchemaTypeId]: schema,
  [KafkaTopicRowKeyDecodeTypeId]: (input) =>
    Effect.gen(function* () {
      const key = decodeKafkaStringKey({
        bytes: input.keyBytes,
        metadata: input.metadata,
      });
      if (topic.getSafeRowKey === undefined) {
        return null;
      }
      return yield* mapKafkaRowKey(() =>
        topic.getSafeRowKey({
          key,
          region: input.region,
          schema,
          metadata: input.metadata,
        }),
      );
    }),
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
      const rowKey = yield* mapKafkaRowKey(() =>
        topic.getSafeRowKey === undefined
          ? topic.getUnsafeRowKey({
              key,
              value,
              region: input.region,
              schema,
              metadata: input.metadata,
            })
          : topic.getSafeRowKey({
              key,
              region: input.region,
              schema,
              metadata: input.metadata,
            }),
      );
      const mappedRow = yield* mapKafkaPayload(() =>
        topic.mapping({
          key,
          value,
          region: input.region,
          schema,
          metadata: input.metadata,
        }),
      );
      const row = yield* completeKafkaMappedRow(schema, keyField, rowKey, mappedRow);
      return {
        viewServerTopic: topic.viewServerTopic,
        rowKey,
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
      const ViewTopic extends KafkaWritableViewTopic<Topics>,
      Mapping extends (
        input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
      ) => KafkaMappedTopicRow<Topics, ViewTopic>,
    >(
      topic: KafkaTopicWithoutKeySafeInput<
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
      const ViewTopic extends KafkaWritableViewTopic<Topics>,
      Mapping extends (
        input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
      ) => KafkaMappedTopicRow<Topics, ViewTopic>,
    >(
      topic: KafkaTopicWithoutKeyUnsafeInput<
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
      Mapping extends (
        input: KafkaMappingInputWithKey<
          Topics,
          ViewTopic,
          TopicRegions[number],
          ValueCodec,
          KeyCodec
        >,
      ) => KafkaMappedTopicRow<Topics, ViewTopic>,
    >(
      topic: KafkaTopicWithKeySafeInput<
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
      ) => KafkaMappedTopicRow<Topics, ViewTopic>,
    >(
      topic: KafkaTopicWithKeyUnsafeInput<
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
      KeyCodec extends KafkaCodec<unknown, unknown>,
      const ViewTopic extends KafkaWritableViewTopic<Topics>,
    >(
      topic: KafkaTopicDefinitionInput<
        Topics,
        Regions,
        ViewTopic,
        ValueCodec,
        KeyCodec,
        TopicRegions,
        (
          input: KafkaMappingInputWithoutKey<Topics, ViewTopic, TopicRegions[number], ValueCodec>,
        ) => KafkaMappedTopicRow<Topics, ViewTopic>
      >,
    ) {
      validateKafkaViewTopicOwnership(topics, topic.viewServerTopic);
      if (kafkaTopicInputHasKey(topic)) {
        const schema = schemaForKafkaTopic<ViewTopic, Topics[ViewTopic]["schema"], Topics>(
          topics,
          topic.viewServerTopic,
        );
        return makeKafkaTopicWithKey(
          topic,
          schema,
          keyFieldForKafkaTopic<ViewTopic, Topics[ViewTopic]["key"], Topics>(
            topics,
            topic.viewServerTopic,
          ),
        );
      }
      const schema = schemaForKafkaTopic<ViewTopic, Topics[ViewTopic]["schema"], Topics>(
        topics,
        topic.viewServerTopic,
      );
      return makeKafkaTopicWithoutKey(
        topic,
        schema,
        keyFieldForKafkaTopic<ViewTopic, Topics[ViewTopic]["key"], Topics>(
          topics,
          topic.viewServerTopic,
        ),
      );
    }

    return topicHelper;
  }

  return forRegions;
};
