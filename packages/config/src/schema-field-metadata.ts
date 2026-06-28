import { Schema, SchemaAST } from "effect";

export type ViewServerSchemaFieldMetadata = {
  readonly isNumeric: boolean;
  readonly isPureBigInt: boolean;
  readonly isString: boolean;
  readonly isStructured: boolean;
  readonly isStructuredObject: boolean;
  readonly sumResultKind?: "bigint" | "bigDecimal";
};

type NumericKind = "none" | "bigint" | "bigDecimal";
type NumericRuntimeDomain = "number" | "bigint" | "bigDecimal";
type NumericRuntimeDomainPathSegment =
  | readonly ["index", number]
  | readonly ["indexWildcard", number]
  | readonly ["property", string]
  | readonly ["propertyWildcard"];
type NumericRuntimeDomainPath = ReadonlyArray<NumericRuntimeDomainPathSegment>;
type NumericRuntimeDomainEntry = {
  readonly domain: NumericRuntimeDomain;
  readonly path: NumericRuntimeDomainPath;
};
type UnsupportedRuntimeDomainDescriptor = {
  readonly name: string;
  readonly run: unknown;
  readonly link: unknown;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const schemaAst = (schema: unknown): SchemaAST.AST | undefined => {
  if (!isRecord(schema)) {
    return undefined;
  }
  const ast = schema["ast"];
  return SchemaAST.isAST(ast) ? ast : undefined;
};

const isBigDecimalAst = (ast: SchemaAST.AST): boolean =>
  SchemaAST.isDeclaration(ast) &&
  isRecord(ast.annotations?.["typeConstructor"]) &&
  ast.annotations["typeConstructor"]["_tag"] === "effect/BigDecimal";

const declarationLink = (ast: SchemaAST.AST): unknown =>
  Reflect.get(Object(Reflect.get(ast, "annotations")), "toCodecJson") ??
  Reflect.get(Object(Reflect.get(ast, "annotations")), "toCodec");

const unsupportedRuntimeDomainDescriptor = (
  name: string,
  schema: { readonly ast: SchemaAST.AST },
): UnsupportedRuntimeDomainDescriptor => ({
  link: declarationLink(schema.ast),
  name,
  run: Reflect.get(schema.ast, "run"),
});

const unsupportedRuntimeDomainDescriptors = [
  unsupportedRuntimeDomainDescriptor("Date", Schema.Date),
  unsupportedRuntimeDomainDescriptor("DateTimeUtc", Schema.DateTimeUtc),
  unsupportedRuntimeDomainDescriptor("DateTimeZoned", Schema.DateTimeZoned),
  unsupportedRuntimeDomainDescriptor("Duration", Schema.Duration),
  unsupportedRuntimeDomainDescriptor("Error", Schema.Error()),
  unsupportedRuntimeDomainDescriptor("ErrorWithStack", Schema.Error({ includeStack: true })),
  unsupportedRuntimeDomainDescriptor("ErrorWithoutCause", Schema.Error({ excludeCause: true })),
  unsupportedRuntimeDomainDescriptor(
    "ErrorWithStackWithoutCause",
    Schema.Error({ includeStack: true, excludeCause: true }),
  ),
  unsupportedRuntimeDomainDescriptor("File", Schema.File),
  unsupportedRuntimeDomainDescriptor("FormData", Schema.FormData),
  unsupportedRuntimeDomainDescriptor("RegExp", Schema.RegExp),
  unsupportedRuntimeDomainDescriptor("Symbol", Schema.Symbol),
  unsupportedRuntimeDomainDescriptor("TimeZone", Schema.TimeZone),
  unsupportedRuntimeDomainDescriptor("TimeZoneNamed", Schema.TimeZoneNamed),
  unsupportedRuntimeDomainDescriptor("TimeZoneOffset", Schema.TimeZoneOffset),
  unsupportedRuntimeDomainDescriptor("Uint8Array", Schema.Uint8Array),
  unsupportedRuntimeDomainDescriptor("URL", Schema.URL),
  unsupportedRuntimeDomainDescriptor("URLSearchParams", Schema.URLSearchParams),
];

const unsupportedRuntimeDeclarationName = (ast: SchemaAST.AST): string | undefined => {
  if (SchemaAST.isUniqueSymbol(ast) || ast._tag === "Symbol") {
    return "Symbol";
  }
  if (!SchemaAST.isDeclaration(ast)) {
    return undefined;
  }
  const run = Reflect.get(ast, "run");
  const link = declarationLink(ast);
  return unsupportedRuntimeDomainDescriptors.find(
    (descriptor) => descriptor.run === run && descriptor.link === link,
  )?.name;
};

const isStringLiteralAst = (ast: SchemaAST.AST): boolean =>
  SchemaAST.isLiteral(ast) && typeof ast.literal === "string";

const numericKindAst = (ast: SchemaAST.AST): NumericKind => {
  if (SchemaAST.isBigInt(ast)) {
    return "bigint";
  }
  if (SchemaAST.isLiteral(ast) && typeof ast.literal === "bigint") {
    return "bigint";
  }
  if (SchemaAST.isNumber(ast) || isBigDecimalAst(ast)) {
    return "bigDecimal";
  }
  if (SchemaAST.isLiteral(ast) && typeof ast.literal === "number") {
    return "bigDecimal";
  }
  if (!SchemaAST.isUnion(ast) || ast.types.length === 0) {
    return "none";
  }
  let sawBigDecimal = false;
  for (const member of ast.types) {
    const kind = numericKindAst(member);
    if (kind === "none") {
      return "none";
    }
    sawBigDecimal ||= kind === "bigDecimal";
  }
  return sawBigDecimal ? "bigDecimal" : "bigint";
};

const addNumericRuntimeDomainAtPath = (
  entries: Array<NumericRuntimeDomainEntry>,
  path: NumericRuntimeDomainPath,
  domain: NumericRuntimeDomain,
) => entries.push({ domain, path });

const numericRuntimePathSegmentsOverlap = (
  left: NumericRuntimeDomainPathSegment,
  right: NumericRuntimeDomainPathSegment,
): boolean => {
  if (
    left[0] === "propertyWildcard" &&
    (right[0] === "property" || right[0] === "propertyWildcard")
  ) {
    return true;
  }
  if (right[0] === "propertyWildcard" && left[0] === "property") {
    return true;
  }
  if (left[0] === "indexWildcard" && (right[0] === "index" || right[0] === "indexWildcard")) {
    return right[0] === "index" ? right[1] >= left[1] : true;
  }
  if (right[0] === "indexWildcard" && left[0] === "index") {
    return left[1] >= right[1];
  }
  if (left[0] === "property" && right[0] === "property") {
    return left[1] === right[1];
  }
  if (left[0] === "index" && right[0] === "index") {
    return left[1] === right[1];
  }
  return false;
};

const numericRuntimePathsOverlap = (
  left: NumericRuntimeDomainPath,
  right: NumericRuntimeDomainPath,
): boolean => {
  if (left.length !== right.length) {
    return false;
  }
  return left.every((segment, index) => numericRuntimePathSegmentsOverlap(segment, right[index]!));
};

const collectNumericRuntimeDomainsByPath = (
  ast: SchemaAST.AST,
  path: NumericRuntimeDomainPath,
  entries: Array<NumericRuntimeDomainEntry>,
  active: Set<SchemaAST.AST>,
): void => {
  if (SchemaAST.isNumber(ast)) {
    addNumericRuntimeDomainAtPath(entries, path, "number");
    return;
  }
  if (SchemaAST.isBigInt(ast)) {
    addNumericRuntimeDomainAtPath(entries, path, "bigint");
    return;
  }
  if (isBigDecimalAst(ast)) {
    addNumericRuntimeDomainAtPath(entries, path, "bigDecimal");
    return;
  }
  if (SchemaAST.isLiteral(ast)) {
    if (typeof ast.literal === "number") {
      addNumericRuntimeDomainAtPath(entries, path, "number");
    }
    if (typeof ast.literal === "bigint") {
      addNumericRuntimeDomainAtPath(entries, path, "bigint");
    }
    return;
  }
  if (active.has(ast)) {
    return;
  }
  active.add(ast);
  if (SchemaAST.isSuspend(ast)) {
    collectNumericRuntimeDomainsByPath(ast.thunk(), path, entries, active);
    active.delete(ast);
    return;
  }
  if (ast.encoding !== undefined) {
    for (const link of ast.encoding) {
      collectNumericRuntimeDomainsByPath(link.to, path, entries, active);
    }
  }
  if (SchemaAST.isDeclaration(ast)) {
    for (const typeParameter of ast.typeParameters) {
      collectNumericRuntimeDomainsByPath(typeParameter, path, entries, active);
    }
  }
  if (SchemaAST.isObjects(ast)) {
    for (const property of ast.propertySignatures) {
      collectNumericRuntimeDomainsByPath(
        property.type,
        [...path, ["property", String(property.name)]],
        entries,
        active,
      );
    }
    for (const index of ast.indexSignatures) {
      collectNumericRuntimeDomainsByPath(
        index.type,
        [...path, ["propertyWildcard"]],
        entries,
        active,
      );
    }
  }
  if (SchemaAST.isArrays(ast)) {
    for (const [index, element] of ast.elements.entries()) {
      collectNumericRuntimeDomainsByPath(element, [...path, ["index", index]], entries, active);
    }
    for (const rest of ast.rest) {
      collectNumericRuntimeDomainsByPath(
        rest,
        [...path, ["indexWildcard", ast.elements.length]],
        entries,
        active,
      );
    }
  }
  if (SchemaAST.isUnion(ast)) {
    for (const member of ast.types) {
      collectNumericRuntimeDomainsByPath(member, path, entries, active);
    }
  }
  active.delete(ast);
};

const nestedMixedNumericRuntimeDomainAst = (ast: SchemaAST.AST): string | undefined => {
  const entries: Array<NumericRuntimeDomainEntry> = [];
  collectNumericRuntimeDomainsByPath(ast, [], entries, new Set());
  for (const left of entries) {
    const domains = new Set<NumericRuntimeDomain>([left.domain]);
    for (const right of entries) {
      if (numericRuntimePathsOverlap(left.path, right.path)) {
        domains.add(right.domain);
      }
    }
    if (domains.size > 1) {
      return `mixed numeric domain: ${Array.from(domains).toSorted().join(", ")}`;
    }
  }
  return undefined;
};

const isStringAst = (ast: SchemaAST.AST): boolean => {
  if (SchemaAST.isString(ast) || isStringLiteralAst(ast)) {
    return true;
  }
  return SchemaAST.isUnion(ast) && ast.types.length > 0 && ast.types.every(isStringAst);
};

const isStructuredAst = (ast: SchemaAST.AST): boolean => {
  if (SchemaAST.isObjects(ast) || SchemaAST.isArrays(ast) || SchemaAST.isObjectKeyword(ast)) {
    return true;
  }
  return SchemaAST.isUnion(ast) && ast.types.length > 0 && ast.types.every(isStructuredAst);
};

const isStructuredObjectAst = (ast: SchemaAST.AST): boolean => {
  if (SchemaAST.isObjects(ast) || SchemaAST.isObjectKeyword(ast)) {
    return true;
  }
  return SchemaAST.isUnion(ast) && ast.types.length > 0 && ast.types.every(isStructuredObjectAst);
};

export const viewServerSchemaFieldMetadata = (schema: unknown): ViewServerSchemaFieldMetadata => {
  const ast = schemaAst(schema);
  if (ast === undefined) {
    return {
      isNumeric: false,
      isPureBigInt: false,
      isString: false,
      isStructured: false,
      isStructuredObject: false,
    };
  }
  const numericKind = numericKindAst(ast);
  const isNumeric = numericKind !== "none";
  const isPureBigInt = SchemaAST.isBigInt(ast);
  return {
    isNumeric,
    isPureBigInt,
    isString: isStringAst(ast),
    isStructured: isStructuredAst(ast),
    isStructuredObject: isStructuredObjectAst(ast),
    ...(isNumeric ? { sumResultKind: numericKind } : {}),
  };
};

const unsupportedRuntimeDomainAst = (
  ast: SchemaAST.AST,
  seen: Set<SchemaAST.AST>,
): string | undefined => {
  if (seen.has(ast)) {
    return undefined;
  }
  seen.add(ast);
  const unsupportedDeclaration = unsupportedRuntimeDeclarationName(ast);
  if (unsupportedDeclaration !== undefined) {
    return unsupportedDeclaration;
  }
  if (SchemaAST.isSuspend(ast)) {
    return unsupportedRuntimeDomainAst(ast.thunk(), seen);
  }
  if (ast.encoding !== undefined) {
    for (const link of ast.encoding) {
      const unsupported = unsupportedRuntimeDomainAst(link.to, seen);
      if (unsupported !== undefined) {
        return unsupported;
      }
    }
  }
  if (SchemaAST.isDeclaration(ast)) {
    for (const typeParameter of ast.typeParameters) {
      const unsupported = unsupportedRuntimeDomainAst(typeParameter, seen);
      if (unsupported !== undefined) {
        return unsupported;
      }
    }
  }
  if (SchemaAST.isObjects(ast)) {
    for (const property of ast.propertySignatures) {
      const unsupported = unsupportedRuntimeDomainAst(property.type, seen);
      if (unsupported !== undefined) {
        return unsupported;
      }
    }
    for (const index of ast.indexSignatures) {
      const unsupportedParameter = unsupportedRuntimeDomainAst(index.parameter, seen);
      if (unsupportedParameter !== undefined) {
        return unsupportedParameter;
      }
      const unsupportedValue = unsupportedRuntimeDomainAst(index.type, seen);
      if (unsupportedValue !== undefined) {
        return unsupportedValue;
      }
    }
  }
  if (SchemaAST.isArrays(ast)) {
    for (const element of ast.elements) {
      const unsupported = unsupportedRuntimeDomainAst(element, seen);
      if (unsupported !== undefined) {
        return unsupported;
      }
    }
    for (const rest of ast.rest) {
      const unsupported = unsupportedRuntimeDomainAst(rest, seen);
      if (unsupported !== undefined) {
        return unsupported;
      }
    }
  }
  if (SchemaAST.isUnion(ast)) {
    for (const member of ast.types) {
      const unsupported = unsupportedRuntimeDomainAst(member, seen);
      if (unsupported !== undefined) {
        return unsupported;
      }
    }
  }
  return undefined;
};

export const viewServerUnsupportedRuntimeFieldDomain = (schema: unknown): string | undefined => {
  const ast = schemaAst(schema);
  if (ast === undefined) {
    return undefined;
  }
  return unsupportedRuntimeDomainAst(ast, new Set()) ?? nestedMixedNumericRuntimeDomainAst(ast);
};
