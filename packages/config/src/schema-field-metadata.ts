import { SchemaAST } from "effect";

export type ViewServerSchemaFieldMetadata = {
  readonly isNumeric: boolean;
  readonly isPureBigInt: boolean;
  readonly isString: boolean;
  readonly isStructured: boolean;
  readonly isStructuredObject: boolean;
  readonly sumResultKind?: "bigint" | "bigDecimal";
};

type NumericKind = "none" | "bigint" | "bigDecimal";

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
