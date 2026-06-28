import { viewServerSchemaFieldMetadata } from "@view-server/config";
import { Schema, SchemaAST } from "effect";
import { isRecord } from "./row-values";

type SchemaWithFields = Schema.Codec<object, unknown, never, unknown> & {
  readonly fields: Record<string, unknown>;
};

export type RangeValueKind = "number" | "bigint" | "bigDecimal";

export type RawQueryCompilerMetadata = {
  readonly fieldNames: ReadonlySet<string>;
  readonly fieldOrder: ReadonlyArray<string>;
  readonly fieldMetadata: ReadonlyMap<string, ReturnType<typeof viewServerSchemaFieldMetadata>>;
  readonly structuredFieldNames: ReadonlySet<string>;
  readonly structuredObjectFieldNames: ReadonlySet<string>;
  readonly stringFieldNames: ReadonlySet<string>;
  readonly numericFieldNames: ReadonlySet<string>;
  readonly numberFieldNames: ReadonlySet<string>;
  readonly bigintFieldNames: ReadonlySet<string>;
  readonly bigDecimalFieldNames: ReadonlySet<string>;
  readonly rangeValueKinds: ReadonlyMap<string, ReadonlySet<RangeValueKind>>;
};

const isSchemaWithFields = (
  schema: Schema.Codec<object, unknown, never, unknown>,
): schema is SchemaWithFields => "fields" in schema && isRecord(schema.fields);

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

const rangeValueKindsAst = (ast: SchemaAST.AST): ReadonlySet<RangeValueKind> => {
  if (SchemaAST.isNumber(ast)) {
    return new Set(["number"]);
  }
  if (SchemaAST.isBigInt(ast)) {
    return new Set(["bigint"]);
  }
  if (isBigDecimalAst(ast)) {
    return new Set(["bigDecimal"]);
  }
  if (SchemaAST.isLiteral(ast)) {
    if (typeof ast.literal === "number") {
      return new Set(["number"]);
    }
    if (typeof ast.literal === "bigint") {
      return new Set(["bigint"]);
    }
    return new Set();
  }
  if (!SchemaAST.isUnion(ast) || ast.types.length === 0) {
    return new Set();
  }
  const kinds = new Set<RangeValueKind>();
  for (const member of ast.types) {
    for (const kind of rangeValueKindsAst(member)) {
      kinds.add(kind);
    }
  }
  return kinds;
};

const isPureNumberAst = (ast: SchemaAST.AST): boolean => {
  if (SchemaAST.isNumber(ast)) {
    return true;
  }
  if (SchemaAST.isLiteral(ast)) {
    return typeof ast.literal === "number";
  }
  return SchemaAST.isUnion(ast) && ast.types.length > 0 && ast.types.every(isPureNumberAst);
};

const isPureBigDecimalAst = (ast: SchemaAST.AST): boolean => {
  if (isBigDecimalAst(ast)) {
    return true;
  }
  return SchemaAST.isUnion(ast) && ast.types.length > 0 && ast.types.every(isPureBigDecimalAst);
};

const schemaFieldNames = (
  schema: Schema.Codec<object, unknown, never, unknown>,
): ReadonlySet<string> =>
  isSchemaWithFields(schema) ? new Set(Object.keys(schema.fields)) : new Set();

const schemaFieldOrder = (
  schema: Schema.Codec<object, unknown, never, unknown>,
): ReadonlyArray<string> => (isSchemaWithFields(schema) ? Object.keys(schema.fields) : []);

const schemaFieldMetadata = (
  schema: Schema.Codec<object, unknown, never, unknown>,
): ReadonlyMap<string, ReturnType<typeof viewServerSchemaFieldMetadata>> => {
  if (!isSchemaWithFields(schema)) {
    return new Map();
  }

  const fields = new Map<string, ReturnType<typeof viewServerSchemaFieldMetadata>>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    fields.set(field, viewServerSchemaFieldMetadata(fieldSchema));
  }
  return fields;
};

const schemaNumericFieldNames = (
  schema: Schema.Codec<object, unknown, never, unknown>,
): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    if (!viewServerSchemaFieldMetadata(fieldSchema).isNumeric) {
      continue;
    }
    fields.add(field);
  }
  return fields;
};

const schemaNumberFieldNames = (
  schema: Schema.Codec<object, unknown, never, unknown>,
): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    const ast = schemaAst(fieldSchema);
    if (ast === undefined || !isPureNumberAst(ast)) {
      continue;
    }
    fields.add(field);
  }
  return fields;
};

const schemaBigintFieldNames = (
  schema: Schema.Codec<object, unknown, never, unknown>,
): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    if (viewServerSchemaFieldMetadata(fieldSchema).isPureBigInt) {
      fields.add(field);
    }
  }
  return fields;
};

const schemaBigDecimalFieldNames = (
  schema: Schema.Codec<object, unknown, never, unknown>,
): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    const ast = schemaAst(fieldSchema);
    if (ast === undefined || !isPureBigDecimalAst(ast)) {
      continue;
    }
    fields.add(field);
  }
  return fields;
};

const schemaRangeValueKinds = (
  schema: Schema.Codec<object, unknown, never, unknown>,
): ReadonlyMap<string, ReadonlySet<RangeValueKind>> => {
  if (!isSchemaWithFields(schema)) {
    return new Map();
  }

  const fields = new Map<string, ReadonlySet<RangeValueKind>>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    const ast = schemaAst(fieldSchema);
    if (ast === undefined) {
      continue;
    }
    const kinds = rangeValueKindsAst(ast);
    if (kinds.size > 0) {
      fields.set(field, kinds);
    }
  }
  return fields;
};

const schemaStringFieldNames = (
  schema: Schema.Codec<object, unknown, never, unknown>,
): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    if (viewServerSchemaFieldMetadata(fieldSchema).isString) {
      fields.add(field);
    }
  }
  return fields;
};

const schemaStructuredFieldNames = (
  schema: Schema.Codec<object, unknown, never, unknown>,
): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    if (viewServerSchemaFieldMetadata(fieldSchema).isStructured) {
      fields.add(field);
    }
  }
  return fields;
};

const schemaStructuredObjectFieldNames = (
  schema: Schema.Codec<object, unknown, never, unknown>,
): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    if (viewServerSchemaFieldMetadata(fieldSchema).isStructuredObject) {
      fields.add(field);
    }
  }
  return fields;
};

export const rawQueryCompilerMetadata = (
  schema: Schema.Codec<object, unknown, never, unknown>,
): RawQueryCompilerMetadata => ({
  fieldNames: schemaFieldNames(schema),
  fieldOrder: schemaFieldOrder(schema),
  fieldMetadata: schemaFieldMetadata(schema),
  structuredFieldNames: schemaStructuredFieldNames(schema),
  structuredObjectFieldNames: schemaStructuredObjectFieldNames(schema),
  stringFieldNames: schemaStringFieldNames(schema),
  numericFieldNames: schemaNumericFieldNames(schema),
  numberFieldNames: schemaNumberFieldNames(schema),
  bigintFieldNames: schemaBigintFieldNames(schema),
  bigDecimalFieldNames: schemaBigDecimalFieldNames(schema),
  rangeValueKinds: schemaRangeValueKinds(schema),
});
