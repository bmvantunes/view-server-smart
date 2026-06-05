import { viewServerSchemaFieldMetadata } from "@view-server/config";
import { Schema, SchemaAST } from "effect";
import { isRecord } from "./row-values";

type SchemaWithFields = Schema.Decoder<object> & {
  readonly fields: Record<string, unknown>;
};

export type RangeValueKind = "number" | "bigint" | "bigDecimal";

export type RawQueryCompilerMetadata = {
  readonly fieldNames: ReadonlySet<string>;
  readonly fieldMetadata: ReadonlyMap<string, ReturnType<typeof viewServerSchemaFieldMetadata>>;
  readonly structuredFieldNames: ReadonlySet<string>;
  readonly structuredObjectFieldNames: ReadonlySet<string>;
  readonly stringFieldNames: ReadonlySet<string>;
  readonly numericFieldNames: ReadonlySet<string>;
  readonly bigintFieldNames: ReadonlySet<string>;
  readonly rangeValueKinds: ReadonlyMap<string, ReadonlySet<RangeValueKind>>;
};

const isSchemaWithFields = (schema: Schema.Decoder<object>): schema is SchemaWithFields =>
  "fields" in schema && isRecord(schema.fields);

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

const schemaFieldNames = (schema: Schema.Decoder<object>): ReadonlySet<string> =>
  isSchemaWithFields(schema) ? new Set(Object.keys(schema.fields)) : new Set();

const schemaFieldMetadata = (
  schema: Schema.Decoder<object>,
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

const schemaNumericFieldNames = (schema: Schema.Decoder<object>): ReadonlySet<string> => {
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

const schemaBigintFieldNames = (schema: Schema.Decoder<object>): ReadonlySet<string> => {
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

const schemaRangeValueKinds = (
  schema: Schema.Decoder<object>,
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

const schemaStringFieldNames = (schema: Schema.Decoder<object>): ReadonlySet<string> => {
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

const schemaStructuredFieldNames = (schema: Schema.Decoder<object>): ReadonlySet<string> => {
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

const schemaStructuredObjectFieldNames = (schema: Schema.Decoder<object>): ReadonlySet<string> => {
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
  schema: Schema.Decoder<object>,
): RawQueryCompilerMetadata => ({
  fieldNames: schemaFieldNames(schema),
  fieldMetadata: schemaFieldMetadata(schema),
  structuredFieldNames: schemaStructuredFieldNames(schema),
  structuredObjectFieldNames: schemaStructuredObjectFieldNames(schema),
  stringFieldNames: schemaStringFieldNames(schema),
  numericFieldNames: schemaNumericFieldNames(schema),
  bigintFieldNames: schemaBigintFieldNames(schema),
  rangeValueKinds: schemaRangeValueKinds(schema),
});
