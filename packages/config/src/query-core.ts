import type { Schema } from "effect";
import type * as BigDecimal from "effect/BigDecimal";

export type TopicName = string;
export type SortDirection = "asc" | "desc";

export type SchemaType<S> = Schema.Schema.Type<S>;
export type RowSchema = Schema.Schema<object>;
export type RowFromSchema<S extends RowSchema> = SchemaType<S>;

export type StringFieldKey<Row> = Extract<
  {
    readonly [Key in keyof Row]-?: Row[Key] extends string ? Key : never;
  }[keyof Row],
  string
>;

export type NumericFieldKey<Row> = Extract<
  {
    readonly [Key in keyof Row]-?: Row[Key] extends number | bigint | BigDecimal.BigDecimal
      ? Key
      : never;
  }[keyof Row],
  string
>;

export type FieldKey<Row> = Extract<keyof Row, string>;

export type TopicDefinition<S extends RowSchema, Key extends string> = {
  readonly schema: S;
  readonly key: Key;
};

export type TopicDefinitions = Record<string, TopicDefinition<RowSchema, string>>;

export type TopicSchema<Topics, Topic extends keyof Topics> = Topics[Topic] extends {
  readonly schema: infer S extends RowSchema;
}
  ? S
  : never;

export type TopicRow<Topics, Topic extends keyof Topics> = RowFromSchema<
  TopicSchema<Topics, Topic>
>;

export type Simplify<T> = { readonly [Key in keyof T]: T[Key] };
