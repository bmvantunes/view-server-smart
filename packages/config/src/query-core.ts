import type { Schema } from "effect";
import type * as BigDecimal from "effect/BigDecimal";
import type { TopicSourceDefinition } from "./source-contract";

export type TopicName = string;
export type SortDirection = "asc" | "desc";

export type SchemaType<S> = Schema.Schema.Type<S>;
export type RowSchema = Schema.Codec<object, unknown, never, never> & {
  readonly fields: Readonly<
    Record<string, Schema.Codec<unknown, unknown, never, never> | undefined>
  >;
};
export type RowFromSchema<S extends RowSchema> = SchemaType<S>;

export type StringFieldKey<Row> = Extract<
  {
    readonly [Key in keyof Row]-?: Row[Key] extends string ? Key : never;
  }[keyof Row],
  string
>;

type NumericValue = number | bigint | BigDecimal.BigDecimal;
type IsNumericFieldValue<Value> = [Value] extends [never]
  ? false
  : undefined extends Value
    ? false
    : [Value] extends [NumericValue]
      ? true
      : false;

export type NumericFieldKey<Row> = Extract<
  {
    readonly [Key in keyof Row]-?: IsNumericFieldValue<Row[Key]> extends true ? Key : never;
  }[keyof Row],
  string
>;

export type FieldKey<Row> = Extract<keyof Row, string>;

export type TopicDefinition<
  S extends RowSchema,
  Key extends string,
  Source extends TopicSourceDefinition | undefined = undefined,
> = {
  readonly schema: S;
  readonly key: Key;
  readonly kafkaSource?: object | undefined;
  readonly grpcSource?: TopicSourceDefinition | undefined;
} & (Source extends TopicSourceDefinition
  ? { readonly source: Source }
  : { readonly source?: undefined });

export type TopicDefinitions = Record<
  string,
  TopicDefinition<RowSchema, string, TopicSourceDefinition | undefined>
>;

export type TopicSchema<Topics, Topic extends keyof Topics> = Topics[Topic] extends {
  readonly schema: infer S extends RowSchema;
}
  ? S
  : never;

export type TopicRow<Topics, Topic extends keyof Topics> = RowFromSchema<
  TopicSchema<Topics, Topic>
>;

export type Simplify<T> = { readonly [Key in keyof T]: T[Key] };
