import type { Schema } from "effect";
import type * as BigDecimal from "effect/BigDecimal";

export type TopicName = string;
export type SortDirection = "asc" | "desc";
export type AggregateKind = "count" | "countDistinct" | "sum" | "min" | "max" | "avg";

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

export type EqualityFilter<Value> =
  | Value
  | {
      readonly eq?: Value;
      readonly neq?: Value;
      readonly in?: ReadonlyArray<Value>;
    };

export type RangeFilter<Value> =
  | Value
  | {
      readonly eq?: Value;
      readonly neq?: Value;
      readonly in?: ReadonlyArray<Value>;
      readonly gt?: Value;
      readonly gte?: Value;
      readonly lt?: Value;
      readonly lte?: Value;
    };

export type StringFilter<Value extends string> =
  | Value
  | {
      readonly eq?: Value;
      readonly neq?: Value;
      readonly in?: ReadonlyArray<Value>;
      readonly startsWith?: string;
    };

export type FieldFilter<Value> = Value extends string
  ? StringFilter<Value>
  : Value extends number | bigint | BigDecimal.BigDecimal
    ? RangeFilter<Value>
    : EqualityFilter<Value>;

export type Where<Row> = {
  readonly [Field in FieldKey<Row>]?: FieldFilter<Row[Field]>;
};

export type OrderBy<Row> = {
  readonly field: FieldKey<Row>;
  readonly direction: SortDirection;
};

export type RawQuery<Row> = {
  readonly where?: Where<Row>;
  readonly orderBy?: ReadonlyArray<OrderBy<Row>>;
  readonly offset?: number;
  readonly limit?: number;
  readonly fields?: ReadonlyArray<FieldKey<Row>>;
};

export type CountAggregate<Alias extends string = string> = {
  readonly type: "count";
  readonly as: Alias;
};

export type CountDistinctAggregate<Row, Alias extends string = string> = {
  readonly type: "countDistinct";
  readonly field: FieldKey<Row>;
  readonly as: Alias;
};

export type SumAggregate<Row, Alias extends string = string> = {
  readonly type: "sum";
  readonly field: NumericFieldKey<Row>;
  readonly as: Alias;
};

export type AverageAggregate<Row, Alias extends string = string> = {
  readonly type: "avg";
  readonly field: NumericFieldKey<Row>;
  readonly as: Alias;
};

export type ComparableAggregate<Row, Alias extends string = string> = {
  readonly type: "min" | "max";
  readonly field: FieldKey<Row>;
  readonly as: Alias;
};

export type Aggregate<Row, Alias extends string = string> =
  | CountAggregate<Alias>
  | CountDistinctAggregate<Row, Alias>
  | SumAggregate<Row, Alias>
  | AverageAggregate<Row, Alias>
  | ComparableAggregate<Row, Alias>;

export type GroupedQuery<Row> = {
  readonly groupBy: ReadonlyArray<FieldKey<Row>>;
  readonly aggregates: ReadonlyArray<Aggregate<Row>>;
  readonly where?: Where<Row>;
  readonly offset?: number;
  readonly limit?: number;
};

export type LiveQuery<Row> = RawQuery<Row> | GroupedQuery<Row>;

type PickRawFields<Row, Query> = Query extends { readonly fields: ReadonlyArray<infer Field> }
  ? Pick<Row, Extract<Field, keyof Row>>
  : Row;

type AggregateResultValue<Row, Agg> = Agg extends { readonly type: "count" | "countDistinct" }
  ? bigint
  : Agg extends { readonly type: "sum"; readonly field: infer Field }
    ? Field extends keyof Row
      ? Row[Field] extends bigint
        ? bigint
        : BigDecimal.BigDecimal
      : never
    : Agg extends { readonly type: "avg" }
      ? BigDecimal.BigDecimal
      : Agg extends { readonly type: "min" | "max"; readonly field: infer Field }
        ? Field extends keyof Row
          ? Row[Field]
          : never
        : never;

type AggregateResultObject<Row, Agg> = Agg extends { readonly as: infer Alias extends string }
  ? {
      readonly [Key in Alias]: AggregateResultValue<Row, Agg>;
    }
  : object;

type UnionToIntersection<Union> = (Union extends unknown ? (value: Union) => void : never) extends (
  value: infer Intersection,
) => void
  ? Intersection
  : never;

type Simplify<T> = { readonly [Key in keyof T]: T[Key] };

type GroupedResult<Row, Query> = Query extends {
  readonly groupBy: ReadonlyArray<infer GroupField>;
  readonly aggregates: ReadonlyArray<infer Agg>;
}
  ? Simplify<
      Pick<Row, Extract<GroupField, keyof Row>> &
        UnionToIntersection<AggregateResultObject<Row, Agg>>
    >
  : never;

export type LiveQueryRow<Row, Query> =
  Query extends GroupedQuery<Row> ? GroupedResult<Row, Query> : PickRawFields<Row, Query>;

export type LiveQueryResult<Row> = {
  readonly rows: ReadonlyArray<Row>;
  readonly totalRows?: number;
  readonly version: number;
};

type RejectBroadAggregateAliases<Query> = Query extends {
  readonly aggregates: ReadonlyArray<infer Agg>;
}
  ? Agg extends { readonly as: infer Alias extends string }
    ? string extends Alias
      ? { readonly aggregates: never }
      : unknown
    : unknown
  : unknown;

type AggregateAliases<Query> = Query extends {
  readonly aggregates: ReadonlyArray<infer Agg>;
}
  ? Agg extends { readonly as: infer Alias extends string }
    ? Alias
    : never
  : never;

type GroupedFields<Query> = Query extends {
  readonly groupBy: ReadonlyArray<infer Field>;
}
  ? Extract<Field, string>
  : never;

type RejectAggregateAliasCollisions<Query> =
  Extract<AggregateAliases<Query>, GroupedFields<Query>> extends never
    ? unknown
    : { readonly aggregates: never };

export type ValidateLiveQuery<Query> = RejectBroadAggregateAliases<Query> &
  RejectAggregateAliasCollisions<Query>;

export type UseLiveQuery<Topics extends object> = <
  Topic extends Extract<keyof Topics, string>,
  const Query extends LiveQuery<TopicRow<Topics, Topic>>,
>(
  topic: Topic,
  query: Query & ValidateLiveQuery<Query>,
) => LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>;
