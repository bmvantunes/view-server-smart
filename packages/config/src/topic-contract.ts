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

export type OrderByField<Field extends string> = {
  readonly field: Field;
  readonly aggregate?: never;
  readonly direction: SortDirection;
};

export type OrderBy<Row> = OrderByField<FieldKey<Row>>;

export type AggregateOrderByField<Alias extends string = string> = {
  readonly aggregate: Alias;
  readonly field?: never;
  readonly direction: SortDirection;
};

export type GroupedOrderBy<Row> = OrderByField<FieldKey<Row>> | AggregateOrderByField;

export type RawQuery<Row> = {
  readonly select: readonly [FieldKey<Row>, ...Array<FieldKey<Row>>];
  readonly where?: Where<Row>;
  readonly orderBy?: ReadonlyArray<OrderBy<Row>>;
  readonly offset?: number;
  readonly limit?: number;
};

type RejectExtraKeys<Candidate, Shape> = {
  readonly [Key in Exclude<keyof Candidate, keyof Shape>]: never;
};

type FieldFilterShape<Value> = Value extends string
  ? {
      readonly eq?: Value;
      readonly neq?: Value;
      readonly in?: ReadonlyArray<Value>;
      readonly startsWith?: string;
    }
  : {
      readonly eq?: Value;
      readonly neq?: Value;
      readonly in?: ReadonlyArray<Value>;
      readonly gt?: Value;
      readonly gte?: Value;
      readonly lt?: Value;
      readonly lte?: Value;
    };

type ExactOperatorFilter<Value, Filter> = Filter extends object
  ? Filter extends ReadonlyArray<unknown>
    ? unknown
    : Filter & RejectExtraKeys<Filter, FieldFilterShape<Value>>
  : unknown;

type ExactFilter<Value, Filter> = Value extends object
  ? unknown
  : ExactOperatorFilter<Value, Filter>;

type ExactWhere<Row, Query> = Query extends {
  readonly where: infer QueryWhere;
}
  ? {
      readonly where: QueryWhere &
        RejectExtraKeys<QueryWhere, { readonly [Field in FieldKey<Row>]?: unknown }> & {
          readonly [Field in Extract<keyof QueryWhere, FieldKey<Row>>]: ExactFilter<
            Row[Field],
            QueryWhere[Field]
          >;
        };
    }
  : unknown;

type ExactOrderByEntry<Entry, Field extends string> = Entry &
  RejectExtraKeys<Entry, OrderByField<Field>> &
  (Entry extends { readonly field: infer QueryField }
    ? { readonly field: QueryField & Field }
    : { readonly field: Field }) &
  (Entry extends { readonly aggregate: unknown } ? { readonly aggregate: never } : unknown) &
  (Entry extends { readonly direction: infer QueryDirection }
    ? { readonly direction: QueryDirection & SortDirection }
    : { readonly direction: SortDirection });

type ExactOrderBy<Row, Query> = Query extends {
  readonly orderBy: ReadonlyArray<infer Entry>;
}
  ? {
      readonly orderBy: ReadonlyArray<ExactOrderByEntry<Entry, FieldKey<Row>>>;
    }
  : unknown;

export type ExactRawQuery<Row, Query> = Query &
  RejectExtraKeys<Query, RawQuery<Row>> & {
    readonly groupBy?: never;
    readonly aggregates?: never;
  } & ExactWhere<Row, Query> &
  ExactOrderBy<Row, Query>;

export type ExactPatch<Row, Patch> = Patch & RejectExtraKeys<Patch, Partial<Row>>;

export type CountAggregate = {
  readonly aggFunc: "count";
};

export type CountDistinctAggregate<Row> = {
  readonly aggFunc: "countDistinct";
  readonly field: FieldKey<Row>;
};

export type SumAggregate<Row> = {
  readonly aggFunc: "sum";
  readonly field: NumericFieldKey<Row>;
};

export type AverageAggregate<Row> = {
  readonly aggFunc: "avg";
  readonly field: NumericFieldKey<Row>;
};

export type ComparableAggregate<Row> = {
  readonly aggFunc: "min" | "max";
  readonly field: FieldKey<Row>;
};

export type Aggregate<Row> =
  | CountAggregate
  | CountDistinctAggregate<Row>
  | SumAggregate<Row>
  | AverageAggregate<Row>
  | ComparableAggregate<Row>;

export type Aggregates<Row> = Readonly<Record<string, Aggregate<Row>>>;

export type GroupedQuery<Row> = {
  readonly groupBy: readonly [FieldKey<Row>, ...Array<FieldKey<Row>>];
  readonly aggregates: Aggregates<Row>;
  readonly select?: never;
  readonly where?: Where<Row>;
  readonly orderBy?: ReadonlyArray<GroupedOrderBy<Row>>;
  readonly offset?: number;
  readonly limit?: number;
};

type NonEmptyFieldTuple<Row, Tuple> = Tuple extends readonly [unknown, ...Array<unknown>]
  ? { readonly [Index in keyof Tuple]: Tuple[Index] & FieldKey<Row> }
  : never;

type ExactAggregates<Row, Candidate> = {
  readonly [Alias in keyof Candidate]: Candidate[Alias] & Aggregate<Row>;
};

type AggregateAliasesFromAggregates<Aggregates> = Extract<keyof Aggregates, string>;

type GroupedOrderByField<Row, GroupBy> = Extract<
  GroupBy extends ReadonlyArray<infer Field> ? Field : never,
  FieldKey<Row>
>;

type ExactGroupedOrderByEntry<Entry, Field extends string, Alias extends string> =
  | (Entry &
      RejectExtraKeys<Entry, OrderByField<Field>> &
      (Entry extends { readonly field: infer QueryField }
        ? { readonly field: QueryField & Field }
        : { readonly field: Field }) &
      (Entry extends { readonly aggregate: unknown } ? { readonly aggregate: never } : unknown) &
      (Entry extends { readonly direction: infer QueryDirection }
        ? { readonly direction: QueryDirection & SortDirection }
        : { readonly direction: SortDirection }))
  | (Entry &
      RejectExtraKeys<Entry, AggregateOrderByField<Alias>> &
      (Entry extends { readonly aggregate: infer QueryAggregate }
        ? { readonly aggregate: QueryAggregate & Alias }
        : { readonly aggregate: Alias }) &
      (Entry extends { readonly field: unknown } ? { readonly field: never } : unknown) &
      (Entry extends { readonly direction: infer QueryDirection }
        ? { readonly direction: QueryDirection & SortDirection }
        : { readonly direction: SortDirection }));

type ExactGroupedOrderBy<Row, Query> = Query extends {
  readonly orderBy: ReadonlyArray<infer Entry>;
  readonly groupBy: infer GroupBy;
  readonly aggregates: infer Aggregates;
}
  ? {
      readonly orderBy: ReadonlyArray<
        ExactGroupedOrderByEntry<
          Entry,
          GroupedOrderByField<Row, GroupBy>,
          AggregateAliasesFromAggregates<Aggregates>
        >
      >;
    }
  : unknown;

export type ExactGroupedQuery<Row, Query> = Query &
  RejectExtraKeys<Query, GroupedQuery<Row>> & {
    readonly select?: never;
  } & ExactWhere<Row, Query> &
  ExactGroupedOrderBy<Row, Query> &
  (Query extends {
    readonly groupBy: infer GroupBy;
    readonly aggregates: infer Aggregates;
  }
    ? {
        readonly groupBy: NonEmptyFieldTuple<Row, GroupBy>;
        readonly aggregates: ExactAggregates<Row, Aggregates>;
      }
    : {
        readonly groupBy: readonly [FieldKey<Row>, ...Array<FieldKey<Row>>];
        readonly aggregates: Aggregates<Row>;
      });

export type LiveQuery<Row> = RawQuery<Row> | GroupedQuery<Row>;

type PickRawFields<Row, Query> = Query extends { readonly select: ReadonlyArray<infer Field> }
  ? Pick<Row, Extract<Field, keyof Row>>
  : never;

type AggregateResultValue<Row, Agg> = Agg extends { readonly aggFunc: "count" | "countDistinct" }
  ? bigint
  : Agg extends { readonly aggFunc: "sum"; readonly field: infer Field }
    ? Field extends keyof Row
      ? Row[Field] extends bigint
        ? bigint
        : BigDecimal.BigDecimal
      : never
    : Agg extends { readonly aggFunc: "avg" }
      ? BigDecimal.BigDecimal
      : Agg extends { readonly aggFunc: "min" | "max"; readonly field: infer Field }
        ? Field extends keyof Row
          ? Row[Field]
          : never
        : never;

type Simplify<T> = { readonly [Key in keyof T]: T[Key] };

type GroupedResult<Row, Query> = Query extends {
  readonly groupBy: ReadonlyArray<infer GroupField>;
  readonly aggregates: infer Aggs;
}
  ? Simplify<
      Pick<Row, Extract<GroupField, keyof Row>> & {
        readonly [Alias in keyof Aggs]: AggregateResultValue<Row, Aggs[Alias]>;
      }
    >
  : never;

export type LiveQueryRow<Row, Query> =
  Query extends GroupedQuery<Row> ? GroupedResult<Row, Query> : PickRawFields<Row, Query>;

export type LiveQueryResult<Row> = {
  readonly rows: ReadonlyArray<Row>;
  readonly totalRows: number;
  readonly version: number;
  readonly status: "loading" | "ready" | "stale" | "closed" | "error";
  readonly statusCode?:
    | "Ready"
    | "SnapshotStale"
    | "SubscriptionClosed"
    | "TransportError"
    | "BackpressureExceeded"
    | undefined;
  readonly message?: string | undefined;
};

type AggregateAliases<Query> = Query extends {
  readonly aggregates: infer Aggs;
}
  ? Extract<keyof Aggs, string>
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

type RejectBroadAggregateAliases<Query> = Query extends {
  readonly aggregates: infer Aggs;
}
  ? string extends keyof Aggs
    ? { readonly aggregates: never }
    : unknown
  : unknown;

export type ValidateLiveQuery<Query> = RejectAggregateAliasCollisions<Query> &
  RejectBroadAggregateAliases<Query>;

export type UseLiveQuery<Topics extends object> = {
  <Topic extends Extract<keyof Topics, string>, const Query extends object>(
    topic: Topic,
    query: ExactGroupedQuery<TopicRow<Topics, Topic>, Query> & ValidateLiveQuery<Query>,
  ): LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>;
  <
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactRawQuery<TopicRow<Topics, Topic>, Query> & ValidateLiveQuery<Query>,
  ): LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>;
};
