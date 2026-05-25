import type {
  Aggregate,
  AggregateAliasesFromAggregates,
  AggregateResultValue,
  Aggregates,
} from "./query-aggregate";
import type { FieldKey, Simplify } from "./query-core";
import type { RejectExtraKeys } from "./query-exact";
import type { ExactWhere, Where } from "./query-filter";
import type { ExactGroupedOrderByEntry, GroupedOrderBy } from "./query-sort";

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

type GroupedOrderByField<Row, GroupBy> = Extract<
  GroupBy extends ReadonlyArray<infer Field> ? Field : never,
  FieldKey<Row>
>;

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

export type GroupedResult<Row, Query> = Query extends {
  readonly groupBy: ReadonlyArray<infer GroupField>;
  readonly aggregates: infer Aggs;
}
  ? Simplify<
      Pick<Row, Extract<GroupField, keyof Row>> & {
        readonly [Alias in keyof Aggs]: AggregateResultValue<Row, Aggs[Alias]>;
      }
    >
  : never;
