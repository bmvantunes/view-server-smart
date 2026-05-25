import type * as BigDecimal from "effect/BigDecimal";
import type { FieldKey, NumericFieldKey } from "./query-core";

export type AggregateKind = "count" | "countDistinct" | "sum" | "min" | "max" | "avg";

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

export type AggregateAliasesFromAggregates<Aggregates> = Extract<keyof Aggregates, string>;

export type AggregateResultValue<Row, Agg> = Agg extends {
  readonly aggFunc: "count" | "countDistinct";
}
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
