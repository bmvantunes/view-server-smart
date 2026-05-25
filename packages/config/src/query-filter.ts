import type * as BigDecimal from "effect/BigDecimal";
import type { FieldKey } from "./query-core";
import type { RejectExtraKeys } from "./query-exact";

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

type ExactFilter<Value, Filter> = Filter extends Value
  ? unknown
  : Filter extends FieldFilter<Value>
    ? ExactOperatorFilter<Value, Filter>
    : never;

export type ExactWhere<Row, Query> = Query extends {
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
