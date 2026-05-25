import type { FieldKey } from "./query-core";
import type { RejectExtraKeys } from "./query-exact";
import type { ExactWhere, Where } from "./query-filter";
import type { ExactRawOrderBy, OrderBy } from "./query-sort";

export type RawSelect<Row> = ReadonlyArray<FieldKey<Row>>;

type RejectBroadSelect<Select> =
  Select extends ReadonlyArray<unknown>
    ? number extends Select["length"]
      ? never
      : unknown
    : never;

type RejectEmptySelect<Select> = Select extends readonly [] ? never : unknown;

type IsUnion<Value, Candidate = Value> = Value extends unknown
  ? [Candidate] extends [Value]
    ? false
    : true
  : false;

type ExactRawSelectField<Row, Field> = [Field] extends [FieldKey<Row>] ? Field : never;

type TupleIndexKeys<Select extends ReadonlyArray<unknown>> = Exclude<
  keyof Select,
  keyof ReadonlyArray<unknown>
>;

type SelectHasUnionField<Select extends ReadonlyArray<unknown>> = true extends {
  readonly [Index in TupleIndexKeys<Select>]: IsUnion<Select[Index]>;
}[TupleIndexKeys<Select>]
  ? true
  : false;

type ExactRawSelectFields<Row, Select> =
  Select extends ReadonlyArray<unknown>
    ? {
        readonly [Index in keyof Select]: ExactRawSelectField<Row, Select[Index]>;
      }
    : never;

type ExactRawSelect<Row, Query> = Query extends {
  readonly select: infer Select;
}
  ? {
      readonly select: Select &
        RejectBroadSelect<Select> &
        RejectEmptySelect<Select> &
        ExactRawSelectFields<Row, Select>;
    }
  : {
      readonly select: RawSelect<Row>;
    };

export type RawQuery<Row> = {
  readonly select: RawSelect<Row>;
  readonly where?: Where<Row>;
  readonly orderBy?: ReadonlyArray<OrderBy<Row>>;
  readonly offset?: number;
  readonly limit?: number;
};

export type ExactRawQuery<Row, Query> = Query &
  RejectExtraKeys<Query, RawQuery<Row>> & {
    readonly groupBy?: never;
    readonly aggregates?: never;
  } & ExactRawSelect<Row, Query> &
  ExactWhere<Row, Query> &
  ExactRawOrderBy<Row, Query>;

export type ExactPatch<Row, Patch> = Patch & RejectExtraKeys<Patch, Partial<Row>>;

export type PickRawFields<Row, Query> = Query extends {
  readonly select: infer Select extends ReadonlyArray<unknown>;
}
  ? SelectHasUnionField<Select> extends true
    ? Partial<Pick<Row, Extract<Select[number], keyof Row>>>
    : Pick<Row, Extract<Select[number], keyof Row>>
  : never;
