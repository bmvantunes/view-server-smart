import type { GroupedQuery, GroupedResult } from "./grouped-query-contract";
import type { PickRawFields, RawQuery } from "./raw-query-contract";

export type LiveQuery<Row> = RawQuery<Row> | GroupedQuery<Row>;

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
    | "InvalidTopic"
    | "InvalidRow"
    | "InvalidQuery"
    | "UnsupportedQuery"
    | "RuntimeUnavailable"
    | "RuntimeResetFailed"
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
