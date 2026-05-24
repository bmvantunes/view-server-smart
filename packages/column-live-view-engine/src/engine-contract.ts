import type {
  DeltaEvent,
  FieldKey,
  LiveQueryRow,
  LiveQueryResult,
  OrderBy,
  RawQuery,
  RowFromSchema,
  RowSchema,
  SnapshotEvent,
  StatusEvent,
  StringFieldKey,
  TopicRow,
} from "@view-server/config";
import type { Effect, Schema, Stream } from "effect";
import type { ColumnLiveViewEngineHealth } from "./engine-health";
import type { ColumnLiveViewEngineError } from "./engine-errors";

export type DecodableTopicDefinitions = Record<
  string,
  {
    readonly schema: RowSchema & Schema.Decoder<object>;
    readonly key: string;
  }
>;

type ValidateEngineTopics<Topics extends DecodableTopicDefinitions> = {
  readonly [Topic in keyof Topics]: Topics[Topic] extends {
    readonly schema: infer S extends RowSchema & Schema.Decoder<object>;
    readonly key: infer Key extends string;
  }
    ? {
        readonly schema: S;
        readonly key: Key & StringFieldKey<RowFromSchema<S>>;
      }
    : never;
};

export type ColumnLiveViewEngineConfig<Topics extends DecodableTopicDefinitions> = {
  readonly topics: Topics & ValidateEngineTopics<Topics>;
  readonly subscriptionQueueCapacity?: number;
};

export type ColumnLiveViewEngineEvent<Row> = SnapshotEvent<Row> | DeltaEvent<Row> | StatusEvent;

export type ColumnLiveViewSubscription<Row> = {
  readonly events: Stream.Stream<ColumnLiveViewEngineEvent<Row>>;
  readonly close: () => Effect.Effect<void, never>;
};

export type AnyTopicRow<Topics extends DecodableTopicDefinitions> = TopicRow<
  Topics,
  Extract<keyof Topics, string>
>;

type RejectExtraKeys<Candidate, Shape> = {
  readonly [Key in Exclude<keyof Candidate, keyof Shape>]: never;
};

type IsUnion<Value, Candidate = Value> = Value extends unknown
  ? [Candidate] extends [Value]
    ? false
    : true
  : false;

type TupleHasUnionElement<Tuple extends ReadonlyArray<unknown>> = Tuple extends readonly [
  infer Head,
  ...infer Tail,
]
  ? IsUnion<Head> extends true
    ? true
    : TupleHasUnionElement<Tail>
  : false;

type ExactRawQuery<Row, Query> = Query &
  RejectExtraKeys<Query, RawQuery<Row>> & {
    readonly groupBy?: never;
    readonly aggregates?: never;
  } & ExactWhere<Row, Query> &
  ExactOrderBy<Row, Query> &
  RejectDynamicRawFields<Row, Query>;

type RejectDynamicRawFields<Row, Query> = "fields" extends keyof Query
  ? Query extends { readonly fields?: infer Fields }
    ? NonNullable<Fields> extends ReadonlyArray<unknown>
      ? undefined extends Query["fields"]
        ? {
            readonly fields: never;
          }
        : IsUnion<NonNullable<Fields>> extends true
          ? {
              readonly fields: never;
            }
          : number extends NonNullable<Fields>["length"]
            ? {
                readonly fields: never;
              }
            : TupleHasUnionElement<NonNullable<Fields>> extends true
              ? {
                  readonly fields: never;
                }
              : NonNullable<Fields>[number] extends FieldKey<Row>
                ? unknown
                : {
                    readonly fields: never;
                  }
      : unknown
    : unknown
  : unknown;

type ExactWhere<Row, Query> = Query extends {
  readonly where: infer Where;
}
  ? {
      readonly where: Where &
        RejectExtraKeys<Where, { readonly [Field in FieldKey<Row>]?: unknown }> & {
          readonly [Field in Extract<keyof Where, FieldKey<Row>>]: ExactFilter<
            Row[Field],
            Where[Field]
          >;
        };
    }
  : unknown;

type ExactFilter<Value, Filter> = Value extends object
  ? unknown
  : ExactOperatorFilter<Value, Filter>;

type ExactOperatorFilter<Value, Filter> = Filter extends object
  ? Filter extends ReadonlyArray<unknown>
    ? unknown
    : Filter & RejectExtraKeys<Filter, FieldFilterShape<Value>>
  : unknown;

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

type ExactOrderBy<Row, Query> = Query extends {
  readonly orderBy: ReadonlyArray<infer Entry>;
}
  ? {
      readonly orderBy: ReadonlyArray<Entry & RejectExtraKeys<Entry, OrderBy<Row>>>;
    }
  : unknown;

type ExactPatch<Row, Patch> = Patch & RejectExtraKeys<Patch, Partial<Row>>;

export type ColumnLiveViewEngine<Topics extends DecodableTopicDefinitions> = {
  readonly publish: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    row: TopicRow<Topics, Topic>,
  ) => Effect.Effect<void, ColumnLiveViewEngineError>;
  readonly publishMany: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    rows: ReadonlyArray<TopicRow<Topics, Topic>>,
  ) => Effect.Effect<void, ColumnLiveViewEngineError>;
  readonly patch: <
    Topic extends Extract<keyof Topics, string>,
    const Patch extends Partial<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    key: string,
    patch: ExactPatch<TopicRow<Topics, Topic>, Patch>,
  ) => Effect.Effect<void, ColumnLiveViewEngineError>;
  readonly delete: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    key: string,
  ) => Effect.Effect<void, ColumnLiveViewEngineError>;
  readonly snapshot: <
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactRawQuery<TopicRow<Topics, Topic>, Query>,
  ) => Effect.Effect<
    LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  readonly subscribe: <
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactRawQuery<TopicRow<Topics, Topic>, Query>,
  ) => Effect.Effect<
    ColumnLiveViewSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  readonly health: () => Effect.Effect<ColumnLiveViewEngineHealth<Topics>, never>;
  readonly reset: () => Effect.Effect<void, never>;
  readonly close: () => Effect.Effect<void, never>;
};
