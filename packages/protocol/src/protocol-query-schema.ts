import { Schema } from "effect";

export const ViewServerWireRawQuerySchema = Schema.Struct({
  select: Schema.Array(Schema.String),
  where: Schema.optionalKey(Schema.Record(Schema.String, Schema.Json)),
  orderBy: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        field: Schema.String,
        direction: Schema.Literals(["asc", "desc"]),
      }),
    ),
  ),
  offset: Schema.optionalKey(Schema.Number),
  limit: Schema.optionalKey(Schema.Number),
});

export type ViewServerWireRawQuery = typeof ViewServerWireRawQuerySchema.Type;

export const ViewServerWireAggregateSchema = Schema.Union([
  Schema.Struct({
    aggFunc: Schema.Literal("count"),
  }),
  Schema.Struct({
    aggFunc: Schema.Literals(["countDistinct", "sum", "avg", "min", "max"]),
    field: Schema.String,
  }),
]);

export const ViewServerWireGroupedQuerySchema = Schema.Struct({
  groupBy: Schema.Array(Schema.String),
  aggregates: Schema.Record(Schema.String, ViewServerWireAggregateSchema),
  where: Schema.optionalKey(Schema.Record(Schema.String, Schema.Json)),
  orderBy: Schema.optionalKey(
    Schema.Array(
      Schema.Union([
        Schema.Struct({
          field: Schema.String,
          direction: Schema.Literals(["asc", "desc"]),
        }),
        Schema.Struct({
          aggregate: Schema.String,
          direction: Schema.Literals(["asc", "desc"]),
        }),
      ]),
    ),
  ),
  offset: Schema.optionalKey(Schema.Number),
  limit: Schema.optionalKey(Schema.Number),
});

export type ViewServerWireGroupedQuery = typeof ViewServerWireGroupedQuerySchema.Type;
export type ViewServerWireLiveQuery = ViewServerWireRawQuery | ViewServerWireGroupedQuery;

export const ViewServerSubscribePayloadSchema = Schema.Struct({
  topic: Schema.String,
  // Keep this loose so excess query keys survive RPC decoding and can be rejected by strict query validation.
  query: Schema.Record(Schema.String, Schema.Unknown),
});

export const ViewServerHealthQuerySchema = Schema.Struct({
  select: Schema.Array(Schema.String),
});

export const LooseWireRawQuerySchema = Schema.Struct({
  select: Schema.Array(Schema.String),
  where: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
  orderBy: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        field: Schema.String,
        direction: Schema.Literals(["asc", "desc"]),
      }),
    ),
  ),
  offset: Schema.optionalKey(Schema.Number),
  limit: Schema.optionalKey(Schema.Number),
});

export type LooseWireRawQuery = typeof LooseWireRawQuerySchema.Type;

export const LooseWireGroupedQuerySchema = Schema.Struct({
  groupBy: Schema.Array(Schema.String),
  aggregates: Schema.Record(Schema.String, ViewServerWireAggregateSchema),
  where: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
  orderBy: Schema.optionalKey(
    Schema.Array(
      Schema.Union([
        Schema.Struct({
          field: Schema.String,
          direction: Schema.Literals(["asc", "desc"]),
        }),
        Schema.Struct({
          aggregate: Schema.String,
          direction: Schema.Literals(["asc", "desc"]),
        }),
      ]),
    ),
  ),
  offset: Schema.optionalKey(Schema.Number),
  limit: Schema.optionalKey(Schema.Number),
});

export type LooseWireGroupedQuery = typeof LooseWireGroupedQuerySchema.Type;
