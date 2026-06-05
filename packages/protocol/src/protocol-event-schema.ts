import type { StatusEvent } from "@view-server/config";
import { Schema } from "effect";

export const ViewServerWireRowSchema: Schema.Codec<Schema.JsonObject> = Schema.Record(
  Schema.String,
  Schema.Json,
);

export type ViewServerWireRow = typeof ViewServerWireRowSchema.Type;

const SnapshotEventSchema = Schema.Struct({
  type: Schema.Literal("snapshot"),
  topic: Schema.String,
  queryId: Schema.String,
  version: Schema.Number,
  keys: Schema.Array(Schema.String),
  rows: Schema.Array(ViewServerWireRowSchema),
  totalRows: Schema.Number,
});

const DeltaOperationSchema = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("insert"),
    key: Schema.String,
    row: ViewServerWireRowSchema,
    index: Schema.Number,
  }),
  Schema.Struct({
    type: Schema.Literal("update"),
    key: Schema.String,
    row: ViewServerWireRowSchema,
    index: Schema.Number,
  }),
  Schema.Struct({
    type: Schema.Literal("move"),
    key: Schema.String,
    fromIndex: Schema.Number,
    toIndex: Schema.Number,
  }),
  Schema.Struct({
    type: Schema.Literal("remove"),
    key: Schema.String,
  }),
]);

const DeltaEventSchema = Schema.Struct({
  type: Schema.Literal("delta"),
  topic: Schema.String,
  queryId: Schema.String,
  fromVersion: Schema.Number,
  toVersion: Schema.Number,
  operations: Schema.Array(DeltaOperationSchema),
  totalRows: Schema.Number,
});

const StatusEventSchema: Schema.Codec<StatusEvent> = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("status"),
    topic: Schema.String,
    queryId: Schema.String,
    status: Schema.Literal("ready"),
    code: Schema.Literal("Ready"),
    message: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("status"),
    topic: Schema.String,
    queryId: Schema.String,
    status: Schema.Literal("stale"),
    code: Schema.Literals(["SnapshotStale", "BackpressureExceeded"]),
    message: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("status"),
    topic: Schema.String,
    queryId: Schema.String,
    status: Schema.Literal("closed"),
    code: Schema.Literals(["SubscriptionClosed", "BackpressureExceeded"]),
    message: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("status"),
    topic: Schema.String,
    queryId: Schema.String,
    status: Schema.Literal("error"),
    code: Schema.Literals([
      "TransportError",
      "BackpressureExceeded",
      "InvalidTopic",
      "InvalidRow",
      "InvalidQuery",
      "UnsupportedQuery",
      "RuntimeUnavailable",
      "RuntimeResetFailed",
    ]),
    message: Schema.optionalKey(Schema.String),
  }),
]);

export const ViewServerWireEventSchema = Schema.Union([
  SnapshotEventSchema,
  DeltaEventSchema,
  StatusEventSchema,
]);

export type ViewServerWireEvent = typeof ViewServerWireEventSchema.Type;
