import type {
  ViewServerBackpressureError,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@view-server/config";
import { Rpc, RpcGroup } from "effect/unstable/rpc";
import { Schema } from "effect";
import { ViewServerWireEventSchema } from "./protocol-event-codec";
import { ViewServerHealthSchema } from "./protocol-health-codec";
import { ViewServerSubscribePayloadSchema } from "./protocol-query-codec";

export const ViewServerBackpressureErrorSchema: Schema.Codec<ViewServerBackpressureError> =
  Schema.TaggedStruct("ViewServerBackpressureError", {
    code: Schema.Literal("BackpressureExceeded"),
    message: Schema.String,
    topic: Schema.optionalKey(Schema.String),
    queryId: Schema.optionalKey(Schema.String),
    queuedEvents: Schema.optionalKey(Schema.Number),
    maxQueueDepth: Schema.optionalKey(Schema.Number),
  });

export const ViewServerRuntimeErrorSchema: Schema.Codec<ViewServerRuntimeError> = Schema.Union([
  ViewServerBackpressureErrorSchema,
  Schema.TaggedStruct("ViewServerRuntimeError", {
    code: Schema.Literals([
      "InvalidTopic",
      "InvalidRow",
      "InvalidQuery",
      "UnsupportedQuery",
      "SnapshotStale",
      "RuntimeUnavailable",
      "RuntimeResetFailed",
    ]),
    message: Schema.String,
    topic: Schema.optionalKey(Schema.String),
  }),
]);

export const ViewServerTransportErrorSchema: Schema.Codec<ViewServerTransportError> = Schema.Union([
  ViewServerBackpressureErrorSchema,
  Schema.TaggedStruct("ViewServerTransportError", {
    code: Schema.Literals(["TransportError", "SubscriptionClosed"]),
    message: Schema.String,
    topic: Schema.optionalKey(Schema.String),
    queryId: Schema.optionalKey(Schema.String),
  }),
]);

export const ViewServerRpcErrorSchema = Schema.Union([
  ViewServerRuntimeErrorSchema,
  ViewServerTransportErrorSchema,
]);

export const ViewServerRpcs = RpcGroup.make(
  Rpc.make("ViewServer.Health", {
    success: ViewServerHealthSchema,
    error: ViewServerRpcErrorSchema,
  }),
  Rpc.make("ViewServer.Subscribe", {
    payload: ViewServerSubscribePayloadSchema,
    success: ViewServerWireEventSchema,
    error: ViewServerRpcErrorSchema,
    stream: true,
  }),
);

export type ViewServerRpcError = typeof ViewServerRpcErrorSchema.Type;

export { ViewServerHealthQuerySchema } from "./protocol-query-codec";
