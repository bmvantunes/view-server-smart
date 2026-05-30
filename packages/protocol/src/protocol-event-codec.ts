import type {
  DeltaEvent,
  SnapshotEvent,
  StatusEvent,
  TopicDefinitions,
  ViewServerRuntimeError,
} from "@view-server/config";
import { Effect, Schema } from "effect";

export type ViewServerProtocolEvent<Row> = SnapshotEvent<Row> | DeltaEvent<Row> | StatusEvent;

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

const invalidRow = (topic: string, message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidRow",
  message,
  topic,
});

const encodeEventJsonFieldValue = Effect.fn("ViewServerProtocol.event.field.encode")(function* (
  topic: string,
  field: string,
  schema: Schema.Codec<unknown>,
  value: unknown,
) {
  const encoded = yield* Schema.encodeUnknownEffect(Schema.toCodecJson(schema))(value).pipe(
    Effect.mapError((error) => invalidRow(topic, `Invalid field ${field}: ${error.message}`)),
  );
  return yield* Schema.decodeUnknownEffect(Schema.Json)(encoded).pipe(
    Effect.mapError((error) =>
      invalidRow(topic, `Field ${field} is not JSON-safe: ${error.message}`),
    ),
  );
});

const encodeProjectedRow = Effect.fn("ViewServerProtocol.row.project.encode")(function* <
  const Topics extends TopicDefinitions,
>(
  config: { readonly topics: Topics },
  topic: Extract<keyof Topics, string>,
  selectedFields: ReadonlySet<string>,
  row: object,
) {
  const topicSchema = config.topics[topic]!.schema;
  const output: Record<string, Schema.Json> = {};
  for (const field of selectedFields) {
    if (!Object.hasOwn(row, field)) {
      return yield* Effect.fail(
        invalidRow(topic, `Missing row field for topic ${topic}: ${field}`),
      );
    }
  }
  for (const [field, value] of Object.entries(row)) {
    if (!selectedFields.has(field)) {
      return yield* Effect.fail(
        invalidRow(topic, `Unexpected row field for topic ${topic}: ${field}`),
      );
    }
    const fieldSchema = topicSchema.fields[field]!;
    output[field] = yield* encodeEventJsonFieldValue(topic, field, fieldSchema, value);
  }
  return output;
});

const decodeProjectedRow = Effect.fn("ViewServerProtocol.row.project.decode")(function* <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  config: { readonly topics: Topics },
  topic: Topic,
  selectedFields: ReadonlySet<string>,
  row: ViewServerWireRow,
) {
  const output: Record<string, unknown> = {};
  for (const field of selectedFields) {
    if (!Object.hasOwn(row, field)) {
      return yield* Effect.fail(
        invalidRow(topic, `Missing row field for topic ${topic}: ${field}`),
      );
    }
  }
  for (const [field, value] of Object.entries(row)) {
    if (!selectedFields.has(field)) {
      return yield* Effect.fail(
        invalidRow(topic, `Unexpected row field for topic ${topic}: ${field}`),
      );
    }
    const fieldSchema = config.topics[topic]!.schema.fields[field]!;
    output[field] = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(fieldSchema))(value).pipe(
      Effect.mapError((error) => invalidRow(topic, `Invalid field ${field}: ${error.message}`)),
    );
  }
  return output;
});

const encodeStatusEvent = (event: StatusEvent): ViewServerWireEvent => event;

export const viewServerEncodeLiveEvent = Effect.fn("ViewServerProtocol.event.encode")(function* <
  const Topics extends TopicDefinitions,
>(
  config: { readonly topics: Topics },
  expectedTopic: Extract<keyof Topics, string>,
  selectedFields: ReadonlySet<string>,
  event: ViewServerProtocolEvent<object>,
) {
  if (event.topic !== expectedTopic) {
    return yield* Effect.fail(
      invalidRow(
        expectedTopic,
        `Received event for ${event.topic} while subscribed to ${expectedTopic}`,
      ),
    );
  }
  if (event.type === "status") {
    return encodeStatusEvent(event);
  }
  if (event.type === "snapshot") {
    const rows = yield* Effect.forEach(event.rows, (row) =>
      encodeProjectedRow(config, expectedTopic, selectedFields, row),
    );
    return {
      ...event,
      rows,
    };
  }
  type WireDeltaOperation = Extract<
    ViewServerWireEvent,
    { readonly type: "delta" }
  >["operations"][number];
  const operations: Array<WireDeltaOperation> = [];
  for (const operation of event.operations) {
    if (operation.type === "insert" || operation.type === "update") {
      const row = yield* encodeProjectedRow(config, expectedTopic, selectedFields, operation.row);
      operations.push({
        ...operation,
        row,
      });
    } else {
      operations.push(operation);
    }
  }
  return {
    ...event,
    operations,
  };
});

function typedLiveEvent<Row>(
  event: ViewServerProtocolEvent<Record<string, unknown>>,
): ViewServerProtocolEvent<Row>;
function typedLiveEvent(
  event: ViewServerProtocolEvent<Record<string, unknown>>,
): ViewServerProtocolEvent<Record<string, unknown>> {
  return event;
}

export const viewServerDecodeLiveEvent = Effect.fn("ViewServerProtocol.event.decode")(function* <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
  Row,
>(
  config: { readonly topics: Topics },
  expectedTopic: Topic,
  selectedFields: ReadonlySet<string>,
  event: ViewServerWireEvent,
) {
  if (event.topic !== expectedTopic) {
    return yield* Effect.fail(
      invalidRow(
        expectedTopic,
        `Received event for ${event.topic} while subscribed to ${expectedTopic}`,
      ),
    );
  }
  if (event.type === "status") {
    return typedLiveEvent<Row>(event);
  }
  if (event.type === "snapshot") {
    const rows = yield* Effect.forEach(event.rows, (row) =>
      decodeProjectedRow(config, expectedTopic, selectedFields, row),
    );
    return typedLiveEvent<Row>({
      ...event,
      rows,
    });
  }
  type DecodedDeltaOperation = Extract<
    ViewServerProtocolEvent<Record<string, unknown>>,
    { readonly type: "delta" }
  >["operations"][number];
  const operations: Array<DecodedDeltaOperation> = [];
  for (const operation of event.operations) {
    if (operation.type === "insert" || operation.type === "update") {
      const row = yield* decodeProjectedRow(config, expectedTopic, selectedFields, operation.row);
      operations.push({
        ...operation,
        row,
      });
    } else {
      operations.push(operation);
    }
  }
  return typedLiveEvent<Row>({
    ...event,
    operations,
  });
});

const encodeSystemRow = Effect.fn("ViewServerProtocol.system.row.encode")(function* <Row>(
  topic: string,
  schema: Schema.Codec<Row, unknown, never, never>,
  row: Row,
) {
  const encoded = yield* Schema.encodeUnknownEffect(Schema.toCodecJson(schema))(row).pipe(
    Effect.mapError((error) => invalidRow(topic, `Invalid system row: ${error.message}`)),
  );
  return yield* Schema.decodeUnknownEffect(ViewServerWireRowSchema)(encoded).pipe(Effect.orDie);
});

const decodeSystemRow = Effect.fn("ViewServerProtocol.system.row.decode")(function* <Row>(
  topic: string,
  schema: Schema.Codec<Row, unknown, never, never>,
  row: ViewServerWireRow,
) {
  return yield* Schema.decodeUnknownEffect(Schema.toCodecJson(schema))(row).pipe(
    Effect.mapError((error) => invalidRow(topic, `Invalid system row: ${error.message}`)),
  );
});

export const encodeSystemLiveEvent = Effect.fn("ViewServerProtocol.system.event.encode")(function* <
  Row,
>(
  expectedTopic: string,
  schema: Schema.Codec<Row, unknown, never, never>,
  event: ViewServerProtocolEvent<Row>,
) {
  if (event.topic !== expectedTopic) {
    return yield* Effect.fail(
      invalidRow(
        expectedTopic,
        `Received event for ${event.topic} while subscribed to ${expectedTopic}`,
      ),
    );
  }
  if (event.type === "status") {
    return encodeStatusEvent(event);
  }
  if (event.type === "snapshot") {
    const rows = yield* Effect.forEach(event.rows, (row) =>
      encodeSystemRow(expectedTopic, schema, row),
    );
    return {
      ...event,
      rows,
    };
  }
  type WireDeltaOperation = Extract<
    ViewServerWireEvent,
    { readonly type: "delta" }
  >["operations"][number];
  const operations: Array<WireDeltaOperation> = [];
  for (const operation of event.operations) {
    if (operation.type === "insert" || operation.type === "update") {
      const row = yield* encodeSystemRow(expectedTopic, schema, operation.row);
      operations.push({
        ...operation,
        row,
      });
    } else {
      operations.push(operation);
    }
  }
  return {
    ...event,
    operations,
  };
});

export const decodeSystemLiveEvent = Effect.fn("ViewServerProtocol.system.event.decode")(function* <
  Row,
>(
  expectedTopic: string,
  schema: Schema.Codec<Row, unknown, never, never>,
  event: ViewServerWireEvent,
) {
  if (event.topic !== expectedTopic) {
    return yield* Effect.fail(
      invalidRow(
        expectedTopic,
        `Received event for ${event.topic} while subscribed to ${expectedTopic}`,
      ),
    );
  }
  if (event.type === "status") {
    return event;
  }
  if (event.type === "snapshot") {
    const rows = yield* Effect.forEach(event.rows, (row) =>
      decodeSystemRow(expectedTopic, schema, row),
    );
    return {
      ...event,
      rows,
    };
  }
  type DecodedDeltaOperation = Extract<
    ViewServerProtocolEvent<Row>,
    { readonly type: "delta" }
  >["operations"][number];
  const operations: Array<DecodedDeltaOperation> = [];
  for (const operation of event.operations) {
    if (operation.type === "insert" || operation.type === "update") {
      const row = yield* decodeSystemRow(expectedTopic, schema, operation.row);
      operations.push({
        ...operation,
        row,
      });
    } else {
      operations.push(operation);
    }
  }
  return {
    ...event,
    operations,
  };
});
