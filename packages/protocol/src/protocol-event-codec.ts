import type {
  DeltaEvent,
  SnapshotEvent,
  StatusEvent,
  TopicDefinitions,
  ViewServerRuntimeError,
} from "@view-server/config";
import { viewServerSchemaFieldMetadata } from "@view-server/config";
import { Effect, Schema } from "effect";
import * as BigDecimal from "effect/BigDecimal";
import {
  type ViewServerWireEvent,
  ViewServerWireRowSchema,
  type ViewServerWireRow,
} from "./protocol-event-schema";

export { ViewServerWireEventSchema, ViewServerWireRowSchema } from "./protocol-event-schema";
export type { ViewServerWireEvent, ViewServerWireRow } from "./protocol-event-schema";

export type ViewServerProtocolEvent<Row> = SnapshotEvent<Row> | DeltaEvent<Row> | StatusEvent;

type ViewServerEventRawQuery = {
  readonly select: ReadonlyArray<string>;
};

type ViewServerEventGroupedAggregate =
  | {
      readonly aggFunc: "count";
    }
  | {
      readonly aggFunc: "countDistinct" | "sum" | "avg" | "min" | "max";
      readonly field: string;
    };

type ViewServerEventGroupedQuery = {
  readonly groupBy: ReadonlyArray<string>;
  readonly aggregates: Readonly<Record<string, ViewServerEventGroupedAggregate>>;
};

type ViewServerEventQuery = ViewServerEventRawQuery | ViewServerEventGroupedQuery;

const invalidRow = (topic: string, message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidRow",
  message,
  topic,
});

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isBigIntFieldSchema = (schema: Schema.Codec<unknown>): boolean =>
  viewServerSchemaFieldMetadata(schema).sumResultKind === "bigint";

const isGroupedQuery = (query: ViewServerEventQuery): query is ViewServerEventGroupedQuery =>
  "groupBy" in query;

const bigintPattern = /^-?\d+$/;

type AggregateEnvelope =
  | {
      readonly _viewServerAggregate: "bigint";
      readonly value: string;
    }
  | {
      readonly _viewServerAggregate: "bigdecimal";
      readonly value: string;
    }
  | {
      readonly _viewServerAggregate: "json";
      readonly value: Schema.Json;
    };

const isAggregateEnvelope = (value: unknown): value is AggregateEnvelope =>
  isRecord(value) &&
  (value["_viewServerAggregate"] === "bigint" ||
    value["_viewServerAggregate"] === "bigdecimal" ||
    value["_viewServerAggregate"] === "json");

const encodeJsonAggregateEnvelope = (value: Schema.Json): AggregateEnvelope => ({
  _viewServerAggregate: "json",
  value,
});

const encodeBigIntAggregateEnvelope = (value: bigint): AggregateEnvelope => ({
  _viewServerAggregate: "bigint",
  value: value.toString(),
});

const encodeBigDecimalAggregateEnvelope = (value: BigDecimal.BigDecimal): AggregateEnvelope => ({
  _viewServerAggregate: "bigdecimal",
  value: BigDecimal.format(value),
});

const decodeAggregateEnvelope = Effect.fn("ViewServerProtocol.row.aggregate.envelope.decode")(
  function* (topic: string, field: string, value: unknown) {
    if (!isAggregateEnvelope(value)) {
      return yield* Effect.fail(
        invalidRow(topic, `Aggregate ${field} must be a View Server aggregate envelope.`),
      );
    }
    return value;
  },
);

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

const encodeBigIntAggregateValue = Effect.fn("ViewServerProtocol.row.aggregate.bigint.encode")(
  function* (topic: string, field: string, value: unknown) {
    if (typeof value !== "bigint") {
      return yield* Effect.fail(invalidRow(topic, `Aggregate ${field} must be a bigint.`));
    }
    return encodeBigIntAggregateEnvelope(value);
  },
);

const decodeBigIntAggregateValue = Effect.fn("ViewServerProtocol.row.aggregate.bigint.decode")(
  function* (topic: string, field: string, value: unknown) {
    const envelope = yield* decodeAggregateEnvelope(topic, field, value);
    if (envelope._viewServerAggregate !== "bigint" || !bigintPattern.test(envelope.value)) {
      return yield* Effect.fail(invalidRow(topic, `Aggregate ${field} must be a bigint envelope.`));
    }
    return BigInt(envelope.value);
  },
);

const encodeBigDecimalAggregateValue = Effect.fn(
  "ViewServerProtocol.row.aggregate.bigDecimal.encode",
)(function* (topic: string, field: string, value: unknown) {
  if (!BigDecimal.isBigDecimal(value)) {
    return yield* Effect.fail(invalidRow(topic, `Aggregate ${field} must be a BigDecimal.`));
  }
  return encodeBigDecimalAggregateEnvelope(value);
});

const decodeBigDecimalAggregateValue = Effect.fn(
  "ViewServerProtocol.row.aggregate.bigDecimal.decode",
)(function* (topic: string, field: string, value: unknown) {
  const envelope = yield* decodeAggregateEnvelope(topic, field, value);
  if (envelope._viewServerAggregate !== "bigdecimal") {
    return yield* Effect.fail(
      invalidRow(topic, `Aggregate ${field} must be a BigDecimal envelope.`),
    );
  }
  return yield* Schema.decodeUnknownEffect(Schema.toCodecJson(Schema.BigDecimal))(
    envelope.value,
  ).pipe(
    Effect.mapError((error) => invalidRow(topic, `Invalid aggregate ${field}: ${error.message}`)),
  );
});

const encodeJsonAggregateValue = Effect.fn("ViewServerProtocol.row.aggregate.json.encode")(
  function* (topic: string, field: string, schema: Schema.Codec<unknown>, value: unknown) {
    const encoded = yield* encodeEventJsonFieldValue(topic, field, schema, value);
    return encodeJsonAggregateEnvelope(encoded);
  },
);

const decodeJsonAggregateValue = Effect.fn("ViewServerProtocol.row.aggregate.json.decode")(
  function* (topic: string, field: string, schema: Schema.Codec<unknown>, value: unknown) {
    const envelope = yield* decodeAggregateEnvelope(topic, field, value);
    if (envelope._viewServerAggregate !== "json") {
      return yield* Effect.fail(
        invalidRow(topic, `Aggregate ${field} must be a JSON aggregate envelope.`),
      );
    }
    return yield* Schema.decodeUnknownEffect(Schema.toCodecJson(schema))(envelope.value).pipe(
      Effect.mapError((error) => invalidRow(topic, `Invalid field ${field}: ${error.message}`)),
    );
  },
);

const aggregateFieldSchema = Effect.fn("ViewServerProtocol.row.aggregate.fieldSchema")(function* <
  const Topics extends TopicDefinitions,
>(config: { readonly topics: Topics }, topic: Extract<keyof Topics, string>, field: string) {
  const fieldSchema = config.topics[topic]!.schema.fields[field];
  if (fieldSchema === undefined) {
    return yield* Effect.fail(
      invalidRow(topic, `Aggregate references unknown field for topic ${topic}: ${field}`),
    );
  }
  return fieldSchema;
});

const encodeAggregateValue = Effect.fn("ViewServerProtocol.row.aggregate.encode")(function* <
  const Topics extends TopicDefinitions,
>(
  config: { readonly topics: Topics },
  topic: Extract<keyof Topics, string>,
  field: string,
  aggregate: ViewServerEventGroupedAggregate,
  value: unknown,
) {
  if (aggregate.aggFunc === "count" || aggregate.aggFunc === "countDistinct") {
    return yield* encodeBigIntAggregateValue(topic, field, value);
  }
  const fieldSchema = yield* aggregateFieldSchema(config, topic, aggregate.field);
  if (aggregate.aggFunc === "avg") {
    return yield* encodeBigDecimalAggregateValue(topic, field, value);
  }
  if (aggregate.aggFunc === "sum") {
    if (isBigIntFieldSchema(fieldSchema)) {
      return yield* encodeBigIntAggregateValue(topic, field, value);
    }
    return yield* encodeBigDecimalAggregateValue(topic, field, value);
  }
  return yield* encodeJsonAggregateValue(topic, field, fieldSchema, value);
});

const decodeAggregateValue = Effect.fn("ViewServerProtocol.row.aggregate.decode")(function* <
  const Topics extends TopicDefinitions,
>(
  config: { readonly topics: Topics },
  topic: Extract<keyof Topics, string>,
  field: string,
  aggregate: ViewServerEventGroupedAggregate,
  value: unknown,
) {
  if (aggregate.aggFunc === "count" || aggregate.aggFunc === "countDistinct") {
    return yield* decodeBigIntAggregateValue(topic, field, value);
  }
  const fieldSchema = yield* aggregateFieldSchema(config, topic, aggregate.field);
  if (aggregate.aggFunc === "avg") {
    return yield* decodeBigDecimalAggregateValue(topic, field, value);
  }
  if (aggregate.aggFunc === "sum") {
    if (isBigIntFieldSchema(fieldSchema)) {
      return yield* decodeBigIntAggregateValue(topic, field, value);
    }
    return yield* decodeBigDecimalAggregateValue(topic, field, value);
  }
  return yield* decodeJsonAggregateValue(topic, field, fieldSchema, value);
});

const encodeGroupedRow = Effect.fn("ViewServerProtocol.row.grouped.encode")(function* <
  const Topics extends TopicDefinitions,
>(
  config: { readonly topics: Topics },
  topic: Extract<keyof Topics, string>,
  query: ViewServerEventGroupedQuery,
  row: object,
) {
  const topicSchema = config.topics[topic]!.schema;
  const groupFields = new Set<string>(query.groupBy);
  const aggregateAliases = new Set<string>(Object.keys(query.aggregates));
  const output: Record<string, Schema.Json> = {};
  for (const field of groupFields) {
    if (!Object.hasOwn(row, field)) {
      return yield* Effect.fail(
        invalidRow(topic, `Missing grouped row field for topic ${topic}: ${field}`),
      );
    }
  }
  for (const alias of aggregateAliases) {
    if (!Object.hasOwn(row, alias)) {
      return yield* Effect.fail(
        invalidRow(topic, `Missing grouped aggregate for topic ${topic}: ${alias}`),
      );
    }
  }
  for (const [field, value] of Object.entries(row)) {
    if (groupFields.has(field)) {
      const fieldSchema = topicSchema.fields[field]!;
      output[field] = yield* encodeEventJsonFieldValue(topic, field, fieldSchema, value);
    } else if (aggregateAliases.has(field)) {
      const aggregate = query.aggregates[field];
      if (aggregate === undefined) {
        return yield* Effect.fail(
          invalidRow(topic, `Missing grouped aggregate definition for topic ${topic}: ${field}`),
        );
      }
      output[field] = yield* encodeAggregateValue(config, topic, field, aggregate, value);
    } else {
      return yield* Effect.fail(
        invalidRow(topic, `Unexpected grouped row field for topic ${topic}: ${field}`),
      );
    }
  }
  return output;
});

const decodeGroupedRow = Effect.fn("ViewServerProtocol.row.grouped.decode")(function* <
  const Topics extends TopicDefinitions,
  Topic extends Extract<keyof Topics, string>,
>(
  config: { readonly topics: Topics },
  topic: Topic,
  query: ViewServerEventGroupedQuery,
  row: ViewServerWireRow,
) {
  const topicSchema = config.topics[topic]!.schema;
  const groupFields = new Set<string>(query.groupBy);
  const aggregateAliases = new Set<string>(Object.keys(query.aggregates));
  const output: Record<string, unknown> = {};
  for (const field of groupFields) {
    if (!Object.hasOwn(row, field)) {
      return yield* Effect.fail(
        invalidRow(topic, `Missing grouped row field for topic ${topic}: ${field}`),
      );
    }
  }
  for (const alias of aggregateAliases) {
    if (!Object.hasOwn(row, alias)) {
      return yield* Effect.fail(
        invalidRow(topic, `Missing grouped aggregate for topic ${topic}: ${alias}`),
      );
    }
  }
  for (const [field, value] of Object.entries(row)) {
    if (groupFields.has(field)) {
      const fieldSchema = topicSchema.fields[field]!;
      output[field] = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(fieldSchema))(
        value,
      ).pipe(
        Effect.mapError((error) => invalidRow(topic, `Invalid field ${field}: ${error.message}`)),
      );
    } else if (aggregateAliases.has(field)) {
      const aggregate = query.aggregates[field];
      if (aggregate === undefined) {
        return yield* Effect.fail(
          invalidRow(topic, `Missing grouped aggregate definition for topic ${topic}: ${field}`),
        );
      }
      output[field] = yield* decodeAggregateValue(config, topic, field, aggregate, value);
    } else {
      return yield* Effect.fail(
        invalidRow(topic, `Unexpected grouped row field for topic ${topic}: ${field}`),
      );
    }
  }
  return output;
});

const encodeStatusEvent = (event: StatusEvent): ViewServerWireEvent => event;

export const viewServerEncodeLiveEvent = Effect.fn("ViewServerProtocol.event.encode")(function* <
  const Topics extends TopicDefinitions,
>(
  config: { readonly topics: Topics },
  expectedTopic: Extract<keyof Topics, string>,
  query: ViewServerEventQuery,
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
    const rows = yield* Effect.forEach(event.rows, (row) => {
      if (isGroupedQuery(query)) {
        return encodeGroupedRow(config, expectedTopic, query, row);
      }
      return encodeProjectedRow(config, expectedTopic, new Set<string>(query.select), row);
    });
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
      const row = yield* isGroupedQuery(query)
        ? encodeGroupedRow(config, expectedTopic, query, operation.row)
        : encodeProjectedRow(config, expectedTopic, new Set<string>(query.select), operation.row);
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
  query: ViewServerEventQuery,
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
    const rows = yield* Effect.forEach(event.rows, (row) => {
      if (isGroupedQuery(query)) {
        return decodeGroupedRow(config, expectedTopic, query, row);
      }
      return decodeProjectedRow(config, expectedTopic, new Set<string>(query.select), row);
    });
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
      const row = yield* isGroupedQuery(query)
        ? decodeGroupedRow(config, expectedTopic, query, operation.row)
        : decodeProjectedRow(config, expectedTopic, new Set<string>(query.select), operation.row);
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
