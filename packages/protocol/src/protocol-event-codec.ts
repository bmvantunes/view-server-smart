import type {
  DeltaEvent,
  SnapshotEvent,
  StatusEvent,
  TopicDefinitions,
  ViewServerRuntimeError,
} from "@view-server/config";
import { Effect, Schema } from "effect";
import { type ViewServerWireEvent } from "./protocol-event-schema";
import {
  decodeGroupedRow,
  decodeProjectedRow,
  decodeSystemRow,
  encodeGroupedRow,
  encodeProjectedRow,
  encodeSystemRow,
  isViewServerEventGroupedQuery,
  type ViewServerEventQuery,
} from "./protocol-row-codec";

export { ViewServerWireEventSchema, ViewServerWireRowSchema } from "./protocol-event-schema";
export type { ViewServerWireEvent, ViewServerWireRow } from "./protocol-event-schema";

export type ViewServerProtocolEvent<Row> = SnapshotEvent<Row> | DeltaEvent<Row> | StatusEvent;

const invalidRow = (topic: string, message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidRow",
  message,
  topic,
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
      if (isViewServerEventGroupedQuery(query)) {
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
      const row = yield* isViewServerEventGroupedQuery(query)
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
      if (isViewServerEventGroupedQuery(query)) {
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
      const row = yield* isViewServerEventGroupedQuery(query)
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
