import type {
  TopicDefinitions,
  ViewServerConfig,
  ViewServerHealthSummaryRow,
  ViewServerRuntimeError,
} from "@view-server/config";
import { VIEW_SERVER_HEALTH_SUMMARY_TOPIC } from "@view-server/config";
import { Effect, Schema } from "effect";
import {
  decodeSystemLiveEvent,
  encodeSystemLiveEvent,
  type ViewServerWireEvent,
} from "./protocol-event-codec";
import { ViewServerWireEventSchema } from "./protocol-event-schema";
import {
  type HealthDeltaOperationInput,
  type HealthStatusEvent,
  invalidHealthRow,
  isStringArray,
  validateExactSummaryKeys,
  validateExactSummaryRowCount,
  validateHealthTopicName,
  type HealthDeltaOperation,
  type ViewServerProtocolEvent,
} from "./protocol-health-common";
import { ViewServerHealthSummaryRowSchema } from "./protocol-health-schema";

type ViewServerHealthSummarySnapshotEvent<Topics extends TopicDefinitions> = {
  readonly type: "snapshot";
  readonly topic: typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC;
  readonly queryId: string;
  readonly version: number;
  readonly keys: readonly ["summary"];
  readonly rows: readonly [ViewServerHealthSummaryRow<Topics>];
  readonly totalRows: 1;
};

type ViewServerHealthSummaryDeltaEvent<Topics extends TopicDefinitions> = {
  readonly type: "delta";
  readonly topic: typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC;
  readonly queryId: string;
  readonly fromVersion: number;
  readonly toVersion: number;
  readonly operations: ReadonlyArray<
    HealthDeltaOperationInput<"summary", ViewServerHealthSummaryRow<Topics>>
  >;
  readonly totalRows: 1;
};

type ViewServerHealthSummaryEvent<Topics extends TopicDefinitions> =
  | HealthStatusEvent<typeof VIEW_SERVER_HEALTH_SUMMARY_TOPIC>
  | ViewServerHealthSummarySnapshotEvent<Topics>
  | ViewServerHealthSummaryDeltaEvent<Topics>;

const validateHealthSummaryRow = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  row: ViewServerHealthSummaryRow<Topics>,
): Effect.Effect<void, ViewServerRuntimeError> => validateHealthSummaryRowEffect(config, row);

const validateHealthSummaryRowEffect = Effect.fn("ViewServerProtocol.healthSummary.row.validate")(
  function* <const Topics extends TopicDefinitions>(
    config: { readonly topics: Topics },
    row: ViewServerHealthSummaryRow<Topics>,
  ) {
    const expectedStatus =
      row.connectionStatus === "connected" ? row.runtimeStatus : row.connectionStatus;
    if (row.status !== expectedStatus) {
      return yield* Effect.fail(
        invalidHealthRow(
          VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          `Health summary status does not match runtime/connection status: ${row.status} != ${expectedStatus}`,
        ),
      );
    }
    for (const topic of row.unhealthyTopics) {
      yield* validateHealthTopicName(config, VIEW_SERVER_HEALTH_SUMMARY_TOPIC, topic);
    }
  },
);

const validateHealthSummaryIndex = Effect.fn("ViewServerProtocol.healthSummary.index.validate")(
  function* (index: number, label: string) {
    if (index !== 0) {
      return yield* Effect.fail(
        invalidHealthRow(
          VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          `Health summary ${label} index must be 0: ${index}`,
        ),
      );
    }
  },
);

const validateHealthSummaryEvent = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  event: ViewServerProtocolEvent<ViewServerHealthSummaryRow<Topics>>,
): Effect.Effect<void, ViewServerRuntimeError> => validateHealthSummaryEventEffect(config, event);

const validateHealthSummaryEventEffect = Effect.fn(
  "ViewServerProtocol.healthSummary.event.validate",
)(function* <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  event: ViewServerProtocolEvent<ViewServerHealthSummaryRow<Topics>>,
) {
  if (event.type === "snapshot") {
    yield* validateExactSummaryKeys(VIEW_SERVER_HEALTH_SUMMARY_TOPIC, event.keys);
    yield* validateExactSummaryRowCount(VIEW_SERVER_HEALTH_SUMMARY_TOPIC, event.rows.length);
    yield* validateExactSummaryRowCount(VIEW_SERVER_HEALTH_SUMMARY_TOPIC, event.totalRows);
    for (const row of event.rows) {
      yield* validateHealthSummaryRow(config, row);
    }
    return;
  }
  if (event.type === "delta") {
    yield* validateExactSummaryRowCount(VIEW_SERVER_HEALTH_SUMMARY_TOPIC, event.totalRows);
    for (const operation of event.operations) {
      if (operation.type === "update") {
        yield* validateHealthSummaryIndex(operation.index, "update");
        yield* validateHealthSummaryRow(config, operation.row);
      }
      if (operation.type === "move") {
        yield* validateHealthSummaryIndex(operation.fromIndex, "move from");
        yield* validateHealthSummaryIndex(operation.toIndex, "move to");
      }
    }
  }
});

const validateWireHealthSummaryDeltaOperation = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  operation: HealthDeltaOperation,
): Effect.Effect<void, ViewServerRuntimeError> =>
  validateWireHealthSummaryDeltaOperationEffect(config, operation);

const validateWireHealthSummaryDeltaOperationEffect = Effect.fn(
  "ViewServerProtocol.healthSummary.wireDeltaOperation.validate",
)(function* <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  operation: HealthDeltaOperation,
) {
  if (operation.key !== "summary") {
    return yield* Effect.fail(
      invalidHealthRow(
        VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        "Health summary delta key must be: summary",
      ),
    );
  }
  if (operation.type === "remove") {
    return yield* Effect.fail(
      invalidHealthRow(
        VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        "Health summary delta cannot remove summary",
      ),
    );
  }
  if (operation.type === "insert") {
    return yield* Effect.fail(
      invalidHealthRow(
        VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
        "Health summary delta cannot insert summary",
      ),
    );
  }
  if (operation.type === "update") {
    yield* validateHealthSummaryIndex(operation.index, "update");
    const id = operation.row["id"];
    if (typeof id === "string" && id !== operation.key) {
      return yield* Effect.fail(
        invalidHealthRow(
          VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          `Health summary delta key does not match row id: ${operation.key} != ${id}`,
        ),
      );
    }
    const unhealthyTopics = operation.row["unhealthyTopics"];
    if (isStringArray(unhealthyTopics)) {
      for (const topic of unhealthyTopics) {
        yield* validateHealthTopicName(config, VIEW_SERVER_HEALTH_SUMMARY_TOPIC, topic);
      }
    }
  }
  if (operation.type === "move") {
    yield* validateHealthSummaryIndex(operation.fromIndex, "move from");
    yield* validateHealthSummaryIndex(operation.toIndex, "move to");
  }
});

const validateWireHealthSummaryEvent = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  event: ViewServerWireEvent,
): Effect.Effect<void, ViewServerRuntimeError> =>
  validateWireHealthSummaryEventEffect(config, event);

const validateWireHealthSummaryEventEffect = Effect.fn(
  "ViewServerProtocol.healthSummary.wireEvent.validate",
)(function* <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  event: ViewServerWireEvent,
) {
  if (event.type === "snapshot") {
    yield* validateExactSummaryKeys(VIEW_SERVER_HEALTH_SUMMARY_TOPIC, event.keys);
    yield* validateExactSummaryRowCount(VIEW_SERVER_HEALTH_SUMMARY_TOPIC, event.rows.length);
    yield* validateExactSummaryRowCount(VIEW_SERVER_HEALTH_SUMMARY_TOPIC, event.totalRows);
    for (const row of event.rows) {
      const unhealthyTopics = row["unhealthyTopics"];
      if (isStringArray(unhealthyTopics)) {
        for (const topic of unhealthyTopics) {
          yield* validateHealthTopicName(config, VIEW_SERVER_HEALTH_SUMMARY_TOPIC, topic);
        }
      }
    }
    return;
  }
  if (event.type === "delta") {
    yield* validateExactSummaryRowCount(VIEW_SERVER_HEALTH_SUMMARY_TOPIC, event.totalRows);
    for (const operation of event.operations) {
      yield* validateWireHealthSummaryDeltaOperation(config, operation);
    }
  }
});

function typedHealthSummaryEvent<Topics extends TopicDefinitions>(
  event: ViewServerProtocolEvent<ViewServerHealthSummaryRow>,
): ViewServerHealthSummaryEvent<Topics>;
function typedHealthSummaryEvent(
  event: ViewServerProtocolEvent<ViewServerHealthSummaryRow>,
): ViewServerProtocolEvent<ViewServerHealthSummaryRow> {
  return event;
}

export const viewServerEncodeHealthSummaryEvent = Effect.fn(
  "ViewServerProtocol.healthSummary.event.encode",
)(function* <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  event: ViewServerHealthSummaryEvent<Topics>,
) {
  const encoded = yield* encodeSystemLiveEvent(
    VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
    ViewServerHealthSummaryRowSchema,
    event,
  );
  yield* validateWireHealthSummaryEvent(config, encoded);
  const decoded = yield* decodeSystemLiveEvent(
    VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
    ViewServerHealthSummaryRowSchema,
    encoded,
  );
  yield* validateHealthSummaryEvent(config, typedHealthSummaryEvent<Topics>(decoded));
  return encoded;
});

export const viewServerDecodeHealthSummaryEvent = Effect.fn(
  "ViewServerProtocol.healthSummary.event.decode",
)(function* <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  event: ViewServerWireEvent,
) {
  const wireEvent = yield* Schema.decodeUnknownEffect(ViewServerWireEventSchema)(event).pipe(
    Effect.mapError((error) =>
      invalidHealthRow(VIEW_SERVER_HEALTH_SUMMARY_TOPIC, `Invalid system event: ${error.message}`),
    ),
  );
  yield* validateWireHealthSummaryEvent(config, wireEvent);
  const decoded = yield* decodeSystemLiveEvent(
    VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
    ViewServerHealthSummaryRowSchema,
    wireEvent,
  );
  yield* validateHealthSummaryEvent(config, typedHealthSummaryEvent<Topics>(decoded));
  return typedHealthSummaryEvent<Topics>(decoded);
});
