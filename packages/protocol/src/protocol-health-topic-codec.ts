import type {
  TopicDefinitions,
  ViewServerConfig,
  ViewServerHealthTopicRow,
  ViewServerRuntimeError,
} from "@view-server/config";
import { VIEW_SERVER_HEALTH_TOPIC } from "@view-server/config";
import { Effect, Schema } from "effect";
import {
  decodeSystemLiveEvent,
  encodeSystemLiveEvent,
  type ViewServerWireEvent,
} from "./protocol-event-codec";
import { ViewServerWireEventSchema } from "./protocol-event-schema";
import {
  configuredTopicNames,
  type HealthEvent,
  invalidHealthRow,
  validateExactConfiguredTopicTotalRows,
  validateExactConfiguredTopicSet,
  validateHealthTopicName,
  type HealthDeltaOperation,
  type ViewServerProtocolEvent,
} from "./protocol-health-common";
import { ViewServerHealthTopicRowSchema } from "./protocol-health-schema";

type ViewServerHealthTopicEvent<Topics extends TopicDefinitions> = HealthEvent<
  typeof VIEW_SERVER_HEALTH_TOPIC,
  Extract<keyof Topics, string>,
  ViewServerHealthTopicRow<Extract<keyof Topics, string>>
>;

const validateHealthTopicRow = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  row: ViewServerHealthTopicRow<Extract<keyof Topics, string>>,
): Effect.Effect<void, ViewServerRuntimeError> => validateHealthTopicRowEffect(config, row);

const validateHealthTopicRowEffect = Effect.fn("ViewServerProtocol.healthTopic.row.validate")(
  function* <const Topics extends TopicDefinitions>(
    config: { readonly topics: Topics },
    row: ViewServerHealthTopicRow<Extract<keyof Topics, string>>,
  ) {
    yield* validateHealthTopicName(config, VIEW_SERVER_HEALTH_TOPIC, row.id);
  },
);

const validateHealthTopicIndex = Effect.fn("ViewServerProtocol.healthTopic.index.validate")(
  function* <const Topics extends TopicDefinitions>(
    config: { readonly topics: Topics },
    index: number,
    label: string,
  ) {
    const topicCount = configuredTopicNames(config).length;
    if (!Number.isInteger(index) || index < 0 || index >= topicCount) {
      return yield* Effect.fail(
        invalidHealthRow(
          VIEW_SERVER_HEALTH_TOPIC,
          `Health topic ${label} index must be within configured topic count: ${index}`,
        ),
      );
    }
  },
);

const validateHealthTopicEvent = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  event: ViewServerProtocolEvent<ViewServerHealthTopicRow<Extract<keyof Topics, string>>>,
): Effect.Effect<void, ViewServerRuntimeError> => validateHealthTopicEventEffect(config, event);

const validateHealthTopicEventEffect = Effect.fn("ViewServerProtocol.healthTopic.event.validate")(
  function* <const Topics extends TopicDefinitions>(
    config: { readonly topics: Topics },
    event: ViewServerProtocolEvent<ViewServerHealthTopicRow<Extract<keyof Topics, string>>>,
  ) {
    if (event.type === "snapshot") {
      yield* validateExactConfiguredTopicTotalRows(
        config,
        VIEW_SERVER_HEALTH_TOPIC,
        event.totalRows,
        "Health topic snapshot",
      );
      yield* validateExactConfiguredTopicSet(
        config,
        VIEW_SERVER_HEALTH_TOPIC,
        event.keys,
        "Health topic snapshot keys",
      );
      const rowIds = event.rows.map((row) => row.id);
      yield* validateExactConfiguredTopicSet(
        config,
        VIEW_SERVER_HEALTH_TOPIC,
        rowIds,
        "Health topic snapshot rows",
      );
      for (const key of event.keys) {
        yield* validateHealthTopicName(config, VIEW_SERVER_HEALTH_TOPIC, key);
      }
      for (const row of event.rows) {
        yield* validateHealthTopicRow(config, row);
      }
      return;
    }
    if (event.type === "delta") {
      yield* validateExactConfiguredTopicTotalRows(
        config,
        VIEW_SERVER_HEALTH_TOPIC,
        event.totalRows,
        "Health topic delta",
      );
      for (const operation of event.operations) {
        yield* validateHealthTopicName(config, VIEW_SERVER_HEALTH_TOPIC, operation.key);
        if (operation.type === "update") {
          yield* validateHealthTopicIndex(config, operation.index, "update");
          yield* validateHealthTopicRow(config, operation.row);
        }
        if (operation.type === "move") {
          yield* validateHealthTopicIndex(config, operation.fromIndex, "move from");
          yield* validateHealthTopicIndex(config, operation.toIndex, "move to");
        }
      }
    }
  },
);

const validateWireHealthTopicSnapshotEvent = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  event: Extract<ViewServerWireEvent, { readonly type: "snapshot" }>,
): Effect.Effect<void, ViewServerRuntimeError> =>
  validateWireHealthTopicSnapshotEventEffect(config, event);

const validateWireHealthTopicSnapshotEventEffect = Effect.fn(
  "ViewServerProtocol.healthTopic.wireSnapshot.validate",
)(function* <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  event: Extract<ViewServerWireEvent, { readonly type: "snapshot" }>,
) {
  yield* validateExactConfiguredTopicTotalRows(
    config,
    VIEW_SERVER_HEALTH_TOPIC,
    event.totalRows,
    "Health topic snapshot",
  );
  yield* validateExactConfiguredTopicSet(
    config,
    VIEW_SERVER_HEALTH_TOPIC,
    event.keys,
    "Health topic snapshot keys",
  );
  const rowIds: Array<string> = [];
  for (const key of event.keys) {
    yield* validateHealthTopicName(config, VIEW_SERVER_HEALTH_TOPIC, key);
  }
  for (const [index, row] of event.rows.entries()) {
    const id = row["id"];
    if (typeof id === "string") {
      const key = event.keys[index];
      if (key !== undefined && key !== id) {
        return yield* Effect.fail(
          invalidHealthRow(
            VIEW_SERVER_HEALTH_TOPIC,
            `Health topic snapshot key does not match row id: ${key} != ${id}`,
          ),
        );
      }
      rowIds.push(id);
      yield* validateHealthTopicName(config, VIEW_SERVER_HEALTH_TOPIC, id);
    }
  }
  yield* validateExactConfiguredTopicSet(
    config,
    VIEW_SERVER_HEALTH_TOPIC,
    rowIds,
    "Health topic snapshot rows",
  );
});

const validateWireHealthTopicDeltaOperation = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  operation: HealthDeltaOperation,
): Effect.Effect<void, ViewServerRuntimeError> =>
  validateWireHealthTopicDeltaOperationEffect(config, operation);

const validateWireHealthTopicDeltaOperationEffect = Effect.fn(
  "ViewServerProtocol.healthTopic.wireDeltaOperation.validate",
)(function* <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  operation: HealthDeltaOperation,
) {
  yield* validateHealthTopicName(config, VIEW_SERVER_HEALTH_TOPIC, operation.key);
  if (operation.type === "remove") {
    return yield* Effect.fail(
      invalidHealthRow(
        VIEW_SERVER_HEALTH_TOPIC,
        `Health topic delta cannot remove configured topic: ${operation.key}`,
      ),
    );
  }
  if (operation.type === "insert") {
    return yield* Effect.fail(
      invalidHealthRow(
        VIEW_SERVER_HEALTH_TOPIC,
        `Health topic delta cannot insert configured topic: ${operation.key}`,
      ),
    );
  }
  if (operation.type === "update") {
    yield* validateHealthTopicIndex(config, operation.index, "update");
    const id = operation.row["id"];
    if (typeof id === "string") {
      if (id !== operation.key) {
        return yield* Effect.fail(
          invalidHealthRow(
            VIEW_SERVER_HEALTH_TOPIC,
            `Health topic delta key does not match row id: ${operation.key} != ${id}`,
          ),
        );
      }
      yield* validateHealthTopicName(config, VIEW_SERVER_HEALTH_TOPIC, id);
    }
  }
  if (operation.type === "move") {
    yield* validateHealthTopicIndex(config, operation.fromIndex, "move from");
    yield* validateHealthTopicIndex(config, operation.toIndex, "move to");
  }
});

const validateWireHealthTopicEvent = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  event: ViewServerWireEvent,
): Effect.Effect<void, ViewServerRuntimeError> => validateWireHealthTopicEventEffect(config, event);

const validateWireHealthTopicEventEffect = Effect.fn(
  "ViewServerProtocol.healthTopic.wireEvent.validate",
)(function* <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  event: ViewServerWireEvent,
) {
  if (event.type === "snapshot") {
    yield* validateWireHealthTopicSnapshotEvent(config, event);
    return;
  }
  if (event.type === "delta") {
    yield* validateExactConfiguredTopicTotalRows(
      config,
      VIEW_SERVER_HEALTH_TOPIC,
      event.totalRows,
      "Health topic delta",
    );
    for (const operation of event.operations) {
      yield* validateWireHealthTopicDeltaOperation(config, operation);
    }
  }
});

function typedHealthTopicEvent<Topics extends TopicDefinitions>(
  event: ViewServerProtocolEvent<ViewServerHealthTopicRow>,
): ViewServerHealthTopicEvent<Topics>;
function typedHealthTopicEvent(
  event: ViewServerProtocolEvent<ViewServerHealthTopicRow>,
): ViewServerProtocolEvent<ViewServerHealthTopicRow> {
  return event;
}

export const viewServerEncodeHealthTopicEvent = Effect.fn(
  "ViewServerProtocol.healthTopic.event.encode",
)(function* <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  event: ViewServerHealthTopicEvent<Topics>,
) {
  const encoded = yield* encodeSystemLiveEvent(
    VIEW_SERVER_HEALTH_TOPIC,
    ViewServerHealthTopicRowSchema,
    event,
  );
  yield* validateWireHealthTopicEvent(config, encoded);
  const decoded = yield* decodeSystemLiveEvent(
    VIEW_SERVER_HEALTH_TOPIC,
    ViewServerHealthTopicRowSchema,
    encoded,
  );
  yield* validateHealthTopicEvent(config, typedHealthTopicEvent<Topics>(decoded));
  return encoded;
});

export const viewServerDecodeHealthTopicEvent = Effect.fn(
  "ViewServerProtocol.healthTopic.event.decode",
)(function* <const Topics extends TopicDefinitions>(
  config: ViewServerConfig<Topics>,
  event: ViewServerWireEvent,
) {
  const wireEvent = yield* Schema.decodeUnknownEffect(ViewServerWireEventSchema)(event).pipe(
    Effect.mapError((error) =>
      invalidHealthRow(VIEW_SERVER_HEALTH_TOPIC, `Invalid system event: ${error.message}`),
    ),
  );
  yield* validateWireHealthTopicEvent(config, wireEvent);
  const decoded = yield* decodeSystemLiveEvent(
    VIEW_SERVER_HEALTH_TOPIC,
    ViewServerHealthTopicRowSchema,
    wireEvent,
  );
  yield* validateHealthTopicEvent(config, typedHealthTopicEvent<Topics>(decoded));
  return typedHealthTopicEvent<Topics>(decoded);
});
