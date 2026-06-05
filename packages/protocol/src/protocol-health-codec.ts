import type {
  TopicDefinitions,
  ViewServerHealth,
  ViewServerHealthSummaryRow,
  ViewServerHealthTopicRow,
  ViewServerRuntimeError,
} from "@view-server/config";
import { VIEW_SERVER_HEALTH_SUMMARY_TOPIC, VIEW_SERVER_HEALTH_TOPIC } from "@view-server/config";
import { Effect } from "effect";
import {
  type ViewServerProtocolEvent as _ViewServerProtocolEvent,
  type ViewServerWireEvent,
  encodeSystemLiveEvent,
  decodeSystemLiveEvent,
} from "./protocol-event-codec";
import {
  ViewServerHealthSummaryRowSchema,
  ViewServerHealthTopicRowSchema,
  type ViewServerWireHealth,
} from "./protocol-health-schema";

export {
  ViewServerHealthSchema,
  ViewServerHealthSummaryRowSchema,
  ViewServerHealthTopicRowSchema,
} from "./protocol-health-schema";
export type { ViewServerWireHealth } from "./protocol-health-schema";

type ViewServerProtocolEvent<Row> = _ViewServerProtocolEvent<Row>;

const hasTopic = <Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  topic: string,
): topic is Extract<keyof Topics, string> => Object.hasOwn(config.topics, topic);

const invalidRow = (topic: string, message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidRow",
  message,
  topic,
});

const configuredTopicNames = <const Topics extends TopicDefinitions>(config: {
  readonly topics: Topics;
}): ReadonlyArray<string> => Object.keys(config.topics);

const validateHealthTopicName = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  systemTopic: string,
  topic: string,
): Effect.Effect<string, ViewServerRuntimeError> =>
  Effect.gen(function* () {
    if (!hasTopic(config, topic)) {
      return yield* Effect.fail(
        invalidRow(systemTopic, `Health payload references unknown topic: ${topic}`),
      );
    }
    return topic;
  });

const validateNoDuplicateValues = (
  systemTopic: string,
  values: ReadonlyArray<string>,
  message: string,
): Effect.Effect<void, ViewServerRuntimeError> =>
  Effect.gen(function* () {
    const seen = new Set<string>();
    for (const value of values) {
      if (seen.has(value)) {
        return yield* Effect.fail(invalidRow(systemTopic, `${message}: ${value}`));
      }
      seen.add(value);
    }
  });

const validateExactSummaryKeys = (
  systemTopic: string,
  keys: ReadonlyArray<string>,
): Effect.Effect<void, ViewServerRuntimeError> =>
  Effect.gen(function* () {
    if (keys.length !== 1 || keys[0] !== "summary") {
      return yield* Effect.fail(
        invalidRow(systemTopic, "Health summary keys must be exactly: summary"),
      );
    }
  });

const validateExactSummaryRowCount = (
  systemTopic: string,
  rowCount: number,
): Effect.Effect<void, ViewServerRuntimeError> =>
  Effect.gen(function* () {
    if (rowCount !== 1) {
      return yield* Effect.fail(
        invalidRow(systemTopic, "Health summary must contain exactly one row"),
      );
    }
  });

const validateExactConfiguredTopicSet = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  systemTopic: string,
  values: ReadonlyArray<string>,
  label: string,
): Effect.Effect<void, ViewServerRuntimeError> =>
  Effect.gen(function* () {
    yield* validateNoDuplicateValues(systemTopic, values, `${label} contains duplicate topic`);
    const expected = configuredTopicNames(config);
    for (const topic of expected) {
      if (!values.includes(topic)) {
        return yield* Effect.fail(invalidRow(systemTopic, `${label} is missing topic: ${topic}`));
      }
    }
    for (const topic of values) {
      yield* validateHealthTopicName(config, systemTopic, topic);
    }
  });

const validateHealthSummaryRow = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  row: ViewServerHealthSummaryRow,
): Effect.Effect<void, ViewServerRuntimeError> =>
  Effect.gen(function* () {
    const expectedStatus =
      row.connectionStatus === "connected" ? row.runtimeStatus : row.connectionStatus;
    if (row.status !== expectedStatus) {
      return yield* Effect.fail(
        invalidRow(
          VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
          `Health summary status does not match runtime/connection status: ${row.status} != ${expectedStatus}`,
        ),
      );
    }
    for (const topic of row.unhealthyTopics) {
      yield* validateHealthTopicName(config, VIEW_SERVER_HEALTH_SUMMARY_TOPIC, topic);
    }
  });

const validateHealthSummaryEvent = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  event: ViewServerProtocolEvent<ViewServerHealthSummaryRow>,
): Effect.Effect<void, ViewServerRuntimeError> =>
  Effect.gen(function* () {
    if (event.type === "snapshot") {
      yield* validateExactSummaryKeys(VIEW_SERVER_HEALTH_SUMMARY_TOPIC, event.keys);
      yield* validateExactSummaryRowCount(VIEW_SERVER_HEALTH_SUMMARY_TOPIC, event.rows.length);
      for (const row of event.rows) {
        yield* validateHealthSummaryRow(config, row);
      }
      return;
    }
    if (event.type === "delta") {
      for (const operation of event.operations) {
        if (operation.type === "insert" || operation.type === "update") {
          yield* validateHealthSummaryRow(config, operation.row);
        }
      }
    }
  });

const validateHealthTopicRow = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  row: ViewServerHealthTopicRow,
): Effect.Effect<void, ViewServerRuntimeError> =>
  validateHealthTopicName(config, VIEW_SERVER_HEALTH_TOPIC, row.id);

const validateHealthTopicEvent = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  event: ViewServerProtocolEvent<ViewServerHealthTopicRow>,
): Effect.Effect<void, ViewServerRuntimeError> =>
  Effect.gen(function* () {
    if (event.type === "snapshot") {
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
      for (const operation of event.operations) {
        yield* validateHealthTopicName(config, VIEW_SERVER_HEALTH_TOPIC, operation.key);
        if (operation.type === "insert" || operation.type === "update") {
          yield* validateHealthTopicRow(config, operation.row);
        }
      }
    }
  });

const isStringArray = (value: unknown): value is ReadonlyArray<string> =>
  Array.isArray(value) && value.every((entry) => typeof entry === "string");

const validateWireHealthSummaryEvent = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  event: ViewServerWireEvent,
): Effect.Effect<void, ViewServerRuntimeError> =>
  Effect.gen(function* () {
    if (event.type === "snapshot") {
      yield* validateExactSummaryKeys(VIEW_SERVER_HEALTH_SUMMARY_TOPIC, event.keys);
      yield* validateExactSummaryRowCount(VIEW_SERVER_HEALTH_SUMMARY_TOPIC, event.rows.length);
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
      for (const operation of event.operations) {
        if (operation.key !== "summary") {
          return yield* Effect.fail(
            invalidRow(
              VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
              "Health summary delta key must be: summary",
            ),
          );
        }
        if (operation.type === "insert" || operation.type === "update") {
          const id = operation.row["id"];
          if (typeof id === "string" && id !== operation.key) {
            return yield* Effect.fail(
              invalidRow(
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
      }
    }
  });

const validateWireHealthTopicEvent = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  event: ViewServerWireEvent,
): Effect.Effect<void, ViewServerRuntimeError> =>
  Effect.gen(function* () {
    if (event.type === "snapshot") {
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
              invalidRow(
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
      return;
    }
    if (event.type === "delta") {
      for (const operation of event.operations) {
        yield* validateHealthTopicName(config, VIEW_SERVER_HEALTH_TOPIC, operation.key);
        if (operation.type === "insert" || operation.type === "update") {
          const id = operation.row["id"];
          if (typeof id === "string") {
            if (id !== operation.key) {
              return yield* Effect.fail(
                invalidRow(
                  VIEW_SERVER_HEALTH_TOPIC,
                  `Health topic delta key does not match row id: ${operation.key} != ${id}`,
                ),
              );
            }
            yield* validateHealthTopicName(config, VIEW_SERVER_HEALTH_TOPIC, id);
          }
        }
      }
    }
  });

function typedHealthSummaryEvent<Topics extends TopicDefinitions>(
  event: ViewServerProtocolEvent<ViewServerHealthSummaryRow>,
): ViewServerProtocolEvent<ViewServerHealthSummaryRow<Topics>>;
function typedHealthSummaryEvent(
  event: ViewServerProtocolEvent<ViewServerHealthSummaryRow>,
): ViewServerProtocolEvent<ViewServerHealthSummaryRow> {
  return event;
}

function typedHealthTopicEvent<Topics extends TopicDefinitions>(
  event: ViewServerProtocolEvent<ViewServerHealthTopicRow>,
): ViewServerProtocolEvent<ViewServerHealthTopicRow<Extract<keyof Topics, string>>>;
function typedHealthTopicEvent(
  event: ViewServerProtocolEvent<ViewServerHealthTopicRow>,
): ViewServerProtocolEvent<ViewServerHealthTopicRow> {
  return event;
}

export const viewServerEncodeHealthSummaryEvent = Effect.fn(
  "ViewServerProtocol.healthSummary.event.encode",
)(function* (event: ViewServerProtocolEvent<ViewServerHealthSummaryRow>) {
  return yield* encodeSystemLiveEvent(
    VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
    ViewServerHealthSummaryRowSchema,
    event,
  );
});

export const viewServerDecodeHealthSummaryEvent = Effect.fn(
  "ViewServerProtocol.healthSummary.event.decode",
)(function* <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  event: ViewServerWireEvent,
) {
  yield* validateWireHealthSummaryEvent(config, event);
  const decoded = yield* decodeSystemLiveEvent(
    VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
    ViewServerHealthSummaryRowSchema,
    event,
  );
  yield* validateHealthSummaryEvent(config, decoded);
  return typedHealthSummaryEvent<Topics>(decoded);
});

export const viewServerEncodeHealthTopicEvent = Effect.fn(
  "ViewServerProtocol.healthTopic.event.encode",
)(function* (event: ViewServerProtocolEvent<ViewServerHealthTopicRow>) {
  return yield* encodeSystemLiveEvent(
    VIEW_SERVER_HEALTH_TOPIC,
    ViewServerHealthTopicRowSchema,
    event,
  );
});

export const viewServerDecodeHealthTopicEvent = Effect.fn(
  "ViewServerProtocol.healthTopic.event.decode",
)(function* <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  event: ViewServerWireEvent,
) {
  yield* validateWireHealthTopicEvent(config, event);
  const decoded = yield* decodeSystemLiveEvent(
    VIEW_SERVER_HEALTH_TOPIC,
    ViewServerHealthTopicRowSchema,
    event,
  );
  yield* validateHealthTopicEvent(config, decoded);
  return typedHealthTopicEvent<Topics>(decoded);
});

function typedHealth<Topics extends TopicDefinitions>(
  health: ViewServerWireHealth,
): ViewServerHealth<Topics>;
function typedHealth(health: ViewServerWireHealth): ViewServerWireHealth {
  return health;
}

export const viewServerDecodeHealth = Effect.fn("ViewServerProtocol.health.decode")(function* <
  const Topics extends TopicDefinitions,
>(config: { readonly topics: Topics }, health: ViewServerWireHealth) {
  const configuredTopics = configuredTopicNames(config);
  const healthTopics = Object.keys(health.engine.topics);
  for (const topic of configuredTopics) {
    if (!Object.hasOwn(health.engine.topics, topic)) {
      return yield* Effect.fail(invalidRow(topic, `Health payload is missing topic: ${topic}`));
    }
  }
  for (const topic of healthTopics) {
    if (!hasTopic(config, topic)) {
      return yield* Effect.fail(
        invalidRow(topic, `Health payload references unknown topic: ${topic}`),
      );
    }
  }
  return typedHealth<Topics>(health);
});
