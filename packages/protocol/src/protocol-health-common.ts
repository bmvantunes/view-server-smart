import type { StatusEvent, TopicDefinitions, ViewServerRuntimeError } from "@view-server/config";
import { Effect } from "effect";
import type {
  ViewServerProtocolEvent as _ViewServerProtocolEvent,
  ViewServerWireEvent,
} from "./protocol-event-codec";

export type ViewServerProtocolEvent<Row> = _ViewServerProtocolEvent<Row>;

export type HealthStatusEvent<Topic extends string> = StatusEvent & {
  readonly topic: Topic;
};

export type HealthSnapshotEvent<Topic extends string, Key extends string, Row> = {
  readonly type: "snapshot";
  readonly topic: Topic;
  readonly queryId: string;
  readonly version: number;
  readonly keys: ReadonlyArray<Key>;
  readonly rows: ReadonlyArray<Row>;
  readonly totalRows: number;
};

type HealthUpdateOperation<Key extends string, Row> = Key extends string
  ? {
      readonly type: "update";
      readonly key: Key;
      readonly row: Row & { readonly id: Key };
      readonly index: number;
    }
  : never;

type HealthMoveOperation<Key extends string> = {
  readonly type: "move";
  readonly key: Key;
  readonly fromIndex: number;
  readonly toIndex: number;
};

export type HealthDeltaOperationInput<Key extends string, Row> =
  | HealthUpdateOperation<Key, Row>
  | HealthMoveOperation<Key>;

export type HealthDeltaEvent<Topic extends string, Key extends string, Row> = {
  readonly type: "delta";
  readonly topic: Topic;
  readonly queryId: string;
  readonly fromVersion: number;
  readonly toVersion: number;
  readonly operations: ReadonlyArray<HealthDeltaOperationInput<Key, Row>>;
  readonly totalRows: number;
};

export type HealthEvent<Topic extends string, Key extends string, Row> =
  | HealthSnapshotEvent<Topic, Key, Row>
  | HealthDeltaEvent<Topic, Key, Row>
  | HealthStatusEvent<Topic>;

export const hasConfiguredTopic = <Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  topic: string,
): topic is Extract<keyof Topics, string> => Object.hasOwn(config.topics, topic);

export const invalidHealthRow = (topic: string, message: string): ViewServerRuntimeError => ({
  _tag: "ViewServerRuntimeError",
  code: "InvalidRow",
  message,
  topic,
});

export const configuredTopicNames = <const Topics extends TopicDefinitions>(config: {
  readonly topics: Topics;
}): ReadonlyArray<string> => Object.keys(config.topics);

export const validateHealthTopicName = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  systemTopic: string,
  topic: string,
): Effect.Effect<string, ViewServerRuntimeError> =>
  validateHealthTopicNameEffect(config, systemTopic, topic);

const validateHealthTopicNameEffect = Effect.fn("ViewServerProtocol.health.topicName.validate")(
  function* <const Topics extends TopicDefinitions>(
    config: { readonly topics: Topics },
    systemTopic: string,
    topic: string,
  ) {
    if (!hasConfiguredTopic(config, topic)) {
      return yield* Effect.fail(
        invalidHealthRow(systemTopic, `Health payload references unknown topic: ${topic}`),
      );
    }
    return topic;
  },
);

const validateNoDuplicateValues = (
  systemTopic: string,
  values: ReadonlyArray<string>,
  message: string,
): Effect.Effect<void, ViewServerRuntimeError> =>
  validateNoDuplicateValuesEffect(systemTopic, values, message);

const validateNoDuplicateValuesEffect = Effect.fn("ViewServerProtocol.health.duplicates.validate")(
  function* (systemTopic: string, values: ReadonlyArray<string>, message: string) {
    const seen = new Set<string>();
    for (const value of values) {
      if (seen.has(value)) {
        return yield* Effect.fail(invalidHealthRow(systemTopic, `${message}: ${value}`));
      }
      seen.add(value);
    }
  },
);

export const validateExactSummaryKeys = (
  systemTopic: string,
  keys: ReadonlyArray<string>,
): Effect.Effect<void, ViewServerRuntimeError> => validateExactSummaryKeysEffect(systemTopic, keys);

const validateExactSummaryKeysEffect = Effect.fn("ViewServerProtocol.health.summaryKeys.validate")(
  function* (systemTopic: string, keys: ReadonlyArray<string>) {
    if (keys.length !== 1 || keys[0] !== "summary") {
      return yield* Effect.fail(
        invalidHealthRow(systemTopic, "Health summary keys must be exactly: summary"),
      );
    }
  },
);

export const validateExactSummaryRowCount = (
  systemTopic: string,
  rowCount: number,
): Effect.Effect<void, ViewServerRuntimeError> =>
  validateExactSummaryRowCountEffect(systemTopic, rowCount);

const validateExactSummaryRowCountEffect = Effect.fn(
  "ViewServerProtocol.health.summaryRowCount.validate",
)(function* (systemTopic: string, rowCount: number) {
  if (rowCount !== 1) {
    return yield* Effect.fail(
      invalidHealthRow(systemTopic, "Health summary must contain exactly one row"),
    );
  }
});

export const validateExactConfiguredTopicSet = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  systemTopic: string,
  values: ReadonlyArray<string>,
  label: string,
): Effect.Effect<void, ViewServerRuntimeError> =>
  validateExactConfiguredTopicSetEffect(config, systemTopic, values, label);

const validateExactConfiguredTopicSetEffect = Effect.fn(
  "ViewServerProtocol.health.configuredTopicSet.validate",
)(function* <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  systemTopic: string,
  values: ReadonlyArray<string>,
  label: string,
) {
  yield* validateNoDuplicateValues(systemTopic, values, `${label} contains duplicate topic`);
  const expected = configuredTopicNames(config);
  const actual = new Set(values);
  for (const topic of expected) {
    if (!actual.has(topic)) {
      return yield* Effect.fail(
        invalidHealthRow(systemTopic, `${label} is missing topic: ${topic}`),
      );
    }
  }
  for (const topic of values) {
    yield* validateHealthTopicName(config, systemTopic, topic);
  }
});

export const validateExactConfiguredTopicTotalRows = <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  systemTopic: string,
  totalRows: number,
  label: string,
): Effect.Effect<void, ViewServerRuntimeError> =>
  validateExactConfiguredTopicTotalRowsEffect(config, systemTopic, totalRows, label);

const validateExactConfiguredTopicTotalRowsEffect = Effect.fn(
  "ViewServerProtocol.health.configuredTopicTotalRows.validate",
)(function* <const Topics extends TopicDefinitions>(
  config: { readonly topics: Topics },
  systemTopic: string,
  totalRows: number,
  label: string,
) {
  const expected = configuredTopicNames(config).length;
  if (totalRows !== expected) {
    return yield* Effect.fail(
      invalidHealthRow(
        systemTopic,
        `${label} totalRows must equal configured topic count: ${totalRows} != ${expected}`,
      ),
    );
  }
});

export const isStringArray = (value: unknown): value is ReadonlyArray<string> =>
  Array.isArray(value) && value.every((entry) => typeof entry === "string");

export type HealthDeltaOperation = Extract<
  ViewServerWireEvent,
  { readonly type: "delta" }
>["operations"][number];
