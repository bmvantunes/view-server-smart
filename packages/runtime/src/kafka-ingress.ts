import { Consumer } from "@platformatic/kafka";
import type {
  ConsumerGroupJoinPayload,
  GroupAssignment,
  Message,
  Offsets,
} from "@platformatic/kafka";
import { Buffer } from "node:buffer";
import {
  decodeKafkaCodec,
  decodeKafkaTopicMessage,
  kafkaErrorIsMapping,
  type KafkaCodec,
  type KafkaDecodeError,
  type RuntimeRegions,
  type KafkaMessageMetadata,
  type KafkaDecodedTopicMessage,
  type TopicRow,
  type ViewServerConfig,
  type ViewServerRuntimeClient,
} from "@effect-view-server/config";
import {
  ignoreLoggedTypedFailuresPreserveNonTypedFailures,
  runAllFinalizers,
} from "@effect-view-server/effect-utils";
import {
  Cause,
  Clock,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  MutableRef,
  Option,
  Queue,
  Ref,
  Schema,
  Semaphore,
  Scope,
} from "effect";
import type { ViewServerKafkaHealthLedger } from "./kafka-health";
import type { ResolvedViewServerKafkaRuntimeOptions } from "./runtime-options";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export class ViewServerKafkaIngressError extends Schema.TaggedErrorClass<ViewServerKafkaIngressError>()(
  "ViewServerKafkaIngressError",
  {
    message: Schema.String,
    cause: Schema.Unknown,
    region: Schema.optionalKey(Schema.String),
    sourceTopic: Schema.optionalKey(Schema.String),
  },
) {}

export type ViewServerKafkaIngress = {
  readonly close: Effect.Effect<void>;
};

export type StartedKafkaRegionConsumer = {
  readonly close: Effect.Effect<void, ViewServerKafkaIngressError>;
};

type KafkaConsumer = Consumer<Buffer, Buffer, Buffer, Buffer>;
type CloseableKafkaConsumer = {
  readonly close: (force?: boolean) => unknown;
};
type CloseableKafkaStream = {
  readonly close: () => unknown;
};
type KafkaConsumerHealthListenerRegistration = {
  readonly close: Effect.Effect<void>;
  readonly processed: Effect.Effect<number>;
  readonly waitForProcessed: (expected: number) => Effect.Effect<void>;
};
export type StartedKafkaConsumerResources = {
  readonly consumer: CloseableKafkaConsumer;
  readonly stream: CloseableKafkaStream;
  readonly healthListeners: () => KafkaConsumerHealthListenerRegistration | null;
};
type KafkaMessageBytes = Buffer | null | undefined;
type KafkaConsumerMessage = Message<KafkaMessageBytes, KafkaMessageBytes, Buffer, Buffer>;
export type KafkaStreamQueueEvent =
  | {
      readonly _tag: "Message";
      readonly message: KafkaConsumerMessage;
    }
  | {
      readonly _tag: "Failed";
      readonly cause: Cause.Cause<ViewServerKafkaIngressError>;
      readonly error: ViewServerKafkaIngressError;
    }
  | {
      readonly _tag: "End";
    };
type KafkaStreamTerminalEvent = Extract<KafkaStreamQueueEvent, { readonly _tag: "Failed" | "End" }>;
type KafkaMessageBatchTakeResult =
  | {
      readonly _tag: "Terminal";
      readonly terminal: KafkaStreamTerminalEvent;
    }
  | {
      readonly _tag: "Batch";
      readonly batch: ReadonlyArray<KafkaConsumerMessage>;
      readonly terminal: KafkaStreamTerminalEvent | null;
    };
type KafkaBatchTopic<Topics extends ViewServerRuntimeTopicDefinitions> = Extract<
  keyof Topics,
  string
>;
type KafkaDecodedTopicTombstone<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly _tag: "Tombstone";
  readonly key: string;
  readonly viewServerTopic: KafkaBatchTopic<Topics>;
};
type KafkaDecodedTopicRow<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly _tag: "Row";
  readonly message: KafkaDecodedTopicMessage<Topics, KafkaBatchTopic<Topics>>;
};
type KafkaDecodedTopicMutation<Topics extends ViewServerRuntimeTopicDefinitions> =
  | KafkaDecodedTopicRow<Topics>
  | KafkaDecodedTopicTombstone<Topics>;
type KafkaTopicWithKeyCodec = {
  readonly key: KafkaCodec<unknown, unknown>;
};
type DecodedKafkaBatchMessage<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly decoded: KafkaDecodedTopicMutation<Topics>;
  readonly message: KafkaConsumerMessage;
  readonly messageBytes: number;
  readonly nowMillis: number;
  readonly sourceTopic: string;
};
type KafkaConsumerHealthListenerWaiter = {
  readonly expected: number;
  readonly deferred: Deferred.Deferred<void>;
};
type KafkaConsumerHealthListenerProcessedState = {
  readonly count: number;
  readonly waiters: ReadonlyArray<KafkaConsumerHealthListenerWaiter>;
};
type ViewServerKafkaHealthRefreshRequest = Effect.Effect<void>;

const emptyMessageBytes = Buffer.alloc(0);
const kafkaKeyTextDecoder = new TextDecoder();
const kafkaMessageBatchSize = 256;
const kafkaMessageQueueCapacity = kafkaMessageBatchSize * 4;
const kafkaMessageBatchFlushInterval = Duration.millis(2);
const kafkaMessageBatchFlushIntervalMillis = Duration.toMillis(kafkaMessageBatchFlushInterval);
const ignoreKafkaConsumerCloseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring Kafka consumer close failure.",
);
const ignoreKafkaStartedResourceCloseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring Kafka started resource close failure.",
);
const ignoreKafkaHealthRefreshFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring Kafka health refresh failure.",
);
const ignoreKafkaAsyncIteratorCloseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring Kafka stream iterator close failure.",
);
const logKafkaHealthListenerDispatchFailure = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<void, never, R> =>
  effect.pipe(
    Effect.asVoid,
    Effect.catchCause((cause) => Effect.logError("Kafka health listener dispatch failed.", cause)),
  );

const kafkaHeaderIsRepeated = (
  value: string | Uint8Array | ReadonlyArray<string | Uint8Array>,
): value is ReadonlyArray<string | Uint8Array> => Array.isArray(value);

export const messageFromUnknown = (error: unknown): string => {
  if (error instanceof Error) {
    return error.message;
  }
  if (typeof error === "object" && error !== null && "message" in error) {
    const message = Reflect.get(error, "message");
    if (typeof message === "string") {
      return message;
    }
  }
  return String(error);
};

export const bootstrapBrokers = (brokers: string): ReadonlyArray<string> =>
  brokers
    .split(",")
    .map((broker) => broker.trim())
    .filter((broker) => broker.length > 0);

export const kafkaHeadersFromMessage = (
  headers: ReadonlyMap<Buffer, Buffer>,
): KafkaMessageMetadata["headers"] => {
  const output: Record<string, string | Uint8Array | ReadonlyArray<string | Uint8Array>> =
    Object.create(null);
  const textDecoder = new TextDecoder();
  for (const [key, value] of headers) {
    const name = textDecoder.decode(key);
    const existing = output[name];
    if (existing === undefined) {
      output[name] = value;
    } else if (kafkaHeaderIsRepeated(existing)) {
      output[name] = [...existing, value];
    } else {
      output[name] = [existing, value];
    }
  }
  return output;
};

export const sourceTopicsForRegion = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
  region: string,
): ReadonlyArray<string> => {
  const topics: Array<string> = [];
  for (const [sourceTopic, topic] of Object.entries(options.topics)) {
    if (topic.regions.some((topicRegion) => topicRegion === region)) {
      topics.push(sourceTopic);
    }
  }
  return topics;
};

export const assignedPartitionsForSourceTopic = (
  assignments: ReadonlyArray<GroupAssignment> | null | undefined,
  sourceTopic: string,
): number => {
  const assignment = assignments?.find((candidate) => candidate.topic === sourceTopic);
  return assignment?.partitions.length ?? 0;
};

const snapshotKafkaAssignments = (
  assignments: ReadonlyArray<GroupAssignment> | null | undefined,
): ReadonlyArray<GroupAssignment> | null | undefined =>
  assignments?.map((assignment) => ({
    topic: assignment.topic,
    partitions: [...assignment.partitions],
  }));

const consumerLagMessagesFromLag = (lags: ReadonlyArray<bigint>): bigint | null => {
  let total = 0n;
  let hasKnownLag = false;
  for (const lag of lags) {
    if (lag >= 0n) {
      hasKnownLag = true;
      total += lag;
    }
  }
  return hasKnownLag ? total : null;
};

export const kafkaConsumerStartError = (
  region: string,
  cause: unknown,
): ViewServerKafkaIngressError =>
  new ViewServerKafkaIngressError({
    message: `Failed to start Kafka consumer for region ${region}`,
    cause,
    region,
  });

export const mapKafkaConsumerStartError =
  (region: string) =>
  (cause: unknown): ViewServerKafkaIngressError =>
    kafkaConsumerStartError(region, cause);

export const kafkaStreamError = (region: string, cause: unknown): ViewServerKafkaIngressError =>
  new ViewServerKafkaIngressError({
    message: `Kafka stream failed for region ${region}`,
    cause,
    region,
  });

export const mapKafkaStreamError =
  (region: string) =>
  (cause: unknown): ViewServerKafkaIngressError =>
    kafkaStreamError(region, cause);

export const kafkaStreamCloseError = (cause: unknown): ViewServerKafkaIngressError =>
  new ViewServerKafkaIngressError({
    message: "Failed to close Kafka stream",
    cause,
  });

export const kafkaConsumerCloseError = (cause: unknown): ViewServerKafkaIngressError =>
  new ViewServerKafkaIngressError({
    message: "Failed to close Kafka consumer",
    cause,
  });

export const kafkaMessageCommitError = (
  region: string,
  sourceTopic: string,
  cause: unknown,
): ViewServerKafkaIngressError =>
  new ViewServerKafkaIngressError({
    message: `Failed to commit Kafka message for source topic ${sourceTopic}`,
    cause,
    region,
    sourceTopic,
  });

export const kafkaMessageDecodeError = (
  region: string,
  sourceTopic: string,
  cause: unknown,
): ViewServerKafkaIngressError =>
  new ViewServerKafkaIngressError({
    message: `Failed to decode Kafka message for source topic ${sourceTopic}`,
    cause,
    region,
    sourceTopic,
  });

export const kafkaMessageMappingError = (
  region: string,
  sourceTopic: string,
  cause: unknown,
): ViewServerKafkaIngressError =>
  new ViewServerKafkaIngressError({
    message: `Failed to map Kafka message for source topic ${sourceTopic}`,
    cause,
    region,
    sourceTopic,
  });

export const kafkaMessageProcessingError = (
  region: string,
  sourceTopic: string,
  cause: unknown,
): ViewServerKafkaIngressError =>
  new ViewServerKafkaIngressError({
    message: `Failed to process Kafka message for source topic ${sourceTopic}`,
    cause,
    region,
    sourceTopic,
  });

const isNonFailureKafkaCauseReason = (
  reason: Cause.Reason<unknown>,
): reason is Cause.Die | Cause.Interrupt =>
  Cause.isDieReason(reason) || Cause.isInterruptReason(reason);

const kafkaFailureCause = (error: unknown, cause: Cause.Cause<unknown>): unknown =>
  cause.reasons.length > 1 || Cause.hasDies(cause) || Cause.hasInterrupts(cause) ? cause : error;

const preservedKafkaFailureError = (
  primaryError: ViewServerKafkaIngressError,
  error: unknown,
): ViewServerKafkaIngressError =>
  error instanceof ViewServerKafkaIngressError
    ? error
    : new ViewServerKafkaIngressError({
        cause: error,
        message: messageFromUnknown(error),
        ...(primaryError.region === undefined ? {} : { region: primaryError.region }),
        ...(primaryError.sourceTopic === undefined
          ? {}
          : { sourceTopic: primaryError.sourceTopic }),
      });

const kafkaIngressFailureCause = (
  error: ViewServerKafkaIngressError,
  cause: Cause.Cause<unknown>,
): Cause.Cause<ViewServerKafkaIngressError> => {
  const failureReasons = cause.reasons
    .filter(Cause.isFailReason)
    .map((reason) => Cause.makeFailReason(preservedKafkaFailureError(error, reason.error)));
  const nonFailureReasons = cause.reasons.filter(isNonFailureKafkaCauseReason);
  const hasPrimaryFailure = failureReasons.some((reason) => reason.error === error);
  return Cause.fromReasons([
    ...(hasPrimaryFailure ? [] : [Cause.makeFailReason(error)]),
    ...failureReasons,
    ...nonFailureReasons,
  ]);
};

const kafkaNonFailureCause = (cause: Cause.Cause<unknown>): Cause.Cause<never> =>
  Cause.fromReasons(cause.reasons.filter(isNonFailureKafkaCauseReason));

export const recordKafkaStreamError = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  health: ViewServerKafkaHealthLedger<Topics>,
  region: string,
  error: ViewServerKafkaIngressError,
  options?: {
    readonly preserveTopicErrors?: boolean;
  },
): Effect.Effect<never, ViewServerKafkaIngressError> =>
  health
    .regionDisconnected(region, error.message, options)
    .pipe(Effect.andThen(Effect.fail(error)));

const recordKafkaStreamCause = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  health: ViewServerKafkaHealthLedger<Topics>,
  region: string,
  error: ViewServerKafkaIngressError,
  cause: Cause.Cause<ViewServerKafkaIngressError>,
  options?: {
    readonly preserveTopicErrors?: boolean;
  },
): Effect.Effect<never, ViewServerKafkaIngressError> =>
  health
    .regionDisconnected(region, error.message, options)
    .pipe(Effect.andThen(Effect.failCause(cause)));

const requestKafkaHealthRefresh = (requestHealthRefresh: ViewServerKafkaHealthRefreshRequest) =>
  requestHealthRefresh.pipe(ignoreKafkaHealthRefreshFailure);

const kafkaMessageHasTombstoneValue = (message: KafkaConsumerMessage): boolean =>
  message.value === null || message.value === undefined;

const kafkaDecodeError = (message: string, cause: unknown): KafkaDecodeError => ({
  _tag: "KafkaDecodeError",
  message,
  cause,
});

const kafkaRuntimeTopicHasKeyCodec = (topic: {
  readonly key?: KafkaCodec<unknown, unknown>;
}): topic is KafkaTopicWithKeyCodec => topic.key !== undefined;

const kafkaDecodedObjectKeyToRowId = (
  key: object,
  rowKeyField: string,
): Effect.Effect<string, KafkaDecodeError> => {
  if (Object.hasOwn(key, rowKeyField)) {
    const rowKey = Reflect.get(key, rowKeyField);
    if (typeof rowKey === "string") {
      return Effect.succeed(rowKey);
    }
  }
  const stringFields = Object.entries(key).filter(
    ([field, value]) => !field.startsWith("$") && typeof value === "string",
  );
  const onlyStringField = stringFields[0];
  if (stringFields.length === 1 && onlyStringField !== undefined) {
    return Effect.succeed(onlyStringField[1]);
  }
  return Effect.fail(
    kafkaDecodeError("Kafka tombstone key must decode to the View Server row id string", key),
  );
};

const decodeKafkaTombstoneRowKey = Effect.fn(
  "ViewServerRuntime.kafka.message.decodeTombstoneRowKey",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  topic: {
    readonly key?: KafkaCodec<unknown, unknown>;
    readonly viewServerTopic: Extract<keyof Topics, string>;
  },
  input: {
    readonly keyBytes: Uint8Array;
    readonly metadata: KafkaMessageMetadata;
  },
) {
  if (!kafkaRuntimeTopicHasKeyCodec(topic)) {
    return kafkaKeyTextDecoder.decode(input.keyBytes);
  }

  const key = yield* decodeKafkaCodec(topic.key, {
    bytes: input.keyBytes,
    metadata: input.metadata,
  });
  if (typeof key === "string") {
    return key;
  }
  if (typeof key === "object" && key !== null) {
    return yield* kafkaDecodedObjectKeyToRowId(key, config.topics[topic.viewServerTopic].key);
  }
  return yield* Effect.fail(
    kafkaDecodeError("Kafka tombstone key must decode to the View Server row id string", key),
  );
});

export const recordKafkaAssignments = Effect.fn(
  "ViewServerRuntime.kafka.consumer.recordAssignments",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  health: ViewServerKafkaHealthLedger<Topics>,
  requestHealthRefresh: ViewServerKafkaHealthRefreshRequest,
  region: string,
  topics: ReadonlyArray<string>,
  assignments: ReadonlyArray<GroupAssignment> | null | undefined,
  nowMillis: number,
) {
  yield* health.regionConnected(region, nowMillis);
  yield* Effect.forEach(
    topics,
    (sourceTopic) =>
      health.topicConnected(
        sourceTopic,
        region,
        assignedPartitionsForSourceTopic(assignments, sourceTopic),
        nowMillis,
      ),
    { discard: true },
  );
  yield* requestKafkaHealthRefresh(requestHealthRefresh);
});

export const recordKafkaLag = Effect.fn("ViewServerRuntime.kafka.consumer.recordLag")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  health: ViewServerKafkaHealthLedger<Topics>,
  requestHealthRefresh: ViewServerKafkaHealthRefreshRequest,
  region: string,
  topics: ReadonlyArray<string>,
  lag: Offsets,
  nowMillis: number,
) {
  yield* health.regionRecovered(region, nowMillis);
  yield* Effect.forEach(
    topics,
    (sourceTopic) => {
      const sourceTopicLag = lag.get(sourceTopic);
      return health.topicLagSampled(sourceTopic, region, {
        consumerLagMessages:
          sourceTopicLag === undefined ? null : consumerLagMessagesFromLag(sourceTopicLag),
        nowMillis,
      });
    },
    { discard: true },
  );
  yield* requestKafkaHealthRefresh(requestHealthRefresh);
});

export const closeKafkaConsumerAfterStartFailure = Effect.fn(
  "ViewServerRuntime.kafka.consumer.closeAfterStartFailure",
)(function* (consumer: CloseableKafkaConsumer) {
  yield* Effect.tryPromise({
    try: () => Promise.resolve(consumer.close(true)),
    catch: kafkaConsumerCloseError,
  }).pipe(ignoreKafkaConsumerCloseFailure);
});

export const closeKafkaConsumerOnStartFailure = Effect.fn(
  "ViewServerRuntime.kafka.consumer.closeOnStartFailure",
)(function* <A>(
  consumer: CloseableKafkaConsumer,
  start: Effect.Effect<A, ViewServerKafkaIngressError>,
) {
  return yield* start.pipe(
    Effect.onExit((exit) =>
      Exit.isFailure(exit) ? closeKafkaConsumerAfterStartFailure(consumer) : Effect.void,
    ),
  );
});

const makeKafkaConsumer = Effect.fn("ViewServerRuntime.kafka.consumer.make")(function* (
  region: string,
  brokers: string,
  topics: ReadonlyArray<string>,
  consume: ResolvedViewServerKafkaRuntimeOptions<ViewServerRuntimeTopicDefinitions>["consume"],
) {
  const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
    autocreateTopics: false,
    bootstrapBrokers: [...bootstrapBrokers(brokers)],
    clientId: `view-server-${region}`,
    groupId: consume.consumerGroupId,
    retries: true,
  });
  const stream = yield* closeKafkaConsumerOnStartFailure(
    consumer,
    Effect.tryPromise({
      try: () =>
        consumer.consume({
          autocommit: false,
          fallbackMode: consume.fallbackMode,
          mode: consume.mode,
          topics: [...topics],
        }),
      catch: mapKafkaConsumerStartError(region),
    }),
  );
  return { consumer, stream };
});

export const closeKafkaConsumer = Effect.fn("ViewServerRuntime.kafka.consumer.close")(
  function* (input: {
    readonly consumer: CloseableKafkaConsumer;
    readonly stream: CloseableKafkaStream;
  }) {
    yield* runAllFinalizers([
      Effect.tryPromise({
        try: () => Promise.resolve(input.stream.close()),
        catch: kafkaStreamCloseError,
      }),
      Effect.tryPromise({
        try: () => Promise.resolve(input.consumer.close(true)),
        catch: kafkaConsumerCloseError,
      }),
    ]);
  },
);

export const closeStartedKafkaConsumerResources = Effect.fn(
  "ViewServerRuntime.kafka.consumer.closeStartedResources",
)(function* (resources: StartedKafkaConsumerResources) {
  const healthListeners = resources.healthListeners();
  const closeConsumer = closeKafkaConsumer({
    consumer: resources.consumer,
    stream: resources.stream,
  });
  yield* runAllFinalizers(
    healthListeners === null ? [closeConsumer] : [healthListeners.close, closeConsumer],
  );
});

export const closeKafkaMessageStreamFiber = Effect.fn("ViewServerRuntime.kafka.stream.closeFiber")(
  function* (
    fiber: Fiber.Fiber<void, ViewServerKafkaIngressError>,
    closeResources: Effect.Effect<void, ViewServerKafkaIngressError>,
  ) {
    const awaitTargetExit = Fiber.await(fiber).pipe(
      Effect.andThen((exit) =>
        Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause)
          ? Effect.failCause(exit.cause)
          : Effect.void,
      ),
    );
    yield* Effect.uninterruptible(
      Effect.gen(function* () {
        yield* Effect.sync(() => {
          fiber.interruptUnsafe();
        });
        yield* runAllFinalizers([closeResources, awaitTargetExit]);
      }),
    );
  },
);

export const makeStartedKafkaConsumerResourcesFinalizer = Effect.fn(
  "ViewServerRuntime.kafka.consumer.makeStartedResourcesFinalizer",
)((resources: StartedKafkaConsumerResources) =>
  Ref.make(false).pipe(
    Effect.map((resourcesClosed) => {
      const closeLock = Semaphore.makeUnsafe(1);
      return Effect.uninterruptible(
        closeLock.withPermits(1)(
          Effect.gen(function* () {
            const alreadyClosed = yield* Ref.get(resourcesClosed);
            if (alreadyClosed) {
              return;
            }
            yield* closeStartedKafkaConsumerResources(resources);
            yield* Ref.set(resourcesClosed, true);
          }),
        ),
      );
    }),
  ),
);

export const registerStartedKafkaConsumerResourcesFinalizer = Effect.fn(
  "ViewServerRuntime.kafka.consumer.registerStartedResourcesFinalizer",
)(function* (scope: Scope.Scope, closeResources: Effect.Effect<void, ViewServerKafkaIngressError>) {
  yield* Scope.addFinalizer(scope, closeResources.pipe(ignoreKafkaStartedResourceCloseFailure));
});

export const closeStartedKafkaRegionConsumers = Effect.fn(
  "ViewServerRuntime.kafka.regions.closeStartedConsumers",
)(function* (consumers: ReadonlyArray<StartedKafkaRegionConsumer>) {
  yield* runAllFinalizers(consumers.map((consumer) => consumer.close));
});

export const closeKafkaConsumerOnPostConsumeStartupFailure = Effect.fn(
  "ViewServerRuntime.kafka.consumer.closeOnPostConsumeStartupFailure",
)(function* <A, E>(
  closeResources: Effect.Effect<void, ViewServerKafkaIngressError>,
  startup: Effect.Effect<A, E>,
) {
  return yield* startup.pipe(
    Effect.onExit((exit) => (Exit.isFailure(exit) ? closeResources : Effect.void)),
  );
});

export const acquireStartedKafkaConsumerResources = Effect.fn(
  "ViewServerRuntime.kafka.consumer.acquireStartedResources",
)(function* <Consumer extends CloseableKafkaConsumer, Stream extends CloseableKafkaStream, E>(
  scope: Scope.Scope,
  acquire: Effect.Effect<
    {
      readonly consumer: Consumer;
      readonly stream: Stream;
    },
    E
  >,
) {
  return yield* Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const { consumer, stream } = yield* restore(acquire);
      let healthListeners: KafkaConsumerHealthListenerRegistration | null = null;
      const resources: StartedKafkaConsumerResources = {
        consumer,
        healthListeners: () => healthListeners,
        stream,
      };
      const closeResources = yield* makeStartedKafkaConsumerResourcesFinalizer(resources);
      yield* registerStartedKafkaConsumerResourcesFinalizer(scope, closeResources);
      return {
        consumer,
        closeResources,
        setHealthListeners: (registration: KafkaConsumerHealthListenerRegistration) => {
          healthListeners = registration;
        },
        stream,
      };
    }),
  );
});

const decodeKafkaMessageForBatch = Effect.fn("ViewServerRuntime.kafka.message.decodeForBatch")(
  function* <
    const Topics extends ViewServerRuntimeTopicDefinitions,
    const Regions extends RuntimeRegions,
  >(
    config: ViewServerConfig<Topics>,
    requestHealthRefresh: ViewServerKafkaHealthRefreshRequest,
    options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
    health: ViewServerKafkaHealthLedger<Topics>,
    region: string,
    message: KafkaConsumerMessage,
  ) {
    const sourceTopic = message.topic;
    const topic = options.topics[sourceTopic];
    if (topic === undefined) {
      return Option.none<DecodedKafkaBatchMessage<Topics>>();
    }
    const topicRegion = topic.regions.find((candidate) => candidate === region);
    if (topicRegion === undefined) {
      return Option.none<DecodedKafkaBatchMessage<Topics>>();
    }
    const nowMillis = yield* Clock.currentTimeMillis;
    const keyBytes = message.key ?? emptyMessageBytes;
    const valueBytes = message.value ?? emptyMessageBytes;
    const messageBytes = valueBytes.byteLength + keyBytes.byteLength;
    const metadata: KafkaMessageMetadata<typeof topicRegion> = {
      sourceTopic,
      sourceRegion: topicRegion,
      partition: message.partition,
      offset: String(message.offset),
      timestamp: Number(message.timestamp),
      headers: kafkaHeadersFromMessage(message.headers),
    };
    const requestHealthRefreshAfterLedgerUpdate = requestKafkaHealthRefresh(requestHealthRefresh);
    if (kafkaMessageHasTombstoneValue(message)) {
      if (message.key === null || message.key === undefined) {
        return yield* health
          .decodeFailed(sourceTopic, region, {
            bytes: messageBytes,
            message: "Kafka tombstone requires a non-null key",
            nowMillis,
          })
          .pipe(
            Effect.andThen(requestHealthRefreshAfterLedgerUpdate),
            Effect.andThen(
              Effect.fail(
                kafkaMessageDecodeError(
                  region,
                  sourceTopic,
                  new Error("Kafka tombstone requires a non-null key"),
                ),
              ),
            ),
          );
      }
      const rowKey = yield* decodeKafkaTombstoneRowKey(config, topic, {
        keyBytes: message.key,
        metadata,
      }).pipe(
        Effect.matchCauseEffect({
          onFailure: (cause) => {
            const error = Cause.findErrorOption(cause);
            if (Cause.hasInterruptsOnly(cause)) {
              return Effect.failCause(kafkaNonFailureCause(cause));
            }
            if (Option.isSome(error)) {
              return health
                .decodeFailed(sourceTopic, region, {
                  bytes: messageBytes,
                  message: messageFromUnknown(error.value),
                  nowMillis,
                })
                .pipe(
                  Effect.andThen(requestHealthRefreshAfterLedgerUpdate),
                  Effect.andThen(
                    Effect.failCause(
                      kafkaIngressFailureCause(
                        kafkaMessageDecodeError(
                          region,
                          sourceTopic,
                          kafkaFailureCause(error.value, cause),
                        ),
                        cause,
                      ),
                    ),
                  ),
                );
            }
            return health
              .decodeFailed(sourceTopic, region, {
                bytes: messageBytes,
                message: messageFromUnknown(Cause.squash(cause)),
                nowMillis,
              })
              .pipe(
                Effect.andThen(requestHealthRefreshAfterLedgerUpdate),
                Effect.andThen(
                  Effect.failCause(
                    kafkaIngressFailureCause(
                      kafkaMessageDecodeError(
                        region,
                        sourceTopic,
                        kafkaFailureCause(Cause.squash(cause), cause),
                      ),
                      cause,
                    ),
                  ),
                ),
              );
          },
          onSuccess: (key) => Effect.succeed(key),
        }),
      );
      const decoded: KafkaDecodedTopicTombstone<Topics> = {
        _tag: "Tombstone",
        key: rowKey,
        viewServerTopic: topic.viewServerTopic,
      };
      return Option.some({
        decoded,
        message,
        messageBytes,
        nowMillis,
        sourceTopic,
      });
    }
    const decoded = yield* decodeKafkaTopicMessage(topic, {
      keyBytes,
      valueBytes,
      region: topicRegion,
      metadata,
    }).pipe(
      Effect.matchCauseEffect({
        onFailure: (cause) => {
          const error = Cause.findErrorOption(cause);
          if (Cause.hasInterruptsOnly(cause)) {
            return Effect.failCause(kafkaNonFailureCause(cause));
          }
          if (Option.isSome(error)) {
            if (kafkaErrorIsMapping(error.value)) {
              return health
                .mappingFailed(sourceTopic, region, {
                  bytes: messageBytes,
                  message: messageFromUnknown(error.value),
                  nowMillis,
                })
                .pipe(
                  Effect.andThen(requestHealthRefreshAfterLedgerUpdate),
                  Effect.andThen(
                    Effect.failCause(
                      kafkaIngressFailureCause(
                        kafkaMessageMappingError(
                          region,
                          sourceTopic,
                          kafkaFailureCause(error.value, cause),
                        ),
                        cause,
                      ),
                    ),
                  ),
                );
            }
            return health
              .decodeFailed(sourceTopic, region, {
                bytes: messageBytes,
                message: messageFromUnknown(error.value),
                nowMillis,
              })
              .pipe(
                Effect.andThen(requestHealthRefreshAfterLedgerUpdate),
                Effect.andThen(
                  Effect.failCause(
                    kafkaIngressFailureCause(
                      kafkaMessageDecodeError(
                        region,
                        sourceTopic,
                        kafkaFailureCause(error.value, cause),
                      ),
                      cause,
                    ),
                  ),
                ),
              );
          }
          return health
            .decodeFailed(sourceTopic, region, {
              bytes: messageBytes,
              message: messageFromUnknown(Cause.squash(cause)),
              nowMillis,
            })
            .pipe(
              Effect.andThen(requestHealthRefreshAfterLedgerUpdate),
              Effect.andThen(
                Effect.failCause(
                  kafkaIngressFailureCause(
                    kafkaMessageDecodeError(
                      region,
                      sourceTopic,
                      kafkaFailureCause(Cause.squash(cause), cause),
                    ),
                    cause,
                  ),
                ),
              ),
            );
        },
        onSuccess: (decodedMessage) => Effect.succeed(decodedMessage),
      }),
    );
    const decodedMutation: KafkaDecodedTopicRow<Topics> = {
      _tag: "Row",
      message: decoded,
    };
    return Option.some({
      decoded: decodedMutation,
      message,
      messageBytes,
      nowMillis,
      sourceTopic,
    });
  },
);

const publishKafkaDecodedBatch = Effect.fn("ViewServerRuntime.kafka.batch.publish")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  client: ViewServerRuntimeClient<Topics>,
  requestHealthRefresh: ViewServerKafkaHealthRefreshRequest,
  health: ViewServerKafkaHealthLedger<Topics>,
  region: string,
  messages: ReadonlyArray<DecodedKafkaBatchMessage<Topics>>,
) {
  type KafkaRowBatchGroup = {
    readonly sourceTopic: string;
    readonly viewServerTopic: KafkaBatchTopic<Topics>;
    readonly rows: Array<TopicRow<Topics, KafkaBatchTopic<Topics>>>;
    readonly messages: Array<DecodedKafkaBatchMessage<Topics>>;
  };
  let rowGroup: KafkaRowBatchGroup | undefined;
  const publishRows = Effect.fn("ViewServerRuntime.kafka.batch.publishRows")(function* (
    group: KafkaRowBatchGroup,
  ) {
    yield* client.publishMany(group.viewServerTopic, group.rows).pipe(
      Effect.matchCauseEffect({
        onFailure: (cause) => {
          if (Cause.hasInterruptsOnly(cause)) {
            return Effect.failCause(kafkaNonFailureCause(cause));
          }
          const error = Cause.findErrorOption(cause);
          const failure = Option.match(error, {
            onNone: () => Cause.squash(cause),
            onSome: (value) => value,
          });
          const processingError = kafkaMessageProcessingError(
            region,
            group.sourceTopic,
            kafkaFailureCause(failure, cause),
          );
          return Effect.forEach(
            group.messages,
            (message) =>
              health.messagePublishFailed(message.sourceTopic, region, {
                bytes: message.messageBytes,
                message: messageFromUnknown(failure),
                nowMillis: message.nowMillis,
              }),
            { discard: true },
          ).pipe(
            Effect.andThen(requestKafkaHealthRefresh(requestHealthRefresh)),
            Effect.andThen(Effect.failCause(kafkaIngressFailureCause(processingError, cause))),
          );
        },
        onSuccess: () => Effect.succeed(true),
      }),
    );
  });
  const flushRows = Effect.fn("ViewServerRuntime.kafka.batch.flushRows")(function* () {
    const group = rowGroup;
    if (group !== undefined) {
      rowGroup = undefined;
      yield* publishRows(group);
    }
  });
  const deleteTombstone = Effect.fn("ViewServerRuntime.kafka.batch.deleteTombstone")(function* (
    message: DecodedKafkaBatchMessage<Topics>,
    tombstone: KafkaDecodedTopicTombstone<Topics>,
  ) {
    yield* client.delete(tombstone.viewServerTopic, tombstone.key).pipe(
      Effect.matchCauseEffect({
        onFailure: (cause) => {
          if (Cause.hasInterruptsOnly(cause)) {
            return Effect.failCause(kafkaNonFailureCause(cause));
          }
          const error = Cause.findErrorOption(cause);
          const failure = Option.match(error, {
            onNone: () => Cause.squash(cause),
            onSome: (value) => value,
          });
          const processingError = kafkaMessageProcessingError(
            region,
            message.sourceTopic,
            kafkaFailureCause(failure, cause),
          );
          return health
            .messagePublishFailed(message.sourceTopic, region, {
              bytes: message.messageBytes,
              message: messageFromUnknown(failure),
              nowMillis: message.nowMillis,
            })
            .pipe(
              Effect.andThen(requestKafkaHealthRefresh(requestHealthRefresh)),
              Effect.andThen(Effect.failCause(kafkaIngressFailureCause(processingError, cause))),
            );
        },
        onSuccess: () => Effect.succeed(true),
      }),
    );
  });
  for (const message of messages) {
    if (message.decoded._tag === "Tombstone") {
      yield* flushRows();
      yield* deleteTombstone(message, message.decoded);
      continue;
    }
    const decodedMessage = message.decoded.message;
    if (rowGroup === undefined) {
      rowGroup = {
        messages: [message],
        rows: [decodedMessage.row],
        sourceTopic: message.sourceTopic,
        viewServerTopic: decodedMessage.viewServerTopic,
      };
      continue;
    }
    if (rowGroup.viewServerTopic !== decodedMessage.viewServerTopic) {
      yield* flushRows();
      rowGroup = {
        messages: [message],
        rows: [decodedMessage.row],
        sourceTopic: message.sourceTopic,
        viewServerTopic: decodedMessage.viewServerTopic,
      };
      continue;
    }
    rowGroup.messages.push(message);
    rowGroup.rows.push(decodedMessage.row);
  }
  yield* flushRows();
});

const commitKafkaDecodedBatch = Effect.fn("ViewServerRuntime.kafka.batch.commit")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  requestHealthRefresh: ViewServerKafkaHealthRefreshRequest,
  health: ViewServerKafkaHealthLedger<Topics>,
  region: string,
  messages: ReadonlyArray<DecodedKafkaBatchMessage<Topics>>,
  options?: {
    readonly preserveLastErrorForSourceTopic: string | undefined;
  },
) {
  if (messages.length === 0) {
    return;
  }
  yield* Effect.gen(function* () {
    for (const message of messages) {
      yield* Effect.uninterruptible(
        Effect.tryPromise({
          try: () => Promise.resolve(message.message.commit()),
          catch: (cause) => kafkaMessageCommitError(region, message.sourceTopic, cause),
        }).pipe(
          Effect.matchEffect({
            onFailure: (error) =>
              health
                .messageCommitFailed(message.sourceTopic, region, {
                  bytes: message.messageBytes,
                  message: `${error.message}: ${messageFromUnknown(error.cause)}`,
                  nowMillis: message.nowMillis,
                })
                .pipe(Effect.andThen(Effect.fail(error))),
            onSuccess: () =>
              health.messageDecoded(message.sourceTopic, region, {
                bytes: message.messageBytes,
                committedOffset: String(message.message.offset + 1n),
                nowMillis: message.nowMillis,
                ...(options?.preserveLastErrorForSourceTopic === message.sourceTopic
                  ? { preserveLastError: true }
                  : {}),
              }),
          }),
        ),
      );
    }
  }).pipe(Effect.ensuring(requestKafkaHealthRefresh(requestHealthRefresh)));
});

export const processKafkaMessageBatch = Effect.fn("ViewServerRuntime.kafka.messageBatch.process")(
  function* <
    const Topics extends ViewServerRuntimeTopicDefinitions,
    const Regions extends RuntimeRegions,
  >(
    config: ViewServerConfig<Topics>,
    client: ViewServerRuntimeClient<Topics>,
    requestHealthRefresh: ViewServerKafkaHealthRefreshRequest,
    options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
    health: ViewServerKafkaHealthLedger<Topics>,
    region: string,
    batch: ReadonlyArray<KafkaConsumerMessage>,
  ) {
    const decodedMessages: Array<DecodedKafkaBatchMessage<Topics>> = [];
    for (const message of batch) {
      const decoded: Option.Option<DecodedKafkaBatchMessage<Topics>> =
        yield* decodeKafkaMessageForBatch(
          config,
          requestHealthRefresh,
          options,
          health,
          region,
          message,
        ).pipe(
          Effect.catchCause((cause) => {
            if (Cause.hasInterruptsOnly(cause)) {
              return Effect.failCause(cause);
            }
            const preserveLastErrorForSourceTopic = Option.getOrUndefined(
              Option.map(Cause.findErrorOption(cause), (error) => error.sourceTopic),
            );
            return Effect.exit(
              publishKafkaDecodedBatch(
                client,
                requestHealthRefresh,
                health,
                region,
                decodedMessages,
              ).pipe(
                Effect.andThen(
                  commitKafkaDecodedBatch(requestHealthRefresh, health, region, decodedMessages, {
                    preserveLastErrorForSourceTopic,
                  }),
                ),
              ),
            ).pipe(
              Effect.andThen((flushExit) =>
                Exit.isFailure(flushExit)
                  ? Effect.failCause(Cause.combine(cause, flushExit.cause))
                  : Effect.failCause(cause),
              ),
            );
          }),
        );
      if (Option.isSome(decoded)) {
        decodedMessages.push(decoded.value);
      }
    }
    yield* publishKafkaDecodedBatch(client, requestHealthRefresh, health, region, decodedMessages);
    yield* commitKafkaDecodedBatch(requestHealthRefresh, health, region, decodedMessages);
  },
);

export const processKafkaMessage = Effect.fn("ViewServerRuntime.kafka.message.process")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  config: ViewServerConfig<Topics>,
  client: ViewServerRuntimeClient<Topics>,
  requestHealthRefresh: ViewServerKafkaHealthRefreshRequest,
  options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
  health: ViewServerKafkaHealthLedger<Topics>,
  region: string,
  message: KafkaConsumerMessage,
) {
  yield* processKafkaMessageBatch(config, client, requestHealthRefresh, options, health, region, [
    message,
  ]);
});

const produceKafkaStreamQueueEvents = Effect.fn(
  "ViewServerRuntime.kafka.stream.produceQueueEvents",
)(function* (
  region: string,
  stream: AsyncIterable<KafkaConsumerMessage>,
  queue: Queue.Enqueue<KafkaStreamQueueEvent>,
) {
  const iterator = yield* Effect.acquireRelease(
    Effect.sync(() => stream[Symbol.asyncIterator]()),
    (currentIterator) => closeKafkaAsyncIterator(region, currentIterator),
  );
  while (true) {
    const item = yield* Effect.tryPromise({
      try: () => iterator.next(),
      catch: mapKafkaStreamError(region),
    }).pipe(
      Effect.catch((error: ViewServerKafkaIngressError) =>
        Queue.offer(queue, {
          _tag: "Failed",
          cause: Cause.fail(error),
          error,
        }).pipe(Effect.as(undefined)),
      ),
    );
    if (item === undefined) {
      return;
    }
    if (item.done === true) {
      yield* Queue.offer(queue, {
        _tag: "End",
      });
      return;
    }
    yield* Queue.offer(queue, {
      _tag: "Message",
      message: item.value,
    });
  }
});

const closeKafkaAsyncIterator = Effect.fn("ViewServerRuntime.kafka.stream.closeIterator")(
  function* (region: string, iterator: AsyncIterator<KafkaConsumerMessage>) {
    yield* Effect.tryPromise({
      try: () => Promise.resolve(iterator.return?.()),
      catch: mapKafkaStreamError(region),
    }).pipe(ignoreKafkaAsyncIteratorCloseFailure);
  },
);

export const offerKafkaStreamProducerFailure = Effect.fn(
  "ViewServerRuntime.kafka.stream.offerProducerFailure",
)(function* (
  region: string,
  queue: Queue.Enqueue<KafkaStreamQueueEvent>,
  cause: Cause.Cause<unknown>,
) {
  if (Cause.hasInterruptsOnly(cause)) {
    return yield* Effect.failCause(cause);
  }
  const error = Cause.findErrorOption(cause).pipe(
    Option.filter((value) => value instanceof ViewServerKafkaIngressError),
    Option.getOrElse(() => kafkaStreamError(region, Cause.squash(cause))),
  );
  yield* Queue.offer(queue, {
    _tag: "Failed",
    cause: kafkaIngressFailureCause(error, cause),
    error,
  });
});

const takeKafkaMessageBatch: (
  queue: Queue.Dequeue<KafkaStreamQueueEvent>,
) => Effect.Effect<KafkaMessageBatchTakeResult> = Effect.fn(
  "ViewServerRuntime.kafka.stream.takeBatch",
)(function* (queue: Queue.Dequeue<KafkaStreamQueueEvent>) {
  const first = yield* Queue.take(queue);
  if (first._tag === "End" || first._tag === "Failed") {
    return {
      _tag: "Terminal",
      terminal: first,
    };
  }
  const batch: Array<KafkaConsumerMessage> = [first.message];
  const batchStartedAt = yield* Clock.currentTimeMillis;
  const batchDeadline = batchStartedAt + kafkaMessageBatchFlushIntervalMillis;
  while (batch.length < kafkaMessageBatchSize) {
    const nowMillis = yield* Clock.currentTimeMillis;
    const remainingMillis = batchDeadline - nowMillis;
    if (remainingMillis <= 0) {
      return {
        _tag: "Batch",
        batch,
        terminal: null,
      };
    }
    const next = yield* Queue.take(queue).pipe(
      Effect.timeoutOption(Duration.millis(remainingMillis)),
    );
    if (Option.isNone(next)) {
      return {
        _tag: "Batch",
        batch,
        terminal: null,
      };
    }
    const event = next.value;
    if (event._tag === "End" || event._tag === "Failed") {
      return {
        _tag: "Batch",
        batch,
        terminal: event,
      };
    }
    batch.push(event.message);
  }
  return {
    _tag: "Batch",
    batch,
    terminal: null,
  };
});

export const runKafkaMessageStream = Effect.fn("ViewServerRuntime.kafka.stream.run")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  config: ViewServerConfig<Topics>,
  client: ViewServerRuntimeClient<Topics>,
  requestHealthRefresh: ViewServerKafkaHealthRefreshRequest,
  options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
  health: ViewServerKafkaHealthLedger<Topics>,
  region: string,
  stream: AsyncIterable<KafkaConsumerMessage>,
) {
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const queue = yield* Queue.bounded<KafkaStreamQueueEvent>(kafkaMessageQueueCapacity);
      yield* produceKafkaStreamQueueEvents(region, stream, queue).pipe(
        Effect.catchCause((cause) => offerKafkaStreamProducerFailure(region, queue, cause)),
        Effect.forkScoped({ startImmediately: true }),
      );
      while (true) {
        const next = yield* takeKafkaMessageBatch(queue);
        if (next._tag === "Terminal") {
          const terminal = next.terminal;
          if (terminal._tag === "End") {
            return;
          }
          return yield* Effect.failCause(terminal.cause);
        }
        const processExit = yield* Effect.exit(
          processKafkaMessageBatch(
            config,
            client,
            requestHealthRefresh,
            options,
            health,
            region,
            next.batch,
          ),
        );
        const terminal = next.terminal;
        if (Exit.isFailure(processExit)) {
          if (terminal?._tag === "Failed") {
            return yield* Effect.failCause(Cause.combine(processExit.cause, terminal.cause));
          }
          return yield* Effect.failCause(processExit.cause);
        }
        if (terminal?._tag === "End") {
          return;
        }
        if (terminal?._tag === "Failed") {
          return yield* Effect.failCause(terminal.cause);
        }
      }
    }),
  ).pipe(
    Effect.catchCause((cause) => {
      if (Cause.hasInterruptsOnly(cause)) {
        return Effect.failCause(kafkaNonFailureCause(cause));
      }
      const error = Cause.findErrorOption(cause);
      if (Option.isSome(error) && error.value instanceof ViewServerKafkaIngressError) {
        return recordKafkaStreamCause(
          health,
          region,
          error.value,
          kafkaIngressFailureCause(error.value, cause),
          {
            preserveTopicErrors: true,
          },
        ).pipe(Effect.ensuring(requestKafkaHealthRefresh(requestHealthRefresh)));
      }
      const streamError = kafkaStreamError(region, Cause.squash(cause));
      return recordKafkaStreamCause(
        health,
        region,
        streamError,
        kafkaIngressFailureCause(streamError, cause),
      ).pipe(Effect.ensuring(requestKafkaHealthRefresh(requestHealthRefresh)));
    }),
  );
});

export const registerKafkaConsumerHealthListeners = Effect.fn(
  "ViewServerRuntime.kafka.consumer.registerHealthListeners",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  consumer: KafkaConsumer,
  health: ViewServerKafkaHealthLedger<Topics>,
  requestHealthRefresh: ViewServerKafkaHealthRefreshRequest,
  region: string,
  topics: ReadonlyArray<string>,
  scope: Scope.Scope,
) {
  const listenersOpen = MutableRef.make(true);
  const healthEventQueue = yield* Queue.unbounded<Effect.Effect<void>>();
  const processed = yield* Ref.make<KafkaConsumerHealthListenerProcessedState>({
    count: 0,
    waiters: [],
  });
  yield* Scope.addFinalizer(
    scope,
    Effect.sync(() => {
      listenersOpen.current = false;
    }),
  );
  const markProcessed = Effect.fn("ViewServerRuntime.kafka.consumer.healthEvent.markProcessed")(
    function* () {
      const ready = yield* Ref.modify(processed, (state) => {
        const count = state.count + 1;
        const ready = state.waiters.filter((waiter) => count >= waiter.expected);
        const pending = state.waiters.filter((waiter) => count < waiter.expected);
        return [
          ready,
          {
            count,
            waiters: pending,
          },
        ];
      });
      yield* Effect.forEach(ready, (waiter) => Deferred.succeed(waiter.deferred, undefined), {
        discard: true,
      });
    },
  );
  const waitForProcessed = Effect.fn("ViewServerRuntime.kafka.consumer.healthEvent.waitProcessed")(
    function* (expected: number) {
      const deferred = yield* Deferred.make<void>();
      const shouldWait = yield* Ref.modify(processed, (state) => {
        if (state.count >= expected) {
          return [false, state];
        }
        return [
          true,
          {
            count: state.count,
            waiters: [
              ...state.waiters,
              {
                deferred,
                expected,
              },
            ],
          },
        ];
      });
      if (!shouldWait) {
        return;
      }
      yield* Deferred.await(deferred);
    },
  );
  yield* Effect.forever(
    Queue.take(healthEventQueue).pipe(
      Effect.flatMap((effect) =>
        effect.pipe(
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause) ? Effect.void : Effect.failCause(cause),
          ),
          logKafkaHealthListenerDispatchFailure,
          Effect.ensuring(markProcessed()),
        ),
      ),
    ),
  ).pipe(Effect.forkIn(scope, { startImmediately: true }));
  yield* Scope.addFinalizer(
    scope,
    Effect.gen(function* () {
      listenersOpen.current = false;
      yield* Queue.shutdown(healthEventQueue);
    }),
  );
  const enqueueHealthEvent = (effect: Effect.Effect<void>) => {
    if (listenersOpen.current) {
      Queue.offerUnsafe(healthEventQueue, effect);
    }
  };
  const groupJoinListener = (payload: ConsumerGroupJoinPayload) => {
    const assignments = snapshotKafkaAssignments(payload.assignments ?? consumer.assignments);
    enqueueHealthEvent(
      Effect.gen(function* () {
        const nowMillis = yield* Clock.currentTimeMillis;
        yield* recordKafkaAssignments(
          health,
          requestHealthRefresh,
          region,
          topics,
          assignments,
          nowMillis,
        );
      }),
    );
  };
  const lagListener = (lag: Offsets) => {
    enqueueHealthEvent(
      Effect.gen(function* () {
        const nowMillis = yield* Clock.currentTimeMillis;
        yield* recordKafkaLag(health, requestHealthRefresh, region, topics, lag, nowMillis);
      }),
    );
  };
  const groupLeaveListener = () => {
    enqueueHealthEvent(
      health
        .regionDisconnected(region, "Kafka consumer left group")
        .pipe(Effect.andThen(requestKafkaHealthRefresh(requestHealthRefresh))),
    );
  };
  const groupRebalanceListener = () => {
    enqueueHealthEvent(
      health
        .regionDisconnected(region, "Kafka consumer group rebalance in progress", {
          preserveTopicErrors: true,
        })
        .pipe(Effect.andThen(requestKafkaHealthRefresh(requestHealthRefresh))),
    );
  };
  const lagErrorListener = (error: unknown) => {
    enqueueHealthEvent(
      health
        .regionDegraded(region, messageFromUnknown(error))
        .pipe(Effect.andThen(requestKafkaHealthRefresh(requestHealthRefresh))),
    );
  };
  consumer.on("consumer:group:join", groupJoinListener);
  consumer.on("consumer:group:leave", groupLeaveListener);
  consumer.on("consumer:group:rebalance", groupRebalanceListener);
  consumer.on("consumer:lag", lagListener);
  consumer.on("consumer:lag:error", lagErrorListener);
  const registration: KafkaConsumerHealthListenerRegistration = {
    close: Effect.gen(function* () {
      yield* Effect.sync(() => {
        listenersOpen.current = false;
        consumer.off("consumer:group:join", groupJoinListener);
        consumer.off("consumer:group:leave", groupLeaveListener);
        consumer.off("consumer:group:rebalance", groupRebalanceListener);
        consumer.off("consumer:lag", lagListener);
        consumer.off("consumer:lag:error", lagErrorListener);
        consumer.stopLagMonitoring();
      });
      yield* Queue.shutdown(healthEventQueue);
    }),
    processed: Ref.get(processed).pipe(Effect.map((state) => state.count)),
    waitForProcessed,
  };
  return registration;
});

const startKafkaLagMonitoring = Effect.fn("ViewServerRuntime.kafka.consumer.startLagMonitoring")(
  function* (consumer: KafkaConsumer, topics: ReadonlyArray<string>) {
    yield* Effect.sync(() => {
      consumer.startLagMonitoring({ topics: [...topics] }, 1_000);
    });
  },
);

const startRegionConsumer = Effect.fn("ViewServerRuntime.kafka.region.start")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  config: ViewServerConfig<Topics>,
  client: ViewServerRuntimeClient<Topics>,
  requestHealthRefresh: ViewServerKafkaHealthRefreshRequest,
  options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
  health: ViewServerKafkaHealthLedger<Topics>,
  region: string,
  brokers: string,
  topics: ReadonlyArray<string>,
  scope: Scope.Scope,
) {
  const { consumer, stream, closeResources, setHealthListeners } =
    yield* acquireStartedKafkaConsumerResources(
      scope,
      makeKafkaConsumer(region, brokers, topics, options.consume),
    );
  return yield* closeKafkaConsumerOnPostConsumeStartupFailure(
    closeResources,
    Effect.gen(function* () {
      const healthListeners = yield* registerKafkaConsumerHealthListeners(
        consumer,
        health,
        requestHealthRefresh,
        region,
        topics,
        scope,
      );
      setHealthListeners(healthListeners);
      yield* startKafkaLagMonitoring(consumer, topics);
      const nowMillis = yield* Clock.currentTimeMillis;
      yield* health.regionConnected(region, nowMillis);
      yield* recordKafkaAssignments(
        health,
        requestHealthRefresh,
        region,
        topics,
        consumer.assignments,
        nowMillis,
      );
      const processStream = runKafkaMessageStream(
        config,
        client,
        requestHealthRefresh,
        options,
        health,
        region,
        stream,
      ).pipe(Effect.ensuring(closeResources.pipe(ignoreKafkaStartedResourceCloseFailure)));
      const fiber = yield* processStream.pipe(Effect.forkIn(scope, { startImmediately: true }));
      return {
        close: closeKafkaMessageStreamFiber(fiber, closeResources),
      };
    }),
  );
});

export const startKafkaRegionConsumers = Effect.fn("ViewServerRuntime.kafka.regions.start")(
  function* <E>(
    regions: Iterable<readonly [string, string]>,
    start: (region: string, brokers: string) => Effect.Effect<StartedKafkaRegionConsumer, E>,
  ) {
    const consumers: Array<StartedKafkaRegionConsumer> = [];
    yield* Effect.forEach(
      regions,
      ([region, brokers]) =>
        Effect.gen(function* () {
          const consumer = yield* start(region, brokers);
          consumers.push(consumer);
        }),
      { discard: true },
    ).pipe(
      Effect.onExit((exit) =>
        Exit.isFailure(exit) ? closeStartedKafkaRegionConsumers(consumers) : Effect.void,
      ),
    );
    return consumers;
  },
);

const makeKafkaIngressClose = (
  consumers: ReadonlyArray<StartedKafkaRegionConsumer>,
  scope: Scope.Scope,
) =>
  Ref.make(false).pipe(
    Effect.map((closed) => makeIdempotentKafkaIngressClose(consumers, scope, closed)),
  );

const makeIdempotentKafkaIngressClose = (
  consumers: ReadonlyArray<StartedKafkaRegionConsumer>,
  scope: Scope.Scope,
  closed: Ref.Ref<boolean>,
): Effect.Effect<void> => {
  const closeLock = Semaphore.makeUnsafe(1);
  return Effect.uninterruptible(
    closeLock.withPermits(1)(
      Effect.gen(function* () {
        const alreadyClosed = yield* Ref.get(closed);
        if (alreadyClosed) {
          return;
        }
        yield* closeStartedKafkaRegionConsumers(consumers).pipe(
          ignoreKafkaStartedResourceCloseFailure,
          Effect.ensuring(Scope.close(scope, Exit.void)),
        );
        yield* Ref.set(closed, true);
      }),
    ),
  );
};

export const makeScopedKafkaIngress = Effect.fn("ViewServerRuntime.kafka.ingress.makeScoped")(
  function* <E>(
    startConsumers: (
      scope: Scope.Scope,
    ) => Effect.Effect<ReadonlyArray<StartedKafkaRegionConsumer>, E>,
  ) {
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const scope = yield* Scope.make("parallel");
        const consumers = yield* restore(startConsumers(scope)).pipe(
          Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void)),
        );
        const close = yield* makeKafkaIngressClose(consumers, scope);
        return {
          close,
        };
      }),
    );
  },
);

export const makeViewServerKafkaIngress: <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  config: ViewServerConfig<Topics>,
  client: ViewServerRuntimeClient<Topics>,
  requestHealthRefresh: ViewServerKafkaHealthRefreshRequest,
  options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
  health: ViewServerKafkaHealthLedger<Topics>,
) => Effect.Effect<ViewServerKafkaIngress, ViewServerKafkaIngressError> = Effect.fn(
  "ViewServerRuntime.kafka.ingress.make",
)(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Regions extends RuntimeRegions,
>(
  config: ViewServerConfig<Topics>,
  client: ViewServerRuntimeClient<Topics>,
  requestHealthRefresh: ViewServerKafkaHealthRefreshRequest,
  options: ResolvedViewServerKafkaRuntimeOptions<Topics, Regions>,
  health: ViewServerKafkaHealthLedger<Topics>,
) {
  return yield* makeScopedKafkaIngress((scope) =>
    startKafkaRegionConsumers(Object.entries(options.regions), (region, brokers) => {
      const topics = sourceTopicsForRegion(options, region);
      if (topics.length === 0) {
        return Effect.succeed({
          close: Effect.void,
        });
      }
      return startRegionConsumer(
        config,
        client,
        requestHealthRefresh,
        options,
        health,
        region,
        brokers,
        topics,
        scope,
      );
    }),
  );
});
