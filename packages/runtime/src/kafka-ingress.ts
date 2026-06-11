import { Consumer } from "@platformatic/kafka";
import type {
  ConsumerGroupJoinPayload,
  GroupAssignment,
  Message,
  Offsets,
} from "@platformatic/kafka";
import { Buffer } from "node:buffer";
import {
  decodeKafkaTopicMessage,
  kafkaErrorIsMapping,
  type KafkaMessageMetadata,
  type ViewServerConfig,
  type ViewServerRuntimeClient,
} from "@view-server/config";
import {
  Cause,
  Clock,
  Deferred,
  Effect,
  Exit,
  Fiber,
  MutableRef,
  Option,
  Ref,
  Schema,
  Scope,
  Stream,
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
  readonly close: Effect.Effect<void>;
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
type KafkaConsumerHealthListenerWaiter = {
  readonly expected: number;
  readonly deferred: Deferred.Deferred<void>;
};
type KafkaConsumerHealthListenerProcessedState = {
  readonly count: number;
  readonly waiters: ReadonlyArray<KafkaConsumerHealthListenerWaiter>;
};

const emptyMessageBytes = Buffer.alloc(0);

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

export const sourceTopicsForRegion = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  options: ResolvedViewServerKafkaRuntimeOptions<Topics>,
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

const consumerLagMessagesFromLag = (lags: ReadonlyArray<bigint>): bigint =>
  lags.reduce((total, lag) => (lag >= 0n ? total + lag : total), 0n);

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

const refreshKafkaHealth = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  client: ViewServerRuntimeClient<Topics>,
) => client.health().pipe(Effect.ignore);

export const recordKafkaAssignments = Effect.fn(
  "ViewServerRuntime.kafka.consumer.recordAssignments",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  health: ViewServerKafkaHealthLedger<Topics>,
  client: ViewServerRuntimeClient<Topics>,
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
  yield* refreshKafkaHealth(client);
});

export const recordKafkaLag = Effect.fn("ViewServerRuntime.kafka.consumer.recordLag")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  health: ViewServerKafkaHealthLedger<Topics>,
  client: ViewServerRuntimeClient<Topics>,
  region: string,
  lag: Offsets,
  nowMillis: number,
) {
  yield* health.regionRecovered(region, nowMillis);
  yield* Effect.forEach(
    lag,
    ([sourceTopic, partitions]) =>
      health.topicLagSampled(sourceTopic, region, {
        consumerLagMessages: consumerLagMessagesFromLag(partitions),
        nowMillis,
      }),
    { discard: true },
  );
  yield* refreshKafkaHealth(client);
});

export const closeKafkaConsumerAfterStartFailure = Effect.fn(
  "ViewServerRuntime.kafka.consumer.closeAfterStartFailure",
)(function* (consumer: CloseableKafkaConsumer) {
  yield* Effect.tryPromise({
    try: () => Promise.resolve(consumer.close(true)),
    catch: kafkaConsumerCloseError,
  }).pipe(Effect.ignore);
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
  consumerGroupId: string,
) {
  const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
    autocreateTopics: false,
    bootstrapBrokers: [...bootstrapBrokers(brokers)],
    clientId: `view-server-${region}`,
    groupId: consumerGroupId,
    retries: true,
  });
  const stream = yield* closeKafkaConsumerOnStartFailure(
    consumer,
    Effect.tryPromise({
      try: () =>
        consumer.consume({
          autocommit: false,
          fallbackMode: "earliest",
          mode: "committed",
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
    yield* Effect.tryPromise({
      try: () => Promise.resolve(input.stream.close()),
      catch: kafkaStreamCloseError,
    }).pipe(Effect.ignore);
    yield* Effect.tryPromise({
      try: () => Promise.resolve(input.consumer.close(true)),
      catch: kafkaConsumerCloseError,
    }).pipe(Effect.ignore);
  },
);

export const closeStartedKafkaConsumerResources = Effect.fn(
  "ViewServerRuntime.kafka.consumer.closeStartedResources",
)(function* (resources: StartedKafkaConsumerResources) {
  const healthListeners = resources.healthListeners();
  if (healthListeners !== null) {
    yield* healthListeners.close.pipe(Effect.ignoreCause);
  }
  yield* closeKafkaConsumer({
    consumer: resources.consumer,
    stream: resources.stream,
  });
});

export const closeKafkaConsumerOnPostConsumeStartupFailure = Effect.fn(
  "ViewServerRuntime.kafka.consumer.closeOnPostConsumeStartupFailure",
)(function* <A, E>(resources: StartedKafkaConsumerResources, startup: Effect.Effect<A, E>) {
  return yield* startup.pipe(
    Effect.onExit((exit) =>
      Exit.isFailure(exit) ? closeStartedKafkaConsumerResources(resources) : Effect.void,
    ),
  );
});

export const processKafkaMessage = Effect.fn("ViewServerRuntime.kafka.message.process")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  config: ViewServerConfig<Topics>,
  client: ViewServerRuntimeClient<Topics>,
  options: ResolvedViewServerKafkaRuntimeOptions<Topics>,
  health: ViewServerKafkaHealthLedger<Topics>,
  region: string,
  message: KafkaConsumerMessage,
) {
  const sourceTopic = message.topic;
  const topic = options.topics[sourceTopic];
  if (topic === undefined) {
    return;
  }
  const nowMillis = yield* Clock.currentTimeMillis;
  const keyBytes = message.key ?? emptyMessageBytes;
  const valueBytes = message.value ?? emptyMessageBytes;
  const messageBytes = valueBytes.byteLength + keyBytes.byteLength;
  const metadata: KafkaMessageMetadata = {
    sourceTopic,
    sourceRegion: region,
    partition: message.partition,
    offset: String(message.offset),
    timestamp: Number(message.timestamp),
    headers: kafkaHeadersFromMessage(message.headers),
  };
  const refreshHealth = refreshKafkaHealth(client);
  const decoded = yield* decodeKafkaTopicMessage(topic, {
    keyBytes,
    valueBytes,
    region,
    metadata,
  }).pipe(
    Effect.matchEffect({
      onFailure: (error) => {
        if (kafkaErrorIsMapping(error)) {
          return health
            .mappingFailed(sourceTopic, region, {
              bytes: messageBytes,
              message: messageFromUnknown(error),
              nowMillis,
            })
            .pipe(
              Effect.andThen(refreshHealth),
              Effect.andThen(Effect.fail(kafkaMessageMappingError(region, sourceTopic, error))),
            );
        }
        return health
          .decodeFailed(sourceTopic, region, {
            bytes: messageBytes,
            message: messageFromUnknown(error),
            nowMillis,
          })
          .pipe(
            Effect.andThen(refreshHealth),
            Effect.andThen(Effect.fail(kafkaMessageDecodeError(region, sourceTopic, error))),
          );
      },
      onSuccess: (decodedMessage) => Effect.succeed(decodedMessage),
    }),
  );
  yield* client.publish(decoded.viewServerTopic, decoded.row).pipe(
    Effect.matchEffect({
      onFailure: (cause) =>
        health
          .messageProcessingFailed(sourceTopic, region, {
            bytes: messageBytes,
            message: messageFromUnknown(cause),
            nowMillis,
          })
          .pipe(
            Effect.andThen(refreshHealth),
            Effect.andThen(Effect.fail(kafkaMessageProcessingError(region, sourceTopic, cause))),
          ),
      onSuccess: () => Effect.succeed(true),
    }),
  );
  yield* Effect.tryPromise({
    try: () => Promise.resolve(message.commit()),
    catch: (cause) => kafkaMessageCommitError(region, sourceTopic, cause),
  });
  yield* health.messageDecoded(sourceTopic, region, {
    bytes: messageBytes,
    committedOffset: String(message.offset + 1n),
    nowMillis,
  });
});

export const runKafkaMessageStream = Effect.fn("ViewServerRuntime.kafka.stream.run")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  config: ViewServerConfig<Topics>,
  client: ViewServerRuntimeClient<Topics>,
  options: ResolvedViewServerKafkaRuntimeOptions<Topics>,
  health: ViewServerKafkaHealthLedger<Topics>,
  region: string,
  stream: AsyncIterable<KafkaConsumerMessage>,
) {
  return yield* Stream.fromAsyncIterable<KafkaConsumerMessage, ViewServerKafkaIngressError>(
    stream,
    mapKafkaStreamError(region),
  ).pipe(
    Stream.runForEach((message) =>
      processKafkaMessage(config, client, options, health, region, message),
    ),
    Effect.catchCause((cause) => {
      if (Cause.hasInterruptsOnly(cause)) {
        return Effect.failCause(cause);
      }
      const error = Cause.findErrorOption(cause);
      if (Option.isSome(error)) {
        return recordKafkaStreamError(health, region, error.value, {
          preserveTopicErrors: true,
        }).pipe(Effect.ensuring(refreshKafkaHealth(client)));
      }
      return recordKafkaStreamError(
        health,
        region,
        kafkaStreamError(region, Cause.squash(cause)),
      ).pipe(Effect.ensuring(refreshKafkaHealth(client)));
    }),
  );
});

export const registerKafkaConsumerHealthListeners = Effect.fn(
  "ViewServerRuntime.kafka.consumer.registerHealthListeners",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  consumer: KafkaConsumer,
  health: ViewServerKafkaHealthLedger<Topics>,
  client: ViewServerRuntimeClient<Topics>,
  region: string,
  topics: ReadonlyArray<string>,
  scope: Scope.Scope,
) {
  const services = yield* Effect.context<never>();
  const runFork = Effect.runForkWith(services);
  const listenersOpen = MutableRef.make(true);
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
  const runScoped = (effect: Effect.Effect<void>) => {
    if (listenersOpen.current) {
      runFork(
        effect.pipe(
          Effect.ensuring(markProcessed()),
          Effect.forkIn(scope, { startImmediately: true }),
          Effect.asVoid,
          Effect.ignoreCause,
        ),
      );
    }
  };
  const groupJoinListener = (payload: ConsumerGroupJoinPayload) => {
    runScoped(
      Effect.gen(function* () {
        const nowMillis = yield* Clock.currentTimeMillis;
        yield* recordKafkaAssignments(
          health,
          client,
          region,
          topics,
          payload.assignments ?? consumer.assignments,
          nowMillis,
        );
      }),
    );
  };
  const lagListener = (lag: Offsets) => {
    runScoped(
      Effect.gen(function* () {
        const nowMillis = yield* Clock.currentTimeMillis;
        yield* recordKafkaLag(health, client, region, lag, nowMillis);
      }),
    );
  };
  const groupLeaveListener = () => {
    runScoped(
      health
        .regionDisconnected(region, "Kafka consumer left group")
        .pipe(Effect.andThen(refreshKafkaHealth(client))),
    );
  };
  const lagErrorListener = (error: unknown) => {
    runScoped(
      health
        .regionDegraded(region, messageFromUnknown(error))
        .pipe(Effect.andThen(refreshKafkaHealth(client))),
    );
  };
  consumer.on("consumer:group:join", groupJoinListener);
  consumer.on("consumer:group:leave", groupLeaveListener);
  consumer.on("consumer:lag", lagListener);
  consumer.on("consumer:lag:error", lagErrorListener);
  const registration: KafkaConsumerHealthListenerRegistration = {
    close: Effect.sync(() => {
      listenersOpen.current = false;
      consumer.off("consumer:group:join", groupJoinListener);
      consumer.off("consumer:group:leave", groupLeaveListener);
      consumer.off("consumer:lag", lagListener);
      consumer.off("consumer:lag:error", lagErrorListener);
      consumer.stopLagMonitoring();
    }).pipe(Effect.ignoreCause),
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
>(
  config: ViewServerConfig<Topics>,
  client: ViewServerRuntimeClient<Topics>,
  options: ResolvedViewServerKafkaRuntimeOptions<Topics>,
  health: ViewServerKafkaHealthLedger<Topics>,
  region: string,
  brokers: string,
  topics: ReadonlyArray<string>,
  scope: Scope.Scope,
) {
  const { consumer, stream } = yield* makeKafkaConsumer(
    region,
    brokers,
    topics,
    options.consumerGroupId,
  );
  let healthListeners: KafkaConsumerHealthListenerRegistration | null = null;
  const resources: StartedKafkaConsumerResources = {
    consumer,
    healthListeners: () => healthListeners,
    stream,
  };
  let resourcesClosed = false;
  const closeResources = Effect.suspend(() => {
    if (resourcesClosed) {
      return Effect.void;
    }
    resourcesClosed = true;
    return closeStartedKafkaConsumerResources(resources);
  });
  return yield* closeKafkaConsumerOnPostConsumeStartupFailure(
    resources,
    Effect.gen(function* () {
      healthListeners = yield* registerKafkaConsumerHealthListeners(
        consumer,
        health,
        client,
        region,
        topics,
        scope,
      );
      yield* startKafkaLagMonitoring(consumer, topics);
      const nowMillis = yield* Clock.currentTimeMillis;
      yield* health.regionConnected(region, nowMillis);
      yield* recordKafkaAssignments(
        health,
        client,
        region,
        topics,
        consumer.assignments,
        nowMillis,
      );
      const processStream = runKafkaMessageStream(
        config,
        client,
        options,
        health,
        region,
        stream,
      ).pipe(Effect.ensuring(closeResources));
      const fiber = yield* processStream.pipe(Effect.forkIn(scope, { startImmediately: true }));
      return {
        close: Effect.all([Fiber.interrupt(fiber), closeResources], {
          concurrency: "unbounded",
          discard: true,
        }),
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
        Exit.isFailure(exit)
          ? Effect.forEach(consumers, (consumer) => consumer.close, { discard: true })
          : Effect.void,
      ),
    );
    return consumers;
  },
);

export const makeViewServerKafkaIngress: <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  client: ViewServerRuntimeClient<Topics>,
  options: ResolvedViewServerKafkaRuntimeOptions<Topics>,
  health: ViewServerKafkaHealthLedger<Topics>,
) => Effect.Effect<ViewServerKafkaIngress, ViewServerKafkaIngressError> = Effect.fn(
  "ViewServerRuntime.kafka.ingress.make",
)(function* <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  client: ViewServerRuntimeClient<Topics>,
  options: ResolvedViewServerKafkaRuntimeOptions<Topics>,
  health: ViewServerKafkaHealthLedger<Topics>,
) {
  const scope = yield* Scope.make("parallel");
  const consumers = yield* startKafkaRegionConsumers(
    Object.entries(options.regions),
    (region, brokers) => {
      const topics = sourceTopicsForRegion(options, region);
      if (topics.length === 0) {
        return Effect.succeed({
          close: Effect.void,
        });
      }
      return startRegionConsumer(config, client, options, health, region, brokers, topics, scope);
    },
  ).pipe(Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void)));
  return {
    close: Effect.forEach(consumers, (consumer) => consumer.close, {
      discard: true,
    }).pipe(Effect.ensuring(Scope.close(scope, Exit.void))),
  };
});
