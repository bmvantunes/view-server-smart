import { describe, expect, it } from "@effect/vitest";
import { Consumer } from "@platformatic/kafka";
import type { Message } from "@platformatic/kafka";
import {
  defineViewServerConfig,
  kafka,
  type ViewServerRuntimeClient,
  type ViewServerRuntimeError,
} from "@view-server/config";
import { makeViewServerRuntimeCore } from "@view-server/runtime-core";
import { Buffer } from "node:buffer";
import { Cause, Deferred, Effect, Exit, Fiber, Logger, References, Schema, Scope } from "effect";
import { makeViewServerKafkaHealthLedger } from "./kafka-health";
import type { ViewServerKafkaHealthLedger } from "./kafka-health";
import {
  assignedPartitionsForSourceTopic,
  bootstrapBrokers,
  closeKafkaConsumer,
  closeKafkaConsumerAfterStartFailure,
  closeKafkaConsumerOnPostConsumeStartupFailure,
  closeKafkaConsumerOnStartFailure,
  closeKafkaMessageStreamFiber,
  closeStartedKafkaRegionConsumers,
  closeStartedKafkaConsumerResources,
  kafkaHeadersFromMessage,
  kafkaConsumerCloseError,
  kafkaConsumerStartError,
  kafkaMessageCommitError,
  kafkaMessageDecodeError,
  kafkaMessageProcessingError,
  kafkaStreamCloseError,
  kafkaStreamError,
  makeViewServerKafkaIngress,
  makeStartedKafkaConsumerResourcesFinalizer,
  mapKafkaConsumerStartError,
  mapKafkaStreamError,
  messageFromUnknown,
  processKafkaMessage,
  recordKafkaAssignments,
  recordKafkaLag,
  recordKafkaStreamError,
  registerKafkaConsumerHealthListeners,
  runKafkaMessageStream,
  sourceTopicsForRegion,
  startKafkaRegionConsumers,
} from "./kafka-ingress";
import type {
  StartedKafkaConsumerResources,
  StartedKafkaRegionConsumer,
  ViewServerKafkaIngressError,
} from "./kafka-ingress";
import type { ResolvedViewServerKafkaRuntimeOptions } from "./runtime-options";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  price: Schema.Number,
});

const IncomingOrder = Schema.Struct({
  customerId: Schema.String,
  price: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

type Topics = typeof viewServer.topics;
type KafkaMessageBytes = Buffer | null | undefined;
type KafkaMessage = Message<KafkaMessageBytes, KafkaMessageBytes, Buffer, Buffer>;
type CapturedLog = {
  readonly cause: Cause.Cause<unknown>;
  readonly message: unknown;
};

const makeCapturedLogs = () => {
  const logs: Array<CapturedLog> = [];
  const logger = Logger.make<unknown, void>((options) => {
    logs.push({
      cause: options.cause,
      message: options.message,
    });
  });
  return { logger, logs };
};

const regions = {
  cold: "localhost:9093",
  local: " localhost:9092, ,localhost:9094 ",
};
const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
const ordersSourceTopic = "orders-source";
const unknownSourceTopic = "unknown-source";
const nonStringTagCodecError: { readonly _tag: 123; readonly message: "non-string tag" } = {
  _tag: 123,
  message: "non-string tag",
};
const forgedMappingTagCodecError: {
  readonly _tag: "KafkaMappingError";
  readonly [key: symbol]: symbol;
  readonly message: "forged mapping tag";
} = {
  _tag: "KafkaMappingError",
  [Symbol.for("@view-server/config/KafkaMappingError")]: Symbol.for(
    "@view-server/config/KafkaMappingError",
  ),
  message: "forged mapping tag",
};

const kafkaOptions: ResolvedViewServerKafkaRuntimeOptions<Topics> = {
  consumerGroupId: "view-server-test",
  regions,
  topics: {
    [ordersSourceTopic]: localKafkaTopic({
      regions: ["local"],
      value: kafka.json(IncomingOrder),
      key: kafka.stringKey(),
      viewServerTopic: "orders",
      mapping: ({ key, value }) => ({
        id: key,
        customerId: value.customerId,
        price: value.price,
      }),
    }),
  },
};

const nullRecord = <Value>(entries: Record<string, Value>): Record<string, Value> => {
  const record: Record<string, Value> = Object.create(null);
  return Object.assign(record, entries);
};

const kafkaMessage = (input: {
  readonly topic: string;
  readonly key?: string | null;
  readonly value?: string | null;
  readonly headers?: ReadonlyMap<Buffer, Buffer>;
  readonly offset?: bigint;
  readonly onCommit?: () => void;
  readonly commitFailure?: Error;
}): KafkaMessage => {
  const headers = new Map(input.headers ?? []);
  const key = input.key === undefined || input.key === null ? input.key : Buffer.from(input.key);
  const value =
    input.value === undefined || input.value === null ? input.value : Buffer.from(input.value);
  const offset = input.offset ?? 0n;
  return {
    key,
    value,
    headers,
    topic: input.topic,
    partition: 0,
    timestamp: 1_234n,
    offset,
    metadata: {},
    commit: () => {
      if (input.commitFailure !== undefined) {
        return Promise.reject(input.commitFailure);
      }
      input.onCommit?.();
      return undefined;
    },
    toJSON: () => ({
      key,
      value,
      headers: Array.from(headers.entries()),
      topic: input.topic,
      partition: 0,
      timestamp: "1234",
      offset: String(offset),
      metadata: {},
    }),
  };
};

const runtimeUnavailable: ViewServerRuntimeError = {
  _tag: "ViewServerRuntimeError",
  code: "RuntimeUnavailable",
  message: "publish failed",
};

const failingClient: ViewServerRuntimeClient<Topics> = {
  delete: () => Effect.fail(runtimeUnavailable),
  health: () => Effect.fail(runtimeUnavailable),
  patch: () => Effect.fail(runtimeUnavailable),
  publish: () => Effect.fail(runtimeUnavailable),
  publishMany: () => Effect.fail(runtimeUnavailable),
  reset: () => Effect.fail(runtimeUnavailable),
  snapshot: () => Effect.fail(runtimeUnavailable),
};

async function* failingKafkaStream(): AsyncIterable<KafkaMessage> {
  yield kafkaMessage({
    topic: ordersSourceTopic,
    key: "order-stream-1",
    value: JSON.stringify({
      customerId: "customer-stream-1",
      price: 30,
    }),
    offset: 4n,
  });
  throw new Error("stream-down");
}

async function* decodeFailureThenSuccessKafkaStream(
  onCommit: () => void,
): AsyncIterable<KafkaMessage> {
  yield kafkaMessage({
    topic: ordersSourceTopic,
    key: "bad-json",
    value: "{",
    offset: 1n,
    onCommit,
  });
  yield kafkaMessage({
    topic: ordersSourceTopic,
    key: "order-after-failure",
    value: JSON.stringify({
      customerId: "customer-after-failure",
      price: 40,
    }),
    offset: 2n,
    onCommit,
  });
}

describe("@view-server/runtime Kafka ingress internals", () => {
  it("normalizes Kafka helper values", () => {
    const headers = new Map([
      [Buffer.from("trace"), Buffer.from("abc")],
      [Buffer.from("trace"), Buffer.from("def")],
      [Buffer.from("trace"), Buffer.from("ghi")],
      [Buffer.from("__proto__"), Buffer.from("safe")],
    ]);
    const normalizedHeaders = kafkaHeadersFromMessage(headers);

    expect({
      errorMessage: messageFromUnknown(new Error("boom")),
      taggedErrorMessage: messageFromUnknown(runtimeUnavailable),
      nonStringMessage: messageFromUnknown({ message: 123 }),
      plainMessage: messageFromUnknown("plain"),
      bootstrapBrokers: bootstrapBrokers(regions.local),
      assignedOrdersPartitions: assignedPartitionsForSourceTopic(
        [{ topic: ordersSourceTopic, partitions: [0, 1] }],
        ordersSourceTopic,
      ),
      assignedMissingPartitions: assignedPartitionsForSourceTopic(
        [{ topic: ordersSourceTopic, partitions: [0, 1] }],
        unknownSourceTopic,
      ),
    }).toStrictEqual({
      errorMessage: "boom",
      taggedErrorMessage: "publish failed",
      nonStringMessage: "[object Object]",
      plainMessage: "plain",
      bootstrapBrokers: ["localhost:9092", "localhost:9094"],
      assignedOrdersPartitions: 2,
      assignedMissingPartitions: 0,
    });
    expect(Object.getPrototypeOf(normalizedHeaders)).toBe(null);
    expect(normalizedHeaders["trace"]).toStrictEqual([
      Buffer.from("abc"),
      Buffer.from("def"),
      Buffer.from("ghi"),
    ]);
    expect(normalizedHeaders["__proto__"]).toStrictEqual(Buffer.from("safe"));
    const consumerError = kafkaConsumerStartError("local", "no-broker");
    const streamError = kafkaStreamError("local", "stream-down");
    const consumerCloseError = kafkaConsumerCloseError("close-down");
    const streamCloseError = kafkaStreamCloseError("stream-close-down");
    const commitError = kafkaMessageCommitError("local", ordersSourceTopic, "commit-down");
    const decodeError = kafkaMessageDecodeError("local", ordersSourceTopic, "decode-down");
    const processingError = kafkaMessageProcessingError(
      "local",
      ordersSourceTopic,
      "processing-down",
    );
    expect({
      _tag: consumerError._tag,
      message: consumerError.message,
      cause: consumerError.cause,
      region: consumerError.region,
    }).toStrictEqual({
      _tag: "ViewServerKafkaIngressError",
      message: "Failed to start Kafka consumer for region local",
      cause: "no-broker",
      region: "local",
    });
    expect({
      _tag: mapKafkaConsumerStartError("local")("no-broker")._tag,
      message: mapKafkaConsumerStartError("local")("no-broker").message,
      cause: mapKafkaConsumerStartError("local")("no-broker").cause,
      region: mapKafkaConsumerStartError("local")("no-broker").region,
    }).toStrictEqual({
      _tag: "ViewServerKafkaIngressError",
      message: "Failed to start Kafka consumer for region local",
      cause: "no-broker",
      region: "local",
    });
    expect({
      _tag: streamError._tag,
      message: streamError.message,
      cause: streamError.cause,
      region: streamError.region,
    }).toStrictEqual({
      _tag: "ViewServerKafkaIngressError",
      message: "Kafka stream failed for region local",
      cause: "stream-down",
      region: "local",
    });
    expect({
      _tag: mapKafkaStreamError("local")("stream-down")._tag,
      message: mapKafkaStreamError("local")("stream-down").message,
      cause: mapKafkaStreamError("local")("stream-down").cause,
      region: mapKafkaStreamError("local")("stream-down").region,
    }).toStrictEqual({
      _tag: "ViewServerKafkaIngressError",
      message: "Kafka stream failed for region local",
      cause: "stream-down",
      region: "local",
    });
    expect({
      _tag: consumerCloseError._tag,
      message: consumerCloseError.message,
      cause: consumerCloseError.cause,
      region: consumerCloseError.region,
    }).toStrictEqual({
      _tag: "ViewServerKafkaIngressError",
      message: "Failed to close Kafka consumer",
      cause: "close-down",
      region: undefined,
    });
    expect({
      _tag: streamCloseError._tag,
      message: streamCloseError.message,
      cause: streamCloseError.cause,
      region: streamCloseError.region,
    }).toStrictEqual({
      _tag: "ViewServerKafkaIngressError",
      message: "Failed to close Kafka stream",
      cause: "stream-close-down",
      region: undefined,
    });
    expect({
      _tag: commitError._tag,
      message: commitError.message,
      cause: commitError.cause,
      region: commitError.region,
      sourceTopic: commitError.sourceTopic,
    }).toStrictEqual({
      _tag: "ViewServerKafkaIngressError",
      message: `Failed to commit Kafka message for source topic ${ordersSourceTopic}`,
      cause: "commit-down",
      region: "local",
      sourceTopic: ordersSourceTopic,
    });
    expect({
      decode: {
        _tag: decodeError._tag,
        message: decodeError.message,
        cause: decodeError.cause,
        region: decodeError.region,
        sourceTopic: decodeError.sourceTopic,
      },
      processing: {
        _tag: processingError._tag,
        message: processingError.message,
        cause: processingError.cause,
        region: processingError.region,
        sourceTopic: processingError.sourceTopic,
      },
    }).toStrictEqual({
      decode: {
        _tag: "ViewServerKafkaIngressError",
        message: `Failed to decode Kafka message for source topic ${ordersSourceTopic}`,
        cause: "decode-down",
        region: "local",
        sourceTopic: ordersSourceTopic,
      },
      processing: {
        _tag: "ViewServerKafkaIngressError",
        message: `Failed to process Kafka message for source topic ${ordersSourceTopic}`,
        cause: "processing-down",
        region: "local",
        sourceTopic: ordersSourceTopic,
      },
    });
    expect(sourceTopicsForRegion(kafkaOptions, "local")).toStrictEqual([ordersSourceTopic]);
    expect(sourceTopicsForRegion(kafkaOptions, "cold")).toStrictEqual([]);
  });

  it.effect("closes constructed Kafka consumers after consume startup failures", () =>
    Effect.gen(function* () {
      let closeForce: boolean | undefined = undefined;

      yield* closeKafkaConsumerAfterStartFailure({
        close: (force) => {
          closeForce = force;
        },
      });

      expect(closeForce).toBe(true);
    }),
  );

  it.effect("closes Kafka consumers when startup effects fail or are interrupted", () =>
    Effect.gen(function* () {
      let failedCloseForce: boolean | undefined = undefined;
      const failedExit = yield* Effect.exit(
        closeKafkaConsumerOnStartFailure(
          {
            close: (force) => {
              failedCloseForce = force;
            },
          },
          Effect.fail(kafkaConsumerStartError("local", "no-broker")),
        ),
      );
      let interruptedCloseForce: boolean | undefined = undefined;
      const interruptedExit = yield* Effect.exit(
        closeKafkaConsumerOnStartFailure(
          {
            close: (force) => {
              interruptedCloseForce = force;
            },
          },
          Effect.interrupt,
        ),
      );

      expect({
        failed: Exit.isFailure(failedExit),
        failedCloseForce,
        interrupted: Exit.hasInterrupts(interruptedExit),
        interruptedCloseForce,
      }).toStrictEqual({
        failed: true,
        failedCloseForce: true,
        interrupted: true,
        interruptedCloseForce: true,
      });
    }),
  );

  it.effect("closes stream and consumer resources", () =>
    Effect.gen(function* () {
      let closeForce: boolean | undefined = undefined;
      let streamCloseCount = 0;

      yield* closeKafkaConsumer({
        consumer: {
          close: (force) => {
            closeForce = force;
          },
        },
        stream: {
          close: () => {
            streamCloseCount += 1;
          },
        },
      });

      expect(streamCloseCount).toBe(1);
      expect(closeForce).toBe(true);
    }),
  );

  it.effect("closes Kafka consumers even when stream close fails", () =>
    Effect.gen(function* () {
      let closeForce: boolean | undefined = undefined;
      let streamCloseCount = 0;

      const closeExit = yield* Effect.exit(
        closeKafkaConsumer({
          consumer: {
            close: (force) => {
              closeForce = force;
            },
          },
          stream: {
            close: () => {
              streamCloseCount += 1;
              throw new Error("stream close failed");
            },
          },
        }),
      );

      expect({
        closeForce,
        closeFailurePreserved: Exit.isFailure(closeExit),
        streamCloseCount,
      }).toStrictEqual({
        closeForce: true,
        closeFailurePreserved: true,
        streamCloseCount: 1,
      });
    }),
  );

  it.effect("closes started Kafka resources with and without health listeners", () =>
    Effect.gen(function* () {
      let closeForce: boolean | undefined = undefined;
      let streamCloseCount = 0;
      let listenerCloseCount = 0;
      const resourcesWithListeners: StartedKafkaConsumerResources = {
        consumer: {
          close: (force) => {
            closeForce = force;
          },
        },
        stream: {
          close: () => {
            streamCloseCount += 1;
          },
        },
        healthListeners: () => ({
          close: Effect.sync(() => {
            listenerCloseCount += 1;
          }),
          processed: Effect.succeed(0),
          waitForProcessed: () => Effect.void,
        }),
      };
      const resourcesWithoutListeners: StartedKafkaConsumerResources = {
        consumer: {
          close: (force) => {
            closeForce = force;
          },
        },
        stream: {
          close: () => {
            streamCloseCount += 1;
          },
        },
        healthListeners: () => null,
      };

      yield* closeStartedKafkaConsumerResources(resourcesWithListeners);
      yield* closeStartedKafkaConsumerResources(resourcesWithoutListeners);

      expect(streamCloseCount).toBe(2);
      expect(listenerCloseCount).toBe(1);
      expect(closeForce).toBe(true);
    }),
  );

  it.effect("closes started Kafka resources when health listener cleanup defects", () =>
    Effect.gen(function* () {
      let closeForce: boolean | undefined = undefined;
      let streamCloseCount = 0;
      const resources: StartedKafkaConsumerResources = {
        consumer: {
          close: (force) => {
            closeForce = force;
          },
        },
        stream: {
          close: () => {
            streamCloseCount += 1;
          },
        },
        healthListeners: () => ({
          close: Effect.die(new Error("listener close failed")),
          processed: Effect.succeed(0),
          waitForProcessed: () => Effect.void,
        }),
      };

      const exit = yield* Effect.exit(closeStartedKafkaConsumerResources(resources));

      expect({
        closeForce,
        defectPreserved: Exit.hasDies(exit),
        failed: Exit.isFailure(exit),
        interrupted: Exit.hasInterrupts(exit),
        streamCloseCount,
      }).toStrictEqual({
        closeForce: true,
        defectPreserved: true,
        failed: true,
        interrupted: false,
        streamCloseCount: 1,
      });
    }),
  );

  it.effect("retries started Kafka resource cleanup after a defecting close attempt", () =>
    Effect.gen(function* () {
      let closeForce: boolean | undefined = undefined;
      let listenerCloseCount = 0;
      let streamCloseCount = 0;
      let listenerClose: Effect.Effect<void> = Effect.die(new Error("listener close failed"));
      const resources: StartedKafkaConsumerResources = {
        consumer: {
          close: (force) => {
            closeForce = force;
          },
        },
        stream: {
          close: () => {
            streamCloseCount += 1;
          },
        },
        healthListeners: () => ({
          close: Effect.suspend(() => {
            listenerCloseCount += 1;
            const close = listenerClose;
            listenerClose = Effect.void;
            return close;
          }),
          processed: Effect.succeed(0),
          waitForProcessed: () => Effect.void,
        }),
      };
      const finalizer = yield* makeStartedKafkaConsumerResourcesFinalizer(resources);
      const firstExit = yield* Effect.exit(finalizer);
      yield* finalizer;
      yield* finalizer;

      expect({
        closeForce,
        firstAttemptDefected: Exit.hasDies(firstExit),
        listenerCloseCount,
        streamCloseCount,
      }).toStrictEqual({
        closeForce: true,
        firstAttemptDefected: true,
        listenerCloseCount: 2,
        streamCloseCount: 2,
      });
    }),
  );

  it.effect("requests Kafka stream interruption before closing resources", () =>
    Effect.gen(function* () {
      let closeResourcesCount = 0;
      let streamFinalizerCount = 0;
      const interruptStarted = yield* Deferred.make<void>();
      const releaseStreamFinalizer = yield* Deferred.make<void>();
      const resourceCloseObservedInterrupt = yield* Deferred.make<boolean>();
      const streamFiber = yield* Effect.never.pipe(
        Effect.onInterrupt(() => Deferred.succeed(interruptStarted, undefined)),
        Effect.ensuring(
          Deferred.await(releaseStreamFinalizer).pipe(
            Effect.andThen(
              Effect.sync(() => {
                streamFinalizerCount += 1;
              }),
            ),
          ),
        ),
        Effect.forkChild({ startImmediately: true }),
      );

      yield* closeKafkaMessageStreamFiber(
        streamFiber,
        Effect.gen(function* () {
          closeResourcesCount += 1;
          yield* Deferred.await(interruptStarted);
          yield* Deferred.succeed(resourceCloseObservedInterrupt, true);
          yield* Deferred.succeed(releaseStreamFinalizer, undefined);
        }),
      );

      expect({
        closeResourcesCount,
        resourceCloseObservedInterrupt: yield* Deferred.await(resourceCloseObservedInterrupt),
        streamFinalizerCount,
      }).toStrictEqual({
        closeResourcesCount: 1,
        resourceCloseObservedInterrupt: true,
        streamFinalizerCount: 1,
      });
    }),
  );

  it.effect("closes Kafka resources to unblock a pending stream before awaiting finalizers", () =>
    Effect.gen(function* () {
      let closeResourcesCount = 0;
      let streamFinalizerCount = 0;
      const unblockPendingStreamRead = yield* Deferred.make<void>();
      const streamFiber = yield* Deferred.await(unblockPendingStreamRead).pipe(
        Effect.uninterruptible,
        Effect.ensuring(
          Effect.sync(() => {
            streamFinalizerCount += 1;
          }),
        ),
        Effect.forkChild({ startImmediately: true }),
      );

      yield* closeKafkaMessageStreamFiber(
        streamFiber,
        Effect.gen(function* () {
          closeResourcesCount += 1;
          yield* Deferred.succeed(unblockPendingStreamRead, undefined);
        }),
      ).pipe(Effect.timeout("1 second"));

      expect({
        closeResourcesCount,
        streamFinalizerCount,
      }).toStrictEqual({
        closeResourcesCount: 1,
        streamFinalizerCount: 1,
      });
    }),
  );

  it.effect(
    "continues Kafka stream cleanup when close is interrupted during resource cleanup",
    () =>
      Effect.gen(function* () {
        let closeResourcesCount = 0;
        let streamFinalizerCount = 0;
        const closeResourcesStarted = yield* Deferred.make<void>();
        const releaseCloseResources = yield* Deferred.make<void>();
        const unblockPendingStreamRead = yield* Deferred.make<void>();
        const streamFiber = yield* Deferred.await(unblockPendingStreamRead).pipe(
          Effect.uninterruptible,
          Effect.ensuring(
            Effect.sync(() => {
              streamFinalizerCount += 1;
            }),
          ),
          Effect.forkChild({ startImmediately: true }),
        );
        const closeFiber = yield* closeKafkaMessageStreamFiber(
          streamFiber,
          Effect.gen(function* () {
            closeResourcesCount += 1;
            yield* Deferred.succeed(closeResourcesStarted, undefined);
            yield* Deferred.await(releaseCloseResources);
            yield* Deferred.succeed(unblockPendingStreamRead, undefined);
          }),
        ).pipe(Effect.forkChild({ startImmediately: true }));

        yield* Deferred.await(closeResourcesStarted).pipe(Effect.timeout("1 second"));
        const interruptCloseFiber = yield* Fiber.interrupt(closeFiber).pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Deferred.succeed(releaseCloseResources, undefined);
        yield* Fiber.join(interruptCloseFiber);

        expect({
          closeResourcesCount,
          streamFinalizerCount,
        }).toStrictEqual({
          closeResourcesCount: 1,
          streamFinalizerCount: 1,
        });
      }),
  );

  it.effect("waits for Kafka stream fiber finalizers when resource cleanup defects", () =>
    Effect.gen(function* () {
      let closeResourcesCount = 0;
      let streamFinalizerCount = 0;
      const streamFiber = yield* Effect.never.pipe(
        Effect.ensuring(
          Effect.sync(() => {
            streamFinalizerCount += 1;
          }),
        ),
        Effect.forkChild({ startImmediately: true }),
      );

      const exit = yield* Effect.exit(
        closeKafkaMessageStreamFiber(
          streamFiber,
          Effect.gen(function* () {
            closeResourcesCount += 1;
            return yield* Effect.die(new Error("resource close failed"));
          }),
        ),
      );

      expect({
        closeResourcesCount,
        defectPreserved: Exit.hasDies(exit),
        streamFinalizerCount,
      }).toStrictEqual({
        closeResourcesCount: 1,
        defectPreserved: true,
        streamFinalizerCount: 1,
      });
    }),
  );

  it.effect("preserves Kafka stream fiber finalizer defects during close", () =>
    Effect.gen(function* () {
      let closeResourcesCount = 0;
      let streamFinalizerCount = 0;
      const streamFiber = yield* Effect.never.pipe(
        Effect.ensuring(
          Effect.gen(function* () {
            streamFinalizerCount += 1;
            return yield* Effect.die(new Error("stream finalizer failed"));
          }),
        ),
        Effect.forkChild({ startImmediately: true }),
      );

      const exit = yield* Effect.exit(
        closeKafkaMessageStreamFiber(
          streamFiber,
          Effect.sync(() => {
            closeResourcesCount += 1;
          }),
        ),
      );

      expect({
        closeResourcesCount,
        defectPreserved: Exit.hasDies(exit),
        streamFinalizerCount,
      }).toStrictEqual({
        closeResourcesCount: 1,
        defectPreserved: true,
        streamFinalizerCount: 1,
      });
    }),
  );

  it.effect("closes all started region consumers before returning close defects", () =>
    Effect.gen(function* () {
      const closed: Array<string> = [];
      const closeExit = yield* Effect.exit(
        closeStartedKafkaRegionConsumers([
          {
            close: Effect.gen(function* () {
              closed.push("first");
              return yield* Effect.die(new Error("first close failed"));
            }),
          },
          {
            close: Effect.sync(() => {
              closed.push("second");
            }),
          },
          {
            close: Effect.sync(() => {
              closed.push("third");
            }),
          },
        ]),
      );

      expect({
        closed,
        defectPreserved: Exit.hasDies(closeExit),
      }).toStrictEqual({
        closed: ["first", "second", "third"],
        defectPreserved: true,
      });
    }),
  );

  it.effect("cleans post-consume Kafka resources only when later startup fails", () =>
    Effect.gen(function* () {
      let successStreamCloseCount = 0;
      let failedStreamCloseCount = 0;
      let failedListenerCloseCount = 0;
      let successCloseForce: boolean | undefined = undefined;
      let failedCloseForce: boolean | undefined = undefined;
      const successResources: StartedKafkaConsumerResources = {
        consumer: {
          close: (force) => {
            successCloseForce = force;
          },
        },
        stream: {
          close: () => {
            successStreamCloseCount += 1;
          },
        },
        healthListeners: () => null,
      };
      const failedResources: StartedKafkaConsumerResources = {
        consumer: {
          close: (force) => {
            failedCloseForce = force;
          },
        },
        stream: {
          close: () => {
            failedStreamCloseCount += 1;
          },
        },
        healthListeners: () => ({
          close: Effect.sync(() => {
            failedListenerCloseCount += 1;
          }),
          processed: Effect.succeed(0),
          waitForProcessed: () => Effect.void,
        }),
      };

      const success = yield* closeKafkaConsumerOnPostConsumeStartupFailure(
        successResources,
        Effect.succeed("started"),
      );
      const failedExit = yield* Effect.exit(
        closeKafkaConsumerOnPostConsumeStartupFailure(
          failedResources,
          Effect.fail(kafkaConsumerStartError("local", "post-consume-down")),
        ),
      );

      expect(success).toBe("started");
      expect(successStreamCloseCount).toBe(0);
      expect(successCloseForce).toBe(undefined);
      expect(Exit.isFailure(failedExit)).toBe(true);
      expect(failedStreamCloseCount).toBe(1);
      expect(failedListenerCloseCount).toBe(1);
      expect(failedCloseForce).toBe(true);
    }),
  );

  it.effect("records Kafka health transitions and ignores unknown ledger keys", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });

      yield* ledger.regionRecovered("local", 1_000);
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 2, 1_000);
      yield* ledger.messageDecoded(ordersSourceTopic, "local", {
        bytes: 10,
        committedOffset: "1",
        nowMillis: 1_000,
      });
      yield* ledger.messageDecoded(ordersSourceTopic, "local", {
        bytes: 20,
        committedOffset: "2",
        nowMillis: 2_000,
      });
      yield* ledger.decodeFailed(ordersSourceTopic, "local", {
        bytes: 5,
        message: "bad-json",
        nowMillis: 2_000,
      });
      yield* ledger.regionConnected("missing", 2_000);
      yield* ledger.regionDegraded("missing", "ignored");
      yield* ledger.regionDegraded("local", "lag monitor failed");
      yield* ledger.regionRecovered("local", 2_000);
      yield* ledger.regionDisconnected("local", "lost");
      yield* ledger.regionRecovered("local", 2_000);
      yield* ledger.regionDisconnected("missing", "ignored");
      yield* ledger.regionRecovered("missing", 2_000);
      yield* ledger.topicConnected("missing", "local", 1, 2_000);
      yield* ledger.messageDecoded("missing", "local", {
        bytes: 1,
        committedOffset: "3",
        nowMillis: 2_000,
      });
      yield* ledger.decodeFailed("missing", "local", {
        bytes: 1,
        message: "ignored",
        nowMillis: 2_000,
      });
      yield* ledger.mappingFailed("missing", "local", {
        bytes: 1,
        message: "ignored",
        nowMillis: 2_000,
      });
      yield* ledger.messageProcessingFailed("missing", "local", {
        bytes: 1,
        message: "ignored",
        nowMillis: 2_000,
      });

      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect({
        status: health.status,
        kafka: health.kafka,
      }).toStrictEqual({
        status: "degraded",
        kafka: {
          regions: nullRecord({
            local: {
              status: "disconnected",
              brokers: regions.local,
              lastConnectedAt: 1_000,
              lastError: "lost",
            },
          }),
          topics: nullRecord({
            [ordersSourceTopic]: {
              status: "degraded",
              sourceTopic: ordersSourceTopic,
              viewServerTopic: "orders",
              regions: nullRecord({
                local: {
                  connected: false,
                  assignedPartitions: 2,
                  messagesPerSecond: 2,
                  bytesPerSecond: 25,
                  decodedMessagesPerSecond: 1,
                  decodeFailuresPerSecond: 1,
                  mappingFailuresPerSecond: 0,
                  processingFailuresPerSecond: 0,
                  lastMessageAt: 2_000,
                  lastCommitAt: 2_000,
                  consumerLagMessages: null,
                  lagSampledAt: null,
                  committedOffset: "2",
                  lastError: "lost",
                },
              }),
            },
          }),
        },
      });

      const splitRegionLedger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
          [unknownSourceTopic]: {
            regions: ["cold"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* splitRegionLedger.regionRecovered("local", 3_000);
      const splitRegionHealth = splitRegionLedger.healthOverlay(
        yield* runtimeCore.client.health(),
        3_000,
      );
      expect(splitRegionHealth.kafka).toStrictEqual({
        regions: nullRecord({
          cold: {
            status: "starting",
            brokers: regions.cold,
            lastConnectedAt: null,
            lastError: null,
          },
          local: {
            status: "connected",
            brokers: regions.local,
            lastConnectedAt: 3_000,
            lastError: null,
          },
        }),
        topics: nullRecord({
          [ordersSourceTopic]: {
            status: "starting",
            sourceTopic: ordersSourceTopic,
            viewServerTopic: "orders",
            regions: nullRecord({
              local: {
                connected: false,
                assignedPartitions: 0,
                messagesPerSecond: 0,
                bytesPerSecond: 0,
                decodedMessagesPerSecond: 0,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: null,
                lastCommitAt: null,
                consumerLagMessages: null,
                lagSampledAt: null,
                committedOffset: null,
                lastError: null,
              },
            }),
          },
          [unknownSourceTopic]: {
            status: "starting",
            sourceTopic: unknownSourceTopic,
            viewServerTopic: "orders",
            regions: nullRecord({
              cold: {
                connected: false,
                assignedPartitions: 0,
                messagesPerSecond: 0,
                bytesPerSecond: 0,
                decodedMessagesPerSecond: 0,
                decodeFailuresPerSecond: 0,
                mappingFailuresPerSecond: 0,
                processingFailuresPerSecond: 0,
                lastMessageAt: null,
                lastCommitAt: null,
                consumerLagMessages: null,
                lagSampledAt: null,
                committedOffset: null,
                lastError: null,
              },
            }),
          },
        }),
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("records Kafka assignments and lag samples", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      let healthRefreshRequestCount = 0;
      const requestHealthRefresh = Effect.sync(() => {
        healthRefreshRequestCount += 1;
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* recordKafkaAssignments(
        ledger,
        requestHealthRefresh,
        "local",
        [ordersSourceTopic],
        [{ topic: ordersSourceTopic, partitions: [0, 1] }],
        1_000,
      );
      yield* recordKafkaLag(
        ledger,
        requestHealthRefresh,
        "local",
        new Map([
          [ordersSourceTopic, [3n, -1n, 2n]],
          [unknownSourceTopic, [99n]],
        ]),
        2_000,
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect(healthRefreshRequestCount).toBe(2);
      expect(health.kafka?.topics[ordersSourceTopic]).toStrictEqual({
        status: "ready",
        sourceTopic: ordersSourceTopic,
        viewServerTopic: "orders",
        regions: nullRecord({
          local: {
            connected: true,
            assignedPartitions: 2,
            messagesPerSecond: 0,
            bytesPerSecond: 0,
            decodedMessagesPerSecond: 0,
            decodeFailuresPerSecond: 0,
            mappingFailuresPerSecond: 0,
            processingFailuresPerSecond: 0,
            lastMessageAt: null,
            lastCommitAt: null,
            consumerLagMessages: 5n,
            lagSampledAt: 2_000,
            committedOffset: null,
            lastError: null,
          },
        }),
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("records Kafka health from consumer listener callbacks", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      let healthRefreshRequestCount = 0;
      const requestHealthRefresh = Effect.sync(() => {
        healthRefreshRequestCount += 1;
      });
      const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
        bootstrapBrokers: ["127.0.0.1:1"],
        clientId: "view-server-listener-test",
        groupId: "view-server-listener-test",
      });
      const scope = yield* Scope.make("parallel");
      yield* ledger.regionConnected("local", 1_000);
      const listenerRegistration = yield* registerKafkaConsumerHealthListeners(
        consumer,
        ledger,
        requestHealthRefresh,
        "local",
        [ordersSourceTopic],
        scope,
      );
      yield* listenerRegistration.waitForProcessed(0);

      const degradedWait = yield* listenerRegistration
        .waitForProcessed(5)
        .pipe(Effect.forkChild({ startImmediately: true }));
      consumer.emit("consumer:group:join", {
        groupId: "view-server-listener-test",
        memberId: "member-1",
      });
      consumer.emit("consumer:group:join", {
        groupId: "view-server-listener-test",
        memberId: "member-1",
        assignments: [{ topic: ordersSourceTopic, partitions: [0, 1] }],
      });
      consumer.emit("consumer:lag", new Map([[ordersSourceTopic, [4n, 1n]]]));
      consumer.emit("consumer:group:leave", {
        groupId: "view-server-listener-test",
        memberId: "member-1",
      });
      consumer.emit("consumer:lag:error", new Error("lag read failed"));
      yield* Fiber.join(degradedWait);
      const degradedProcessed = yield* listenerRegistration.processed;
      const degradedHealth = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        healthRefreshRequestCount,
        processed: degradedProcessed,
      }).toStrictEqual({
        healthRefreshRequestCount: 5,
        processed: 5,
      });
      expect({
        region: degradedHealth.kafka?.regions["local"],
        topicStatus: degradedHealth.kafka?.topics[ordersSourceTopic]?.status,
        topicRegion: degradedHealth.kafka?.topics[ordersSourceTopic]?.regions["local"],
      }).toStrictEqual({
        region: {
          status: "degraded",
          brokers: regions.local,
          lastConnectedAt: expect.any(Number),
          lastError: "lag read failed",
        },
        topicStatus: "degraded",
        topicRegion: {
          connected: false,
          assignedPartitions: 2,
          messagesPerSecond: 0,
          bytesPerSecond: 0,
          decodedMessagesPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          processingFailuresPerSecond: 0,
          lastMessageAt: null,
          lastCommitAt: null,
          consumerLagMessages: 5n,
          lagSampledAt: expect.any(Number),
          committedOffset: null,
          lastError: "lag read failed",
        },
      });

      const recoveredWait = yield* listenerRegistration
        .waitForProcessed(7)
        .pipe(Effect.forkChild({ startImmediately: true }));
      consumer.emit("consumer:group:join", {
        groupId: "view-server-listener-test",
        memberId: "member-1",
        assignments: [{ topic: ordersSourceTopic, partitions: [0, 1] }],
      });
      consumer.emit("consumer:lag", new Map([[ordersSourceTopic, [0n, 0n]]]));
      yield* Fiber.join(recoveredWait);
      const recoveredProcessed = yield* listenerRegistration.processed;
      const recoveredHealth = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        healthRefreshRequestCount,
        processed: recoveredProcessed,
      }).toStrictEqual({
        healthRefreshRequestCount: 7,
        processed: 7,
      });
      expect({
        region: recoveredHealth.kafka?.regions["local"],
        topicStatus: recoveredHealth.kafka?.topics[ordersSourceTopic]?.status,
        topicRegion: recoveredHealth.kafka?.topics[ordersSourceTopic]?.regions["local"],
      }).toStrictEqual({
        region: {
          status: "connected",
          brokers: regions.local,
          lastConnectedAt: expect.any(Number),
          lastError: null,
        },
        topicStatus: "ready",
        topicRegion: {
          connected: true,
          assignedPartitions: 2,
          messagesPerSecond: 0,
          bytesPerSecond: 0,
          decodedMessagesPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          processingFailuresPerSecond: 0,
          lastMessageAt: null,
          lastCommitAt: null,
          consumerLagMessages: 0n,
          lagSampledAt: expect.any(Number),
          committedOffset: null,
          lastError: null,
        },
      });

      yield* listenerRegistration.close;
      yield* Scope.close(scope, Exit.void);
      yield* Effect.promise(() => Promise.resolve(consumer.close(true)));
      yield* runtimeCore.close;
    }),
  );

  it.effect("logs Kafka listener callback failures after applying ledger updates", () => {
    const { logger, logs } = makeCapturedLogs();

    return Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      const failingLedger: ViewServerKafkaHealthLedger<Topics> = {
        ...ledger,
        topicConnected: (sourceTopic, region, assignedPartitions, nowMillis) =>
          ledger
            .topicConnected(sourceTopic, region, assignedPartitions, nowMillis)
            .pipe(Effect.andThen(Effect.die(new Error("listener ledger defect")))),
      };
      const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
        bootstrapBrokers: ["127.0.0.1:1"],
        clientId: "view-server-listener-failure-test",
        groupId: "view-server-listener-failure-test",
      });
      const scope = yield* Scope.make("parallel");
      const listenerRegistration = yield* registerKafkaConsumerHealthListeners(
        consumer,
        failingLedger,
        runtimeCore.requestHealthRefresh,
        "local",
        [ordersSourceTopic],
        scope,
      );
      const processedWait = yield* listenerRegistration
        .waitForProcessed(1)
        .pipe(Effect.forkChild({ startImmediately: true }));

      consumer.emit("consumer:group:join", {
        groupId: "view-server-listener-failure-test",
        memberId: "member-1",
        assignments: [{ topic: ordersSourceTopic, partitions: [0] }],
      });
      yield* Fiber.join(processedWait);
      const processed = yield* listenerRegistration.processed;
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);
      const log = logs[0];

      expect({
        logCauseHasDefect: Cause.hasDies(log?.cause ?? Cause.empty),
        logMessage: log?.message,
        logCount: logs.length,
        processed,
        region: health.kafka?.regions["local"],
        topicRegion: health.kafka?.topics[ordersSourceTopic]?.regions["local"],
      }).toStrictEqual({
        logCauseHasDefect: true,
        logMessage: ["Kafka health listener dispatch failed."],
        logCount: 1,
        processed: 1,
        region: {
          status: "connected",
          brokers: regions.local,
          lastConnectedAt: expect.any(Number),
          lastError: null,
        },
        topicRegion: {
          connected: true,
          assignedPartitions: 1,
          messagesPerSecond: 0,
          bytesPerSecond: 0,
          decodedMessagesPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          processingFailuresPerSecond: 0,
          lastMessageAt: null,
          lastCommitAt: null,
          consumerLagMessages: null,
          lagSampledAt: null,
          committedOffset: null,
          lastError: null,
        },
      });

      yield* listenerRegistration.close;
      yield* Scope.close(scope, Exit.void);
      yield* Effect.promise(() => Promise.resolve(consumer.close(true)));
      yield* runtimeCore.close;
    }).pipe(
      Effect.provide(Logger.layer([logger])),
      Effect.provideService(References.MinimumLogLevel, "Trace"),
    );
  });

  it.effect("does not log pure Kafka listener interruptions", () => {
    const { logger, logs } = makeCapturedLogs();

    return Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      const interruptingLedger: ViewServerKafkaHealthLedger<Topics> = {
        ...ledger,
        topicConnected: () => Effect.interrupt,
      };
      const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
        bootstrapBrokers: ["127.0.0.1:1"],
        clientId: "view-server-listener-interrupt-test",
        groupId: "view-server-listener-interrupt-test",
      });
      const scope = yield* Scope.make("parallel");
      const listenerRegistration = yield* registerKafkaConsumerHealthListeners(
        consumer,
        interruptingLedger,
        runtimeCore.requestHealthRefresh,
        "local",
        [ordersSourceTopic],
        scope,
      );
      const processedWait = yield* listenerRegistration
        .waitForProcessed(1)
        .pipe(Effect.forkChild({ startImmediately: true }));

      consumer.emit("consumer:group:join", {
        groupId: "view-server-listener-interrupt-test",
        memberId: "member-1",
        assignments: [{ topic: ordersSourceTopic, partitions: [0] }],
      });
      yield* Fiber.join(processedWait);
      const processed = yield* listenerRegistration.processed;

      expect({
        logCount: logs.length,
        processed,
      }).toStrictEqual({
        logCount: 0,
        processed: 1,
      });

      yield* listenerRegistration.close;
      yield* Scope.close(scope, Exit.void);
      yield* Effect.promise(() => Promise.resolve(consumer.close(true)));
      yield* runtimeCore.close;
    }).pipe(
      Effect.provide(Logger.layer([logger])),
      Effect.provideService(References.MinimumLogLevel, "Trace"),
    );
  });

  it.effect("reports Kafka overlay ready and starting runtime statuses", () =>
    Effect.gen(function* () {
      const readyRuntimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const readyLedger = makeViewServerKafkaHealthLedger<Topics>({
        regions: {
          local: regions.local,
        },
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* readyLedger.regionConnected("local", 1_000);
      yield* readyLedger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      const readyHealth = readyLedger.healthOverlay(yield* readyRuntimeCore.client.health(), 1_000);

      const startingRuntimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const startingLedger = makeViewServerKafkaHealthLedger<Topics>({
        regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local", "cold"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* startingLedger.regionConnected("local", 2_000);
      yield* startingLedger.topicConnected(ordersSourceTopic, "local", 1, 2_000);
      const startingHealth = startingLedger.healthOverlay(
        yield* startingRuntimeCore.client.health(),
        2_000,
      );

      expect({
        ready: {
          status: readyHealth.status,
          kafka: readyHealth.kafka,
        },
        starting: {
          status: startingHealth.status,
          kafka: startingHealth.kafka,
        },
      }).toStrictEqual({
        ready: {
          status: "ready",
          kafka: {
            regions: nullRecord({
              local: {
                status: "connected",
                brokers: regions.local,
                lastConnectedAt: 1_000,
                lastError: null,
              },
            }),
            topics: nullRecord({
              [ordersSourceTopic]: {
                status: "ready",
                sourceTopic: ordersSourceTopic,
                viewServerTopic: "orders",
                regions: nullRecord({
                  local: {
                    connected: true,
                    assignedPartitions: 1,
                    messagesPerSecond: 0,
                    bytesPerSecond: 0,
                    decodedMessagesPerSecond: 0,
                    decodeFailuresPerSecond: 0,
                    mappingFailuresPerSecond: 0,
                    processingFailuresPerSecond: 0,
                    lastMessageAt: null,
                    lastCommitAt: null,
                    consumerLagMessages: null,
                    lagSampledAt: null,
                    committedOffset: null,
                    lastError: null,
                  },
                }),
              },
            }),
          },
        },
        starting: {
          status: "starting",
          kafka: {
            regions: nullRecord({
              cold: {
                status: "starting",
                brokers: regions.cold,
                lastConnectedAt: null,
                lastError: null,
              },
              local: {
                status: "connected",
                brokers: regions.local,
                lastConnectedAt: 2_000,
                lastError: null,
              },
            }),
            topics: nullRecord({
              [ordersSourceTopic]: {
                status: "starting",
                sourceTopic: ordersSourceTopic,
                viewServerTopic: "orders",
                regions: nullRecord({
                  cold: {
                    connected: false,
                    assignedPartitions: 0,
                    messagesPerSecond: 0,
                    bytesPerSecond: 0,
                    decodedMessagesPerSecond: 0,
                    decodeFailuresPerSecond: 0,
                    mappingFailuresPerSecond: 0,
                    processingFailuresPerSecond: 0,
                    lastMessageAt: null,
                    lastCommitAt: null,
                    consumerLagMessages: null,
                    lagSampledAt: null,
                    committedOffset: null,
                    lastError: null,
                  },
                  local: {
                    connected: true,
                    assignedPartitions: 1,
                    messagesPerSecond: 0,
                    bytesPerSecond: 0,
                    decodedMessagesPerSecond: 0,
                    decodeFailuresPerSecond: 0,
                    mappingFailuresPerSecond: 0,
                    processingFailuresPerSecond: 0,
                    lastMessageAt: null,
                    lastCommitAt: null,
                    consumerLagMessages: null,
                    lagSampledAt: null,
                    committedOffset: null,
                    lastError: null,
                  },
                }),
              },
            }),
          },
        },
      });

      yield* readyRuntimeCore.close;
      yield* startingRuntimeCore.close;
    }),
  );

  it.effect("resets idle Kafka per-second counters on health reads", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      yield* ledger.messageDecoded(ordersSourceTopic, "local", {
        bytes: 10,
        committedOffset: "1",
        nowMillis: 1_000,
      });

      const activeHealth = ledger.healthOverlay(yield* runtimeCore.client.health(), 1_000);
      const idleHealth = ledger.healthOverlay(yield* runtimeCore.client.health(), 2_000);

      expect(activeHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]).toStrictEqual({
        connected: true,
        assignedPartitions: 1,
        messagesPerSecond: 1,
        bytesPerSecond: 10,
        decodedMessagesPerSecond: 1,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        processingFailuresPerSecond: 0,
        lastMessageAt: 1_000,
        lastCommitAt: 1_000,
        consumerLagMessages: null,
        lagSampledAt: null,
        committedOffset: "1",
        lastError: null,
      });
      expect(idleHealth.kafka?.topics[ordersSourceTopic]?.regions["local"]).toStrictEqual({
        connected: true,
        assignedPartitions: 1,
        messagesPerSecond: 0,
        bytesPerSecond: 0,
        decodedMessagesPerSecond: 0,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        processingFailuresPerSecond: 0,
        lastMessageAt: 1_000,
        lastCommitAt: 1_000,
        consumerLagMessages: null,
        lagSampledAt: null,
        committedOffset: "1",
        lastError: null,
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("recovers degraded topic status after successful Kafka decoding resumes", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });

      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      yield* ledger.decodeFailed(ordersSourceTopic, "local", {
        bytes: 5,
        message: "bad-json",
        nowMillis: 1_000,
      });
      yield* ledger.messageDecoded(ordersSourceTopic, "local", {
        bytes: 10,
        committedOffset: "2",
        nowMillis: 1_000,
      });
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect(health.status).toBe("ready");
      expect(health.kafka?.topics[ordersSourceTopic]).toStrictEqual({
        status: "ready",
        sourceTopic: ordersSourceTopic,
        viewServerTopic: "orders",
        regions: nullRecord({
          local: {
            connected: true,
            assignedPartitions: 1,
            messagesPerSecond: 2,
            bytesPerSecond: 15,
            decodedMessagesPerSecond: 1,
            decodeFailuresPerSecond: 1,
            mappingFailuresPerSecond: 0,
            processingFailuresPerSecond: 0,
            lastMessageAt: 1_000,
            lastCommitAt: 1_000,
            consumerLagMessages: null,
            lagSampledAt: null,
            committedOffset: "2",
            lastError: null,
          },
        }),
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("closes already started region consumers when a later region fails", () =>
    Effect.gen(function* () {
      const closedConsumers: Array<string> = [];
      const starts: Record<
        string,
        Effect.Effect<StartedKafkaRegionConsumer, ViewServerKafkaIngressError>
      > = {
        cold: Effect.fail(kafkaConsumerStartError("cold", "no-broker")),
        local: Effect.succeed({
          close: Effect.sync(() => {
            closedConsumers.push("local");
          }),
        }),
      };
      const regionStarts: ReadonlyArray<readonly [string, string]> = [
        ["local", regions.local],
        ["cold", regions.cold],
      ];

      const exit = yield* Effect.exit(
        startKafkaRegionConsumers(
          regionStarts,
          (region) =>
            starts[region] ?? Effect.fail(kafkaConsumerStartError(region, "unexpected-region")),
        ),
      );

      expect({
        startupFailed: Exit.isFailure(exit),
        closedConsumers,
      }).toStrictEqual({
        startupFailed: true,
        closedConsumers: ["local"],
      });
    }),
  );

  it.effect("records stream errors before refailing them", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      const streamError = kafkaStreamError("local", "stream-down");

      const exit = yield* Effect.exit(recordKafkaStreamError(ledger, "local", streamError));
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        streamRecordingFailed: Exit.isFailure(exit),
        regions: health.kafka?.regions,
      }).toStrictEqual({
        streamRecordingFailed: true,
        regions: nullRecord({
          local: {
            status: "disconnected",
            brokers: regions.local,
            lastConnectedAt: null,
            lastError: "Kafka stream failed for region local",
          },
        }),
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("records defects in Kafka stream processing as generic stream failures", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      const defectiveClient: ViewServerRuntimeClient<Topics> = {
        ...runtimeCore.client,
        publish: () => Effect.die("publish defect"),
      };

      const error = yield* Effect.flip(
        runKafkaMessageStream(
          viewServer,
          defectiveClient,
          runtimeCore.requestHealthRefresh,
          kafkaOptions,
          ledger,
          "local",
          (async function* () {
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "order-defective-publish",
              value: JSON.stringify({
                customerId: "customer-defective-publish",
                price: 60,
              }),
            });
          })(),
        ),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        error: {
          message: error.message,
          region: error.region,
          sourceTopic: error.sourceTopic,
        },
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        error: {
          message: "Kafka stream failed for region local",
          region: "local",
          sourceTopic: undefined,
        },
        kafkaTopic: {
          status: "degraded",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: false,
              assignedPartitions: 1,
              messagesPerSecond: 0,
              bytesPerSecond: 0,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 0,
              mappingFailuresPerSecond: 0,
              processingFailuresPerSecond: 0,
              lastMessageAt: null,
              lastCommitAt: null,
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: null,
              lastError: "Kafka stream failed for region local",
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("preserves Kafka health when message stream processing is interrupted", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      const interruptingClient: ViewServerRuntimeClient<Topics> = {
        ...runtimeCore.client,
        publish: () => Effect.interrupt,
      };

      const exit = yield* Effect.exit(
        runKafkaMessageStream(
          viewServer,
          interruptingClient,
          runtimeCore.requestHealthRefresh,
          kafkaOptions,
          ledger,
          "local",
          (async function* () {
            yield kafkaMessage({
              topic: ordersSourceTopic,
              key: "order-interrupted",
              value: JSON.stringify({
                customerId: "customer-interrupted",
                price: 80,
              }),
            });
          })(),
        ),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect({
        interrupted: Exit.hasInterrupts(exit),
        region: health.kafka?.regions["local"],
        topic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        interrupted: true,
        region: {
          status: "connected",
          brokers: regions.local,
          lastConnectedAt: 1_000,
          lastError: null,
        },
        topic: {
          status: "ready",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: true,
              assignedPartitions: 1,
              messagesPerSecond: 0,
              bytesPerSecond: 0,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 0,
              mappingFailuresPerSecond: 0,
              processingFailuresPerSecond: 0,
              lastMessageAt: null,
              lastCommitAt: null,
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: null,
              lastError: null,
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );
  it.effect("runs Kafka message streams and records stream failures", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);

      const exit = yield* Effect.exit(
        runKafkaMessageStream(
          viewServer,
          runtimeCore.client,
          runtimeCore.requestHealthRefresh,
          kafkaOptions,
          ledger,
          "local",
          failingKafkaStream(),
        ),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        streamFailed: Exit.isFailure(exit),
        snapshot,
      }).toStrictEqual({
        streamFailed: true,
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "order-stream-1",
              customerId: "customer-stream-1",
              price: 30,
            },
          ],
          totalRows: 1,
          version: 1,
        },
      });
      expect({
        status: health.status,
        kafka: health.kafka,
      }).toStrictEqual({
        status: "degraded",
        kafka: {
          regions: nullRecord({
            local: {
              status: "disconnected",
              brokers: regions.local,
              lastConnectedAt: 1_000,
              lastError: "Kafka stream failed for region local",
            },
          }),
          topics: nullRecord({
            [ordersSourceTopic]: {
              status: "degraded",
              sourceTopic: ordersSourceTopic,
              viewServerTopic: "orders",
              regions: nullRecord({
                local: {
                  connected: false,
                  assignedPartitions: 1,
                  messagesPerSecond: 1,
                  bytesPerSecond: 59,
                  decodedMessagesPerSecond: 1,
                  decodeFailuresPerSecond: 0,
                  mappingFailuresPerSecond: 0,
                  processingFailuresPerSecond: 0,
                  lastMessageAt: 0,
                  lastCommitAt: 0,
                  consumerLagMessages: null,
                  lagSampledAt: null,
                  committedOffset: "5",
                  lastError: "Kafka stream failed for region local",
                },
              }),
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("fails Kafka streams before later records can skip failed offsets", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);

      let committedMessages = 0;
      const error = yield* Effect.flip(
        runKafkaMessageStream(
          viewServer,
          runtimeCore.client,
          runtimeCore.requestHealthRefresh,
          kafkaOptions,
          ledger,
          "local",
          decodeFailureThenSuccessKafkaStream(() => {
            committedMessages += 1;
          }),
        ),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        streamFailure: {
          message: error.message,
          region: error.region,
          sourceTopic: error.sourceTopic,
        },
        committedMessages,
        snapshot,
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        streamFailure: {
          message: `Failed to decode Kafka message for source topic ${ordersSourceTopic}`,
          region: "local",
          sourceTopic: ordersSourceTopic,
        },
        committedMessages: 0,
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [],
          totalRows: 0,
          version: 0,
        },
        kafkaTopic: {
          status: "degraded",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: false,
              assignedPartitions: 1,
              messagesPerSecond: 1,
              bytesPerSecond: 9,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 1,
              mappingFailuresPerSecond: 0,
              processingFailuresPerSecond: 0,
              lastMessageAt: 0,
              lastCommitAt: null,
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: null,
              lastError: "Failed to parse Kafka JSON payload",
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("creates a no-op ingress when no regions own source topics", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const emptyKafkaOptions: ResolvedViewServerKafkaRuntimeOptions<Topics> = {
        consumerGroupId: "view-server-empty-test",
        regions: {
          cold: "localhost:9093",
        },
        topics: {},
      };
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: emptyKafkaOptions.regions,
        topics: {},
      });

      const ingress = yield* makeViewServerKafkaIngress(
        viewServer,
        runtimeCore.client,
        runtimeCore.requestHealthRefresh,
        emptyKafkaOptions,
        ledger,
      );
      yield* ingress.close;
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect(health.status).toBe("ready");
      expect(health.kafka).toStrictEqual({
        regions: nullRecord({}),
        topics: nullRecord({}),
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("preserves dangerous Kafka health source topic and region keys", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const dangerousRegions: Record<string, string> = Object.create(null);
      dangerousRegions["__proto__"] = "localhost:9092";
      const dangerousTopics: Record<
        string,
        {
          readonly viewServerTopic: "orders";
          readonly regions: ReadonlyArray<string>;
        }
      > = Object.create(null);
      dangerousTopics["__proto__"] = {
        regions: ["__proto__"],
        viewServerTopic: "orders",
      };
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: dangerousRegions,
        topics: dangerousTopics,
      });

      yield* ledger.regionConnected("__proto__", 1_000);
      yield* ledger.topicConnected("__proto__", "__proto__", 2, 1_000);
      yield* ledger.messageDecoded("__proto__", "__proto__", {
        bytes: 12,
        committedOffset: "4",
        nowMillis: 2_000,
      });

      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 2_000);
      const expectedRegions: Record<string, unknown> = Object.create(null);
      expectedRegions["__proto__"] = {
        status: "connected",
        brokers: "localhost:9092",
        lastConnectedAt: 1_000,
        lastError: null,
      };
      const expectedTopicRegions: Record<string, unknown> = Object.create(null);
      expectedTopicRegions["__proto__"] = {
        connected: true,
        assignedPartitions: 2,
        messagesPerSecond: 1,
        bytesPerSecond: 12,
        decodedMessagesPerSecond: 1,
        decodeFailuresPerSecond: 0,
        mappingFailuresPerSecond: 0,
        processingFailuresPerSecond: 0,
        lastMessageAt: 2_000,
        lastCommitAt: 2_000,
        consumerLagMessages: null,
        lagSampledAt: null,
        committedOffset: "4",
        lastError: null,
      };
      const expectedTopics: Record<string, unknown> = Object.create(null);
      expectedTopics["__proto__"] = {
        status: "ready",
        sourceTopic: "__proto__",
        viewServerTopic: "orders",
        regions: expectedTopicRegions,
      };

      expect(Object.hasOwn(health.kafka?.regions ?? {}, "__proto__")).toBe(true);
      expect(Object.hasOwn(health.kafka?.topics ?? {}, "__proto__")).toBe(true);
      expect(Object.hasOwn(health.kafka?.topics["__proto__"]?.regions ?? {}, "__proto__")).toBe(
        true,
      );
      expect(health.kafka).toStrictEqual({
        regions: expectedRegions,
        topics: expectedTopics,
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("does not run Kafka listener callbacks after their scope closes", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      let healthRefreshRequestCount = 0;
      const requestHealthRefresh = Effect.sync(() => {
        healthRefreshRequestCount += 1;
      });
      const consumer = new Consumer<Buffer, Buffer, Buffer, Buffer>({
        bootstrapBrokers: ["127.0.0.1:1"],
        clientId: "view-server-scoped-listener-test",
        groupId: "view-server-scoped-listener-test",
      });
      const scope = yield* Scope.make("parallel");
      const listenerRegistration = yield* registerKafkaConsumerHealthListeners(
        consumer,
        ledger,
        requestHealthRefresh,
        "local",
        [ordersSourceTopic],
        scope,
      );

      yield* Scope.close(scope, Exit.void);
      consumer.emit("consumer:group:join", {
        groupId: "view-server-scoped-listener-test",
        memberId: "member-1",
        assignments: [{ topic: ordersSourceTopic, partitions: [0] }],
      });
      consumer.emit("consumer:lag:error", new Error("late lag failure"));
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        healthRefreshRequestCount,
        region: health.kafka?.regions["local"],
        topic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        healthRefreshRequestCount: 0,
        region: {
          status: "starting",
          brokers: regions.local,
          lastConnectedAt: null,
          lastError: null,
        },
        topic: {
          status: "starting",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: false,
              assignedPartitions: 0,
              messagesPerSecond: 0,
              bytesPerSecond: 0,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 0,
              mappingFailuresPerSecond: 0,
              processingFailuresPerSecond: 0,
              lastMessageAt: null,
              lastCommitAt: null,
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: null,
              lastError: null,
            },
          }),
        },
      });

      yield* listenerRegistration.close;
      yield* Effect.promise(() => Promise.resolve(consumer.close(true)));
      yield* runtimeCore.close;
    }),
  );

  it.effect("fails Kafka ingress startup when Kafka consumer cannot start", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const invalidKafkaOptions: ResolvedViewServerKafkaRuntimeOptions<Topics> = {
        consumerGroupId: "view-server-invalid-broker-test",
        regions: {
          local: "",
        },
        topics: {
          [ordersSourceTopic]: localKafkaTopic({
            regions: ["local"],
            value: kafka.json(IncomingOrder),
            key: kafka.stringKey(),
            viewServerTopic: "orders",
            mapping: ({ key, value }) => ({
              id: key,
              customerId: value.customerId,
              price: value.price,
            }),
          }),
        },
      };
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: invalidKafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });

      const exit = yield* Effect.exit(
        makeViewServerKafkaIngress(
          viewServer,
          runtimeCore.client,
          runtimeCore.requestHealthRefresh,
          invalidKafkaOptions,
          ledger,
        ),
      );

      expect(Exit.isFailure(exit)).toBe(true);

      yield* runtimeCore.close;
    }),
  );

  it.effect("processes source messages into runtime rows and Kafka health", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);

      let committedMessages = 0;
      yield* processKafkaMessage(
        viewServer,
        runtimeCore.client,
        runtimeCore.requestHealthRefresh,
        kafkaOptions,
        ledger,
        "local",
        kafkaMessage({
          topic: unknownSourceTopic,
          key: "ignored",
          value: "{}",
        }),
      );
      yield* processKafkaMessage(
        viewServer,
        runtimeCore.client,
        runtimeCore.requestHealthRefresh,
        kafkaOptions,
        ledger,
        "local",
        kafkaMessage({
          topic: ordersSourceTopic,
          key: "order-1",
          value: JSON.stringify({
            customerId: "customer-1",
            price: 10,
          }),
          headers: new Map([[Buffer.from("trace"), Buffer.from("abc")]]),
          offset: 1n,
          onCommit: () => {
            committedMessages += 1;
          },
        }),
      );
      const decodeExit = yield* Effect.exit(
        processKafkaMessage(
          viewServer,
          runtimeCore.client,
          runtimeCore.requestHealthRefresh,
          kafkaOptions,
          ledger,
          "local",
          kafkaMessage({
            topic: ordersSourceTopic,
            key: "bad-json",
            value: "{",
            offset: 2n,
            onCommit: () => {
              committedMessages += 1;
            },
          }),
        ),
      );

      const publishExit = yield* Effect.exit(
        processKafkaMessage(
          viewServer,
          failingClient,
          runtimeCore.requestHealthRefresh,
          kafkaOptions,
          ledger,
          "local",
          kafkaMessage({
            topic: ordersSourceTopic,
            key: "order-2",
            value: JSON.stringify({
              customerId: "customer-2",
              price: 20,
            }),
            offset: 3n,
            onCommit: () => {
              committedMessages += 1;
            },
          }),
        ),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        decodeFailed: Exit.isFailure(decodeExit),
        publishFailed: Exit.isFailure(publishExit),
        committedMessages,
        snapshot,
      }).toStrictEqual({
        decodeFailed: true,
        publishFailed: true,
        committedMessages: 1,
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "order-1",
              customerId: "customer-1",
              price: 10,
            },
          ],
          totalRows: 1,
          version: 1,
        },
      });
      expect(health.kafka?.topics[ordersSourceTopic]).toStrictEqual({
        status: "degraded",
        sourceTopic: ordersSourceTopic,
        viewServerTopic: "orders",
        regions: nullRecord({
          local: {
            connected: true,
            assignedPartitions: 1,
            messagesPerSecond: 3,
            bytesPerSecond: 99,
            decodedMessagesPerSecond: 1,
            decodeFailuresPerSecond: 1,
            mappingFailuresPerSecond: 0,
            processingFailuresPerSecond: 1,
            lastMessageAt: 0,
            lastCommitAt: 0,
            consumerLagMessages: null,
            lagSampledAt: null,
            committedOffset: "2",
            lastError: "publish failed",
          },
        }),
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("records mapping failures separately from decode failures", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      const throwingKafkaOptions: ResolvedViewServerKafkaRuntimeOptions<Topics> = {
        consumerGroupId: "view-server-mapping-failure-test",
        regions,
        topics: {
          [ordersSourceTopic]: localKafkaTopic({
            regions: ["local"],
            value: kafka.json(IncomingOrder),
            key: kafka.stringKey(),
            viewServerTopic: "orders",
            mapping: () => {
              throw new Error("mapping failed");
            },
          }),
        },
      };
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      let healthRefreshRequestCount = 0;
      const requestHealthRefresh = Effect.sync(() => {
        healthRefreshRequestCount += 1;
      });

      const error = yield* Effect.flip(
        processKafkaMessage(
          viewServer,
          runtimeCore.client,
          requestHealthRefresh,
          throwingKafkaOptions,
          ledger,
          "local",
          kafkaMessage({
            topic: ordersSourceTopic,
            key: "order-mapping-failed",
            value: JSON.stringify({
              customerId: "customer-mapping-failed",
              price: 70,
            }),
            offset: 7n,
          }),
        ),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        error: {
          causeMessage: messageFromUnknown(error.cause),
          message: error.message,
          region: error.region,
          sourceTopic: error.sourceTopic,
        },
        healthRefreshRequestCount,
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        error: {
          causeMessage: "Failed to map Kafka payload",
          message: `Failed to map Kafka message for source topic ${ordersSourceTopic}`,
          region: "local",
          sourceTopic: ordersSourceTopic,
        },
        healthRefreshRequestCount: 1,
        kafkaTopic: {
          status: "degraded",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: true,
              assignedPartitions: 1,
              messagesPerSecond: 1,
              bytesPerSecond: 71,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 0,
              mappingFailuresPerSecond: 1,
              processingFailuresPerSecond: 0,
              lastMessageAt: 0,
              lastCommitAt: null,
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: null,
              lastError: "Failed to map Kafka payload",
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("records untagged codec failures as decode failures", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      const untaggedDecodeKafkaOptions: ResolvedViewServerKafkaRuntimeOptions<Topics> = {
        consumerGroupId: "view-server-untagged-decode-failure-test",
        regions,
        topics: {
          [ordersSourceTopic]: localKafkaTopic({
            regions: ["local"],
            value: kafka.codec({
              name: "untagged-error",
              decode: () => Effect.fail(nonStringTagCodecError),
            }),
            key: kafka.stringKey(),
            viewServerTopic: "orders",
            mapping: ({ key }) => ({
              id: key,
              customerId: "unused",
              price: 0,
            }),
          }),
        },
      };
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);

      const exit = yield* Effect.exit(
        processKafkaMessage(
          viewServer,
          runtimeCore.client,
          runtimeCore.requestHealthRefresh,
          untaggedDecodeKafkaOptions,
          ledger,
          "local",
          kafkaMessage({
            topic: ordersSourceTopic,
            key: "order-untagged-codec-failed",
            value: JSON.stringify({
              customerId: "customer-untagged-codec-failed",
              price: 90,
            }),
            offset: 8n,
          }),
        ),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        decodeFailed: Exit.isFailure(exit),
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        decodeFailed: true,
        kafkaTopic: {
          status: "degraded",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: true,
              assignedPartitions: 1,
              messagesPerSecond: 1,
              bytesPerSecond: 85,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 1,
              mappingFailuresPerSecond: 0,
              processingFailuresPerSecond: 0,
              lastMessageAt: 0,
              lastCommitAt: null,
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: null,
              lastError: "non-string tag",
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("does not classify custom codec errors as mapping failures by public tag alone", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      const forgedMappingTagKafkaOptions: ResolvedViewServerKafkaRuntimeOptions<Topics> = {
        consumerGroupId: "view-server-forged-mapping-tag-test",
        regions,
        topics: {
          [ordersSourceTopic]: localKafkaTopic({
            regions: ["local"],
            value: kafka.codec({
              name: "forged-mapping-tag-error",
              decode: () => Effect.fail(forgedMappingTagCodecError),
            }),
            key: kafka.stringKey(),
            viewServerTopic: "orders",
            mapping: ({ key }) => ({
              id: key,
              customerId: "unused",
              price: 0,
            }),
          }),
        },
      };
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);

      const error = yield* Effect.flip(
        processKafkaMessage(
          viewServer,
          runtimeCore.client,
          runtimeCore.requestHealthRefresh,
          forgedMappingTagKafkaOptions,
          ledger,
          "local",
          kafkaMessage({
            topic: ordersSourceTopic,
            key: "order-forged-mapping-tag-codec-failed",
            value: JSON.stringify({
              customerId: "customer-forged-mapping-tag-codec-failed",
              price: 95,
            }),
            offset: 9n,
          }),
        ),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        error: {
          message: error.message,
          sourceTopic: error.sourceTopic,
        },
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        error: {
          message: `Failed to decode Kafka message for source topic ${ordersSourceTopic}`,
          sourceTopic: ordersSourceTopic,
        },
        kafkaTopic: {
          status: "degraded",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: true,
              assignedPartitions: 1,
              messagesPerSecond: 1,
              bytesPerSecond: 105,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 1,
              mappingFailuresPerSecond: 0,
              processingFailuresPerSecond: 0,
              lastMessageAt: 0,
              lastCommitAt: null,
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: null,
              lastError: "forged mapping tag",
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("records primitive codec failures as decode failures", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      const primitiveDecodeKafkaOptions: ResolvedViewServerKafkaRuntimeOptions<Topics> = {
        consumerGroupId: "view-server-primitive-decode-failure-test",
        regions,
        topics: {
          [ordersSourceTopic]: localKafkaTopic({
            regions: ["local"],
            value: kafka.codec({
              name: "primitive-error",
              decode: () => Effect.fail("raw codec failed"),
            }),
            key: kafka.stringKey(),
            viewServerTopic: "orders",
            mapping: ({ key }) => ({
              id: key,
              customerId: "unused",
              price: 0,
            }),
          }),
        },
      };
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);

      const exit = yield* Effect.exit(
        processKafkaMessage(
          viewServer,
          runtimeCore.client,
          runtimeCore.requestHealthRefresh,
          primitiveDecodeKafkaOptions,
          ledger,
          "local",
          kafkaMessage({
            topic: ordersSourceTopic,
            key: "order-primitive-codec-failed",
            value: JSON.stringify({
              customerId: "customer-primitive-codec-failed",
              price: 100,
            }),
            offset: 9n,
          }),
        ),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect({
        decodeFailed: Exit.isFailure(exit),
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        decodeFailed: true,
        kafkaTopic: {
          status: "degraded",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: true,
              assignedPartitions: 1,
              messagesPerSecond: 1,
              bytesPerSecond: 88,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 1,
              mappingFailuresPerSecond: 0,
              processingFailuresPerSecond: 0,
              lastMessageAt: 0,
              lastCommitAt: null,
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: null,
              lastError: "raw codec failed",
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("records nullable Kafka key and value bytes as decode failures", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);
      let healthRefreshRequestCount = 0;
      const requestHealthRefresh = Effect.sync(() => {
        healthRefreshRequestCount += 1;
      });

      const exit = yield* Effect.exit(
        processKafkaMessage(
          viewServer,
          runtimeCore.client,
          requestHealthRefresh,
          kafkaOptions,
          ledger,
          "local",
          kafkaMessage({
            topic: ordersSourceTopic,
            key: null,
            value: null,
            offset: 6n,
          }),
        ),
      );
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 0);

      expect(Exit.isFailure(exit)).toBe(true);
      expect(healthRefreshRequestCount).toBe(1);
      expect(health.kafka?.topics[ordersSourceTopic]).toStrictEqual({
        status: "degraded",
        sourceTopic: ordersSourceTopic,
        viewServerTopic: "orders",
        regions: nullRecord({
          local: {
            connected: true,
            assignedPartitions: 1,
            messagesPerSecond: 1,
            bytesPerSecond: 0,
            decodedMessagesPerSecond: 0,
            decodeFailuresPerSecond: 1,
            mappingFailuresPerSecond: 0,
            processingFailuresPerSecond: 0,
            lastMessageAt: 0,
            lastCommitAt: null,
            consumerLagMessages: null,
            lagSampledAt: null,
            committedOffset: null,
            lastError: "Failed to parse Kafka JSON payload",
          },
        }),
      });

      yield* runtimeCore.close;
    }),
  );

  it.effect("fails processing when Kafka commit fails after publish", () =>
    Effect.gen(function* () {
      const runtimeCore = yield* makeViewServerRuntimeCore(viewServer, {});
      const ledger = makeViewServerKafkaHealthLedger<Topics>({
        regions: kafkaOptions.regions,
        topics: {
          [ordersSourceTopic]: {
            regions: ["local"],
            viewServerTopic: "orders",
          },
        },
      });
      yield* ledger.regionConnected("local", 1_000);
      yield* ledger.topicConnected(ordersSourceTopic, "local", 1, 1_000);

      const exit = yield* Effect.exit(
        processKafkaMessage(
          viewServer,
          runtimeCore.client,
          runtimeCore.requestHealthRefresh,
          kafkaOptions,
          ledger,
          "local",
          kafkaMessage({
            topic: ordersSourceTopic,
            key: "order-commit-failed",
            value: JSON.stringify({
              customerId: "customer-commit-failed",
              price: 50,
            }),
            offset: 5n,
            commitFailure: new Error("commit failed"),
          }),
        ),
      );
      const snapshot = yield* runtimeCore.client.snapshot("orders", {
        select: ["id", "customerId", "price"],
        orderBy: [{ field: "id", direction: "asc" }],
        limit: 10,
      });
      const health = ledger.healthOverlay(yield* runtimeCore.client.health(), 1_000);

      expect({
        commitFailed: Exit.isFailure(exit),
        snapshot,
        kafkaTopic: health.kafka?.topics[ordersSourceTopic],
      }).toStrictEqual({
        commitFailed: true,
        snapshot: {
          status: "ready",
          statusCode: "Ready",
          rows: [
            {
              id: "order-commit-failed",
              customerId: "customer-commit-failed",
              price: 50,
            },
          ],
          totalRows: 1,
          version: 1,
        },
        kafkaTopic: {
          status: "ready",
          sourceTopic: ordersSourceTopic,
          viewServerTopic: "orders",
          regions: nullRecord({
            local: {
              connected: true,
              assignedPartitions: 1,
              messagesPerSecond: 0,
              bytesPerSecond: 0,
              decodedMessagesPerSecond: 0,
              decodeFailuresPerSecond: 0,
              mappingFailuresPerSecond: 0,
              processingFailuresPerSecond: 0,
              lastMessageAt: null,
              lastCommitAt: null,
              consumerLagMessages: null,
              lagSampledAt: null,
              committedOffset: null,
              lastError: null,
            },
          }),
        },
      });

      yield* runtimeCore.close;
    }),
  );
});
