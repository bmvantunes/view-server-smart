// Benchmarks intentionally import Vitest directly: @effect/vitest does not expose `bench`.
import { afterAll, beforeAll, bench, describe } from "vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { create, toBinary } from "@bufbuild/protobuf";
import { Admin, Producer, stringSerializers } from "@platformatic/kafka";
import { defineViewServerConfig, kafka, type ViewServerHealth } from "@view-server/config";
import { ignoreLoggedTypedFailuresPreserveNonTypedFailures } from "@view-server/effect-utils";
import { Buffer } from "node:buffer";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { performance } from "node:perf_hooks";
import { Crypto, Effect, Exit, Schedule, Schema } from "effect";
import { makeViewServerRuntime, type ViewServerRuntime } from "./index";
import { OrderKeySchema, OrderValueSchema } from "./test-fixtures/runtime_orders_pb";

declare const process: {
  readonly cwd: () => string;
  readonly env: Record<string, string | undefined>;
  readonly memoryUsage: () => {
    readonly arrayBuffers: number;
    readonly external: number;
    readonly heapTotal: number;
    readonly heapUsed: number;
    readonly rss: number;
  };
};

type BenchmarkMemorySnapshot = {
  readonly arrayBuffersBytes: number;
  readonly externalBytes: number;
  readonly heapTotalBytes: number;
  readonly heapUsedBytes: number;
  readonly rssBytes: number;
};

type BenchmarkProfile = {
  jsonProducedRows: number;
  memoryAfterSetup: BenchmarkMemorySnapshot | undefined;
  protobufProducedRows: number;
  runtime: ViewServerRuntime<Topics> | undefined;
  stringProducer: Producer<string, string, string, string> | undefined;
  binaryProducer: Producer<Buffer, Buffer, Buffer, Buffer> | undefined;
  sourceTopics:
    | {
        jsonOrders: string;
        protobufOrders: string;
      }
    | undefined;
};

type BenchmarkCase = {
  readonly name: string;
  readonly run: () => Promise<void>;
};

type IngestSample = {
  readonly convergenceMs: number;
  readonly name: string;
  readonly producerSendMs: number;
  readonly rows: number;
  readonly rowsPerSecond: number;
  readonly totalMs: number;
};

type IngestThroughputCaseSummary = {
  readonly aggregateRowsPerSecond: number;
  readonly maxTotalMs: number;
  readonly meanConvergenceMs: number;
  readonly meanProducerSendMs: number;
  readonly meanRowsPerSecond: number;
  readonly meanTotalMs: number;
  readonly minRowsPerSecond: number;
  readonly name: string;
  readonly producedRowsPerSample: number;
  readonly sampleCount: number;
  readonly totalProducedRows: number;
};

class KafkaIngestBenchmarkError extends Schema.TaggedErrorClass<KafkaIngestBenchmarkError>()(
  "KafkaIngestBenchmarkError",
  {
    message: Schema.String,
  },
) {}

const JsonOrder = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  price: Schema.Number,
});

const ProtobufOrder = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  price: Schema.Number,
});

const IncomingJsonOrder = Schema.Struct({
  customerId: Schema.String,
  price: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    jsonOrders: {
      schema: JsonOrder,
      key: "id",
    },
    protobufOrders: {
      schema: ProtobufOrder,
      key: "id",
    },
  },
});

type Topics = typeof viewServer.topics;

const defaultBatchSize = 1_000;
const defaultBenchmarkTimeMs = 250;
const defaultIterations = 3;
const defaultSustainedBatchCount = 4;
const defaultWarmupIterations = 0;
const defaultWarmupTimeMs = 0;
const kafkaBootstrapServers =
  process.env["VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS"] ?? "localhost:9092";
const memoryBefore = memorySnapshot();
const healthPollSchedule = Schedule.addDelay(Schedule.recurs(2_400), () =>
  Effect.succeed("25 millis"),
);
const ignoreKafkaBenchAdminCloseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring Kafka ingest benchmark admin close failure.",
);

const positiveIntegerFromEnv = (name: string, fallback: number): number => {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  const trimmed = raw.trim();
  if (!/^[1-9]\d*$/u.test(trimmed)) {
    throw new Error(`${name} must be a positive integer.`);
  }
  const parsed = Number.parseInt(trimmed, 10);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer.`);
  }
  return parsed;
};

const nonNegativeIntegerFromEnv = (name: string, fallback: number): number => {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  const trimmed = raw.trim();
  if (!/^(0|[1-9]\d*)$/u.test(trimmed)) {
    throw new Error(`${name} must be a non-negative integer.`);
  }
  const parsed = Number.parseInt(trimmed, 10);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer.`);
  }
  return parsed;
};

const batchSize = positiveIntegerFromEnv(
  "VIEW_SERVER_RUNTIME_BENCH_KAFKA_BATCH_SIZE",
  defaultBatchSize,
);
const burstMultiplier = positiveIntegerFromEnv(
  "VIEW_SERVER_RUNTIME_BENCH_KAFKA_BURST_MULTIPLIER",
  4,
);
const sustainedBatchCount = positiveIntegerFromEnv(
  "VIEW_SERVER_RUNTIME_BENCH_KAFKA_SUSTAINED_BATCHES",
  defaultSustainedBatchCount,
);
const benchmarkMode = process.env["VIEW_SERVER_RUNTIME_BENCH_KAFKA_MODE"] ?? "batch";
if (benchmarkMode !== "batch" && benchmarkMode !== "sustained-firehose") {
  throw new Error("VIEW_SERVER_RUNTIME_BENCH_KAFKA_MODE must be batch or sustained-firehose.");
}
const benchmarkScope =
  benchmarkMode === "sustained-firehose"
    ? "runtime-kafka-sustained-firehose"
    : "runtime-kafka-ingest";
const benchmarkName =
  benchmarkMode === "sustained-firehose"
    ? "Kafka sustained firehose runtime benchmark"
    : "Kafka ingest runtime benchmark";
const benchmarkCaseNames =
  benchmarkMode === "sustained-firehose"
    ? ["sustained mixed firehose ingest"]
    : ["json source batch ingest", "protobuf source batch ingest", "mixed source burst ingest"];
const defaultOutputJsonName =
  benchmarkMode === "sustained-firehose"
    ? `kafka-sustained-firehose-${batchSize}rows-${sustainedBatchCount}batches.json`
    : `kafka-ingest-${batchSize}rows.json`;
const outputJsonPath = benchmarkOutputJsonPath(defaultOutputJsonName);
const benchOptions = {
  iterations: positiveIntegerFromEnv("VIEW_SERVER_RUNTIME_BENCH_ITERATIONS", defaultIterations),
  time: positiveIntegerFromEnv("VIEW_SERVER_RUNTIME_BENCH_TIME_MS", defaultBenchmarkTimeMs),
  warmupIterations: nonNegativeIntegerFromEnv(
    "VIEW_SERVER_RUNTIME_BENCH_WARMUP_ITERATIONS",
    defaultWarmupIterations,
  ),
  warmupTime: nonNegativeIntegerFromEnv(
    "VIEW_SERVER_RUNTIME_BENCH_WARMUP_TIME_MS",
    defaultWarmupTimeMs,
  ),
};

const profile: BenchmarkProfile = {
  binaryProducer: undefined,
  jsonProducedRows: 0,
  memoryAfterSetup: undefined,
  protobufProducedRows: 0,
  runtime: undefined,
  sourceTopics: undefined,
  stringProducer: undefined,
};

const ingestSamples: Array<IngestSample> = [];
const internalTopicNames = ["jsonOrders", "protobufOrders"] as const;

function memorySnapshot(): BenchmarkMemorySnapshot {
  const memory = process.memoryUsage();
  return {
    arrayBuffersBytes: memory.arrayBuffers,
    externalBytes: memory.external,
    heapTotalBytes: memory.heapTotal,
    heapUsedBytes: memory.heapUsed,
    rssBytes: memory.rss,
  };
}

function memoryDelta(
  before: BenchmarkMemorySnapshot,
  after: BenchmarkMemorySnapshot,
): BenchmarkMemorySnapshot {
  return {
    arrayBuffersBytes: after.arrayBuffersBytes - before.arrayBuffersBytes,
    externalBytes: after.externalBytes - before.externalBytes,
    heapTotalBytes: after.heapTotalBytes - before.heapTotalBytes,
    heapUsedBytes: after.heapUsedBytes - before.heapUsedBytes,
    rssBytes: after.rssBytes - before.rssBytes,
  };
}

function benchmarkOutputJsonPath(fallbackName: string): string {
  const configured = process.env["VIEW_SERVER_RUNTIME_BENCH_OUTPUT_JSON"];
  if (configured !== undefined && configured.trim() !== "") {
    return configured.trim();
  }
  return join(".artifacts", fallbackName);
}

function benchmarkSummaryPath(path: string): string {
  if (path.endsWith(".json")) {
    return `${path.slice(0, -".json".length)}.summary.json`;
  }
  return `${path}.summary.json`;
}

function writeJsonFile(path: string, value: unknown): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(
    path,
    `${JSON.stringify(
      value,
      (_key, item: unknown) => (typeof item === "bigint" ? item.toString() : item),
      2,
    )}\n`,
  );
}

const uniqueName = Effect.fn("ViewServerRuntime.kafka.bench.uniqueName")(function* (
  prefix: string,
) {
  const crypto = yield* Crypto.Crypto;
  const uuid = yield* crypto.randomUUIDv7;
  return `view-server-bench-${prefix}-${uuid.replaceAll("-", "")}`;
});

const createKafkaTopics = Effect.fn("ViewServerRuntime.kafka.bench.createTopics")(function* (
  bootstrapServers: string,
  topics: ReadonlyArray<string>,
) {
  const admin = new Admin({
    bootstrapBrokers: [bootstrapServers],
    clientId: "view-server-kafka-ingest-bench-admin",
  });
  return yield* Effect.acquireUseRelease(
    Effect.succeed(admin),
    (currentAdmin) =>
      Effect.promise(() =>
        currentAdmin.createTopics({
          partitions: 1,
          replicas: 1,
          topics: [...topics],
        }),
      ),
    (currentAdmin) =>
      Effect.promise(() => currentAdmin.close()).pipe(ignoreKafkaBenchAdminCloseFailure),
  );
});

const setupBenchmark = Effect.fn("ViewServerRuntime.kafka.bench.setup")(function* () {
  const jsonOrdersSourceTopic = yield* uniqueName("json-orders");
  const protobufOrdersSourceTopic = yield* uniqueName("protobuf-orders");
  const consumerGroupId = yield* uniqueName("group");
  yield* createKafkaTopics(kafkaBootstrapServers, [
    jsonOrdersSourceTopic,
    protobufOrdersSourceTopic,
  ]);
  const regions = {
    local: kafkaBootstrapServers,
  };
  const localKafkaTopic = viewServer.kafkaTopic<typeof regions>();
  const runtime = yield* makeViewServerRuntime(viewServer, {
    host: "127.0.0.1",
    websocketPort: 0,
    kafka: {
      consumerGroupId,
      regions,
      topics: {
        [jsonOrdersSourceTopic]: localKafkaTopic({
          regions: ["local"],
          value: kafka.json(IncomingJsonOrder),
          key: kafka.stringKey(),
          viewServerTopic: "jsonOrders",
          mapping: ({ key, value }) => ({
            id: key,
            customerId: value.customerId,
            price: value.price,
          }),
        }),
        [protobufOrdersSourceTopic]: localKafkaTopic({
          regions: ["local"],
          value: kafka.protobuf(OrderValueSchema),
          key: kafka.protobuf(OrderKeySchema),
          viewServerTopic: "protobufOrders",
          mapping: ({ key, value }) => ({
            id: key.orderId,
            customerId: value.customerId,
            price: value.price,
          }),
        }),
      },
    },
  });
  const stringProducer = new Producer<string, string, string, string>({
    bootstrapBrokers: [kafkaBootstrapServers],
    clientId: "view-server-kafka-ingest-bench-json-producer",
    serializers: stringSerializers,
  });
  const binaryProducer = new Producer<Buffer, Buffer, Buffer, Buffer>({
    bootstrapBrokers: [kafkaBootstrapServers],
    clientId: "view-server-kafka-ingest-bench-protobuf-producer",
  });
  return {
    binaryProducer,
    runtime,
    sourceTopics: {
      jsonOrders: jsonOrdersSourceTopic,
      protobufOrders: protobufOrdersSourceTopic,
    },
    stringProducer,
  };
});

const startedRuntime = (): ViewServerRuntime<Topics> => {
  const runtime = profile.runtime;
  if (runtime === undefined) {
    throw new Error("Kafka ingest benchmark runtime is not started.");
  }
  return runtime;
};

const sourceTopics = (): NonNullable<BenchmarkProfile["sourceTopics"]> => {
  const topics = profile.sourceTopics;
  if (topics === undefined) {
    throw new Error("Kafka ingest benchmark source topics are not configured.");
  }
  return topics;
};

const stringProducer = (): Producer<string, string, string, string> => {
  const producer = profile.stringProducer;
  if (producer === undefined) {
    throw new Error("Kafka ingest benchmark string producer is not started.");
  }
  return producer;
};

const binaryProducer = (): Producer<Buffer, Buffer, Buffer, Buffer> => {
  const producer = profile.binaryProducer;
  if (producer === undefined) {
    throw new Error("Kafka ingest benchmark binary producer is not started.");
  }
  return producer;
};

const jsonMessages = (
  sourceTopic: string,
  startIndex: number,
  count: number,
): Parameters<Producer<string, string, string, string>["send"]>[0]["messages"] =>
  Array.from({ length: count }, (_value, offset) => {
    const index = startIndex + offset;
    return {
      topic: sourceTopic,
      key: `json-order-${index}`,
      value: JSON.stringify({
        customerId: `customer-${index % 10_000}`,
        price: index % 1_000,
      }),
    };
  });

const protobufMessages = (
  sourceTopic: string,
  startIndex: number,
  count: number,
): Parameters<Producer<Buffer, Buffer, Buffer, Buffer>["send"]>[0]["messages"] =>
  Array.from({ length: count }, (_value, offset) => {
    const index = startIndex + offset;
    return {
      topic: sourceTopic,
      key: Buffer.from(
        toBinary(
          OrderKeySchema,
          create(OrderKeySchema, {
            orderId: `protobuf-order-${index}`,
          }),
        ),
      ),
      value: Buffer.from(
        toBinary(
          OrderValueSchema,
          create(OrderValueSchema, {
            customerId: `customer-${index % 10_000}`,
            price: index % 1_000,
          }),
        ),
      ),
    };
  });

const waitForRows = Effect.fn("ViewServerRuntime.kafka.bench.waitForRows")(function* (
  topic: "jsonOrders" | "protobufOrders",
  expectedRows: number,
) {
  const expectedSourceTopic =
    topic === "jsonOrders" ? sourceTopics().jsonOrders : sourceTopics().protobufOrders;
  const health = yield* startedRuntime()
    .client.health()
    .pipe(
      Effect.repeat({
        schedule: healthPollSchedule,
        until: (currentHealth) =>
          currentHealth.engine.topics[topic].rowCount === expectedRows &&
          committedOffset(currentHealth, expectedSourceTopic) === expectedRows,
      }),
    );
  const actualRows = health.engine.topics[topic].rowCount;
  const actualCommittedOffset = committedOffset(health, expectedSourceTopic);
  if (actualRows !== expectedRows || actualCommittedOffset !== expectedRows) {
    return yield* new KafkaIngestBenchmarkError({
      message: [
        `Kafka ingest benchmark ${topic} did not converge.`,
        `rows=${actualRows}/${expectedRows}`,
        `committedOffset=${actualCommittedOffset}/${expectedRows}`,
      ].join(" "),
    });
  }
  return health;
});

const committedOffset = (health: ViewServerHealth<Topics>, sourceTopic: string): number => {
  const rawOffset = health.kafka?.topics[sourceTopic]?.regions["local"]?.committedOffset;
  if (rawOffset === undefined || rawOffset === null) {
    return 0;
  }
  const parsedOffset = Number.parseInt(rawOffset, 10);
  return Number.isSafeInteger(parsedOffset) && parsedOffset >= 0 ? parsedOffset : 0;
};

const waitForFinalHealth = Effect.fn("ViewServerRuntime.kafka.bench.waitForFinalHealth")(
  function* () {
    const topics = sourceTopics();
    const health = yield* startedRuntime()
      .client.health()
      .pipe(
        Effect.repeat({
          schedule: healthPollSchedule,
          until: (health) =>
            health.engine.topics.jsonOrders.rowCount === profile.jsonProducedRows &&
            health.engine.topics.protobufOrders.rowCount === profile.protobufProducedRows &&
            committedOffset(health, topics.jsonOrders) === profile.jsonProducedRows &&
            committedOffset(health, topics.protobufOrders) === profile.protobufProducedRows,
        }),
      );
    const jsonRows = health.engine.topics.jsonOrders.rowCount;
    const protobufRows = health.engine.topics.protobufOrders.rowCount;
    const jsonOffset = committedOffset(health, topics.jsonOrders);
    const protobufOffset = committedOffset(health, topics.protobufOrders);
    if (
      jsonRows !== profile.jsonProducedRows ||
      protobufRows !== profile.protobufProducedRows ||
      jsonOffset !== profile.jsonProducedRows ||
      protobufOffset !== profile.protobufProducedRows
    ) {
      return yield* new KafkaIngestBenchmarkError({
        message: [
          "Kafka ingest benchmark final health did not converge.",
          `jsonRows=${jsonRows}/${profile.jsonProducedRows}`,
          `protobufRows=${protobufRows}/${profile.protobufProducedRows}`,
          `jsonCommittedOffset=${jsonOffset}/${profile.jsonProducedRows}`,
          `protobufCommittedOffset=${protobufOffset}/${profile.protobufProducedRows}`,
        ].join(" "),
      });
    }
    return health;
  },
);

const topicHealthValues = (health: ViewServerHealth<Topics>) =>
  internalTopicNames.map((topic) => health.engine.topics[topic]);

const cleanupLeakCountFromHealth = (health: ViewServerHealth<Topics>): number => {
  let leakCount = 0;
  for (const topicHealth of topicHealthValues(health)) {
    leakCount +=
      topicHealth.activeSubscriptions + topicHealth.activeViews + topicHealth.queuedEvents;
  }
  return leakCount;
};

const queuedEventCountFromHealth = (health: ViewServerHealth<Topics>): number => {
  let queuedEventCount = 0;
  for (const topicHealth of topicHealthValues(health)) {
    queuedEventCount += topicHealth.queuedEvents;
  }
  return queuedEventCount;
};

const backpressureCountFromHealth = (health: ViewServerHealth<Topics>): number => {
  let backpressureCount = 0;
  for (const topicHealth of topicHealthValues(health)) {
    backpressureCount += topicHealth.backpressureEvents;
  }
  return backpressureCount;
};

const timed = async <A>(operation: () => Promise<A>): Promise<readonly [A, number]> => {
  const startedAt = performance.now();
  const value = await operation();
  return [value, performance.now() - startedAt];
};

const recordIngestSample = ({
  convergenceMs,
  name,
  producerSendMs,
  rows,
  totalMs,
}: Omit<IngestSample, "rowsPerSecond">): void => {
  ingestSamples.push({
    convergenceMs,
    name,
    producerSendMs,
    rows,
    rowsPerSecond: rows / (totalMs / 1_000),
    totalMs,
  });
};

const mean = (values: ReadonlyArray<number>): number =>
  values.reduce((total, value) => total + value, 0) / values.length;

const settledValuesOrThrow = <A>(
  results: ReadonlyArray<PromiseSettledResult<A>>,
  message: string,
): ReadonlyArray<A> => {
  const values: Array<A> = [];
  const failures: Array<unknown> = [];
  for (const result of results) {
    if (result.status === "fulfilled") {
      values.push(result.value);
    } else {
      failures.push(result.reason);
    }
  }
  if (failures.length > 0) {
    throw new AggregateError(failures, message);
  }
  return values;
};

const ingestThroughputCases = (): ReadonlyArray<IngestThroughputCaseSummary> => {
  const samplesByName = Map.groupBy(ingestSamples, (sample) => sample.name);
  return [...samplesByName.entries()]
    .map(([name, samples]) => {
      const totalProducedRows = samples.reduce((total, sample) => total + sample.rows, 0);
      const totalMs = samples.reduce((total, sample) => total + sample.totalMs, 0);
      return {
        aggregateRowsPerSecond: totalProducedRows / (totalMs / 1_000),
        maxTotalMs: Math.max(...samples.map((sample) => sample.totalMs)),
        meanConvergenceMs: mean(samples.map((sample) => sample.convergenceMs)),
        meanProducerSendMs: mean(samples.map((sample) => sample.producerSendMs)),
        meanRowsPerSecond: mean(samples.map((sample) => sample.rowsPerSecond)),
        meanTotalMs: mean(samples.map((sample) => sample.totalMs)),
        minRowsPerSecond: Math.min(...samples.map((sample) => sample.rowsPerSecond)),
        name,
        producedRowsPerSample: samples[0]?.rows ?? 0,
        sampleCount: samples.length,
        totalProducedRows,
      };
    })
    .sort((left, right) => left.name.localeCompare(right.name));
};

const publishJsonBatchTimed = async (count: number) => {
  const nextTotal = profile.jsonProducedRows + count;
  const [_sendResult, producerSendMs] = await timed(() =>
    stringProducer().send({
      messages: jsonMessages(sourceTopics().jsonOrders, profile.jsonProducedRows, count),
    }),
  );
  profile.jsonProducedRows = nextTotal;
  const [_health, convergenceMs] = await timed(() =>
    Effect.runPromise(waitForRows("jsonOrders", nextTotal)),
  );
  return {
    convergenceMs,
    producerSendMs,
    rows: count,
    totalMs: producerSendMs + convergenceMs,
  };
};

const publishProtobufBatchTimed = async (count: number) => {
  const nextTotal = profile.protobufProducedRows + count;
  const [_sendResult, producerSendMs] = await timed(() =>
    binaryProducer().send({
      messages: protobufMessages(
        sourceTopics().protobufOrders,
        profile.protobufProducedRows,
        count,
      ),
    }),
  );
  profile.protobufProducedRows = nextTotal;
  const [_health, convergenceMs] = await timed(() =>
    Effect.runPromise(waitForRows("protobufOrders", nextTotal)),
  );
  return {
    convergenceMs,
    producerSendMs,
    rows: count,
    totalMs: producerSendMs + convergenceMs,
  };
};

const publishJsonBatchWithoutWaiting = async (count: number): Promise<number> => {
  const [_sendResult, producerSendMs] = await timed(() =>
    stringProducer().send({
      messages: jsonMessages(sourceTopics().jsonOrders, profile.jsonProducedRows, count),
    }),
  );
  profile.jsonProducedRows += count;
  return producerSendMs;
};

const publishProtobufBatchWithoutWaiting = async (count: number): Promise<number> => {
  const [_sendResult, producerSendMs] = await timed(() =>
    binaryProducer().send({
      messages: protobufMessages(
        sourceTopics().protobufOrders,
        profile.protobufProducedRows,
        count,
      ),
    }),
  );
  profile.protobufProducedRows += count;
  return producerSendMs;
};

const publishJsonBatch = async (count: number): Promise<void> => {
  const sample = await publishJsonBatchTimed(count);
  recordIngestSample({
    ...sample,
    name: "json source batch ingest",
  });
};

const publishProtobufBatch = async (count: number): Promise<void> => {
  const sample = await publishProtobufBatchTimed(count);
  recordIngestSample({
    ...sample,
    name: "protobuf source batch ingest",
  });
};

const publishMixedBatch = async (count: number): Promise<void> => {
  const startedAt = performance.now();
  const results = await Promise.allSettled([
    publishJsonBatchTimed(count),
    publishProtobufBatchTimed(count),
  ]);
  const samples = settledValuesOrThrow(results, "Kafka ingest benchmark mixed burst failed.");
  recordIngestSample({
    convergenceMs: Math.max(...samples.map((sample) => sample.convergenceMs)),
    name: "mixed source burst ingest",
    producerSendMs: Math.max(...samples.map((sample) => sample.producerSendMs)),
    rows: samples.reduce((total, sample) => total + sample.rows, 0),
    totalMs: performance.now() - startedAt,
  });
};

const publishSustainedMixedFirehose = async (): Promise<void> => {
  const startedAt = performance.now();
  let producerSendMs = 0;
  for (let batchIndex = 0; batchIndex < sustainedBatchCount; batchIndex += 1) {
    const results = await Promise.allSettled([
      publishJsonBatchWithoutWaiting(batchSize),
      publishProtobufBatchWithoutWaiting(batchSize),
    ]);
    const sendDurations = settledValuesOrThrow(
      results,
      `Kafka ingest benchmark sustained firehose batch ${batchIndex} failed.`,
    );
    producerSendMs += Math.max(...sendDurations);
  }
  const [_health, convergenceMs] = await timed(() => Effect.runPromise(waitForFinalHealth()));
  recordIngestSample({
    convergenceMs,
    name: "sustained mixed firehose ingest",
    producerSendMs,
    rows: batchSize * sustainedBatchCount * 2,
    totalMs: performance.now() - startedAt,
  });
};

beforeAll(async () => {
  const setup = await Effect.runPromise(setupBenchmark().pipe(Effect.provide(NodeCrypto.layer)));
  profile.binaryProducer = setup.binaryProducer;
  profile.runtime = setup.runtime;
  profile.sourceTopics = setup.sourceTopics;
  profile.stringProducer = setup.stringProducer;
  profile.memoryAfterSetup = memorySnapshot();
}, 0);

afterAll(async () => {
  const runtime = profile.runtime;
  if (runtime === undefined) {
    throw new Error("Kafka ingest benchmark runtime was not started.");
  }
  const finalSourceTopics = sourceTopics();
  const finalHealthExit = await Effect.runPromiseExit(waitForFinalHealth());
  const health = Exit.isSuccess(finalHealthExit)
    ? finalHealthExit.value
    : await Effect.runPromise(runtime.client.health());
  const producerCloseResults = await Promise.allSettled([
    profile.stringProducer?.close(),
    profile.binaryProducer?.close(),
  ]);
  if (runtime !== undefined) {
    await Effect.runPromise(runtime.close);
  }
  profile.binaryProducer = undefined;
  profile.runtime = undefined;
  profile.sourceTopics = undefined;
  profile.stringProducer = undefined;
  const memoryAfterSetup = profile.memoryAfterSetup ?? memoryBefore;
  const memoryAfterBenchmark = memorySnapshot();
  const cleanupLeakCount = cleanupLeakCountFromHealth(health);
  const backpressureCount = backpressureCountFromHealth(health);
  const queuedEventCount = queuedEventCountFromHealth(health);
  writeJsonFile(benchmarkSummaryPath(outputJsonPath), {
    artifactKind: "runtime-benchmark-summary",
    backpressureCount,
    benchmarkCases: benchmarkCaseNames,
    benchmarkName,
    benchmarkScope,
    cleanupLeakCount,
    health,
    kafka: {
      bootstrapServers: kafkaBootstrapServers,
      burstMultiplier,
      mode: benchmarkMode,
      sustainedBatchCount,
      ingestLanes: [
        {
          internalTopic: "jsonOrders",
          lane: "jsonOrders",
          producedRows: profile.jsonProducedRows,
          region: "local",
          sourceTopic: finalSourceTopics.jsonOrders,
          sourceTopicAlias: "unique-topic-per-run:jsonOrders",
        },
        {
          internalTopic: "protobufOrders",
          lane: "protobufOrders",
          producedRows: profile.protobufProducedRows,
          region: "local",
          sourceTopic: finalSourceTopics.protobufOrders,
          sourceTopicAlias: "unique-topic-per-run:protobufOrders",
        },
      ],
      sourceTopics: {
        jsonOrders: "unique-topic-per-run",
        protobufOrders: "unique-topic-per-run",
      },
    },
    latency: {
      outputJsonPath,
      source: "vitest-output-json",
    },
    memory: {
      afterBenchmark: memoryAfterBenchmark,
      afterSetup: memoryAfterSetup,
      before: memoryBefore,
      setupDelta: memoryDelta(memoryBefore, memoryAfterSetup),
      totalDelta: memoryDelta(memoryBefore, memoryAfterBenchmark),
    },
    mutationCount: profile.jsonProducedRows + profile.protobufProducedRows,
    notes: [
      "Latency percentiles are emitted by Vitest in outputJsonPath.",
      "Timed path keeps Kafka producers and View Server runtime alive, then measures producer send through health-observed runtime ingestion convergence.",
      "Sustained firehose mode sends multiple producer batches before waiting for final runtime convergence.",
      "Kafka source topics are unique per run; artifact topics stay stable as internal View Server topic names for baseline comparison.",
    ],
    queuedEventCount,
    rowCount: batchSize,
    subscriberCount: 0,
    topics: ["jsonOrders", "protobufOrders"],
    throughput: {
      cases: ingestThroughputCases(),
      source: "benchmark-operation-timers",
    },
  });
  if (Exit.isFailure(finalHealthExit)) {
    throw new Error(
      `Kafka ingest benchmark final convergence failed: ${String(finalHealthExit.cause)}`,
    );
  }
  if (cleanupLeakCount > 0) {
    throw new Error(
      `Kafka ingest benchmark cleanup leaked ${cleanupLeakCount} active resource(s).`,
    );
  }
  const failedProducerCloses = producerCloseResults.filter(
    (result) => result.status === "rejected",
  );
  if (failedProducerCloses.length > 0) {
    throw new AggregateError(
      failedProducerCloses.map((result) => result.reason),
      `Kafka ingest benchmark failed to close ${failedProducerCloses.length} producer(s).`,
    );
  }
}, 0);

const benchmarkGroupName =
  benchmarkMode === "sustained-firehose"
    ? `${benchmarkName}: ${batchSize} rows per producer batch`
    : `${benchmarkName}: ${batchSize} rows per batch`;

describe(benchmarkGroupName, () => {
  const batchBenchmarkDefinitions: ReadonlyArray<BenchmarkCase> = [
    {
      name: "json source batch ingest",
      run: async () => {
        await publishJsonBatch(batchSize);
      },
    },
    {
      name: "protobuf source batch ingest",
      run: async () => {
        await publishProtobufBatch(batchSize);
      },
    },
    {
      name: "mixed source burst ingest",
      run: async () => {
        const burstBatchSize = batchSize * burstMultiplier;
        await publishMixedBatch(burstBatchSize);
      },
    },
  ];
  const sustainedBenchmarkDefinitions: ReadonlyArray<BenchmarkCase> = [
    {
      name: "sustained mixed firehose ingest",
      run: publishSustainedMixedFirehose,
    },
  ];
  const benchmarkDefinitions =
    benchmarkMode === "sustained-firehose"
      ? sustainedBenchmarkDefinitions
      : batchBenchmarkDefinitions;

  for (const benchmarkCase of benchmarkDefinitions) {
    bench(benchmarkCase.name, benchmarkCase.run, benchOptions);
  }
});
