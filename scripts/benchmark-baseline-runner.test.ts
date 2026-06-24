import { describe, expect, it } from "@effect/vitest";
import { EventEmitter } from "node:events";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  defaultBenchmarkThresholds,
  readBenchmarkBaseline,
  writeBenchmarkBaseline,
} from "./benchmark-baseline.mjs";
import {
  childIsRunning,
  createBenchmarkTaskRunner,
  runBenchmarkBaselineCli,
} from "./benchmark-baseline-cli.mjs";
import {
  assertTaskArtifactsWritten,
  baselinePath,
  cleanBenchmarkEnvironment,
  exitCodeForSignal,
  profiles,
  removeTaskArtifacts,
  runBenchmarkBaseline,
  summaryPath,
} from "./benchmark-baseline-runner.mjs";

const vitestOutput = {
  files: [
    {
      groups: [
        {
          fullName: "src/runner-example.bench.ts > runner example benchmark group",
          benchmarks: [
            {
              max: 3,
              mean: 2,
              min: 1,
              name: "case a",
              p99: 3,
              sampleCount: 7,
            },
          ],
        },
      ],
    },
  ],
};

const summary = {
  artifactKind: "engine-benchmark-summary",
  backpressureCount: 0,
  benchmarkCases: ["case a"],
  benchmarkName: "runner example benchmark",
  benchmarkScope: "engine-runner",
  cleanupLeakCount: 0,
  latency: {
    outputJsonPath: "actual.json",
    source: "vitest-output-json",
  },
  memory: {
    totalDelta: {
      rssBytes: 1024,
    },
  },
  mutationCount: 100,
  queuedEventCount: 0,
  rowCount: 100,
  subscriberCount: 1,
  topics: ["orders"],
};

const observation = {
  artifactKind: "engine-benchmark-summary",
  backpressureCount: 0,
  benchmarkCases: ["case a"],
  benchmarkName: "runner example benchmark",
  benchmarkScope: "engine-runner",
  benchmarks: [
    {
      groupName: "src/runner-example.bench.ts > runner example benchmark group",
      maxMs: 3,
      meanMs: 2,
      minMs: 1,
      name: "case a",
      p99Ms: 3,
      sampleCount: 7,
    },
  ],
  browser: undefined,
  cleanupLeakCount: 0,
  groupedWriteAdmission: undefined,
  kafkaIngestLanes: undefined,
  latencySource: "vitest-output-json",
  memoryRssTotalDeltaBytes: 1024,
  minimumSampleCount: 5,
  mutationCount: 100,
  outputJsonPath: "actual.json",
  queuedEventCount: 0,
  rowCount: 100,
  seedBatchSize: undefined,
  subscriberCount: 1,
  summaryPath: "actual.summary.json",
  taskLabel: "task a",
  topics: ["orders"],
};

const makeDirectory = () => mkdtempSync(join(tmpdir(), "view-server-benchmark-runner-"));

const makeTask = (directory: string) => ({
  args: ["run", "--no-cache", "fake#bench"],
  command: "vp",
  env: {
    VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: "actual.json",
  },
  expectedArtifactKind: "engine-benchmark-summary",
  expectedBenchmarkScope: "engine-runner",
  expectedRowCount: 100,
  label: "task a",
  minimumSampleCount: 5,
  outputJsonPath: join(directory, "actual.json"),
  packageOutputJsonPath: "actual.json",
  summaryPath: join(directory, "actual.summary.json"),
});

const writeArtifacts = (
  task: ReturnType<typeof makeTask>,
  nextSummary = summary,
  nextVitestOutput = vitestOutput,
) => {
  mkdirSync(join(task.outputJsonPath, ".."), { recursive: true });
  writeFileSync(task.outputJsonPath, `${JSON.stringify(nextVitestOutput)}\n`);
  writeFileSync(task.summaryPath, `${JSON.stringify(nextSummary)}\n`);
};

const silentLogger = () => {
  const messages: Array<string> = [];
  return {
    logger: {
      error: (message: string) => {
        messages.push(message);
      },
      log: (message: string) => {
        messages.push(message);
      },
    },
    messages,
  };
};

class FakeChildProcess extends EventEmitter {
  exitCode: number | null = null;
  killedSignals: Array<string> = [];
  signalCode: string | null = null;

  kill(signal: string) {
    this.killedSignals.push(signal);
    this.signalCode = signal;
    return true;
  }
}

describe("benchmark baseline runner", () => {
  it("computes runner utility values", () => {
    expect({
      baseline: baselinePath("smoke"),
      cleanedEnvironment: cleanBenchmarkEnvironment({
        KEEP_ME: "yes",
        VIEW_SERVER_BENCH_BASELINE_PROFILE: "smoke",
        VIEW_SERVER_ENGINE_BENCH_ROWS: "100",
        VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS: "localhost:19092",
        VIEW_SERVER_REACT_BENCH_ROWS: "100",
        VIEW_SERVER_RUNTIME_BENCH_KAFKA_BATCH_SIZE: "100",
        VITE_VIEW_SERVER_REACT_BENCH_ROWS: "100",
      }),
      knownSignalExitCode: exitCodeForSignal("SIGTERM"),
      nonJsonSummary: summaryPath(".artifacts/result"),
      summary: summaryPath(".artifacts/result.json"),
      unknownSignalExitCode: exitCodeForSignal("NOT_A_SIGNAL"),
    }).toStrictEqual({
      baseline: "benchmarks/baselines/smoke.json",
      cleanedEnvironment: {
        KEEP_ME: "yes",
      },
      knownSignalExitCode: 143,
      nonJsonSummary: ".artifacts/result.summary.json",
      summary: ".artifacts/result.summary.json",
      unknownSignalExitCode: 1,
    });
  });

  it("keeps targeted grouped baseline scripts in compare mode by default", () => {
    const scripts = JSON.parse(readFileSync("package.json", "utf8")).scripts;

    expect({
      activeQuerySharing: scripts["bench:baseline:active-query-sharing"],
      activeQuerySharingUpdate: scripts["bench:baseline:active-query-sharing:update"],
      groupedAdmission: scripts["bench:baseline:grouped-admission"],
      groupedAdmissionUpdate: scripts["bench:baseline:grouped-admission:update"],
      groupedOrderNeutral: scripts["bench:baseline:grouped-order-neutral"],
      groupedOrderNeutralUpdate: scripts["bench:baseline:grouped-order-neutral:update"],
      kafkaIngest: scripts["bench:baseline:kafka-ingest"],
      kafkaIngestUpdate: scripts["bench:baseline:kafka-ingest:update"],
      kafkaSustainedFirehose: scripts["bench:baseline:kafka-sustained-firehose"],
      kafkaSustainedFirehoseUpdate: scripts["bench:baseline:kafka-sustained-firehose:update"],
      rawReadWrite: scripts["bench:baseline:raw-read-write"],
      rawReadWriteUpdate: scripts["bench:baseline:raw-read-write:update"],
      release: scripts["bench:baseline:release"],
      webSocketFirehose: scripts["bench:baseline:websocket-firehose"],
      webSocketFirehoseUpdate: scripts["bench:baseline:websocket-firehose:update"],
    }).toStrictEqual({
      activeQuerySharing:
        "node scripts/run-benchmark-baseline.mjs --profile=active-query-sharing",
      activeQuerySharingUpdate:
        "node scripts/run-benchmark-baseline.mjs --profile=active-query-sharing --update-baseline",
      groupedAdmission: "node scripts/run-benchmark-baseline.mjs --profile=grouped-admission",
      groupedAdmissionUpdate:
        "node scripts/run-benchmark-baseline.mjs --profile=grouped-admission --update-baseline",
      groupedOrderNeutral: "node scripts/run-benchmark-baseline.mjs --profile=grouped-order-neutral",
      groupedOrderNeutralUpdate:
        "node scripts/run-benchmark-baseline.mjs --profile=grouped-order-neutral --update-baseline",
      kafkaIngest: "node scripts/run-benchmark-baseline.mjs --profile=kafka-ingest",
      kafkaIngestUpdate:
        "node scripts/run-benchmark-baseline.mjs --profile=kafka-ingest --update-baseline",
      kafkaSustainedFirehose:
        "node scripts/run-benchmark-baseline.mjs --profile=kafka-sustained-firehose",
      kafkaSustainedFirehoseUpdate:
        "node scripts/run-benchmark-baseline.mjs --profile=kafka-sustained-firehose --update-baseline",
      rawReadWrite: "node scripts/run-benchmark-baseline.mjs --profile=raw-read-write",
      rawReadWriteUpdate:
        "node scripts/run-benchmark-baseline.mjs --profile=raw-read-write --update-baseline",
      release: "node scripts/run-benchmark-baseline.mjs --profile=release --no-compare",
      webSocketFirehose: "node scripts/run-benchmark-baseline.mjs --profile=websocket-firehose",
      webSocketFirehoseUpdate:
        "node scripts/run-benchmark-baseline.mjs --profile=websocket-firehose --update-baseline",
    });
  });

  it("defines raw read and write performance gate tasks", () => {
    const rawReadWriteTasks = profiles.get("raw-read-write") ?? [];

    expect(
      rawReadWriteTasks.map((task) => ({
        batchSize: task.env["VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE"],
        benchmarkScope: task.expectedBenchmarkScope,
        iterations: task.env["VIEW_SERVER_ENGINE_BENCH_ITERATIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_ENGINE_BENCH_ROWS"],
        task: task.args,
        timeMs: task.env["VIEW_SERVER_ENGINE_BENCH_TIME_MS"],
        writeMode: task.env["VIEW_SERVER_ENGINE_BENCH_WRITE_MODE"],
      })),
    ).toStrictEqual([
      {
        batchSize: undefined,
        benchmarkScope: "engine-raw-snapshot",
        iterations: "20",
        outputJsonPath: ".artifacts/raw-snapshot-100000rows.json",
        rowCount: "100000",
        task: ["run", "--no-cache", "column-live-view-engine#bench:raw-snapshot"],
        timeMs: "1",
        writeMode: undefined,
      },
      {
        batchSize: undefined,
        benchmarkScope: "engine-raw-predicate-index",
        iterations: "20",
        outputJsonPath: ".artifacts/raw-predicate-index-100000rows.json",
        rowCount: "100000",
        task: ["run", "--no-cache", "column-live-view-engine#bench:raw-predicate-index"],
        timeMs: "1",
        writeMode: undefined,
      },
      {
        batchSize: "1000",
        benchmarkScope: "engine-raw-write",
        iterations: "20",
        outputJsonPath: ".artifacts/raw-write-base-100000rows.json",
        rowCount: "100000",
        task: ["run", "--no-cache", "column-live-view-engine#bench:raw-write"],
        timeMs: "0",
        writeMode: "base",
      },
      {
        batchSize: "1000",
        benchmarkScope: "engine-raw-write",
        iterations: "20",
        outputJsonPath: ".artifacts/raw-write-indexed-100000rows.json",
        rowCount: "100000",
        task: ["run", "--no-cache", "column-live-view-engine#bench:raw-write"],
        timeMs: "0",
        writeMode: "indexed",
      },
    ]);
  });

  it("defines exact raw write smoke tasks", () => {
    const rawWriteSmokeTasks = (profiles.get("smoke") ?? []).filter(
      (task) => task.expectedBenchmarkScope === "engine-raw-write",
    );

    expect(
      rawWriteSmokeTasks.map((task) => ({
        batchSize: task.env["VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE"],
        iterations: task.env["VIEW_SERVER_ENGINE_BENCH_ITERATIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        timeMs: task.env["VIEW_SERVER_ENGINE_BENCH_TIME_MS"],
        writeMode: task.env["VIEW_SERVER_ENGINE_BENCH_WRITE_MODE"],
      })),
    ).toStrictEqual([
      {
        batchSize: "100",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-write-base-1000rows.json",
        timeMs: "0",
        writeMode: "base",
      },
      {
        batchSize: "100",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-write-indexed-1000rows.json",
        timeMs: "0",
        writeMode: "indexed",
      },
    ]);
  });

  it("defines active-query sharing fanout tasks", () => {
    const activeQuerySharingTasks = profiles.get("active-query-sharing") ?? [];

    expect(
      activeQuerySharingTasks.map((task) => ({
        batchSize: task.env["VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE"],
        benchmarkScope: task.expectedBenchmarkScope,
        fanoutCase: task.env["VIEW_SERVER_ENGINE_BENCH_FANOUT_CASE"],
        iterations: task.env["VIEW_SERVER_ENGINE_BENCH_ITERATIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_ENGINE_BENCH_ROWS"],
        subscriberCount: task.env["VIEW_SERVER_ENGINE_BENCH_SUBSCRIBERS"],
        task: task.args,
        timeMs: task.env["VIEW_SERVER_ENGINE_BENCH_TIME_MS"],
      })),
    ).toStrictEqual([
      {
        batchSize: "1000",
        benchmarkScope: "engine-raw-live-fanout",
        fanoutCase: "same-window",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-live-fanout-same-window-10000rows-50subs.json",
        rowCount: "10000",
        subscriberCount: "50",
        task: ["run", "--no-cache", "column-live-view-engine#bench:raw-live-fanout"],
        timeMs: "1",
      },
      {
        batchSize: "1000",
        benchmarkScope: "engine-raw-live-fanout",
        fanoutCase: "ten-window",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-live-fanout-ten-window-10000rows-50subs.json",
        rowCount: "10000",
        subscriberCount: "50",
        task: ["run", "--no-cache", "column-live-view-engine#bench:raw-live-fanout"],
        timeMs: "1",
      },
      {
        batchSize: "1000",
        benchmarkScope: "engine-raw-live-fanout",
        fanoutCase: "unique-window",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-live-fanout-unique-window-10000rows-50subs.json",
        rowCount: "10000",
        subscriberCount: "50",
        task: ["run", "--no-cache", "column-live-view-engine#bench:raw-live-fanout"],
        timeMs: "1",
      },
      {
        batchSize: "1000",
        benchmarkScope: "engine-raw-live-fanout",
        fanoutCase: "unique-shape",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-live-fanout-unique-shape-10000rows-50subs.json",
        rowCount: "10000",
        subscriberCount: "50",
        task: ["run", "--no-cache", "column-live-view-engine#bench:raw-live-fanout"],
        timeMs: "1",
      },
    ]);
  });

  it("defines the Kafka ingest runtime benchmark task", () => {
    const kafkaIngestTasks = profiles.get("kafka-ingest") ?? [];

    expect(
      kafkaIngestTasks.map((task) => ({
        artifactKind: task.expectedArtifactKind,
        benchmarkScope: task.expectedBenchmarkScope,
        broker: task.env["VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_RUNTIME_BENCH_KAFKA_BATCH_SIZE"],
        task: task.args,
      })),
    ).toStrictEqual([
      {
        artifactKind: "runtime-benchmark-summary",
        benchmarkScope: "runtime-kafka-ingest",
        broker: "localhost:9092",
        outputJsonPath: ".artifacts/kafka-ingest-250rows.json",
        rowCount: "250",
        task: ["run", "--no-cache", "runtime#bench:kafka-ingest"],
      },
    ]);
  });

  it("defines the Kafka sustained firehose runtime benchmark task", () => {
    const kafkaSustainedFirehoseTasks = profiles.get("kafka-sustained-firehose") ?? [];

    expect(
      kafkaSustainedFirehoseTasks.map((task) => ({
        artifactKind: task.expectedArtifactKind,
        benchmarkMode: task.env["VIEW_SERVER_RUNTIME_BENCH_KAFKA_MODE"],
        benchmarkScope: task.expectedBenchmarkScope,
        broker: task.env["VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_RUNTIME_BENCH_KAFKA_BATCH_SIZE"],
        sustainedBatches: task.env["VIEW_SERVER_RUNTIME_BENCH_KAFKA_SUSTAINED_BATCHES"],
        task: task.args,
      })),
    ).toStrictEqual([
      {
        artifactKind: "runtime-benchmark-summary",
        benchmarkMode: "sustained-firehose",
        benchmarkScope: "runtime-kafka-sustained-firehose",
        broker: "localhost:9092",
        outputJsonPath: ".artifacts/kafka-sustained-firehose-250rows-4batches.json",
        rowCount: "250",
        sustainedBatches: "4",
        task: ["run", "--no-cache", "runtime#bench:kafka-ingest"],
      },
    ]);
  });

  it("defines the WebSocket firehose runtime benchmark tasks", () => {
    const webSocketFirehoseTasks = profiles.get("websocket-firehose") ?? [];

    expect(
      webSocketFirehoseTasks.map((task) => ({
        artifactKind: task.expectedArtifactKind,
        benchmarkCase: task.env["VIEW_SERVER_RUNTIME_BENCH_WEBSOCKET_CASE"],
        benchmarkScope: task.expectedBenchmarkScope,
        iterations: task.env["VIEW_SERVER_RUNTIME_BENCH_ITERATIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_RUNTIME_BENCH_WEBSOCKET_ROWS"],
        subscriberCount: task.env["VIEW_SERVER_RUNTIME_BENCH_WEBSOCKET_SUBSCRIBERS"],
        task: task.args,
        timeMs: task.env["VIEW_SERVER_RUNTIME_BENCH_TIME_MS"],
      })),
    ).toStrictEqual([
      {
        artifactKind: "runtime-benchmark-summary",
        benchmarkCase: "same-window",
        benchmarkScope: "runtime-websocket-firehose",
        iterations: "5",
        outputJsonPath: ".artifacts/websocket-firehose-same-window-1000rows-10subs.json",
        rowCount: "1000",
        subscriberCount: "10",
        task: ["run", "--no-cache", "runtime#bench:websocket-firehose"],
        timeMs: "1",
      },
      {
        artifactKind: "runtime-benchmark-summary",
        benchmarkCase: "ten-window",
        benchmarkScope: "runtime-websocket-firehose",
        iterations: "5",
        outputJsonPath: ".artifacts/websocket-firehose-ten-window-1000rows-10subs.json",
        rowCount: "1000",
        subscriberCount: "10",
        task: ["run", "--no-cache", "runtime#bench:websocket-firehose"],
        timeMs: "1",
      },
    ]);
  });

  it("defines isolated grouped order-neutral tasks without changing dual grouped-write artifacts", () => {
    const groupedOrderNeutralTasks = profiles.get("grouped-order-neutral") ?? [];
    const releaseGroupedWriteTasks = (profiles.get("release") ?? []).filter((task) =>
      task.label.startsWith("grouped write "),
    );
    const smokeGroupedWriteTasks = (profiles.get("smoke") ?? []).filter((task) =>
      task.label.startsWith("grouped write "),
    );

    expect(
      groupedOrderNeutralTasks.map((task) => ({
        outputJsonPath: task.packageOutputJsonPath,
        readerProfile: task.env["VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_READER_PROFILE"],
        rowCount: task.env["VIEW_SERVER_ENGINE_BENCH_ROWS"],
      })),
    ).toStrictEqual([
      {
        outputJsonPath: ".artifacts/grouped-write-incremental-order-neutral-100000rows-1mutations.json",
        readerProfile: "order-neutral",
        rowCount: "100000",
      },
      {
        outputJsonPath:
          ".artifacts/grouped-write-incremental-order-neutral-1000000rows-1mutations.json",
        readerProfile: "order-neutral",
        rowCount: "1000000",
      },
      {
        outputJsonPath:
          ".artifacts/grouped-write-incremental-order-neutral-5000000rows-1mutations.json",
        readerProfile: "order-neutral",
        rowCount: "5000000",
      },
    ]);
    expect(
      smokeGroupedWriteTasks.map((task) => task.packageOutputJsonPath),
    ).toStrictEqual([".artifacts/grouped-write-incremental-1000rows-1mutations.json"]);
    expect(
      releaseGroupedWriteTasks.map((task) => task.packageOutputJsonPath),
    ).toStrictEqual([
      ".artifacts/grouped-write-incremental-100000rows-1mutations.json",
      ".artifacts/grouped-write-incremental-1000000rows-1mutations.json",
      ".artifacts/grouped-write-incremental-5000000rows-1mutations.json",
    ]);
  });

  it("defines grouped key width smoke and release tasks", () => {
    const smokeGroupedKeyWidthTasks = (profiles.get("smoke") ?? []).filter((task) =>
      task.label.startsWith("grouped key width "),
    );
    const releaseGroupedKeyWidthTasks = (profiles.get("release") ?? []).filter((task) =>
      task.label.startsWith("grouped key width "),
    );

    expect(
      smokeGroupedKeyWidthTasks.map((task) => ({
        benchmarkScope: task.expectedBenchmarkScope,
        iterations: task.env["VIEW_SERVER_ENGINE_BENCH_ITERATIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_ENGINE_BENCH_ROWS"],
        timeMs: task.env["VIEW_SERVER_ENGINE_BENCH_TIME_MS"],
      })),
    ).toStrictEqual([
      {
        benchmarkScope: "engine-grouped-key-width",
        iterations: "5",
        outputJsonPath: ".artifacts/grouped-key-width-1000rows.json",
        rowCount: "1000",
        timeMs: "1",
      },
    ]);
    expect(
      releaseGroupedKeyWidthTasks.map((task) => ({
        benchmarkScope: task.expectedBenchmarkScope,
        iterations: task.env["VIEW_SERVER_ENGINE_BENCH_ITERATIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_ENGINE_BENCH_ROWS"],
        timeMs: task.env["VIEW_SERVER_ENGINE_BENCH_TIME_MS"],
      })),
    ).toStrictEqual([
      {
        benchmarkScope: "engine-grouped-key-width",
        iterations: "3",
        outputJsonPath: ".artifacts/grouped-key-width-100000rows.json",
        rowCount: "100000",
        timeMs: "0",
      },
      {
        benchmarkScope: "engine-grouped-key-width",
        iterations: "3",
        outputJsonPath: ".artifacts/grouped-key-width-1000000rows.json",
        rowCount: "1000000",
        timeMs: "0",
      },
    ]);
  });

  it("defines query delta operation smoke and release tasks", () => {
    const smokeDeltaTasks = (profiles.get("smoke") ?? []).filter((task) =>
      task.label.startsWith("query delta operations "),
    );
    const releaseDeltaTasks = (profiles.get("release") ?? []).filter((task) =>
      task.label.startsWith("query delta operations "),
    );

    expect(
      smokeDeltaTasks.map((task) => ({
        benchmarkScope: task.expectedBenchmarkScope,
        caseName: task.env["VIEW_SERVER_ENGINE_BENCH_DELTA_OPERATION_CASE"],
        operationCount: task.env["VIEW_SERVER_ENGINE_BENCH_DELTA_OPERATION_COUNT"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_ENGINE_BENCH_ROWS"],
      })),
    ).toStrictEqual([
      {
        benchmarkScope: "engine-query-delta-operations",
        caseName: "head-replacement-batch",
        operationCount: "16",
        outputJsonPath:
          ".artifacts/query-delta-operations-head-replacement-batch-1000rows-32ops.json",
        rowCount: "1000",
      },
    ]);
    expect(
      releaseDeltaTasks.map((task) => ({
        benchmarkScope: task.expectedBenchmarkScope,
        caseName: task.env["VIEW_SERVER_ENGINE_BENCH_DELTA_OPERATION_CASE"],
        operationCount: task.env["VIEW_SERVER_ENGINE_BENCH_DELTA_OPERATION_COUNT"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_ENGINE_BENCH_ROWS"],
      })),
    ).toStrictEqual([
      {
        benchmarkScope: "engine-query-delta-operations",
        caseName: "head-replacement-batch",
        operationCount: "64",
        outputJsonPath:
          ".artifacts/query-delta-operations-head-replacement-batch-10000rows-128ops.json",
        rowCount: "10000",
      },
      {
        benchmarkScope: "engine-query-delta-operations",
        caseName: "middle-replacement-batch",
        operationCount: "64",
        outputJsonPath:
          ".artifacts/query-delta-operations-middle-replacement-batch-10000rows-128ops.json",
        rowCount: "10000",
      },
      {
        benchmarkScope: "engine-query-delta-operations",
        caseName: "tail-replacement-batch",
        operationCount: "64",
        outputJsonPath:
          ".artifacts/query-delta-operations-tail-replacement-batch-10000rows-128ops.json",
        rowCount: "10000",
      },
    ]);
  });

  it("defines retained delta move cases for smoke and release baseline gates", () => {
    const smokeRetainedDeltaTasks = (profiles.get("smoke") ?? []).filter((task) =>
      task.label.startsWith("raw active retained delta "),
    );
    const releaseRetainedDeltaTasks = (profiles.get("release") ?? []).filter((task) =>
      task.label.startsWith("raw active retained delta "),
    );

    expect(
      smokeRetainedDeltaTasks.map((task) => ({
        caseName: task.env["VIEW_SERVER_ENGINE_BENCH_RETAINED_CASE"],
        iterations: task.env["VIEW_SERVER_ENGINE_BENCH_ITERATIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_ENGINE_BENCH_ROWS"],
      })),
    ).toStrictEqual([
      {
        caseName: "noop",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-active-retained-delta-noop-101rows.json",
        rowCount: "101",
      },
      {
        caseName: "match-update",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-active-retained-delta-match-update-101rows.json",
        rowCount: "101",
      },
      {
        caseName: "match-move-down",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-active-retained-delta-match-move-down-101rows.json",
        rowCount: "101",
      },
      {
        caseName: "match-replacement-batch",
        iterations: "5",
        outputJsonPath:
          ".artifacts/raw-active-retained-delta-match-replacement-batch-101rows.json",
        rowCount: "101",
      },
      {
        caseName: "predicate-enter",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-active-retained-delta-predicate-enter-101rows.json",
        rowCount: "101",
      },
      {
        caseName: "visible-delete",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-active-retained-delta-visible-delete-101rows.json",
        rowCount: "101",
      },
      {
        caseName: "exhausted-lookahead",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-active-retained-delta-exhausted-lookahead-101rows.json",
        rowCount: "101",
      },
      {
        caseName: "count-only",
        iterations: "5",
        outputJsonPath: ".artifacts/raw-active-retained-delta-count-only-101rows.json",
        rowCount: "101",
      },
    ]);
    expect(
      releaseRetainedDeltaTasks.map((task) => ({
        batchSize: task.env["VIEW_SERVER_ENGINE_BENCH_REPLACEMENT_BATCH_SIZE"],
        caseName: task.env["VIEW_SERVER_ENGINE_BENCH_RETAINED_CASE"],
        iterations: task.env["VIEW_SERVER_ENGINE_BENCH_ITERATIONS"],
        outputJsonPath: task.packageOutputJsonPath,
        rowCount: task.env["VIEW_SERVER_ENGINE_BENCH_ROWS"],
        windowLimit: task.env["VIEW_SERVER_ENGINE_BENCH_RETAINED_WINDOW_LIMIT"],
      })),
    ).toStrictEqual([
      {
        batchSize: undefined,
        caseName: "noop",
        iterations: "100",
        outputJsonPath: ".artifacts/raw-active-retained-delta-noop-100000rows.json",
        rowCount: "100000",
        windowLimit: undefined,
      },
      {
        batchSize: undefined,
        caseName: "noop",
        iterations: "100",
        outputJsonPath:
          ".artifacts/raw-active-retained-delta-noop-100000rows-1000limit-2batch.json",
        rowCount: "100000",
        windowLimit: "1000",
      },
      {
        batchSize: undefined,
        caseName: "match-update",
        iterations: "100",
        outputJsonPath: ".artifacts/raw-active-retained-delta-match-update-100000rows.json",
        rowCount: "100000",
        windowLimit: undefined,
      },
      {
        batchSize: undefined,
        caseName: "match-move-down",
        iterations: "49",
        outputJsonPath: ".artifacts/raw-active-retained-delta-match-move-down-100000rows.json",
        rowCount: "100000",
        windowLimit: undefined,
      },
      {
        batchSize: undefined,
        caseName: "match-replacement-batch",
        iterations: "24",
        outputJsonPath:
          ".artifacts/raw-active-retained-delta-match-replacement-batch-100000rows.json",
        rowCount: "100000",
        windowLimit: undefined,
      },
      {
        batchSize: "64",
        caseName: "match-replacement-batch",
        iterations: "5",
        outputJsonPath:
          ".artifacts/raw-active-retained-delta-match-replacement-batch-100000rows-1000limit-64batch.json",
        rowCount: "100000",
        windowLimit: "1000",
      },
      {
        batchSize: "16",
        caseName: "visible-delete-batch",
        iterations: "4",
        outputJsonPath:
          ".artifacts/raw-active-retained-delta-visible-delete-batch-100000rows-1000limit-16batch.json",
        rowCount: "100000",
        windowLimit: "1000",
      },
      {
        batchSize: undefined,
        caseName: "predicate-enter",
        iterations: "100",
        outputJsonPath: ".artifacts/raw-active-retained-delta-predicate-enter-100000rows.json",
        rowCount: "100000",
        windowLimit: undefined,
      },
      {
        batchSize: undefined,
        caseName: "visible-delete",
        iterations: "100",
        outputJsonPath: ".artifacts/raw-active-retained-delta-visible-delete-100000rows.json",
        rowCount: "100000",
        windowLimit: undefined,
      },
      {
        batchSize: undefined,
        caseName: "exhausted-lookahead",
        iterations: "100",
        outputJsonPath: ".artifacts/raw-active-retained-delta-exhausted-lookahead-100000rows.json",
        rowCount: "100000",
        windowLimit: undefined,
      },
      {
        batchSize: undefined,
        caseName: "count-only",
        iterations: "100",
        outputJsonPath: ".artifacts/raw-active-retained-delta-count-only-100000rows.json",
        rowCount: "100000",
        windowLimit: undefined,
      },
    ]);
  });

  it("updates and compares a tiny profile with fresh artifacts", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profileMap = new Map([["tiny", [task]]]);
    const baselineFile = join(directory, "baseline.json");
    const capturedEnvironments: Array<Record<string, string>> = [];
    const { logger } = silentLogger();
    const runTask = async (currentTask: typeof task) => {
      capturedEnvironments.push(currentTask.env);
      writeArtifacts(currentTask);
      return 0;
    };

    const updateExitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny", "--update-baseline"],
      baselinePathForProfile: () => baselineFile,
      environment: {
        KEEP_ME: "yes",
        VIEW_SERVER_ENGINE_BENCH_ROWS: "stale",
      },
      logger,
      profileMap,
      runTask,
    });
    const compareExitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny"],
      baselinePathForProfile: () => baselineFile,
      environment: {
        KEEP_ME: "yes",
      },
      logger,
      profileMap,
      runTask,
    });

    expect({
      baseline: readBenchmarkBaseline(baselineFile),
      capturedEnvironments,
      compareExitCode,
      updateExitCode,
    }).toStrictEqual({
      baseline: {
        artifactKind: "view-server-benchmark-baseline",
        profile: "tiny",
        tasks: [
          {
            ...observation,
            groupedKeyWidthParameters: undefined,
            outputJsonPath: task.outputJsonPath,
            summaryPath: task.summaryPath,
            throughputCases: undefined,
          },
        ],
        thresholds: defaultBenchmarkThresholds,
      },
      capturedEnvironments: [
        {
          KEEP_ME: "yes",
          VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: "actual.json",
        },
        {
          KEEP_ME: "yes",
          VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: "actual.json",
        },
      ],
      compareExitCode: 0,
      updateExitCode: 0,
    });
  });

  it("runs no-compare profiles without an existing baseline", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profileMap = new Map([["tiny", [task]]]);
    const { logger } = silentLogger();

    const exitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny", "--no-compare"],
      baselinePathForProfile: () => join(directory, "missing-baseline.json"),
      environment: {},
      logger,
      profileMap,
      runTask: async (currentTask: typeof task) => {
        writeArtifacts(currentTask);
        return 0;
      },
    });

    expect(exitCode).toBe(0);
  });

  it("uses the profile from the benchmark environment when no profile argument is provided", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profileMap = new Map([["tiny", [task]]]);
    const { logger } = silentLogger();

    const exitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--no-compare"],
      baselinePathForProfile: () => join(directory, "missing-baseline.json"),
      environment: {
        VIEW_SERVER_BENCH_BASELINE_PROFILE: "tiny",
      },
      logger,
      profileMap,
      runTask: async (currentTask: typeof task) => {
        writeArtifacts(currentTask);
        return 0;
      },
    });

    expect(exitCode).toBe(0);
  });

  it("uses the smoke profile by default", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profileMap = new Map([["smoke", [task]]]);
    const { logger } = silentLogger();

    const exitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--no-compare"],
      baselinePathForProfile: () => join(directory, "missing-baseline.json"),
      environment: {},
      logger,
      profileMap,
      runTask: async (currentTask: typeof task) => {
        writeArtifacts(currentTask);
        return 0;
      },
    });

    expect(exitCode).toBe(0);
  });


  it("returns child task failures without reading stale artifacts", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profileMap = new Map([["tiny", [task]]]);
    const { logger } = silentLogger();

    writeArtifacts(task);
    const exitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny"],
      baselinePathForProfile: () => join(directory, "baseline.json"),
      environment: {},
      logger,
      profileMap,
      runTask: async () => 42,
    });

    expect({
      exitCode,
      outputStillExists: existsSync(task.outputJsonPath),
      summaryStillExists: existsSync(task.summaryPath),
    }).toStrictEqual({
      exitCode: 42,
      outputStillExists: false,
      summaryStillExists: false,
    });
  });

  it("rejects successful tasks that do not write expected artifacts", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profileMap = new Map([["tiny", [task]]]);
    const { logger } = silentLogger();

    await expect(
      runBenchmarkBaseline({
        argv: ["node", "script", "--profile=tiny"],
        baselinePathForProfile: () => join(directory, "baseline.json"),
        environment: {},
        logger,
        profileMap,
        runTask: async () => 0,
      }),
    ).rejects.toThrow(`${task.label}: missing benchmark output ${task.outputJsonPath}.`);
  });

  it("rejects missing summaries after output was written", () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    writeFileSync(task.outputJsonPath, `${JSON.stringify(vitestOutput)}\n`);

    expect(() => assertTaskArtifactsWritten(task)).toThrow(
      `${task.label}: missing benchmark summary ${task.summaryPath}.`,
    );
  });

  it("returns comparison failures", async () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    const profileMap = new Map([["tiny", [task]]]);
    const baselineFile = join(directory, "baseline.json");
    const { logger, messages } = silentLogger();
    writeBenchmarkBaseline(baselineFile, {
      artifactKind: "view-server-benchmark-baseline",
      profile: "tiny",
      tasks: [
        {
          ...observation,
          outputJsonPath: task.outputJsonPath,
          summaryPath: task.summaryPath,
        },
      ],
      thresholds: defaultBenchmarkThresholds,
    });

    const exitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=tiny"],
      baselinePathForProfile: () => baselineFile,
      environment: {},
      logger,
      profileMap,
      runTask: async (currentTask: typeof task) => {
        writeArtifacts(currentTask, summary, {
          files: [
            {
              groups: [
                {
                  fullName: "src/runner-example.bench.ts > runner example benchmark group",
                  benchmarks: [
                    {
                      max: 100,
                      mean: 100,
                      min: 100,
                      name: "case a",
                      p99: 100,
                      sampleCount: 5,
                    },
                  ],
                },
              ],
            },
          ],
        });
        return 0;
      },
    });

    expect({
      exitCode,
      firstError: messages[3],
    }).toStrictEqual({
      exitCode: 1,
      firstError:
        "\ntiny benchmark baseline regressed:\n- task a / src/runner-example.bench.ts > runner example benchmark group / case a: mean regressed from 2.000ms to 100.000ms; allowed <= 16.000ms.\n- task a / src/runner-example.bench.ts > runner example benchmark group / case a: p99 regressed from 3.000ms to 100.000ms; allowed <= 24.000ms.",
    });
  });

  it("runs benchmark child processes and maps process exit states", async () => {
    const processLike = new EventEmitter();
    const child = new FakeChildProcess();
    const spawned: Array<{
      args: ReadonlyArray<string>;
      command: string;
      env: Record<string, string>;
      stdio: string;
    }> = [];
    const taskRunner = createBenchmarkTaskRunner({
      processLike,
      spawn: (command, args, options) => {
        spawned.push({
          args,
          command,
          env: options.env,
          stdio: options.stdio,
        });
        return child;
      },
    });
    const task = makeTask(makeDirectory());

    const exitCodePromise = taskRunner.runTask(task);
    child.exitCode = 7;
    child.emit("exit", 7, null);

    await expect(exitCodePromise).resolves.toBe(7);
    expect({
      childIsRunningAfterExit: childIsRunning(child),
      spawned,
    }).toStrictEqual({
      childIsRunningAfterExit: false,
      spawned: [
        {
          args: ["run", "--no-cache", "fake#bench"],
          command: "vp",
          env: {
            VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: "actual.json",
          },
          stdio: "inherit",
        },
      ],
    });
  });

  it("reports child running state from exit and signal fields", () => {
    const running = new FakeChildProcess();
    const exited = new FakeChildProcess();
    const signalled = new FakeChildProcess();
    exited.exitCode = 0;
    signalled.signalCode = "SIGTERM";

    expect({
      exited: childIsRunning(exited),
      running: childIsRunning(running),
      signalled: childIsRunning(signalled),
    }).toStrictEqual({
      exited: false,
      running: true,
      signalled: false,
    });
  });

  it("ignores direct termination when no benchmark child is active", () => {
    const taskRunner = createBenchmarkTaskRunner({
      processLike: new EventEmitter(),
      spawn: () => new FakeChildProcess(),
    });

    expect(taskRunner.terminateActiveChild("SIGTERM")).toBeUndefined();
  });

  it("returns the recorded parent termination code without spawning a future task", async () => {
    const processLike = new EventEmitter();
    const spawnedCommands: Array<string> = [];
    const taskRunner = createBenchmarkTaskRunner({
      processLike,
      spawn: (command) => {
        spawnedCommands.push(command);
        return new FakeChildProcess();
      },
    });

    processLike.emit("SIGTERM");

    await expect(taskRunner.runTask(makeTask(makeDirectory()))).resolves.toBe(143);
    expect(spawnedCommands).toStrictEqual([]);
  });

  it("leaves the active benchmark child intact when an older child exits", async () => {
    const firstChild = new FakeChildProcess();
    const secondChild = new FakeChildProcess();
    const children = [firstChild, secondChild];
    const taskRunner = createBenchmarkTaskRunner({
      processLike: new EventEmitter(),
      spawn: () => children.shift(),
    });

    const firstExitCode = taskRunner.runTask(makeTask(makeDirectory()));
    const secondExitCode = taskRunner.runTask(makeTask(makeDirectory()));
    firstChild.exitCode = 0;
    firstChild.emit("exit", 0, null);
    taskRunner.terminateActiveChild("SIGTERM");
    secondChild.emit("exit", null, "SIGTERM");

    await expect(firstExitCode).resolves.toBe(0);
    await expect(secondExitCode).resolves.toBe(143);
    expect(secondChild.killedSignals).toStrictEqual(["SIGTERM"]);
  });

  it("keeps the active benchmark child when an older child reports an error", async () => {
    const firstChild = new FakeChildProcess();
    const secondChild = new FakeChildProcess();
    const children = [firstChild, secondChild];
    const taskRunner = createBenchmarkTaskRunner({
      processLike: new EventEmitter(),
      spawn: () => children.shift(),
    });

    const firstExitCode = taskRunner.runTask(makeTask(makeDirectory()));
    const secondExitCode = taskRunner.runTask(makeTask(makeDirectory()));
    firstChild.emit("error", new Error("old child failed"));
    taskRunner.terminateActiveChild("SIGTERM");
    secondChild.emit("exit", null, "SIGTERM");

    await expect(firstExitCode).rejects.toThrow("old child failed");
    await expect(secondExitCode).resolves.toBe(143);
    expect(secondChild.killedSignals).toStrictEqual(["SIGTERM"]);
  });

  it("forwards parent termination signals to the active benchmark child", async () => {
    const processLike = new EventEmitter();
    const child = new FakeChildProcess();
    const taskRunner = createBenchmarkTaskRunner({
      processLike,
      spawn: () => child,
    });

    const exitCodePromise = taskRunner.runTask(makeTask(makeDirectory()));
    processLike.emit("SIGINT");
    child.emit("exit", null, "SIGTERM");

    await expect(exitCodePromise).resolves.toBe(130);
    expect(child.killedSignals).toStrictEqual(["SIGINT"]);
  });

  it("maps child signal exits when the parent did not initiate termination", async () => {
    const processLike = new EventEmitter();
    const child = new FakeChildProcess();
    const taskRunner = createBenchmarkTaskRunner({
      processLike,
      spawn: () => child,
    });

    const exitCodePromise = taskRunner.runTask(makeTask(makeDirectory()));
    child.signalCode = "SIGTERM";
    child.emit("exit", null, "SIGTERM");

    await expect(exitCodePromise).resolves.toBe(143);
  });

  it("maps null child exit codes to failure", async () => {
    const processLike = new EventEmitter();
    const child = new FakeChildProcess();
    const taskRunner = createBenchmarkTaskRunner({
      processLike,
      spawn: () => child,
    });

    const exitCodePromise = taskRunner.runTask(makeTask(makeDirectory()));
    child.emit("exit", null, null);

    await expect(exitCodePromise).resolves.toBe(1);
  });

  it("rejects benchmark child spawn errors", async () => {
    const processLike = new EventEmitter();
    const child = new FakeChildProcess();
    const taskRunner = createBenchmarkTaskRunner({
      processLike,
      spawn: () => child,
    });
    const exitCodePromise = taskRunner.runTask(makeTask(makeDirectory()));
    const error = new Error("spawn failed");

    child.emit("error", error);

    await expect(exitCodePromise).rejects.toThrow("spawn failed");
  });

  it("wires the CLI runner to the baseline core with a benchmark task runner", async () => {
    const processLike = new EventEmitter();
    const child = new FakeChildProcess();
    const exitCode = await runBenchmarkBaselineCli({
      argv: ["node", "script"],
      environment: {},
      logger: silentLogger().logger,
      processLike,
      runBaseline: async ({ runTask }) => {
        const taskExitCode = runTask(makeTask(makeDirectory()));
        child.exitCode = 12;
        child.emit("exit", 12, null);
        return taskExitCode;
      },
      spawn: () => child,
    });

    expect(exitCode).toBe(12);
  });

  it("returns the parent termination code when termination happens outside child execution", async () => {
    const processLike = new EventEmitter();
    const exitCode = await runBenchmarkBaselineCli({
      argv: ["node", "script"],
      environment: {},
      logger: silentLogger().logger,
      processLike,
      runBaseline: async () => {
        processLike.emit("SIGHUP");
        return 0;
      },
      spawn: () => new FakeChildProcess(),
    });

    expect(exitCode).toBe(129);
  });

  it("returns unknown profile failures", async () => {
    const { logger, messages } = silentLogger();

    const exitCode = await runBenchmarkBaseline({
      argv: ["node", "script", "--profile=missing"],
      environment: {},
      logger,
      runTask: async () => 0,
    });

    expect({
      exitCode,
      message: messages[0],
    }).toStrictEqual({
      exitCode: 1,
      message:
        "Unknown benchmark baseline profile: missing\nAvailable profiles: smoke, kafka-ingest, kafka-sustained-firehose, websocket-firehose, active-query-sharing, raw-read-write, grouped-admission, grouped-order-neutral, release",
    });
  });

  it("removes expected artifacts before each run", () => {
    const directory = makeDirectory();
    const task = makeTask(directory);
    writeArtifacts(task);

    removeTaskArtifacts(task);

    expect({
      outputStillExists: existsSync(task.outputJsonPath),
      summaryStillExists: existsSync(task.summaryPath),
    }).toStrictEqual({
      outputStillExists: false,
      summaryStillExists: false,
    });
  });
});
