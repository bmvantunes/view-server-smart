import { existsSync, rmSync } from "node:fs";
import { constants as osConstants } from "node:os";
import {
  buildBenchmarkBaseline,
  compareBenchmarkBaseline,
  readBenchmarkBaseline,
  readBenchmarkObservation,
  writeBenchmarkBaseline,
} from "./benchmark-baseline.mjs";

const enginePackageDirectory = "packages/column-live-view-engine";
const reactPackageDirectory = "packages/react";

export const baselinePath = (profile) => `benchmarks/baselines/${profile}.json`;

export const summaryPath = (outputJsonPath) =>
  outputJsonPath.endsWith(".json")
    ? `${outputJsonPath.slice(0, -".json".length)}.summary.json`
    : `${outputJsonPath}.summary.json`;

const packageArtifactPath = (packageDirectory, outputJsonPath) =>
  `${packageDirectory}/${outputJsonPath}`;

const engineArtifactName = (name) => `.artifacts/${name}`;

const reactArtifactName = (name) => `.artifacts/${name}`;

const commonEngineSmokeEnv = {
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "5",
  VIEW_SERVER_ENGINE_BENCH_TIME_MS: "1",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS: "0",
};

const commonReactSmokeEnv = {
  VIEW_SERVER_REACT_BENCH_ITERATIONS: "5",
  VIEW_SERVER_REACT_BENCH_TIME_MS: "1",
  VIEW_SERVER_REACT_BENCH_WARMUP_ITERATIONS: "0",
  VIEW_SERVER_REACT_BENCH_WARMUP_TIME_MS: "0",
};

const retainedDeltaSmokeEnv = {
  ...commonEngineSmokeEnv,
  VIEW_SERVER_ENGINE_BENCH_TIME_MS: "0",
};

const retainedDeltaReleaseEnv = {
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "100",
  VIEW_SERVER_ENGINE_BENCH_TIME_MS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS: "0",
};

const retainedDeltaMoveDownReleaseEnv = {
  ...retainedDeltaReleaseEnv,
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "49",
};

const retainedDeltaReplacementBatchReleaseEnv = {
  ...retainedDeltaReleaseEnv,
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "24",
};

const groupedReadReleaseEnv = {
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "3",
  VIEW_SERVER_ENGINE_BENCH_TIME_MS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS: "0",
};

const groupedWriteReleaseEnv = {
  VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_MODE: "incremental",
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "3",
  VIEW_SERVER_ENGINE_BENCH_TIME_MS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS: "0",
  VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE: "1",
};

const groupedAdmissionReleaseEnv = {
  VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_MODE: "incremental",
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "3",
  VIEW_SERVER_ENGINE_BENCH_TIME_MS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS: "0",
  VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE: "32",
};

const forcedGroupedFallbackAdmissionEnv = {
  VIEW_SERVER_ENGINE_BENCH_EXPECTED_GROUPED_ADMISSION: "fallback",
  VIEW_SERVER_ENGINE_BENCH_GROUPED_INCREMENTAL_MAX_GROUPS: "1",
  VIEW_SERVER_ENGINE_BENCH_GROUPED_INCREMENTAL_MAX_MEMBERS: "1",
  VIEW_SERVER_ENGINE_BENCH_GROUPED_INCREMENTAL_MAX_MEMBERS_PER_GROUP: "1",
  VIEW_SERVER_ENGINE_BENCH_GROUPED_INCREMENTAL_MAX_RETAINED_VALUE_ENTRIES: "1",
};

const task = ({
  artifactKind,
  benchmarkScope,
  env,
  label,
  minimumSampleCount,
  outputJsonPath,
  packageDirectory,
  rowCount,
  vpTask,
}) => ({
  args: ["run", "--no-cache", vpTask],
  command: "vp",
  env,
  expectedArtifactKind: artifactKind,
  expectedBenchmarkScope: benchmarkScope,
  expectedRowCount: rowCount,
  label,
  minimumSampleCount,
  outputJsonPath: packageArtifactPath(packageDirectory, outputJsonPath),
  packageOutputJsonPath: outputJsonPath,
  summaryPath: packageArtifactPath(packageDirectory, summaryPath(outputJsonPath)),
});

const minimumSampleCountFrom = (env, key) => Number.parseInt(env[key] ?? "5", 10);

const rawSnapshotTask = (rowCount, env = {}) => {
  const outputJsonPath = engineArtifactName(`raw-snapshot-${rowCount}rows.json`);
  return task({
    artifactKind: "engine-benchmark-summary",
    benchmarkScope: "engine-raw-snapshot",
    env: {
      VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: outputJsonPath,
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      ...env,
    },
    label: `raw snapshot ${rowCount} rows`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_ENGINE_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: enginePackageDirectory,
    rowCount,
    vpTask: "column-live-view-engine#bench:raw-snapshot",
  });
};

const rawPredicateIndexTask = (rowCount, env = {}) => {
  const outputJsonPath = engineArtifactName(`raw-predicate-index-${rowCount}rows.json`);
  return task({
    artifactKind: "engine-benchmark-summary",
    benchmarkScope: "engine-raw-predicate-index",
    env: {
      VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: outputJsonPath,
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      ...env,
    },
    label: `raw predicate index ${rowCount} rows`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_ENGINE_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: enginePackageDirectory,
    rowCount,
    vpTask: "column-live-view-engine#bench:raw-predicate-index",
  });
};

const rawWriteTask = (writeMode, rowCount, env = {}) => {
  const outputJsonPath = engineArtifactName(`raw-write-${writeMode}-${rowCount}rows.json`);
  return task({
    artifactKind: "engine-benchmark-summary",
    benchmarkScope: "engine-raw-write",
    env: {
      VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: outputJsonPath,
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      VIEW_SERVER_ENGINE_BENCH_WRITE_MODE: writeMode,
      ...env,
    },
    label: `raw write ${writeMode} ${rowCount} rows`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_ENGINE_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: enginePackageDirectory,
    rowCount,
    vpTask: "column-live-view-engine#bench:raw-write",
  });
};

const rawLiveFanoutTask = (fanoutCase, rowCount, subscriberCount, env = {}) => {
  const outputJsonPath = engineArtifactName(
    `raw-live-fanout-${fanoutCase}-${rowCount}rows-${subscriberCount}subs.json`,
  );
  return task({
    artifactKind: "engine-benchmark-summary",
    benchmarkScope: "engine-raw-live-fanout",
    env: {
      VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: outputJsonPath,
      VIEW_SERVER_ENGINE_BENCH_FANOUT_CASE: fanoutCase,
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      VIEW_SERVER_ENGINE_BENCH_SUBSCRIBERS: String(subscriberCount),
      ...env,
    },
    label: `raw live fanout ${fanoutCase} ${rowCount} rows ${subscriberCount} subscribers`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_ENGINE_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: enginePackageDirectory,
    rowCount,
    vpTask: "column-live-view-engine#bench:raw-live-fanout",
  });
};

const groupedAggregateTask = (rowCount, env) => {
  const outputJsonPath = engineArtifactName(`grouped-aggregate-${rowCount}rows.json`);
  return task({
    artifactKind: "engine-benchmark-summary",
    benchmarkScope: "engine-grouped-aggregate",
    env: {
      VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: outputJsonPath,
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      ...env,
    },
    label: `grouped aggregate ${rowCount} rows`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_ENGINE_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: enginePackageDirectory,
    rowCount,
    vpTask: "column-live-view-engine#bench:grouped-aggregate",
  });
};

const groupedKeyWidthTask = (rowCount, env) => {
  const outputJsonPath = engineArtifactName(`grouped-key-width-${rowCount}rows.json`);
  return task({
    artifactKind: "engine-benchmark-summary",
    benchmarkScope: "engine-grouped-key-width",
    env: {
      VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: outputJsonPath,
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      ...env,
    },
    label: `grouped key width ${rowCount} rows`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_ENGINE_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: enginePackageDirectory,
    rowCount,
    vpTask: "column-live-view-engine#bench:grouped-key-width",
  });
};

const groupedWriteTask = (mode, rowCount, env) => {
  const writeBatchSize = env.VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE;
  const readerProfile = env.VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_READER_PROFILE ?? "dual";
  const readerProfileLabel = readerProfile === "dual" ? "" : ` ${readerProfile}`;
  const readerProfileSegment = readerProfile === "dual" ? "" : `-${readerProfile}`;
  const labelSuffix =
    env.VIEW_SERVER_ENGINE_BENCH_ARTIFACT_SUFFIX === undefined
      ? ""
      : ` ${env.VIEW_SERVER_ENGINE_BENCH_ARTIFACT_SUFFIX}`;
  const suffix =
    env.VIEW_SERVER_ENGINE_BENCH_ARTIFACT_SUFFIX === undefined
      ? ""
      : `-${env.VIEW_SERVER_ENGINE_BENCH_ARTIFACT_SUFFIX}`;
  const outputJsonPath = engineArtifactName(
    `grouped-write-${mode}${readerProfileSegment}-${rowCount}rows-${writeBatchSize}mutations${suffix}.json`,
  );
  return task({
    artifactKind: "engine-benchmark-summary",
    benchmarkScope: "engine-grouped-write",
    env: {
      VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: outputJsonPath,
      VIEW_SERVER_ENGINE_BENCH_EXPECTED_GROUPED_ADMISSION: mode,
      VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_MODE: mode,
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      ...env,
    },
    label: `grouped write ${mode}${readerProfileLabel} ${rowCount} rows ${writeBatchSize} mutations${labelSuffix}`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_ENGINE_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: enginePackageDirectory,
    rowCount,
    vpTask: "column-live-view-engine#bench:grouped-write",
  });
};

const rawActiveRetainedDeltaTask = (retainedCase, rowCount, env) => {
  const outputJsonPath = engineArtifactName(
    `raw-active-retained-delta-${retainedCase}-${rowCount}rows.json`,
  );
  return task({
    artifactKind: "engine-benchmark-summary",
    benchmarkScope: "engine-raw-active-retained-delta",
    env: {
      VIEW_SERVER_ENGINE_BENCH_OUTPUT_JSON: outputJsonPath,
      VIEW_SERVER_ENGINE_BENCH_RETAINED_CASE: retainedCase,
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      ...env,
    },
    label: `raw active retained delta ${retainedCase} ${rowCount} rows`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_ENGINE_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: enginePackageDirectory,
    rowCount,
    vpTask: "column-live-view-engine#bench:raw-active-retained-delta",
  });
};

const reactInMemoryTask = (browser, rowCount, env = {}) => {
  const outputJsonPath = reactArtifactName(`in-memory-live-query-${rowCount}rows-${browser}.json`);
  return task({
    artifactKind: "react-browser-benchmark-summary",
    benchmarkScope: "react-in-memory-live-query",
    env: {
      VIEW_SERVER_REACT_BENCH_OUTPUT_JSON: outputJsonPath,
      VIEW_SERVER_REACT_BENCH_BROWSER: browser,
      VIEW_SERVER_REACT_BENCH_ROWS: String(rowCount),
      ...env,
    },
    label: `React in-memory ${browser} ${rowCount} rows`,
    minimumSampleCount: minimumSampleCountFrom(env, "VIEW_SERVER_REACT_BENCH_ITERATIONS"),
    outputJsonPath,
    packageDirectory: reactPackageDirectory,
    rowCount,
    vpTask: "react#bench:in-memory-live-query",
  });
};

export const profiles = new Map([
  [
    "smoke",
    [
      rawSnapshotTask(1_000, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "500",
        ...commonEngineSmokeEnv,
      }),
      rawPredicateIndexTask(1_000, commonEngineSmokeEnv),
      rawWriteTask("base", 1_000, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "100",
        ...commonEngineSmokeEnv,
      }),
      rawWriteTask("indexed", 1_000, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "100",
        ...commonEngineSmokeEnv,
      }),
      rawLiveFanoutTask("same-window", 1_000, 5, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "500",
        ...commonEngineSmokeEnv,
      }),
      rawLiveFanoutTask("ten-window", 1_000, 5, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "500",
        ...commonEngineSmokeEnv,
      }),
      groupedAggregateTask(1_000, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "500",
        ...commonEngineSmokeEnv,
      }),
      groupedKeyWidthTask(1_000, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "500",
        ...commonEngineSmokeEnv,
      }),
      groupedWriteTask("incremental", 1_000, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "500",
        VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE: "1",
        ...retainedDeltaSmokeEnv,
      }),
      rawActiveRetainedDeltaTask("noop", 101, retainedDeltaSmokeEnv),
      rawActiveRetainedDeltaTask("match-update", 101, retainedDeltaSmokeEnv),
      rawActiveRetainedDeltaTask("match-move-down", 101, retainedDeltaSmokeEnv),
      rawActiveRetainedDeltaTask("match-replacement-batch", 101, retainedDeltaSmokeEnv),
      rawActiveRetainedDeltaTask("predicate-enter", 101, retainedDeltaSmokeEnv),
      rawActiveRetainedDeltaTask("visible-delete", 101, retainedDeltaSmokeEnv),
      rawActiveRetainedDeltaTask("exhausted-lookahead", 101, retainedDeltaSmokeEnv),
      rawActiveRetainedDeltaTask("count-only", 101, retainedDeltaSmokeEnv),
      reactInMemoryTask("chromium", 20, {
        VIEW_SERVER_REACT_BENCH_BATCH_SIZE: "10",
        ...commonReactSmokeEnv,
      }),
    ],
  ],
  [
    "grouped-admission",
    [
      groupedWriteTask("incremental", 100_000, groupedAdmissionReleaseEnv),
      groupedWriteTask("incremental", 1_000_000, groupedAdmissionReleaseEnv),
      groupedWriteTask("incremental", 1_000_000, {
        ...groupedAdmissionReleaseEnv,
        VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE: "128",
      }),
      groupedWriteTask("incremental", 100_000, {
        ...groupedAdmissionReleaseEnv,
        ...forcedGroupedFallbackAdmissionEnv,
        VIEW_SERVER_ENGINE_BENCH_ARTIFACT_SUFFIX: "forced-fallback-admission",
      }),
      groupedWriteTask("fallback", 100_000, {
        ...groupedAdmissionReleaseEnv,
        VIEW_SERVER_ENGINE_BENCH_ARTIFACT_SUFFIX: "broad-fallback",
        VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_MODE: "fallback",
      }),
    ],
  ],
  [
    "grouped-order-neutral",
    [
      groupedWriteTask("incremental", 100_000, {
        ...groupedWriteReleaseEnv,
        VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_READER_PROFILE: "order-neutral",
      }),
      groupedWriteTask("incremental", 1_000_000, {
        ...groupedWriteReleaseEnv,
        VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_READER_PROFILE: "order-neutral",
      }),
      groupedWriteTask("incremental", 5_000_000, {
        ...groupedWriteReleaseEnv,
        VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_READER_PROFILE: "order-neutral",
      }),
    ],
  ],
  [
    "release",
    [
      rawSnapshotTask(100_000),
      rawSnapshotTask(1_000_000),
      rawSnapshotTask(10_000_000),
      rawPredicateIndexTask(100_000),
      rawPredicateIndexTask(1_000_000),
      rawPredicateIndexTask(10_000_000),
      rawWriteTask("base", 100_000),
      rawWriteTask("indexed", 100_000),
      rawWriteTask("base", 1_000_000),
      rawWriteTask("indexed", 1_000_000),
      rawWriteTask("base", 10_000_000),
      rawWriteTask("indexed", 10_000_000),
      rawLiveFanoutTask("same-window", 100_000, 50),
      rawLiveFanoutTask("ten-window", 100_000, 50),
      rawLiveFanoutTask("same-window", 1_000_000, 250),
      rawLiveFanoutTask("ten-window", 1_000_000, 250),
      groupedAggregateTask(100_000, groupedReadReleaseEnv),
      groupedAggregateTask(1_000_000, groupedReadReleaseEnv),
      groupedAggregateTask(5_000_000, groupedReadReleaseEnv),
      groupedKeyWidthTask(100_000, groupedReadReleaseEnv),
      groupedKeyWidthTask(1_000_000, groupedReadReleaseEnv),
      groupedWriteTask("incremental", 100_000, groupedWriteReleaseEnv),
      groupedWriteTask("incremental", 1_000_000, groupedWriteReleaseEnv),
      groupedWriteTask("incremental", 5_000_000, groupedWriteReleaseEnv),
      rawActiveRetainedDeltaTask("noop", 100_000, retainedDeltaReleaseEnv),
      rawActiveRetainedDeltaTask("match-update", 100_000, retainedDeltaReleaseEnv),
      rawActiveRetainedDeltaTask(
        "match-move-down",
        100_000,
        retainedDeltaMoveDownReleaseEnv,
      ),
      rawActiveRetainedDeltaTask(
        "match-replacement-batch",
        100_000,
        retainedDeltaReplacementBatchReleaseEnv,
      ),
      rawActiveRetainedDeltaTask("predicate-enter", 100_000, retainedDeltaReleaseEnv),
      rawActiveRetainedDeltaTask("visible-delete", 100_000, retainedDeltaReleaseEnv),
      rawActiveRetainedDeltaTask("exhausted-lookahead", 100_000, retainedDeltaReleaseEnv),
      rawActiveRetainedDeltaTask("count-only", 100_000, retainedDeltaReleaseEnv),
      reactInMemoryTask("chromium", 10_000),
      reactInMemoryTask("firefox", 10_000),
      reactInMemoryTask("webkit", 10_000),
    ],
  ],
]);

export const isBenchmarkEnvironmentKey = (key) =>
  key === "VIEW_SERVER_BENCH_BASELINE_PROFILE" ||
  key.startsWith("VIEW_SERVER_ENGINE_BENCH_") ||
  key.startsWith("VIEW_SERVER_REACT_BENCH_") ||
  key.startsWith("VITE_VIEW_SERVER_REACT_BENCH_");

export const cleanBenchmarkEnvironment = (environment) =>
  Object.fromEntries(
    Object.entries(environment).filter(([key]) => !isBenchmarkEnvironmentKey(key)),
  );

export const exitCodeForSignal = (signal) => {
  const signalNumber = osConstants.signals[signal];
  return typeof signalNumber === "number" ? 128 + signalNumber : 1;
};

export const removeTaskArtifacts = (currentTask) => {
  rmSync(currentTask.outputJsonPath, { force: true });
  rmSync(currentTask.summaryPath, { force: true });
};

export const assertTaskArtifactsWritten = (currentTask) => {
  if (!existsSync(currentTask.outputJsonPath)) {
    throw new Error(`${currentTask.label}: missing benchmark output ${currentTask.outputJsonPath}.`);
  }
  if (!existsSync(currentTask.summaryPath)) {
    throw new Error(`${currentTask.label}: missing benchmark summary ${currentTask.summaryPath}.`);
  }
};

const requestedProfileFrom = (argv, environment) => {
  const profileArgument = argv.find((argument) => argument.startsWith("--profile="));
  return (
    profileArgument?.slice("--profile=".length) ??
    environment["VIEW_SERVER_BENCH_BASELINE_PROFILE"] ??
    "smoke"
  );
};

export const runBenchmarkBaseline = async ({
  argv,
  baselinePathForProfile = baselinePath,
  environment,
  logger,
  profileMap = profiles,
  runTask,
}) => {
  const compareBaseline = !argv.includes("--no-compare");
  const updateBaseline = argv.includes("--update-baseline");
  const requestedProfile = requestedProfileFrom(argv, environment);
  const tasks = profileMap.get(requestedProfile);
  const parentEnvironment = cleanBenchmarkEnvironment(environment);

  if (tasks === undefined) {
    logger.error(
      [
        `Unknown benchmark baseline profile: ${requestedProfile}`,
        `Available profiles: ${[...profileMap.keys()].join(", ")}`,
      ].join("\n"),
    );
    return 1;
  }

  logger.log(`Running ${requestedProfile} benchmark baseline serially (${tasks.length} tasks).`);

  for (const [index, currentTask] of tasks.entries()) {
    const taskNumber = index + 1;
    const startedAt = process.hrtime.bigint();
    logger.log(`\n[${taskNumber}/${tasks.length}] ${currentTask.label}`);
    removeTaskArtifacts(currentTask);
    const exitCode = await runTask({
      ...currentTask,
      env: {
        ...parentEnvironment,
        ...currentTask.env,
      },
    });
    if (exitCode !== 0) {
      return exitCode;
    }
    assertTaskArtifactsWritten(currentTask);
    const elapsedMs = Number(process.hrtime.bigint() - startedAt) / 1_000_000;
    logger.log(`[${taskNumber}/${tasks.length}] completed in ${elapsedMs.toFixed(0)}ms`);
  }

  const observations = tasks.map(readBenchmarkObservation);
  const actualBaseline = buildBenchmarkBaseline(requestedProfile, observations);
  const profileBaselinePath = baselinePathForProfile(requestedProfile);

  if (updateBaseline) {
    writeBenchmarkBaseline(profileBaselinePath, actualBaseline);
    logger.log(`\nUpdated benchmark baseline: ${profileBaselinePath}`);
  } else if (compareBaseline) {
    const baseline = readBenchmarkBaseline(profileBaselinePath);
    const comparison = compareBenchmarkBaseline(baseline, actualBaseline);
    if (!comparison.ok) {
      logger.error(
        [
          `\n${requestedProfile} benchmark baseline regressed:`,
          ...comparison.regressions.map((regression) => `- ${regression}`),
        ].join("\n"),
      );
      return 1;
    }
    logger.log(`\n${requestedProfile} benchmark baseline comparison passed.`);
  }

  logger.log(`\n${requestedProfile} benchmark baseline completed.`);
  return 0;
};
