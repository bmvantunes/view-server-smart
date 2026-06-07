import { spawn } from "node:child_process";
import { constants as osConstants } from "node:os";

const commonEngineSmokeEnv = {
  VIEW_SERVER_ENGINE_BENCH_ITERATIONS: "1",
  VIEW_SERVER_ENGINE_BENCH_TIME_MS: "1",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS: "0",
  VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS: "0",
};

const commonReactSmokeEnv = {
  VIEW_SERVER_REACT_BENCH_ITERATIONS: "1",
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

const groupedAggregateReleaseEnv = {
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

const task = (label, vpTask, env) => ({
  args: ["run", "--no-cache", vpTask],
  command: "vp",
  env,
  label,
});

const rawSnapshotTask = (rowCount, env = {}) =>
  task(`raw snapshot ${rowCount} rows`, "column-live-view-engine#bench:raw-snapshot", {
    VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
    ...env,
  });

const rawPredicateIndexTask = (rowCount, env = {}) =>
  task(
    `raw predicate index ${rowCount} rows`,
    "column-live-view-engine#bench:raw-predicate-index",
    {
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      ...env,
    },
  );

const rawWriteTask = (writeMode, rowCount, env = {}) =>
  task(`raw write ${writeMode} ${rowCount} rows`, "column-live-view-engine#bench:raw-write", {
    VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
    VIEW_SERVER_ENGINE_BENCH_WRITE_MODE: writeMode,
    ...env,
  });

const rawLiveFanoutTask = (fanoutCase, rowCount, subscriberCount, env = {}) =>
  task(
    `raw live fanout ${fanoutCase} ${rowCount} rows ${subscriberCount} subscribers`,
    "column-live-view-engine#bench:raw-live-fanout",
    {
      VIEW_SERVER_ENGINE_BENCH_FANOUT_CASE: fanoutCase,
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      VIEW_SERVER_ENGINE_BENCH_SUBSCRIBERS: String(subscriberCount),
      ...env,
    },
  );

const groupedAggregateTask = (rowCount, env = {}) =>
  task(`grouped aggregate ${rowCount} rows`, "column-live-view-engine#bench:grouped-aggregate", {
    VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
    ...env,
  });

const groupedWriteTask = (mode, rowCount, env = {}) =>
  task(
    `grouped write ${mode} ${rowCount} rows ${env.VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE ?? "1"} mutations${env.VIEW_SERVER_ENGINE_BENCH_ARTIFACT_SUFFIX === undefined ? "" : ` ${env.VIEW_SERVER_ENGINE_BENCH_ARTIFACT_SUFFIX}`}`,
    "column-live-view-engine#bench:grouped-write",
    {
      VIEW_SERVER_ENGINE_BENCH_EXPECTED_GROUPED_ADMISSION: mode,
      VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_MODE: mode,
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      ...env,
    },
  );

const rawActiveRetainedDeltaTask = (retainedCase, rowCount, env = {}) =>
  task(
    `raw active retained delta ${retainedCase} ${rowCount} rows`,
    "column-live-view-engine#bench:raw-active-retained-delta",
    {
      VIEW_SERVER_ENGINE_BENCH_RETAINED_CASE: retainedCase,
      VIEW_SERVER_ENGINE_BENCH_ROWS: String(rowCount),
      ...env,
    },
  );

const reactInMemoryTask = (browser, rowCount, env = {}) =>
  task(`React in-memory ${browser} ${rowCount} rows`, "react#bench:in-memory-live-query", {
    VIEW_SERVER_REACT_BENCH_BROWSER: browser,
    VIEW_SERVER_REACT_BENCH_ROWS: String(rowCount),
    ...env,
  });

const profiles = new Map([
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
      groupedWriteTask("incremental", 1_000, {
        VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE: "500",
        VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE: "1",
        ...retainedDeltaSmokeEnv,
      }),
      rawActiveRetainedDeltaTask("noop", 101, retainedDeltaSmokeEnv),
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
      groupedAggregateTask(100_000, groupedAggregateReleaseEnv),
      groupedAggregateTask(1_000_000, groupedAggregateReleaseEnv),
      groupedAggregateTask(5_000_000, groupedAggregateReleaseEnv),
      groupedWriteTask("incremental", 100_000, groupedWriteReleaseEnv),
      groupedWriteTask("incremental", 1_000_000, groupedWriteReleaseEnv),
      groupedWriteTask("incremental", 5_000_000, groupedWriteReleaseEnv),
      rawActiveRetainedDeltaTask("noop", 100_000, retainedDeltaReleaseEnv),
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

const isBenchmarkEnvironmentKey = (key) =>
  key === "VIEW_SERVER_BENCH_BASELINE_PROFILE" ||
  key.startsWith("VIEW_SERVER_ENGINE_BENCH_") ||
  key.startsWith("VIEW_SERVER_REACT_BENCH_") ||
  key.startsWith("VITE_VIEW_SERVER_REACT_BENCH_");

const cleanParentEnvironment = () =>
  Object.fromEntries(
    Object.entries(process.env).filter(([key]) => !isBenchmarkEnvironmentKey(key)),
  );

const exitCodeForSignal = (signal) => {
  const signalNumber = osConstants.signals[signal];
  return typeof signalNumber === "number" ? 128 + signalNumber : 1;
};

let activeChild;
let terminatingExitCode;

const childIsRunning = (child) => child.exitCode === null && child.signalCode === null;

for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) {
  process.on(signal, () => {
    terminatingExitCode ??= exitCodeForSignal(signal);
    if (activeChild !== undefined && childIsRunning(activeChild)) {
      activeChild.kill(signal);
      return;
    }
    process.exit(terminatingExitCode);
  });
}

const runTask = (currentTask) =>
  new Promise((resolve, reject) => {
    const child = spawn(currentTask.command, currentTask.args, {
      env: {
        ...parentEnvironment,
        ...currentTask.env,
      },
      stdio: "inherit",
    });
    activeChild = child;

    const clearActiveChild = () => {
      if (activeChild === child) {
        activeChild = undefined;
      }
    };

    child.once("error", (error) => {
      clearActiveChild();
      reject(error);
    });
    child.once("close", (status, signal) => {
      clearActiveChild();
      if (terminatingExitCode !== undefined) {
        resolve(terminatingExitCode);
        return;
      }
      if (signal !== null) {
        resolve(exitCodeForSignal(signal));
        return;
      }
      resolve(status ?? 1);
    });
  });

const profileArgument = process.argv.find((argument) => argument.startsWith("--profile="));
const requestedProfile =
  profileArgument?.slice("--profile=".length) ??
  process.env["VIEW_SERVER_BENCH_BASELINE_PROFILE"] ??
  "smoke";
const tasks = profiles.get(requestedProfile);
const parentEnvironment = cleanParentEnvironment();

if (tasks === undefined) {
  console.error(
    [
      `Unknown benchmark baseline profile: ${requestedProfile}`,
      `Available profiles: ${[...profiles.keys()].join(", ")}`,
    ].join("\n"),
  );
  process.exit(1);
}

console.log(`Running ${requestedProfile} benchmark baseline serially (${tasks.length} tasks).`);

for (const [index, currentTask] of tasks.entries()) {
  const taskNumber = index + 1;
  const startedAt = process.hrtime.bigint();
  console.log(`\n[${taskNumber}/${tasks.length}] ${currentTask.label}`);
  const exitCode = await runTask(currentTask);
  if (exitCode !== 0) {
    process.exit(exitCode);
  }
  const elapsedMs = Number(process.hrtime.bigint() - startedAt) / 1_000_000;
  console.log(`[${taskNumber}/${tasks.length}] completed in ${elapsedMs.toFixed(0)}ms`);
}

console.log(`\n${requestedProfile} benchmark baseline completed.`);
