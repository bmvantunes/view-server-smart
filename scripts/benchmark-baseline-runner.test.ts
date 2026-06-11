import { describe, expect, it } from "@effect/vitest";
import { EventEmitter } from "node:events";
import { existsSync, mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { readBenchmarkBaseline, writeBenchmarkBaseline } from "./benchmark-baseline.mjs";
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
        VIEW_SERVER_REACT_BENCH_ROWS: "100",
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
            outputJsonPath: task.outputJsonPath,
            summaryPath: task.summaryPath,
          },
        ],
        thresholds: {
          latencyMean: {
            maxAbsoluteDeltaMs: 5,
            maxRatio: 8,
          },
          latencyP99: {
            maxAbsoluteDeltaMs: 10,
            maxRatio: 8,
          },
          memoryRssTotalDelta: {
            maxAbsoluteDeltaBytes: 134217728,
            maxRatio: 3,
          },
        },
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
      thresholds: {
        latencyMean: {
          maxAbsoluteDeltaMs: 5,
          maxRatio: 8,
        },
        latencyP99: {
          maxAbsoluteDeltaMs: 10,
          maxRatio: 8,
        },
        memoryRssTotalDelta: {
          maxAbsoluteDeltaBytes: 134217728,
          maxRatio: 3,
        },
      },
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
        "\ntiny benchmark baseline regressed:\n- task a / src/runner-example.bench.ts > runner example benchmark group / case a: mean regressed from 2.000ms to 100.000ms; allowed <= 7.000ms.\n- task a / src/runner-example.bench.ts > runner example benchmark group / case a: p99 regressed from 3.000ms to 100.000ms; allowed <= 13.000ms.",
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
        "Unknown benchmark baseline profile: missing\nAvailable profiles: smoke, grouped-admission, release",
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
