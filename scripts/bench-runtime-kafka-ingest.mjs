import { spawn } from "node:child_process";
import { mkdirSync } from "node:fs";
import { dirname, isAbsolute, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const defaultRootDirectory = fileURLToPath(new URL("../", import.meta.url));
const defaultRuntimeDirectory = fileURLToPath(new URL("../packages/runtime/", import.meta.url));

const exitCodeForSignal = (signal) =>
  signal === "SIGINT" ? 130 : signal === "SIGTERM" ? 143 : 129;

const exitCodeForChildSignal = (signal) =>
  signal === "SIGINT"
    ? 130
    : signal === "SIGTERM"
      ? 143
    : signal === "SIGHUP"
      ? 129
    : signal === "SIGKILL"
      ? 137
    : 1;

export const createKafkaIngestBenchmarkRunner = ({
  env,
  rootDirectory = defaultRootDirectory,
  runtimeDirectory = defaultRuntimeDirectory,
  outputExists,
  processId = process.pid,
  removeOutput,
  spawnProcess,
  stdio,
}) => {
  const kafkaBootstrapServers = env.VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS ?? "localhost:9092";
  const batchSize = env.VIEW_SERVER_RUNTIME_BENCH_KAFKA_BATCH_SIZE ?? "1000";
  const benchmarkMode = env.VIEW_SERVER_RUNTIME_BENCH_KAFKA_MODE ?? "batch";
  const defaultOutputName =
    benchmarkMode === "sustained-firehose"
      ? `.artifacts/kafka-sustained-firehose-${batchSize}rows-${env.VIEW_SERVER_RUNTIME_BENCH_KAFKA_SUSTAINED_BATCHES ?? "4"}batches.json`
      : `.artifacts/kafka-ingest-${batchSize}rows.json`;
  const outputJsonPath = env.VIEW_SERVER_RUNTIME_BENCH_OUTPUT_JSON ?? defaultOutputName;
  const resolvedOutputJsonPath = isAbsolute(outputJsonPath)
    ? outputJsonPath
    : resolve(runtimeDirectory, outputJsonPath);
  const composeProjectName = `view-server-kafka-ingest-${processId}`;

  let activeChild = undefined;
  let activeRun = undefined;
  let cleanupPromise = undefined;
  let interruptedSignal = undefined;
  let isCleaning = false;
  let shouldCleanupDocker = false;

  const run = (command, args, options = {}) => {
    let child = undefined;
    try {
      child = spawnProcess(command, args, {
        cwd: options.cwd ?? runtimeDirectory,
        env: options.env ?? env,
        stdio,
      });
    } catch {
      activeChild = undefined;
      activeRun = Promise.resolve(1);
      return activeRun;
    }
    activeChild = child;
    activeRun = new Promise((resolveExitCode) => {
      child.once("error", () => {
        if (activeChild === child) {
          activeChild = undefined;
        }
        resolveExitCode(1);
      });
      child.once("close", (code, signal) => {
        if (activeChild === child) {
          activeChild = undefined;
        }
        resolveExitCode(code ?? exitCodeForChildSignal(signal));
      });
    });
    return activeRun;
  };

  const terminateActiveChild = () => {
    if (isCleaning || activeChild === undefined || activeChild.killed === true) {
      return;
    }
    activeChild.kill("SIGTERM");
  };

  const cleanup = async () => {
    if (!shouldCleanupDocker) {
      return 0;
    }
    if (cleanupPromise !== undefined) {
      return cleanupPromise;
    }
    isCleaning = true;
    cleanupPromise = yieldDockerDown().finally(() => {
      isCleaning = false;
    });
    return cleanupPromise;
  };

  const yieldDockerDown = () =>
    run("docker", ["compose", "-f", "compose.yaml", "down"], {
      cwd: rootDirectory,
      env: {
        ...env,
        COMPOSE_PROJECT_NAME: composeProjectName,
      },
    });

  const returnInterrupted = async () => {
    const cleanupExitCode = await cleanup();
    return cleanupExitCode === 0 ? exitCodeForSignal(interruptedSignal) : cleanupExitCode;
  };

  const handleSignal = async (signal) => {
    interruptedSignal = signal;
    terminateActiveChild();
    await activeRun;
    return returnInterrupted();
  };

  const runMain = async () => {
    let exitCode = await run("vp", ["run", "-t", "@view-server/effect-utils#build"]);
    if (interruptedSignal !== undefined) {
      return returnInterrupted();
    }

    if (exitCode === 0) {
      exitCode = await run("vp", ["run", "-t", "@view-server/runtime-core#build"]);
      if (interruptedSignal !== undefined) {
        return returnInterrupted();
      }
    }

    if (exitCode === 0) {
      exitCode = await run("vp", ["run", "-t", "@view-server/server#build"]);
      if (interruptedSignal !== undefined) {
        return returnInterrupted();
      }
    }

    if (exitCode === 0) {
      mkdirSync(dirname(resolvedOutputJsonPath), { recursive: true });
      shouldCleanupDocker = true;
      try {
        exitCode = await run(
          "docker",
          ["compose", "-f", "compose.yaml", "up", "-d", "--wait", "kafka"],
          {
            cwd: rootDirectory,
            env: {
              ...env,
              COMPOSE_PROJECT_NAME: composeProjectName,
            },
          },
        );
        if (interruptedSignal !== undefined) {
          return returnInterrupted();
        }

        if (exitCode === 0) {
          removeOutput(resolvedOutputJsonPath);
          exitCode = await run(
            "vp",
            [
              "test",
              "bench",
              "src/kafka-ingest.bench.ts",
              "--run",
              "--testTimeout",
              "0",
              "--outputJson",
              outputJsonPath,
            ],
            {
              env: {
                ...env,
                VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS: kafkaBootstrapServers,
                VIEW_SERVER_RUNTIME_BENCH_OUTPUT_JSON: outputJsonPath,
              },
            },
          );
          if (interruptedSignal !== undefined) {
            return returnInterrupted();
          }
        }

        if (exitCode === 0 && !outputExists(resolvedOutputJsonPath)) {
          exitCode = 1;
        }
      } catch {
        exitCode = 1;
      }
    }

    const cleanupExitCode = await cleanup();
    return exitCode === 0 ? cleanupExitCode : exitCode;
  };

  return {
    cleanup,
    handleSignal,
    runMain,
  };
};

export const runKafkaIngestBenchmarkCli = async ({
  env,
  exit,
  processEvents,
  rootDirectory,
  runtimeDirectory,
  outputExists,
  processId,
  removeOutput,
  spawnProcess,
  stdio,
}) => {
  const runner = createKafkaIngestBenchmarkRunner({
    env,
    outputExists,
    processId,
    removeOutput,
    rootDirectory,
    runtimeDirectory,
    spawnProcess,
    stdio,
  });
  let isExiting = false;
  let signalExitPromise = undefined;
  for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) {
    processEvents.on(signal, () => {
      if (isExiting) {
        return;
      }
      isExiting = true;
      signalExitPromise = runner.handleSignal(signal).then((exitCode) => {
        exit(exitCode);
        return exitCode;
      });
    });
  }
  const exitCode = await runner.runMain();
  if (signalExitPromise !== undefined) {
    return signalExitPromise;
  }
  isExiting = true;
  exit(exitCode);
  return exitCode;
};
