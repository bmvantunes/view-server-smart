import { describe, expect, it } from "@effect/vitest";
import { EventEmitter } from "node:events";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createKafkaIngestBenchmarkRunner, runKafkaIngestBenchmarkCli } from "./bench-runtime-kafka-ingest.mjs";

type SpawnOptions = {
  cwd?: string;
  env?: NodeJS.ProcessEnv;
  stdio?: string;
};

type SpawnCall = {
  args: ReadonlyArray<string>;
  command: string;
  options: SpawnOptions;
};

class FakeChildProcess extends EventEmitter {
  killed = false;
  readonly killSignals: Array<string> = [];

  kill(signal = "SIGTERM") {
    this.killed = true;
    this.killSignals.push(signal);
    this.emit("close", null, signal);
    return true;
  }
}

const createFakeSpawn = () => {
  const calls: Array<SpawnCall> = [];
  const children: Array<FakeChildProcess> = [];
  const spawnProcess = (command: string, args: ReadonlyArray<string>, options: SpawnOptions) => {
    const child = new FakeChildProcess();
    calls.push({
      args,
      command,
      options,
    });
    children.push(child);
    return child;
  };
  return {
    calls,
    children,
    spawnProcess,
  };
};

const createRunner = (
  fakeSpawn: ReturnType<typeof createFakeSpawn>,
  options: {
    env?: NodeJS.ProcessEnv;
    outputExists?: (path: string) => boolean;
    processId?: number;
    removeOutput?: (path: string) => void;
    rootDirectory?: string;
    runtimeDirectory?: string;
    spawnProcess?: (
      command: string,
      args: ReadonlyArray<string>,
      options: SpawnOptions,
    ) => FakeChildProcess;
  } = {},
) =>
  createKafkaIngestBenchmarkRunner({
    env: options.env ?? {},
    outputExists: options.outputExists ?? (() => true),
    processId: options.processId ?? 12345,
    removeOutput: options.removeOutput ?? (() => {}),
    rootDirectory: options.rootDirectory ?? mkdtempSync(join(tmpdir(), "view-server-kafka-wrapper-root-")),
    runtimeDirectory:
      options.runtimeDirectory ?? mkdtempSync(join(tmpdir(), "view-server-kafka-wrapper-runtime-")),
    spawnProcess: options.spawnProcess ?? fakeSpawn.spawnProcess,
    stdio: "ignore",
  });

const settleSpawn = async (children: ReadonlyArray<FakeChildProcess>, index: number, code: number) => {
  await Promise.resolve();
  expect(children).toHaveLength(index + 1);
  children[index].emit("close", code);
};

const settleSpawnSignal = async (
  children: ReadonlyArray<FakeChildProcess>,
  index: number,
  signal: string,
) => {
  await Promise.resolve();
  expect(children).toHaveLength(index + 1);
  children[index].emit("close", null, signal);
};

describe("Kafka ingest benchmark wrapper", () => {
  it("runs build, docker, benchmark, and cleanup commands in order", async () => {
    const runtimeDirectory = mkdtempSync(join(tmpdir(), "view-server-kafka-wrapper-runtime-"));
    const rootDirectory = mkdtempSync(join(tmpdir(), "view-server-kafka-wrapper-root-"));
    const fakeSpawn = createFakeSpawn();
    const removedOutputPaths: Array<string> = [];
    const runner = createRunner(fakeSpawn, {
      env: {
        VIEW_SERVER_RUNTIME_BENCH_KAFKA_BATCH_SIZE: "250",
      },
      removeOutput: (path) => {
        removedOutputPaths.push(path);
      },
      rootDirectory,
      runtimeDirectory,
    });

    const exitCodePromise = runner.runMain();

    await settleSpawn(fakeSpawn.children, 0, 0);
    await settleSpawn(fakeSpawn.children, 1, 0);
    await settleSpawn(fakeSpawn.children, 2, 0);
    await settleSpawn(fakeSpawn.children, 3, 0);
    await settleSpawn(fakeSpawn.children, 4, 0);
    await settleSpawn(fakeSpawn.children, 5, 0);

    await expect(exitCodePromise).resolves.toBe(0);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
      ["vp", "run", "-t", "@view-server/runtime-core#build"],
      ["vp", "run", "-t", "@view-server/server#build"],
      ["docker", "compose", "-f", "compose.yaml", "up", "-d", "--wait", "kafka"],
      [
        "vp",
        "test",
        "bench",
        "src/kafka-ingest.bench.ts",
        "--run",
        "--testTimeout",
        "0",
        "--outputJson",
        ".artifacts/kafka-ingest-250rows.json",
      ],
      ["docker", "compose", "-f", "compose.yaml", "down"],
    ]);
    expect(fakeSpawn.calls.map(({ options }) => options.cwd)).toStrictEqual([
      runtimeDirectory,
      runtimeDirectory,
      runtimeDirectory,
      rootDirectory,
      runtimeDirectory,
      rootDirectory,
    ]);
    expect(fakeSpawn.calls.map(({ options }) => options.env?.COMPOSE_PROJECT_NAME)).toStrictEqual([
      undefined,
      undefined,
      undefined,
      "view-server-kafka-ingest-12345",
      undefined,
      "view-server-kafka-ingest-12345",
    ]);
    expect(removedOutputPaths).toStrictEqual([
      resolve(runtimeDirectory, ".artifacts/kafka-ingest-250rows.json"),
    ]);
  });

  it("skips Docker startup, benchmark, and cleanup when the first build fails", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn);

    const exitCodePromise = runner.runMain();

    await settleSpawn(fakeSpawn.children, 0, 1);

    await expect(exitCodePromise).resolves.toBe(1);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
    ]);
  });

  it("uses repository and runtime directory defaults when none are injected", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createKafkaIngestBenchmarkRunner({
      env: {},
      outputExists: () => true,
      removeOutput: () => {},
      spawnProcess: fakeSpawn.spawnProcess,
      stdio: "ignore",
    });

    const exitCodePromise = runner.runMain();

    await settleSpawn(fakeSpawn.children, 0, 1);

    await expect(exitCodePromise).resolves.toBe(1);
    expect(fakeSpawn.calls).toStrictEqual([
      {
        args: ["run", "-t", "@view-server/effect-utils#build"],
        command: "vp",
        options: {
          cwd: fileURLToPath(new URL("../packages/runtime/", import.meta.url)),
          env: {},
          stdio: "ignore",
        },
      },
    ]);
  });

  it("returns Docker startup failures after cleanup", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn);

    const exitCodePromise = runner.runMain();

    await settleSpawn(fakeSpawn.children, 0, 0);
    await settleSpawn(fakeSpawn.children, 1, 0);
    await settleSpawn(fakeSpawn.children, 2, 0);
    await settleSpawn(fakeSpawn.children, 3, 4);
    await settleSpawn(fakeSpawn.children, 4, 0);

    await expect(exitCodePromise).resolves.toBe(4);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
      ["vp", "run", "-t", "@view-server/runtime-core#build"],
      ["vp", "run", "-t", "@view-server/server#build"],
      ["docker", "compose", "-f", "compose.yaml", "up", "-d", "--wait", "kafka"],
      ["docker", "compose", "-f", "compose.yaml", "down"],
    ]);
  });

  it("ignores caller Compose project names so cleanup targets only this benchmark run", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn, {
      env: {
        COMPOSE_PROJECT_NAME: "developer-project",
        VIEW_SERVER_RUNTIME_BENCH_COMPOSE_PROJECT_NAME: "legacy-benchmark-project",
      },
      processId: 98765,
    });

    const exitCodePromise = runner.runMain();

    await settleSpawn(fakeSpawn.children, 0, 0);
    await settleSpawn(fakeSpawn.children, 1, 0);
    await settleSpawn(fakeSpawn.children, 2, 0);
    await settleSpawn(fakeSpawn.children, 3, 0);
    await settleSpawn(fakeSpawn.children, 4, 0);
    await settleSpawn(fakeSpawn.children, 5, 0);

    await expect(exitCodePromise).resolves.toBe(0);
    expect(
      fakeSpawn.calls
        .filter(({ command }) => command === "docker")
        .map(({ options }) => options.env?.COMPOSE_PROJECT_NAME),
    ).toStrictEqual(["view-server-kafka-ingest-98765", "view-server-kafka-ingest-98765"]);
  });

  it("returns benchmark failures after cleanup", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn);

    const exitCodePromise = runner.runMain();

    await settleSpawn(fakeSpawn.children, 0, 0);
    await settleSpawn(fakeSpawn.children, 1, 0);
    await settleSpawn(fakeSpawn.children, 2, 0);
    await settleSpawn(fakeSpawn.children, 3, 0);
    await settleSpawn(fakeSpawn.children, 4, 9);
    await settleSpawn(fakeSpawn.children, 5, 0);

    await expect(exitCodePromise).resolves.toBe(9);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
      ["vp", "run", "-t", "@view-server/runtime-core#build"],
      ["vp", "run", "-t", "@view-server/server#build"],
      ["docker", "compose", "-f", "compose.yaml", "up", "-d", "--wait", "kafka"],
      [
        "vp",
        "test",
        "bench",
        "src/kafka-ingest.bench.ts",
        "--run",
        "--testTimeout",
        "0",
        "--outputJson",
        ".artifacts/kafka-ingest-1000rows.json",
      ],
      ["docker", "compose", "-f", "compose.yaml", "down"],
    ]);
  });

  it("returns failure after cleanup when stale output removal fails", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn, {
      removeOutput: () => {
        throw new Error("cannot remove stale output");
      },
    });

    const exitCodePromise = runner.runMain();

    await settleSpawn(fakeSpawn.children, 0, 0);
    await settleSpawn(fakeSpawn.children, 1, 0);
    await settleSpawn(fakeSpawn.children, 2, 0);
    await settleSpawn(fakeSpawn.children, 3, 0);
    await settleSpawn(fakeSpawn.children, 4, 0);

    await expect(exitCodePromise).resolves.toBe(1);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
      ["vp", "run", "-t", "@view-server/runtime-core#build"],
      ["vp", "run", "-t", "@view-server/server#build"],
      ["docker", "compose", "-f", "compose.yaml", "up", "-d", "--wait", "kafka"],
      ["docker", "compose", "-f", "compose.yaml", "down"],
    ]);
  });

  it("cleans Docker when stale output removal throws after startup", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn, {
      removeOutput: () => {
        throw new Error("cannot remove old artifact");
      },
    });

    const exitCodePromise = runner.runMain();

    await settleSpawn(fakeSpawn.children, 0, 0);
    await settleSpawn(fakeSpawn.children, 1, 0);
    await settleSpawn(fakeSpawn.children, 2, 0);
    await settleSpawn(fakeSpawn.children, 3, 0);
    await settleSpawn(fakeSpawn.children, 4, 0);

    await expect(exitCodePromise).resolves.toBe(1);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
      ["vp", "run", "-t", "@view-server/runtime-core#build"],
      ["vp", "run", "-t", "@view-server/server#build"],
      ["docker", "compose", "-f", "compose.yaml", "up", "-d", "--wait", "kafka"],
      ["docker", "compose", "-f", "compose.yaml", "down"],
    ]);
  });

  it("cleans Docker when benchmark process spawning throws after startup", async () => {
    const fakeSpawn = createFakeSpawn();
    let spawnIndex = 0;
    const throwingSpawn = () => {
      throw new Error("cannot spawn benchmark");
    };
    const spawnSteps = [
      fakeSpawn.spawnProcess,
      fakeSpawn.spawnProcess,
      fakeSpawn.spawnProcess,
      fakeSpawn.spawnProcess,
      throwingSpawn,
      fakeSpawn.spawnProcess,
    ];
    const runner = createRunner(fakeSpawn, {
      spawnProcess: (command, args, options) => spawnSteps[spawnIndex++](command, args, options),
    });

    const exitCodePromise = runner.runMain();

    await settleSpawn(fakeSpawn.children, 0, 0);
    await settleSpawn(fakeSpawn.children, 1, 0);
    await settleSpawn(fakeSpawn.children, 2, 0);
    await settleSpawn(fakeSpawn.children, 3, 0);
    await settleSpawn(fakeSpawn.children, 4, 0);

    await expect(exitCodePromise).resolves.toBe(1);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
      ["vp", "run", "-t", "@view-server/runtime-core#build"],
      ["vp", "run", "-t", "@view-server/server#build"],
      ["docker", "compose", "-f", "compose.yaml", "up", "-d", "--wait", "kafka"],
      ["docker", "compose", "-f", "compose.yaml", "down"],
    ]);
  });

  it("returns failure when the benchmark output file is missing", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn, {
      outputExists: () => false,
    });

    const exitCodePromise = runner.runMain();

    await settleSpawn(fakeSpawn.children, 0, 0);
    await settleSpawn(fakeSpawn.children, 1, 0);
    await settleSpawn(fakeSpawn.children, 2, 0);
    await settleSpawn(fakeSpawn.children, 3, 0);
    await settleSpawn(fakeSpawn.children, 4, 0);
    await settleSpawn(fakeSpawn.children, 5, 0);

    await expect(exitCodePromise).resolves.toBe(1);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
      ["vp", "run", "-t", "@view-server/runtime-core#build"],
      ["vp", "run", "-t", "@view-server/server#build"],
      ["docker", "compose", "-f", "compose.yaml", "up", "-d", "--wait", "kafka"],
      [
        "vp",
        "test",
        "bench",
        "src/kafka-ingest.bench.ts",
        "--run",
        "--testTimeout",
        "0",
        "--outputJson",
        ".artifacts/kafka-ingest-1000rows.json",
      ],
      ["docker", "compose", "-f", "compose.yaml", "down"],
    ]);
  });

  it("uses sustained-firehose output naming and caller-provided Kafka bootstrap servers", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn, {
      env: {
        VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS: "kafka.local:9092",
        VIEW_SERVER_RUNTIME_BENCH_KAFKA_BATCH_SIZE: "25",
        VIEW_SERVER_RUNTIME_BENCH_KAFKA_MODE: "sustained-firehose",
        VIEW_SERVER_RUNTIME_BENCH_KAFKA_SUSTAINED_BATCHES: "8",
      },
    });

    const exitCodePromise = runner.runMain();

    await settleSpawn(fakeSpawn.children, 0, 0);
    await settleSpawn(fakeSpawn.children, 1, 0);
    await settleSpawn(fakeSpawn.children, 2, 0);
    await settleSpawn(fakeSpawn.children, 3, 0);
    await settleSpawn(fakeSpawn.children, 4, 0);
    await settleSpawn(fakeSpawn.children, 5, 0);

    await expect(exitCodePromise).resolves.toBe(0);
    expect(fakeSpawn.calls[4].args).toStrictEqual([
      "test",
      "bench",
      "src/kafka-ingest.bench.ts",
      "--run",
      "--testTimeout",
      "0",
      "--outputJson",
      ".artifacts/kafka-sustained-firehose-25rows-8batches.json",
    ]);
    expect(fakeSpawn.calls[4].options.env?.VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS).toBe(
      "kafka.local:9092",
    );
  });

  it("passes absolute output paths through to Vitest bench", async () => {
    const absoluteOutputPath = resolve(tmpdir(), "view-server-kafka-wrapper-output.json");
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn, {
      env: {
        VIEW_SERVER_RUNTIME_BENCH_OUTPUT_JSON: absoluteOutputPath,
      },
    });

    const exitCodePromise = runner.runMain();

    await settleSpawn(fakeSpawn.children, 0, 0);
    await settleSpawn(fakeSpawn.children, 1, 0);
    await settleSpawn(fakeSpawn.children, 2, 0);
    await settleSpawn(fakeSpawn.children, 3, 0);
    await settleSpawn(fakeSpawn.children, 4, 0);
    await settleSpawn(fakeSpawn.children, 5, 0);

    await expect(exitCodePromise).resolves.toBe(0);
    expect(fakeSpawn.calls[4].args).toStrictEqual([
      "test",
      "bench",
      "src/kafka-ingest.bench.ts",
      "--run",
      "--testTimeout",
      "0",
      "--outputJson",
      absoluteOutputPath,
    ]);
  });

  it("uses the default sustained-firehose batch count when none is configured", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn, {
      env: {
        VIEW_SERVER_RUNTIME_BENCH_KAFKA_BATCH_SIZE: "10",
        VIEW_SERVER_RUNTIME_BENCH_KAFKA_MODE: "sustained-firehose",
      },
    });

    const exitCodePromise = runner.runMain();

    await settleSpawn(fakeSpawn.children, 0, 0);
    await settleSpawn(fakeSpawn.children, 1, 0);
    await settleSpawn(fakeSpawn.children, 2, 0);
    await settleSpawn(fakeSpawn.children, 3, 0);
    await settleSpawn(fakeSpawn.children, 4, 0);
    await settleSpawn(fakeSpawn.children, 5, 0);

    await expect(exitCodePromise).resolves.toBe(0);
    expect(fakeSpawn.calls[4].args).toStrictEqual([
      "test",
      "bench",
      "src/kafka-ingest.bench.ts",
      "--run",
      "--testTimeout",
      "0",
      "--outputJson",
      ".artifacts/kafka-sustained-firehose-10rows-4batches.json",
    ]);
  });

  it("returns spawn errors before Docker startup without cleanup", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn);

    const exitCodePromise = runner.runMain();

    await Promise.resolve();
    expect(fakeSpawn.children).toHaveLength(1);
    fakeSpawn.children[0].emit("error", new Error("build failed"));
    await expect(runner.cleanup()).resolves.toBe(0);

    await expect(exitCodePromise).resolves.toBe(1);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
    ]);
  });

  it("returns failure when spawning a child throws synchronously", async () => {
    const runner = createKafkaIngestBenchmarkRunner({
      env: {},
      outputExists: () => true,
      removeOutput: () => {},
      spawnProcess: () => {
        throw new Error("spawn unavailable");
      },
      stdio: "ignore",
    });

    await expect(runner.runMain()).resolves.toBe(1);
    await expect(runner.cleanup()).resolves.toBe(0);
  });

  it("preserves Docker cleanup failures across repeated cleanup calls", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn);

    const exitCodePromise = runner.runMain();
    await settleSpawn(fakeSpawn.children, 0, 0);
    await settleSpawn(fakeSpawn.children, 1, 0);
    await settleSpawn(fakeSpawn.children, 2, 0);
    await settleSpawn(fakeSpawn.children, 3, 4);
    await settleSpawn(fakeSpawn.children, 4, 7);

    await expect(exitCodePromise).resolves.toBe(4);
    await expect(runner.cleanup()).resolves.toBe(7);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
      ["vp", "run", "-t", "@view-server/runtime-core#build"],
      ["vp", "run", "-t", "@view-server/server#build"],
      ["docker", "compose", "-f", "compose.yaml", "up", "-d", "--wait", "kafka"],
      ["docker", "compose", "-f", "compose.yaml", "down"],
    ]);
  });

  it("maps signalled child exits and still cleans Docker", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn);

    const exitCodePromise = runner.runMain();

    await settleSpawnSignal(fakeSpawn.children, 0, "SIGKILL");

    await expect(exitCodePromise).resolves.toBe(137);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
    ]);
  });

  it("maps interrupted child exits and still cleans Docker", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn);

    const exitCodePromise = runner.runMain();

    await settleSpawnSignal(fakeSpawn.children, 0, "SIGINT");

    await expect(exitCodePromise).resolves.toBe(130);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
    ]);
  });

  it("maps hung-up child exits and still cleans Docker", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn);

    const exitCodePromise = runner.runMain();

    await settleSpawnSignal(fakeSpawn.children, 0, "SIGHUP");

    await expect(exitCodePromise).resolves.toBe(129);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
    ]);
  });

  it("maps unknown child signal exits to failure and still cleans Docker", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn);

    const exitCodePromise = runner.runMain();

    await settleSpawnSignal(fakeSpawn.children, 0, "SIGUSR1");

    await expect(exitCodePromise).resolves.toBe(1);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
    ]);
  });

  it("stops before server build when interrupted during runtime-core build", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn);

    const exitCodePromise = runner.runMain();
    await settleSpawn(fakeSpawn.children, 0, 0);
    await Promise.resolve();
    expect(fakeSpawn.children).toHaveLength(2);
    const interruptPromise = runner.handleSignal("SIGINT");

    await expect(interruptPromise).resolves.toBe(130);
    await expect(exitCodePromise).resolves.toBe(130);
    expect(fakeSpawn.children[1].killSignals).toStrictEqual(["SIGTERM"]);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
      ["vp", "run", "-t", "@view-server/runtime-core#build"],
    ]);
  });

  it("stops before Docker startup when interrupted during server build", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn);

    const exitCodePromise = runner.runMain();
    await settleSpawn(fakeSpawn.children, 0, 0);
    await settleSpawn(fakeSpawn.children, 1, 0);
    await Promise.resolve();
    expect(fakeSpawn.children).toHaveLength(3);
    const interruptPromise = runner.handleSignal("SIGINT");

    await expect(interruptPromise).resolves.toBe(130);
    await expect(exitCodePromise).resolves.toBe(130);
    expect(fakeSpawn.children[2].killSignals).toStrictEqual(["SIGTERM"]);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
      ["vp", "run", "-t", "@view-server/runtime-core#build"],
      ["vp", "run", "-t", "@view-server/server#build"],
    ]);
  });

  it("cleans Docker when interrupted during the benchmark run", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn);

    const exitCodePromise = runner.runMain();
    await settleSpawn(fakeSpawn.children, 0, 0);
    await settleSpawn(fakeSpawn.children, 1, 0);
    await settleSpawn(fakeSpawn.children, 2, 0);
    await settleSpawn(fakeSpawn.children, 3, 0);
    await Promise.resolve();
    expect(fakeSpawn.children).toHaveLength(5);
    const interruptPromise = runner.handleSignal("SIGINT");
    await settleSpawn(fakeSpawn.children, 5, 0);

    await expect(interruptPromise).resolves.toBe(130);
    await expect(exitCodePromise).resolves.toBe(130);
    expect(fakeSpawn.children[4].killSignals).toStrictEqual(["SIGTERM"]);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
      ["vp", "run", "-t", "@view-server/runtime-core#build"],
      ["vp", "run", "-t", "@view-server/server#build"],
      ["docker", "compose", "-f", "compose.yaml", "up", "-d", "--wait", "kafka"],
      [
        "vp",
        "test",
        "bench",
        "src/kafka-ingest.bench.ts",
        "--run",
        "--testTimeout",
        "0",
        "--outputJson",
        ".artifacts/kafka-ingest-1000rows.json",
      ],
      ["docker", "compose", "-f", "compose.yaml", "down"],
    ]);
  });

  it("does not let stale child errors clear the active cleanup child", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn);

    const exitCodePromise = runner.runMain();
    await settleSpawn(fakeSpawn.children, 0, 0);
    await settleSpawn(fakeSpawn.children, 1, 0);
    await settleSpawn(fakeSpawn.children, 2, 0);
    const cleanupPromise = runner.cleanup();
    await Promise.resolve();
    expect(fakeSpawn.children).toHaveLength(5);
    fakeSpawn.children[3].emit("error", new Error("stale startup failed"));
    await settleSpawn(fakeSpawn.children, 4, 0);

    await expect(cleanupPromise).resolves.toBe(0);
    await expect(exitCodePromise).resolves.toBe(1);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
      ["vp", "run", "-t", "@view-server/runtime-core#build"],
      ["vp", "run", "-t", "@view-server/server#build"],
      ["docker", "compose", "-f", "compose.yaml", "up", "-d", "--wait", "kafka"],
      ["docker", "compose", "-f", "compose.yaml", "down"],
    ]);
  });

  it("does not let stale child closes clear the active cleanup child", async () => {
    const fakeSpawn = createFakeSpawn();
    const runner = createRunner(fakeSpawn);

    const exitCodePromise = runner.runMain();
    await settleSpawn(fakeSpawn.children, 0, 0);
    await settleSpawn(fakeSpawn.children, 1, 0);
    await settleSpawn(fakeSpawn.children, 2, 0);
    const cleanupPromise = runner.cleanup();
    await Promise.resolve();
    expect(fakeSpawn.children).toHaveLength(5);
    fakeSpawn.children[3].emit("close", 1);
    await settleSpawn(fakeSpawn.children, 4, 0);

    await expect(cleanupPromise).resolves.toBe(0);
    await expect(exitCodePromise).resolves.toBe(1);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
      ["vp", "run", "-t", "@view-server/runtime-core#build"],
      ["vp", "run", "-t", "@view-server/server#build"],
      ["docker", "compose", "-f", "compose.yaml", "up", "-d", "--wait", "kafka"],
      ["docker", "compose", "-f", "compose.yaml", "down"],
    ]);
  });

  it("maps handled signals to process exit codes without an active child", async () => {
    const interruptSpawn = createFakeSpawn();
    const hangupSpawn = createFakeSpawn();
    const interruptRunner = createRunner(interruptSpawn);
    const hangupRunner = createRunner(hangupSpawn);

    const interruptPromise = interruptRunner.handleSignal("SIGINT");
    const hangupPromise = hangupRunner.handleSignal("SIGHUP");

    await expect(interruptPromise).resolves.toBe(130);
    await expect(hangupPromise).resolves.toBe(129);
    expect(interruptSpawn.calls).toStrictEqual([]);
    expect(hangupSpawn.calls).toStrictEqual([]);
  });

  it("terminates the active child without Docker cleanup before Compose starts", async () => {
    const fakeSpawn = createFakeSpawn();
    const processEvents = new EventEmitter();
    const exitCodes: Array<number> = [];
    const exitCodePromise = runKafkaIngestBenchmarkCli({
      env: {},
      exit: (code?: number | string | null) => {
        exitCodes.push(Number(code));
      },
      outputExists: () => true,
      removeOutput: () => {},
      processEvents,
      rootDirectory: mkdtempSync(join(tmpdir(), "view-server-kafka-wrapper-root-")),
      runtimeDirectory: mkdtempSync(join(tmpdir(), "view-server-kafka-wrapper-runtime-")),
      spawnProcess: fakeSpawn.spawnProcess,
      stdio: "ignore",
    });

    await Promise.resolve();
    expect(fakeSpawn.children).toHaveLength(1);
    processEvents.emit("SIGTERM");

    await expect(exitCodePromise).resolves.toBe(143);
    expect(exitCodes).toStrictEqual([143]);
    expect(fakeSpawn.children[0].killSignals).toStrictEqual(["SIGTERM"]);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
    ]);
  });

  it("returns cleanup failure when a signal arrives after Compose starts and Docker cleanup fails", async () => {
    const fakeSpawn = createFakeSpawn();
    const processEvents = new EventEmitter();
    const exitCodes: Array<number> = [];
    const exitCodePromise = runKafkaIngestBenchmarkCli({
      env: {},
      exit: (code?: number | string | null) => {
        exitCodes.push(Number(code));
      },
      outputExists: () => true,
      removeOutput: () => {},
      processEvents,
      rootDirectory: mkdtempSync(join(tmpdir(), "view-server-kafka-wrapper-root-")),
      runtimeDirectory: mkdtempSync(join(tmpdir(), "view-server-kafka-wrapper-runtime-")),
      spawnProcess: fakeSpawn.spawnProcess,
      stdio: "ignore",
    });

    await settleSpawn(fakeSpawn.children, 0, 0);
    await settleSpawn(fakeSpawn.children, 1, 0);
    await settleSpawn(fakeSpawn.children, 2, 0);
    await Promise.resolve();
    expect(fakeSpawn.children).toHaveLength(4);
    processEvents.emit("SIGTERM");
    await settleSpawn(fakeSpawn.children, 4, 7);

    await expect(exitCodePromise).resolves.toBe(7);
    expect(exitCodes).toStrictEqual([7]);
    expect(fakeSpawn.children[3].killSignals).toStrictEqual(["SIGTERM"]);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
      ["vp", "run", "-t", "@view-server/runtime-core#build"],
      ["vp", "run", "-t", "@view-server/server#build"],
      ["docker", "compose", "-f", "compose.yaml", "up", "-d", "--wait", "kafka"],
      ["docker", "compose", "-f", "compose.yaml", "down"],
    ]);
  });

  it("exits once with the normal run result when no signal arrives", async () => {
    const fakeSpawn = createFakeSpawn();
    const processEvents = new EventEmitter();
    const exitCodes: Array<number> = [];
    const exitCodePromise = runKafkaIngestBenchmarkCli({
      env: {},
      exit: (code?: number | string | null) => {
        exitCodes.push(Number(code));
      },
      outputExists: () => true,
      removeOutput: () => {},
      processEvents,
      rootDirectory: mkdtempSync(join(tmpdir(), "view-server-kafka-wrapper-root-")),
      runtimeDirectory: mkdtempSync(join(tmpdir(), "view-server-kafka-wrapper-runtime-")),
      spawnProcess: fakeSpawn.spawnProcess,
      stdio: "ignore",
    });

    await settleSpawn(fakeSpawn.children, 0, 0);
    await settleSpawn(fakeSpawn.children, 1, 0);
    await settleSpawn(fakeSpawn.children, 2, 0);
    await settleSpawn(fakeSpawn.children, 3, 0);
    await settleSpawn(fakeSpawn.children, 4, 0);
    await settleSpawn(fakeSpawn.children, 5, 0);

    await expect(exitCodePromise).resolves.toBe(0);
    expect(exitCodes).toStrictEqual([0]);
  });

  it("does not terminate Docker cleanup when another signal arrives during cleanup", async () => {
    const fakeSpawn = createFakeSpawn();
    const processEvents = new EventEmitter();
    const exitCodes: Array<number> = [];
    const exitCodePromise = runKafkaIngestBenchmarkCli({
      env: {},
      exit: (code?: number | string | null) => {
        exitCodes.push(Number(code));
      },
      outputExists: () => true,
      removeOutput: () => {},
      processEvents,
      rootDirectory: mkdtempSync(join(tmpdir(), "view-server-kafka-wrapper-root-")),
      runtimeDirectory: mkdtempSync(join(tmpdir(), "view-server-kafka-wrapper-runtime-")),
      spawnProcess: fakeSpawn.spawnProcess,
      stdio: "ignore",
    });

    await settleSpawn(fakeSpawn.children, 0, 0);
    await settleSpawn(fakeSpawn.children, 1, 0);
    await settleSpawn(fakeSpawn.children, 2, 0);
    await Promise.resolve();
    expect(fakeSpawn.children).toHaveLength(4);
    processEvents.emit("SIGTERM");
    await Promise.resolve();
    expect(fakeSpawn.children).toHaveLength(5);
    processEvents.emit("SIGINT");
    await settleSpawn(fakeSpawn.children, 4, 0);

    await expect(exitCodePromise).resolves.toBe(143);
    expect(exitCodes).toStrictEqual([143]);
    expect(fakeSpawn.children[4].killSignals).toStrictEqual([]);
    expect(fakeSpawn.calls.map(({ command, args }) => [command, ...args])).toStrictEqual([
      ["vp", "run", "-t", "@view-server/effect-utils#build"],
      ["vp", "run", "-t", "@view-server/runtime-core#build"],
      ["vp", "run", "-t", "@view-server/server#build"],
      ["docker", "compose", "-f", "compose.yaml", "up", "-d", "--wait", "kafka"],
      ["docker", "compose", "-f", "compose.yaml", "down"],
    ]);
  });
});
