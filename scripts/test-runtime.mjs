import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const runtimePackage = new URL("../packages/runtime/", import.meta.url);
const runtimeDirectory = fileURLToPath(runtimePackage);
const kafkaBootstrapServers =
  process.env.VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS ?? "localhost:9092";
const londonKafkaBootstrapServers =
  process.env.VIEW_SERVER_KAFKA_LONDON_BOOTSTRAP_SERVERS ?? "localhost:9094";
const testArguments = process.argv.slice(2);
const vitestArguments = testArguments.filter((argument) => argument !== "--");
const testFilters = vitestArguments.filter(
  (argument) => argument !== "--" && !argument.startsWith("-") && argument.includes(".test"),
);
const hasCoverageFlag = testArguments.some(
  (argument) =>
    argument === "--coverage" ||
    argument.startsWith("--coverage=") ||
    argument === "--no-coverage",
);
const shouldCollectCoverage = vitestArguments.length === 0 && !hasCoverageFlag;
const shouldStartKafka =
  testFilters.length === 0 ||
  testFilters.some(
    (argument) =>
      argument.includes("kafka-ingress.test") && !argument.includes("kafka-ingress.internal.test"),
  );

const run = (command, args, options = {}) => {
  const result = spawnSync(command, args, {
    cwd: options.cwd ?? root,
    env: options.env ?? process.env,
    stdio: "inherit",
    shell: false,
  });
  return result.status ?? 1;
};

const vpTransitiveBuild = (packageTask) =>
  run("vp", ["run", "--concurrency-limit", "1", "-t", packageTask], {
    cwd: runtimeDirectory,
  });

let didCleanup = false;
const cleanup = () => {
  if (!shouldStartKafka || didCleanup) {
    return 0;
  }
  didCleanup = true;
  return run("docker", ["compose", "-f", "compose.yaml", "down"]);
};

process.once("SIGINT", () => {
  cleanup();
  process.exit(130);
});
process.once("SIGTERM", () => {
  cleanup();
  process.exit(143);
});

let exitCode = shouldStartKafka
  ? run("docker", [
      "compose",
      "-f",
      "compose.yaml",
      "up",
      "-d",
      "--wait",
      "kafka",
      "kafka-london",
    ])
  : 0;

if (exitCode === 0) {
  exitCode = vpTransitiveBuild("@effect-view-server/config#build");
}

if (exitCode === 0) {
  exitCode = vpTransitiveBuild("@effect-view-server/effect-utils#build");
}

if (exitCode === 0) {
  exitCode = vpTransitiveBuild("@effect-view-server/runtime-core#build");
}

if (exitCode === 0) {
  exitCode = vpTransitiveBuild("@effect-view-server/server#build");
}

if (exitCode === 0) {
  exitCode = run(
    "vp",
    [
      "test",
      "run",
      ...(shouldCollectCoverage ? ["--coverage"] : []),
      "--typecheck",
      ...vitestArguments,
    ],
    {
      cwd: runtimeDirectory,
      env: {
        ...process.env,
        VIEW_SERVER_KAFKA_BOOTSTRAP_SERVERS: kafkaBootstrapServers,
        VIEW_SERVER_KAFKA_LONDON_BOOTSTRAP_SERVERS: londonKafkaBootstrapServers,
      },
    },
  );
}

const cleanupExitCode = cleanup();
process.exit(exitCode === 0 ? cleanupExitCode : exitCode);
