import { spawn } from "node:child_process";
import { existsSync, rmSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { runKafkaIngestBenchmarkCli } from "./bench-runtime-kafka-ingest.mjs";

void runKafkaIngestBenchmarkCli({
  env: process.env,
  exit: process.exit,
  outputExists: existsSync,
  removeOutput: (path) => {
    rmSync(path, { force: true });
  },
  processEvents: process,
  rootDirectory: fileURLToPath(new URL("../", import.meta.url)),
  runtimeDirectory: fileURLToPath(new URL("../packages/runtime/", import.meta.url)),
  spawnProcess: spawn,
  stdio: "inherit",
});
