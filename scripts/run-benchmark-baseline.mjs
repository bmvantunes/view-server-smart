import { spawn } from "node:child_process";
import { runBenchmarkBaselineCli } from "./benchmark-baseline-cli.mjs";
import { runBenchmarkBaseline } from "./benchmark-baseline-runner.mjs";

const exitCode = await runBenchmarkBaselineCli({
  argv: process.argv,
  environment: process.env,
  logger: console,
  processLike: process,
  runBaseline: runBenchmarkBaseline,
  spawn,
});

process.exit(exitCode);
