import { exitCodeForSignal } from "./benchmark-baseline-runner.mjs";

export const childIsRunning = (child) => child.exitCode === null && child.signalCode === null;

export const createBenchmarkTaskRunner = ({ processLike, spawn }) => {
  let activeChild;
  let terminatingExitCode;

  const terminateActiveChild = (signal) => {
    if (activeChild !== undefined && childIsRunning(activeChild)) {
      activeChild.kill(signal);
    }
  };

  for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) {
    processLike.on(signal, () => {
      terminatingExitCode = exitCodeForSignal(signal);
      terminateActiveChild(signal);
    });
  }

  const runTask = (currentTask) =>
    new Promise((resolve, reject) => {
      const child = spawn(currentTask.command, currentTask.args, {
        env: currentTask.env,
        stdio: "inherit",
      });
      activeChild = child;

      child.on("error", (error) => {
        if (activeChild === child) {
          activeChild = undefined;
        }
        reject(error);
      });

      child.on("exit", (code, signal) => {
        if (activeChild === child) {
          activeChild = undefined;
        }
        if (terminatingExitCode !== undefined) {
          resolve(terminatingExitCode);
          return;
        }
        if (signal !== null) {
          resolve(exitCodeForSignal(signal));
          return;
        }
        resolve(code ?? 1);
      });
    });

  return {
    runTask,
    terminateActiveChild,
  };
};

export const runBenchmarkBaselineCli = ({
  argv,
  environment,
  logger,
  processLike,
  runBaseline,
  spawn,
}) => {
  const taskRunner = createBenchmarkTaskRunner({
    processLike,
    spawn,
  });
  return runBaseline({
    argv,
    environment,
    logger,
    runTask: taskRunner.runTask,
  });
};
