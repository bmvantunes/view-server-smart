// Benchmarks intentionally import Vitest directly: @effect/vitest does not expose `bench`.
import { afterAll, bench, describe, expect } from "vitest";
import {
  benchmarkOutputJsonPath,
  memorySnapshot,
  writeBenchmarkArtifact,
} from "./benchmark-artifact";
import { deltaOperations, type QueryEvaluation } from "./query-result";
import type { DeltaOperation } from "@view-server/config";

declare const process: {
  readonly env: Record<string, string | undefined>;
};

type Row = {
  readonly id: string;
  readonly score: number;
};

type DeltaOperationCaseName =
  | "head-replacement-batch"
  | "middle-replacement-batch"
  | "tail-replacement-batch";

type BenchmarkCase = {
  readonly effectiveOperationCount: number;
  readonly expectedOperations: ReadonlyArray<DeltaOperation<Row>>;
  readonly expectedFirstOperationType: string;
  readonly expectedOperationCount: number;
  readonly label: string;
  readonly next: QueryEvaluation<Row>;
  readonly previous: QueryEvaluation<Row>;
};

const defaultBenchmarkTimeMs = 250;
const defaultIterations = 5;
const defaultOperationCount = 64;
const defaultRowCount = 10_000;
const defaultWarmupIterations = 0;
const defaultWarmupTimeMs = 0;

const positiveIntegerFromEnv = (name: string, fallback: number): number => {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  const trimmed = raw.trim();
  if (!/^[1-9]\d*$/u.test(trimmed)) {
    throw new Error(`${name} must be a positive integer.`);
  }
  const parsed = Number.parseInt(trimmed, 10);
  if (Number.isSafeInteger(parsed) && parsed > 0) {
    return parsed;
  }
  throw new Error(`${name} must be a positive integer.`);
};

const nonNegativeIntegerFromEnv = (name: string, fallback: number): number => {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  const trimmed = raw.trim();
  if (!/^(0|[1-9]\d*)$/u.test(trimmed)) {
    throw new Error(`${name} must be a non-negative integer.`);
  }
  const parsed = Number.parseInt(trimmed, 10);
  if (Number.isSafeInteger(parsed) && parsed >= 0) {
    return parsed;
  }
  throw new Error(`${name} must be a non-negative integer.`);
};

const operationCaseNameFromEnv = (): DeltaOperationCaseName => {
  const raw = process.env["VIEW_SERVER_ENGINE_BENCH_DELTA_OPERATION_CASE"];
  if (raw === undefined || raw.trim() === "") {
    return "head-replacement-batch";
  }
  const trimmed = raw.trim();
  if (
    trimmed === "head-replacement-batch" ||
    trimmed === "middle-replacement-batch" ||
    trimmed === "tail-replacement-batch"
  ) {
    return trimmed;
  }
  throw new Error(
    "VIEW_SERVER_ENGINE_BENCH_DELTA_OPERATION_CASE must be head-replacement-batch, middle-replacement-batch, or tail-replacement-batch.",
  );
};

const benchmarkRowCount = positiveIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_ROWS", defaultRowCount);
const operationCount = positiveIntegerFromEnv(
  "VIEW_SERVER_ENGINE_BENCH_DELTA_OPERATION_COUNT",
  defaultOperationCount,
);
const deltaOperationCaseName = operationCaseNameFromEnv();
const memoryBefore = memorySnapshot();
const benchOptions = {
  iterations: positiveIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_ITERATIONS", defaultIterations),
  time: nonNegativeIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_TIME_MS", defaultBenchmarkTimeMs),
  warmupIterations: nonNegativeIntegerFromEnv(
    "VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS",
    defaultWarmupIterations,
  ),
  warmupTime: nonNegativeIntegerFromEnv(
    "VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS",
    defaultWarmupTimeMs,
  ),
};

if (operationCount >= benchmarkRowCount) {
  throw new Error("VIEW_SERVER_ENGINE_BENCH_DELTA_OPERATION_COUNT must be smaller than rows.");
}

const row = (index: number): Row => ({
  id: `row-${index}`,
  score: index,
});

const evaluationFromRows = (rows: ReadonlyArray<Row>, version: number): QueryEvaluation<Row> => ({
  keys: rows.map((entry) => entry.id),
  rows,
  totalRows: rows.length,
  version,
  window: rows.map((entry) => ({
    key: entry.id,
    row: entry,
  })),
});

const previousRows = Array.from({ length: benchmarkRowCount }, (_value, index) => row(index));

const replacementRows = (prefix: string): ReadonlyArray<Row> =>
  Array.from({ length: operationCount }, (_value, index) => ({
    id: `${prefix}-${index}`,
    score: 1_000_000 + index,
  }));

const headReplacementRows = replacementRows("head");
const middleReplacementRows = replacementRows("middle");
const tailReplacementRows = replacementRows("tail");
const middleStartIndex = Math.floor((benchmarkRowCount - operationCount) / 2);
const headReplacementNextRows = [...headReplacementRows, ...previousRows.slice(operationCount)];
const middleReplacementNextRows = [
  ...previousRows.slice(0, middleStartIndex),
  ...middleReplacementRows,
  ...previousRows.slice(middleStartIndex + operationCount),
];
const tailReplacementNextRows = [
  ...previousRows.slice(0, benchmarkRowCount - operationCount),
  ...tailReplacementRows,
];

const replacementRemoveOperations = (
  removedRows: ReadonlyArray<Row>,
): ReadonlyArray<DeltaOperation<Row>> =>
  removedRows.map((removedRow) => ({
    type: "remove",
    key: removedRow.id,
  }));

const replacementInsertOperations = (
  insertedRows: ReadonlyArray<Row>,
  offset: number,
): ReadonlyArray<DeltaOperation<Row>> =>
  insertedRows.map((insertedRow, index) => ({
    type: "insert",
    key: insertedRow.id,
    row: insertedRow,
    index: offset + index,
  }));

const benchmarkCases: Record<DeltaOperationCaseName, BenchmarkCase> = {
  "head-replacement-batch": {
    effectiveOperationCount: operationCount * 2,
    expectedOperations: [
      ...replacementRemoveOperations(previousRows.slice(0, operationCount)),
      ...replacementInsertOperations(headReplacementRows, 0),
    ],
    expectedFirstOperationType: "remove",
    expectedOperationCount: operationCount * 2,
    label: "delta operations head replacement batch",
    next: evaluationFromRows(headReplacementNextRows, 2),
    previous: evaluationFromRows(previousRows, 1),
  },
  "middle-replacement-batch": {
    effectiveOperationCount: operationCount * 2,
    expectedOperations: [
      ...replacementRemoveOperations(
        previousRows.slice(middleStartIndex, middleStartIndex + operationCount),
      ),
      ...replacementInsertOperations(middleReplacementRows, middleStartIndex),
    ],
    expectedFirstOperationType: "remove",
    expectedOperationCount: operationCount * 2,
    label: "delta operations middle replacement batch",
    next: evaluationFromRows(middleReplacementNextRows, 2),
    previous: evaluationFromRows(previousRows, 1),
  },
  "tail-replacement-batch": {
    effectiveOperationCount: operationCount * 2,
    expectedOperations: [
      ...replacementRemoveOperations(previousRows.slice(benchmarkRowCount - operationCount)),
      ...replacementInsertOperations(tailReplacementRows, benchmarkRowCount - operationCount),
    ],
    expectedFirstOperationType: "remove",
    expectedOperationCount: operationCount * 2,
    label: "delta operations tail replacement batch",
    next: evaluationFromRows(tailReplacementNextRows, 2),
    previous: evaluationFromRows(previousRows, 1),
  },
};

const benchmarkCase = benchmarkCases[deltaOperationCaseName];
const outputJsonPath = benchmarkOutputJsonPath(
  `query-delta-operations-${deltaOperationCaseName}-${benchmarkRowCount}rows-${benchmarkCase.effectiveOperationCount}ops.json`,
);
const memoryAfterSetup = memorySnapshot();

const validateCase = (operations: ReturnType<typeof deltaOperations<Row>>): void => {
  expect(operations.length).toBe(benchmarkCase.expectedOperationCount);
  expect(operations[0]?.type).toBe(benchmarkCase.expectedFirstOperationType);
  expect(operations).toStrictEqual(benchmarkCase.expectedOperations);
};

validateCase(deltaOperations(benchmarkCase.previous, benchmarkCase.next));

const runCase = (): void => {
  const operations = deltaOperations(benchmarkCase.previous, benchmarkCase.next);
  expect(operations.length).toBe(benchmarkCase.expectedOperationCount);
};

afterAll(() => {
  writeBenchmarkArtifact({
    artifactKind: "engine-benchmark-summary",
    backpressureCount: 0,
    benchmarkCases: [benchmarkCase.label],
    benchmarkName: `query delta operations benchmark (${deltaOperationCaseName})`,
    benchmarkScope: "engine-query-delta-operations",
    cleanupLeakCount: 0,
    health: {
      activeSubscriptions: 0,
      backpressureEvents: 0,
      maxQueueDepth: 0,
      queuedEvents: 0,
    },
    latency: {
      outputJsonPath,
      source: "vitest-output-json",
    },
    memoryAfterBenchmark: memorySnapshot(),
    memoryAfterSetup,
    memoryBefore,
    mutationCount: benchmarkCase.effectiveOperationCount,
    notes: [
      "Scanner-level benchmark for deltaOperations; no engine runtime or subscription resources are created.",
      "Operation count is the number of emitted delta operations for the selected previous/next windows.",
    ],
    outputJsonPath,
    queuedEventCount: 0,
    rowCount: benchmarkRowCount,
    subscriberCount: 1,
    topics: ["orders"],
  });
});

describe(`query delta operations benchmark: ${deltaOperationCaseName}, ${benchmarkRowCount} rows`, () => {
  bench(benchmarkCase.label, runCase, benchOptions);
});
