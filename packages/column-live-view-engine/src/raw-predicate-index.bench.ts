// Benchmarks intentionally import Vitest directly: @effect/vitest does not expose `bench`.
import { beforeAll, bench, describe, expect } from "vitest";
import { Schema } from "effect";
import { compareQueryValue } from "./query-value";
import { rawQueryCompilerMetadata } from "./raw-query-compiler";
import { fieldValue, scalarEqualityKey } from "./row-values";
import type { TopicRowEntry } from "./row-scan";
import type { TopicRawWindowScanPlan } from "./raw-window-scan";
import {
  createScalarPredicateIndexes,
  selectedPredicateCandidateSlots,
} from "./topic-predicate-candidate-index";
import { scanTopicRawWindow, type TopicRawWindowScanState } from "./topic-raw-window-scanner";

declare const process: {
  readonly env: Record<string, string | undefined>;
};

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Finite,
  region: Schema.String,
  updatedAt: Schema.Number,
});

type OrderRow = typeof Order.Type;
type OrderStatus = OrderRow["status"];
type PredicateBenchState = {
  readonly state: TopicRawWindowScanState;
};
type RowCallbackCounter = {
  count: number;
};
type CandidateExpectation = {
  readonly allowScalarIndexBuild: boolean;
  readonly exactRangeCandidates: boolean;
  readonly excludedField: string;
  readonly expectedSlots: ReadonlyArray<number> | undefined;
};
type BenchmarkCase = {
  readonly name: string;
  readonly counter: RowCallbackCounter;
  readonly candidateExpectation: CandidateExpectation;
  readonly expectedCallbackCount: number;
  readonly expectedKeys: ReadonlyArray<string>;
  readonly expectedTotalRows: number;
  readonly plan: TopicRawWindowScanPlan<object>;
};

const defaultRowCount = 100_000;
const minimumRowCount = 101;
const defaultBenchmarkTimeMs = 250;
const defaultIterations = 5;
const defaultWarmupIterations = 0;
const defaultWarmupTimeMs = 0;
const noExcludedField = "__view_server_bench_no_excluded_field__";

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

const rowCountFromEnv = (): number => {
  const raw = process.env["VIEW_SERVER_ENGINE_BENCH_ROWS"];
  if (raw === undefined || raw.trim() === "") {
    return defaultRowCount;
  }
  if (raw.includes(",")) {
    throw new Error("VIEW_SERVER_ENGINE_BENCH_ROWS accepts one row count per benchmark run.");
  }
  const parsed = positiveIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_ROWS", defaultRowCount);
  if (parsed < minimumRowCount) {
    throw new Error(`VIEW_SERVER_ENGINE_BENCH_ROWS must be at least ${minimumRowCount}.`);
  }
  return parsed;
};

const rowCount = rowCountFromEnv();
const benchOptions = {
  iterations: positiveIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_ITERATIONS", defaultIterations),
  time: positiveIntegerFromEnv("VIEW_SERVER_ENGINE_BENCH_TIME_MS", defaultBenchmarkTimeMs),
  warmupIterations: nonNegativeIntegerFromEnv(
    "VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS",
    defaultWarmupIterations,
  ),
  warmupTime: nonNegativeIntegerFromEnv(
    "VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS",
    defaultWarmupTimeMs,
  ),
};

let profile: PredicateBenchState | undefined;
const targetCustomerIndex = Math.floor(rowCount / 2);
const targetCustomerId = `customer-${targetCustomerIndex}`;
const targetLastCustomerIndex = rowCount - 10;
const targetCustomerIds = ["customer-10", targetCustomerId, `customer-${targetLastCustomerIndex}`];

const orderStatus = (index: number): OrderStatus => {
  if (index % 11 === 0) {
    return "cancelled";
  }
  if (index % 3 === 0) {
    return "closed";
  }
  return "open";
};

const region = (index: number): string => {
  if (index % 7 === 0) {
    return "apac";
  }
  if (index % 5 === 0) {
    return "amer";
  }
  return "emea";
};

const order = (index: number): OrderRow => ({
  id: `order-${index}`,
  customerId: `customer-${index}`,
  status: orderStatus(index),
  price: index,
  region: region(index),
  updatedAt: rowCount - index,
});

const profileState = (): PredicateBenchState => {
  if (profile === undefined) {
    throw new Error("Raw predicate index benchmark profile is not initialized.");
  }
  return profile;
};

const compareByUpdatedAtDesc = (
  left: TopicRowEntry<object>,
  right: TopicRowEntry<object>,
): number => {
  const updatedAtComparison = compareQueryValue(
    fieldValue(left.row, "updatedAt"),
    fieldValue(right.row, "updatedAt"),
  );
  if (updatedAtComparison !== 0) {
    return -updatedAtComparison;
  }
  return left.key.localeCompare(right.key);
};

const compareByCustomerIdAsc = (
  left: TopicRowEntry<object>,
  right: TopicRowEntry<object>,
): number => {
  const customerComparison = compareQueryValue(
    fieldValue(left.row, "customerId"),
    fieldValue(right.row, "customerId"),
  );
  if (customerComparison !== 0) {
    return customerComparison;
  }
  return left.key.localeCompare(right.key);
};

const rowHasCustomerId = (row: object, customerId: string): boolean =>
  fieldValue(row, "customerId") === customerId;

const rowHasAnyCustomerId = (row: object, customerIds: ReadonlyArray<string>): boolean => {
  const customerId = fieldValue(row, "customerId");
  for (const candidate of customerIds) {
    if (customerId === candidate) {
      return true;
    }
  }
  return false;
};

const rowPriceInUpperTail = (row: object): boolean => {
  const price = fieldValue(row, "price");
  return typeof price === "number" && price >= rowCount - 100;
};

const resetCounter = (counter: RowCallbackCounter): void => {
  counter.count = 0;
};

const orderKeysFromRange = (startInclusive: number, endExclusive: number): ReadonlyArray<string> =>
  Array.from(
    { length: endExclusive - startInclusive },
    (_value, index) => `order-${startInclusive + index}`,
  );

const slotIndexesFromRange = (
  startInclusive: number,
  endExclusive: number,
): ReadonlyArray<number> =>
  Array.from({ length: endExclusive - startInclusive }, (_value, index) => startInclusive + index);

const scalarKey = (value: string): string => {
  const key = scalarEqualityKey(value);
  if (key === undefined) {
    throw new Error(`Expected scalar equality key for ${value}.`);
  }
  return key;
};

const countedMatch = (
  counter: RowCallbackCounter,
  predicate: (row: object) => boolean,
): ((row: object) => boolean) => {
  return (row) => {
    counter.count += 1;
    return predicate(row);
  };
};

const unexpectedCallback = (message: string): ((row: object) => boolean) => {
  return () => {
    throw new Error(message);
  };
};

const fullScanSelectiveCounter: RowCallbackCounter = { count: 0 };
const fullScanUpperTailCounter: RowCallbackCounter = { count: 0 };
const selectiveScalarCounter: RowCallbackCounter = { count: 0 };
const multiKeyCounter: RowCallbackCounter = { count: 0 };
const broadRejectedCounter: RowCallbackCounter = { count: 0 };

const assertCandidateExpectation = (
  state: TopicRawWindowScanState,
  filters: TopicRawWindowScanPlan<object>["predicate"]["filters"],
  expectation: CandidateExpectation,
): void => {
  const candidateSlots = selectedPredicateCandidateSlots(state, filters, {
    allowScalarIndexBuild: expectation.allowScalarIndexBuild,
    exactRangeCandidates: expectation.exactRangeCandidates,
    excludedField: expectation.excludedField,
    maxSlotCount: state.slots.length,
  });
  expect(candidateSlots?.slots).toStrictEqual(expectation.expectedSlots);
};

const benchmarkCases = (): ReadonlyArray<BenchmarkCase> => {
  return [
    {
      name: "full scan callback baseline: selective customer + fallback sort",
      counter: fullScanSelectiveCounter,
      candidateExpectation: {
        allowScalarIndexBuild: false,
        exactRangeCandidates: false,
        excludedField: noExcludedField,
        expectedSlots: undefined,
      },
      expectedCallbackCount: rowCount,
      expectedKeys: [`order-${targetCustomerIndex}`],
      expectedTotalRows: 1,
      plan: {
        predicate: {
          filters: [],
          callbackRequired: true,
        },
        orderBy: [{ field: "updatedAt", direction: "desc" }],
        matches: countedMatch(fullScanSelectiveCounter, (row) =>
          rowHasCustomerId(row, targetCustomerId),
        ),
        compare: compareByUpdatedAtDesc,
        offset: 0,
        limit: 50,
      },
    },
    {
      name: "exact scalar candidate: selective customer + fallback sort",
      counter: selectiveScalarCounter,
      candidateExpectation: {
        allowScalarIndexBuild: false,
        exactRangeCandidates: false,
        excludedField: noExcludedField,
        expectedSlots: [targetCustomerIndex],
      },
      expectedCallbackCount: 1,
      expectedKeys: [`order-${targetCustomerIndex}`],
      expectedTotalRows: 1,
      plan: {
        predicate: {
          filters: [{ field: "customerId", operator: "eq", value: targetCustomerId }],
          callbackRequired: true,
        },
        orderBy: [{ field: "updatedAt", direction: "desc" }],
        matches: countedMatch(selectiveScalarCounter, (row) =>
          rowHasCustomerId(row, targetCustomerId),
        ),
        compare: compareByUpdatedAtDesc,
        offset: 0,
        limit: 50,
      },
    },
    {
      name: "exact scalar candidate: selective customer without callback",
      counter: { count: 0 },
      candidateExpectation: {
        allowScalarIndexBuild: false,
        exactRangeCandidates: false,
        excludedField: noExcludedField,
        expectedSlots: [targetCustomerIndex],
      },
      expectedCallbackCount: 0,
      expectedKeys: [`order-${targetCustomerIndex}`],
      expectedTotalRows: 1,
      plan: {
        predicate: {
          filters: [{ field: "customerId", operator: "eq", value: targetCustomerId }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [{ field: "updatedAt", direction: "desc" }],
        matches: unexpectedCallback("exact scalar candidate should not call row callbacks"),
        compare: compareByUpdatedAtDesc,
        offset: 0,
        limit: 50,
      },
    },
    {
      name: "exact multi-key in candidate: three customers + fallback sort",
      counter: multiKeyCounter,
      candidateExpectation: {
        allowScalarIndexBuild: false,
        exactRangeCandidates: false,
        excludedField: noExcludedField,
        expectedSlots: [10, targetCustomerIndex, targetLastCustomerIndex],
      },
      expectedCallbackCount: targetCustomerIds.length,
      expectedKeys: [
        `order-10`,
        `order-${targetCustomerIndex}`,
        `order-${targetLastCustomerIndex}`,
      ],
      expectedTotalRows: targetCustomerIds.length,
      plan: {
        predicate: {
          filters: [
            {
              field: "customerId",
              operator: "in",
              values: targetCustomerIds,
              valueKeys: new Set(targetCustomerIds.map(scalarKey)),
            },
          ],
          callbackRequired: true,
        },
        orderBy: [{ field: "updatedAt", direction: "desc" }],
        matches: countedMatch(multiKeyCounter, (row) =>
          rowHasAnyCustomerId(row, targetCustomerIds),
        ),
        compare: compareByUpdatedAtDesc,
        offset: 0,
        limit: 50,
      },
    },
    {
      name: "exact range candidate: upper price tail + fallback sort",
      counter: { count: 0 },
      candidateExpectation: {
        allowScalarIndexBuild: false,
        exactRangeCandidates: true,
        excludedField: noExcludedField,
        expectedSlots: slotIndexesFromRange(rowCount - 100, rowCount),
      },
      expectedCallbackCount: 0,
      expectedKeys: orderKeysFromRange(rowCount - 100, rowCount - 50),
      expectedTotalRows: 100,
      plan: {
        predicate: {
          filters: [{ field: "price", operator: "gte", value: rowCount - 100 }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [{ field: "updatedAt", direction: "desc" }],
        matches: unexpectedCallback("exact range candidate should not call row callbacks"),
        compare: compareByUpdatedAtDesc,
        offset: 0,
        limit: 50,
      },
    },
    {
      name: "full scan callback baseline: upper price tail + fallback sort",
      counter: fullScanUpperTailCounter,
      candidateExpectation: {
        allowScalarIndexBuild: false,
        exactRangeCandidates: false,
        excludedField: noExcludedField,
        expectedSlots: undefined,
      },
      expectedCallbackCount: rowCount,
      expectedKeys: orderKeysFromRange(rowCount - 100, rowCount - 50),
      expectedTotalRows: 100,
      plan: {
        predicate: {
          filters: [],
          callbackRequired: true,
        },
        orderBy: [{ field: "updatedAt", direction: "desc" }],
        matches: countedMatch(fullScanUpperTailCounter, rowPriceInUpperTail),
        compare: compareByUpdatedAtDesc,
        offset: 0,
        limit: 50,
      },
    },
    {
      name: "ordered equality seek: customer storage order",
      counter: { count: 0 },
      candidateExpectation: {
        allowScalarIndexBuild: false,
        exactRangeCandidates: true,
        excludedField: "customerId",
        expectedSlots: undefined,
      },
      expectedCallbackCount: 0,
      expectedKeys: [`order-${targetCustomerIndex}`],
      expectedTotalRows: 1,
      plan: {
        predicate: {
          filters: [{ field: "customerId", operator: "eq", value: targetCustomerId }],
          callbackRequired: false,
          callbackSkippable: true,
        },
        orderBy: [{ field: "customerId", direction: "asc" }],
        storageOrderBy: [{ field: "customerId", direction: "asc" }],
        matches: unexpectedCallback("ordered equality seek should not call row callbacks"),
        compare: compareByCustomerIdAsc,
        offset: 0,
        limit: 50,
      },
    },
    {
      name: "failed broad scalar candidate build + full scan",
      counter: broadRejectedCounter,
      candidateExpectation: {
        allowScalarIndexBuild: false,
        exactRangeCandidates: false,
        excludedField: noExcludedField,
        expectedSlots: undefined,
      },
      expectedCallbackCount: rowCount,
      expectedKeys: orderKeysFromRange(0, 50),
      expectedTotalRows: rowCount,
      plan: {
        predicate: {
          filters: [
            {
              field: "status",
              operator: "in",
              values: ["open", "closed", "cancelled"],
            },
          ],
          callbackRequired: true,
        },
        orderBy: [{ field: "updatedAt", direction: "desc" }],
        matches: countedMatch(broadRejectedCounter, () => true),
        compare: compareByUpdatedAtDesc,
        offset: 0,
        limit: 50,
      },
    },
  ];
};

const runBenchmarkCase = (benchmarkCase: BenchmarkCase): void => {
  const current = profileState();
  resetCounter(benchmarkCase.counter);
  assertCandidateExpectation(
    current.state,
    benchmarkCase.plan.predicate.filters,
    benchmarkCase.candidateExpectation,
  );
  const result = scanTopicRawWindow(current.state, benchmarkCase.plan);
  expect(result.keys).toStrictEqual(benchmarkCase.expectedKeys);
  expect(result.totalRows).toBe(benchmarkCase.expectedTotalRows);
  expect(benchmarkCase.counter.count).toBe(benchmarkCase.expectedCallbackCount);
};

beforeAll(() => {
  const rows = Array.from({ length: rowCount }, (_value, index) => order(index));
  const slots = rows.map((row) => ({
    key: row.id,
    row,
  }));
  const state: TopicRawWindowScanState = {
    columns: new Map<string, ReadonlyArray<unknown>>([
      ["id", rows.map((row) => row.id)],
      ["customerId", rows.map((row) => row.customerId)],
      ["status", rows.map((row) => row.status)],
      ["price", rows.map((row) => row.price)],
      ["region", rows.map((row) => row.region)],
      ["updatedAt", rows.map((row) => row.updatedAt)],
    ]),
    orderedSlotIndexes: new Map(),
    rawQueryMetadata: rawQueryCompilerMetadata(Order),
    scalarPredicateIndexes: createScalarPredicateIndexes(),
    slots,
  };
  profile = {
    state,
  };

  scanTopicRawWindow(state, {
    predicate: {
      filters: [{ field: "price", operator: "gte", value: 0 }],
      callbackRequired: false,
      callbackSkippable: true,
    },
    orderBy: [{ field: "price", direction: "asc" }],
    storageOrderBy: [{ field: "price", direction: "asc" }],
    matches: unexpectedCallback("range index warmup should not call row callbacks"),
    compare: compareByUpdatedAtDesc,
    offset: 0,
    limit: 1,
  });

  assertCandidateExpectation(
    state,
    [{ field: "customerId", operator: "eq", value: targetCustomerId }],
    {
      allowScalarIndexBuild: true,
      exactRangeCandidates: false,
      excludedField: noExcludedField,
      expectedSlots: [targetCustomerIndex],
    },
  );
  assertCandidateExpectation(
    state,
    [
      {
        field: "customerId",
        operator: "in",
        values: targetCustomerIds,
        valueKeys: new Set(targetCustomerIds.map(scalarKey)),
      },
    ],
    {
      allowScalarIndexBuild: true,
      exactRangeCandidates: false,
      excludedField: noExcludedField,
      expectedSlots: [10, targetCustomerIndex, targetLastCustomerIndex],
    },
  );

  for (const benchmarkCase of benchmarkCases()) {
    runBenchmarkCase(benchmarkCase);
  }
}, 0);

describe(`raw predicate candidate index benchmark: ${rowCount} rows`, () => {
  for (const benchmarkCase of benchmarkCases()) {
    bench(
      benchmarkCase.name,
      () => {
        runBenchmarkCase(benchmarkCase);
      },
      benchOptions,
    );
  }
});
