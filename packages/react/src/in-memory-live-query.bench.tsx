// Benchmarks intentionally import Vitest directly: @effect/vitest does not expose `bench`.
import { afterAll, beforeAll, bench, describe, expect } from "vitest";
import { commands, server } from "@vitest/browser/context";
import {
  defineViewServerConfig,
  type ViewServerHealth,
  type ViewServerRuntimeClient,
  type ViewServerRuntimeError,
} from "@view-server/config";
import { createInMemoryViewServer, type ViewServerInMemoryInstance } from "@view-server/in-memory";
import { Effect, Schema } from "effect";
import { render } from "vitest-browser-react";
import { createViewServerReact } from "./index";
import { ViewServerReactClientProvider } from "./internal";

declare global {
  interface ImportMeta {
    readonly env: ImportMetaEnv;
  }

  interface ImportMetaEnv {
    readonly VITE_VIEW_SERVER_REACT_BENCH_BATCH_SIZE?: string;
    readonly VITE_VIEW_SERVER_REACT_BENCH_ITERATIONS?: string;
    readonly VITE_VIEW_SERVER_REACT_BENCH_OUTPUT_JSON?: string;
    readonly VITE_VIEW_SERVER_REACT_BENCH_ROWS?: string;
    readonly VITE_VIEW_SERVER_REACT_BENCH_TIME_MS?: string;
    readonly VITE_VIEW_SERVER_REACT_BENCH_WARMUP_ITERATIONS?: string;
    readonly VITE_VIEW_SERVER_REACT_BENCH_WARMUP_TIME_MS?: string;
  }
}

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Finite,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
  },
});

const react = createViewServerReact(viewServer);
const { useLiveQuery } = react;
const ViewServerClientProvider = react[ViewServerReactClientProvider];

type Topics = typeof viewServer.topics;
type OrderRow = typeof Order.Type;
type RenderedView = Awaited<ReturnType<typeof render>>;
type Runtime = ViewServerInMemoryInstance<Topics>;
type Health = ViewServerHealth<Topics>;

type BenchmarkProfile = {
  readonly rowCount: number;
  healthAfterSetup: Health | undefined;
  nextDeltaIndex: number;
  renderedMutationCount: number;
  runtime: Runtime | undefined;
  view: RenderedView | undefined;
};

type ReactBrowserBenchmarkArtifact = {
  readonly artifactKind: "react-browser-benchmark-summary";
  readonly benchmarkCases: ReadonlyArray<string>;
  readonly benchmarkName: string;
  readonly benchmarkScope: "react-in-memory-live-query";
  readonly browser: {
    readonly browser: string;
    readonly provider: string;
  };
  readonly cleanupLeakCount: number;
  readonly healthAfterCleanup: Health;
  readonly healthAfterSetup: Health;
  readonly latency: {
    readonly outputJsonPath: string;
    readonly source: "vitest-output-json";
  };
  readonly backpressureCount: number;
  readonly memory: {
    readonly reason: string;
    readonly source: "browser-unavailable";
  };
  readonly mutationCount: number;
  readonly notes: ReadonlyArray<string>;
  readonly outputJsonPath: string;
  readonly queuedEventCount: number;
  readonly rowCount: number;
  readonly seedBatchSize: number;
  readonly subscriberCount: number;
  readonly topics: ReadonlyArray<string>;
};

const defaultRowCount = 10_000;
const defaultSeedBatchSize = 1_000;
const defaultIterations = 5;
const defaultBenchmarkTimeMs = 250;
const defaultWarmupIterations = 0;
const defaultWarmupTimeMs = 0;

const positiveIntegerFromEnv = (
  raw: string | undefined,
  name: string,
  fallback: number,
): number => {
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

const nonNegativeIntegerFromEnv = (
  raw: string | undefined,
  name: string,
  fallback: number,
): number => {
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

const benchmarkOutputJsonPath = (): string => {
  const configured = import.meta.env.VITE_VIEW_SERVER_REACT_BENCH_OUTPUT_JSON;
  if (configured !== undefined && configured.trim() !== "") {
    return configured.trim();
  }
  return `.artifacts/in-memory-live-query-${rowCount}rows-${server.browser}.json`;
};

const benchmarkSummaryPath = (outputJsonPath: string): string => {
  if (outputJsonPath.endsWith(".json")) {
    return `${outputJsonPath.slice(0, -".json".length)}.summary.json`;
  }
  return `${outputJsonPath}.summary.json`;
};

const rowCount = positiveIntegerFromEnv(
  import.meta.env.VITE_VIEW_SERVER_REACT_BENCH_ROWS,
  "VITE_VIEW_SERVER_REACT_BENCH_ROWS",
  defaultRowCount,
);
const seedBatchSize = positiveIntegerFromEnv(
  import.meta.env.VITE_VIEW_SERVER_REACT_BENCH_BATCH_SIZE,
  "VITE_VIEW_SERVER_REACT_BENCH_BATCH_SIZE",
  defaultSeedBatchSize,
);
const outputJsonPath = benchmarkOutputJsonPath();
const benchOptions = {
  iterations: positiveIntegerFromEnv(
    import.meta.env.VITE_VIEW_SERVER_REACT_BENCH_ITERATIONS,
    "VITE_VIEW_SERVER_REACT_BENCH_ITERATIONS",
    defaultIterations,
  ),
  time: positiveIntegerFromEnv(
    import.meta.env.VITE_VIEW_SERVER_REACT_BENCH_TIME_MS,
    "VITE_VIEW_SERVER_REACT_BENCH_TIME_MS",
    defaultBenchmarkTimeMs,
  ),
  warmupIterations: nonNegativeIntegerFromEnv(
    import.meta.env.VITE_VIEW_SERVER_REACT_BENCH_WARMUP_ITERATIONS,
    "VITE_VIEW_SERVER_REACT_BENCH_WARMUP_ITERATIONS",
    defaultWarmupIterations,
  ),
  warmupTime: nonNegativeIntegerFromEnv(
    import.meta.env.VITE_VIEW_SERVER_REACT_BENCH_WARMUP_TIME_MS,
    "VITE_VIEW_SERVER_REACT_BENCH_WARMUP_TIME_MS",
    defaultWarmupTimeMs,
  ),
};

const profile: BenchmarkProfile = {
  healthAfterSetup: undefined,
  nextDeltaIndex: rowCount,
  renderedMutationCount: 0,
  rowCount,
  runtime: undefined,
  view: undefined,
};

const seedOrder = (index: number): OrderRow => ({
  id: `order-${index}`,
  customerId: `customer-${index % 10_000}`,
  status: "open",
  price: index % 1_000_000,
  region: index % 2 === 0 ? "usa" : "london",
  updatedAt: index,
});

const deltaOrder = (index: number): OrderRow => ({
  id: `delta-${index}`,
  customerId: `customer-delta-${index % 10_000}`,
  status: "open",
  price: 1_000_000 + (index % 10_000),
  region: "usa",
  updatedAt: 1_000_000_000 + index,
});

const publishSeedRows: (
  client: ViewServerRuntimeClient<Topics>,
  count: number,
) => Effect.Effect<void, ViewServerRuntimeError> = Effect.fn(
  "ViewServerReact.bench.inMemoryLiveQuery.seed",
)(function* (client: ViewServerRuntimeClient<Topics>, count: number) {
  let next = 0;
  while (next < count) {
    const batchCount = Math.min(seedBatchSize, count - next);
    const rows = Array.from({ length: batchCount }, (_value, offset) => seedOrder(next + offset));
    yield* client.publishMany("orders", rows);
    next += batchCount;
  }
});

const profileRuntime = (profile: BenchmarkProfile): Runtime => {
  if (profile.runtime === undefined) {
    throw new Error(`React in-memory benchmark ${profile.rowCount} rows is not initialized.`);
  }
  return profile.runtime;
};

const profileView = (profile: BenchmarkProfile): RenderedView => {
  if (profile.view === undefined) {
    throw new Error(`React in-memory benchmark ${profile.rowCount} rows has no rendered view.`);
  }
  return profile.view;
};

const cleanupLeakCountFromHealth = (health: Health): number => {
  const orders = health.engine.topics.orders;
  return orders.activeSubscriptions + orders.activeViews + orders.queuedEvents;
};

const queuedEventCountFromHealth = (health: Health): number => {
  const orders = health.engine.topics.orders;
  return orders.queuedEvents;
};

const backpressureCountFromHealth = (health: Health): number => {
  const orders = health.engine.topics.orders;
  return orders.backpressureEvents;
};

const waitForCleanupHealth: (
  client: ViewServerRuntimeClient<Topics>,
) => Effect.Effect<Health, ViewServerRuntimeError> = Effect.fn(
  "ViewServerReact.bench.inMemoryLiveQuery.cleanupHealth",
)(function* (client: ViewServerRuntimeClient<Topics>) {
  let attempts = 0;
  let health = yield* client.health();
  while (cleanupLeakCountFromHealth(health) > 0 && attempts < 50) {
    attempts += 1;
    yield* Effect.sleep("10 millis");
    health = yield* client.health();
  }
  return health;
});

const writeBenchmarkArtifact = (input: ReactBrowserBenchmarkArtifact): Promise<void> =>
  commands.writeFile(
    benchmarkSummaryPath(input.outputJsonPath),
    `${JSON.stringify(
      {
        artifactKind: input.artifactKind,
        benchmarkCases: input.benchmarkCases,
        benchmarkName: input.benchmarkName,
        benchmarkScope: input.benchmarkScope,
        browser: input.browser,
        cleanupLeakCount: input.cleanupLeakCount,
        healthAfterCleanup: input.healthAfterCleanup,
        healthAfterSetup: input.healthAfterSetup,
        latency: input.latency,
        backpressureCount: input.backpressureCount,
        memory: input.memory,
        mutationCount: input.mutationCount,
        notes: input.notes,
        outputJsonPath: input.outputJsonPath,
        queuedEventCount: input.queuedEventCount,
        rowCount: input.rowCount,
        seedBatchSize: input.seedBatchSize,
        subscriberCount: input.subscriberCount,
        topics: input.topics,
      },
      undefined,
      2,
    )}\n`,
  );

function OrdersView() {
  const result = useLiveQuery("orders", {
    select: ["id", "price", "updatedAt"],
    where: {
      status: { eq: "open" },
    },
    orderBy: [{ field: "updatedAt", direction: "desc" }],
    limit: 1,
  });
  const row = result.rows[0];
  return (
    <output aria-label="orders live query" role="status">
      {row === undefined ? "top: none total: 0" : `top: ${row.id} total: ${result.totalRows}`}
    </output>
  );
}

beforeAll(async () => {
  const runtime = createInMemoryViewServer(viewServer);
  profile.runtime = runtime;
  await Effect.runPromise(publishSeedRows(runtime.client, profile.rowCount));
  const view = await render(
    <ViewServerClientProvider client={runtime.liveClient}>
      <OrdersView />
    </ViewServerClientProvider>,
  );
  const expectedInitialText = `top: order-${profile.rowCount - 1} total: ${profile.rowCount}`;
  const initialElement = await view
    .getByText(expectedInitialText, {
      exact: true,
    })
    .findElement({ timeout: 5_000 });
  expect(initialElement.textContent).toBe(expectedInitialText);
  profile.healthAfterSetup = await Effect.runPromise(runtime.client.health());
  profile.view = view;
}, 0);

afterAll(async () => {
  const runtime = profile.runtime;
  const view = profile.view;
  const healthAfterSetup = profile.healthAfterSetup;
  if (runtime === undefined) {
    return;
  }
  if (view === undefined || healthAfterSetup === undefined) {
    await Effect.runPromise(runtime.close);
    profile.runtime = undefined;
    profile.healthAfterSetup = undefined;
    return;
  }
  await view.unmount();
  const healthAfterCleanup = await Effect.runPromise(waitForCleanupHealth(runtime.client));
  const cleanupLeakCount = cleanupLeakCountFromHealth(healthAfterCleanup);
  await writeBenchmarkArtifact({
    artifactKind: "react-browser-benchmark-summary",
    benchmarkCases: [
      "publish matching row through runtime client and observe through in-memory live client",
    ],
    benchmarkName: "React in-memory useLiveQuery browser benchmark",
    benchmarkScope: "react-in-memory-live-query",
    browser: {
      browser: server.browser,
      provider: server.provider,
    },
    cleanupLeakCount,
    healthAfterCleanup,
    healthAfterSetup,
    latency: {
      outputJsonPath,
      source: "vitest-output-json",
    },
    backpressureCount: backpressureCountFromHealth(healthAfterCleanup),
    memory: {
      reason:
        "Browser engines do not expose comparable per-run RSS/heap across Chromium, Firefox, and WebKit.",
      source: "browser-unavailable",
    },
    mutationCount: profile.nextDeltaIndex,
    notes: [
      "Latency percentiles are emitted by Vitest in outputJsonPath.",
      "This browser benchmark exercises publish -> runtime-core -> engine subscription -> React hook/render.",
      "mutationCount includes setup seed rows plus measured live publishes.",
      "Memory is marked browser-unavailable because browser engines do not expose comparable per-run RSS/heap across Chromium, Firefox, and WebKit.",
    ],
    outputJsonPath,
    queuedEventCount: queuedEventCountFromHealth(healthAfterCleanup),
    rowCount: profile.rowCount,
    seedBatchSize,
    subscriberCount: 1,
    topics: ["orders"],
  });
  await Effect.runPromise(runtime.close);
  profile.healthAfterSetup = undefined;
  profile.runtime = undefined;
  profile.view = undefined;
  if (cleanupLeakCount > 0) {
    throw new Error(`React in-memory benchmark leaked ${cleanupLeakCount} active resource(s).`);
  }
}, 0);

describe(`React in-memory useLiveQuery browser benchmark: ${profile.rowCount} rows`, () => {
  bench(
    "publish matching row -> rendered top row",
    async () => {
      const row = deltaOrder(profile.nextDeltaIndex);
      const expectedTotalRows = profile.rowCount + profile.renderedMutationCount + 1;
      const expectedText = `top: ${row.id} total: ${expectedTotalRows}`;
      profile.nextDeltaIndex += 1;
      await Effect.runPromise(profileRuntime(profile).client.publish("orders", row));
      const element = await profileView(profile)
        .getByText(expectedText, {
          exact: true,
        })
        .findElement({ timeout: 5_000 });
      expect(element.textContent).toBe(expectedText);
      profile.renderedMutationCount += 1;
    },
    benchOptions,
  );
});
