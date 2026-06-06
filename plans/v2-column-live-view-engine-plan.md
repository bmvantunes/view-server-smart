# View Server: Column Live View Engine Plan

This is the planning handoff for the production View Server implementation.

## Core Decision

The live hot path should not depend on an external analytical database. The product is a live columnar view engine, not a database wrapper.

The main internal abstraction is:

```ts
ColumnLiveViewEngine;
```

The core per-topic store is:

```ts
ColumnarTopicStore;
```

One View Server topic is one logical table. The same authoritative in-memory columnar store serves:

- initial snapshots
- live deltas
- counts
- grouped views
- subscription change streams

This keeps initial snapshots and live deltas on one authoritative state model.

## Effect v4 Setup Rules

Before implementing product code, set up Effect v4 beta and the Effect language service.

Required package baseline:

- Use the latest `effect` v4 beta.
- Current known baseline: `effect@4.0.0-beta.70`.
- Use `@effect/vitest` for all tests.
- Check for updated beta versions at the start of each new implementation day.

Local reference repositories:

- Effect source of truth: `/Users/bruno/projects/effect-smol`
- Effect language service source: `/Users/bruno/projects/language-service`
- Production Effect v4 beta reference app: `/Users/bruno/projects/t3code`

If implementation patterns conflict, `effect-smol` is the single source of truth.

Useful source files:

- `/Users/bruno/projects/effect-smol/packages/effect/src/Config.ts`
- `/Users/bruno/projects/effect-smol/packages/effect/src/ConfigProvider.ts`
- `/Users/bruno/projects/effect-smol/packages/effect/src/Clock.ts`
- `/Users/bruno/projects/effect-smol/packages/vitest`

GitHub references if the local repo is not available:

- `https://github.com/Effect-TS/effect-smol/blob/main/packages/effect/src/Config.ts`
- `https://github.com/Effect-TS/effect-smol/blob/main/packages/effect/src/ConfigProvider.ts`
- `https://github.com/Effect-TS/effect-smol/blob/main/packages/effect/src/Clock.ts`

Runtime configuration must use Effect Config:

```ts
import { Config, ConfigProvider, Effect } from "effect";
```

If required environment variables are missing, startup should fail loudly through Effect Config. Do not silently default production Kafka brokers, ports, auth secrets, or required topic settings.

Date/time rule:

- Domain timestamps should be represented as `bigint` nanoseconds or numeric fields, not JavaScript `Date` values.
- The query DSL does not need Date-specific filters, sorting, or serialization semantics; UIs can convert timestamp fields to Temporal values for display.
- Do not call `Date.now()`, `new Date()`, or direct timer APIs in product logic.
- Use Effect `Clock` so tests can control time.
- Direct performance timers are allowed only in benchmark/instrumentation code where explicitly isolated.

Testing rule:

- Use `@effect/vitest` for tests.
- Do not import directly from plain `vitest` unless there is a documented repository-level wrapper exception.

Effect LSP must be installed and configured aggressively before product work starts. Recommended TypeScript plugin config:

```json
{
  "compilerOptions": {
    "plugins": [
      {
        "name": "@effect/language-service",
        "refactors": true,
        "diagnostics": true,
        "diagnosticSeverity": {
          "floatingEffect": "warning"
        },
        "diagnosticsName": true,
        "missingDiagnosticNextLine": "warning",
        "includeSuggestionsInTsc": true,
        "ignoreEffectWarningsInTscExitCode": false,
        "ignoreEffectErrorsInTscExitCode": false,
        "ignoreEffectSuggestionsInTscExitCode": true,
        "skipDisabledOptimization": false,
        "quickinfo": true,
        "quickinfoEffectParameters": "whenTruncated",
        "quickinfoMaximumLength": -1,
        "completions": true,
        "goto": true,
        "inlays": true,
        "allowedDuplicatedPackages": [],
        "barrelImportPackages": [],
        "namespaceImportPackages": ["effect", "@effect/*"],
        "topLevelNamedReexports": "ignore",
        "importAliases": { "Array": "Arr" },
        "noExternal": false,
        "keyPatterns": [{ "target": "service", "pattern": "default", "skipLeadingPath": ["src/"] }],
        "effectFn": ["span"],
        "layerGraphFollowDepth": 0,
        "mermaidProvider": "mermaid.live"
      }
    ]
  }
}
```

LSP diagnostics must be part of the normal validation loop and must report zero errors/warnings/messages before completion.

## Public API Direction

The external API should prioritize strong type inference and low boilerplate:

```ts
// view-server.config.ts
import { Schema } from "effect";
import { defineViewServerConfig } from "@view-server/smart";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literal("open", "closed", "cancelled"),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  quantity: Schema.Number,
  price: Schema.Number,
  region: Schema.String,
});

export const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
    trades: {
      schema: Trade,
      key: "id",
    },
  },
});

export const { ViewServerProvider, useLiveQuery } = viewServer.react;
```

The browser URL must not be baked into `viewServer.react(...)`. Apps often deploy the same compiled artifact to dev/UAT/prod and inject runtime config outside the bundle.

Correct browser shape:

```tsx
import { ViewServerProvider } from "./view-server.config";

export function AppRoot() {
  return (
    <ViewServerProvider url={window.__APP_CONFIG__.VIEW_SERVER_URL}>
      <App />
    </ViewServerProvider>
  );
}
```

Test/browser-in-memory shape:

```tsx
import { createInMemoryViewServer } from "./view-server.config";

const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

client.publish("orders", {
  id: "order-1",
  customerId: "customer-1",
  status: "open",
  price: 42,
  region: "usa",
  updatedAt: 1,
});

export function TestRoot() {
  return (
    <ViewServerInMemoryProvider>
      <App />
    </ViewServerInMemoryProvider>
  );
}
```

Query usage:

```tsx
import { useLiveQuery } from "./view-server.config";

export function OrdersGrid() {
  const orders = useLiveQuery("orders", {
    where: {
      status: "open",
    },
    orderBy: [{ field: "price", direction: "desc" }],
    limit: 50,
  });

  return <Grid rows={orders.rows} />;
}
```

The hook must be fully type-safe from `defineViewServerConfig` without requiring users to define indexes.

`ViewServerProvider` and `ViewServerInMemoryProvider` must expose the same hook behavior. The only difference is the provider adapter behind the shared React client seam:

- `ViewServerProvider` connects to a real server through the configured URL.
- `ViewServerInMemoryProvider` creates the shared runtime core with a real in-memory `ColumnLiveViewEngine` inside the browser/test process.
- `useLiveQuery` and `useViewServerHealth` must depend only on the internal transport-neutral React client contract.
- The in-memory provider supplies that client through the in-process adapter.
- The real provider supplies that client through the WebSocket/Effect RPC transport adapter.
- Both providers must exercise the same runtime core, query compiler, store, snapshot/delta, health, and lifecycle implementation. Only transport/ingress adapters differ.

No mock query engine should exist. Browser tests should exercise the same query compiler, columnar store, snapshot logic, delta logic, and grouped accumulator as production.

Because there is no external snapshot backend, browser-mode Vitest can run full View Server behavior in memory. Each in-memory provider instance owns fresh state by default.

## Runtime API Direction

`createRuntime` owns deploy-time/server-only wiring. This includes ports, Kafka brokers, Kafka topic mapping, TCP publishing, gRPC publishing, memory budgets, WAL/checkpoints, and similar runtime concerns.

Runtime config must not be imported by browser bundles.

Kafka implementation decision:

- Use `@platformatic/kafka`.
- Do not use KafkaJS.
- Do not use the Confluent client.
- Revisit only if a dedicated benchmark shows `@platformatic/kafka` cannot meet throughput/lag/operational requirements.

Preferred shape:

```ts
// runtime.ts
import { Effect } from "effect";
import {
  ordersBufProtoKey,
  ordersBufProtoValue,
  tradesBufProtoValue,
} from "@buf/generated_code/orders_buf_proto";
import { viewServer } from "./view-server.config";

export const runtime = viewServer.createRuntime({
  websocketPort: 8080,
  tcpPublishPort: 8081,

  kafka: {
    regions: {
      usa: "broker-a:9092,broker-b:9092",
      london: "broker-c:9092,broker-d:9092",
    },

    topics: {
      orders: {
        regions: ["usa", "london"],

        protoValue: ordersBufProtoValue,
        protoKey: ordersBufProtoKey,

        viewServerTopic: "orders",

        mapping: ({ key, value, region, schema, metadata }) => {
          return {
            id: key.orderId,
            customerId: value.customerId,
            status: value.status,
            price: value.price,
            region,
            updatedAt: value.updatedAt,
          };
        },
      },

      trades: {
        regions: ["usa"],

        protoValue: tradesBufProtoValue,

        viewServerTopic: "trades",

        mapping: ({ key, value, region, schema, metadata }) => {
          return {
            id: key,
            symbol: value.symbol,
            quantity: value.quantity,
            price: value.price,
            region,
          };
        },
      },
    },
  },
});

Effect.runPromise(runtime);
```

## In-Memory Browser/Test API

The in-memory API should be generated from the same config:

```ts
export const { ViewServerProvider, useLiveQuery, createInMemoryViewServer } = viewServer.react;
```

Recommended test usage:

```tsx
import { Effect } from "effect";
import { createInMemoryViewServer, useLiveQuery } from "./view-server.config";

function Orders() {
  const result = useLiveQuery("orders", {
    where: { status: "open" },
    orderBy: [{ field: "price", direction: "desc" }],
    select: ["id", "price"],
    limit: 50,
  });

  return <div>{result.rows.length}</div>;
}

const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

render(
  <ViewServerInMemoryProvider>
    <Orders />
  </ViewServerInMemoryProvider>,
);

Effect.runPromise(
  client.publish("orders", {
    id: "order-2",
    customerId: "customer-2",
    status: "open",
    price: 99,
    region: "london",
    updatedAt: 2,
  }),
);
```

The in-memory client should expose typed publishing helpers:

```ts
client.publish(topic, row);
client.publishMany(topic, rows);
client.patch(topic, key, patch);
client.delete(topic, key);
client.snapshot(topic, query);
client.health();
client.reset();
```

These helpers must be fully typed from `defineViewServerConfig`.

Important boundary:

- `ViewServerInMemoryProvider` is public for tests, demos, Storybook, and local browser benchmarks.
- It must not import server-only Kafka/TCP/gRPC adapters.
- It should be backed by the same core engine package used by the server runtime.
- `useLiveQuery` must not know whether it is under `ViewServerProvider` or `ViewServerInMemoryProvider`.
- Test setup should publish through the external `client`, not through a React hook.
- Do not expose a runtime/test hook from the React API. Publishing into the in-memory server happens through the `client` returned by `createInMemoryViewServer()`.

Provider options:

```tsx
const { ViewServerInMemoryProvider, client } = createInMemoryViewServer({
  subscriptionQueueCapacity: 1,
});

<ViewServerInMemoryProvider>
  <App />
</ViewServerInMemoryProvider>;
```

Default behavior:

- `createInMemoryViewServer()` creates a fresh engine and typed client.
- Provider supplies that engine to hooks.
- Setup data goes through `client.publish` / `client.publishMany`; provider seed data is not supported.
- Disposes all subscriptions and engine state on unmount.
- Does not share state across tests unless the same in-memory instance is explicitly reused.
- Mutation helpers must not synchronously rebuild the full health snapshot on every publish.
- `client.health()` may read fresh engine health on demand, while provider health updates should use cached/coalesced refreshes so hot publish paths stay cheap.

This is also the correct path for real browser benchmarks. There is no external database process, no production-only snapshot backend, and no external database requirement, so Vitest browser mode can benchmark real `useLiveQuery` behavior end to end.

## Health And Observability

The runtime needs first-class health. Kafka/source lag, ingestion pressure, active view pressure, and transport pressure must be observable from the first production slice.

Runtime health should be available from:

```ts
const health = await runtime.health();
```

And over the network:

```txt
GET /health
GET /metrics
```

The browser provider should also expose client-side stream health:

```ts
const health = useViewServerHealth();
```

Suggested shape:

```ts
type ViewServerHealth = {
  status: "ready" | "degraded" | "starting" | "stopping";
  version: number;
  uptimeMs: number;

  engine: {
    topics: Record<
      string,
      {
        status: "ready" | "degraded" | "starting";
        topic: string;
        rowCount: number;
        liveRowCount: number;
        deletedRowCount: number;
        version: number;
        lastMutationAt: number | null;
        mutationsPerSecond: number;
        rowsPerSecond: number;
        pendingMutationBatches: number;
        activeViews: number;
        activeSubscriptions: number;
        queuedEvents: number;
        maxQueueDepth: number;
        memoryBytes: number;
        tombstoneCount: number;
        compactionPending: boolean;
      }
    >;
  };

  kafka?: {
    regions: Record<
      string,
      {
        status: "connected" | "disconnected" | "degraded" | "starting";
        brokers: string;
        lastConnectedAt: number | null;
        lastError: string | null;
      }
    >;

    topics: Record<
      string,
      {
        status: "ready" | "degraded" | "starting" | "stalled";
        sourceTopic: string;
        viewServerTopic: string;
        regions: Record<
          string,
          {
            connected: boolean;
            assignedPartitions: number;
            messagesPerSecond: number;
            bytesPerSecond: number;
            decodedMessagesPerSecond: number;
            decodeFailuresPerSecond: number;
            lastMessageAt: number | null;
            lastCommitAt: number | null;
            consumerLagMessages: number | null;
            consumerLagMs: number | null;
            lagSampledAt: number | null;
            highWatermarkOffset: string | null;
            committedOffset: string | null;
            lastError: string | null;
          }
        >;
      }
    >;
  };

  transport: {
    activeClients: number;
    activeStreams: number;
    activeSubscriptions: number;
    messagesPerSecond: number;
    bytesPerSecond: number;
    queuedMessages: number;
    queuedBytes: number;
    droppedClients: number;
    backpressureEvents: number;
    reconnects: number;
    lastError: string | null;
  };
};
```

`engine.topics[topic].rowCount` is required. This is the current internal View Server topic row count, not the Kafka source topic count. It should be visible in `/health` so operators can quickly answer:

- did this topic seed at all?
- is this topic still growing?
- did an ingest/mapping bug drop all rows?
- are deletes/tombstones accumulating?
- is Kafka lag high while in-memory row count is stale?

The count fields should mean:

- `rowCount`: total occupied rows including live rows and tombstones.
- `liveRowCount`: rows currently visible to queries.
- `deletedRowCount`: tombstoned rows waiting for compaction.

Kafka health should always include `viewServerTopic`, so source-topic lag can be correlated with the internal topic row counts.

Kafka lag policy:

- Include Kafka consumer lag when it is cheap/native from the Kafka client or already available from consumed high-watermark metadata.
- Sample lag on the same cached health cadence, around once per second.
- Do not add per-message or per-batch broker round trips just to compute lag.
- If precise lag is expensive, expose the best cheap approximation and mark the sample time with `lagSampledAt`.
- If lag cannot be obtained cheaply, return `consumerLagMessages: null` / `consumerLagMs: null` rather than harming ingest throughput.
- Lag should never be computed in the row ingestion hot loop.

Health snapshot cadence:

- `/health` should return a cached snapshot refreshed at most once per second by default.
- Hot paths may increment cheap counters, but they must not rebuild the full health object per message.
- A topic receiving 1M messages/sec should still update the exported health JSON around 1 time/sec, not 1M times/sec.
- `/metrics` can expose monotonic counters/histograms suitable for Prometheus-style scraping.
- Tests should assert health eventually reflects row counts/lag, not that it updates synchronously after every mutation.

Health endpoint shape:

- `GET /health` returns one full cached runtime snapshot.
- There is no `GET /health/stream` endpoint.
- If a client needs streaming health, it should subscribe through the normal WebSocket/live-query path.

`/health` should be the default operational contract:

```ts
{
  status: "ready",
  engine: {
    topics: {
      orders: { liveRowCount: 10_000_000, version: 1234 },
      trades: { liveRowCount: 2_000_000, version: 456 },
    },
  },
  kafka: {},
  transport: {},
}
```

Streaming health should use the same WebSocket transport as everything else, but the public API should be a dedicated hook instead of a raw system-topic query:

```ts
const health = useViewServerHealth();
```

`useViewServerHealth()` should return the whole runtime health object, including all topics. The common operator question is overall runtime health, not a single topic's health. Topic filtering can be derived by the UI from the returned object if needed.

Kubernetes/readiness/liveness should use the full cached `/health` document. Dashboards that need live health should use WebSockets through `useViewServerHealth()`. Do not add HTTP streaming for health.

Health degradation rules:

- Kafka region disconnected -> runtime degraded.
- Kafka topic lag above configured threshold -> topic degraded.
- No Kafka messages for a configured heartbeat/staleness window -> topic stalled/degraded.
- Decode failures above threshold -> topic degraded.
- Engine pending mutation batches above threshold -> topic degraded.
- Transport queue depth/bytes above threshold -> runtime degraded.
- Memory budget exceeded -> runtime degraded or admission closed.
- Failed topic should not degrade unrelated topics unless shared runtime capacity is exhausted.

This health model should also drive tests and benchmarks:

- firehose benchmarks must record messages/sec, rows/sec, lag, queue depth, and active subscriptions.
- cleanup tests must assert subscribers, active views, queues, Kafka pending work, and lag return to zero or stable values.
- browser in-memory provider should expose the same health shape where Kafka fields are absent.
- production health should be machine-readable and stable enough for Kubernetes readiness/liveness probes.

The design should avoid adding observability only after performance bugs appear. Health is part of the runtime contract.

## Tracing And Spans

Use Effect's traced function helper for named runtime boundaries:

```ts
const publishKafkaBatch = Effect.fn("runtime.kafka.publishBatch")(function* (
  batch: KafkaDecodedBatch,
) {
  // ingest, map, validate, append, fan out
});
```

In Effect v4, the traced helper is `Effect.fn("span.name")(...)`. It creates an Effect-returning function with a named span and stack frames. Use `Effect.fnUntraced(...)` only for deliberately untraced hot helpers.

Preferred rule:

- Public/runtime operations should be `Effect.fn("runtime.operation.name")`.
- Kafka ingest stages should be `Effect.fn("kafka.region.topic.stage")`.
- Engine boundaries should be `Effect.fn("engine.topic.operation")`.
- Transport boundaries should be `Effect.fn("transport.websocket.operation")`.
- Tests should assert important spans exist for ingest -> engine append -> active view update -> fanout.

Example span boundaries:

```ts
const decodeKafkaBatch = Effect.fn("kafka.decodeBatch")(function* (batch) {});
const mapKafkaBatch = Effect.fn("kafka.mapBatch")(function* (batch) {});
const appendRows = Effect.fn("engine.appendRows")(function* (topic, rows) {});
const executeSnapshot = Effect.fn("engine.executeSnapshot")(function* (topic, query) {});
const updateActiveViews = Effect.fn("engine.updateActiveViews")(function* (topic, changes) {});
const fanoutDeltas = Effect.fn("transport.fanoutDeltas")(function* (topic, deltas) {});
```

Do not create spans inside per-row tight loops. That would destroy the performance signal and add overhead exactly where the engine is trying to be fast. For tight loops, record aggregate counters/timers around the loop:

```ts
const scanColumn = Effect.fn("engine.scanColumn")(function* (topic, column, predicate) {
  const start = performance.now();
  const result = scanColumnUnsafe(column, predicate);
  yield* Metrics.histogram("engine.scanColumn.ms").record(performance.now() - start);
  yield* Metrics.counter("engine.scanColumn.rows").increment(column.length);
  return result;
});
```

This gives us proper trace breakdowns without turning the columnar engine into a tracing benchmark.

## Runtime Naming Decisions

Use:

```ts
websocketPort;
tcpPublishPort;
viewServerTopic;
protoValue;
protoKey;
```

Avoid:

```ts
webScoketsPort;
tcpPort;
viewServertTopic;
protoSchemaValue;
protoSchemaKey;
```

`tcpPublishPort` is intentionally explicit because future control/admin/debug ports may exist.

## Kafka Type Guarantees

The runtime API should enforce these at compile time:

- `kafka.regions` values are bootstrap strings, not arrays.
- `topics[...].regions` only accepts keys from `kafka.regions`.
- `regions: ["usa", "london"]` passes.
- `regions: ["USA"]` fails.
- `viewServerTopic` only accepts topic keys from `defineViewServerConfig`.
- If `protoKey` is provided, `mapping({ key })` is inferred from that protobuf key type.
- If `protoKey` is omitted, `mapping({ key })` is a string.
- `mapping({ value })` is inferred from `protoValue`.
- `mapping({ region })` is narrowed to the configured region literals for that Kafka topic.
- `mapping` return value must match the target `viewServerTopic` row schema.
- `schema` in `mapping` should be the Effect Schema from the target View Server topic.
- `metadata` should leave room for Kafka headers, partition, offset, timestamp, source topic, and source region.

The mapping function should receive one object argument, not positional arguments. Positional arguments will age badly once metadata, tracing, validation, or decode context is added.

## No User-Defined Indexes

Users should not need to define indexes.

The engine should optimize automatically because the product constraints are narrow:

- one topic is one table
- schemas are known
- query language is controlled
- output windows are usually small
- live deltas are versioned
- most UI queries are top-k/windowed

Optional index hints can be considered much later, but the design should not depend on DBA-style index configuration.

## Internal Engine Direction

The columnar engine should be its own package:

```txt
packages/column-live-view-engine
```

This package should have no Kafka, WebSocket, React, TCP, or server runtime dependency. It owns only:

- schema-driven columnar storage
- query compilation
- snapshots
- subscription state
- deltas
- counts
- grouped aggregation
- active view maintenance
- benchmarks for ingest/query/subscribe/update/fanout behavior

The server runtime package should compose this engine with Kafka, TCP/gRPC publishing, WebSocket transport, health, auth, and deployment concerns.

Initial modules:

```txt
ColumnLiveViewEngine
  ColumnarTopicStore
  QueryCompiler
  RawSnapshotExecutor
  CountExecutor
  GroupedAccumulator
  ActiveWindowIndex
  ChangeStream
```

`ColumnarTopicStore` should be schema-driven:

- numbers: `Float64Array`, `Int32Array`, or narrower typed arrays where safe
- booleans: `Uint8Array`
- strings/enums: dictionary encoding to integer arrays
- null/missing: validity bitsets
- row lookup: `rowKey -> rowIndex`
- reverse lookup: `rowIndex -> rowKey`
- deletes: tombstone bitset plus later compaction
- versions: monotonic topic version per mutation batch

The first implementation can be TypeScript/Node typed arrays. Prior prototypes showed typed arrays are credible for simplified raw filter/sort/top-k cases without indexes. Rust/native can remain a later acceleration path for projection/count/group-heavy paths if numbers require it.

## Engine Subscribe Contract

The engine should expose a direct subscription API independent of WebSockets:

```ts
const subscription = engine.subscribe("orders", {
  where: { status: "open" },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 50,
});

for await (const event of subscription.events) {
  // first event is snapshot
  // following events are deltas/status
}

await subscription.close();
```

The first event must be a snapshot:

```ts
type SnapshotEvent<Row> = {
  type: "snapshot";
  topic: string;
  queryId: string;
  version: number;
  rows: ReadonlyArray<Row>;
  totalRows?: number;
};
```

After the snapshot, the engine should emit only changes:

```ts
type DeltaEvent<Row> = {
  type: "delta";
  topic: string;
  queryId: string;
  fromVersion: number;
  toVersion: number;
  operations: ReadonlyArray<
    | { type: "insert"; key: string; row: Row; index: number }
    | { type: "update"; key: string; row: Row; index: number }
    | { type: "move"; key: string; fromIndex: number; toIndex: number }
    | { type: "remove"; key: string }
  >;
  totalRows?: number;
};
```

`fromVersion` / `toVersion` must be contiguous per subscription. A client applying snapshot then all deltas must converge exactly to a fresh snapshot at `toVersion`.

Subscription rules:

- `subscribe` creates the active view if it does not exist.
- Same query shape/window should share materialized active state where possible.
- First event is always a complete snapshot for that query.
- Later events are deltas only.
- Updates that do not affect the result window should not emit row payloads.
- Updates that affect filter membership should emit insert/remove.
- Updates that affect sort position should emit move/update as needed.
- Deletes should emit remove only if the row was visible.
- Inserts should emit insert only if the row enters the visible result/window.
- Grouped subscriptions follow the same snapshot-then-delta model, but keys are group keys instead of row keys.
- `close()` must be idempotent and release active view/subscription references.
- Backpressure should be bounded. If a subscriber falls too far behind, emit a typed status/error and close or mark stale according to policy.

The engine-level subscription API is the contract that WebSocket, in-memory React provider, browser tests, and benchmarks should all use.

## Transport And Effect RPC Policy

The engine package must not depend on Effect RPC. It should expose typed engine APIs and async subscription streams. Network transport is an edge adapter.

Recommended policy:

- Use Effect RPC for the control plane.
- Start with Effect RPC WebSocket for live subscriptions.
- Keep the live data plane replaceable behind a transport adapter.
- If benchmarks show Effect RPC adds material overhead to live deltas, keep Effect RPC for control and switch live events to a custom WebSocket frame protocol.

Effect RPC is a good fit for:

- auth/session setup
- health/admin operations
- one-off snapshots
- runtime control commands
- typed request/response errors
- schema-safe wire boundaries

Live subscription events are the only questionable path because they can become the firehose.

Transport adapter shape:

```ts
type LiveTransportAdapter = {
  subscribe<Row>(
    topic: string,
    query: Query,
  ): Effect.Effect<{
    events: AsyncIterable<SnapshotEvent<Row> | DeltaEvent<Row> | StatusEvent>;
    close: () => Effect.Effect<void>;
  }>;
};
```

BigDecimal/Decimal safety must be part of the View Server wire codec contract, not an accidental benefit of Effect RPC. If custom WebSocket frames are added later, decimals should be encoded explicitly:

```ts
{ _tag: "Decimal", value: "123456789.1234" }
```

Acceptance rule:

- Effect RPC WebSocket is acceptable for the first implementation.
- Benchmark it early against a custom raw WebSocket transport using the same engine payloads.
- If Effect RPC is within roughly 30-40% for realistic payloads, keep it for simplicity and typed errors.
- If Effect RPC is multiple times slower on live events, replace only the live event data plane and keep Effect RPC for control plane operations.
- Do not let transport choices leak into `ColumnLiveViewEngine`.

## Engine Testing Policy

Correctness tests should be end-to-end through the public engine API, not fragile unit tests against internal column arrays.

All packages should enforce 100% coverage for statements, branches, functions, and lines from the first implementation. The target is strict because this is the product core and should not accumulate coverage debt.

Coverage must be earned through meaningful public-contract tests wherever possible. Do not satisfy coverage by locking tests to private typed-array layout details that will break future SIMD/Rust/native refactors.

Preferred correctness test shape:

1. Create an engine from Effect schemas.
2. Insert/seed rows.
3. Subscribe with raw filter/sort/window query.
4. Assert the first event is the expected snapshot.
5. Publish inserts/updates/deletes.
6. Assert delta events.
7. Apply deltas to the client-side snapshot.
8. Compare the result against a fresh engine snapshot.
9. Repeat for grouped/count/aggregation queries.
10. Close subscriptions and assert health/ref counts/queues return to zero.

Example scenarios:

- raw query, no filter, sorted top 50
- raw query with equality filter
- raw query with range filter
- raw query with compound filter
- filter enter/leave after update
- sort movement after update
- hidden-field-only update emits nothing visible
- delete visible row
- delete invisible row
- delete and reinsert same id
- null/missing/value transitions
- duplicate sort values with stable row-key tiebreak
- grouped count
- grouped sum/min/max/avg
- grouped row moving from one group to another
- grouped aggregate becoming empty after delete
- subscription close during active mutation batch
- slow subscriber/backpressure
- many subscribers sharing same query
- many subscribers with different windows over same query shape

Avoid low-value tests like "internal Float64Array column stores this value at index 3". Those tests will fight future refactors to SIMD, Rust, native addons, or a different column layout. The stable contract is: input rows + query + mutations -> snapshots/deltas/convergence.

## Engine Benchmark Policy

Performance is paramount. Benchmarks should exist from the first spike.

Use Vitest bench where possible for:

- seed/append throughput
- snapshot filter/sort/top-k latency
- grouped aggregation latency
- subscription setup latency
- mutation batch latency
- delta construction latency
- many-subscriber fanout latency
- memory/RSS/heap pressure
- cleanup/ref-count behavior under churn

Benchmarks should prefer end-to-end engine operations:

```txt
seed rows -> subscribe -> receive snapshot -> publish mutation batch -> receive deltas -> close
```

Isolated microbenches are allowed for hot loops:

- scan predicate
- top-k heap
- grouped accumulator
- dictionary encode/decode
- delta diff
- compaction

But microbenches do not replace end-to-end benches. We need both the local hot-loop number and the full system number.

Vitest benchmarks should not replace correctness tests. Benchmarks are for performance regression detection; correctness tests are for semantic guarantees. If Vitest bench does not contribute normal coverage, that is fine. Coverage is less important here than strong end-to-end correctness plus benchmark artifacts.

## Benchmark Truth So Far

Simplified no-index Node typed-array benchmark:

- 10M equality filter: about 9ms
- 10M range filter matching 5M rows: about 59ms
- 10M compound filter: about 10ms
- 10M top-k sort: about 17-24ms
- 5M filtered count: about 70ms
- 5M group-by with 50 numeric columns: about 1.6s

Simplified Rust columnar benchmark:

- 10M equality filter: about 10ms
- 10M range filter matching 5M rows: about 52ms
- 10M compound filter: about 8ms
- 10M top-k sort: about 27-36ms
- 5M filtered count: about 4ms
- 5M group-by with 50 numeric columns: about 759ms

Takeaway:

- TypeScript typed arrays are credible for raw live views.
- Rust/native is probably useful later for count/group/projection-heavy workloads.
- The live engine should start with our own authoritative columnar store, not Mongo/Perspective/Polars as the primary runtime engine.

## Acceptance Criteria For The First Production Slice

The first production slice should prove:

- Same public type inference as the API examples above.
- No codegen required for `useLiveQuery`.
- Runtime URL comes from `ViewServerProvider`, not config.
- Runtime Kafka mappings are fully type-safe.
- No user-defined indexes.
- 10M-row raw filter/sort/top-k snapshot path is competitive with the typed-array benchmark.
- Initial snapshot and live delta use the same store.
- Deletes, updates, and inserts converge against a fresh snapshot.
- Query semantics are explicitly documented and tested before production use.

## Production E2E Build Contract

This section is the "agent can go build it" contract. The implementation should aim for a production-ready vertical slice, not a toy spike.

### Package Layout

Recommended packages:

```txt
packages/config
packages/column-live-view-engine
packages/runtime-core
packages/client
packages/protocol
packages/in-memory
packages/server
packages/runtime
packages/react
apps/examples
```

Responsibilities:

- `packages/config`: `defineViewServerConfig`, query DSL types, schema/topic typing, shared public types.
- `packages/column-live-view-engine`: in-memory columnar store, snapshot, subscribe, deltas, grouped aggregates, health core.
- `packages/runtime-core`: shared engine-backed runtime Module; owns the `ColumnLiveViewEngine`, runtime client, live client, pushed health streams, and lifecycle.
- `packages/client`: transport-neutral live client contracts, query state, and remote client entrypoints.
- `packages/protocol`: Effect RPC WebSocket wire schema and encode/decode boundary.
- `packages/in-memory`: in-process adapter over `runtime-core`; no alternate query engine, fake hook, fake health model, or duplicate runtime logic.
- `packages/server`: Effect RPC WebSocket server and same-server HTTP health endpoint.
- `packages/runtime`: production composition of `runtime-core`, server, health URL, lifecycle, and future Kafka/TCP/gRPC ingress adapters.
- `packages/react`: production React provider/hooks plus a separate testing entrypoint for the in-memory provider.
- `apps/examples`: minimal real app proving browser usage and runtime URL injection.

All packages must have explicit public exports. Do not leak internals accidentally.

### First Production Milestone

Build this full vertical slice first:

1. Define two topics with Effect Schema.
2. Create a runtime with one Kafka region and one Kafka topic mapping.
3. Ingest protobuf Kafka messages through `@platformatic/kafka`.
4. Map Kafka messages into internal View Server rows.
5. Store rows in `ColumnLiveViewEngine`.
6. Serve `useLiveQuery` over Effect RPC WebSocket.
7. Serve `useViewServerHealth`.
8. Serve `GET /health` and `GET /metrics`.
9. Support `ViewServerInMemoryProvider` in browser tests.
10. Prove raw filter/sort/window query snapshot and deltas.
11. Prove grouped count/sum/min/max/avg snapshot and deltas.
12. Prove cleanup on component unmount, provider unmount, WebSocket disconnect, runtime shutdown.

Do not defer the in-memory provider. It is a core production testing feature.

### Runtime Lifecycle

Runtime must support:

- `Effect.runPromise(runtime)` for server start.
- Graceful shutdown on interruption.
- Stop Kafka consumers before destroying engine state.
- Close WebSocket sessions and subscriptions.
- Flush final metrics/health state if possible.
- Release all active subscriptions and engine references.
- Bounded shutdown timeout.
- Idempotent shutdown.

Shutdown tests must assert:

- active clients = 0
- active subscriptions = 0
- pending mutation batches = 0
- queued events = 0
- Kafka consumers stopped
- health reports `stopping` then runtime exits

### Persistence And Recovery Policy

The runtime is in-memory first, but production needs an explicit recovery story.

Initial policy:

- Kafka is the source of truth.
- On startup, runtime rebuilds in-memory state by consuming configured topics from the configured start position.
- The default production start policy should be explicit, not magic:

```ts
startFrom: "earliest" | "latest" | { committedConsumerGroup: string };
```

- If `startFrom: "latest"`, health should clearly show topics are not backfilled.
- WAL/checkpoints are allowed later, but should not be required for the first production milestone.
- The engine must expose hooks that would allow future checkpoints without changing `useLiveQuery`.

Recovery tests:

- Start runtime, ingest rows, stop runtime, restart from earliest, verify snapshot converges.
- Start from latest, verify old rows are intentionally absent and health reports the policy.
- Restart during active subscriptions, clients reconnect and receive fresh snapshots.

### Query Semantics

The product must define and test query semantics explicitly.

Required semantics:

- stable row-key tiebreak for sorting
- null/missing ordering documented and tested
- string ordering documented and tested
- BigDecimal/Decimal wire representation documented and tested
- unsupported values rejected before engine insert
- filters behave identically for snapshot and live deltas
- grouped aggregates handle null/missing consistently
- snapshot result after applying deltas equals fresh snapshot

### Backpressure And Admission

Production runtime must have bounded queues.

Required policies:

- per-subscription queued event limit
- per-client queued byte/message limit
- runtime-wide queued byte/message limit
- typed backpressure status/error
- slow subscriber cleanup
- health degradation when backpressure occurs
- benchmark artifact recording backpressure count

Default behavior should prefer correctness over infinite buffering:

- if a subscriber falls too far behind, mark stale or close with typed `BackpressureExceeded`
- reconnect/resubscribe should receive a fresh snapshot

### Security And Auth

Do not build a complex auth system in the first milestone, but leave the seam.

Runtime should support:

```ts
auth?: {
  validateRequest?: (request) => Effect.Effect<AuthContext, AuthError>;
}
```

Auth context should be available to:

- WebSocket/session setup
- TCP publish if exposed
- health/admin endpoints if configured
- future per-topic access checks

Default local/dev mode can allow anonymous access, but production docs must make that explicit.

### TCP Publish API

`tcpPublishPort` exists for non-browser publishers.

The first implementation should support a simple typed publish path:

- publish row
- patch row
- delete row
- publish batch

It must use the same engine mutation path as Kafka. No second mutation implementation.

TCP publish tests should cover:

- publish -> live query receives delta
- patch -> filter enter/leave
- delete -> remove
- batch -> one coherent version range
- invalid payload -> typed failure, no partial mutation unless explicitly documented

### React Provider Contract

`ViewServerProvider`:

- gets URL at runtime
- opens WebSocket lazily or on mount
- cleans up on unmount
- reconnects with fresh snapshots
- does not leak subscriptions on route changes
- exposes typed errors/status/loading state
- supports multiple concurrent `useLiveQuery` hooks
- supports multiple topics

`ViewServerInMemoryProvider`:

- is created by `createInMemoryViewServer()`
- uses the same internal React client contract as `ViewServerProvider`
- supports setup data through the external `client.publish` / `client.publishMany` API
- disposes engine on unmount
- never imports Kafka/TCP/WebSocket server code

Do not add `seed`, `onRuntime`, `runtime`, or `testing` props to the provider. Those make the provider too smart and couple app components to test setup concerns.

React tests must run in browser mode and prove real hook behavior.

### CI And Quality Gates

Required gates:

- `vp check --fix`
- Effect LSP diagnostics: 0 errors/warnings/messages
- `vp run -r test`
- `vp run -r build`
- 100% coverage across all packages
- policy scan: no `console.*` in source
- policy scan: no `node:assert` / `node:test`
- policy scan: no direct Vitest imports in tests if the repo uses a test wrapper
- policy scan: no `as any`, `as unknown`, `as never` in source/tests except generated code with documented exclusion
- package/export smoke test from packed tarballs
- browser test for `ViewServerInMemoryProvider`
- runtime WebSocket smoke
- Kafka ingest smoke
- health/readiness smoke

Benchmarks should be reportable artifacts, not hidden local notes.

### Production Benchmarks

Minimum benchmark profiles:

- 100k rows raw snapshot filter/sort/top-k
- 1M rows raw snapshot filter/sort/top-k
- 10M rows raw snapshot filter/sort/top-k
- 100k rows grouped count/sum/min/max/avg
- 1M rows grouped count/sum/min/max/avg
- 5M rows grouped count/sum/min/max/avg
- 50 clients x 30 subscriptions
- 50 clients x 150 subscriptions product-shaped
- 50 clients x 150 subscriptions hot-topic torture
- Kafka sustained ingest at 10k msg/sec
- Kafka burst ingest above 10k msg/sec
- browser in-memory `useLiveQuery` benchmark
- reconnect/route-change churn
- slow-client/backpressure

Current engine raw benchmark harness:

```bash
vp run --no-cache column-live-view-engine#bench:raw-snapshot
```

Use `--no-cache` because benchmark row counts and timing are environment-driven. This harness is
currently a timing-focused raw engine benchmark; the full production benchmark artifact fields below
still need to be added to the broader benchmark suite. It defaults to a 100k-row smoke profile and
writes `packages/column-live-view-engine/.artifacts/raw-snapshot.json`. Run each row count in a
separate process so previous profiles do not contaminate GC/RSS/latency. For example:

```bash
VIEW_SERVER_ENGINE_BENCH_ROWS=100000 vp run --no-cache column-live-view-engine#bench:raw-snapshot
VIEW_SERVER_ENGINE_BENCH_ROWS=1000000 vp run --no-cache column-live-view-engine#bench:raw-snapshot
VIEW_SERVER_ENGINE_BENCH_ROWS=10000000 vp run --no-cache column-live-view-engine#bench:raw-snapshot
```

Raw snapshot knobs:

- `VIEW_SERVER_ENGINE_BENCH_ROWS`: row count for this benchmark process.
- `VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE`: publish batch size while seeding.
- `VIEW_SERVER_ENGINE_BENCH_ITERATIONS`: benchmark iterations per case.
- `VIEW_SERVER_ENGINE_BENCH_TIME_MS`: benchmark time budget per case.
- `VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS`: warmup iterations per case.
- `VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS`: warmup time budget per case.

Current raw predicate candidate index benchmark harness:

```bash
vp run --no-cache column-live-view-engine#bench:raw-predicate-index
```

This harness uses Vitest `bench()`, scanner-level callback counters, and returned-key assertions to
prove exact predicate candidate indexes reduce steady-state row callback work without breaking window
ordering. It pre-warms the ordered range index before timed samples; it is not a cold index-build
benchmark. It compares:

- full-scan callback baseline for selective customer filters
- exact scalar candidate for selective `eq`
- exact scalar candidate with no row callback when the predicate is callback-skippable
- exact multi-key `in` candidate using authoritative value keys
- exact range candidate over an existing ordered index
- ordered equality seek over storage order
- failed broad scalar candidate build plus full scan

It writes `packages/column-live-view-engine/.artifacts/raw-predicate-index.json`. Run each row count
in a separate process. The benchmark rejects row counts below 101 because several cases assert exact
50/100-row windows and the range-candidate case must stay narrower than the whole table.

```bash
VIEW_SERVER_ENGINE_BENCH_ROWS=100000 vp run --no-cache column-live-view-engine#bench:raw-predicate-index
VIEW_SERVER_ENGINE_BENCH_ROWS=1000000 vp run --no-cache column-live-view-engine#bench:raw-predicate-index
VIEW_SERVER_ENGINE_BENCH_ROWS=10000000 vp run --no-cache column-live-view-engine#bench:raw-predicate-index
```

Raw predicate index knobs:

- `VIEW_SERVER_ENGINE_BENCH_ROWS`: row count for this benchmark process.
- `VIEW_SERVER_ENGINE_BENCH_ITERATIONS`: benchmark iterations per case.
- `VIEW_SERVER_ENGINE_BENCH_TIME_MS`: benchmark time budget per case.
- `VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS`: warmup iterations per case.
- `VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS`: warmup time budget per case.

For decision-quality large-row runs, keep `--no-cache` and use multiple iterations. One-sample runs
are useful only to verify feasibility and approximate scale.

Each production/runtime benchmark artifact must include:

- row count
- mutation count
- subscribers
- topics
- p50/p95/p99/max latency
- memory/RSS/heap
- queued events/bytes
- backpressure count
- cleanup leak count
- health snapshot after cleanup

### Documentation

Production docs required before calling the slice done:

- public API guide
- runtime config guide
- Kafka mapping guide
- in-memory browser testing guide
- health/metrics guide
- query semantics guide
- benchmark/capacity guide
- deployment guide
- migration notes when needed

### Definition Of Done

The production e2e implementation is not done until:

- example app works with real runtime
- example app works with `ViewServerInMemoryProvider`
- Kafka ingest works with protobuf value and optional protobuf key
- `useLiveQuery` works for raw and grouped queries
- `useViewServerHealth` works
- `/health` and `/metrics` work
- all cleanup paths are leak-free
- all packages have 100% coverage
- all validation gates pass
- benchmark artifacts exist and are documented
- transport is replaceable without changing `useLiveQuery`
- no external analytical database dependency exists in the live hot path
