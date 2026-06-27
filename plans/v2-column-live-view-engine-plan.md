# View Server: Column Live View Engine Plan

This is the planning handoff for the production View Server implementation.

## Core Decision

The live hot path should not depend on an external analytical database. The product is a live columnar view engine, not a database wrapper.

The main internal abstraction is:

```ts
ColumnLiveViewEngine;
```

The core per-topic seam is the Topic Store Module:

```ts
TopicStore;
```

The current implementation behind that seam is `TopicRowStorage`: a row-oriented authoritative store
with private column vectors and query indexes where they are already useful. A future
`ColumnarTopicStore` implementation may replace that storage behind the same `TopicStore` seam when
benchmarks prove the write/read tradeoff is better.

One View Server topic is one logical table. The same authoritative in-memory Topic Store serves:

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

No mock query engine should exist. Browser tests should exercise the same query compiler, TopicStore, TopicRowStorage, snapshot logic, delta logic, and grouped accumulator as production.

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
import { NodeRuntime } from "@effect/platform-node";
import {
  ordersBufProtoKey,
  ordersBufProtoValue,
  tradesBufProtoValue,
} from "@buf/generated_code/orders_buf_proto";
import { kafka } from "@view-server/config";
import { viewServer } from "./view-server.config";

const kafkaRegions = {
  usa: "broker-a:9092,broker-b:9092",
  london: "broker-c:9092,broker-d:9092",
};

const kafkaTopic = viewServer.kafkaTopic<typeof kafkaRegions>();

export const runtime = viewServer.createRuntime({
  websocketPort: 8080,
  tcpPublishPort: 8081,

  kafka: {
    regions: kafkaRegions,

    topics: {
      orders: kafkaTopic({
        regions: ["usa", "london"],

        value: kafka.protobuf(ordersBufProtoValue),
        key: kafka.protobuf(ordersBufProtoKey),

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
      }),

      trades: kafkaTopic({
        regions: ["usa"],

        value: kafka.protobuf(tradesBufProtoValue),

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
      }),
    },
  },
});

NodeRuntime.runMain(runtime);
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
        rowCount: number;
        liveRowCount: number;
        deletedRowCount: number;
        version: number;
        lastMutationAt: number | null;
        mutationsPerSecond: number;
        rowsPerSecond: number;
        pendingMutationBatches: number;
        activeFallbackGroupedViews: number;
        activeIncrementalGroupedViews: number;
        activeViews: number;
        groupedFullEvaluationCount: number;
        groupedPatchedEvaluationCount: number;
        activeSubscriptions: number;
        queuedEvents: number;
        maxQueueDepth: number;
        backpressureEvents: number;
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
            mappingFailuresPerSecond: number;
            publishFailuresPerSecond: number;
            commitFailuresPerSecond: number;
            processingFailuresPerSecond: number;
            lastMessageAt: number | null;
            lastCommitAt: number | null;
            consumerLagMessages: bigint | null;
            lagSampledAt: number | null;
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
- If lag cannot be obtained cheaply, return `consumerLagMessages: null` rather than harming ingest throughput.
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
value;
key;
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
- Kafka source topic definitions must be created with `viewServer.kafkaTopic<typeof kafkaRegions>()(...)`; direct unwrapped objects are rejected so nested topic contracts cannot widen.
- `topics[...].regions` only accepts keys from `kafka.regions`.
- `regions: ["usa", "london"]` passes.
- `regions: ["USA"]` fails.
- `viewServerTopic` only accepts topic keys from `defineViewServerConfig`.
- `value` must be an explicit Kafka source codec such as `kafka.protobuf(...)`, `kafka.json(...)`, `kafka.string()`, `kafka.bytes()`, or `kafka.codec(...)`.
- If `key` is provided, `mapping({ key })` is inferred from that Kafka key codec.
- If `key` is omitted, `mapping({ key })` is a UTF-8 string.
- `mapping({ value })` is inferred from the Kafka value codec.
- `mapping({ region })` is narrowed to the configured region literals for that Kafka topic.
- `mapping` return value must match the target `viewServerTopic` row schema.
- `schema` in `mapping` should be the Effect Schema from the target View Server topic.
- `metadata` should leave room for Kafka headers, partition, offset, timestamp, source topic, and source region.
- Protobuf source codecs should accept the direct generated Buf descriptor/code.
- JSON source codecs should validate decoded JSON through the provided Effect Schema.
- Custom source codecs should keep arbitrary formats behind one typed decoder seam.

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
  TopicStore
    TopicRowStorage
    future ColumnarTopicStore implementation
  QueryCompiler
  RawSnapshotExecutor
  CountExecutor
  GroupedAccumulator
  ActiveWindowIndex
  ChangeStream
```

The future `ColumnarTopicStore` implementation should be schema-driven:

- numbers: `Float64Array`, `Int32Array`, or narrower typed arrays where safe
- booleans: `Uint8Array`
- strings/enums: dictionary encoding to integer arrays
- null/missing: validity bitsets
- row lookup: `rowKey -> rowIndex`
- reverse lookup: `rowIndex -> rowKey`
- deletes: tombstone bitset plus later compaction
- versions: monotonic topic version per mutation batch

The first implementation can be TypeScript/Node typed arrays. Prior prototypes showed typed arrays are credible for simplified raw filter/sort/top-k cases without indexes. Rust/native can remain a later acceleration path for projection/count/group-heavy paths if numbers require it.

Schema specialization must measure both sides of the tradeoff. A Topic Column Vector or index can
make Raw Query reads faster while adding publish/patch/delete maintenance cost. Every storage
optimization that adds per-field state should have a matching write-path benchmark so read wins do
not hide write regressions.

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
- The live engine should start with our own authoritative TopicStore and TopicRowStorage implementation, not Mongo/Perspective/Polars as the primary runtime engine.

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
apps/example
```

Responsibilities:

- `packages/config`: `defineViewServerConfig`, query DSL types, schema/topic typing, shared public types.
- `packages/column-live-view-engine`: in-memory TopicStore/TopicRowStorage, snapshot, subscribe, deltas, grouped aggregates, health core.
- `packages/runtime-core`: shared engine-backed runtime Module; owns the `ColumnLiveViewEngine`, runtime client, live client, pushed health streams, and lifecycle.
- `packages/client`: transport-neutral live client contracts, query state, and remote client entrypoints.
- `packages/protocol`: Effect RPC WebSocket wire schema and encode/decode boundary.
- `packages/in-memory`: in-process adapter over `runtime-core`; no alternate query engine, fake hook, fake health model, or duplicate runtime logic.
- `packages/server`: Effect RPC WebSocket server and same-server HTTP health endpoint.
- `packages/runtime`: production composition of `runtime-core`, server, health URL, lifecycle, and future Kafka/TCP/gRPC ingress adapters.
- `packages/react`: production React provider/hooks plus a separate testing entrypoint for the in-memory provider.
- `apps/example`: minimal real app proving browser usage and runtime URL injection.

All publishable `packages/*` must have explicit public exports. Do not leak internals accidentally.

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

- `NodeRuntime.runMain(runtime)` for Node server entrypoints so process signals interrupt the main fiber and run Effect finalizers.
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
- Current implementation status: the runtime exposes a configured Kafka consumer group and an explicit Kafka `startFrom` policy. The default preserves the current live-process behavior: consume committed offsets for the configured group with an earliest fallback for a new group. Runtime config can also request `startFrom: "earliest"`, `startFrom: "latest"`, or a committed consumer group with `earliest`, `latest`, or `fail` fallback. The runtime microbatches consumed messages, publishes mapped rows with `publishMany` grouped by internal View Server Topic, and commits original Kafka messages only after the relevant publish succeeds. Success health records the committed offset after commit.
- Microbatch failure policy: publish failure leaves the affected Kafka messages uncommitted so they can replay. If a later message in a batch fails decode or mapping after earlier messages were already decoded, the decoded prefix is published and committed before the failing message surfaces. If the Kafka stream fails after yielding messages, the yielded batch is flushed before the stream failure marks health degraded.
- Current restart contract: because Runtime Core rows are in memory and no durable WAL/checkpoint exists yet, committed consumer-group resume can skip rows that were committed before process death. It is a live-process ingestion mode, not a lossless full rebuild strategy.
- Until WAL/checkpoints exist, production deployments that need rebuild-after-restart semantics must replay Kafka from an authoritative position, such as earliest offsets for the Source Topics or a dedicated rebuild group.
- Production startup policy is explicit, not magic:

```ts
startFrom:
  | "earliest"
  | "latest"
  | {
      committedConsumerGroup: string;
      fallback?: "earliest" | "latest" | "fail";
    };
```

- If `startFrom: "latest"`, health should clearly show topics are not backfilled.
- If `startFrom: { committedConsumerGroup }`, health and docs must clearly state that this assumes durable View Server state already exists or that skipped committed rows are acceptable.
- WAL/checkpoints are allowed later, but should not be required for the first production milestone.
- The engine must expose hooks that would allow future checkpoints without changing `useLiveQuery`.
- Current consumer-group assumption: one runtime process owns the configured Region consumers for the group. Full rebalance/revoke handoff, multiple active consumers in one group, and checkpoint handoff are roadmap items, not current guarantees.

Recovery tests:

- Start runtime, ingest rows, stop runtime, restart from earliest, verify snapshot converges.
- Start runtime in committed mode, ingest and commit rows, stop runtime, append new rows, restart with a distinct top-level `consumerGroupId` and `startFrom.committedConsumerGroup` pointing at the committed group, verify committed mode follows the committed group's offsets and only newly consumed rows are present unless durable state/checkpoints are added.
- Start from latest, verify old rows are intentionally absent, newly appended rows are consumed, and health reports the no-backfill policy.
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

`tcpPublishPort` exists for non-browser publishers. When configured, the runtime
opens a plain TCP NDJSON endpoint and returns `tcpPublishUrl`.

TCP publish has a separate `tcpPublishHost` and defaults to `127.0.0.1`. It must
not inherit the public WebSocket/HTTP host because it is a mutation ingress, not
a browser endpoint.

The TCP endpoint accepts one JSON command per line:

```json
{ "op": "publish", "topic": "orders", "row": { "id": "o1" } }
{ "op": "publishMany", "topic": "orders", "rows": [{ "id": "o1" }] }
{ "op": "patch", "topic": "orders", "key": "o1", "patch": { "status": "done" } }
{ "op": "delete", "topic": "orders", "key": "o1" }
```

Each response is also one JSON line:

```json
{ "ok": true }
{ "ok": false, "error": { "_tag": "ViewServerTcpPublishIngressError", "phase": "decode", "message": "..." } }
```

It uses the same runtime-core mutation path as Kafka, gRPC materialized ingress,
and in-memory tests. No second mutation implementation exists.

TCP publish is only for topics whose source of truth is the TCP publisher. The
runtime must reject TCP mutations for Kafka-owned or gRPC-owned topics.

TCP publish tests should cover:

- publish -> live query receives delta
- patch -> filter enter/leave
- delete -> remove
- batch -> one coherent version range
- invalid payload -> typed failure, no partial mutation unless explicitly documented
- startup failure when the configured TCP port is unavailable
- runtime shutdown closes the TCP endpoint
- connection/line/queue bounds and source-owned topic rejection

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

Preferred serial benchmark runner:

```bash
pnpm run bench:baseline:smoke
pnpm run bench:baseline
pnpm run bench:baseline:grouped-admission
pnpm run bench:baseline:grouped-order-neutral
pnpm run bench:baseline:kafka-ingest
pnpm run bench:baseline:release
```

`bench:baseline` and `bench:baseline:smoke` run one small Chromium/browser profile plus small
engine profiles with smoke-sized Vitest benchmark settings. Engine smoke tasks use five iterations
to avoid one-sample write/read noise; browser smoke stays intentionally tiny so CI remains practical.
The smoke profile is the committed performance-regression gate: it compares fresh Vitest benchmark
artifacts against `benchmarks/baselines/smoke.json` and fails on cleanup, backpressure, queued-event,
RSS, mean-latency, or p99-latency regressions beyond the configured thresholds. Refresh the smoke
baseline intentionally with `pnpm run bench:baseline:smoke:update` when a performance change is
accepted.

`bench:baseline:release` is the release-quality serial profile and runs the documented row
counts/browser profiles in separate processes. It intentionally executes one benchmark process at a
time so GC, RSS, browser state, and artifact files are not polluted by competing benchmark suites.
The grouped-admission, grouped-order-neutral, and Kafka-ingest profiles are committed comparison
profiles. The Kafka-ingest profile starts Apache Kafka from `compose.yaml`, uses
`@platformatic/kafka`, creates unique source topics per run, and measures JSON/protobuf
produce-to-health-observed runtime ingest convergence. Release remains report-only/no-compare by default
because it is heavy; use `bench:baseline:release:update` only when accepting a release baseline
manifest.

The serial runner is `scripts/run-benchmark-baseline.mjs`. It supports
`VIEW_SERVER_BENCH_BASELINE_PROFILE=smoke|kafka-ingest|release|grouped-admission|grouped-order-neutral`
or `--profile=smoke|kafka-ingest|release|grouped-admission|grouped-order-neutral`; an explicit
`--profile` argument wins over the environment variable, so the root package scripts always run the
profile they name. Use the environment variable only when invoking the Node script directly. The
runner scrubs benchmark-specific environment variables before each child process so stale local
tuning cannot pollute baseline runs. Pass `--update-baseline` to write
`benchmarks/baselines/<profile>.json` from the fresh artifacts, or `--no-compare` to run a profile as
serial benchmarks only.

Current Kafka ingest benchmark harness:

```bash
pnpm run bench:baseline:kafka-ingest
pnpm run bench:baseline:kafka-ingest:update
```

The profile runs `runtime#bench:kafka-ingest` through the serial benchmark runner. It starts the
Apache Kafka service from `compose.yaml`, which uses tmpfs storage, auto-topic creation, and
`KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0`. The benchmark still creates unique source topics per run
so Kafka state cannot bleed across samples.

Kafka ingest benchmark cases:

- JSON source batch ingest: publish one JSON batch, wait until the runtime consumes it and health sees
  the new rows.
- Protobuf source batch ingest: publish one protobuf key/value batch generated from Buf schemas, wait
  until the runtime consumes it and health sees the new rows.
- Mixed source burst ingest: publish JSON and protobuf bursts concurrently. The committed baseline
  uses `250` rows per source and a burst multiplier of `4`, which is `2,000` Kafka messages per timed
  sample. The larger 10k+/sec Kafka firehose profile remains a manual/future benchmark until runtime
  ingest throughput is optimized enough for a stable baseline gate.

Kafka ingest knobs:

- `VIEW_SERVER_RUNTIME_BENCH_KAFKA_BATCH_SIZE`: per-source sustained batch size.
- `VIEW_SERVER_RUNTIME_BENCH_KAFKA_BURST_MULTIPLIER`: multiplier for the mixed burst case.
- `VIEW_SERVER_RUNTIME_BENCH_ITERATIONS`: benchmark iterations per case.
- `VIEW_SERVER_RUNTIME_BENCH_TIME_MS`: benchmark time budget per case.
- `VIEW_SERVER_RUNTIME_BENCH_WARMUP_ITERATIONS`: warmup iterations per case.
- `VIEW_SERVER_RUNTIME_BENCH_WARMUP_TIME_MS`: warmup time budget per case.
- `VIEW_SERVER_RUNTIME_BENCH_OUTPUT_JSON`: Vitest timing artifact path.

Kafka ingest artifacts:

- `packages/runtime/.artifacts/kafka-ingest-250rows.json`
- `packages/runtime/.artifacts/kafka-ingest-250rows.summary.json`
- `benchmarks/baselines/kafka-ingest.json`

The sidecar summary records runtime health, per-lane Kafka committed offsets, RSS deltas, cleanup
leaks, queued events, and backpressure counters. It intentionally keeps
`topics` stable as internal View Server topic names while source Kafka topic names remain unique per
run.

Current engine raw benchmark harness:

```bash
vp run --no-cache column-live-view-engine#bench:raw-snapshot
```

Use `--no-cache` because benchmark row counts and timing are environment-driven. It defaults to a
100k-row smoke profile and writes both Vitest timing JSON and a View Server summary sidecar under
`packages/column-live-view-engine/.artifacts/`, for example
`raw-snapshot-100000rows.json` and `raw-snapshot-100000rows.summary.json`. Run each row count in a
separate process so previous profiles do not contaminate GC/RSS/latency or overwrite artifacts. For
example:

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

Current broad-scan optimization signal:

- Exact raw scans precompile slot predicates per scan, resolve columns once, use direct numeric
  column comparisons for finite number ranges/orderings, and avoid row-object reads when
  `callbackSkippable` proves column predicates are authoritative.
- Count-only raw scans (`limit: 0`) now count matching slots without ordering/window materialization.
  Selective predicate candidates are still used when available; broad predicates fall back to an
  O(rows) count-only scan without comparator/sort/window work.
- These are warm/shared-engine benchmark signals: the raw snapshot harness has an existing live
  subscription and shared same-process indexes. They are not cold index-build numbers.
- 10M raw snapshot `compound filter + top-k sort`: ~1983ms mean -> ~959ms mean.
- 10M raw snapshot `filtered totalRows via zero-row window`: ~727ms mean -> ~635ms mean -> one
  noisy ~470ms mean run after the count-only branch. This case remains a warm/shared-engine signal
  and still scans broad predicates.
- 10M raw snapshot `equality filter + top-k sort`: ~442ms mean -> ~280ms mean.
- 10M raw snapshot `range filter + top-k sort`: ~224ms mean -> ~52ms mean.
- 1M raw write sanity, base mode: single append ~0.049ms mean, batch append ~14.7ms mean.
- 1M raw write sanity, indexed mode: single append ~0.423ms mean, batch append ~15.5ms mean.
- 10M raw write sanity, base mode: single append ~0.054ms mean, batch append ~16.7ms mean.
- 10M raw write sanity, indexed mode: single append ~2.14ms mean, batch append ~161ms mean and
  very noisy. Treat the batch result as benchmark-state/GC pressure after warmed indexed writes, not
  a direct per-row ordered-index splice cost measurement.

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

Scanner-level predicate candidate selection uses a materialization safety budget, not a semantic
filter. New candidate builds up to 100k slots may be materialized; broader scalar/range candidates
fall back to the normal scan path so 10M-row topics do not allocate and sort multi-million-slot
candidate arrays. Already-warmed scalar buckets that later grow beyond the retained bucket budget
are evicted and rejected for scan selection, so broad buckets do not keep adding write-path
maintenance and memory pressure after they stop being selective. This fallback must preserve
`totalRows`, deterministic ordering, and exact predicate semantics.

It writes profile-specific Vitest timing JSON plus a View Server summary sidecar, for example
`raw-predicate-index-100000rows.json` and `raw-predicate-index-100000rows.summary.json`. Run each row
count in a separate process. The benchmark rejects row counts below 101 because several cases assert
exact 50/100-row windows and the range-candidate case must stay narrower than the whole table.

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

Current raw write benchmark harness:

```bash
vp run --no-cache column-live-view-engine#bench:raw-write
```

This harness uses Vitest `bench()` against the public `ColumnLiveViewEngine` mutation path. It seeds a
single topic, optionally warms read-path indexes, then measures:

- `publishMany` appends
- single-row `publish` appends
- `publishMany` replacements of existing rows
- per-row `patch`
- append followed by `delete`

The benchmark exists to catch the other side of read optimization work: schema-specialized Topic
Column Vectors, predicate indexes, ordered window indexes, and materialized state can improve reads
while adding write maintenance cost. Treat raw-write results as part of the same decision as raw
snapshot/predicate/fanout results.

Run both modes when evaluating a storage change:

- `base`: decoded engine writes without pre-warmed read-path indexes.
- `indexed`: pre-warms scalar predicate buckets and one ordered Raw Window Index before timed writes.
  Single-row appends pay ordered-index insertion cost; batch writes measure scalar predicate index
  maintenance plus the current ordered-index invalidation/clear behavior.

It writes `raw-write-<mode>-<rows>rows.json` plus a matching `.summary.json` sidecar. Run each mode
and row count in a separate process:

```bash
VIEW_SERVER_ENGINE_BENCH_WRITE_MODE=base VIEW_SERVER_ENGINE_BENCH_ROWS=100000 vp run --no-cache column-live-view-engine#bench:raw-write
VIEW_SERVER_ENGINE_BENCH_WRITE_MODE=indexed VIEW_SERVER_ENGINE_BENCH_ROWS=100000 vp run --no-cache column-live-view-engine#bench:raw-write
VIEW_SERVER_ENGINE_BENCH_WRITE_MODE=base VIEW_SERVER_ENGINE_BENCH_ROWS=1000000 vp run --no-cache column-live-view-engine#bench:raw-write
VIEW_SERVER_ENGINE_BENCH_WRITE_MODE=indexed VIEW_SERVER_ENGINE_BENCH_ROWS=1000000 vp run --no-cache column-live-view-engine#bench:raw-write
VIEW_SERVER_ENGINE_BENCH_WRITE_MODE=base VIEW_SERVER_ENGINE_BENCH_ROWS=10000000 vp run --no-cache column-live-view-engine#bench:raw-write
VIEW_SERVER_ENGINE_BENCH_WRITE_MODE=indexed VIEW_SERVER_ENGINE_BENCH_ROWS=10000000 vp run --no-cache column-live-view-engine#bench:raw-write
```

Raw write knobs:

- `VIEW_SERVER_ENGINE_BENCH_WRITE_MODE`: `base` or `indexed`; defaults to `indexed`.
- `VIEW_SERVER_ENGINE_BENCH_ROWS`: seeded row count for this benchmark process.
- `VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE`: publish batch size while seeding and timed write batch size.
- `VIEW_SERVER_ENGINE_BENCH_ITERATIONS`: benchmark iterations per case.
- `VIEW_SERVER_ENGINE_BENCH_TIME_MS`: benchmark time budget per case.
- `VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS`: must remain `0`; raw-write mutates shared engine state.
- `VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS`: must remain `0`; raw-write mutates shared engine state.

Current grouped aggregate benchmark harness:

```bash
vp run --no-cache column-live-view-engine#bench:grouped-aggregate
```

This harness uses Vitest `bench()` against the public `ColumnLiveViewEngine` snapshot, subscribe,
and publish path. It seeds one topic and measures grouped reads with count, countDistinct, sum, avg,
min, max, aggregate ordering, group-key ordering, filters, high-cardinality groups, and zero-row
group counts. It also opens one selective grouped live subscription, drains the initial snapshot,
publishes a matching row, and waits for the grouped delta. The live query intentionally filters to an
upper price tail so the initial matched member count stays below the current grouped incremental
member target; the public artifact validates the member bound but does not expose the internal
execution mode directly.

It writes `grouped-aggregate-<rows>rows.json` plus a matching `.summary.json` sidecar under
`packages/column-live-view-engine/.artifacts/`. Run each row count in a separate process so previous
profiles do not contaminate GC/RSS/latency or overwrite artifacts:

```bash
VIEW_SERVER_ENGINE_BENCH_ROWS=100000 vp run --no-cache column-live-view-engine#bench:grouped-aggregate
VIEW_SERVER_ENGINE_BENCH_ROWS=1000000 vp run --no-cache column-live-view-engine#bench:grouped-aggregate
VIEW_SERVER_ENGINE_BENCH_ROWS=5000000 vp run --no-cache column-live-view-engine#bench:grouped-aggregate
```

The release baseline runner includes those three grouped row counts with `iterations=3` and
`time=0`, so the 5M profile is bounded by sample count instead of a time-budget loop. The smoke
runner uses 1k rows with one iteration and small seed batches to verify wiring quickly.

Grouped aggregate benchmark cases:

- `status grouped count/sum/min/max/avg`
- `region+status grouped count/sum/min/max/avg`
- `high-cardinality desk grouped aggregates`
- `high-cardinality desk group count via zero-row window`
- `filtered status grouped aggregates`
- `live grouped aggregate delta after publish`

Grouped aggregate knobs:

- `VIEW_SERVER_ENGINE_BENCH_ROWS`: row count for this benchmark process.
- `VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE`: publish batch size while seeding.
- `VIEW_SERVER_ENGINE_BENCH_ITERATIONS`: benchmark iterations per case.
- `VIEW_SERVER_ENGINE_BENCH_TIME_MS`: benchmark time budget per case.
- `VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS`: warmup iterations per case.
- `VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS`: warmup time budget per case.

Interpretation notes:

- Snapshot cases are grouped read baselines and intentionally use the public engine API rather than
  grouped internals so future columnar/SIMD/Rust rewrites remain comparable.
- The live grouped delta case is iteration-bound and disables time/warmup sampling. It records the
  number of live publishes in the summary sidecar.
- Raw write benchmarks remain the write-path guardrail for storage/index maintenance, but they do
  not prove grouped materialized patch/delete/group-move costs. Any future grouped materialized index
  must add matching grouped write-path cases before adoption, because faster grouped reads can easily
  regress publish/patch/delete costs.

Current grouped key width benchmark harness:

```bash
vp run --no-cache column-live-view-engine#bench:grouped-key-width
```

This harness uses Vitest `bench()` against the public `ColumnLiveViewEngine` grouped snapshot path.
It keeps aggregate work intentionally small (`count` only) and varies the number of grouped key
columns: one, two, four, and eight while keeping grouped cardinality constant. It also includes an
eight-key grouped-field ordering case that orders by `groupKey2..groupKey8`, so early order fields
have ties and later fields must participate as tie-breakers. The purpose is to make grouped key
materialization and grouped field order-plan costs visible without mixing them with BigDecimal
sum/avg, live delta work, or accidental group-count growth.

It writes `grouped-key-width-<rows>rows.json` plus a matching `.summary.json` sidecar under
`packages/column-live-view-engine/.artifacts/`. Run each row count in a separate process:

```bash
VIEW_SERVER_ENGINE_BENCH_ROWS=100000 vp run --no-cache column-live-view-engine#bench:grouped-key-width
VIEW_SERVER_ENGINE_BENCH_ROWS=1000000 vp run --no-cache column-live-view-engine#bench:grouped-key-width
```

The smoke baseline runner includes the 1k-row profile. The release baseline runner includes 100k
and 1M rows with the same bounded grouped read iteration/time settings as grouped aggregate.

Grouped key width benchmark cases:

- `groupBy one key`
- `groupBy two keys`
- `groupBy four keys`
- `groupBy eight keys`
- `groupBy eight ordered keys`

Interpretation notes:

- This is a grouped read benchmark only. Pair it with grouped write benchmarks before adopting any
  per-group or per-key-width cached state.
- The eight-key ordered case measures grouped field orderBy work in addition to key materialization,
  including real tie-breaker participation across multiple grouped fields.
- It is intentionally public-API based so a future columnar/SIMD/Rust-backed grouped engine can keep
  the same benchmark contract.

Current grouped write benchmark harness:

```bash
vp run --no-cache column-live-view-engine#bench:grouped-write
```

This harness uses Vitest `bench()` against the public `ColumnLiveViewEngine` subscribe and mutation
path. It seeds one topic, opens grouped live subscriptions, drains their initial snapshots, then
times grouped writes while also draining one delta from each active subscription. This is an
end-to-end grouped write signal: publish/patch/delete plus grouped delta publication, not only raw
mutation enqueue cost.

It defaults to `VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_MODE=incremental`, which uses selective
grouped subscriptions sized under the current grouped incremental admission limits. An explicit
`VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_MODE=fallback` keeps broad grouped subscriptions and measures
full grouped fallback rebuild pressure; do not interpret fallback mode as materialized grouped write
maintenance.

It writes `grouped-write-<mode>-<rows>rows-<mutations>mutations.json` for the default dual-reader
profile, or `grouped-write-<mode>-<reader-profile>-<rows>rows-<mutations>mutations.json` for
isolated reader profiles, plus a matching `.summary.json` sidecar under
`packages/column-live-view-engine/.artifacts/`. Run each row count in a separate process so previous
profiles do not contaminate GC/RSS/latency or overwrite artifacts:

```bash
VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_MODE=incremental VIEW_SERVER_ENGINE_BENCH_ROWS=100000 vp run --no-cache column-live-view-engine#bench:grouped-write
VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_MODE=incremental VIEW_SERVER_ENGINE_BENCH_ROWS=1000000 vp run --no-cache column-live-view-engine#bench:grouped-write
VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_MODE=incremental VIEW_SERVER_ENGINE_BENCH_ROWS=5000000 vp run --no-cache column-live-view-engine#bench:grouped-write
```

The release baseline runner includes those three grouped write row counts with `iterations=3`,
`time=0`, and `VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE=1`. The smoke runner also uses a one-row
write batch to verify wiring quickly. Larger write batches are supported through
`VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE`, but should be run intentionally because grouped
patch/delete/group-move costs can become very expensive at high row counts.

Grouped admission tuning uses a dedicated serial baseline profile:

```bash
pnpm run bench:baseline:grouped-admission
```

That profile runs default incremental grouped writes with larger write batches, a forced-fallback
incremental admission run, and a broad fallback run. Forced fallback and broad fallback use
`VIEW_SERVER_ENGINE_BENCH_ARTIFACT_SUFFIX` so they do not overwrite the default incremental artifact
for the same row count and write batch. The summary sidecar includes `groupedWriteAdmission` and
`preCleanupHealth`, so every run records whether grouped subscriptions were admitted as incremental
views or demoted to fallback before cleanup. `groupedWriteAdmission` also records grouped full
evaluation and patched-evaluation counters after setup and before cleanup, so order-neutral patch
regressions are visible in baseline comparisons. The default command compares fresh artifacts against
`benchmarks/baselines/grouped-admission.json`; use
`pnpm run bench:baseline:grouped-admission:update` only when accepting a new grouped-admission
baseline.

Order-neutral grouped evaluation patching uses a dedicated serial baseline profile:

```bash
pnpm run bench:baseline:grouped-order-neutral
```

That profile sets `VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_READER_PROFILE=order-neutral` and runs the
same 100k, 1M, and 5M grouped write row counts as the release grouped-write profile. It keeps only
the field-ordered grouped subscription open, so `grouped patch aggregate values` isolates the
order-neutral evaluation patch path instead of mixing it with an aggregate-ordered subscriber that
must rebuild the grouped window. The default command compares fresh artifacts against
`benchmarks/baselines/grouped-order-neutral.json`; use
`pnpm run bench:baseline:grouped-order-neutral:update` only when accepting a new order-neutral
baseline.

Grouped write benchmark cases:

- `grouped publishMany append batch`
- `grouped publishMany replace extrema batch`
- `grouped patch aggregate values`
- `grouped patch group moves`
- `grouped delete existing rows`

Grouped write knobs:

- `VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_MODE`: `incremental` or `fallback`; defaults to
  `incremental`.
- `VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_READER_PROFILE`: `dual`, `order-neutral`, or
  `aggregate-ordered`; defaults to `dual`. `dual` keeps the historical mixed-reader benchmark.
  `order-neutral` keeps only the field-ordered grouped subscription. `aggregate-ordered` keeps only
  the aggregate-ordered grouped subscription.
- `VIEW_SERVER_ENGINE_BENCH_ROWS`: row count for this benchmark process.
- `VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE`: publish batch size while seeding.
- `VIEW_SERVER_ENGINE_BENCH_WRITE_BATCH_SIZE`: number of rows mutated by batch-style samples and by
  the per-row patch/delete loops inside each sample. Defaults to `1` for release practicality.
- `VIEW_SERVER_ENGINE_BENCH_ARTIFACT_SUFFIX`: optional suffix added to the grouped-write artifact
  filename. Use it when running the same mode/row-count/write-batch with different admission limits.
- `VIEW_SERVER_ENGINE_BENCH_GROUPED_INCREMENTAL_MAX_GROUPS`: max groups retained by an admitted
  incremental grouped view.
- `VIEW_SERVER_ENGINE_BENCH_GROUPED_INCREMENTAL_MAX_MEMBERS`: max retained matching rows for an
  admitted incremental grouped view.
- `VIEW_SERVER_ENGINE_BENCH_GROUPED_INCREMENTAL_MAX_MEMBERS_PER_GROUP`: max retained rows in one
  group for an admitted incremental grouped view.
- `VIEW_SERVER_ENGINE_BENCH_GROUPED_INCREMENTAL_MAX_RETAINED_VALUE_ENTRIES`: max retained reverse
  entries used by `countDistinct`, `min`, and `max` aggregate maintenance.
- `VIEW_SERVER_ENGINE_BENCH_ITERATIONS`: benchmark iterations per case.
- `VIEW_SERVER_ENGINE_BENCH_TIME_MS`: must remain `0`; grouped write mutates shared engine state.
- `VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS`: must remain `0`; grouped write mutates shared engine
  state.
- `VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS`: must remain `0`; grouped write mutates shared engine
  state.

Interpretation notes:

- Timed bodies include grouped mutation work and live grouped event drain for every active grouped
  subscription selected by `VIEW_SERVER_ENGINE_BENCH_GROUPED_WRITE_READER_PROFILE`.
- Incremental mode covers admitted grouped insert/update/move/delete write pressure. Fallback mode
  covers broad grouped rebuild pressure and is intentionally a different signal.
- Current incremental grouped write maintenance updates aggregate states reversibly for inserts,
  removals, same-group patches, and group moves. Count, count-distinct, sum, avg, min, and max are
  adjusted from the changed row instead of recomputing every dirty group from its member map.
- Same-group patches whose changed aggregates are not used by grouped `orderBy` patch the current
  materialized grouped evaluation in place. Visible dirty groups get fresh finalized aggregate rows;
  dirty groups outside the visible window only advance the evaluation version. Group add/delete/move,
  predicate enter/leave, and aggregate changes that affect grouped ordering still rebuild the grouped
  window.
- The default grouped-write benchmark keeps both a field-ordered grouped subscription and an
  aggregate-ordered grouped subscription open. `grouped patch aggregate values` is therefore a mixed
  signal: the field-ordered subscription can use the order-neutral evaluation patch, while the
  aggregate-ordered subscription still rebuilds its grouped window. Use
  `pnpm run bench:baseline:grouped-order-neutral` when the decision needs an isolated order-neutral
  write-path signal.
- Retained `min`/`max` removals can invalidate the current extremum. The incremental executor batches
  those invalidations and recomputes each dirty `{group, aggregate}` once after the mutation batch,
  avoiding repeated retained-map scans when a single `publishMany` replaces or deletes many extrema.
- Incremental grouped admission also budgets retained value state for `countDistinct`, `min`, and
  `max`. Queries that would retain too many alias/member reverse entries demote to fallback rather
  than keeping high-cardinality reversible state alive between writes.
- Check `groupedWriteAdmission.activeIncrementalGroupedViewsBeforeCleanup` and
  `groupedWriteAdmission.activeFallbackGroupedViewsBeforeCleanup` before interpreting benchmark
  numbers. A run configured as incremental but admitted as fallback is a fallback result.
- Check `groupedWriteAdmission.groupedFullEvaluationCountBeforeCleanup` and
  `groupedWriteAdmission.groupedPatchedEvaluationCountBeforeCleanup` when evaluating grouped write
  changes. Order-neutral patch cases should increase patched evaluations; aggregate-ordered or
  fallback cases legitimately increase full evaluations.
- The benchmark does not prove a future grouped materialized index is worthwhile by itself; compare
  it with grouped read benchmarks before adopting storage that adds write maintenance.
- Keep grouped write and grouped aggregate benchmarks together when evaluating grouped engine
  changes, because read wins can hide write regressions.

Current raw live fanout benchmark harness:

```bash
vp run --no-cache column-live-view-engine#bench:raw-live-fanout
```

This harness uses Vitest `bench()` against the public `ColumnLiveViewEngine` subscribe/publish path,
not scanner internals. It seeds one topic, opens many raw live subscriptions, drains initial
snapshots, publishes a matching row, and waits for every subscriber to receive a delta for the
publish version. The same-window case also verifies the inserted row appears in every subscriber
delta. It runs one fanout case per process so large profiles do not hold multiple full engines in
memory:

- same-query/same-window subscribers
- same-query/ten-window subscribers

It writes case-specific artifacts under `packages/column-live-view-engine/.artifacts/`, such as
`raw-live-fanout-same-window-100000rows-50subs.json` and
`raw-live-fanout-ten-window-1000000rows-250subs.json`, plus matching `.summary.json` sidecars. Run
each row count in a separate process, and run each fanout case separately, so previous profiles do
not contaminate GC/RSS/latency or overwrite each other:

```bash
VIEW_SERVER_ENGINE_BENCH_FANOUT_CASE=same-window VIEW_SERVER_ENGINE_BENCH_ROWS=100000 VIEW_SERVER_ENGINE_BENCH_SUBSCRIBERS=50 vp run --no-cache column-live-view-engine#bench:raw-live-fanout
VIEW_SERVER_ENGINE_BENCH_FANOUT_CASE=ten-window VIEW_SERVER_ENGINE_BENCH_ROWS=100000 VIEW_SERVER_ENGINE_BENCH_SUBSCRIBERS=50 vp run --no-cache column-live-view-engine#bench:raw-live-fanout
VIEW_SERVER_ENGINE_BENCH_FANOUT_CASE=same-window VIEW_SERVER_ENGINE_BENCH_ROWS=1000000 VIEW_SERVER_ENGINE_BENCH_SUBSCRIBERS=250 vp run --no-cache column-live-view-engine#bench:raw-live-fanout
VIEW_SERVER_ENGINE_BENCH_FANOUT_CASE=ten-window VIEW_SERVER_ENGINE_BENCH_ROWS=1000000 VIEW_SERVER_ENGINE_BENCH_SUBSCRIBERS=250 vp run --no-cache column-live-view-engine#bench:raw-live-fanout
```

For the default case-specific scripts:

```bash
VIEW_SERVER_ENGINE_BENCH_ROWS=100000 VIEW_SERVER_ENGINE_BENCH_SUBSCRIBERS=50 vp run --no-cache column-live-view-engine#bench:raw-live-fanout:same-window
VIEW_SERVER_ENGINE_BENCH_ROWS=100000 VIEW_SERVER_ENGINE_BENCH_SUBSCRIBERS=50 vp run --no-cache column-live-view-engine#bench:raw-live-fanout:ten-window
```

Active raw queries retain the row-change journal while the active query is leased. Finite non-zero
active windows retain one extra lookahead row beyond the shared visible base window. Retained insert
batches can update the shared base window incrementally by merging the previous retained window with
matching inserted rows, sorting that retained candidate window, and preserving `totalRows`. Retained
updates/deletes that provably do not match the predicate are ignored without rescanning. Retained
deletes or predicate-leaving updates can refill visible windows from the lookahead row when one
changed row leaves the retained window. Matching retained match-to-match updates that compare equal
or better than their previous retained entry update the retained candidate window without rescanning.
Matching deletes outside the retained window update `totalRows` without touching the visible window.
For `limit: 0` count-only subscriptions, retained inserts, updates, and deletes adjust `totalRows`
directly because there is no visible window to refill. Unavailable retained changes, exhausted
lookahead, match-to-match updates outside retained lookahead, match-to-match updates that move
worse within finite retained windows, and base-window shape changes still fall back to a full raw
window scan. This includes same-index tail updates that become worse than outside lookahead rows.
This keeps correctness local while making append-heavy live top-k deltas, simple retained
deletes/leaves, safe retained match-to-match updates, and count-only retained changes avoid full
10M-row re-evaluation.

Current directional result after the insert-only path:

- 10M raw snapshot `live subscription delta after publish`: ~564ms mean -> ~5.3ms mean.
- 1M rows / 250 subscribers / same-window fanout: ~49.5ms mean -> ~9.7ms mean.
- 1M rows / 250 subscribers / ten-window fanout: ~50.7ms mean -> ~10.0ms mean.

Raw live fanout knobs:

- `VIEW_SERVER_ENGINE_BENCH_FANOUT_CASE`: `same-window` or `ten-window`.
- `VIEW_SERVER_ENGINE_BENCH_ROWS`: row count for this benchmark process.
- `VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE`: publish batch size while seeding.
- `VIEW_SERVER_ENGINE_BENCH_SUBSCRIBERS`: live subscriber count.
- `VIEW_SERVER_ENGINE_BENCH_ITERATIONS`: benchmark iterations per case.
- `VIEW_SERVER_ENGINE_BENCH_TIME_MS`: benchmark time budget per case.
- `VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS`: warmup iterations per case.
- `VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS`: warmup time budget per case.

Current raw active retained delta benchmark harness:

```bash
vp run --no-cache column-live-view-engine#bench:raw-active-retained-delta
```

This harness uses Vitest `bench()` against the public `ColumnLiveViewEngine` subscribe and mutation
path. It seeds one topic, opens one active raw subscription, drains the initial snapshot, then runs
one retained-change case per process so subscription queues and mutable engine state do not
cross-contaminate cases:

- `noop`: insert, update, and delete a nonmatching closed row while asserting no queued event.
- `match-update`: patch the current top row so it remains matching and retained, asserting one
  update delta. The no-rescan guarantee is enforced by the retained-window correctness tests with
  scan-count instrumentation.
- `predicate-enter`: insert a closed row, patch it into the predicate, and read the retained delta.
- `visible-delete`: repeatedly delete the current visible top row and read the refill/fallback
  delta sequence.
- `exhausted-lookahead`: delete two visible rows before reading, measuring one lookahead refill
  followed by the exhausted-lookahead fallback path.
- `count-only`: publish one matching row into a `limit: 0` count-only subscription.

It writes case-specific artifacts under `packages/column-live-view-engine/.artifacts/`, such as
`raw-active-retained-delta-visible-delete-100000rows.json` and matching `.summary.json` sidecars.
Run each case and row count in a separate process, and do not run competing benchmark suites in
parallel:

```bash
VIEW_SERVER_ENGINE_BENCH_RETAINED_CASE=noop VIEW_SERVER_ENGINE_BENCH_ROWS=100000 vp run --no-cache column-live-view-engine#bench:raw-active-retained-delta
VIEW_SERVER_ENGINE_BENCH_RETAINED_CASE=match-update VIEW_SERVER_ENGINE_BENCH_ROWS=100000 vp run --no-cache column-live-view-engine#bench:raw-active-retained-delta
VIEW_SERVER_ENGINE_BENCH_RETAINED_CASE=predicate-enter VIEW_SERVER_ENGINE_BENCH_ROWS=100000 vp run --no-cache column-live-view-engine#bench:raw-active-retained-delta
VIEW_SERVER_ENGINE_BENCH_RETAINED_CASE=visible-delete VIEW_SERVER_ENGINE_BENCH_ROWS=100000 vp run --no-cache column-live-view-engine#bench:raw-active-retained-delta
VIEW_SERVER_ENGINE_BENCH_RETAINED_CASE=exhausted-lookahead VIEW_SERVER_ENGINE_BENCH_ROWS=100000 vp run --no-cache column-live-view-engine#bench:raw-active-retained-delta
VIEW_SERVER_ENGINE_BENCH_RETAINED_CASE=count-only VIEW_SERVER_ENGINE_BENCH_ROWS=100000 vp run --no-cache column-live-view-engine#bench:raw-active-retained-delta
```

For the default case-specific scripts:

```bash
VIEW_SERVER_ENGINE_BENCH_ROWS=100000 vp run --no-cache column-live-view-engine#bench:raw-active-retained-delta:noop
VIEW_SERVER_ENGINE_BENCH_ROWS=100000 vp run --no-cache column-live-view-engine#bench:raw-active-retained-delta:match-update
VIEW_SERVER_ENGINE_BENCH_ROWS=100000 vp run --no-cache column-live-view-engine#bench:raw-active-retained-delta:predicate-enter
VIEW_SERVER_ENGINE_BENCH_ROWS=100000 vp run --no-cache column-live-view-engine#bench:raw-active-retained-delta:visible-delete
VIEW_SERVER_ENGINE_BENCH_ROWS=100000 vp run --no-cache column-live-view-engine#bench:raw-active-retained-delta:exhausted-lookahead
VIEW_SERVER_ENGINE_BENCH_ROWS=100000 vp run --no-cache column-live-view-engine#bench:raw-active-retained-delta:count-only
```

Raw active retained delta knobs:

- `VIEW_SERVER_ENGINE_BENCH_RETAINED_CASE`: `noop`, `match-update`, `predicate-enter`,
  `visible-delete`, `exhausted-lookahead`, or `count-only`.
- `VIEW_SERVER_ENGINE_BENCH_ROWS`: seeded row count for this benchmark process; minimum 101.
- `VIEW_SERVER_ENGINE_BENCH_BATCH_SIZE`: publish batch size while seeding.
- `VIEW_SERVER_ENGINE_BENCH_ITERATIONS`: exact benchmark iterations per case.
- `VIEW_SERVER_ENGINE_BENCH_TIME_MS`: must remain `0`; this stateful benchmark is iteration-bound
  so timed loops cannot silently exhaust finite retained windows.
- `VIEW_SERVER_ENGINE_BENCH_WARMUP_ITERATIONS`: must remain `0`; this benchmark mutates shared
  engine state.
- `VIEW_SERVER_ENGINE_BENCH_WARMUP_TIME_MS`: must remain `0`; this benchmark mutates shared engine
  state.

Current browser in-memory React benchmark harness:

```bash
vp run --no-cache react#bench:in-memory-live-query
```

This harness uses Vitest Browser Mode `bench()` with `vitest-browser-react`. It seeds an in-memory
View Server through the runtime client, renders a real component through the React client provider
with an in-memory live client, subscribes with `useLiveQuery`, publishes a matching row, and waits
for the top row to appear in the rendered output. Publish uses the runtime client; observation uses
the in-memory live client and React hook/provider seam. It measures:

```text
publish -> runtime-core -> Column Live View Engine subscription -> React hook/store -> render
```

It intentionally excludes Kafka, TCP/gRPC ingress, Effect RPC WebSocket, remote global setup, and
network latency. Use it to catch browser/provider/hook regressions after engine changes and to prove
the in-memory live client uses the same runtime-core/engine path as production. It samples cleanup
after React unmount/subscription release and before force-closing the runtime. It writes Vitest
timing JSON plus a View Server summary sidecar under `packages/react/.artifacts/`, for example
`in-memory-live-query-10000rows-chromium.json` and
`in-memory-live-query-10000rows-chromium.summary.json`.

Run each browser/row-count combination in a separate process and do not run competing benchmark
suites in parallel:

```bash
VIEW_SERVER_REACT_BENCH_ROWS=10000 VIEW_SERVER_REACT_BENCH_BROWSER=chromium vp run --no-cache react#bench:in-memory-live-query
VIEW_SERVER_REACT_BENCH_ROWS=10000 VIEW_SERVER_REACT_BENCH_BROWSER=firefox vp run --no-cache react#bench:in-memory-live-query
VIEW_SERVER_REACT_BENCH_ROWS=10000 VIEW_SERVER_REACT_BENCH_BROWSER=webkit vp run --no-cache react#bench:in-memory-live-query
```

Browser in-memory React knobs:

- `VIEW_SERVER_REACT_BENCH_BROWSER`: Vitest browser instance, usually `chromium`, `firefox`, or `webkit`.
- `VIEW_SERVER_REACT_BENCH_ROWS`: row count for this benchmark process.
- `VIEW_SERVER_REACT_BENCH_BATCH_SIZE`: publish batch size while seeding.
- `VIEW_SERVER_REACT_BENCH_ITERATIONS`: benchmark iterations per case.
- `VIEW_SERVER_REACT_BENCH_TIME_MS`: benchmark time budget per case.
- `VIEW_SERVER_REACT_BENCH_WARMUP_ITERATIONS`: warmup iterations per case.
- `VIEW_SERVER_REACT_BENCH_WARMUP_TIME_MS`: warmup time budget per case.
- `VIEW_SERVER_REACT_BENCH_OUTPUT_JSON`: optional artifact path override.

Each production/runtime benchmark sidecar must include:

- row count
- mutation count
- subscribers
- topics
- latency source pointing at the Vitest timing JSON containing p50/p95/p99/max samples
- memory/RSS/heap before setup, after setup, and after clearing retained benchmark fixture references,
  or an explicit `browser-unavailable` marker for browser-only benchmarks
- top-level queued event count
- backpressure count
- cleanup leak count, with non-zero engine cleanup leaks failing the benchmark after the sidecar is written
- health snapshot after explicit subscription cleanup and before force-closing the engine

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

- example app keeps real runtime URL injection at the provider boundary
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
