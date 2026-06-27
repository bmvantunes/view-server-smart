# View Server gRPC Ingress Plan

This plan defines the gRPC ingress model for View Server.

It intentionally keeps the existing architecture intact:

- one View Server topic is one logical table
- `ColumnLiveViewEngine` remains the only query/snapshot/delta engine
- runtime-core and the engine stay transport/ingress-neutral
- Kafka, gRPC, TCP, and future sources are ingress Adapters only
- React hooks continue to use the same provider/client seam
- Effect RPC WebSocket with NDJSON remains the production browser transport

Non-goals:

- do not add a gRPC-specific query API
- do not add a gRPC-specific React hook
- do not expose leased feed instances as public topics
- do not let one user query merge multiple upstream gRPC streams
- do not make gRPC a second query engine or second storage API
- do not let one-shot snapshot/read APIs bypass leased-feed lifecycle or feed partitioning

## Compatibility With The Column Live View Engine Plan

This plan does not conflict with `plans/v2-column-live-view-engine-plan.md`.

The existing plan says:

- runtime config owns Kafka, TCP publishing, gRPC publishing, and deploy-time/server-only wiring
- browser bundles must not import runtime config
- the engine package must not depend on Kafka, WebSocket, React, TCP, gRPC, or server runtime code
- the same authoritative in-memory Topic Store serves snapshots, deltas, counts, grouped views, and subscription change streams
- only transport/ingress Adapters differ between production and in-memory/test paths

The gRPC design follows those rules by making gRPC an ingress Adapter that publishes rows into the existing runtime-core and engine. It must not create a second query engine, a second subscription system, or a gRPC-specific React hook.

Validation result:

- Compatible with the v2 plan's core rule that the `ColumnLiveViewEngine` is the only query/snapshot/delta engine.
- Compatible with the one-topic-one-logical-table model because leased feed instances are internal runtime partitions for one public View Server topic, not public topics and not a second storage API.
- Compatible with the runtime boundary because gRPC clients, upstream requests, session headers, reconnect policy, and source lifecycle are server-only runtime concerns.
- Compatible with the React/provider model because `useLiveQuery` does not learn about gRPC. The server resolves leased feeds behind the same Live Query contract.
- Compatible with the health cadence rule because gRPC health is a ledger/cached snapshot, not a full per-message health rebuild.
- Compatible with the transport policy because browser live data continues over Effect RPC WebSocket with NDJSON. gRPC is ingress only.
- Compatible with the one-engine rule because leased-feed row isolation is an internal partitioning concern before a query reaches the existing engine. The engine still executes the same query semantics; the runtime only narrows the retained row universe to the single resolved feed instance.

Important boundaries carried forward from the v2 plan:

- Browser code must not import runtime/gRPC config. The only browser-facing API is still the typed React provider/hooks.
- In-memory testing must continue to use the same runtime-core and engine. gRPC leased/materialized feeds are server runtime Adapters, not a special test engine.
- Production browser live traffic stays on Effect RPC WebSocket + NDJSON until a separate transport decision changes that. gRPC is not a browser transport.
- Runtime health must remain cached/coalesced. gRPC message ingestion may update cheap counters, but it must not rebuild full health per upstream event.
- Topic ownership must stay explicit. One public View Server topic cannot be silently populated by Kafka and gRPC, or by multiple gRPC feed definitions, unless a future multi-source contract defines ordering, dedupe, health, and restart semantics.

## Core Vocabulary

Use these names consistently:

- `topic`: the public View Server logical table, for example `orders`.
- `materializedFeed`: a gRPC source that starts on View Server startup and remains active until runtime shutdown.
- `leasedFeed`: a gRPC source that starts only while at least one subscription needs a specific upstream route.
- `routeBy`: topic row fields that must be present as exact equality predicates in user queries for a leased feed.
- `route`: the extracted route values from a user query.
- `feedKey`: the derived internal identity for one upstream stream instance.
- `request`: the typed upstream gRPC request built from `route`.
- `acquire`: the Effect operation that opens the upstream gRPC stream.
- `release`: optional Effect cleanup beyond stream interruption.
- `view`: one user query over a materialized topic or leased feed.
- `lease`: the refcount/lifecycle handle that keeps a leased feed alive while subscriptions use it.

Avoid the names `hot`, `cold`, `eager`, and `lazy` in code. They are useful conversational shortcuts, but they blur the real contract:

- `materializedFeed` means runtime-owned, startup-acquired, retained state.
- `leasedFeed` means subscription-owned, route-keyed, retained only while there is demand.

Do not call leased feed instances "topics" in public APIs. Health can expose feed instances under a topic, but the public topic remains stable.

A leased feed instance is temporary retained state for one upstream route. It is not a View Server Topic, it is not visible to `useLiveQuery`, and it must
not change the external topic name or result typing.

## Source Lifecycle Modes

### Materialized Feed

A materialized feed is always-on.

Behavior:

- starts when the runtime starts
- is scoped to runtime lifetime
- marks health degraded if the upstream stream completes or fails
- retains state even with zero subscribers
- serves snapshots immediately when a user subscribes
- behaves similarly to Kafka materialized topics

Automatic reconnect policy is a later runtime policy slice. The first materialized runtime slice
must be honest: one scoped upstream stream per feed, deterministic cleanup, and explicit degraded
health on completion/failure.

Generated gRPC clients own protobuf decode at the ConnectRPC boundary. For this slice, decode
failures surface as upstream stream failures and degrade the feed. The `decodeFailuresPerSecond`
field remains zero unless a future Adapter introduces a raw payload decode boundary that can
attribute decode failures separately from stream failures.

Use for bounded or globally useful sources, for example all strategies.

```ts
const grpcFeed = viewServer.grpcFeed<typeof grpcClients>();

grpcFeed.materializedFeed({
  topic: "strategies",
  client: "strategies",
  method: "streamStrategies",
  request: () => ({}),

  acquire: ({ client, session }) =>
    Stream.fromAsyncIterable(
      client.streamStrategies(
        {},
        {
          headers: session.systemHeaders,
        },
      ),
      (cause) => new GrpcUpstreamError({ cause }),
    ),

  map: ({ value, schema }) => ({
    id: value.strategyId,
    name: value.name,
    region: value.region,
    updatedAt: value.updatedAt,
  }),
});
```

### Leased Feed

A leased feed is on-demand and route-keyed.

The runtime lease manager owns leased feed lifecycle. Materialized startup must ignore leased feeds;
it must not connect them early and must not leave them in health as permanently starting.

Behavior:

- does not connect on runtime startup
- requires all `routeBy` fields in the user query
- opens one upstream gRPC stream per distinct `feedKey`
- shares that upstream stream across all users with the same route
- applies remaining user filters/order/grouping locally inside View Server
- closes the upstream stream and drops retained rows for that feed when the last subscription releases it
- rejects queries that cannot be routed to exactly one upstream stream

Use for huge upstream sources where an unfiltered stream is impossible or dangerous.

Do not model each possible user filter combination as a separate configured topic. A leased feed has a small explicit `routeBy` contract that maps to
the upstream gRPC service's real access path, such as `strategyId` and `region`. The View Server then runs the full user query locally on the retained
rows for that route.

```ts
const grpcFeed = viewServer.grpcFeed<typeof grpcClients>();

grpcFeed.leasedFeed({
  topic: "orders",
  client: "orders",
  method: "streamOrders",
  routeBy: ["strategyId", "region"],

  request: ({ strategyId, region }) => ({
    strategyId,
    region,
  }),

  acquire: ({ client, request }) =>
    Stream.fromAsyncIterable(
      client.streamOrders(request),
      (cause) => new GrpcUpstreamError({ cause }),
    ),

  release: ({ client, request }) => client.closeOrdersStream?.(request) ?? Effect.void,

  map: ({ value, route, schema }) => ({
    id: value.orderId,
    strategyId: route.strategyId,
    region: route.region,
    instrumentId: value.instrumentId,
    status: value.status,
    price: value.price,
    updatedAt: value.updatedAt,
  }),
});
```

## Query Routing Invariant

A leased-feed user query must resolve to exactly one feed key.

If `orders` is configured with:

```ts
routeBy: ["strategyId", "region"];
```

then this is valid:

```ts
useLiveQuery("orders", {
  where: {
    strategyId: { eq: "strategy-1" },
    region: { eq: "usa" },
    status: { eq: "open" },
  },
  orderBy: [{ field: "updatedAt", direction: "desc" }],
  select: ["id", "status", "price", "updatedAt"],
  limit: 50,
});
```

These must fail at compile time and runtime:

```ts
useLiveQuery("orders", {
  where: {
    strategyId: { eq: "strategy-1" },
  },
  select: ["id", "price"],
  limit: 50,
});
```

```ts
useLiveQuery("orders", {
  where: {
    strategyId: { in: ["strategy-1", "strategy-2"] },
    region: { eq: "usa" },
  },
  select: ["id", "price"],
  limit: 50,
});
```

Route predicates must be exact equality predicates. Do not allow `in`, `startsWith`, `gte`, `lte`, ranges, missing route fields, or ambiguous access paths for leased feeds.

The runtime must reject invalid leased-feed queries with `InvalidQueryError`. Never fall back to an unfiltered upstream stream.

The type system should reject the same invalid leased-feed query shapes whenever the query object is statically visible. Runtime validation still remains
mandatory because remote clients and decoded wire payloads can bypass TypeScript.

## Local Query Semantics

The full user query still runs locally in the View Server engine.

For a leased `orders` feed routed by `strategyId` and `region`:

- `strategyId` and `region` select the upstream feed
- all rows from that upstream feed are retained in memory for that feed
- extra predicates such as `status`, `instrumentId`, `price`, and text filters are local engine filters
- order, projection, grouped aggregation, pagination/windowing, counts, and deltas are local engine work

This avoids creating one upstream stream per full UI query and avoids merging multiple upstream streams.

If the upstream gRPC API only supports `strategyId` and `region`, those two fields are the route contract. A user may add more local filters, but the runtime
must not attempt to satisfy a broader query by opening every possible route or by stitching together multiple routes.

The route contract is not derived from all possible filters in the UI. It is declared by the feed author from the upstream API contract. This avoids the
factorial/permutation problem where ten possible UI filters would imply many fake source topics or access paths.

Leased feed row isolation must not rely on upstream rows telling the truth about route fields. The runtime must isolate rows by the resolved internal
feed partition before executing the user's local query. For example, if a broken upstream stream for `region = london` emits a row whose `region` field
is `usa`, that row must not appear in the `region = usa` feed. Public row keys and result rows must be externalized so users never observe internal feed
partition keys.

One-shot snapshots for leased topics are dangerous if they bypass lease acquisition. A runtime may either implement a scoped snapshot that acquires the
lease, waits for the configured readiness condition, runs the partitioned snapshot, and releases it, or reject one-shot snapshots for leased topics. The
current slice should reject one-shot snapshots for leased topics and require live subscriptions, because subscriptions naturally own the lease lifetime.

Example:

```txt
User A query:
  strategyId = s1, region = usa, status = open

User B query:
  strategyId = s1, region = usa, price >= 100

Shared leased feed:
  orders route strategyId=s1 region=usa

Separate local views:
  A applies status = open
  B applies price >= 100
```

## Type Guarantees

The gRPC API must be as type-safe as the Kafka API.

Compile-time guarantees:

- `topic` only accepts keys from `defineViewServerConfig`.
- `routeBy` only accepts keys from the target topic row schema.
- leased-feed topics require exact equality filters for every `routeBy` field in `useLiveQuery`.
- route fields in `request(route)` are inferred from the configured topic row schema.
- `method` only accepts server-streaming methods from the configured ConnectRPC service.
- `request` must return the generated ConnectRPC request type for `method`.
- `acquire` receives a generated ConnectRPC client and request typed from `method`.
- `acquire` must return an Effect `Stream` whose value type matches the generated ConnectRPC response type for `method`.
- `map` is required for the first implementation slice, so schema conversion is explicit.
- `map` receives `value` inferred from the stream value and the return value must exactly match the target topic row schema.
- extra returned fields in `map` must fail.
- missing returned fields in `map` must fail.
- wrong route fields, wrong route operators, invalid topic names, invalid select/order/group/aggregate fields, and invalid mapping output must have type tests.

Do not require users to write `as const` to preserve route, select, or query inference.

## ConnectRPC-Specific Public API

The public gRPC API should be specific to ConnectRPC/generated clients instead of pretending to be a generic stream adapter.

Reasoning:

- generated service/client types should drive inference
- authentication and header forwarding are gRPC/Connect-specific
- users should not hand-wire low-level stream protocols for the common path
- a future generic stream-source seam can exist internally if it earns its keep

Sketch:

```ts
const grpcClients = {
  orders: grpc.connectClient({
    service: OrderService,
    baseUrl: Config.string("ORDERS_GRPC_URL"),
  }),
};
const grpcFeed = viewServer.grpcFeed<typeof grpcClients>();

export const runtime = viewServer.createRuntime({
  websocketPort: 8080,

  grpc: {
    clients: grpcClients,

    feeds: {
      ordersByStrategyRegion: grpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["strategyId", "region"],

        request: ({ strategyId, region }) => ({
          strategyId,
          region,
        }),

        acquire: ({ client, request }) =>
          Stream.fromAsyncIterable(
            client.streamOrders(request),
            (cause) => new GrpcUpstreamError({ cause }),
          ),

        map: ({ value, route, schema }) => ({
          id: value.orderId,
          strategyId: route.strategyId,
          region: route.region,
          instrumentId: value.instrumentId,
          status: value.status,
          price: value.price,
          updatedAt: value.updatedAt,
        }),
      }),
    },
  },
});
```

Exact package/function names can change during implementation, but the semantics should not.

## Topic Ownership Rule

For this slice, a View Server topic has one ingress owner.

This rule protects the one-topic-one-logical-table invariant from the v2 plan. A topic can be owned by Kafka, a materialized gRPC feed, or one leased gRPC
feed definition, but not by more than one ingress path at the same time.

Allowed examples, each in a separate runtime config:

```txt
orders -> Kafka materialized source
strategies -> gRPC materialized feed
orders -> gRPC leased feed definition ordersByStrategyRegion
```

The repeated `orders` name above is only showing alternative valid ownership modes. A single
configured View Server Topic must choose one ingress owner.

Rejected:

```txt
orders <- Kafka source and gRPC materialized feed
orders <- Kafka source and gRPC leased feed
orders <- gRPC materialized feed and gRPC leased feed
```

`ordersByStrategyRegion` is a feed definition name, not a public View Server topic name. A leased
feed definition may create many internal feed instances such as `orders/ordersByStrategyRegion/...`,
but `useLiveQuery` still queries the public `orders` topic and the runtime must route that query to
exactly one internal feed instance before executing local filters/sorts/groups.

The current public config shape can remain for now, but runtime/config validation must reject:

- Kafka and gRPC both targeting the same View Server topic
- multiple materialized gRPC feeds targeting the same View Server topic
- multiple leased gRPC feed definitions targeting the same View Server topic
- a materialized and leased gRPC feed both targeting the same View Server topic

If a future design needs multi-source topics, it must be explicit and must define ordering, deduplication, health, and restart semantics first.

A later API cleanup can reshape the config toward:

```ts
topics: {
  orders: kafka.topic(...),
  liveOrders: grpc.leasedTopic(...),
  strategies: grpc.materializedTopic(...),
}
```

Do not block the first gRPC implementation on that larger public API migration.

The important invariant is ownership, not the exact config nesting. The current config can keep `grpc.clients` and `grpc.feeds`, but validation must make
it impossible for two ingress sources to publish independently into the same View Server topic. That includes accidental combinations such as Kafka +
gRPC, materialized gRPC + leased gRPC, or two leased definitions for the same topic.

## Current Config Model

The first implementation keeps a small topic source marker on the public View Server topic:

```ts
const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
      source: grpc.leased({
        routeBy: ["strategyId", "region"],
      }),
    },

    strategies: {
      schema: Strategy,
      key: "id",
      source: grpc.materialized(),
    },
  },
});
```

Runtime feed definitions then reference those topics:

```ts
const grpcFeed = viewServer.grpcFeed<typeof grpcClients>();

viewServer.createRuntime({
  grpc: {
    clients: grpcClients,
    feeds: {
      ordersByStrategyRegion: grpcFeed.leasedFeed({
        topic: "orders",
        client: "orders",
        method: "streamOrders",
        routeBy: ["strategyId", "region"],
        request: ({ strategyId, region }) => ({ strategyId, region }),
        acquire: ({ client, request }) =>
          Stream.fromAsyncIterable(client.streamOrders(request), GrpcUpstreamError.fromUnknown),
        map: ({ value, route }) => ({
          id: value.orderId,
          strategyId: route.strategyId,
          region: route.region,
          instrumentId: value.instrumentId,
          status: value.status,
          price: value.price,
          updatedAt: value.updatedAt,
        }),
      }),
    },
  },
});
```

This is intentionally not the final "one source constructor per topic" API. It is the smallest compatible step that gives TypeScript enough information
to reject invalid `useLiveQuery` route predicates while preserving the existing runtime shape.

Validation rules:

- A topic marked `source: grpc.leased(...)` must have exactly one leased feed definition targeting it.
- A topic marked `source: grpc.materialized()` must have exactly one materialized feed definition targeting it.
- Kafka topics must not target gRPC-owned View Server topics.
- Plain/no-source topics remain writable by in-memory/TCP-style public publish clients.
- Public in-memory/runtime clients must reject direct publish/snapshot/subscribe/reset operations that would bypass leased-feed ownership.

Do not let users manually return `feedKey` from `acquire`. The runtime derives `feedKey` from topic, feed name, `routeBy`, and canonical route values.
That keeps feed identity stable, auditable, and independent from user callback bugs.

## Feed Key

Users should not manually return `feedKey` from `acquire`.

The framework should derive it from:

- View Server topic
- feed definition name
- routeBy field names
- canonical route values

Example:

```txt
topic: orders
feed: ordersByStrategyRegion
route: { strategyId: "s1", region: "usa" }

feedKey:
orders/ordersByStrategyRegion/region=usa/strategyId=s1
```

Canonicalization rules:

- stable field ordering from configured `routeBy`
- stable value encoding that preserves strings, numbers, bigint, and BigDecimal-like values
- no `JSON.stringify` on arbitrary user query objects as the authoritative key
- selected fields must not affect feed identity
- local-only filters must not affect feed identity
- order/group/aggregate/window must not affect feed identity

The feed key is internal but should appear in health and benchmark artifacts.

## Health

Health should expose gRPC separately from engine topic health.

Suggested shape:

```ts
grpc: {
  clients: {
    orders: {
      status: "connected" | "disconnected" | "degraded" | "starting";
      baseUrl: string;
      activeFeeds: number;
      lastConnectedAt: number | null;
      lastError: string | null;
    }
  }

  feeds: {
    orders: {
      materialized: Record<string, GrpcFeedHealth>;
      leased: Record<string, GrpcFeedHealth>;
    }
  }
}
```

Feed health should include:

```ts
type GrpcFeedHealth = {
  status: "starting" | "ready" | "degraded" | "stopping";
  lifecycle: "materialized" | "leased";
  feedName: string;
  feedKey: string;
  topic: string;
  subscriberCount: number;
  rowCount: number;
  messagesPerSecond: number;
  rowsPerSecond: number;
  decodeFailuresPerSecond: number;
  mappingFailuresPerSecond: number;
  publishFailuresPerSecond: number;
  reconnects: number;
  lastMessageAt: number | null;
  lastError: string | null;
};
```

`lastMessageAt` follows the runtime health convention from
`plans/v2-column-live-view-engine-plan.md`: health timestamps are numeric
runtime timestamps or `null`. Domain rows may still use `bigint` nanoseconds as
topic data, but health should not introduce a second timestamp encoding unless a
future health contract explicitly changes all runtime health timestamps together.

Health cadence rules from the v2 plan still apply:

- hot paths may update cheap counters
- do not rebuild full health per message
- `/health` returns a cached snapshot
- health updates should be around once per second by default

## Lifecycle And Resource Ownership

All gRPC streams must be scoped Effect resources.

Rules:

- materialized feeds are acquired when runtime starts and released on runtime shutdown
- leased feeds are acquired on first matching subscription
- leased feeds increment a lease count per active subscription/view
- leased feeds release when the last lease closes
- release closes the upstream stream and drops feed-owned rows/state
- parent runtime interruption must release all materialized and leased feeds
- stream defects must mark feed/client health degraded and cleanly release resources
- user-level subscription close must decrement the lease even if client disconnects mid-stream
- do not use detached fibers for long-lived stream ownership

Public callback shape:

```ts
acquire: ({ client, request, route, session }) => Stream.Stream<GrpcValue, GrpcError>;
release?: ({ client, request, route, session }) => Effect.Effect<void, GrpcError>;
```

Implementation should wrap user callbacks in named `Effect.fn` spans.

## Session And Auth

Session context must be available to gRPC feeds.

Use cases:

- forward selected browser/user headers to upstream gRPC
- attach service credentials for materialized feeds
- apply per-user auth for leased feeds
- support future session-owned feeds that must not be shared across users

Current slice:

- leased feeds are shared, system-scoped, and route-keyed
- `session.id` is `null`
- `session.forwardedHeaders` and `session.systemHeaders` are empty
- user/session-scoped auth forwarding is deferred until the server can pass an authenticated session into the lease manager

Future sharing modes:

- `shared`: route-keyed feed shared across all sessions
- `session`: route-keyed feed scoped to one session/user

Do not implement arbitrary auth policy in the engine. Auth belongs in runtime/server/gRPC Adapter code.

For `shared` leased feeds, be careful with forwarded user headers. If upstream results depend on user identity, the feed must be `session` scoped or include an explicit auth partition in the feed key.

## Runtime Integration

The runtime should compose:

```txt
runtime package
  -> runtime-core
  -> server/WebSocket adapter
  -> Kafka ingress adapter
  -> gRPC ingress adapter
```

The gRPC Adapter publishes rows into runtime-core exactly like Kafka does:

```txt
upstream gRPC event
  -> decode/generated type
  -> map to topic row
  -> schema validate
  -> runtime-core publish/publishMany
  -> engine mutation batch
  -> active query fanout
```

Leased feed row storage must be isolated per feed instance, while still using the same engine/query code path. The implementation can model this as an internal feed partition under a topic, but public queries must see only the rows for their resolved feed.

Do not merge multiple leased feed instances to satisfy one user query.

## Testing Strategy

Use Vitest and Effect tests according to repository rules.

Required type tests:

- materialized feed accepts valid topic and mapping output
- leased feed `routeBy` accepts only topic row fields
- leased feed `request` receives correctly typed route values
- `acquire` receives correctly typed ConnectRPC client and request
- `map` receives correctly typed stream value and route
- `map` rejects missing fields
- `map` rejects extra fields
- `useLiveQuery` rejects leased topic queries missing route fields
- `useLiveQuery` rejects non-eq route operators
- `useLiveQuery` accepts route fields plus additional local filters
- `useLiveQuery` return type remains based on select/aggregates, not route internals

Current materialized runtime/e2e tests:

- materialized feed starts on runtime startup with zero subscribers
- materialized feed serves snapshot to first subscriber without opening a new upstream stream
- materialized feed startup ignores leased feed definitions because the lease manager owns them
- materialized feed startup rejects duplicate topic ownership across Kafka and gRPC
- materialized feed startup rejects multiple gRPC owners for one View Server topic
- stream completion marks feed/client degraded and releases resources
- stream failure marks feed/client degraded and releases resources
- runtime shutdown releases all materialized gRPC streams
- health reports materialized feed keys, row counts, rates, and failures
- materialized benchmark exercises the production ingress path into runtime-core and the engine

Current leased manager runtime/e2e tests:

- leased feed does not open before first subscriber
- first leased subscriber opens one upstream stream
- second subscriber with same route reuses the same upstream stream
- subscriber with different route opens a second upstream stream
- extra local filters produce different views over the same leased feed
- last subscriber for a feed closes upstream and drops feed rows
- invalid leased query returns `InvalidQueryError`
- health reports leased feed keys, subscriber counts, row counts, and failures
- runtime shutdown releases all leased gRPC streams

Future leased tests:

- session-scoped feed does not share across users once session-scoped leases exist
- real ConnectRPC integration coverage if the tooling cost is acceptable

Tests should use a fake/in-process generated-compatible gRPC stream first so lifecycle, routing, and retained-row cleanup stay deterministic.

## Benchmarks And Gates

Use Vitest benchmark mode only.

Initial benchmark profiles:

- materialized startup seed latency
- materialized write throughput
- materialized filtered/sorted snapshot latency
- materialized grouped snapshot latency once grouped gRPC scenarios exist
- mapping/schema validation throughput
- health refresh overhead with many materialized feeds

Current materialized-feed benchmark command:

```sh
pnpm --filter @view-server/runtime run bench:grpc-materialized
```

This benchmark uses a Queue-backed in-process gRPC stream but still exercises the
production materialized ingress path:

```txt
Stream -> groupedWithin -> map -> runtime-core publishMany -> snapshot/readback -> health overlay
```

It records stream convergence, filtered/sorted snapshot latency, health overlay
latency, rows/sec, final health, and memory deltas. Keep it as the materialized
baseline.

Current leased-feed benchmark command:

```sh
pnpm --filter @view-server/runtime run bench:grpc-leased
```

This benchmark uses the production lease manager with Queue-backed in-process streams:

```txt
first matching subscription -> route validation -> lease acquire/reuse -> stream rows -> runtime-core publishMany -> snapshot/readback -> health overlay -> subscription close cleanup
```

Current leased benchmark profiles:

- leased first-subscriber acquisition latency
- leased same-route reuse latency
- leased local-filter live convergence over the configured rows per feed
- many routes with one subscriber each
- one route with many subscribers
- health refresh overhead with many active leased feeds

Future leased benchmark profiles:

- retained local-filter snapshot latency over 50k rows
- leased last-subscriber cleanup latency as an explicit case
- leased delta fanout latency for multiple subscribers over one feed
- write tax from feed partitioning

Add baseline automation only after repeated local runs are stable. Noisy max latency can remain report-only until stable.

## Acceptance Criteria

The current materialized gRPC slice is not complete until:

- public API has type tests for all new inference and rejection behavior
- package seam checks reject unapproved gRPC internals
- strict Effect LSP passes
- changed package tests pass with 100% coverage
- `vp check` passes
- focused runtime/config/protocol/client/server tests pass
- pre-existing `pnpm run pre-grpc:gate` still passes or is intentionally extended
- new gRPC e2e tests prove materialized behavior and that leased feeds are ignored by materialized ingress
- health shows materialized feed instances without rebuilding per message
- no long-lived stream uses detached/hand-rolled lifecycle
- no public API requires consumer `as const`
- no casts hide topic/route/request/value/map type erasure

The leased gRPC slice is not complete until:

- `useLiveQuery` type tests reject missing/non-eq route fields for leased topics
- decoded remote queries get the same route validation at runtime
- first subscription opens exactly one upstream stream for one feed key
- same-route subscribers share the upstream stream and retained feed state
- different-route subscribers open different upstream streams
- last subscriber closes the upstream stream and drops feed-owned rows
- health shows leased feed instances, subscriber counts, route/feed keys, row counts, and failures
- invalid leased queries never fall back to an unfiltered upstream stream
- all leased stream lifecycles are scoped and released on runtime shutdown
- a Vitest benchmark covers first subscriber, same-route reuse, local-filter snapshot, many routes, and health overlay
- leased feed row isolation is enforced by internal feed partitioning, not by trusting route fields on upstream rows
- runtime snapshot/read APIs cannot bypass the lease manager; the current slice rejects one-shot snapshots for leased topics

## Implementation Sequence

Implement in slices that keep `pnpm run ready`, strict Effect LSP, package seam checks, and package tests green after each PR:

1. Source contracts and type gates
   - Add `grpc.materialized()` and `grpc.leased({ routeBy })` topic source markers.
   - Add `grpcFeed.materializedFeed(...)` and `grpcFeed.leasedFeed(...)` runtime helpers.
   - Add type tests for topic names, route fields, request inference, acquire value inference, mapping output exactness, and invalid leased query predicates.

2. Runtime ownership validation
   - Reject Kafka + gRPC ownership conflicts.
   - Reject multiple gRPC owners per View Server topic.
   - Reject lifecycle mismatch between topic source marker and runtime feed definition.
   - Keep browser/react packages free from runtime/gRPC imports.

3. Materialized feed runtime
   - Acquire materialized streams at runtime startup.
   - Publish mapped rows through runtime-core.
   - Mark health degraded on stream completion/failure.
   - Release streams on runtime shutdown.
   - Add e2e tests and a Vitest benchmark.

4. Leased feed runtime
   - Route subscriptions by exact `eq` predicates for every `routeBy` field.
   - Acquire one upstream stream per feed key.
   - Reuse same-route feeds across subscribers.
   - Isolate retained rows per internal feed partition.
   - Release/drop feed state when the last subscriber closes.
   - Reject one-shot snapshots/reads for leased topics in this slice.
   - Add e2e tests and a Vitest benchmark.

5. Health and lifecycle hardening
   - Expose gRPC client/feed health separately from engine topic health.
   - Keep health cached/coalesced.
   - Handle stream defects with degraded health and deterministic cleanup.
   - Avoid detached fibers for stream ownership.

6. Review and benchmark gate
   - Run focused package tests, `pnpm run ready`, strict Effect LSP, package seam checks, forbidden-pattern scans, and gRPC benchmarks.
   - Only add CI regression thresholds after repeated benchmark runs are stable.

## Deferred Decisions

Do not implement these in the first gRPC slice unless needed:

- full public config migration to `topics: { orders: kafka.topic(...), liveOrders: grpc.leasedTopic(...) }`
- generic non-gRPC stream-source API
- multi-source topics
- merging multiple leased feed instances for one user query
- arbitrary access-path priority selection
- custom live-event transport replacing Effect RPC WebSocket
- persistent leased feed cache after last subscriber disconnects
- WAL/checkpointing for materialized gRPC feeds
