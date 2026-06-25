# View Server gRPC Ingress Plan

This plan defines the gRPC ingress model for View Server.

It intentionally keeps the existing architecture intact:

- one View Server topic is one logical table
- `ColumnLiveViewEngine` remains the only query/snapshot/delta engine
- runtime-core and the engine stay transport/ingress-neutral
- Kafka, gRPC, TCP, and future sources are ingress Adapters only
- React hooks continue to use the same provider/client seam
- Effect RPC WebSocket with NDJSON remains the production browser transport

## Compatibility With The Column Live View Engine Plan

This plan does not conflict with `plans/v2-column-live-view-engine-plan.md`.

The existing plan says:

- runtime config owns Kafka, TCP publishing, gRPC publishing, and deploy-time/server-only wiring
- browser bundles must not import runtime config
- the engine package must not depend on Kafka, WebSocket, React, TCP, gRPC, or server runtime code
- the same authoritative in-memory Topic Store serves snapshots, deltas, counts, grouped views, and subscription change streams
- only transport/ingress Adapters differ between production and in-memory/test paths

The gRPC design follows those rules by making gRPC an ingress Adapter that publishes rows into the existing runtime-core and engine. It must not create a second query engine, a second subscription system, or a gRPC-specific React hook.

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

Do not call leased feed instances "topics" in public APIs. Health can expose feed instances under a topic, but the public topic remains stable.

## Source Lifecycle Modes

### Materialized Feed

A materialized feed is always-on.

Behavior:

- starts when the runtime starts
- reconnects according to runtime policy
- retains state even with zero subscribers
- serves snapshots immediately when a user subscribes
- behaves similarly to Kafka materialized topics

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

Behavior:

- does not connect on runtime startup
- requires all `routeBy` fields in the user query
- opens one upstream gRPC stream per distinct `feedKey`
- shares that upstream stream across all users with the same route
- applies remaining user filters/order/grouping locally inside View Server
- closes the upstream stream and drops retained rows for that feed when the last subscription releases it

Use for huge upstream sources where an unfiltered stream is impossible or dangerous.

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

  acquire: ({ client, request, session }) =>
    Stream.fromAsyncIterable(
      client.streamOrders(request, {
        headers: session.forwardedHeaders,
      }),
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

## Local Query Semantics

The full user query still runs locally in the View Server engine.

For a leased `orders` feed routed by `strategyId` and `region`:

- `strategyId` and `region` select the upstream feed
- all rows from that upstream feed are retained in memory for that feed
- extra predicates such as `status`, `instrumentId`, `price`, and text filters are local engine filters
- order, projection, grouped aggregation, pagination/windowing, counts, and deltas are local engine work

This avoids creating one upstream stream per full UI query and avoids merging multiple upstream streams.

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

        acquire: ({ client, request, session }) =>
          Stream.fromAsyncIterable(
            client.streamOrders(request, {
              headers: session.forwardedHeaders,
            }),
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

Allowed:

```txt
orders -> Kafka materialized source
strategies -> gRPC materialized feed
ordersByStrategy -> gRPC leased feed
```

Rejected:

```txt
orders <- Kafka source
orders <- gRPC materialized feed
orders <- gRPC leased feed
```

The current public config shape can remain for now, but runtime/config validation must reject multiple ingress owners targeting the same View Server topic unless a future explicit multi-source design exists.

A later API cleanup can reshape the config toward:

```ts
topics: {
  orders: kafka.topic(...),
  liveOrders: grpc.leasedTopic(...),
  strategies: grpc.materializedTopic(...),
}
```

Do not block the first gRPC implementation on that larger public API migration.

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
  lastMessageAt: bigint | null;
  lastError: string | null;
};
```

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

Initial sharing modes:

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

Required runtime/e2e tests:

- materialized feed starts on runtime startup with zero subscribers
- materialized feed serves snapshot to first subscriber without opening a new upstream stream
- leased feed does not open before first subscriber
- first leased subscriber opens one upstream stream
- second subscriber with same route reuses the same upstream stream
- subscriber with different route opens a second upstream stream
- extra local filters produce different views over the same leased feed
- last subscriber for a feed closes upstream and drops feed rows
- session-scoped feed does not share across users
- stream failure marks feed/client degraded and releases resources
- runtime shutdown releases all gRPC streams
- invalid leased query returns `InvalidQueryError`
- health reports active feed keys, subscriber counts, row counts, and failures

Tests should use a fake/in-process generated-compatible gRPC stream where possible first, then add a real ConnectRPC integration test if the tooling cost is acceptable.

## Benchmarks And Gates

Use Vitest benchmark mode only.

Initial benchmark profiles:

- materialized startup seed latency
- leased first-subscriber acquisition latency
- leased same-route reuse latency
- leased last-subscriber cleanup latency
- leased local-filter snapshot latency over 50k rows
- leased delta fanout latency for multiple subscribers over one feed
- many routes with one subscriber each
- one route with many subscribers
- mapping/schema validation throughput
- write tax from feed partitioning
- health refresh overhead with many active feeds

Add baseline automation only after repeated local runs are stable. Noisy max latency can remain report-only until stable.

## Acceptance Criteria

The gRPC slice is not complete until:

- public API has type tests for all new inference and rejection behavior
- package seam checks reject unapproved gRPC internals
- strict Effect LSP passes
- changed package tests pass with 100% coverage
- `vp check` passes
- focused runtime/config/protocol/client/server tests pass
- pre-existing `pnpm run pre-grpc:gate` still passes or is intentionally extended
- new gRPC e2e tests prove materialized and leased behavior
- health shows materialized and leased feed instances without rebuilding per message
- no long-lived stream uses detached/hand-rolled lifecycle
- no public API requires consumer `as const`
- no casts hide topic/route/request/value/map type erasure

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
