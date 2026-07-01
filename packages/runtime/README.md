# @effect-view-server/runtime

Production runtime composition for View Server.

This package wires Runtime Core, Effect RPC WebSocket transport, `GET /health`,
`GET /metrics`, optional Kafka/gRPC ingestion, and optional TCP publish ingress.
The in-memory engine remains the single mutation path; Kafka, gRPC, TCP, and
tests publish into the same runtime core.

## Entrypoint

Node entrypoints should use `NodeRuntime.runMain` so process signals interrupt the
main fiber and run Effect finalizers.

```ts
import { NodeRuntime } from "@effect/platform-node";
import { runViewServerRuntime } from "@effect-view-server/runtime";
import { viewServer } from "./view-server-config";

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    host: "0.0.0.0",
    websocketPort: 8080,
    tcpPublishHost: "127.0.0.1",
    tcpPublishPort: 8081,
  }),
);
```

`runViewServerRuntime` logs the WebSocket, health, metrics, and TCP publish URLs
when those endpoints are configured, then keeps the server alive until the main
fiber is interrupted.

## Health

The runtime exposes a same-server `GET /health` endpoint for deployment
readiness checks. It returns `200` when the runtime status is `ready` and a
non-`200` status when the runtime is starting, degraded, or stopping.

The JSON response is the current runtime health snapshot. Internal `bigint`
fields, such as Kafka lag, are encoded as decimal strings.

React applications should use the pushed health hooks from `@effect-view-server/react`;
`GET /health` is for infrastructure and smoke checks, not UI polling.

## Metrics

The runtime exposes a same-server `GET /metrics` endpoint for Prometheus-style
scrapes. The response uses `text/plain; version=0.0.4; charset=utf-8` and is
derived from the same cached runtime health snapshot as `GET /health`.

Metrics include runtime status/version/uptime, transport pressure, engine topic
rows/versions/queues/backpressure, Kafka region lag and failure rates, and gRPC
client/feed counters. It intentionally keeps labels low-cardinality: raw error
messages, timestamps, committed offsets, and route-specific leased feed keys
remain available from `GET /health` instead of Prometheus labels. If health
cannot be read or decoded, the endpoint returns `200` with
`view_server_metrics_error 1` so the scrape result is still visible to the
metrics system.

`metricsPath` can override the default `/metrics` path:

```ts
NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    websocketPort: 8080,
    metricsPath: "/view-server/metrics",
  }),
);
```

## Kafka Ingestion

Kafka is optional. Prefer topic-owned `kafkaSource` definitions in the View
Server config. Runtime options then provide only operational values such as the
consumer group and optional start position:

```ts
import { Config } from "effect";
import { NodeRuntime } from "@effect/platform-node";
import { defineViewServerConfig, kafka } from "@effect-view-server/config";
import { runViewServerRuntime } from "@effect-view-server/runtime";
import { KafkaTrade, Order, Trade } from "./schemas";
import { OrdersValueSchema } from "./generated/orders";

const kafkaRegions = {
  usa: Config.string("KAFKA_USA_BOOTSTRAP"),
  london: Config.string("KAFKA_LONDON_BOOTSTRAP"),
};

const viewServer = defineViewServerConfig({
  kafka: kafkaRegions,
  topics: {
    orders: {
      schema: Order,
      key: "id",
      kafkaSource: kafka.source({
        topic: "sourceOrdersUsa",
        regions: ["usa"],
        value: kafka.protobuf(OrdersValueSchema),
        key: kafka.stringKey(),
        map: ({ value, region, rowKey }) => ({
          id: rowKey,
          customerId: value.customerId,
          status: value.status,
          price: value.price,
          region,
          updatedAt: value.updatedAt,
        }),
      }),
    },
    trades: {
      schema: Trade,
      key: "id",
      kafkaSource: kafka.source({
        topic: "sourceTradesLondon",
        regions: ["london"],
        value: kafka.json(KafkaTrade),
        key: kafka.stringKey(),
        map: ({ value, region, rowKey }) => ({
          id: rowKey,
          symbol: value.symbol,
          side: value.side,
          quantity: value.quantity,
          region,
          updatedAt: value.updatedAt,
        }),
      }),
    },
  },
});

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "orders-view-server",
      startFrom: "latest",
    },
  }),
);
```

Startup fails through Effect `Config` if required environment-backed Kafka
broker values are missing. Do not silently default brokers or production
secrets.

The older `viewServer.kafkaTopic()` + `runtime.kafka.topics` shape remains
available for admin-owned/manual source wiring. Do not mix it with topic-owned
`kafkaSource` definitions in the same runtime.

## Kafka Delivery Contract

During a live process, Kafka messages are decoded, mapped, grouped into small
microbatches, and published into Runtime Core with `publishMany`.

Offsets are committed only after the corresponding Runtime Core publish
succeeds. If a publish fails, the original Kafka messages remain uncommitted so
Kafka can replay them.

If a later message in a batch fails decode or mapping after earlier messages
were decoded, the decoded prefix is published and committed before the failing
message is reported. If the Kafka stream fails after yielding messages, the
yielded batch is flushed before Kafka health marks the region disconnected and
overall runtime health becomes degraded.

## Start Position And Restart Semantics

`startFrom` controls where Kafka consumers begin:

- `"earliest"` replays from the beginning for the configured consumer group.
- `"latest"` starts from the latest offsets for the configured consumer group.
- `{ committedConsumerGroup, fallback }` resumes committed offsets for an
  existing group and uses `fallback` when no committed offsets exist. `fallback`
  can be `"earliest"`, `"latest"`, or `"fail"`.

The default is `{ committedConsumerGroup: consumerGroupId, fallback: "earliest" }`.

`startFrom` is a runtime-level policy for the Kafka consumers in one View Server
runtime. If one source must replay from `"earliest"` and another must tail from
`"latest"`, run separate runtime instances with separate consumer groups. Do not
model that as one runtime until per-source Kafka seek policy exists.

```ts
NodeRuntime.runMain(
  runViewServerRuntime(rebuildViewServer, {
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "orders-view-server-rebuild",
      startFrom: "earliest",
    },
  }),
);

NodeRuntime.runMain(
  runViewServerRuntime(liveTailViewServer, {
    websocketPort: 8081,
    kafka: {
      consumerGroupId: "orders-view-server-live-tail",
      startFrom: "latest",
    },
  }),
);
```

Important: committed consumer-group resume is not durable View Server recovery
by itself. Runtime Core rows are in memory and this package does not yet provide
a WAL or checkpoint. If the process dies after committing Kafka offsets, a
restart from committed offsets can skip rows that existed only in memory.

Deployments that need rebuild-after-restart semantics must replay Kafka from an
authoritative position, such as `startFrom: "earliest"` or a fresh/reset
dedicated rebuild consumer group, until durable checkpoints are added.

## gRPC Ingestion

gRPC sources are also declared on the topic that owns the rows. Runtime options
then provide the concrete ConnectRPC clients and feed implementations:

```ts
import { defineViewServerConfig, grpc } from "@effect-view-server/config";
import { runViewServerRuntime } from "@effect-view-server/runtime";
import { NodeRuntime } from "@effect/platform-node";
import { Stream } from "effect";
import { Order, Strategy } from "./schemas";
import { ordersService, strategiesService } from "./generated/grpc";

const viewServer = defineViewServerConfig({
  grpc: {
    clients: {
      orders: grpc.connectClient({
        service: ordersService,
        baseUrl: "https://orders-grpc.example.com",
      }),
      strategies: grpc.connectClient({
        service: strategiesService,
        baseUrl: "https://strategies-grpc.example.com",
      }),
    },
  },
  topics: {
    orders: {
      schema: Order,
      key: "id",
      grpcSource: grpc.leased({ routeBy: ["strategyId", "region"] }),
    },
    strategies: {
      schema: Strategy,
      key: "id",
      grpcSource: grpc.materialized(),
    },
  },
});

const grpcFeed = viewServer.grpcFeed();

const ordersByStrategy = grpcFeed.leasedFeed({
  topic: "orders",
  client: "orders",
  method: "streamOrders",
  routeBy: ["strategyId", "region"],
  request: ({ strategyId, region }) => ({ strategyId, region }),
  acquire: ({ client, request }) =>
    Stream.fromAsyncIterable(client.streamOrders(request), (cause) => cause),
  map: ({ value, route }) => ({
    id: `${route.strategyId}:${route.region}:${value.orderId}`,
    strategyId: route.strategyId,
    region: route.region,
    customerId: value.customerId,
    status: value.status,
    price: value.price,
    updatedAt: value.updatedAt,
  }),
});

const strategies = grpcFeed.materializedFeed({
  topic: "strategies",
  client: "strategies",
  method: "streamStrategies",
  request: () => ({ universe: "global" }),
  acquire: ({ client, request }) =>
    Stream.fromAsyncIterable(client.streamStrategies(request), (cause) => cause),
  map: ({ value }) => ({
    id: `${value.strategyId}:${value.region}`,
    strategyId: value.strategyId,
    region: value.region,
    status: value.status,
    notional: value.notional,
    updatedAt: value.updatedAt,
  }),
});

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    websocketPort: 8080,
    grpc: {
      feeds: { ordersByStrategy, strategies },
    },
  }),
);
```

Leased gRPC topics require the `routeBy` fields in user queries, so the runtime
can open exactly one shared upstream stream per route key instead of accidentally
materializing an unbounded upstream dataset.

## TCP Publish Ingress

`tcpPublishPort` enables a non-browser publisher ingress for systems that need a
small push path without Kafka or gRPC. The runtime returns `tcpPublishUrl` when
the endpoint is configured.

TCP publish has its own `tcpPublishHost` and defaults to `127.0.0.1`. It does
not inherit the public WebSocket/HTTP `host`, so binding the runtime server to
`0.0.0.0` does not accidentally expose a mutation port. Bind TCP publish to a
different interface only behind your own network controls.

The protocol is NDJSON over TCP: one JSON command per line and one JSON response
per line.

Supported commands:

```json
{ "op": "publish", "topic": "orders", "row": { "id": "o1", "price": 10 } }
{ "op": "publishMany", "topic": "orders", "rows": [{ "id": "o1", "price": 10 }] }
{ "op": "patch", "topic": "orders", "key": "o1", "patch": { "price": 20 } }
{ "op": "delete", "topic": "orders", "key": "o1" }
```

Responses:

```json
{ "ok": true }
{ "ok": false, "error": { "_tag": "ViewServerTcpPublishIngressError", "phase": "decode", "message": "..." } }
```

Valid TCP publish mutations use the same runtime-core mutation methods as Kafka,
gRPC, and in-memory tests. Invalid TCP row or patch payloads fail at the TCP
decode boundary before mutation, so invalid batches do not partially publish.

The endpoint is bounded: excessive connections, oversized lines, and excessive
queued commands return typed `ViewServerTcpPublishIngressError` responses and
close or reject the offending socket. TCP publish also refuses Kafka/gRPC-owned
View Server topics; use it only for topics whose source of truth is the TCP
publisher.

## Current Consumer Group Assumption

The current runtime starts one consumer per configured region that has at least
one Source Topic. The actual group used is the normalized consumer group from
`startFrom`: `consumerGroupId` for `"earliest"` / `"latest"`, or
`committedConsumerGroup` for committed resume. Runtime health records that
normalized group, assignments, and lag for the current process, but full
rebalance/revoke handoff and checkpoint handoff are not implemented yet.

## Cleanup

Runtime finalizers stop Kafka consumers, close the WebSocket server, and close
Runtime Core resources. Use Effect runtime ownership (`NodeRuntime.runMain`,
scopes, and finalizers) rather than process-level ad-hoc cleanup.
