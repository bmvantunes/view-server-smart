# view-server-smart

## Guides

- [Public API](./docs/public-api.md)
- [Runtime Config](./docs/runtime-config.md)
- [Kafka Mapping](./docs/kafka-mapping.md)
- [In-Memory Browser Testing](./docs/in-memory-browser-testing.md)
- [Health And Metrics](./docs/health-and-metrics.md)
- [Query Semantics](./docs/query-semantics.md)
- [Benchmarks And Capacity](./docs/benchmarks-and-capacity.md)
- [Deployment](./docs/deployment.md)
- [Examples](./examples/README.md)

## Remote React provider

Server code starts a runtime through Effect RPC WebSocket plus same-server
`GET /health` and `GET /metrics` endpoints:

Node entrypoints should use `@effect/platform-node`'s `NodeRuntime.runMain` so
`SIGINT` and `SIGTERM` interrupt the main fiber and run Effect finalizers.

```ts
import { NodeRuntime } from "@effect/platform-node";
import { runViewServerRuntime } from "@view-server/runtime";
import { viewServer } from "./view-server-config";

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    host: "127.0.0.1",
    websocketPort: 8080,
    tcpPublishPort: 8081,
  }),
);
```

The same-server `GET /health` endpoint serves the cached runtime health snapshot
for deployment readiness checks. Internal `bigint` health fields, such as Kafka
lag, are encoded as decimal strings in the JSON response.

When `tcpPublishPort` is configured, the runtime also opens a non-browser TCP
NDJSON publish endpoint and exposes its `tcpPublishUrl`. That endpoint supports
`publish`, `publishMany`, `patch`, and `delete` commands and routes every
mutation through the same Runtime Core path as Kafka, gRPC, and in-memory tests.
TCP publish is for externally-published topics only; Kafka/gRPC-owned topics are
rejected so one View Server topic has one source of truth. TCP publish has its
own `tcpPublishHost` and defaults to `127.0.0.1`; it does not inherit the public
WebSocket/HTTP host. The TCP endpoint is bounded by connection, line-size, and
queued-command limits.

The same-server `GET /metrics` endpoint serves Prometheus text exposition derived
from the same cached health snapshot. It exposes scrape-safe runtime, transport,
engine, Kafka, and gRPC gauges/counters. It is not a full mirror of health:
high-cardinality values such as raw error messages and route-specific leased
feed keys remain in `GET /health`. Scrape failures that cannot decode health return `200` with
`view_server_metrics_error 1` so the scrape itself remains observable.

Browser React code keeps using the normal provider and hooks:

```tsx
import { createViewServerReact } from "@view-server/react";
import { viewServer } from "./view-server-config";

const react = createViewServerReact(viewServer);

export function App() {
  return (
    <react.ViewServerProvider url={window.__APP_CONFIG__.VIEW_SERVER_URL}>
      <Orders />
    </react.ViewServerProvider>
  );
}

function Orders() {
  const orders = react.useLiveQuery("orders", {
    select: ["id", "price"],
    orderBy: [{ field: "price", direction: "asc" }],
    limit: 20,
  });

  return <pre>{JSON.stringify(orders.rows, null, 2)}</pre>;
}
```
