# Runtime Config

## Entrypoint

Node entrypoints should use `NodeRuntime.runMain` from `@effect/platform-node`.
That lets process signals interrupt the main Effect fiber and run finalizers.

```ts
import { NodeRuntime } from "@effect/platform-node";
import { runViewServerRuntime } from "effect-view-server/runtime";
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

`runViewServerRuntime` logs configured WebSocket, health, metrics, and TCP
publish URLs and keeps the runtime alive until interrupted.

## Options

- `host`: host for the WebSocket, health, and metrics HTTP server.
- `websocketPort`: runtime server port. Set this explicitly in production;
  omitting it uses an ephemeral port.
- `healthPath`: health endpoint path. The default is `/health`.
- `metricsPath`: metrics endpoint path. The default is `/metrics`.
- `tcpPublishHost`: host for optional TCP publish ingress. Defaults to
  `127.0.0.1`.
- `tcpPublishPort`: enables optional TCP publish ingress.
- `tcpPublishMaxConnections`: bounds TCP publisher connections.
- `kafka`: optional Kafka source configuration.
- `grpc`: optional gRPC source configuration.

Environment-backed values should use Effect `Config`. Missing required brokers,
URLs, or secrets should fail startup rather than silently defaulting.

```ts
import { Config } from "effect";

const kafkaRegions = {
  usa: Config.string("KAFKA_USA_BOOTSTRAP"),
  london: Config.string("KAFKA_LONDON_BOOTSTRAP"),
};
```

Kafka regions usually live on `defineViewServerConfig`, while runtime options
provide the deployment consumer group and start policy:

```ts
runViewServerRuntime(viewServer, {
  websocketPort: 8080,
  kafka: {
    consumerGroupId: "orders-view-server-prod",
    startFrom: "latest",
  },
});
```

`startFrom` applies to all Kafka source topics in one runtime instance. If a
deployment needs one topic replayed from `"earliest"` and another tailed from
`"latest"`, split them into separate View Server runtime instances with
separate consumer groups.

```ts
runViewServerRuntime(rebuildViewServer, {
  websocketPort: 8080,
  kafka: {
    consumerGroupId: "orders-view-server-rebuild",
    startFrom: "earliest",
  },
});

runViewServerRuntime(liveTailViewServer, {
  websocketPort: 8081,
  kafka: {
    consumerGroupId: "orders-view-server-live-tail",
    startFrom: "latest",
  },
});
```

gRPC clients can also live on `defineViewServerConfig`, while runtime options
provide the feed implementations:

```ts
runViewServerRuntime(viewServer, {
  websocketPort: 8080,
  grpc: {
    feeds: {
      ordersByStrategy,
      strategies,
    },
  },
});
```

## Source Ownership

Each View Server topic has one source of truth. Kafka-owned and gRPC-owned topics
cannot also be mutated through TCP publish. This prevents two independent
ingress paths from racing into the same in-memory topic.

## Runtime Ownership

The production runtime owns:

- Runtime Core and the in-memory engine.
- Effect RPC WebSocket server.
- Same-server `GET /health` and `GET /metrics`.
- Optional Kafka consumers.
- Optional materialized and leased gRPC feeds.
- Optional TCP publish ingress.

React production code owns only the provider URL. In-memory browser tests use
the testing provider described in [In-Memory Browser Testing](./in-memory-browser-testing.md).
