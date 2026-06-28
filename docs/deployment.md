# Deployment

## Runtime Process

Run View Server as a Node process using `NodeRuntime.runMain`:

```ts
import { NodeRuntime } from "@effect/platform-node";
import { runViewServerRuntime } from "@view-server/runtime";
import { viewServer } from "./view-server-config";

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    host: "0.0.0.0",
    websocketPort: 8080,
    healthPath: "/health",
    metricsPath: "/metrics",
  }),
);
```

The runtime handles process signal interruption through Effect finalizers. Do
not add separate process-level cleanup unless it is outside View Server
ownership.

## Kubernetes

Use `GET /health` for readiness and startup checks. The endpoint returns `200`
only when the runtime is ready, and returns a non-`200` status while the runtime
is starting, degraded, or stopping. Do not use it as liveness unless degraded
source/runtime health should restart the only active runtime and rebuild
in-memory state. Prefer a process-level or TCP liveness check until a separate
liveness endpoint exists. If runtime auth is enabled, readiness probes must be
accepted by `auth.validateRequest` or auth must whitelist the health path;
otherwise probes can receive auth failures instead of runtime health. Use
`GET /metrics` for Prometheus scraping.

Kafka and gRPC credentials, broker addresses, and base URLs should be loaded
through Effect `Config`. Missing required values should fail startup.

The current supported deployment model is one active View Server runtime per
logical deployment. Give each deployment a unique Kafka consumer group id. Kafka
multi-replica rebalance/revoke handoff is intentionally out of scope for the
current milestone.

## Network Surface

- Browser clients connect through Effect RPC WebSocket with NDJSON.
- `GET /health` and `GET /metrics` share the runtime HTTP server.
- TCP publish is optional and binds separately through `tcpPublishHost` and
  `tcpPublishPort`.

Keep TCP publish on a private interface unless protected by your own network
controls. TCP publish is a mutation ingress, not a browser API.

## Recovery

Current Runtime Core state is in memory. There is no durable WAL/checkpoint.
Deployments that need full rebuild after restart must replay source data from an
authoritative source:

- Kafka: use `startFrom: "earliest"` or a dedicated rebuild consumer group.
- Materialized gRPC: reconnect and replay from the upstream stream contract.
- Leased gRPC: rebuilt on demand from active subscriptions.
- TCP publish: external publisher must be authoritative if replay is required.

## Release Gate

Before promoting a runtime build, run:

```sh
vp run -w release-candidate:capacity
```

The gate covers examples, build, package seam checks, strict Effect diagnostics,
tests, coverage, and benchmark baseline profiles. See
[Operations](./operations.md) for Prometheus, probe, and resource guidance.
