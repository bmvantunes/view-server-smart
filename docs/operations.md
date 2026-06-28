# Operations

## Runtime Topology

The current supported production topology is one active View Server runtime per
logical deployment. Configure a unique Kafka consumer group id for that runtime.
Do not run multiple active replicas in the same logical consumer group unless a
future rebalance/revoke handoff contract is added.

Use Kubernetes rolling updates carefully: the replacement pod should become
ready only after its sources are connected and runtime health is ready.

## Prometheus Metrics

Scrape `GET /metrics` on the same HTTP server as WebSocket RPC. Metrics use
low-cardinality labels and are derived from cached runtime health.

Useful alert/query examples:

```promql
view_server_runtime_status{status!="ready"} == 1
```

```promql
max(view_server_kafka_consumer_lag_messages)
```

```promql
increase(view_server_transport_backpressure_events[5m]) > 0
```

```promql
increase(view_server_engine_topic_backpressure_events[5m]) > 0
```

```promql
max(view_server_transport_active_subscriptions)
```

```promql
max_over_time(view_server_grpc_feed_decode_failures_per_second[5m]) > 0
```

```promql
max_over_time(view_server_grpc_feed_mapping_failures_per_second[5m]) > 0
```

```promql
max_over_time(view_server_grpc_feed_publish_failures_per_second[5m]) > 0
```

Metric names can grow as the health contract grows. Treat these examples as
starting points and validate names against the current `/metrics` output.

## Kubernetes Probes

Use `GET /health` for readiness and startup checks. It returns `200` only when
the runtime is ready, and returns a non-`200` status while the runtime is
starting, degraded, or stopping.

```yaml
readinessProbe:
  httpGet:
    path: /health
    port: websocket
  periodSeconds: 5
  failureThreshold: 3
livenessProbe:
  tcpSocket:
    port: websocket
  periodSeconds: 10
  failureThreshold: 6
```

Do not use `GET /health` as liveness unless degraded source/runtime health
should restart the only active pod and rebuild in-memory state. Prefer a
process-level or TCP liveness check until a separate liveness endpoint exists.

If runtime auth is configured, either allow readiness probe requests in
`auth.validateRequest` or configure readiness probes to send accepted
credentials.

## Resource Sizing

Do not size View Server from row count alone. Capacity depends on:

- total rows per topic
- number of active topics
- number of active raw and grouped queries
- active browser clients
- subscriptions per client
- Kafka input rate
- gRPC leased route count
- WebSocket fanout shape
- selected fields and grouped aggregate width

Start with conservative memory limits and run:

```sh
vp run -w release-candidate:capacity
```

Then run a production-like soak using your real topic shapes. Watch RSS, heap,
event loop delay, Kafka lag, gRPC reconnects, WebSocket queue depth, and
backpressure metrics.

## TCP Publish

TCP publish is a private mutation ingress. Bind it to `127.0.0.1` or a private
network interface unless external network controls protect it. TCP publish is
schema-safe, but it is still a write path.

Do not use TCP publish for Kafka-owned or gRPC-owned topics. One View Server
topic must have one source of truth.

## Failure Triage

- Runtime not ready: inspect `/health` first.
- Kafka lag increasing: inspect Kafka region health and broker/topic health.
- Backpressure increasing: reduce subscription fanout, inspect slow clients, or
  raise queue limits only after measuring memory.
- gRPC leased routes retained longer than expected: inspect active
  subscriptions and leased feed health.
- Metrics scrape returns `view_server_metrics_error 1`: inspect `/health`
  encoding/decoding errors.
