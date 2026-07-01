# Production Deployment Example

This example is a deployment recipe, not a runnable TanStack app. The runnable
source-specific examples live beside it. Use this when wiring a real production
service with:

- one View Server runtime instance
- one unique Kafka consumer group per deployment
- runtime configuration from Effect `Config`
- browser URL injection at deploy time
- Kubernetes health probes
- Prometheus metrics scraping

## Runtime Entrypoint

```ts
import { NodeRuntime } from "@effect/platform-node";
import { runViewServerRuntime } from "effect-view-server/runtime";
import { Config, Effect } from "effect";
import { viewServer, grpcClients, grpcFeeds } from "./view-server.config";

const program = Effect.gen(function* () {
  const websocketPort = yield* Config.number("VIEW_SERVER_WEBSOCKET_PORT");
  const kafkaConsumerGroupId = yield* Config.string("VIEW_SERVER_KAFKA_GROUP_ID");

  return yield* runViewServerRuntime(viewServer, {
    host: "0.0.0.0",
    websocketPort,
    healthPath: "/health",
    metricsPath: "/metrics",
    kafka: {
      consumerGroupId: kafkaConsumerGroupId,
      startFrom: "latest",
    },
    grpc: {
      clients: grpcClients,
      feeds: grpcFeeds,
    },
  });
});

NodeRuntime.runMain(program);
```

Kafka regions and source mappings should be declared on `viewServer` with
topic-owned `kafkaSource` definitions; runtime options only provide the
deployment consumer group and start policy.

Use a deployment-unique `VIEW_SERVER_KAFKA_GROUP_ID`. The current supported
deployment model is one active View Server runtime for a logical deployment.
Multi-replica Kafka rebalance/revoke handoff is intentionally out of scope.

One runtime instance has one Kafka start policy. If one Kafka-backed topic must
replay from `"earliest"` and another must tail from `"latest"`, deploy two View
Server runtime instances with separate configs and consumer groups. Do not try
to mix start positions inside one runtime.

## React Entrypoint

React code should receive the runtime URL from deploy-time configuration, not
from build-time environment variables.

```tsx
import { ViewServerProvider } from "./view-server.config";

declare global {
  interface Window {
    readonly __APP_CONFIG__: {
      readonly VIEW_SERVER_URL: string;
    };
  }
}

export function AppRoot() {
  return (
    <ViewServerProvider url={window.__APP_CONFIG__.VIEW_SERVER_URL}>
      <App />
    </ViewServerProvider>
  );
}
```

Serve a small config script before the app bundle:

```html
<script>
  window.__APP_CONFIG__ = {
    VIEW_SERVER_URL: "wss://view-server.example.com/rpc",
  };
</script>
```

The same app components can be tested in Vitest browser mode by wrapping them in
`createInMemoryViewServerReact(...).ViewServerInMemoryProvider`.

## Kubernetes Sketch

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: view-server
spec:
  replicas: 1
  selector:
    matchLabels:
      app: view-server
  template:
    metadata:
      labels:
        app: view-server
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: "/metrics"
    spec:
      containers:
        - name: view-server
          image: registry.example.com/view-server:VERSION
          ports:
            - name: websocket
              containerPort: 8080
          env:
            - name: VIEW_SERVER_WEBSOCKET_PORT
              value: "8080"
            - name: VIEW_SERVER_KAFKA_GROUP_ID
              value: "view-server-prod-orders-v1"
            - name: KAFKA_USA_BOOTSTRAP
              valueFrom:
                secretKeyRef:
                  name: view-server-kafka
                  key: usa-bootstrap
            - name: KAFKA_LONDON_BOOTSTRAP
              valueFrom:
                secretKeyRef:
                  name: view-server-kafka
                  key: london-bootstrap
            - name: ORDERS_GRPC_URL
              valueFrom:
                secretKeyRef:
                  name: view-server-grpc
                  key: orders-url
            - name: STRATEGIES_GRPC_URL
              valueFrom:
                secretKeyRef:
                  name: view-server-grpc
                  key: strategies-url
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
          resources:
            requests:
              cpu: "1"
              memory: 1Gi
            limits:
              cpu: "4"
              memory: 4Gi
```

Size CPU and memory from `vp run -w release-candidate:capacity` on a
production-like machine. Do not copy the resource values above without testing
your topic count, row count, grouped queries, Kafka rate, gRPC routes, and
WebSocket fanout.

`GET /health` is a readiness/startup probe: it returns a non-`200` status while
the runtime is starting, degraded, or stopping. Use a process-level or TCP
liveness check unless you intentionally want recoverable source degradation to
restart the pod and rebuild in-memory state.

## Release Candidate Gate

Before promoting a runtime image:

```sh
vp run -w release-candidate:capacity
```

This runs examples, builds, readiness checks, pre-gRPC benchmark gates, gRPC
benchmark gates, and the broad no-compare release benchmark profile serially.
