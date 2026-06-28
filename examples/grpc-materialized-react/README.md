# Materialized gRPC React Example

TanStack Start app backed by a startup materialized gRPC feed.

Run:

```bash
vp run @view-server/example-grpc-materialized-react#runtime
vp run @view-server/example-grpc-materialized-react#dev
```

This example demonstrates:

- `grpc.materialized()` source ownership.
- Runtime startup stream acquisition.
- React querying an already-retained View Server topic.
- Health summary for runtime/source status.
