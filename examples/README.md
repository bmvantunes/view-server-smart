# View Server Examples

All examples are generated TanStack Start React apps, then adapted to consume the
public View Server packages. They are intentionally small: each one isolates one
runtime/source shape while keeping the React code close to normal application
code.

| Example                                                | Source                       | Runtime              | Purpose                                                                        |
| ------------------------------------------------------ | ---------------------------- | -------------------- | ------------------------------------------------------------------------------ |
| [`in-memory-react`](./in-memory-react)                 | in-memory client             | browser/test runtime | Fastest way to test components with the real runtime core and engine.          |
| [`kafka-react`](./kafka-react)                         | Kafka                        | server runtime       | Hot/eager Apache Kafka source feeding React over WebSocket live queries.       |
| [`grpc-leased-react`](./grpc-leased-react)             | gRPC leased                  | server runtime       | On-demand shared routes with required route filters.                           |
| [`grpc-materialized-react`](./grpc-materialized-react) | gRPC materialized            | server runtime       | Startup materialized stream retained as a View Server topic.                   |
| [`combined-sources-react`](./combined-sources-react)   | Kafka + gRPC                 | server runtime       | Production-shaped app with Kafka, leased gRPC, and materialized gRPC together. |
| [`tcp-publisher-react`](./tcp-publisher-react)         | external TCP                 | server runtime       | Non-browser publisher pushing rows into schema-safe TCP ingress.               |
| [`ssr-react`](./ssr-react)                             | optional WebSocket live data | TanStack Start SSR   | Server-rendered shell with browser-only live query hydration.                  |

## Commands

Run commands through Vite+:

```bash
vp run @view-server/example-in-memory-react#test
vp run @view-server/example-in-memory-react#build
vp run @view-server/example-kafka-react#build
```

Every example has:

- `vp run <package>#dev` for the TanStack Start dev server.
- `vp run <package>#build` for production build.
- `vp run <package>#test` for type/browser checks where applicable.

Each React example also includes a Vitest browser-mode test that renders the
same production component under `createInMemoryViewServerReact`. Application
code keeps using the same `useLiveQuery` and health hooks; tests only swap the
provider, so component tests do not need Kafka, gRPC, TCP, or a WebSocket server.

Source-backed examples also expose `runtime` scripts that start the View Server
runtime:

```bash
vp run @view-server/example-kafka-react#runtime
vp run @view-server/example-grpc-leased-react#runtime
vp run @view-server/example-grpc-materialized-react#runtime
vp run @view-server/example-combined-sources-react#runtime
vp run @view-server/example-tcp-publisher-react#runtime
```

The TCP example additionally exposes an external publisher:

```bash
vp run @view-server/example-tcp-publisher-react#publisher
vp run @view-server/example-tcp-publisher-react#publisher:invalid
```

Kafka examples assume Apache Kafka is reachable at `127.0.0.1:9092`, matching
the repository Docker Compose defaults.
