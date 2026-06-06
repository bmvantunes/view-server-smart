# view-server-smart

## Remote React provider

Server code starts a runtime through Effect RPC WebSocket and same-server
`GET /health`:

Node entrypoints should use `@effect/platform-node`'s `NodeRuntime.runMain` so
`SIGINT` and `SIGTERM` interrupt the main fiber and run Effect finalizers.

```ts
import { NodeRuntime } from "@effect/platform-node";
import { createViewServerRuntime } from "@view-server/runtime";
import { Effect } from "effect";
import { viewServer } from "./view-server-config";

const program = Effect.scoped(
  Effect.gen(function* () {
    const runtime = yield* createViewServerRuntime(viewServer, {
      host: "127.0.0.1",
      websocketPort: 8080,
    });

    yield* Effect.addFinalizer(() => runtime.close);
    yield* Effect.logInfo(`View Server WebSocket listening at ${runtime.url}`);
    yield* Effect.logInfo(`View Server health endpoint listening at ${runtime.healthUrl}`);
    yield* Effect.never;
  }),
);

NodeRuntime.runMain(program);
```

`runtime.healthUrl` serves the cached runtime health snapshot for deployment
readiness checks. Internal `bigint` health fields, such as Kafka lag, are encoded
as decimal strings in the JSON response.

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
