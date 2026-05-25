import { describe, expect, it, vi } from "@effect/vitest";
import { defineViewServerConfig } from "@view-server/config";
import { createInMemoryViewServer as createCoreInMemoryViewServer } from "@view-server/in-memory";
import { Effect, Schema } from "effect";
import { Component, type ReactNode } from "react";
import { render } from "vitest-browser-react";
import { createViewServerReact } from "./index";
import { createInMemoryViewServerReact, type ViewServerInMemoryOptions } from "./testing";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  quantity: Schema.BigInt,
  price: Schema.Number,
  region: Schema.String,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
    trades: {
      schema: Trade,
      key: "id",
    },
  },
});

const react = createViewServerReact(viewServer);
const { useLiveQuery, useViewServerHealth } = react;

const createInMemoryViewServer = (options?: ViewServerInMemoryOptions) =>
  createInMemoryViewServerReact(react, options);

type OrderRow = typeof Order.Type;

const order = (id: string, price: number): OrderRow => ({
  id,
  customerId: `customer-${id}`,
  status: "open",
  price,
  region: "usa",
  updatedAt: price,
});

class ProviderErrorBoundary extends Component<
  { readonly children: ReactNode },
  { readonly message: string | null }
> {
  override readonly state: { readonly message: string | null } = {
    message: null,
  };

  static getDerivedStateFromError(error: unknown): { readonly message: string } {
    return {
      message: error instanceof Error ? error.message : String(error),
    };
  }

  override render(): ReactNode {
    if (this.state.message !== null) {
      return (
        <output aria-label="provider error" role="alert">
          {this.state.message}
        </output>
      );
    }
    return this.props.children;
  }
}

describe("createViewServerReact", () => {
  it("cleans subscriptions without closing caller-owned generic provider clients", async () => {
    const inMemory = createCoreInMemoryViewServer(viewServer);

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="orders" role="status">
          {result.rows.map((row) => row.id).join("|")}
        </output>
      );
    }

    const view = await render(
      <react.ViewServerProvider client={inMemory.liveClient}>
        <OrdersView />
      </react.ViewServerProvider>,
    );
    const orders = view.getByRole("status", { name: "orders" });

    await Effect.runPromise(inMemory.client.publish("orders", order("a", 10)));
    await expect.element(orders).toHaveTextContent(/^a$/);

    await view.unmount();

    await expect
      .poll(async () => {
        const health = await Effect.runPromise(inMemory.client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(0);

    await Effect.runPromise(inMemory.client.publish("orders", order("b", 20)));
    const health = await Effect.runPromise(inMemory.client.health());
    expect(health.status).toBe("ready");
    expect(health.engine.topics.orders.rowCount).toBe(2);
    await Effect.runPromise(inMemory.close);
  });

  it("surfaces missing provider clients", async () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);

    function HealthView() {
      const health = useViewServerHealth();
      return <output role="status">{health.status}</output>;
    }

    try {
      const missingProvider = await render(
        <ProviderErrorBoundary>
          <HealthView />
        </ProviderErrorBoundary>,
      );
      const error = missingProvider.getByRole("alert", { name: "provider error" });
      await expect.element(error).toHaveTextContent(/^ViewServerProvider is missing a client\.$/);
      await missingProvider.unmount();
    } finally {
      consoleError.mockRestore();
    }
  });

  it("switches hook clients when the generic provider client prop changes", async () => {
    const first = createCoreInMemoryViewServer(viewServer);
    const second = createCoreInMemoryViewServer(viewServer);

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="orders" role="status">
          {result.rows.map((row) => row.id).join("|")}
        </output>
      );
    }

    const view = await render(
      <react.ViewServerProvider client={first.liveClient}>
        <OrdersView />
      </react.ViewServerProvider>,
    );
    const orders = view.getByRole("status", { name: "orders" });

    await Effect.runPromise(first.client.publish("orders", order("first", 10)));
    await expect.element(orders).toHaveTextContent(/^first$/);

    await view.rerender(
      <react.ViewServerProvider client={second.liveClient}>
        <OrdersView />
      </react.ViewServerProvider>,
    );
    await Effect.runPromise(second.client.publish("orders", order("second", 20)));
    await expect.element(orders).toHaveTextContent(/^second$/);

    await expect
      .poll(async () => {
        const health = await Effect.runPromise(first.client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(0);

    await view.unmount();
    await Effect.runPromise(first.close);
    await Effect.runPromise(second.close);
  });

  it("keeps nested provider contexts isolated per binding instance", async () => {
    const outerReact = createViewServerReact(viewServer);
    const innerReact = createViewServerReact(viewServer);
    const outer = createInMemoryViewServerReact(outerReact);
    const inner = createInMemoryViewServerReact(innerReact);

    function OuterOrdersView() {
      const result = outerReact.useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="outer orders" role="status">
          outer orders: {result.rows.map((row) => row.id).join("|")}
        </output>
      );
    }

    function InnerOrdersView() {
      const result = innerReact.useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="inner orders" role="status">
          inner orders: {result.rows.map((row) => row.id).join("|")}
        </output>
      );
    }

    const view = await render(
      <outer.ViewServerInMemoryProvider>
        <inner.ViewServerInMemoryProvider>
          <OuterOrdersView />
          <InnerOrdersView />
        </inner.ViewServerInMemoryProvider>
      </outer.ViewServerInMemoryProvider>,
    );

    await Effect.runPromise(outer.client.publish("orders", order("outer", 10)));
    await Effect.runPromise(inner.client.publish("orders", order("inner", 20)));

    const outerOrders = view.getByRole("status", { name: "outer orders" });
    const innerOrders = view.getByRole("status", { name: "inner orders" });
    await expect.element(outerOrders).toHaveTextContent(/^outer orders: outer$/);
    await expect.element(innerOrders).toHaveTextContent(/^inner orders: inner$/);

    await view.unmount();
  });

  it("streams runtime-published snapshots and live deltas in browser providers", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function OrdersView() {
      const result = useLiveQuery("orders", {
        orderBy: [{ field: "price", direction: "asc" }],
        select: ["id", "price"],
        limit: 10,
      });
      const rows = result.rows.map((row) => `${row.id}:${row.price}`).join("|");
      return (
        <output aria-label="orders" role="status">
          {rows === "" ? "orders: none" : `orders: ${rows}`}
        </output>
      );
    }
    function HealthView() {
      const health = useViewServerHealth();
      return (
        <output aria-label="health" role="status">
          {health.engine.topics.orders.rowCount}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
        <HealthView />
      </ViewServerInMemoryProvider>,
    );
    const orders = view.getByRole("status", { name: "orders" });
    const health = view.getByRole("status", { name: "health" });
    await expect.element(orders).toHaveTextContent(/^orders: none$/);

    await Effect.runPromise(client.publishMany("orders", [order("b", 20), order("a", 10)]));

    await expect.element(orders).toHaveTextContent(/^orders: a:10\|b:20$/);
    await expect.element(health).toHaveTextContent(/^2$/);

    await Effect.runPromise(client.publish("orders", order("c", 5)));

    await expect.element(orders).toHaveTextContent(/^orders: c:5\|a:10\|b:20$/);
    await expect.element(health).toHaveTextContent(/^3$/);
    await view.unmount();
  });

  it("closes live subscriptions when browser components unmount", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const rows = result.rows.map((row) => row.id).join("|");
      return (
        <output aria-label="orders" role="status">
          {rows === "" ? "orders: none" : `orders: ${rows}`}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    const orders = view.getByRole("status", { name: "orders" });
    await expect.element(orders).toHaveTextContent(/^orders: none$/);

    await Effect.runPromise(client.publish("orders", order("a", 10)));
    await expect.element(orders).toHaveTextContent(/^orders: a$/);

    await view.rerender(<ViewServerInMemoryProvider></ViewServerInMemoryProvider>);

    await expect
      .poll(async () => {
        const health = await Effect.runPromise(client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(0);
    await view.unmount();
  });

  it("keeps the in-memory engine open while a mounted provider has no hook consumers", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const rows = result.rows.map((row) => `${row.id}:${row.price}`).join("|");
      return (
        <output aria-label="orders" role="status">
          {rows === "" ? "orders: none" : `orders: ${rows}`}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    const orders = view.getByRole("status", { name: "orders" });

    await Effect.runPromise(client.publish("orders", order("a", 10)));
    await expect.element(orders).toHaveTextContent(/^orders: a:10$/);

    await view.rerender(<ViewServerInMemoryProvider></ViewServerInMemoryProvider>);
    await expect
      .poll(async () => {
        const health = await Effect.runPromise(client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(0);

    await Effect.runPromise(client.publish("orders", order("b", 20)));

    await view.rerender(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    await expect.element(orders).toHaveTextContent(/^orders: a:10\|b:20$/);
    await view.unmount();
  });

  it("applies update, move, remove, patch, snapshot, and reset paths", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function OrdersView() {
      const result = useLiveQuery("orders", {
        orderBy: [{ field: "price", direction: "asc" }],
        select: ["id", "price"],
        limit: 10,
      });
      const rows = result.rows.map((row) => `${row.id}:${row.price}`).join("|");
      return (
        <output aria-label="orders" role="status">
          {rows === "" ? "orders: none" : `orders: ${rows}`}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    const orders = view.getByRole("status", { name: "orders" });
    await expect.element(orders).toHaveTextContent(/^orders: none$/);

    await Effect.runPromise(client.publishMany("orders", [order("a", 10), order("b", 20)]));
    await expect.element(orders).toHaveTextContent(/^orders: a:10\|b:20$/);

    await Effect.runPromise(client.publish("orders", order("a", 30)));
    await expect.element(orders).toHaveTextContent(/^orders: b:20\|a:30$/);

    await Effect.runPromise(client.patch("orders", "a", { price: 5 }));
    await expect.element(orders).toHaveTextContent(/^orders: a:5\|b:20$/);

    await Effect.runPromise(client.delete("orders", "a"));
    await expect.element(orders).toHaveTextContent(/^orders: b:20$/);

    const snapshot = await Effect.runPromise(
      client.snapshot("orders", {
        select: ["id", "price"],
        limit: 10,
      }),
    );
    expect(snapshot.rows).toEqual([{ id: "b", price: 20 }]);

    await Effect.runPromise(client.reset());
    expect((await Effect.runPromise(client.health())).engine.topics.orders.rowCount).toBe(0);
    await expect.element(orders).toHaveTextContent(/^orders: none$/);
    await view.unmount();
  });

  it("coalesces in-memory health refreshes under concurrent publishes", async () => {
    const { client } = createInMemoryViewServer();

    await Promise.all(
      Array.from({ length: 50 }, (_, index) =>
        Effect.runPromise(client.publish("orders", order(`coalesced-${index}`, index))),
      ),
    );

    await expect
      .poll(async () => {
        const health = await Effect.runPromise(client.health());
        return health.engine.topics.orders.rowCount;
      })
      .toBe(50);
  });

  it("surfaces live query failures as error results", async () => {
    const { ViewServerInMemoryProvider } = createInMemoryViewServer();

    function BrokenOrdersView() {
      const result = useLiveQuery("orders", {
        // @ts-expect-error invalid selected fields are still surfaced through the hook result.
        select: ["prcie"],
      });
      return (
        <output aria-label="orders" role="status">
          {result.status}:{result.statusCode}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <BrokenOrdersView />
      </ViewServerInMemoryProvider>,
    );
    const orders = view.getByRole("status", { name: "orders" });

    await expect.element(orders).toHaveTextContent(/^error:InvalidQuery$/);
    await view.unmount();
  });

  it("maps runtime errors", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    const view = await render(<ViewServerInMemoryProvider></ViewServerInMemoryProvider>);

    await Effect.runPromise(client.publish("orders", order("a", 10)));

    const invalidTopic = await Effect.runPromise(
      Effect.flip(
        // @ts-expect-error hostile runtime callers can still send unknown topics.
        client.publish("missing", order("b", 20)),
      ),
    );
    const invalidRow = await Effect.runPromise(
      Effect.flip(
        client.publish("orders", {
          id: "bad",
          customerId: "customer-bad",
          // @ts-expect-error hostile runtime callers can still send malformed rows.
          status: "unknown",
          price: 20,
          region: "usa",
          updatedAt: 20,
        }),
      ),
    );
    const groupedSnapshot = await Effect.runPromise(
      Effect.flip(
        client.snapshot("orders", {
          // @ts-expect-error grouped queries are rejected by the raw in-memory runtime slice.
          groupBy: ["status"],
          // @ts-expect-error grouped queries are rejected by the raw in-memory runtime slice.
          aggregates: { count: { aggFunc: "count" } },
        }),
      ),
    );
    const invalidQuery = await Effect.runPromise(
      Effect.flip(
        client.snapshot("orders", {
          // @ts-expect-error hostile runtime callers can still send unknown projected fields.
          select: ["prcie"],
        }),
      ),
    );

    expect(invalidTopic.code).toBe("InvalidTopic");
    expect(invalidRow.code).toBe("InvalidRow");
    expect(groupedSnapshot.code).toBe("UnsupportedQuery");
    expect(invalidQuery.code).toBe("InvalidQuery");
    await view.unmount();
  });

  it("keeps query memoization safe for bigint query values", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function TradesView() {
      const result = useLiveQuery("trades", {
        where: {
          quantity: { gte: 10n },
        },
        select: ["id", "quantity"],
        limit: 10,
      });
      const rows = result.rows.map((row) => `${row.id}:${row.quantity}`).join("|");
      return (
        <output aria-label="trades" role="status">
          {rows === "" ? "trades: none" : `trades: ${rows}`}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <TradesView />
      </ViewServerInMemoryProvider>,
    );
    const trades = view.getByRole("status", { name: "trades" });
    await expect.element(trades).toHaveTextContent(/^trades: none$/);

    await Effect.runPromise(
      client.publishMany("trades", [
        { id: "a", symbol: "AAPL", quantity: 5n, price: 100, region: "usa" },
        { id: "b", symbol: "MSFT", quantity: 10n, price: 200, region: "usa" },
      ]),
    );

    await expect.element(trades).toHaveTextContent(/^trades: b:10$/);
    await view.unmount();
  });

  it("closes the owned in-memory runtime after provider disposal", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function HealthView() {
      const health = useViewServerHealth();
      return (
        <output aria-label="health" role="status">
          {health.status}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <HealthView />
      </ViewServerInMemoryProvider>,
    );
    const health = view.getByRole("status", { name: "health" });
    await expect.element(health).toHaveTextContent(/^ready$/);

    await view.unmount();
    await expect
      .poll(async () => {
        return Effect.runPromise(Effect.flip(client.publish("orders", order("a", 10)))).then(
          (error) => error.code,
          () => "success",
        );
      })
      .toBe("RuntimeUnavailable");
  });

  it("surfaces closed status and clears rows when runtime closes while subscribed", async () => {
    const { ViewServerInMemoryProvider, client, close } = createInMemoryViewServer();

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="orders" role="status">
          {result.status}:{result.statusCode}:{result.rows.map((row) => row.id).join("|")}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    const orders = view.getByRole("status", { name: "orders" });

    await Effect.runPromise(client.publish("orders", order("a", 10)));
    await expect.element(orders).toHaveTextContent(/^ready:Ready:a$/);

    await Effect.runPromise(close);

    await expect.element(orders).toHaveTextContent(/^closed:SubscriptionClosed:$/);
    await view.unmount();
  });

  it("returns close for disposing in-memory helpers without mounting a provider", async () => {
    const { client, close } = createInMemoryViewServer();

    await Effect.runPromise(client.publish("orders", order("a", 10)));
    await Effect.runPromise(close);

    const health = await Effect.runPromise(client.health());
    expect(health.status).toBe("stopping");

    await expect
      .poll(async () => {
        return Effect.runPromise(Effect.flip(client.publish("orders", order("b", 20)))).then(
          (error) => error.code,
          () => "success",
        );
      })
      .toBe("RuntimeUnavailable");
  });

  it("surfaces status events from bounded subscription queues", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer({
      subscriptionQueueCapacity: 1,
    });

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="orders" role="status">
          {result.status}:{result.statusCode}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    const orders = view.getByRole("status", { name: "orders" });

    await Effect.runPromise(client.publish("orders", order("a", 10)));
    await expect.element(orders).toHaveTextContent(/^ready:Ready$/);

    for (let index = 0; index < 50; index += 1) {
      await Effect.runPromise(client.publish("orders", order(`burst-${index}`, index)));
    }

    expect((await Effect.runPromise(client.health())).transport.backpressureEvents).toBe(1);
    await expect.element(orders).toHaveTextContent(/^closed:BackpressureExceeded$/);
    await view.unmount();
  });
});
