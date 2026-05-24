import { afterEach, describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig, type ViewServerInMemoryRuntime } from "@view-server/config";
import { Effect, Schema } from "effect";
import type { ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { page } from "vitest/browser";
import { createViewServerReact } from "./index";

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

const { ViewServerInMemoryProvider, useLiveQuery, useViewServerHealth, useViewServerTestRuntime } =
  createViewServerReact(viewServer);

type Topics = typeof viewServer.topics;
type OrderRow = typeof Order.Type;

const order = (id: string, price: number): OrderRow => ({
  id,
  customerId: `customer-${id}`,
  status: "open",
  price,
  region: "usa",
  updatedAt: price,
});

const getRuntime = (
  runtime: ViewServerInMemoryRuntime<Topics> | undefined,
): ViewServerInMemoryRuntime<Topics> => {
  expect(runtime).toBeDefined();
  return runtime as ViewServerInMemoryRuntime<Topics>;
};

const roots = new Set<Root>();

const mount = (children: ReactNode) => {
  const container = document.createElement("main");
  document.body.append(container);
  const root = createRoot(container);
  roots.add(root);
  root.render(children);

  return {
    rerender: (nextChildren: ReactNode) => {
      root.render(nextChildren);
    },
    unmount: () => {
      root.unmount();
      roots.delete(root);
      container.remove();
    },
  };
};

describe("createViewServerReact", () => {
  afterEach(() => {
    for (const root of roots) {
      root.unmount();
    }
    roots.clear();
    document.body.replaceChildren();
  });

  it("streams runtime-published snapshots and live deltas in browser providers", async () => {
    let runtime: ViewServerInMemoryRuntime<Topics> | undefined;

    function RuntimeCapture() {
      runtime = useViewServerTestRuntime();
      return null;
    }
    function OrdersView() {
      const result = useLiveQuery("orders", {
        orderBy: [{ field: "price", direction: "asc" }],
        select: ["id", "price"],
        limit: 10,
      });
      return (
        <output aria-label="orders" role="status">
          {result.rows.map((row) => `${row.id}:${row.price}`).join("|")}
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

    const view = mount(
      <ViewServerInMemoryProvider>
        <RuntimeCapture />
        <OrdersView />
        <HealthView />
      </ViewServerInMemoryProvider>,
    );
    const orders = page.getByRole("status", { name: "orders" });
    const health = page.getByRole("status", { name: "health" });
    await expect.element(orders).toHaveTextContent("");

    Effect.runSync(getRuntime(runtime).publishMany("orders", [order("b", 20), order("a", 10)]));

    await expect.element(orders).toHaveTextContent("a:10|b:20");
    await expect.element(health).toHaveTextContent("2");

    Effect.runSync(getRuntime(runtime).publish("orders", order("c", 5)));

    await expect.element(orders).toHaveTextContent("c:5|a:10|b:20");
    await expect.element(health).toHaveTextContent("3");
    view.unmount();
  });

  it("closes live subscriptions when browser components unmount", async () => {
    let runtime: ViewServerInMemoryRuntime<Topics> | undefined;

    function RuntimeCapture() {
      runtime = useViewServerTestRuntime();
      return null;
    }
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

    const view = mount(
      <ViewServerInMemoryProvider>
        <RuntimeCapture />
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    const orders = page.getByRole("status", { name: "orders" });
    await expect.element(orders).toHaveTextContent("");

    Effect.runSync(getRuntime(runtime).publish("orders", order("a", 10)));
    await expect.element(orders).toHaveTextContent("a");

    view.rerender(
      <ViewServerInMemoryProvider>
        <RuntimeCapture />
      </ViewServerInMemoryProvider>,
    );

    await expect
      .poll(
        () => Effect.runSync(getRuntime(runtime).health()).engine.topics.orders.activeSubscriptions,
      )
      .toBe(0);
    view.unmount();
  });

  it("applies update, move, remove, patch, snapshot, and reset paths", async () => {
    let runtime: ViewServerInMemoryRuntime<Topics> | undefined;

    function RuntimeCapture() {
      runtime = useViewServerTestRuntime();
      return null;
    }
    function OrdersView() {
      const result = useLiveQuery("orders", {
        orderBy: [{ field: "price", direction: "asc" }],
        select: ["id", "price"],
        limit: 10,
      });
      return (
        <output aria-label="orders" role="status">
          {result.rows.map((row) => `${row.id}:${row.price}`).join("|")}
        </output>
      );
    }

    const view = mount(
      <ViewServerInMemoryProvider>
        <RuntimeCapture />
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    const orders = page.getByRole("status", { name: "orders" });
    await expect.element(orders).toHaveTextContent("");

    Effect.runSync(getRuntime(runtime).publishMany("orders", [order("a", 10), order("b", 20)]));
    await expect.element(orders).toHaveTextContent("a:10|b:20");

    Effect.runSync(getRuntime(runtime).publish("orders", order("a", 30)));
    await expect.element(orders).toHaveTextContent("b:20|a:30");

    Effect.runSync(getRuntime(runtime).patch("orders", "a", { price: 5 }));
    await expect.element(orders).toHaveTextContent("a:5|b:20");

    Effect.runSync(getRuntime(runtime).delete("orders", "a"));
    await expect.element(orders).toHaveTextContent("b:20");

    const snapshot = Effect.runSync(
      getRuntime(runtime).snapshot("orders", {
        select: ["id", "price"],
        limit: 10,
      }),
    );
    expect(snapshot.rows).toEqual([{ id: "b", price: 20 }]);

    Effect.runSync(getRuntime(runtime).reset());
    expect(Effect.runSync(getRuntime(runtime).health()).engine.topics.orders.rowCount).toBe(0);
    view.unmount();
  });

  it("maps runtime errors", async () => {
    let runtime: ViewServerInMemoryRuntime<Topics> | undefined;

    function RuntimeCapture() {
      runtime = useViewServerTestRuntime();
      return null;
    }

    const view = mount(
      <ViewServerInMemoryProvider>
        <RuntimeCapture />
      </ViewServerInMemoryProvider>,
    );
    await expect.poll(() => runtime).not.toBeUndefined();

    Effect.runSync(getRuntime(runtime).publish("orders", order("a", 10)));

    const invalidTopic = Effect.runSyncExit(
      // @ts-expect-error hostile runtime callers can still send unknown topics.
      getRuntime(runtime).publish("missing", order("b", 20)),
    );
    const invalidRow = Effect.runSyncExit(
      getRuntime(runtime).publish("orders", {
        id: "bad",
        customerId: "customer-bad",
        // @ts-expect-error hostile runtime callers can still send malformed rows.
        status: "unknown",
        price: 20,
        region: "usa",
        updatedAt: 20,
      }),
    );
    const groupedSnapshot = Effect.runSyncExit(
      getRuntime(runtime).snapshot("orders", {
        // @ts-expect-error grouped queries are rejected by the raw in-memory runtime slice.
        groupBy: ["status"],
        // @ts-expect-error grouped queries are rejected by the raw in-memory runtime slice.
        aggregates: [{ type: "count", as: "count" }],
      }),
    );
    const invalidQuery = Effect.runSyncExit(
      getRuntime(runtime).snapshot("orders", {
        // @ts-expect-error hostile runtime callers can still send unknown projected fields.
        select: ["prcie"],
      }),
    );

    expect(invalidTopic._tag).toBe("Failure");
    expect(invalidRow._tag).toBe("Failure");
    expect(groupedSnapshot._tag).toBe("Failure");
    expect(invalidQuery._tag).toBe("Failure");
    view.unmount();
  });

  it("keeps query memoization safe for bigint query values", async () => {
    let runtime: ViewServerInMemoryRuntime<Topics> | undefined;

    function RuntimeCapture() {
      runtime = useViewServerTestRuntime();
      return null;
    }
    function TradesView() {
      const result = useLiveQuery("trades", {
        where: {
          quantity: { gte: 10n },
        },
        select: ["id", "quantity"],
        limit: 10,
      });
      return (
        <output aria-label="trades" role="status">
          {result.rows.map((row) => `${row.id}:${row.quantity}`).join("|")}
        </output>
      );
    }

    const view = mount(
      <ViewServerInMemoryProvider>
        <RuntimeCapture />
        <TradesView />
      </ViewServerInMemoryProvider>,
    );
    const trades = page.getByRole("status", { name: "trades" });
    await expect.element(trades).toHaveTextContent("");

    Effect.runSync(
      getRuntime(runtime).publishMany("trades", [
        { id: "a", symbol: "AAPL", quantity: 5n, price: 100, region: "usa" },
        { id: "b", symbol: "MSFT", quantity: 10n, price: 200, region: "usa" },
      ]),
    );

    await expect.element(trades).toHaveTextContent("b:10");
    view.unmount();
  });

  it("surfaces runtime unavailable after provider disposal", async () => {
    let runtime: ViewServerInMemoryRuntime<Topics> | undefined;

    function RuntimeCapture() {
      runtime = useViewServerTestRuntime();
      return null;
    }

    const view = mount(
      <ViewServerInMemoryProvider>
        <RuntimeCapture />
      </ViewServerInMemoryProvider>,
    );
    await expect.poll(() => runtime).not.toBeUndefined();

    view.unmount();
    await expect
      .poll(() => Effect.runSyncExit(getRuntime(runtime).publish("orders", order("a", 10)))._tag)
      .toBe("Failure");
  });

  it("surfaces status events from bounded subscription queues", async () => {
    let runtime: ViewServerInMemoryRuntime<Topics> | undefined;

    function RuntimeCapture() {
      runtime = useViewServerTestRuntime();
      return null;
    }
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

    const view = mount(
      <ViewServerInMemoryProvider subscriptionQueueCapacity={1}>
        <RuntimeCapture />
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    const orders = page.getByRole("status", { name: "orders" });
    await expect.poll(() => runtime).not.toBeUndefined();

    Effect.runSync(getRuntime(runtime).publish("orders", order("a", 10)));
    await expect.element(orders).toHaveTextContent("ready:Ready");

    for (let index = 0; index < 50; index += 1) {
      Effect.runSync(getRuntime(runtime).publish("orders", order(`burst-${index}`, index)));
    }

    expect(Effect.runSync(getRuntime(runtime).health()).transport.backpressureEvents).toBe(1);
    await expect.element(orders).toHaveTextContent("closed:BackpressureExceeded");
    view.unmount();
  });
});
