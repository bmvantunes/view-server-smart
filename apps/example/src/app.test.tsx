import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { render } from "vitest-browser-react";
import { AppRoot } from "./app-root";
import { OrdersApp } from "./app";
import { createInMemoryExampleViewServer } from "./testing";
import { ViewServerProvider } from "./view-server.config";

describe("OrdersApp", () => {
  it("keeps the production websocket URL at the provider boundary", () => {
    const configuredRoot = AppRoot({
      children: <p>runtime URL configured</p>,
      config: { VIEW_SERVER_URL: "ws://127.0.0.1:8080/rpc" },
    });
    const defaultRoot = AppRoot({
      config: { VIEW_SERVER_URL: "ws://127.0.0.1:8080/rpc" },
    });

    expect(configuredRoot).toStrictEqual(
      <ViewServerProvider url="ws://127.0.0.1:8080/rpc">
        <p>runtime URL configured</p>
      </ViewServerProvider>,
    );
    expect(defaultRoot).toStrictEqual(
      <ViewServerProvider url="ws://127.0.0.1:8080/rpc">
        <OrdersApp />
      </ViewServerProvider>,
    );
  });

  it("uses the in-memory provider path with the same live query hook as production", async () => {
    const inMemory = createInMemoryExampleViewServer();
    const view = await render(
      <inMemory.ViewServerInMemoryProvider>
        <OrdersApp />
      </inMemory.ViewServerInMemoryProvider>,
    );

    await expect.element(view.getByRole("heading", { name: "Orders" })).toBeVisible();
    await expect.element(view.getByText("Open orders: 0", { exact: true })).toBeVisible();

    await Effect.runPromise(
      inMemory.client.publish("orders", {
        id: "order-a",
        customerId: "customer-a",
        status: "open",
        price: 42,
        region: "usa",
        updatedAt: 1,
      }),
    );
    await Effect.runPromise(
      inMemory.client.publish("orders", {
        id: "order-b",
        customerId: "customer-b",
        status: "closed",
        price: 99,
        region: "usa",
        updatedAt: 2,
      }),
    );

    await expect.element(view.getByText("Open orders: 1", { exact: true })).toBeVisible();
    await expect.element(view.getByRole("cell", { name: "order-a" })).toBeVisible();
    await expect.element(view.getByRole("cell", { name: "42" })).toBeVisible();
    await expect.element(view.getByRole("cell", { name: "order-b" })).not.toBeInTheDocument();

    await view.unmount();
    await Effect.runPromise(inMemory.close);
  });
});
