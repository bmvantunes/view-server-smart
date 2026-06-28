import { describe, expect, it } from "@effect/vitest";
import { createInMemoryViewServerReact } from "@view-server/react/testing";
import { Effect } from "effect";
import { render } from "vitest-browser-react";
import { CombinedSourcesExampleApp } from "./view-server.example";
import { viewServerReact } from "./view-server.config";

describe("combined sources React example", () => {
  it("renders the production component with an in-memory provider", async () => {
    const inMemoryExample = createInMemoryViewServerReact(viewServerReact);
    const screen = await render(
      <inMemoryExample.ViewServerInMemoryProvider>
        <CombinedSourcesExampleApp />
      </inMemoryExample.ViewServerInMemoryProvider>,
    );

    await expect
      .element(screen.getByRole("heading", { name: "Kafka plus leased and materialized gRPC" }))
      .toBeVisible();
    await expect.element(screen.getByRole("heading", { name: "Leased orders" })).toBeVisible();
    await expect
      .element(screen.getByRole("heading", { name: "Materialized strategies" }))
      .toBeVisible();
    await expect.element(screen.getByRole("heading", { name: "Kafka trades" })).toBeVisible();
    await Effect.runPromise(
      Effect.all(
        [
          inMemoryExample.client.publish("orders", {
            id: "combined-order-browser",
            customerId: "customer-combined-browser",
            status: "open",
            price: 60,
            region: "usa",
            strategyId: "strategy-alpha",
            updatedAt: 1,
          }),
          inMemoryExample.client.publish("strategies", {
            id: "combined-strategy-browser",
            strategyId: "strategy-combined",
            region: "usa",
            status: "active",
            notional: 2_000,
            updatedAt: 1,
          }),
          inMemoryExample.client.publish("trades", {
            id: "trade-browser",
            symbol: "AAPL",
            side: "buy",
            quantity: 100,
            region: "usa",
            updatedAt: 1,
          }),
        ],
        { concurrency: 1 },
      ),
    );
    await expect.element(screen.getByText("combined-order-browser", { exact: true })).toBeVisible();
    await expect
      .element(screen.getByText("combined-strategy-browser", { exact: true }))
      .toBeVisible();
    await expect.element(screen.getByText("trade-browser", { exact: true })).toBeVisible();
    await screen.unmount();
    await Effect.runPromise(inMemoryExample.close);
  });
});
