import { describe, expect, it } from "@effect/vitest";
import { createInMemoryViewServerReact } from "@view-server/react/testing";
import { Effect } from "effect";
import { render } from "vitest-browser-react";
import { GrpcLeasedExampleApp } from "./view-server.example";
import { viewServerReact } from "./view-server.config";

describe("leased gRPC React example", () => {
  it("renders the production component with an in-memory provider", async () => {
    const inMemoryExample = createInMemoryViewServerReact(viewServerReact);
    const screen = await render(
      <inMemoryExample.ViewServerInMemoryProvider>
        <GrpcLeasedExampleApp />
      </inMemoryExample.ViewServerInMemoryProvider>,
    );

    await expect
      .element(screen.getByRole("heading", { name: "On-demand shared gRPC route" }))
      .toBeVisible();
    await expect
      .element(screen.getByRole("heading", { name: "Strategy alpha orders" }))
      .toBeVisible();
    await expect.element(screen.getByRole("status")).toHaveTextContent("Runtime status: ready");
    await Effect.runPromise(
      inMemoryExample.client.publish("orders", {
        id: "leased-order-browser",
        customerId: "customer-leased-browser",
        status: "open",
        price: 77,
        region: "usa",
        strategyId: "strategy-alpha",
        updatedAt: 1,
      }),
    );
    await expect
      .element(
        screen.getByText("leased-order-browser / customer-leased-browser / 77", { exact: true }),
      )
      .toBeVisible();
    await screen.unmount();
    await Effect.runPromise(inMemoryExample.close);
  });
});
