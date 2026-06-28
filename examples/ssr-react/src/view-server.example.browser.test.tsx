import { describe, expect, it } from "@effect/vitest";
import { createInMemoryViewServerReact } from "@view-server/react/testing";
import { Effect } from "effect";
import { render } from "vitest-browser-react";
import { SsrExampleApp } from "./view-server.example";
import { viewServerReact } from "./view-server.config";

describe("SSR React example", () => {
  it("renders the browser-only live panel with an in-memory provider", async () => {
    const inMemoryExample = createInMemoryViewServerReact(viewServerReact);
    await Effect.runPromise(inMemoryExample.client.reset());

    const screen = await render(
      <SsrExampleApp
        wrapLiveOrdersPanel={(liveOrdersPanel) => (
          <inMemoryExample.ViewServerInMemoryProvider>
            {liveOrdersPanel}
          </inMemoryExample.ViewServerInMemoryProvider>
        )}
      />,
    );

    await expect
      .element(
        screen.getByRole("heading", { name: "TanStack Start shell with client-only live data" }),
      )
      .toBeVisible();
    await screen.getByRole("button", { name: "Connect live data" }).click();
    await expect.element(screen.getByRole("heading", { name: "Live orders" })).toBeVisible();
    await Effect.runPromise(
      inMemoryExample.client.publish("orders", {
        id: "order-ssr-browser",
        customerId: "customer-ssr-browser",
        status: "open",
        price: 99,
        region: "usa",
        updatedAt: 1,
      }),
    );
    await expect
      .element(screen.getByText("order-ssr-browser / customer-ssr-browser / 99", { exact: true }))
      .toBeVisible();
    await screen.unmount();
    await Effect.runPromise(inMemoryExample.close);
  });
});
