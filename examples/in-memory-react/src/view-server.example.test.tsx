import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { render } from "vitest-browser-react";
import { InMemoryExampleApp, createInMemoryExample } from "./view-server.example";

describe("in-memory React example", () => {
  it("publishes rows through the in-memory client and renders live raw and grouped results", async () => {
    const inMemoryExample = createInMemoryExample();
    await Effect.runPromise(inMemoryExample.client.reset());
    const screen = await render(
      <inMemoryExample.ViewServerInMemoryProvider>
        <InMemoryExampleApp onPublishOrder={() => Promise.resolve()} publishedCount={0} />
      </inMemoryExample.ViewServerInMemoryProvider>,
    );

    await expect
      .element(screen.getByRole("heading", { name: "Live orders without a server process" }))
      .toBeVisible();
    await expect.element(screen.getByText("Total rows: 0", { exact: true })).toBeVisible();

    await Effect.runPromise(
      inMemoryExample.client.publish("orders", {
        id: "order-browser",
        customerId: "customer-browser",
        status: "open",
        price: 25,
        region: "usa",
        updatedAt: 1,
      }),
    );

    await expect.element(screen.getByRole("cell", { name: "order-browser" })).toBeVisible();
    await expect.element(screen.getByRole("cell", { name: "customer-browser" })).toBeVisible();
    await expect.element(screen.getByText("Grouped rows: 1", { exact: true })).toBeVisible();
    await screen.unmount();
    await Effect.runPromise(inMemoryExample.close);
  });
});
