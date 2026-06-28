import { describe, expect, it } from "@effect/vitest";
import { createInMemoryViewServerReact } from "@view-server/react/testing";
import { Effect } from "effect";
import { render } from "vitest-browser-react";
import { TcpPublisherExampleApp } from "./view-server.example";
import { viewServerReact } from "./view-server.config";

describe("TCP publisher React example", () => {
  it("renders the production component with an in-memory provider", async () => {
    const inMemoryExample = createInMemoryViewServerReact(viewServerReact);
    await Effect.runPromise(inMemoryExample.client.reset());

    const screen = await render(
      <inMemoryExample.ViewServerInMemoryProvider>
        <TcpPublisherExampleApp />
      </inMemoryExample.ViewServerInMemoryProvider>,
    );

    await expect
      .element(screen.getByRole("heading", { name: "TCP ingress to live React table" }))
      .toBeVisible();
    await Effect.runPromise(
      inMemoryExample.client.publish("orders", {
        id: "tcp-order-browser",
        customerId: "tcp-customer-browser",
        status: "open",
        price: 55,
        region: "london",
        updatedAt: 1,
      }),
    );
    await expect
      .element(
        screen.getByText("tcp-order-browser / tcp-customer-browser / london / 55", { exact: true }),
      )
      .toBeVisible();
    await screen.unmount();
    await Effect.runPromise(inMemoryExample.close);
  });
});
