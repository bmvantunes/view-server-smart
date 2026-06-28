import { describe, expect, it } from "@effect/vitest";
import { createInMemoryViewServerReact } from "@view-server/react/testing";
import { Effect } from "effect";
import { render } from "vitest-browser-react";
import { GrpcMaterializedExampleApp } from "./view-server.example";
import { viewServerReact } from "./view-server.config";

describe("materialized gRPC React example", () => {
  it("renders the production component with an in-memory provider", async () => {
    const inMemoryExample = createInMemoryViewServerReact(viewServerReact);
    const screen = await render(
      <inMemoryExample.ViewServerInMemoryProvider>
        <GrpcMaterializedExampleApp />
      </inMemoryExample.ViewServerInMemoryProvider>,
    );

    await expect
      .element(screen.getByRole("heading", { name: "Startup materialized strategy stream" }))
      .toBeVisible();
    await expect.element(screen.getByRole("heading", { name: "Active strategies" })).toBeVisible();
    await expect.element(screen.getByRole("status")).toHaveTextContent("Runtime status: ready");
    await Effect.runPromise(
      inMemoryExample.client.publish("strategies", {
        id: "strategy-browser-row",
        strategyId: "strategy-browser",
        region: "usa",
        status: "active",
        notional: 1_000,
        updatedAt: 1,
      }),
    );
    await expect
      .element(screen.getByText("strategy-browser / usa / 1000", { exact: true }))
      .toBeVisible();
    await screen.unmount();
    await Effect.runPromise(inMemoryExample.close);
  });
});
