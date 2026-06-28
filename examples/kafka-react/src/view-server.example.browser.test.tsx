import { describe, expect, it } from "@effect/vitest";
import { createInMemoryViewServerReact } from "@view-server/react/testing";
import { Effect } from "effect";
import { render } from "vitest-browser-react";
import { KafkaExampleApp } from "./view-server.example";
import { viewServerReact } from "./view-server.config";

describe("Kafka React example", () => {
  it("renders the production component with an in-memory provider", async () => {
    const inMemoryExample = createInMemoryViewServerReact(viewServerReact);
    await Effect.runPromise(inMemoryExample.client.reset());

    const screen = await render(
      <inMemoryExample.ViewServerInMemoryProvider>
        <KafkaExampleApp />
      </inMemoryExample.ViewServerInMemoryProvider>,
    );

    await expect
      .element(screen.getByRole("heading", { name: "Apache Kafka to View Server to React" }))
      .toBeVisible();
    await Effect.runPromise(
      inMemoryExample.client.publish("orders", {
        id: "order-kafka-browser",
        customerId: "customer-kafka-browser",
        status: "open",
        price: 42,
        region: "usa",
        updatedAt: 1,
      }),
    );
    await expect.element(screen.getByRole("cell", { name: "order-kafka-browser" })).toBeVisible();
    await expect
      .element(screen.getByRole("cell", { name: "customer-kafka-browser" }))
      .toBeVisible();
    await screen.unmount();
    await Effect.runPromise(inMemoryExample.close);
  });
});
