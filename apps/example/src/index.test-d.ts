import { describe, expectTypeOf, it } from "@effect/vitest";
import type { LiveQueryResult } from "@view-server/config";
import type { ReactNode } from "react";
import { AppRoot, useLiveQuery, type ExampleRuntimeConfig } from "./index";
import { createInMemoryExampleViewServer } from "./testing";

describe("example app type contracts", () => {
  it("keeps the runtime URL at the provider boundary", () => {
    const config = {
      VIEW_SERVER_URL: "ws://127.0.0.1:8080/rpc",
    } satisfies ExampleRuntimeConfig;
    const app = AppRoot({ config });
    expectTypeOf(app).toMatchTypeOf<ReactNode>();
  });

  it("preserves selected order row types", () => {
    const result = useLiveQuery("orders", {
      select: ["id", "price"],
      where: {
        status: { eq: "open" },
      },
      orderBy: [{ field: "price", direction: "desc" }],
      limit: 20,
    });

    expectTypeOf(result).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly price: number;
      }>
    >();
  });

  it("rejects implicit all-column queries", () => {
    const missingSelectQuery = {
      where: {
        status: { eq: "open" },
      },
      limit: 20,
    };
    // @ts-expect-error raw example queries must explicitly select columns.
    useLiveQuery("orders", missingSelectQuery);
  });

  it("exposes an in-memory provider and typed client for examples and tests", () => {
    const inMemory = createInMemoryExampleViewServer();
    expectTypeOf(inMemory.client.publish).toBeFunction();
    expectTypeOf(inMemory.ViewServerInMemoryProvider).toBeFunction();
  });
});
