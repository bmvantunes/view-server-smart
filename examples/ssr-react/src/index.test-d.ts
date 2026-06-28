import { describe, expectTypeOf, it } from "@effect/vitest";
import type { LiveQueryResult } from "@view-server/config";
import type { ReactNode } from "react";
import { SsrExampleApp } from "./view-server.example";
import { useLiveQuery } from "./view-server.config";

describe("SSR example type contracts", () => {
  it("keeps live query row inference available behind the client-only panel", () => {
    const result = useLiveQuery("orders", {
      select: ["id", "price"],
      limit: 10,
    });

    expectTypeOf(result).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly price: number;
      }>
    >();
  });

  it("renders an SSR-safe shell component", () => {
    expectTypeOf(SsrExampleApp()).toMatchTypeOf<ReactNode>();
  });
});
