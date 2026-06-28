import { describe, expectTypeOf, it } from "@effect/vitest";
import type { LiveQueryResult } from "@view-server/config";
import type { ViewServerInMemoryProviderProps } from "@view-server/react/testing";
import { createInMemoryExample } from "./view-server.example";
import { useLiveQuery } from "./view-server.config";

describe("in-memory example type contracts", () => {
  it("preserves selected raw row types", () => {
    const result = useLiveQuery("orders", {
      select: ["id", "price"],
      where: { status: { eq: "open" } },
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

  it("preserves grouped aggregate aliases and precision types", () => {
    const result = useLiveQuery("orders", {
      groupBy: ["region"],
      aggregates: {
        rowCount: { aggFunc: "count" },
        totalPrice: { aggFunc: "sum", field: "price" },
      },
      orderBy: [{ aggregate: "rowCount", direction: "desc" }],
      limit: 10,
    });

    expectTypeOf(result.rows).toEqualTypeOf<
      ReadonlyArray<{
        readonly region: string;
        readonly rowCount: bigint;
        readonly totalPrice: import("effect/BigDecimal").BigDecimal;
      }>
    >();
  });

  it("rejects implicit all-column raw queries", () => {
    // @ts-expect-error raw queries must explicitly select columns.
    useLiveQuery("orders", { where: { status: { eq: "open" } }, limit: 20 });
  });

  it("exposes a typed in-memory client", () => {
    const inMemoryExample = createInMemoryExample();

    expectTypeOf(inMemoryExample.client.publish).parameter(0).toEqualTypeOf<"orders">();
    expectTypeOf(inMemoryExample.ViewServerInMemoryProvider)
      .parameter(0)
      .toEqualTypeOf<ViewServerInMemoryProviderProps>();
  });
});
