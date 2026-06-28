import { describe, expectTypeOf, it } from "@effect/vitest";
import type { LiveQueryResult } from "@view-server/config";
import { useLiveQuery } from "./view-server.config";

describe("TCP publisher example type contracts", () => {
  it("preserves selected order row types", () => {
    const result = useLiveQuery("orders", {
      select: ["id", "customerId", "price"],
      limit: 20,
    });

    expectTypeOf(result).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly customerId: string;
        readonly price: number;
      }>
    >();
  });
});
