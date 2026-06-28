import { describe, expectTypeOf, it } from "@effect/vitest";
import type { LiveQueryResult } from "@view-server/config";
import { grpcClients, useLiveQuery } from "./view-server.config";

describe("materialized gRPC example type contracts", () => {
  it("preserves selected strategy row types", () => {
    const result = useLiveQuery("strategies", {
      select: ["id", "strategyId", "notional"],
      where: { status: { eq: "active" } },
      limit: 20,
    });

    expectTypeOf(result).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly strategyId: string;
        readonly notional: number;
      }>
    >();
  });

  it("keeps the generated gRPC client descriptor typed", () => {
    expectTypeOf(
      grpcClients.strategies.service.method.streamStrategies.methodKind,
    ).toEqualTypeOf<"server_streaming">();
  });
});
