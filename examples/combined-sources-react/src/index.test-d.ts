import { describe, expectTypeOf, it } from "@effect/vitest";
import type { LiveQueryResult } from "@view-server/config";
import { kafkaTopics, useLiveQuery } from "./view-server.config";

describe("combined sources example type contracts", () => {
  it("types Kafka, leased gRPC, and materialized gRPC topics independently", () => {
    const orders = useLiveQuery("orders", {
      select: ["id", "strategyId", "region"],
      where: {
        strategyId: { eq: "strategy-alpha" },
        region: { eq: "usa" },
      },
      limit: 10,
    });
    const strategies = useLiveQuery("strategies", {
      select: ["id", "notional"],
      where: { status: { eq: "active" } },
      limit: 10,
    });
    const trades = useLiveQuery("trades", {
      select: ["id", "symbol"],
      limit: 10,
    });

    expectTypeOf(orders).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly strategyId: string;
        readonly region: string;
      }>
    >();
    expectTypeOf(strategies).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly notional: number;
      }>
    >();
    expectTypeOf(trades).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly symbol: string;
      }>
    >();
  });

  it("keeps Kafka ownership separate from gRPC source topics", () => {
    expectTypeOf(
      kafkaTopics["view-server-example-trades"].viewServerTopic,
    ).toEqualTypeOf<"trades">();
  });
});
