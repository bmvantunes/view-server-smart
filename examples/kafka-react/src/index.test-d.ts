import { describe, expectTypeOf, it } from "@effect/vitest";
import type { LiveQueryResult } from "effect-view-server/config";
import { useLiveQuery, viewServer } from "./view-server.config";

describe("kafka example type contracts", () => {
  it("preserves selected Kafka-backed order row types", () => {
    const result = useLiveQuery("orders", {
      select: ["id", "region", "price"],
      where: { status: { eq: "open" } },
      limit: 20,
    });

    expectTypeOf(result).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly region: string;
        readonly price: number;
      }>
    >();
  });

  it("preserves selected Kafka-backed trade row types", () => {
    const result = useLiveQuery("trades", {
      select: ["id", "symbol", "side", "region"],
      where: { side: { eq: "buy" } },
      limit: 20,
    });

    expectTypeOf(result).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly symbol: string;
        readonly side: "buy" | "sell";
        readonly region: string;
      }>
    >();
  });

  it("keeps the Kafka mapping typed", () => {
    expectTypeOf(
      viewServer.topics.orders.kafkaSource.topic,
    ).toEqualTypeOf<"view-server-example-orders-usa">();
    expectTypeOf(viewServer.topics.orders.kafkaSource.regions).toEqualTypeOf<readonly ["usa"]>();
    expectTypeOf(
      viewServer.topics.trades.kafkaSource.topic,
    ).toEqualTypeOf<"view-server-example-trades-london">();
    expectTypeOf(viewServer.topics.trades.kafkaSource.regions).toEqualTypeOf<readonly ["london"]>();
  });
});
