import { describe, expectTypeOf, it } from "@effect/vitest";
import type { LiveQueryResult } from "@view-server/config";
import { kafkaTopics, useLiveQuery } from "./view-server.config";

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

  it("keeps the Kafka mapping typed", () => {
    expectTypeOf(
      kafkaTopics["view-server-example-orders"].viewServerTopic,
    ).toEqualTypeOf<"orders">();
  });
});
