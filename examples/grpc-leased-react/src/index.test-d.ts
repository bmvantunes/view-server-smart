import { describe, expectTypeOf, it } from "@effect/vitest";
import type {
  ExactLiveQueryInputForTopic,
  LiveQueryResult,
  RawQuery,
  TopicRow,
} from "@view-server/config";
import { grpcClients, useLiveQuery, viewServer } from "./view-server.config";

describe("leased gRPC example type contracts", () => {
  it("requires leased route filters and preserves selected row types", () => {
    const result = useLiveQuery("orders", {
      select: ["id", "strategyId", "region"],
      where: {
        strategyId: { eq: "strategy-alpha" },
        region: { eq: "usa" },
      },
      limit: 20,
    });

    expectTypeOf(result).toEqualTypeOf<
      LiveQueryResult<{
        readonly id: string;
        readonly strategyId: string;
        readonly region: string;
      }>
    >();
  });

  it("rejects leased queries missing required route fields", () => {
    const missingRouteQuery = {
      select: ["id", "region"],
      where: { region: { eq: "usa" } },
      limit: 20,
    } satisfies {
      readonly select: readonly ["id", "region"];
      readonly where: {
        readonly region: {
          readonly eq: "usa";
        };
      };
      readonly limit: 20;
    };
    type Topics = typeof viewServer.topics;
    // @ts-expect-error leased gRPC order queries must include the strategyId route filter.
    const invalidRouteQuery: RawQuery<TopicRow<Topics, "orders">> &
      ExactLiveQueryInputForTopic<Topics, "orders", typeof missingRouteQuery> = missingRouteQuery;

    expectTypeOf(invalidRouteQuery).not.toBeAny();
  });

  it("keeps the generated gRPC client descriptor typed", () => {
    expectTypeOf(
      grpcClients.orders.service.method.streamOrders.methodKind,
    ).toEqualTypeOf<"server_streaming">();
  });
});
