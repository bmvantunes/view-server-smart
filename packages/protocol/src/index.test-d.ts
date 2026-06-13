import { describe, expectTypeOf, it } from "@effect/vitest";
import { defineViewServerConfig } from "@view-server/config";
import { Effect, Schema } from "effect";
import type * as Protocol from "./index";
import {
  viewServerDecodeHealthSummaryEvent,
  viewServerDecodeHealthTopicEvent,
  viewServerDecodeTrustedLiveEvent,
  viewServerEncodeHealthSummaryEvent,
  viewServerEncodeHealthTopicEvent,
} from "./index";

const TypeOrder = Schema.Struct({
  id: Schema.String,
});

const typeViewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: TypeOrder,
      key: "id",
    },
    trades: {
      schema: TypeOrder,
      key: "id",
    },
  },
});

declare const wireEvent: Protocol.ViewServerWireEvent;
declare const trustedWireEvent: Protocol.ViewServerTrustedWireEvent;

describe("@view-server/protocol type contract", () => {
  it("does not export transport-neutral live client contracts", () => {
    expectTypeOf<keyof typeof Protocol>().not.toEqualTypeOf<
      "ViewServerLiveClient" | "ViewServerLiveEvent" | "ViewServerLiveSubscription"
    >();

    // @ts-expect-error live client contracts belong to @view-server/client.
    expectTypeOf<Protocol.ViewServerLiveClient<never>>().toBeNever();
    // @ts-expect-error live event contracts belong to @view-server/client.
    expectTypeOf<Protocol.ViewServerLiveEvent<never>>().toBeNever();
    // @ts-expect-error live subscription contracts belong to @view-server/client.
    expectTypeOf<Protocol.ViewServerLiveSubscription<never>>().toBeNever();
  });

  it("types health event encoder inputs from configured topics", () => {
    const validSummaryEncode = viewServerEncodeHealthSummaryEvent(typeViewServer, {
      type: "snapshot",
      topic: "__view_server_health_summary",
      queryId: "health-summary",
      version: 1,
      keys: ["summary"],
      rows: [
        {
          id: "summary",
          status: "degraded",
          runtimeStatus: "degraded",
          connectionStatus: "connected",
          unhealthyTopics: ["orders"],
          updatedAtNanos: 1n,
          maxKafkaLag: 0n,
        },
      ],
      totalRows: 1,
    });
    expectTypeOf(validSummaryEncode).not.toBeAny();

    const invalidConnectedSummaryStatusEncode = viewServerEncodeHealthSummaryEvent(typeViewServer, {
      type: "snapshot",
      topic: "__view_server_health_summary",
      queryId: "health-summary",
      version: 1,
      keys: ["summary"],
      rows: [
        {
          id: "summary",
          // @ts-expect-error connected is a connectionStatus, not a merged health status.
          status: "connected",
          runtimeStatus: "ready",
          connectionStatus: "connected",
          unhealthyTopics: [],
          updatedAtNanos: 1n,
          maxKafkaLag: 0n,
        },
      ],
      totalRows: 1,
    });
    expectTypeOf(invalidConnectedSummaryStatusEncode).not.toBeAny();

    const invalidSummaryEmptyRowsEncode = viewServerEncodeHealthSummaryEvent(typeViewServer, {
      type: "snapshot",
      topic: "__view_server_health_summary",
      queryId: "health-summary",
      version: 1,
      keys: ["summary"],
      // @ts-expect-error health summary snapshots must contain exactly one row.
      rows: [],
      totalRows: 1,
    });
    expectTypeOf(invalidSummaryEmptyRowsEncode).not.toBeAny();

    const invalidSummaryTotalRowsEncode = viewServerEncodeHealthSummaryEvent(typeViewServer, {
      type: "snapshot",
      topic: "__view_server_health_summary",
      queryId: "health-summary",
      version: 1,
      keys: ["summary"],
      rows: [
        {
          id: "summary",
          status: "ready",
          runtimeStatus: "ready",
          connectionStatus: "connected",
          unhealthyTopics: [],
          updatedAtNanos: 1n,
          maxKafkaLag: 0n,
        },
      ],
      // @ts-expect-error health summary totalRows is always 1.
      totalRows: 2,
    });
    expectTypeOf(invalidSummaryTotalRowsEncode).not.toBeAny();

    const invalidSummaryTopicEncode = viewServerEncodeHealthSummaryEvent(typeViewServer, {
      type: "status",
      // @ts-expect-error health summary events must use the summary system topic.
      topic: "__view_server_health",
      queryId: "health-summary",
      status: "ready",
      code: "Ready",
    });
    expectTypeOf(invalidSummaryTopicEncode).not.toBeAny();

    const invalidSummaryKeyEncode = viewServerEncodeHealthSummaryEvent(typeViewServer, {
      type: "snapshot",
      topic: "__view_server_health_summary",
      queryId: "health-summary",
      version: 1,
      // @ts-expect-error health summary snapshot keys are always summary.
      keys: ["orders"],
      rows: [
        {
          id: "summary",
          status: "ready",
          runtimeStatus: "ready",
          connectionStatus: "connected",
          unhealthyTopics: [],
          updatedAtNanos: 1n,
          maxKafkaLag: 0n,
        },
      ],
      totalRows: 1,
    });
    expectTypeOf(invalidSummaryKeyEncode).not.toBeAny();

    const invalidSummaryDeltaKeyEncode = viewServerEncodeHealthSummaryEvent(typeViewServer, {
      type: "delta",
      topic: "__view_server_health_summary",
      queryId: "health-summary",
      fromVersion: 1,
      toVersion: 2,
      operations: [
        {
          type: "move",
          // @ts-expect-error health summary delta keys are always summary.
          key: "orders",
          fromIndex: 0,
          toIndex: 0,
        },
      ],
      totalRows: 1,
    });
    expectTypeOf(invalidSummaryDeltaKeyEncode).not.toBeAny();

    const invalidSummaryEncode = viewServerEncodeHealthSummaryEvent(typeViewServer, {
      type: "snapshot",
      topic: "__view_server_health_summary",
      queryId: "health-summary",
      version: 1,
      keys: ["summary"],
      rows: [
        {
          id: "summary",
          status: "degraded",
          runtimeStatus: "degraded",
          connectionStatus: "connected",
          // @ts-expect-error unknown unhealthy topics are rejected at compile time.
          unhealthyTopics: ["missing"],
          updatedAtNanos: 1n,
          maxKafkaLag: 0n,
        },
      ],
      totalRows: 1,
    });
    expectTypeOf(invalidSummaryEncode).not.toBeAny();

    const validTopicEncode = viewServerEncodeHealthTopicEvent(typeViewServer, {
      type: "delta",
      topic: "__view_server_health",
      queryId: "health-detail",
      fromVersion: 1,
      toVersion: 2,
      operations: [
        {
          type: "update",
          key: "orders",
          row: {
            id: "orders",
            status: "ready",
            rowCount: 1,
            liveRowCount: 1,
            deletedRowCount: 0,
            version: 1,
            lastMutationAt: null,
            mutationsPerSecond: 0,
            rowsPerSecond: 0,
            pendingMutationBatches: 0,
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: 0,
            activeViews: 0,
            groupedFullEvaluationCount: 0,
            groupedPatchedEvaluationCount: 0,
            activeSubscriptions: 0,
            queuedEvents: 0,
            maxQueueDepth: 0,
            backpressureEvents: 0,
            memoryBytes: 0,
            tombstoneCount: 0,
            compactionPending: false,
            kafkaLag: 0n,
            updatedAtNanos: 1n,
          },
          index: 0,
        },
      ],
      totalRows: 2,
    });
    expectTypeOf(validTopicEncode).not.toBeAny();

    const invalidTopicEventTopicEncode = viewServerEncodeHealthTopicEvent(typeViewServer, {
      type: "status",
      // @ts-expect-error health detail events must use the detail system topic.
      topic: "__view_server_health_summary",
      queryId: "health-detail",
      status: "ready",
      code: "Ready",
    });
    expectTypeOf(invalidTopicEventTopicEncode).not.toBeAny();

    const invalidTopicMoveKeyEncode = viewServerEncodeHealthTopicEvent(typeViewServer, {
      type: "delta",
      topic: "__view_server_health",
      queryId: "health-detail",
      fromVersion: 1,
      toVersion: 2,
      operations: [
        {
          type: "move",
          // @ts-expect-error health detail operation keys must be configured topics.
          key: "missing",
          fromIndex: 0,
          toIndex: 1,
        },
      ],
      totalRows: 2,
    });
    expectTypeOf(invalidTopicMoveKeyEncode).not.toBeAny();

    const mismatchedTopicDeltaRowEncode = viewServerEncodeHealthTopicEvent(typeViewServer, {
      type: "delta",
      topic: "__view_server_health",
      queryId: "health-detail",
      fromVersion: 1,
      toVersion: 2,
      operations: [
        {
          type: "update",
          key: "orders",
          row: {
            // @ts-expect-error health detail operation row ids must match operation keys.
            id: "trades",
            status: "ready",
            rowCount: 1,
            liveRowCount: 1,
            deletedRowCount: 0,
            version: 1,
            lastMutationAt: null,
            mutationsPerSecond: 0,
            rowsPerSecond: 0,
            pendingMutationBatches: 0,
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: 0,
            activeViews: 0,
            groupedFullEvaluationCount: 0,
            groupedPatchedEvaluationCount: 0,
            activeSubscriptions: 0,
            queuedEvents: 0,
            maxQueueDepth: 0,
            backpressureEvents: 0,
            memoryBytes: 0,
            tombstoneCount: 0,
            compactionPending: false,
            kafkaLag: 0n,
            updatedAtNanos: 1n,
          },
          index: 0,
        },
      ],
      totalRows: 2,
    });
    expectTypeOf(mismatchedTopicDeltaRowEncode).not.toBeAny();

    const invalidTopicEncode = viewServerEncodeHealthTopicEvent(typeViewServer, {
      type: "delta",
      topic: "__view_server_health",
      queryId: "health-detail",
      fromVersion: 1,
      toVersion: 2,
      operations: [
        {
          type: "update",
          key: "orders",
          row: {
            // @ts-expect-error unknown health topic rows are rejected at compile time.
            id: "missing",
            status: "ready",
            rowCount: 1,
            liveRowCount: 1,
            deletedRowCount: 0,
            version: 1,
            lastMutationAt: null,
            mutationsPerSecond: 0,
            rowsPerSecond: 0,
            pendingMutationBatches: 0,
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: 0,
            activeViews: 0,
            groupedFullEvaluationCount: 0,
            groupedPatchedEvaluationCount: 0,
            activeSubscriptions: 0,
            queuedEvents: 0,
            maxQueueDepth: 0,
            backpressureEvents: 0,
            memoryBytes: 0,
            tombstoneCount: 0,
            compactionPending: false,
            kafkaLag: 0n,
            updatedAtNanos: 1n,
          },
          index: 0,
        },
      ],
      totalRows: 2,
    });
    expectTypeOf(invalidTopicEncode).not.toBeAny();

    const invalidSummaryRemoveEncode = viewServerEncodeHealthSummaryEvent(typeViewServer, {
      type: "delta",
      topic: "__view_server_health_summary",
      queryId: "health-summary",
      fromVersion: 1,
      toVersion: 2,
      operations: [
        {
          // @ts-expect-error fixed-cardinality health summary deltas cannot remove the summary row.
          type: "remove",
          key: "summary",
        },
      ],
      totalRows: 1,
    });
    expectTypeOf(invalidSummaryRemoveEncode).not.toBeAny();

    const invalidSummaryInsertEncode = viewServerEncodeHealthSummaryEvent(typeViewServer, {
      type: "delta",
      topic: "__view_server_health_summary",
      queryId: "health-summary",
      fromVersion: 1,
      toVersion: 2,
      operations: [
        {
          // @ts-expect-error fixed-cardinality health summary deltas cannot insert the summary row.
          type: "insert",
          key: "summary",
          row: {
            id: "summary",
            status: "ready",
            runtimeStatus: "ready",
            connectionStatus: "connected",
            unhealthyTopics: [],
            updatedAtNanos: 1n,
            maxKafkaLag: 0n,
          },
          index: 0,
        },
      ],
      totalRows: 1,
    });
    expectTypeOf(invalidSummaryInsertEncode).not.toBeAny();

    const invalidTopicRemoveEncode = viewServerEncodeHealthTopicEvent(typeViewServer, {
      type: "delta",
      topic: "__view_server_health",
      queryId: "health-detail",
      fromVersion: 1,
      toVersion: 2,
      operations: [
        {
          // @ts-expect-error fixed-cardinality health detail deltas cannot remove configured topics.
          type: "remove",
          key: "orders",
        },
      ],
      totalRows: 2,
    });
    expectTypeOf(invalidTopicRemoveEncode).not.toBeAny();

    const invalidTopicInsertEncode = viewServerEncodeHealthTopicEvent(typeViewServer, {
      type: "delta",
      topic: "__view_server_health",
      queryId: "health-detail",
      fromVersion: 1,
      toVersion: 2,
      operations: [
        {
          // @ts-expect-error fixed-cardinality health detail deltas cannot insert configured topics.
          type: "insert",
          key: "orders",
          row: {
            id: "orders",
            status: "ready",
            rowCount: 1,
            liveRowCount: 1,
            deletedRowCount: 0,
            version: 1,
            lastMutationAt: null,
            mutationsPerSecond: 0,
            rowsPerSecond: 0,
            pendingMutationBatches: 0,
            activeFallbackGroupedViews: 0,
            activeIncrementalGroupedViews: 0,
            activeViews: 0,
            groupedFullEvaluationCount: 0,
            groupedPatchedEvaluationCount: 0,
            activeSubscriptions: 0,
            queuedEvents: 0,
            maxQueueDepth: 0,
            backpressureEvents: 0,
            memoryBytes: 0,
            tombstoneCount: 0,
            compactionPending: false,
            kafkaLag: 0n,
            updatedAtNanos: 1n,
          },
          index: 0,
        },
      ],
      totalRows: 2,
    });
    expectTypeOf(invalidTopicInsertEncode).not.toBeAny();
  });

  it("requires schema-validated event proof for trusted live event decoding", () => {
    const validTrustedDecode = viewServerDecodeTrustedLiveEvent(
      typeViewServer,
      "orders",
      { select: ["id"] },
      trustedWireEvent,
    );
    expectTypeOf(validTrustedDecode).not.toBeAny();

    const invalidTrustedDecode = viewServerDecodeTrustedLiveEvent(
      typeViewServer,
      "orders",
      { select: ["id"] },
      // @ts-expect-error trusted decoder requires ViewServerTrustedWireEvent.
      wireEvent,
    );
    expectTypeOf(invalidTrustedDecode).not.toBeAny();
  });

  it("preserves health event decoder output generics", () => {
    const summaryDecode = viewServerDecodeHealthSummaryEvent(typeViewServer, wireEvent);
    type SummaryEvent = Effect.Success<typeof summaryDecode>;
    type SummarySnapshot = Extract<SummaryEvent, { readonly type: "snapshot" }>;
    type SummaryDeltaOperation = Extract<
      SummaryEvent,
      { readonly type: "delta" }
    >["operations"][number];
    expectTypeOf<SummarySnapshot["topic"]>().toEqualTypeOf<"__view_server_health_summary">();
    expectTypeOf<SummarySnapshot["keys"]>().toEqualTypeOf<readonly ["summary"]>();
    expectTypeOf<SummarySnapshot["rows"][0]["id"]>().toEqualTypeOf<"summary">();
    expectTypeOf<SummarySnapshot["rows"][0]["unhealthyTopics"][number]>().toEqualTypeOf<
      "orders" | "trades"
    >();
    expectTypeOf<SummarySnapshot["totalRows"]>().toEqualTypeOf<1>();
    expectTypeOf<SummaryDeltaOperation["key"]>().toEqualTypeOf<"summary">();
    expectTypeOf<
      Extract<SummaryDeltaOperation, { readonly type: "insert" }>
    >().toEqualTypeOf<never>();
    expectTypeOf<
      Extract<SummaryDeltaOperation, { readonly type: "remove" }>
    >().toEqualTypeOf<never>();

    const topicDecode = viewServerDecodeHealthTopicEvent(typeViewServer, wireEvent);
    type TopicEvent = Effect.Success<typeof topicDecode>;
    type TopicSnapshot = Extract<TopicEvent, { readonly type: "snapshot" }>;
    type TopicDeltaOperation = Extract<
      TopicEvent,
      { readonly type: "delta" }
    >["operations"][number];
    expectTypeOf<TopicSnapshot["topic"]>().toEqualTypeOf<"__view_server_health">();
    expectTypeOf<TopicSnapshot["keys"][number]>().toEqualTypeOf<"orders" | "trades">();
    expectTypeOf<TopicSnapshot["rows"][number]["id"]>().toEqualTypeOf<"orders" | "trades">();
    expectTypeOf<TopicDeltaOperation["key"]>().toEqualTypeOf<"orders" | "trades">();
    expectTypeOf<
      Extract<TopicDeltaOperation, { readonly type: "insert" }>
    >().toEqualTypeOf<never>();
    expectTypeOf<
      Extract<TopicDeltaOperation, { readonly type: "remove" }>
    >().toEqualTypeOf<never>();
  });
});
