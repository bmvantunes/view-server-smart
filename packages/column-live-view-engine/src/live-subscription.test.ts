import { describe, expect, it } from "@effect/vitest";
import { Effect, Option, Schema } from "effect";
import type { LiveQueryExecution } from "./active-query";
import { makeLiveSubscription } from "./live-subscription";
import { makeTopicStoreSubscriptionPermit, TopicStore, topicStoreState } from "./topic-store-state";

const Row = Schema.Struct({
  id: Schema.String,
});

type Row = typeof Row.Type;

const emptyEvaluation = {
  rows: [],
  keys: [],
  window: [],
  totalRows: 0,
  version: 0,
};

describe("live subscription observability", () => {
  it.effect("runs subscriber notification under a named span with topic and query attributes", () =>
    Effect.gen(function* () {
      const store = new TopicStore("orders", Row, "id", () => {});
      const permit = makeTopicStoreSubscriptionPermit(store);
      let observedSpan: {
        readonly name: string;
        readonly queryId: unknown;
        readonly topic: unknown;
      } | null = null;
      const execution: LiveQueryExecution<Row> = {
        initial: (queryId) => ({
          type: "snapshot",
          topic: "orders",
          queryId,
          version: 0,
          keys: [],
          rows: [],
          totalRows: 0,
        }),
        createCursor: () => ({
          evaluation: emptyEvaluation,
        }),
        next: () =>
          Effect.gen(function* () {
            const span = yield* Effect.currentSpan.pipe(Effect.orDie);
            observedSpan = {
              name: span.name,
              queryId: span.attributes.get("queryId"),
              topic: span.attributes.get("topic"),
            };
            return Option.none();
          }),
      };

      const subscription = yield* makeLiveSubscription({
        queryId: "query-1",
        queueCapacity: 8,
        execution,
        permit,
        release: Effect.void,
      });
      const subscribers = Array.from(topicStoreState(store).subscribers);

      expect(subscribers).toHaveLength(1);
      for (const subscriber of subscribers) {
        yield* subscriber.notify(store);
      }
      expect(observedSpan).toStrictEqual({
        name: "ColumnLiveViewEngine.liveSubscription.notify",
        queryId: "query-1",
        topic: "orders",
      });

      yield* subscription.close();
    }),
  );
});
