import { describe, expect, it } from "@effect/vitest";
import { createTopicHealthLedger } from "./topic-health-ledger";

describe("column-live-view-engine topic health ledger", () => {
  it("tracks and guards subscription lifecycle totals", () => {
    const ledger = createTopicHealthLedger();

    const opened = { id: "opened" };
    const unknown = { id: "unknown" };

    expect(ledger.snapshot().activeSubscriptions).toBe(0);

    ledger.openSubscription(opened);
    ledger.openSubscription(opened);
    expect(ledger.snapshot()).toStrictEqual({
      activeSubscriptions: 1,
      queuedEvents: 0,
      maxQueueDepth: 0,
      backpressureEvents: 0,
    });

    ledger.updateQueueDepth(unknown, 3);
    expect(ledger.snapshot().queuedEvents).toBe(0);

    ledger.markBackpressure(unknown);
    expect(ledger.snapshot().backpressureEvents).toBe(0);

    ledger.updateQueueDepth(opened, 2);
    ledger.markBackpressure(opened);

    expect(ledger.snapshot()).toStrictEqual({
      activeSubscriptions: 1,
      queuedEvents: 2,
      maxQueueDepth: 2,
      backpressureEvents: 1,
    });

    ledger.updateQueueDepth(opened, 5);
    expect(ledger.snapshot()).toStrictEqual({
      activeSubscriptions: 1,
      queuedEvents: 5,
      maxQueueDepth: 5,
      backpressureEvents: 1,
    });

    ledger.closeSubscription(opened);
    expect(ledger.snapshot()).toStrictEqual({
      activeSubscriptions: 0,
      queuedEvents: 0,
      maxQueueDepth: 5,
      backpressureEvents: 1,
    });

    ledger.closeSubscription(opened);
    expect(ledger.snapshot()).toStrictEqual({
      activeSubscriptions: 0,
      queuedEvents: 0,
      maxQueueDepth: 5,
      backpressureEvents: 1,
    });

    ledger.reset();
    expect(ledger.snapshot()).toStrictEqual({
      activeSubscriptions: 0,
      queuedEvents: 0,
      maxQueueDepth: 0,
      backpressureEvents: 0,
    });
  });
});
