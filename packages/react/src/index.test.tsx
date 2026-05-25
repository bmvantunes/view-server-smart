import { describe, expect, it } from "@effect/vitest";
import { defineViewServerConfig } from "@view-server/config";
import { Cause, Deferred, Effect, Schema } from "effect";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import { render } from "vitest-browser-react";
import { liveQueryFailureResult } from "./hook-error";
import { liveQueryResultFromAsyncResult } from "./hook-result";
import { createViewServerReact } from "./index";
import { makeHealthRefreshScheduler } from "./in-memory-runtime";
import { applyEvent, initialClientState, type ClientState } from "./live-query-state";
import { stableQueryKey } from "./query-key";

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  quantity: Schema.BigInt,
  price: Schema.Number,
  region: Schema.String,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
    trades: {
      schema: Trade,
      key: "id",
    },
  },
});

const { createInMemoryViewServer, useLiveQuery, useViewServerHealth } =
  createViewServerReact(viewServer);

type OrderRow = typeof Order.Type;

const order = (id: string, price: number): OrderRow => ({
  id,
  customerId: `customer-${id}`,
  status: "open",
  price,
  region: "usa",
  updatedAt: price,
});

describe("createViewServerReact", () => {
  it("applies remove delta operations in the client reducer", () => {
    const state = applyEvent(
      {
        ...initialClientState<{ readonly id: string }>(),
        rows: [{ id: "a" }, { id: "b" }],
        keys: ["a", "b"],
        totalRows: 2,
        version: 1,
        status: "ready",
      },
      {
        type: "delta",
        topic: "orders",
        queryId: "query-1",
        fromVersion: 1,
        toVersion: 2,
        totalRows: 1,
        operations: [{ type: "remove", key: "a" }],
      },
    );

    expect(state.rows).toEqual([{ id: "b" }]);
    expect(state.keys).toEqual(["b"]);
    expect(state.totalRows).toBe(1);
  });

  it("streams runtime-published snapshots and live deltas in browser providers", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function OrdersView() {
      const result = useLiveQuery("orders", {
        orderBy: [{ field: "price", direction: "asc" }],
        select: ["id", "price"],
        limit: 10,
      });
      return (
        <output aria-label="orders" role="status">
          {result.rows.map((row) => `${row.id}:${row.price}`).join("|")}
        </output>
      );
    }
    function HealthView() {
      const health = useViewServerHealth();
      return (
        <output aria-label="health" role="status">
          {health.engine.topics.orders.rowCount}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
        <HealthView />
      </ViewServerInMemoryProvider>,
    );
    const orders = view.getByRole("status", { name: "orders" });
    const health = view.getByRole("status", { name: "health" });
    await expect.element(orders).toHaveTextContent("");

    await Effect.runPromise(client.publishMany("orders", [order("b", 20), order("a", 10)]));

    await expect.element(orders).toHaveTextContent("a:10|b:20");
    await expect.element(health).toHaveTextContent("2");

    await Effect.runPromise(client.publish("orders", order("c", 5)));

    await expect.element(orders).toHaveTextContent("c:5|a:10|b:20");
    await expect.element(health).toHaveTextContent("3");
    await view.unmount();
  });

  it("closes live subscriptions when browser components unmount", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="orders" role="status">
          {result.rows.map((row) => row.id).join("|")}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    const orders = view.getByRole("status", { name: "orders" });
    await expect.element(orders).toHaveTextContent("");

    await Effect.runPromise(client.publish("orders", order("a", 10)));
    await expect.element(orders).toHaveTextContent("a");

    await view.rerender(<ViewServerInMemoryProvider></ViewServerInMemoryProvider>);

    await expect
      .poll(async () => {
        const health = await Effect.runPromise(client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(0);
    await view.unmount();
  });

  it("keeps the in-memory engine open while a mounted provider has no hook consumers", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="orders" role="status">
          {result.rows.map((row) => `${row.id}:${row.price}`).join("|")}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    const orders = view.getByRole("status", { name: "orders" });

    await Effect.runPromise(client.publish("orders", order("a", 10)));
    await expect.element(orders).toHaveTextContent("a:10");

    await view.rerender(<ViewServerInMemoryProvider></ViewServerInMemoryProvider>);
    await expect
      .poll(async () => {
        const health = await Effect.runPromise(client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(0);

    await Effect.runPromise(client.publish("orders", order("b", 20)));

    await view.rerender(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    await expect.element(orders).toHaveTextContent("a:10|b:20");
    await view.unmount();
  });

  it("applies update, move, remove, patch, snapshot, and reset paths", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function OrdersView() {
      const result = useLiveQuery("orders", {
        orderBy: [{ field: "price", direction: "asc" }],
        select: ["id", "price"],
        limit: 10,
      });
      return (
        <output aria-label="orders" role="status">
          {result.rows.map((row) => `${row.id}:${row.price}`).join("|")}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    const orders = view.getByRole("status", { name: "orders" });
    await expect.element(orders).toHaveTextContent("");

    await Effect.runPromise(client.publishMany("orders", [order("a", 10), order("b", 20)]));
    await expect.element(orders).toHaveTextContent("a:10|b:20");

    await Effect.runPromise(client.publish("orders", order("a", 30)));
    await expect.element(orders).toHaveTextContent("b:20|a:30");

    await Effect.runPromise(client.patch("orders", "a", { price: 5 }));
    await expect.element(orders).toHaveTextContent("a:5|b:20");

    await Effect.runPromise(client.delete("orders", "a"));
    await expect.element(orders).toHaveTextContent("b:20");

    const snapshot = await Effect.runPromise(
      client.snapshot("orders", {
        select: ["id", "price"],
        limit: 10,
      }),
    );
    expect(snapshot.rows).toEqual([{ id: "b", price: 20 }]);

    await Effect.runPromise(client.reset());
    expect((await Effect.runPromise(client.health())).engine.topics.orders.rowCount).toBe(0);
    await expect.element(orders).toHaveTextContent("");
    await view.unmount();
  });

  it("coalesces in-memory health refreshes under concurrent publishes", async () => {
    const { client } = createInMemoryViewServer();

    await Promise.all(
      Array.from({ length: 50 }, (_, index) =>
        Effect.runPromise(client.publish("orders", order(`coalesced-${index}`, index))),
      ),
    );

    await expect
      .poll(async () => {
        const health = await Effect.runPromise(client.health());
        return health.engine.topics.orders.rowCount;
      })
      .toBe(50);
  });

  it("queues a trailing health scheduler refresh when requested while refresh is pending", async () => {
    const firstStarted = await Effect.runPromise(Deferred.make<void>());
    const firstFinished = await Effect.runPromise(Deferred.make<void>());
    const secondStarted = await Effect.runPromise(Deferred.make<void>());
    const secondFinished = await Effect.runPromise(Deferred.make<void>());
    let refreshCount = 0;

    const requestRefresh = makeHealthRefreshScheduler(
      Effect.gen(function* () {
        const currentRefresh = yield* Effect.sync(() => {
          refreshCount += 1;
          return refreshCount;
        });
        if (currentRefresh === 1) {
          yield* Deferred.succeed(firstStarted, undefined);
          yield* Deferred.await(firstFinished);
          return;
        }
        yield* Deferred.succeed(secondStarted, undefined);
        yield* Deferred.await(secondFinished);
      }),
    );

    await Effect.runPromise(requestRefresh);
    await Effect.runPromise(Deferred.await(firstStarted));

    await Effect.runPromise(requestRefresh);
    await Effect.runPromise(Deferred.succeed(firstFinished, undefined));
    await Effect.runPromise(Deferred.await(secondStarted));

    expect(refreshCount).toBe(2);
    await Effect.runPromise(Deferred.succeed(secondFinished, undefined));
  });

  it("surfaces live query failures as error results", async () => {
    const { ViewServerInMemoryProvider } = createInMemoryViewServer();

    function BrokenOrdersView() {
      const result = useLiveQuery("orders", {
        // @ts-expect-error invalid selected fields are still surfaced through the hook result.
        select: ["prcie"],
      });
      return (
        <output aria-label="orders" role="status">
          {result.status}:{result.statusCode}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <BrokenOrdersView />
      </ViewServerInMemoryProvider>,
    );
    const orders = view.getByRole("status", { name: "orders" });

    await expect.element(orders).toHaveTextContent("error:SnapshotStale");
    await view.unmount();
  });

  it("distinguishes non-plain object query values in stable query keys", () => {
    class FilterValue {
      readonly label = "same";
    }

    const firstFilter = new FilterValue();
    const secondFilter = new FilterValue();

    expect(stableQueryKey({ where: { custom: { eq: firstFilter } } })).toBe(
      stableQueryKey({ where: { custom: { eq: firstFilter } } }),
    );
    expect(stableQueryKey({ where: { custom: { eq: firstFilter } } })).not.toBe(
      stableQueryKey({ where: { custom: { eq: secondFilter } } }),
    );
    expect(
      stableQueryKey({
        where: {
          custom: {
            eq: new Map<string, number>([
              ["b", 1],
              ["a", 2],
            ]),
          },
        },
      }),
    ).toBe(
      stableQueryKey({
        where: {
          custom: {
            eq: new Map<string, number>([
              ["a", 2],
              ["b", 1],
            ]),
          },
        },
      }),
    );
    expect(stableQueryKey({ where: { custom: { eq: new Set(["b", "a"]) } } })).toBe(
      stableQueryKey({ where: { custom: { eq: new Set(["a", "b"]) } } }),
    );
  });

  it("preserves typed hook failure status codes", () => {
    for (const code of [
      "Ready",
      "SnapshotStale",
      "SubscriptionClosed",
      "TransportError",
      "BackpressureExceeded",
      "InvalidTopic",
      "InvalidRow",
      "RuntimeUnavailable",
      "RuntimeResetFailed",
    ]) {
      expect(
        liveQueryFailureResult<never>(
          Cause.fail({
            _tag: "ViewServerRuntimeError",
            code,
            message: code,
          }),
        ),
      ).toMatchObject({
        status: "error",
        statusCode: code,
        message: code,
      });
    }
    expect(
      liveQueryFailureResult<never>(
        Cause.fail({
          _tag: "ViewServerRuntimeError",
          code: "InvalidRow",
          message: "invalid row",
        }),
      ),
    ).toMatchObject({
      status: "error",
      statusCode: "InvalidRow",
      message: "invalid row",
    });
    expect(
      liveQueryFailureResult<never>(
        Cause.fail({
          _tag: "UnknownFailure",
          code: "UnexpectedCode",
          message: "unexpected",
        }),
      ),
    ).toMatchObject({
      status: "error",
      statusCode: "TransportError",
      message: "unexpected",
    });
    expect(liveQueryFailureResult<never>(Cause.fail("plain failure"))).toMatchObject({
      status: "error",
      statusCode: "TransportError",
      message: "plain failure",
    });
    expect(liveQueryFailureResult<never>(Cause.fail({ code: "InvalidRow" }))).toMatchObject({
      status: "error",
      statusCode: "TransportError",
      message: "[object Object]",
    });
  });

  it("maps async atom lifecycle states into live query results", () => {
    expect(liveQueryResultFromAsyncResult(AsyncResult.initial())).toMatchObject({
      status: "loading",
      rows: [],
      totalRows: 0,
    });
    expect(
      liveQueryResultFromAsyncResult(
        AsyncResult.success({
          ...initialClientState<{ readonly id: string }>(),
          rows: [{ id: "a" }],
          keys: ["a"],
          totalRows: 1,
          version: 1,
          status: "ready",
        }),
      ),
    ).toMatchObject({
      status: "ready",
      rows: [{ id: "a" }],
      totalRows: 1,
    });
    expect(
      liveQueryResultFromAsyncResult(
        AsyncResult.failure<ClientState<{ readonly id: string }>, string>(Cause.fail("boom")),
      ),
    ).toMatchObject({
      status: "error",
      statusCode: "TransportError",
      message: "boom",
    });
  });

  it("maps runtime errors", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    const view = await render(<ViewServerInMemoryProvider></ViewServerInMemoryProvider>);

    await Effect.runPromise(client.publish("orders", order("a", 10)));

    const invalidTopic = await Effect.runPromiseExit(
      // @ts-expect-error hostile runtime callers can still send unknown topics.
      client.publish("missing", order("b", 20)),
    );
    const invalidRow = await Effect.runPromiseExit(
      client.publish("orders", {
        id: "bad",
        customerId: "customer-bad",
        // @ts-expect-error hostile runtime callers can still send malformed rows.
        status: "unknown",
        price: 20,
        region: "usa",
        updatedAt: 20,
      }),
    );
    const groupedSnapshot = await Effect.runPromiseExit(
      client.snapshot("orders", {
        // @ts-expect-error grouped queries are rejected by the raw in-memory runtime slice.
        groupBy: ["status"],
        // @ts-expect-error grouped queries are rejected by the raw in-memory runtime slice.
        aggregates: { count: { aggFunc: "count" } },
      }),
    );
    const invalidQuery = await Effect.runPromiseExit(
      client.snapshot("orders", {
        // @ts-expect-error hostile runtime callers can still send unknown projected fields.
        select: ["prcie"],
      }),
    );

    expect(invalidTopic._tag).toBe("Failure");
    expect(invalidRow._tag).toBe("Failure");
    expect(groupedSnapshot._tag).toBe("Failure");
    expect(invalidQuery._tag).toBe("Failure");
    await view.unmount();
  });

  it("keeps query memoization safe for bigint query values", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function TradesView() {
      const result = useLiveQuery("trades", {
        where: {
          quantity: { gte: 10n },
        },
        select: ["id", "quantity"],
        limit: 10,
      });
      return (
        <output aria-label="trades" role="status">
          {result.rows.map((row) => `${row.id}:${row.quantity}`).join("|")}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <TradesView />
      </ViewServerInMemoryProvider>,
    );
    const trades = view.getByRole("status", { name: "trades" });
    await expect.element(trades).toHaveTextContent("");

    await Effect.runPromise(
      client.publishMany("trades", [
        { id: "a", symbol: "AAPL", quantity: 5n, price: 100, region: "usa" },
        { id: "b", symbol: "MSFT", quantity: 10n, price: 200, region: "usa" },
      ]),
    );

    await expect.element(trades).toHaveTextContent("b:10");
    await view.unmount();
  });

  it("surfaces runtime unavailable after provider disposal", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function HealthView() {
      const health = useViewServerHealth();
      return (
        <output aria-label="health" role="status">
          {health.status}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <HealthView />
      </ViewServerInMemoryProvider>,
    );
    await expect.element(view.getByRole("status", { name: "health" })).toHaveTextContent("ready");

    await view.unmount();
    await expect
      .poll(async () => {
        const exit = await Effect.runPromiseExit(client.publish("orders", order("a", 10)));
        return exit._tag;
      })
      .toBe("Failure");
  });

  it("surfaces status events from bounded subscription queues", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer({
      subscriptionQueueCapacity: 1,
    });

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="orders" role="status">
          {result.status}:{result.statusCode}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    const orders = view.getByRole("status", { name: "orders" });

    await Effect.runPromise(client.publish("orders", order("a", 10)));
    await expect.element(orders).toHaveTextContent("ready:Ready");

    for (let index = 0; index < 50; index += 1) {
      await Effect.runPromise(client.publish("orders", order(`burst-${index}`, index)));
    }

    expect((await Effect.runPromise(client.health())).transport.backpressureEvents).toBe(1);
    await expect.element(orders).toHaveTextContent("closed:BackpressureExceeded");
    await view.unmount();
  });
});
