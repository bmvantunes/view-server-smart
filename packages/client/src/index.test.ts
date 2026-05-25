import { describe, expect, it } from "@effect/vitest";
import { Cause } from "effect";
import { fromStringUnsafe } from "effect/BigDecimal";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import {
  applyEvent,
  initialClientState,
  liveQueryFailureResult,
  liveQueryResult,
  liveQueryResultFromAsyncResult,
  stableQueryKey,
  type ClientState,
} from "./index";

describe("@view-server/client", () => {
  it("applies snapshot, delta, and status events", () => {
    const snapshotState = applyEvent(initialClientState<{ readonly id: string }>(), {
      type: "snapshot",
      topic: "orders",
      queryId: "query-1",
      version: 1,
      keys: ["a", "b"],
      rows: [{ id: "a" }, { id: "b" }],
      totalRows: 2,
    });

    const inserted = applyEvent(snapshotState, {
      type: "delta",
      topic: "orders",
      queryId: "query-1",
      fromVersion: 1,
      toVersion: 2,
      totalRows: 3,
      operations: [{ type: "insert", key: "c", row: { id: "c" }, index: 1 }],
    });
    const updated = applyEvent(inserted, {
      type: "delta",
      topic: "orders",
      queryId: "query-1",
      fromVersion: 2,
      toVersion: 3,
      totalRows: 3,
      operations: [{ type: "update", key: "c", row: { id: "c-updated" }, index: 1 }],
    });
    const moved = applyEvent(updated, {
      type: "delta",
      topic: "orders",
      queryId: "query-1",
      fromVersion: 3,
      toVersion: 4,
      totalRows: 3,
      operations: [{ type: "move", key: "a", fromIndex: 0, toIndex: 2 }],
    });
    const removed = applyEvent(moved, {
      type: "delta",
      topic: "orders",
      queryId: "query-1",
      fromVersion: 4,
      toVersion: 5,
      totalRows: 2,
      operations: [{ type: "remove", key: "c" }],
    });

    expect(liveQueryResult(snapshotState)).toMatchObject({
      status: "ready",
      rows: [{ id: "a" }, { id: "b" }],
      totalRows: 2,
      version: 1,
    });
    expect(inserted.rows).toEqual([{ id: "a" }, { id: "c" }, { id: "b" }]);
    expect(updated.rows).toEqual([{ id: "a" }, { id: "c-updated" }, { id: "b" }]);
    expect(moved.rows).toEqual([{ id: "c-updated" }, { id: "b" }, { id: "a" }]);
    expect(removed.rows).toEqual([{ id: "b" }, { id: "a" }]);

    expect(
      applyEvent(removed, {
        type: "status",
        topic: "orders",
        queryId: "query-1",
        status: "stale",
        code: "SnapshotStale",
        message: "refreshing",
      }),
    ).toMatchObject({
      rows: removed.rows,
      status: "stale",
      statusCode: "SnapshotStale",
      message: "refreshing",
    });
    expect(
      applyEvent(removed, {
        type: "status",
        topic: "orders",
        queryId: "query-1",
        status: "closed",
        code: "SubscriptionClosed",
      }),
    ).toMatchObject({
      rows: [],
      status: "closed",
      statusCode: "SubscriptionClosed",
    });
    expect(
      applyEvent(removed, {
        type: "status",
        topic: "orders",
        queryId: "query-1",
        status: "closed",
        code: "BackpressureExceeded",
      }),
    ).toMatchObject({
      rows: [],
      keys: [],
      totalRows: 0,
      status: "closed",
      statusCode: "BackpressureExceeded",
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

  it("preserves typed failure status codes", () => {
    for (const code of [
      "Ready",
      "SnapshotStale",
      "SubscriptionClosed",
      "TransportError",
      "BackpressureExceeded",
      "InvalidTopic",
      "InvalidRow",
      "InvalidQuery",
      "UnsupportedQuery",
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

  it("builds stable query keys without JSON collisions", () => {
    const symbol = Symbol("filter");
    const firstFunction = () => undefined;
    const secondFunction = () => undefined;
    const anonymousFunction = (() =>
      function () {
        return undefined;
      })();

    expect(stableQueryKey({ value: undefined })).not.toBe(stableQueryKey({ value: null }));
    expect(stableQueryKey({ b: 1, a: 2 })).toBe(stableQueryKey({ a: 2, b: 1 }));
    expect(stableQueryKey({ value: Symbol("filter") })).not.toBe(
      stableQueryKey({ value: Symbol("filter") }),
    );
    expect(stableQueryKey({ value: symbol })).toBe(stableQueryKey({ value: symbol }));
    expect(stableQueryKey({ value: firstFunction })).toBe(stableQueryKey({ value: firstFunction }));
    expect(stableQueryKey({ value: firstFunction })).not.toBe(
      stableQueryKey({ value: secondFunction }),
    );
    expect(stableQueryKey({ value: anonymousFunction })).toBe(
      stableQueryKey({ value: anonymousFunction }),
    );
    expect(stableQueryKey({ value: Number.NaN })).not.toBe(stableQueryKey({ value: null }));
    expect(stableQueryKey({ value: Number.NaN })).not.toBe(
      stableQueryKey({ value: Number.POSITIVE_INFINITY }),
    );
    expect(stableQueryKey({ value: -0 })).not.toBe(stableQueryKey({ value: 0 }));
    expect(stableQueryKey({ value: true })).not.toBe(stableQueryKey({ value: "true" }));
  });

  it("supports maps, sets, bigint values, non-plain objects, and cycles", () => {
    class FilterValue {
      readonly label = "same";
    }

    const firstFilter = new FilterValue();
    const secondFilter = new FilterValue();
    const recursiveObject: { self?: unknown } = {};
    recursiveObject.self = recursiveObject;
    const recursiveArray: Array<unknown> = [];
    recursiveArray.push(recursiveArray);
    const recursiveMap = new Map<unknown, unknown>();
    recursiveMap.set(recursiveMap, "self");
    const recursiveSet = new Set<unknown>();
    recursiveSet.add(recursiveSet);
    const objectWithoutFunctionConstructor = Object.create({ constructor: { name: "nope" } });

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
            eq: new Map<unknown, unknown>([
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
            eq: new Map<unknown, unknown>([
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
    expect(stableQueryKey({ value: 10n })).toBe(stableQueryKey({ value: 10n }));
    expect(stableQueryKey({ where: { price: { eq: fromStringUnsafe("1.50") } } })).toBe(
      stableQueryKey({ where: { price: { eq: fromStringUnsafe("1.5") } } }),
    );
    expect(stableQueryKey({ where: { price: { eq: fromStringUnsafe("1.50") } } })).not.toBe(
      stableQueryKey({ where: { price: { eq: fromStringUnsafe("2") } } }),
    );
    expect(stableQueryKey({ value: recursiveObject })).toBe(
      stableQueryKey({ value: recursiveObject }),
    );
    expect(stableQueryKey({ value: recursiveArray })).toBe(
      stableQueryKey({ value: recursiveArray }),
    );
    expect(stableQueryKey({ value: recursiveMap })).toBe(stableQueryKey({ value: recursiveMap }));
    expect(stableQueryKey({ value: recursiveSet })).toBe(stableQueryKey({ value: recursiveSet }));
    expect(stableQueryKey({ value: objectWithoutFunctionConstructor })).toBe(
      stableQueryKey({ value: objectWithoutFunctionConstructor }),
    );
  });
});
