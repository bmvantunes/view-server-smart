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
    expect(inserted.rows).toStrictEqual([{ id: "a" }, { id: "c" }, { id: "b" }]);
    expect(updated.rows).toStrictEqual([{ id: "a" }, { id: "c-updated" }, { id: "b" }]);
    expect(moved.rows).toStrictEqual([{ id: "c-updated" }, { id: "b" }, { id: "a" }]);
    expect(removed.rows).toStrictEqual([{ id: "b" }, { id: "a" }]);

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

  it("marks malformed snapshots stale", () => {
    const previous = applyEvent(initialClientState<{ readonly id: string }>(), {
      type: "snapshot",
      topic: "orders",
      queryId: "query-invalid-snapshot",
      version: 1,
      keys: ["a"],
      rows: [{ id: "a" }],
      totalRows: 1,
    });
    const fractionalVersion = applyEvent(initialClientState<{ readonly id: string }>(), {
      type: "snapshot",
      topic: "orders",
      queryId: "query-invalid-snapshot",
      version: 1.5,
      keys: ["a"],
      rows: [{ id: "a" }],
      totalRows: 1,
    });
    const negativeTotalRows = applyEvent(initialClientState<{ readonly id: string }>(), {
      type: "snapshot",
      topic: "orders",
      queryId: "query-invalid-snapshot",
      version: 1,
      keys: ["a"],
      rows: [{ id: "a" }],
      totalRows: -1,
    });
    const totalRowsBelowWindow = applyEvent(initialClientState<{ readonly id: string }>(), {
      type: "snapshot",
      topic: "orders",
      queryId: "query-invalid-snapshot",
      version: 1,
      keys: ["a"],
      rows: [{ id: "a" }],
      totalRows: 0,
    });
    const mismatchedKeysAndRows = applyEvent(initialClientState<{ readonly id: string }>(), {
      type: "snapshot",
      topic: "orders",
      queryId: "query-invalid-snapshot",
      version: 1,
      keys: ["a", "b"],
      rows: [{ id: "a" }],
      totalRows: 2,
    });
    const duplicateKeys = applyEvent(initialClientState<{ readonly id: string }>(), {
      type: "snapshot",
      topic: "orders",
      queryId: "query-invalid-snapshot",
      version: 1,
      keys: ["a", "a"],
      rows: [{ id: "a" }, { id: "a-copy" }],
      totalRows: 2,
    });
    const invalidRefresh = applyEvent(previous, {
      type: "snapshot",
      topic: "orders",
      queryId: "query-invalid-snapshot",
      version: 2,
      keys: ["a"],
      rows: [{ id: "a-refresh" }],
      totalRows: -1,
    });

    expect(fractionalVersion).toMatchObject({
      rows: [],
      keys: [],
      totalRows: 0,
      version: 0,
      status: "stale",
      statusCode: "SnapshotStale",
    });
    expect(negativeTotalRows).toMatchObject({
      rows: [],
      keys: [],
      totalRows: 0,
      version: 0,
      status: "stale",
      statusCode: "SnapshotStale",
    });
    expect(totalRowsBelowWindow).toMatchObject({
      rows: [],
      keys: [],
      totalRows: 0,
      version: 0,
      status: "stale",
      statusCode: "SnapshotStale",
    });
    expect(mismatchedKeysAndRows).toMatchObject({
      rows: [],
      keys: [],
      totalRows: 0,
      version: 0,
      status: "stale",
      statusCode: "SnapshotStale",
    });
    expect(duplicateKeys).toMatchObject({
      rows: [],
      keys: [],
      totalRows: 0,
      version: 0,
      status: "stale",
      statusCode: "SnapshotStale",
    });
    expect(invalidRefresh).toMatchObject({
      rows: [{ id: "a" }],
      keys: ["a"],
      totalRows: 1,
      version: 1,
      status: "stale",
      statusCode: "SnapshotStale",
    });
  });

  it("applies multi-operation deltas without mutating the previous state", () => {
    const previous = applyEvent(initialClientState<{ readonly id: string }>(), {
      type: "snapshot",
      topic: "orders",
      queryId: "query-batch",
      version: 1,
      keys: ["a", "b", "c"],
      rows: [{ id: "a" }, { id: "b" }, { id: "c" }],
      totalRows: 3,
    });
    const previousRows = previous.rows;
    const previousKeys = previous.keys;

    const next = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-batch",
      fromVersion: 1,
      toVersion: 2,
      totalRows: 3,
      operations: [
        { type: "move", key: "c", fromIndex: 2, toIndex: 0 },
        { type: "insert", key: "d", row: { id: "d" }, index: 2 },
        { type: "update", key: "a", row: { id: "a-updated" }, index: 1 },
        { type: "remove", key: "b" },
      ],
    });

    expect(previous.rows).toBe(previousRows);
    expect(previous.keys).toBe(previousKeys);
    expect(previous.rows).toStrictEqual([{ id: "a" }, { id: "b" }, { id: "c" }]);
    expect(previous.keys).toStrictEqual(["a", "b", "c"]);
    expect(next.rows).toStrictEqual([{ id: "c" }, { id: "a-updated" }, { id: "d" }]);
    expect(next.keys).toStrictEqual(["c", "a", "d"]);
    expect(next.totalRows).toBe(3);
    expect(next.version).toBe(2);
  });

  it("moves rows by key even when the row value is undefined", () => {
    const previous = applyEvent(initialClientState<undefined>(), {
      type: "snapshot",
      topic: "orders",
      queryId: "query-undefined-row",
      version: 1,
      keys: ["a", "b"],
      rows: [undefined, undefined],
      totalRows: 2,
    });

    const next = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-undefined-row",
      fromVersion: 1,
      toVersion: 2,
      totalRows: 2,
      operations: [{ type: "move", key: "b", fromIndex: 1, toIndex: 0 }],
    });

    expect(next.rows).toStrictEqual([undefined, undefined]);
    expect(next.keys).toStrictEqual(["b", "a"]);
    expect(next.status).toBe("ready");
    expect(next.version).toBe(2);
  });

  it("marks the state stale when a delta cannot apply to the current snapshot", () => {
    const previous = applyEvent(initialClientState<{ readonly id: string }>(), {
      type: "snapshot",
      topic: "orders",
      queryId: "query-invalid-delta",
      version: 1,
      keys: ["a", "b", "c"],
      rows: [{ id: "a" }, { id: "b" }, { id: "c" }],
      totalRows: 3,
    });

    const staleVersion = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 0,
      toVersion: 2,
      totalRows: 3,
      operations: [],
    });
    const equalToVersion = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 1,
      toVersion: 1,
      totalRows: 3,
      operations: [],
    });
    const backwardToVersion = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 1,
      toVersion: 0,
      totalRows: 3,
      operations: [],
    });
    const fractionalToVersion = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 1,
      toVersion: 1.5,
      totalRows: 3,
      operations: [],
    });
    const infiniteToVersion = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 1,
      toVersion: Number.POSITIVE_INFINITY,
      totalRows: 3,
      operations: [],
    });
    const nanFromVersion = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: Number.NaN,
      toVersion: 2,
      totalRows: 3,
      operations: [],
    });
    const negativeFromVersion = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: -1,
      toVersion: 2,
      totalRows: 3,
      operations: [],
    });
    const fractionalStateVersion = applyEvent(
      {
        ...previous,
        version: 1.5,
      },
      {
        type: "delta",
        topic: "orders",
        queryId: "query-invalid-delta",
        fromVersion: 1.5,
        toVersion: 2,
        totalRows: 3,
        operations: [],
      },
    );
    const negativeDeltaTotalRows = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 1,
      toVersion: 2,
      totalRows: -1,
      operations: [],
    });
    const fractionalDeltaTotalRows = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 1,
      toVersion: 2,
      totalRows: 1.5,
      operations: [],
    });
    const totalRowsBelowWindow = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 1,
      toVersion: 2,
      totalRows: 2,
      operations: [],
    });
    const loadingStateDelta = applyEvent(initialClientState<{ readonly id: string }>(), {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 0,
      toVersion: 1,
      totalRows: 0,
      operations: [],
    });
    const duplicateInsert = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 1,
      toVersion: 2,
      totalRows: 4,
      operations: [{ type: "insert", key: "a", row: { id: "a-duplicate" }, index: 1 }],
    });
    const negativeInsert = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 1,
      toVersion: 2,
      totalRows: 4,
      operations: [{ type: "insert", key: "d", row: { id: "d" }, index: -1 }],
    });
    const nanInsert = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 1,
      toVersion: 2,
      totalRows: 4,
      operations: [{ type: "insert", key: "d", row: { id: "d" }, index: Number.NaN }],
    });
    const outOfRangeInsert = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 1,
      toVersion: 2,
      totalRows: 4,
      operations: [{ type: "insert", key: "d", row: { id: "d" }, index: 4 }],
    });
    const mismatchedUpdate = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 1,
      toVersion: 2,
      totalRows: 3,
      operations: [{ type: "update", key: "z", row: { id: "z" }, index: 1 }],
    });
    const fractionalUpdate = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 1,
      toVersion: 2,
      totalRows: 3,
      operations: [{ type: "update", key: "b", row: { id: "b" }, index: 1.5 }],
    });
    const missingMoveSource = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 1,
      toVersion: 2,
      totalRows: 3,
      operations: [{ type: "move", key: "missing", fromIndex: 99, toIndex: 0 }],
    });
    const mismatchedMoveKey = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 1,
      toVersion: 2,
      totalRows: 3,
      operations: [{ type: "move", key: "z", fromIndex: 0, toIndex: 1 }],
    });
    const negativeMoveTarget = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 1,
      toVersion: 2,
      totalRows: 3,
      operations: [{ type: "move", key: "a", fromIndex: 0, toIndex: -1 }],
    });
    const fractionalMoveSource = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 1,
      toVersion: 2,
      totalRows: 3,
      operations: [{ type: "move", key: "a", fromIndex: 0.5, toIndex: 1 }],
    });
    const fractionalMoveTarget = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 1,
      toVersion: 2,
      totalRows: 3,
      operations: [{ type: "move", key: "a", fromIndex: 0, toIndex: 1.5 }],
    });
    const outOfRangeMoveTarget = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 1,
      toVersion: 2,
      totalRows: 3,
      operations: [{ type: "move", key: "a", fromIndex: 0, toIndex: 3 }],
    });
    const missingRemove = applyEvent(previous, {
      type: "delta",
      topic: "orders",
      queryId: "query-invalid-delta",
      fromVersion: 1,
      toVersion: 2,
      totalRows: 2,
      operations: [{ type: "remove", key: "missing" }],
    });

    expect(staleVersion).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(equalToVersion).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(backwardToVersion).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(fractionalToVersion).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(infiniteToVersion).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(nanFromVersion).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(negativeFromVersion).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(fractionalStateVersion).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1.5,
    });
    expect(negativeDeltaTotalRows).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(fractionalDeltaTotalRows).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(totalRowsBelowWindow).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(loadingStateDelta).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 0,
    });
    expect(duplicateInsert).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(negativeInsert).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(nanInsert).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(outOfRangeInsert).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(mismatchedUpdate).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(fractionalUpdate).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(missingMoveSource).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(mismatchedMoveKey).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(negativeMoveTarget).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(fractionalMoveSource).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(fractionalMoveTarget).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(outOfRangeMoveTarget).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
    });
    expect(missingRemove).toMatchObject({
      status: "stale",
      statusCode: "SnapshotStale",
      version: 1,
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

    expect(stableQueryKey({ value: undefined })).not.toBe(stableQueryKey({ value: null }));
    expect(stableQueryKey({ b: 1, a: 2 })).toBe(stableQueryKey({ a: 2, b: 1 }));
    expect(stableQueryKey({ value: Symbol("filter") })).toBe(
      stableQueryKey({ value: Symbol("filter") }),
    );
    expect(stableQueryKey({ value: symbol })).toBe(stableQueryKey({ value: symbol }));
    expect(stableQueryKey({ value: firstFunction })).toBe(stableQueryKey({ value: firstFunction }));
    expect(stableQueryKey({ value: firstFunction })).toBe(
      stableQueryKey({ value: secondFunction }),
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
    expect(stableQueryKey({ where: { custom: { eq: firstFilter } } })).toBe(
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
