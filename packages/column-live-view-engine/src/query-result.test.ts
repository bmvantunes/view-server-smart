import { describe, expect, it } from "@effect/vitest";
import { deltaOperations, type QueryEvaluation } from "./query-result";

type TestRow = {
  readonly id: string;
  readonly value: number;
};

const evaluation = (rows: ReadonlyArray<TestRow>, version: number): QueryEvaluation<TestRow> => ({
  rows,
  keys: rows.map((row) => row.id),
  window: rows.map((row) => ({
    key: row.id,
    row,
  })),
  totalRows: rows.length,
  version,
});

describe("query result delta operations", () => {
  it("uses batched replacement operations when shared rows stay stable", () => {
    const previous = evaluation(
      [
        { id: "old-1", value: 1 },
        { id: "old-2", value: 2 },
        { id: "old-3", value: 3 },
        { id: "old-4", value: 4 },
        { id: "keep-1", value: 5 },
        { id: "keep-2", value: 6 },
      ],
      1,
    );
    const next = evaluation(
      [
        { id: "new-1", value: 7 },
        { id: "new-2", value: 8 },
        { id: "new-3", value: 9 },
        { id: "new-4", value: 10 },
        { id: "keep-1", value: 5 },
        { id: "keep-2", value: 6 },
      ],
      2,
    );

    const operations = deltaOperations(previous, next);

    expect(operations).toStrictEqual([
      { type: "remove", key: "old-1" },
      { type: "remove", key: "old-2" },
      { type: "remove", key: "old-3" },
      { type: "remove", key: "old-4" },
      { type: "insert", key: "new-1", row: { id: "new-1", value: 7 }, index: 0 },
      { type: "insert", key: "new-2", row: { id: "new-2", value: 8 }, index: 1 },
      { type: "insert", key: "new-3", row: { id: "new-3", value: 9 }, index: 2 },
      { type: "insert", key: "new-4", row: { id: "new-4", value: 10 }, index: 3 },
    ]);
  });

  it("uses the regular path for small mixed replacement batches", () => {
    const previous = evaluation(
      [
        { id: "old-1", value: 1 },
        { id: "old-2", value: 2 },
        { id: "keep-1", value: 3 },
      ],
      1,
    );
    const next = evaluation(
      [
        { id: "new-1", value: 4 },
        { id: "new-2", value: 5 },
        { id: "keep-1", value: 3 },
      ],
      2,
    );

    const operations = deltaOperations(previous, next);

    expect(operations).toStrictEqual([
      { type: "remove", key: "old-1" },
      { type: "remove", key: "old-2" },
      { type: "insert", key: "new-1", row: { id: "new-1", value: 4 }, index: 0 },
      { type: "insert", key: "new-2", row: { id: "new-2", value: 5 }, index: 1 },
    ]);
  });

  it("keeps move operations when shared rows reorder inside a replacement batch", () => {
    const previous = evaluation(
      [
        { id: "old-1", value: 1 },
        { id: "old-2", value: 2 },
        { id: "old-3", value: 3 },
        { id: "old-4", value: 4 },
        { id: "keep-1", value: 5 },
        { id: "keep-2", value: 6 },
      ],
      1,
    );
    const next = evaluation(
      [
        { id: "new-1", value: 7 },
        { id: "new-2", value: 8 },
        { id: "new-3", value: 9 },
        { id: "new-4", value: 10 },
        { id: "keep-2", value: 6 },
        { id: "keep-1", value: 5 },
      ],
      2,
    );

    const operations = deltaOperations(previous, next);

    expect(operations).toStrictEqual([
      { type: "remove", key: "old-1" },
      { type: "remove", key: "old-2" },
      { type: "remove", key: "old-3" },
      { type: "remove", key: "old-4" },
      { type: "insert", key: "new-1", row: { id: "new-1", value: 7 }, index: 0 },
      { type: "insert", key: "new-2", row: { id: "new-2", value: 8 }, index: 1 },
      { type: "insert", key: "new-3", row: { id: "new-3", value: 9 }, index: 2 },
      { type: "insert", key: "new-4", row: { id: "new-4", value: 10 }, index: 3 },
      { type: "move", key: "keep-2", fromIndex: 5, toIndex: 4 },
    ]);
  });

  it("keeps update operations when shared rows change inside a replacement batch", () => {
    const previous = evaluation(
      [
        { id: "old-1", value: 1 },
        { id: "old-2", value: 2 },
        { id: "old-3", value: 3 },
        { id: "old-4", value: 4 },
        { id: "keep-1", value: 5 },
        { id: "keep-2", value: 6 },
      ],
      1,
    );
    const next = evaluation(
      [
        { id: "new-1", value: 7 },
        { id: "new-2", value: 8 },
        { id: "new-3", value: 9 },
        { id: "new-4", value: 10 },
        { id: "keep-1", value: 50 },
        { id: "keep-2", value: 6 },
      ],
      2,
    );

    const operations = deltaOperations(previous, next);

    expect(operations).toStrictEqual([
      { type: "remove", key: "old-1" },
      { type: "remove", key: "old-2" },
      { type: "remove", key: "old-3" },
      { type: "remove", key: "old-4" },
      { type: "insert", key: "new-1", row: { id: "new-1", value: 7 }, index: 0 },
      { type: "insert", key: "new-2", row: { id: "new-2", value: 8 }, index: 1 },
      { type: "insert", key: "new-3", row: { id: "new-3", value: 9 }, index: 2 },
      { type: "insert", key: "new-4", row: { id: "new-4", value: 10 }, index: 3 },
      { type: "update", key: "keep-1", row: { id: "keep-1", value: 50 }, index: 4 },
    ]);
  });

  it("uses the regular path for delete-only and update-only changes", () => {
    const deleteOnlyPrevious = evaluation(
      [
        { id: "old-1", value: 1 },
        { id: "keep-1", value: 2 },
      ],
      1,
    );
    const deleteOnlyNext = evaluation([{ id: "keep-1", value: 2 }], 2);
    const updateOnlyPrevious = evaluation([{ id: "keep-1", value: 2 }], 3);
    const updateOnlyNext = evaluation([{ id: "keep-1", value: 20 }], 4);

    const deleteOperations = deltaOperations(deleteOnlyPrevious, deleteOnlyNext);
    const updateOperations = deltaOperations(updateOnlyPrevious, updateOnlyNext);

    expect(deleteOperations).toStrictEqual([{ type: "remove", key: "old-1" }]);
    expect(updateOperations).toStrictEqual([
      { type: "update", key: "keep-1", row: { id: "keep-1", value: 20 }, index: 0 },
    ]);
  });
});
