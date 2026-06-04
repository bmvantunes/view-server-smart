import type {
  DeltaEvent,
  DeltaOperation,
  LiveQueryResult,
  SnapshotEvent,
} from "@view-server/config";
import { cloneRow, rowsEqual } from "./row-values";

type RowObject = object;

export type StoredRowOf<Row extends RowObject> = {
  readonly key: string;
  readonly row: Row;
};

export type QueryEvaluation<ResultRow extends RowObject> = {
  readonly rows: ReadonlyArray<ResultRow>;
  readonly keys: ReadonlyArray<string>;
  readonly window: ReadonlyArray<StoredRowOf<ResultRow>>;
  readonly totalRows: number;
  readonly version: number;
};

export const liveQueryResult = <Row extends RowObject>(
  evaluation: QueryEvaluation<Row>,
): LiveQueryResult<Row> => ({
  rows: evaluation.rows,
  totalRows: evaluation.totalRows,
  version: evaluation.version,
  status: "ready",
  statusCode: "Ready",
});

export const snapshotEvent = <Row extends RowObject>(
  store: { readonly topic: string },
  queryId: string,
  evaluation: QueryEvaluation<Row>,
): SnapshotEvent<Row> => ({
  type: "snapshot",
  topic: store.topic,
  queryId,
  version: evaluation.version,
  keys: [...evaluation.keys],
  rows: evaluation.rows.map(cloneRow),
  totalRows: evaluation.totalRows,
});

const reindexCurrentKeys = (
  keys: ReadonlyArray<string>,
  keyIndexes: Map<string, number>,
  startIndex: number,
): void => {
  for (let index = startIndex; index < keys.length; index += 1) {
    keyIndexes.set(keys[index]!, index);
  }
};

export const deltaOperations = <Row extends RowObject>(
  previous: QueryEvaluation<Row>,
  next: QueryEvaluation<Row>,
): ReadonlyArray<DeltaOperation<Row>> => {
  const operations: Array<DeltaOperation<Row>> = [];
  const nextKeys = new Set(next.keys);
  const currentKeys = [...previous.keys];
  const currentRows = [...previous.rows];
  const currentKeyIndexes = new Map(currentKeys.map((key, index) => [key, index]));

  for (const key of previous.keys) {
    if (!nextKeys.has(key)) {
      const index = currentKeyIndexes.get(key)!;
      currentKeys.splice(index, 1);
      currentRows.splice(index, 1);
      currentKeyIndexes.delete(key);
      reindexCurrentKeys(currentKeys, currentKeyIndexes, index);
      operations.push({
        type: "remove",
        key,
      });
    }
  }

  for (const [index, { key, row }] of next.window.entries()) {
    const currentIndex = currentKeyIndexes.get(key);
    if (currentIndex === undefined) {
      currentKeys.splice(index, 0, key);
      currentRows.splice(index, 0, row);
      reindexCurrentKeys(currentKeys, currentKeyIndexes, index);
      operations.push({
        type: "insert",
        key,
        row,
        index,
      });
      continue;
    }

    if (currentIndex !== index) {
      const movedKeys = currentKeys.splice(currentIndex, 1);
      const movedRows = currentRows.splice(currentIndex, 1);
      currentKeys.splice(index, 0, ...movedKeys);
      currentRows.splice(index, 0, ...movedRows);
      reindexCurrentKeys(currentKeys, currentKeyIndexes, Math.min(currentIndex, index));
      operations.push({
        type: "move",
        key,
        fromIndex: currentIndex,
        toIndex: index,
      });
    }

    const currentRow = currentRows[index];
    if (currentRow === undefined || !rowsEqual(currentRow, row)) {
      currentRows[index] = row;
      operations.push({
        type: "update",
        key,
        row,
        index,
      });
    }
  }

  return operations;
};

const cloneDeltaOperations = <Row extends RowObject>(
  operations: ReadonlyArray<DeltaOperation<Row>>,
): ReadonlyArray<DeltaOperation<Row>> =>
  operations.map((operation) => {
    if (operation.type === "insert" || operation.type === "update") {
      return {
        ...operation,
        row: cloneRow(operation.row),
      };
    }
    return operation;
  });

export const deltaEvent = <Row extends RowObject>(
  store: { readonly topic: string },
  queryId: string,
  fromVersion: number,
  next: QueryEvaluation<Row>,
  operations: ReadonlyArray<DeltaOperation<Row>>,
): DeltaEvent<Row> => ({
  type: "delta",
  topic: store.topic,
  queryId,
  fromVersion,
  toVersion: next.version,
  operations: cloneDeltaOperations(operations),
  totalRows: next.totalRows,
});
