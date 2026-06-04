import type { DeltaEvent, LiveQueryResult, StatusEvent } from "@view-server/config";
import type { ViewServerLiveEvent } from "./live-client";

export type ClientState<Row> = {
  readonly rows: ReadonlyArray<Row>;
  readonly keys: ReadonlyArray<string>;
  readonly totalRows: number;
  readonly version: number;
  readonly status: LiveQueryResult<Row>["status"];
  readonly statusCode?: LiveQueryResult<Row>["statusCode"];
  readonly message?: string | undefined;
};

export const initialClientState = <Row>(): ClientState<Row> => ({
  rows: [],
  keys: [],
  totalRows: 0,
  version: 0,
  status: "loading",
});

export const liveQueryResult = <Row>(state: ClientState<Row>): LiveQueryResult<Row> => ({
  rows: state.rows,
  totalRows: state.totalRows,
  version: state.version,
  status: state.status,
  statusCode: state.statusCode,
  message: state.message,
});

const applySnapshot = <Row>(
  state: ClientState<Row>,
  event: Extract<ViewServerLiveEvent<Row>, { readonly type: "snapshot" }>,
): ClientState<Row> => {
  if (!isValidSnapshotEvent(event)) {
    return staleSnapshotState(state);
  }
  return {
    rows: event.rows,
    keys: event.keys,
    totalRows: event.totalRows,
    version: event.version,
    status: "ready",
    statusCode: "Ready",
  };
};

const reindexKeys = (
  keys: ReadonlyArray<string>,
  keyIndexes: Map<string, number>,
  startIndex: number,
): void => {
  for (let index = startIndex; index < keys.length; index += 1) {
    keyIndexes.set(keys[index]!, index);
  }
};

const isInsertIndex = (index: number, length: number): boolean =>
  Number.isSafeInteger(index) && index >= 0 && index <= length;

const isExistingIndex = (index: number, length: number): boolean =>
  Number.isSafeInteger(index) && index >= 0 && index < length;

const isNonNegativeSafeInteger = (value: number): boolean =>
  Number.isSafeInteger(value) && value >= 0;

const hasUniqueKeys = (keys: ReadonlyArray<string>): boolean => {
  const seenKeys = new Set<string>();
  for (const key of keys) {
    if (seenKeys.has(key)) {
      return false;
    }
    seenKeys.add(key);
  }
  return true;
};

const isValidSnapshotEvent = <Row>(
  event: Extract<ViewServerLiveEvent<Row>, { readonly type: "snapshot" }>,
): boolean =>
  isNonNegativeSafeInteger(event.version) &&
  isNonNegativeSafeInteger(event.totalRows) &&
  event.totalRows >= event.rows.length &&
  event.keys.length === event.rows.length &&
  hasUniqueKeys(event.keys);

const applyDeltaOperation = <Row>(
  rows: Array<Row>,
  keys: Array<string>,
  keyIndexes: Map<string, number>,
  operation: DeltaEvent<Row>["operations"][number],
): boolean => {
  if (operation.type === "insert") {
    if (!isInsertIndex(operation.index, keys.length) || keyIndexes.has(operation.key)) {
      return false;
    }
    rows.splice(operation.index, 0, operation.row);
    keys.splice(operation.index, 0, operation.key);
    reindexKeys(keys, keyIndexes, operation.index);
    return true;
  }
  if (operation.type === "update") {
    if (!isExistingIndex(operation.index, keys.length)) {
      return false;
    }
    const previousKey = keys[operation.index];
    if (previousKey !== operation.key) {
      return false;
    }
    rows[operation.index] = operation.row;
    keyIndexes.set(operation.key, operation.index);
    return true;
  }
  if (operation.type === "move") {
    if (
      !isExistingIndex(operation.fromIndex, keys.length) ||
      !isExistingIndex(operation.toIndex, keys.length)
    ) {
      return false;
    }
    const key = keys[operation.fromIndex];
    if (key !== operation.key) {
      return false;
    }
    const movedRows = rows.splice(operation.fromIndex, 1);
    const movedKeys = keys.splice(operation.fromIndex, 1);
    rows.splice(operation.toIndex, 0, ...movedRows);
    keys.splice(operation.toIndex, 0, ...movedKeys);
    reindexKeys(keys, keyIndexes, Math.min(operation.fromIndex, operation.toIndex));
    return true;
  }
  const index = keyIndexes.get(operation.key);
  if (index === undefined) {
    return false;
  }
  rows.splice(index, 1);
  keys.splice(index, 1);
  keyIndexes.delete(operation.key);
  reindexKeys(keys, keyIndexes, index);
  return true;
};

const staleDeltaState = <Row>(state: ClientState<Row>): ClientState<Row> => ({
  ...state,
  status: "stale",
  statusCode: "SnapshotStale",
  message: "Received an invalid delta; waiting for a fresh snapshot.",
});

const staleSnapshotState = <Row>(state: ClientState<Row>): ClientState<Row> => ({
  ...state,
  status: "stale",
  statusCode: "SnapshotStale",
  message: "Received an invalid snapshot; waiting for a fresh snapshot.",
});

const canApplyDeltaFromVersion = <Row>(
  state: ClientState<Row>,
  event: DeltaEvent<Row>,
): boolean => {
  if (state.status !== "ready") {
    return false;
  }
  return (
    isNonNegativeSafeInteger(state.version) &&
    isNonNegativeSafeInteger(event.fromVersion) &&
    isNonNegativeSafeInteger(event.toVersion) &&
    isNonNegativeSafeInteger(event.totalRows) &&
    state.version === event.fromVersion &&
    event.toVersion > state.version
  );
};

const applyDelta = <Row>(state: ClientState<Row>, event: DeltaEvent<Row>): ClientState<Row> => {
  if (!canApplyDeltaFromVersion(state, event)) {
    return staleDeltaState(state);
  }
  const rows = state.rows.slice();
  const keys = state.keys.slice();
  const keyIndexes = new Map(keys.map((key, index) => [key, index]));
  for (const operation of event.operations) {
    if (!applyDeltaOperation(rows, keys, keyIndexes, operation)) {
      return staleDeltaState(state);
    }
  }
  if (event.totalRows < rows.length) {
    return staleDeltaState(state);
  }
  return {
    rows,
    keys,
    totalRows: event.totalRows,
    version: event.toVersion,
    status: "ready",
    statusCode: "Ready",
  };
};

const applyStatus = <Row>(state: ClientState<Row>, event: StatusEvent): ClientState<Row> => {
  const baseState = event.status === "closed" ? initialClientState<Row>() : state;
  return {
    ...baseState,
    status: event.status,
    statusCode: event.code,
    message: event.message,
  };
};

export const applyEvent = <Row>(
  state: ClientState<Row>,
  event: ViewServerLiveEvent<Row>,
): ClientState<Row> => {
  if (event.type === "snapshot") {
    return applySnapshot(state, event);
  }
  if (event.type === "delta") {
    return applyDelta(state, event);
  }
  return applyStatus(state, event);
};
