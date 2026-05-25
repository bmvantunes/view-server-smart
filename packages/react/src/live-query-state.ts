import type { DeltaEvent, LiveQueryResult, StatusEvent } from "@view-server/config";
import type { ColumnLiveViewEngineEvent } from "@view-server/column-live-view-engine";

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
  event: Extract<ColumnLiveViewEngineEvent<Row>, { readonly type: "snapshot" }>,
): ClientState<Row> => ({
  rows: event.rows,
  keys: event.keys,
  totalRows: event.totalRows,
  version: event.version,
  status: "ready",
  statusCode: "Ready",
});

const applyDeltaOperation = <Row>(
  state: ClientState<Row>,
  operation: DeltaEvent<Row>["operations"][number],
): ClientState<Row> => {
  if (operation.type === "insert") {
    const nextRows = state.rows.slice();
    const nextKeys = state.keys.slice();
    nextRows.splice(operation.index, 0, operation.row);
    nextKeys.splice(operation.index, 0, operation.key);
    return { ...state, rows: nextRows, keys: nextKeys };
  }
  if (operation.type === "update") {
    const nextRows = state.rows.slice();
    const nextKeys = state.keys.slice();
    nextRows[operation.index] = operation.row;
    nextKeys[operation.index] = operation.key;
    return { ...state, rows: nextRows, keys: nextKeys };
  }
  if (operation.type === "move") {
    const nextRows = state.rows.slice();
    const nextKeys = state.keys.slice();
    nextRows.splice(operation.toIndex, 0, ...nextRows.splice(operation.fromIndex, 1));
    nextKeys.splice(operation.toIndex, 0, ...nextKeys.splice(operation.fromIndex, 1));
    return { ...state, rows: nextRows, keys: nextKeys };
  }
  const nextRows = state.rows.filter((_row, index) => state.keys[index] !== operation.key);
  const nextKeys = state.keys.filter((key) => key !== operation.key);
  return { ...state, rows: nextRows, keys: nextKeys };
};

const applyDelta = <Row>(state: ClientState<Row>, event: DeltaEvent<Row>): ClientState<Row> => {
  let nextState = state;
  for (const operation of event.operations) {
    nextState = applyDeltaOperation(nextState, operation);
  }
  return {
    rows: nextState.rows,
    keys: nextState.keys,
    totalRows: event.totalRows,
    version: event.toVersion,
    status: "ready",
    statusCode: "Ready",
  };
};

const applyStatus = <Row>(state: ClientState<Row>, event: StatusEvent): ClientState<Row> => ({
  ...(event.code === "SubscriptionClosed" ? initialClientState<Row>() : state),
  status: event.status,
  statusCode: event.code,
  message: event.message,
});

export const applyEvent = <Row>(
  state: ClientState<Row>,
  event: ColumnLiveViewEngineEvent<Row>,
): ClientState<Row> => {
  if (event.type === "snapshot") {
    return applySnapshot(event);
  }
  if (event.type === "delta") {
    return applyDelta(state, event);
  }
  return applyStatus(state, event);
};
