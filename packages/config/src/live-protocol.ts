import type { Effect } from "effect";
import type { ViewServerTransportError } from "./runtime-contract";
import type { LiveQuery } from "./topic-contract";

export type SnapshotEvent<Row> = {
  readonly type: "snapshot";
  readonly topic: string;
  readonly queryId: string;
  readonly version: number;
  readonly keys: ReadonlyArray<string>;
  readonly rows: ReadonlyArray<Row>;
  readonly totalRows?: number;
};

export type DeltaOperation<Row> =
  | {
      readonly type: "insert";
      readonly key: string;
      readonly row: Row;
      readonly index: number;
    }
  | {
      readonly type: "update";
      readonly key: string;
      readonly row: Row;
      readonly index: number;
    }
  | {
      readonly type: "move";
      readonly key: string;
      readonly fromIndex: number;
      readonly toIndex: number;
    }
  | {
      readonly type: "remove";
      readonly key: string;
    };

export type DeltaEvent<Row> = {
  readonly type: "delta";
  readonly topic: string;
  readonly queryId: string;
  readonly fromVersion: number;
  readonly toVersion: number;
  readonly operations: ReadonlyArray<DeltaOperation<Row>>;
  readonly totalRows?: number;
};

export type StatusEventCode =
  | "Ready"
  | "SnapshotStale"
  | "SubscriptionClosed"
  | "TransportError"
  | "BackpressureExceeded";

export type StatusEvent =
  | {
      readonly type: "status";
      readonly topic: string;
      readonly queryId: string;
      readonly status: "ready";
      readonly code: "Ready";
      readonly message?: string;
    }
  | {
      readonly type: "status";
      readonly topic: string;
      readonly queryId: string;
      readonly status: "stale";
      readonly code: "SnapshotStale" | "BackpressureExceeded";
      readonly message?: string;
    }
  | {
      readonly type: "status";
      readonly topic: string;
      readonly queryId: string;
      readonly status: "closed";
      readonly code: "SubscriptionClosed" | "BackpressureExceeded";
      readonly message?: string;
    }
  | {
      readonly type: "status";
      readonly topic: string;
      readonly queryId: string;
      readonly status: "error";
      readonly code: "TransportError" | "BackpressureExceeded";
      readonly message?: string;
    };

export type LiveSubscription<Row> = {
  readonly events: AsyncIterable<SnapshotEvent<Row> | DeltaEvent<Row> | StatusEvent>;
  readonly close: () => Effect.Effect<void, ViewServerTransportError>;
};

export type LiveTransportAdapter = {
  readonly subscribe: <Row>(
    topic: string,
    query: LiveQuery<Row>,
  ) => Effect.Effect<LiveSubscription<Row>, ViewServerTransportError>;
};
