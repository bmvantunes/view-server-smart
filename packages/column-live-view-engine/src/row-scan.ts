type RowObject = object;

export type TopicRowEntry<Row extends RowObject> = {
  readonly key: string;
  readonly row: Row;
};

export type TopicRowVisitor<Row extends RowObject> = (key: string, row: Row) => void;

export type TopicRawOrderByPlan = {
  readonly field: string;
  readonly direction: "asc" | "desc";
};

export type TopicRawPredicateFilterPlan =
  | {
      readonly field: string;
      readonly operator: "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "startsWith";
      readonly value: unknown;
    }
  | {
      readonly field: string;
      readonly operator: "in";
      readonly values: ReadonlyArray<unknown>;
    };

export type TopicRawPredicatePlan = {
  /**
   * Safe scalar hints that storage can use to narrow a raw scan.
   * `matches` remains the correctness guard unless an adapter implements a
   * proven equivalent for every emitted hint.
   */
  readonly filters: ReadonlyArray<TopicRawPredicateFilterPlan>;
  /**
   * True when the compiler intentionally omitted part of the predicate from
   * `filters`, for example structured fields or malformed runtime filters.
   */
  readonly callbackRequired: boolean;
};

export type TopicRawWindowScanPlan<Row extends RowObject> = {
  readonly predicate: TopicRawPredicatePlan;
  readonly orderBy: ReadonlyArray<TopicRawOrderByPlan>;
  /**
   * Compiler-proven ordering hint for storage pushdown. `compare` remains the
   * source of truth for custom scan plans unless this hint is present.
   */
  readonly storageOrderBy?: ReadonlyArray<TopicRawOrderByPlan>;
  readonly matches: (row: Row) => boolean;
  readonly compare: (left: TopicRowEntry<Row>, right: TopicRowEntry<Row>) => number;
  readonly offset: number;
  readonly limit: number | undefined;
};

export type TopicRawWindowScanResult<Row extends RowObject> = {
  readonly keys: ReadonlyArray<string>;
  readonly window: ReadonlyArray<TopicRowEntry<Row>>;
  readonly totalRows: number;
};

export type TopicRowScan<Row extends RowObject> = {
  readonly scanRows: (visitor: TopicRowVisitor<Row>) => void;
  readonly version: () => number;
};

export type TopicRawWindowScan<Row extends RowObject> = {
  readonly scanRawWindow: (plan: TopicRawWindowScanPlan<Row>) => TopicRawWindowScanResult<Row>;
};
