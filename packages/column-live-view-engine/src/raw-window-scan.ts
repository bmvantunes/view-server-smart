import type { TopicRawPredicatePlan } from "./raw-predicate-plan";
import type { TopicRowEntry } from "./row-scan";

type RowObject = object;

export type TopicRawOrderByPlan = {
  readonly field: string;
  readonly direction: "asc" | "desc";
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

export type TopicRawWindowScan<Row extends RowObject> = {
  readonly compareRawSlots?: (
    plan: TopicRawWindowScanPlan<Row>,
  ) => ((leftSlot: number, rightSlot: number) => number) | undefined;
  readonly projectRawRow?: (slot: number, selectedFields: ReadonlyArray<string>) => RowObject;
  readonly scanRawWindow: (plan: TopicRawWindowScanPlan<Row>) => TopicRawWindowScanResult<Row>;
  readonly slotForKey?: (key: string) => number | undefined;
};
