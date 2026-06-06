import { Effect, Schema } from "effect";
import { createActiveQueryRegistry, type ActiveQueryStoreState } from "./active-query";
import type {
  TopicRowChange,
  TopicRowChangeBatch,
  TopicRowEntry,
  TopicRowVisitor,
} from "./row-scan";
import type { TopicRawWindowScanPlan, TopicRawWindowScanResult } from "./raw-window-scan";
import type { OrderedSlotIndex } from "./topic-ordered-window";
import { rawQueryCompilerMetadata, type RawQueryCompilerMetadata } from "./raw-query-compiler";
import { fieldValue } from "./row-values";
import { createTopicColumnValues, type MutableTopicColumnValues } from "./topic-column-vector";
import {
  addSlotToScalarPredicateIndexes,
  createScalarPredicateIndexes,
  removeSlotFromScalarPredicateIndexes,
} from "./topic-predicate-candidate-index";
import { TopicRowChangeJournal } from "./topic-row-change-journal";
import {
  prepareTopicPatch,
  prepareTopicRow,
  type InvalidRowErrorFactory,
  type PreparedTopicRow,
  type TopicRowPreparationContext,
} from "./topic-row-preparation";
import {
  insertSlotIntoRawWindowIndexes,
  scanTopicRawWindow,
  type TopicRawWindowScanState,
} from "./topic-raw-window-scanner";

type RowObject = object;

export class ColumnarTopicStore {
  readonly rawQueryMetadata: RawQueryCompilerMetadata;
  readonly readModel: ActiveQueryStoreState;

  private readonly slots: Array<TopicRowEntry<object>> = [];
  private readonly keyToSlot = new Map<string, number>();
  private readonly columns = new Map<string, MutableTopicColumnValues>();
  private readonly orderedSlotIndexes = new Map<string, OrderedSlotIndex>();
  private readonly scalarPredicateIndexes = createScalarPredicateIndexes();
  private readonly rowChangeJournal = new TopicRowChangeJournal<object>();
  private readonly rowPreparation: TopicRowPreparationContext;
  private readonly rawWindowScanState: TopicRawWindowScanState;
  private versionValue = 0;

  constructor(
    readonly topic: string,
    schema: Schema.Decoder<object>,
    keyField: string,
  ) {
    this.rawQueryMetadata = rawQueryCompilerMetadata(schema);
    this.rawWindowScanState = {
      columns: this.columns,
      orderedSlotIndexes: this.orderedSlotIndexes,
      rawQueryMetadata: this.rawQueryMetadata,
      scalarPredicateIndexes: this.scalarPredicateIndexes,
      slots: this.slots,
    };
    this.rowPreparation = {
      fieldNames: this.rawQueryMetadata.fieldNames,
      keyField,
      schema,
      topic,
    };
    for (const field of this.rawQueryMetadata.fieldNames) {
      this.columns.set(field, createTopicColumnValues(field, this.rawQueryMetadata));
    }
    this.readModel = {
      activeQueries: createActiveQueryRegistry(),
      topic,
      changesSince: (version) => this.changesSince(version),
      releaseChanges: () => this.releaseChanges(),
      retainChanges: () => this.retainChanges(),
      scanRows: (visitor) => this.scanRows(visitor),
      scanRawWindow: (plan) => this.scanRawWindow(plan),
      version: () => this.versionValue,
    };
  }

  get rowCount(): number {
    return this.slots.length;
  }

  get version(): number {
    return this.versionValue;
  }

  advanceVersion(): number {
    this.versionValue += 1;
    this.rowChangeJournal.commit(this.versionValue);
    return this.versionValue;
  }

  clear(): void {
    this.slots.length = 0;
    this.keyToSlot.clear();
    this.orderedSlotIndexes.clear();
    this.scalarPredicateIndexes.clear();
    this.rowChangeJournal.clear(this.versionValue);
    for (const column of this.columns.values()) {
      column.clear();
    }
    this.versionValue = 0;
  }

  setPrepared(prepared: PreparedTopicRow): void {
    const existingSlot = this.keyToSlot.get(prepared.key);
    if (existingSlot !== undefined) {
      const previous = this.slots[existingSlot]!.row;
      this.removeSlotFromScalarIndexes(existingSlot);
      this.writeSlot(existingSlot, prepared);
      this.addSlotToScalarIndexes(existingSlot);
      this.recordRowChange({
        key: prepared.key,
        previous,
        next: prepared.row,
      });
      this.orderedSlotIndexes.clear();
      return;
    }

    const slot = this.slots.length;
    this.keyToSlot.set(prepared.key, slot);
    this.writeSlot(slot, prepared);
    this.addSlotToScalarIndexes(slot);
    this.recordRowChange({
      key: prepared.key,
      previous: undefined,
      next: prepared.row,
    });
    this.insertSlotIntoOrderedIndexes(slot);
  }

  setPreparedMany(preparedRows: ReadonlyArray<PreparedTopicRow>): void {
    if (preparedRows.length > 1 && this.orderedSlotIndexes.size > 0) {
      this.orderedSlotIndexes.clear();
      for (const prepared of preparedRows) {
        this.setPreparedWithoutIndexMaintenance(prepared);
      }
      return;
    }

    for (const prepared of preparedRows) {
      this.setPrepared(prepared);
    }
  }

  delete(key: string): number {
    const slot = this.keyToSlot.get(key);
    if (slot === undefined) {
      return 0;
    }

    this.orderedSlotIndexes.clear();
    const lastSlot = this.slots.length - 1;
    const lastEntry = this.slots[lastSlot]!;
    const previous = this.slots[slot]!.row;
    this.removeSlotFromScalarIndexes(slot);
    this.keyToSlot.delete(key);
    if (slot !== lastSlot) {
      this.removeSlotFromScalarIndexes(lastSlot);
      this.slots[slot] = lastEntry;
      this.keyToSlot.set(lastEntry.key, slot);
      for (const column of this.columns.values()) {
        column.copySlot(slot, lastSlot);
      }
      this.addSlotToScalarIndexes(slot);
    }
    this.slots.pop();
    for (const column of this.columns.values()) {
      column.pop();
    }
    this.recordRowChange({
      key,
      previous,
      next: undefined,
    });
    return 1;
  }

  changesSince(version: number): ReadonlyArray<TopicRowChangeBatch<object>> | undefined {
    return this.rowChangeJournal.changesSince(version, this.versionValue);
  }

  scanRows(visitor: TopicRowVisitor<object>): void {
    for (let slot = 0; slot < this.slots.length; slot += 1) {
      const entry = this.slots[slot]!;
      if (visitor(entry.key, entry.row) === false) {
        break;
      }
    }
  }

  scanRawWindow(plan: TopicRawWindowScanPlan<object>): TopicRawWindowScanResult<object> {
    return scanTopicRawWindow(this.rawWindowScanState, plan);
  }

  prepareRow = Effect.fn("ColumnLiveViewEngine.columnarTopicStore.row.prepare")(function* <
    Error,
    Row extends RowObject,
  >(this: ColumnarTopicStore, row: Row, invalidRow: InvalidRowErrorFactory<Error>) {
    return yield* prepareTopicRow(this.rowPreparation, row, invalidRow);
  });

  prepareRows = Effect.fn("ColumnLiveViewEngine.columnarTopicStore.rows.prepare")(function* <
    Error,
    Row extends RowObject,
  >(this: ColumnarTopicStore, rows: ReadonlyArray<Row>, invalidRow: InvalidRowErrorFactory<Error>) {
    return yield* Effect.forEach(rows, (row) => this.prepareRow(row, invalidRow));
  });

  preparePatch = Effect.fn("ColumnLiveViewEngine.columnarTopicStore.patch.prepare")(function* <
    Patch extends Partial<RowObject>,
    Error,
  >(
    this: ColumnarTopicStore,
    key: string,
    patch: Patch,
    invalidRow: InvalidRowErrorFactory<Error>,
  ) {
    return yield* prepareTopicPatch(
      this.rowPreparation,
      key,
      this.rowForKey(key),
      patch,
      invalidRow,
    );
  });

  private writeSlot(slot: number, prepared: PreparedTopicRow): void {
    this.slots[slot] = {
      key: prepared.key,
      row: prepared.row,
    };
    for (const [field, column] of this.columns) {
      column.set(slot, fieldValue(prepared.row, field));
    }
  }

  private insertSlotIntoOrderedIndexes(slot: number): void {
    insertSlotIntoRawWindowIndexes(this.rawWindowScanState, slot);
  }

  private setPreparedWithoutIndexMaintenance(prepared: PreparedTopicRow): void {
    const existingSlot = this.keyToSlot.get(prepared.key);
    if (existingSlot !== undefined) {
      const previous = this.slots[existingSlot]!.row;
      this.removeSlotFromScalarIndexes(existingSlot);
      this.writeSlot(existingSlot, prepared);
      this.addSlotToScalarIndexes(existingSlot);
      this.recordRowChange({
        key: prepared.key,
        previous,
        next: prepared.row,
      });
      return;
    }

    const slot = this.slots.length;
    this.keyToSlot.set(prepared.key, slot);
    this.writeSlot(slot, prepared);
    this.addSlotToScalarIndexes(slot);
    this.recordRowChange({
      key: prepared.key,
      previous: undefined,
      next: prepared.row,
    });
  }

  private recordRowChange(change: TopicRowChange<object>): void {
    this.rowChangeJournal.record(change, this.versionValue);
  }

  private addSlotToScalarIndexes(slot: number): void {
    addSlotToScalarPredicateIndexes(this.scalarPredicateIndexes, this.columns, slot);
  }

  private removeSlotFromScalarIndexes(slot: number): void {
    removeSlotFromScalarPredicateIndexes(this.scalarPredicateIndexes, this.columns, slot);
  }

  private releaseChanges(): void {
    this.rowChangeJournal.release(this.versionValue);
  }

  private retainChanges(): void {
    this.rowChangeJournal.retain(this.versionValue);
  }

  private rowForKey(key: string): object | undefined {
    const slot = this.keyToSlot.get(key);
    if (slot === undefined) {
      return undefined;
    }
    return this.slots[slot]!.row;
  }
}
