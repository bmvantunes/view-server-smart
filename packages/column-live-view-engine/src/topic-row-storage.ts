import { Effect, Schema } from "effect";
import { createActiveQueryRegistry, type ActiveQueryStoreState } from "./active-query";
import type {
  TopicRowChange,
  TopicRowChangeBatch,
  TopicRowChangedFields,
  TopicRowEntry,
  TopicRowVisitor,
} from "./row-scan";
import { topicRowChangedFieldsFromRows } from "./row-scan";
import type {
  TopicRawOrderByPlan,
  TopicRawWindowScanPlan,
  TopicRawWindowScanResult,
} from "./raw-window-scan";
import type { TopicRawPredicatePlan } from "./raw-predicate-plan";
import type { OrderedSlotIndex, RawStorageOrderColumn } from "./topic-ordered-window";
import { rawQueryCompilerMetadata, type RawQueryCompilerMetadata } from "./raw-query-compiler";
import { cloneUnknown, rowsEqual, trustedFieldValue } from "./row-values";
import {
  columnValue,
  createTopicColumnValues,
  type MutableTopicColumnValues,
} from "./topic-column-vector";
import {
  addSlotToScalarPredicateIndexes,
  createScalarPredicateIndexes,
  removeSlotFromScalarPredicateIndexes,
} from "./topic-predicate-candidate-index";
import {
  TopicRowChangeJournal,
  type TopicRowChangeJournalLimits,
} from "./topic-row-change-journal";
import {
  prepareTopicPatch,
  prepareTopicRow,
  prepareTopicRowWithStorageKey,
  type InvalidRowErrorFactory,
  type PreparedTopicRow,
  type TopicRowPreparationContext,
} from "./topic-row-preparation";
import {
  insertSlotIntoRawWindowIndex,
  insertSlotIntoRawWindowIndexes,
  removeSlotFromRawWindowIndex,
  removeSlotFromRawWindowIndexes,
  scanTopicRawWindow,
  type TopicRawWindowScanState,
} from "./topic-raw-window-scanner";
import type { SlotFilterMatcher } from "./topic-slot-predicate";
import {
  compareSlotsByStorageOrder,
  compiledRawStorageOrder,
} from "./topic-raw-ordered-window-index";

type RowObject = object;

type RawProjectionColumn = {
  readonly column: MutableTopicColumnValues;
  readonly field: string;
  readonly shouldClone: boolean;
};

type AppendBatchReservation = {
  reserveFrom(startIndex: number): void;
};

type PreparedTopicRowReplacement = {
  readonly changedFields: TopicRowChangedFields | undefined;
  readonly previous: object;
};

const noopAppendBatchReservation: AppendBatchReservation = {
  reserveFrom: () => {},
};

export class TopicRowStorage {
  readonly rawQueryMetadata: RawQueryCompilerMetadata;
  readonly readModel: ActiveQueryStoreState;

  private readonly slots: Array<TopicRowEntry<object>> = [];
  private readonly keyToSlot = new Map<string, number>();
  private readonly columns = new Map<string, MutableTopicColumnValues>();
  private readonly columnWritePlan: Array<MutableTopicColumnValues> = [];
  private readonly columnWriteFields: Array<string> = [];
  private readonly rawProjectionPlans = new WeakMap<
    ReadonlyArray<string>,
    ReadonlyArray<RawProjectionColumn>
  >();
  private readonly rawStorageOrderPlans = new WeakMap<
    ReadonlyArray<TopicRawOrderByPlan>,
    ReadonlyArray<RawStorageOrderColumn>
  >();
  private readonly rawPredicateSlotMatchers = new WeakMap<
    TopicRawPredicatePlan,
    SlotFilterMatcher
  >();
  private readonly reservableColumns: Array<MutableTopicColumnValues> = [];
  private readonly orderedSlotIndexes = new Map<string, OrderedSlotIndex>();
  private readonly scalarPredicateIndexes = createScalarPredicateIndexes();
  private readonly rowChangeJournal: TopicRowChangeJournal<object>;
  private readonly rowPreparation: TopicRowPreparationContext;
  private readonly rawWindowScanState: TopicRawWindowScanState;
  private versionValue = 0;

  constructor(
    readonly topic: string,
    schema: Schema.Decoder<object>,
    keyField: string,
    rowChangeJournalLimits?: TopicRowChangeJournalLimits,
  ) {
    this.rowChangeJournal = new TopicRowChangeJournal<object>(rowChangeJournalLimits);
    this.rawQueryMetadata = rawQueryCompilerMetadata(schema);
    this.rawWindowScanState = {
      columns: this.columns,
      orderedSlotIndexes: this.orderedSlotIndexes,
      rawPredicateSlotMatchers: this.rawPredicateSlotMatchers,
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
      const column = createTopicColumnValues(field, this.rawQueryMetadata);
      this.columns.set(field, column);
    }
    for (const field of this.rawQueryMetadata.fieldOrder) {
      const column = this.columns.get(field)!;
      this.columnWritePlan.push(column);
      this.columnWriteFields.push(field);
      if (column.kind === "number") {
        this.reservableColumns.push(column);
      }
    }
    this.readModel = {
      activeQueries: createActiveQueryRegistry(),
      topic,
      changesSince: (version) => this.changesSince(version),
      compareRawSlots: (plan) => this.compareRawSlots(plan),
      keyAtSlot: (slot) => this.keyAtSlot(slot),
      projectRawRow: (slot, selectedFields) => this.projectRawRow(slot, selectedFields),
      releaseChanges: () => this.releaseChanges(),
      retainChanges: () => this.retainChanges(),
      scanRows: (visitor) => this.scanRows(visitor),
      scanRawWindow: (plan) => this.scanRawWindow(plan),
      slotForKey: (key) => this.slotForKey(key),
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

  setPrepared(prepared: PreparedTopicRow): number {
    const existingSlot = this.keyToSlot.get(prepared.key);
    if (existingSlot !== undefined) {
      const replacement = this.preparedReplacementForCurrentSlot(prepared, existingSlot);
      if (replacement === undefined) {
        return 0;
      }
      this.removeSlotFromScalarIndexes(existingSlot);
      this.removeSlotFromReplacementOrderedIndexes(existingSlot, replacement.changedFields);
      this.writeSlot(existingSlot, prepared);
      this.addSlotToScalarIndexes(existingSlot);
      this.recordPreparedReplacementChange(prepared, replacement);
      this.insertSlotIntoReplacementOrderedIndexes(existingSlot, replacement.changedFields);
      return 1;
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
    return 1;
  }

  setPreparedMany(preparedRows: ReadonlyArray<PreparedTopicRow>): number {
    const appendReservation = this.createAppendBatchReservation(preparedRows);
    if (preparedRows.length > 1 && this.orderedSlotIndexes.size > 0) {
      this.orderedSlotIndexes.clear();
      let rowsChanged = 0;
      for (let index = 0; index < preparedRows.length; index += 1) {
        rowsChanged += this.setPreparedWithoutIndexMaintenance(
          preparedRows[index]!,
          appendReservation,
          index,
        );
      }
      return rowsChanged;
    }

    let rowsChanged = 0;
    for (let index = 0; index < preparedRows.length; index += 1) {
      rowsChanged += this.setPreparedInBatch(preparedRows[index]!, appendReservation, index);
    }
    return rowsChanged;
  }

  delete(key: string): number {
    const slot = this.keyToSlot.get(key);
    if (slot === undefined) {
      return 0;
    }

    const lastSlot = this.slots.length - 1;
    const lastEntry = this.slots[lastSlot]!;
    const previous = this.slots[slot]!.row;
    this.removeSlotFromScalarIndexes(slot);
    this.removeSlotFromOrderedIndexes(slot);
    this.keyToSlot.delete(key);
    if (slot !== lastSlot) {
      this.removeSlotFromScalarIndexes(lastSlot);
      this.removeSlotFromOrderedIndexes(lastSlot);
      this.slots[slot] = lastEntry;
      this.keyToSlot.set(lastEntry.key, slot);
      for (const column of this.columns.values()) {
        column.copySlot(slot, lastSlot);
      }
      this.addSlotToScalarIndexes(slot);
      this.insertSlotIntoOrderedIndexes(slot);
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

  private compareRawSlots(
    plan: TopicRawWindowScanPlan<object>,
  ): ((left: number, right: number) => number) | undefined {
    const storageOrderBy = plan.storageOrderBy;
    if (storageOrderBy === undefined) {
      return undefined;
    }
    const orderColumns = this.rawStorageOrderPlan(storageOrderBy);
    if (orderColumns === undefined) {
      return undefined;
    }
    return (left, right) =>
      compareSlotsByStorageOrder(this.rawWindowScanState, left, right, orderColumns);
  }

  private projectRawRow(slot: number, selectedFields: ReadonlyArray<string>): RowObject {
    const projected: Record<string, unknown> = {};
    for (const projection of this.rawProjectionPlan(selectedFields)) {
      const value = columnValue(projection.column, slot);
      projected[projection.field] = projection.shouldClone ? cloneUnknown(value) : value;
    }
    return projected;
  }

  slotForKey(key: string): number | undefined {
    return this.keyToSlot.get(key);
  }

  keyAtSlot(slot: number): string | undefined {
    return this.slots[slot]?.key;
  }

  prepareRow = Effect.fn("ColumnLiveViewEngine.topicRowStorage.row.prepare")(function* <
    Error,
    Row extends RowObject,
  >(this: TopicRowStorage, row: Row, invalidRow: InvalidRowErrorFactory<Error>) {
    return yield* prepareTopicRow(this.rowPreparation, row, invalidRow);
  });

  prepareRows = Effect.fn("ColumnLiveViewEngine.topicRowStorage.rows.prepare")(function* <
    Error,
    Row extends RowObject,
  >(this: TopicRowStorage, rows: ReadonlyArray<Row>, invalidRow: InvalidRowErrorFactory<Error>) {
    return yield* Effect.forEach(rows, (row) => this.prepareRow(row, invalidRow));
  });

  prepareRowWithStorageKey = Effect.fn(
    "ColumnLiveViewEngine.topicRowStorage.row.prepareWithStorageKey",
  )(function* <Error, Row extends RowObject>(
    this: TopicRowStorage,
    row: Row,
    storageKey: string,
    invalidRow: InvalidRowErrorFactory<Error>,
  ) {
    return yield* prepareTopicRowWithStorageKey(this.rowPreparation, row, storageKey, invalidRow);
  });

  preparePatch = Effect.fn("ColumnLiveViewEngine.topicRowStorage.patch.prepare")(function* <
    Patch extends Partial<RowObject>,
    Error,
  >(this: TopicRowStorage, key: string, patch: Patch, invalidRow: InvalidRowErrorFactory<Error>) {
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
    for (let index = 0; index < this.columnWritePlan.length; index += 1) {
      this.columnWritePlan[index]!.set(
        slot,
        trustedFieldValue(prepared.row, this.columnWriteFields[index]!),
      );
    }
  }

  private rawProjectionPlan(
    selectedFields: ReadonlyArray<string>,
  ): ReadonlyArray<RawProjectionColumn> {
    const cached = this.rawProjectionPlans.get(selectedFields);
    if (cached !== undefined) {
      return cached;
    }

    const plan = selectedFields.map((field) => {
      const column = this.columns.get(field)!;
      return {
        column,
        field,
        shouldClone: column.kind === "generic",
      };
    });
    this.rawProjectionPlans.set(selectedFields, plan);
    return plan;
  }

  private rawStorageOrderPlan(
    storageOrderBy: ReadonlyArray<TopicRawOrderByPlan>,
  ): ReadonlyArray<RawStorageOrderColumn> | undefined {
    const cached = this.rawStorageOrderPlans.get(storageOrderBy);
    if (cached !== undefined) {
      return cached;
    }
    const plan = compiledRawStorageOrder(this.rawWindowScanState, storageOrderBy);
    if (plan !== undefined) {
      this.rawStorageOrderPlans.set(storageOrderBy, plan);
    }
    return plan;
  }

  private createAppendBatchReservation(
    preparedRows: ReadonlyArray<PreparedTopicRow>,
  ): AppendBatchReservation {
    if (this.reservableColumns.length === 0) {
      return noopAppendBatchReservation;
    }
    let reserved = false;
    return {
      reserveFrom: (startIndex) => {
        if (reserved) {
          return;
        }
        reserved = true;
        let appendCount = 0;
        const appendKeys = new Set<string>();
        for (let index = startIndex; index < preparedRows.length; index += 1) {
          const key = preparedRows[index]!.key;
          if (!this.keyToSlot.has(key) && !appendKeys.has(key)) {
            appendKeys.add(key);
            appendCount += 1;
          }
        }
        const minimumCapacity = this.slots.length + appendCount;
        for (const column of this.reservableColumns) {
          column.reserve(minimumCapacity);
        }
      },
    };
  }

  private setPreparedInBatch(
    prepared: PreparedTopicRow,
    appendReservation: AppendBatchReservation,
    batchIndex: number,
  ): number {
    const existingSlot = this.keyToSlot.get(prepared.key);
    if (existingSlot !== undefined) {
      const replacement = this.preparedReplacementForCurrentSlot(prepared, existingSlot);
      if (replacement === undefined) {
        return 0;
      }
      this.removeSlotFromScalarIndexes(existingSlot);
      this.removeSlotFromReplacementOrderedIndexes(existingSlot, replacement.changedFields);
      this.writeSlot(existingSlot, prepared);
      this.addSlotToScalarIndexes(existingSlot);
      this.recordPreparedReplacementChange(prepared, replacement);
      this.insertSlotIntoReplacementOrderedIndexes(existingSlot, replacement.changedFields);
      return 1;
    }

    appendReservation.reserveFrom(batchIndex);
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
    return 1;
  }

  private insertSlotIntoOrderedIndexes(slot: number): void {
    insertSlotIntoRawWindowIndexes(this.rawWindowScanState, slot);
  }

  private removeSlotFromOrderedIndexes(slot: number): void {
    removeSlotFromRawWindowIndexes(this.rawWindowScanState, slot);
  }

  private insertSlotIntoReplacementOrderedIndexes(
    slot: number,
    changedFields: TopicRowChangedFields | undefined,
  ): void {
    for (const index of this.replacementOrderedIndexes(changedFields)) {
      insertSlotIntoRawWindowIndex(this.rawWindowScanState, index, slot);
    }
  }

  private removeSlotFromReplacementOrderedIndexes(
    slot: number,
    changedFields: TopicRowChangedFields | undefined,
  ): void {
    for (const index of this.replacementOrderedIndexes(changedFields)) {
      removeSlotFromRawWindowIndex(index, slot);
    }
  }

  private replacementOrderedIndexes(
    changedFields: TopicRowChangedFields | undefined,
  ): ReadonlyArray<OrderedSlotIndex> {
    if (changedFields === undefined) {
      return [...this.orderedSlotIndexes.values()];
    }
    return [...this.orderedSlotIndexes.values()].filter((index) =>
      index.orderBy.some((order) => changedFields.fields.has(order.field)),
    );
  }

  private setPreparedWithoutIndexMaintenance(
    prepared: PreparedTopicRow,
    appendReservation: AppendBatchReservation,
    batchIndex: number,
  ): number {
    const existingSlot = this.keyToSlot.get(prepared.key);
    if (existingSlot !== undefined) {
      const replacement = this.preparedReplacementForCurrentSlot(prepared, existingSlot);
      if (replacement === undefined) {
        return 0;
      }
      this.removeSlotFromScalarIndexes(existingSlot);
      this.writeSlot(existingSlot, prepared);
      this.addSlotToScalarIndexes(existingSlot);
      this.recordPreparedReplacementChange(prepared, replacement);
      return 1;
    }

    appendReservation.reserveFrom(batchIndex);
    const slot = this.slots.length;
    this.keyToSlot.set(prepared.key, slot);
    this.writeSlot(slot, prepared);
    this.addSlotToScalarIndexes(slot);
    this.recordRowChange({
      key: prepared.key,
      previous: undefined,
      next: prepared.row,
    });
    return 1;
  }

  private preparedReplacementForCurrentSlot(
    prepared: PreparedTopicRow,
    slot: number,
  ): PreparedTopicRowReplacement | undefined {
    const previous = this.slots[slot]!.row;
    if (rowsEqual(previous, prepared.row)) {
      return undefined;
    }
    if (prepared.source === "row") {
      return {
        changedFields: undefined,
        previous,
      };
    }
    return {
      changedFields: topicRowChangedFieldsFromRows(
        previous,
        prepared.row,
        this.rawQueryMetadata.fieldNames,
      ),
      previous,
    };
  }

  private recordRowChange(change: TopicRowChange<object>): void {
    this.rowChangeJournal.record(change, this.versionValue);
  }

  private recordPreparedReplacementChange(
    prepared: PreparedTopicRow,
    replacement: PreparedTopicRowReplacement,
  ): void {
    if (replacement.changedFields === undefined) {
      this.recordRowChange({
        key: prepared.key,
        previous: replacement.previous,
        next: prepared.row,
      });
      return;
    }
    this.recordRowChange({
      changedFields: replacement.changedFields,
      key: prepared.key,
      previous: replacement.previous,
      next: prepared.row,
    });
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
