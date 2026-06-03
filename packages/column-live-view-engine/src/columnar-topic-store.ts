import { Effect, Schema } from "effect";
import type { ActiveQueryStoreState } from "./active-query";
import type {
  TopicRawOrderByPlan,
  TopicRawPredicateFilterPlan,
  TopicRawWindowScanPlan,
  TopicRawWindowScanResult,
  TopicRowEntry,
  TopicRowVisitor,
} from "./row-scan";
import {
  compareQueryValue,
  rawQueryCompilerMetadata,
  type RawQueryCompilerMetadata,
} from "./raw-query-compiler";
import { cloneRow, fieldValue, isPlainRecord, valuesEqual } from "./row-values";

type RowObject = object;

type InvalidRowErrorFactory<Error> = (topic: string, message: string) => Error;
type ColumnValues = Array<unknown>;

type OrderedSlotIndex = {
  readonly orderBy: ReadonlyArray<TopicRawOrderByPlan>;
  readonly slots: Array<number>;
};

type OrderedRawWindow = {
  readonly limit: number;
  readonly slots: ReadonlyArray<number>;
};

export type PreparedTopicRow = {
  readonly key: string;
  readonly row: object;
};

export class ColumnarTopicStore {
  readonly rawQueryMetadata: RawQueryCompilerMetadata;
  readonly readModel: ActiveQueryStoreState;

  private readonly slots: Array<TopicRowEntry<object>> = [];
  private readonly keyToSlot = new Map<string, number>();
  private readonly columns = new Map<string, ColumnValues>();
  private readonly orderedSlotIndexes = new Map<string, OrderedSlotIndex>();
  private versionValue = 0;

  constructor(
    readonly topic: string,
    private readonly schema: Schema.Decoder<object>,
    private readonly keyField: string,
  ) {
    this.rawQueryMetadata = rawQueryCompilerMetadata(schema);
    for (const field of this.rawQueryMetadata.fieldNames) {
      this.columns.set(field, []);
    }
    this.readModel = {
      identity: this,
      topic,
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
    return this.versionValue;
  }

  clear(): void {
    this.slots.length = 0;
    this.keyToSlot.clear();
    this.orderedSlotIndexes.clear();
    for (const column of this.columns.values()) {
      column.length = 0;
    }
    this.versionValue = 0;
  }

  setPrepared(prepared: PreparedTopicRow): void {
    const existingSlot = this.keyToSlot.get(prepared.key);
    if (existingSlot !== undefined) {
      this.writeSlot(existingSlot, prepared);
      this.orderedSlotIndexes.clear();
      return;
    }

    const slot = this.slots.length;
    this.keyToSlot.set(prepared.key, slot);
    this.writeSlot(slot, prepared);
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
    this.keyToSlot.delete(key);
    if (slot !== lastSlot) {
      this.slots[slot] = lastEntry;
      this.keyToSlot.set(lastEntry.key, slot);
      for (const column of this.columns.values()) {
        column[slot] = column[lastSlot];
      }
    }
    this.slots.pop();
    for (const column of this.columns.values()) {
      column.pop();
    }
    return 1;
  }

  scanRows(visitor: TopicRowVisitor<object>): void {
    for (let slot = 0; slot < this.slots.length; slot += 1) {
      const entry = this.slots[slot]!;
      visitor(entry.key, entry.row);
    }
  }

  scanRawWindow(plan: TopicRawWindowScanPlan<object>): TopicRawWindowScanResult<object> {
    const orderedWindow = this.rawWindowOrderedWindow(plan);
    if (orderedWindow !== undefined) {
      return this.scanRawWindowOrderedSlots(plan, orderedWindow);
    }

    const compareSlots = this.rawWindowSlotComparator(plan);
    if (compareSlots !== undefined) {
      return this.scanRawWindowSlots(plan, compareSlots);
    }

    const filtered: Array<TopicRowEntry<object>> = [];
    for (let slot = 0; slot < this.slots.length; slot += 1) {
      const entry = this.slots[slot]!;
      if (this.slotMayMatchFilters(slot, plan.predicate.filters) && plan.matches(entry.row)) {
        filtered.push(entry);
      }
    }
    const ordered = filtered.toSorted(plan.compare);
    const window = ordered.slice(
      plan.offset,
      plan.limit === undefined ? undefined : plan.offset + plan.limit,
    );
    return {
      keys: window.map((entry) => entry.key),
      window,
      totalRows: filtered.length,
    };
  }

  private scanRawWindowOrderedSlots(
    plan: TopicRawWindowScanPlan<object>,
    orderedWindow: OrderedRawWindow,
  ): TopicRawWindowScanResult<object> {
    let totalRows = 0;
    const windowSlots: Array<number> = [];
    const windowEnd = plan.offset + orderedWindow.limit;
    for (const slot of orderedWindow.slots) {
      const entry = this.slots[slot]!;
      if (!this.slotMayMatchFilters(slot, plan.predicate.filters) || !plan.matches(entry.row)) {
        continue;
      }
      const matchIndex = totalRows;
      totalRows += 1;
      if (matchIndex >= plan.offset && matchIndex < windowEnd) {
        windowSlots.push(slot);
      }
    }
    const window = windowSlots.map((slot) => this.slots[slot]!);
    return {
      keys: window.map((entry) => entry.key),
      window,
      totalRows,
    };
  }

  private scanRawWindowSlots(
    plan: TopicRawWindowScanPlan<object>,
    compareSlots: (left: number, right: number) => number,
  ): TopicRawWindowScanResult<object> {
    let totalRows = 0;
    const filteredSlots: Array<number> = [];
    for (let slot = 0; slot < this.slots.length; slot += 1) {
      const entry = this.slots[slot]!;
      if (!this.slotMayMatchFilters(slot, plan.predicate.filters) || !plan.matches(entry.row)) {
        continue;
      }
      totalRows += 1;
      if (plan.limit !== 0) {
        filteredSlots.push(slot);
      }
    }
    filteredSlots.sort(compareSlots);
    const windowSlots = filteredSlots.slice(
      plan.offset,
      plan.limit === undefined ? undefined : plan.offset + plan.limit,
    );
    const window = windowSlots.map((slot) => this.slots[slot]!);
    return {
      keys: window.map((entry) => entry.key),
      window,
      totalRows,
    };
  }

  private rawWindowOrderedWindow(
    plan: TopicRawWindowScanPlan<object>,
  ): OrderedRawWindow | undefined {
    const storageOrderBy = plan.storageOrderBy;
    if (
      plan.limit === undefined ||
      !Number.isSafeInteger(plan.limit) ||
      plan.limit <= 0 ||
      storageOrderBy === undefined ||
      storageOrderBy.length !== 1
    ) {
      return undefined;
    }
    if (
      plan.predicate.callbackRequired ||
      !predicateFiltersAreOrderedIndexAdmissible(plan.predicate.filters)
    ) {
      return undefined;
    }
    if (!this.storageOrderByFieldsExist(storageOrderBy)) {
      return undefined;
    }

    const indexKey = orderedSlotIndexKey(storageOrderBy);
    const existing = this.orderedSlotIndexes.get(indexKey);
    if (existing !== undefined) {
      return {
        limit: plan.limit,
        slots: existing.slots,
      };
    }

    const slots = Array.from({ length: this.slots.length }, (_value, slot) => slot);
    slots.sort((left, right) => this.compareSlotsByStorageOrder(left, right, storageOrderBy));
    this.orderedSlotIndexes.set(indexKey, {
      orderBy: storageOrderBy,
      slots,
    });
    return {
      limit: plan.limit,
      slots,
    };
  }

  private rawWindowSlotComparator(
    plan: TopicRawWindowScanPlan<object>,
  ): ((left: number, right: number) => number) | undefined {
    const storageOrderBy = plan.storageOrderBy;
    if (storageOrderBy === undefined) {
      return undefined;
    }
    if (!this.storageOrderByFieldsExist(storageOrderBy)) {
      return undefined;
    }

    return (left, right) => {
      return this.compareSlotsByStorageOrder(left, right, storageOrderBy);
    };
  }

  private compareSlotsByStorageOrder(
    left: number,
    right: number,
    storageOrderBy: ReadonlyArray<TopicRawOrderByPlan>,
  ): number {
    for (const order of storageOrderBy) {
      const column = this.columns.get(order.field)!;
      const comparison = compareQueryValue(column[left], column[right]);
      if (comparison !== undefined && comparison !== 0) {
        return order.direction === "asc" ? comparison : -comparison;
      }
    }
    const leftKey = this.slots[left]!.key;
    const rightKey = this.slots[right]!.key;
    return Number(leftKey > rightKey) - Number(leftKey < rightKey);
  }

  private storageOrderByFieldsExist(storageOrderBy: ReadonlyArray<TopicRawOrderByPlan>): boolean {
    for (const order of storageOrderBy) {
      if (!this.columns.has(order.field)) {
        return false;
      }
    }
    return true;
  }

  prepareRow = Effect.fn("ColumnLiveViewEngine.columnarTopicStore.row.prepare")(function* <
    Error,
    Row extends RowObject,
  >(this: ColumnarTopicStore, row: Row, invalidRow: InvalidRowErrorFactory<Error>) {
    const decoded = yield* this.decodeRow(row, invalidRow);
    const key = yield* this.rowKey(decoded, invalidRow);
    return {
      key,
      row: decoded,
    } satisfies PreparedTopicRow;
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
    yield* this.validatePatchKeys(patch, invalidRow);
    const current = this.rowForKey(key);
    if (current === undefined) {
      return yield* Effect.fail(invalidRow(this.topic, `Cannot patch missing key: ${key}`));
    }
    const decoded = yield* this.decodeRow({ ...current, ...patch }, invalidRow);
    const decodedKey = yield* this.rowKey(decoded, invalidRow);
    if (decodedKey !== key) {
      return yield* Effect.fail(invalidRow(this.topic, "Patch must not change the row key."));
    }
    return {
      key,
      row: decoded,
    } satisfies PreparedTopicRow;
  });

  private decodeRow = Effect.fn("ColumnLiveViewEngine.columnarTopicStore.row.decode")(function* <
    Error,
  >(this: ColumnarTopicStore, row: RowObject, invalidRow: InvalidRowErrorFactory<Error>) {
    return yield* Effect.try({
      try: () => {
        const decoded = Schema.decodeUnknownSync(this.schema)(row);
        return cloneRow(decoded);
      },
      catch: (cause) => invalidRow(this.topic, String(cause)),
    });
  });

  private rowKey = Effect.fn("ColumnLiveViewEngine.columnarTopicStore.row.key")(function* <Error>(
    this: ColumnarTopicStore,
    row: RowObject,
    invalidRow: InvalidRowErrorFactory<Error>,
  ) {
    const key = fieldValue(row, this.keyField);
    if (typeof key !== "string") {
      return yield* Effect.fail(
        invalidRow(this.topic, `Key field ${this.keyField} must decode to a string.`),
      );
    }
    return key;
  });

  private validatePatchKeys = Effect.fn(
    "ColumnLiveViewEngine.columnarTopicStore.patchKeys.validate",
  )(function* <Error>(
    this: ColumnarTopicStore,
    patch: unknown,
    invalidRow: InvalidRowErrorFactory<Error>,
  ) {
    if (!isPlainRecord(patch)) {
      return yield* Effect.fail(invalidRow(this.topic, "Patch must be a plain object."));
    }
    for (const key of Reflect.ownKeys(patch)) {
      if (typeof key !== "string" || !this.rawQueryMetadata.fieldNames.has(key)) {
        return yield* Effect.fail(
          invalidRow(this.topic, `Patch contains unknown field: ${String(key)}.`),
        );
      }
    }
  });

  private writeSlot(slot: number, prepared: PreparedTopicRow): void {
    this.slots[slot] = {
      key: prepared.key,
      row: prepared.row,
    };
    for (const [field, column] of this.columns) {
      column[slot] = fieldValue(prepared.row, field);
    }
  }

  private insertSlotIntoOrderedIndexes(slot: number): void {
    for (const index of this.orderedSlotIndexes.values()) {
      const insertAt = orderedSlotIndexInsertionPoint(index.slots, slot, (left, right) =>
        this.compareSlotsByStorageOrder(left, right, index.orderBy),
      );
      index.slots.splice(insertAt, 0, slot);
    }
  }

  private setPreparedWithoutIndexMaintenance(prepared: PreparedTopicRow): void {
    const existingSlot = this.keyToSlot.get(prepared.key);
    if (existingSlot !== undefined) {
      this.writeSlot(existingSlot, prepared);
      return;
    }

    const slot = this.slots.length;
    this.keyToSlot.set(prepared.key, slot);
    this.writeSlot(slot, prepared);
  }

  private rowForKey(key: string): object | undefined {
    const slot = this.keyToSlot.get(key);
    if (slot === undefined) {
      return undefined;
    }
    return this.slots[slot]!.row;
  }

  private slotMayMatchFilters(
    slot: number,
    filters: ReadonlyArray<TopicRawPredicateFilterPlan>,
  ): boolean {
    for (const filter of filters) {
      if (!this.slotMayMatchFilter(slot, filter)) {
        return false;
      }
    }
    return true;
  }

  private slotMayMatchFilter(slot: number, filter: TopicRawPredicateFilterPlan): boolean {
    const column = this.columns.get(filter.field);
    if (column === undefined) {
      return true;
    }
    const value = column[slot];

    if (filter.operator === "eq") {
      return valuesEqual(value, filter.value);
    }
    if (filter.operator === "neq") {
      return !valuesEqual(value, filter.value);
    }
    if (filter.operator === "in") {
      return filter.values.some((candidate) => valuesEqual(value, candidate));
    }
    if (filter.operator === "startsWith") {
      if (typeof filter.value !== "string") {
        return true;
      }
      return typeof value === "string" && value.startsWith(filter.value);
    }

    const comparison = compareRangeColumnValue(value, filter.value);
    if (comparison === undefined) {
      return true;
    }
    if (filter.operator === "gt") {
      return comparison > 0;
    }
    if (filter.operator === "gte") {
      return comparison >= 0;
    }
    if (filter.operator === "lt") {
      return comparison < 0;
    }
    return comparison <= 0;
  }
}

const compareRangeColumnValue = (left: unknown, right: unknown): number | undefined => {
  if (typeof left === "number" && typeof right === "number") {
    if (!Number.isFinite(left) || !Number.isFinite(right)) {
      return undefined;
    }
    return left - right;
  }
  if (typeof left === "bigint" && typeof right === "bigint") {
    if (left === right) {
      return 0;
    }
    return left < right ? -1 : 1;
  }
  return undefined;
};

const orderedSlotIndexKey = (orderBy: ReadonlyArray<TopicRawOrderByPlan>): string => {
  const order = orderBy[0]!;
  return `${order.field.length}:${order.field}:${order.direction}`;
};

const orderedSlotIndexInsertionPoint = (
  slots: ReadonlyArray<number>,
  slot: number,
  compareSlots: (left: number, right: number) => number,
): number => {
  let low = 0;
  let high = slots.length;
  while (low < high) {
    const middle = Math.floor((low + high) / 2);
    if (compareSlots(slots[middle]!, slot) <= 0) {
      low = middle + 1;
    } else {
      high = middle;
    }
  }
  return low;
};

const predicateFiltersAreOrderedIndexAdmissible = (
  filters: ReadonlyArray<TopicRawPredicateFilterPlan>,
): boolean => {
  for (const filter of filters) {
    if (filter.operator !== "eq" && filter.operator !== "in") {
      return false;
    }
  }
  return true;
};
