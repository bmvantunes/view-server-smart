import { Effect, Schema } from "effect";
import { createActiveQueryRegistry, type ActiveQueryStoreState } from "./active-query";
import type {
  TopicRawOrderByPlan,
  TopicRawPredicateFilterPlan,
  TopicRawWindowScanPlan,
  TopicRawWindowScanResult,
  TopicRowChange,
  TopicRowChangeBatch,
  TopicRowEntry,
  TopicRowVisitor,
} from "./row-scan";
import {
  columnValueDoesNotEqual,
  compareExactRangeColumnValue,
  compareRangeColumnValue,
  isComparableRangeValue,
} from "./topic-range-value";
import {
  selectedPredicateCandidateFilter,
  type PredicateCandidateFilter,
} from "./topic-predicate-candidate-filter";
import {
  compareQueryValue,
  isRangePlanValue,
  rawQueryCompilerMetadata,
  type RawQueryCompilerMetadata,
} from "./raw-query-compiler";
import { cloneRow, fieldValue, isPlainRecord, scalarEqualityKey, valuesEqual } from "./row-values";
import { TopicRowChangeJournal } from "./topic-row-change-journal";

type RowObject = object;

type InvalidRowErrorFactory<Error> = (topic: string, message: string) => Error;
type ColumnValues = Array<unknown>;

type OrderedSlotIndex = {
  readonly orderBy: ReadonlyArray<TopicRawOrderByPlan>;
  readonly slots: Array<number>;
};

type OrderedRawWindowSpan = {
  readonly endIndex: number;
  readonly startIndex: number;
};

type OrderedRawWindow = {
  readonly candidateExcludedField: string;
  readonly limit: number;
  readonly slots: ReadonlyArray<number>;
  readonly spans: ReadonlyArray<OrderedRawWindowSpan>;
};

type OrderedRangeBound = {
  readonly exclusive: boolean;
  readonly value: unknown;
};

type OrderedRangeBounds = {
  readonly lower: OrderedRangeBound | undefined;
  readonly upper: OrderedRangeBound | undefined;
};

type TopicRawRangePredicateFilterPlan = TopicRawPredicateFilterPlan & {
  readonly operator: "gt" | "gte" | "lt" | "lte";
  readonly value: unknown;
};

type TopicRawEqualityPredicateFilterPlan = TopicRawPredicateFilterPlan & {
  readonly operator: "eq";
  readonly value: unknown;
};

type TopicRawInPredicateFilterPlan = TopicRawPredicateFilterPlan & {
  readonly operator: "in";
  readonly values: ReadonlyArray<unknown>;
};

const noOrderedRangeBounds: OrderedRangeBounds = {
  lower: undefined,
  upper: undefined,
};
const maxBoundedRawWindowEnd = 1_024;

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
  private readonly rowChangeJournal = new TopicRowChangeJournal<object>();
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
    this.rowChangeJournal.clear(this.versionValue);
    for (const column of this.columns.values()) {
      column.length = 0;
    }
    this.versionValue = 0;
  }

  setPrepared(prepared: PreparedTopicRow): void {
    const existingSlot = this.keyToSlot.get(prepared.key);
    if (existingSlot !== undefined) {
      const previous = this.slots[existingSlot]!.row;
      this.writeSlot(existingSlot, prepared);
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
    const orderedWindow = this.rawWindowOrderedWindow(plan);
    if (orderedWindow !== undefined) {
      const candidateFilter =
        plan.predicate.callbackSkippable === true &&
        orderedRawWindowSlotCount(orderedWindow) * 2 > this.slots.length
          ? selectedPredicateCandidateFilter(
              plan.predicate.filters,
              this.columns,
              this.slots.length,
              orderedWindow.candidateExcludedField,
            )
          : undefined;
      return this.scanRawWindowOrderedSlots(plan, orderedWindow, candidateFilter);
    }

    const compareSlots =
      this.rawWindowSlotComparator(plan) ??
      ((left, right) => plan.compare(this.slots[left]!, this.slots[right]!));
    return this.scanRawWindowSlots(plan, compareSlots);
  }

  private scanRawWindowOrderedSlots(
    plan: TopicRawWindowScanPlan<object>,
    orderedWindow: OrderedRawWindow,
    candidateFilter: PredicateCandidateFilter | undefined,
  ): TopicRawWindowScanResult<object> {
    let totalRows = 0;
    const windowSlots: Array<number> = [];
    const windowEnd = plan.offset + orderedWindow.limit;
    for (const span of orderedWindow.spans) {
      for (let slotIndex = span.startIndex; slotIndex < span.endIndex; slotIndex += 1) {
        const slot = orderedWindow.slots[slotIndex]!;
        if (
          candidateFilter !== undefined &&
          !candidateFilter.matches(candidateFilter.column[slot])
        ) {
          continue;
        }
        const entry = this.slots[slot]!;
        if (!this.slotMatchesPredicatePlan(slot, plan, entry.row)) {
          continue;
        }
        const matchIndex = totalRows;
        totalRows += 1;
        if (matchIndex >= plan.offset && matchIndex < windowEnd) {
          windowSlots.push(slot);
        }
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
    const candidateFilter =
      plan.predicate.callbackSkippable === true
        ? selectedPredicateCandidateFilter(plan.predicate.filters, this.columns, this.slots.length)
        : undefined;
    const boundedWindowEnd = boundedRawWindowEnd(plan);
    if (boundedWindowEnd !== undefined) {
      return this.scanRawWindowBoundedSlots(plan, compareSlots, boundedWindowEnd, candidateFilter);
    }

    let totalRows = 0;
    const filteredSlots: Array<number> = [];
    for (let slot = 0; slot < this.slots.length; slot += 1) {
      if (candidateFilter !== undefined && !candidateFilter.matches(candidateFilter.column[slot])) {
        continue;
      }
      const entry = this.slots[slot]!;
      if (!this.slotMatchesPredicatePlan(slot, plan, entry.row)) {
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

  private scanRawWindowBoundedSlots(
    plan: TopicRawWindowScanPlan<object>,
    compareSlots: (left: number, right: number) => number,
    windowEnd: number,
    candidateFilter: PredicateCandidateFilter | undefined,
  ): TopicRawWindowScanResult<object> {
    let totalRows = 0;
    const windowSlots: Array<number> = [];
    for (let slot = 0; slot < this.slots.length; slot += 1) {
      if (candidateFilter !== undefined && !candidateFilter.matches(candidateFilter.column[slot])) {
        continue;
      }
      const entry = this.slots[slot]!;
      if (!this.slotMatchesPredicatePlan(slot, plan, entry.row)) {
        continue;
      }
      totalRows += 1;
      if (windowSlots.length < windowEnd) {
        const insertAt = orderedSlotIndexInsertionPoint(windowSlots, slot, compareSlots);
        windowSlots.splice(insertAt, 0, slot);
        continue;
      }
      const worstSlot = windowSlots[windowSlots.length - 1]!;
      if (compareSlots(slot, worstSlot) < 0) {
        const insertAt = orderedSlotIndexInsertionPoint(windowSlots, slot, compareSlots);
        windowSlots.splice(insertAt, 0, slot);
        windowSlots.pop();
      }
    }
    const window = windowSlots.slice(plan.offset).map((slot) => this.slots[slot]!);
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
    const orderField = storageOrderBy[0]!.field;
    if (plan.predicate.callbackSkippable !== true) {
      return undefined;
    }
    if (!predicateFiltersAreOrderedIndexAdmissible(plan.predicate.filters, orderField)) {
      return undefined;
    }
    if (!this.storageOrderByFieldsExist(storageOrderBy)) {
      return undefined;
    }

    const rangeBounds = orderedRangeBoundsForField(
      plan.predicate.filters,
      orderField,
      this.rawQueryMetadata,
    );
    if (rangeBounds !== undefined && rangeBoundsAreEmpty(rangeBounds)) {
      return {
        candidateExcludedField: orderField,
        limit: plan.limit,
        slots: [],
        spans: [],
      };
    }
    const seekBounds = rangeBounds ?? noOrderedRangeBounds;
    const equalityValues = orderedEqualityValuesForField(
      plan.predicate.filters,
      orderField,
      this.rawQueryMetadata,
    );
    const indexKey = orderedSlotIndexKey(storageOrderBy);
    const existing = this.orderedSlotIndexes.get(indexKey);
    if (existing !== undefined) {
      return {
        candidateExcludedField: orderField,
        limit: plan.limit,
        slots: existing.slots,
        spans: this.orderedSlotIndexSpans(existing, seekBounds, equalityValues),
      };
    }

    const slots = Array.from({ length: this.slots.length }, (_value, slot) => slot);
    slots.sort((left, right) => this.compareSlotsByStorageOrder(left, right, storageOrderBy));
    this.orderedSlotIndexes.set(indexKey, {
      orderBy: storageOrderBy,
      slots,
    });
    return {
      candidateExcludedField: orderField,
      limit: plan.limit,
      slots,
      spans: this.orderedSlotIndexSpans(
        { orderBy: storageOrderBy, slots },
        seekBounds,
        equalityValues,
      ),
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
      if (comparison !== 0) {
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

  private orderedSlotIndexSpans(
    index: OrderedSlotIndex,
    rangeBounds: OrderedRangeBounds,
    equalityValues: ReadonlyArray<unknown> | undefined,
  ): ReadonlyArray<OrderedRawWindowSpan> {
    if (equalityValues === undefined) {
      return [this.orderedSlotIndexBounds(index, rangeBounds)];
    }
    const seekValues = distinctOrderedEqualityValues(equalityValues).filter((value) =>
      equalityValueSatisfiesRangeBounds(value, rangeBounds),
    );
    const spans = seekValues.map((value) =>
      this.orderedSlotIndexBounds(index, {
        lower: {
          exclusive: false,
          value,
        },
        upper: {
          exclusive: false,
          value,
        },
      }),
    );
    return orderedWindowSpansInIndexOrder(spans);
  }

  private orderedSlotIndexBounds(
    index: OrderedSlotIndex,
    rangeBounds: OrderedRangeBounds,
  ): OrderedRawWindowSpan {
    const order = index.orderBy[0]!;
    const column = this.columns.get(order.field)!;
    if (order.direction === "asc") {
      const startIndex =
        rangeBounds.lower === undefined
          ? 0
          : orderedSlotBoundIndex(
              index.slots,
              column,
              rangeBounds.lower.value,
              rangeBounds.lower.exclusive
                ? (comparison) => comparison > 0
                : (comparison) => comparison >= 0,
            );
      const endIndex =
        rangeBounds.upper === undefined
          ? index.slots.length
          : orderedSlotBoundIndex(
              index.slots,
              column,
              rangeBounds.upper.value,
              rangeBounds.upper.exclusive
                ? (comparison) => comparison >= 0
                : (comparison) => comparison > 0,
            );
      return {
        endIndex: Math.max(startIndex, endIndex),
        startIndex,
      };
    }
    const startIndex =
      rangeBounds.upper === undefined
        ? 0
        : orderedSlotBoundIndex(
            index.slots,
            column,
            rangeBounds.upper.value,
            rangeBounds.upper.exclusive
              ? (comparison) => comparison < 0
              : (comparison) => comparison <= 0,
          );
    const endIndex =
      rangeBounds.lower === undefined
        ? index.slots.length
        : orderedSlotBoundIndex(
            index.slots,
            column,
            rangeBounds.lower.value,
            rangeBounds.lower.exclusive
              ? (comparison) => comparison <= 0
              : (comparison) => comparison < 0,
          );
    return {
      endIndex: Math.max(startIndex, endIndex),
      startIndex,
    };
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
      const previous = this.slots[existingSlot]!.row;
      this.writeSlot(existingSlot, prepared);
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
    this.recordRowChange({
      key: prepared.key,
      previous: undefined,
      next: prepared.row,
    });
  }

  private recordRowChange(change: TopicRowChange<object>): void {
    this.rowChangeJournal.record(change, this.versionValue);
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

  private slotMatchesPredicatePlan(
    slot: number,
    plan: TopicRawWindowScanPlan<object>,
    row: object,
  ): boolean {
    const exact = plan.predicate.callbackSkippable === true;
    if (!this.slotMayMatchFilters(slot, plan.predicate.filters, exact)) {
      return false;
    }
    return exact || plan.matches(row);
  }

  private slotMayMatchFilters(
    slot: number,
    filters: ReadonlyArray<TopicRawPredicateFilterPlan>,
    exact: boolean,
  ): boolean {
    for (const filter of filters) {
      if (!this.slotMayMatchFilter(slot, filter, exact)) {
        return false;
      }
    }
    return true;
  }

  private slotMayMatchFilter(
    slot: number,
    filter: TopicRawPredicateFilterPlan,
    exact: boolean,
  ): boolean {
    const column = this.columns.get(filter.field);
    if (column === undefined) {
      return true;
    }
    const value = column[slot];

    if (filter.operator === "eq") {
      return valuesEqual(value, filter.value);
    }
    if (filter.operator === "neq") {
      if (exact) {
        return columnValueDoesNotEqual(value, filter.value);
      }
      return !valuesEqual(value, filter.value);
    }
    if (filter.operator === "in") {
      if (filter.valueKeys !== undefined) {
        const key = scalarEqualityKey(value);
        return key !== undefined && filter.valueKeys.has(key);
      }
      return filter.values.some((candidate) => valuesEqual(value, candidate));
    }
    if (filter.operator === "startsWith") {
      if (typeof filter.value !== "string") {
        return true;
      }
      return typeof value === "string" && value.startsWith(filter.value);
    }

    if (exact && !isComparableRangeValue(filter.value)) {
      return true;
    }
    if (exact) {
      const exactComparison = compareExactRangeColumnValue(value, filter.value);
      if (exactComparison === undefined) {
        return false;
      }
      if (filter.operator === "gt") {
        return exactComparison > 0;
      }
      if (filter.operator === "gte") {
        return exactComparison >= 0;
      }
      if (filter.operator === "lt") {
        return exactComparison < 0;
      }
      return exactComparison <= 0;
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

const orderedSlotIndexKey = (orderBy: ReadonlyArray<TopicRawOrderByPlan>): string => {
  const order = orderBy[0]!;
  return `${order.field.length}:${order.field}:${order.direction}`;
};

const boundedRawWindowEnd = (plan: TopicRawWindowScanPlan<object>): number | undefined => {
  if (plan.limit === undefined || plan.limit <= 0) {
    return undefined;
  }
  if (!Number.isSafeInteger(plan.offset) || plan.offset < 0 || !Number.isSafeInteger(plan.limit)) {
    return undefined;
  }
  const windowEnd = plan.offset + plan.limit;
  if (!Number.isSafeInteger(windowEnd) || windowEnd > maxBoundedRawWindowEnd) {
    return undefined;
  }
  return windowEnd;
};

const orderedRawWindowSlotCount = (window: OrderedRawWindow): number => {
  let count = 0;
  for (const span of window.spans) {
    count += span.endIndex - span.startIndex;
  }
  return count;
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
  orderField: string,
): boolean => {
  for (const filter of filters) {
    if (filter.operator === "eq" || filter.operator === "in") {
      continue;
    }
    if (isRangeFilterPlan(filter) && filter.field === orderField) {
      continue;
    }
    return false;
  }
  return true;
};

const orderedEqualityValuesForField = (
  filters: ReadonlyArray<TopicRawPredicateFilterPlan>,
  field: string,
  metadata: RawQueryCompilerMetadata,
): ReadonlyArray<unknown> | undefined => {
  let values: ReadonlyArray<unknown> = [];
  let hasEqualityFilter = false;
  let hasSafeEqualityFilter = false;
  let hasUnsafeEqualityFilter = false;
  let hasEmptyInFilter = false;
  for (const filter of filters) {
    if (filter.field !== field || !isEqualityFilterPlan(filter)) {
      continue;
    }
    hasEqualityFilter = true;
    const nextValues: Array<unknown> = [];
    if (filter.operator === "eq") {
      if (isEqualitySeekPlanValue(field, filter.value, metadata)) {
        nextValues.push(filter.value);
      } else {
        hasUnsafeEqualityFilter = true;
      }
    } else if (filter.values.length === 0) {
      hasEmptyInFilter = true;
    } else {
      for (const value of filter.values) {
        if (isEqualitySeekPlanValue(field, value, metadata)) {
          nextValues.push(value);
        } else {
          hasUnsafeEqualityFilter = true;
        }
      }
    }
    if (nextValues.length > 0) {
      if (!hasSafeEqualityFilter) {
        values = nextValues;
        hasSafeEqualityFilter = true;
      } else {
        values = intersectOrderedEqualityValues(values, nextValues);
      }
    }
  }
  if (hasEmptyInFilter) {
    return [];
  }
  if (hasUnsafeEqualityFilter || !hasEqualityFilter) {
    return undefined;
  }
  return values;
};

const isEqualitySeekPlanValue = (
  field: string,
  value: unknown,
  metadata: RawQueryCompilerMetadata,
): boolean => {
  if (metadata.stringFieldNames.has(field)) {
    return typeof value === "string";
  }
  return isRangePlanValue(field, value, metadata);
};

const orderedRangeBoundsForField = (
  filters: ReadonlyArray<TopicRawPredicateFilterPlan>,
  field: string,
  metadata: RawQueryCompilerMetadata,
): OrderedRangeBounds | undefined => {
  let lower: OrderedRangeBound | undefined;
  let upper: OrderedRangeBound | undefined;
  for (const filter of filters) {
    if (filter.field !== field || !isRangeFilterPlan(filter)) {
      continue;
    }
    if (!isRangePlanValue(field, filter.value, metadata)) {
      return undefined;
    }
    switch (filter.operator) {
      case "gt": {
        lower = strongerLowerBound(lower, {
          exclusive: true,
          value: filter.value,
        });
        break;
      }
      case "gte": {
        lower = strongerLowerBound(lower, {
          exclusive: false,
          value: filter.value,
        });
        break;
      }
      case "lt": {
        upper = strongerUpperBound(upper, {
          exclusive: true,
          value: filter.value,
        });
        break;
      }
      case "lte": {
        upper = strongerUpperBound(upper, {
          exclusive: false,
          value: filter.value,
        });
        break;
      }
    }
  }
  return {
    lower,
    upper,
  };
};

const strongerLowerBound = (
  current: OrderedRangeBound | undefined,
  candidate: OrderedRangeBound,
): OrderedRangeBound => {
  if (current === undefined) {
    return candidate;
  }
  const comparison = compareOrderedRangeValue(candidate.value, current.value);
  if (comparison > 0) {
    return candidate;
  }
  if (comparison === 0 && candidate.exclusive && !current.exclusive) {
    return candidate;
  }
  return current;
};

const strongerUpperBound = (
  current: OrderedRangeBound | undefined,
  candidate: OrderedRangeBound,
): OrderedRangeBound => {
  if (current === undefined) {
    return candidate;
  }
  const comparison = compareOrderedRangeValue(candidate.value, current.value);
  if (comparison < 0) {
    return candidate;
  }
  if (comparison === 0 && candidate.exclusive && !current.exclusive) {
    return candidate;
  }
  return current;
};

const rangeBoundsAreEmpty = (bounds: OrderedRangeBounds): boolean => {
  if (bounds.lower === undefined || bounds.upper === undefined) {
    return false;
  }
  const comparison = compareOrderedRangeValue(bounds.lower.value, bounds.upper.value);
  if (comparison > 0) {
    return true;
  }
  return comparison === 0 && (bounds.lower.exclusive || bounds.upper.exclusive);
};

const orderedSlotBoundIndex = (
  slots: ReadonlyArray<number>,
  column: ColumnValues,
  value: unknown,
  predicate: (comparison: number) => boolean,
): number => {
  let low = 0;
  let high = slots.length;
  while (low < high) {
    const middle = Math.floor((low + high) / 2);
    const comparison = compareOrderedRangeValue(column[slots[middle]!], value);
    if (predicate(comparison)) {
      high = middle;
    } else {
      low = middle + 1;
    }
  }
  return low;
};

const orderedWindowSpansInIndexOrder = (
  spans: ReadonlyArray<OrderedRawWindowSpan>,
): ReadonlyArray<OrderedRawWindowSpan> => {
  return spans
    .filter((span) => span.startIndex < span.endIndex)
    .toSorted((left, right) => left.startIndex - right.startIndex);
};

const distinctOrderedEqualityValues = (values: ReadonlyArray<unknown>): ReadonlyArray<unknown> => {
  const sorted = values.toSorted(compareOrderedRangeValue);
  const distinct: Array<unknown> = [];
  for (const value of sorted) {
    const previous = distinct.at(-1);
    if (previous === undefined || compareOrderedRangeValue(previous, value) !== 0) {
      distinct.push(value);
    }
  }
  return distinct;
};

const intersectOrderedEqualityValues = (
  leftValues: ReadonlyArray<unknown>,
  rightValues: ReadonlyArray<unknown>,
): ReadonlyArray<unknown> => {
  const left = distinctOrderedEqualityValues(leftValues);
  const right = distinctOrderedEqualityValues(rightValues);
  const intersection: Array<unknown> = [];
  let rightIndex = 0;
  for (const leftValue of left) {
    while (
      rightIndex < right.length &&
      compareOrderedRangeValue(right[rightIndex]!, leftValue) < 0
    ) {
      rightIndex += 1;
    }
    if (
      rightIndex < right.length &&
      compareOrderedRangeValue(right[rightIndex]!, leftValue) === 0
    ) {
      intersection.push(leftValue);
    }
  }
  return intersection;
};

const equalityValueSatisfiesRangeBounds = (
  value: unknown,
  rangeBounds: OrderedRangeBounds,
): boolean => {
  if (rangeBounds.lower !== undefined) {
    const comparison = compareOrderedRangeValue(value, rangeBounds.lower.value);
    if (comparison < 0 || (comparison === 0 && rangeBounds.lower.exclusive)) {
      return false;
    }
  }
  if (rangeBounds.upper !== undefined) {
    const comparison = compareOrderedRangeValue(value, rangeBounds.upper.value);
    if (comparison > 0 || (comparison === 0 && rangeBounds.upper.exclusive)) {
      return false;
    }
  }
  return true;
};

const compareOrderedRangeValue = (left: unknown, right: unknown): number =>
  compareQueryValue(left, right);

const isEqualityFilterPlan = (
  filter: TopicRawPredicateFilterPlan,
): filter is TopicRawEqualityPredicateFilterPlan | TopicRawInPredicateFilterPlan => {
  if (filter.operator === "eq" || filter.operator === "in") {
    return true;
  }
  return false;
};

const isRangeFilterPlan = (
  filter: TopicRawPredicateFilterPlan,
): filter is TopicRawRangePredicateFilterPlan => {
  if (
    filter.operator === "gt" ||
    filter.operator === "gte" ||
    filter.operator === "lt" ||
    filter.operator === "lte"
  ) {
    return true;
  }
  return false;
};
