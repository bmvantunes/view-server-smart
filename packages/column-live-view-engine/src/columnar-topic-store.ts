import { Effect, Schema } from "effect";
import type { ActiveQueryStoreState } from "./active-query";
import { rawQueryCompilerMetadata, type RawQueryCompilerMetadata } from "./raw-query-compiler";
import { cloneRow, fieldValue, isPlainRecord } from "./row-values";

type RowObject = object;

type InvalidRowErrorFactory<Error> = (topic: string, message: string) => Error;

export type PreparedTopicRow = {
  readonly key: string;
  readonly row: object;
};

export class ColumnarTopicStore {
  readonly rawQueryMetadata: RawQueryCompilerMetadata;
  readonly readModel: ActiveQueryStoreState;

  private readonly rows = new Map<string, object>();
  private versionValue = 0;

  constructor(
    readonly topic: string,
    private readonly schema: Schema.Decoder<object>,
    private readonly keyField: string,
  ) {
    this.rawQueryMetadata = rawQueryCompilerMetadata(schema);
    this.readModel = {
      identity: this,
      topic,
      rows: () => this.rows,
      version: () => this.versionValue,
    };
  }

  get rowCount(): number {
    return this.rows.size;
  }

  get version(): number {
    return this.versionValue;
  }

  advanceVersion(): number {
    this.versionValue += 1;
    return this.versionValue;
  }

  clear(): void {
    this.rows.clear();
    this.versionValue = 0;
  }

  setPrepared(prepared: PreparedTopicRow): void {
    this.rows.set(prepared.key, prepared.row);
  }

  setPreparedMany(preparedRows: ReadonlyArray<PreparedTopicRow>): void {
    for (const prepared of preparedRows) {
      this.setPrepared(prepared);
    }
  }

  delete(key: string): number {
    return this.rows.delete(key) ? 1 : 0;
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
    const current = this.rows.get(key);
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
}
