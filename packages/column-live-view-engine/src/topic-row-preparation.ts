import { Effect, Schema } from "effect";
import { topicRowChangedFieldsFromRows, type TopicRowChangedFields } from "./row-scan";
import { cloneRow, fieldValue, isPlainRecord } from "./row-values";

type RowObject = object;

export type InvalidRowErrorFactory<Error> = (topic: string, message: string) => Error;

export type PreparedTopicRow = {
  readonly changedFields?: TopicRowChangedFields;
  readonly key: string;
  readonly row: object;
  readonly source: "patch" | "row";
};

export type TopicRowPreparationContext = {
  readonly fieldNames: ReadonlySet<string>;
  readonly keyField: string;
  readonly schema: Schema.Codec<object, unknown, never, unknown>;
  readonly topic: string;
};

const decodeTopicRow = Effect.fn("ColumnLiveViewEngine.topicRow.decode")(function* <Error>(
  context: TopicRowPreparationContext,
  row: RowObject,
  invalidRow: InvalidRowErrorFactory<Error>,
) {
  return yield* Effect.try({
    try: () => {
      const decoded = Schema.decodeUnknownSync(context.schema)(row);
      const cloned = cloneRow(decoded);
      for (const field of context.fieldNames) {
        if (!Object.hasOwn(cloned, field)) {
          Object.defineProperty(cloned, field, {
            configurable: true,
            enumerable: false,
            value: undefined,
            writable: true,
          });
        }
      }
      return cloned;
    },
    catch: (cause) => invalidRow(context.topic, String(cause)),
  });
});

const topicRowKey = Effect.fn("ColumnLiveViewEngine.topicRow.key")(function* <Error>(
  context: TopicRowPreparationContext,
  row: RowObject,
  invalidRow: InvalidRowErrorFactory<Error>,
) {
  const key = fieldValue(row, context.keyField);
  if (typeof key !== "string") {
    return yield* Effect.fail(
      invalidRow(context.topic, `Key field ${context.keyField} must decode to a string.`),
    );
  }
  return key;
});

const validateTopicPatchKeys = Effect.fn("ColumnLiveViewEngine.topicRow.patchKeys.validate")(
  function* <Error>(
    context: TopicRowPreparationContext,
    patch: unknown,
    invalidRow: InvalidRowErrorFactory<Error>,
  ) {
    if (!isPlainRecord(patch)) {
      return yield* Effect.fail(invalidRow(context.topic, "Patch must be a plain object."));
    }
    for (const key of Reflect.ownKeys(patch)) {
      if (typeof key !== "string" || !context.fieldNames.has(key)) {
        return yield* Effect.fail(
          invalidRow(context.topic, `Patch contains unknown field: ${String(key)}.`),
        );
      }
    }
  },
);

export const prepareTopicRow = Effect.fn("ColumnLiveViewEngine.topicRow.prepare")(function* <
  Error,
  Row extends RowObject,
>(context: TopicRowPreparationContext, row: Row, invalidRow: InvalidRowErrorFactory<Error>) {
  const decoded = yield* decodeTopicRow(context, row, invalidRow);
  const key = yield* topicRowKey(context, decoded, invalidRow);
  return {
    key,
    row: decoded,
    source: "row",
  } satisfies PreparedTopicRow;
});

export const prepareTopicRowWithStorageKey = Effect.fn(
  "ColumnLiveViewEngine.topicRow.prepareWithStorageKey",
)(function* <Error, Row extends RowObject>(
  context: TopicRowPreparationContext,
  row: Row,
  storageKey: string,
  invalidRow: InvalidRowErrorFactory<Error>,
) {
  const decoded = yield* decodeTopicRow(context, row, invalidRow);
  yield* topicRowKey(context, decoded, invalidRow);
  return {
    key: storageKey,
    row: decoded,
    source: "row",
  } satisfies PreparedTopicRow;
});

export const prepareTopicPatch = Effect.fn("ColumnLiveViewEngine.topicRow.patch.prepare")(
  function* <Patch extends Partial<RowObject>, Error>(
    context: TopicRowPreparationContext,
    key: string,
    current: RowObject | undefined,
    patch: Patch,
    invalidRow: InvalidRowErrorFactory<Error>,
  ) {
    yield* validateTopicPatchKeys(context, patch, invalidRow);
    if (current === undefined) {
      return yield* Effect.fail(invalidRow(context.topic, `Cannot patch missing key: ${key}`));
    }
    const decoded = yield* decodeTopicRow(context, { ...current, ...patch }, invalidRow);
    const decodedKey = yield* topicRowKey(context, decoded, invalidRow);
    if (decodedKey !== key) {
      return yield* Effect.fail(invalidRow(context.topic, "Patch must not change the row key."));
    }
    const decodedFieldNames = new Set([...Object.keys(current), ...Object.keys(decoded)]);
    const topicRowChangedFields = topicRowChangedFieldsFromRows(
      current,
      decoded,
      decodedFieldNames,
    );
    if (topicRowChangedFields === undefined) {
      return {
        key,
        row: decoded,
        source: "patch",
      } satisfies PreparedTopicRow;
    }
    return {
      changedFields: topicRowChangedFields,
      key,
      row: decoded,
      source: "patch",
    } satisfies PreparedTopicRow;
  },
);
