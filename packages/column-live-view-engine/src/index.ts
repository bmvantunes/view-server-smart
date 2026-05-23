import type {
  DeltaEvent,
  DeltaOperation,
  FieldKey,
  LiveQueryRow,
  LiveQueryResult,
  OrderBy,
  RawQuery,
  RowFromSchema,
  RowSchema,
  SnapshotEvent,
  StatusEvent,
  StringFieldKey,
  TopicRow,
} from "@view-server/config";
import { Cause, Effect, Queue, Schema, Semaphore, Stream } from "effect";
import { equals, isBigDecimal, Order } from "effect/BigDecimal";

export type DecodableTopicDefinitions = Record<
  string,
  {
    readonly schema: RowSchema & Schema.Decoder<object>;
    readonly key: string;
  }
>;

type ValidateEngineTopics<Topics extends DecodableTopicDefinitions> = {
  readonly [Topic in keyof Topics]: Topics[Topic] extends {
    readonly schema: infer S extends RowSchema & Schema.Decoder<object>;
    readonly key: infer Key extends string;
  }
    ? {
        readonly schema: S;
        readonly key: Key & StringFieldKey<RowFromSchema<S>>;
      }
    : never;
};

export type ColumnLiveViewEngineConfig<Topics extends DecodableTopicDefinitions> = {
  readonly topics: Topics & ValidateEngineTopics<Topics>;
  readonly subscriptionQueueCapacity?: number;
};

export class InvalidTopicError extends Schema.TaggedErrorClass<InvalidTopicError>()(
  "InvalidTopicError",
  {
    topic: Schema.String,
    message: Schema.String,
  },
) {}

export class InvalidRowError extends Schema.TaggedErrorClass<InvalidRowError>()("InvalidRowError", {
  topic: Schema.String,
  message: Schema.String,
}) {}

export class UnsupportedQueryError extends Schema.TaggedErrorClass<UnsupportedQueryError>()(
  "UnsupportedQueryError",
  {
    topic: Schema.String,
    message: Schema.String,
  },
) {}

export class InvalidQueryError extends Schema.TaggedErrorClass<InvalidQueryError>()(
  "InvalidQueryError",
  {
    topic: Schema.String,
    message: Schema.String,
  },
) {}

export class EngineClosedError extends Schema.TaggedErrorClass<EngineClosedError>()(
  "EngineClosedError",
  {
    message: Schema.String,
  },
) {}

export type ColumnLiveViewEngineError =
  | InvalidTopicError
  | InvalidRowError
  | UnsupportedQueryError
  | InvalidQueryError
  | EngineClosedError;

export type ColumnLiveViewEngineEvent<Row> = SnapshotEvent<Row> | DeltaEvent<Row> | StatusEvent;

export type ColumnLiveViewSubscription<Row> = {
  readonly events: Stream.Stream<ColumnLiveViewEngineEvent<Row>>;
  readonly close: () => Effect.Effect<void, never>;
};

export type ColumnLiveViewTopicHealth = {
  readonly status: "ready" | "degraded";
  readonly rowCount: number;
  readonly version: number;
  readonly activeSubscriptions: number;
  readonly queuedEvents: number;
  readonly maxQueueDepth: number;
  readonly backpressureEvents: number;
};

export type ColumnLiveViewEngineHealth<
  Topics extends DecodableTopicDefinitions = DecodableTopicDefinitions,
> = {
  readonly status: "ready" | "stopping";
  readonly version: number;
  readonly topics: {
    readonly [Topic in Extract<keyof Topics, string>]: ColumnLiveViewTopicHealth;
  };
  readonly activeSubscriptions: number;
  readonly queuedEvents: number;
  readonly maxQueueDepth: number;
  readonly backpressureEvents: number;
};

type MutableHealthTopics<Topics extends DecodableTopicDefinitions> = {
  -readonly [Topic in Extract<keyof Topics, string>]: ColumnLiveViewTopicHealth;
};

type RejectExtraKeys<Candidate, Shape> = {
  readonly [Key in Exclude<keyof Candidate, keyof Shape>]: never;
};

type ExactRawQuery<Row, Query> = Query &
  RejectExtraKeys<Query, RawQuery<Row>> & {
    readonly groupBy?: never;
    readonly aggregates?: never;
  } & ExactWhere<Row, Query> &
  ExactOrderBy<Row, Query>;

type ExactWhere<Row, Query> = Query extends {
  readonly where: infer Where;
}
  ? {
      readonly where: Where &
        RejectExtraKeys<Where, { readonly [Field in FieldKey<Row>]?: unknown }> & {
          readonly [Field in Extract<keyof Where, FieldKey<Row>>]: ExactFilter<
            Row[Field],
            Where[Field]
          >;
        };
    }
  : unknown;

type ExactFilter<Value, Filter> = Value extends object
  ? unknown
  : ExactOperatorFilter<Value, Filter>;

type ExactOperatorFilter<Value, Filter> = Filter extends object
  ? Filter extends ReadonlyArray<unknown>
    ? unknown
    : Filter & RejectExtraKeys<Filter, FieldFilterShape<Value>>
  : unknown;

type FieldFilterShape<Value> = Value extends string
  ? {
      readonly eq?: Value;
      readonly neq?: Value;
      readonly in?: ReadonlyArray<Value>;
      readonly startsWith?: string;
    }
  : {
      readonly eq?: Value;
      readonly neq?: Value;
      readonly in?: ReadonlyArray<Value>;
      readonly gt?: Value;
      readonly gte?: Value;
      readonly lt?: Value;
      readonly lte?: Value;
    };

type ExactOrderBy<Row, Query> = Query extends {
  readonly orderBy: ReadonlyArray<infer Entry>;
}
  ? {
      readonly orderBy: ReadonlyArray<Entry & RejectExtraKeys<Entry, OrderBy<Row>>>;
    }
  : unknown;

type ExactPatch<Row, Patch> = Patch & RejectExtraKeys<Patch, Partial<Row>>;

export type ColumnLiveViewEngine<Topics extends DecodableTopicDefinitions> = {
  readonly publish: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    row: TopicRow<Topics, Topic>,
  ) => Effect.Effect<void, ColumnLiveViewEngineError>;
  readonly publishMany: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    rows: ReadonlyArray<TopicRow<Topics, Topic>>,
  ) => Effect.Effect<void, ColumnLiveViewEngineError>;
  readonly patch: <
    Topic extends Extract<keyof Topics, string>,
    const Patch extends Partial<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    key: string,
    patch: ExactPatch<TopicRow<Topics, Topic>, Patch>,
  ) => Effect.Effect<void, ColumnLiveViewEngineError>;
  readonly delete: <Topic extends Extract<keyof Topics, string>>(
    topic: Topic,
    key: string,
  ) => Effect.Effect<void, ColumnLiveViewEngineError>;
  readonly snapshot: <
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactRawQuery<TopicRow<Topics, Topic>, Query>,
  ) => Effect.Effect<
    LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  readonly subscribe: <
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactRawQuery<TopicRow<Topics, Topic>, Query>,
  ) => Effect.Effect<
    ColumnLiveViewSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ColumnLiveViewEngineError
  >;
  readonly health: () => Effect.Effect<ColumnLiveViewEngineHealth<Topics>, never>;
  readonly reset: () => Effect.Effect<void, never>;
  readonly close: () => Effect.Effect<void, never>;
};

type RowObject = object;
const defaultSubscriptionQueueCapacity = 1_024;

type RuntimeRawQuery = {
  readonly where?: object;
  readonly orderBy?: ReadonlyArray<{
    readonly field: string;
    readonly direction: "asc" | "desc";
  }>;
  readonly offset?: number;
  readonly limit?: number;
  readonly fields?: ReadonlyArray<string>;
};

type SchemaWithFields = Schema.Decoder<object> & {
  readonly fields: Record<string, unknown>;
};

type StoredRow = {
  readonly key: string;
  readonly row: RowObject;
};

type QueryEvaluation = {
  readonly rows: ReadonlyArray<RowObject>;
  readonly keys: ReadonlyArray<string>;
  readonly window: ReadonlyArray<StoredRow>;
  readonly totalRows: number;
  readonly version: number;
};

type Subscriber = {
  readonly topic: string;
  readonly queryId: string;
  readonly query: RuntimeRawQuery;
  readonly queue: Queue.Queue<ColumnLiveViewEngineEvent<RowObject>, Cause.Done>;
  lastEvaluation: QueryEvaluation;
  maxQueueDepth: number;
  backpressureEvents: number;
  closed: boolean;
};

type FilterObject = {
  readonly eq?: unknown;
  readonly neq?: unknown;
  readonly in?: ReadonlyArray<unknown>;
  readonly gt?: unknown;
  readonly gte?: unknown;
  readonly lt?: unknown;
  readonly lte?: unknown;
  readonly startsWith?: string;
};

const rawQueryKeys = new Set(["where", "orderBy", "offset", "limit", "fields"]);
const filterOperatorKeys = new Set(["eq", "neq", "in", "gt", "gte", "lt", "lte", "startsWith"]);

const isDenseArray = (value: ReadonlyArray<unknown>): boolean => {
  for (let index = 0; index < value.length; index += 1) {
    if (!(index in value)) {
      return false;
    }
  }
  return true;
};

class TopicStore {
  readonly rows = new Map<string, RowObject>();
  readonly subscribers = new Set<Subscriber>();
  readonly mutationSemaphore = Semaphore.makeUnsafe(1);
  version = 0;
  maxQueueDepth = 0;
  backpressureEvents = 0;

  constructor(
    readonly topic: string,
    readonly schema: Schema.Decoder<RowObject>,
    readonly keyField: string,
    readonly fieldNames: ReadonlySet<string>,
    readonly structuredFieldNames: ReadonlySet<string>,
  ) {}
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isPlainRecord = (value: unknown): value is Record<string, unknown> => {
  if (!isRecord(value) || isBigDecimal(value)) {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype;
};

const isGroupedQuery = (query: unknown): boolean =>
  isRecord(query) && ("groupBy" in query || "aggregates" in query);

const isValidWindowNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0;

const isSchemaWithFields = (schema: Schema.Decoder<object>): schema is SchemaWithFields =>
  "fields" in schema && isRecord(schema.fields);

const schemaFieldNames = (schema: Schema.Decoder<object>): ReadonlySet<string> =>
  isSchemaWithFields(schema) ? new Set(Object.keys(schema.fields)) : new Set();

const schemaStructuredFieldNames = (schema: Schema.Decoder<object>): ReadonlySet<string> => {
  if (!isSchemaWithFields(schema)) {
    return new Set();
  }

  const fields = new Set<string>();
  for (const [field, fieldSchema] of Object.entries(schema.fields)) {
    const tag = Object(Object(fieldSchema)["ast"])["_tag"];
    if (tag === "Objects" || tag === "Arrays" || tag === "ObjectKeyword") {
      fields.add(field);
    }
  }
  return fields;
};

const cloneUnknown = (value: unknown): unknown => {
  if (isBigDecimal(value)) {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map(cloneUnknown);
  }
  if (isPlainRecord(value)) {
    return cloneRecord(value);
  }
  if (typeof value !== "object" || value === null) {
    return value;
  }
  return structuredClone(value);
};

const cloneRecord = (value: Record<string, unknown>): Record<string, unknown> => {
  const cloned: Record<string, unknown> = {};
  for (const [key, entry] of Object.entries(value)) {
    cloned[key] = cloneUnknown(entry);
  }
  return cloned;
};

const cloneRow = (row: RowObject): RowObject => {
  const cloned: Record<string, unknown> = {};
  for (const [key, entry] of Object.entries(row)) {
    cloned[key] = cloneUnknown(entry);
  }
  return cloned;
};

const safeCloneRow = Effect.fn("ColumnLiveViewEngine.safeCloneRow")(function* (
  store: TopicStore,
  row: RowObject,
) {
  return yield* Effect.try({
    try: () => cloneRow(row),
    catch: (cause) =>
      InvalidRowError.make({
        topic: store.topic,
        message: String(cause),
      }),
  });
});

const decodeRawQuery = (
  topic: string,
  fieldNames: ReadonlySet<string>,
  query: unknown,
): Effect.Effect<RuntimeRawQuery, InvalidQueryError> => {
  if (query === undefined) {
    return Effect.succeed({});
  }
  if (!isPlainRecord(query)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query must be a plain object.",
    });
  }
  for (const key of Object.keys(query)) {
    if (!rawQueryKeys.has(key)) {
      return InvalidQueryError.make({
        topic,
        message: `Raw query contains unsupported key: ${key}.`,
      });
    }
  }

  const where = query["where"];
  if (where !== undefined && !isPlainRecord(where)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query where must be a plain object.",
    });
  }
  if (where !== undefined) {
    for (const field of Object.keys(where)) {
      if (!fieldNames.has(field)) {
        return InvalidQueryError.make({
          topic,
          message: `Raw query where contains unknown field: ${field}.`,
        });
      }
    }
  }

  const orderBy = query["orderBy"];
  if (orderBy !== undefined && !Array.isArray(orderBy)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query orderBy must be an array.",
    });
  }

  const fields = query["fields"];
  if (
    fields !== undefined &&
    (!Array.isArray(fields) || !fields.every((field) => typeof field === "string"))
  ) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query fields must be an array of strings.",
    });
  }
  if (Array.isArray(fields)) {
    for (const field of fields) {
      if (!fieldNames.has(field)) {
        return InvalidQueryError.make({
          topic,
          message: `Raw query fields contains unknown field: ${field}.`,
        });
      }
    }
  }

  const offset = query["offset"];
  if (offset !== undefined && !isValidWindowNumber(offset)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query offset must be a non-negative safe integer.",
    });
  }

  const limit = query["limit"];
  if (limit !== undefined && !isValidWindowNumber(limit)) {
    return InvalidQueryError.make({
      topic,
      message: "Raw query limit must be a non-negative safe integer.",
    });
  }

  const decoded: {
    where?: object;
    orderBy?: Array<{ readonly field: string; readonly direction: "asc" | "desc" }>;
    offset?: number;
    limit?: number;
    fields?: Array<string>;
  } = {};

  if (where !== undefined) {
    let clonedWhere: Record<string, unknown>;
    try {
      clonedWhere = cloneRecord(where);
    } catch (cause) {
      return InvalidQueryError.make({
        topic,
        message: `Raw query where could not be cloned: ${String(cause)}`,
      });
    }
    decoded.where = clonedWhere;
  }
  if (offset !== undefined) {
    decoded.offset = offset;
  }
  if (limit !== undefined) {
    decoded.limit = limit;
  }
  if (Array.isArray(fields)) {
    decoded.fields = [...fields];
  }

  const clonedOrderBy: Array<{ readonly field: string; readonly direction: "asc" | "desc" }> = [];
  if (Array.isArray(orderBy)) {
    for (const entry of orderBy) {
      if (!isPlainRecord(entry)) {
        return InvalidQueryError.make({
          topic,
          message: "Raw query orderBy entries must be plain objects.",
        });
      }
      for (const key of Object.keys(entry)) {
        if (key !== "field" && key !== "direction") {
          return InvalidQueryError.make({
            topic,
            message: `Raw query orderBy contains unsupported key: ${key}.`,
          });
        }
      }
      const field = entry["field"];
      if (typeof field !== "string") {
        return InvalidQueryError.make({
          topic,
          message: "Raw query orderBy field must be a string.",
        });
      }
      if (!fieldNames.has(field)) {
        return InvalidQueryError.make({
          topic,
          message: `Raw query orderBy contains unknown field: ${field}.`,
        });
      }
      const direction = entry["direction"];
      if (direction !== "asc" && direction !== "desc") {
        return InvalidQueryError.make({
          topic,
          message: "Raw query orderBy direction must be asc or desc.",
        });
      }
      clonedOrderBy.push({
        field,
        direction,
      });
    }
  }
  if (clonedOrderBy.length > 0) {
    decoded.orderBy = clonedOrderBy;
  }

  return Effect.succeed(decoded);
};

const validateRuntimeQueryAgainstStore = (
  store: TopicStore,
  query: RuntimeRawQuery,
): Effect.Effect<void, InvalidQueryError> =>
  Effect.gen(function* () {
    if (query.where === undefined) {
      return;
    }

    for (const [field, filter] of Object.entries(query.where)) {
      if (!isPlainRecord(filter) || isBigDecimal(filter)) {
        continue;
      }
      const keys = Object.keys(filter);
      const operatorKeyCount = keys.filter((key) => filterOperatorKeys.has(key)).length;
      if (operatorKeyCount > 0 && operatorKeyCount !== keys.length) {
        return yield* InvalidQueryError.make({
          topic: store.topic,
          message: `Raw query where field ${field} contains unsupported filter operator.`,
        });
      }
      if (operatorKeyCount === 0 && !store.structuredFieldNames.has(field)) {
        return yield* InvalidQueryError.make({
          topic: store.topic,
          message: `Raw query where field ${field} contains unsupported filter operator.`,
        });
      }
    }
  });

const fieldValue = (row: RowObject, field: string): unknown => {
  for (const [key, value] of Object.entries(row)) {
    if (key === field) {
      return value;
    }
  }
  return undefined;
};

const equalsValue = (left: unknown, right: unknown): boolean => {
  if (isBigDecimal(left) && isBigDecimal(right)) {
    return equals(left, right);
  }
  if (Array.isArray(left) && Array.isArray(right)) {
    return (
      left.length === right.length && left.every((entry, index) => equalsValue(entry, right[index]))
    );
  }
  if (isPlainRecord(left) && isPlainRecord(right)) {
    const leftEntries = Object.entries(left);
    const rightKeys = new Set(Object.keys(right));
    return (
      leftEntries.length === rightKeys.size &&
      leftEntries.every(([key, entry]) => rightKeys.has(key) && equalsValue(entry, right[key]))
    );
  }
  return Object.is(left, right);
};

const stableValueString = (value: unknown): string => {
  if (Array.isArray(value)) {
    return `[${value.map(stableValueString).join(",")}]`;
  }
  if (isPlainRecord(value)) {
    return `{${Object.keys(value)
      .toSorted()
      .map((key) => `${key}:${stableValueString(value[key])}`)
      .join(",")}}`;
  }
  return `${typeof value}:${
    JSON.stringify(value, (_key, entry: unknown) =>
      typeof entry === "bigint" ? entry.toString() : entry,
    ) ?? ""
  }`;
};

const valueRank = (value: unknown): number => {
  if (value == null) {
    return 0;
  }
  if (typeof value === "boolean") {
    return 1;
  }
  if (typeof value === "number" || typeof value === "bigint" || isBigDecimal(value)) {
    return 2;
  }
  if (typeof value === "string") {
    return 3;
  }
  if (Array.isArray(value)) {
    return 4;
  }
  return 5;
};

const compareFilterValue = (left: unknown, right: unknown): number | undefined => {
  if (typeof left === "number" && typeof right === "number") {
    if (!Number.isFinite(left) || !Number.isFinite(right)) {
      return undefined;
    }
    return left === right ? 0 : left < right ? -1 : 1;
  }
  if (typeof left === "string" && typeof right === "string") {
    return left === right ? 0 : left < right ? -1 : 1;
  }
  if (typeof left === "bigint" && typeof right === "bigint") {
    return left === right ? 0 : left < right ? -1 : 1;
  }
  if (isBigDecimal(left) && isBigDecimal(right)) {
    return Order(left, right);
  }
  return undefined;
};

const compareByStableString = (left: unknown, right: unknown): number => {
  const leftString = stableValueString(left);
  const rightString = stableValueString(right);
  return Number(leftString > rightString) - Number(leftString < rightString);
};

const compareValue = (left: unknown, right: unknown): number | undefined => {
  const leftRank = valueRank(left);
  const rightRank = valueRank(right);
  if (leftRank !== rightRank) {
    return Number(leftRank > rightRank) - Number(leftRank < rightRank);
  }
  if (typeof left === "boolean" && typeof right === "boolean") {
    return left === right ? 0 : left ? 1 : -1;
  }
  const filterComparison = compareFilterValue(left, right);
  if (filterComparison !== undefined) {
    return filterComparison;
  }
  return compareByStableString(left, right);
};

const isEqualityComparable = (left: unknown, right: unknown): boolean => {
  if (isBigDecimal(left) || isBigDecimal(right)) {
    return isBigDecimal(left) && isBigDecimal(right);
  }
  if (typeof left === "number" || typeof right === "number") {
    return typeof left === "number" && typeof right === "number" && Number.isFinite(right);
  }
  return typeof left === typeof right;
};

const isOperatorFilterObject = (filter: Record<string, unknown>): filter is FilterObject => {
  const keys = Object.keys(filter);
  return keys.length > 0 && keys.every((key) => filterOperatorKeys.has(key));
};

const includesValue = (values: ReadonlyArray<unknown>, value: unknown): boolean => {
  for (const candidate of values) {
    if (equalsValue(value, candidate)) {
      return true;
    }
  }
  return false;
};

const matchesFilter = (value: unknown, filter: unknown): boolean => {
  if (filter === undefined) {
    return false;
  }
  if (!isPlainRecord(filter) || isBigDecimal(filter)) {
    return equalsValue(value, filter);
  }
  const valueIsStructured = (isPlainRecord(value) && !isBigDecimal(value)) || Array.isArray(value);
  if (valueIsStructured) {
    if (equalsValue(value, filter)) {
      return true;
    }
    const filterKeys = Object.keys(filter);
    const eq = filter["eq"];
    if (filterKeys.length === 1 && eq !== undefined) {
      return equalsValue(value, eq);
    }
    const oneOf = filter["in"];
    if (filterKeys.length === 1 && Array.isArray(oneOf)) {
      return (
        isDenseArray(oneOf) &&
        !oneOf.some((candidate) => candidate === undefined) &&
        includesValue(oneOf, value)
      );
    }
    const notEqual = filter["neq"];
    if (filterKeys.length === 1 && notEqual !== undefined) {
      return !equalsValue(value, notEqual);
    }
    return false;
  }
  /* v8 ignore next -- runtime validation rejects scalar object filters before evaluation. */
  if (!isOperatorFilterObject(filter)) {
    return equalsValue(value, filter);
  }

  const filterKeys = Object.keys(filter);
  if (filterKeys.some((key) => filter[key as keyof FilterObject] === undefined)) {
    return false;
  }

  if (filter.eq !== undefined && !equalsValue(value, filter.eq)) {
    return false;
  }
  if (filter.neq !== undefined) {
    if (!isEqualityComparable(value, filter.neq) || equalsValue(value, filter.neq)) {
      return false;
    }
  }
  if (filter.in !== undefined) {
    if (
      !Array.isArray(filter.in) ||
      !isDenseArray(filter.in) ||
      filter.in.some((candidate) => candidate === undefined) ||
      !includesValue(filter.in, value)
    ) {
      return false;
    }
  }
  if (filter.startsWith !== undefined) {
    if (
      typeof filter.startsWith !== "string" ||
      typeof value !== "string" ||
      !value.startsWith(filter.startsWith)
    ) {
      return false;
    }
  }

  if (filter.gt !== undefined) {
    const comparison = compareFilterValue(value, filter.gt);
    if (comparison === undefined || comparison <= 0) {
      return false;
    }
  }
  if (filter.gte !== undefined) {
    const comparison = compareFilterValue(value, filter.gte);
    if (comparison === undefined || comparison < 0) {
      return false;
    }
  }
  if (filter.lt !== undefined) {
    const comparison = compareFilterValue(value, filter.lt);
    if (comparison === undefined || comparison >= 0) {
      return false;
    }
  }
  if (filter.lte !== undefined) {
    const comparison = compareFilterValue(value, filter.lte);
    if (comparison === undefined || comparison > 0) {
      return false;
    }
  }

  return true;
};

const matchesWhere = (row: RowObject, query: RuntimeRawQuery): boolean => {
  if (query.where === undefined) {
    return true;
  }

  for (const [field, filter] of Object.entries(query.where)) {
    if (!matchesFilter(fieldValue(row, field), filter)) {
      return false;
    }
  }

  return true;
};

const compareRows = (
  left: StoredRow,
  right: StoredRow,
  orderBy: ReadonlyArray<OrderBy<Record<string, unknown>>>,
): number => {
  for (const order of orderBy) {
    const comparison = compareValue(
      fieldValue(left.row, order.field),
      fieldValue(right.row, order.field),
    );
    if (comparison !== undefined && comparison !== 0) {
      return order.direction === "asc" ? comparison : -comparison;
    }
  }
  return Number(left.key > right.key) - Number(left.key < right.key);
};

const projectRow = (
  row: RowObject,
  fields: ReadonlyArray<FieldKey<Record<string, unknown>>> | undefined,
): RowObject => {
  if (fields === undefined) {
    return cloneRow(row);
  }

  const projected: Record<string, unknown> = {};
  for (const field of fields) {
    projected[field] = cloneUnknown(fieldValue(row, field));
  }
  return projected;
};

const rowEquals = (left: RowObject, right: RowObject): boolean => {
  const leftEntries = Object.entries(left);
  const rightEntries = Object.entries(right);
  if (leftEntries.length !== rightEntries.length) {
    return false;
  }
  for (const [key, value] of leftEntries) {
    if (!equalsValue(value, fieldValue(right, key))) {
      return false;
    }
  }
  return true;
};

const evaluateRawQuery = (store: TopicStore, query: RuntimeRawQuery): QueryEvaluation => {
  const filtered = Array.from(store.rows, ([key, row]) => ({ key, row })).filter((entry) =>
    matchesWhere(entry.row, query),
  );
  const ordered = filtered.toSorted((left, right) => compareRows(left, right, query.orderBy ?? []));
  const offset = query.offset ?? 0;
  const windowed = ordered.slice(
    offset,
    query.limit === undefined ? undefined : offset + query.limit,
  );
  const window = windowed.map((entry) => ({
    key: entry.key,
    row: projectRow(entry.row, query.fields),
  }));

  return {
    rows: window.map((entry) => entry.row),
    keys: window.map((entry) => entry.key),
    window,
    totalRows: filtered.length,
    version: store.version,
  };
};

const liveQueryResult = <Row>(evaluation: QueryEvaluation): LiveQueryResult<Row> =>
  ({
    rows: evaluation.rows,
    totalRows: evaluation.totalRows,
    version: evaluation.version,
  }) as LiveQueryResult<Row>;

const decodeRow = Effect.fn("ColumnLiveViewEngine.decodeRow")(function* (
  store: TopicStore,
  row: RowObject,
) {
  return yield* Effect.try({
    try: () => Schema.decodeUnknownSync(store.schema)(row),
    catch: (cause) =>
      InvalidRowError.make({
        topic: store.topic,
        message: String(cause),
      }),
  });
});

const rowKey = Effect.fn("ColumnLiveViewEngine.rowKey")(function* (
  store: TopicStore,
  row: RowObject,
) {
  const key = fieldValue(row, store.keyField);
  if (typeof key !== "string") {
    return yield* InvalidRowError.make({
      topic: store.topic,
      message: `Key field ${store.keyField} must decode to a string.`,
    });
  }
  return key;
});

const snapshotEvent = (
  store: TopicStore,
  queryId: string,
  evaluation: QueryEvaluation,
): SnapshotEvent<RowObject> => ({
  type: "snapshot",
  topic: store.topic,
  queryId,
  version: evaluation.version,
  keys: [...evaluation.keys],
  rows: evaluation.rows.map(cloneRow),
  totalRows: evaluation.totalRows,
});

const deltaOperations = (
  previous: QueryEvaluation,
  next: QueryEvaluation,
): ReadonlyArray<DeltaOperation<RowObject>> => {
  const operations: Array<DeltaOperation<RowObject>> = [];
  const nextKeys = new Set(next.keys);
  const currentKeys = [...previous.keys];
  const currentRows = [...previous.rows];

  for (const key of previous.keys) {
    if (!nextKeys.has(key)) {
      const index = currentKeys.indexOf(key);
      currentKeys.splice(index, 1);
      currentRows.splice(index, 1);
      operations.push({
        type: "remove",
        key,
      });
    }
  }

  for (const [index, { key, row }] of next.window.entries()) {
    const currentIndex = currentKeys.indexOf(key);
    if (currentIndex < 0) {
      currentKeys.splice(index, 0, key);
      currentRows.splice(index, 0, row);
      operations.push({
        type: "insert",
        key,
        row,
        index,
      });
      continue;
    }

    if (currentIndex !== index) {
      const currentRow = currentRows[currentIndex]!;
      currentKeys.splice(currentIndex, 1);
      currentRows.splice(currentIndex, 1);
      currentKeys.splice(index, 0, key);
      currentRows.splice(index, 0, currentRow);
      operations.push({
        type: "move",
        key,
        fromIndex: currentIndex,
        toIndex: index,
      });
    }

    const currentRow = currentRows[index];
    if (currentRow === undefined || !rowEquals(currentRow, row)) {
      currentRows[index] = row;
      operations.push({
        type: "update",
        key,
        row,
        index,
      });
    }
  }

  return operations;
};

const cloneDeltaOperations = (
  operations: ReadonlyArray<DeltaOperation<RowObject>>,
): ReadonlyArray<DeltaOperation<RowObject>> =>
  operations.map((operation) => {
    if (operation.type === "insert" || operation.type === "update") {
      return {
        ...operation,
        row: cloneRow(operation.row),
      };
    }
    return operation;
  });

const deltaEvent = (
  store: TopicStore,
  subscriber: Subscriber,
  next: QueryEvaluation,
  operations: ReadonlyArray<DeltaOperation<RowObject>>,
): DeltaEvent<RowObject> => ({
  type: "delta",
  topic: store.topic,
  queryId: subscriber.queryId,
  fromVersion: subscriber.lastEvaluation.version,
  toVersion: next.version,
  operations: cloneDeltaOperations(operations),
  totalRows: next.totalRows,
});

const backpressureStatusEvent = (store: TopicStore, subscriber: Subscriber): StatusEvent => ({
  type: "status",
  topic: store.topic,
  queryId: subscriber.queryId,
  status: "closed",
  code: "BackpressureExceeded",
  message: "Subscription closed because its event queue exceeded capacity.",
});

class InMemoryColumnLiveViewEngine<
  Topics extends DecodableTopicDefinitions,
> implements ColumnLiveViewEngine<Topics> {
  private readonly stores = new Map<string, TopicStore>();
  private readonly subscriptionQueueCapacity: number;
  private engineVersion = 0;
  private nextQueryId = 0;
  private closed = false;

  constructor(config: ColumnLiveViewEngineConfig<Topics>) {
    const configuredCapacity = config.subscriptionQueueCapacity ?? defaultSubscriptionQueueCapacity;
    this.subscriptionQueueCapacity =
      Number.isSafeInteger(configuredCapacity) && configuredCapacity > 0
        ? configuredCapacity
        : defaultSubscriptionQueueCapacity;
    for (const [topic, definition] of Object.entries(config.topics)) {
      this.stores.set(
        topic,
        new TopicStore(
          topic,
          definition.schema,
          definition.key,
          schemaFieldNames(definition.schema),
          schemaStructuredFieldNames(definition.schema),
        ),
      );
    }
  }

  private getStore(topic: string): Effect.Effect<TopicStore, InvalidTopicError> {
    return Effect.gen({ self: this }, function* () {
      const store = this.stores.get(topic);
      if (store === undefined) {
        return yield* InvalidTopicError.make({
          topic,
          message: `Unknown topic: ${topic}`,
        });
      }
      return store;
    });
  }

  private ensureOpen(): Effect.Effect<void, EngineClosedError> {
    return Effect.gen({ self: this }, function* () {
      if (this.closed) {
        return yield* EngineClosedError.make({
          message: "ColumnLiveViewEngine is closed.",
        });
      }
    });
  }

  private notifySubscribers = Effect.fn("ColumnLiveViewEngine.notifySubscribers")(function* (
    store: TopicStore,
  ) {
    for (const subscriber of store.subscribers) {
      const next = evaluateRawQuery(store, subscriber.query);
      const operations = deltaOperations(subscriber.lastEvaluation, next);
      if (operations.length === 0 && subscriber.lastEvaluation.totalRows === next.totalRows) {
        continue;
      }

      const offered = yield* Queue.offer(
        subscriber.queue,
        deltaEvent(store, subscriber, next, operations),
      );
      if (!offered) {
        subscriber.backpressureEvents += 1;
        store.backpressureEvents += 1;
        subscriber.closed = true;
        store.subscribers.delete(subscriber);
        yield* Queue.takeAll(subscriber.queue).pipe(Effect.ignore);
        yield* Queue.offer(subscriber.queue, backpressureStatusEvent(store, subscriber)).pipe(
          Effect.ignore,
        );
        yield* Queue.end(subscriber.queue);
        continue;
      }

      const queueDepth = yield* Queue.size(subscriber.queue);
      subscriber.maxQueueDepth = Math.max(subscriber.maxQueueDepth, queueDepth);
      store.maxQueueDepth = Math.max(store.maxQueueDepth, subscriber.maxQueueDepth);
      subscriber.lastEvaluation = next;
    }
  });

  private commit(store: TopicStore): Effect.Effect<void> {
    return Effect.gen({ self: this }, function* () {
      store.version += 1;
      this.engineVersion += 1;
      yield* this.notifySubscribers(store);
    });
  }

  readonly publish: ColumnLiveViewEngine<Topics>["publish"] = (topic, row) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      const decoded = yield* decodeRow(store, row);
      const key = yield* rowKey(store, decoded);
      const cloned = yield* safeCloneRow(store, decoded);
      yield* store.mutationSemaphore.withPermits(1)(
        Effect.gen({ self: this }, function* () {
          store.rows.set(key, cloned);
          yield* this.commit(store);
        }),
      );
    });
  };

  readonly publishMany: ColumnLiveViewEngine<Topics>["publishMany"] = (topic, rows) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      const decodedRows = yield* Effect.forEach(rows, (row) => decodeRow(store, row));
      const keyedRows = yield* Effect.forEach(decodedRows, (row) =>
        Effect.gen(function* () {
          const key = yield* rowKey(store, row);
          const cloned = yield* safeCloneRow(store, row);
          return { key, row: cloned };
        }),
      );
      yield* store.mutationSemaphore.withPermits(1)(
        Effect.gen({ self: this }, function* () {
          for (const { key, row } of keyedRows) {
            store.rows.set(key, row);
          }
          yield* this.commit(store);
        }),
      );
    });
  };

  readonly patch: ColumnLiveViewEngine<Topics>["patch"] = (topic, key, patch) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      yield* store.mutationSemaphore.withPermits(1)(
        Effect.gen({ self: this }, function* () {
          const current = store.rows.get(key);
          if (current === undefined) {
            return yield* InvalidRowError.make({
              topic,
              message: `Cannot patch missing key: ${key}`,
            });
          }
          const decoded = yield* decodeRow(store, { ...current, ...patch });
          const decodedKey = yield* rowKey(store, decoded);
          if (decodedKey !== key) {
            return yield* InvalidRowError.make({
              topic,
              message: "Patch must not change the row key.",
            });
          }
          const cloned = yield* safeCloneRow(store, decoded);
          store.rows.set(key, cloned);
          yield* this.commit(store);
        }),
      );
    });
  };

  readonly delete: ColumnLiveViewEngine<Topics>["delete"] = (topic, key) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      const store = yield* this.getStore(topic);
      yield* store.mutationSemaphore.withPermits(1)(
        Effect.gen({ self: this }, function* () {
          store.rows.delete(key);
          yield* this.commit(store);
        }),
      );
    });
  };

  readonly snapshot: ColumnLiveViewEngine<Topics>["snapshot"] = (topic, query) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      if (isGroupedQuery(query)) {
        return yield* UnsupportedQueryError.make({
          topic,
          message: "Grouped aggregate queries are not implemented in this slice.",
        });
      }
      const store = yield* this.getStore(topic);
      const rawQuery = yield* decodeRawQuery(topic, store.fieldNames, query);
      yield* validateRuntimeQueryAgainstStore(store, rawQuery);
      return liveQueryResult<LiveQueryRow<TopicRow<Topics, typeof topic>, typeof query>>(
        evaluateRawQuery(store, rawQuery),
      );
    });
  };

  readonly subscribe: ColumnLiveViewEngine<Topics>["subscribe"] = (topic, query) => {
    return Effect.gen({ self: this }, function* () {
      yield* this.ensureOpen();
      if (isGroupedQuery(query)) {
        return yield* UnsupportedQueryError.make({
          topic,
          message: "Grouped aggregate queries are not implemented in this slice.",
        });
      }
      const store = yield* this.getStore(topic);
      const rawQuery = yield* decodeRawQuery(topic, store.fieldNames, query);
      yield* validateRuntimeQueryAgainstStore(store, rawQuery);
      const queryId = `query-${this.nextQueryId}`;
      this.nextQueryId += 1;
      const queue = yield* Queue.dropping<ColumnLiveViewEngineEvent<RowObject>, Cause.Done>(
        this.subscriptionQueueCapacity,
      );
      const evaluation = evaluateRawQuery(store, rawQuery);
      const subscriber: Subscriber = {
        topic,
        queryId,
        query: rawQuery,
        queue,
        lastEvaluation: evaluation,
        maxQueueDepth: 0,
        backpressureEvents: 0,
        closed: false,
      };

      store.subscribers.add(subscriber);
      yield* Queue.offer(queue, snapshotEvent(store, queryId, evaluation));
      subscriber.maxQueueDepth = yield* Queue.size(queue);
      store.maxQueueDepth = Math.max(store.maxQueueDepth, subscriber.maxQueueDepth);

      const close = Effect.fn("ColumnLiveViewEngine.subscription.close")(function* () {
        if (!subscriber.closed) {
          subscriber.closed = true;
          store.subscribers.delete(subscriber);
          yield* Queue.end(queue);
        }
      });

      return {
        events: Stream.fromQueue(queue).pipe(Stream.ensuring(close())) as Stream.Stream<
          ColumnLiveViewEngineEvent<LiveQueryRow<TopicRow<Topics, typeof topic>, typeof query>>
        >,
        close,
      };
    });
  };

  readonly health: ColumnLiveViewEngine<Topics>["health"] = () => {
    return Effect.gen({ self: this }, function* () {
      const topics = {} as MutableHealthTopics<Topics>;
      let activeSubscriptions = 0;
      let queuedEvents = 0;
      let maxQueueDepth = 0;
      let backpressureEvents = 0;
      for (const [topic, store] of this.stores) {
        activeSubscriptions += store.subscribers.size;
        let topicQueuedEvents = 0;
        let topicMaxQueueDepth = store.maxQueueDepth;
        let topicBackpressureEvents = store.backpressureEvents;
        for (const subscriber of store.subscribers) {
          const subscriberQueueDepth = yield* Queue.size(subscriber.queue);
          topicQueuedEvents += subscriberQueueDepth;
          topicMaxQueueDepth = Math.max(topicMaxQueueDepth, subscriber.maxQueueDepth);
          topicBackpressureEvents += subscriber.backpressureEvents;
        }
        queuedEvents += topicQueuedEvents;
        maxQueueDepth = Math.max(maxQueueDepth, topicMaxQueueDepth);
        backpressureEvents += topicBackpressureEvents;
        const typedTopic = topic as Extract<keyof Topics, string>;
        topics[typedTopic] = {
          status: this.closed ? "degraded" : "ready",
          rowCount: store.rows.size,
          version: store.version,
          activeSubscriptions: store.subscribers.size,
          queuedEvents: topicQueuedEvents,
          maxQueueDepth: topicMaxQueueDepth,
          backpressureEvents: topicBackpressureEvents,
        };
      }

      return {
        status: this.closed ? "stopping" : "ready",
        version: this.engineVersion,
        topics,
        activeSubscriptions,
        queuedEvents,
        maxQueueDepth,
        backpressureEvents,
      };
    });
  };

  readonly reset: ColumnLiveViewEngine<Topics>["reset"] = () => {
    return Effect.gen({ self: this }, function* () {
      for (const store of this.stores.values()) {
        for (const subscriber of store.subscribers) {
          subscriber.closed = true;
          yield* Queue.end(subscriber.queue);
        }
        store.subscribers.clear();
        store.rows.clear();
        store.version = 0;
        store.maxQueueDepth = 0;
        store.backpressureEvents = 0;
      }
      this.engineVersion = 0;
    });
  };

  readonly close: ColumnLiveViewEngine<Topics>["close"] = () => {
    return Effect.gen({ self: this }, function* () {
      if (!this.closed) {
        this.closed = true;
        for (const store of this.stores.values()) {
          for (const subscriber of store.subscribers) {
            subscriber.closed = true;
            yield* Queue.end(subscriber.queue);
          }
          store.subscribers.clear();
        }
      }
    });
  };
}

export const createColumnLiveViewEngine = <const Topics extends DecodableTopicDefinitions>(
  config: ColumnLiveViewEngineConfig<Topics>,
): Effect.Effect<ColumnLiveViewEngine<Topics>> =>
  Effect.sync(() => new InMemoryColumnLiveViewEngine(config));
