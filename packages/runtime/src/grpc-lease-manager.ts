import type {
  ExactLiveQueryInputForTopic,
  GrpcRuntimeClients,
  GroupedQuery,
  LiveQueryRow,
  RawQuery,
  RowSchema,
  StatusEvent,
  TopicRow,
  ViewServerConfig,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
  ViewServerTransportError,
} from "@view-server/config";
import { validateLiveQuerySourceRoute } from "@view-server/config";
import {
  ignoreLoggedTypedFailuresPreserveNonTypedFailures,
  runAllFinalizers,
} from "@view-server/effect-utils";
import type {
  ViewServerLiveEvent,
  ViewServerRuntimeLiveClient,
  ViewServerLiveSubscription,
} from "@view-server/client";
import {
  Cause,
  Clock,
  Effect,
  Exit,
  Option,
  Queue,
  Schema,
  Sink,
  Scope,
  Semaphore,
  Stream,
} from "effect";
import * as BigDecimal from "effect/BigDecimal";
import type { ViewServerGrpcHealthLedger } from "./grpc-health";
import {
  makeDefaultGrpcClient,
  ViewServerGrpcIngressError,
  type ViewServerGrpcClientFactory,
} from "./grpc-ingress";
import type { ResolvedViewServerGrpcRuntimeOptions } from "./runtime-options";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";
import type { ViewServerRuntimeCoreInternalLiveClient } from "@view-server/runtime-core/internal";

type ViewServerGrpcHealthRefreshRequest = Effect.Effect<void>;

type RuntimeCallable = (...args: ReadonlyArray<never>) => unknown;

type RuntimeLeasedFeedDefinition = {
  readonly lifecycle: "leased";
  readonly topic: string;
  readonly client: string;
  readonly routeBy: ReadonlyArray<string>;
  readonly request: RuntimeCallable;
  readonly acquire: RuntimeCallable;
  readonly release?: RuntimeCallable;
  readonly map: RuntimeCallable;
};

type RuntimeTopicDefinition = {
  readonly schema: RowSchema & Schema.Decoder<object>;
  readonly key: string;
};

type CanonicalRouteValue =
  | BigDecimal.BigDecimal
  | bigint
  | string
  | number
  | boolean
  | null
  | ReadonlyArray<CanonicalRouteValue>
  | { readonly [key: string]: CanonicalRouteValue };

type LeasedFeedRoute = Readonly<Record<string, CanonicalRouteValue>>;

type LeasedFeedRuntimeInput = {
  readonly client: unknown;
  readonly request: unknown;
  readonly route: LeasedFeedRoute;
  readonly session: {
    readonly id: string | null;
    readonly forwardedHeaders: Readonly<Record<string, string>>;
    readonly systemHeaders: Readonly<Record<string, string>>;
  };
};

type ActiveLease = {
  readonly feedName: string;
  readonly feedKey: string;
  readonly feed: RuntimeLeasedFeedDefinition;
  readonly route: LeasedFeedRoute;
  readonly scope: Scope.Scope;
  readonly publicToInternalKeys: Map<string, string>;
  readonly internalToPublicKeys: Map<string, string>;
  readonly statusQueues: Set<Queue.Queue<StatusEvent>>;
  subscribers: number;
  acceptingSubscribers: boolean;
  resourcesReleased: boolean;
};

type AcquiredLease = {
  readonly lease: ActiveLease;
  readonly statusQueue: Queue.Queue<StatusEvent>;
};

export type ViewServerGrpcLeaseManager<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly liveClient: ViewServerRuntimeLiveClient<Topics>;
  readonly close: Effect.Effect<void>;
};

const grpcMessageBatchSize = 256;
const grpcMessageBatchFlushInterval = "2 millis";

const sharedLeasedFeedSession = {
  id: null,
  forwardedHeaders: {},
  systemHeaders: {},
};

const isRuntimeLeasedStream = (value: unknown): value is Stream.Stream<unknown, unknown, never> =>
  Stream.isStream(value);

const isRuntimeReleaseEffect = (value: unknown): value is Effect.Effect<void, unknown, never> =>
  Effect.isEffect(value);

const isRuntimeMutationEffect = (value: unknown): value is Effect.Effect<unknown, unknown, never> =>
  Effect.isEffect(value);

const ignoreGrpcFeedReleaseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring leased gRPC feed release failure.",
);
const ignoreGrpcHealthRefreshFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring leased gRPC health refresh failure.",
);
const ignoreLeasedSubscriptionCloseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring leased gRPC subscription close failure.",
);
const ignoreLeasedReleaseFailure = ignoreLoggedTypedFailuresPreserveNonTypedFailures(
  "Ignoring leased gRPC release failure.",
);

const runtimeError = (input: {
  readonly code: Extract<
    ViewServerRuntimeError,
    { readonly _tag: "ViewServerRuntimeError" }
  >["code"];
  readonly topic?: string;
  readonly message: string;
}): ViewServerRuntimeError => {
  if (input.topic === undefined) {
    return {
      _tag: "ViewServerRuntimeError",
      code: input.code,
      message: input.message,
    };
  }
  return {
    _tag: "ViewServerRuntimeError",
    code: input.code,
    topic: input.topic,
    message: input.message,
  };
};

const grpcLeaseError = (input: {
  readonly message: string;
  readonly cause: unknown;
  readonly feedName: string;
  readonly topic: string;
}) =>
  new ViewServerGrpcIngressError({
    message: input.message,
    cause: input.cause,
    feedName: input.feedName,
    topic: input.topic,
  });

const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isPlainRouteRecord = (value: unknown): value is Readonly<Record<string, unknown>> => {
  if (!isRecord(value)) {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return (
    (prototype === null || prototype === Object.prototype) &&
    Object.getOwnPropertySymbols(value).length === 0 &&
    Object.getOwnPropertyNames(value).length === Object.keys(value).length
  );
};

const isCanonicalRouteValue = (value: unknown): value is CanonicalRouteValue => {
  if (BigDecimal.isBigDecimal(value)) {
    return true;
  }
  if (
    typeof value === "bigint" ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    value === null
  ) {
    return true;
  }
  if (typeof value === "number") {
    return Number.isFinite(value);
  }
  if (Array.isArray(value)) {
    return value.every(isCanonicalRouteValue);
  }
  if (isPlainRouteRecord(value)) {
    return Object.keys(value).every((key) => isCanonicalRouteValue(value[key]));
  }
  return false;
};

const exactEqValue = (value: unknown): Option.Option<unknown> => {
  if (!isRecord(value) || Object.keys(value).length !== 1 || !Object.hasOwn(value, "eq")) {
    return Option.none();
  }
  return Option.some(value["eq"]);
};

const extractRoute = Effect.fn("ViewServerRuntime.grpc.leased.route.extract")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Topic extends Extract<keyof Topics, string>,
>(
  config: ViewServerConfig<Topics>,
  topic: Topic,
  feed: RuntimeLeasedFeedDefinition,
  query: unknown,
) {
  const routeError = validateLiveQuerySourceRoute(config.topics, topic, query);
  if (routeError !== undefined) {
    return yield* Effect.fail(
      runtimeError({
        code: "InvalidQuery",
        topic,
        message: routeError,
      }),
    );
  }
  if (!isRecord(query) || !isRecord(query["where"])) {
    return yield* Effect.fail(
      runtimeError({
        code: "InvalidQuery",
        topic,
        message: `Leased topic ${topic} requires exact equality filters for route fields: ${feed.routeBy.join(", ")}.`,
      }),
    );
  }
  const topicDefinition = config.topics[topic];
  const route: Record<string, CanonicalRouteValue> = Object.create(null);
  for (const field of feed.routeBy) {
    const value = exactEqValue(query["where"][field]);
    if (Option.isNone(value)) {
      return yield* Effect.fail(
        runtimeError({
          code: "InvalidQuery",
          topic,
          message: `Leased topic ${topic} route field ${field} must use an exact eq filter.`,
        }),
      );
    }
    const fieldSchema = topicDefinition.schema.fields[field];
    if (fieldSchema === undefined) {
      return yield* Effect.fail(
        runtimeError({
          code: "InvalidQuery",
          topic,
          message: `Leased topic ${topic} route field ${field} is not in the topic schema.`,
        }),
      );
    }
    const routeValue = yield* Schema.decodeUnknownEffect(fieldSchema)(value.value).pipe(
      Effect.mapError(() =>
        runtimeError({
          code: "InvalidQuery",
          topic,
          message: `Leased topic ${topic} route field ${field} value does not match the topic schema.`,
        }),
      ),
    );
    if (!isCanonicalRouteValue(routeValue)) {
      return yield* Effect.fail(
        runtimeError({
          code: "InvalidQuery",
          topic,
          message: `Leased topic ${topic} route field ${field} value cannot be used as a stable leased gRPC route key.`,
        }),
      );
    }
    route[field] = routeValue;
  }
  return route;
});

const encodeFrame = (tag: string, payload: string): string => `${tag}:${payload.length}:${payload}`;

const isCanonicalRouteArray = (
  value: CanonicalRouteValue,
): value is ReadonlyArray<CanonicalRouteValue> => Array.isArray(value);

const encodeRouteRecord = (value: Readonly<Record<string, CanonicalRouteValue>>): string => {
  const entries: Array<string> = [];
  for (const [key, routeValue] of Object.entries(value).sort(([left], [right]) =>
    left.localeCompare(right),
  )) {
    const encodedValue = encodeRouteValue(routeValue);
    const encodedKey = JSON.stringify(key);
    entries.push(`${encodedKey.length}:${encodedKey}${encodedValue.length}:${encodedValue}`);
  }
  return entries.join("");
};

const encodeRouteValue = (value: CanonicalRouteValue): string => {
  if (BigDecimal.isBigDecimal(value)) {
    return encodeFrame("bigDecimal", BigDecimal.format(BigDecimal.normalize(value)));
  }
  if (typeof value === "bigint") {
    return encodeFrame("bigint", value.toString());
  }
  if (typeof value === "string") {
    return encodeFrame("string", value);
  }
  if (typeof value === "number") {
    return encodeFrame("number", Object.is(value, -0) ? "-0" : value.toString());
  }
  if (typeof value === "boolean") {
    return encodeFrame("boolean", value ? "true" : "false");
  }
  if (value === null) {
    return encodeFrame("null", "null");
  }
  if (isCanonicalRouteArray(value)) {
    const entries: Array<string> = [];
    for (const entry of value) {
      const encodedEntry = encodeRouteValue(entry);
      entries.push(`${encodedEntry.length}:${encodedEntry}`);
    }
    return encodeFrame("array", entries.join(""));
  }
  return encodeFrame("object", encodeRouteRecord(value));
};

const routeFeedKey = Effect.fn("ViewServerRuntime.grpc.leased.route.feedKey")(function* <
  Topic extends string,
>(topic: Topic, feedName: string, feed: RuntimeLeasedFeedDefinition, route: LeasedFeedRoute) {
  const parts: Array<string> = [];
  for (const field of feed.routeBy) {
    const routeValue = route[field];
    if (routeValue === undefined) {
      return yield* grpcLeaseError({
        message: `Leased gRPC route is missing configured field ${field}`,
        cause: route,
        feedName,
        topic,
      });
    }
    const encodedValue = encodeRouteValue(routeValue);
    parts.push(`${encodeURIComponent(field)}=${encodeURIComponent(encodedValue)}`);
  }
  return `${topic}/${feedName}/leased/${parts.join("&")}`;
});

const internalRowKey = (feedKey: string, publicKey: string): string =>
  `${feedKey}/row/${publicKey}`;

const internalRowKeyPrefix = (feedKey: string): string => `${feedKey}/row/`;

const callFeedRequest: (
  feedName: string,
  feed: RuntimeLeasedFeedDefinition,
  route: LeasedFeedRoute,
) => Effect.Effect<unknown, ViewServerGrpcIngressError, never> = Effect.fn(
  "ViewServerRuntime.grpc.leased.request",
)(function* (feedName, feed, route) {
  return yield* Effect.try({
    try: () => Reflect.apply(feed.request, undefined, [route]),
    catch: (cause) =>
      grpcLeaseError({
        message: `gRPC leased feed request creation failed for ${feedName}`,
        cause,
        feedName,
        topic: feed.topic,
      }),
  });
});

const callFeedAcquire: (
  feedName: string,
  feed: RuntimeLeasedFeedDefinition,
  input: LeasedFeedRuntimeInput,
) => Effect.Effect<Stream.Stream<unknown, unknown, never>, ViewServerGrpcIngressError, never> =
  Effect.fn("ViewServerRuntime.grpc.leased.acquire")(function* (feedName, feed, input) {
    const stream = yield* Effect.try({
      try: () => Reflect.apply(feed.acquire, undefined, [input]),
      catch: (cause) =>
        grpcLeaseError({
          message: `gRPC leased feed acquire failed for ${feedName}`,
          cause,
          feedName,
          topic: feed.topic,
        }),
    });
    if (isRuntimeLeasedStream(stream)) {
      return stream;
    }
    return yield* grpcLeaseError({
      message: `gRPC leased feed acquire did not return a Stream for ${feedName}`,
      cause: stream,
      feedName,
      topic: feed.topic,
    });
  });

const callFeedRelease: (
  feedName: string,
  feed: RuntimeLeasedFeedDefinition,
  input: LeasedFeedRuntimeInput,
) => Effect.Effect<void, unknown, never> = Effect.fn("ViewServerRuntime.grpc.leased.release")(
  function* (feedName, feed, input) {
    const releaseCallback = feed.release;
    if (releaseCallback === undefined) {
      return;
    }
    const release = yield* Effect.try({
      try: () => Reflect.apply(releaseCallback, undefined, [input]),
      catch: (cause) =>
        grpcLeaseError({
          message: `gRPC leased feed release failed for ${feedName}`,
          cause,
          feedName,
          topic: feed.topic,
        }),
    });
    if (isRuntimeReleaseEffect(release)) {
      yield* release.pipe(Effect.asVoid);
    }
  },
);

const topicDefinitionFor = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  topic: string,
  feedName: string,
): Effect.Effect<RuntimeTopicDefinition, ViewServerGrpcIngressError> =>
  Effect.suspend(() => {
    const topicDefinition = config.topics[topic];
    if (topicDefinition !== undefined) {
      return Effect.succeed(topicDefinition);
    }
    return grpcLeaseError({
      message: `gRPC leased feed ${feedName} references unknown topic ${topic}`,
      cause: topic,
      feedName,
      topic,
    });
  });

const mapLeasedValue = Effect.fn("ViewServerRuntime.grpc.leased.map")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  config: ViewServerConfig<Topics>,
  feedName: string,
  feed: RuntimeLeasedFeedDefinition,
  route: LeasedFeedRoute,
  value: unknown,
) {
  const topicDefinition = yield* topicDefinitionFor(config, feed.topic, feedName);
  const row = yield* Effect.try({
    try: () =>
      Reflect.apply(feed.map, undefined, [
        {
          value,
          route,
          schema: topicDefinition.schema,
        },
      ]),
    catch: (cause) =>
      grpcLeaseError({
        message: `gRPC leased feed mapping failed for ${feedName}`,
        cause,
        feedName,
        topic: feed.topic,
      }),
  });
  const decoded = yield* Schema.decodeUnknownEffect(topicDefinition.schema)(row).pipe(
    Effect.mapError((cause) =>
      grpcLeaseError({
        message: `gRPC leased feed mapping produced an invalid row for ${feedName}`,
        cause,
        feedName,
        topic: feed.topic,
      }),
    ),
  );
  return decoded;
});

const rowKey = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerConfig<Topics>,
  topic: string,
  feedName: string,
  row: object,
): Effect.Effect<string, ViewServerGrpcIngressError> =>
  Effect.gen(function* () {
    const topicDefinition = yield* topicDefinitionFor(config, topic, feedName);
    const keyField: string = topicDefinition.key;
    const key = Reflect.get(row, keyField);
    if (typeof key === "string") {
      return key;
    }
    return yield* grpcLeaseError({
      message: `gRPC leased feed row key ${keyField} for ${topic} is not a string`,
      cause: key,
      feedName,
      topic,
    });
  });

const topicKeyField = Effect.fn("ViewServerRuntime.grpc.leased.topicKeyField")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(config: ViewServerConfig<Topics>, topic: string, feedName: string) {
  const topicDefinition = yield* topicDefinitionFor(config, topic, feedName);
  return topicDefinition.key;
});

const replaceObjectField = (value: object, field: string, replacement: unknown): object => ({
  ...value,
  [field]: replacement,
});

const internalizeLeasedRow = Effect.fn("ViewServerRuntime.grpc.leased.row.internalize")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(config: ViewServerConfig<Topics>, lease: ActiveLease, row: object) {
  const publicKey = yield* rowKey(config, lease.feed.topic, lease.feedName, row);
  const internalKey = internalRowKey(lease.feedKey, publicKey);
  lease.publicToInternalKeys.set(publicKey, internalKey);
  lease.internalToPublicKeys.set(internalKey, publicKey);
  const keyField = yield* topicKeyField(config, lease.feed.topic, lease.feedName);
  return replaceObjectField(row, keyField, internalKey);
});

const publicKeyForInternalKey = (lease: ActiveLease, key: string): string =>
  lease.internalToPublicKeys.get(key) ?? key;

const externalizeLeasedRow = <Row extends object>(
  lease: ActiveLease,
  keyField: string,
  row: Row,
): Row => {
  const rowKeyValue = Reflect.get(row, keyField);
  if (typeof rowKeyValue !== "string") {
    return row;
  }
  const cloned = Object.assign({}, row);
  Reflect.set(cloned, keyField, publicKeyForInternalKey(lease, rowKeyValue));
  return cloned;
};

const rewriteKeyPredicateValue = (lease: ActiveLease, value: unknown): unknown => {
  if (typeof value === "string") {
    return lease.publicToInternalKeys.get(value) ?? internalRowKey(lease.feedKey, value);
  }
  if (Array.isArray(value)) {
    return value.map((entry) => rewriteKeyPredicateValue(lease, entry));
  }
  return value;
};

const rewriteKeyFilter = (
  lease: ActiveLease,
  filter: Readonly<Record<string, unknown>>,
): Record<string, unknown> => {
  const rewritten: Record<string, unknown> = {};
  for (const [operator, value] of Object.entries(filter)) {
    rewritten[operator] = rewriteKeyPredicateValue(lease, value);
  }
  return rewritten;
};

const internalizeLeasedQuery = <Query extends Readonly<Record<string, unknown>>>(
  lease: ActiveLease,
  keyField: string,
  query: Query,
): Query => {
  const currentWhere: Record<string, unknown> = Object(query["where"]);
  const where: Record<string, unknown> = { ...currentWhere };
  const currentKeyFilter = Reflect.get(currentWhere, keyField);
  const routeIncludesKeyField = lease.feed.routeBy.includes(keyField);
  const exactKeyFilter = exactEqValue(currentKeyFilter);
  const partitionFilter = { startsWith: internalRowKeyPrefix(lease.feedKey) };
  const keyFilter =
    routeIncludesKeyField && Option.isSome(exactKeyFilter)
      ? {
          eq: rewriteKeyPredicateValue(lease, exactKeyFilter.value),
        }
      : currentKeyFilter === undefined
        ? partitionFilter
        : isRecord(currentKeyFilter)
          ? {
              ...partitionFilter,
              ...rewriteKeyFilter(lease, currentKeyFilter),
            }
          : {
              ...partitionFilter,
              eq: rewriteKeyPredicateValue(lease, currentKeyFilter),
            };
  Reflect.set(where, keyField, keyFilter);
  return {
    ...query,
    where,
  };
};

const leasedFeedUnavailableStatus = (lease: ActiveLease, message: string): StatusEvent => ({
  type: "status",
  topic: lease.feed.topic,
  queryId: lease.feedKey,
  status: "error",
  code: "RuntimeUnavailable",
  message,
});

const notifyLeaseSubscribers = Effect.fn("ViewServerRuntime.grpc.leased.subscribers.notify")(
  function* (lease: ActiveLease, message: string) {
    const status = leasedFeedUnavailableStatus(lease, message);
    yield* Effect.forEach(lease.statusQueues, (queue) => Queue.offer(queue, status), {
      discard: true,
    });
  },
);

const externalizeLeasedEvent = <Row extends object>(
  lease: ActiveLease,
  keyField: string,
  event: ViewServerLiveEvent<Row>,
): ViewServerLiveEvent<Row> => {
  if (event.type === "snapshot") {
    return {
      ...event,
      keys: event.keys.map((key) => publicKeyForInternalKey(lease, key)),
      rows: event.rows.map((row) => externalizeLeasedRow(lease, keyField, row)),
    };
  }
  if (event.type === "delta") {
    return {
      ...event,
      operations: event.operations.map((operation) => {
        if (operation.type === "move" || operation.type === "remove") {
          return {
            ...operation,
            key: publicKeyForInternalKey(lease, operation.key),
          };
        }
        return {
          ...operation,
          key: publicKeyForInternalKey(lease, operation.key),
          row: externalizeLeasedRow(lease, keyField, operation.row),
        };
      }),
    };
  }
  return event;
};

const callRuntimePublishMany = Effect.fn("ViewServerRuntime.grpc.leased.runtime.publishMany")(
  function* <const Topics extends ViewServerRuntimeTopicDefinitions, const Topic extends string>(
    runtimeClient: ViewServerRuntimeClient<Topics>,
    topic: Topic,
    rows: ReadonlyArray<object>,
    feedName: string,
  ) {
    const effect = Reflect.apply(runtimeClient.publishMany, runtimeClient, [topic, rows]);
    if (!isRuntimeMutationEffect(effect)) {
      return yield* grpcLeaseError({
        message: `Runtime publishMany did not return an Effect for leased gRPC feed ${feedName}`,
        cause: effect,
        feedName,
        topic,
      });
    }
    yield* effect.pipe(
      Effect.asVoid,
      Effect.mapError((cause) =>
        grpcLeaseError({
          message: `gRPC leased feed publish failed for ${feedName}`,
          cause,
          feedName,
          topic,
        }),
      ),
    );
  },
);

const callRuntimeDelete = Effect.fn("ViewServerRuntime.grpc.leased.runtime.delete")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Topic extends string,
>(runtimeClient: ViewServerRuntimeClient<Topics>, topic: Topic, key: string, feedName: string) {
  const effect = Reflect.apply(runtimeClient.delete, runtimeClient, [topic, key]);
  if (!isRuntimeMutationEffect(effect)) {
    return yield* grpcLeaseError({
      message: `Runtime delete did not return an Effect for leased gRPC feed ${feedName}`,
      cause: effect,
      feedName,
      topic,
    });
  }
  yield* effect.pipe(
    Effect.asVoid,
    Effect.mapError((cause) =>
      grpcLeaseError({
        message: `gRPC leased feed row cleanup failed for ${feedName}`,
        cause,
        feedName,
        topic,
      }),
    ),
  );
});

const publishLeasedBatch = Effect.fn("ViewServerRuntime.grpc.leased.publishBatch")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  config: ViewServerConfig<Topics>,
  runtimeClient: ViewServerRuntimeClient<Topics>,
  requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
  health: ViewServerGrpcHealthLedger<Topics>,
  lease: ActiveLease,
  route: LeasedFeedRoute,
  values: ReadonlyArray<unknown>,
) {
  const rows = yield* Effect.forEach(values, (value) =>
    mapLeasedValue(config, lease.feedName, lease.feed, route, value).pipe(
      Effect.tapError((error) =>
        Clock.currentTimeMillis.pipe(
          Effect.flatMap((nowMillis) =>
            health.mappingFailed(lease.feedKey, {
              message: error.message,
              nowMillis,
            }),
          ),
        ),
      ),
    ),
  );
  const internalRows = yield* Effect.forEach(rows, (row) =>
    internalizeLeasedRow(config, lease, row),
  );
  yield* callRuntimePublishMany(runtimeClient, lease.feed.topic, internalRows, lease.feedName).pipe(
    Effect.tapError((error) =>
      Clock.currentTimeMillis.pipe(
        Effect.flatMap((nowMillis) =>
          health.publishFailed(lease.feedKey, {
            message: error.message,
            nowMillis,
          }),
        ),
      ),
    ),
  );
  const nowMillis = yield* Clock.currentTimeMillis;
  yield* health.rowsPublished(lease.feedKey, {
    messages: values.length,
    rows: rows.length,
    rowCount: lease.publicToInternalKeys.size,
    nowMillis,
  });
  yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
});

const internalFeedFailureMessage = (feedName: string, cause: Cause.Cause<unknown>): string =>
  `gRPC leased feed ${feedName} failed: ${Cause.pretty(cause)}`;

const startLeaseStream = Effect.fn("ViewServerRuntime.grpc.leased.stream.start")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(
  config: ViewServerConfig<Topics>,
  runtimeClient: ViewServerRuntimeClient<Topics>,
  requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
  health: ViewServerGrpcHealthLedger<Topics>,
  lease: ActiveLease,
  lock: Semaphore.Semaphore,
  route: LeasedFeedRoute,
  input: LeasedFeedRuntimeInput,
) {
  const releaseResources = Effect.fn("ViewServerRuntime.grpc.leased.resources.release")(
    function* () {
      if (lease.resourcesReleased) {
        return;
      }
      lease.resourcesReleased = true;
      yield* ignoreGrpcFeedReleaseFailure(callFeedRelease(lease.feedName, lease.feed, input));
    },
  );
  yield* Scope.addFinalizer(lease.scope, releaseResources());
  const stream = yield* callFeedAcquire(lease.feedName, lease.feed, input);
  const degradeInactiveLease = (input: {
    readonly publicMessage: string;
    readonly healthMessage: string;
  }) =>
    lock.withPermit(
      Effect.gen(function* () {
        lease.acceptingSubscribers = false;
        yield* notifyLeaseSubscribers(lease, input.publicMessage);
        yield* releaseResources();
        yield* health.feedDegraded(lease.feedKey, input.healthMessage);
        yield* health.clientDegraded(lease.feed.client, input.healthMessage);
        yield* closeLeaseRows(runtimeClient, lease).pipe(ignoreLeasedReleaseFailure);
        yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
      }),
    );
  const runFeed = stream.pipe(
    Stream.mapError((cause) =>
      grpcLeaseError({
        message: `gRPC leased feed stream failed for ${lease.feedName}`,
        cause,
        feedName: lease.feedName,
        topic: lease.feed.topic,
      }),
    ),
    Stream.groupedWithin(grpcMessageBatchSize, grpcMessageBatchFlushInterval),
    Stream.runForEach((values) =>
      publishLeasedBatch(config, runtimeClient, requestHealthRefresh, health, lease, route, values),
    ),
    Effect.exit,
    Effect.flatMap((exit) => {
      if (Exit.isSuccess(exit)) {
        return degradeInactiveLease({
          publicMessage: "gRPC leased upstream completed unexpectedly.",
          healthMessage: `gRPC leased feed ${lease.feedName} completed unexpectedly.`,
        });
      }
      if (Cause.hasInterruptsOnly(exit.cause)) {
        return degradeInactiveLease({
          publicMessage: "gRPC leased upstream interrupted unexpectedly.",
          healthMessage: `gRPC leased feed ${lease.feedName} interrupted unexpectedly.`,
        });
      }
      return degradeInactiveLease({
        publicMessage: "gRPC leased upstream failed.",
        healthMessage: internalFeedFailureMessage(lease.feedName, exit.cause),
      });
    }),
  );
  yield* runFeed.pipe(Effect.forkIn(lease.scope, { startImmediately: true }));
});

const closeLeaseRows = Effect.fn("ViewServerRuntime.grpc.leased.rows.close")(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(runtimeClient: ViewServerRuntimeClient<Topics>, lease: ActiveLease) {
  yield* Effect.forEach(
    lease.internalToPublicKeys.keys(),
    (key) => callRuntimeDelete(runtimeClient, lease.feed.topic, key, lease.feedName),
    {
      discard: true,
    },
  );
  lease.publicToInternalKeys.clear();
  lease.internalToPublicKeys.clear();
});

const leasedFeedsByTopic = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
): Map<string, readonly [string, RuntimeLeasedFeedDefinition]> => {
  const feeds = new Map<string, readonly [string, RuntimeLeasedFeedDefinition]>();
  for (const [feedName, feed] of Object.entries(options.feeds)) {
    if (feed.lifecycle === "leased") {
      feeds.set(feed.topic, [feedName, feed]);
    }
  }
  return feeds;
};

const grpcLeasedSourceTopics = <const Topics extends ViewServerRuntimeTopicDefinitions>(
  config: ViewServerConfig<Topics>,
): Set<string> => {
  const topics = new Set<string>();
  for (const [topic, definition] of Object.entries(config.topics)) {
    const source = Reflect.get(definition, "source");
    if (
      typeof source === "object" &&
      source !== null &&
      Reflect.get(source, "kind") === "grpc" &&
      Reflect.get(source, "lifecycle") === "leased"
    ) {
      topics.add(topic);
    }
  }
  return topics;
};

const leasedRuntimeAccessError = (topic: string): ViewServerRuntimeError =>
  runtimeError({
    code: "UnsupportedQuery",
    topic,
    message:
      "Leased gRPC topics do not support direct runtime mutations or one-shot snapshots; use a live subscription so the runtime can own lease lifecycle.",
  });

export const makeViewServerGrpcLeaseManager = Effect.fn(
  "ViewServerRuntime.grpc.leased.makeManager",
)(function* <
  const Topics extends ViewServerRuntimeTopicDefinitions,
  const Clients extends GrpcRuntimeClients,
>(
  config: ViewServerConfig<Topics>,
  runtimeClient: ViewServerRuntimeClient<Topics>,
  liveClient: ViewServerRuntimeLiveClient<Topics>,
  internalLiveClient: ViewServerRuntimeCoreInternalLiveClient<Topics>,
  requestHealthRefresh: ViewServerGrpcHealthRefreshRequest,
  options: ResolvedViewServerGrpcRuntimeOptions<Topics, Clients>,
  health: ViewServerGrpcHealthLedger<Topics>,
  makeClient: ViewServerGrpcClientFactory = makeDefaultGrpcClient,
) {
  const leases = new Map<string, ActiveLease>();
  const feedsByTopic = leasedFeedsByTopic(options);
  const leasedTopics = grpcLeasedSourceTopics(config);
  const lock = yield* Semaphore.make(1);
  let closed = false;

  const acquireLease = Effect.fn("ViewServerRuntime.grpc.leased.acquireLease")(function* <
    const Topic extends Extract<keyof Topics, string>,
  >(topic: Topic, query: unknown) {
    const configuredFeed = feedsByTopic.get(topic);
    if (configuredFeed === undefined) {
      if (leasedTopics.has(topic)) {
        return yield* Effect.fail(
          runtimeError({
            code: "RuntimeUnavailable",
            topic,
            message: `Leased gRPC topic ${topic} has no configured leased feed.`,
          }),
        );
      }
      return Option.none<AcquiredLease>();
    }
    const [feedName, feed] = configuredFeed;
    const route = yield* extractRoute(config, topic, feed, query);
    const feedKey = yield* routeFeedKey(topic, feedName, feed, route).pipe(
      Effect.mapError((error) =>
        runtimeError({
          code: "RuntimeUnavailable",
          topic,
          message: error.message,
        }),
      ),
    );
    if (closed) {
      return yield* Effect.fail(
        runtimeError({
          code: "RuntimeUnavailable",
          topic,
          message: "gRPC leased feed manager is closed.",
        }),
      );
    }
    const existing = leases.get(feedKey);
    const statusQueue = yield* Queue.unbounded<StatusEvent>();
    if (existing !== undefined) {
      if (!existing.acceptingSubscribers) {
        yield* Queue.shutdown(statusQueue);
        return yield* Effect.fail(
          runtimeError({
            code: "RuntimeUnavailable",
            topic,
            message:
              "gRPC leased upstream is not accepting new subscribers after completion or failure.",
          }),
        );
      }
      existing.subscribers += 1;
      existing.statusQueues.add(statusQueue);
      yield* health.subscriberAdded(feedKey);
      yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
      return Option.some({
        lease: existing,
        statusQueue,
      });
    }
    const clientDefinition = options.clients[feed.client];
    if (clientDefinition === undefined) {
      return yield* Effect.fail(
        runtimeError({
          code: "RuntimeUnavailable",
          topic,
          message: `gRPC leased feed ${feedName} references missing client: ${feed.client}`,
        }),
      );
    }
    const baseUrl = options.clientBaseUrls[feed.client];
    if (baseUrl === undefined) {
      return yield* Effect.fail(
        runtimeError({
          code: "RuntimeUnavailable",
          topic,
          message: `gRPC leased feed ${feedName} references unresolved client URL: ${feed.client}`,
        }),
      );
    }
    const grpcClient = yield* Effect.try({
      try: () => makeClient(clientDefinition, baseUrl),
      catch: (cause) =>
        grpcLeaseError({
          message: `gRPC leased client creation failed for ${feedName}`,
          cause,
          feedName,
          topic,
        }),
    }).pipe(
      Effect.mapError((error) =>
        runtimeError({
          code: "RuntimeUnavailable",
          topic,
          message: error.message,
        }),
      ),
    );
    const request = yield* callFeedRequest(feedName, feed, route).pipe(
      Effect.mapError((error) =>
        runtimeError({
          code: "RuntimeUnavailable",
          topic,
          message: error.message,
        }),
      ),
    );
    const scope = yield* Scope.make("parallel");
    const lease: ActiveLease = {
      feedName,
      feedKey,
      feed,
      route,
      scope,
      publicToInternalKeys: new Map<string, string>(),
      internalToPublicKeys: new Map<string, string>(),
      statusQueues: new Set<Queue.Queue<StatusEvent>>([statusQueue]),
      subscribers: 1,
      acceptingSubscribers: true,
      resourcesReleased: false,
    };
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        leases.set(feedKey, lease);
        const input: LeasedFeedRuntimeInput = {
          client: grpcClient,
          request,
          route,
          session: sharedLeasedFeedSession,
        };
        const startedAt = yield* Clock.currentTimeMillis;
        yield* health.clientConnected(feed.client, startedAt);
        yield* health.leasedFeedStarting({
          feedName,
          feedKey,
          topic,
          clientName: feed.client,
        });
        yield* health.subscriberAdded(feedKey);
        yield* health.feedReady(feedKey);
        yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
        yield* restore(
          startLeaseStream(
            config,
            runtimeClient,
            requestHealthRefresh,
            health,
            lease,
            lock,
            route,
            input,
          ),
        ).pipe(
          Effect.onError((cause) =>
            Scope.close(scope, Exit.void).pipe(
              Effect.andThen(
                health.clientDegraded(
                  feed.client,
                  `gRPC leased feed ${feedName} failed to start: ${String(cause)}`,
                ),
              ),
              Effect.andThen(Queue.shutdown(statusQueue)),
              Effect.andThen(health.leasedFeedRemoved(feedKey)),
              Effect.andThen(Effect.sync(() => leases.delete(feedKey))),
              Effect.andThen(ignoreGrpcHealthRefreshFailure(requestHealthRefresh)),
            ),
          ),
          Effect.mapError((error) =>
            runtimeError({
              code: "RuntimeUnavailable",
              topic,
              message: error.message,
            }),
          ),
        );
        return Option.some({
          lease,
          statusQueue,
        });
      }),
    );
  });

  const releaseLeaseUnderPermit: (
    lease: ActiveLease,
  ) => Effect.Effect<Option.Option<ActiveLease>, never, never> = Effect.fn(
    "ViewServerRuntime.grpc.leased.releaseLeaseUnderPermit",
  )(function* (lease: ActiveLease) {
    const current = leases.get(lease.feedKey);
    if (current === undefined) {
      return Option.none();
    }
    current.subscribers -= 1;
    yield* health.subscriberRemoved(lease.feedKey);
    if (current.subscribers > 0) {
      yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
      return Option.none();
    }
    current.acceptingSubscribers = false;
    yield* health.feedStopping(lease.feedKey);
    yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
    return Option.some(current);
  });

  const cleanupReleasedLease: (lease: ActiveLease) => Effect.Effect<void, never, never> = Effect.fn(
    "ViewServerRuntime.grpc.leased.releaseLease.cleanup",
  )(function* (lease: ActiveLease) {
    yield* Scope.close(lease.scope, Exit.void);
    yield* closeLeaseRows(runtimeClient, lease).pipe(ignoreLeasedReleaseFailure);
    yield* lock.withPermit(
      Effect.gen(function* () {
        leases.delete(lease.feedKey);
        yield* health.leasedFeedRemoved(lease.feedKey);
      }),
    );
    yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
  });

  const releaseLease: (lease: ActiveLease) => Effect.Effect<void, never, never> = Effect.fn(
    "ViewServerRuntime.grpc.leased.releaseLease",
  )(function* (lease: ActiveLease) {
    const releasedLease = yield* lock.withPermit(releaseLeaseUnderPermit(lease));
    if (Option.isSome(releasedLease)) {
      yield* cleanupReleasedLease(releasedLease.value);
    }
  });

  const withLeaseClose = <Row extends object>(input: {
    readonly subscription: ViewServerLiveSubscription<Row>;
    readonly lease: ActiveLease;
    readonly keyField: string;
    readonly statusQueue: Queue.Queue<StatusEvent>;
  }): ViewServerLiveSubscription<Row> => {
    let closed = false;
    const close: () => Effect.Effect<void, never, never> = Effect.fn(
      "ViewServerRuntime.grpc.leased.subscription.close",
    )(function* () {
      yield* Effect.uninterruptible(
        Effect.gen(function* () {
          if (closed) {
            return;
          }
          closed = true;
          input.lease.statusQueues.delete(input.statusQueue);
          yield* Queue.shutdown(input.statusQueue);
          yield* input.subscription.close().pipe(ignoreLeasedSubscriptionCloseFailure);
          yield* releaseLease(input.lease);
        }),
      );
    });
    const runtimeEvents = input.subscription.events.pipe(
      Stream.map((event) => externalizeLeasedEvent(input.lease, input.keyField, event)),
    );
    const fallbackStatusQueryId = `${input.lease.feed.topic}/leased-status`;
    const statusEventsWithQueryId = (queryId: string) =>
      Stream.fromQueue(input.statusQueue).pipe(
        Stream.map((event) => ({
          ...event,
          queryId,
        })),
      );
    const eventsWithStableStatusQueryId = Stream.unwrap(
      Effect.gen(function* () {
        const [initialEvents, remainingEvents] = yield* Stream.peel(runtimeEvents, Sink.take(1));
        const initialEvent = initialEvents[0];
        if (initialEvent === undefined) {
          return statusEventsWithQueryId(fallbackStatusQueryId);
        }
        const statusEvents = statusEventsWithQueryId(initialEvent.queryId);
        return Stream.succeed(initialEvent).pipe(
          Stream.concat(remainingEvents.pipe(Stream.merge(statusEvents))),
        );
      }),
    );
    return {
      events: eventsWithStableStatusQueryId.pipe(Stream.ensuring(close())),
      close: () => close(),
    };
  };

  const releaseAcquiredLeaseUnderPermit = (
    acquired: AcquiredLease,
  ): Effect.Effect<Option.Option<ActiveLease>, never, never> =>
    Effect.gen(function* () {
      acquired.lease.statusQueues.delete(acquired.statusQueue);
      yield* Queue.shutdown(acquired.statusQueue);
      return yield* releaseLeaseUnderPermit(acquired.lease);
    });
  const releaseAcquiredLease = (acquired: AcquiredLease): Effect.Effect<void, never, never> =>
    Effect.gen(function* () {
      const releasedLease = yield* lock.withPermit(releaseAcquiredLeaseUnderPermit(acquired));
      if (Option.isSome(releasedLease)) {
        yield* cleanupReleasedLease(releasedLease.value);
      }
    });

  function subscribe<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  ): Effect.Effect<
    ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError | ViewServerTransportError
  >;
  function subscribe<
    Topic extends Extract<keyof Topics, string>,
    const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
  >(
    topic: Topic,
    query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
  ): Effect.Effect<
    ViewServerLiveSubscription<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
    ViewServerRuntimeError | ViewServerTransportError
  > {
    return Effect.gen(function* () {
      const lease = yield* lock.withPermit(acquireLease(topic, query));
      if (Option.isNone(lease)) {
        return yield* internalLiveClient.subscribeInternal<Topic, Query>(topic, query);
      }
      const acquired = lease.value;
      return yield* Effect.gen(function* () {
        const keyField = yield* topicKeyField(config, topic, acquired.lease.feedName).pipe(
          Effect.mapError((error) =>
            runtimeError({
              code: "RuntimeUnavailable",
              topic,
              message: error.message,
            }),
          ),
        );
        const internalQuery = internalizeLeasedQuery(acquired.lease, keyField, query);
        const subscription = yield* internalLiveClient.subscribeInternal<Topic, Query>(
          topic,
          internalQuery,
        );
        return withLeaseClose({
          subscription,
          lease: acquired.lease,
          keyField,
          statusQueue: acquired.statusQueue,
        });
      }).pipe(Effect.onError(() => releaseAcquiredLease(acquired)));
    });
  }

  const subscribeRuntime: ViewServerRuntimeLiveClient<Topics>["subscribeRuntime"] = (
    topic,
    query,
  ) =>
    Effect.gen(function* () {
      const lease = yield* lock.withPermit(acquireLease(topic, query));
      if (Option.isNone(lease)) {
        return yield* internalLiveClient.subscribeRuntimeInternal(topic, query);
      }
      const acquired = lease.value;
      return yield* Effect.gen(function* () {
        const keyField = yield* topicKeyField(config, topic, acquired.lease.feedName).pipe(
          Effect.mapError((error) =>
            runtimeError({
              code: "RuntimeUnavailable",
              topic,
              message: error.message,
            }),
          ),
        );
        const internalQuery = internalizeLeasedQuery(acquired.lease, keyField, query);
        const subscription = yield* internalLiveClient.subscribeRuntimeInternal(
          topic,
          internalQuery,
        );
        return withLeaseClose({
          subscription,
          lease: acquired.lease,
          keyField,
          statusQueue: acquired.statusQueue,
        });
      }).pipe(Effect.onError(() => releaseAcquiredLease(acquired)));
    });

  const snapshot: ViewServerRuntimeClient<Topics>["snapshot"] = (topic, query) =>
    Effect.gen(function* () {
      if (leasedTopics.has(topic)) {
        return yield* Effect.fail(leasedRuntimeAccessError(topic));
      }
      return yield* runtimeClient.snapshot(topic, query);
    });

  const rejectLeasedMutation = (topic: string): Effect.Effect<never, ViewServerRuntimeError> =>
    Effect.fail(leasedRuntimeAccessError(topic));

  const publish: ViewServerRuntimeClient<Topics>["publish"] = (topic, row) =>
    leasedTopics.has(topic) ? rejectLeasedMutation(topic) : runtimeClient.publish(topic, row);
  const publishMany: ViewServerRuntimeClient<Topics>["publishMany"] = (topic, rows) =>
    leasedTopics.has(topic) ? rejectLeasedMutation(topic) : runtimeClient.publishMany(topic, rows);
  const patch: ViewServerRuntimeClient<Topics>["patch"] = (topic, key, patchValue) =>
    leasedTopics.has(topic)
      ? rejectLeasedMutation(topic)
      : runtimeClient.patch(topic, key, patchValue);
  const deleteRow: ViewServerRuntimeClient<Topics>["delete"] = (topic, key) =>
    leasedTopics.has(topic) ? rejectLeasedMutation(topic) : runtimeClient.delete(topic, key);

  const reset: ViewServerRuntimeClient<Topics>["reset"] = () =>
    leasedTopics.size === 0
      ? runtimeClient.reset()
      : Effect.fail(
          runtimeError({
            code: "UnsupportedQuery",
            message:
              "Leased gRPC topics do not support direct runtime reset; close the runtime or leased subscriptions so the lease manager owns cleanup.",
          }),
        );

  const client: ViewServerRuntimeClient<Topics> = {
    publish,
    publishMany,
    patch,
    delete: deleteRow,
    snapshot,
    health: runtimeClient.health,
    reset,
  };

  const close: () => Effect.Effect<void, never, never> = Effect.fn(
    "ViewServerRuntime.grpc.leased.close",
  )(function* () {
    const activeLeases = yield* lock.withPermit(
      Effect.sync(() => {
        closed = true;
        const currentLeases = Array.from(leases.values());
        leases.clear();
        for (const lease of currentLeases) {
          lease.acceptingSubscribers = false;
        }
        return currentLeases;
      }),
    );
    yield* runAllFinalizers(
      activeLeases.map((lease) =>
        Scope.close(lease.scope, Exit.void).pipe(
          Effect.andThen(
            Effect.forEach(lease.statusQueues, (queue) => Queue.shutdown(queue), {
              discard: true,
            }),
          ),
          Effect.andThen(Effect.sync(() => lease.statusQueues.clear())),
          Effect.andThen(closeLeaseRows(runtimeClient, lease).pipe(ignoreLeasedReleaseFailure)),
          Effect.andThen(health.leasedFeedRemoved(lease.feedKey)),
        ),
      ),
    );
    yield* ignoreGrpcHealthRefreshFailure(requestHealthRefresh);
  });

  return {
    client,
    liveClient: {
      close: liveClient.close.pipe(Effect.ensuring(close())),
      health: liveClient.health,
      subscribe,
      subscribeRuntime,
      subscribeHealth: liveClient.subscribeHealth,
      subscribeHealthSummary: liveClient.subscribeHealthSummary,
    },
    close: close(),
  };
});
