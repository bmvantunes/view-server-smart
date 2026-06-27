import type {
  GrpcClientHealth,
  GrpcFeedHealth,
  GrpcTopicFeedsHealth,
  ViewServerHealth,
} from "@view-server/config";
import { Effect } from "effect";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

type GrpcHealthSnapshot<Topics extends ViewServerRuntimeTopicDefinitions> = NonNullable<
  ViewServerHealth<Topics>["grpc"]
>;

type GrpcClientLedger = {
  status: GrpcClientHealth["status"];
  baseUrl: string;
  activeFeeds: number;
  lastConnectedAt: number | null;
  lastError: string | null;
};

type GrpcFeedLedger<Topic extends string> = {
  status: GrpcFeedHealth["status"];
  lifecycle: GrpcFeedHealth["lifecycle"];
  feedName: string;
  feedKey: string;
  topic: Topic;
  clientName: string;
  subscriberCount: number;
  rowCount: number;
  messagesPerSecond: number;
  rowsPerSecond: number;
  decodeFailuresPerSecond: number;
  mappingFailuresPerSecond: number;
  publishFailuresPerSecond: number;
  reconnects: number;
  lastMessageAt: number | null;
  lastError: string | null;
  rateBuckets: Array<GrpcRateBucket | undefined>;
};

type GrpcRateBucket = {
  occurredAt: number;
  messages: number;
  rows: number;
  decodeFailed: number;
  mappingFailed: number;
  publishFailed: number;
};

const grpcRateWindowMillis = 1_000;
const maxGrpcRateBuckets = grpcRateWindowMillis + 1;

export type ViewServerGrpcHealthLedger<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly healthOverlay: (
    health: ViewServerHealth<Topics>,
    nowMillis: number,
  ) => ViewServerHealth<Topics>;
  readonly clientConnected: (clientName: string, nowMillis: number) => Effect.Effect<void>;
  readonly clientDegraded: (clientName: string, message: string) => Effect.Effect<void>;
  readonly feedReady: (feedName: string) => Effect.Effect<void>;
  readonly feedStopping: (feedName: string) => Effect.Effect<void>;
  readonly feedDegraded: (feedName: string, message: string) => Effect.Effect<void>;
  readonly leasedFeedStarting: (input: {
    readonly feedName: string;
    readonly feedKey: string;
    readonly topic: Extract<keyof Topics, string>;
    readonly clientName: string;
  }) => Effect.Effect<void>;
  readonly leasedFeedRemoved: (feedKey: string) => Effect.Effect<void>;
  readonly subscriberAdded: (feedKey: string) => Effect.Effect<void>;
  readonly subscriberRemoved: (feedKey: string) => Effect.Effect<void>;
  readonly rowsPublished: (
    feedName: string,
    input: {
      readonly messages: number;
      readonly rows: number;
      readonly rowCount?: number;
      readonly nowMillis: number;
    },
  ) => Effect.Effect<void>;
  readonly mappingFailed: (
    feedName: string,
    input: {
      readonly message: string;
      readonly nowMillis: number;
    },
  ) => Effect.Effect<void>;
  readonly publishFailed: (
    feedName: string,
    input: {
      readonly message: string;
      readonly nowMillis: number;
    },
  ) => Effect.Effect<void>;
};

const initialRateBuckets = (): Array<GrpcRateBucket | undefined> =>
  Array.from({ length: maxGrpcRateBuckets }, () => undefined);

const copyClientHealth = (client: GrpcClientLedger): GrpcClientHealth => ({
  status: client.status,
  baseUrl: client.baseUrl,
  activeFeeds: client.activeFeeds,
  lastConnectedAt: client.lastConnectedAt,
  lastError: client.lastError,
});

const copyFeedHealth = <Topic extends string>(
  feed: GrpcFeedLedger<Topic>,
): GrpcFeedHealth<Topic> => ({
  status: feed.status,
  lifecycle: feed.lifecycle,
  feedName: feed.feedName,
  feedKey: feed.feedKey,
  topic: feed.topic,
  subscriberCount: feed.subscriberCount,
  rowCount: feed.rowCount,
  messagesPerSecond: feed.messagesPerSecond,
  rowsPerSecond: feed.rowsPerSecond,
  decodeFailuresPerSecond: feed.decodeFailuresPerSecond,
  mappingFailuresPerSecond: feed.mappingFailuresPerSecond,
  publishFailuresPerSecond: feed.publishFailuresPerSecond,
  reconnects: feed.reconnects,
  lastMessageAt: feed.lastMessageAt,
  lastError: feed.lastError,
});

const incrementWindow = (
  feed: GrpcFeedLedger<string>,
  nowMillis: number,
  counters: {
    readonly messages: number;
    readonly rows: number;
    readonly decodeFailed: number;
    readonly mappingFailed: number;
    readonly publishFailed: number;
  },
) => {
  const occurredAt = Math.trunc(nowMillis);
  const bucketIndex = Math.abs(occurredAt % maxGrpcRateBuckets);
  const existingBucket = feed.rateBuckets[bucketIndex];
  if (existingBucket !== undefined && existingBucket.occurredAt === occurredAt) {
    existingBucket.messages += counters.messages;
    existingBucket.rows += counters.rows;
    existingBucket.decodeFailed += counters.decodeFailed;
    existingBucket.mappingFailed += counters.mappingFailed;
    existingBucket.publishFailed += counters.publishFailed;
    return;
  }
  feed.rateBuckets[bucketIndex] = {
    occurredAt,
    messages: counters.messages,
    rows: counters.rows,
    decodeFailed: counters.decodeFailed,
    mappingFailed: counters.mappingFailed,
    publishFailed: counters.publishFailed,
  };
};

const resetIdleWindow = (feed: GrpcFeedLedger<string>, nowMillis: number) => {
  const occurredBefore = Math.trunc(nowMillis) - grpcRateWindowMillis;
  const occurredAfter = Math.trunc(nowMillis);
  let messagesPerSecond = 0;
  let rowsPerSecond = 0;
  let decodeFailuresPerSecond = 0;
  let mappingFailuresPerSecond = 0;
  let publishFailuresPerSecond = 0;
  for (const bucket of feed.rateBuckets) {
    if (
      bucket === undefined ||
      bucket.occurredAt < occurredBefore ||
      bucket.occurredAt > occurredAfter
    ) {
      continue;
    }
    messagesPerSecond += bucket.messages;
    rowsPerSecond += bucket.rows;
    decodeFailuresPerSecond += bucket.decodeFailed;
    mappingFailuresPerSecond += bucket.mappingFailed;
    publishFailuresPerSecond += bucket.publishFailed;
  }
  feed.messagesPerSecond = messagesPerSecond;
  feed.rowsPerSecond = rowsPerSecond;
  feed.decodeFailuresPerSecond = decodeFailuresPerSecond;
  feed.mappingFailuresPerSecond = mappingFailuresPerSecond;
  feed.publishFailuresPerSecond = publishFailuresPerSecond;
};

const grpcRuntimeStatus = <Topics extends ViewServerRuntimeTopicDefinitions>(
  snapshot: GrpcHealthSnapshot<Topics>,
): ViewServerHealth<Topics>["status"] => {
  const clientStatuses = Object.values(snapshot.clients).map((client) => client.status);
  const feedStatuses = Object.values(snapshot.feeds).flatMap((feeds) => [
    ...Object.values(feeds.materialized).map((feed) => feed.status),
    ...Object.values(feeds.leased).map((feed) => feed.status),
  ]);
  if (
    clientStatuses.some((status) => status === "disconnected" || status === "degraded") ||
    feedStatuses.some((status) => status === "degraded")
  ) {
    return "degraded";
  }
  if (
    clientStatuses.some((status) => status === "starting") ||
    feedStatuses.some((status) => status === "starting" || status === "stopping")
  ) {
    return "starting";
  }
  return "ready";
};

const mergeRuntimeStatus = <Topics extends ViewServerRuntimeTopicDefinitions>(
  health: ViewServerHealth<Topics>,
  grpc: GrpcHealthSnapshot<Topics>,
): ViewServerHealth<Topics>["status"] => {
  if (health.status === "stopping" || health.status === "degraded") {
    return health.status;
  }
  const grpcStatus = grpcRuntimeStatus(grpc);
  if (grpcStatus === "degraded") {
    return "degraded";
  }
  if (health.status === "starting" || grpcStatus === "starting") {
    return "starting";
  }
  return "ready";
};

const createTopicFeedsHealth = <Topic extends string>(): GrpcTopicFeedsHealth<Topic> => ({
  materialized: Object.create(null),
  leased: Object.create(null),
});

export const makeViewServerGrpcHealthLedger = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(input: {
  readonly clients: Readonly<Record<string, string>>;
  readonly feeds: Readonly<
    Record<
      string,
      {
        readonly lifecycle: GrpcFeedHealth["lifecycle"];
        readonly topic: Extract<keyof Topics, string>;
        readonly client: string;
      }
    >
  >;
}): ViewServerGrpcHealthLedger<Topics> => {
  const clients = new Map<string, GrpcClientLedger>();
  const feeds = new Map<string, GrpcFeedLedger<Extract<keyof Topics, string>>>();
  const materializedFeedsByClient = new Map<string, number>();
  for (const feed of Object.values(input.feeds)) {
    if (feed.lifecycle === "materialized") {
      materializedFeedsByClient.set(
        feed.client,
        (materializedFeedsByClient.get(feed.client) ?? 0) + 1,
      );
    }
  }

  for (const [clientName, client] of Object.entries(input.clients)) {
    clients.set(clientName, {
      status: materializedFeedsByClient.has(clientName) ? "starting" : "connected",
      baseUrl: client,
      activeFeeds: 0,
      lastConnectedAt: null,
      lastError: null,
    });
  }

  for (const [feedName, feed] of Object.entries(input.feeds)) {
    if (feed.lifecycle !== "materialized") {
      continue;
    }
    feeds.set(feedName, {
      status: "starting",
      lifecycle: feed.lifecycle,
      feedName,
      feedKey: `${feed.topic}/${feedName}/${feed.lifecycle}`,
      topic: feed.topic,
      clientName: feed.client,
      subscriberCount: 0,
      rowCount: 0,
      messagesPerSecond: 0,
      rowsPerSecond: 0,
      decodeFailuresPerSecond: 0,
      mappingFailuresPerSecond: 0,
      publishFailuresPerSecond: 0,
      reconnects: 0,
      lastMessageAt: null,
      lastError: null,
      rateBuckets: initialRateBuckets(),
    });
  }

  const refreshClientFromFeeds = (clientName: string) => {
    const client = clients.get(clientName);
    if (client === undefined) {
      return;
    }
    const feedsForClient = Array.from(feeds.values()).filter(
      (feed) => feed.clientName === clientName,
    );
    client.activeFeeds = feedsForClient.filter((feed) => feed.status === "ready").length;
    const degradedFeed = feedsForClient.find((feed) => feed.status === "degraded");
    if (degradedFeed !== undefined) {
      client.status = "degraded";
      client.lastError = degradedFeed.lastError;
      return;
    }
    const pendingFeed = feedsForClient.find(
      (feed) => feed.status === "starting" || feed.status === "stopping",
    );
    if (pendingFeed !== undefined) {
      client.status = "starting";
      client.lastError = null;
      return;
    }
    client.status = "connected";
    client.lastError = null;
  };

  const refreshClientsFromFeeds = () => {
    for (const clientName of clients.keys()) {
      refreshClientFromFeeds(clientName);
    }
    for (const feed of feeds.values()) {
      refreshClientFromFeeds(feed.clientName);
    }
  };

  const syncFeedRowCounts = (health: ViewServerHealth<Topics>) => {
    for (const feed of feeds.values()) {
      if (feed.lifecycle === "leased") {
        continue;
      }
      const topicHealth = health.engine.topics[feed.topic];
      feed.rowCount = topicHealth.rowCount;
    }
  };

  const snapshot = (
    health: ViewServerHealth<Topics>,
    nowMillis: number,
  ): GrpcHealthSnapshot<Topics> => {
    syncFeedRowCounts(health);
    refreshClientsFromFeeds();
    const clientHealth: Record<string, GrpcClientHealth> = Object.create(null);
    const feedHealth: GrpcHealthSnapshot<Topics>["feeds"] = Object.create(null);
    for (const [clientName, client] of clients) {
      clientHealth[clientName] = copyClientHealth(client);
    }
    for (const [feedName, feed] of feeds) {
      resetIdleWindow(feed, nowMillis);
      const topicFeeds = feedHealth[feed.topic] ?? createTopicFeedsHealth();
      feedHealth[feed.topic] = topicFeeds;
      if (feed.lifecycle === "materialized") {
        topicFeeds.materialized[feedName] = copyFeedHealth(feed);
        continue;
      }
      topicFeeds.leased[feedName] = copyFeedHealth(feed);
    }
    return {
      clients: clientHealth,
      feeds: feedHealth,
    };
  };

  return {
    healthOverlay: (health, nowMillis) => {
      const grpc = snapshot(health, nowMillis);
      return {
        ...health,
        status: mergeRuntimeStatus(health, grpc),
        grpc,
      };
    },
    clientConnected: (clientName, nowMillis) =>
      Effect.sync(() => {
        const client = clients.get(clientName);
        if (client !== undefined) {
          client.lastConnectedAt = client.lastConnectedAt ?? nowMillis;
          refreshClientFromFeeds(clientName);
        }
      }),
    clientDegraded: (clientName, message) =>
      Effect.sync(() => {
        const client = clients.get(clientName);
        if (client !== undefined) {
          client.status = "degraded";
          client.lastError = message;
        }
      }),
    feedReady: (feedName) =>
      Effect.sync(() => {
        const feed = feeds.get(feedName);
        if (feed !== undefined) {
          feed.status = "ready";
          feed.lastError = null;
          refreshClientFromFeeds(feed.clientName);
        }
      }),
    feedStopping: (feedName) =>
      Effect.sync(() => {
        const feed = feeds.get(feedName);
        if (feed !== undefined) {
          feed.status = "stopping";
          refreshClientFromFeeds(feed.clientName);
        }
      }),
    feedDegraded: (feedName, message) =>
      Effect.sync(() => {
        const feed = feeds.get(feedName);
        if (feed !== undefined) {
          feed.status = "degraded";
          feed.lastError = message;
          refreshClientFromFeeds(feed.clientName);
        }
      }),
    leasedFeedStarting: (input) =>
      Effect.sync(() => {
        feeds.set(input.feedKey, {
          status: "starting",
          lifecycle: "leased",
          feedName: input.feedName,
          feedKey: input.feedKey,
          topic: input.topic,
          clientName: input.clientName,
          subscriberCount: 0,
          rowCount: 0,
          messagesPerSecond: 0,
          rowsPerSecond: 0,
          decodeFailuresPerSecond: 0,
          mappingFailuresPerSecond: 0,
          publishFailuresPerSecond: 0,
          reconnects: 0,
          lastMessageAt: null,
          lastError: null,
          rateBuckets: initialRateBuckets(),
        });
        refreshClientFromFeeds(input.clientName);
      }),
    leasedFeedRemoved: (feedKey) =>
      Effect.sync(() => {
        const removedFeed = feeds.get(feedKey);
        feeds.delete(feedKey);
        if (removedFeed === undefined) {
          return;
        }
        refreshClientFromFeeds(removedFeed.clientName);
      }),
    subscriberAdded: (feedKey) =>
      Effect.sync(() => {
        const feed = feeds.get(feedKey);
        if (feed !== undefined) {
          feed.subscriberCount += 1;
        }
      }),
    subscriberRemoved: (feedKey) =>
      Effect.sync(() => {
        const feed = feeds.get(feedKey);
        if (feed !== undefined && feed.subscriberCount > 0) {
          feed.subscriberCount -= 1;
        }
      }),
    rowsPublished: (feedName, input) =>
      Effect.sync(() => {
        const feed = feeds.get(feedName);
        if (feed !== undefined) {
          if (input.rowCount !== undefined) {
            feed.rowCount = input.rowCount;
          }
          incrementWindow(feed, input.nowMillis, {
            messages: input.messages,
            rows: input.rows,
            decodeFailed: 0,
            mappingFailed: 0,
            publishFailed: 0,
          });
          feed.lastMessageAt = input.nowMillis;
          if (input.messages > 0 || input.rows > 0) {
            feed.lastError = null;
          }
        }
      }),
    mappingFailed: (feedName, input) =>
      Effect.sync(() => {
        const feed = feeds.get(feedName);
        if (feed !== undefined) {
          feed.status = "degraded";
          incrementWindow(feed, input.nowMillis, {
            messages: 1,
            rows: 0,
            decodeFailed: 0,
            mappingFailed: 1,
            publishFailed: 0,
          });
          feed.lastMessageAt = input.nowMillis;
          feed.lastError = input.message;
        }
      }),
    publishFailed: (feedName, input) =>
      Effect.sync(() => {
        const feed = feeds.get(feedName);
        if (feed !== undefined) {
          feed.status = "degraded";
          incrementWindow(feed, input.nowMillis, {
            messages: 1,
            rows: 0,
            decodeFailed: 0,
            mappingFailed: 0,
            publishFailed: 1,
          });
          feed.lastMessageAt = input.nowMillis;
          feed.lastError = input.message;
        }
      }),
  };
};
