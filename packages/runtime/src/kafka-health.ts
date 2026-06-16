import type {
  KafkaRegionHealth,
  KafkaStartFromHealth,
  KafkaTopicHealth,
  KafkaTopicRegionHealth,
  ViewServerHealth,
} from "@view-server/config";
import { Effect } from "effect";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

type KafkaHealthSnapshot<Topics extends ViewServerRuntimeTopicDefinitions> = NonNullable<
  ViewServerHealth<Topics>["kafka"]
>;

type KafkaRegionLedger = {
  status: KafkaRegionHealth["status"];
  brokers: string;
  lastConnectedAt: number | null;
  lastError: string | null;
};

type KafkaTopicRegionLedger = {
  connected: boolean;
  assignedPartitions: number;
  messagesPerSecond: number;
  bytesPerSecond: number;
  decodedMessagesPerSecond: number;
  decodeFailuresPerSecond: number;
  mappingFailuresPerSecond: number;
  publishFailuresPerSecond: number;
  commitFailuresPerSecond: number;
  processingFailuresPerSecond: number;
  lastMessageAt: number | null;
  lastCommitAt: number | null;
  consumerLagMessages: bigint | null;
  lagSampledAt: number | null;
  committedOffset: string | null;
  lastError: string | null;
  regionLastError: string | null;
  rateBuckets: Array<KafkaRateBucket | undefined>;
};

type KafkaRateBucket = {
  occurredAt: number;
  messages: number;
  bytes: number;
  decoded: number;
  failed: number;
  mappingFailed: number;
  publishFailed: number;
  commitFailed: number;
  processingFailed: number;
};

const kafkaRateWindowMillis = 1_000;
const maxKafkaRateBuckets = kafkaRateWindowMillis + 1;

type KafkaTopicLedger = {
  status: KafkaTopicHealth["status"];
  sourceTopic: string;
  viewServerTopic: string;
  regions: Map<string, KafkaTopicRegionLedger>;
};

export type ViewServerKafkaHealthLedger<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly healthOverlay: (
    health: ViewServerHealth<Topics>,
    nowMillis: number,
  ) => ViewServerHealth<Topics>;
  readonly regionConnected: (region: string, nowMillis: number) => Effect.Effect<void>;
  readonly regionDisconnected: (
    region: string,
    message: string,
    options?: {
      readonly preserveTopicErrors?: boolean;
    },
  ) => Effect.Effect<void>;
  readonly regionDegraded: (region: string, message: string) => Effect.Effect<void>;
  readonly regionRecovered: (region: string, nowMillis: number) => Effect.Effect<void>;
  readonly topicConnected: (
    sourceTopic: string,
    region: string,
    assignedPartitions: number,
    nowMillis: number,
  ) => Effect.Effect<void>;
  readonly topicLagSampled: (
    sourceTopic: string,
    region: string,
    input: {
      readonly consumerLagMessages: bigint | null;
      readonly nowMillis: number;
    },
  ) => Effect.Effect<void>;
  readonly messageDecoded: (
    sourceTopic: string,
    region: string,
    input: {
      readonly bytes: number;
      readonly committedOffset: string;
      readonly nowMillis: number;
    },
  ) => Effect.Effect<void>;
  readonly decodeFailed: (
    sourceTopic: string,
    region: string,
    input: {
      readonly bytes: number;
      readonly message: string;
      readonly nowMillis: number;
    },
  ) => Effect.Effect<void>;
  readonly mappingFailed: (
    sourceTopic: string,
    region: string,
    input: {
      readonly bytes: number;
      readonly message: string;
      readonly nowMillis: number;
    },
  ) => Effect.Effect<void>;
  readonly messagePublishFailed: (
    sourceTopic: string,
    region: string,
    input: {
      readonly bytes: number;
      readonly message: string;
      readonly nowMillis: number;
    },
  ) => Effect.Effect<void>;
  readonly messageCommitFailed: (
    sourceTopic: string,
    region: string,
    input: {
      readonly bytes: number;
      readonly message: string;
      readonly nowMillis: number;
    },
  ) => Effect.Effect<void>;
};

const initialTopicRegionLedger = (): KafkaTopicRegionLedger => ({
  connected: false,
  assignedPartitions: 0,
  messagesPerSecond: 0,
  bytesPerSecond: 0,
  decodedMessagesPerSecond: 0,
  decodeFailuresPerSecond: 0,
  mappingFailuresPerSecond: 0,
  publishFailuresPerSecond: 0,
  commitFailuresPerSecond: 0,
  processingFailuresPerSecond: 0,
  lastMessageAt: null,
  lastCommitAt: null,
  consumerLagMessages: null,
  lagSampledAt: null,
  committedOffset: null,
  lastError: null,
  regionLastError: null,
  rateBuckets: Array.from({ length: maxKafkaRateBuckets }, () => undefined),
});

const copyTopicRegionHealth = (region: KafkaTopicRegionLedger): KafkaTopicRegionHealth => ({
  connected: region.connected,
  assignedPartitions: region.assignedPartitions,
  messagesPerSecond: region.messagesPerSecond,
  bytesPerSecond: region.bytesPerSecond,
  decodedMessagesPerSecond: region.decodedMessagesPerSecond,
  decodeFailuresPerSecond: region.decodeFailuresPerSecond,
  mappingFailuresPerSecond: region.mappingFailuresPerSecond,
  publishFailuresPerSecond: region.publishFailuresPerSecond,
  commitFailuresPerSecond: region.commitFailuresPerSecond,
  processingFailuresPerSecond: region.processingFailuresPerSecond,
  lastMessageAt: region.lastMessageAt,
  lastCommitAt: region.lastCommitAt,
  consumerLagMessages: region.consumerLagMessages,
  lagSampledAt: region.lagSampledAt,
  committedOffset: region.committedOffset,
  lastError: region.lastError ?? region.regionLastError,
});

const incrementWindow = (
  region: KafkaTopicRegionLedger,
  nowMillis: number,
  counters: {
    readonly messages: number;
    readonly bytes: number;
    readonly decoded: number;
    readonly failed: number;
    readonly mappingFailed: number;
    readonly publishFailed: number;
    readonly commitFailed: number;
    readonly processingFailed: number;
  },
) => {
  const occurredAt = Math.trunc(nowMillis);
  const bucketIndex = Math.abs(occurredAt % maxKafkaRateBuckets);
  const existingBucket = region.rateBuckets[bucketIndex];
  if (existingBucket !== undefined && existingBucket.occurredAt === occurredAt) {
    existingBucket.messages += counters.messages;
    existingBucket.bytes += counters.bytes;
    existingBucket.decoded += counters.decoded;
    existingBucket.failed += counters.failed;
    existingBucket.mappingFailed += counters.mappingFailed;
    existingBucket.publishFailed += counters.publishFailed;
    existingBucket.commitFailed += counters.commitFailed;
    existingBucket.processingFailed += counters.processingFailed;
    return;
  }
  region.rateBuckets[bucketIndex] = {
    occurredAt,
    messages: counters.messages,
    bytes: counters.bytes,
    decoded: counters.decoded,
    failed: counters.failed,
    mappingFailed: counters.mappingFailed,
    publishFailed: counters.publishFailed,
    commitFailed: counters.commitFailed,
    processingFailed: counters.processingFailed,
  };
};

const resetIdleWindow = (region: KafkaTopicRegionLedger, nowMillis: number) => {
  const occurredBefore = Math.trunc(nowMillis) - kafkaRateWindowMillis;
  const occurredAfter = Math.trunc(nowMillis);
  let messagesPerSecond = 0;
  let bytesPerSecond = 0;
  let decodedMessagesPerSecond = 0;
  let decodeFailuresPerSecond = 0;
  let mappingFailuresPerSecond = 0;
  let publishFailuresPerSecond = 0;
  let commitFailuresPerSecond = 0;
  let processingFailuresPerSecond = 0;
  for (const bucket of region.rateBuckets) {
    if (
      bucket === undefined ||
      bucket.occurredAt < occurredBefore ||
      bucket.occurredAt > occurredAfter
    ) {
      continue;
    }
    messagesPerSecond += bucket.messages;
    bytesPerSecond += bucket.bytes;
    decodedMessagesPerSecond += bucket.decoded;
    decodeFailuresPerSecond += bucket.failed;
    mappingFailuresPerSecond += bucket.mappingFailed;
    publishFailuresPerSecond += bucket.publishFailed;
    commitFailuresPerSecond += bucket.commitFailed;
    processingFailuresPerSecond += bucket.processingFailed;
  }
  region.messagesPerSecond = messagesPerSecond;
  region.bytesPerSecond = bytesPerSecond;
  region.decodedMessagesPerSecond = decodedMessagesPerSecond;
  region.decodeFailuresPerSecond = decodeFailuresPerSecond;
  region.mappingFailuresPerSecond = mappingFailuresPerSecond;
  region.publishFailuresPerSecond = publishFailuresPerSecond;
  region.commitFailuresPerSecond = commitFailuresPerSecond;
  region.processingFailuresPerSecond = processingFailuresPerSecond;
};

const copyRegionHealth = (region: KafkaRegionLedger): KafkaRegionHealth => ({
  status: region.status,
  brokers: region.brokers,
  lastConnectedAt: region.lastConnectedAt,
  lastError: region.lastError,
});

const getTopicRegion = (
  topics: Map<string, KafkaTopicLedger>,
  sourceTopic: string,
  region: string,
): KafkaTopicRegionLedger | undefined => topics.get(sourceTopic)?.regions.get(region);

const refreshTopicStatus = (topic: KafkaTopicLedger) => {
  const regions = [...topic.regions.values()];
  if (regions.some((region) => region.lastError !== null || region.regionLastError !== null)) {
    topic.status = "degraded";
    return;
  }
  if (regions.every((region) => region.connected)) {
    topic.status = "ready";
    return;
  }
  topic.status = "starting";
};

const kafkaRuntimeStatus = <Topics extends ViewServerRuntimeTopicDefinitions>(
  snapshot: KafkaHealthSnapshot<Topics>,
): ViewServerHealth<Topics>["status"] => {
  const regionStatuses = Object.values(snapshot.regions).map((region) => region.status);
  const topicStatuses = Object.values(snapshot.topics).map((topic) => topic.status);
  if (
    regionStatuses.some((status) => status === "disconnected" || status === "degraded") ||
    topicStatuses.some((status) => status === "degraded" || status === "stalled")
  ) {
    return "degraded";
  }
  if (
    regionStatuses.some((status) => status === "starting") ||
    topicStatuses.some((status) => status === "starting")
  ) {
    return "starting";
  }
  return "ready";
};

const mergeRuntimeStatus = <Topics extends ViewServerRuntimeTopicDefinitions>(
  health: ViewServerHealth<Topics>,
  kafka: KafkaHealthSnapshot<Topics>,
): ViewServerHealth<Topics>["status"] => {
  if (health.status === "stopping" || health.status === "degraded") {
    return health.status;
  }
  const kafkaStatus = kafkaRuntimeStatus(kafka);
  if (kafkaStatus === "degraded") {
    return "degraded";
  }
  if (health.status === "starting" || kafkaStatus === "starting") {
    return "starting";
  }
  return "ready";
};

export const makeViewServerKafkaHealthLedger = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>(input: {
  readonly startFrom: KafkaStartFromHealth;
  readonly regions: Readonly<Record<string, string>>;
  readonly topics: Readonly<
    Record<
      string,
      {
        readonly viewServerTopic: Extract<keyof Topics, string>;
        readonly regions: ReadonlyArray<string>;
      }
    >
  >;
}): ViewServerKafkaHealthLedger<Topics> => {
  const regions = new Map<string, KafkaRegionLedger>();
  const topics = new Map<string, KafkaTopicLedger>();
  const activeRegions = new Set<string>();

  for (const topic of Object.values(input.topics)) {
    for (const region of topic.regions) {
      activeRegions.add(region);
    }
  }

  for (const [region, brokers] of Object.entries(input.regions)) {
    if (activeRegions.has(region)) {
      regions.set(region, {
        status: "starting",
        brokers,
        lastConnectedAt: null,
        lastError: null,
      });
    }
  }

  for (const [sourceTopic, topic] of Object.entries(input.topics)) {
    const topicRegions = new Map<string, KafkaTopicRegionLedger>();
    for (const region of topic.regions) {
      topicRegions.set(region, initialTopicRegionLedger());
    }
    topics.set(sourceTopic, {
      status: "starting",
      sourceTopic,
      viewServerTopic: topic.viewServerTopic,
      regions: topicRegions,
    });
  }

  const snapshot = (nowMillis: number): KafkaHealthSnapshot<Topics> => {
    const regionHealth: Record<string, KafkaRegionHealth> = Object.create(null);
    const topicHealth: Record<string, KafkaTopicHealth> = Object.create(null);

    for (const [region, ledger] of regions) {
      regionHealth[region] = copyRegionHealth(ledger);
    }

    for (const [sourceTopic, topic] of topics) {
      const topicRegions: Record<string, KafkaTopicRegionHealth> = Object.create(null);
      for (const [region, ledger] of topic.regions) {
        resetIdleWindow(ledger, nowMillis);
        topicRegions[region] = copyTopicRegionHealth(ledger);
      }
      topicHealth[sourceTopic] = {
        status: topic.status,
        sourceTopic: topic.sourceTopic,
        viewServerTopic: topic.viewServerTopic,
        regions: topicRegions,
      };
    }

    return {
      startFrom: input.startFrom,
      regions: regionHealth,
      topics: topicHealth,
    };
  };

  return {
    healthOverlay: (health, nowMillis) => {
      const kafka = snapshot(nowMillis);
      return {
        ...health,
        status: mergeRuntimeStatus(health, kafka),
        kafka,
      };
    },
    regionConnected: (region, nowMillis) =>
      Effect.sync(() => {
        const ledger = regions.get(region);
        if (ledger !== undefined) {
          ledger.status = "connected";
          ledger.lastConnectedAt = nowMillis;
          ledger.lastError = null;
        }
      }),
    regionDisconnected: (region, message, options) =>
      Effect.sync(() => {
        const ledger = regions.get(region);
        if (ledger !== undefined) {
          ledger.status = "disconnected";
          ledger.lastError = message;
        }
        for (const topic of topics.values()) {
          const topicRegion = topic.regions.get(region);
          if (topicRegion !== undefined) {
            topicRegion.connected = false;
            topicRegion.assignedPartitions = 0;
            topicRegion.regionLastError = message;
            if (options?.preserveTopicErrors !== true) {
              topicRegion.lastError = null;
            }
            refreshTopicStatus(topic);
          }
        }
      }),
    regionDegraded: (region, message) =>
      Effect.sync(() => {
        const ledger = regions.get(region);
        if (ledger !== undefined) {
          ledger.status = "degraded";
          ledger.lastError = message;
        }
        for (const topic of topics.values()) {
          const topicRegion = topic.regions.get(region);
          if (topicRegion !== undefined) {
            topicRegion.regionLastError = message;
            refreshTopicStatus(topic);
          }
        }
      }),
    regionRecovered: (region, nowMillis) =>
      Effect.sync(() => {
        const ledger = regions.get(region);
        if (ledger !== undefined && ledger.status !== "disconnected") {
          ledger.status = "connected";
          ledger.lastConnectedAt = ledger.lastConnectedAt ?? nowMillis;
          ledger.lastError = null;
          for (const topic of topics.values()) {
            const topicRegion = topic.regions.get(region);
            if (topicRegion !== undefined) {
              topicRegion.regionLastError = null;
              refreshTopicStatus(topic);
            }
          }
        }
      }),
    topicConnected: (sourceTopic, region, assignedPartitions, _nowMillis) =>
      Effect.sync(() => {
        const ledger = getTopicRegion(topics, sourceTopic, region);
        const topic = topics.get(sourceTopic);
        if (ledger !== undefined && topic !== undefined) {
          ledger.connected = true;
          ledger.assignedPartitions = assignedPartitions;
          ledger.regionLastError = null;
          refreshTopicStatus(topic);
        }
      }),
    topicLagSampled: (sourceTopic, region, input) =>
      Effect.sync(() => {
        const ledger = getTopicRegion(topics, sourceTopic, region);
        if (ledger !== undefined) {
          ledger.consumerLagMessages = input.consumerLagMessages;
          ledger.lagSampledAt = input.nowMillis;
        }
      }),
    messageDecoded: (sourceTopic, region, input) =>
      Effect.sync(() => {
        const ledger = getTopicRegion(topics, sourceTopic, region);
        const topic = topics.get(sourceTopic);
        if (ledger !== undefined && topic !== undefined) {
          incrementWindow(ledger, input.nowMillis, {
            bytes: input.bytes,
            decoded: 1,
            failed: 0,
            mappingFailed: 0,
            publishFailed: 0,
            commitFailed: 0,
            messages: 1,
            processingFailed: 0,
          });
          ledger.lastMessageAt = input.nowMillis;
          ledger.lastCommitAt = input.nowMillis;
          ledger.committedOffset = input.committedOffset;
          ledger.lastError = null;
          refreshTopicStatus(topic);
        }
      }),
    decodeFailed: (sourceTopic, region, input) =>
      Effect.sync(() => {
        const ledger = getTopicRegion(topics, sourceTopic, region);
        const topic = topics.get(sourceTopic);
        if (ledger !== undefined && topic !== undefined) {
          topic.status = "degraded";
          incrementWindow(ledger, input.nowMillis, {
            bytes: input.bytes,
            decoded: 0,
            failed: 1,
            mappingFailed: 0,
            publishFailed: 0,
            commitFailed: 0,
            messages: 1,
            processingFailed: 0,
          });
          ledger.lastMessageAt = input.nowMillis;
          ledger.lastError = input.message;
        }
      }),
    mappingFailed: (sourceTopic, region, input) =>
      Effect.sync(() => {
        const ledger = getTopicRegion(topics, sourceTopic, region);
        const topic = topics.get(sourceTopic);
        if (ledger !== undefined && topic !== undefined) {
          topic.status = "degraded";
          incrementWindow(ledger, input.nowMillis, {
            bytes: input.bytes,
            decoded: 0,
            failed: 0,
            mappingFailed: 1,
            publishFailed: 0,
            commitFailed: 0,
            messages: 1,
            processingFailed: 0,
          });
          ledger.lastMessageAt = input.nowMillis;
          ledger.lastError = input.message;
        }
      }),
    messagePublishFailed: (sourceTopic, region, input) =>
      Effect.sync(() => {
        const ledger = getTopicRegion(topics, sourceTopic, region);
        const topic = topics.get(sourceTopic);
        if (ledger !== undefined && topic !== undefined) {
          incrementWindow(ledger, input.nowMillis, {
            bytes: input.bytes,
            decoded: 0,
            failed: 0,
            mappingFailed: 0,
            publishFailed: 1,
            commitFailed: 0,
            messages: 1,
            processingFailed: 1,
          });
          ledger.lastMessageAt = input.nowMillis;
          ledger.lastError = input.message;
          refreshTopicStatus(topic);
        }
      }),
    messageCommitFailed: (sourceTopic, region, input) =>
      Effect.sync(() => {
        const ledger = getTopicRegion(topics, sourceTopic, region);
        const topic = topics.get(sourceTopic);
        if (ledger !== undefined && topic !== undefined) {
          incrementWindow(ledger, input.nowMillis, {
            bytes: input.bytes,
            decoded: 0,
            failed: 0,
            mappingFailed: 0,
            publishFailed: 0,
            commitFailed: 1,
            messages: 1,
            processingFailed: 1,
          });
          ledger.lastMessageAt = input.nowMillis;
          ledger.lastError = input.message;
          refreshTopicStatus(topic);
        }
      }),
  };
};
