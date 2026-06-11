import type {
  KafkaRegionHealth,
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
  processingFailuresPerSecond: number;
  lastMessageAt: number | null;
  lastCommitAt: number | null;
  consumerLagMessages: bigint | null;
  lagSampledAt: number | null;
  committedOffset: string | null;
  lastError: string | null;
  regionLastError: string | null;
  windowSecond: number | null;
};

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
      readonly consumerLagMessages: bigint;
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
  readonly messageProcessingFailed: (
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
  processingFailuresPerSecond: 0,
  lastMessageAt: null,
  lastCommitAt: null,
  consumerLagMessages: null,
  lagSampledAt: null,
  committedOffset: null,
  lastError: null,
  regionLastError: null,
  windowSecond: null,
});

const copyTopicRegionHealth = (region: KafkaTopicRegionLedger): KafkaTopicRegionHealth => ({
  connected: region.connected,
  assignedPartitions: region.assignedPartitions,
  messagesPerSecond: region.messagesPerSecond,
  bytesPerSecond: region.bytesPerSecond,
  decodedMessagesPerSecond: region.decodedMessagesPerSecond,
  decodeFailuresPerSecond: region.decodeFailuresPerSecond,
  mappingFailuresPerSecond: region.mappingFailuresPerSecond,
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
    readonly processingFailed: number;
  },
) => {
  const nextSecond = Math.floor(nowMillis / 1000);
  if (region.windowSecond !== nextSecond) {
    region.windowSecond = nextSecond;
    region.messagesPerSecond = 0;
    region.bytesPerSecond = 0;
    region.decodedMessagesPerSecond = 0;
    region.decodeFailuresPerSecond = 0;
    region.mappingFailuresPerSecond = 0;
    region.processingFailuresPerSecond = 0;
  }
  region.messagesPerSecond += counters.messages;
  region.bytesPerSecond += counters.bytes;
  region.decodedMessagesPerSecond += counters.decoded;
  region.decodeFailuresPerSecond += counters.failed;
  region.mappingFailuresPerSecond += counters.mappingFailed;
  region.processingFailuresPerSecond += counters.processingFailed;
};

const resetIdleWindow = (region: KafkaTopicRegionLedger, nowMillis: number) => {
  const nextSecond = Math.floor(nowMillis / 1000);
  if (region.windowSecond !== null && region.windowSecond !== nextSecond) {
    region.windowSecond = nextSecond;
    region.messagesPerSecond = 0;
    region.bytesPerSecond = 0;
    region.decodedMessagesPerSecond = 0;
    region.decodeFailuresPerSecond = 0;
    region.mappingFailuresPerSecond = 0;
    region.processingFailuresPerSecond = 0;
  }
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
          ledger.lastError = null;
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
            messages: 1,
            processingFailed: 0,
          });
          ledger.lastMessageAt = input.nowMillis;
          ledger.lastError = input.message;
        }
      }),
    messageProcessingFailed: (sourceTopic, region, input) =>
      Effect.sync(() => {
        const ledger = getTopicRegion(topics, sourceTopic, region);
        const topic = topics.get(sourceTopic);
        if (ledger !== undefined && topic !== undefined) {
          incrementWindow(ledger, input.nowMillis, {
            bytes: input.bytes,
            decoded: 0,
            failed: 0,
            mappingFailed: 0,
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
