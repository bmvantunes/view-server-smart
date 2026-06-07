import type { ViewServerLiveClient } from "@view-server/client";
import type { GroupedIncrementalAdmissionLimits } from "@view-server/runtime-core";
import type {
  KafkaRuntimeTopicDefinition,
  RuntimeRegions,
  RowSchema,
  TopicDefinitions,
  ViewServerHealth,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
} from "@view-server/config";
import type { Effect, Schema } from "effect";

export type ViewServerRuntimeTopicDefinitions = TopicDefinitions &
  Record<
    string,
    {
      readonly schema: RowSchema & Schema.Decoder<object>;
      readonly key: string;
    }
  >;

type RuntimeHttpPath = `/${string}`;

export type ViewServerKafkaRuntimeOptions<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Regions extends RuntimeRegions = RuntimeRegions,
> = {
  readonly consumerGroupId?: string;
  readonly regions: Regions;
  readonly topics: Record<string, KafkaRuntimeTopicDefinition<Topics, Regions>>;
};

export type ViewServerRuntimeOptions<
  Topics extends ViewServerRuntimeTopicDefinitions = ViewServerRuntimeTopicDefinitions,
  Regions extends RuntimeRegions = RuntimeRegions,
> = {
  readonly host?: string;
  readonly websocketPort?: number;
  readonly rpcPath?: RuntimeHttpPath;
  readonly healthPath?: RuntimeHttpPath;
  readonly kafka?: ViewServerKafkaRuntimeOptions<Topics, Regions>;
  readonly groupedIncrementalAdmissionLimits?: Partial<GroupedIncrementalAdmissionLimits>;
  readonly subscriptionQueueCapacity?: number;
};

type RejectExtraKeys<Candidate, Shape> = {
  readonly [Key in Exclude<keyof Candidate, keyof Shape>]: never;
};

type RuntimeKafkaExactKeysConstraint<Options> = Options extends {
  readonly kafka: infer CandidateKafka;
}
  ? {
      readonly kafka: CandidateKafka &
        RejectExtraKeys<
          CandidateKafka,
          ViewServerKafkaRuntimeOptions<ViewServerRuntimeTopicDefinitions>
        >;
    }
  : unknown;

type RuntimeKafkaRegionConstraint<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Options,
> = Options extends {
  readonly kafka: {
    readonly regions: infer Regions extends RuntimeRegions;
    readonly topics: infer KafkaTopics extends Record<string, object>;
  };
}
  ? {
      readonly kafka: {
        readonly topics: {
          readonly [SourceTopic in keyof KafkaTopics]: KafkaTopics[SourceTopic] extends KafkaRuntimeTopicDefinition<
            Topics,
            Regions
          >
            ? KafkaTopics[SourceTopic]
            : never;
        };
      };
    }
  : unknown;

export type ViewServerRuntimeOptionsInput<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Options extends ViewServerRuntimeOptions<Topics> = ViewServerRuntimeOptions<Topics>,
> = Options &
  RejectExtraKeys<Options, ViewServerRuntimeOptions<Topics>> &
  RuntimeKafkaExactKeysConstraint<Options> &
  RuntimeKafkaRegionConstraint<Topics, Options>;

export type ViewServerRuntime<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly url: string;
  readonly healthUrl: string;
  readonly client: ViewServerRuntimeClient<Topics>;
  readonly liveClient: ViewServerLiveClient<Topics>;
  readonly health: () => Effect.Effect<ViewServerHealth<Topics>, ViewServerRuntimeError>;
  readonly close: Effect.Effect<void>;
};
