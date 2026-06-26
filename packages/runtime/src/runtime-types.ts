import type { ViewServerLiveClient } from "@view-server/client";
import type { GroupedIncrementalAdmissionLimits } from "@view-server/runtime-core";
import type {
  KafkaRuntimeTopicDefinition,
  LiveQueryRow,
  LiveQueryResult,
  RawQuery,
  GroupedQuery,
  ExactLiveQueryInputForTopic,
  ExactPatch,
  RuntimeRegions,
  RowSchema,
  TopicRouteBy,
  TopicRow,
  TopicDefinitions,
  ViewServerHealth,
  ViewServerKafkaStartFrom,
  ViewServerRuntimeClient,
  ViewServerRuntimeError,
  GrpcFeedDefinition,
  GrpcRuntimeClients,
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
  readonly consumerGroupId: string;
  readonly startFrom?: ViewServerKafkaStartFrom;
  readonly regions: Regions;
  readonly topics: Record<string, KafkaRuntimeTopicDefinition<Topics, Regions>>;
};

export type ViewServerGrpcRuntimeOptions<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Clients extends GrpcRuntimeClients = GrpcRuntimeClients,
> = {
  readonly clients: Clients;
  readonly feeds: Record<string, GrpcFeedDefinition<Topics, Clients>>;
};

export type ViewServerRuntimeOptions<
  Topics extends ViewServerRuntimeTopicDefinitions = ViewServerRuntimeTopicDefinitions,
  Regions extends RuntimeRegions = RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
> = {
  readonly host?: string;
  readonly websocketPort?: number;
  readonly rpcPath?: RuntimeHttpPath;
  readonly healthPath?: RuntimeHttpPath;
  readonly kafka?: ViewServerKafkaRuntimeOptions<Topics, Regions>;
  readonly grpc?: ViewServerGrpcRuntimeOptions<Topics, GrpcClients>;
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

type RuntimeKafkaStartFromExactKeysConstraint<Options> = Options extends {
  readonly kafka: {
    readonly startFrom: infer CandidateStartFrom;
  };
}
  ? CandidateStartFrom extends object
    ? {
        readonly kafka: {
          readonly startFrom: CandidateStartFrom &
            RejectExtraKeys<CandidateStartFrom, Extract<ViewServerKafkaStartFrom, object>>;
        };
      }
    : unknown
  : unknown;

type RuntimeGrpcExactKeysConstraint<Options> = Options extends {
  readonly grpc: infer CandidateGrpc;
}
  ? {
      readonly grpc: CandidateGrpc &
        RejectExtraKeys<
          CandidateGrpc,
          ViewServerGrpcRuntimeOptions<ViewServerRuntimeTopicDefinitions>
        >;
    }
  : unknown;

type RuntimeGrpcFeedConstraint<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Options,
> = Options extends {
  readonly grpc: {
    readonly clients: infer Clients extends GrpcRuntimeClients;
    readonly feeds: infer Feeds extends Record<string, object>;
  };
}
  ? {
      readonly grpc: {
        readonly feeds: {
          readonly [FeedName in keyof Feeds]: Feeds[FeedName] extends GrpcFeedDefinition<
            Topics,
            Clients
          >
            ? Feeds[FeedName]
            : never;
        };
      };
    }
  : unknown;

type RuntimeGroupedIncrementalAdmissionLimitsExactKeysConstraint<Options> = Options extends {
  readonly groupedIncrementalAdmissionLimits: infer CandidateLimits;
}
  ? {
      readonly groupedIncrementalAdmissionLimits: CandidateLimits &
        RejectExtraKeys<CandidateLimits, Partial<GroupedIncrementalAdmissionLimits>>;
    }
  : unknown;

type RuntimeRegionsOf<Options> = Options extends {
  readonly kafka: {
    readonly regions: infer Regions extends RuntimeRegions;
  };
}
  ? Regions
  : RuntimeRegions;

type RuntimeGrpcClientsOf<Options> = Options extends {
  readonly grpc: {
    readonly clients: infer Clients extends GrpcRuntimeClients;
  };
}
  ? Clients
  : GrpcRuntimeClients;

export type ViewServerRuntimeOptionsInput<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Options extends object = ViewServerRuntimeOptions<Topics>,
> = Options &
  ViewServerRuntimeOptions<Topics, RuntimeRegionsOf<Options>, RuntimeGrpcClientsOf<Options>> &
  RejectExtraKeys<
    Options,
    ViewServerRuntimeOptions<Topics, RuntimeRegionsOf<Options>, RuntimeGrpcClientsOf<Options>>
  > &
  RuntimeKafkaExactKeysConstraint<Options> &
  RuntimeKafkaRegionConstraint<Topics, Options> &
  RuntimeKafkaStartFromExactKeysConstraint<Options> &
  RuntimeGrpcExactKeysConstraint<Options> &
  RuntimeGrpcFeedConstraint<Topics, Options> &
  RuntimeGroupedIncrementalAdmissionLimitsExactKeysConstraint<Options>;

type RuntimePublicMutationTopic<Topics extends object> = Extract<
  {
    readonly [Topic in keyof Topics]: [TopicRouteBy<Topics, Topic>] extends [never] ? Topic : never;
  }[keyof Topics],
  string
>;

type RuntimeLeasedTopic<Topics extends object> = Extract<
  {
    readonly [Topic in keyof Topics]: [TopicRouteBy<Topics, Topic>] extends [never] ? never : Topic;
  }[keyof Topics],
  string
>;

type RuntimePublicReset<Topics extends object> = [RuntimeLeasedTopic<Topics>] extends [never]
  ? {
      readonly reset: ViewServerRuntimeClient<Topics>["reset"];
    }
  : {
      readonly reset: (...args: never) => ReturnType<ViewServerRuntimeClient<Topics>["reset"]>;
    };

type RuntimePublicSnapshot<Topics extends object> = <
  Topic extends RuntimePublicMutationTopic<Topics>,
  const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
>(
  topic: Topic,
  query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
) => Effect.Effect<
  LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
  ViewServerRuntimeError
>;

type ViewServerPublicRuntimeClient<Topics extends object> = Omit<
  ViewServerRuntimeClient<Topics>,
  "delete" | "patch" | "publish" | "publishMany" | "reset" | "snapshot"
> & {
  readonly publish: <Topic extends RuntimePublicMutationTopic<Topics>>(
    topic: Topic,
    row: TopicRow<Topics, Topic>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly publishMany: <Topic extends RuntimePublicMutationTopic<Topics>>(
    topic: Topic,
    rows: ReadonlyArray<TopicRow<Topics, Topic>>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly patch: <Topic extends RuntimePublicMutationTopic<Topics>, const Patch>(
    topic: Topic,
    key: string,
    patch: Patch & Partial<TopicRow<Topics, Topic>> & ExactPatch<TopicRow<Topics, Topic>, Patch>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly delete: <Topic extends RuntimePublicMutationTopic<Topics>>(
    topic: Topic,
    key: string,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly snapshot: RuntimePublicSnapshot<Topics>;
} & RuntimePublicReset<Topics>;

export type ViewServerRuntime<Topics extends ViewServerRuntimeTopicDefinitions> = {
  readonly url: string;
  readonly healthUrl: string;
  readonly client: ViewServerPublicRuntimeClient<Topics>;
  readonly liveClient: ViewServerLiveClient<Topics>;
  readonly health: () => Effect.Effect<ViewServerHealth<Topics>, ViewServerRuntimeError>;
  readonly close: Effect.Effect<void>;
};
