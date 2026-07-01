import type { ViewServerLiveClient } from "@effect-view-server/client";
import type { GroupedIncrementalAdmissionLimits } from "@effect-view-server/runtime-core";
import type { ViewServerAuth } from "@effect-view-server/server";
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
} from "@effect-view-server/config";
import type { Duration, Effect, Schema } from "effect";

export type ViewServerRuntimeTopicDefinitions = TopicDefinitions &
  Record<
    string,
    {
      readonly schema: RowSchema & Schema.Codec<object, unknown, never, unknown>;
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
  readonly regions?: Regions;
  readonly topics?: Record<string, KafkaRuntimeTopicDefinition<Topics, Regions>>;
};

export type ViewServerGrpcRuntimeOptions<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Clients extends GrpcRuntimeClients = GrpcRuntimeClients,
> = {
  readonly clients?: Clients;
  readonly feeds: Record<string, GrpcFeedDefinition<Topics, Clients>>;
  readonly materializedReconnect?: {
    readonly maxReconnects?: number;
    readonly delay?: Duration.Input;
  };
};

export type ViewServerRuntimeOptions<
  Topics extends ViewServerRuntimeTopicDefinitions = ViewServerRuntimeTopicDefinitions,
  Regions extends RuntimeRegions = RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
> = {
  readonly host?: string;
  readonly websocketPort?: number;
  readonly tcpPublishHost?: string;
  readonly tcpPublishMaxConnections?: number;
  readonly tcpPublishPort?: number;
  readonly rpcPath?: RuntimeHttpPath;
  readonly healthPath?: RuntimeHttpPath;
  readonly metricsPath?: RuntimeHttpPath;
  readonly auth?: ViewServerAuth;
  readonly kafka?: ViewServerKafkaRuntimeOptions<Topics, Regions>;
  readonly grpc?: ViewServerGrpcRuntimeOptions<Topics, GrpcClients>;
  readonly groupedIncrementalAdmissionLimits?: Partial<GroupedIncrementalAdmissionLimits>;
  readonly subscriptionQueueCapacity?: number;
};

type RejectExtraKeys<Candidate, Shape> = {
  readonly [Key in Exclude<keyof Candidate, keyof Shape>]: never;
};

type IsUnion<Value, Candidate = Value> = Value extends unknown
  ? [Candidate] extends [Value]
    ? false
    : true
  : false;

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
  ConfigRegions extends RuntimeRegions,
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
  : Options extends {
        readonly kafka: {
          readonly topics: infer KafkaTopics extends Record<string, object>;
        };
      }
    ? {
        readonly kafka: {
          readonly topics: {
            readonly [SourceTopic in keyof KafkaTopics]: KafkaTopics[SourceTopic] extends KafkaRuntimeTopicDefinition<
              Topics,
              ConfigRegions
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
  ConfigClients extends GrpcRuntimeClients,
  Options,
> = Options extends {
  readonly grpc: {
    readonly feeds: infer Feeds extends Record<string, object>;
  };
}
  ? {
      readonly grpc: {
        readonly feeds: {
          readonly [FeedName in keyof Feeds]: Feeds[FeedName] extends GrpcFeedDefinition<
            Topics,
            RuntimeGrpcClientsFor<ConfigClients, Options>
          >
            ? Feeds[FeedName]
            : never;
        };
      };
    }
  : unknown;

type RuntimeGrpcClientsConstraint<
  ConfigClients extends GrpcRuntimeClients,
  Options,
> = string extends keyof ConfigClients
  ? Options extends {
      readonly grpc: infer CandidateGrpc;
    }
    ? CandidateGrpc extends {
        readonly clients: GrpcRuntimeClients;
      }
      ? unknown
      : {
          readonly grpc: CandidateGrpc & {
            readonly clients: GrpcRuntimeClients;
          };
        }
    : unknown
  : unknown;

type RuntimeGrpcMaterializedReconnectExactKeysConstraint<Options> = Options extends {
  readonly grpc: {
    readonly materializedReconnect: infer CandidateReconnect;
  };
}
  ? {
      readonly grpc: {
        readonly materializedReconnect: CandidateReconnect &
          RejectExtraKeys<
            CandidateReconnect,
            NonNullable<
              ViewServerGrpcRuntimeOptions<ViewServerRuntimeTopicDefinitions>["materializedReconnect"]
            >
          >;
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

type TopicOwnedKafkaSourceTopic<Topics extends object> = Extract<
  {
    readonly [Topic in keyof Topics]: Topics[Topic] extends {
      readonly kafkaSource: object;
    }
      ? Topic
      : never;
  }[keyof Topics],
  string
>;

type TopicOwnedGrpcSourceTopic<Topics extends object> = Extract<
  {
    readonly [Topic in keyof Topics]: Topics[Topic] extends {
      readonly grpcSource: object;
    }
      ? Topic
      : Topics[Topic] extends {
            readonly source: { readonly kind: "grpc" };
          }
        ? Topic
        : never;
  }[keyof Topics],
  string
>;

type TopicOwnedSourceTopic<Topics extends object> =
  | TopicOwnedKafkaSourceTopic<Topics>
  | TopicOwnedGrpcSourceTopic<Topics>;

type TopicOwnedKafkaSourceRegion<Topics extends object> = Extract<
  {
    readonly [Topic in keyof Topics]: Topics[Topic] extends {
      readonly kafkaSource: {
        readonly regions: infer Regions extends ReadonlyArray<string>;
      };
    }
      ? Regions[number]
      : never;
  }[keyof Topics],
  string
>;

type RuntimeRegionsAreBroad<Regions extends RuntimeRegions> = string extends keyof Regions
  ? true
  : false;

type RuntimeKafkaExplicitTopicRegionsConstraint<
  ConfigRegions extends RuntimeRegions,
  Options,
> = Options extends {
  readonly kafka: {
    readonly topics: Record<string, object>;
  };
}
  ? Options extends {
      readonly kafka: {
        readonly regions: RuntimeRegions;
      };
    }
    ? unknown
    : RuntimeRegionsAreBroad<ConfigRegions> extends true
      ? {
          readonly kafka: {
            readonly regions: never;
          };
        }
      : unknown
  : unknown;

type RuntimeKafkaSourceRegionConstraint<
  Topics extends object,
  ConfigRegions extends RuntimeRegions,
  Options,
> = [TopicOwnedKafkaSourceRegion<Topics>] extends [never]
  ? unknown
  : Options extends {
        readonly kafka: {
          readonly regions: infer Regions extends RuntimeRegions;
        };
      }
    ? RuntimeRegionsAreBroad<Regions> extends true
      ? {
          readonly kafka: {
            readonly regions: never;
          };
        }
      : Exclude<TopicOwnedKafkaSourceRegion<Topics>, keyof Regions> extends never
        ? unknown
        : {
            readonly kafka: {
              readonly regions: never;
            };
          }
    : RuntimeRegionsAreBroad<ConfigRegions> extends true
      ? {
          readonly kafka: {
            readonly regions: never;
          };
        }
      : unknown;

type RuntimeKafkaSourceOwnershipConstraint<Topics extends object, Options> = [
  TopicOwnedKafkaSourceTopic<Topics>,
] extends [never]
  ? unknown
  : Options extends {
        readonly kafka: infer CandidateKafka;
      }
    ? {
        readonly kafka: CandidateKafka & {
          readonly consumerGroupId: string;
          readonly topics?: never;
        };
      }
    : {
        readonly kafka: never;
      };

type RuntimeGrpcSingleFeedTopic<Feed> = [Feed] extends [
  {
    readonly topic: infer Topic extends string;
  },
]
  ? string extends Topic
    ? never
    : IsUnion<Topic> extends true
      ? never
      : Topic
  : never;

type RuntimeGrpcFeedTopic<Feeds extends Record<string, object>> = Extract<
  {
    readonly [FeedName in keyof Feeds]: RuntimeGrpcSingleFeedTopic<Feeds[FeedName]>;
  }[keyof Feeds],
  string
>;

type RuntimeKafkaOptionOwnedTopic<Options> = Options extends {
  readonly kafka: {
    readonly topics: infer KafkaTopics extends Record<string, object>;
  };
}
  ? Extract<
      {
        readonly [SourceTopic in keyof KafkaTopics]: KafkaTopics[SourceTopic] extends {
          readonly viewServerTopic: infer Topic extends string;
        }
          ? string extends Topic
            ? never
            : Topic
          : never;
      }[keyof KafkaTopics],
      string
    >
  : never;

type RuntimeGrpcOptionOwnedTopic<Options> = Options extends {
  readonly grpc: {
    readonly feeds: infer Feeds extends Record<string, object>;
  };
}
  ? RuntimeGrpcFeedTopic<Feeds>
  : never;

type RuntimeSourceOwnedTopic<Topics extends object, Options> = Extract<
  | TopicOwnedSourceTopic<Topics>
  | RuntimeKafkaOptionOwnedTopic<Options>
  | RuntimeGrpcOptionOwnedTopic<Options>,
  Extract<keyof Topics, string>
>;

type RuntimeGrpcSourceOwnershipConstraint<Topics extends object, Options> = [
  TopicOwnedGrpcSourceTopic<Topics>,
] extends [never]
  ? unknown
  : Options extends {
        readonly grpc: {
          readonly feeds: infer Feeds extends Record<string, object>;
        };
      }
    ? string extends keyof Feeds
      ? {
          readonly grpc: {
            readonly feeds: never;
          };
        }
      : Exclude<TopicOwnedGrpcSourceTopic<Topics>, RuntimeGrpcFeedTopic<Feeds>> extends never
        ? unknown
        : {
            readonly grpc: {
              readonly feeds: never;
            };
          }
    : {
        readonly grpc: never;
      };

type RuntimeRegionsOf<Options, ConfigRegions extends RuntimeRegions> = Options extends {
  readonly kafka: {
    readonly regions: infer Regions extends RuntimeRegions;
  };
}
  ? Regions
  : ConfigRegions;

type RuntimeGrpcClientsOf<Options, ConfigClients extends GrpcRuntimeClients> = Options extends {
  readonly grpc: {
    readonly clients: infer Clients extends GrpcRuntimeClients;
  };
}
  ? Clients
  : ConfigClients;

type RuntimeGrpcClientsFor<ConfigClients extends GrpcRuntimeClients, Options> = Options extends {
  readonly grpc: {
    readonly clients: infer Clients extends GrpcRuntimeClients;
  };
}
  ? Clients
  : ConfigClients;

export type ViewServerRuntimeOptionsInput<
  Topics extends ViewServerRuntimeTopicDefinitions,
  ConfigRegions extends RuntimeRegions = RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
  Options extends object = ViewServerRuntimeOptions<Topics, ConfigRegions, GrpcClients>,
> = Options &
  ViewServerRuntimeOptions<
    Topics,
    RuntimeRegionsOf<Options, ConfigRegions>,
    RuntimeGrpcClientsOf<Options, GrpcClients>
  > &
  RejectExtraKeys<
    Options,
    ViewServerRuntimeOptions<
      Topics,
      RuntimeRegionsOf<Options, ConfigRegions>,
      RuntimeGrpcClientsOf<Options, GrpcClients>
    >
  > &
  RuntimeKafkaExactKeysConstraint<Options> &
  RuntimeKafkaRegionConstraint<Topics, ConfigRegions, Options> &
  RuntimeKafkaStartFromExactKeysConstraint<Options> &
  RuntimeGrpcExactKeysConstraint<Options> &
  RuntimeGrpcClientsConstraint<GrpcClients, Options> &
  RuntimeGrpcFeedConstraint<Topics, GrpcClients, Options> &
  RuntimeGrpcMaterializedReconnectExactKeysConstraint<Options> &
  RuntimeGroupedIncrementalAdmissionLimitsExactKeysConstraint<Options> &
  RuntimeKafkaExplicitTopicRegionsConstraint<ConfigRegions, Options> &
  RuntimeKafkaSourceOwnershipConstraint<Topics, Options> &
  RuntimeKafkaSourceRegionConstraint<Topics, ConfigRegions, Options> &
  RuntimeGrpcSourceOwnershipConstraint<Topics, Options>;

export type ViewServerRuntimeOptionsArgs<
  Topics extends ViewServerRuntimeTopicDefinitions,
  ConfigRegions extends RuntimeRegions = RuntimeRegions,
  GrpcClients extends GrpcRuntimeClients = GrpcRuntimeClients,
  Options extends object = ViewServerRuntimeOptions<Topics, ConfigRegions, GrpcClients>,
> = [TopicOwnedKafkaSourceTopic<Topics>] extends [never]
  ? [TopicOwnedSourceTopic<Topics>] extends [never]
    ? [options?: ViewServerRuntimeOptionsInput<Topics, ConfigRegions, GrpcClients, Options>]
    : [options: ViewServerRuntimeOptionsInput<Topics, ConfigRegions, GrpcClients, Options>]
  : [options: ViewServerRuntimeOptionsInput<Topics, ConfigRegions, GrpcClients, Options>];

type RuntimePublicMutationTopic<Topics extends object, SourceOwnedTopics extends string> = Extract<
  {
    readonly [Topic in keyof Topics]: Topic extends SourceOwnedTopics
      ? never
      : Topics[Topic] extends { readonly kafkaSource: object }
        ? never
        : Topics[Topic] extends { readonly grpcSource: object }
          ? never
          : Topics[Topic] extends { readonly source: object }
            ? never
            : [TopicRouteBy<Topics, Topic>] extends [never]
              ? Topic
              : never;
  }[keyof Topics],
  string
>;

type RuntimePublicSnapshotTopic<Topics extends object> = Extract<
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

type RuntimeSourceOwnedOrLeasedTopic<Topics extends object, SourceOwnedTopics extends string> =
  | SourceOwnedTopics
  | RuntimeLeasedTopic<Topics>;

type RuntimePublicReset<Topics extends object, SourceOwnedTopics extends string> = [
  RuntimeSourceOwnedOrLeasedTopic<Topics, SourceOwnedTopics>,
] extends [never]
  ? [Extract<keyof Topics, string>] extends [RuntimePublicMutationTopic<Topics, SourceOwnedTopics>]
    ? {
        readonly reset: ViewServerRuntimeClient<Topics>["reset"];
      }
    : {
        readonly reset: (...args: never) => ReturnType<ViewServerRuntimeClient<Topics>["reset"]>;
      }
  : {
      readonly reset: (...args: never) => ReturnType<ViewServerRuntimeClient<Topics>["reset"]>;
    };

type RuntimePublicSnapshot<Topics extends object> = <
  Topic extends RuntimePublicSnapshotTopic<Topics>,
  const Query extends RawQuery<TopicRow<Topics, Topic>> | GroupedQuery<TopicRow<Topics, Topic>>,
>(
  topic: Topic,
  query: ExactLiveQueryInputForTopic<Topics, Topic, Query>,
) => Effect.Effect<
  LiveQueryResult<LiveQueryRow<TopicRow<Topics, Topic>, Query>>,
  ViewServerRuntimeError
>;

type ViewServerPublicRuntimeClient<Topics extends object, SourceOwnedTopics extends string> = Omit<
  ViewServerRuntimeClient<Topics>,
  "delete" | "patch" | "publish" | "publishMany" | "reset" | "snapshot"
> & {
  readonly publish: <Topic extends RuntimePublicMutationTopic<Topics, SourceOwnedTopics>>(
    topic: Topic,
    row: TopicRow<Topics, Topic>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly publishMany: <Topic extends RuntimePublicMutationTopic<Topics, SourceOwnedTopics>>(
    topic: Topic,
    rows: ReadonlyArray<TopicRow<Topics, Topic>>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly patch: <
    Topic extends RuntimePublicMutationTopic<Topics, SourceOwnedTopics>,
    const Patch,
  >(
    topic: Topic,
    key: string,
    patch: Patch & Partial<TopicRow<Topics, Topic>> & ExactPatch<TopicRow<Topics, Topic>, Patch>,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly delete: <Topic extends RuntimePublicMutationTopic<Topics, SourceOwnedTopics>>(
    topic: Topic,
    key: string,
  ) => Effect.Effect<void, ViewServerRuntimeError>;
  readonly snapshot: RuntimePublicSnapshot<Topics>;
} & RuntimePublicReset<Topics, SourceOwnedTopics>;

export type ViewServerRuntime<
  Topics extends ViewServerRuntimeTopicDefinitions,
  Options extends object = object,
> = {
  readonly url: string;
  readonly healthUrl: string;
  readonly metricsUrl: string;
  readonly tcpPublishUrl?: string;
  readonly client: ViewServerPublicRuntimeClient<Topics, RuntimeSourceOwnedTopic<Topics, Options>>;
  readonly liveClient: ViewServerLiveClient<Topics>;
  readonly health: () => Effect.Effect<ViewServerHealth<Topics>, ViewServerRuntimeError>;
  readonly close: Effect.Effect<void>;
};
