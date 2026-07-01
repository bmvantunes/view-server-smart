import {
  isKafkaRuntimeTopicSourceDefinition,
  isKafkaTopicSourceDefinition,
  makeKafkaRuntimeTopicSources,
  type KafkaRuntimeTopicSourceDefinition,
  type RuntimeRegions,
} from "./kafka-contract";
import type { RowSchema } from "./topic-contract";

type KafkaSourceTopicRegistry = Record<
  string,
  {
    readonly schema: RowSchema;
    readonly key: string;
    readonly kafkaSource?: object | undefined;
  }
>;

export const makeKafkaRuntimeTopicsForConfig = <
  const Topics extends KafkaSourceTopicRegistry,
  const Regions extends RuntimeRegions,
>(config: {
  readonly topics: Topics;
}): ReadonlyArray<
  KafkaRuntimeTopicSourceDefinition<Topics, Regions, Extract<keyof Topics, string>>
> => makeKafkaRuntimeTopicSources<Topics, Regions>(config.topics);

export { isKafkaRuntimeTopicSourceDefinition, isKafkaTopicSourceDefinition };
export type {
  KafkaRuntimeTopicSourceDefinition,
  KafkaRuntimeSourceTopicDefinition,
} from "./kafka-contract";
