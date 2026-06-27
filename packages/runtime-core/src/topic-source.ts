import type { DecodableTopicDefinitions } from "@view-server/column-live-view-engine";
import type { ViewServerConfig } from "@view-server/config";

export const grpcLeasedSourceTopics = <const Topics extends DecodableTopicDefinitions>(
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
