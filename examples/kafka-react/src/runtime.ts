import { NodeRuntime } from "@effect/platform-node";
import { runViewServerRuntime } from "@view-server/runtime";
import { kafkaRegions, kafkaTopics, viewServer } from "./view-server.config";

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "view-server-example-kafka-react",
      startFrom: "latest",
      regions: kafkaRegions,
      topics: kafkaTopics,
    },
  }),
);
