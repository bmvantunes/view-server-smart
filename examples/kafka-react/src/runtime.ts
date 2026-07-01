import { NodeRuntime } from "@effect/platform-node";
import { runViewServerRuntime } from "effect-view-server/runtime";
import { viewServer } from "./view-server.config";

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    websocketPort: 8080,
    kafka: {
      consumerGroupId: "view-server-example-kafka-react",
      startFrom: "latest",
    },
  }),
);
