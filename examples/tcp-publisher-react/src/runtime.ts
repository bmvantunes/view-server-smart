import { NodeRuntime } from "@effect/platform-node";
import { runViewServerRuntime } from "@view-server/runtime";
import { viewServer } from "./view-server.config";

NodeRuntime.runMain(
  runViewServerRuntime(viewServer, {
    websocketPort: 8080,
    tcpPublishHost: "127.0.0.1",
    tcpPublishPort: 8081,
  }),
);
