import { createInMemoryViewServerReact } from "@view-server/react/testing";
import { viewServerReact } from "./view-server.config";

export const createInMemoryExampleViewServer = () => createInMemoryViewServerReact(viewServerReact);
