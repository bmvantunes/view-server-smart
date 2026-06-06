import type { RuntimeCoreTransportHealth } from "@view-server/runtime-core";
import { Effect } from "effect";
import type { ViewServerRuntimeTopicDefinitions } from "./runtime-types";

export const makeViewServerRuntimeTransportHealth = <
  const Topics extends ViewServerRuntimeTopicDefinitions,
>() => {
  let activeStreams = 0;

  const streamOpened = Effect.sync(() => {
    activeStreams += 1;
  });
  const streamClosed = Effect.sync(() => {
    activeStreams = Math.max(0, activeStreams - 1);
  });
  const transportHealth: RuntimeCoreTransportHealth<Topics> = (engineHealth) => ({
    activeClients: 0,
    activeStreams,
    activeSubscriptions: engineHealth.activeSubscriptions,
    messagesPerSecond: 0,
    bytesPerSecond: 0,
    queuedMessages: engineHealth.queuedEvents,
    queuedBytes: 0,
    droppedClients: 0,
    backpressureEvents: engineHealth.backpressureEvents,
    reconnects: 0,
    lastError: null,
  });

  return {
    transportHealth,
    streamOpened,
    streamClosed,
  };
};
