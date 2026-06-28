import { createFileRoute } from "@tanstack/react-router";
import { useSyncExternalStore } from "react";
import { GrpcLeasedExampleApp, ViewServerProvider } from "../view-server.example";

export const Route = createFileRoute("/")({ component: Home });

const subscribeToBrowserReady = (notify: () => void) => {
  queueMicrotask(notify);
  return () => undefined;
};

const browserSnapshot = () => true;
const serverSnapshot = () => false;

function useBrowserReady() {
  return useSyncExternalStore(subscribeToBrowserReady, browserSnapshot, serverSnapshot);
}

function Home() {
  const isBrowserReady = useBrowserReady();

  return isBrowserReady ? (
    <ViewServerProvider url="ws://127.0.0.1:8080/rpc">
      <GrpcLeasedExampleApp />
    </ViewServerProvider>
  ) : (
    <main className="example-shell">
      <section className="panel" aria-label="leased grpc client placeholder">
        <h1>Leased gRPC React example</h1>
        <p>Live leased gRPC data connects in the browser.</p>
      </section>
    </main>
  );
}
