import { useLiveQuery, useViewServerHealthSummary, ViewServerProvider } from "./view-server.config";
import { useState } from "react";
import { useSyncExternalStore } from "react";

const subscribeToBrowserReady = (notify: () => void) => {
  queueMicrotask(notify);
  return () => undefined;
};

const browserSnapshot = () => true;
const serverSnapshot = () => false;

function useBrowserReady() {
  return useSyncExternalStore(subscribeToBrowserReady, browserSnapshot, serverSnapshot);
}

export function SsrExampleApp() {
  return (
    <main className="example-shell">
      <header>
        <p className="eyebrow">SSR shell</p>
        <h1>TanStack Start shell with client-only live data</h1>
        <p>
          The page shell is safe to server-render. The View Server WebSocket provider only mounts in
          the browser.
        </p>
      </header>
      <ClientOnlyLivePanel />
    </main>
  );
}

function ClientOnlyLivePanel() {
  const isBrowserReady = useBrowserReady();
  const [isLivePanelEnabled, setLivePanelEnabled] = useState(false);

  if (!isBrowserReady) {
    return (
      <section className="panel" aria-label="ssr placeholder">
        <h2>Live data</h2>
        <p>Live queries hydrate in the browser.</p>
      </section>
    );
  }

  if (!isLivePanelEnabled) {
    return (
      <section className="panel" aria-label="optional live orders">
        <h2>Live orders</h2>
        <p>Start the View Server runtime, then connect the browser-only live panel.</p>
        <button type="button" onClick={() => setLivePanelEnabled(true)}>
          Connect live data
        </button>
      </section>
    );
  }

  return (
    <ViewServerProvider url="ws://127.0.0.1:8080/rpc">
      <LiveOrdersPanel />
    </ViewServerProvider>
  );
}

function LiveOrdersPanel() {
  const health = useViewServerHealthSummary();
  const orders = useLiveQuery("orders", {
    select: ["id", "customerId", "price"],
    orderBy: [{ field: "updatedAt", direction: "desc" }],
    limit: 10,
  });

  return (
    <section className="panel" aria-label="hydrated live orders">
      <h2>Live orders</h2>
      <p role="status">Runtime status: {health.status}</p>
      <p>Total rows: {orders.totalRows}</p>
      <ul>
        {orders.rows.map((order) => (
          <li key={order.id}>
            {order.id} / {order.customerId} / {order.price}
          </li>
        ))}
      </ul>
    </section>
  );
}
