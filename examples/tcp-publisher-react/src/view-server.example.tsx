import { useLiveQuery, useViewServerHealthSummary, ViewServerProvider } from "./view-server.config";

export { ViewServerProvider };

export function TcpPublisherExampleApp() {
  const health = useViewServerHealthSummary();
  const orders = useLiveQuery("orders", {
    select: ["id", "customerId", "region", "price"],
    orderBy: [{ field: "updatedAt", direction: "desc" }],
    limit: 20,
  });

  return (
    <main className="example-shell">
      <header>
        <p className="eyebrow">External TCP publisher</p>
        <h1>TCP ingress to live React table</h1>
        <p>
          Run the runtime, then run the publisher process. Rows flow through schema-safe TCP publish
          ingress and arrive over WebSocket live queries.
        </p>
      </header>
      <section className="panel" aria-label="tcp health">
        <p role="status">Runtime status: {health.status}</p>
      </section>
      <section className="panel" aria-label="tcp orders">
        <h2>TCP orders</h2>
        <p>Total rows: {orders.totalRows}</p>
        <ul>
          {orders.rows.map((order) => (
            <li key={order.id}>
              {order.id} / {order.customerId} / {order.region} / {order.price}
            </li>
          ))}
        </ul>
      </section>
    </main>
  );
}
