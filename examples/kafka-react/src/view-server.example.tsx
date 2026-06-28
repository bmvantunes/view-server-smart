import {
  useLiveQuery,
  useViewServerHealth,
  useViewServerHealthSummary,
  ViewServerProvider,
} from "./view-server.config";

export { ViewServerProvider };

export function KafkaExampleApp() {
  const summary = useViewServerHealthSummary();
  const health = useViewServerHealth();
  const orders = useLiveQuery("orders", {
    select: ["id", "customerId", "status", "price", "region"],
    where: { status: { eq: "open" } },
    orderBy: [{ field: "updatedAt", direction: "desc" }],
    limit: 20,
  });

  return (
    <main className="example-shell">
      <header>
        <p className="eyebrow">Kafka source</p>
        <h1>Apache Kafka to View Server to React</h1>
        <p>
          Run the runtime process, publish JSON messages to Kafka, and the UI receives live rows
          over the normal WebSocket provider.
        </p>
      </header>
      <section className="panel" aria-label="kafka health">
        <h2>Health</h2>
        <p role="status">Runtime status: {summary.status}</p>
        <p>Detailed rows: {health.totalRows}</p>
        <p>Max Kafka lag: {String(summary.maxKafkaLag ?? "n/a")}</p>
      </section>
      <section className="panel" aria-label="kafka orders">
        <h2>Open orders</h2>
        <p>Total rows: {orders.totalRows}</p>
        <table>
          <thead>
            <tr>
              <th scope="col">Order</th>
              <th scope="col">Customer</th>
              <th scope="col">Region</th>
              <th scope="col">Price</th>
            </tr>
          </thead>
          <tbody>
            {orders.rows.map((order) => (
              <tr key={order.id}>
                <td>{order.id}</td>
                <td>{order.customerId}</td>
                <td>{order.region}</td>
                <td>{order.price}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </main>
  );
}
