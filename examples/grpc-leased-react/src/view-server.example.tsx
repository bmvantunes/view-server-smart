import { useLiveQuery, useViewServerHealthSummary, ViewServerProvider } from "./view-server.config";

export { ViewServerProvider };

export function GrpcLeasedExampleApp() {
  const health = useViewServerHealthSummary();
  const orders = useLiveQuery("orders", {
    select: ["id", "customerId", "price", "strategyId", "region"],
    where: {
      strategyId: { eq: "strategy-alpha" },
      region: { eq: "usa" },
      customerId: { startsWith: "customer-" },
    },
    orderBy: [{ field: "price", direction: "desc" }],
    limit: 20,
  });

  return (
    <main className="example-shell">
      <header>
        <p className="eyebrow">Leased gRPC source</p>
        <h1>On-demand shared gRPC route</h1>
        <p>
          The query must include exact route filters for strategy and region; the runtime shares one
          upstream route for matching subscribers.
        </p>
      </header>
      <section className="panel" aria-label="leased grpc health">
        <p role="status">Runtime status: {health.status}</p>
      </section>
      <section className="panel" aria-label="leased grpc orders">
        <h2>Strategy alpha orders</h2>
        <p>Total rows: {orders.totalRows}</p>
        <ul>
          {orders.rows.map((order) => (
            <li key={order.id}>
              {order.id} / {order.customerId} / {order.price}
            </li>
          ))}
        </ul>
      </section>
    </main>
  );
}
