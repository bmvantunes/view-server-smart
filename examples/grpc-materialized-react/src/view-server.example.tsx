import { useLiveQuery, useViewServerHealthSummary, ViewServerProvider } from "./view-server.config";

export { ViewServerProvider };

export function GrpcMaterializedExampleApp() {
  const health = useViewServerHealthSummary();
  const strategies = useLiveQuery("strategies", {
    select: ["id", "strategyId", "region", "status", "notional"],
    where: { status: { eq: "active" } },
    orderBy: [{ field: "notional", direction: "desc" }],
    limit: 20,
  });

  return (
    <main className="example-shell">
      <header>
        <p className="eyebrow">Materialized gRPC source</p>
        <h1>Startup materialized strategy stream</h1>
        <p>
          The runtime starts the gRPC stream immediately and React reads from the retained topic.
        </p>
      </header>
      <section className="panel" aria-label="materialized grpc health">
        <p role="status">Runtime status: {health.status}</p>
      </section>
      <section className="panel" aria-label="strategies">
        <h2>Active strategies</h2>
        <p>Total rows: {strategies.totalRows}</p>
        <ul>
          {strategies.rows.map((strategy) => (
            <li key={strategy.id}>
              {strategy.strategyId} / {strategy.region} / {strategy.notional}
            </li>
          ))}
        </ul>
      </section>
    </main>
  );
}
