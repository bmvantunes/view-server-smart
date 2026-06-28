import {
  useLiveQuery,
  useViewServerHealth,
  useViewServerHealthSummary,
  ViewServerProvider,
} from "./view-server.config";

export { ViewServerProvider };

export function CombinedSourcesExampleApp() {
  const summary = useViewServerHealthSummary();
  const health = useViewServerHealth();
  const orders = useLiveQuery("orders", {
    select: ["id", "customerId", "strategyId", "region", "price"],
    where: {
      strategyId: { eq: "strategy-alpha" },
      region: { eq: "usa" },
    },
    limit: 10,
  });
  const strategies = useLiveQuery("strategies", {
    select: ["id", "strategyId", "status", "notional"],
    where: { status: { eq: "active" } },
    limit: 10,
  });
  const trades = useLiveQuery("trades", {
    select: ["id", "symbol", "side", "quantity"],
    orderBy: [{ field: "updatedAt", direction: "desc" }],
    limit: 10,
  });

  return (
    <main className="example-shell">
      <header>
        <p className="eyebrow">Combined sources</p>
        <h1>Kafka plus leased and materialized gRPC</h1>
        <p>
          This is the production-shaped example: one React app, one runtime, three source types.
        </p>
      </header>
      <section className="panel" aria-label="combined health">
        <h2>Health</h2>
        <p role="status">Runtime status: {summary.status}</p>
        <p>Detailed health rows: {health.totalRows}</p>
      </section>
      <section className="panel-grid">
        <SourcePanel
          title="Leased orders"
          totalRows={orders.totalRows}
          rows={orders.rows.map((row) => row.id)}
        />
        <SourcePanel
          title="Materialized strategies"
          totalRows={strategies.totalRows}
          rows={strategies.rows.map((row) => row.id)}
        />
        <SourcePanel
          title="Kafka trades"
          totalRows={trades.totalRows}
          rows={trades.rows.map((row) => row.id)}
        />
      </section>
    </main>
  );
}

function SourcePanel(props: {
  readonly title: string;
  readonly totalRows: number;
  readonly rows: ReadonlyArray<string>;
}) {
  return (
    <article className="panel">
      <h2>{props.title}</h2>
      <p>Total rows: {props.totalRows}</p>
      <ul>
        {props.rows.map((row) => (
          <li key={row}>{row}</li>
        ))}
      </ul>
    </article>
  );
}
