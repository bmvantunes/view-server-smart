import { createInMemoryViewServerReact } from "@view-server/react/testing";
import { useLiveQuery, useViewServerHealthSummary, viewServerReact } from "./view-server.config";

export const createInMemoryExample = () => createInMemoryViewServerReact(viewServerReact);

export function InMemoryExampleApp(props: {
  readonly onPublishOrder: () => Promise<void>;
  readonly publishedCount: number;
}) {
  const health = useViewServerHealthSummary();
  const openOrders = useLiveQuery("orders", {
    select: ["id", "customerId", "price", "status", "region"],
    where: {
      status: { eq: "open" },
      customerId: { startsWith: "customer-" },
      price: { gte: 0 },
    },
    orderBy: [{ field: "price", direction: "desc" }],
    limit: 20,
  });
  const ordersByRegion = useLiveQuery("orders", {
    groupBy: ["region"],
    aggregates: {
      rowCount: { aggFunc: "count" },
      totalPrice: { aggFunc: "sum", field: "price" },
    },
    orderBy: [{ aggregate: "rowCount", direction: "desc" }],
    limit: 10,
  });

  return (
    <main className="example-shell">
      <header>
        <p className="eyebrow">In-memory View Server</p>
        <h1>Live orders without a server process</h1>
        <p>
          This TanStack Start app uses the same React hooks as production, but its provider is
          backed by an in-memory runtime for browser tests and demos.
        </p>
      </header>

      <section aria-label="runtime controls" className="panel">
        <p role="status">Runtime status: {health.status}</p>
        <p>Published from UI: {props.publishedCount}</p>
        <button type="button" onClick={props.onPublishOrder}>
          Publish next order
        </button>
      </section>

      <section aria-label="open orders" className="panel">
        <h2>Open orders</h2>
        <p>Total rows: {openOrders.totalRows}</p>
        <table>
          <thead>
            <tr>
              <th scope="col">Order</th>
              <th scope="col">Customer</th>
              <th scope="col">Status</th>
              <th scope="col">Region</th>
              <th scope="col">Price</th>
            </tr>
          </thead>
          <tbody>
            {openOrders.rows.map((order) => (
              <tr key={order.id}>
                <td>{order.id}</td>
                <td>{order.customerId}</td>
                <td>{order.status}</td>
                <td>{order.region}</td>
                <td>{order.price}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section aria-label="orders by region" className="panel">
        <h2>Orders by region</h2>
        <p>Grouped rows: {ordersByRegion.totalRows}</p>
        <table>
          <thead>
            <tr>
              <th scope="col">Region</th>
              <th scope="col">Rows</th>
              <th scope="col">Total price</th>
            </tr>
          </thead>
          <tbody>
            {ordersByRegion.rows.map((row) => (
              <tr key={row.region}>
                <td>{row.region}</td>
                <td>{String(row.rowCount)}</td>
                <td>{String(row.totalPrice)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </main>
  );
}
