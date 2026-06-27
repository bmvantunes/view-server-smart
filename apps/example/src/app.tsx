import { useLiveQuery, useViewServerHealthSummary } from "./view-server.config";

export function OrdersApp() {
  return (
    <main>
      <h1>Orders</h1>
      <RuntimeHealth />
      <OrdersTable />
    </main>
  );
}

function RuntimeHealth() {
  const health = useViewServerHealthSummary();
  return (
    <output aria-label="runtime health" role="status">
      runtime: {health.status}
    </output>
  );
}

function OrdersTable() {
  const orders = useLiveQuery("orders", {
    select: ["id", "price", "status"],
    where: {
      status: { eq: "open" },
    },
    orderBy: [{ field: "price", direction: "desc" }],
    limit: 20,
  });

  return (
    <table aria-label="open orders">
      <caption>Open orders: {orders.totalRows}</caption>
      <thead>
        <tr>
          <th scope="col">Order</th>
          <th scope="col">Status</th>
          <th scope="col">Price</th>
        </tr>
      </thead>
      <tbody>
        {orders.rows.map((order) => (
          <tr key={order.id}>
            <td>{order.id}</td>
            <td>{order.status}</td>
            <td>{order.price}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
