import { createFileRoute } from "@tanstack/react-router";
import { Effect } from "effect";
import { useRef, useState } from "react";
import { InMemoryExampleApp, createInMemoryExample } from "../view-server.example";

export const Route = createFileRoute("/")({ component: Home });

const inMemoryExample = createInMemoryExample();

function Home() {
  const [publishedCount, setPublishedCount] = useState(0);
  const nextOrderIndex = useRef(0);

  const publishOrder = async () => {
    const next = nextOrderIndex.current + 1;
    nextOrderIndex.current = next;
    await Effect.runPromise(
      inMemoryExample.client.publish("orders", {
        id: `order-${next}`,
        customerId: `customer-${next}`,
        status: next % 2 === 0 ? "closed" : "open",
        price: next * 10,
        region: next % 3 === 0 ? "london" : "usa",
        updatedAt: next,
      }),
    );
    setPublishedCount((count) => Math.max(count, next));
  };

  return (
    <inMemoryExample.ViewServerInMemoryProvider>
      <InMemoryExampleApp onPublishOrder={publishOrder} publishedCount={publishedCount} />
    </inMemoryExample.ViewServerInMemoryProvider>
  );
}
