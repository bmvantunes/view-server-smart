import { describe, expect, inject, it, vi } from "@effect/vitest";
import type { ViewServerLiveClient } from "@view-server/client";
import { makeViewServerClient } from "@view-server/client/remote";
import {
  defineViewServerConfig,
  VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
  VIEW_SERVER_HEALTH_TOPIC,
  type ViewServerHealthSummaryRow,
  type ViewServerHealthTopicRow,
} from "@view-server/config";
import { createInMemoryViewServer as createCoreInMemoryViewServer } from "@view-server/in-memory";
import { Effect, Schema, Stream } from "effect";
import { Component, type ReactNode } from "react";
import { render } from "vitest-browser-react";
import { createViewServerReact } from "./index";
import { ViewServerReactClientProvider } from "./internal";
import { createInMemoryViewServerReact, type ViewServerInMemoryOptions } from "./testing";

declare module "vitest" {
  export interface ProvidedContext {
    readonly viewServerRemoteUrl: string;
  }
}

const Order = Schema.Struct({
  id: Schema.String,
  customerId: Schema.String,
  status: Schema.Literals(["open", "closed", "cancelled"]),
  price: Schema.Number,
  region: Schema.String,
  updatedAt: Schema.Number,
});

const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  quantity: Schema.BigInt,
  price: Schema.Number,
  region: Schema.String,
});

const viewServer = defineViewServerConfig({
  topics: {
    orders: {
      schema: Order,
      key: "id",
    },
    trades: {
      schema: Trade,
      key: "id",
    },
  },
});

const react = createViewServerReact(viewServer);
const { useLiveQuery, useViewServerHealth, useViewServerHealthSummary } = react;
const ViewServerClientProvider = react[ViewServerReactClientProvider];

const createInMemoryViewServer = (options?: ViewServerInMemoryOptions) =>
  createInMemoryViewServerReact(react, options);

type OrderRow = typeof Order.Type;

const order = (id: string, price: number): OrderRow => ({
  id,
  customerId: `customer-${id}`,
  status: "open",
  price,
  region: "usa",
  updatedAt: price,
});

const healthTopicRow = (
  status: "ready" | "degraded" | "starting" | "stopping",
): ViewServerHealthTopicRow<"orders"> => ({
  id: "orders",
  status,
  rowCount: 0,
  liveRowCount: 0,
  deletedRowCount: 0,
  version: 0,
  lastMutationAt: null,
  mutationsPerSecond: 0,
  rowsPerSecond: 0,
  pendingMutationBatches: 0,
  activeViews: 0,
  activeSubscriptions: 0,
  queuedEvents: 0,
  maxQueueDepth: 0,
  backpressureEvents: 0,
  memoryBytes: 0,
  tombstoneCount: 0,
  compactionPending: false,
  kafkaLag: 0n,
  updatedAtNanos: 1n,
});

const healthSummaryRow = (
  runtimeStatus: "ready" | "degraded" | "starting" | "stopping",
): ViewServerHealthSummaryRow<typeof viewServer.topics> => ({
  id: "summary",
  status: runtimeStatus,
  runtimeStatus,
  connectionStatus: "connected",
  unhealthyTopics: runtimeStatus === "ready" ? [] : ["orders"],
  updatedAtNanos: 1n,
  maxKafkaLag: 0n,
});

const fakeHealthClient = (
  status: "ready" | "degraded" | "starting" | "stopping",
): {
  readonly close: Effect.Effect<void>;
  readonly client: ViewServerLiveClient<typeof viewServer.topics>;
} => {
  const inMemory = createCoreInMemoryViewServer(viewServer);
  return {
    close: inMemory.close,
    client: {
      ...inMemory.liveClient,
      subscribeHealthSummary: () =>
        Effect.succeed({
          events: Stream.make({
            type: "snapshot",
            topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
            queryId: "health-summary",
            version: 1,
            keys: ["summary"],
            rows: [healthSummaryRow(status)],
            totalRows: 1,
          }),
          close: () => Effect.void,
        }),
      subscribeHealth: () =>
        Effect.succeed({
          events: Stream.make({
            type: "snapshot",
            topic: VIEW_SERVER_HEALTH_TOPIC,
            queryId: "health",
            version: 1,
            keys: ["orders"],
            rows: [healthTopicRow(status)],
            totalRows: 1,
          }),
          close: () => Effect.void,
        }),
    },
  };
};

class ProviderErrorBoundary extends Component<
  { readonly children: ReactNode },
  { readonly message: string | null }
> {
  override readonly state: { readonly message: string | null } = {
    message: null,
  };

  static getDerivedStateFromError(error: unknown): { readonly message: string } {
    return {
      message: error instanceof Error ? error.message : String(error),
    };
  }

  override render(): ReactNode {
    if (this.state.message !== null) {
      return (
        <output aria-label="provider error" role="alert">
          {this.state.message}
        </output>
      );
    }
    return this.props.children;
  }
}

describe("createViewServerReact", () => {
  it("cleans subscriptions without closing caller-owned generic provider clients", async () => {
    const inMemory = createCoreInMemoryViewServer(viewServer);

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="orders" role="status">
          orders: {result.rows.map((row) => row.id).join("|")}
        </output>
      );
    }

    const view = await render(
      <ViewServerClientProvider client={inMemory.liveClient}>
        <OrdersView />
      </ViewServerClientProvider>,
    );
    await Effect.runPromise(inMemory.client.publish("orders", order("a", 10)));
    await expect.element(view.getByText("orders: a", { exact: true })).toBeVisible();

    await view.unmount();

    await expect
      .poll(async () => {
        const health = await Effect.runPromise(inMemory.client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(0);

    await Effect.runPromise(inMemory.client.publish("orders", order("b", 20)));
    const health = await Effect.runPromise(inMemory.client.health());
    expect(health.status).toBe("ready");
    expect(health.engine.topics.orders.rowCount).toBe(2);
    await Effect.runPromise(inMemory.close);
  });

  it("surfaces missing provider clients", async () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);

    function HealthView() {
      const health = useViewServerHealthSummary();
      return <output role="status">{health.status}</output>;
    }

    const missingProvider = await render(
      <ProviderErrorBoundary>
        <HealthView />
      </ProviderErrorBoundary>,
    );
    await expect
      .element(
        missingProvider.getByText("ViewServerProvider is missing a client.", { exact: true }),
      )
      .toBeVisible();
    await missingProvider.unmount();
    consoleError.mockRestore();
  });

  it("merges disconnected summary subscription status into public health status", async () => {
    const inMemory = createCoreInMemoryViewServer(viewServer);
    const disconnectedClient = {
      ...inMemory.liveClient,
      subscribeHealthSummary: () =>
        Effect.succeed({
          events: Stream.make({
            type: "status",
            topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
            queryId: "health-summary",
            status: "error",
            code: "TransportError",
            message: "socket closed",
          }),
          close: () => Effect.void,
        }),
    } satisfies ViewServerLiveClient<typeof viewServer.topics>;

    function HealthView() {
      const health = useViewServerHealthSummary();
      return (
        <output role="status">
          {health.runtimeStatus}:{health.connectionStatus}:{health.status}
        </output>
      );
    }

    const view = await render(
      <ViewServerClientProvider client={disconnectedClient}>
        <HealthView />
      </ViewServerClientProvider>,
    );
    await expect
      .element(view.getByText("starting:disconnected:disconnected", { exact: true }))
      .toBeVisible();
    await view.unmount();
    await Effect.runPromise(inMemory.close);
  });

  it("derives detailed runtime status from pushed health rows", async () => {
    const stopping = fakeHealthClient("stopping");
    const degraded = fakeHealthClient("degraded");
    const starting = fakeHealthClient("starting");

    function HealthView() {
      const health = useViewServerHealth();
      return (
        <output role="status">
          {`${health.runtimeStatus}:${health.connectionStatus}:${health.status}:${health.statusCode ?? "none"}`}
        </output>
      );
    }

    const view = await render(
      <ViewServerClientProvider client={stopping.client}>
        <HealthView />
      </ViewServerClientProvider>,
    );
    await expect
      .element(view.getByText("stopping:connected:stopping:none", { exact: true }))
      .toBeVisible();

    await view.rerender(
      <ViewServerClientProvider client={degraded.client}>
        <HealthView />
      </ViewServerClientProvider>,
    );
    await expect
      .element(view.getByText("degraded:connected:degraded:none", { exact: true }))
      .toBeVisible();

    await view.rerender(
      <ViewServerClientProvider client={starting.client}>
        <HealthView />
      </ViewServerClientProvider>,
    );
    await expect
      .element(view.getByText("starting:connected:starting:none", { exact: true }))
      .toBeVisible();

    await view.unmount();
    await Effect.runPromise(stopping.close);
    await Effect.runPromise(degraded.close);
    await Effect.runPromise(starting.close);
  });

  it("derives health status while summary and detail streams are connecting or disconnected", async () => {
    const summaryConnectedNoRowRuntime = createCoreInMemoryViewServer(viewServer);
    const summaryDisconnectedWithRowRuntime = createCoreInMemoryViewServer(viewServer);
    const detailConnectingRuntime = createCoreInMemoryViewServer(viewServer);
    const detailDisconnectedWithRowRuntime = createCoreInMemoryViewServer(viewServer);
    const summaryConnectedNoRowClient = {
      ...summaryConnectedNoRowRuntime.liveClient,
      subscribeHealthSummary: () =>
        Effect.succeed({
          events: Stream.make({
            type: "status",
            topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
            queryId: "health-summary",
            status: "ready",
            code: "Ready",
          }),
          close: () => Effect.void,
        }),
    } satisfies ViewServerLiveClient<typeof viewServer.topics>;
    const summaryDisconnectedWithRowClient = {
      ...summaryDisconnectedWithRowRuntime.liveClient,
      subscribeHealthSummary: () =>
        Effect.succeed({
          events: Stream.make(
            {
              type: "snapshot",
              topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
              queryId: "health-summary",
              version: 1,
              keys: ["summary"],
              rows: [
                {
                  id: "summary",
                  status: "degraded",
                  runtimeStatus: "degraded",
                  connectionStatus: "connected",
                  unhealthyTopics: ["orders"],
                  updatedAtNanos: 1n,
                  maxKafkaLag: 0n,
                },
              ],
              totalRows: 1,
            },
            {
              type: "status",
              topic: VIEW_SERVER_HEALTH_SUMMARY_TOPIC,
              queryId: "health-summary",
              status: "error",
              code: "TransportError",
              message: "socket closed",
            },
          ),
          close: () => Effect.void,
        }),
    } satisfies ViewServerLiveClient<typeof viewServer.topics>;
    const detailConnectingClient = {
      ...detailConnectingRuntime.liveClient,
      subscribeHealth: () =>
        Effect.succeed({
          events: Stream.fromEffect(Effect.never),
          close: () => Effect.void,
        }),
    } satisfies ViewServerLiveClient<typeof viewServer.topics>;
    const detailDisconnectedWithRowClient = {
      ...detailDisconnectedWithRowRuntime.liveClient,
      subscribeHealth: () =>
        Effect.succeed({
          events: Stream.make(
            {
              type: "snapshot",
              topic: VIEW_SERVER_HEALTH_TOPIC,
              queryId: "health",
              version: 1,
              keys: ["orders"],
              rows: [healthTopicRow("ready")],
              totalRows: 1,
            },
            {
              type: "status",
              topic: VIEW_SERVER_HEALTH_TOPIC,
              queryId: "health",
              status: "error",
              code: "TransportError",
              message: "socket closed",
            },
          ),
          close: () => Effect.void,
        }),
    } satisfies ViewServerLiveClient<typeof viewServer.topics>;

    function SummaryHealthView() {
      const health = useViewServerHealthSummary();
      return (
        <output role="status">
          {health.runtimeStatus}:{health.connectionStatus}:{health.status}
        </output>
      );
    }

    function DetailedHealthView() {
      const health = useViewServerHealth();
      return (
        <output role="status">
          {`${health.runtimeStatus}:${health.connectionStatus}:${health.status}:${health.statusCode ?? "none"}`}
        </output>
      );
    }

    const summaryConnectedView = await render(
      <ViewServerClientProvider client={summaryConnectedNoRowClient}>
        <SummaryHealthView />
      </ViewServerClientProvider>,
    );
    await expect
      .element(summaryConnectedView.getByText("starting:connected:starting", { exact: true }))
      .toBeVisible();
    await summaryConnectedView.unmount();

    const summaryDisconnectedView = await render(
      <ViewServerClientProvider client={summaryDisconnectedWithRowClient}>
        <SummaryHealthView />
      </ViewServerClientProvider>,
    );
    await expect
      .element(
        summaryDisconnectedView.getByText("degraded:disconnected:disconnected", { exact: true }),
      )
      .toBeVisible();
    await summaryDisconnectedView.unmount();

    const detailSummaryDisconnectedView = await render(
      <ViewServerClientProvider client={summaryDisconnectedWithRowClient}>
        <DetailedHealthView />
      </ViewServerClientProvider>,
    );
    await expect
      .element(
        detailSummaryDisconnectedView.getByText("degraded:disconnected:disconnected:none", {
          exact: true,
        }),
      )
      .toBeVisible();
    await detailSummaryDisconnectedView.unmount();

    const detailConnectingView = await render(
      <ViewServerClientProvider client={detailConnectingClient}>
        <DetailedHealthView />
      </ViewServerClientProvider>,
    );
    await expect
      .element(detailConnectingView.getByText("ready:connecting:connecting:none", { exact: true }))
      .toBeVisible();
    await detailConnectingView.unmount();

    const detailDisconnectedView = await render(
      <ViewServerClientProvider client={detailDisconnectedWithRowClient}>
        <DetailedHealthView />
      </ViewServerClientProvider>,
    );
    await expect
      .element(
        detailDisconnectedView.getByText("ready:disconnected:disconnected:TransportError", {
          exact: true,
        }),
      )
      .toBeVisible();
    await detailDisconnectedView.unmount();

    await Effect.runPromise(summaryConnectedNoRowRuntime.close);
    await Effect.runPromise(summaryDisconnectedWithRowRuntime.close);
    await Effect.runPromise(detailConnectingRuntime.close);
    await Effect.runPromise(detailDisconnectedWithRowRuntime.close);
  });

  it("switches hook clients when the generic provider client prop changes", async () => {
    const first = createCoreInMemoryViewServer(viewServer);
    const second = createCoreInMemoryViewServer(viewServer);

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="orders" role="status">
          orders: {result.rows.map((row) => row.id).join("|")}
        </output>
      );
    }

    const view = await render(
      <ViewServerClientProvider client={first.liveClient}>
        <OrdersView />
      </ViewServerClientProvider>,
    );
    await Effect.runPromise(first.client.publish("orders", order("first", 10)));
    await expect.element(view.getByText("orders: first", { exact: true })).toBeVisible();

    await view.rerender(
      <ViewServerClientProvider client={second.liveClient}>
        <OrdersView />
      </ViewServerClientProvider>,
    );
    await Effect.runPromise(second.client.publish("orders", order("second", 20)));
    await expect.element(view.getByText("orders: second", { exact: true })).toBeVisible();

    await expect
      .poll(async () => {
        const health = await Effect.runPromise(first.client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(0);

    await view.unmount();
    await Effect.runPromise(first.close);
    await Effect.runPromise(second.close);
  });

  it("keeps nested provider contexts isolated per binding instance", async () => {
    const outerReact = createViewServerReact(viewServer);
    const innerReact = createViewServerReact(viewServer);
    const outer = createInMemoryViewServerReact(outerReact);
    const inner = createInMemoryViewServerReact(innerReact);

    function OuterOrdersView() {
      const result = outerReact.useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="outer orders" role="status">
          outer orders: {result.rows.map((row) => row.id).join("|")}
        </output>
      );
    }

    function InnerOrdersView() {
      const result = innerReact.useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="inner orders" role="status">
          inner orders: {result.rows.map((row) => row.id).join("|")}
        </output>
      );
    }

    const view = await render(
      <outer.ViewServerInMemoryProvider>
        <inner.ViewServerInMemoryProvider>
          <OuterOrdersView />
          <InnerOrdersView />
        </inner.ViewServerInMemoryProvider>
      </outer.ViewServerInMemoryProvider>,
    );

    await Effect.runPromise(outer.client.publish("orders", order("outer", 10)));
    await Effect.runPromise(inner.client.publish("orders", order("inner", 20)));

    await expect.element(view.getByText("outer orders: outer", { exact: true })).toBeVisible();
    await expect.element(view.getByText("inner orders: inner", { exact: true })).toBeVisible();

    await view.unmount();
  });

  it("streams runtime-published snapshots and live deltas in browser providers", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function OrdersView() {
      const result = useLiveQuery("orders", {
        orderBy: [{ field: "price", direction: "asc" }],
        select: ["id", "price"],
        limit: 10,
      });
      const rows = result.rows.map((row) => `${row.id}:${row.price}`).join("|");
      return (
        <output aria-label="orders" role="status">
          {rows === "" ? "orders: none" : `orders: ${rows}`}
        </output>
      );
    }
    function HealthView() {
      const health = useViewServerHealth();
      const rowCount = health.rows[0]?.rowCount ?? 0;
      return (
        <output aria-label="health" role="status">
          {rowCount}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
        <HealthView />
      </ViewServerInMemoryProvider>,
    );
    await expect.element(view.getByText("orders: none", { exact: true })).toBeVisible();

    await Effect.runPromise(client.publishMany("orders", [order("b", 20), order("a", 10)]));

    await expect.element(view.getByText("orders: a:10|b:20", { exact: true })).toBeVisible();
    await expect.element(view.getByText("2", { exact: true })).toBeVisible();

    await Effect.runPromise(client.publish("orders", order("c", 5)));

    await expect.element(view.getByText("orders: c:5|a:10|b:20", { exact: true })).toBeVisible();
    await expect.element(view.getByText("3", { exact: true })).toBeVisible();
    await view.unmount();
  });

  it("uses the same component with in-memory and remote providers", async () => {
    function OrdersView(props: { readonly id: string }) {
      const result = useLiveQuery("orders", {
        where: {
          id: { eq: props.id },
        },
        orderBy: [{ field: "price", direction: "asc" }],
        select: ["id", "price"],
        limit: 10,
      });
      const rows = result.rows.map((row) => `${row.id}:${row.price}`).join("|");
      return (
        <output aria-label={`orders ${props.id}`} role="status">
          {rows === "" ? `orders ${props.id}: none` : `orders ${props.id}: ${rows}`}
        </output>
      );
    }
    function HealthView(props: { readonly label: string }) {
      const health = useViewServerHealthSummary();
      return (
        <output aria-label={props.label} role="status">
          {props.label}: {health.status}
        </output>
      );
    }

    const local = createInMemoryViewServer();
    const localId = `local-${crypto.randomUUID()}`;
    const localView = await render(
      <local.ViewServerInMemoryProvider>
        <OrdersView id={localId} />
        <HealthView label={`local health ${localId}`} />
      </local.ViewServerInMemoryProvider>,
    );
    await expect
      .element(localView.getByText(`orders ${localId}: none`, { exact: true }))
      .toBeVisible();
    await expect
      .element(localView.getByText(`local health ${localId}: ready`, { exact: true }))
      .toBeVisible();

    await Effect.runPromise(local.client.publish("orders", order(localId, 10)));
    await expect
      .element(localView.getByText(`orders ${localId}: ${localId}:10`, { exact: true }))
      .toBeVisible();
    await localView.unmount();

    const remoteId = `remote-${crypto.randomUUID()}`;
    const remoteView = await render(
      <react.ViewServerProvider url={inject("viewServerRemoteUrl")}>
        <OrdersView id={remoteId} />
        <HealthView label={`remote health ${remoteId}`} />
      </react.ViewServerProvider>,
    );
    await expect
      .element(remoteView.getByText(`orders ${remoteId}: none`, { exact: true }))
      .toBeVisible();
    await expect
      .element(remoteView.getByText(`remote health ${remoteId}: ready`, { exact: true }))
      .toBeVisible();

    const remoteProbe = await Effect.runPromise(
      makeViewServerClient(viewServer, {
        url: inject("viewServerRemoteUrl"),
      }),
    );
    const readRemoteOrderSubscriptions = async () => {
      const subscription = await Effect.runPromise(remoteProbe.subscribeHealth());
      const events = await Effect.runPromise(
        subscription.events.pipe(Stream.take(1), Stream.runCollect),
      );
      await Effect.runPromise(subscription.close());
      const event = events[0];
      return event?.type === "snapshot"
        ? (event.rows.find((row) => row.id === "orders")?.activeSubscriptions ?? -1)
        : -1;
    };
    await expect.poll(readRemoteOrderSubscriptions).toBe(1);

    await remoteView.unmount();

    await expect.poll(readRemoteOrderSubscriptions).toBe(0);
    await Effect.runPromise(remoteProbe.close);
  });

  it("owns remote client creation from provider URL and options", async () => {
    function HealthView(props: { readonly label: string }) {
      const health = useViewServerHealthSummary();
      return (
        <output aria-label={props.label} role="status">
          {props.label}: {health.status}
        </output>
      );
    }

    const remoteProviderView = await render(
      <react.ViewServerProvider subscriptionBufferSize={8} url={inject("viewServerRemoteUrl")}>
        <HealthView label="remote provider health" />
      </react.ViewServerProvider>,
    );
    await expect
      .element(remoteProviderView.getByText("remote provider health: ready", { exact: true }))
      .toBeVisible();
    await remoteProviderView.unmount();
  });

  it("recreates remote provider clients when URL options change", async () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);

    function HealthView() {
      const health = useViewServerHealthSummary();
      return <output role="status">{health.status}</output>;
    }

    const provider = await render(
      <ProviderErrorBoundary>
        <react.ViewServerProvider url={inject("viewServerRemoteUrl")}>
          <HealthView />
        </react.ViewServerProvider>
      </ProviderErrorBoundary>,
    );
    await expect.element(provider.getByText("ready", { exact: true })).toBeVisible();

    await provider.rerender(
      <ProviderErrorBoundary>
        <react.ViewServerProvider url="ws://127.0.0.1:1/rpc">
          <HealthView />
        </react.ViewServerProvider>
      </ProviderErrorBoundary>,
    );
    await expect.element(provider.getByRole("alert")).toBeVisible();
    await provider.unmount();
    consoleError.mockRestore();
  });

  it("surfaces remote provider connection failures through error boundaries", async () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);

    function HealthView() {
      const health = useViewServerHealthSummary();
      return <output role="status">{health.status}</output>;
    }

    const failedProvider = await render(
      <ProviderErrorBoundary>
        <react.ViewServerProvider url="ws://127.0.0.1:1/rpc">
          <HealthView />
        </react.ViewServerProvider>
      </ProviderErrorBoundary>,
    );
    await expect.element(failedProvider.getByRole("alert")).toBeVisible();
    await failedProvider.unmount();
    consoleError.mockRestore();
  });

  it("closes live subscriptions when browser components unmount", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const rows = result.rows.map((row) => row.id).join("|");
      return (
        <output aria-label="orders" role="status">
          {rows === "" ? "orders: none" : `orders: ${rows}`}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    await expect.element(view.getByText("orders: none", { exact: true })).toBeVisible();

    await Effect.runPromise(client.publish("orders", order("a", 10)));
    await expect.element(view.getByText("orders: a", { exact: true })).toBeVisible();

    await view.rerender(<ViewServerInMemoryProvider></ViewServerInMemoryProvider>);

    await expect
      .poll(async () => {
        const health = await Effect.runPromise(client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(0);
    await view.unmount();
  });

  it("keeps the in-memory engine open while a mounted provider has no hook consumers", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id", "price"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      const rows = result.rows.map((row) => `${row.id}:${row.price}`).join("|");
      return (
        <output aria-label="orders" role="status">
          {rows === "" ? "orders: none" : `orders: ${rows}`}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    await Effect.runPromise(client.publish("orders", order("a", 10)));
    await expect.element(view.getByText("orders: a:10", { exact: true })).toBeVisible();

    await view.rerender(<ViewServerInMemoryProvider></ViewServerInMemoryProvider>);
    await expect
      .poll(async () => {
        const health = await Effect.runPromise(client.health());
        return health.engine.topics.orders.activeSubscriptions;
      })
      .toBe(0);

    await Effect.runPromise(client.publish("orders", order("b", 20)));

    await view.rerender(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    await expect.element(view.getByText("orders: a:10|b:20", { exact: true })).toBeVisible();
    await view.unmount();
  });

  it("applies update, move, remove, patch, snapshot, and reset paths", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function OrdersView() {
      const result = useLiveQuery("orders", {
        orderBy: [{ field: "price", direction: "asc" }],
        select: ["id", "price"],
        limit: 10,
      });
      const rows = result.rows.map((row) => `${row.id}:${row.price}`).join("|");
      return (
        <output aria-label="orders" role="status">
          {rows === "" ? "orders: none" : `orders: ${rows}`}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );
    await expect.element(view.getByText("orders: none", { exact: true })).toBeVisible();

    await Effect.runPromise(client.publishMany("orders", [order("a", 10), order("b", 20)]));
    await expect.element(view.getByText("orders: a:10|b:20", { exact: true })).toBeVisible();

    await Effect.runPromise(client.publish("orders", order("a", 30)));
    await expect.element(view.getByText("orders: b:20|a:30", { exact: true })).toBeVisible();

    await Effect.runPromise(client.patch("orders", "a", { price: 5 }));
    await expect.element(view.getByText("orders: a:5|b:20", { exact: true })).toBeVisible();

    await Effect.runPromise(client.delete("orders", "a"));
    await expect.element(view.getByText("orders: b:20", { exact: true })).toBeVisible();

    const snapshot = await Effect.runPromise(
      client.snapshot("orders", {
        select: ["id", "price"],
        limit: 10,
      }),
    );
    expect(snapshot.rows).toStrictEqual([{ id: "b", price: 20 }]);

    await Effect.runPromise(client.reset());
    expect((await Effect.runPromise(client.health())).engine.topics.orders.rowCount).toBe(0);
    await expect.element(view.getByText("orders: none", { exact: true })).toBeVisible();
    await view.unmount();
  });

  it("coalesces in-memory health refreshes under concurrent publishes", async () => {
    const { client } = createInMemoryViewServer();

    await Promise.all(
      Array.from({ length: 50 }, (_, index) =>
        Effect.runPromise(client.publish("orders", order(`coalesced-${index}`, index))),
      ),
    );

    await expect
      .poll(async () => {
        const health = await Effect.runPromise(client.health());
        return health.engine.topics.orders.rowCount;
      })
      .toBe(50);
  });

  it("surfaces live query failures as error results", async () => {
    const { ViewServerInMemoryProvider } = createInMemoryViewServer();

    function BrokenOrdersView() {
      const result = useLiveQuery("orders", {
        // @ts-expect-error invalid selected fields are still surfaced through the hook result.
        select: ["prcie"],
      });
      return (
        <output aria-label="orders" role="status">
          {result.status}:{result.statusCode}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <BrokenOrdersView />
      </ViewServerInMemoryProvider>,
    );
    await expect.element(view.getByText("error:InvalidQuery", { exact: true })).toBeVisible();
    await view.unmount();
  });

  it("maps runtime errors", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    const view = await render(<ViewServerInMemoryProvider></ViewServerInMemoryProvider>);

    await Effect.runPromise(client.publish("orders", order("a", 10)));

    const invalidTopic = await Effect.runPromise(
      Effect.flip(
        // @ts-expect-error hostile runtime callers can still send unknown topics.
        client.publish("missing", order("b", 20)),
      ),
    );
    const invalidRow = await Effect.runPromise(
      Effect.flip(
        client.publish("orders", {
          id: "bad",
          customerId: "customer-bad",
          // @ts-expect-error hostile runtime callers can still send malformed rows.
          status: "unknown",
          price: 20,
          region: "usa",
          updatedAt: 20,
        }),
      ),
    );
    const groupedSnapshot = await Effect.runPromise(
      Effect.flip(
        client.snapshot("orders", {
          // @ts-expect-error grouped queries are rejected by the raw in-memory runtime slice.
          groupBy: ["status"],
          // @ts-expect-error grouped queries are rejected by the raw in-memory runtime slice.
          aggregates: { count: { aggFunc: "count" } },
        }),
      ),
    );
    const invalidQuery = await Effect.runPromise(
      Effect.flip(
        client.snapshot("orders", {
          // @ts-expect-error hostile runtime callers can still send unknown projected fields.
          select: ["prcie"],
        }),
      ),
    );

    expect(invalidTopic.code).toBe("InvalidTopic");
    expect(invalidRow.code).toBe("InvalidRow");
    expect(groupedSnapshot.code).toBe("UnsupportedQuery");
    expect(invalidQuery.code).toBe("InvalidQuery");
    await view.unmount();
  });

  it("keeps query memoization safe for bigint query values", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function TradesView() {
      const result = useLiveQuery("trades", {
        where: {
          quantity: { gte: 10n },
        },
        select: ["id", "quantity"],
        limit: 10,
      });
      const rows = result.rows.map((row) => `${row.id}:${row.quantity}`).join("|");
      return (
        <output aria-label="trades" role="status">
          {rows === "" ? "trades: none" : `trades: ${rows}`}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <TradesView />
      </ViewServerInMemoryProvider>,
    );
    await expect.element(view.getByText("trades: none", { exact: true })).toBeVisible();

    await Effect.runPromise(
      client.publishMany("trades", [
        { id: "a", symbol: "AAPL", quantity: 5n, price: 100, region: "usa" },
        { id: "b", symbol: "MSFT", quantity: 10n, price: 200, region: "usa" },
      ]),
    );

    await expect.element(view.getByText("trades: b:10", { exact: true })).toBeVisible();
    await view.unmount();
  });

  it("closes the owned in-memory runtime after provider disposal", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer();

    function HealthView() {
      const health = useViewServerHealthSummary();
      return (
        <output aria-label="health" role="status">
          {health.status}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <HealthView />
      </ViewServerInMemoryProvider>,
    );
    await expect.element(view.getByText("ready", { exact: true })).toBeVisible();

    await view.unmount();
    await expect
      .poll(async () => {
        return Effect.runPromise(Effect.flip(client.publish("orders", order("a", 10)))).then(
          (error) => error.code,
          () => "success",
        );
      })
      .toBe("RuntimeUnavailable");
  });

  it("surfaces closed status and clears rows when runtime closes while subscribed", async () => {
    const { ViewServerInMemoryProvider, client, close } = createInMemoryViewServer();

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="orders" role="status">
          {result.status}:{result.statusCode}:{result.rows.map((row) => row.id).join("|")}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );

    await Effect.runPromise(client.publish("orders", order("a", 10)));
    await expect.element(view.getByText("ready:Ready:a", { exact: true })).toBeVisible();

    await Effect.runPromise(close);

    await expect
      .element(view.getByText("closed:SubscriptionClosed:", { exact: true }))
      .toBeVisible();
    await view.unmount();
  });

  it("returns close for disposing in-memory helpers without mounting a provider", async () => {
    const { client, close } = createInMemoryViewServer();

    await Effect.runPromise(client.publish("orders", order("a", 10)));
    await Effect.runPromise(close);

    const health = await Effect.runPromise(client.health());
    expect(health.status).toBe("stopping");

    await expect
      .poll(async () => {
        return Effect.runPromise(Effect.flip(client.publish("orders", order("b", 20)))).then(
          (error) => error.code,
          () => "success",
        );
      })
      .toBe("RuntimeUnavailable");
  });

  it("surfaces status events from bounded subscription queues", async () => {
    const { ViewServerInMemoryProvider, client } = createInMemoryViewServer({
      subscriptionQueueCapacity: 1,
    });

    function OrdersView() {
      const result = useLiveQuery("orders", {
        select: ["id"],
        orderBy: [{ field: "price", direction: "asc" }],
        limit: 10,
      });
      return (
        <output aria-label="orders" role="status">
          {result.status}:{result.statusCode}
        </output>
      );
    }

    const view = await render(
      <ViewServerInMemoryProvider>
        <OrdersView />
      </ViewServerInMemoryProvider>,
    );

    await Effect.runPromise(client.publish("orders", order("a", 10)));
    await expect.element(view.getByText("ready:Ready", { exact: true })).toBeVisible();

    for (let index = 0; index < 50; index += 1) {
      await Effect.runPromise(client.publish("orders", order(`burst-${index}`, index)));
    }

    expect((await Effect.runPromise(client.health())).transport.backpressureEvents).toBe(1);
    await expect
      .element(view.getByText("closed:BackpressureExceeded", { exact: true }))
      .toBeVisible();
    await view.unmount();
  });
});
