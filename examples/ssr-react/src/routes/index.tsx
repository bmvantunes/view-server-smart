import { createFileRoute } from "@tanstack/react-router";
import { SsrExampleApp } from "../view-server.example";

export const Route = createFileRoute("/")({ component: Home });

function Home() {
  return <SsrExampleApp />;
}
