import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import tailwindcss from "@tailwindcss/vite";
import viteReact from "@vitejs/plugin-react";
import { defineConfig } from "vite-plus";
import { playwright } from "vitest/browser/providers/playwright";

export default defineConfig({
  optimizeDeps: {
    include: ["react-dom/client"],
    exclude: ["@tanstack/react-router", "@tanstack/react-start", "@tanstack/router-plugin"],
  },
  plugins: [tailwindcss(), tanstackStart(), viteReact()],
  test: {
    include: ["src/**/*.test.ts", "src/**/*.test.tsx"],
    typecheck: {
      enabled: true,
      checker: "tsc",
      include: ["src/**/*.test-d.ts"],
      tsconfig: "./tsconfig.json",
    },
    browser: {
      enabled: true,
      provider: playwright(),
      headless: true,
      instances: [
        { browser: "chromium", name: "chromium" },
        { browser: "firefox", name: "firefox" },
        { browser: "webkit", name: "webkit" },
      ],
    },
    coverage: {
      provider: "istanbul",
      include: ["src/**/*.ts", "src/**/*.tsx"],
      exclude: [
        "src/router.tsx",
        "src/routeTree.gen.ts",
        "src/routes/**/*.tsx",
        "src/**/*.test.ts",
        "src/**/*.test.tsx",
        "src/**/*.test-d.ts",
      ],
      reporter: ["text"],
      thresholds: {
        "100": true,
      },
    },
  },
  lint: {
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  fmt: {},
});
