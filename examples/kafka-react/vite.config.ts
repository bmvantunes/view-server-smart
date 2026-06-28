import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import tailwindcss from "@tailwindcss/vite";
import viteReact from "@vitejs/plugin-react";
import { defineConfig } from "vite-plus";
import { playwright } from "vitest/browser/providers/playwright";

export default defineConfig({
  optimizeDeps: {
    include: ["@effect/vitest", "react-dom/client", "vitest-browser-react"],
    exclude: ["@tanstack/react-router", "@tanstack/react-start", "@tanstack/router-plugin"],
  },
  plugins: [tailwindcss(), tanstackStart(), viteReact()],
  test: {
    include: ["src/**/*.test.ts"],
    typecheck: {
      enabled: true,
      checker: "tsc",
      include: ["src/**/*.test-d.ts", "src/**/*.browser.test.tsx"],
      tsconfig: "./tsconfig.json",
    },
    browser: {
      enabled: true,
      provider: playwright(),
      headless: true,
      instances: [
        { browser: "chromium", name: "chromium", include: ["src/**/*.browser.test.tsx"] },
        { browser: "firefox", name: "firefox", include: ["src/**/*.browser.test.tsx"] },
        { browser: "webkit", name: "webkit", include: ["src/**/*.browser.test.tsx"] },
      ],
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
