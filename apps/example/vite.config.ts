import { defineConfig } from "vite-plus";
import { playwright } from "vitest/browser/providers/playwright";

export default defineConfig({
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
      exclude: ["src/**/*.test.ts", "src/**/*.test.tsx", "src/**/*.test-d.ts"],
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
