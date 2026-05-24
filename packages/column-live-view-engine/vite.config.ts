import { defineConfig } from "vite-plus";

export default defineConfig({
  test: {
    include: ["src/**/*.test.ts"],
    coverage: {
      include: ["src/**/*.ts"],
      exclude: ["src/**/*.test.ts", "src/**/*.test-d.ts"],
      reporter: ["text"],
      thresholds: {
        "100": true,
      },
    },
  },
  pack: {
    entry: "src/index.ts",
    dts: true,
    fixedExtension: false,
    exports: false,
  },
  lint: {
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  fmt: {},
});
