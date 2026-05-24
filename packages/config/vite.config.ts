import { defineConfig } from "vite-plus";

export default defineConfig({
  test: {
    include: ["src/**/*.test.ts"],
    coverage: {
      include: ["src/**/*.ts"],
      exclude: ["src/**/*.test.ts"],
      reporter: ["text"],
      thresholds: {
        "100": true,
      },
    },
  },
  pack: {
    entry: ["src/index.ts", "src/runtime.ts"],
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
