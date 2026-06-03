import { defineConfig } from "vite-plus";
import { libraryPack } from "../../vite.pack";

export default defineConfig({
  test: {
    include: ["src/**/*.test.ts"],
    benchmark: {
      include: ["src/**/*.bench.ts"],
    },
    coverage: {
      provider: "istanbul",
      include: ["src/**/*.ts"],
      exclude: ["src/**/*.bench.ts", "src/**/*.test.ts", "src/**/*.test-d.ts"],
      reporter: ["text"],
      thresholds: {
        "100": true,
      },
    },
  },
  pack: libraryPack("src/index.ts"),
  lint: {
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  fmt: {},
});
