import { defineConfig } from "vite-plus";
import { libraryPack } from "../../vite.pack";

export default defineConfig({
  test: {
    include: ["src/**/*.test.ts"],
    coverage: {
      provider: "istanbul",
      include: ["src/**/*.ts"],
      exclude: ["src/**/*.test.ts"],
      reporter: ["text"],
      thresholds: {
        "100": true,
      },
    },
  },
  pack: libraryPack([
    "src/index.ts",
    "src/runtime.ts",
    "src/topic-contract.ts",
    "src/health-contract.ts",
    "src/live-protocol.ts",
    "src/kafka-contract.ts",
    "src/grpc-contract.ts",
    "src/internal.ts",
  ]),
  lint: {
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  fmt: {},
});
