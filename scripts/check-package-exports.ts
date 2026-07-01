import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import type { ViewServerLiveClient } from "@effect-view-server/client";
import * as clientPackage from "@effect-view-server/client";
import * as clientRemotePackage from "@effect-view-server/client/remote";
import type { ColumnLiveViewEngine } from "@effect-view-server/column-live-view-engine";
import * as enginePackage from "@effect-view-server/column-live-view-engine";
import * as engineInternalPackage from "@effect-view-server/column-live-view-engine/internal";
import * as effectUtilsPackage from "@effect-view-server/effect-utils";
import type { ViewServerHealth } from "@effect-view-server/config/health";
import type { KafkaMappingInput } from "@effect-view-server/config/kafka";
import type { LiveSubscription } from "@effect-view-server/config/live-protocol";
import type { RawQuery } from "@effect-view-server/config/query";
import type { RuntimeEnvironmentConfig } from "@effect-view-server/config/runtime";
import { createInMemoryViewServer } from "@effect-view-server/in-memory";
import * as inMemoryPackage from "@effect-view-server/in-memory";
import * as inMemoryTestingPackage from "@effect-view-server/in-memory/testing";
import * as protocolPackage from "@effect-view-server/protocol";
import type { ViewServerWireEvent } from "@effect-view-server/protocol";
import { createViewServerReact } from "@effect-view-server/react";
import { createInMemoryViewServerReact } from "@effect-view-server/react/testing";
import { createViewServerRuntime } from "@effect-view-server/runtime";
import type { ViewServerRuntime } from "@effect-view-server/runtime";
import { createViewServerRuntimeCore } from "@effect-view-server/runtime-core";
import * as runtimeCoreInternalPackage from "@effect-view-server/runtime-core/internal";
import * as runtimeCorePackage from "@effect-view-server/runtime-core";
import { createViewServerWebSocketServer } from "@effect-view-server/server";
import type { ViewServerHealthHttpJson, ViewServerWebSocketServer } from "@effect-view-server/server";
import * as configPackage from "@effect-view-server/config";
import * as healthPackage from "@effect-view-server/config/health";
import * as grpcPackage from "@effect-view-server/config/grpc";
import * as kafkaPackage from "@effect-view-server/config/kafka";
import * as liveProtocolPackage from "@effect-view-server/config/live-protocol";
import * as queryPackage from "@effect-view-server/config/query";
import * as runtimePackage from "@effect-view-server/config/runtime";
import * as reactPackage from "@effect-view-server/react";
import * as reactTestingPackage from "@effect-view-server/react/testing";
import * as runtimeRootPackage from "@effect-view-server/runtime";
import * as serverPackage from "@effect-view-server/server";
import * as publicClientPackage from "effect-view-server/client";
import * as publicClientRemotePackage from "effect-view-server/client/remote";
import * as publicRootPackage from "effect-view-server";
import * as publicConfigPackage from "effect-view-server/config";
import * as publicConfigGrpcPackage from "effect-view-server/config/grpc";
import * as publicConfigHealthPackage from "effect-view-server/config/health";
import * as publicConfigKafkaPackage from "effect-view-server/config/kafka";
import type {
  ExactRuntimeOptions as PublicKafkaExactRuntimeOptions,
  KafkaRuntimeSourceTopicDefinition as PublicKafkaRuntimeSourceTopicDefinition,
  KafkaRuntimeTopicSourceDefinition as PublicKafkaRuntimeTopicSourceDefinition,
  KafkaTopicSourceDefinition as PublicKafkaTopicSourceDefinition,
  KafkaTopicSourceMapInput as PublicKafkaTopicSourceMapInput,
  ValidateKafkaTopicSource as PublicValidateKafkaTopicSource,
} from "effect-view-server/config/kafka";
import * as publicConfigLiveProtocolPackage from "effect-view-server/config/live-protocol";
import * as publicConfigQueryPackage from "effect-view-server/config/query";
import * as publicConfigRuntimePackage from "effect-view-server/config/runtime";
import * as publicEnginePackage from "effect-view-server/column-live-view-engine";
import * as publicInMemoryPackage from "effect-view-server/in-memory";
import * as publicInMemoryTestingPackage from "effect-view-server/in-memory/testing";
import * as publicReactPackage from "effect-view-server/react";
import * as publicReactTestingPackage from "effect-view-server/react/testing";
import * as publicRuntimePackage from "effect-view-server/runtime";
import * as publicServerPackage from "effect-view-server/server";

const repoRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const packagesRoot = join(repoRoot, "packages");

type PublicKafkaExportTopics = {
  readonly orders: {
    readonly schema: never;
  };
};
type PublicKafkaExportRegions = {
  readonly local: string;
};
type PublicKafkaTypeExports = readonly [
  PublicKafkaExactRuntimeOptions<PublicKafkaExportTopics, PublicKafkaExportRegions, {}>,
  PublicKafkaRuntimeSourceTopicDefinition<PublicKafkaExportTopics, PublicKafkaExportRegions>,
  PublicKafkaRuntimeTopicSourceDefinition<PublicKafkaExportTopics, PublicKafkaExportRegions>,
  PublicKafkaTopicSourceDefinition<PublicKafkaExportTopics, PublicKafkaExportRegions, "orders">,
  PublicKafkaTopicSourceMapInput<PublicKafkaExportTopics, "orders", "local", never, undefined>,
  PublicValidateKafkaTopicSource<PublicKafkaExportTopics, PublicKafkaExportRegions, "orders", unknown>,
];

type PackageManifest = {
  readonly name: string;
  readonly exports: Record<string, unknown>;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const readPackageManifest = (packageDirectory: string): PackageManifest => {
  const manifestPath = join(packagesRoot, packageDirectory, "package.json");
  const parsed: unknown = JSON.parse(readFileSync(manifestPath, "utf8"));

  if (!isRecord(parsed) || typeof parsed.name !== "string" || !isRecord(parsed.exports)) {
    throw new Error(`${manifestPath} must declare a package name and exports object`);
  }

  return {
    name: parsed.name,
    exports: parsed.exports,
  };
};

const exportedSpecifier = (manifest: PackageManifest, exportPath: string): string => {
  if (exportPath === ".") {
    return manifest.name;
  }
  if (!exportPath.startsWith("./")) {
    throw new Error(`${manifest.name} has unsupported export path ${exportPath}`);
  }
  return `${manifest.name}/${exportPath.slice("./".length)}`;
};

const workspacePackageDirectories = readdirSync(packagesRoot, { withFileTypes: true })
  .filter((entry) => entry.isDirectory())
  .map((entry) => entry.name)
  .sort();

const packageManifests = workspacePackageDirectories.map(readPackageManifest);

const manifestPackageExports = packageManifests
  .flatMap((manifest) =>
    Object.keys(manifest.exports)
      .sort()
      .map((exportPath) => exportedSpecifier(manifest, exportPath)),
  )
  .sort();

const approvedPackageExports = [
  "@effect-view-server/client",
  "@effect-view-server/client/remote",
  "@effect-view-server/column-live-view-engine",
  "@effect-view-server/column-live-view-engine/internal",
  "@effect-view-server/config",
  "@effect-view-server/config/grpc",
  "@effect-view-server/config/health",
  "@effect-view-server/config/internal",
  "@effect-view-server/config/kafka",
  "@effect-view-server/config/live-protocol",
  "@effect-view-server/config/query",
  "@effect-view-server/config/runtime",
  "@effect-view-server/effect-utils",
  "@effect-view-server/in-memory",
  "@effect-view-server/in-memory/testing",
  "@effect-view-server/protocol",
  "@effect-view-server/react",
  "@effect-view-server/react/testing",
  "@effect-view-server/runtime",
  "@effect-view-server/runtime-core",
  "@effect-view-server/runtime-core/internal",
  "@effect-view-server/server",
  "effect-view-server",
  "effect-view-server/client",
  "effect-view-server/client/remote",
  "effect-view-server/column-live-view-engine",
  "effect-view-server/config",
  "effect-view-server/config/grpc",
  "effect-view-server/config/health",
  "effect-view-server/config/kafka",
  "effect-view-server/config/live-protocol",
  "effect-view-server/config/query",
  "effect-view-server/config/runtime",
  "effect-view-server/in-memory",
  "effect-view-server/in-memory/testing",
  "effect-view-server/react",
  "effect-view-server/react/testing",
  "effect-view-server/runtime",
  "effect-view-server/server",
].sort();

const staleViewServerScope = "@view" + "-server";
const stalePackageExports = approvedPackageExports
  .filter((specifier) => specifier.startsWith("@effect-view-server"))
  .map((specifier) => specifier.replace("@effect-view-server", staleViewServerScope));

const describeSpecifiers = (specifiers: ReadonlyArray<string>): string =>
  specifiers.map((specifier) => `- ${specifier}`).join("\n");

const manifestExportSet = new Set(manifestPackageExports);
const approvedExportSet = new Set(approvedPackageExports);
const unapprovedManifestExports = manifestPackageExports.filter(
  (specifier) => !approvedExportSet.has(specifier),
);
const missingManifestExports = approvedPackageExports.filter(
  (specifier) => !manifestExportSet.has(specifier),
);

if (unapprovedManifestExports.length > 0 || missingManifestExports.length > 0) {
  throw new Error(
    [
      "Package manifests must match the approved public export surface.",
      unapprovedManifestExports.length === 0
        ? undefined
        : `Unapproved manifest exports:\n${describeSpecifiers(unapprovedManifestExports)}`,
      missingManifestExports.length === 0
        ? undefined
        : `Approved exports missing from manifests:\n${describeSpecifiers(missingManifestExports)}`,
    ]
      .filter((message) => message !== undefined)
      .join("\n"),
  );
}

const publicPackageExports = manifestPackageExports;

const requireExport = (moduleName: string, moduleValue: object, exportName: string) => {
  if (!(exportName in moduleValue)) {
    throw new Error(`${moduleName} is missing export ${exportName}`);
  }
};

const rejectExport = (moduleName: string, moduleValue: object, exportName: string) => {
  if (exportName in moduleValue) {
    throw new Error(`${moduleName} unexpectedly exports ${exportName}`);
  }
};

const requireModule = (moduleName: string, moduleValue: unknown) => {
  if (typeof moduleValue !== "object" || moduleValue === null) {
    throw new Error(`${moduleName} did not resolve to an ES module namespace object`);
  }
};

const requireResolvablePackageExport = (specifier: string) => {
  try {
    return import.meta.resolve(specifier);
  } catch (error) {
    throw new Error(`${specifier} should resolve as a public package export`, { cause: error });
  }
};

const publicPackageDistRoot = join(packagesRoot, "effect-view-server", "dist");

const distFiles = (directory: string): ReadonlyArray<string> =>
  readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      return distFiles(path);
    }
    return entry.isFile() && (entry.name.endsWith(".js") || entry.name.endsWith(".d.ts"))
      ? [path]
      : [];
  });

const internalViewServerImportPattern =
  /\b(?:import|export)\s+(?:[^"']*?\s+from\s+)?["']@effect-view-server(?:\/[^"']*)?["']|\bimport\s*\(\s*["']@effect-view-server(?:\/[^"']*)?["']\s*\)/;

const publicPackageInternalImportViolations = (): ReadonlyArray<string> =>
  distFiles(publicPackageDistRoot).flatMap((path) =>
    internalViewServerImportPattern.test(readFileSync(path, "utf8"))
      ? [`${path.slice(repoRoot.length + 1)} imports internal @effect-view-server/* packages.`]
      : [],
  );

const rejectResolvablePackageExport = (specifier: string) => {
  const unexpectedMessage = `${specifier} unexpectedly resolves as a public package export`;
  try {
    const resolved = import.meta.resolve(specifier);
    throw new Error(`${unexpectedMessage}: ${resolved}`);
  } catch (error) {
    if (error instanceof Error && error.message.startsWith(unexpectedMessage)) {
      throw error;
    }
  }
};

const forbiddenDeepImportSuffixes = [
  "dist/index.d.ts",
  "dist/index.js",
  "dist/internal",
  "dist/internal.js",
  "internal",
  "src/index",
  "src/index.ts",
  "src/internal",
];

const forbiddenPackageSubpathDeepImports = publicPackageExports
  .flatMap((specifier) => forbiddenDeepImportSuffixes.map((suffix) => `${specifier}/${suffix}`))
  .filter((specifier) => !approvedExportSet.has(specifier));

const forbiddenPackageDeepImports = [
  ...forbiddenPackageSubpathDeepImports,
  "@effect-view-server/client/src/index",
  "@effect-view-server/client/dist/index.js",
  "@effect-view-server/client/src/live-client",
  "@effect-view-server/client/src/remote-client",
  "@effect-view-server/client/remote/client",
  "@effect-view-server/client/remote/internal",
  "@effect-view-server/column-live-view-engine/src/index",
  "@effect-view-server/column-live-view-engine/dist/index.js",
  "@effect-view-server/column-live-view-engine/src/topic-store-state",
  "@effect-view-server/column-live-view-engine/topic-store-state",
  "@effect-view-server/config/src/index",
  "@effect-view-server/config/dist/index.js",
  "@effect-view-server/config/src/grpc-contract",
  "@effect-view-server/config/src/source-contract",
  "@effect-view-server/config/src/source-query-contract",
  "@effect-view-server/config/dist/grpc-contract.js",
  "@effect-view-server/config/dist/source-contract.js",
  "@effect-view-server/config/dist/source-query-contract.js",
  "@effect-view-server/config/dist/topic-contract.js",
  "@effect-view-server/config/src/topic-contract",
  "@effect-view-server/config/query/raw-query-contract",
  "@effect-view-server/config/query/src/topic-contract",
  "@effect-view-server/effect-utils/src/index",
  "@effect-view-server/effect-utils/dist/index.js",
  "@effect-view-server/in-memory/src/index",
  "@effect-view-server/in-memory/dist/index.js",
  "@effect-view-server/protocol/src/index",
  "@effect-view-server/protocol/dist/index.js",
  "@effect-view-server/protocol/src/protocol-json-field-codec",
  "@effect-view-server/protocol/protocol-json-field-codec",
  "@effect-view-server/protocol/src/protocol-row-codec",
  "@effect-view-server/protocol/protocol-row-codec",
  "@effect-view-server/react/src/index",
  "@effect-view-server/react/dist/index.js",
  "@effect-view-server/react/dist/testing.js",
  "@effect-view-server/react/src/testing",
  "@effect-view-server/react/testing/internal",
  "@effect-view-server/runtime/src/internal",
  "@effect-view-server/runtime/dist/index.js",
  "@effect-view-server/runtime/internal",
  "@effect-view-server/runtime-core/src/health",
  "@effect-view-server/runtime-core/dist/index.js",
  "@effect-view-server/runtime-core/health",
  "@effect-view-server/server/src/rpc-handlers",
  "@effect-view-server/server/dist/index.js",
  "@effect-view-server/server/rpc-handlers",
];

for (const specifier of publicPackageExports) {
  requireResolvablePackageExport(specifier);
}

for (const specifier of stalePackageExports) {
  rejectResolvablePackageExport(specifier);
}

for (const specifier of forbiddenPackageDeepImports) {
  rejectResolvablePackageExport(specifier);
}

const internalImportViolations = publicPackageInternalImportViolations();
if (internalImportViolations.length > 0) {
  throw new Error(
    [
      "Public effect-view-server package must not emit imports to internal workspace packages.",
      ...internalImportViolations.map((violation) => `- ${violation}`),
    ].join("\n"),
  );
}

requireExport("@effect-view-server/config", configPackage, "defineViewServerConfig");
requireExport("@effect-view-server/config", configPackage, "defineKafkaTopic");
requireExport("@effect-view-server/config", configPackage, "defineGrpcFeed");
requireExport("@effect-view-server/config", configPackage, "grpc");
requireExport("@effect-view-server/config", configPackage, "kafka");
requireModule("@effect-view-server/config/query", queryPackage);
requireModule("@effect-view-server/config/grpc", grpcPackage);
requireModule("@effect-view-server/config/health", healthPackage);
requireModule("@effect-view-server/config/live-protocol", liveProtocolPackage);
rejectExport("@effect-view-server/config/query", queryPackage, "grpc");
rejectExport("@effect-view-server/config/query", queryPackage, "defineGrpcFeed");
rejectExport("@effect-view-server/config/query", queryPackage, "GrpcTopicSource");
rejectExport("@effect-view-server/config/query", queryPackage, "GrpcLeasedTopicSource");
requireExport("@effect-view-server/config/kafka", kafkaPackage, "defineKafkaTopic");
requireExport("@effect-view-server/config/kafka", kafkaPackage, "kafka");
requireExport("@effect-view-server/config/grpc", grpcPackage, "grpc");
requireExport("@effect-view-server/config/grpc", grpcPackage, "defineGrpcFeed");
requireExport("@effect-view-server/config/runtime", runtimePackage, "runtimeConfig");
requireExport("@effect-view-server/config/runtime", runtimePackage, "runtimeEnvironmentConfig");
requireExport("@effect-view-server/client", clientPackage, "stableQueryKey");
requireExport("@effect-view-server/client", clientPackage, "applyEvent");
rejectExport("@effect-view-server/client", clientPackage, "makeViewServerClient");
rejectExport("@effect-view-server/client", clientPackage, "createViewServerClient");
rejectExport("@effect-view-server/client", clientPackage, "ViewServerRpcs");
rejectExport(
  "@effect-view-server/client",
  clientPackage,
  "ignoreLoggedTypedFailuresPreserveNonTypedFailures",
);
requireExport("@effect-view-server/client/remote", clientRemotePackage, "makeViewServerClient");
requireExport("@effect-view-server/client/remote", clientRemotePackage, "createViewServerClient");
requireExport(
  "@effect-view-server/effect-utils",
  effectUtilsPackage,
  "ignoreLoggedTypedFailuresPreserveNonTypedFailures",
);
requireExport("@effect-view-server/protocol", protocolPackage, "ViewServerRpcs");
requireExport("@effect-view-server/protocol", protocolPackage, "ViewServerWireRowSchema");
requireExport("@effect-view-server/column-live-view-engine", enginePackage, "createColumnLiveViewEngine");
requireExport("@effect-view-server/column-live-view-engine", enginePackage, "InvalidTopicError");
requireExport(
  "@effect-view-server/column-live-view-engine/internal",
  engineInternalPackage,
  "createColumnLiveViewEngineInternal",
);
rejectExport(
  "@effect-view-server/column-live-view-engine",
  enginePackage,
  "createColumnLiveViewEngineInternal",
);
requireExport("@effect-view-server/in-memory", inMemoryPackage, "createInMemoryViewServer");
requireExport("@effect-view-server/in-memory", inMemoryPackage, "makeInMemoryViewServer");
requireExport(
  "@effect-view-server/in-memory/testing",
  inMemoryTestingPackage,
  "createInMemoryViewServerTesting",
);
requireExport(
  "@effect-view-server/in-memory/testing",
  inMemoryTestingPackage,
  "makeInMemoryViewServerTesting",
);
rejectExport("@effect-view-server/in-memory", inMemoryPackage, "createInMemoryViewServerTesting");
rejectExport("@effect-view-server/in-memory", inMemoryPackage, "makeInMemoryViewServerTesting");
rejectExport("@effect-view-server/in-memory", inMemoryPackage, "readHealth");
rejectExport("@effect-view-server/in-memory", inMemoryPackage, "refreshHealth");
rejectExport("@effect-view-server/in-memory", inMemoryPackage, "makeHealthRefreshScheduler");
requireExport("@effect-view-server/runtime-core", runtimeCorePackage, "createViewServerRuntimeCore");
requireExport("@effect-view-server/runtime-core", runtimeCorePackage, "makeViewServerRuntimeCore");
requireExport(
  "@effect-view-server/runtime-core/internal",
  runtimeCoreInternalPackage,
  "makeViewServerRuntimeCoreInternal",
);
rejectExport("@effect-view-server/runtime-core", runtimeCorePackage, "makeViewServerRuntimeCoreInternal");
rejectExport(
  "@effect-view-server/runtime-core",
  runtimeCorePackage,
  "getViewServerRuntimeCoreInternalLiveClient",
);
rejectExport("@effect-view-server/runtime-core", runtimeCorePackage, "ViewServerRuntimeCoreInternalInstance");
rejectExport("@effect-view-server/runtime-core", runtimeCorePackage, "ViewServerRuntimeCoreInternalLiveClient");
rejectExport("@effect-view-server/runtime-core", runtimeCorePackage, "readHealth");
rejectExport("@effect-view-server/runtime-core", runtimeCorePackage, "refreshHealth");
rejectExport("@effect-view-server/runtime-core", runtimeCorePackage, "makeHealthRefreshScheduler");
requireExport("@effect-view-server/react", reactPackage, "createViewServerReact");
rejectExport("@effect-view-server/react", reactPackage, "createInMemoryViewServerReact");
requireExport("@effect-view-server/react/testing", reactTestingPackage, "createInMemoryViewServerReact");
requireExport("@effect-view-server/runtime", runtimeRootPackage, "makeViewServerRuntime");
requireExport("@effect-view-server/runtime", runtimeRootPackage, "createViewServerRuntime");
requireExport("@effect-view-server/runtime", runtimeRootPackage, "runViewServerRuntime");
requireExport("@effect-view-server/server", serverPackage, "makeViewServerWebSocketServer");
requireExport("@effect-view-server/server", serverPackage, "createViewServerWebSocketServer");

requireExport("effect-view-server", publicRootPackage, "defineViewServerConfig");
requireExport("effect-view-server", publicRootPackage, "defineKafkaTopic");
requireExport("effect-view-server", publicRootPackage, "defineGrpcFeed");
requireExport("effect-view-server", publicRootPackage, "grpc");
requireExport("effect-view-server", publicRootPackage, "kafka");
requireExport("effect-view-server/config", publicConfigPackage, "defineViewServerConfig");
requireExport("effect-view-server/config", publicConfigPackage, "defineKafkaTopic");
requireExport("effect-view-server/config", publicConfigPackage, "defineGrpcFeed");
requireExport("effect-view-server/config", publicConfigPackage, "grpc");
requireExport("effect-view-server/config", publicConfigPackage, "kafka");
requireModule("effect-view-server/config/query", publicConfigQueryPackage);
requireModule("effect-view-server/config/grpc", publicConfigGrpcPackage);
requireModule("effect-view-server/config/health", publicConfigHealthPackage);
requireModule("effect-view-server/config/live-protocol", publicConfigLiveProtocolPackage);
rejectExport("effect-view-server/config/query", publicConfigQueryPackage, "grpc");
rejectExport("effect-view-server/config/query", publicConfigQueryPackage, "defineGrpcFeed");
rejectExport("effect-view-server/config/query", publicConfigQueryPackage, "GrpcTopicSource");
rejectExport("effect-view-server/config/query", publicConfigQueryPackage, "GrpcLeasedTopicSource");
requireExport("effect-view-server/config/kafka", publicConfigKafkaPackage, "defineKafkaTopic");
requireExport("effect-view-server/config/kafka", publicConfigKafkaPackage, "kafka");
rejectExport("effect-view-server/config/kafka", publicConfigKafkaPackage, "makeKafkaRuntimeTopicSources");
requireExport("effect-view-server/config/grpc", publicConfigGrpcPackage, "grpc");
requireExport("effect-view-server/config/grpc", publicConfigGrpcPackage, "defineGrpcFeed");
requireExport("effect-view-server/config/runtime", publicConfigRuntimePackage, "runtimeConfig");
requireExport(
  "effect-view-server/config/runtime",
  publicConfigRuntimePackage,
  "runtimeEnvironmentConfig",
);
requireExport("effect-view-server/client", publicClientPackage, "stableQueryKey");
requireExport("effect-view-server/client", publicClientPackage, "applyEvent");
rejectExport("effect-view-server/client", publicClientPackage, "makeViewServerClient");
rejectExport("effect-view-server/client", publicClientPackage, "createViewServerClient");
rejectExport("effect-view-server/client", publicClientPackage, "ViewServerRpcs");
requireExport("effect-view-server/client/remote", publicClientRemotePackage, "makeViewServerClient");
requireExport(
  "effect-view-server/client/remote",
  publicClientRemotePackage,
  "createViewServerClient",
);
requireExport(
  "effect-view-server/column-live-view-engine",
  publicEnginePackage,
  "createColumnLiveViewEngine",
);
requireExport(
  "effect-view-server/column-live-view-engine",
  publicEnginePackage,
  "InvalidTopicError",
);
rejectExport(
  "effect-view-server/column-live-view-engine",
  publicEnginePackage,
  "createColumnLiveViewEngineInternal",
);
requireExport("effect-view-server/in-memory", publicInMemoryPackage, "createInMemoryViewServer");
requireExport("effect-view-server/in-memory", publicInMemoryPackage, "makeInMemoryViewServer");
requireExport(
  "effect-view-server/in-memory/testing",
  publicInMemoryTestingPackage,
  "createInMemoryViewServerTesting",
);
requireExport(
  "effect-view-server/in-memory/testing",
  publicInMemoryTestingPackage,
  "makeInMemoryViewServerTesting",
);
rejectExport("effect-view-server/in-memory", publicInMemoryPackage, "createInMemoryViewServerTesting");
rejectExport("effect-view-server/in-memory", publicInMemoryPackage, "makeInMemoryViewServerTesting");
requireExport("effect-view-server/react", publicReactPackage, "createViewServerReact");
rejectExport("effect-view-server/react", publicReactPackage, "createInMemoryViewServerReact");
requireExport("effect-view-server/react/testing", publicReactTestingPackage, "createInMemoryViewServerReact");
requireExport("effect-view-server/runtime", publicRuntimePackage, "makeViewServerRuntime");
requireExport("effect-view-server/runtime", publicRuntimePackage, "createViewServerRuntime");
requireExport("effect-view-server/runtime", publicRuntimePackage, "runViewServerRuntime");
requireExport("effect-view-server/server", publicServerPackage, "makeViewServerWebSocketServer");
requireExport("effect-view-server/server", publicServerPackage, "createViewServerWebSocketServer");

const _clientType: ViewServerLiveClient<Record<string, never>> | undefined = undefined;
const _engineType: ColumnLiveViewEngine<Record<string, never>> | undefined = undefined;
const _runtimeConfigType: RuntimeEnvironmentConfig | undefined = undefined;
const _queryType: RawQuery<{ readonly id: string }> | undefined = undefined;
const _healthType: ViewServerHealth<{ readonly orders: { readonly id: string } }> | undefined =
  undefined;
const _subscriptionType: LiveSubscription<{ readonly id: string }> | undefined = undefined;
const _serverType: ViewServerWebSocketServer | undefined = undefined;
const _runtimeType: ViewServerRuntime<never> | undefined = undefined;
const _healthHttpJsonType: ViewServerHealthHttpJson | undefined = undefined;
const _wireEventType: ViewServerWireEvent | undefined = undefined;
const _mappingInputType:
  | KafkaMappingInput<
      { readonly orders: { readonly schema: never } },
      "orders",
      "usa",
      never,
      undefined
    >
  | undefined = undefined;

void _engineType;
void _clientType;
void _runtimeConfigType;
void _queryType;
void _healthType;
void _subscriptionType;
void _serverType;
void _runtimeType;
void _healthHttpJsonType;
void _wireEventType;
void _mappingInputType;
void createInMemoryViewServer;
void createViewServerRuntimeCore;
void createViewServerReact;
void createInMemoryViewServerReact;
void createViewServerRuntime;
void createViewServerWebSocketServer;
