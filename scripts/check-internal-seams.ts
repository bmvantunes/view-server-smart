import { readdirSync, readFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const packageSourceRoot = (name: string): string => join(repoRoot, "packages", name, "src");
const engineSourceRoot = join(repoRoot, "packages", "column-live-view-engine", "src");
const topicStoreFile = join(engineSourceRoot, "topic-store.ts");
const topicStoreHealthFile = join(engineSourceRoot, "topic-store-health.ts");
const topicStoreLifecycleFile = join(engineSourceRoot, "topic-store-lifecycle.ts");
const topicStoreMutationFile = join(engineSourceRoot, "topic-store-mutation.ts");
const topicStoreQueryFile = join(engineSourceRoot, "topic-store-query.ts");
const topicStoreStateFile = join(engineSourceRoot, "topic-store-state.ts");
const topicStoreSubscriptionFile = join(engineSourceRoot, "topic-store-subscription.ts");

const restrictedTopicStoreHelpers = [
  {
    name: "makeTopicStoreSubscriptionPermit",
    pattern: /\bmakeTopicStoreSubscriptionPermit\b/,
    allowedPaths: new Set([topicStoreStateFile, topicStoreSubscriptionFile]),
  },
  {
    name: "topicStoreRawQueryMetadata",
    pattern: /\btopicStoreRawQueryMetadata\b/,
    allowedPaths: new Set([topicStoreQueryFile, topicStoreStateFile]),
  },
  {
    name: "topicStoreReadModel",
    pattern: /\btopicStoreReadModel\b/,
    allowedPaths: new Set([
      topicStoreHealthFile,
      topicStoreLifecycleFile,
      topicStoreQueryFile,
      topicStoreStateFile,
    ]),
  },
  {
    name: "topicStoreState",
    pattern: /\btopicStoreState\b/,
    allowedPaths: new Set([
      topicStoreHealthFile,
      topicStoreLifecycleFile,
      topicStoreMutationFile,
      topicStoreStateFile,
      topicStoreSubscriptionFile,
    ]),
  },
] as const;

const sourceFiles = (directory: string): ReadonlyArray<string> => {
  const entries = readdirSync(directory, { withFileTypes: true });
  const files: Array<string> = [];

  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      files.push(...sourceFiles(path));
      continue;
    }
    if (entry.isFile() && entry.name.endsWith(".ts")) {
      files.push(path);
    }
  }

  return files;
};

const isTestFile = (path: string): boolean =>
  path.endsWith(".test.ts") || path.endsWith(".test-d.ts");

const violations: Array<string> = [];
const topicStoreStateExportViolations: Array<string> = [];

const restrictedTopicStoreStateExports = [
  {
    label: "namespace import",
    pattern: /import\s+\*\s+as\s+\w+\s+from\s+["']\.\/topic-store-state["']/,
  },
  {
    label: "wildcard re-export",
    pattern: /export\s+\*\s+from\s+["']\.\/topic-store-state["']/,
  },
  {
    label: "namespace re-export",
    pattern: /export\s+\*\s+as\s+\w+\s+from\s+["']\.\/topic-store-state["']/,
  },
  {
    label: "subscription permit factory re-export",
    pattern:
      /export\s+\{[^}]*\bmakeTopicStoreSubscriptionPermit\b[^}]*\}\s+from\s+["']\.\/topic-store-state["']/s,
  },
  {
    label: "local subscription permit factory re-export",
    pattern: /export\s+\{[^}]*\bmakeTopicStoreSubscriptionPermit\b[^}]*\}/s,
  },
  {
    label: "raw query metadata helper re-export",
    pattern:
      /export\s+\{[^}]*\btopicStoreRawQueryMetadata\b[^}]*\}\s+from\s+["']\.\/topic-store-state["']/s,
  },
  {
    label: "local raw query metadata helper re-export",
    pattern: /export\s+\{[^}]*\btopicStoreRawQueryMetadata\b[^}]*\}/s,
  },
  {
    label: "read model helper re-export",
    pattern:
      /export\s+\{[^}]*\btopicStoreReadModel\b[^}]*\}\s+from\s+["']\.\/topic-store-state["']/s,
  },
  {
    label: "local read model helper re-export",
    pattern: /export\s+\{[^}]*\btopicStoreReadModel\b[^}]*\}/s,
  },
  {
    label: "state helper re-export",
    pattern:
      /export\s+\{[^}]*\btopicStoreState\b[^}]*\}\s+from\s+["']\.\/topic-store-state["']/s,
  },
  {
    label: "local state helper re-export",
    pattern: /export\s+\{[^}]*\btopicStoreState\b[^}]*\}/s,
  },
] as const;

for (const path of sourceFiles(engineSourceRoot)) {
  if (isTestFile(path)) {
    continue;
  }

  const contents = readFileSync(path, "utf8");
  if (path !== topicStoreStateFile) {
    for (const restriction of restrictedTopicStoreStateExports) {
      if (!restriction.pattern.test(contents)) {
        continue;
      }
      topicStoreStateExportViolations.push(
        `${relative(repoRoot, path)} has a restricted ${restriction.label}`,
      );
    }
  }

  for (const helper of restrictedTopicStoreHelpers) {
    if (helper.allowedPaths.has(path) || !helper.pattern.test(contents)) {
      continue;
    }
    violations.push(`${relative(repoRoot, path)} uses ${helper.name}`);
  }
}

if (violations.length > 0) {
  throw new Error(
    [
      "Production engine modules must not use restricted TopicStore state helpers.",
      "Route query/read-model behavior through TopicStore helper operations instead.",
      ...violations.map((path) => `- ${path}`),
    ].join("\n"),
  );
}

if (topicStoreStateExportViolations.length > 0) {
  throw new Error(
    [
      "Production engine modules must not re-export restricted TopicStore state internals.",
      ...topicStoreStateExportViolations.map((path) => `- ${path}`),
    ].join("\n"),
  );
}

const restrictedPackageImports = [
  {
    packageName: "runtime",
    pattern: /from\s+["']@view-server\/in-memory["']/,
    message: "Production runtime must compose runtime-core directly, not the in-memory Adapter.",
  },
  {
    packageName: "in-memory",
    pattern: /from\s+["']@view-server\/column-live-view-engine["']/,
    message:
      "The in-memory Adapter must use runtime-core instead of reaching into the engine package.",
  },
] as const;

const packageImportViolations: Array<string> = [];

for (const restriction of restrictedPackageImports) {
  for (const path of sourceFiles(packageSourceRoot(restriction.packageName))) {
    if (isTestFile(path)) {
      continue;
    }
    const contents = readFileSync(path, "utf8");
    if (restriction.pattern.test(contents)) {
      packageImportViolations.push(
        `${relative(repoRoot, path)}: ${restriction.message}`,
      );
    }
  }
}

if (packageImportViolations.length > 0) {
  throw new Error(
    [
      "Package architecture seam violations found.",
      ...packageImportViolations.map((path) => `- ${path}`),
    ].join("\n"),
  );
}
