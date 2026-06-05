import { readdirSync, readFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const engineSourceRoot = join(repoRoot, "packages", "column-live-view-engine", "src");
const restrictedTopicStoreHelpers =
  /\b(makeTopicStoreSubscriptionPermit|topicStoreRawQueryMetadata|topicStoreReadModel|topicStoreState)\b/;

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

const isAllowedTopicStoreOwner = (path: string): boolean =>
  path === join(engineSourceRoot, "topic-store.ts") ||
  path === join(engineSourceRoot, "topic-store-state.ts");

const violations: Array<string> = [];

for (const path of sourceFiles(engineSourceRoot)) {
  if (isTestFile(path) || isAllowedTopicStoreOwner(path)) {
    continue;
  }

  const contents = readFileSync(path, "utf8");
  if (!restrictedTopicStoreHelpers.test(contents)) {
    continue;
  }

  violations.push(relative(repoRoot, path));
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
