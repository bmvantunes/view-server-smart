import { spawnSync } from "node:child_process";
import { cpSync, existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import {
  isDuplicateStagePublishOutput,
  packageTagName,
  oidcPublishEnvironmentViolations,
  publishedFileViolations,
  publicPackageName,
  publishDecision,
  sanitizePublicPackageJson,
  stagedPackageTagName,
  stagePublishCommandArguments,
  stripSourceMapReference,
} from "./release-publish-policy.mjs";

const packageUrl = new URL("../packages/effect-view-server/package.json", import.meta.url);
const packageJson = JSON.parse(readFileSync(packageUrl, "utf8"));
const version = packageJson.version;
const workspacePackages = [];
const workspacePackageDirectories = ["apps", "examples", "packages", "tools"];

for (const directory of workspacePackageDirectories) {
  const directoryUrl = new URL(`../${directory}/`, import.meta.url);

  if (!existsSync(directoryUrl)) {
    continue;
  }

  for (const entry of readdirSync(directoryUrl, { withFileTypes: true })) {
    if (!entry.isDirectory()) {
      continue;
    }

    const workspacePackageUrl = new URL(`${entry.name}/package.json`, directoryUrl);

    if (!existsSync(workspacePackageUrl)) {
      continue;
    }

    workspacePackages.push(JSON.parse(readFileSync(workspacePackageUrl, "utf8")));
  }
}

const decision = publishDecision({
  env: process.env,
  version,
  workspacePackages,
});

if (decision._tag === "Skip") {
  process.stdout.write(`${decision.message}\n`);
  process.exit(0);
}

if (decision._tag === "Refuse") {
  process.stderr.write(`${decision.message}\n`);
  process.exit(1);
}

const commandResult = (command, args, options = {}) => {
  const result = spawnSync(command, args, options);

  if (result.error !== undefined) {
    throw result.error;
  }

  return result;
};

const run = (command, args, options = {}) => {
  const result = commandResult(command, args, {
    stdio: "inherit",
    ...options,
  });

  if (result.status !== 0) {
    throw Object.assign(new Error(`${command} ${args.join(" ")} failed.`), {
      exitCode: result.status ?? 1,
    });
  }
};

const collectPublishedFiles = (directory, baseDirectory = directory) => {
  const files = [];

  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);

    if (entry.isDirectory()) {
      files.push(...collectPublishedFiles(path, baseDirectory));
      continue;
    }

    files.push({
      relativePath: relative(baseDirectory, path).replaceAll("\\", "/"),
      contents: readFileSync(path, "utf8"),
    });
  }

  return files;
};

const stripPublishedSourceMapReferences = (directory) => {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);

    if (entry.isDirectory()) {
      stripPublishedSourceMapReferences(path);
      continue;
    }

    if (!path.endsWith(".js") && !path.endsWith(".d.ts")) {
      continue;
    }

    writeFileSync(path, stripSourceMapReference(readFileSync(path, "utf8")));
  }
};

const assertCleanPublishedFiles = (stageDirectory) => {
  const violations = publishedFileViolations(collectPublishedFiles(stageDirectory));

  if (violations.length > 0) {
    throw new Error(
      [
        "Refusing npm stage publish because the staged package contains private workspace artifacts.",
        ...violations.map((violation) => `- ${violation}`),
      ].join("\n"),
    );
  }
};

const isPackageAlreadyCreated = () => {
  const result = commandResult("npm", ["view", publicPackageName, "name", "--json"], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  });

  return result.status === 0 && JSON.parse(result.stdout) === publicPackageName;
};

const isVersionAlreadyPublished = () => {
  const result = commandResult(
    "npm",
    ["view", `${publicPackageName}@${version}`, "version", "--json"],
    {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    },
  );

  return result.status === 0 && JSON.parse(result.stdout) === version;
};

const runStagePublish = (stageDirectory) => {
  const result = commandResult("npm", stagePublishCommandArguments(stageDirectory), {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });

  process.stdout.write(result.stdout);
  process.stderr.write(result.stderr);

  if (result.status === 0) {
    return {
      _tag: "Staged",
    };
  }

  if (
    isDuplicateStagePublishOutput({
      stderr: result.stderr,
      stdout: result.stdout,
      version,
    })
  ) {
    return {
      _tag: "AlreadyStaged",
    };
  }

  throw Object.assign(new Error(`npm ${stagePublishCommandArguments(stageDirectory).join(" ")} failed.`), {
    exitCode: result.status ?? 1,
  });
};

const gitTagExists = (tagName) => {
  const existingTag = commandResult("git", ["rev-parse", "--quiet", "--verify", `refs/tags/${tagName}`], {
    stdio: "ignore",
  });

  return existingTag.status === 0;
};

const ensureGitTag = (tagName) => {
  if (gitTagExists(tagName)) {
    return;
  }

  run("git", ["tag", "-a", tagName, "-m", tagName]);
};

let exitCode = 0;
const stageDirectory = mkdtempSync(join(tmpdir(), "effect-view-server-publish-"));

try {
  const distUrl = new URL("../packages/effect-view-server/dist/", import.meta.url);
  const distDirectory = join(stageDirectory, "dist");

  cpSync(distUrl, distDirectory, {
    recursive: true,
    filter: (source) => !source.endsWith(".map"),
  });
  stripPublishedSourceMapReferences(distDirectory);
  cpSync(new URL("../README.md", import.meta.url), join(stageDirectory, "README.md"));
  writeFileSync(
    join(stageDirectory, "package.json"),
    `${JSON.stringify(sanitizePublicPackageJson(packageJson), null, 2)}\n`,
  );

  assertCleanPublishedFiles(stageDirectory);

  if (isVersionAlreadyPublished()) {
    process.stdout.write(`${publicPackageName}@${version} is already published; ensuring git tag.\n`);
    ensureGitTag(packageTagName(version));
  } else {
    if (!isPackageAlreadyCreated()) {
      throw new Error(
        `${publicPackageName} must exist on npm before staged publishing can be used. Publish the first version manually, then rerun this workflow.`,
      );
    }

    const stagedTagName = stagedPackageTagName(version);

    if (gitTagExists(stagedTagName)) {
      process.stdout.write(`${publicPackageName}@${version} is already staged; skipping npm stage publish.\n`);
    } else {
      const oidcViolations = oidcPublishEnvironmentViolations(process.env);
      if (oidcViolations.length > 0) {
        throw new Error(
          [
            "Refusing npm stage publish because GitHub Actions OIDC is unavailable.",
            ...oidcViolations.map((violation) => `- ${violation}`),
          ].join("\n"),
        );
      }

      const stageResult = runStagePublish(stageDirectory);

      if (stageResult._tag === "AlreadyStaged") {
        process.stdout.write(
          `${publicPackageName}@${version} is already staged in npm; repairing staged marker tag.\n`,
        );
      }

      ensureGitTag(stagedTagName);
    }
  }
} catch (error) {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  exitCode =
    typeof error === "object" &&
    error !== null &&
    "exitCode" in error &&
    typeof error.exitCode === "number"
      ? error.exitCode
      : 1;
} finally {
  rmSync(stageDirectory, {
    force: true,
    recursive: true,
  });
}

process.exit(exitCode);
