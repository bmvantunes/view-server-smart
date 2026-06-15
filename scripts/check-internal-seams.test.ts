import { describe, expect, it } from "@effect/vitest";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import {
  assertNoPackageImportViolations,
  assertNoEngineSeamViolations,
  collectEngineSeamViolations,
  collectPackageImportViolations,
  importSpecifiersFromSource,
  packageImportViolationsFor,
  packageImportViolationsForFile,
  packageImportViolationMessage,
  packageRelativeImportViolationsFor,
  sourceFiles,
  sourceWithoutComments,
  topicStoreHelperViolationMessage,
  topicStoreHelperViolationsForFile,
  topicStoreStateExportViolationMessage,
  topicStoreStateExportViolationsForFile,
  toPosixRelativePath,
} from "./check-internal-seams";

const makeDirectory = () => mkdtempSync(join(tmpdir(), "view-server-internal-seams-"));

describe("internal seam checker", () => {
  it("scans TypeScript and TSX source files recursively", () => {
    const directory = makeDirectory();
    const nested = join(directory, "nested");
    mkdirSync(nested);
    writeFileSync(join(directory, "index.ts"), "");
    writeFileSync(join(directory, "component.tsx"), "");
    writeFileSync(join(directory, "ignore.js"), "");
    writeFileSync(join(nested, "testing.tsx"), "");

    expect(sourceFiles(directory).map((path) => basename(path)).sort()).toStrictEqual([
      "component.tsx",
      "index.ts",
      "testing.tsx",
    ]);
  });

  it("collects engine seam violations for restricted helpers and re-exports", () => {
    const engineFile = join(
      process.cwd(),
      "packages",
      "column-live-view-engine",
      "src",
      "engine.ts",
    );
    const indexFile = join(
      process.cwd(),
      "packages",
      "column-live-view-engine",
      "src",
      "index.ts",
    );

    expect(
      topicStoreHelperViolationsForFile({
        contents: "const helper = topicStoreState;",
        path: engineFile,
      }),
    ).toStrictEqual(["packages/column-live-view-engine/src/engine.ts uses topicStoreState"]);
    expect(
      topicStoreStateExportViolationsForFile({
        contents: 'export { topicStoreState } from "./topic-store-state";',
        path: indexFile,
      }),
    ).toStrictEqual([
      "packages/column-live-view-engine/src/index.ts has a restricted state helper re-export",
      "packages/column-live-view-engine/src/index.ts has a restricted local state helper re-export",
    ]);
  });

  it("allows restricted engine helpers in their owning files", () => {
    const topicStoreStateFile = join(
      process.cwd(),
      "packages",
      "column-live-view-engine",
      "src",
      "topic-store-state.ts",
    );

    expect(
      topicStoreHelperViolationsForFile({
        contents: "const helper = topicStoreState;",
        path: topicStoreStateFile,
      }),
    ).toStrictEqual([]);
  });

  it("formats and throws engine seam violation summaries", () => {
    const helperViolations = ["packages/column-live-view-engine/src/engine.ts uses topicStoreState"];
    const stateExportViolations = [
      "packages/column-live-view-engine/src/index.ts has a restricted state helper re-export",
    ];

    expect(topicStoreHelperViolationMessage(helperViolations)).toStrictEqual(
      [
        "Production engine modules must not use restricted TopicStore state helpers.",
        "Route query/read-model behavior through TopicStore helper operations instead.",
        "- packages/column-live-view-engine/src/engine.ts uses topicStoreState",
      ].join("\n"),
    );
    expect(topicStoreStateExportViolationMessage(stateExportViolations)).toStrictEqual(
      [
        "Production engine modules must not re-export restricted TopicStore state internals.",
        "- packages/column-live-view-engine/src/index.ts has a restricted state helper re-export",
      ].join("\n"),
    );
    expect(() =>
      assertNoEngineSeamViolations({
        helperViolations,
        stateExportViolations: [],
      }),
    ).toThrowError("Production engine modules must not use restricted TopicStore state helpers.");
    expect(() =>
      assertNoEngineSeamViolations({
        helperViolations: [],
        stateExportViolations,
      }),
    ).toThrowError("Production engine modules must not re-export restricted TopicStore state internals.");
    expect(
      assertNoEngineSeamViolations({
        helperViolations: [],
        stateExportViolations: [],
      }),
    ).toStrictEqual(undefined);
  });

  it("keeps the current engine source free of internal seam violations", () => {
    expect(collectEngineSeamViolations()).toStrictEqual({
      helperViolations: [],
      stateExportViolations: [],
    });
  });

  it("reports restricted package imports including subexports and dynamic imports", () => {
    const restriction = {
      forbiddenSpecifiers: new Set([
        "@view-server/in-memory",
        "@view-server/runtime",
        "@view-server/server",
      ]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'import "@view-server/runtime";',
          'import { createInMemoryViewServer } from "@view-server/in-memory";',
          'const runtime = import("@view-server/runtime/internal");',
          "const server = import(`@view-server/server`);",
          'import type { ViewServerLiveClient } from "@view-server/client";',
        ].join("\n"),
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @view-server/runtime: React production must stay transport-neutral.",
      "src/index.tsx imports @view-server/in-memory: React production must stay transport-neutral.",
      "src/index.tsx imports @view-server/runtime/internal: View Server imports must use approved package exports.",
      "src/index.tsx imports @view-server/server: React production must stay transport-neutral.",
    ]);
  });

  it("rejects deep imports even when the package root is allowed", () => {
    const restriction = {
      allowedSpecifiers: new Set(["@view-server/client"]),
      forbiddenSpecifiers: new Set(["@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'import type { ViewServerLiveClient } from "@view-server/client";',
          'import { makeViewServerClient } from "@view-server/client/remote/internal";',
        ].join("\n"),
        relativePath: "src/index.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.tsx imports @view-server/client/remote/internal: View Server imports must use approved package exports.",
    ]);
  });

  it("rejects approved subexports that are not explicitly allowed for a package", () => {
    const restriction = {
      allowedSpecifiers: new Set(["@view-server/client"]),
      forbiddenSpecifiers: new Set(["@view-server/client"]),
      message: "Server code may depend on client contracts only.",
      packageName: "server",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'import type { ViewServerLiveClient } from "@view-server/client";',
          'import { makeViewServerClient } from "@view-server/client/remote";',
        ].join("\n"),
        relativePath: "src/index.ts",
        restriction,
      }),
    ).toStrictEqual([
      "src/index.ts imports @view-server/client/remote: Server code may depend on client contracts only.",
    ]);
  });

  it("allows intentionally carved testing entrypoints", () => {
    const restriction = {
      allowedRelativePathSpecifiers: new Map([
        ["src/testing.tsx", new Set(["@view-server/in-memory"])],
      ]),
      forbiddenSpecifiers: new Set(["@view-server/in-memory"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'import { createInMemoryViewServer } from "@view-server/in-memory";',
        relativePath: "src/testing.tsx",
        restriction,
      }),
    ).toStrictEqual([]);
  });

  it("matches relative path carveouts across path separators", () => {
    const restriction = {
      allowedRelativePathSpecifiers: new Map([
        ["src/testing.tsx", new Set(["@view-server/in-memory"])],
      ]),
      forbiddenSpecifiers: new Set(["@view-server/in-memory"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: 'import { createInMemoryViewServer } from "@view-server/in-memory";',
        relativePath: toPosixRelativePath("src\\testing.tsx"),
        restriction,
      }),
    ).toStrictEqual([]);
  });

  it("does not allow testing entrypoint carveouts to hide unrelated forbidden packages", () => {
    const restriction = {
      allowedRelativePathSpecifiers: new Map([
        ["src/testing.tsx", new Set(["@view-server/in-memory"])],
      ]),
      forbiddenSpecifiers: new Set(["@view-server/in-memory", "@view-server/runtime"]),
      message: "React production must stay transport-neutral.",
      packageName: "react",
    };

    expect(
      packageImportViolationsFor({
        contents: [
          'import { createInMemoryViewServer } from "@view-server/in-memory";',
          'import { createViewServerRuntime } from "@view-server/runtime";',
        ].join("\n"),
        relativePath: "src/testing.tsx",
        restriction,
      }),
    ).toStrictEqual([
      "src/testing.tsx imports @view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("rejects relative imports that escape a package root", () => {
    const packageRoot = join("/repo", "packages", "react");
    const path = join(packageRoot, "src", "index.tsx");

    expect(
      packageRelativeImportViolationsFor({
        contents: [
          'import { local } from "./internal";',
          'import { createViewServerRuntime } from "../../runtime/src/index";',
        ].join("\n"),
        packageRoot,
        path,
      }),
    ).toStrictEqual([
      "src/index.tsx imports ../../runtime/src/index: relative imports must not cross package seams.",
    ]);
  });

  it("collects relative and package import violations for a package file", () => {
    const packageRoot = join("/repo", "packages", "react");
    const path = join(packageRoot, "src", "index.tsx");

    expect(
      packageImportViolationsForFile({
        contents: [
          'import { local } from "./internal";',
          'import { createViewServerRuntime } from "@view-server/runtime";',
          'import { server } from "../../server/src/index";',
        ].join("\n"),
        packageRoot,
        path,
        restriction: {
          allowedSpecifiers: new Set(["@view-server/client"]),
          forbiddenSpecifiers: new Set(["@view-server/runtime"]),
          message: "React production must stay transport-neutral.",
          packageName: "react",
        },
      }),
    ).toStrictEqual([
      "packages/react/src/index.tsx imports ../../server/src/index: relative imports must not cross package seams.",
      "packages/react/src/index.tsx imports @view-server/runtime: React production must stay transport-neutral.",
    ]);
  });

  it("formats and throws package import violation summaries", () => {
    const violations = ["packages/react/src/index.tsx imports @view-server/runtime: no"];

    expect(packageImportViolationMessage(violations)).toStrictEqual(
      [
        "Package architecture seam violations found.",
        "- packages/react/src/index.tsx imports @view-server/runtime: no",
      ].join("\n"),
    );
    expect(() => assertNoPackageImportViolations(violations)).toThrowError(
      "Package architecture seam violations found.",
    );
    expect(assertNoPackageImportViolations([])).toStrictEqual(undefined);
  });

  it("keeps the current repository free of package import violations", () => {
    expect(collectPackageImportViolations()).toStrictEqual([]);
  });

  it("ignores import-like text in comments", () => {
    expect(
      sourceWithoutComments(
        [
          'import { client } from "@view-server/client";',
          '// import { runtime } from "@view-server/runtime";',
          '/* import { server } from "@view-server/server"; */',
          'const example = "import from comment-like string";',
        ].join("\n"),
      ),
    ).toStrictEqual(
      [
        'import { client } from "@view-server/client";',
        "",
        "",
        'const example = "import from comment-like string";',
      ].join("\n"),
    );
  });

  it("does not treat import-like text inside strings as imports", () => {
    expect(
      importSpecifiersFromSource(
        [
          'const message = "Do not import from \\"@view-server/runtime\\"";',
          "const docs = `import { server } from \"@view-server/server\"`;",
          'import { client } from "@view-server/client";',
          "const runtime = import(`@view-server/runtime`);",
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/client", "@view-server/runtime"]);
  });

  it("detects imports inside template literal expressions", () => {
    expect(
      importSpecifiersFromSource(
        [
          "const text = `plain import { server } from \"@view-server/server\"`;",
          "const runtime = `${await import(\"@view-server/runtime\")}`;",
        ].join("\n"),
      ),
    ).toStrictEqual(["@view-server/runtime"]);
  });

  it("handles unfinished template literal expressions conservatively", () => {
    expect(importSpecifiersFromSource("const text = `${await import(")).toStrictEqual([]);
  });

  it("handles unfinished plain template literals conservatively", () => {
    expect(importSpecifiersFromSource("const text = `unterminated")).toStrictEqual([]);
  });

  it("ignores unterminated quoted import specifiers", () => {
    expect(importSpecifiersFromSource('const broken = import("@view-server/runtime')).toStrictEqual(
      [],
    );
  });

  it("ignores from keywords that are not followed by quoted specifiers", () => {
    expect(importSpecifiersFromSource("from notAString")).toStrictEqual([]);
  });
});
