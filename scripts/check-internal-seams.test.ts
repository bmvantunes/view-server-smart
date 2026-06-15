import { describe, expect, it } from "@effect/vitest";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import {
  packageImportViolationsFor,
  packageRelativeImportViolationsFor,
  sourceFiles,
  sourceWithoutComments,
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
});
