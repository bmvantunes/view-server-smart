import { readFileSync, writeFileSync } from "node:fs";

const tsNoCheck = ["// @ts", "nocheck"].join("-");

const cleanRouteTree = (file) => {
  const source = readFileSync(file, "utf8");
  const cleaned = source
    .replace("/* eslint-disable */\n\n", "")
    .replace(`${tsNoCheck}\n\n`, "")
    .replace("// noinspection JSUnusedGlobalSymbols\n\n", "")
    .replace(/\nimport type \{ createStart \} from ["']@tanstack\/react-start["']/g, "")
    .replace(/\nimport type \{ getRouter \} from ["']\.\/router\.tsx["'];?\ndeclare module ["']@tanstack\/react-start["'] \{[\s\S]*$/g, "\n")
    .replace(
      "const IndexRoute = IndexRouteImport.update({\n  id:",
      "const IndexRoute = IndexRouteImport.update({\n  // @ts-expect-error TanStack Router route registration includes generated id metadata.\n  id:",
    )
    .replace(/\}\s+as\s+any\)/g, "})");

  writeFileSync(file, cleaned);
};

for (const file of process.argv.slice(2)) {
  cleanRouteTree(file);
}
