import { readdirSync, readFileSync } from "node:fs";
import { dirname, isAbsolute, join, relative, resolve } from "node:path";
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
    allowedPaths: new Set([topicStoreQueryFile, topicStoreStateFile]),
  },
  {
    name: "topicStoreState",
    pattern: /\btopicStoreState\b/,
    allowedPaths: new Set([topicStoreMutationFile, topicStoreStateFile]),
  },
] as const;

export const sourceFiles = (directory: string): ReadonlyArray<string> => {
  const entries = readdirSync(directory, { withFileTypes: true });
  const files: Array<string> = [];

  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      files.push(...sourceFiles(path));
      continue;
    }
    if (entry.isFile() && (entry.name.endsWith(".ts") || entry.name.endsWith(".tsx"))) {
      files.push(path);
    }
  }

  return files;
};

const isTestFile = (path: string): boolean =>
  path.endsWith(".test.ts") ||
  path.endsWith(".test.tsx") ||
  path.endsWith(".test-d.ts") ||
  path.endsWith(".bench.ts") ||
  path.endsWith(".bench.tsx");

type RestrictedPackageImport = {
  readonly allowedRelativePathSpecifiers?: ReadonlyMap<string, ReadonlySet<string>>;
  readonly allowedSpecifiers?: ReadonlySet<string>;
  readonly forbiddenSpecifiers: ReadonlySet<string>;
  readonly message: string;
  readonly packageName: string;
};

const isViewServerSpecifier = (specifier: string): boolean =>
  specifier === "@view-server" || specifier.startsWith("@view-server/");

const previousNonWhitespaceCharacter = (contents: string, index: number): string | undefined => {
  let nextIndex = index;
  while (nextIndex >= 0) {
    const character = contents.charAt(nextIndex);
    if (!/\s/.test(character)) {
      return character;
    }
    nextIndex -= 1;
  }
  return undefined;
};

const isRegexLiteralStartContext = (contents: string, index: number): boolean => {
  const previous = previousNonWhitespaceCharacter(contents, index - 1);
  const previousSource = contents.slice(0, index).trimEnd();
  return (
    previous === undefined ||
    previous === "(" ||
    previous === "[" ||
    previous === "{" ||
    previous === "=" ||
    previous === ":" ||
    previous === "," ||
    previous === "?" ||
    previous === ">" ||
    previous === ";" ||
    previous === "&" ||
    previous === "|" ||
    previous === "!" ||
    previous === "+" ||
    previous === "-" ||
    previous === "*" ||
    previous === "~" ||
    previous === "^" ||
    previous === "<" ||
    /\b(?:return|throw|case|yield|await|typeof|void|delete|in|of|instanceof|else|do)$/.test(previousSource) ||
    /\b(?:if|while|for|with)\s*\([\s\S]*\)$/.test(previousSource)
  );
};

const skipRegexLiteral = (contents: string, index: number): number => {
  let insideCharacterClass = false;
  let nextIndex = index + 1;

  while (nextIndex < contents.length) {
    const character = contents.charAt(nextIndex);
    if (character === "\n") {
      return index + 1;
    }
    if (character === "\\") {
      nextIndex += 2;
      continue;
    }
    if (character === "[") {
      insideCharacterClass = true;
      nextIndex += 1;
      continue;
    }
    if (character === "]") {
      insideCharacterClass = false;
      nextIndex += 1;
      continue;
    }
    if (character === "/" && !insideCharacterClass) {
      nextIndex += 1;
      while (/[A-Za-z]/.test(contents.charAt(nextIndex))) {
        nextIndex += 1;
      }
      return nextIndex;
    }
    nextIndex += 1;
  }

  return index + 1;
};

export const sourceWithoutComments = (contents: string): string => {
  let output = "";
  let index = 0;
  let quote: '"' | "'" | "`" | undefined;
  let lineComment = false;
  let blockComment = false;

  while (index < contents.length) {
    const character = contents.charAt(index);
    const nextCharacter = contents.charAt(index + 1);

    if (lineComment) {
      if (character === "\n") {
        lineComment = false;
        output += character;
      }
      index += 1;
      continue;
    }

    if (blockComment) {
      if (character === "\n") {
        output += character;
      }
      if (character === "*" && nextCharacter === "/") {
        blockComment = false;
        index += 2;
        continue;
      }
      index += 1;
      continue;
    }

    if (quote !== undefined) {
      output += character;
      if (character === "\\") {
        output += nextCharacter;
        index += 2;
        continue;
      }
      if (character === quote) {
        quote = undefined;
      }
      index += 1;
      continue;
    }

    if (isJsxTagStart(contents, index)) {
      const jsxElement = jsxElementImportSpecifiers(contents, index);
      const nextIndex = jsxElement.nextIndex;
      output += contents.slice(index, nextIndex);
      index = nextIndex;
      continue;
    }

    if (
      character === "/" &&
      nextCharacter !== "/" &&
      nextCharacter !== "*" &&
      isRegexLiteralStartContext(contents, index)
    ) {
      const nextIndex = skipRegexLiteral(contents, index);
      output += contents.slice(index, nextIndex);
      index = nextIndex;
      continue;
    }

    if (character === "/" && nextCharacter === "/") {
      lineComment = true;
      index += 2;
      continue;
    }

    if (character === "/" && nextCharacter === "*") {
      blockComment = true;
      index += 2;
      continue;
    }

    if (character === '"' || character === "'" || character === "`") {
      quote = character;
    }
    output += character;
    index += 1;
  }

  return output;
};

const importedViewServerSpecifiers = (contents: string): ReadonlyArray<string> =>
  importSpecifiersFromSource(contents).filter(isViewServerSpecifier);

const specifierMatches = (specifier: string, packageSpecifier: string): boolean =>
  specifier === packageSpecifier || specifier.startsWith(`${packageSpecifier}/`);

const isImportQuote = (character: string): character is '"' | "'" | "`" =>
  character === '"' || character === "'" || character === "`";

const identifierCharacterPattern = /[A-Za-z0-9_$]/;

const isIdentifierCharacter = (character: string | undefined): boolean =>
  character !== undefined && identifierCharacterPattern.test(character);

const isValidCodePoint = (codePoint: number): boolean =>
  Number.isFinite(codePoint) && codePoint >= 0 && codePoint <= 0x10ffff;

const readEscapedIdentifierCharacter = (
  contents: string,
  index: number,
): { readonly character: string; readonly nextIndex: number } | undefined => {
  if (contents.charAt(index) !== "\\" || contents.charAt(index + 1) !== "u") {
    return undefined;
  }
  if (contents.charAt(index + 2) === "{") {
    const closeBraceIndex = contents.indexOf("}", index + 3);
    const hex = contents.slice(index + 3, Math.max(index + 3, closeBraceIndex));
    const codePoint = Number.parseInt(hex, 16);
    return /^[0-9A-Fa-f]+$/.test(hex) && isValidCodePoint(codePoint)
      ? {
          character: String.fromCodePoint(codePoint),
          nextIndex: closeBraceIndex + 1,
        }
      : undefined;
  }
  const hex = contents.slice(index + 2, index + 6);
  const codePoint = Number.parseInt(hex, 16);
  return /^[0-9A-Fa-f]{4}$/.test(hex) && isValidCodePoint(codePoint)
    ? {
        character: String.fromCodePoint(codePoint),
        nextIndex: index + 6,
      }
    : undefined;
};

const readIdentifierNameAt = (
  contents: string,
  index: number,
): { readonly name: string; readonly nextIndex: number } | undefined => {
  let name = "";
  let nextIndex = index;
  while (nextIndex < contents.length) {
    const escaped = readEscapedIdentifierCharacter(contents, nextIndex);
    if (escaped !== undefined) {
      name += escaped.character;
      nextIndex = escaped.nextIndex;
      continue;
    }
    const character = contents.charAt(nextIndex);
    if (!isIdentifierCharacter(character)) {
      break;
    }
    name += character;
    nextIndex += 1;
  }
  return name === ""
    ? undefined
    : {
        name,
        nextIndex,
      };
};

const afterKeywordAt = (contents: string, index: number, keyword: string): number | undefined => {
  const identifier = readIdentifierNameAt(contents, index);
  return identifier?.name === keyword &&
    !isIdentifierCharacter(contents[index - 1]) &&
    !isIdentifierCharacter(contents[identifier.nextIndex])
    ? identifier.nextIndex
    : undefined;
};

const skipWhitespace = (contents: string, index: number): number => {
  let nextIndex = index;
  while (nextIndex < contents.length && /\s/.test(contents.charAt(nextIndex))) {
    nextIndex += 1;
  }
  return nextIndex;
};

const readBracketAccessorAt = (
  contents: string,
  index: number,
  property: string,
): number | undefined => {
  if (contents.charAt(index) !== "[") {
    return undefined;
  }
  const propertySpecifier = readQuotedSpecifier(contents, skipWhitespace(contents, index + 1));
  if (propertySpecifier?.specifier !== property) {
    return undefined;
  }
  const closeBracketIndex = skipWhitespace(contents, propertySpecifier.nextIndex);
  return contents.charAt(closeBracketIndex) === "]" ? closeBracketIndex + 1 : undefined;
};

const readPropertyAccessor = (
  contents: string,
  index: number,
  property: string,
): number | undefined => {
  const nextIndex = skipWhitespace(contents, index);
  if (contents.charAt(nextIndex) === ".") {
    const propertyIndex = skipWhitespace(contents, nextIndex + 1);
    return afterKeywordAt(contents, propertyIndex, property);
  }
  const bracketAccessor = readBracketAccessorAt(contents, nextIndex, property);
  if (bracketAccessor !== undefined) {
    return bracketAccessor;
  }
  if (contents.charAt(nextIndex) !== "?") {
    return undefined;
  }
  const dotIndex = skipWhitespace(contents, nextIndex + 1);
  if (contents.charAt(dotIndex) !== ".") {
    return undefined;
  }
  const propertyIndex = skipWhitespace(contents, dotIndex + 1);
  const optionalBracketAccessor = readBracketAccessorAt(contents, propertyIndex, property);
  if (optionalBracketAccessor !== undefined) {
    return optionalBracketAccessor;
  }
  return afterKeywordAt(contents, propertyIndex, property);
};

const readOptionalCallOpenParen = (contents: string, index: number): number | undefined => {
  const nextIndex = skipWhitespace(contents, index);
  if (contents.charAt(nextIndex) !== "?") {
    return undefined;
  }
  const dotIndex = skipWhitespace(contents, nextIndex + 1);
  if (contents.charAt(dotIndex) !== ".") {
    return undefined;
  }
  const openParenIndex = skipWhitespace(contents, dotIndex + 1);
  return contents.charAt(openParenIndex) === "(" ? openParenIndex : undefined;
};

const readQuotedSpecifier = (
  contents: string,
  index: number,
): { readonly nextIndex: number; readonly specifier: string } | undefined => {
  const quote = contents.charAt(index);
  if (!isImportQuote(quote)) {
    return undefined;
  }

  let specifier = "";
  let nextIndex = index + 1;
  while (nextIndex < contents.length) {
    const character = contents.charAt(nextIndex);
    const nextCharacter = contents.charAt(nextIndex + 1);
    if (character === "\\") {
      if (nextCharacter === "u") {
        if (contents.charAt(nextIndex + 2) === "{") {
          const closeBraceIndex = contents.indexOf("}", nextIndex + 3);
          const hex = contents.slice(
            nextIndex + 3,
            Math.max(nextIndex + 3, closeBraceIndex),
          );
          const codePoint = Number.parseInt(hex, 16);
          if (/^[0-9A-Fa-f]+$/.test(hex) && isValidCodePoint(codePoint)) {
            specifier += String.fromCodePoint(codePoint);
            nextIndex = closeBraceIndex + 1;
            continue;
          }
        }
        const hex = contents.slice(nextIndex + 2, nextIndex + 6);
        const codePoint = Number.parseInt(hex, 16);
        if (/^[0-9A-Fa-f]{4}$/.test(hex) && isValidCodePoint(codePoint)) {
          specifier += String.fromCodePoint(codePoint);
          nextIndex += 6;
          continue;
        }
      }
      if (nextCharacter === "x") {
        const hex = contents.slice(nextIndex + 2, nextIndex + 4);
        const codePoint = Number.parseInt(hex, 16);
        if (/^[0-9A-Fa-f]{2}$/.test(hex) && isValidCodePoint(codePoint)) {
          specifier += String.fromCodePoint(codePoint);
          nextIndex += 4;
          continue;
        }
      }
      specifier += nextCharacter;
      nextIndex += nextCharacter === "" ? 1 : 2;
      continue;
    }
    if (character === quote) {
      return {
        nextIndex: nextIndex + 1,
        specifier,
      };
    }
    specifier += character;
    nextIndex += 1;
  }

  return undefined;
};

const readStaticQuotedSpecifier = (
  contents: string,
  index: number,
): { readonly nextIndex: number; readonly specifier: string } | undefined => {
  const quoted = readQuotedSpecifier(contents, index);
  if (quoted === undefined) {
    return undefined;
  }
  if (
    contents.charAt(index) === "`" &&
    quoted.specifier.includes("${") &&
    !isViewServerSpecifier(quoted.specifier)
  ) {
    return undefined;
  }
  return quoted;
};

const skipQuotedLiteral = (contents: string, index: number): number =>
  readQuotedSpecifier(contents, index)?.nextIndex ?? index + 1;

const isJsxStartContext = (contents: string, index: number): boolean => {
  const previous = previousNonWhitespaceCharacter(contents, index - 1);
  const previousSource = contents.slice(0, index).trimEnd();
  return (
    previous === undefined ||
    previous === "(" ||
    previous === "[" ||
    previous === "{" ||
    previous === "=" ||
    previous === ":" ||
    previous === "," ||
    previous === "?" ||
    previous === ">" ||
    previous === ";" ||
    previous === "&" ||
    previous === "|" ||
    previous === "!" ||
    /\breturn$/.test(previousSource)
  );
};

const isJsxTagStart = (contents: string, index: number): boolean => {
  const nextCharacter = contents.charAt(index + 1);
  return (
    contents.charAt(index) === "<" &&
    (nextCharacter === ">" || /[A-Za-z_$/.]/.test(nextCharacter)) &&
    isJsxStartContext(contents, index)
  );
};

const isNestedJsxTagStart = (contents: string, index: number): boolean => {
  const nextCharacter = contents.charAt(index + 1);
  return contents.charAt(index) === "<" && (nextCharacter === ">" || /[A-Za-z_$/.]/.test(nextCharacter));
};

const freeRequireKeywordEndAt = (contents: string, index: number): number | undefined => {
  const previous = previousNonWhitespaceCharacter(contents, index - 1);
  const afterRequire = afterKeywordAt(contents, index, "require");
  return afterRequire !== undefined && previous !== "." && previous !== "#" ? afterRequire : undefined;
};

const freeCreateRequireKeywordEndAt = (
  contents: string,
  index: number,
): number | undefined => {
  const previous = previousNonWhitespaceCharacter(contents, index - 1);
  const afterCreateRequire = afterKeywordAt(contents, index, "createRequire");
  return afterCreateRequire !== undefined && previous !== "." && previous !== "#"
    ? afterCreateRequire
    : undefined;
};

const moduleRequireAccessorAt = (contents: string, index: number): number | undefined => {
  const previous = previousNonWhitespaceCharacter(contents, index - 1);
  if (previous === "." || previous === "#") {
    return undefined;
  }
  const afterModule = afterKeywordAt(contents, index, "module");
  if (afterModule === undefined) {
    return undefined;
  }
  return readAccessorAfterCallee(contents, index, afterModule, "require");
};

const callOpenParenIndex = (contents: string, index: number): number | undefined => {
  const nextIndex = skipWhitespace(contents, index);
  if (contents[nextIndex] === "(") {
    return nextIndex;
  }
  return readOptionalCallOpenParen(contents, nextIndex);
};

const resolveAccessorAfterRequire = (
  contents: string,
  requireStartIndex: number,
  index: number,
): number | undefined => {
  return readAccessorAfterCallee(contents, requireStartIndex, index, "resolve");
};

const importMetaResolveAccessorAt = (contents: string, index: number): number | undefined => {
  const previous = previousNonWhitespaceCharacter(contents, index - 1);
  if (previous === "." || previous === "#") {
    return undefined;
  }
  const afterImportKeyword = afterKeywordAt(contents, index, "import");
  if (afterImportKeyword === undefined) {
    return undefined;
  }
  const afterImport = skipWhitespace(contents, afterImportKeyword);
  if (contents.charAt(afterImport) !== ".") {
    return undefined;
  }
  const metaIndex = skipWhitespace(contents, afterImport + 1);
  const afterMeta = afterKeywordAt(contents, metaIndex, "meta");
  if (afterMeta === undefined) {
    return undefined;
  }
  return readAccessorAfterCallee(contents, index, afterMeta, "resolve");
};

const readCallSpecifier = (
  contents: string,
  index: number,
): { readonly nextIndex: number; readonly specifier: string } | undefined => {
  const openParen = callOpenParenIndex(contents, index);
  if (openParen === undefined) {
    return undefined;
  }
  return readStaticQuotedSpecifier(contents, skipWhitespace(contents, openParen + 1));
};

const readCallSecondSpecifier = (
  contents: string,
  index: number,
): { readonly nextIndex: number; readonly specifier: string } | undefined => {
  const openParen = callOpenParenIndex(contents, index);
  if (openParen === undefined) {
    return undefined;
  }
  let depth = 0;
  let nextIndex = skipWhitespace(contents, openParen + 1);
  while (nextIndex < contents.length) {
    const character = contents.charAt(nextIndex);
    const nextCharacter = contents.charAt(nextIndex + 1);
    if (character === '"' || character === "'" || character === "`") {
      nextIndex = skipQuotedLiteral(contents, nextIndex);
      continue;
    }
    if (
      character === "/" &&
      nextCharacter !== "/" &&
      nextCharacter !== "*" &&
      isRegexLiteralStartContext(contents, nextIndex)
    ) {
      nextIndex = skipRegexLiteral(contents, nextIndex);
      continue;
    }
    if (character === "(" || character === "[" || character === "{") {
      depth += 1;
      nextIndex += 1;
      continue;
    }
    if (character === ")" && depth === 0) {
      return undefined;
    }
    if (character === ")" || character === "]" || character === "}") {
      depth -= 1;
      nextIndex += 1;
      continue;
    }
    if (character === "," && depth === 0) {
      return readStaticQuotedSpecifier(contents, skipWhitespace(contents, nextIndex + 1));
    }
    nextIndex += 1;
  }
  return undefined;
};

const readCallExpressionEnd = (contents: string, index: number): number | undefined => {
  const openParen = callOpenParenIndex(contents, index);
  if (openParen === undefined) {
    return undefined;
  }
  let depth = 0;
  let nextIndex = openParen + 1;
  while (nextIndex < contents.length) {
    const character = contents.charAt(nextIndex);
    const nextCharacter = contents.charAt(nextIndex + 1);
    if (character === '"' || character === "'" || character === "`") {
      nextIndex = skipQuotedLiteral(contents, nextIndex);
      continue;
    }
    if (
      character === "/" &&
      nextCharacter !== "/" &&
      nextCharacter !== "*" &&
      isRegexLiteralStartContext(contents, nextIndex)
    ) {
      nextIndex = skipRegexLiteral(contents, nextIndex);
      continue;
    }
    if (character === "(" || character === "[" || character === "{") {
      depth += 1;
      nextIndex += 1;
      continue;
    }
    if (character === ")" && depth === 0) {
      return nextIndex + 1;
    }
    if (character === ")" || character === "]" || character === "}") {
      depth -= 1;
      nextIndex += 1;
      continue;
    }
    nextIndex += 1;
  }
  return undefined;
};

const readApplyArraySpecifier = (
  contents: string,
  index: number,
): { readonly nextIndex: number; readonly specifier: string } | undefined => {
  const openParen = callOpenParenIndex(contents, index);
  if (openParen === undefined) {
    return undefined;
  }
  let depth = 0;
  let nextIndex = openParen + 1;
  while (nextIndex < contents.length) {
    const character = contents.charAt(nextIndex);
    const nextCharacter = contents.charAt(nextIndex + 1);
    if (character === '"' || character === "'" || character === "`") {
      nextIndex = skipQuotedLiteral(contents, nextIndex);
      continue;
    }
    if (
      character === "/" &&
      nextCharacter !== "/" &&
      nextCharacter !== "*" &&
      isRegexLiteralStartContext(contents, nextIndex)
    ) {
      nextIndex = skipRegexLiteral(contents, nextIndex);
      continue;
    }
    if (character === "(" || character === "[" || character === "{") {
      depth += 1;
      nextIndex += 1;
      continue;
    }
    if (character === ")" && depth === 0) {
      return undefined;
    }
    if (character === ")" || character === "]" || character === "}") {
      depth -= 1;
      nextIndex += 1;
      continue;
    }
    if (character === "," && depth === 0) {
      const openArrayIndex = skipWhitespace(contents, nextIndex + 1);
      if (contents.charAt(openArrayIndex) !== "[") {
        return undefined;
      }
      const specifier = readStaticQuotedSpecifier(
        contents,
        skipWhitespace(contents, openArrayIndex + 1),
      );
      if (specifier === undefined) {
        return undefined;
      }
      return {
        nextIndex: specifier.nextIndex,
        specifier: specifier.specifier,
      };
    }
    nextIndex += 1;
  }
  return undefined;
};

const matchingCloseParenIndexFromOpen = (
  contents: string,
  openParenIndex: number,
): number | undefined => {
  let depth = 0;
  let nextIndex = openParenIndex + 1;
  let closeParenIndex: number | undefined;
  while (nextIndex < contents.length) {
    const character = contents.charAt(nextIndex);
    const nextCharacter = contents.charAt(nextIndex + 1);
    if (character === '"' || character === "'" || character === "`") {
      nextIndex = skipQuotedLiteral(contents, nextIndex);
      continue;
    }
    if (
      character === "/" &&
      nextCharacter !== "/" &&
      nextCharacter !== "*" &&
      isRegexLiteralStartContext(contents, nextIndex)
    ) {
      nextIndex = skipRegexLiteral(contents, nextIndex);
      continue;
    }
    if (character === "(") {
      depth += 1;
      nextIndex += 1;
      continue;
    }
    if (character === ")") {
      if (depth === 0) {
        closeParenIndex = nextIndex;
        break;
      }
      depth -= 1;
      nextIndex += 1;
      continue;
    }
    nextIndex += 1;
  }
  return closeParenIndex;
};

const endsWithControlCondition = (contents: string): boolean => {
  const trimmed = contents.trimEnd();
  const controlKeywords = ["if", "while", "with", "for", "switch"];
  let index = 0;
  while (index < trimmed.length) {
    for (const keyword of controlKeywords) {
      const afterKeyword = afterKeywordAt(trimmed, index, keyword);
      if (afterKeyword === undefined) {
        continue;
      }
      let openParenIndex = skipWhitespace(trimmed, afterKeyword);
      if (keyword === "for") {
        const afterAwait = afterKeywordAt(trimmed, openParenIndex, "await");
        if (afterAwait !== undefined) {
          openParenIndex = skipWhitespace(trimmed, afterAwait);
        }
      }
      if (trimmed.charAt(openParenIndex) !== "(") {
        continue;
      }
      const closeParenIndex = matchingCloseParenIndexFromOpen(trimmed, openParenIndex);
      if (closeParenIndex !== undefined && skipWhitespace(trimmed, closeParenIndex + 1) === trimmed.length) {
        return true;
      }
    }
    index += 1;
  }
  return false;
};

const wrappingOpenParenCountBefore = (contents: string, index: number): number => {
  let count = 0;
  let nextIndex = index;
  while (nextIndex > 0) {
    const before = contents.slice(0, nextIndex).trimEnd();
    if (before.at(-1) !== "(") {
      return count;
    }
    const beforeOpenParen = before.slice(0, -1).trimEnd();
    const precedingCharacter = beforeOpenParen.at(-1);
    const previousToken = beforeOpenParen.match(/[A-Za-z_$][\w$]*$/)?.[0];
    const followsKeyword =
      previousToken === "return" ||
      previousToken === "await" ||
      previousToken === "void" ||
      previousToken === "throw" ||
      previousToken === "yield" ||
      previousToken === "delete" ||
      previousToken === "typeof" ||
      previousToken === "new" ||
      previousToken === "else" ||
      previousToken === "do";
    const followsControlCondition =
      precedingCharacter === ")" && endsWithControlCondition(beforeOpenParen);
    const isExpressionGrouping =
      precedingCharacter === undefined ||
      precedingCharacter === "(" ||
      followsKeyword ||
      followsControlCondition ||
      "([{=,:;!?&|+-*/%~^<>".includes(precedingCharacter);
    if (!isExpressionGrouping) {
      return 0;
    }
    count += 1;
    nextIndex = before.length - 1;
  }
  return count;
};

const afterWrappingCloseParens = (
  contents: string,
  index: number,
  count: number,
): number | undefined => {
  let nextIndex = skipWhitespace(contents, index);
  for (let parenIndex = 0; parenIndex < count; parenIndex += 1) {
    if (contents.charAt(nextIndex) !== ")") {
      return undefined;
    }
    nextIndex = skipWhitespace(contents, nextIndex + 1);
  }
  return nextIndex;
};

const readAccessorAfterCallee = (
  contents: string,
  calleeStartIndex: number,
  afterCalleeIndex: number,
  property: string,
): number | undefined => {
  const directAccessor = readPropertyAccessor(contents, afterCalleeIndex, property);
  if (directAccessor !== undefined) {
    return directAccessor;
  }
  const wrappingOpenParens = wrappingOpenParenCountBefore(contents, calleeStartIndex);
  if (wrappingOpenParens === 0) {
    return undefined;
  }
  for (let parenCount = wrappingOpenParens; parenCount > 0; parenCount -= 1) {
    const afterWrappedCallee = afterWrappingCloseParens(contents, afterCalleeIndex, parenCount);
    if (afterWrappedCallee === undefined) {
      continue;
    }
    const wrappedAccessor = readPropertyAccessor(contents, afterWrappedCallee, property);
    if (wrappedAccessor !== undefined) {
      return wrappedAccessor;
    }
  }
  return undefined;
};

const readBoundOrAppliedSpecifier = (
  contents: string,
  index: number,
): { readonly nextIndex: number; readonly specifier: string } | undefined => {
  const afterBindAccessor = readPropertyAccessor(contents, index, "bind");
  if (afterBindAccessor !== undefined) {
    const boundSpecifier = readCallSecondSpecifier(contents, afterBindAccessor);
    if (boundSpecifier !== undefined) {
      return boundSpecifier;
    }
    const afterBindCall = readCallExpressionEnd(contents, afterBindAccessor);
    if (afterBindCall !== undefined) {
      const boundCall = readCallSpecifier(contents, afterBindCall);
      if (boundCall !== undefined) {
        return boundCall;
      }
    }
  }

  const afterApplyAccessor = readPropertyAccessor(contents, index, "apply");
  return afterApplyAccessor === undefined
    ? undefined
    : readApplyArraySpecifier(contents, afterApplyAccessor);
};

const expressionWrapperCloseParenIndex = (contents: string, index: number): number | undefined => {
  let depth = 0;
  let nextIndex = index;
  while (nextIndex < contents.length) {
    const character = contents.charAt(nextIndex);
    const nextCharacter = contents.charAt(nextIndex + 1);
    if (character === '"' || character === "'" || character === "`") {
      nextIndex = skipQuotedLiteral(contents, nextIndex);
      continue;
    }
    if (
      character === "/" &&
      nextCharacter !== "/" &&
      nextCharacter !== "*" &&
      isRegexLiteralStartContext(contents, nextIndex)
    ) {
      nextIndex = skipRegexLiteral(contents, nextIndex);
      continue;
    }
    if (character === "(" || character === "[" || character === "{") {
      depth += 1;
      nextIndex += 1;
      continue;
    }
    if (character === ")" && depth === 0) {
      return nextIndex;
    }
    if (character === ")" || character === "]" || character === "}") {
      depth -= 1;
      nextIndex += 1;
      continue;
    }
    nextIndex += 1;
  }
  return undefined;
};

const readSpecifierAfterCallableExpression = (
  contents: string,
  index: number,
): { readonly nextIndex: number; readonly specifier: string } | undefined => {
  const directCall = readCallSpecifier(contents, index);
  if (directCall !== undefined) {
    return directCall;
  }
  const afterCallAccessor = readPropertyAccessor(contents, index, "call");
  if (afterCallAccessor !== undefined) {
    const calledSpecifier = readCallSecondSpecifier(contents, afterCallAccessor);
    if (calledSpecifier !== undefined) {
      return calledSpecifier;
    }
  }
  return readBoundOrAppliedSpecifier(contents, index);
};

const afterExpressionWrappedCallee = (
  contents: string,
  calleeStartIndex: number,
  afterCalleeIndex: number,
): number | undefined => {
  let previous = contents.slice(0, calleeStartIndex).trimEnd();
  while (previous.endsWith("(")) {
    previous = previous.slice(0, -1).trimEnd();
  }
  const hasExpressionWrapper =
    previous.endsWith(",") ||
    previous.endsWith("||") ||
    previous.endsWith("&&") ||
    previous.endsWith("?") ||
    previous.endsWith(":") ||
    previous.endsWith("??");
  if (!hasExpressionWrapper) {
    const afterCallee = skipWhitespace(contents, afterCalleeIndex);
    const startsWrappedExpression =
      contents.startsWith("??", afterCallee) ||
      contents.startsWith("||", afterCallee) ||
      contents.startsWith(":", afterCallee);
    if (!startsWrappedExpression) {
      return undefined;
    }
    const closeParenIndex = expressionWrapperCloseParenIndex(contents, afterCallee + 1);
    return closeParenIndex === undefined ? undefined : closeParenIndex + 1;
  }
  const afterCallee = skipWhitespace(contents, afterCalleeIndex);
  if (contents.charAt(afterCallee) === ")") {
    return afterCallee + 1;
  }
  const startsTernaryTail = previous.endsWith("?") && contents.charAt(afterCallee) === ":";
  if (!startsTernaryTail) {
    return undefined;
  }
  const closeParenIndex = expressionWrapperCloseParenIndex(contents, afterCallee + 1);
  return closeParenIndex === undefined ? undefined : closeParenIndex + 1;
};

const readCalleeCallSpecifier = (
  contents: string,
  calleeStartIndex: number,
  afterCalleeIndex: number,
): { readonly nextIndex: number; readonly specifier: string } | undefined => {
  const directCall = readCallSpecifier(contents, afterCalleeIndex);
  if (directCall !== undefined) {
    return directCall;
  }

  const boundOrApplied = readBoundOrAppliedSpecifier(contents, afterCalleeIndex);
  if (boundOrApplied !== undefined) {
    return boundOrApplied;
  }

  const afterExpressionWrapper = afterExpressionWrappedCallee(
    contents,
    calleeStartIndex,
    afterCalleeIndex,
  );
  if (afterExpressionWrapper !== undefined) {
    let nextExpressionWrapper = skipWhitespace(contents, afterExpressionWrapper);
    while (nextExpressionWrapper < contents.length) {
      const expressionWrappedSpecifier = readSpecifierAfterCallableExpression(
        contents,
        nextExpressionWrapper,
      );
      if (expressionWrappedSpecifier !== undefined) {
        return expressionWrappedSpecifier;
      }
      if (contents.charAt(nextExpressionWrapper) !== ")") {
        break;
      }
      nextExpressionWrapper = skipWhitespace(contents, nextExpressionWrapper + 1);
    }
  }

  const wrappingOpenParens = wrappingOpenParenCountBefore(contents, calleeStartIndex);
  if (wrappingOpenParens > 0) {
    for (let parenCount = wrappingOpenParens; parenCount > 0; parenCount -= 1) {
      const afterWrappedCallee = afterWrappingCloseParens(contents, afterCalleeIndex, parenCount);
      if (afterWrappedCallee === undefined) {
        continue;
      }
      const parenthesizedCall = readCallSpecifier(contents, afterWrappedCallee);
      if (parenthesizedCall !== undefined) {
        return parenthesizedCall;
      }
      const afterParenthesizedCallAccessor = readPropertyAccessor(
        contents,
        afterWrappedCallee,
        "call",
      );
      if (afterParenthesizedCallAccessor !== undefined) {
        return readCallSecondSpecifier(contents, afterParenthesizedCallAccessor);
      }
      const parenthesizedBoundOrApplied = readBoundOrAppliedSpecifier(contents, afterWrappedCallee);
      if (parenthesizedBoundOrApplied !== undefined) {
        return parenthesizedBoundOrApplied;
      }
      const afterWrappedExpressionWrapper = afterExpressionWrappedCallee(
        contents,
        calleeStartIndex,
        afterWrappedCallee,
      );
      if (afterWrappedExpressionWrapper !== undefined) {
        let nextWrappedExpression = skipWhitespace(contents, afterWrappedExpressionWrapper);
        while (nextWrappedExpression < contents.length) {
          const wrappedExpressionSpecifier = readSpecifierAfterCallableExpression(
            contents,
            nextWrappedExpression,
          );
          if (wrappedExpressionSpecifier !== undefined) {
            return wrappedExpressionSpecifier;
          }
          if (contents.charAt(nextWrappedExpression) !== ")") {
            break;
          }
          nextWrappedExpression = skipWhitespace(contents, nextWrappedExpression + 1);
        }
      }
    }
  }

  const afterCallAccessor = readPropertyAccessor(contents, afterCalleeIndex, "call");
  if (afterCallAccessor !== undefined) {
    return readCallSecondSpecifier(contents, afterCallAccessor);
  }

  return undefined;
};

const readTemplateExpression = (
  contents: string,
  index: number,
): { readonly expression: string; readonly nextIndex: number } | undefined => {
  let depth = 1;
  let nextIndex = index;

  while (nextIndex < contents.length) {
    const character = contents.charAt(nextIndex);
    const nextCharacter = contents.charAt(nextIndex + 1);
    if (isImportQuote(character)) {
      nextIndex = skipQuotedLiteral(contents, nextIndex);
      continue;
    }
    if (
      character === "/" &&
      nextCharacter !== "/" &&
      nextCharacter !== "*" &&
      isRegexLiteralStartContext(contents, nextIndex)
    ) {
      nextIndex = skipRegexLiteral(contents, nextIndex);
      continue;
    }
    if (character === "/" && nextCharacter === "/") {
      const nextLineIndex = contents.indexOf("\n", nextIndex + 2);
      nextIndex = nextLineIndex + 1 || contents.length;
      continue;
    }
    if (character === "/" && nextCharacter === "*") {
      const commentEndIndex = contents.indexOf("*/", nextIndex + 2);
      nextIndex = commentEndIndex === -1 ? contents.length : commentEndIndex + 2;
      continue;
    }
    if (character === "{") {
      depth += 1;
    }
    if (character === "}") {
      depth -= 1;
      if (depth === 0) {
        return {
          expression: contents.slice(index, nextIndex),
          nextIndex: nextIndex + 1,
        };
      }
    }
    nextIndex += 1;
  }

  return undefined;
};

const templateExpressionImportSpecifiers = (
  contents: string,
  index: number,
): { readonly nextIndex: number; readonly specifiers: ReadonlyArray<string> } => {
  const specifiers: Array<string> = [];
  let nextIndex = index + 1;

  while (nextIndex < contents.length) {
    const character = contents.charAt(nextIndex);
    const nextCharacter = contents.charAt(nextIndex + 1);
    if (character === "\\") {
      nextIndex += 2;
      continue;
    }
    if (character === "`") {
      return {
        nextIndex: nextIndex + 1,
        specifiers,
      };
    }
    if (character === "$" && nextCharacter === "{") {
      const expression = readTemplateExpression(contents, nextIndex + 2);
      if (expression === undefined) {
        return {
          nextIndex: contents.length,
          specifiers,
        };
      }
      specifiers.push(...importSpecifiersFromSource(expression.expression));
      nextIndex = expression.nextIndex;
      continue;
    }
    nextIndex += 1;
  }

  return {
    nextIndex,
    specifiers,
  };
};

const readJsxTag = (
  contents: string,
  index: number,
): {
  readonly _tag: "complete";
  readonly closing: boolean;
  readonly nextIndex: number;
  readonly selfClosing: boolean;
  readonly specifiers: ReadonlyArray<string>;
} | {
  readonly _tag: "incomplete";
  readonly nextIndex: number;
  readonly specifiers: ReadonlyArray<string>;
} => {
  const specifiers: Array<string> = [];
  const closing = contents.charAt(index + 1) === "/";
  let nextIndex = index + 1;

  while (nextIndex < contents.length) {
    const character = contents.charAt(nextIndex);
    if (isImportQuote(character)) {
      nextIndex = skipQuotedLiteral(contents, nextIndex);
      continue;
    }
    if (character === "{") {
      const expression = readTemplateExpression(contents, nextIndex + 1);
      if (expression === undefined) {
        return {
          _tag: "incomplete",
          nextIndex: contents.length,
          specifiers,
        };
      }
      specifiers.push(...importSpecifiersFromSource(expression.expression));
      nextIndex = expression.nextIndex;
      continue;
    }
    if (character === ">") {
      return {
        _tag: "complete",
        closing,
        nextIndex: nextIndex + 1,
        selfClosing: previousNonWhitespaceCharacter(contents, nextIndex - 1) === "/",
        specifiers,
      };
    }
    nextIndex += 1;
  }

  return {
    _tag: "incomplete",
    nextIndex,
    specifiers,
  };
};

const jsxElementImportSpecifiers = (
  contents: string,
  index: number,
): { readonly nextIndex: number; readonly specifiers: ReadonlyArray<string> } => {
  const specifiers: Array<string> = [];
  let depth = 0;
  let nextIndex = index;

  while (nextIndex < contents.length) {
    const character = contents.charAt(nextIndex);
    if (character === "<" && isNestedJsxTagStart(contents, nextIndex)) {
      const tag = readJsxTag(contents, nextIndex);
      if (tag._tag === "incomplete") {
        return {
          nextIndex: index + 1,
          specifiers: [],
        };
      }
      specifiers.push(...tag.specifiers);
      nextIndex = tag.nextIndex;
      if (tag.closing) {
        depth -= 1;
        if (depth <= 0) {
          return {
            nextIndex,
            specifiers,
          };
        }
        continue;
      }
      if (!tag.selfClosing) {
        depth += 1;
        continue;
      }
      if (depth > 0) {
        continue;
      }
      return {
        nextIndex,
        specifiers,
      };
    }
    if (character === "{") {
      const expression = readTemplateExpression(contents, nextIndex + 1);
      if (expression === undefined) {
        return {
          nextIndex: contents.length,
          specifiers,
        };
      }
      specifiers.push(...importSpecifiersFromSource(expression.expression));
      nextIndex = expression.nextIndex;
      continue;
    }
    nextIndex += 1;
  }

  return {
    nextIndex: index + 1,
    specifiers: [],
  };
};

export const importSpecifiersFromSource = (contents: string): ReadonlyArray<string> => {
  const source = sourceWithoutComments(contents);
  const specifiers: Array<string> = [];
  let index = 0;

  while (index < source.length) {
    const character = source.charAt(index);
    if (isJsxTagStart(source, index)) {
      const jsxElement = jsxElementImportSpecifiers(source, index);
      specifiers.push(...jsxElement.specifiers);
      index = jsxElement.nextIndex;
      continue;
    }
    if (character === "`") {
      const template = templateExpressionImportSpecifiers(source, index);
      specifiers.push(...template.specifiers);
      index = template.nextIndex;
      continue;
    }
    if (character === '"' || character === "'") {
      index = skipQuotedLiteral(source, index);
      continue;
    }

    const afterFromKeyword = afterKeywordAt(source, index, "from");
    if (afterFromKeyword !== undefined) {
      const specifier = readStaticQuotedSpecifier(
        source,
        skipWhitespace(source, afterFromKeyword),
      );
      if (specifier !== undefined) {
        specifiers.push(specifier.specifier);
        index = specifier.nextIndex;
        continue;
      }
    }

    const afterImportKeyword = afterKeywordAt(source, index, "import");
    if (afterImportKeyword !== undefined) {
      const previousImportCharacter = previousNonWhitespaceCharacter(source, index - 1);
      const isFreeImport = previousImportCharacter !== "." && previousImportCharacter !== "#";
      if (!isFreeImport) {
        index += 1;
        continue;
      }
      const afterImport = skipWhitespace(source, afterImportKeyword);
      const sideEffectSpecifier = readStaticQuotedSpecifier(source, afterImport);
      if (sideEffectSpecifier !== undefined) {
        specifiers.push(sideEffectSpecifier.specifier);
        index = sideEffectSpecifier.nextIndex;
        continue;
      }
      if (source[afterImport] === "(") {
        const dynamicSpecifier = readStaticQuotedSpecifier(
          source,
          skipWhitespace(source, afterImport + 1),
        );
        if (dynamicSpecifier !== undefined) {
          specifiers.push(dynamicSpecifier.specifier);
          index = dynamicSpecifier.nextIndex;
          continue;
        }
      }
    }

    const afterImportMetaResolveAccessor = importMetaResolveAccessorAt(source, index);
    if (afterImportMetaResolveAccessor !== undefined) {
      const resolvedSpecifier = readCalleeCallSpecifier(
        source,
        index,
        afterImportMetaResolveAccessor,
      );
      if (resolvedSpecifier !== undefined) {
        specifiers.push(resolvedSpecifier.specifier);
        index = resolvedSpecifier.nextIndex;
        continue;
      }
    }

    const afterRequireKeyword = freeRequireKeywordEndAt(source, index);
    if (afterRequireKeyword !== undefined) {
      const afterRequire = skipWhitespace(source, afterRequireKeyword);
      const requireSpecifier = readCalleeCallSpecifier(source, index, afterRequire);
      if (requireSpecifier !== undefined) {
        specifiers.push(requireSpecifier.specifier);
        index = requireSpecifier.nextIndex;
        continue;
      }
      const afterResolve = resolveAccessorAfterRequire(source, index, afterRequire);
      if (afterResolve !== undefined) {
        const resolvedSpecifier = readCalleeCallSpecifier(source, index, afterResolve);
        if (resolvedSpecifier !== undefined) {
          specifiers.push(resolvedSpecifier.specifier);
          index = resolvedSpecifier.nextIndex;
          continue;
        }
      }
    }

    const afterCreateRequireKeyword = freeCreateRequireKeywordEndAt(source, index);
    if (afterCreateRequireKeyword !== undefined) {
      const afterCreateRequireFactory = readCallExpressionEnd(source, afterCreateRequireKeyword);
      if (afterCreateRequireFactory !== undefined) {
        const createRequireSpecifier = readCalleeCallSpecifier(
          source,
          index,
          afterCreateRequireFactory,
        );
        if (createRequireSpecifier !== undefined) {
          specifiers.push(createRequireSpecifier.specifier);
          index = createRequireSpecifier.nextIndex;
          continue;
        }
        const afterResolve = readAccessorAfterCallee(
          source,
          index,
          afterCreateRequireFactory,
          "resolve",
        );
        if (afterResolve !== undefined) {
          const resolvedSpecifier = readCalleeCallSpecifier(source, index, afterResolve);
          if (resolvedSpecifier !== undefined) {
            specifiers.push(resolvedSpecifier.specifier);
            index = resolvedSpecifier.nextIndex;
            continue;
          }
        }
      }
    }

    const afterRequireAccessor = moduleRequireAccessorAt(source, index);
    if (afterRequireAccessor !== undefined) {
      const afterRequire = skipWhitespace(source, afterRequireAccessor);
      const moduleRequireSpecifier = readCalleeCallSpecifier(source, index, afterRequire);
      if (moduleRequireSpecifier !== undefined) {
        specifiers.push(moduleRequireSpecifier.specifier);
        index = moduleRequireSpecifier.nextIndex;
        continue;
      }
    }

    index += 1;
  }

  return specifiers;
};

export const packageImportViolationsFor = ({
  contents,
  relativePath,
  restriction,
}: {
  readonly contents: string;
  readonly relativePath: string;
  readonly restriction: RestrictedPackageImport;
}): ReadonlyArray<string> =>
  importedViewServerSpecifiers(contents).flatMap((specifier) => {
    if (!approvedPublicViewServerSpecifiers.has(specifier)) {
      return [
        `${relativePath} imports ${specifier}: View Server imports must use approved package exports.`,
      ];
    }

    const isAllowed = (() => {
      const relativePathAllowedSpecifiers =
        restriction.allowedRelativePathSpecifiers?.get(relativePath);
      return (
        relativePathAllowedSpecifiers?.has(specifier) === true ||
        restriction.allowedSpecifiers?.has(specifier) === true
      );
    })();

    if (isAllowed) {
      return [];
    }

    const isForbidden = Array.from(restriction.forbiddenSpecifiers).some((forbiddenSpecifier) =>
      specifierMatches(specifier, forbiddenSpecifier),
    );

    return isForbidden
      ? [`${relativePath} imports ${specifier}: ${restriction.message}`]
      : [];
  });

const relativeImportSpecifiers = (contents: string): ReadonlyArray<string> =>
  importSpecifiersFromSource(contents).filter((specifier) => specifier.startsWith("."));

const approvedPublicViewServerSpecifiers = new Set([
  "@view-server/client",
  "@view-server/client/remote",
  "@view-server/column-live-view-engine",
  "@view-server/config",
  "@view-server/config/health",
  "@view-server/config/kafka",
  "@view-server/config/live-protocol",
  "@view-server/config/query",
  "@view-server/config/runtime",
  "@view-server/effect-utils",
  "@view-server/in-memory",
  "@view-server/protocol",
  "@view-server/react",
  "@view-server/react/testing",
  "@view-server/runtime",
  "@view-server/runtime-core",
  "@view-server/server",
]);

const isInsideDirectory = (parentDirectory: string, childPath: string): boolean => {
  const relativeChildPath = relative(parentDirectory, childPath);
  return (
    relativeChildPath === "" ||
    (!relativeChildPath.startsWith("..") && !isAbsolute(relativeChildPath))
  );
};

export const packageRelativeImportViolationsFor = ({
  contents,
  packageRoot,
  path,
}: {
  readonly contents: string;
  readonly packageRoot: string;
  readonly path: string;
}): ReadonlyArray<string> =>
  relativeImportSpecifiers(contents)
    .map((specifier) => ({
      resolvedPath: resolve(dirname(path), specifier),
      specifier,
    }))
    .filter(({ resolvedPath }) => !isInsideDirectory(packageRoot, resolvedPath))
    .map(
      ({ specifier }) =>
        `${relative(packageRoot, path)} imports ${specifier}: relative imports must not cross package seams.`,
    );

export const toPosixRelativePath = (path: string): string => path.replaceAll("\\", "/");

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

export const topicStoreStateExportViolationsForFile = ({
  contents,
  path,
}: {
  readonly contents: string;
  readonly path: string;
}): ReadonlyArray<string> => {
  if (path === topicStoreStateFile) {
    return [];
  }

  const violations: Array<string> = [];
  for (const restriction of restrictedTopicStoreStateExports) {
    if (restriction.pattern.test(contents)) {
      violations.push(`${relative(repoRoot, path)} has a restricted ${restriction.label}`);
    }
  }
  return violations;
};

export const topicStoreHelperViolationsForFile = ({
  contents,
  path,
}: {
  readonly contents: string;
  readonly path: string;
}): ReadonlyArray<string> => {
  const violations: Array<string> = [];

  for (const helper of restrictedTopicStoreHelpers) {
    if (!helper.allowedPaths.has(path) && helper.pattern.test(contents)) {
      violations.push(`${relative(repoRoot, path)} uses ${helper.name}`);
    }
  }

  return violations;
};

export const collectEngineSeamViolations = () => {
  const helperViolations: Array<string> = [];
  const stateExportViolations: Array<string> = [];

  for (const path of sourceFiles(engineSourceRoot)) {
    if (isTestFile(path)) {
      continue;
    }

    const contents = readFileSync(path, "utf8");
    helperViolations.push(...topicStoreHelperViolationsForFile({ contents, path }));
    stateExportViolations.push(...topicStoreStateExportViolationsForFile({ contents, path }));
  }

  return {
    helperViolations,
    stateExportViolations,
  };
};

export const topicStoreHelperViolationMessage = (violations: ReadonlyArray<string>): string =>
  [
    "Production engine modules must not use restricted TopicStore state helpers.",
    "Route query/read-model behavior through TopicStore helper operations instead.",
    ...violations.map((path) => `- ${path}`),
  ].join("\n");

export const topicStoreStateExportViolationMessage = (
  violations: ReadonlyArray<string>,
): string =>
  [
    "Production engine modules must not re-export restricted TopicStore state internals.",
    ...violations.map((path) => `- ${path}`),
  ].join("\n");

export const assertNoEngineSeamViolations = ({
  helperViolations,
  stateExportViolations,
}: {
  readonly helperViolations: ReadonlyArray<string>;
  readonly stateExportViolations: ReadonlyArray<string>;
}) => {
  if (helperViolations.length > 0) {
    throw new Error(topicStoreHelperViolationMessage(helperViolations));
  }
  if (stateExportViolations.length > 0) {
    throw new Error(topicStoreStateExportViolationMessage(stateExportViolations));
  }
};

assertNoEngineSeamViolations(collectEngineSeamViolations());

const viewServerPackages = {
  client: "@view-server/client",
  config: "@view-server/config",
  effectUtils: "@view-server/effect-utils",
  engine: "@view-server/column-live-view-engine",
  inMemory: "@view-server/in-memory",
  protocol: "@view-server/protocol",
  react: "@view-server/react",
  runtime: "@view-server/runtime",
  runtimeCore: "@view-server/runtime-core",
  server: "@view-server/server",
} as const;

const allViewServerPackages = new Set(Object.values(viewServerPackages));

const restrictedPackageImports: ReadonlyArray<RestrictedPackageImport> = [
  {
    packageName: "config",
    forbiddenSpecifiers: allViewServerPackages,
    message: "Config contracts must stay at the bottom of the dependency graph.",
  },
  {
    packageName: "effect-utils",
    forbiddenSpecifiers: allViewServerPackages,
    message: "Effect utility helpers must stay independent of View Server packages.",
  },
  {
    packageName: "protocol",
    allowedSpecifiers: new Set([viewServerPackages.config]),
    forbiddenSpecifiers: allViewServerPackages,
    message: "Protocol may depend on config contracts only.",
  },
  {
    packageName: "client",
    allowedSpecifiers: new Set([
      viewServerPackages.config,
      viewServerPackages.effectUtils,
      viewServerPackages.protocol,
    ]),
    forbiddenSpecifiers: allViewServerPackages,
    message: "Client code must not depend on runtime, server, React, in-memory, or engine code.",
  },
  {
    packageName: "column-live-view-engine",
    allowedSpecifiers: new Set([viewServerPackages.config]),
    forbiddenSpecifiers: allViewServerPackages,
    message: "The engine must stay transport/runtime independent.",
  },
  {
    packageName: "runtime-core",
    allowedSpecifiers: new Set([
      viewServerPackages.client,
      viewServerPackages.config,
      viewServerPackages.effectUtils,
      viewServerPackages.engine,
    ]),
    forbiddenSpecifiers: allViewServerPackages,
    message: "Runtime core may compose client contracts, config, effect utils, and engine only.",
  },
  {
    packageName: "runtime",
    allowedSpecifiers: new Set([
      viewServerPackages.client,
      viewServerPackages.config,
      viewServerPackages.effectUtils,
      viewServerPackages.runtimeCore,
      viewServerPackages.server,
    ]),
    forbiddenSpecifiers: allViewServerPackages,
    message: "Production runtime must compose runtime-core/server directly.",
  },
  {
    packageName: "in-memory",
    allowedSpecifiers: new Set([
      viewServerPackages.client,
      viewServerPackages.config,
      viewServerPackages.runtimeCore,
    ]),
    forbiddenSpecifiers: allViewServerPackages,
    message: "The in-memory Adapter must use runtime-core instead of reaching into lower layers.",
  },
  {
    packageName: "server",
    allowedSpecifiers: new Set([
      viewServerPackages.client,
      viewServerPackages.config,
      viewServerPackages.effectUtils,
      viewServerPackages.protocol,
    ]),
    forbiddenSpecifiers: allViewServerPackages,
    message: "Server code may depend on protocol/client contracts, not runtime or React adapters.",
  },
  {
    allowedRelativePathSpecifiers: new Map([
      ["src/testing.tsx", new Set([viewServerPackages.inMemory])],
    ]),
    packageName: "react",
    allowedSpecifiers: new Set([
      viewServerPackages.client,
      `${viewServerPackages.client}/remote`,
      viewServerPackages.config,
      viewServerPackages.effectUtils,
    ]),
    forbiddenSpecifiers: allViewServerPackages,
    message:
      "React bindings may use client transports but must not import runtime, server, engine, or in-memory outside the testing entrypoint.",
  },
] as const;

export const packageImportViolationsForFile = ({
  contents,
  packageRoot,
  path,
  restriction,
}: {
  readonly contents: string;
  readonly packageRoot: string;
  readonly path: string;
  readonly restriction: RestrictedPackageImport;
}): ReadonlyArray<string> => {
  const violations: Array<string> = [];

  for (const violation of packageRelativeImportViolationsFor({
    contents,
    packageRoot,
    path,
  })) {
    violations.push(`packages/${restriction.packageName}/${violation}`);
  }

  for (const violation of packageImportViolationsFor({
    contents,
    relativePath: toPosixRelativePath(relative(packageRoot, path)),
    restriction,
  })) {
    violations.push(`packages/${restriction.packageName}/${violation}`);
  }

  return violations;
};

export const collectPackageImportViolations = (): ReadonlyArray<string> => {
  const violations: Array<string> = [];

  for (const restriction of restrictedPackageImports) {
    for (const path of sourceFiles(packageSourceRoot(restriction.packageName))) {
      if (isTestFile(path)) {
        continue;
      }
      violations.push(
        ...packageImportViolationsForFile({
          contents: readFileSync(path, "utf8"),
          packageRoot: join(repoRoot, "packages", restriction.packageName),
          path,
          restriction,
        }),
      );
    }
  }

  return violations;
};

export const packageImportViolationMessage = (violations: ReadonlyArray<string>): string =>
  [
    "Package architecture seam violations found.",
    ...violations.map((path) => `- ${path}`),
  ].join("\n");

export const assertNoPackageImportViolations = (violations: ReadonlyArray<string>) => {
  if (violations.length === 0) {
    return;
  }
  throw new Error(packageImportViolationMessage(violations));
};

assertNoPackageImportViolations(collectPackageImportViolations());
