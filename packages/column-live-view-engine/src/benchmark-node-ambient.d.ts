declare module "node:fs" {
  export const mkdirSync: (path: string, options: { readonly recursive: true }) => void;
  export const readFileSync: (path: string, encoding: "utf8") => string;
  export const writeFileSync: (path: string, data: string) => void;
}

declare module "node:path" {
  export const dirname: (path: string) => string;
  export const join: (...paths: ReadonlyArray<string>) => string;
}
