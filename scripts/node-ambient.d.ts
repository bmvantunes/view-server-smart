declare module "node:fs" {
  export type Dirent = {
    readonly name: string;
    readonly isDirectory: () => boolean;
    readonly isFile: () => boolean;
  };

  export function readdirSync(
    path: string,
    options: { readonly withFileTypes: true },
  ): Array<Dirent>;

  export function readFileSync(path: string, encoding: "utf8"): string;
}

declare module "node:path" {
  export function dirname(path: string): string;
  export function join(...paths: ReadonlyArray<string>): string;
  export function relative(from: string, to: string): string;
}

declare module "node:url" {
  export function fileURLToPath(url: string): string;
}
