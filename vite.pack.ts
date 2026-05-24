type LibraryPackEntry = string | Array<string>;

export const libraryPack = (entry: LibraryPackEntry) => ({
  entry,
  dts: true,
  fixedExtension: false,
  exports: false,
});
