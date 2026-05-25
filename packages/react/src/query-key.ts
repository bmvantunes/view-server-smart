const stableQueryValue = (value: unknown): unknown => {
  if (typeof value === "bigint") {
    return ["bigint", value.toString()];
  }
  if (Array.isArray(value)) {
    return ["array", value.map(stableQueryValue)];
  }
  if (typeof value === "object" && value !== null) {
    return [
      "object",
      Object.entries(value)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, entry]) => [key, stableQueryValue(entry)]),
    ];
  }
  return ["primitive", value];
};

export const stableQueryKey = (query: object): string => JSON.stringify(stableQueryValue(query));
