import type { RawQueryCompilerMetadata } from "./raw-query-metadata";

export type TopicColumnKind = "generic" | "number";

export type TopicColumnValues = {
  readonly kind: TopicColumnKind;
  readonly length: number;
  get(slot: number): unknown;
};

export type MutableTopicColumnValues = TopicColumnValues & {
  clear(): void;
  copySlot(targetSlot: number, sourceSlot: number): void;
  pop(): void;
  set(slot: number, value: unknown): void;
};

class GenericTopicColumn implements MutableTopicColumnValues {
  readonly kind = "generic";
  private readonly values: Array<unknown> = [];

  get length(): number {
    return this.values.length;
  }

  get(slot: number): unknown {
    return this.values[slot];
  }

  set(slot: number, value: unknown): void {
    this.values[slot] = value;
  }

  copySlot(targetSlot: number, sourceSlot: number): void {
    this.values[targetSlot] = this.values[sourceSlot];
  }

  pop(): void {
    this.values.pop();
  }

  clear(): void {
    this.values.length = 0;
  }
}

class NumberTopicColumn implements MutableTopicColumnValues {
  readonly kind = "number";
  private values = new Float64Array(0);
  private validity = new Uint8Array(0);
  private lengthValue = 0;

  get length(): number {
    return this.lengthValue;
  }

  get(slot: number): unknown {
    if (slot < 0 || slot >= this.lengthValue || this.validity[slot] !== 1) {
      return undefined;
    }
    return this.values[slot];
  }

  set(slot: number, value: unknown): void {
    this.ensureCapacity(slot + 1);
    if (slot >= this.lengthValue) {
      this.lengthValue = slot + 1;
    }
    if (typeof value === "number") {
      this.values[slot] = value;
      this.validity[slot] = 1;
      return;
    }
    this.values[slot] = 0;
    this.validity[slot] = 0;
  }

  copySlot(targetSlot: number, sourceSlot: number): void {
    this.ensureCapacity(targetSlot + 1);
    if (targetSlot >= this.lengthValue) {
      this.lengthValue = targetSlot + 1;
    }
    this.values[targetSlot] = this.values[sourceSlot] ?? 0;
    this.validity[targetSlot] = this.validity[sourceSlot] ?? 0;
  }

  pop(): void {
    if (this.lengthValue === 0) {
      return;
    }
    this.lengthValue -= 1;
    this.values[this.lengthValue] = 0;
    this.validity[this.lengthValue] = 0;
  }

  clear(): void {
    this.values = new Float64Array(0);
    this.validity = new Uint8Array(0);
    this.lengthValue = 0;
  }

  private ensureCapacity(minimumCapacity: number): void {
    if (minimumCapacity <= this.values.length) {
      return;
    }
    let nextCapacity = Math.max(16, this.values.length * 2);
    while (nextCapacity < minimumCapacity) {
      nextCapacity *= 2;
    }
    const nextValues = new Float64Array(nextCapacity);
    nextValues.set(this.values);
    const nextValidity = new Uint8Array(nextCapacity);
    nextValidity.set(this.validity);
    this.values = nextValues;
    this.validity = nextValidity;
  }
}

export const columnValue = (column: TopicColumnValues, slot: number): unknown => column.get(slot);

export const createTopicColumnValues = (
  field: string,
  metadata: RawQueryCompilerMetadata,
): MutableTopicColumnValues => {
  if (metadata.numberFieldNames.has(field)) {
    return new NumberTopicColumn();
  }
  return new GenericTopicColumn();
};

export const createTopicColumnValuesFromArray = (
  field: string,
  metadata: RawQueryCompilerMetadata,
  values: ReadonlyArray<unknown>,
): TopicColumnValues => {
  const column = createTopicColumnValues(field, metadata);
  for (let slot = 0; slot < values.length; slot += 1) {
    column.set(slot, values[slot]);
  }
  return column;
};
