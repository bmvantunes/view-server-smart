type RowObject = object;

export type TopicRowEntry<Row extends RowObject> = {
  readonly key: string;
  readonly row: Row;
};

export type TopicRowVisitor<Row extends RowObject> = (key: string, row: Row) => void;

export type TopicRawWindowScanPlan<Row extends RowObject> = {
  readonly matches: (row: Row) => boolean;
  readonly compare: (left: TopicRowEntry<Row>, right: TopicRowEntry<Row>) => number;
  readonly offset: number;
  readonly limit: number | undefined;
};

export type TopicRawWindowScanResult<Row extends RowObject> = {
  readonly keys: ReadonlyArray<string>;
  readonly window: ReadonlyArray<TopicRowEntry<Row>>;
  readonly totalRows: number;
};

export type TopicRowScan<Row extends RowObject> = {
  readonly scanRows: (visitor: TopicRowVisitor<Row>) => void;
  readonly version: () => number;
};

export type TopicRawWindowScan<Row extends RowObject> = {
  readonly scanRawWindow: (plan: TopicRawWindowScanPlan<Row>) => TopicRawWindowScanResult<Row>;
};
