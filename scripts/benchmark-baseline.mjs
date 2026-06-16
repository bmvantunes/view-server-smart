import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

export const defaultBenchmarkThresholds = {
  latencyMean: {
    maxAbsoluteDeltaMs: 5,
    maxRatio: 8,
  },
  latencyP99: {
    maxAbsoluteDeltaMs: 10,
    maxRatio: 8,
  },
  memoryRssTotalDelta: {
    maxAbsoluteDeltaBytes: 128 * 1024 * 1024,
    maxRatio: 3,
  },
  throughputAggregateRowsPerSecond: {
    minRatio: 0.5,
  },
};

const kafkaReadSnapshotThresholds = {
  throughputReadSnapshotMax: {
    maxAbsoluteDeltaMs: 50,
    maxRatio: 10,
  },
  throughputReadSnapshotMean: {
    maxAbsoluteDeltaMs: 25,
    maxRatio: 8,
  },
};

export const groupedOrderNeutralBenchmarkThresholds = {
  latencyMean: {
    maxAbsoluteDeltaMs: 0.5,
    maxRatio: 6,
  },
  latencyP99: {
    maxAbsoluteDeltaMs: 1,
    maxRatio: 6,
  },
  memoryRssTotalDelta: defaultBenchmarkThresholds.memoryRssTotalDelta,
  throughputAggregateRowsPerSecond:
    defaultBenchmarkThresholds.throughputAggregateRowsPerSecond,
};

export const kafkaIngestBenchmarkThresholds = {
  latencyMean: {
    maxAbsoluteDeltaMs: 2_000,
    maxRatio: 1.5,
  },
  latencyP99: {
    maxAbsoluteDeltaMs: 2_500,
    maxRatio: 1.5,
  },
  memoryRssTotalDelta: defaultBenchmarkThresholds.memoryRssTotalDelta,
  throughputAggregateRowsPerSecond: {
    minRatio: 0.75,
  },
  ...kafkaReadSnapshotThresholds,
};

export const kafkaSustainedFirehoseBenchmarkThresholds = {
  latencyMean: {
    maxAbsoluteDeltaMs: 5_000,
    maxRatio: 1.75,
  },
  latencyP99: {
    maxAbsoluteDeltaMs: 6_000,
    maxRatio: 1.75,
  },
  memoryRssTotalDelta: defaultBenchmarkThresholds.memoryRssTotalDelta,
  throughputAggregateRowsPerSecond: {
    minRatio: 0.75,
  },
  ...kafkaReadSnapshotThresholds,
};

export const benchmarkThresholdsForProfile = (profile) =>
  profile === "grouped-order-neutral"
    ? groupedOrderNeutralBenchmarkThresholds
    : profile === "kafka-ingest"
      ? kafkaIngestBenchmarkThresholds
    : profile === "kafka-sustained-firehose"
      ? kafkaSustainedFirehoseBenchmarkThresholds
    : defaultBenchmarkThresholds;

const readJsonFile = (path) => JSON.parse(readFileSync(path, "utf8"));

const writeJsonFile = (path, value) => {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(value, undefined, 2)}\n`);
};

const finiteNumber = (value, path) => {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`Benchmark artifact field ${path} must be a finite number.`);
  }
  return value;
};

const stringValue = (value, path) => {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`Benchmark artifact field ${path} must be a non-empty string.`);
  }
  return value;
};

const objectValue = (value, path) => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`Benchmark artifact field ${path} must be an object.`);
  }
  return value;
};

const arrayValue = (value, path) => {
  if (!Array.isArray(value)) {
    throw new Error(`Benchmark artifact field ${path} must be an array.`);
  }
  return value;
};

const optionalObjectValue = (value, path) =>
  value === undefined ? undefined : objectValue(value, path);

const optionalArrayValue = (value, path) =>
  value === undefined ? undefined : arrayValue(value, path);

const optionalFiniteNumber = (value, path) =>
  value === undefined ? undefined : finiteNumber(value, path);

const positiveFiniteNumber = (value, path) => {
  const number = finiteNumber(value, path);
  if (number <= 0) {
    throw new Error(`Benchmark artifact field ${path} must be a positive finite number.`);
  }
  return number;
};

const stringArrayValue = (value, path) =>
  arrayValue(value, path).map((item, index) => stringValue(item, `${path}[${index}]`));

const metricLimit = (baseline, threshold) =>
  Math.max(baseline * threshold.maxRatio, baseline + threshold.maxAbsoluteDeltaMs);

const byteMetricLimit = (baseline, threshold) =>
  Math.min(baseline * threshold.maxRatio, baseline + threshold.maxAbsoluteDeltaBytes);

const benchmarkKey = (benchmark) => `${benchmark.groupName} / ${benchmark.name}`;

const baselineArtifactKind = (value, path) => {
  const artifactKind = stringValue(value, path);
  if (artifactKind !== "view-server-benchmark-baseline") {
    throw new Error(`Benchmark artifact field ${path} must be view-server-benchmark-baseline.`);
  }
  return artifactKind;
};

const summaryArtifactKind = (value, path) => {
  const artifactKind = stringValue(value, path);
  if (
    artifactKind !== "engine-benchmark-summary" &&
    artifactKind !== "react-browser-benchmark-summary" &&
    artifactKind !== "runtime-benchmark-summary"
  ) {
    throw new Error(
      `Benchmark artifact field ${path} must be engine-benchmark-summary, react-browser-benchmark-summary, or runtime-benchmark-summary.`,
    );
  }
  return artifactKind;
};

const positiveInteger = (value, path) => {
  const number = finiteNumber(value, path);
  if (!Number.isInteger(number) || number <= 0) {
    throw new Error(`Benchmark artifact field ${path} must be a positive integer.`);
  }
  return number;
};

const nonNegativeInteger = (value, path) => {
  const number = finiteNumber(value, path);
  if (!Number.isInteger(number) || number < 0) {
    throw new Error(`Benchmark artifact field ${path} must be a non-negative integer.`);
  }
  return number;
};

const comparableBenchmark = (groupName, benchmark) => ({
  groupName,
  maxMs: finiteNumber(benchmark.max, `${benchmark.name}.max`),
  meanMs: finiteNumber(benchmark.mean, `${benchmark.name}.mean`),
  minMs: finiteNumber(benchmark.min, `${benchmark.name}.min`),
  name: stringValue(benchmark.name, "benchmark.name"),
  p99Ms: finiteNumber(benchmark.p99, `${benchmark.name}.p99`),
  sampleCount: positiveInteger(benchmark.sampleCount, `${benchmark.name}.sampleCount`),
});

const nonNegativeIntegerString = (value, path) => {
  const text = stringValue(value, path);
  if (!/^(0|[1-9]\d*)$/u.test(text)) {
    throw new Error(`Benchmark artifact field ${path} must be a non-negative integer string.`);
  }
  return Number.parseInt(text, 10);
};

const kafkaIngestLaneValue = (value, path) => {
  const lane = objectValue(value, path);
  return {
    internalTopic: stringValue(lane.internalTopic, `${path}.internalTopic`),
    lane: stringValue(lane.lane, `${path}.lane`),
    producedRows: nonNegativeInteger(lane.producedRows, `${path}.producedRows`),
    region: stringValue(lane.region, `${path}.region`),
    sourceTopic: stringValue(lane.sourceTopic, `${path}.sourceTopic`),
    sourceTopicAlias: stringValue(lane.sourceTopicAlias, `${path}.sourceTopicAlias`),
  };
};

const comparableKafkaIngestLane = (lane) => ({
  internalTopic: lane.internalTopic,
  lane: lane.lane,
  producedRows: lane.producedRows,
  region: lane.region,
  sourceTopicAlias: lane.sourceTopicAlias,
});

const comparableKafkaIngestLaneValue = (value, path) => {
  const lane = objectValue(value, path);
  return {
    internalTopic: stringValue(lane.internalTopic, `${path}.internalTopic`),
    lane: stringValue(lane.lane, `${path}.lane`),
    producedRows: nonNegativeInteger(lane.producedRows, `${path}.producedRows`),
    region: stringValue(lane.region, `${path}.region`),
    sourceTopicAlias: stringValue(lane.sourceTopicAlias, `${path}.sourceTopicAlias`),
  };
};

const throughputCaseValue = (value, path, options) => {
  const throughputCase = objectValue(value, path);
  const readSnapshotMetrics =
    options.requireReadSnapshot === true
      ? {
          maxReadSnapshotMs: positiveFiniteNumber(
            throughputCase.maxReadSnapshotMs,
            `${path}.maxReadSnapshotMs`,
          ),
          meanReadSnapshotMs: positiveFiniteNumber(
            throughputCase.meanReadSnapshotMs,
            `${path}.meanReadSnapshotMs`,
          ),
          readSnapshotRowsPerSample: positiveInteger(
            throughputCase.readSnapshotRowsPerSample,
            `${path}.readSnapshotRowsPerSample`,
          ),
        }
      : {};
  const result = {
    aggregateRowsPerSecond: positiveFiniteNumber(
      throughputCase.aggregateRowsPerSecond,
      `${path}.aggregateRowsPerSecond`,
    ),
    maxTotalMs: positiveFiniteNumber(throughputCase.maxTotalMs, `${path}.maxTotalMs`),
    meanConvergenceMs: positiveFiniteNumber(
      throughputCase.meanConvergenceMs,
      `${path}.meanConvergenceMs`,
    ),
    meanProducerSendMs: positiveFiniteNumber(
      throughputCase.meanProducerSendMs,
      `${path}.meanProducerSendMs`,
    ),
    meanRowsPerSecond: positiveFiniteNumber(
      throughputCase.meanRowsPerSecond,
      `${path}.meanRowsPerSecond`,
    ),
    meanTotalMs: positiveFiniteNumber(throughputCase.meanTotalMs, `${path}.meanTotalMs`),
    minRowsPerSecond: positiveFiniteNumber(throughputCase.minRowsPerSecond, `${path}.minRowsPerSecond`),
    name: stringValue(throughputCase.name, `${path}.name`),
    producedRowsPerSample: positiveInteger(
      throughputCase.producedRowsPerSample,
      `${path}.producedRowsPerSample`,
    ),
    ...readSnapshotMetrics,
    sampleCount: positiveInteger(throughputCase.sampleCount, `${path}.sampleCount`),
    totalProducedRows: positiveInteger(
      throughputCase.totalProducedRows,
      `${path}.totalProducedRows`,
    ),
  };
  const expectedTotalProducedRows = result.producedRowsPerSample * result.sampleCount;
  if (result.totalProducedRows !== expectedTotalProducedRows) {
    throw new Error(
      `Benchmark artifact field ${path}.totalProducedRows must equal producedRowsPerSample * sampleCount (${expectedTotalProducedRows}).`,
    );
  }
  const expectedAggregateRowsPerSecond = (result.producedRowsPerSample * 1000) / result.meanTotalMs;
  const aggregateTolerance = Math.max(1e-9, expectedAggregateRowsPerSecond * 1e-9);
  if (Math.abs(result.aggregateRowsPerSecond - expectedAggregateRowsPerSecond) > aggregateTolerance) {
    throw new Error(
      `Benchmark artifact field ${path}.aggregateRowsPerSecond must match producedRowsPerSample * 1000 / meanTotalMs.`,
    );
  }
  if (result.minRowsPerSecond > result.meanRowsPerSecond) {
    throw new Error(
      `Benchmark artifact field ${path}.minRowsPerSecond must be less than or equal to meanRowsPerSecond.`,
    );
  }
  if (result.meanTotalMs > result.maxTotalMs) {
    throw new Error(
      `Benchmark artifact field ${path}.meanTotalMs must be less than or equal to maxTotalMs.`,
    );
  }
  if (result.meanProducerSendMs > result.meanTotalMs) {
    throw new Error(
      `Benchmark artifact field ${path}.meanProducerSendMs must be less than or equal to meanTotalMs.`,
    );
  }
  if (result.meanConvergenceMs > result.meanTotalMs) {
    throw new Error(
      `Benchmark artifact field ${path}.meanConvergenceMs must be less than or equal to meanTotalMs.`,
    );
  }
  if (options.requireReadSnapshot === true) {
    if (result.meanReadSnapshotMs > result.maxReadSnapshotMs) {
      throw new Error(
        `Benchmark artifact field ${path}.meanReadSnapshotMs must be less than or equal to maxReadSnapshotMs.`,
      );
    }
    if (result.meanReadSnapshotMs > result.meanTotalMs) {
      throw new Error(
        `Benchmark artifact field ${path}.meanReadSnapshotMs must be less than or equal to meanTotalMs.`,
      );
    }
  }
  return result;
};

const throughputCasesValue = (value, path, options) => {
  const throughput = objectValue(value, path);
  const source = stringValue(throughput.source, `${path}.source`);
  if (source !== "benchmark-operation-timers") {
    throw new Error(`Benchmark artifact field ${path}.source must be benchmark-operation-timers.`);
  }
  return nonEmptyArrayValue(throughput.cases, `${path}.cases`).map((throughputCase, index) =>
    throughputCaseValue(throughputCase, `${path}.cases[${index}]`, options),
  );
};

const validateThroughputCasesMatchBenchmarks = (throughputCases, benchmarks, path) => {
  const benchmarkSampleCountByName = new Map();
  for (const benchmark of benchmarks) {
    const previousSampleCount = benchmarkSampleCountByName.get(benchmark.name);
    if (previousSampleCount !== undefined && previousSampleCount !== benchmark.sampleCount) {
      throw new Error(
        `Benchmark artifact field ${path}.benchmarks contains ambiguous benchmark sampleCount values for ${benchmark.name}.`,
      );
    }
    benchmarkSampleCountByName.set(benchmark.name, benchmark.sampleCount);
  }
  const throughputByName = throughputCaseByName(throughputCases, path);
  for (const [benchmarkName, benchmarkSampleCount] of benchmarkSampleCountByName) {
    const throughputCase = throughputByName.get(benchmarkName);
    if (throughputCase === undefined) {
      throw new Error(`Benchmark artifact field ${path} is missing throughput case ${benchmarkName}.`);
    }
    if (throughputCase.sampleCount !== benchmarkSampleCount) {
      throw new Error(
        `Benchmark artifact field ${path}.${benchmarkName}.sampleCount must equal benchmark sampleCount ${benchmarkSampleCount} but was ${throughputCase.sampleCount}.`,
      );
    }
  }
  for (const throughputCase of throughputCases) {
    if (!benchmarkSampleCountByName.has(throughputCase.name)) {
      throw new Error(
        `Benchmark artifact field ${path} contains throughput case without matching benchmark: ${throughputCase.name}.`,
      );
    }
  }
};

const validateThroughputCasesMatchMutationCount = (throughputCases, mutationCount, path) => {
  const totalProducedRows = throughputCases.reduce(
    (total, throughputCase) => total + throughputCase.totalProducedRows,
    0,
  );
  if (totalProducedRows !== mutationCount) {
    throw new Error(
      `Benchmark artifact field ${path} totalProducedRows must equal mutationCount ${mutationCount} but was ${totalProducedRows}.`,
    );
  }
};

const validateRuntimeSummaryIngestCompleteness = (summary, path, mutationCount) => {
  const health = objectValue(summary.health, `${path}.health`);
  const engine = objectValue(health.engine, `${path}.health.engine`);
  const engineTopics = objectValue(engine.topics, `${path}.health.engine.topics`);
  const kafka = objectValue(health.kafka, `${path}.health.kafka`);
  const kafkaTopics = objectValue(kafka.topics, `${path}.health.kafka.topics`);
  const lanes = nonEmptyArrayValue(
    objectValue(summary.kafka, `${path}.kafka`).ingestLanes,
    `${path}.kafka.ingestLanes`,
  ).map((lane, index) => kafkaIngestLaneValue(lane, `${path}.kafka.ingestLanes[${index}]`));
  const uniqueKeys = new Map();
  const requireUniqueLaneKey = (key, label, lane) => {
    const previousLane = uniqueKeys.get(`${label}:${key}`);
    if (previousLane !== undefined) {
      throw new Error(
        `Benchmark artifact field ${path}.kafka.ingestLanes contains duplicate ${label} ${key} in lanes ${previousLane} and ${lane}.`,
      );
    }
    uniqueKeys.set(`${label}:${key}`, lane);
  };

  let totalProducedRows = 0;
  for (const lane of lanes) {
    requireUniqueLaneKey(lane.lane, "lane", lane.lane);
    requireUniqueLaneKey(lane.internalTopic, "internalTopic", lane.lane);
    requireUniqueLaneKey(lane.sourceTopicAlias, "sourceTopicAlias", lane.lane);
    requireUniqueLaneKey(`${lane.sourceTopic}:${lane.region}`, "sourceTopic+region", lane.lane);
    totalProducedRows += lane.producedRows;
    const topicHealth = objectValue(
      engineTopics[lane.internalTopic],
      `${path}.health.engine.topics.${lane.internalTopic}`,
    );
    const rowCount = nonNegativeInteger(
      topicHealth.rowCount,
      `${path}.health.engine.topics.${lane.internalTopic}.rowCount`,
    );
    if (rowCount !== lane.producedRows) {
      throw new Error(
        `Benchmark artifact field ${path}.health.engine.topics.${lane.internalTopic}.rowCount must equal producedRows ${lane.producedRows} for Kafka ingest lane ${lane.lane} but was ${rowCount}.`,
      );
    }

    const kafkaTopicHealth = objectValue(
      kafkaTopics[lane.sourceTopic],
      `${path}.health.kafka.topics.${lane.sourceTopic}`,
    );
    const viewServerTopic = stringValue(
      kafkaTopicHealth.viewServerTopic,
      `${path}.health.kafka.topics.${lane.sourceTopic}.viewServerTopic`,
    );
    if (viewServerTopic !== lane.internalTopic) {
      throw new Error(
        `Benchmark artifact field ${path}.health.kafka.topics.${lane.sourceTopic}.viewServerTopic must equal internalTopic ${lane.internalTopic} for Kafka ingest lane ${lane.lane} but was ${viewServerTopic}.`,
      );
    }
    const committedOffset = nonNegativeIntegerString(
      objectValue(
        objectValue(
          kafkaTopicHealth.regions,
          `${path}.health.kafka.topics.${lane.sourceTopic}.regions`,
        )[lane.region],
        `${path}.health.kafka.topics.${lane.sourceTopic}.regions.${lane.region}`,
      ).committedOffset,
      `${path}.health.kafka.topics.${lane.sourceTopic}.regions.${lane.region}.committedOffset`,
    );
    if (committedOffset !== lane.producedRows) {
      throw new Error(
        `Benchmark artifact field ${path}.health.kafka.topics.${lane.sourceTopic}.regions.${lane.region}.committedOffset must equal producedRows ${lane.producedRows} for Kafka ingest lane ${lane.lane} but was ${committedOffset}.`,
      );
    }
  }
  if (totalProducedRows !== mutationCount) {
    throw new Error(
      `Benchmark artifact field ${path}.kafka.ingestLanes producedRows total must equal mutationCount ${mutationCount} but was ${totalProducedRows}.`,
    );
  }

  return lanes.map(comparableKafkaIngestLane);
};

export const comparableBenchmarksFromVitestOutput = (vitestOutput) =>
  arrayValue(objectValue(vitestOutput, "vitestOutput").files, "vitestOutput.files").flatMap(
    (file, fileIndex) =>
      arrayValue(objectValue(file, `files[${fileIndex}]`).groups, `files[${fileIndex}].groups`)
        .flatMap((group, groupIndex) => {
          const groupPath = `files[${fileIndex}].groups[${groupIndex}]`;
          const groupName = stringValue(
            objectValue(group, groupPath).fullName,
            `${groupPath}.fullName`,
          );
          return arrayValue(
            objectValue(group, `files[${fileIndex}].groups[${groupIndex}]`).benchmarks,
            `files[${fileIndex}].groups[${groupIndex}].benchmarks`,
          ).map((benchmark) => comparableBenchmark(groupName, benchmark));
        }),
  );

export const readBenchmarkObservation = (task) => {
  const summary = objectValue(readJsonFile(task.summaryPath), task.summaryPath);
  const vitestOutput = readJsonFile(task.outputJsonPath);
  const artifactKind = summaryArtifactKind(summary.artifactKind, `${task.summaryPath}.artifactKind`);
  const latency = objectValue(summary.latency, `${task.summaryPath}.latency`);
  const latencyOutputJsonPath = stringValue(
    latency.outputJsonPath,
    `${task.summaryPath}.latency.outputJsonPath`,
  );
  if (latencyOutputJsonPath !== task.packageOutputJsonPath) {
    throw new Error(
      `Benchmark artifact field ${task.summaryPath}.latency.outputJsonPath changed from ${task.packageOutputJsonPath} to ${latencyOutputJsonPath}.`,
    );
  }
  const latencySource = stringValue(latency.source, `${task.summaryPath}.latency.source`);
  const memory = objectValue(summary.memory, `${task.summaryPath}.memory`);
  const totalDelta =
    "totalDelta" in memory
      ? objectValue(memory.totalDelta, `${task.summaryPath}.memory.totalDelta`)
      : undefined;
  const rssBytes =
    totalDelta === undefined
      ? undefined
      : optionalFiniteNumber(totalDelta.rssBytes, `${task.summaryPath}.memory.totalDelta.rssBytes`);
  if (
    (artifactKind === "engine-benchmark-summary" ||
      artifactKind === "runtime-benchmark-summary") &&
    rssBytes === undefined
  ) {
    throw new Error(
      `Benchmark artifact field ${task.summaryPath}.memory.totalDelta.rssBytes is required for ${artifactKind}.`,
    );
  }
  if (task.expectedArtifactKind !== undefined && artifactKind !== task.expectedArtifactKind) {
    throw new Error(
      `${task.label}: artifactKind changed from ${task.expectedArtifactKind} to ${artifactKind}.`,
    );
  }
  const benchmarkScope = stringValue(summary.benchmarkScope, `${task.summaryPath}.benchmarkScope`);
  if (task.expectedBenchmarkScope !== undefined && benchmarkScope !== task.expectedBenchmarkScope) {
    throw new Error(
      `${task.label}: benchmarkScope changed from ${task.expectedBenchmarkScope} to ${benchmarkScope}.`,
    );
  }
  const rowCount = finiteNumber(summary.rowCount, `${task.summaryPath}.rowCount`);
  if (task.expectedRowCount !== undefined && rowCount !== task.expectedRowCount) {
    throw new Error(`${task.label}: rowCount changed from ${task.expectedRowCount} to ${rowCount}.`);
  }
  const benchmarks = comparableBenchmarksFromVitestOutput(vitestOutput);
  const minimumSampleCount = positiveInteger(
    task.minimumSampleCount,
    `${task.label}.minimumSampleCount`,
  );
  for (const benchmark of benchmarks) {
    if (benchmark.sampleCount < minimumSampleCount) {
      throw new Error(
        `${task.label} / ${benchmarkKey(benchmark)}: sampleCount must be at least ${minimumSampleCount} but was ${benchmark.sampleCount}.`,
      );
    }
  }

  const benchmarkCases = stringArrayValue(summary.benchmarkCases, `${task.summaryPath}.benchmarkCases`);
  const mutationCount = nonNegativeInteger(summary.mutationCount, `${task.summaryPath}.mutationCount`);
  const topics = stringArrayValue(summary.topics, `${task.summaryPath}.topics`);
  const requiresKafkaThroughput =
    artifactKind === "runtime-benchmark-summary" && benchmarkScope.startsWith("runtime-kafka-");
  const kafkaIngestLanes =
    artifactKind === "runtime-benchmark-summary"
      ? validateRuntimeSummaryIngestCompleteness(summary, task.summaryPath, mutationCount)
      : undefined;
  const throughputCases =
    summary.throughput === undefined
      ? undefined
      : throughputCasesValue(summary.throughput, `${task.summaryPath}.throughput`, {
          requireReadSnapshot: requiresKafkaThroughput,
        });
  if (requiresKafkaThroughput && throughputCases === undefined) {
    throw new Error(
      `Benchmark artifact field ${task.summaryPath}.throughput is required for ${benchmarkScope}.`,
    );
  }
  if (throughputCases !== undefined) {
    validateThroughputCasesMatchBenchmarks(
      throughputCases,
      benchmarks,
      `${task.summaryPath}.throughput.cases`,
    );
    if (requiresKafkaThroughput) {
      validateThroughputCasesMatchMutationCount(
        throughputCases,
        mutationCount,
        `${task.summaryPath}.throughput.cases`,
      );
    }
  }

  return {
    ...(summary.activeViewCountBeforeCleanup === undefined
      ? {}
      : {
          activeViewCountBeforeCleanup: nonNegativeInteger(
            summary.activeViewCountBeforeCleanup,
            `${task.summaryPath}.activeViewCountBeforeCleanup`,
          ),
        }),
    artifactKind,
    backpressureCount: finiteNumber(
      summary.backpressureCount,
      `${task.summaryPath}.backpressureCount`,
    ),
    benchmarks,
    benchmarkCases,
    benchmarkName: stringValue(summary.benchmarkName, `${task.summaryPath}.benchmarkName`),
    benchmarkScope,
    browser: optionalObjectValue(summary.browser, `${task.summaryPath}.browser`),
    cleanupLeakCount: finiteNumber(summary.cleanupLeakCount, `${task.summaryPath}.cleanupLeakCount`),
    groupedKeyWidthParameters: optionalObjectValue(
      summary.groupedKeyWidthParameters,
      `${task.summaryPath}.groupedKeyWidthParameters`,
    ),
    groupedWriteAdmission: optionalObjectValue(
      summary.groupedWriteAdmission,
      `${task.summaryPath}.groupedWriteAdmission`,
    ),
    kafkaIngestLanes,
    latencySource,
    memoryRssTotalDeltaBytes: rssBytes,
    minimumSampleCount,
    mutationCount,
    outputJsonPath: task.outputJsonPath,
    queuedEventCount: finiteNumber(summary.queuedEventCount, `${task.summaryPath}.queuedEventCount`),
    rowCount,
    seedBatchSize: optionalFiniteNumber(summary.seedBatchSize, `${task.summaryPath}.seedBatchSize`),
    subscriberCount: finiteNumber(summary.subscriberCount, `${task.summaryPath}.subscriberCount`),
    summaryPath: task.summaryPath,
    taskLabel: task.label,
    throughputCases,
    topics,
  };
};

export const buildBenchmarkBaseline = (profile, observations) => ({
  artifactKind: "view-server-benchmark-baseline",
  profile,
  tasks: observations,
  thresholds: benchmarkThresholdsForProfile(profile),
});

const writableBenchmarkBaseline = (path, baseline) => {
  const validated = validateBenchmarkBaseline(baseline, path);
  const comparison = compareBenchmarkBaseline(validated, validated);
  if (!comparison.ok) {
    throw new Error(
      [`Benchmark baseline ${path} is not writable:`, ...comparison.regressions].join("\n"),
    );
  }
  return validated;
};

export const readBenchmarkBaseline = (path) => writableBenchmarkBaseline(path, readJsonFile(path));

export const writeBenchmarkBaseline = (path, baseline) => {
  writeJsonFile(path, writableBenchmarkBaseline(path, baseline));
};

const nonEmptyArrayValue = (value, path) => {
  const array = arrayValue(value, path);
  if (array.length === 0) {
    throw new Error(`Benchmark artifact field ${path} must be a non-empty array.`);
  }
  return array;
};

const thresholdsValue = (value, path, expectedThresholds) => {
  const thresholds = objectValue(value, path);
  const validatedThresholds = {
    latencyMean: {
      maxAbsoluteDeltaMs: finiteNumber(
        objectValue(thresholds.latencyMean, `${path}.latencyMean`).maxAbsoluteDeltaMs,
        `${path}.latencyMean.maxAbsoluteDeltaMs`,
      ),
      maxRatio: finiteNumber(thresholds.latencyMean.maxRatio, `${path}.latencyMean.maxRatio`),
    },
    latencyP99: {
      maxAbsoluteDeltaMs: finiteNumber(
        objectValue(thresholds.latencyP99, `${path}.latencyP99`).maxAbsoluteDeltaMs,
        `${path}.latencyP99.maxAbsoluteDeltaMs`,
      ),
      maxRatio: finiteNumber(thresholds.latencyP99.maxRatio, `${path}.latencyP99.maxRatio`),
    },
    memoryRssTotalDelta: {
      maxAbsoluteDeltaBytes: finiteNumber(
        objectValue(thresholds.memoryRssTotalDelta, `${path}.memoryRssTotalDelta`)
          .maxAbsoluteDeltaBytes,
        `${path}.memoryRssTotalDelta.maxAbsoluteDeltaBytes`,
      ),
      maxRatio: finiteNumber(
        thresholds.memoryRssTotalDelta.maxRatio,
        `${path}.memoryRssTotalDelta.maxRatio`,
      ),
    },
    throughputAggregateRowsPerSecond: {
      minRatio: finiteNumber(
        objectValue(
          thresholds.throughputAggregateRowsPerSecond,
          `${path}.throughputAggregateRowsPerSecond`,
        ).minRatio,
        `${path}.throughputAggregateRowsPerSecond.minRatio`,
      ),
    },
  };
  if (expectedThresholds.throughputReadSnapshotMax !== undefined) {
    validatedThresholds.throughputReadSnapshotMax = {
      maxAbsoluteDeltaMs: finiteNumber(
        objectValue(thresholds.throughputReadSnapshotMax, `${path}.throughputReadSnapshotMax`)
          .maxAbsoluteDeltaMs,
        `${path}.throughputReadSnapshotMax.maxAbsoluteDeltaMs`,
      ),
      maxRatio: finiteNumber(
        thresholds.throughputReadSnapshotMax.maxRatio,
        `${path}.throughputReadSnapshotMax.maxRatio`,
      ),
    };
  }
  if (expectedThresholds.throughputReadSnapshotMean !== undefined) {
    validatedThresholds.throughputReadSnapshotMean = {
      maxAbsoluteDeltaMs: finiteNumber(
        objectValue(thresholds.throughputReadSnapshotMean, `${path}.throughputReadSnapshotMean`)
          .maxAbsoluteDeltaMs,
        `${path}.throughputReadSnapshotMean.maxAbsoluteDeltaMs`,
      ),
      maxRatio: finiteNumber(
        thresholds.throughputReadSnapshotMean.maxRatio,
        `${path}.throughputReadSnapshotMean.maxRatio`,
      ),
    };
  }
  if (JSON.stringify(validatedThresholds) !== JSON.stringify(expectedThresholds)) {
    throw new Error(`Benchmark artifact field ${path} must match code-owned profile thresholds.`);
  }
  return validatedThresholds;
};

const validateBenchmark = (benchmark, path) => ({
  groupName: stringValue(benchmark.groupName, `${path}.groupName`),
  maxMs: finiteNumber(benchmark.maxMs, `${path}.maxMs`),
  meanMs: finiteNumber(benchmark.meanMs, `${path}.meanMs`),
  minMs: finiteNumber(benchmark.minMs, `${path}.minMs`),
  name: stringValue(benchmark.name, `${path}.name`),
  p99Ms: finiteNumber(benchmark.p99Ms, `${path}.p99Ms`),
  sampleCount: positiveInteger(benchmark.sampleCount, `${path}.sampleCount`),
});

const validateTask = (task, path) => {
  const artifactKind = summaryArtifactKind(task.artifactKind, `${path}.artifactKind`);
  const benchmarkScope = stringValue(task.benchmarkScope, `${path}.benchmarkScope`);
  const mutationCount = nonNegativeInteger(task.mutationCount, `${path}.mutationCount`);
  const requiresKafkaThroughput =
    artifactKind === "runtime-benchmark-summary" && benchmarkScope.startsWith("runtime-kafka-");
  const memoryRssTotalDeltaBytes = optionalFiniteNumber(
    task.memoryRssTotalDeltaBytes,
    `${path}.memoryRssTotalDeltaBytes`,
  );
  if (
    (artifactKind === "engine-benchmark-summary" ||
      artifactKind === "runtime-benchmark-summary") &&
    memoryRssTotalDeltaBytes === undefined
  ) {
    throw new Error(
      `Benchmark artifact field ${path}.memoryRssTotalDeltaBytes is required for ${artifactKind}.`,
    );
  }
  const benchmarks = nonEmptyArrayValue(task.benchmarks, `${path}.benchmarks`).map(
    (benchmark, index) => validateBenchmark(benchmark, `${path}.benchmarks[${index}]`),
  );
  const throughputCases =
    task.throughputCases === undefined
      ? undefined
      : nonEmptyArrayValue(task.throughputCases, `${path}.throughputCases`).map(
          (throughputCase, index) =>
            throughputCaseValue(throughputCase, `${path}.throughputCases[${index}]`, {
              requireReadSnapshot: requiresKafkaThroughput,
            }),
        );
  if (requiresKafkaThroughput && throughputCases === undefined) {
    throw new Error(
      `Benchmark artifact field ${path}.throughputCases is required for ${benchmarkScope}.`,
    );
  }
  if (throughputCases !== undefined) {
    validateThroughputCasesMatchBenchmarks(throughputCases, benchmarks, `${path}.throughputCases`);
    if (requiresKafkaThroughput) {
      validateThroughputCasesMatchMutationCount(
        throughputCases,
        mutationCount,
        `${path}.throughputCases`,
      );
    }
  }
  return {
    ...(task.activeViewCountBeforeCleanup === undefined
      ? {}
      : {
          activeViewCountBeforeCleanup: nonNegativeInteger(
            task.activeViewCountBeforeCleanup,
            `${path}.activeViewCountBeforeCleanup`,
          ),
        }),
    artifactKind,
    backpressureCount: finiteNumber(task.backpressureCount, `${path}.backpressureCount`),
    benchmarks,
    benchmarkCases: stringArrayValue(task.benchmarkCases, `${path}.benchmarkCases`),
    benchmarkName: stringValue(task.benchmarkName, `${path}.benchmarkName`),
    benchmarkScope,
    browser: optionalObjectValue(task.browser, `${path}.browser`),
    cleanupLeakCount: finiteNumber(task.cleanupLeakCount, `${path}.cleanupLeakCount`),
    groupedKeyWidthParameters: optionalObjectValue(
      task.groupedKeyWidthParameters,
      `${path}.groupedKeyWidthParameters`,
    ),
    groupedWriteAdmission: optionalObjectValue(
      task.groupedWriteAdmission,
      `${path}.groupedWriteAdmission`,
    ),
    kafkaIngestLanes: optionalArrayValue(task.kafkaIngestLanes, `${path}.kafkaIngestLanes`)?.map(
      (lane, index) => comparableKafkaIngestLaneValue(lane, `${path}.kafkaIngestLanes[${index}]`),
    ),
    latencySource: stringValue(task.latencySource, `${path}.latencySource`),
    memoryRssTotalDeltaBytes,
    minimumSampleCount: positiveInteger(task.minimumSampleCount, `${path}.minimumSampleCount`),
    mutationCount,
    outputJsonPath: stringValue(task.outputJsonPath, `${path}.outputJsonPath`),
    queuedEventCount: finiteNumber(task.queuedEventCount, `${path}.queuedEventCount`),
    rowCount: finiteNumber(task.rowCount, `${path}.rowCount`),
    seedBatchSize: optionalFiniteNumber(task.seedBatchSize, `${path}.seedBatchSize`),
    subscriberCount: finiteNumber(task.subscriberCount, `${path}.subscriberCount`),
    summaryPath: stringValue(task.summaryPath, `${path}.summaryPath`),
    taskLabel: stringValue(task.taskLabel, `${path}.taskLabel`),
    throughputCases,
    topics: stringArrayValue(task.topics, `${path}.topics`),
  };
};

export const validateBenchmarkBaseline = (baseline, path = "baseline") => {
  const baselineObject = objectValue(baseline, path);
  const profile = stringValue(baselineObject.profile, `${path}.profile`);
  return {
    artifactKind: baselineArtifactKind(baselineObject.artifactKind, `${path}.artifactKind`),
    profile,
    tasks: nonEmptyArrayValue(baselineObject.tasks, `${path}.tasks`).map((task, index) =>
      validateTask(task, `${path}.tasks[${index}]`),
    ),
    thresholds: thresholdsValue(
      baselineObject.thresholds,
      `${path}.thresholds`,
      benchmarkThresholdsForProfile(profile),
    ),
  };
};

const mapByUniqueKey = (values, key, path, label) => {
  const entries = [];
  const seen = new Set();
  for (const value of values) {
    const valueKey = key(value);
    if (seen.has(valueKey)) {
      throw new Error(`Benchmark artifact field ${path} contains duplicate ${label}: ${valueKey}.`);
    }
    seen.add(valueKey);
    entries.push([valueKey, value]);
  }
  return new Map(entries);
};

const taskByLabel = (tasks, path) =>
  mapByUniqueKey(tasks, (task) => task.taskLabel, path, "taskLabel");

const benchmarkByName = (benchmarks, path) =>
  mapByUniqueKey(benchmarks, benchmarkKey, path, "benchmark case");

const throughputCaseByName = (throughputCases, path) =>
  mapByUniqueKey(throughputCases, (throughputCase) => throughputCase.name, path, "throughput case");

const pushRegression = (regressions, message) => {
  regressions.push(message);
};

const compareZeroCounter = (regressions, taskLabel, name, actual) => {
  if (actual !== 0) {
    pushRegression(regressions, `${taskLabel}: ${name} must stay 0 but was ${actual}.`);
  }
};

const compareExact = (regressions, taskLabel, name, baseline, actual) => {
  if (actual !== baseline) {
    pushRegression(regressions, `${taskLabel}: ${name} changed from ${baseline} to ${actual}.`);
  }
};

const compareExactJson = (regressions, taskLabel, name, baseline, actual) => {
  const baselineJson = JSON.stringify(baseline);
  const actualJson = JSON.stringify(actual);
  if (actualJson !== baselineJson) {
    pushRegression(regressions, `${taskLabel}: ${name} changed from ${baselineJson} to ${actualJson}.`);
  }
};

const compareMinimumCount = (regressions, taskLabel, name, baseline, actual) => {
  const allowedDrop = Math.max(10, baseline * 0.05);
  const minimum = baseline - allowedDrop;
  if (actual < minimum) {
    pushRegression(
      regressions,
      `${taskLabel}: ${name} dropped from ${baseline} to ${actual}; allowed >= ${Math.ceil(minimum)}.`,
    );
  }
};

const compareLatency = (regressions, taskLabel, benchmarkName, metricName, threshold, baseline, actual) => {
  const limit = metricLimit(baseline, threshold);
  if (actual > limit) {
    pushRegression(
      regressions,
      `${taskLabel} / ${benchmarkName}: ${metricName} regressed from ${baseline.toFixed(
        3,
      )}ms to ${actual.toFixed(3)}ms; allowed <= ${limit.toFixed(3)}ms.`,
    );
  }
};

const compareRss = (regressions, taskLabel, threshold, baseline, actual) => {
  const limit = byteMetricLimit(baseline, threshold);
  if (actual > limit) {
    pushRegression(
      regressions,
      `${taskLabel}: total RSS delta regressed from ${baseline} bytes to ${actual} bytes; allowed <= ${Math.round(
        limit,
      )} bytes.`,
    );
  }
};

const compareThroughput = (
  regressions,
  taskLabel,
  caseName,
  metricName,
  threshold,
  baseline,
  actual,
) => {
  const minimum = baseline * threshold.minRatio;
  if (actual < minimum) {
    pushRegression(
      regressions,
      `${taskLabel} / ${caseName}: ${metricName} throughput regressed from ${baseline.toFixed(
        3,
      )} rows/sec to ${actual.toFixed(3)} rows/sec; allowed >= ${minimum.toFixed(3)} rows/sec.`,
    );
  }
};

const compareThroughputCases = (regressions, taskLabel, threshold, baselineCases, actualCases) => {
  if (baselineCases === undefined && actualCases === undefined) {
    return;
  }
  if (baselineCases === undefined || actualCases === undefined) {
    pushRegression(regressions, `${taskLabel}: throughputCases presence changed.`);
    return;
  }
  const baselineByName = throughputCaseByName(baselineCases, `baseline.tasks[${taskLabel}]`);
  const actualByName = throughputCaseByName(actualCases, `actual.tasks[${taskLabel}]`);
  for (const caseName of actualByName.keys()) {
    if (!baselineByName.has(caseName)) {
      pushRegression(regressions, `${taskLabel}: unexpected throughput case ${caseName}.`);
    }
  }
  for (const baselineCase of baselineCases) {
    const actualCase = actualByName.get(baselineCase.name);
    if (actualCase === undefined) {
      pushRegression(regressions, `${taskLabel}: missing throughput case ${baselineCase.name}.`);
      continue;
    }
    compareExact(
      regressions,
      taskLabel,
      `${baselineCase.name} throughput producedRowsPerSample`,
      baselineCase.producedRowsPerSample,
      actualCase.producedRowsPerSample,
    );
    compareExact(
      regressions,
      taskLabel,
      `${baselineCase.name} throughput sampleCount`,
      baselineCase.sampleCount,
      actualCase.sampleCount,
    );
    compareExact(
      regressions,
      taskLabel,
      `${baselineCase.name} throughput totalProducedRows`,
      baselineCase.totalProducedRows,
      actualCase.totalProducedRows,
    );
    compareThroughput(
      regressions,
      taskLabel,
      baselineCase.name,
      "aggregateRowsPerSecond",
      threshold.throughputAggregateRowsPerSecond,
      baselineCase.aggregateRowsPerSecond,
      actualCase.aggregateRowsPerSecond,
    );
    if (threshold.throughputReadSnapshotMean !== undefined) {
      compareExact(
        regressions,
        taskLabel,
        `${baselineCase.name} throughput readSnapshotRowsPerSample`,
        baselineCase.readSnapshotRowsPerSample,
        actualCase.readSnapshotRowsPerSample,
      );
      compareLatency(
        regressions,
        taskLabel,
        baselineCase.name,
        "meanReadSnapshotMs",
        threshold.throughputReadSnapshotMean,
        baselineCase.meanReadSnapshotMs,
        actualCase.meanReadSnapshotMs,
      );
      compareLatency(
        regressions,
        taskLabel,
        baselineCase.name,
        "maxReadSnapshotMs",
        threshold.throughputReadSnapshotMax,
        baselineCase.maxReadSnapshotMs,
        actualCase.maxReadSnapshotMs,
      );
    }
  }
};

export const compareBenchmarkBaseline = (baseline, actualBaseline) => {
  const validatedBaseline = validateBenchmarkBaseline(baseline, "baseline");
  const validatedActual = validateBenchmarkBaseline(actualBaseline, "actual");
  const thresholds = validatedBaseline.thresholds;
  const baselineTasks = taskByLabel(validatedBaseline.tasks, "baseline.tasks");
  const actualTasks = taskByLabel(validatedActual.tasks, "actual.tasks");
  const regressions = [];

  compareExact(
    regressions,
    validatedBaseline.profile,
    "baseline artifactKind",
    validatedBaseline.artifactKind,
    validatedActual.artifactKind,
  );
  compareExact(
    regressions,
    validatedBaseline.profile,
    "profile",
    validatedBaseline.profile,
    validatedActual.profile,
  );

  for (const taskLabel of actualTasks.keys()) {
    if (!baselineTasks.has(taskLabel)) {
      pushRegression(regressions, `${taskLabel}: unexpected benchmark task in actual run.`);
    }
  }

  for (const [taskLabel, baselineTask] of baselineTasks) {
    const actualTask = actualTasks.get(taskLabel);
    if (actualTask === undefined) {
      pushRegression(regressions, `${taskLabel}: missing benchmark task in actual run.`);
      continue;
    }

    compareExact(
      regressions,
      taskLabel,
      "artifactKind",
      baselineTask.artifactKind,
      actualTask.artifactKind,
    );
    compareExact(
      regressions,
      taskLabel,
      "benchmarkScope",
      baselineTask.benchmarkScope,
      actualTask.benchmarkScope,
    );
    compareExact(
      regressions,
      taskLabel,
      "benchmarkName",
      baselineTask.benchmarkName,
      actualTask.benchmarkName,
    );
    compareExactJson(
      regressions,
      taskLabel,
      "benchmarkCases",
      baselineTask.benchmarkCases,
      actualTask.benchmarkCases,
    );
    compareExact(regressions, taskLabel, "rowCount", baselineTask.rowCount, actualTask.rowCount);
    if (baselineTask.benchmarkScope === "runtime-kafka-ingest") {
      compareExact(
        regressions,
        taskLabel,
        "mutationCount",
        baselineTask.mutationCount,
        actualTask.mutationCount,
      );
    } else {
      compareMinimumCount(
        regressions,
        taskLabel,
        "mutationCount",
        baselineTask.mutationCount,
        actualTask.mutationCount,
      );
    }
    compareExact(
      regressions,
      taskLabel,
      "subscriberCount",
      baselineTask.subscriberCount,
      actualTask.subscriberCount,
    );
    compareExactJson(regressions, taskLabel, "topics", baselineTask.topics, actualTask.topics);
    compareExact(
      regressions,
      taskLabel,
      "latencySource",
      baselineTask.latencySource,
      actualTask.latencySource,
    );
    compareExactJson(regressions, taskLabel, "browser", baselineTask.browser, actualTask.browser);
    if (baselineTask.activeViewCountBeforeCleanup !== undefined) {
      compareExact(
        regressions,
        taskLabel,
        "activeViewCountBeforeCleanup",
        baselineTask.activeViewCountBeforeCleanup,
        actualTask.activeViewCountBeforeCleanup,
      );
    }
    compareExactJson(
      regressions,
      taskLabel,
      "kafkaIngestLanes",
      baselineTask.kafkaIngestLanes,
      actualTask.kafkaIngestLanes,
    );
    compareThroughputCases(
      regressions,
      taskLabel,
      thresholds,
      baselineTask.throughputCases,
      actualTask.throughputCases,
    );
    compareExact(
      regressions,
      taskLabel,
      "seedBatchSize",
      baselineTask.seedBatchSize,
      actualTask.seedBatchSize,
    );
    compareExactJson(
      regressions,
      taskLabel,
      "groupedKeyWidthParameters",
      baselineTask.groupedKeyWidthParameters,
      actualTask.groupedKeyWidthParameters,
    );
    compareExactJson(
      regressions,
      taskLabel,
      "groupedWriteAdmission",
      baselineTask.groupedWriteAdmission,
      actualTask.groupedWriteAdmission,
    );
    compareExact(
      regressions,
      taskLabel,
      "minimumSampleCount",
      baselineTask.minimumSampleCount,
      actualTask.minimumSampleCount,
    );
    compareExact(
      regressions,
      taskLabel,
      "outputJsonPath",
      baselineTask.outputJsonPath,
      actualTask.outputJsonPath,
    );
    compareExact(
      regressions,
      taskLabel,
      "summaryPath",
      baselineTask.summaryPath,
      actualTask.summaryPath,
    );
    compareZeroCounter(regressions, taskLabel, "cleanupLeakCount", actualTask.cleanupLeakCount);
    compareZeroCounter(regressions, taskLabel, "backpressureCount", actualTask.backpressureCount);
    compareZeroCounter(regressions, taskLabel, "queuedEventCount", actualTask.queuedEventCount);

    if (
      baselineTask.memoryRssTotalDeltaBytes !== undefined &&
      actualTask.memoryRssTotalDeltaBytes !== undefined
    ) {
      compareRss(
        regressions,
        taskLabel,
        thresholds.memoryRssTotalDelta,
        baselineTask.memoryRssTotalDeltaBytes,
        actualTask.memoryRssTotalDeltaBytes,
      );
    } else if (baselineTask.memoryRssTotalDeltaBytes !== actualTask.memoryRssTotalDeltaBytes) {
      pushRegression(
        regressions,
        `${taskLabel}: memoryRssTotalDeltaBytes presence changed between baseline and actual run.`,
      );
    }

    const baselineBenchmarks = benchmarkByName(
      baselineTask.benchmarks,
      `baseline.tasks[${taskLabel}].benchmarks`,
    );
    const actualBenchmarks = benchmarkByName(
      actualTask.benchmarks,
      `actual.tasks[${taskLabel}].benchmarks`,
    );
    for (const benchmarkName of actualBenchmarks.keys()) {
      if (!baselineBenchmarks.has(benchmarkName)) {
        pushRegression(regressions, `${taskLabel}: unexpected benchmark case ${benchmarkName}.`);
      }
    }
    for (const baselineBenchmark of baselineTask.benchmarks) {
      const baselineBenchmarkKey = benchmarkKey(baselineBenchmark);
      const actualBenchmark = actualBenchmarks.get(baselineBenchmarkKey);
      if (actualBenchmark === undefined) {
        pushRegression(
          regressions,
          `${taskLabel}: missing benchmark case ${baselineBenchmarkKey}.`,
        );
        continue;
      }
      if (actualBenchmark.sampleCount < actualTask.minimumSampleCount) {
        pushRegression(
          regressions,
          `${taskLabel} / ${baselineBenchmarkKey}: sampleCount must be at least ${actualTask.minimumSampleCount} but was ${actualBenchmark.sampleCount}.`,
        );
      }
      compareLatency(
        regressions,
        taskLabel,
        baselineBenchmarkKey,
        "mean",
        thresholds.latencyMean,
        baselineBenchmark.meanMs,
        actualBenchmark.meanMs,
      );
      compareLatency(
        regressions,
        taskLabel,
        baselineBenchmarkKey,
        "p99",
        thresholds.latencyP99,
        baselineBenchmark.p99Ms,
        actualBenchmark.p99Ms,
      );
    }
  }

  return {
    ok: regressions.length === 0,
    regressions,
  };
};
