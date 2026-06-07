export type GroupedIncrementalAdmissionLimits = {
  readonly maxGroups: number;
  readonly maxMembers: number;
  readonly maxMembersPerGroup: number;
  readonly maxRetainedValueEntries: number;
};

export const defaultGroupedIncrementalAdmissionLimits: GroupedIncrementalAdmissionLimits = {
  maxGroups: 8_192,
  maxMembers: 65_536,
  maxMembersPerGroup: 4_096,
  maxRetainedValueEntries: 65_536,
};

const positiveSafeIntegerOrDefault = (value: number | undefined, fallback: number): number =>
  value !== undefined && Number.isSafeInteger(value) && value > 0 ? value : fallback;

export const groupedIncrementalAdmissionLimitsFromConfig = (
  config: Partial<GroupedIncrementalAdmissionLimits> | undefined,
): GroupedIncrementalAdmissionLimits => ({
  maxGroups: positiveSafeIntegerOrDefault(
    config?.maxGroups,
    defaultGroupedIncrementalAdmissionLimits.maxGroups,
  ),
  maxMembers: positiveSafeIntegerOrDefault(
    config?.maxMembers,
    defaultGroupedIncrementalAdmissionLimits.maxMembers,
  ),
  maxMembersPerGroup: positiveSafeIntegerOrDefault(
    config?.maxMembersPerGroup,
    defaultGroupedIncrementalAdmissionLimits.maxMembersPerGroup,
  ),
  maxRetainedValueEntries: positiveSafeIntegerOrDefault(
    config?.maxRetainedValueEntries,
    defaultGroupedIncrementalAdmissionLimits.maxRetainedValueEntries,
  ),
});
