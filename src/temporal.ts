export type TimestampInput = number | Date;
export type IntervalValue = string | number;
export type TimeRangeInput =
  | readonly [start: TimestampInput, end: TimestampInput]
  | { start: TimestampInput; end: TimestampInput };
export type IntervalInput =
  | readonly [value: IntervalValue, start: TimestampInput, end: TimestampInput]
  | { value: IntervalValue; start: TimestampInput; end: TimestampInput };
export type TemporalLike =
  | TimestampInput
  | TimeRangeInput
  | IntervalInput
  | { begin(): number; end(): number }
  | { timeRange(): import("./TimeRange.js").TimeRange };

export interface EventKey {
  readonly kind: "time" | "interval" | "timeRange";
  type(): "time" | "interval" | "timeRange";
  begin(): number;
  end(): number;
  timeRange(): import("./TimeRange.js").TimeRange;
  duration(): number;
  equals(other: EventKey): boolean;
  compare(other: EventKey): number;
  overlaps(other: TemporalLike): boolean;
  contains(other: TemporalLike): boolean;
  isBefore(other: TemporalLike): boolean;
  isAfter(other: TemporalLike): boolean;
  intersection(other: TemporalLike): import("./TimeRange.js").TimeRange | undefined;
  trim(other: TemporalLike): EventKey | undefined;
}

export function normalizeTimestamp(value: TimestampInput, label: string): number {
  const timestamp = value instanceof Date ? value.getTime() : value;
  if (!Number.isFinite(timestamp)) {
    throw new TypeError(`${label} must be a finite timestamp`);
  }
  return timestamp;
}

export function compareEventKeys(left: EventKey, right: EventKey): number {
  if (left.begin() !== right.begin()) {
    return left.begin() - right.begin();
  }
  if (left.end() !== right.end()) {
    return left.end() - right.end();
  }
  return left.type().localeCompare(right.type());
}

export function compareIntervalValues(left: IntervalValue, right: IntervalValue): number {
  if (left === right) {
    return 0;
  }
  if (typeof left === "number" && typeof right === "number") {
    return left - right;
  }
  if (typeof left !== typeof right) {
    return typeof left === "number" ? -1 : 1;
  }
  return String(left).localeCompare(String(right));
}
