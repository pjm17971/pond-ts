import { BoundedSequence } from './BoundedSequence.js';
import {
  type CalendarOptions,
  type CalendarUnit,
  type WeekStartsOn,
  normalizeWeekStartsOn,
  nextCalendarStart,
  plainDateToStart,
  resolveTimeZone,
  toPlainDateStart,
} from './calendar.js';
import { Interval } from './Interval.js';
import { TimeRange } from './TimeRange.js';
import type {
  TemporalLike,
  TimestampInput,
  TimeRangeInput,
  IntervalInput,
} from './temporal.js';

export type DurationInput = number | `${number}${'ms' | 's' | 'm' | 'h' | 'd'}`;
export type SequenceSample = 'begin' | 'center';

type FixedSequenceInput = {
  every: DurationInput;
  anchor?: TimestampInput;
};

type CalendarSequenceInput = {
  unit: CalendarUnit;
  timeZone: string;
  weekStartsOn?: WeekStartsOn;
};

function normalizeTimestamp(value: TimestampInput): number {
  return value instanceof Date ? value.getTime() : value;
}

function toTimeRange(value: TemporalLike): TimeRange {
  if (
    typeof value === 'object' &&
    value !== null &&
    'timeRange' in value &&
    typeof value.timeRange === 'function'
  ) {
    return value.timeRange();
  }
  if (
    typeof value === 'object' &&
    value !== null &&
    'begin' in value &&
    'end' in value
  ) {
    return new TimeRange({ start: value.begin(), end: value.end() });
  }
  if (value instanceof Date || typeof value === 'number') {
    const timestamp = normalizeTimestamp(value);
    return new TimeRange({ start: timestamp, end: timestamp });
  }
  if (Array.isArray(value)) {
    if (value.length === 2) {
      return new TimeRange(value as TimeRangeInput);
    }
    return new Interval(value as IntervalInput).timeRange();
  }
  if ('value' in value) {
    return new Interval(value as IntervalInput).timeRange();
  }
  return new TimeRange(value as TimeRangeInput);
}

function parseDuration(value: DurationInput): number {
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || value <= 0) {
      throw new TypeError(
        'sequence duration must be a positive finite number of milliseconds',
      );
    }
    return value;
  }

  const match = /^(\d+)(ms|s|m|h|d)$/.exec(value);
  if (!match) {
    throw new TypeError(`unsupported duration '${value}'`);
  }

  const amount = Number(match[1]);
  const unit = match[2];
  const multiplier =
    unit === 'ms'
      ? 1
      : unit === 's'
        ? 1_000
        : unit === 'm'
          ? 60_000
          : unit === 'h'
            ? 3_600_000
            : 86_400_000;
  return amount * multiplier;
}

/**
 * An unbounded fixed-step grid definition used for alignment or aggregation.
 *
 * `Sequence` defines where buckets fall. Call `bounded(...)` to realize a finite `BoundedSequence`
 * over a specific range.
 *
 * Important distinction:
 * - the sequence `anchor` defines where the unbounded grid starts
 * - the caller-supplied `range` defines which finite slice is realized
 *
 * The default anchor is Unix epoch `0`.
 */
export class Sequence {
  readonly #kind: 'fixed' | 'calendar';
  readonly #stepMs?: number;
  readonly #anchorMs?: number;
  readonly #calendarUnit?: CalendarUnit;
  readonly #timeZone?: string;
  readonly #weekStartsOn?: 1 | 2 | 3 | 4 | 5 | 6 | 7;

  constructor(input: FixedSequenceInput | CalendarSequenceInput) {
    if ('every' in input) {
      this.#kind = 'fixed';
      this.#stepMs = parseDuration(input.every);
      this.#anchorMs = normalizeTimestamp(input.anchor ?? 0);
    } else {
      this.#kind = 'calendar';
      this.#calendarUnit = input.unit;
      this.#timeZone = input.timeZone;
      this.#weekStartsOn = normalizeWeekStartsOn(input.weekStartsOn);
    }
    Object.freeze(this);
  }

  /**
   * Creates an unbounded fixed-step sequence.
   *
   * The returned sequence is a grid definition, not a finite bucket list. By default the grid is
   * anchored at Unix epoch `0`, which makes independently-created sequences line up by default.
   * Use `bounded(...)` or series operations like `align(...)` / `aggregate(...)` to realize a
   * finite slice of the grid over a concrete range.
   */
  static every(
    every: DurationInput,
    options: { anchor?: TimestampInput } = {},
  ): Sequence {
    return options.anchor === undefined
      ? new Sequence({ every })
      : new Sequence({ every, anchor: options.anchor });
  }

  /** Example: `Sequence.hourly()`. Creates an hourly fixed-step sequence. */
  static hourly(options: { anchor?: TimestampInput } = {}): Sequence {
    return Sequence.every('1h', options);
  }

  /** Example: `Sequence.daily()`. Creates a daily fixed-step sequence. */
  static daily(options: { anchor?: TimestampInput } = {}): Sequence {
    return Sequence.every('1d', options);
  }

  /**
   * Creates an unbounded calendar-aware sequence.
   *
   * Calendar sequences step by local calendar boundaries in an IANA time zone instead of by a
   * fixed millisecond duration. Supported units are `"day"`, `"week"`, and `"month"`.
   *
   * Defaults:
   * - `timeZone`: `"UTC"`
   */
  static calendar(unit: CalendarUnit, options: CalendarOptions = {}): Sequence {
    const timeZone = resolveTimeZone(options);
    return options.weekStartsOn === undefined
      ? new Sequence({ unit, timeZone })
      : new Sequence({ unit, timeZone, weekStartsOn: options.weekStartsOn });
  }

  /** Example: `sequence.kind()`. Returns whether this sequence is fixed-step or calendar-aware. */
  kind(): 'fixed' | 'calendar' {
    return this.#kind;
  }

  /** Example: `sequence.anchor()`. Returns the millisecond anchor used by this grid definition. */
  anchor(): number {
    if (this.#kind !== 'fixed') {
      throw new TypeError(
        'calendar sequences do not have a fixed millisecond anchor',
      );
    }
    return this.#anchorMs!;
  }

  /** Example: `sequence.stepMs()`. Returns the fixed interval size in milliseconds. */
  stepMs(): number {
    if (this.#kind !== 'fixed') {
      throw new TypeError(
        'calendar sequences do not have a fixed millisecond step size',
      );
    }
    return this.#stepMs!;
  }

  /** Example: `sequence.timeZone()`. Returns the IANA time zone for calendar-aware sequences, if any. */
  timeZone(): string | undefined {
    return this.#timeZone;
  }

  /**
   * Example: `sequence.bounded(new TimeRange({ start, end }))`.
   * Realizes a finite `BoundedSequence` over the supplied range.
   *
   * Sample position controls which interval starts are selected:
   * `begin` includes buckets whose starts fall within the range, while `center` includes buckets
   * whose midpoints fall within the range.
   */
  bounded(
    range: TemporalLike,
    options: { sample?: SequenceSample } = {},
  ): BoundedSequence {
    const sample = options.sample ?? 'begin';
    const requested = toTimeRange(range);
    const intervals: Interval[] = [];

    if (this.#kind === 'fixed') {
      const stepMs = this.#stepMs!;
      const anchorMs = this.#anchorMs!;
      const sampleOffset = sample === 'center' ? stepMs / 2 : 0;
      const firstIndex = Math.ceil(
        (requested.begin() - sampleOffset - anchorMs) / stepMs,
      );
      const lastIndex = Math.floor(
        (requested.end() - sampleOffset - anchorMs) / stepMs,
      );

      for (let index = firstIndex; index <= lastIndex; index += 1) {
        const start = anchorMs + index * stepMs;
        intervals.push(
          new Interval({ value: start, start, end: start + stepMs }),
        );
      }

      return new BoundedSequence(intervals);
    }

    const timeZone = this.#timeZone!;
    const unit = this.#calendarUnit!;
    const weekStartsOn = this.#weekStartsOn!;
    let currentDate = toPlainDateStart(
      requested.begin(),
      timeZone,
      unit,
      weekStartsOn,
    );

    while (true) {
      const currentStart = plainDateToStart(currentDate, timeZone);
      const nextDate = nextCalendarStart(currentDate, unit);
      const nextStart = plainDateToStart(nextDate, timeZone);
      const start = currentStart.epochMilliseconds;
      const end = nextStart.epochMilliseconds;
      const sampleTime =
        sample === 'center' ? start + (end - start) / 2 : start;

      if (sampleTime > requested.end()) {
        break;
      }

      if (sampleTime >= requested.begin()) {
        intervals.push(new Interval({ value: start, start, end }));
      }

      currentDate = nextDate;
    }

    return new BoundedSequence(intervals);
  }
}
