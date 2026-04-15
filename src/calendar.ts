import { Temporal } from "@js-temporal/polyfill";

export type CalendarUnit = "day" | "week" | "month";
export type WeekStartsOn = 1 | 2 | 3 | 4 | 5 | 6 | 7;
export type TimeZoneOptions = {
  timeZone?: string;
};
export type CalendarOptions = TimeZoneOptions & {
  weekStartsOn?: WeekStartsOn;
};

const DATE_ONLY_RE = /^\d{4}-\d{2}-\d{2}$/;
const YEAR_MONTH_RE = /^\d{4}-\d{2}$/;
const DATE_TIME_LOCAL_RE = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d{1,9})?)?$/;

export function resolveTimeZone(options: TimeZoneOptions = {}): string {
  return options.timeZone ?? "UTC";
}

export function normalizeWeekStartsOn(value: number | undefined): WeekStartsOn {
  const weekStartsOn = value ?? 1;
  if (!Number.isInteger(weekStartsOn) || weekStartsOn < 1 || weekStartsOn > 7) {
    throw new TypeError("weekStartsOn must be an integer from 1 (Monday) to 7 (Sunday)");
  }
  return weekStartsOn as WeekStartsOn;
}

function zonedDateTimeFromPlainDateTime(
  plain: Temporal.PlainDateTime,
  timeZone: string,
): Temporal.ZonedDateTime {
  return Temporal.ZonedDateTime.from({
    timeZone,
    year: plain.year,
    month: plain.month,
    day: plain.day,
    hour: plain.hour,
    minute: plain.minute,
    second: plain.second,
    millisecond: plain.millisecond,
    microsecond: plain.microsecond,
    nanosecond: plain.nanosecond,
  });
}

export function parseTimestampString(value: string, options: TimeZoneOptions = {}): number {
  const timeZone = resolveTimeZone(options);

  if (YEAR_MONTH_RE.test(value)) {
    const date = Temporal.PlainYearMonth.from(value).toPlainDate({ day: 1 });
    return date
      .toZonedDateTime({ timeZone })
      .startOfDay()
      .epochMilliseconds;
  }

  if (DATE_ONLY_RE.test(value)) {
    return Temporal.PlainDate.from(value)
      .toZonedDateTime({ timeZone })
      .startOfDay()
      .epochMilliseconds;
  }

  if (DATE_TIME_LOCAL_RE.test(value)) {
    return zonedDateTimeFromPlainDateTime(Temporal.PlainDateTime.from(value), timeZone).epochMilliseconds;
  }

  return Temporal.Instant.from(value).epochMilliseconds;
}

export function toPlainDateStart(
  instantMs: number,
  timeZone: string,
  unit: CalendarUnit,
  weekStartsOn: WeekStartsOn,
): Temporal.PlainDate {
  const zoned = Temporal.Instant.fromEpochMilliseconds(instantMs).toZonedDateTimeISO(timeZone);
  const date = zoned.toPlainDate();

  if (unit === "day") {
    return date;
  }

  if (unit === "month") {
    return Temporal.PlainDate.from({ year: date.year, month: date.month, day: 1 });
  }

  const offset = (date.dayOfWeek - weekStartsOn + 7) % 7;
  return date.subtract({ days: offset });
}

export function plainDateToStart(date: Temporal.PlainDate, timeZone: string): Temporal.ZonedDateTime {
  return date.toZonedDateTime({ timeZone }).startOfDay();
}

export function nextCalendarStart(
  current: Temporal.PlainDate,
  unit: CalendarUnit,
): Temporal.PlainDate {
  if (unit === "day") {
    return current.add({ days: 1 });
  }
  if (unit === "week") {
    return current.add({ weeks: 1 });
  }
  return current.add({ months: 1 });
}

export function dayRangeForDate(
  reference: string,
  options: TimeZoneOptions = {},
): { start: number; end: number } {
  const timeZone = resolveTimeZone(options);
  const start = Temporal.PlainDate.from(reference).toZonedDateTime({ timeZone }).startOfDay();
  const end = start.add({ days: 1 });
  return {
    start: start.epochMilliseconds,
    end: end.epochMilliseconds,
  };
}

export function calendarRangeForReference(
  unit: CalendarUnit,
  reference: string,
  options: CalendarOptions = {},
): { start: number; end: number } {
  const timeZone = resolveTimeZone(options);
  const weekStartsOn = normalizeWeekStartsOn(options.weekStartsOn);
  const referenceMs = parseTimestampString(reference, { timeZone });
  const startDate = toPlainDateStart(referenceMs, timeZone, unit, weekStartsOn);
  const start = plainDateToStart(startDate, timeZone);
  const end = plainDateToStart(nextCalendarStart(startDate, unit), timeZone);
  return {
    start: start.epochMilliseconds,
    end: end.epochMilliseconds,
  };
}
