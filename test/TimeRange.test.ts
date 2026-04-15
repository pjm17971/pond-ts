import { describe, expect, it } from "vitest";
import { Time, TimeRange } from "../src/index.js";

describe("TimeRange", () => {
  it("constructs an interval and exposes duration and midpoint", () => {
    const range = new TimeRange({ start: 1000, end: 3000 });

    expect(range.type()).toBe("timeRange");
    expect(range.begin()).toBe(1000);
    expect(range.end()).toBe(3000);
    expect(range.duration()).toBe(2000);
    expect(range.midpoint()).toBe(2000);
    expect(range.timeRange()).toBe(range);
  });

  it("supports containment, overlap, intersection and trim", () => {
    const outer = new TimeRange({ start: 1000, end: 3000 });
    const inner = new TimeRange({ start: 1500, end: 2500 });
    const overlap = new TimeRange({ start: 2500, end: 3500 });
    const disjoint = new TimeRange({ start: 4000, end: 5000 });

    expect(outer.contains(inner)).toBe(true);
    expect(outer.contains(new Time(2000))).toBe(true);
    expect(outer.overlaps(overlap)).toBe(true);
    expect(outer.overlaps(disjoint)).toBe(false);
    expect(outer.intersection(overlap)).toEqual(new TimeRange({ start: 2500, end: 3000 }));
    expect(outer.trim(new TimeRange({ start: 1200, end: 1800 }))).toEqual(
      new TimeRange({ start: 1200, end: 1800 }),
    );
    expect(outer.isBefore(disjoint)).toBe(true);
    expect(disjoint.isAfter(outer)).toBe(true);
  });

  it("builds local-day and calendar ranges from string references", () => {
    expect(TimeRange.fromDate("2025-01-01", { timeZone: "Europe/Madrid" })).toEqual(
      new TimeRange({
        start: Date.parse("2024-12-31T23:00:00.000Z"),
        end: Date.parse("2025-01-01T23:00:00.000Z"),
      }),
    );
    expect(TimeRange.fromCalendar("week", "2025-01-01", { timeZone: "UTC", weekStartsOn: 1 })).toEqual(
      new TimeRange({
        start: Date.parse("2024-12-30T00:00:00.000Z"),
        end: Date.parse("2025-01-06T00:00:00.000Z"),
      }),
    );
    expect(TimeRange.fromCalendar("month", "2025-01", { timeZone: "UTC" })).toEqual(
      new TimeRange({
        start: Date.parse("2025-01-01T00:00:00.000Z"),
        end: Date.parse("2025-02-01T00:00:00.000Z"),
      }),
    );
  });
});
