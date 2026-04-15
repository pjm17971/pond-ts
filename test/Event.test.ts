import { describe, expect, it } from "vitest";
import { Event, Interval, Time, TimeRange } from "../src/index.js";

describe("Event", () => {
  it("exposes typed payload access and immutable updates", () => {
    const event = new Event(new Time(1000), { cpu: 0.5, host: "api-1", healthy: true });

    expect(event.type()).toBe("time");
    expect(event.get("cpu")).toBe(0.5);
    expect(event.data()).toEqual({ cpu: 0.5, host: "api-1", healthy: true });
    expect(event.set("cpu", 0.9).get("cpu")).toBe(0.9);
    expect(event.merge({ source: "derived" }).get("source")).toBe("derived");
    expect(event.select("cpu", "healthy").data()).toEqual({ cpu: 0.5, healthy: true });
    expect(event.rename({ cpu: "usage" }).data()).toEqual({ usage: 0.5, host: "api-1", healthy: true });
  });

  it("supports select, rename and collapse while preserving the key", () => {
    const event = new Event(new Time(1000), { in: 10, out: 20, host: "api-1", healthy: true });

    expect(event.select("in", "healthy").data()).toEqual({ in: 10, healthy: true });
    expect(event.rename({ host: "server" }).data()).toEqual({
      in: 10,
      out: 20,
      server: "api-1",
      healthy: true,
    });
    expect(event.collapse(["in", "out"], "avg", ({ in: inValue, out }) => (inValue + out) / 2).data()).toEqual({
      avg: 15,
      host: "api-1",
      healthy: true,
    });
    expect(
      event.collapse(["in", "out"], "avg", ({ in: inValue, out }) => (inValue + out) / 2, { append: true }).data(),
    ).toEqual({
      in: 10,
      out: 20,
      host: "api-1",
      healthy: true,
      avg: 15,
    });
  });

  it("supports temporal delegation and trim", () => {
    const event = new Event(new TimeRange({ start: 1000, end: 2000 }), { value: 1 });

    expect(event.overlaps(new TimeRange({ start: 1500, end: 2500 }))).toBe(true);
    expect(event.contains(new Time(1500))).toBe(true);
    expect(event.intersection(new TimeRange({ start: 1500, end: 2500 }))).toEqual(
      new TimeRange({ start: 1500, end: 2000 }),
    );
    expect(event.trim(new TimeRange({ start: 1500, end: 2500 }))?.key()).toEqual(
      new TimeRange({ start: 1500, end: 2000 }),
    );
  });

  it("supports converting event key types", () => {
    const rangeEvent = new Event(new TimeRange({ start: 1000, end: 2000 }), { value: 1 });
    const intervalEvent = rangeEvent.asInterval("bucket-a");

    expect(rangeEvent.asTime().key()).toEqual(new Time(1000));
    expect(rangeEvent.asTime({ at: "center" }).key()).toEqual(new Time(1500));
    expect(rangeEvent.asTime({ at: "end" }).key()).toEqual(new Time(2000));
    expect(rangeEvent.asTimeRange().key()).toEqual(new TimeRange({ start: 1000, end: 2000 }));
    expect(intervalEvent.key()).toEqual(new Interval({ value: "bucket-a", start: 1000, end: 2000 }));
    expect(intervalEvent.get("value")).toBe(1);
  });
});
