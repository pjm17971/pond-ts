import { describe, expect, it } from 'vitest';
import { Time, TimeRange } from '../src/index.js';

describe('Time', () => {
  it('constructs a point key and exposes its temporal API', () => {
    const time = new Time(new Date('2025-01-01T00:00:00.000Z'));
    const same = new Time(1735689600000);
    const later = new Time(1735689660000);

    expect(time.type()).toBe('time');
    expect(time.begin()).toBe(1735689600000);
    expect(time.end()).toBe(1735689600000);
    expect(time.timestampMs()).toBe(1735689600000);
    expect(time.duration()).toBe(0);
    expect(time.timeRange()).toEqual(
      new TimeRange({ start: 1735689600000, end: 1735689600000 }),
    );
    expect(time.equals(same)).toBe(true);
    expect(time.compare(later)).toBeLessThan(0);
  });

  it('supports temporal relation and trim operations', () => {
    const time = new Time(1000);
    const containing = new TimeRange({ start: 500, end: 1500 });
    const disjoint = new TimeRange({ start: 1001, end: 2000 });

    expect(time.overlaps(containing)).toBe(true);
    expect(time.contains(1000)).toBe(true);
    expect(time.isBefore(new Time(2000))).toBe(true);
    expect(time.isAfter(new Time(500))).toBe(true);
    expect(time.intersection(containing)).toEqual(
      new TimeRange({ start: 1000, end: 1000 }),
    );
    expect(time.trim(containing)).toBe(time);
    expect(time.trim(disjoint)).toBeUndefined();
  });

  it('converts to a native Date via toDate()', () => {
    const time = new Time(1735689600000);
    const date = time.toDate();
    expect(date).toBeInstanceOf(Date);
    expect(date.getTime()).toBe(1735689600000);
    expect(date.toISOString()).toBe('2025-01-01T00:00:00.000Z');
  });

  it('parses local and absolute timestamp strings with timezone awareness', () => {
    expect(
      Time.parse('2025-01-01T09:00', { timeZone: 'Europe/Madrid' }),
    ).toEqual(new Time(Date.parse('2025-01-01T08:00:00.000Z')));
    expect(Time.parse('2025-01-01', { timeZone: 'UTC' })).toEqual(
      new Time(Date.parse('2025-01-01T00:00:00.000Z')),
    );
    expect(Time.parse('2025-01-01T09:00:00+01:00')).toEqual(
      new Time(Date.parse('2025-01-01T08:00:00.000Z')),
    );
  });
});
