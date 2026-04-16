import { describe, expect, it } from 'vitest';
import { Interval, Time, TimeSeries } from '../src/index.js';

describe('TimeSeries includesKey performance regression coverage', () => {
  it('matches exact-key semantics for time and interval keyed series', () => {
    const timeSchema = [
      { name: 'time', kind: 'time' },
      { name: 'value', kind: 'number' },
    ] as const;

    const timeSeries = new TimeSeries({
      name: 'cpu',
      schema: timeSchema,
      rows: [
        [0, 1],
        [10, 2],
        [20, 3],
      ],
    });

    expect(timeSeries.includesKey(new Time(10))).toBe(true);
    expect(timeSeries.includesKey(new Time(11))).toBe(false);

    const intervalSchema = [
      { name: 'interval', kind: 'interval' },
      { name: 'value', kind: 'number' },
    ] as const;

    const intervalSeries = new TimeSeries({
      name: 'windowed',
      schema: intervalSchema,
      rows: [
        [new Interval({ value: 'a', start: 0, end: 10 }), 1],
        [new Interval({ value: 'b', start: 10, end: 20 }), 2],
        [new Interval({ value: 'c', start: 20, end: 30 }), 3],
      ],
    });

    expect(
      intervalSeries.includesKey(
        new Interval({ value: 'b', start: 10, end: 20 }),
      ),
    ).toBe(true);
    expect(
      intervalSeries.includesKey(
        new Interval({ value: 'missing', start: 10, end: 20 }),
      ),
    ).toBe(false);
  });
});
