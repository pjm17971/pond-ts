import { describe, expect, it } from 'vitest';
import { Interval, Sequence, TimeRange, TimeSeries } from '../src/index.js';

describe('TimeSeries aggregate performance regression coverage', () => {
  it('matches expected reducer behavior for point-series bucket aggregation', () => {
    const schema = [
      { name: 'time', kind: 'time' },
      { name: 'value', kind: 'number' },
      { name: 'status', kind: 'string', required: false },
    ] as const;

    const series = new TimeSeries({
      name: 'cpu',
      schema,
      rows: [
        [0, 1, 'a'],
        [5, 2, undefined],
        [10, 3, 'b'],
        [15, 4, 'c'],
        [20, 5, 'd'],
        [25, 6, undefined],
      ],
    });

    const aggregated = series.aggregate(
      Sequence.every(10),
      {
        value: 'sum',
        status: 'first',
      },
      { range: new TimeRange({ start: 0, end: 25 }) },
    );

    const latest = series.aggregate(
      Sequence.every(10),
      {
        value: 'count',
        status: 'last',
      },
      { range: new TimeRange({ start: 0, end: 25 }) },
    );

    expect(aggregated.length).toBe(3);
    expect(aggregated.at(0)?.key()).toEqual(
      new Interval({ value: 0, start: 0, end: 10 }),
    );
    expect(aggregated.at(0)?.get('value')).toBe(3);
    expect(aggregated.at(0)?.get('status')).toBe('a');
    expect(aggregated.at(1)?.get('value')).toBe(7);
    expect(aggregated.at(1)?.get('status')).toBe('b');
    expect(aggregated.at(2)?.get('value')).toBe(11);
    expect(aggregated.at(2)?.get('status')).toBe('d');

    expect(latest.length).toBe(3);
    expect(latest.at(0)?.get('value')).toBe(2);
    expect(latest.at(0)?.get('status')).toBe('a');
    expect(latest.at(1)?.get('value')).toBe(2);
    expect(latest.at(1)?.get('status')).toBe('c');
    expect(latest.at(2)?.get('value')).toBe(2);
    expect(latest.at(2)?.get('status')).toBe('d');
  });
});
