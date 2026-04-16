import { describe, expect, it } from 'vitest';
import {
  BoundedSequence,
  Interval,
  Sequence,
  TimeRange,
  TimeSeries,
} from '../src/index.js';

describe('TimeSeries linear align performance regression coverage', () => {
  it('preserves exact samples, interpolates between points, and falls back to the previous event past the right edge', () => {
    const schema = [
      { name: 'time', kind: 'time' },
      { name: 'value', kind: 'number' },
      { name: 'status', kind: 'string' },
    ] as const;

    const series = new TimeSeries({
      name: 'cpu',
      schema,
      rows: [
        [10, 0, 'cold'],
        [20, 10, 'warm'],
        [30, 30, 'hot'],
      ],
    });

    const aligned = series.align(Sequence.every(5), {
      method: 'linear',
      range: new TimeRange({ start: 10, end: 30 }),
    });

    expect(aligned.at(0)?.get('value')).toBe(0);
    expect(aligned.at(0)?.get('status')).toBe('cold');
    expect(aligned.at(1)?.get('value')).toBe(5);
    expect(aligned.at(1)?.get('status')).toBe('cold');
    expect(aligned.at(2)?.get('value')).toBe(10);
    expect(aligned.at(2)?.get('status')).toBe('warm');
    expect(aligned.at(3)?.get('value')).toBe(20);
    expect(aligned.at(3)?.get('status')).toBe('warm');
    expect(aligned.at(4)?.get('value')).toBe(30);
    expect(aligned.at(4)?.get('status')).toBe('hot');

    const beyondEnd = series.align(
      new BoundedSequence([new Interval({ value: 35, start: 35, end: 40 })]),
      { method: 'linear' },
    );

    expect(beyondEnd.at(0)?.get('value')).toBe(30);
    expect(beyondEnd.at(0)?.get('status')).toBe('hot');
  });
});
