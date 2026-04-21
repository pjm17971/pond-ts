import { describe, expect, it } from 'vitest';
import { TimeSeries } from '../src/index.js';

describe('TimeSeries loess performance regression coverage', () => {
  it('preserves output-column semantics and ignores undefined source values', () => {
    const schema = [
      { name: 'time', kind: 'time' },
      { name: 'value', kind: 'number', required: false },
      { name: 'status', kind: 'string' },
    ] as const;

    const series = new TimeSeries({
      name: 'cpu',
      schema,
      rows: [
        [0, 0, 'a'],
        [10, undefined, 'b'],
        [20, 20, 'c'],
        [30, 30, 'd'],
        [40, undefined, 'e'],
        [50, 50, 'f'],
      ],
    });

    const smoothed = series.smooth('value', 'loess', {
      span: 0.75,
      output: 'valueLoess',
    });

    expect(smoothed.length).toBe(series.length);
    expect(smoothed.at(0)?.get('value')).toBe(0);
    expect(smoothed.at(1)?.get('value')).toBeUndefined();
    expect(smoothed.at(2)?.get('status')).toBe('c');
    expect(smoothed.at(0)?.get('valueLoess')).toBeTypeOf('number');
    expect(smoothed.at(1)?.get('valueLoess')).toBeTypeOf('number');
    expect(smoothed.at(5)?.get('valueLoess')).toBeTypeOf('number');
    expect(smoothed.at(2)?.get('valueLoess')).toBeGreaterThan(15);
    expect(smoothed.at(2)?.get('valueLoess')).toBeLessThan(25);
    expect(smoothed.at(5)?.get('valueLoess')).toBeGreaterThan(45);
    expect(smoothed.at(5)?.get('valueLoess')).toBeLessThan(55);
  });
});
