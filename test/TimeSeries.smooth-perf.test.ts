import { describe, expect, it } from 'vitest';
import { Interval, Time, TimeSeries } from '../src/index.js';

type Alignment = 'trailing' | 'leading' | 'centered';

function valuesInWindow(
  anchors: number[],
  values: Array<number | undefined>,
  anchor: number,
  windowMs: number,
  alignment: Alignment,
): number[] {
  return anchors.flatMap((candidate, index) => {
    const included =
      alignment === 'trailing'
        ? candidate > anchor - windowMs && candidate <= anchor
        : alignment === 'leading'
          ? candidate >= anchor && candidate < anchor + windowMs
          : candidate >= anchor - windowMs / 2 &&
            candidate < anchor + windowMs / 2;
    const value = values[index];
    return included && value !== undefined ? [value] : [];
  });
}

describe('TimeSeries smooth performance regression coverage', () => {
  it('matches a naive moving-average implementation across alignments', () => {
    const schema = [
      { name: 'time', kind: 'time' },
      { name: 'value', kind: 'number', required: false },
    ] as const;

    const rows = [
      [0, 1],
      [5, undefined],
      [10, 3],
      [15, 5],
      [20, 7],
    ] as const;

    const series = new TimeSeries({
      name: 'cpu',
      schema,
      rows,
    });

    const anchors = rows.map(([time]) => time);
    const values = rows.map(([, value]) => value);
    const windowMs = 10;

    for (const alignment of ['trailing', 'leading', 'centered'] as const) {
      const smoothed = series.smooth('value', 'movingAverage', {
        window: windowMs,
        alignment,
      });

      for (let index = 0; index < rows.length; index += 1) {
        const contributingValues = valuesInWindow(
          anchors,
          [...values],
          anchors[index]!,
          windowMs,
          alignment,
        );
        const expected =
          contributingValues.length === 0
            ? undefined
            : contributingValues.reduce((sum, value) => sum + value, 0) /
              contributingValues.length;

        expect(smoothed.at(index)?.key()).toEqual(new Time(anchors[index]!));
        expect(smoothed.at(index)?.get('value')).toBe(expected);
      }
    }
  });

  it('preserves key types and supports appended moving-average output columns', () => {
    const schema = [
      { name: 'interval', kind: 'interval' },
      { name: 'value', kind: 'number' },
      { name: 'status', kind: 'string' },
    ] as const;

    const series = new TimeSeries({
      name: 'windows',
      schema,
      rows: [
        [{ value: 'a', start: 0, end: 10 }, 1, 'cold'],
        [{ value: 'b', start: 20, end: 30 }, 3, 'warm'],
        [{ value: 'c', start: 40, end: 50 }, 5, 'hot'],
      ],
    });

    const smoothed = series.smooth('value', 'movingAverage', {
      window: 50,
      alignment: 'centered',
      output: 'valueAvg',
    });

    expect(smoothed.at(0)?.key()).toEqual(
      new Interval({ value: 'a', start: 0, end: 10 }),
    );
    expect(smoothed.at(0)?.get('value')).toBe(1);
    expect(smoothed.at(0)?.get('valueAvg')).toBe(2);
    expect(smoothed.at(1)?.get('status')).toBe('warm');
    expect(smoothed.at(1)?.get('valueAvg')).toBe(3);
    expect(smoothed.at(2)?.get('valueAvg')).toBe(4);
  });
});
