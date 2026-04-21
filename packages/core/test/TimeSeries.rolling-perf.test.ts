import { describe, expect, it } from 'vitest';
import { Time, TimeSeries, type AggregateFunction } from '../src/index.js';

type Alignment = 'trailing' | 'leading' | 'centered';

function naiveWindowMembers(
  times: number[],
  anchor: number,
  windowMs: number,
  alignment: Alignment,
): number[] {
  return times.filter((candidate) => {
    if (alignment === 'trailing') {
      return candidate > anchor - windowMs && candidate <= anchor;
    }
    if (alignment === 'leading') {
      return candidate >= anchor && candidate < anchor + windowMs;
    }

    const halfWindow = windowMs / 2;
    return candidate >= anchor - halfWindow && candidate < anchor + halfWindow;
  });
}

function aggregate(operation: AggregateFunction, values: number[]): number {
  if (operation === 'count') {
    return values.length;
  }
  if (operation === 'sum') {
    return values.reduce((sum, value) => sum + value, 0);
  }
  if (operation === 'avg') {
    return values.reduce((sum, value) => sum + value, 0) / values.length;
  }
  if (operation === 'min') {
    return Math.min(...values);
  }
  if (operation === 'max') {
    return Math.max(...values);
  }

  throw new TypeError(`unsupported numeric reducer in test: ${operation}`);
}

describe('TimeSeries rolling performance regression coverage', () => {
  it('preserves first and last reducers in event-driven rolling windows', () => {
    const schema = [
      { name: 'time', kind: 'time' },
      { name: 'status', kind: 'string', required: false },
    ] as const;

    const series = new TimeSeries({
      name: 'cpu',
      schema,
      rows: [
        [0, 'a'],
        [5, undefined],
        [10, 'b'],
        [15, 'c'],
      ],
    });

    const rolled = series.rolling(10, { status: 'first' });
    const latest = series.rolling(10, { status: 'last' });

    expect(rolled.at(0)?.get('status')).toBe('a');
    expect(rolled.at(1)?.get('status')).toBe('a');
    expect(rolled.at(2)?.get('status')).toBe('b');
    expect(rolled.at(3)?.get('status')).toBe('b');

    expect(latest.at(0)?.get('status')).toBe('a');
    expect(latest.at(1)?.get('status')).toBe('a');
    expect(latest.at(2)?.get('status')).toBe('b');
    expect(latest.at(3)?.get('status')).toBe('c');
  });

  it('matches a naive event-driven rolling implementation across alignments', () => {
    const schema = [
      { name: 'time', kind: 'time' },
      { name: 'value', kind: 'number' },
      { name: 'load', kind: 'number' },
    ] as const;

    const rows = Array.from({ length: 24 }, (_, index) => {
      const time = index * 5;
      return [time, index + 1, (index % 4) + 1] as const;
    });

    const series = new TimeSeries({
      name: 'cpu',
      schema,
      rows,
    });

    const times = rows.map(([time]) => time);
    const windowMs = 20;

    for (const alignment of ['trailing', 'leading', 'centered'] as const) {
      const rolled = series.rolling(
        windowMs,
        { value: 'avg', load: 'sum' },
        { alignment },
      );

      expect(rolled.length).toBe(series.length);

      for (let index = 0; index < rows.length; index += 1) {
        const anchor = times[index]!;
        const members = naiveWindowMembers(times, anchor, windowMs, alignment);
        const memberIndexes = members.map((time) => times.indexOf(time));
        const valueInputs = memberIndexes.map(
          (memberIndex) => rows[memberIndex]![1],
        );
        const loadInputs = memberIndexes.map(
          (memberIndex) => rows[memberIndex]![2],
        );

        const event = rolled.at(index);
        expect(event?.key()).toEqual(new Time(anchor));
        expect(event?.get('value')).toBe(aggregate('avg', valueInputs));
        expect(event?.get('load')).toBe(aggregate('sum', loadInputs));
      }
    }
  });
});
