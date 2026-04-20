import { describe, expect, it } from 'vitest';
import { Sequence, TimeRange, TimeSeries } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
] as const;

function makeSeries() {
  return new TimeSeries({
    name: 'cpu',
    schema,
    rows: [
      [0, 10],
      [5, 20],
      [10, 30],
      [15, 40],
      [20, 50],
    ],
  });
}

describe('chained operations on derived series', () => {
  it('filter then aggregate', () => {
    const filtered = makeSeries().filter(
      (e) => (e.get('value') as number) > 10,
    );
    const agg = filtered.aggregate(
      Sequence.every(10),
      { value: 'sum' },
      {
        range: new TimeRange({ start: 0, end: 19 }),
      },
    );
    expect(agg.length).toBe(2);
    expect(agg.at(0)?.get('value')).toBe(20);
    expect(agg.at(1)?.get('value')).toBe(70);
  });

  it('filter then align (hold)', () => {
    const filtered = makeSeries().filter(
      (e) => (e.get('value') as number) >= 20,
    );
    const aligned = filtered.align(Sequence.every(5), {
      range: new TimeRange({ start: 5, end: 15 }),
    });
    expect(aligned.length).toBe(3);
    expect(aligned.at(0)?.get('value')).toBe(20);
    expect(aligned.at(1)?.get('value')).toBe(30);
    expect(aligned.at(2)?.get('value')).toBe(40);
  });

  it('filter then align (linear)', () => {
    const filtered = makeSeries().filter(
      (e) => (e.get('value') as number) <= 30,
    );
    const aligned = filtered.align(Sequence.every(5), {
      method: 'linear',
      range: new TimeRange({ start: 0, end: 10 }),
    });
    expect(aligned.length).toBe(3);
    expect(aligned.at(0)?.get('value')).toBe(10);
    expect(aligned.at(1)?.get('value')).toBe(20);
    expect(aligned.at(2)?.get('value')).toBe(30);
  });

  it('select then aggregate', () => {
    const selected = makeSeries().select('value');
    const agg = selected.aggregate(
      Sequence.every(10),
      { value: 'avg' },
      {
        range: new TimeRange({ start: 0, end: 9 }),
      },
    );
    expect(agg.length).toBe(1);
    expect(agg.at(0)?.get('value')).toBe(15);
  });

  it('slice then align', () => {
    const sliced = makeSeries().slice(1, 4);
    const aligned = sliced.align(Sequence.every(5), {
      range: new TimeRange({ start: 5, end: 15 }),
    });
    expect(aligned.length).toBe(3);
  });

  it('triple chain: filter then select then aggregate', () => {
    const result = makeSeries()
      .filter((e) => (e.get('value') as number) > 10)
      .select('value')
      .aggregate(
        Sequence.every(10),
        { value: 'count' },
        {
          range: new TimeRange({ start: 0, end: 19 }),
        },
      );
    expect(result.at(0)?.get('value')).toBe(1);
    expect(result.at(1)?.get('value')).toBe(2);
  });
});
