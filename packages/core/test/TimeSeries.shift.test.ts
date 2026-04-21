import { describe, expect, it } from 'vitest';
import { TimeSeries } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
  { name: 'label', kind: 'string' },
] as const;

function makeSeries() {
  return new TimeSeries({
    name: 'test',
    schema,
    rows: [
      [0, 10, 'a'],
      [1000, 20, 'b'],
      [2000, 30, 'c'],
      [3000, 40, 'd'],
      [4000, 50, 'e'],
    ],
  });
}

describe('TimeSeries.shift', () => {
  it('lags by 1 (positive offset)', () => {
    const s = makeSeries().shift('value', 1);
    expect(s.at(0)?.get('value')).toBeUndefined();
    expect(s.at(1)?.get('value')).toBe(10);
    expect(s.at(2)?.get('value')).toBe(20);
    expect(s.at(3)?.get('value')).toBe(30);
    expect(s.at(4)?.get('value')).toBe(40);
  });

  it('leads by 1 (negative offset)', () => {
    const s = makeSeries().shift('value', -1);
    expect(s.at(0)?.get('value')).toBe(20);
    expect(s.at(1)?.get('value')).toBe(30);
    expect(s.at(2)?.get('value')).toBe(40);
    expect(s.at(3)?.get('value')).toBe(50);
    expect(s.at(4)?.get('value')).toBeUndefined();
  });

  it('lags by 2', () => {
    const s = makeSeries().shift('value', 2);
    expect(s.at(0)?.get('value')).toBeUndefined();
    expect(s.at(1)?.get('value')).toBeUndefined();
    expect(s.at(2)?.get('value')).toBe(10);
    expect(s.at(3)?.get('value')).toBe(20);
    expect(s.at(4)?.get('value')).toBe(30);
  });

  it('shift by 0 is identity', () => {
    const s = makeSeries().shift('value', 0);
    for (let i = 0; i < 5; i++) {
      expect(s.at(i)?.get('value')).toBe(makeSeries().at(i)?.get('value'));
    }
  });

  it('preserves non-target columns', () => {
    const s = makeSeries().shift('value', 1);
    expect(s.at(0)?.get('label')).toBe('a');
    expect(s.at(1)?.get('label')).toBe('b');
  });

  it('preserves event keys', () => {
    const s = makeSeries().shift('value', 1);
    expect(s.at(0)?.begin()).toBe(0);
    expect(s.at(1)?.begin()).toBe(1000);
  });

  it('supports multiple columns', () => {
    const s = new TimeSeries({
      name: 'multi',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'a', kind: 'number' },
        { name: 'b', kind: 'number' },
      ] as const,
      rows: [
        [0, 1, 100],
        [1000, 2, 200],
        [2000, 3, 300],
      ],
    });
    const shifted = s.shift(['a', 'b'], 1);
    expect(shifted.at(0)?.get('a')).toBeUndefined();
    expect(shifted.at(0)?.get('b')).toBeUndefined();
    expect(shifted.at(1)?.get('a')).toBe(1);
    expect(shifted.at(1)?.get('b')).toBe(100);
  });

  it('handles empty series', () => {
    const s = new TimeSeries({ name: 'empty', schema, rows: [] });
    expect(s.shift('value', 1).length).toBe(0);
  });

  it('handles single-event series', () => {
    const s = new TimeSeries({
      name: 'single',
      schema,
      rows: [[0, 42, 'x']],
    });
    const lagged = s.shift('value', 1);
    expect(lagged.length).toBe(1);
    expect(lagged.at(0)?.get('value')).toBeUndefined();
  });

  it('shift larger than series length fills all with undefined', () => {
    const s = makeSeries().shift('value', 10);
    for (let i = 0; i < 5; i++) {
      expect(s.at(i)?.get('value')).toBeUndefined();
    }
  });

  it('composes with groupBy', () => {
    const s = new TimeSeries({
      name: 'grouped',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'value', kind: 'number' },
        { name: 'host', kind: 'string' },
      ] as const,
      rows: [
        [0, 100, 'a'],
        [1000, 200, 'a'],
        [2000, 50, 'b'],
        [3000, 75, 'b'],
      ],
    });
    const groups = s.groupBy('host', (g) => g.shift('value', 1));
    expect(groups.get('a')!.at(0)?.get('value')).toBeUndefined();
    expect(groups.get('a')!.at(1)?.get('value')).toBe(100);
    expect(groups.get('b')!.at(0)?.get('value')).toBeUndefined();
    expect(groups.get('b')!.at(1)?.get('value')).toBe(50);
  });
});
