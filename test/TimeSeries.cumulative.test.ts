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
    ],
  });
}

describe('TimeSeries.cumulative', () => {
  describe('sum', () => {
    it('computes running sum', () => {
      const c = makeSeries().cumulative({ value: 'sum' });
      expect(c.at(0)?.get('value')).toBe(10);
      expect(c.at(1)?.get('value')).toBe(30);
      expect(c.at(2)?.get('value')).toBe(60);
      expect(c.at(3)?.get('value')).toBe(100);
    });
  });

  describe('max', () => {
    it('computes running maximum', () => {
      const s = new TimeSeries({
        name: 'max',
        schema,
        rows: [
          [0, 30, 'a'],
          [1000, 10, 'b'],
          [2000, 50, 'c'],
          [3000, 20, 'd'],
        ],
      });
      const c = s.cumulative({ value: 'max' });
      expect(c.at(0)?.get('value')).toBe(30);
      expect(c.at(1)?.get('value')).toBe(30);
      expect(c.at(2)?.get('value')).toBe(50);
      expect(c.at(3)?.get('value')).toBe(50);
    });
  });

  describe('min', () => {
    it('computes running minimum', () => {
      const s = new TimeSeries({
        name: 'min',
        schema,
        rows: [
          [0, 30, 'a'],
          [1000, 10, 'b'],
          [2000, 50, 'c'],
          [3000, 5, 'd'],
        ],
      });
      const c = s.cumulative({ value: 'min' });
      expect(c.at(0)?.get('value')).toBe(30);
      expect(c.at(1)?.get('value')).toBe(10);
      expect(c.at(2)?.get('value')).toBe(10);
      expect(c.at(3)?.get('value')).toBe(5);
    });
  });

  describe('count', () => {
    it('computes running count', () => {
      const c = makeSeries().cumulative({ value: 'count' });
      expect(c.at(0)?.get('value')).toBe(1);
      expect(c.at(1)?.get('value')).toBe(2);
      expect(c.at(2)?.get('value')).toBe(3);
      expect(c.at(3)?.get('value')).toBe(4);
    });
  });

  describe('custom accumulator', () => {
    it('applies a custom function', () => {
      const c = makeSeries().cumulative({
        value: (acc, v) => acc * v,
      });
      expect(c.at(0)?.get('value')).toBe(10);
      expect(c.at(1)?.get('value')).toBe(200);
      expect(c.at(2)?.get('value')).toBe(6000);
      expect(c.at(3)?.get('value')).toBe(240000);
    });
  });

  it('preserves non-accumulated columns', () => {
    const c = makeSeries().cumulative({ value: 'sum' });
    expect(c.at(0)?.get('label')).toBe('a');
    expect(c.at(2)?.get('label')).toBe('c');
  });

  it('handles empty series', () => {
    const s = new TimeSeries({ name: 'empty', schema, rows: [] });
    expect(s.cumulative({ value: 'sum' }).length).toBe(0);
  });

  it('handles single-event series', () => {
    const s = new TimeSeries({
      name: 'single',
      schema,
      rows: [[0, 42, 'x']],
    });
    expect(s.cumulative({ value: 'sum' }).at(0)?.get('value')).toBe(42);
  });

  it('handles undefined gaps in source data', () => {
    const s = new TimeSeries({
      name: 'gappy',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'value', kind: 'number', required: false },
      ] as const,
      rows: [
        [0, 10],
        [1000, undefined],
        [2000, 30],
      ],
    });
    const c = s.cumulative({ value: 'sum' });
    expect(c.at(0)?.get('value')).toBe(10);
    expect(c.at(1)?.get('value')).toBe(10);
    expect(c.at(2)?.get('value')).toBe(40);
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
    const c = s.cumulative({ a: 'sum', b: 'max' });
    expect(c.at(2)?.get('a')).toBe(6);
    expect(c.at(2)?.get('b')).toBe(300);
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
        [0, 10, 'a'],
        [1000, 20, 'a'],
        [2000, 5, 'b'],
        [3000, 15, 'b'],
      ],
    });
    const groups = s.groupBy('host', (g) => g.cumulative({ value: 'sum' }));
    expect(groups.get('a')!.at(1)?.get('value')).toBe(30);
    expect(groups.get('b')!.at(1)?.get('value')).toBe(20);
  });
});
