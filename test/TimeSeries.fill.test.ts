import { describe, expect, it } from 'vitest';
import { TimeSeries } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number', required: false },
  { name: 'host', kind: 'string', required: false },
] as const;

function makeGappy() {
  return new TimeSeries({
    name: 'gappy',
    schema,
    rows: [
      [1000, 10, 'a'],
      [2000, undefined, undefined],
      [3000, undefined, undefined],
      [4000, 40, 'b'],
      [5000, 50, 'c'],
    ],
  });
}

describe('TimeSeries.fill', () => {
  describe('hold strategy', () => {
    it('forward fills undefined values', () => {
      const filled = makeGappy().fill('hold');
      expect(filled.at(0)?.get('value')).toBe(10);
      expect(filled.at(1)?.get('value')).toBe(10);
      expect(filled.at(2)?.get('value')).toBe(10);
      expect(filled.at(3)?.get('value')).toBe(40);
      expect(filled.at(4)?.get('value')).toBe(50);
    });

    it('forward fills string columns', () => {
      const filled = makeGappy().fill('hold');
      expect(filled.at(0)?.get('host')).toBe('a');
      expect(filled.at(1)?.get('host')).toBe('a');
      expect(filled.at(2)?.get('host')).toBe('a');
      expect(filled.at(3)?.get('host')).toBe('b');
    });

    it('leaves leading undefined unfilled', () => {
      const ts = new TimeSeries({
        name: 'leading',
        schema,
        rows: [
          [1000, undefined, undefined],
          [2000, undefined, undefined],
          [3000, 30, 'a'],
        ],
      });
      const filled = ts.fill('hold');
      expect(filled.at(0)?.get('value')).toBeUndefined();
      expect(filled.at(1)?.get('value')).toBeUndefined();
      expect(filled.at(2)?.get('value')).toBe(30);
    });

    it('respects limit', () => {
      const filled = makeGappy().fill('hold', { limit: 1 });
      expect(filled.at(1)?.get('value')).toBe(10);
      expect(filled.at(2)?.get('value')).toBeUndefined();
    });
  });

  describe('zero strategy', () => {
    it('fills undefined with 0', () => {
      const filled = makeGappy().fill('zero');
      expect(filled.at(1)?.get('value')).toBe(0);
      expect(filled.at(2)?.get('value')).toBe(0);
    });

    it('fills leading undefined', () => {
      const ts = new TimeSeries({
        name: 'leading',
        schema,
        rows: [
          [1000, undefined, undefined],
          [2000, 20, 'a'],
        ],
      });
      const filled = ts.fill('zero');
      expect(filled.at(0)?.get('value')).toBe(0);
      expect(filled.at(0)?.get('host')).toBe(0);
    });

    it('respects limit', () => {
      const filled = makeGappy().fill('zero', { limit: 1 });
      expect(filled.at(1)?.get('value')).toBe(0);
      expect(filled.at(2)?.get('value')).toBeUndefined();
    });
  });

  describe('linear strategy', () => {
    it('interpolates between known values', () => {
      const filled = makeGappy().fill('linear');
      expect(filled.at(0)?.get('value')).toBe(10);
      expect(filled.at(1)?.get('value')).toBe(20);
      expect(filled.at(2)?.get('value')).toBe(30);
      expect(filled.at(3)?.get('value')).toBe(40);
    });

    it('leaves leading undefined unfilled', () => {
      const ts = new TimeSeries({
        name: 'leading',
        schema,
        rows: [
          [1000, undefined, undefined],
          [2000, undefined, undefined],
          [3000, 30, 'a'],
          [4000, 40, 'b'],
        ],
      });
      const filled = ts.fill('linear');
      expect(filled.at(0)?.get('value')).toBeUndefined();
      expect(filled.at(1)?.get('value')).toBeUndefined();
    });

    it('leaves trailing undefined unfilled', () => {
      const ts = new TimeSeries({
        name: 'trailing',
        schema,
        rows: [
          [1000, 10, 'a'],
          [2000, 20, 'b'],
          [3000, undefined, undefined],
          [4000, undefined, undefined],
        ],
      });
      const filled = ts.fill('linear');
      expect(filled.at(2)?.get('value')).toBeUndefined();
      expect(filled.at(3)?.get('value')).toBeUndefined();
    });

    it('handles non-uniform time spacing', () => {
      const ts = new TimeSeries({
        name: 'nonuniform',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'value', kind: 'number', required: false },
        ] as const,
        rows: [
          [0, 0],
          [1000, undefined],
          [3000, undefined],
          [4000, 40],
        ],
      });
      const filled = ts.fill('linear');
      expect(filled.at(1)?.get('value')).toBe(10);
      expect(filled.at(2)?.get('value')).toBe(30);
    });

    it('respects limit', () => {
      const ts = new TimeSeries({
        name: 'long-gap',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'value', kind: 'number', required: false },
        ] as const,
        rows: [
          [0, 0],
          [1000, undefined],
          [2000, undefined],
          [3000, undefined],
          [4000, 40],
        ],
      });
      const filled = ts.fill('linear', { limit: 1 });
      expect(filled.at(1)?.get('value')).toBe(10);
      expect(filled.at(2)?.get('value')).toBeUndefined();
      expect(filled.at(3)?.get('value')).toBeUndefined();
    });

    it('handles same-time events', () => {
      const ts = new TimeSeries({
        name: 'same-time',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'value', kind: 'number', required: false },
        ] as const,
        rows: [
          [1000, 10],
          [1000, undefined],
          [1000, 30],
        ],
      });
      const filled = ts.fill('linear');
      expect(filled.at(1)?.get('value')).toBe(10);
    });
  });

  describe('per-column strategies', () => {
    it('applies different strategies per column', () => {
      const filled = makeGappy().fill({ value: 'linear', host: 'hold' });
      expect(filled.at(1)?.get('value')).toBe(20);
      expect(filled.at(1)?.get('host')).toBe('a');
    });

    it('leaves unmentioned columns as-is', () => {
      const filled = makeGappy().fill({ value: 'hold' });
      expect(filled.at(1)?.get('value')).toBe(10);
      expect(filled.at(1)?.get('host')).toBeUndefined();
    });

    it('supports literal fill values', () => {
      const filled = makeGappy().fill({ value: -1, host: 'unknown' });
      expect(filled.at(1)?.get('value')).toBe(-1);
      expect(filled.at(1)?.get('host')).toBe('unknown');
      expect(filled.at(2)?.get('value')).toBe(-1);
    });

    it('literal with limit', () => {
      const filled = makeGappy().fill({ value: -1 }, { limit: 1 });
      expect(filled.at(1)?.get('value')).toBe(-1);
      expect(filled.at(2)?.get('value')).toBeUndefined();
    });
  });

  describe('edge cases', () => {
    it('empty series returns itself', () => {
      const empty = new TimeSeries({ name: 'e', schema, rows: [] });
      const filled = empty.fill('hold');
      expect(filled.length).toBe(0);
    });

    it('no undefined values returns equivalent series', () => {
      const ts = new TimeSeries({
        name: 'full',
        schema,
        rows: [
          [1000, 10, 'a'],
          [2000, 20, 'b'],
        ],
      });
      const filled = ts.fill('hold');
      expect(filled.at(0)?.get('value')).toBe(10);
      expect(filled.at(1)?.get('value')).toBe(20);
    });

    it('single event with undefined stays undefined for hold', () => {
      const ts = new TimeSeries({
        name: 's',
        schema,
        rows: [[1000, undefined, undefined]],
      });
      const filled = ts.fill('hold');
      expect(filled.at(0)?.get('value')).toBeUndefined();
    });

    it('single event with undefined fills with zero', () => {
      const ts = new TimeSeries({
        name: 's',
        schema,
        rows: [[1000, undefined, undefined]],
      });
      const filled = ts.fill('zero');
      expect(filled.at(0)?.get('value')).toBe(0);
    });

    it('all undefined with hold stays undefined', () => {
      const ts = new TimeSeries({
        name: 'all-undef',
        schema,
        rows: [
          [1000, undefined, undefined],
          [2000, undefined, undefined],
          [3000, undefined, undefined],
        ],
      });
      const filled = ts.fill('hold');
      expect(filled.at(0)?.get('value')).toBeUndefined();
      expect(filled.at(1)?.get('value')).toBeUndefined();
      expect(filled.at(2)?.get('value')).toBeUndefined();
    });

    it('all undefined with linear stays undefined', () => {
      const ts = new TimeSeries({
        name: 'all-undef',
        schema,
        rows: [
          [1000, undefined, undefined],
          [2000, undefined, undefined],
        ],
      });
      const filled = ts.fill('linear');
      expect(filled.at(0)?.get('value')).toBeUndefined();
      expect(filled.at(1)?.get('value')).toBeUndefined();
    });

    it('preserves event keys', () => {
      const filled = makeGappy().fill('hold');
      expect(filled.at(0)?.begin()).toBe(1000);
      expect(filled.at(1)?.begin()).toBe(2000);
      expect(filled.at(4)?.begin()).toBe(5000);
    });

    it('composes with diff', () => {
      const ts = new TimeSeries({
        name: 'diff-fill',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'value', kind: 'number' },
        ] as const,
        rows: [
          [1000, 10],
          [2000, 30],
          [3000, 60],
        ],
      });
      const diffed = ts.diff('value');
      expect(diffed.at(0)?.get('value')).toBeUndefined();
      const filled = diffed.fill('zero');
      expect(filled.at(0)?.get('value')).toBe(0);
      expect(filled.at(1)?.get('value')).toBe(20);
      expect(filled.at(2)?.get('value')).toBe(30);
    });

    it('composes with groupBy', () => {
      const ts = new TimeSeries({
        name: 'grouped',
        schema,
        rows: [
          [1000, 10, 'a'],
          [1000, 100, 'b'],
          [2000, undefined, 'a'],
          [2000, undefined, 'b'],
          [3000, 30, 'a'],
          [3000, undefined, 'b'],
        ],
      });
      const groups = ts.groupBy('host', (group) => group.fill('hold'));
      expect(groups.get('a')!.at(1)?.get('value')).toBe(10);
      expect(groups.get('b')!.at(1)?.get('value')).toBe(100);
      expect(groups.get('b')!.at(2)?.get('value')).toBe(100);
    });

    it('multiple gaps with linear', () => {
      const ts = new TimeSeries({
        name: 'multi-gap',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'value', kind: 'number', required: false },
        ] as const,
        rows: [
          [0, 0],
          [1000, undefined],
          [2000, 20],
          [3000, undefined],
          [4000, 40],
        ],
      });
      const filled = ts.fill('linear');
      expect(filled.at(1)?.get('value')).toBe(10);
      expect(filled.at(3)?.get('value')).toBe(30);
    });
  });
});
