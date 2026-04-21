import { describe, expect, it } from 'vitest';
import { Sequence, TimeRange, TimeSeries } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

function makeSeries() {
  return new TimeSeries({
    name: 'metrics',
    schema,
    rows: [
      [0, 10, 'a'],
      [5, 20, 'b'],
      [10, 30, 'a'],
      [15, 40, 'b'],
      [20, 50, 'a'],
    ],
  });
}

describe('TimeSeries.groupBy', () => {
  describe('without transform', () => {
    it('partitions into groups keyed by column value', () => {
      const groups = makeSeries().groupBy('host');
      expect(groups.size).toBe(2);
      expect(groups.has('a')).toBe(true);
      expect(groups.has('b')).toBe(true);
    });

    it('preserves event count across groups', () => {
      const groups = makeSeries().groupBy('host');
      expect(groups.get('a')!.length).toBe(3);
      expect(groups.get('b')!.length).toBe(2);
    });

    it('preserves event order within groups', () => {
      const groups = makeSeries().groupBy('host');
      const a = groups.get('a')!;
      expect(a.at(0)?.begin()).toBe(0);
      expect(a.at(1)?.begin()).toBe(10);
      expect(a.at(2)?.begin()).toBe(20);
    });

    it('preserves schema in each group', () => {
      const groups = makeSeries().groupBy('host');
      expect(groups.get('a')!.schema).toEqual(makeSeries().schema);
    });

    it('preserves all payload data in groups', () => {
      const groups = makeSeries().groupBy('host');
      const a = groups.get('a')!;
      expect(a.at(0)?.get('value')).toBe(10);
      expect(a.at(0)?.get('host')).toBe('a');
      expect(a.at(1)?.get('value')).toBe(30);
    });

    it('single-group series returns one entry', () => {
      const ts = new TimeSeries({
        name: 'uniform',
        schema,
        rows: [
          [0, 10, 'x'],
          [5, 20, 'x'],
        ],
      });
      const groups = ts.groupBy('host');
      expect(groups.size).toBe(1);
      expect(groups.get('x')!.length).toBe(2);
    });

    it('empty series returns empty map', () => {
      const ts = new TimeSeries({ name: 'empty', schema, rows: [] });
      const groups = ts.groupBy('host');
      expect(groups.size).toBe(0);
    });

    it('groups by numeric column', () => {
      const ts = new TimeSeries({
        name: 'buckets',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'value', kind: 'number' },
          { name: 'tier', kind: 'number' },
        ] as const,
        rows: [
          [0, 10, 1],
          [5, 20, 2],
          [10, 30, 1],
        ],
      });
      const groups = ts.groupBy('tier');
      expect(groups.size).toBe(2);
      expect(groups.get('1')!.length).toBe(2);
      expect(groups.get('2')!.length).toBe(1);
    });
  });

  describe('with transform', () => {
    it('applies transform to each group', () => {
      const groups = makeSeries().groupBy('host', (group) =>
        group.reduce('value', 'avg'),
      );
      expect(groups.get('a')).toBe(30);
      expect(groups.get('b')).toBe(30);
    });

    it('transform receives the group key', () => {
      const keys: string[] = [];
      makeSeries().groupBy('host', (_group, key) => {
        keys.push(key);
      });
      expect(keys.sort()).toEqual(['a', 'b']);
    });

    it('works with rolling transform', () => {
      const groups = makeSeries().groupBy('host', (group) =>
        group.rolling(20, { value: 'avg' }),
      );
      const a = groups.get('a')!;
      expect(a.length).toBe(3);
      expect(a.at(0)?.get('value')).toBe(10);
      expect(a.at(1)?.get('value')).toBe(20);
      expect(a.at(2)?.get('value')).toBe(40);
    });

    it('works with multi-column reduce', () => {
      const groups = makeSeries().groupBy('host', (group) =>
        group.reduce({ value: 'sum', host: 'count' }),
      );
      expect(groups.get('a')).toEqual({ value: 90, host: 3 });
      expect(groups.get('b')).toEqual({ value: 60, host: 2 });
    });

    it('works with aggregate transform', () => {
      const groups = makeSeries().groupBy('host', (group) =>
        group.aggregate(
          Sequence.every(10),
          { value: 'sum' },
          {
            range: new TimeRange({ start: 0, end: 19 }),
          },
        ),
      );
      const a = groups.get('a')!;
      expect(a.length).toBe(2);
      expect(a.at(0)?.get('value')).toBe(10);
      expect(a.at(1)?.get('value')).toBe(30);
    });

    it('works with filter transform', () => {
      const groups = makeSeries().groupBy('host', (group) =>
        group.filter((e) => (e.get('value') as number) > 20),
      );
      expect(groups.get('a')!.length).toBe(2);
      expect(groups.get('b')!.length).toBe(1);
    });

    it('empty series with transform returns empty map', () => {
      const ts = new TimeSeries({ name: 'empty', schema, rows: [] });
      const groups = ts.groupBy('host', (group) =>
        group.reduce('value', 'avg'),
      );
      expect(groups.size).toBe(0);
    });
  });
});
