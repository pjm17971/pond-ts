import { describe, expect, it } from 'vitest';
import { Sequence, TimeSeries } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number', required: false },
  { name: 'host', kind: 'string', required: false },
] as const;

const compositeSchema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number', required: false },
  { name: 'host', kind: 'string' },
  { name: 'region', kind: 'string' },
] as const;

function makeSeries(
  rows: ReadonlyArray<
    readonly [number, number | undefined, string | undefined]
  >,
) {
  return new TimeSeries({
    name: 'metrics',
    schema,
    rows: rows.map(
      (r) => [...r] as [number, number | undefined, string | undefined],
    ),
  });
}

describe('TimeSeries.partitionBy({ groups })', () => {
  describe('toMap with declared groups', () => {
    it('iterates in declared order, not insertion order', () => {
      // Insertion order: 'b', 'a'. Declared order: 'a', 'b'.
      // Map should reflect declared order.
      const ts = makeSeries([
        [0, 0.5, 'b'],
        [0, 0.3, 'a'],
        [60_000, 0.6, 'b'],
        [60_000, 0.4, 'a'],
      ]);
      const HOSTS = ['a', 'b'] as const;
      const m = ts.partitionBy('host', { groups: HOSTS }).toMap();
      expect([...m.keys()]).toEqual(['a', 'b']);
    });

    it('emits empty TimeSeries entries for declared groups with no events', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [60_000, 0.6, 'a'],
      ]);
      const HOSTS = ['a', 'b', 'c'] as const;
      const m = ts.partitionBy('host', { groups: HOSTS }).toMap();
      expect(m.size).toBe(3);
      expect(m.get('a')?.length).toBe(2);
      expect(m.get('b')?.length).toBe(0);
      expect(m.get('c')?.length).toBe(0);
    });

    it('throws on construction when an event has a value not in groups', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [0, 0.3, 'rogue'], // not in declared groups
        [60_000, 0.6, 'a'],
      ]);
      expect(() =>
        ts.partitionBy('host', { groups: ['a', 'b'] as const }),
      ).toThrow(/not in declared groups/);
    });

    it('throws on construction with undefined value not in groups', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [0, 0.3, undefined],
        [60_000, 0.6, 'a'],
      ]);
      expect(() =>
        ts.partitionBy('host', { groups: ['a', 'b'] as const }),
      ).toThrow(/not in declared groups/);
    });

    it('accepts the leading-space sentinel " undefined" in groups', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [0, 0.3, undefined],
        [60_000, 0.6, 'a'],
      ]);
      const m = ts
        .partitionBy('host', { groups: ['a', ' undefined'] as const })
        .toMap();
      expect(m.size).toBe(2);
      expect(m.get('a')?.length).toBe(2);
      expect(m.get(' undefined')?.length).toBe(1);
    });

    it('declared groups with empty source still produce empty entries', () => {
      const ts = new TimeSeries({ name: 'm', schema, rows: [] });
      const m = ts.partitionBy('host', { groups: ['a', 'b'] as const }).toMap();
      expect(m.size).toBe(2);
      expect(m.get('a')?.length).toBe(0);
      expect(m.get('b')?.length).toBe(0);
    });
  });

  describe('chains preserve groups', () => {
    it('groups survive through a sugar method (fill)', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [0, 0.3, 'b'],
        [60_000, undefined, 'a'],
        [60_000, undefined, 'b'],
        [120_000, 0.7, 'a'],
        [120_000, 0.5, 'b'],
      ]);
      const HOSTS = ['a', 'b'] as const;
      const m = ts
        .partitionBy('host', { groups: HOSTS })
        .fill({ cpu: 'linear' })
        .toMap();
      expect([...m.keys()]).toEqual(['a', 'b']); // declared order preserved
      expect(m.get('a')?.length).toBe(3);
      expect(m.get('b')?.length).toBe(3);
      // Linear interp filled the missing cpu values
      expect(m.get('a')?.at(1)?.get('cpu')).toBeCloseTo(0.6, 5);
      expect(m.get('b')?.at(1)?.get('cpu')).toBeCloseTo(0.4, 5);
    });

    it('groups survive multi-step chain (dedupe + fill + rolling)', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [0, 0.55, 'a'], // duplicate
        [0, 0.3, 'b'],
        [60_000, 0.6, 'a'],
        [60_000, 0.4, 'b'],
      ]);
      const HOSTS = ['a', 'b', 'c'] as const;
      const m = ts
        .partitionBy('host', { groups: HOSTS })
        .dedupe({ keep: 'last' })
        .fill({ cpu: 'linear' })
        .toMap();
      // 'c' has no events but is still in the Map per declared groups
      expect([...m.keys()]).toEqual(['a', 'b', 'c']);
      expect(m.get('a')?.length).toBe(2);
      expect(m.get('b')?.length).toBe(2);
      expect(m.get('c')?.length).toBe(0);
      // dedupe-last picked 0.55 for a@0
      expect(m.get('a')?.at(0)?.get('cpu')).toBe(0.55);
    });

    it('apply() also runs only over partitions in the source (declared empty groups skipped)', () => {
      // apply concats per-partition results — declared-but-empty groups
      // contribute zero events to the output. Behavior identical to
      // groups-not-passed for apply.
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [60_000, 0.6, 'a'],
      ]);
      const HOSTS = ['a', 'b'] as const;
      const out = ts.partitionBy('host', { groups: HOSTS }).apply((g) => g);
      expect(out.length).toBe(2); // only 'a' had events
    });
  });

  describe('composite partitions reject groups', () => {
    it('throws TypeError when groups is supplied with array partition columns', () => {
      const ts = new TimeSeries({
        name: 'm',
        schema: compositeSchema,
        rows: [[0, 0.5, 'a', 'eu']],
      });
      expect(() =>
        // @ts-expect-error groups not allowed on array `by` (overload only matches single-column)
        ts.partitionBy(['host', 'region'], { groups: ['a'] as const }),
      ).toThrow(/single partition column/);
    });
  });

  describe('runtime returns the same object shape regardless of typing', () => {
    it('PartitionedTimeSeries instance with .by, .groups, and sugar methods', () => {
      const ts = makeSeries([[0, 0.5, 'a']]);
      const p = ts.partitionBy('host', { groups: ['a', 'b'] as const });
      expect(p.by).toEqual(['host']);
      expect(p.groups).toEqual(['a', 'b']);
      expect(typeof p.fill).toBe('function');
      expect(typeof p.toMap).toBe('function');
      expect(typeof p.collect).toBe('function');
    });
  });

  describe('toMap with transform under typed groups', () => {
    it('transform runs per declared group; empty groups receive an empty TimeSeries', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [60_000, 0.6, 'a'],
      ]);
      const HOSTS = ['a', 'b'] as const;
      const m = ts
        .partitionBy('host', { groups: HOSTS })
        .toMap((g) => g.length);
      expect(m.get('a')).toBe(2);
      expect(m.get('b')).toBe(0);
    });
  });

  describe('full v0.10 chain — partitionBy(groups) + materialize + fill', () => {
    it('composes with materialize and yields one entry per declared group in order', () => {
      // 'c' has no events — declared groups still emit an empty entry.
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [60_000, 0.5, 'b'],
        [120_000, 0.7, 'a'],
      ]);
      const HOSTS = ['a', 'b', 'c'] as const;
      const m = ts
        .partitionBy('host', { groups: HOSTS })
        .materialize(Sequence.every('1m'), {
          range: { start: 0, end: 120_000 },
        })
        .toMap();
      expect([...m.keys()]).toEqual(['a', 'b', 'c']);
      // 'a' has events at 0 and 120k → 3 grid rows (0, 60k, 120k); 60k empty
      expect(m.get('a')?.length).toBe(3);
      // 'b' has one event at 60k → 3 grid rows; 0 and 120k empty
      expect(m.get('b')?.length).toBe(3);
      // 'c' empty group → empty TimeSeries
      expect(m.get('c')?.length).toBe(0);
    });
  });
});
