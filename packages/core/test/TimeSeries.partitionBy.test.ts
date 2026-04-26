import { describe, expect, it } from 'vitest';
import { TimeSeries, PartitionedTimeSeries, Sequence } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number', required: false },
  { name: 'host', kind: 'string' },
] as const;

const compositeSchema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number', required: false },
  { name: 'host', kind: 'string' },
  { name: 'region', kind: 'string' },
] as const;

function makeSeries(
  rows: ReadonlyArray<readonly [number, number | undefined, string]>,
) {
  return new TimeSeries({
    name: 'metrics',
    schema,
    rows: rows.map((r) => [...r] as [number, number | undefined, string]),
  });
}

describe('TimeSeries.partitionBy', () => {
  describe('construction', () => {
    it('returns a PartitionedTimeSeries', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [60_000, 0.6, 'b'],
      ]);
      const partitioned = ts.partitionBy('host');
      expect(partitioned).toBeInstanceOf(PartitionedTimeSeries);
    });

    it('accepts a single column name', () => {
      const ts = makeSeries([[0, 0.5, 'a']]);
      const p = ts.partitionBy('host');
      expect(p.by).toEqual(['host']);
    });

    it('accepts an array of column names for composite partitioning', () => {
      const ts = new TimeSeries({
        name: 'metrics',
        schema: compositeSchema,
        rows: [[0, 0.5, 'a', 'eu']],
      });
      const p = ts.partitionBy(['host', 'region']);
      expect(p.by).toEqual(['host', 'region']);
    });

    it('throws on empty partition column list', () => {
      const ts = makeSeries([[0, 0.5, 'a']]);
      expect(() => ts.partitionBy([])).toThrow(
        /requires at least one partition column/,
      );
    });

    it('throws on column not in schema', () => {
      const ts = makeSeries([[0, 0.5, 'a']]);
      // @ts-expect-error invalid column at type level
      expect(() => ts.partitionBy('not_a_column')).toThrow(/not in schema/);
    });
  });

  describe('apply (escape hatch)', () => {
    it('runs the transform per partition and reassembles', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [0, 0.7, 'b'],
        [60_000, 0.6, 'a'],
        [60_000, 0.8, 'b'],
        [120_000, 0.7, 'a'],
        [120_000, 0.9, 'b'],
      ]);

      // Compute per-host max via apply
      const out = ts.partitionBy('host').apply((g) => g.tail('999d'));
      expect(out.length).toBe(6);
      // Time-sorted output
      expect(out.toPoints().map((p) => p.ts)).toEqual([
        0, 0, 60_000, 60_000, 120_000, 120_000,
      ]);
    });

    it('isolates per-partition state — no cross-partition leakage', () => {
      // Cross-host fill leakage scenario from the CSV-cleaner agent runs
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [60_000, undefined, 'a'],
        [60_000, 0.9, 'b'],
        [120_000, 0.7, 'a'],
        [120_000, 1.0, 'b'],
      ]);

      // Without partitioning, fill('linear') would interpolate api-a@60k
      // using b@60k=0.9 as a "neighbor" — wrong.
      const partitioned = ts
        .partitionBy('host')
        .apply((g) => g.fill({ cpu: 'linear' }));

      // a@60k should be 0.6 (linear between 0.5 and 0.7), not anything
      // from host b.
      const aAt60k = partitioned
        .toPoints()
        .find((p) => p.ts === 60_000 && p.host === 'a');
      expect(aAt60k?.cpu).toBeCloseTo(0.6, 5);
    });

    it('returns empty series when source is empty', () => {
      const ts = new TimeSeries({ name: 'empty', schema, rows: [] });
      const out = ts.partitionBy('host').apply((g) => g);
      expect(out.length).toBe(0);
      expect(out.schema).toEqual(schema);
    });

    it('empty source still calls fn so output schema is determined by fn', () => {
      const ts = new TimeSeries({ name: 'empty', schema, rows: [] });
      let called = false;
      ts.partitionBy('host').apply((g) => {
        called = true;
        return g;
      });
      expect(called).toBe(true);
    });

    it('handles composite partition keys', () => {
      const ts = new TimeSeries({
        name: 'metrics',
        schema: compositeSchema,
        rows: [
          [0, 0.5, 'a', 'eu'],
          [60_000, undefined, 'a', 'eu'],
          [60_000, 0.9, 'b', 'eu'],
          [60_000, 0.3, 'a', 'us'], // same host, different region
          [120_000, 0.7, 'a', 'eu'],
        ],
      });

      const partitioned = ts
        .partitionBy(['host', 'region'])
        .apply((g) => g.fill({ cpu: 'linear' }));

      // a@eu @60k should be filled from a@eu's neighbors only — not
      // from the same-time but different-region a@us, and not from b.
      const aEuAt60k = partitioned
        .toPoints()
        .find((p) => p.ts === 60_000 && p.host === 'a' && p.region === 'eu');
      expect(aEuAt60k?.cpu).toBeCloseTo(0.6, 5);
    });
  });

  describe('sugar: fill', () => {
    it('runs fill per partition', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [60_000, undefined, 'a'],
        [60_000, 0.9, 'b'],
        [120_000, 0.7, 'a'],
        [120_000, 1.0, 'b'],
      ]);

      const out = ts.partitionBy('host').fill({ cpu: 'linear' });
      const aAt60k = out
        .toPoints()
        .find((p) => p.ts === 60_000 && p.host === 'a');
      expect(aAt60k?.cpu).toBeCloseTo(0.6, 5);
    });

    it('preserves schema', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [60_000, 0.6, 'a'],
      ]);
      const out = ts.partitionBy('host').fill('hold');
      expect(out.schema).toEqual(schema);
    });
  });

  describe('sugar: rolling', () => {
    it('runs rolling per partition without cross-host averaging', () => {
      const ts = makeSeries([
        [0, 1.0, 'a'],
        [0, 10.0, 'b'],
        [60_000, 2.0, 'a'],
        [60_000, 20.0, 'b'],
        [120_000, 3.0, 'a'],
        [120_000, 30.0, 'b'],
      ]);

      // Include `host: 'last'` in the mapping so the output keeps host
      // for assertion purposes — rolling drops columns not in the mapping.
      const out = ts
        .partitionBy('host')
        .rolling('5m', { cpu: 'avg', host: 'last' }, { alignment: 'trailing' });

      // For host 'a' at 120k, rolling avg over [0, 60k, 120k] = 2.0
      // (NOT mixed with b's 10/20/30 which would be 11.0)
      const aAt120k = out
        .toPoints()
        .find((p) => p.ts === 120_000 && p.host === 'a');
      expect(aAt120k?.cpu).toBeCloseTo(2.0, 5);

      const bAt120k = out
        .toPoints()
        .find((p) => p.ts === 120_000 && p.host === 'b');
      expect(bAt120k?.cpu).toBeCloseTo(20.0, 5);
    });
  });

  describe('sugar: diff', () => {
    it('runs diff per partition', () => {
      const ts = makeSeries([
        [0, 1.0, 'a'],
        [60_000, 100.0, 'b'], // huge value but it's a different host
        [120_000, 2.0, 'a'],
      ]);

      const out = ts.partitionBy('host').diff('cpu');
      // For host 'a' at 120k, diff = 2.0 - 1.0 = 1.0
      // (NOT 2.0 - 100.0 = -98.0 from cross-host)
      const aAt120k = out
        .toPoints()
        .find((p) => p.ts === 120_000 && p.host === 'a');
      expect(aAt120k?.cpu).toBeCloseTo(1.0, 5);
    });
  });

  describe('sugar: cumulative', () => {
    it('runs cumulative per partition', () => {
      const ts = makeSeries([
        [0, 1.0, 'a'],
        [60_000, 100.0, 'b'],
        [120_000, 2.0, 'a'],
        [180_000, 200.0, 'b'],
      ]);

      const out = ts.partitionBy('host').cumulative({ cpu: 'sum' });

      const aPoints = out.toPoints().filter((p) => p.host === 'a');
      const bPoints = out.toPoints().filter((p) => p.host === 'b');

      // a: cumsum of [1, 2] = [1, 3]
      expect(aPoints.map((p) => p.cpu)).toEqual([1, 3]);
      // b: cumsum of [100, 200] = [100, 300]
      expect(bPoints.map((p) => p.cpu)).toEqual([100, 300]);
    });
  });

  describe('sugar: aggregate', () => {
    it('runs aggregate per partition', () => {
      // TimeSeries requires sorted input, so interleave the hosts.
      const ts = makeSeries([
        [0, 1.0, 'a'],
        [0, 100.0, 'b'],
        [30_000, 2.0, 'a'],
        [30_000, 200.0, 'b'],
      ]);

      // Keep host in the mapping so aggregated output keeps it for
      // per-host assertion.
      const out = ts
        .partitionBy('host')
        .aggregate(Sequence.every('1m'), { cpu: 'avg', host: 'last' });

      // Each host's bucket [0, 60k) averages its own events
      const aPoints = out.toPoints().filter((p) => p.host === 'a');
      const bPoints = out.toPoints().filter((p) => p.host === 'b');
      expect(aPoints.map((p) => p.cpu)).toEqual([1.5]);
      expect(bPoints.map((p) => p.cpu)).toEqual([150]);
    });
  });

  describe('integration: closes the CSV-cleaner agent loop', () => {
    it('replaces the manual groupBy + fill + concat workaround', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [0, 1.5, 'b'],
        [60_000, undefined, 'a'],
        [60_000, undefined, 'b'],
        [120_000, 0.7, 'a'],
        [120_000, 1.7, 'b'],
      ]);

      // Old (manual) workaround:
      const manual = TimeSeries.concat([
        ...ts.groupBy('host', (g) => g.fill({ cpu: 'linear' })).values(),
      ]);

      // New (sugar):
      const sugared = ts.partitionBy('host').fill({ cpu: 'linear' });

      // Same result
      expect(sugared.toPoints()).toEqual(manual.toPoints());
    });
  });
});
