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
      // Cross-host fill leakage scenario from the CSV-cleaner agent runs.
      // Compare partitioned vs un-partitioned to prove the partitioning
      // changes the result (not just that the partitioned value is 0.6
      // by coincidence).
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [60_000, undefined, 'a'],
        [60_000, 0.9, 'b'],
        [120_000, 0.7, 'a'],
        [120_000, 1.0, 'b'],
      ]);

      // Without partitioning: linear-fill walks the events array
      // chronologically. `a`@60k's missing cpu sits between `a`@0=0.5
      // and `b`@60k=0.9 — interp gives ~0.7 (1/3 of the way through
      // the timestamp gap). Different from the per-host correct value.
      const unpartitioned = ts.fill({ cpu: 'linear' });
      const aAt60kUnpartitioned = unpartitioned
        .toPoints()
        .find((p) => p.ts === 60_000 && p.host === 'a');

      // With partitioning: linear fill within host 'a' uses 0.5 and 0.7
      // as neighbors — interp gives 0.6.
      const partitioned = ts
        .partitionBy('host')
        .apply((g) => g.fill({ cpu: 'linear' }));
      const aAt60kPartitioned = partitioned
        .toPoints()
        .find((p) => p.ts === 60_000 && p.host === 'a');

      // The partitioned value is the per-host correct one.
      expect(aAt60kPartitioned?.cpu).toBeCloseTo(0.6, 5);
      // And the partitioned result differs from the unpartitioned one —
      // proving partitioning actually changed something.
      expect(aAt60kPartitioned?.cpu).not.toBeCloseTo(
        aAt60kUnpartitioned?.cpu ?? NaN,
        2,
      );
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

    it('composite-key encoding does not collide on values containing separators', () => {
      // Naive `parts.join(' ')` would collapse these to the same key:
      //   ['a b', 'c'] and ['a', 'b c'] both → 'a b c'
      // Real risk for region names like 'us east'. JSON-encoding fixes it.
      const ts = new TimeSeries({
        name: 'metrics',
        schema: compositeSchema,
        rows: [
          [0, 0.5, 'a b', 'c'], // composite key A
          [0, 0.9, 'a', 'b c'], // composite key B — must NOT collide with A
          [60_000, 0.6, 'a b', 'c'],
          [60_000, 1.0, 'a', 'b c'],
        ],
      });

      // Build per-partition counts to verify the partitions stayed
      // separate.
      let partitionCount = 0;
      ts.partitionBy(['host', 'region']).apply((g) => {
        partitionCount += 1;
        return g;
      });
      expect(partitionCount).toBe(2);
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

      const out = ts.partitionBy('host').fill({ cpu: 'linear' }).collect();
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
      const out = ts.partitionBy('host').fill('hold').collect();
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
        .rolling('5m', { cpu: 'avg', host: 'last' }, { alignment: 'trailing' })
        .collect();

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

    it('threads minSamples through to per-partition rolling', () => {
      const ts = makeSeries([
        [0, 1.0, 'a'],
        [0, 10.0, 'b'],
        [60_000, 2.0, 'a'],
        [60_000, 20.0, 'b'],
        [120_000, 3.0, 'a'],
        [120_000, 30.0, 'b'],
      ]);

      const out = ts
        .partitionBy('host')
        .rolling('10m', { cpu: 'avg', host: 'last' }, { minSamples: 3 })
        .collect();

      const points = out.toPoints();
      // Per-partition counts: each host hits 3 samples only at t=120000.
      // Earlier rows for either host should have undefined cpu.
      for (const p of points) {
        if (p.ts === 120_000) {
          expect(typeof p.cpu).toBe('number');
        } else {
          expect(p.cpu).toBeUndefined();
        }
      }
    });
  });

  describe('sugar: diff', () => {
    it('runs diff per partition', () => {
      const ts = makeSeries([
        [0, 1.0, 'a'],
        [60_000, 100.0, 'b'], // huge value but it's a different host
        [120_000, 2.0, 'a'],
      ]);

      const out = ts.partitionBy('host').diff('cpu').collect();
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

      const out = ts.partitionBy('host').cumulative({ cpu: 'sum' }).collect();

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
        .aggregate(Sequence.every('1m'), { cpu: 'avg', host: 'last' })
        .collect();

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
      const sugared = ts.partitionBy('host').fill({ cpu: 'linear' }).collect();

      // Same result
      expect(sugared.toPoints()).toEqual(manual.toPoints());
    });
  });

  describe('persistent partition: chained per-partition ops', () => {
    it('chains multiple stateful ops without re-partitioning', () => {
      // The use case the persistent-partition design exists for.
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [0, 1.5, 'b'],
        [60_000, undefined, 'a'],
        [60_000, undefined, 'b'],
        [120_000, 0.7, 'a'],
        [120_000, 1.7, 'b'],
      ]);

      const out = ts
        .partitionBy('host')
        .fill({ cpu: 'linear' })
        .rolling('5m', { cpu: 'avg', host: 'last' }, { alignment: 'trailing' })
        .collect();

      // Per-host fill + rolling. After fill, host 'a' has cpu values
      // [0.5, 0.6, 0.7]. Trailing-5m rolling avg at 120k (which sees
      // all three) is 0.6.
      const aAt120k = out
        .toPoints()
        .find((p) => p.ts === 120_000 && p.host === 'a');
      expect(aAt120k?.cpu).toBeCloseTo(0.6, 5);
    });

    it('collect() returns a TimeSeries with the same schema', () => {
      const ts = makeSeries([[0, 0.5, 'a']]);
      const out = ts.partitionBy('host').collect();
      expect(out).toBeInstanceOf(TimeSeries);
      expect(out.schema).toEqual(schema);
    });

    it('apply() exits the partition view (returns TimeSeries directly)', () => {
      const ts = makeSeries([[0, 0.5, 'a']]);
      const out = ts.partitionBy('host').apply((g) => g);
      expect(out).toBeInstanceOf(TimeSeries);
    });
  });

  describe('fill option pass-through (regression — v0.9.1)', () => {
    // The headline v0.9.0 chain — partitionBy('host').fill('linear', { maxGap }).
    // PR #78 added `maxGap` to `TimeSeries.fill` but the partitioned sugar's
    // option type was not updated, so this exact call failed type checking
    // until v0.9.1.

    it('accepts maxGap option without type or runtime error', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [60_000, undefined, 'a'], // 1m gap
        [120_000, 0.7, 'a'],
      ]);
      const out = ts
        .partitionBy('host')
        .fill('linear', { maxGap: '5m' })
        .collect();
      // Gap span is 2m, fits within 5m cap → linear interp fills it.
      const aAt60k = [...out.events].find((e) => e.begin() === 60_000);
      expect(aAt60k?.get('cpu')).toBeCloseTo(0.6, 5);
    });

    it('all-or-nothing maxGap respects per-partition gap span', () => {
      // host-a has a 2-minute gap (0 → 120k → 240k);
      // host-b has a 10-minute gap (240k → 840k → 1440k).
      // With maxGap '5m', a's gap fills, b's stays unfilled.
      // Rows must be in time order; interleave hosts where times match.
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [120_000, undefined, 'a'],
        [240_000, 0.7, 'a'],
        [240_000, 0.5, 'b'],
        [840_000, undefined, 'b'],
        [1_440_000, 0.7, 'b'],
      ]);
      const out = ts
        .partitionBy('host')
        .fill('linear', { maxGap: '5m' })
        .collect();
      const aMid = [...out.events].find(
        (e) => e.begin() === 120_000 && e.get('host') === 'a',
      );
      const bMid = [...out.events].find(
        (e) => e.begin() === 840_000 && e.get('host') === 'b',
      );
      expect(aMid?.get('cpu')).toBeCloseTo(0.6, 5);
      expect(bMid?.get('cpu')).toBeUndefined();
    });

    it('limit and maxGap compose on the partitioned view', () => {
      // 1-cell gap fits limit:1; 2m span fits maxGap:5m.
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [120_000, undefined, 'a'],
        [240_000, 0.7, 'a'],
      ]);
      const out = ts
        .partitionBy('host')
        .fill('linear', { limit: 1, maxGap: '5m' })
        .collect();
      const mid = [...out.events].find((e) => e.begin() === 120_000);
      expect(mid?.get('cpu')).toBeCloseTo(0.6, 5);
    });

    it('full v0.9.0 chain — partitionBy + dedupe + fill(maxGap) — works end to end', () => {
      const ts = makeSeries([
        [0, 0.5, 'a'],
        [0, 0.55, 'a'], // duplicate at t=0 for host 'a'
        [0, 0.3, 'b'],
        [60_000, undefined, 'a'],
        [60_000, undefined, 'b'],
        [120_000, 0.7, 'a'],
        [120_000, 0.5, 'b'],
      ]);
      const out = ts
        .partitionBy('host')
        .dedupe({ keep: 'last' })
        .fill('linear', { maxGap: '5m' })
        .collect();
      // host a, t=0: dedupe-last → 0.55
      const aAt0 = [...out.events].find(
        (e) => e.begin() === 0 && e.get('host') === 'a',
      );
      expect(aAt0?.get('cpu')).toBe(0.55);
      // host a, t=60k: linear interp between 0.55 and 0.7 → 0.625
      const aMid = [...out.events].find(
        (e) => e.begin() === 60_000 && e.get('host') === 'a',
      );
      expect(aMid?.get('cpu')).toBeCloseTo(0.625, 5);
      // host b, t=60k: linear interp between 0.3 and 0.5 → 0.4
      const bMid = [...out.events].find(
        (e) => e.begin() === 60_000 && e.get('host') === 'b',
      );
      expect(bMid?.get('cpu')).toBeCloseTo(0.4, 5);
    });
  });

  describe('composite partition keys round-trip (v0.9.1)', () => {
    // Verifies that partitionBy(['host', 'region']) preserves both key
    // columns through the per-partition transforms and back into the
    // collected/applied output, including the schema. Flagged as a
    // refinement target by the dashboard agent against v0.9.0.

    function makeComposite(
      rows: ReadonlyArray<
        readonly [number, number | undefined, string, string]
      >,
    ) {
      return new TimeSeries({
        name: 'metrics',
        schema: compositeSchema,
        rows: rows.map(
          (r) => [...r] as [number, number | undefined, string, string],
        ),
      });
    }

    it('preserves both partition columns in the collected output schema', () => {
      const ts = makeComposite([[0, 0.5, 'a', 'eu']]);
      const out = ts.partitionBy(['host', 'region']).collect();
      expect(out.schema).toEqual(compositeSchema);
    });

    it('preserves both partition column values on every output event', () => {
      const ts = makeComposite([
        [0, 0.5, 'a', 'eu'],
        [0, 0.3, 'a', 'us'],
        [0, 0.4, 'b', 'eu'],
        [60_000, undefined, 'a', 'eu'],
        [60_000, undefined, 'a', 'us'],
        [120_000, 0.7, 'a', 'eu'],
        [120_000, 0.5, 'a', 'us'],
        [120_000, 0.6, 'b', 'eu'],
      ]);
      const out = ts
        .partitionBy(['host', 'region'])
        .fill('linear', { maxGap: '5m' })
        .collect();
      // Every output event must carry both keys
      for (const e of out.events) {
        expect(typeof e.get('host')).toBe('string');
        expect(typeof e.get('region')).toBe('string');
      }
    });

    it('keeps (host, region) tuples distinct (does not collapse on host alone)', () => {
      // host 'a' appears in two regions. After per-partition fill, both
      // sub-series must survive — no cross-region mixing.
      const ts = makeComposite([
        [0, 0.5, 'a', 'eu'],
        [0, 0.3, 'a', 'us'],
        [60_000, undefined, 'a', 'eu'],
        [60_000, undefined, 'a', 'us'],
        [120_000, 0.7, 'a', 'eu'],
        [120_000, 0.5, 'a', 'us'],
      ]);
      const out = ts
        .partitionBy(['host', 'region'])
        .fill('linear', { maxGap: '5m' })
        .collect();
      const aEuMid = [...out.events].find(
        (e) =>
          e.begin() === 60_000 &&
          e.get('host') === 'a' &&
          e.get('region') === 'eu',
      );
      const aUsMid = [...out.events].find(
        (e) =>
          e.begin() === 60_000 &&
          e.get('host') === 'a' &&
          e.get('region') === 'us',
      );
      // Each gap fills against its own sub-series only.
      expect(aEuMid?.get('cpu')).toBeCloseTo(0.6, 5); // (0.5 + 0.7) / 2
      expect(aUsMid?.get('cpu')).toBeCloseTo(0.4, 5); // (0.3 + 0.5) / 2
      // If composite keys collapsed on host alone, the four 'a' points
      // would interleave and produce a different (wrong) interpolated value.
    });

    it('apply() preserves both partition columns', () => {
      const ts = makeComposite([
        [0, 0.5, 'a', 'eu'],
        [0, 0.3, 'a', 'us'],
        [60_000, 0.6, 'a', 'eu'],
        [60_000, 0.4, 'a', 'us'],
      ]);
      const out = ts.partitionBy(['host', 'region']).apply((g) => g);
      expect(out.schema).toEqual(compositeSchema);
      const hostsRegions = [...out.events].map((e) => [
        e.get('host'),
        e.get('region'),
      ]);
      expect(hostsRegions).toContainEqual(['a', 'eu']);
      expect(hostsRegions).toContainEqual(['a', 'us']);
    });

    it('composite partitioning + dedupe + fill chain works end to end', () => {
      const ts = makeComposite([
        [0, 0.5, 'a', 'eu'],
        [0, 0.55, 'a', 'eu'], // duplicate within ('a', 'eu')
        [0, 0.3, 'a', 'us'], // same (time, host) as 'eu' duplicate but different region — NOT a duplicate
        [60_000, undefined, 'a', 'eu'],
        [120_000, 0.7, 'a', 'eu'],
      ]);
      const out = ts
        .partitionBy(['host', 'region'])
        .dedupe({ keep: 'last' })
        .fill('linear', { maxGap: '5m' })
        .collect();
      // ('a', 'eu') @ 0: dedupe-last → 0.55 (NOT collapsed against 'us')
      const aEuAt0 = [...out.events].find(
        (e) =>
          e.begin() === 0 && e.get('host') === 'a' && e.get('region') === 'eu',
      );
      expect(aEuAt0?.get('cpu')).toBe(0.55);
      // ('a', 'us') @ 0 stays at 0.3 — different partition
      const aUsAt0 = [...out.events].find(
        (e) =>
          e.begin() === 0 && e.get('host') === 'a' && e.get('region') === 'us',
      );
      expect(aUsAt0?.get('cpu')).toBe(0.3);
    });
  });
});
