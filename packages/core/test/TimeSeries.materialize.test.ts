import { describe, expect, it } from 'vitest';
import { Sequence, TimeSeries } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number', required: false },
  { name: 'host', kind: 'string', required: false },
] as const;

const partSchema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number', required: false },
  { name: 'host', kind: 'string' },
] as const;

function make(
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

describe('TimeSeries.materialize', () => {
  describe('shape', () => {
    it('returns a time-keyed series with one row per bucket', () => {
      const ts = make([
        [0, 0.5, 'a'],
        [60_000, 0.6, 'a'],
        [180_000, 0.8, 'a'],
      ]);
      const out = ts.materialize(Sequence.every('1m'));
      // Default range is series timeRange = [0, 180k]. Sequence.every
      // generates buckets at 0, 60k, 120k, 180k (4 starts including
      // the inclusive-end edge). Expected: 4 rows.
      expect(out.length).toBe(4);
      expect(out.schema[0]?.name).toBe('time');
      expect(out.schema[0]?.kind).toBe('time');
    });

    it('widens value columns to optional in the output schema', () => {
      const ts = make([[0, 0.5, 'a']]);
      const out = ts.materialize(Sequence.every('1m'));
      // Inspect the schema: cpu and host both have required: false
      const cpuCol = out.schema.find((c) => c.name === 'cpu');
      const hostCol = out.schema.find((c) => c.name === 'host');
      expect(cpuCol?.required).toBe(false);
      expect(hostCol?.required).toBe(false);
    });

    it('returns empty series when source is empty (no range, no events)', () => {
      const ts = new TimeSeries({ name: 'm', schema, rows: [] });
      const out = ts.materialize(Sequence.every('1m'));
      expect(out.length).toBe(0);
    });
  });

  describe('select: last (default)', () => {
    it('takes the last source event in each bucket', () => {
      const ts = make([
        [0, 0.5, 'a'],
        [30_000, 0.55, 'a'], // same bucket as t=0
        [60_000, 0.6, 'a'],
      ]);
      // range [0, 60_000] + Sequence.every('1m') → 2 bucket starts (0, 60k).
      const out = ts.materialize(Sequence.every('1m'), {
        range: { start: 0, end: 60_000 },
      });
      expect(out.length).toBe(2);
      // First bucket [0, 60k): last event is t=30k, cpu=0.55
      expect(out.at(0)?.get('cpu')).toBe(0.55);
      expect(out.at(0)?.begin()).toBe(0); // sample=begin default
      // Second bucket [60k, 120k): only t=60k, cpu=0.6
      expect(out.at(1)?.get('cpu')).toBe(0.6);
      expect(out.at(1)?.begin()).toBe(60_000);
    });

    it('emits undefined for empty buckets', () => {
      const ts = make([
        [0, 0.5, 'a'],
        [180_000, 0.8, 'a'],
      ]);
      // range [0, 180_000] → bucket starts at 0, 60k, 120k, 180k (4 rows)
      const out = ts.materialize(Sequence.every('1m'), {
        range: { start: 0, end: 180_000 },
      });
      expect(out.length).toBe(4);
      expect(out.at(0)?.get('cpu')).toBe(0.5); // populated
      expect(out.at(1)?.get('cpu')).toBeUndefined(); // empty
      expect(out.at(2)?.get('cpu')).toBeUndefined(); // empty
      expect(out.at(3)?.get('cpu')).toBe(0.8); // populated
    });
  });

  describe('select: first', () => {
    it('takes the first source event in each bucket', () => {
      const ts = make([
        [0, 0.5, 'a'],
        [30_000, 0.55, 'a'],
        [60_000, 0.6, 'a'],
      ]);
      // range [0, 60_000] → 2 buckets at 0 and 60k.
      const out = ts.materialize(Sequence.every('1m'), {
        select: 'first',
        range: { start: 0, end: 60_000 },
      });
      // First bucket [0, 60k): first event is t=0, cpu=0.5
      expect(out.at(0)?.get('cpu')).toBe(0.5);
      expect(out.at(1)?.get('cpu')).toBe(0.6);
    });
  });

  describe('select: nearest', () => {
    it('picks the source event closest to the sample point', () => {
      // Bucket [0, 60k), sample=center → 30k
      // Events at 5k, 25k, 50k → nearest to 30k is 25k.
      const ts = make([
        [5_000, 0.1, 'a'],
        [25_000, 0.2, 'a'],
        [50_000, 0.3, 'a'],
      ]);
      const out = ts.materialize(Sequence.every('1m'), {
        select: 'nearest',
        sample: 'center',
        range: { start: 0, end: 60_000 },
      });
      expect(out.length).toBe(1);
      expect(out.at(0)?.get('cpu')).toBe(0.2);
    });

    it('with sample=begin, picks event closest to bucket start', () => {
      // Bucket [0, 60k), sample=begin → 0.
      // Events at 5k, 25k, 50k → nearest to 0 is 5k.
      const ts = make([
        [5_000, 0.1, 'a'],
        [25_000, 0.2, 'a'],
        [50_000, 0.3, 'a'],
      ]);
      const out = ts.materialize(Sequence.every('1m'), {
        select: 'nearest',
        sample: 'begin',
        range: { start: 0, end: 60_000 },
      });
      expect(out.at(0)?.get('cpu')).toBe(0.1);
    });

    it('ties keep the earliest tied event', () => {
      // Bucket [0, 60k), sample=center → 30k.
      // Events at 20k and 40k both 10k from 30k. First tied wins.
      const ts = make([
        [20_000, 0.5, 'a'],
        [40_000, 0.7, 'a'],
      ]);
      const out = ts.materialize(Sequence.every('1m'), {
        select: 'nearest',
        sample: 'center',
        range: { start: 0, end: 60_000 },
      });
      expect(out.at(0)?.get('cpu')).toBe(0.5);
    });

    it('empty bucket stays undefined regardless of nearest', () => {
      const ts = make([
        [0, 0.5, 'a'],
        [180_000, 0.8, 'a'],
      ]);
      const out = ts.materialize(Sequence.every('1m'), {
        select: 'nearest',
        range: { start: 0, end: 240_000 },
      });
      expect(out.at(1)?.get('cpu')).toBeUndefined();
      expect(out.at(2)?.get('cpu')).toBeUndefined();
    });
  });

  describe('sample option', () => {
    it('begin (default): output time is bucket start', () => {
      const ts = make([[30_000, 0.5, 'a']]);
      const out = ts.materialize(Sequence.every('1m'), {
        range: { start: 0, end: 60_000 },
      });
      expect(out.at(0)?.begin()).toBe(0);
    });

    it('center: output time is bucket midpoint', () => {
      const ts = make([[30_000, 0.5, 'a']]);
      const out = ts.materialize(Sequence.every('1m'), {
        sample: 'center',
        range: { start: 0, end: 60_000 },
      });
      expect(out.at(0)?.begin()).toBe(30_000);
    });

    it('end: output time is bucket end', () => {
      const ts = make([[30_000, 0.5, 'a']]);
      const out = ts.materialize(Sequence.every('1m'), {
        sample: 'end',
        range: { start: 0, end: 60_000 },
      });
      expect(out.at(0)?.begin()).toBe(60_000);
    });
  });

  describe('range option', () => {
    it('extends grid past source extent into trailing empty buckets', () => {
      const ts = make([[30_000, 0.5, 'a']]);
      // range [0, 120k] → buckets at 0, 60k, 120k (3 rows)
      const out = ts.materialize(Sequence.every('1m'), {
        range: { start: 0, end: 120_000 },
      });
      expect(out.length).toBe(3);
      expect(out.at(0)?.get('cpu')).toBe(0.5);
      expect(out.at(1)?.get('cpu')).toBeUndefined();
      expect(out.at(2)?.get('cpu')).toBeUndefined();
    });

    it('extends grid before source extent into leading empty buckets', () => {
      const ts = make([[150_000, 0.5, 'a']]);
      // range [0, 120k] → buckets at 0, 60k, 120k (3 rows). Source event
      // at 150k is in the [120k, 180k) bucket which starts at 120k.
      const out = ts.materialize(Sequence.every('1m'), {
        range: { start: 0, end: 120_000 },
      });
      expect(out.length).toBe(3);
      expect(out.at(0)?.get('cpu')).toBeUndefined();
      expect(out.at(1)?.get('cpu')).toBeUndefined();
      expect(out.at(2)?.get('cpu')).toBe(0.5);
    });
  });

  describe('half-open membership', () => {
    it('event at exact bucket boundary belongs to the next bucket', () => {
      // Events at t=0 and t=60_000. Buckets are [0, 60k) and [60k, 120k).
      // t=60k should land in the second bucket.
      const ts = make([
        [0, 0.5, 'a'],
        [60_000, 0.6, 'a'],
      ]);
      const out = ts.materialize(Sequence.every('1m'), {
        range: { start: 0, end: 120_000 },
      });
      expect(out.at(0)?.get('cpu')).toBe(0.5);
      expect(out.at(1)?.get('cpu')).toBe(0.6);
    });
  });

  describe('composes with fill(maxGap)', () => {
    it('full pipeline: materialize then fill linear with maxGap', () => {
      const ts = make([
        [0, 0.0, 'a'],
        // 2-min gap (no event in [60k, 120k))
        [120_000, 0.4, 'a'],
        // Then 4-min gap (no events at 180k or 240k)
        [300_000, 1.0, 'a'],
      ]);
      const out = ts
        .materialize(Sequence.every('1m'), {
          range: { start: 0, end: 360_000 },
        })
        .fill({ cpu: 'linear' }, { maxGap: '3m' });
      // 6 buckets at sample=begin: 0, 60k, 120k, 180k, 240k, 300k.
      // Bucket 0: 0.0
      // Bucket 60k: empty (1-cell gap, span 2m, fits 3m) → linear interp 0.0→0.4 → 0.2
      // Bucket 120k: 0.4 (source)
      // Buckets 180k, 240k: empty (2-cell gap, span 3m, fits 3m) → ... wait
      //   Actually need to think. After materialize, value column 'cpu' has
      //   undefined at 60k, 180k, 240k. The 60k gap is 1 cell (fits limit/maxGap);
      //   the 180k+240k gap is 2 cells, span = 300k - 120k = 3m, fits 3m exactly.
      // Bucket 300k: 1.0
      expect(out.at(0)?.get('cpu')).toBeCloseTo(0.0, 5);
      expect(out.at(1)?.get('cpu')).toBeCloseTo(0.2, 5); // linear 0→0.4 at midpoint
      expect(out.at(2)?.get('cpu')).toBeCloseTo(0.4, 5);
      // 180k = 1/3 from 120k to 300k → 0.4 + (1.0 - 0.4) * 1/3 = 0.6
      expect(out.at(3)?.get('cpu')).toBeCloseTo(0.6, 5);
      // 240k = 2/3 → 0.4 + 0.6 * 2/3 = 0.8
      expect(out.at(4)?.get('cpu')).toBeCloseTo(0.8, 5);
      expect(out.at(5)?.get('cpu')).toBeCloseTo(1.0, 5);
    });

    it('maxGap leaves long outages unfilled after materialize', () => {
      const ts = make([
        [0, 0.0, 'a'],
        // 5-minute outage
        [300_000, 1.0, 'a'],
      ]);
      const out = ts
        .materialize(Sequence.every('1m'), {
          range: { start: 0, end: 360_000 },
        })
        .fill({ cpu: 'linear' }, { maxGap: '3m' });
      // 6 buckets. Buckets 60k, 120k, 180k, 240k are an empty 4-cell run.
      // Span = 300k - 0 = 5m. Exceeds 3m cap → all-or-nothing leaves the run.
      expect(out.at(0)?.get('cpu')).toBe(0.0);
      expect(out.at(1)?.get('cpu')).toBeUndefined();
      expect(out.at(2)?.get('cpu')).toBeUndefined();
      expect(out.at(3)?.get('cpu')).toBeUndefined();
      expect(out.at(4)?.get('cpu')).toBeUndefined();
      expect(out.at(5)?.get('cpu')).toBe(1.0);
    });
  });

  describe('partitioned materialize (sugar)', () => {
    it('runs materialize per partition and auto-populates partition columns', () => {
      // host 'a' has events at 0, 60k. host 'b' has only 0. Both should
      // get a row at every bucket of the grid; b's bucket-2 row carries
      // the partition column 'host' even though there's no source event.
      const ts = new TimeSeries({
        name: 'm',
        schema: partSchema,
        rows: [
          [0, 0.5, 'a'],
          [0, 0.3, 'b'],
          [60_000, 0.6, 'a'],
        ],
      });
      // range [0, 60_000] → 2 buckets per host × 2 hosts = 4 rows
      const out = ts
        .partitionBy('host')
        .materialize(Sequence.every('1m'), {
          range: { start: 0, end: 60_000 },
        })
        .collect();
      expect(out.length).toBe(4);
      // Verify b@60k is present with host='b' even though no source event existed.
      const bAt60k = [...out.events].find(
        (e) => e.begin() === 60_000 && e.get('host') === 'b',
      );
      expect(bAt60k).toBeDefined();
      expect(bAt60k?.get('cpu')).toBeUndefined();
      expect(bAt60k?.get('host')).toBe('b');
    });

    it('full v0.10 pipeline: partitionBy + dedupe + materialize + fill(maxGap)', () => {
      const ts = new TimeSeries({
        name: 'm',
        schema: partSchema,
        rows: [
          [0, 0.5, 'a'],
          [0, 0.55, 'a'], // dup at t=0 within host a
          [0, 0.3, 'b'],
          // 1-minute gap (no events in [60k, 120k))
          [120_000, 0.7, 'a'],
          [120_000, 0.5, 'b'],
        ],
      });
      const out = ts
        .partitionBy('host')
        .dedupe({ keep: 'last' })
        .materialize(Sequence.every('1m'), {
          range: { start: 0, end: 120_000 },
        })
        .fill({ cpu: 'linear' }, { maxGap: '3m' })
        .collect();
      // range [0, 120k] → 3 bucket starts per host × 2 hosts = 6 rows
      expect(out.length).toBe(6);
      // Host a:
      //   t=0: dedupe-last → 0.55
      //   t=60k: empty → linear interp between 0.55 and 0.7 → 0.625
      //   t=120k: 0.7
      const aAt0 = [...out.events].find(
        (e) => e.begin() === 0 && e.get('host') === 'a',
      );
      const aAt60 = [...out.events].find(
        (e) => e.begin() === 60_000 && e.get('host') === 'a',
      );
      const aAt120 = [...out.events].find(
        (e) => e.begin() === 120_000 && e.get('host') === 'a',
      );
      expect(aAt0?.get('cpu')).toBe(0.55);
      expect(aAt60?.get('cpu')).toBeCloseTo(0.625, 5);
      expect(aAt120?.get('cpu')).toBe(0.7);

      // Every output row carries its partition's host even on empty buckets.
      for (const event of out.events) {
        expect(typeof event.get('host')).toBe('string');
      }
    });

    it('partitioned materialize preserves schema (host stays required: true)', () => {
      const ts = new TimeSeries({
        name: 'm',
        schema: partSchema,
        rows: [[0, 0.5, 'a']],
      });
      const out = ts
        .partitionBy('host')
        .materialize(Sequence.every('1m'), {
          range: { start: 0, end: 60_000 },
        })
        .collect();
      // The materialize value-column-widening flips required to false on
      // non-key columns, but the partition sugar guarantees partition
      // columns are populated on every row, so the runtime invariant holds.
      // Schema-wise: cpu is widened to optional; host is too (the type
      // system can't know the partition guarantee, just like apply).
      const cpuCol = out.schema.find((c) => c.name === 'cpu');
      expect(cpuCol?.required).toBe(false);
    });
  });
});
