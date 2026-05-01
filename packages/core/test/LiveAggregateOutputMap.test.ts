/**
 * Tests for `AggregateOutputMap` support on the live accumulators
 * (`LiveSeries.rolling`, `LiveSeries.aggregate`, the chainable
 * `LiveView`/`LivePartitionedSeries`/`LivePartitionedView` variants,
 * and synchronised partitioned rolling). Added in v0.13.0 to close
 * the feature-parity gap with batch (`TimeSeries.rolling/aggregate`).
 *
 * The runtime helper `normalizeAggregateColumns` (shared with batch)
 * already accepted both shapes; this file pins the public live
 * surface that exposes them.
 */
import { describe, expect, it } from 'vitest';
import { LiveSeries, Sequence, Trigger } from '../src/index.js';

const flatSchema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

const partitionedSchema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

function makeFlat() {
  return new LiveSeries({ name: 'metrics', schema: flatSchema });
}

function makePartitioned() {
  return new LiveSeries({ name: 'metrics', schema: partitionedSchema });
}

// ── LiveSeries.rolling with AggregateOutputMap ─────────────────────

describe('LiveSeries.rolling — AggregateOutputMap', () => {
  it('computes multiple reducers over the same source column', () => {
    const live = makeFlat();
    const rolling = live.rolling('5s', {
      mean: { from: 'value', using: 'avg' },
      sd: { from: 'value', using: 'stdev' },
    });

    live.push(
      [0, 10, 'a'],
      [1000, 20, 'a'],
      [2000, 30, 'a'],
      [3000, 40, 'a'],
      [4000, 50, 'a'],
    );

    const v = rolling.value();
    expect(v.mean).toBe(30); // avg(10..50)
    expect(v.sd as number).toBeCloseTo(Math.sqrt(200), 5);
    rolling.dispose();
  });

  it('renames output columns independently from sources', () => {
    const live = makeFlat();
    const rolling = live.rolling('5s', {
      cpuAvg: { from: 'value', using: 'avg' },
      cpuMax: { from: 'value', using: 'max' },
    });

    live.push([0, 10, 'a'], [1000, 30, 'a']);

    expect(rolling.schema.map((c) => c.name)).toEqual([
      'time',
      'cpuAvg',
      'cpuMax',
    ]);
    const v = rolling.value();
    expect(v.cpuAvg).toBe(20);
    expect(v.cpuMax).toBe(30);
    rolling.dispose();
  });

  it('emits per-event with both reducer outputs populated', () => {
    const live = makeFlat();
    const rolling = live.rolling('5s', {
      mean: { from: 'value', using: 'avg' },
      sd: { from: 'value', using: 'stdev' },
    });
    const seen: Array<Record<string, unknown>> = [];
    rolling.on('event', (e) =>
      seen.push({
        mean: e.get('mean'),
        sd: e.get('sd'),
      }),
    );

    live.push([0, 10, 'a'], [1000, 20, 'a']);
    expect(seen).toHaveLength(2);
    expect(seen[1]!.mean).toBe(15);
    rolling.dispose();
  });

  it('respects an explicit kind override on the output spec', () => {
    const live = makeFlat();
    const rolling = live.rolling('5s', {
      mean: { from: 'value', using: 'avg', kind: 'number' as const },
    });
    expect(rolling.schema.find((c) => c.name === 'mean')!.kind).toBe('number');
    rolling.dispose();
  });

  it('rejects an output spec referencing an unknown source column', () => {
    const live = makeFlat();
    expect(() =>
      // Cast away the type error — runtime guard is the asserted behavior
      live.rolling('5s', {
        mean: { from: 'nonexistent' as 'value', using: 'avg' },
      }),
    ).toThrow(/unknown source column/);
  });

  it('rejects custom-function reducers with a clear error pointing at the workaround', () => {
    const live = makeFlat();
    expect(() =>
      live.rolling('5s', {
        mean: {
          from: 'value',
          using: ((vs: ReadonlyArray<number | undefined>) => {
            const nums = vs.filter((v): v is number => typeof v === 'number');
            return nums.reduce((s, v) => s + v, 0) / Math.max(1, nums.length);
          }) as any,
          kind: 'number' as const,
        },
      }),
    ).toThrow(/custom function reducers are not supported on live rolling/);
  });
});

// ── LiveSeries.aggregate with AggregateOutputMap ───────────────────

describe('LiveSeries.aggregate — AggregateOutputMap', () => {
  it('aggregates over fixed buckets with multiple aliased reducers', () => {
    const live = makeFlat();
    const agg = live.aggregate(Sequence.every('1s'), {
      mean: { from: 'value', using: 'avg' },
      hi: { from: 'value', using: 'max' },
    });

    // Bucket [0, 1000): events at 0 and 999 → avg = 15, max = 20
    // Bucket [1000, 2000): event at 1000 → avg = 30, max = 30
    live.push([0, 10, 'a'], [999, 20, 'a'], [1000, 30, 'a']);
    // Push a watermark beyond the first bucket to close it
    live.push([2000, 40, 'a']);

    const closed = agg.closed();
    expect(closed.schema.map((c) => c.name)).toEqual(['time', 'mean', 'hi']);
    expect(closed.length).toBeGreaterThan(0);
    expect(closed.at(0)!.get('mean')).toBe(15);
    expect(closed.at(0)!.get('hi')).toBe(20);
    agg.dispose();
  });

  it('rejects custom-function reducers on live aggregation', () => {
    const live = makeFlat();
    expect(() =>
      live.aggregate(Sequence.every('1s'), {
        mean: {
          from: 'value',
          using: ((vs: ReadonlyArray<number | undefined>) => {
            const nums = vs.filter((v): v is number => typeof v === 'number');
            return nums.reduce((s, v) => s + v, 0);
          }) as any,
          kind: 'number' as const,
        },
      }),
    ).toThrow(/custom function reducers are not supported on live aggregation/);
  });
});

// ── LiveView (chained) → rolling/aggregate with OutputMap ──────────

describe('LiveView.rolling/aggregate — AggregateOutputMap', () => {
  it('rolls multiple reducers over a filtered chain', () => {
    const live = makeFlat();
    const filtered = live.filter((e) => (e.get('value') as number) >= 20);
    const rolling = filtered.rolling('5s', {
      mean: { from: 'value', using: 'avg' },
      sd: { from: 'value', using: 'stdev' },
    });

    live.push(
      [0, 10, 'a'], // filtered out
      [1000, 20, 'a'],
      [2000, 30, 'a'],
      [3000, 40, 'a'],
    );

    const v = rolling.value();
    expect(v.mean).toBe(30); // avg(20,30,40)
    expect((v.sd as number) > 0).toBe(true);
    rolling.dispose();
  });

  it('aggregates multiple reducers over a filtered chain', () => {
    const live = makeFlat();
    const filtered = live.filter((e) => (e.get('value') as number) >= 20);
    const agg = filtered.aggregate(Sequence.every('1s'), {
      mean: { from: 'value', using: 'avg' },
      hi: { from: 'value', using: 'max' },
    });

    live.push(
      [0, 10, 'a'], // filtered out
      [500, 20, 'a'], // bucket [0,1000)
      [800, 40, 'a'], // bucket [0,1000)
      [1500, 30, 'a'], // bucket [1000,2000)
      [2500, 50, 'a'], // bucket [2000,3000) — closes the prior bucket
    );

    const closed = agg.closed();
    expect(closed.schema.map((c) => c.name)).toEqual(['time', 'mean', 'hi']);
    expect(closed.length).toBeGreaterThan(0);
    // First bucket [0, 1000): events 20 + 40 → avg=30, max=40
    expect(closed.at(0)!.get('mean')).toBe(30);
    expect(closed.at(0)!.get('hi')).toBe(40);
    agg.dispose();
  });
});

// ── LiveAggregation.rolling and LiveRollingAggregation.aggregate ───

describe('chainable accumulators with AggregateOutputMap', () => {
  it('LiveAggregation.rolling rolls aliased outputs over closed buckets', () => {
    const live = makeFlat();
    const agg = live.aggregate(Sequence.every('1s'), { value: 'avg' });
    const rolled = agg.rolling('5s', {
      avgOfAvg: { from: 'value', using: 'avg' },
      maxOfAvg: { from: 'value', using: 'max' },
    });

    // Buckets close as the watermark crosses each bucket.end:
    //   [0,1000): avg(10,30) = 20  → closes at t=1500
    //   [1000,2000): avg(40)  = 40  → closes at t=2500
    //   [2000,3000): avg(50)  = 50  → closes at t=3500
    live.push(
      [0, 10, 'a'],
      [500, 30, 'a'],
      [1500, 40, 'a'],
      [2500, 50, 'a'],
      [3500, 60, 'a'],
    );

    expect(rolled.schema.map((c) => c.name)).toEqual([
      'time',
      'avgOfAvg',
      'maxOfAvg',
    ]);
    // After all closes, the rolling window holds bucket-avgs [20, 40, 50].
    // 5s window keyed at the bucket starts [0, 1000, 2000] — none evicted.
    const v = rolled.value();
    expect(v.avgOfAvg as number).toBeCloseTo((20 + 40 + 50) / 3, 5);
    expect(v.maxOfAvg).toBe(50);
    rolled.dispose();
  });

  it('LiveRollingAggregation.aggregate buckets aliased outputs over rolling-output events', () => {
    const live = makeFlat();
    const rolled = live.rolling('5s', { value: 'avg' });
    const agg = rolled.aggregate(Sequence.every('1s'), {
      mean: { from: 'value', using: 'avg' },
      hi: { from: 'value', using: 'max' },
    });

    // Rolling emits one event per push, keyed at the source ts:
    //   t=0    rolling-avg = 10
    //   t=500  rolling-avg = 15
    //   t=1500 rolling-avg = 20  (avg of 10,20,30)
    //   t=2500 rolling-avg = 25  (avg of 10,20,30,40)
    // Aggregated into 1s buckets:
    //   [0,1000):    rolling-avgs [10, 15]  → mean=12.5, hi=15  (closes at t=1500)
    //   [1000,2000): rolling-avgs [20]      → mean=20,   hi=20  (closes at t=2500)
    live.push([0, 10, 'a'], [500, 20, 'a'], [1500, 30, 'a'], [2500, 40, 'a']);

    expect(agg.schema.map((c) => c.name)).toEqual(['time', 'mean', 'hi']);
    const closed = agg.closed();
    expect(closed.length).toBeGreaterThanOrEqual(2);
    expect(closed.at(0)!.get('mean')).toBe(12.5);
    expect(closed.at(0)!.get('hi')).toBe(15);
    expect(closed.at(1)!.get('mean')).toBe(20);
    expect(closed.at(1)!.get('hi')).toBe(20);
    agg.dispose();
  });
});

// ── Per-partition rolling with AggregateOutputMap (event trigger) ──

describe('partitionBy().rolling — AggregateOutputMap (per-partition)', () => {
  it('per-partition rolling exposes multiple reducer outputs per partition', () => {
    const live = makePartitioned();
    const partitioned = live.partitionBy('host', {
      groups: ['api-1', 'api-2'] as const,
    });
    const rolledByHost = partitioned.rolling('5s', {
      mean: { from: 'cpu', using: 'avg' },
      hi: { from: 'cpu', using: 'max' },
    });

    // Globally monotonic timestamps (LiveSeries defaults to strict
    // ordering). The router still demuxes them per host.
    live.push(
      [0, 0.4, 'api-1'],
      [500, 0.5, 'api-2'],
      [1000, 0.6, 'api-1'],
      [1500, 0.9, 'api-2'],
    );

    const map = rolledByHost.toMap();
    // api-1 saw [0.4, 0.6] in its 5s window → avg=0.5, max=0.6
    const apiOne = map.get('api-1')!;
    const last1 = apiOne.at(apiOne.length - 1)!;
    expect(last1.get('mean') as number).toBeCloseTo(0.5, 5);
    expect(last1.get('hi')).toBe(0.6);

    // api-2 saw [0.5, 0.9] in its 5s window → avg=0.7, max=0.9
    const apiTwo = map.get('api-2')!;
    const last2 = apiTwo.at(apiTwo.length - 1)!;
    expect(last2.get('mean') as number).toBeCloseTo(0.7, 5);
    expect(last2.get('hi')).toBe(0.9);

    partitioned.dispose();
  });
});

// ── Synchronised partitioned rolling with AggregateOutputMap ───────

describe('partitionBy().rolling clock trigger — AggregateOutputMap', () => {
  it('emits multiple reducer outputs per partition at each boundary', () => {
    const live = makePartitioned();
    const ticks = live.partitionBy('host').rolling(
      '1m',
      {
        mean: { from: 'cpu', using: 'avg' },
        hi: { from: 'cpu', using: 'max' },
      },
      { trigger: Trigger.clock(Sequence.every('30s')) },
    );

    // Schema = [time, host, mean, hi]
    expect(ticks.schema.map((c) => c.name)).toEqual([
      'time',
      'host',
      'mean',
      'hi',
    ]);

    live.push([0, 0.4, 'api-1']);
    live.push([5_000, 0.5, 'api-2']);
    live.push([10_000, 0.6, 'api-1']);
    // Boundary-crossing event at 30_001 — added to api-1's window
    // BEFORE the boundary tick fires, so it contributes to the
    // emitted snapshot at the 30_000 boundary.
    live.push([30_001, 0.9, 'api-1']);

    expect(ticks.length).toBe(2);
    const byHost = new Map<unknown, { mean: unknown; hi: unknown }>();
    for (let i = 0; i < ticks.length; i++) {
      const e = ticks.at(i)!;
      byHost.set(e.get('host'), { mean: e.get('mean'), hi: e.get('hi') });
    }
    // api-1 window holds [0.4, 0.6, 0.9] → avg=0.6333, max=0.9
    expect(byHost.get('api-1')!.mean as number).toBeCloseTo(
      (0.4 + 0.6 + 0.9) / 3,
      5,
    );
    expect(byHost.get('api-1')!.hi).toBe(0.9);
    // api-2 had only 0.5 in its window → avg=0.5, max=0.5
    expect(byHost.get('api-2')!.mean).toBe(0.5);
    expect(byHost.get('api-2')!.hi).toBe(0.5);
  });

  it('rejects when an OUTPUT alias collides with the partition column', () => {
    const live = makePartitioned();
    expect(() =>
      live.partitionBy('host').rolling(
        '1m',
        // Aliased output 'host' collides with the partition column —
        // collision check compares against output name, not source.
        { host: { from: 'cpu', using: 'avg' } as any },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      ),
    ).toThrowError(/collides with a reducer-output column/);
  });

  it('preserves the existing AggregateMap shape unchanged on sync rolling', () => {
    const live = makePartitioned();
    const ticks = live
      .partitionBy('host')
      .rolling(
        '1m',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );

    expect(ticks.schema.map((c) => c.name)).toEqual(['time', 'host', 'cpu']);
  });
});
