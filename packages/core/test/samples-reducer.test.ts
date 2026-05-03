/**
 * Tests for the `'samples'` reducer (v0.14.1) — returns the bucket's
 * defined values as an array. Sits beside `unique` (which dedups)
 * and `top${N}` (which bounds). Use case: anomaly density (raw values
 * for thresholding against a separate baseline), histogramming,
 * custom downstream computation.
 */
import { describe, expect, it } from 'vitest';
import { LiveSeries, Sequence, TimeSeries, Trigger } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

function makeSeries() {
  return new TimeSeries({
    name: 'metrics',
    schema,
    rows: [
      [0, 10, 'api-1'],
      [1000, 20, 'api-1'],
      [2000, 30, 'api-2'],
      [3000, 40, 'api-2'],
      [4000, 50, 'api-3'],
    ],
  });
}

function makeLive() {
  return new LiveSeries({ name: 'metrics', schema });
}

// ── Batch reduce ─────────────────────────────────────────────────

describe('samples reducer — batch reduce', () => {
  it('returns all defined values in arrival order', () => {
    const result = makeSeries().reduce({
      vals: { from: 'cpu', using: 'samples' },
    });
    expect(result.vals).toEqual([10, 20, 30, 40, 50]);
  });

  it('preserves duplicates (distinct from unique)', () => {
    const ts = new TimeSeries({
      name: 'dup',
      schema,
      rows: [
        [0, 10, 'a'],
        [1000, 10, 'a'], // duplicate value
        [2000, 20, 'b'],
        [3000, 10, 'a'], // and another
      ],
    });
    const result = ts.reduce({
      vals: { from: 'cpu', using: 'samples' },
    });
    expect(result.vals).toEqual([10, 10, 20, 10]);
  });

  it('skips undefined values', () => {
    const optSchema = [
      { name: 'time', kind: 'time' },
      { name: 'cpu', kind: 'number', required: false },
    ] as const;
    const ts = new TimeSeries({
      name: 'with-gaps',
      schema: optSchema,
      rows: [
        [0, 10],
        [1000, undefined],
        [2000, 20],
        [3000, undefined],
      ],
    });
    const result = ts.reduce({
      vals: { from: 'cpu', using: 'samples' },
    });
    expect(result.vals).toEqual([10, 20]);
  });

  it('flattens one level of array sources (matches unique)', () => {
    const tagSchema = [
      { name: 'time', kind: 'time' },
      { name: 'tags', kind: 'array' },
    ] as const;
    const ts = new TimeSeries({
      name: 'tags',
      schema: tagSchema,
      rows: [
        [0, ['a', 'b']],
        [1000, ['b', 'c']],
      ],
    });
    const result = ts.reduce({
      flat: { from: 'tags', using: 'samples' },
    });
    expect(result.flat).toEqual(['a', 'b', 'b', 'c']);
  });

  it('returns empty array for empty bucket', () => {
    const empty = new TimeSeries({
      name: 'empty',
      schema,
      rows: [],
    });
    const result = empty.reduce({
      vals: { from: 'cpu', using: 'samples' },
    });
    expect(result.vals).toEqual([]);
  });
});

// ── Batch aggregate ──────────────────────────────────────────────

describe('samples reducer — batch aggregate', () => {
  it('produces per-bucket arrays of values', () => {
    const agg = makeSeries().aggregate(Sequence.every('2s'), {
      vals: { from: 'cpu', using: 'samples' },
    });
    // Bucket [0, 2000): [10, 20]
    // Bucket [2000, 4000): [30, 40]
    // Bucket [4000, 6000): [50]
    expect(agg.at(0)!.get('vals')).toEqual([10, 20]);
    expect(agg.at(1)!.get('vals')).toEqual([30, 40]);
    expect(agg.at(2)!.get('vals')).toEqual([50]);
  });

  it('output column kind is array', () => {
    const agg = makeSeries().aggregate(Sequence.every('2s'), {
      vals: { from: 'cpu', using: 'samples' },
    });
    const col = agg.schema.find((c) => c.name === 'vals');
    expect(col?.kind).toBe('array');
  });
});

// ── Batch rolling ────────────────────────────────────────────────

describe('samples reducer — batch rolling', () => {
  it('returns trailing-window values per source event', () => {
    const rolled = makeSeries().rolling('3s', {
      vals: { from: 'cpu', using: 'samples' },
    });
    // At t=4000, trailing 3s window holds events at t∈(1000, 4000]:
    // values [30, 40, 50]
    expect(rolled.at(4)!.get('vals')).toEqual([30, 40, 50]);
  });

  it('values evict from the rolling window in arrival order', () => {
    const rolled = makeSeries().rolling('3s', {
      vals: { from: 'cpu', using: 'samples' },
    });
    // At t=0, only event 0 is in window: [10]
    expect(rolled.at(0)!.get('vals')).toEqual([10]);
    // At t=2000, window holds [10, 20, 30]
    expect(rolled.at(2)!.get('vals')).toEqual([10, 20, 30]);
    // At t=3000, window holds [20, 30, 40] (10 evicted)
    expect(rolled.at(3)!.get('vals')).toEqual([20, 30, 40]);
  });
});

// ── Live aggregate ───────────────────────────────────────────────

describe('samples reducer — live aggregate', () => {
  it('emits per-bucket arrays as buckets close', () => {
    const live = makeLive();
    const agg = live.aggregate(Sequence.every('1s'), {
      vals: { from: 'cpu', using: 'samples' },
    });

    // Bucket [0, 1000): [10, 20]
    live.push([0, 10, 'a'], [500, 20, 'a']);
    // Watermark cross to close the first bucket:
    live.push([1500, 30, 'a']);

    const closed = agg.closed();
    expect(closed.length).toBe(1);
    expect(closed.at(0)!.get('vals')).toEqual([10, 20]);
    agg.dispose();
  });
});

// ── Live rolling ─────────────────────────────────────────────────

describe('samples reducer — live rolling', () => {
  it('exposes the current window as an array via rolling.value()', () => {
    const live = makeLive();
    const rolling = live.rolling('5s', {
      vals: { from: 'cpu', using: 'samples' },
    });

    live.push([0, 10, 'a'], [1000, 20, 'a'], [2000, 30, 'a']);
    expect(rolling.value().vals).toEqual([10, 20, 30]);
    rolling.dispose();
  });

  it('evicts old values from the window correctly', () => {
    const live = makeLive();
    const rolling = live.rolling('3s', {
      vals: { from: 'cpu', using: 'samples' },
    });

    // Window cutoff at last event minus 3s. Event 0 is excluded once
    // an event ≥ 3000ms after it lands.
    live.push([0, 10, 'a'], [1000, 20, 'a'], [2000, 30, 'a']);
    expect(rolling.value().vals).toEqual([10, 20, 30]);

    live.push([4000, 40, 'a']);
    // cutoff = 4000 - 3000 = 1000; event at 0 evicted (begin < 1000)
    expect(rolling.value().vals).toEqual([20, 30, 40]);
    rolling.dispose();
  });

  it('survives multiple emit/evict cycles', () => {
    const live = makeLive();
    const rolling = live.rolling('2s', {
      vals: { from: 'cpu', using: 'samples' },
    });

    live.push([0, 1, 'a'], [1000, 2, 'a'], [2000, 3, 'a']);
    expect(rolling.value().vals).toEqual([1, 2, 3]);

    live.push([3000, 4, 'a']);
    expect(rolling.value().vals).toEqual([2, 3, 4]);

    live.push([4000, 5, 'a']);
    expect(rolling.value().vals).toEqual([3, 4, 5]);

    rolling.dispose();
  });

  it('synced partitioned rolling with samples per partition', () => {
    const live = new LiveSeries({
      name: 'partitioned',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'cpu', kind: 'number' },
        { name: 'host', kind: 'string' },
      ] as const,
    });

    const ticks = live
      .partitionBy('host')
      .rolling(
        '1m',
        { vals: { from: 'cpu', using: 'samples' } },
        { trigger: Trigger.every('30s') },
      );

    live.push([0, 0.4, 'api-1']);
    live.push([5_000, 0.5, 'api-2']);
    live.push([10_000, 0.6, 'api-1']);
    live.push([30_001, 0.9, 'api-1']); // crosses 30s boundary

    expect(ticks.length).toBe(2);
    const byHost = new Map<unknown, unknown>();
    for (let i = 0; i < ticks.length; i++) {
      const e = ticks.at(i)!;
      byHost.set(e.get('host'), e.get('vals'));
    }
    // api-1 window holds [0.4, 0.6, 0.9] in arrival order
    expect(byHost.get('api-1')).toEqual([0.4, 0.6, 0.9]);
    // api-2 window holds [0.5]
    expect(byHost.get('api-2')).toEqual([0.5]);
  });
});

// ── End-to-end use case: anomaly density ─────────────────────────

describe('samples reducer — anomaly density use case', () => {
  it('counts samples exceeding k·sigma from a baseline mean', () => {
    // Recreates the gRPC experiment's anomaly-density pattern that
    // motivated this reducer.
    const baselineMean = 0.5;
    const baselineSd = 0.1;
    const thresholds = [1, 1.5, 2];

    const live = makeLive();
    const rolling = live.rolling('200ms', {
      vals: { from: 'cpu', using: 'samples' },
    });

    live.push(
      [0, 0.5, 'a'], // 0 sigma
      [50, 0.65, 'a'], // 1.5 sigma
      [100, 0.8, 'a'], // 3 sigma
      [150, 0.55, 'a'], // 0.5 sigma
    );

    const samples = rolling.value().vals as ReadonlyArray<number>;
    const counts = thresholds.map(
      (k) => samples.filter((v) => v - baselineMean > k * baselineSd).length,
    );
    expect(counts).toEqual([2, 1, 1]); // > 1σ: 0.65 & 0.8; > 1.5σ: 0.8; > 2σ: 0.8
    rolling.dispose();
  });
});
