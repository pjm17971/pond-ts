/**
 * Tests for `TimeSeries.outliers`, `toPoints`, and `TimeSeries.fromPoints`
 * — landed in v0.5.8 to close the dashboard ergonomics loop for
 * rolling-baseline anomaly detection.
 */
import { describe, expect, it } from 'vitest';
import { Sequence, TimeSeries } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

describe('TimeSeries.outliers', () => {
  it('flags events outside the rolling ±σ band', () => {
    // 40 samples mostly at 0.5 with a single spike at 0.95 at i=35.
    // Use a 10s trailing window (10 events) so 9 normal samples
    // establish the baseline before the spike.
    const rows: Array<[number, number, string]> = [];
    for (let i = 0; i < 40; i += 1) {
      const cpu = i === 35 ? 0.95 : 0.5 + ((i * 0.003) % 0.02);
      rows.push([i * 1000, cpu, 'api-1']);
    }
    const s = new TimeSeries({ name: 'cpu', schema, rows });

    const anomalies = s.outliers('cpu', { window: '10s', sigma: 2 });

    expect(anomalies.length).toBe(1);
    expect(anomalies.at(0)!.get('cpu')).toBe(0.95);
  });

  it('preserves the source schema (all columns pass through)', () => {
    const rows: Array<[number, number, string]> = [];
    for (let i = 0; i < 40; i += 1) {
      const cpu = i === 20 ? 2.5 : 0.5; // huge spike
      rows.push([i * 1000, cpu, i % 2 === 0 ? 'api-1' : 'api-2']);
    }
    const s = new TimeSeries({ name: 'cpu', schema, rows });

    const anomalies = s.outliers('cpu', { window: '10s', sigma: 2 });
    expect(anomalies.schema).toEqual(s.schema);
    // Host column travels with the flagged event (i=20 is even → api-1)
    expect(anomalies.at(0)!.get('host')).toBe('api-1');
  });

  it('does not flag flat series (sd=0 → nothing to flag against)', () => {
    const rows: Array<[number, number, string]> = Array.from(
      { length: 20 },
      (_, i) => [i * 1000, 0.5, 'api-1'],
    );
    const s = new TimeSeries({ name: 'cpu', schema, rows });

    expect(s.outliers('cpu', { window: '5s', sigma: 1 }).length).toBe(0);
  });

  it('narrower sigma flags more events; wider sigma flags fewer', () => {
    // Noisy series with several modest spikes.
    const rows: Array<[number, number, string]> = [];
    for (let i = 0; i < 40; i += 1) {
      let cpu = 0.5 + Math.sin(i / 3) * 0.02;
      if (i === 20) cpu = 0.9;
      if (i === 30) cpu = 0.85;
      rows.push([i * 1000, cpu, 'api-1']);
    }
    const s = new TimeSeries({ name: 'cpu', schema, rows });

    const wide = s.outliers('cpu', { window: '5s', sigma: 3 }).length;
    const narrow = s.outliers('cpu', { window: '5s', sigma: 1 }).length;
    expect(narrow).toBeGreaterThanOrEqual(wide);
  });

  it('composes with aggregate for bucketed anomaly counts', () => {
    // Dashboard pattern: outlier detection → 15s count-per-bucket.
    const rows: Array<[number, number, string]> = [];
    for (let i = 0; i < 60; i += 1) {
      // 3 clustered spikes in the middle 15s window
      const cpu = i === 30 || i === 31 || i === 32 ? 0.95 : 0.5;
      rows.push([i * 1000, cpu, 'api-1']);
    }
    const s = new TimeSeries({ name: 'cpu', schema, rows });

    const anomalyCounts = s
      .outliers('cpu', { window: '5s', sigma: 2 })
      .aggregate(Sequence.every('15s'), { cpu: 'count' });

    expect(anomalyCounts.length).toBeGreaterThan(0);
    // The clustered-spike bucket should be non-empty.
    const total = anomalyCounts
      .toArray()
      .reduce((sum, e) => sum + (e.get('cpu') as number), 0);
    expect(total).toBeGreaterThanOrEqual(1);
  });

  it('rejects invalid sigma values', () => {
    const s = new TimeSeries({
      name: 'cpu',
      schema,
      rows: [[0, 0.5, 'api-1']],
    });
    expect(() => s.outliers('cpu', { window: '5s', sigma: 0 })).toThrow(
      /positive finite/,
    );
    expect(() => s.outliers('cpu', { window: '5s', sigma: -1 })).toThrow(
      /positive finite/,
    );
    expect(() => s.outliers('cpu', { window: '5s', sigma: NaN })).toThrow(
      /positive finite/,
    );
    expect(() => s.outliers('cpu', { window: '5s', sigma: Infinity })).toThrow(
      /positive finite/,
    );
  });

  it('honors the alignment option', () => {
    // Centered alignment has access to future samples — it may flag
    // differently than trailing. Just verifies the option threads
    // through and both produce defined results.
    const rows: Array<[number, number, string]> = [];
    for (let i = 0; i < 40; i += 1) {
      rows.push([i * 1000, i === 20 ? 2.0 : 0.5, 'api-1']);
    }
    const s = new TimeSeries({ name: 'cpu', schema, rows });

    const trailing = s.outliers('cpu', {
      window: '10s',
      sigma: 2,
      alignment: 'trailing',
    });
    const centered = s.outliers('cpu', {
      window: '10s',
      sigma: 2,
      alignment: 'centered',
    });
    // Both should find at least the big spike
    expect(trailing.length).toBeGreaterThan(0);
    expect(centered.length).toBeGreaterThan(0);
  });

  it('returns an empty series when given an empty source', () => {
    const s = new TimeSeries({ name: 'cpu', schema, rows: [] });
    expect(s.outliers('cpu', { window: '5s', sigma: 2 }).length).toBe(0);
  });
});

describe('TimeSeries.toPoints', () => {
  it('returns flat { ts, value }[] pairs', () => {
    const s = new TimeSeries({
      name: 'cpu',
      schema,
      rows: [
        [0, 0.3, 'api-1'],
        [1000, 0.5, 'api-1'],
        [2000, 0.7, 'api-2'],
      ],
    });
    expect(s.toPoints('cpu')).toEqual([
      { ts: 0, value: 0.3 },
      { ts: 1000, value: 0.5 },
      { ts: 2000, value: 0.7 },
    ]);
  });

  it('returns a frozen array', () => {
    const s = new TimeSeries({
      name: 'cpu',
      schema,
      rows: [[0, 0.5, 'api-1']],
    });
    const pts = s.toPoints('cpu');
    expect(Object.isFrozen(pts)).toBe(true);
  });

  it('drops events where the column is undefined', () => {
    const optionalSchema = [
      { name: 'time', kind: 'time' },
      { name: 'cpu', kind: 'number', required: false },
    ] as const;
    const s = new TimeSeries({
      name: 'cpu',
      schema: optionalSchema,
      rows: [
        [0, 0.3],
        [1000, undefined],
        [2000, 0.7],
      ],
    });
    expect(s.toPoints('cpu')).toEqual([
      { ts: 0, value: 0.3 },
      { ts: 2000, value: 0.7 },
    ]);
  });

  it('uses begin() for interval-keyed series', () => {
    const intervalSchema = [
      { name: 'interval', kind: 'interval' },
      { name: 'value', kind: 'number' },
    ] as const;
    const s = new TimeSeries({
      name: 'agg',
      schema: intervalSchema,
      rows: [
        [{ value: '0', start: 0, end: 1000 }, 10],
        [{ value: '1', start: 1000, end: 2000 }, 20],
      ],
    });
    expect(s.toPoints('value')).toEqual([
      { ts: 0, value: 10 },
      { ts: 1000, value: 20 },
    ]);
  });

  it('returns an empty array for an empty series', () => {
    const s = new TimeSeries({ name: 'cpu', schema, rows: [] });
    expect(s.toPoints('cpu')).toEqual([]);
  });
});

describe('TimeSeries.fromPoints', () => {
  it('constructs a series from flat { ts, value } points', () => {
    const ts = TimeSeries.fromPoints(
      [
        { ts: 0, value: 0.3 },
        { ts: 1000, value: 0.5 },
        { ts: 2000, value: 0.7 },
      ],
      {
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'cpu', kind: 'number' },
        ] as const,
      },
    );
    expect(ts.length).toBe(3);
    expect(ts.at(0)!.get('cpu')).toBe(0.3);
    expect(ts.at(2)!.get('cpu')).toBe(0.7);
  });

  it('accepts a name option, defaults to "points"', () => {
    const defaultName = TimeSeries.fromPoints([{ ts: 0, value: 1 }], {
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'v', kind: 'number' },
      ] as const,
    });
    expect(defaultName.name).toBe('points');

    const custom = TimeSeries.fromPoints([{ ts: 0, value: 1 }], {
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'v', kind: 'number' },
      ] as const,
      name: 'anomalies',
    });
    expect(custom.name).toBe('anomalies');
  });

  it('rejects schemas with other than 2 columns', () => {
    expect(() =>
      TimeSeries.fromPoints([{ ts: 0, value: 1 }], {
        schema: [{ name: 'time', kind: 'time' }] as const,
      }),
    ).toThrow(/exactly two columns/);

    expect(() =>
      TimeSeries.fromPoints([{ ts: 0, value: 1 }], {
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'a', kind: 'number' },
          { name: 'b', kind: 'number' },
        ] as const,
      }),
    ).toThrow(/exactly two columns/);
  });

  it('round-trips with toPoints', () => {
    const original = TimeSeries.fromPoints(
      [
        { ts: 0, value: 0.3 },
        { ts: 1000, value: 0.5 },
      ],
      {
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'cpu', kind: 'number' },
        ] as const,
      },
    );
    const pts = original.toPoints('cpu');
    expect(pts).toEqual([
      { ts: 0, value: 0.3 },
      { ts: 1000, value: 0.5 },
    ]);
  });

  it('accepts Date and Time ts inputs', () => {
    const ts = TimeSeries.fromPoints(
      [
        { ts: new Date(0), value: 1 },
        { ts: 1000, value: 2 },
      ],
      {
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'v', kind: 'number' },
        ] as const,
      },
    );
    expect(ts.length).toBe(2);
    expect(ts.at(0)!.begin()).toBe(0);
    expect(ts.at(1)!.begin()).toBe(1000);
  });

  it('replays the dashboard pattern: outliers → points → fromPoints → aggregate', () => {
    // Setup: a series with clustered anomalies.
    const rows: Array<[number, number, string]> = [];
    for (let i = 0; i < 60; i += 1) {
      const cpu = i === 30 || i === 31 || i === 32 ? 0.95 : 0.5;
      rows.push([i * 1000, cpu, 'api-1']);
    }
    const s = new TimeSeries({ name: 'cpu', schema, rows });

    // Dashboard pattern: flatten outliers to points (e.g. for chart
    // rendering), then round-trip back to a TimeSeries for bucketed
    // counting.
    const pts = s.outliers('cpu', { window: '5s', sigma: 2 }).toPoints('cpu');
    expect(pts.length).toBeGreaterThan(0);

    const restored = TimeSeries.fromPoints(pts, {
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'cpu', kind: 'number' },
      ] as const,
    });
    const buckets = restored.aggregate(Sequence.every('15s'), {
      cpu: 'count',
    });
    // At least one bucket has the clustered anomalies.
    const counts = buckets.toArray().map((e) => e.get('cpu') as number);
    expect(counts.some((c) => c > 0)).toBe(true);
  });
});
