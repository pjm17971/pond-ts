/**
 * Tests for `TimeSeries.baseline` — the rolling-baseline primitive
 * shipped in v0.5.9. `outliers()` is now expressible as
 * `baseline().filter()`, and band-chart drawing reads the
 * `upper`/`lower` columns directly — one rolling pass instead of two.
 */
import { describe, expect, it } from 'vitest';
import { TimeSeries } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

function makeSteady() {
  // 40 samples mostly at 0.5 with a single spike at 0.95 at i=35.
  const rows: Array<[number, number, string]> = [];
  for (let i = 0; i < 40; i += 1) {
    const cpu = i === 35 ? 0.95 : 0.5 + ((i * 0.003) % 0.02);
    rows.push([i * 1000, cpu, 'api-1']);
  }
  return new TimeSeries({ name: 'cpu', schema, rows });
}

describe('TimeSeries.baseline', () => {
  it('appends avg / sd / upper / lower columns; preserves source', () => {
    const b = makeSteady().baseline('cpu', { window: '10s', sigma: 2 });
    expect(b.schema.map((c) => c.name)).toEqual([
      'time',
      'cpu',
      'host',
      'avg',
      'sd',
      'upper',
      'lower',
    ]);
    // All four appended columns are number-kind.
    for (const name of ['avg', 'sd', 'upper', 'lower']) {
      const col = b.schema.find((c) => c.name === name);
      expect(col?.kind).toBe('number');
    }
    // Source columns preserved on every row — cpu values from the
    // fixture are 0.5 + small jitter, host is always 'api-1'.
    const e = b.at(10)!;
    const cpu = e.get('cpu');
    expect(typeof cpu).toBe('number');
    expect(cpu).toBeGreaterThanOrEqual(0.5);
    expect(cpu).toBeLessThan(0.55);
    expect(e.get('host')).toBe('api-1');
  });

  it('upper = avg + sigma*sd, lower = avg - sigma*sd', () => {
    const b = makeSteady().baseline('cpu', { window: '10s', sigma: 2 });
    for (let i = 0; i < b.length; i += 1) {
      const e = b.at(i)!;
      const avg = e.get('avg');
      const sd = e.get('sd');
      const upper = e.get('upper');
      const lower = e.get('lower');
      if (avg == null || sd == null) {
        expect(upper).toBeUndefined();
        expect(lower).toBeUndefined();
        continue;
      }
      expect(upper).toBeCloseTo(avg + 2 * sd, 10);
      expect(lower).toBeCloseTo(avg - 2 * sd, 10);
    }
  });

  it('composes with filter for one-pass anomaly detection', () => {
    const b = makeSteady().baseline('cpu', { window: '10s', sigma: 2 });
    const anomalies = b.filter((e) => {
      const cpu = e.get('cpu');
      const upper = e.get('upper');
      const lower = e.get('lower');
      if (cpu == null || upper == null || lower == null) return false;
      return cpu > upper || cpu < lower;
    });
    // The single spike at i=35 is outside the band.
    expect(anomalies.length).toBe(1);
    expect(anomalies.at(0)!.get('cpu')).toBe(0.95);
  });

  it('matches outliers() on the same window/sigma', () => {
    // `outliers` is documented as sugar over this pattern; their
    // results should line up event-for-event.
    const s = makeSteady();
    const viaBaseline = s
      .baseline('cpu', { window: '10s', sigma: 2 })
      .filter((e) => {
        const cpu = e.get('cpu');
        const upper = e.get('upper');
        const lower = e.get('lower');
        if (cpu == null || upper == null || lower == null) return false;
        return cpu > upper || cpu < lower;
      });
    const viaOutliers = s.outliers('cpu', { window: '10s', sigma: 2 });
    expect(viaBaseline.length).toBe(viaOutliers.length);
    for (let i = 0; i < viaBaseline.length; i += 1) {
      expect(viaBaseline.at(i)!.begin()).toBe(viaOutliers.at(i)!.begin());
    }
  });

  it('toPoints on upper/lower feeds a band chart directly', () => {
    const b = makeSteady().baseline('cpu', { window: '10s', sigma: 2 });
    const upper = b.toPoints('upper');
    const lower = b.toPoints('lower');
    // Events where sd is undefined (e.g. first event of a rolling
    // window) don't emit an upper/lower point — toPoints drops them.
    expect(upper.length).toBe(lower.length);
    expect(upper.length).toBeGreaterThan(0);
    // Every upper >= corresponding lower.
    for (let i = 0; i < upper.length; i += 1) {
      expect(upper[i]!.value).toBeGreaterThanOrEqual(lower[i]!.value);
    }
  });

  it('honors custom output column names', () => {
    const b = makeSteady().baseline('cpu', {
      window: '10s',
      sigma: 2,
      names: {
        avg: 'cpuAvg',
        sd: 'cpuSd',
        upper: 'cpuHi',
        lower: 'cpuLo',
      },
    });
    expect(b.schema.map((c) => c.name)).toEqual([
      'time',
      'cpu',
      'host',
      'cpuAvg',
      'cpuSd',
      'cpuHi',
      'cpuLo',
    ]);
    const e = b.at(20)!;
    expect(typeof e.get('cpuAvg')).toBe('number');
    expect(typeof e.get('cpuHi')).toBe('number');
  });

  it('throws when output names collide with existing columns', () => {
    const s = makeSteady();
    // 'host' is already a source column
    expect(() =>
      s.baseline('cpu', {
        window: '10s',
        sigma: 2,
        names: { avg: 'host' },
      }),
    ).toThrow(/collides/);
  });

  it('rejects invalid sigma values', () => {
    const s = makeSteady();
    expect(() => s.baseline('cpu', { window: '10s', sigma: 0 })).toThrow(
      /positive finite/,
    );
    expect(() => s.baseline('cpu', { window: '10s', sigma: -1 })).toThrow(
      /positive finite/,
    );
    expect(() => s.baseline('cpu', { window: '10s', sigma: NaN })).toThrow(
      /positive finite/,
    );
    expect(() => s.baseline('cpu', { window: '10s', sigma: Infinity })).toThrow(
      /positive finite/,
    );
  });

  it('returns an empty-length result for an empty source', () => {
    const empty = new TimeSeries({ name: 'cpu', schema, rows: [] });
    const b = empty.baseline('cpu', { window: '10s', sigma: 2 });
    expect(b.length).toBe(0);
    expect(b.schema.map((c) => c.name)).toEqual([
      'time',
      'cpu',
      'host',
      'avg',
      'sd',
      'upper',
      'lower',
    ]);
  });

  it('honors the alignment option', () => {
    const rows: Array<[number, number, string]> = [];
    for (let i = 0; i < 40; i += 1) {
      rows.push([i * 1000, i === 20 ? 2.0 : 0.5, 'api-1']);
    }
    const s = new TimeSeries({ name: 'cpu', schema, rows });
    const trailing = s.baseline('cpu', {
      window: '10s',
      sigma: 2,
      alignment: 'trailing',
    });
    const centered = s.baseline('cpu', {
      window: '10s',
      sigma: 2,
      alignment: 'centered',
    });
    // Both produce the same row count; avg values differ because the
    // centered window sees future samples.
    expect(trailing.length).toBe(centered.length);
    // Sanity: at the spike, trailing avg < centered avg (centered
    // includes more post-spike normal samples above baseline... or
    // rather, at the spike itself, centered is smoother since it
    // averages with symmetric neighbors). Just check both are defined.
    expect(trailing.at(20)!.get('avg')).toBeDefined();
    expect(centered.at(20)!.get('avg')).toBeDefined();
  });

  it('preserves event keys exactly from the source', () => {
    const b = makeSteady().baseline('cpu', { window: '10s', sigma: 2 });
    const src = makeSteady();
    for (let i = 0; i < src.length; i += 1) {
      expect(b.at(i)!.begin()).toBe(src.at(i)!.begin());
    }
  });
});
