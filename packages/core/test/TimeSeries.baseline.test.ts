/**
 * Tests for `TimeSeries.baseline` — the rolling-baseline primitive
 * shipped in v0.5.9. Band-chart drawing reads the `upper`/`lower`
 * columns directly, and `outliers()` is conceptually equivalent to
 * `baseline().filter(...)` (same flat-window skip, independent
 * implementation).
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

  it('upper = avg + sigma*sd, lower = avg - sigma*sd (undefined when flat)', () => {
    const b = makeSteady().baseline('cpu', { window: '10s', sigma: 2 });
    for (let i = 0; i < b.length; i += 1) {
      const e = b.at(i)!;
      const avg = e.get('avg');
      const sd = e.get('sd');
      const upper = e.get('upper');
      const lower = e.get('lower');
      if (avg == null || sd == null || sd === 0) {
        // No baseline yet, or flat window: band is undefined by
        // design so filters don't flag every non-equal point.
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
    // Both paths should line up event-for-event on the jittery
    // fixture. This case does not exercise the sd===0 branch; see
    // the flat-run test below for that.
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

  it('emits undefined bands when the rolling window is flat (sd === 0)', () => {
    // A strictly flat run followed by a single spike: inside the flat
    // region the rolling stdev is exactly 0, so upper/lower must be
    // undefined — a zero-width band would flag every non-equal point
    // as anomalous, which is the bug this guards.
    const rows: Array<[number, number, string]> = [];
    for (let i = 0; i < 40; i += 1) {
      rows.push([i * 1000, i === 35 ? 0.95 : 0.5, 'api-1']);
    }
    const s = new TimeSeries({ name: 'cpu', schema, rows });
    const b = s.baseline('cpu', { window: '10s', sigma: 2 });

    // At i=20 (mid flat run) the window has 10 samples all equal to
    // 0.5: avg=0.5, sd=0, band undefined.
    const e = b.at(20)!;
    expect(e.get('avg')).toBe(0.5);
    expect(e.get('sd')).toBe(0);
    expect(e.get('upper')).toBeUndefined();
    expect(e.get('lower')).toBeUndefined();

    // A naive `value > upper || value < lower` filter must not flag
    // the flat-window events. Only the genuine spike at i=35 should
    // survive, and only because by then the spike itself has entered
    // the rolling window and sd > 0.
    const flagged = b.filter((ev) => {
      const cpu = ev.get('cpu');
      const upper = ev.get('upper');
      const lower = ev.get('lower');
      if (cpu == null || upper == null || lower == null) return false;
      return cpu > upper || cpu < lower;
    });
    // The genuine spike must survive — pin the count so the "only the
    // spike" claim can't silently pass by flagging nothing.
    expect(flagged.length).toBeGreaterThan(0);
    for (let i = 0; i < flagged.length; i += 1) {
      expect(flagged.at(i)!.get('cpu')).toBe(0.95);
    }

    // baseline().filter() and outliers() agree event-for-event on the
    // flat fixture — the sd===0 behavior is shared.
    const viaOutliers = s.outliers('cpu', { window: '10s', sigma: 2 });
    expect(flagged.length).toBe(viaOutliers.length);
    for (let i = 0; i < flagged.length; i += 1) {
      expect(flagged.at(i)!.begin()).toBe(viaOutliers.at(i)!.begin());
    }
  });

  it('toPoints on upper/lower drops warm-up events and widens each frame', () => {
    const b = makeSteady().baseline('cpu', { window: '10s', sigma: 2 });
    const upper = b.toPoints('upper');
    const lower = b.toPoints('lower');
    // Warm-up events (sd still undefined) produce undefined bands;
    // toPoints drops those, so the emitted point count is strictly
    // smaller than the total event count.
    expect(upper.length).toBe(lower.length);
    expect(upper.length).toBeGreaterThan(0);
    expect(upper.length).toBeLessThan(b.length);
    // For every emitted frame, upper is *strictly* above lower. Strict
    // inequality holds because the `makeSteady` fixture has jitter — no
    // rolling window inside it has sd===0 once warm-up passes. If a
    // future fixture edit introduces a flat region, those rows would
    // be dropped here (undefined band) before this assertion runs.
    for (let i = 0; i < upper.length; i += 1) {
      expect(upper[i]!.value).toBeGreaterThan(lower[i]!.value);
    }
    // Point timestamps align with the source events in order.
    for (let i = 0; i < upper.length; i += 1) {
      expect(upper[i]!.ts).toBe(lower[i]!.ts);
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
    // Step-function fixture: flat 0.5 for the first half, flat 1.0 for
    // the second half. The midpoint exposes a real alignment delta —
    // trailing sees only the old plateau, centered sees both.
    const rows: Array<[number, number, string]> = [];
    for (let i = 0; i < 40; i += 1) {
      rows.push([i * 1000, i < 20 ? 0.5 : 1.0, 'api-1']);
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
    expect(trailing.length).toBe(centered.length);

    // At i=19 (last sample of the old plateau), trailing's 10s
    // window has consumed only 0.5 samples. Centered's window
    // straddles the step and picks up post-step 1.0 samples, so its
    // avg lands strictly between the two plateaus.
    const trailingAvg = trailing.at(19)!.get('avg') as number;
    const centeredAvg = centered.at(19)!.get('avg') as number;
    expect(trailingAvg).toBe(0.5);
    expect(centeredAvg).toBeGreaterThan(0.5);
    expect(centeredAvg).toBeLessThan(1.0);
    // Once the step has fully left the trailing window, avg snaps
    // to the new plateau.
    expect(trailing.at(35)!.get('avg')).toBe(1.0);
  });

  it('preserves event keys exactly from the source', () => {
    const b = makeSteady().baseline('cpu', { window: '10s', sigma: 2 });
    const src = makeSteady();
    for (let i = 0; i < src.length; i += 1) {
      expect(b.at(i)!.begin()).toBe(src.at(i)!.begin());
    }
  });
});
