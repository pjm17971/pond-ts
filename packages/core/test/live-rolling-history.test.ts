/**
 * `history: false | RetentionPolicy` on live rolling outputs. Pins
 * the three modes (default `true`, `false`, RetentionPolicy) for both
 * `LiveRollingAggregation` and `LiveFusedRolling`.
 *
 * The reducer state itself is unaffected by `history`; only the
 * accumulator's own output buffer (read by `length` / `at(i)`) is.
 */
import { describe, expect, it } from 'vitest';
import { LiveSeries, Trigger } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
] as const;

function makeLive(opts?: Partial<ConstructorParameters<typeof LiveSeries>[0]>) {
  return new LiveSeries({ name: 'test', schema, ...opts });
}

// ── LiveRollingAggregation ─────────────────────────────────────

describe('LiveRollingAggregation { history }', () => {
  it('default: keeps every emitted event (back-compat)', () => {
    const live = makeLive();
    const rolling = live.rolling(3, { value: 'avg' });
    for (let i = 1; i <= 5; i++) {
      live.push([i * 1000, i]);
    }
    expect(rolling.length).toBe(5);
    expect(rolling.at(0)).toBeDefined();
    expect(rolling.at(-1)).toBeDefined();
  });

  it('history: true is identical to the default', () => {
    const live = makeLive();
    const rolling = live.rolling(3, { value: 'avg' }, { history: true });
    for (let i = 1; i <= 5; i++) {
      live.push([i * 1000, i]);
    }
    expect(rolling.length).toBe(5);
  });

  it('history: false skips output retention; length stays 0, listeners still fire', () => {
    const live = makeLive();
    const rolling = live.rolling(3, { value: 'avg' }, { history: false });
    const seen: any[] = [];
    rolling.on('event', (e) => seen.push(e));
    for (let i = 1; i <= 5; i++) {
      live.push([i * 1000, i]);
    }
    expect(rolling.length).toBe(0);
    expect(rolling.at(0)).toBeUndefined();
    expect(rolling.at(-1)).toBeUndefined();
    // Listeners get every emit.
    expect(seen).toHaveLength(5);
    // value() still tracks reducer state.
    expect(rolling.value().value).toBeCloseTo(4); // avg of [3, 4, 5]
  });

  it('history: { maxEvents: N } caps the output buffer at N entries', () => {
    const live = makeLive();
    const rolling = live.rolling(
      5,
      { value: 'avg' },
      { history: { maxEvents: 3 } },
    );
    for (let i = 1; i <= 10; i++) {
      live.push([i * 1000, i]);
    }
    expect(rolling.length).toBe(3);
    // Newest 3 emits retained — the oldest dropped first.
    expect(rolling.at(-1)?.begin()).toBe(10_000);
    expect(rolling.at(0)?.begin()).toBe(8_000);
  });

  it('history: { maxAge: "5s" } drops emits older than 5s relative to latest', () => {
    const live = makeLive();
    const rolling = live.rolling(
      5,
      { value: 'avg' },
      { history: { maxAge: '5s' } },
    );
    // Emits at 1s, 2s, 3s, 4s, 5s, 6s, 7s, 8s, 9s, 10s.
    // Latest emit at 10s; cutoff = 10s - 5s = 5s. Emits with ts < 5s drop.
    for (let i = 1; i <= 10; i++) {
      live.push([i * 1000, i]);
    }
    // Retained: ts in [5_000, 10_000] — 6 emits (5,6,7,8,9,10).
    expect(rolling.length).toBe(6);
    expect(rolling.at(0)?.begin()).toBe(5_000);
    expect(rolling.at(-1)?.begin()).toBe(10_000);
  });

  it('history: { maxEvents, maxAge } combines both caps (whichever is stricter wins)', () => {
    const live = makeLive();
    const rolling = live.rolling(
      5,
      { value: 'avg' },
      { history: { maxEvents: 4, maxAge: '5s' } },
    );
    for (let i = 1; i <= 10; i++) {
      live.push([i * 1000, i]);
    }
    // maxAge would keep 6 (ts 5-10); maxEvents caps further at 4.
    expect(rolling.length).toBe(4);
    expect(rolling.at(0)?.begin()).toBe(7_000);
  });

  it('history: { maxEvents: 0 } throws — must be a positive integer', () => {
    const live = makeLive();
    expect(() =>
      live.rolling(3, { value: 'avg' }, { history: { maxEvents: 0 } }),
    ).toThrow(/positive integer/);
  });

  it('history: { maxEvents: -1 } throws', () => {
    const live = makeLive();
    expect(() =>
      live.rolling(3, { value: 'avg' }, { history: { maxEvents: -1 } }),
    ).toThrow(/positive integer/);
  });

  it('history: { maxEvents: 1.5 } throws — must be integer', () => {
    const live = makeLive();
    expect(() =>
      live.rolling(3, { value: 'avg' }, { history: { maxEvents: 1.5 } }),
    ).toThrow(/positive integer/);
  });

  it('history: false works under Trigger.count too — emissions fire, no retention', () => {
    const live = makeLive();
    const rolling = live.rolling(
      5,
      { value: 'avg' },
      { history: false, trigger: Trigger.count(3) },
    );
    const seen: any[] = [];
    rolling.on('event', (e) => seen.push(e));
    for (let i = 1; i <= 9; i++) {
      live.push([i * 1000, i]);
    }
    expect(rolling.length).toBe(0);
    expect(seen).toHaveLength(3); // floor(9 / 3)
  });

  it('history: {} (empty object) is treated as both caps Infinity', () => {
    const live = makeLive();
    const rolling = live.rolling(3, { value: 'avg' }, { history: {} });
    for (let i = 1; i <= 5; i++) {
      live.push([i * 1000, i]);
    }
    expect(rolling.length).toBe(5); // no cap, same as history: true
  });
});

// ── LiveFusedRolling ───────────────────────────────────────────

describe('LiveFusedRolling { history }', () => {
  it('default: keeps every emitted event', () => {
    const live = makeLive();
    const fused = live.rolling({ '5s': { value: 'avg' } });
    for (let i = 1; i <= 5; i++) {
      live.push([i * 1000, i]);
    }
    expect(fused.length).toBe(5);
  });

  it('history: false skips output retention for the merged stream', () => {
    const live = makeLive();
    const fused = live.rolling({ '5s': { value: 'avg' } }, { history: false });
    const seen: any[] = [];
    fused.on('event', (e) => seen.push(e));
    for (let i = 1; i <= 5; i++) {
      live.push([i * 1000, i]);
    }
    expect(fused.length).toBe(0);
    expect(fused.at(0)).toBeUndefined();
    expect(seen).toHaveLength(5);
    // value() still tracks reducer state across all windows.
    expect(fused.value().value).toBeCloseTo(3); // avg of [1,2,3,4,5]
  });

  it('history: { maxEvents } caps the merged output buffer', () => {
    const live = makeLive();
    const fused = live.rolling(
      {
        '5s': { value_avg: { from: 'value', using: 'avg' } },
        '10s': { value_sum: { from: 'value', using: 'sum' } },
      },
      { history: { maxEvents: 3 } },
    );
    for (let i = 1; i <= 10; i++) {
      live.push([i * 1000, i]);
    }
    expect(fused.length).toBe(3);
    expect(fused.at(-1)?.begin()).toBe(10_000);
  });

  it('history: { maxAge } drops emits older than the cap', () => {
    const live = makeLive();
    const fused = live.rolling(
      { '5s': { value: 'avg' } },
      { history: { maxAge: '3s' } },
    );
    for (let i = 1; i <= 10; i++) {
      live.push([i * 1000, i]);
    }
    // Latest 10s; cutoff 10s - 3s = 7s. Emits with ts < 7s drop.
    // Retained: 7,8,9,10 (4 emits).
    expect(fused.length).toBe(4);
    expect(fused.at(0)?.begin()).toBe(7_000);
  });

  it('history: { maxEvents: 0 } throws on the fused class too', () => {
    const live = makeLive();
    expect(() =>
      live.rolling({ '5s': { value: 'avg' } }, { history: { maxEvents: 0 } }),
    ).toThrow(/positive integer/);
  });
});

// Stats-interaction tests live in `live-stats.test.ts` once PR #123
// (stats() accessor) merges — emission counters fire regardless of
// `history` retention, but pinning that requires the stats() API.
