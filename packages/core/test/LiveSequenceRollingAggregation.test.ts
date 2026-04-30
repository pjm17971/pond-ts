import { describe, expect, it, vi } from 'vitest';
import {
  LiveSeries,
  LiveSequenceRollingAggregation,
  LiveView,
  Sequence,
} from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
] as const;

function makeLive() {
  return new LiveSeries({ name: 'test', schema });
}

// ── Basic emission ──────────────────────────────────────────────────

describe('LiveSequenceRollingAggregation — basic emission', () => {
  it('does not emit before first boundary is crossed', () => {
    const live = makeLive();
    const seq = live.rolling(Sequence.every('30s'), '1m', { value: 'avg' });

    live.push([0, 10], [5_000, 20], [15_000, 30]);
    expect(seq.length).toBe(0);

    seq.dispose();
  });

  it('emits once when a boundary is crossed', () => {
    const live = makeLive();
    const seq = live.rolling(Sequence.every('30s'), '1m', { value: 'avg' });

    live.push([0, 10], [15_000, 20], [30_001, 30]); // last event crosses 30 s mark
    expect(seq.length).toBe(1);

    seq.dispose();
  });

  it('emits at the epoch-aligned boundary timestamp', () => {
    const live = makeLive();
    const seq = live.rolling(Sequence.every('30s'), '1m', { value: 'avg' });

    live.push([0, 10], [30_001, 20]);
    expect(seq.at(0)!.begin()).toBe(30_000); // bucket boundary, not event ts

    seq.dispose();
  });

  it('emits once when a single event crosses multiple boundaries', () => {
    // Event at 0 → bucket 0. Event at 90 001 → bucket 3.
    // Buckets 1 (30 s) and 2 (60 s) had no data → no emission for them.
    // One emission at ts=90 000 (start of bucket 3).
    const live = makeLive();
    const seq = live.rolling(Sequence.every('30s'), '1m', { value: 'avg' });

    live.push([0, 10]);
    live.push([90_001, 20]);
    expect(seq.length).toBe(1);
    expect(seq.at(0)!.begin()).toBe(90_000);

    seq.dispose();
  });

  it('emits two events when two boundaries are crossed', () => {
    const live = makeLive();
    const seq = live.rolling(Sequence.every('30s'), '1m', { value: 'avg' });

    live.push([0, 10]);
    live.push([30_001, 20]); // crosses 30 s
    live.push([60_001, 30]); // crosses 60 s
    expect(seq.length).toBe(2);
    expect(seq.at(0)!.begin()).toBe(30_000);
    expect(seq.at(1)!.begin()).toBe(60_000);

    seq.dispose();
  });
});

// ── Rolling window content ──────────────────────────────────────────

describe('LiveSequenceRollingAggregation — rolling window content', () => {
  it('emitted value reflects trailing rolling window', () => {
    const live = makeLive();
    // 1-minute rolling avg, emit every 30 s
    const seq = live.rolling(Sequence.every('30s'), '1m', { value: 'avg' });

    // Push events at 0 s, 10 s, 20 s — all inside the 30 s window
    live.push([0, 10], [10_000, 20], [20_000, 30]);
    // Cross the 30 s boundary
    live.push([30_001, 40]);

    // At the 30 s boundary, rolling window = [0, 30001-60000 cutoff]
    // All four events are within the 1-minute window
    const emitted = seq.at(0)!;
    expect(emitted.get('value')).toBeCloseTo((10 + 20 + 30 + 40) / 4, 5);

    seq.dispose();
  });

  it('respects minSamples — emits undefined while window is cold', () => {
    const live = makeLive();
    const seq = live.rolling(
      Sequence.every('30s'),
      '1m',
      { value: 'avg' },
      { minSamples: 10 },
    );

    // Push only 2 events before crossing the boundary — window not yet warm
    live.push([0, 100], [10_000, 200], [30_001, 300]);
    expect(seq.at(0)!.get('value')).toBeUndefined();

    seq.dispose();
  });

  it('eviction affects emitted value', () => {
    const live = makeLive();
    // 30-second rolling window, emit every 30 s
    const seq = live.rolling(Sequence.every('30s'), '30s', { value: 'avg' });

    // Events at 0 and 15 s are inside the first 30 s window
    live.push([0, 10], [15_000, 20]);
    // Event at 40 s: window = [10 000, 40 001) — only the 40 s event is in the window
    // (10 000 cutoff evicts 0 and 15 000 ... wait, 40000 - 30000 = 10000)
    // 0 < 10000, 15000 >= 10000, 40001 >= 10000
    // so window contains 15 000 and 40 001
    // But we cross the 30 s boundary so seq emits at ts=30 000
    // At the moment of crossing, we've pushed [40_001, 30] but the rolling
    // window is from the ROLLING state AFTER processing [40 001, 30].
    // cutoff = 40001 - 30000 = 10001; events at 0 and 15 000: 0 < 10001 (evicted),
    // 15000 >= 10001 (kept). So window = [15 000, 40 001] → avg(20, 30) = 25.
    live.push([40_001, 30]);
    expect(seq.length).toBe(1);
    expect(seq.at(0)!.get('value')).toBeCloseTo(25, 5);

    seq.dispose();
  });
});

// ── Event subscription ─────────────────────────────────────────────

describe('LiveSequenceRollingAggregation — on(event)', () => {
  it('fires the event listener on boundary crossing', () => {
    const live = makeLive();
    const seq = live.rolling(Sequence.every('30s'), '1m', { value: 'sum' });

    const received: number[] = [];
    seq.on('event', (e) => received.push(e.begin()));

    live.push([0, 1], [30_001, 2]);
    expect(received).toEqual([30_000]);

    live.push([60_001, 3]);
    expect(received).toEqual([30_000, 60_000]);

    seq.dispose();
  });

  it('unsubscribe stops receiving events', () => {
    const live = makeLive();
    const seq = live.rolling(Sequence.every('30s'), '1m', { value: 'sum' });

    const spy = vi.fn();
    const unsub = seq.on('event', spy);

    live.push([0, 1], [30_001, 2]);
    expect(spy).toHaveBeenCalledTimes(1);

    unsub();
    live.push([60_001, 3]);
    expect(spy).toHaveBeenCalledTimes(1); // no new calls after unsub

    seq.dispose();
  });
});

// ── LiveSource contract ────────────────────────────────────────────

describe('LiveSequenceRollingAggregation — LiveSource contract', () => {
  it('exposes name and schema from rolling source', () => {
    const live = makeLive();
    const seq = live.rolling(Sequence.every('30s'), '1m', { value: 'avg' });

    expect(seq.name).toBe('test');
    expect(seq.schema[0]!.name).toBe('time');
    expect(seq.schema[1]!.name).toBe('value');

    seq.dispose();
  });

  it('at() with negative index reads from the end', () => {
    const live = makeLive();
    const seq = live.rolling(Sequence.every('30s'), '1m', { value: 'sum' });

    live.push([0, 1], [30_001, 2], [60_001, 3]);
    expect(seq.length).toBe(2);
    expect(seq.at(-1)!.begin()).toBe(60_000);
    expect(seq.at(0)!.begin()).toBe(30_000);

    seq.dispose();
  });
});

// ── View-transform chaining ────────────────────────────────────────

describe('LiveSequenceRollingAggregation — chaining', () => {
  it('filter() returns a LiveView', () => {
    const live = makeLive();
    const seq = live.rolling(Sequence.every('30s'), '1m', { value: 'avg' });
    const filtered = seq.filter(() => true);
    expect(filtered).toBeInstanceOf(LiveView);

    seq.dispose();
  });

  it('.sequence() on a LiveRollingAggregation still works (step-by-step form)', () => {
    // rolling.sequence(interval) is the lower-level form — still valid
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'avg' });
    const seq = rolling.sequence('30s');
    expect(seq).toBeInstanceOf(LiveSequenceRollingAggregation);

    live.push([0, 10], [30_001, 20]);
    expect(seq.length).toBe(1);

    rolling.dispose();
    seq.dispose();
  });
});

// ── Sequence with anchor ───────────────────────────────────────────

describe('LiveSequenceRollingAggregation — Sequence anchor', () => {
  it('respects a non-zero anchor from a Sequence object', () => {
    const live = makeLive();
    // Anchor at 5 000 ms → boundaries at 5 000, 35 000, 65 000 …
    const seq = live.rolling(
      Sequence.every('30s', { anchor: 5_000 }),
      '1m',
      { value: 'avg' },
    );

    live.push([5_000, 10], [35_001, 20]); // crosses 35 000 boundary
    expect(seq.length).toBe(1);
    expect(seq.at(0)!.begin()).toBe(35_000);

    seq.dispose();
  });
});

// ── dispose ────────────────────────────────────────────────────────

describe('LiveSequenceRollingAggregation — dispose', () => {
  it('stops emitting after dispose', () => {
    const live = makeLive();
    const seq = live.rolling(Sequence.every('30s'), '1m', { value: 'sum' });

    const spy = vi.fn();
    seq.on('event', spy);

    live.push([0, 1], [30_001, 2]);
    expect(spy).toHaveBeenCalledTimes(1);

    seq.dispose();
    live.push([60_001, 3]);
    expect(spy).toHaveBeenCalledTimes(1); // seq disposed; no new calls
  });
});
