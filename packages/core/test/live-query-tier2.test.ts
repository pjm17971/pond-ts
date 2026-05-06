/**
 * Tier 2 query primitives on `LiveSeries` and `LiveView`. Mirrors
 * the matching `TimeSeries` methods (`find` / `some` / `every` /
 * `includesKey` / `bisect` / `atOrBefore` / `atOrAfter`).
 *
 * These methods are pure parity additions — same shape, same
 * return-type semantics. Tests focus on:
 *   - Empty buffer behavior (sane defaults)
 *   - Predicate / index argument plumbing
 *   - Binary-search edge cases (before, exact, after, between)
 *   - Live mutation: methods reflect the buffer's current state
 *   - LiveView (filtered / windowed) sees the post-process buffer
 */
import { describe, expect, it } from 'vitest';
import { LiveSeries, Time } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
] as const;

function makeLive() {
  return new LiveSeries({ name: 'test', schema });
}

// ── LiveSeries.find / some / every ──────────────────────────────

describe('LiveSeries.find', () => {
  it('returns undefined on an empty buffer', () => {
    expect(makeLive().find(() => true)).toBeUndefined();
  });

  it('returns the first matching event', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1],
      [2000, 5],
      [3000, 10],
    ]);
    const found = live.find((e) => (e.get('value') as number) >= 5);
    expect(found?.begin()).toBe(2000);
  });

  it('passes the index to the predicate', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1],
      [2000, 2],
      [3000, 3],
    ]);
    const indices: number[] = [];
    live.find((_, i) => {
      indices.push(i);
      return false;
    });
    expect(indices).toEqual([0, 1, 2]);
  });
});

describe('LiveSeries.some', () => {
  it('returns false on an empty buffer', () => {
    expect(makeLive().some(() => true)).toBe(false);
  });

  it('returns true when any event matches', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1],
      [2000, 5],
    ]);
    expect(live.some((e) => (e.get('value') as number) > 3)).toBe(true);
  });

  it('returns false when no event matches', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1],
      [2000, 2],
    ]);
    expect(live.some((e) => (e.get('value') as number) > 10)).toBe(false);
  });
});

describe('LiveSeries.every', () => {
  it('returns true on an empty buffer (vacuously true, matches Array)', () => {
    expect(makeLive().every(() => false)).toBe(true);
  });

  it('returns true when all events match', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 5],
      [2000, 10],
    ]);
    expect(live.every((e) => (e.get('value') as number) > 0)).toBe(true);
  });

  it('returns false when any event fails the predicate', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 5],
      [2000, -1],
    ]);
    expect(live.every((e) => (e.get('value') as number) > 0)).toBe(false);
  });
});

// ── LiveSeries.bisect / includesKey / atOrBefore / atOrAfter ────

describe('LiveSeries.bisect', () => {
  it('returns 0 for an empty buffer', () => {
    expect(makeLive().bisect(new Time(1000))).toBe(0);
  });

  it('returns 0 when the key is before all events', () => {
    const live = makeLive();
    live.pushMany([
      [2000, 1],
      [3000, 2],
    ]);
    expect(live.bisect(new Time(1000))).toBe(0);
  });

  it('returns length when the key is after all events', () => {
    const live = makeLive();
    live.pushMany([
      [2000, 1],
      [3000, 2],
    ]);
    expect(live.bisect(new Time(9999))).toBe(2);
  });

  it('returns the index of the matching key when present', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1],
      [2000, 2],
      [3000, 3],
    ]);
    expect(live.bisect(new Time(2000))).toBe(1);
  });

  it('returns the insertion point between events', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1],
      [3000, 3],
      [5000, 5],
    ]);
    expect(live.bisect(new Time(2000))).toBe(1);
    expect(live.bisect(new Time(4000))).toBe(2);
  });

  it('accepts a numeric timestamp shorthand', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1],
      [2000, 2],
    ]);
    expect(live.bisect(2000)).toBe(1);
  });
});

describe('LiveSeries.includesKey', () => {
  it('returns false on an empty buffer', () => {
    expect(makeLive().includesKey(new Time(1000))).toBe(false);
  });

  it('returns true for an exact match', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1],
      [2000, 2],
    ]);
    expect(live.includesKey(new Time(2000))).toBe(true);
  });

  it('returns false for a key between events', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1],
      [3000, 3],
    ]);
    expect(live.includesKey(new Time(2000))).toBe(false);
  });

  it('accepts a numeric shorthand', () => {
    const live = makeLive();
    live.push([1000, 1]);
    expect(live.includesKey(1000)).toBe(true);
  });
});

describe('LiveSeries.atOrBefore', () => {
  it('returns undefined on an empty buffer', () => {
    expect(makeLive().atOrBefore(new Time(1000))).toBeUndefined();
  });

  it('returns the exact match when present', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1],
      [2000, 2],
    ]);
    expect(live.atOrBefore(new Time(2000))?.get('value')).toBe(2);
  });

  it('returns the most recent prior event when no exact match', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1],
      [3000, 3],
    ]);
    expect(live.atOrBefore(new Time(2500))?.get('value')).toBe(1);
  });

  it('returns undefined when the key is before all events', () => {
    const live = makeLive();
    live.push([2000, 1]);
    expect(live.atOrBefore(new Time(1000))).toBeUndefined();
  });

  it('returns the last event when the key is after all events', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1],
      [2000, 2],
    ]);
    expect(live.atOrBefore(new Time(9999))?.get('value')).toBe(2);
  });
});

describe('LiveSeries.atOrAfter', () => {
  it('returns undefined on an empty buffer', () => {
    expect(makeLive().atOrAfter(new Time(1000))).toBeUndefined();
  });

  it('returns the exact match when present', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1],
      [2000, 2],
    ]);
    expect(live.atOrAfter(new Time(2000))?.get('value')).toBe(2);
  });

  it('returns the next event after the key when no exact match', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1],
      [3000, 3],
    ]);
    expect(live.atOrAfter(new Time(2500))?.get('value')).toBe(3);
  });

  it('returns the first event when the key is before all events', () => {
    const live = makeLive();
    live.pushMany([
      [2000, 1],
      [3000, 2],
    ]);
    expect(live.atOrAfter(new Time(1000))?.get('value')).toBe(1);
  });

  it('returns undefined when the key is after all events', () => {
    const live = makeLive();
    live.push([1000, 1]);
    expect(live.atOrAfter(new Time(9999))).toBeUndefined();
  });
});

// ── Live mutation: methods reflect current buffer state ─────────

describe('LiveSeries query methods reflect current buffer', () => {
  it('find / some / every update as events arrive', () => {
    const live = makeLive();
    expect(live.some(() => true)).toBe(false);
    live.push([1000, 5]);
    expect(live.some((e) => (e.get('value') as number) === 5)).toBe(true);
    expect(live.find((e) => (e.get('value') as number) === 5)?.begin()).toBe(
      1000,
    );
  });

  it('bisect / atOrBefore reflect retention evictions', () => {
    const live = new LiveSeries({
      name: 'test',
      schema,
      retention: { maxEvents: 2 },
    });
    live.pushMany([
      [1000, 1],
      [2000, 2],
    ]);
    expect(live.atOrBefore(new Time(1500))?.begin()).toBe(1000);
    live.push([3000, 3]); // evicts 1000
    expect(live.atOrBefore(new Time(1500))).toBeUndefined();
    expect(live.atOrBefore(new Time(2500))?.begin()).toBe(2000);
  });
});

// ── LiveView parity ─────────────────────────────────────────────

describe('LiveView query primitives', () => {
  it('find on a filtered view sees only post-filter events', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1],
      [2000, 5],
      [3000, 10],
    ]);
    const positive = live.filter((e) => (e.get('value') as number) > 3);
    const found = positive.find(() => true);
    expect(found?.begin()).toBe(2000);
  });

  it('bisect on a windowed view binary-searches the windowed buffer', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1],
      [2000, 2],
      [3000, 3],
      [4000, 4],
    ]);
    const view = live.window(2); // last 2 events
    expect(view.length).toBe(2);
    expect(view.bisect(new Time(3000))).toBe(0);
    expect(view.bisect(new Time(4000))).toBe(1);
    expect(view.bisect(new Time(2000))).toBe(0); // before view start
  });

  it('atOrBefore on a windowed view honors the window boundary', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1],
      [2000, 2],
      [3000, 3],
      [4000, 4],
    ]);
    const view = live.window(2);
    // Buffer holds [3000, 4000]. atOrBefore(2500) should return undefined
    // since the view's earliest event is 3000.
    expect(view.atOrBefore(new Time(2500))).toBeUndefined();
    expect(view.atOrBefore(new Time(3500))?.begin()).toBe(3000);
  });

  it('every on a filtered view evaluates over the filtered subset', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1],
      [2000, 5],
      [3000, 10],
    ]);
    const positive = live.filter((e) => (e.get('value') as number) > 3);
    expect(positive.every((e) => (e.get('value') as number) > 3)).toBe(true);
  });

  it('includesKey on a filtered view returns false when the event was filtered out', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1],
      [2000, 5],
    ]);
    const positive = live.filter((e) => (e.get('value') as number) > 3);
    expect(positive.includesKey(new Time(1000))).toBe(false); // filtered out
    expect(positive.includesKey(new Time(2000))).toBe(true);
  });
});

// ── Symmetry with TimeSeries ────────────────────────────────────

describe('LiveSeries query parity with TimeSeries', () => {
  it('snapshotting a live buffer to TimeSeries yields identical query results', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1],
      [2000, 2],
      [3000, 3],
    ]);
    const snap = live.toTimeSeries();
    expect(live.bisect(new Time(2000))).toBe(snap.bisect(new Time(2000)));
    expect(live.includesKey(new Time(2000))).toBe(
      snap.includesKey(new Time(2000)),
    );
    expect(live.atOrBefore(new Time(2500))?.begin()).toBe(
      snap.atOrBefore(new Time(2500))?.begin(),
    );
    expect(live.atOrAfter(new Time(2500))?.begin()).toBe(
      snap.atOrAfter(new Time(2500))?.begin(),
    );
  });
});
