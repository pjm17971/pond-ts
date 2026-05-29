import { describe, expect, it, vi } from 'vitest';

import { LiveSeries } from '../../src/live/live-series.js';
import { TimeRange } from '../../src/core/time-range.js';

/* -------------------------------------------------------------------------- */
/* Step 7 invariant pins — RingLiveStorage-backed LiveSeries                    */
/*                                                                             */
/* These pin the guarantees that are NEW risk surface under the ring backing:  */
/* lazy event materialization + cache, eviction cache-remap, materialized      */
/* value correctness across kinds, storage selection, and the preserved        */
/* public contracts (listener ordering, retention, query primitives). The      */
/* broad LiveSeries suite also runs through the ring for strict/drop modes;    */
/* this file is the explicit Step 7 contract.                                  */
/* -------------------------------------------------------------------------- */

const SCHEMA = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

function makeLive(opts: Record<string, unknown> = {}) {
  return new LiveSeries({ name: 't', schema: SCHEMA, ...opts });
}

/* ── Reference stability (RFC invariant) ──────────────────────────────────── */

describe('Step 7: at(i) reference stability', () => {
  it('at(i) === at(i) across repeated calls', () => {
    const live = makeLive();
    for (let i = 0; i < 100; i += 1) live.push([i * 1000, i, 'a']);
    for (const idx of [0, 7, 42, 99]) {
      expect(live.at(idx)).toBe(live.at(idx));
    }
  });

  it('at(i) === at(i) survives eviction (cache remap)', () => {
    const live = makeLive({ retention: { maxEvents: 50 } });
    for (let i = 0; i < 50; i += 1) live.push([i * 1000, i, 'a']);
    const before = live.at(40);
    expect(before).toBe(live.at(40)); // cached at logical 40
    // Push 10 more → evict 10 oldest. Old logical 40 → new logical 30.
    for (let i = 50; i < 60; i += 1) live.push([i * 1000, i, 'a']);
    const after = live.at(30);
    expect(after).toBe(before); // same object survived the index shift
    expect(after!.get('value')).toBe(40);
  });
});

/* ── Materialized value correctness across kinds ──────────────────────────── */

describe('Step 7: materialized event value correctness', () => {
  it('number / string columns round-trip', () => {
    const live = makeLive();
    live.push([1000, 42, 'api-1']);
    const e = live.at(0)!;
    expect(e.begin()).toBe(1000);
    expect(e.get('value')).toBe(42);
    expect(e.get('host')).toBe('api-1');
  });

  it('undefined (nullable) cells round-trip', () => {
    const live = new LiveSeries({
      name: 'n',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'value', kind: 'number', required: false },
      ] as const,
    });
    live.pushJson([{ time: 1000, value: null }]);
    live.push([2000, 7]);
    expect(live.at(0)!.get('value')).toBeUndefined();
    expect(live.at(1)!.get('value')).toBe(7);
  });

  it('boolean columns round-trip (true / false / undefined)', () => {
    const live = new LiveSeries({
      name: 'b',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'flag', kind: 'boolean', required: false },
      ] as const,
    });
    live.push([1000, true]);
    live.push([2000, false]);
    live.pushJson([{ time: 3000, flag: null }]);
    expect(live.at(0)!.get('flag')).toBe(true);
    expect(live.at(1)!.get('flag')).toBe(false);
    expect(live.at(2)!.get('flag')).toBeUndefined();
  });

  it('array columns round-trip and stay frozen', () => {
    const live = new LiveSeries({
      name: 'a',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'samples', kind: 'array' },
      ] as const,
    });
    live.push([1000, [1, 2, 3]]);
    const cell = live.at(0)!.get('samples') as ReadonlyArray<number>;
    expect(cell).toEqual([1, 2, 3]);
    expect(Object.isFrozen(cell)).toBe(true);
  });

  it('timeRange keys round-trip', () => {
    const live = new LiveSeries({
      name: 'tr',
      schema: [
        { name: 'window', kind: 'timeRange' },
        { name: 'value', kind: 'number' },
      ] as const,
    });
    live.push([new TimeRange({ start: 1000, end: 2000 }), 5]);
    const e = live.at(0)!;
    expect(e.begin()).toBe(1000);
    expect(e.end()).toBe(2000);
    expect(e.get('value')).toBe(5);
  });
});

/* ── Storage selection ────────────────────────────────────────────────────── */

describe('Step 7: storage selection by ordering + key kind', () => {
  // We can't read #storage directly, so we probe an observable
  // difference: reorder mode accepts an out-of-order insert (only the
  // array backing can sorted-insert); strict mode throws. Both still
  // expose identical public behavior, so this also pins that the ring
  // path didn't break ordered queries.

  it('strict mode (ring-backed) throws on out-of-order', () => {
    const live = makeLive({ ordering: 'strict' });
    live.push([2000, 2, 'a']);
    expect(() => live.push([1000, 1, 'a'])).toThrow();
  });

  it('drop mode (ring-backed) silently drops out-of-order', () => {
    const live = makeLive({ ordering: 'drop' });
    live.push([2000, 2, 'a']);
    live.push([1000, 1, 'a']); // dropped
    expect(live.length).toBe(1);
    expect(live.stats().rejected).toBe(1);
  });

  it('reorder mode (array-backed) sorted-inserts out-of-order', () => {
    const live = makeLive({ ordering: 'reorder' });
    live.push([1000, 1, 'a']);
    live.push([3000, 3, 'a']);
    live.push([2000, 2, 'a']); // inserts between
    expect(live.length).toBe(3);
    expect(live.at(0)!.begin()).toBe(1000);
    expect(live.at(1)!.begin()).toBe(2000);
    expect(live.at(2)!.begin()).toBe(3000);
  });

  it('interval-keyed series (array-backed) work under strict ordering', () => {
    const live = new LiveSeries({
      name: 'iv',
      schema: [
        { name: 'span', kind: 'interval' },
        { name: 'value', kind: 'number' },
      ] as const,
    });
    live.pushJson([
      { span: { value: 'lo', start: 1000, end: 2000 }, value: 1 },
    ]);
    expect(live.length).toBe(1);
    expect(live.at(0)!.get('value')).toBe(1);
  });
});

/* ── Listener fan-out + ordering ──────────────────────────────────────────── */

describe('Step 7: listener fan-out preserved on ring backing', () => {
  it('event → batch → evict ordering, per-event before retention', () => {
    const live = makeLive({ retention: { maxEvents: 2 } });
    const order: string[] = [];
    let lenAtFirstEvent = -1;
    let eventCount = 0;
    live.on('event', () => {
      eventCount += 1;
      if (eventCount === 3) lenAtFirstEvent = live.length;
      order.push('event');
    });
    live.on('batch', () => order.push('batch'));
    live.on('evict', () => order.push('evict'));

    live.push([0, 0, 'a']);
    live.push([1000, 1, 'a']);
    order.length = 0;
    // Third push: per-event fires (pre-retention: length still 3),
    // then retention evicts 1, then batch, then evict.
    live.push([2000, 2, 'a']);
    expect(order).toEqual(['event', 'batch', 'evict']);
    expect(lenAtFirstEvent).toBe(3); // observed pre-retention buffer
    expect(live.length).toBe(2);
  });

  it('evict listener receives the correct evicted events (value + order)', () => {
    const live = makeLive({ retention: { maxEvents: 2 } });
    const evicted: number[] = [];
    live.on('evict', (evs) => {
      for (const e of evs) evicted.push(e.get('value') as number);
    });
    live.push([0, 10, 'a']);
    live.push([1000, 20, 'a']);
    live.push([2000, 30, 'a']); // evicts value=10
    live.push([3000, 40, 'a']); // evicts value=20
    expect(evicted).toEqual([10, 20]);
  });

  it('batch listener fires once per pushMany with all added events', () => {
    const live = makeLive();
    const batchSizes: number[] = [];
    live.on('batch', (evs) => batchSizes.push(evs.length));
    live.pushMany([
      [0, 1, 'a'],
      [1000, 2, 'a'],
      [2000, 3, 'a'],
    ]);
    expect(batchSizes).toEqual([3]);
  });
});

/* ── Retention ────────────────────────────────────────────────────────────── */

describe('Step 7: retention on ring backing', () => {
  it('maxEvents cap honored', () => {
    const live = makeLive({ retention: { maxEvents: 3 } });
    for (let i = 0; i < 10; i += 1) live.push([i * 1000, i, 'a']);
    expect(live.length).toBe(3);
    expect(live.at(0)!.get('value')).toBe(7);
    expect(live.at(2)!.get('value')).toBe(9);
  });

  it('maxAge cap honored', () => {
    const live = makeLive({ retention: { maxAge: '5s' } });
    live.push([0, 0, 'a']);
    live.push([1000, 1, 'a']);
    live.push([7000, 7, 'a']); // 0 and 1000 are > 5s behind 7000
    expect(live.length).toBe(1);
    expect(live.at(0)!.get('value')).toBe(7);
  });
});

/* ── Query primitives on ring backing ─────────────────────────────────────── */

describe('Step 7: query primitives on ring backing', () => {
  const live = makeLive();
  for (let i = 0; i < 10; i += 1) live.push([i * 1000, i, 'a']);

  it('bisect / includesKey', () => {
    expect(live.bisect(5000)).toBe(5);
    expect(live.includesKey(5000)).toBe(true);
    expect(live.includesKey(5500)).toBe(false);
  });

  it('atOrBefore / atOrAfter', () => {
    expect(live.atOrBefore(5500)!.get('value')).toBe(5);
    expect(live.atOrAfter(5500)!.get('value')).toBe(6);
    expect(live.atOrBefore(-1)).toBeUndefined();
  });

  it('find / some / every with index', () => {
    expect(live.find((e) => e.get('value') === 7)!.begin()).toBe(7000);
    expect(live.some((e) => e.get('value') === 3)).toBe(true);
    expect(live.every((e) => (e.get('value') as number) < 100)).toBe(true);
  });
});

/* ── Snapshot ─────────────────────────────────────────────────────────────── */

describe('Step 7: snapshot on ring backing', () => {
  it('toTimeSeries snapshot matches buffer and is independent', () => {
    const live = makeLive();
    live.push([0, 1, 'a']);
    live.push([1000, 2, 'b']);
    const ts = live.toTimeSeries('snap');
    expect(ts.length).toBe(2);
    expect(ts.at(0)!.get('value')).toBe(1);
    expect(ts.at(1)!.get('host')).toBe('b');
    live.push([2000, 3, 'c']); // mutate after snapshot
    expect(ts.length).toBe(2); // snapshot unaffected
  });

  it('empty buffer snapshots to an empty series', () => {
    const live = makeLive();
    expect(live.toTimeSeries().length).toBe(0);
  });
});

/* ── Empty / edge ─────────────────────────────────────────────────────────── */

describe('Step 7: empty / edge behaviors on ring backing', () => {
  it('empty buffer point accessors', () => {
    const live = makeLive();
    expect(live.length).toBe(0);
    expect(live.at(0)).toBeUndefined();
    expect(live.first()).toBeUndefined();
    expect(live.last()).toBeUndefined();
  });

  it('negative index normalization', () => {
    const live = makeLive();
    live.push([0, 10, 'a']);
    live.push([1000, 20, 'a']);
    expect(live.at(-1)!.get('value')).toBe(20);
    expect(live.at(-2)!.get('value')).toBe(10);
    expect(live.at(-3)).toBeUndefined();
  });

  it('clear empties buffer and fires evict', () => {
    const live = makeLive();
    const evictSpy = vi.fn();
    live.on('evict', evictSpy);
    live.push([0, 1, 'a']);
    live.push([1000, 2, 'a']);
    live.clear();
    expect(live.length).toBe(0);
    expect(evictSpy).toHaveBeenCalledOnce();
    expect(evictSpy.mock.calls[0]![0].length).toBe(2);
  });

  it('pushMany([]) is a no-op', () => {
    const live = makeLive();
    const spy = vi.fn();
    live.on('event', spy);
    live.pushMany([]);
    expect(live.length).toBe(0);
    expect(spy).not.toHaveBeenCalled();
  });
});

/* ── LiveReduce FIFO eviction over a ring-backed source ───────────────────── */

describe('Step 7: LiveReduce eviction over ring-backed source', () => {
  it('removes evicted events from reducer state by FIFO position', () => {
    const live = makeLive({ retention: { maxEvents: 2 } });
    const r = live.reduce({ value: 'avg' });
    live.push([0, 10, 'a']);
    live.push([1000, 20, 'a']);
    expect(r.value().value).toBe(15);
    live.push([2000, 30, 'a']); // evicts value=10 → avg(20,30)=25
    expect(r.value().value).toBe(25);
    r.dispose();
  });
});
