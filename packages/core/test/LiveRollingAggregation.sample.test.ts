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

describe('rolling.sample(sequence) — basic emission', () => {
  it('does not emit before first boundary is crossed', () => {
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'avg' });
    const sample = rolling.sample(Sequence.every('30s'));

    live.push([0, 10], [5_000, 20], [15_000, 30]);
    expect(sample.length).toBe(0);

    sample.dispose();
    rolling.dispose();
  });

  it('emits once when a boundary is crossed', () => {
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'avg' });
    const sample = rolling.sample(Sequence.every('30s'));

    live.push([0, 10], [15_000, 20], [30_001, 30]); // last event crosses 30 s mark
    expect(sample.length).toBe(1);

    sample.dispose();
    rolling.dispose();
  });

  it('emits at the epoch-aligned boundary timestamp', () => {
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'avg' });
    const sample = rolling.sample(Sequence.every('30s'));

    live.push([0, 10], [30_001, 20]);
    expect(sample.at(0)!.begin()).toBe(30_000);

    sample.dispose();
    rolling.dispose();
  });

  it('emits once when a single event crosses multiple boundaries', () => {
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'avg' });
    const sample = rolling.sample(Sequence.every('30s'));

    live.push([0, 10]);
    live.push([90_001, 20]);
    expect(sample.length).toBe(1);
    expect(sample.at(0)!.begin()).toBe(90_000);

    sample.dispose();
    rolling.dispose();
  });

  it('emits two events when two boundaries are crossed', () => {
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'avg' });
    const sample = rolling.sample(Sequence.every('30s'));

    live.push([0, 10]);
    live.push([30_001, 20]);
    live.push([60_001, 30]);
    expect(sample.length).toBe(2);
    expect(sample.at(0)!.begin()).toBe(30_000);
    expect(sample.at(1)!.begin()).toBe(60_000);

    sample.dispose();
    rolling.dispose();
  });
});

// ── Rolling window content ──────────────────────────────────────────

describe('rolling.sample(sequence) — rolling window content', () => {
  it('emitted value reflects trailing rolling window', () => {
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'avg' });
    const sample = rolling.sample(Sequence.every('30s'));

    live.push([0, 10], [10_000, 20], [20_000, 30]);
    live.push([30_001, 40]); // crosses 30 s

    const emitted = sample.at(0)!;
    expect(emitted.get('value')).toBeCloseTo((10 + 20 + 30 + 40) / 4, 5);

    sample.dispose();
    rolling.dispose();
  });

  it('respects minSamples — emits undefined while window is cold', () => {
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'avg' }, { minSamples: 10 });
    const sample = rolling.sample(Sequence.every('30s'));

    live.push([0, 100], [10_000, 200], [30_001, 300]);
    expect(sample.at(0)!.get('value')).toBeUndefined();

    sample.dispose();
    rolling.dispose();
  });

  it('eviction affects emitted value', () => {
    const live = makeLive();
    const rolling = live.rolling('30s', { value: 'avg' });
    const sample = rolling.sample(Sequence.every('30s'));

    live.push([0, 10], [15_000, 20]);
    // After [40_001, 30]: cutoff = 40001 - 30000 = 10001;
    // 0 evicted, 15000 kept, 40001 kept. avg(20, 30) = 25.
    live.push([40_001, 30]);
    expect(sample.length).toBe(1);
    expect(sample.at(0)!.get('value')).toBeCloseTo(25, 5);

    sample.dispose();
    rolling.dispose();
  });
});

// ── Event subscription ─────────────────────────────────────────────

describe('rolling.sample(sequence) — on(event)', () => {
  it('fires the event listener on boundary crossing', () => {
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'sum' });
    const sample = rolling.sample(Sequence.every('30s'));

    const received: number[] = [];
    sample.on('event', (e) => received.push(e.begin()));

    live.push([0, 1], [30_001, 2]);
    expect(received).toEqual([30_000]);

    live.push([60_001, 3]);
    expect(received).toEqual([30_000, 60_000]);

    sample.dispose();
    rolling.dispose();
  });

  it('unsubscribe stops receiving events', () => {
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'sum' });
    const sample = rolling.sample(Sequence.every('30s'));

    const spy = vi.fn();
    const unsub = sample.on('event', spy);

    live.push([0, 1], [30_001, 2]);
    expect(spy).toHaveBeenCalledTimes(1);

    unsub();
    live.push([60_001, 3]);
    expect(spy).toHaveBeenCalledTimes(1);

    sample.dispose();
    rolling.dispose();
  });
});

// ── LiveSource contract ────────────────────────────────────────────

describe('rolling.sample(sequence) — LiveSource contract', () => {
  it('exposes name and schema from the rolling source', () => {
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'avg' });
    const sample = rolling.sample(Sequence.every('30s'));

    expect(sample.name).toBe('test');
    expect(sample.schema).toBe(rolling.schema);

    sample.dispose();
    rolling.dispose();
  });

  it('at() with negative index reads from the end', () => {
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'sum' });
    const sample = rolling.sample(Sequence.every('30s'));

    live.push([0, 1], [30_001, 2], [60_001, 3]);
    expect(sample.length).toBe(2);
    expect(sample.at(-1)!.begin()).toBe(60_000);
    expect(sample.at(0)!.begin()).toBe(30_000);

    sample.dispose();
    rolling.dispose();
  });

  it('returns instanceof LiveSequenceRollingAggregation', () => {
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'avg' });
    const sample = rolling.sample(Sequence.every('30s'));

    expect(sample).toBeInstanceOf(LiveSequenceRollingAggregation);

    sample.dispose();
    rolling.dispose();
  });
});

// ── Sequence anchor ────────────────────────────────────────────────

describe('rolling.sample(sequence) — Sequence anchor', () => {
  it('respects a non-zero anchor', () => {
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'avg' });
    const sample = rolling.sample(Sequence.every('30s', { anchor: 5_000 }));

    live.push([5_000, 10], [35_001, 20]); // crosses 35 000 boundary
    expect(sample.length).toBe(1);
    expect(sample.at(0)!.begin()).toBe(35_000);

    sample.dispose();
    rolling.dispose();
  });
});

// ── View transforms ────────────────────────────────────────────────

describe('rolling.sample(sequence) — chaining', () => {
  it('filter() returns a LiveView', () => {
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'avg' });
    const sample = rolling.sample(Sequence.every('30s'));
    const filtered = sample.filter(() => true);
    expect(filtered).toBeInstanceOf(LiveView);

    sample.dispose();
    rolling.dispose();
  });

  it('map() returns a LiveView and forwards events', () => {
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'avg' });
    const sample = rolling.sample(Sequence.every('30s'));
    const mapped = sample.map((e) => e);
    expect(mapped).toBeInstanceOf(LiveView);

    const received: number[] = [];
    mapped.on('event', (e) => received.push(e.begin()));

    live.push([0, 10], [30_001, 20]);
    expect(received).toEqual([30_000]);

    sample.dispose();
    rolling.dispose();
  });

  it('select() narrows the schema', () => {
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'avg' });
    const sample = rolling.sample(Sequence.every('30s'));
    const selected = sample.select('value');
    expect(selected).toBeInstanceOf(LiveView);
    expect(selected.schema.length).toBe(2); // time + value

    sample.dispose();
    rolling.dispose();
  });
});

// ── Rejection of calendar sequences ─────────────────────────────────

describe('rolling.sample(sequence) — calendar sequence rejection', () => {
  it('throws a sampler-aware error for calendar sequences', () => {
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'avg' });
    // Calendar sequences (e.g. `Sequence.calendar('day')`) have no
    // constant millisecond step; .sample() should reject them upfront
    // with a sampler-specific error rather than the generic
    // 'calendar sequences do not have a fixed millisecond step size'
    // from sequence.stepMs() invoked deeper in the constructor.
    expect(() => rolling.sample(Sequence.calendar('day'))).toThrowError(
      /rolling\.sample.*fixed-step Sequence/,
    );
    rolling.dispose();
  });
});

// ── Out-of-order events ─────────────────────────────────────────────

describe('rolling.sample(sequence) — out-of-order events', () => {
  it('does not emit on events that go backward in bucket index', () => {
    // With ordering: 'reorder', a late event can land before the latest.
    // The rolling will emit one event for the late insertion; the
    // sampler should ignore it (no boundary advanced).
    const live = new LiveSeries({
      name: 'ooo',
      schema,
      ordering: 'reorder',
      graceWindow: '5m',
    });
    const rolling = live.rolling('1m', { value: 'avg' });
    const sample = rolling.sample(Sequence.every('30s'));

    live.push([0, 10]);
    live.push([60_001, 20]); // crosses 30s, 60s — emits 1 at boundary 60_000
    expect(sample.length).toBe(1);
    expect(sample.at(0)!.begin()).toBe(60_000);

    // Late event landing in bucket 0 (out of order) — bucket index
    // (0) < lastBucketIdx (2), so no emission.
    live.push([5_000, 15]);
    expect(sample.length).toBe(1);

    sample.dispose();
    rolling.dispose();
  });
});

// ── Independent lifetimes ──────────────────────────────────────────

describe('rolling.sample(sequence) — dispose independence', () => {
  it('sample.dispose() does not stop the upstream rolling', () => {
    // The rolling and the sample have independent lifetimes — disposing
    // the sample must NOT detach the rolling from the source. This lets
    // one rolling drive multiple downstream consumers (e.g. one
    // .sample('30s') for backend reporting plus direct rolling.value()
    // reads for an in-app display) without coupling.
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'avg' });
    const sample = rolling.sample(Sequence.every('30s'));

    live.push([0, 10]);
    sample.dispose();
    live.push([30_001, 20]);

    // Rolling continued ingesting after sample disposed
    expect(rolling.value().value).toBeCloseTo(15, 5);

    rolling.dispose();
  });

  it('rolling.dispose() ends the sample silently (no further emissions)', () => {
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'sum' });
    const sample = rolling.sample(Sequence.every('30s'));

    const spy = vi.fn();
    sample.on('event', spy);

    live.push([0, 1], [30_001, 2]);
    expect(spy).toHaveBeenCalledTimes(1);

    rolling.dispose();
    live.push([60_001, 3]);
    // Rolling no longer ingests, so no boundary event reaches the sample
    expect(spy).toHaveBeenCalledTimes(1);

    sample.dispose();
  });

  it('sample.dispose() does not leak source listeners through the rolling', () => {
    // Sanity: confirm the sample only subscribes to the rolling, not to
    // the source directly. Disposing the sample should leave exactly one
    // source listener (the rolling itself).
    const live = makeLive();
    const realOn = live.on.bind(live);
    let activeSourceSubs = 0;
    live.on = ((type: any, fn: any) => {
      const unsub = realOn(type, fn);
      activeSourceSubs++;
      return (() => {
        activeSourceSubs--;
        if (typeof unsub === 'function') return unsub();
      }) as any;
    }) as any;

    const rolling = live.rolling('1m', { value: 'avg' });
    expect(activeSourceSubs).toBe(1); // rolling subscribed

    const sample = rolling.sample(Sequence.every('30s'));
    expect(activeSourceSubs).toBe(1); // sample subscribed to rolling, not source

    sample.dispose();
    expect(activeSourceSubs).toBe(1); // rolling still subscribed

    rolling.dispose();
    expect(activeSourceSubs).toBe(0); // now everything is detached
  });
});
