import { describe, expect, it } from 'vitest';
import { LiveSeries, TailReduce } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

function makeLive() {
  return new LiveSeries({ name: 'test', schema });
}

// ── Time-based window ───────────────────────────────────────────

describe('TailReduce time-based', () => {
  it('computes aggregate over time window', () => {
    const live = makeLive();
    live.push(
      [0, 10, 'a'],
      [1000, 20, 'a'],
      [2000, 30, 'a'],
      [3000, 40, 'a'],
      [4000, 50, 'a'],
    );
    const tail = new TailReduce(live, '5s', { value: 'avg' });
    expect(tail.value().value).toBe(30); // avg(10,20,30,40,50)
    tail.dispose();
  });

  it('evicts events outside the window', () => {
    const live = makeLive();
    live.push([0, 10, 'a'], [1000, 20, 'a'], [2000, 30, 'a']);
    const tail = new TailReduce(live, '3s', { value: 'sum' });
    // window covers [0, 2000], cutoff = 2000-3000 = -1000, all included
    expect(tail.value().value).toBe(60);

    live.push([5000, 40, 'a']);
    // cutoff = 5000-3000 = 2000, events at 0 and 1000 are evicted
    expect(tail.value().value).toBe(70); // 30+40
    tail.dispose();
  });

  it('updates on each push', () => {
    const live = makeLive();
    const tail = new TailReduce(live, '10s', { value: 'sum' });
    const updates: number[] = [];
    tail.on('update', (v) => updates.push(v.value as number));

    live.push([0, 10, 'a']);
    live.push([1000, 20, 'a']);
    expect(updates).toEqual([10, 30]);
    tail.dispose();
  });

  it('handles empty source', () => {
    const live = makeLive();
    const tail = new TailReduce(live, '5s', { value: 'avg' });
    expect(tail.value().value).toBeUndefined();
    expect(tail.windowSize).toBe(0);
    tail.dispose();
  });

  it('processes existing events', () => {
    const live = makeLive();
    live.push([0, 10, 'a'], [1000, 20, 'a']);
    const tail = new TailReduce(live, '5s', { value: 'sum' });
    expect(tail.value().value).toBe(30);
    tail.dispose();
  });

  it('keeps events exactly at boundary', () => {
    const live = makeLive();
    live.push([0, 10, 'a'], [5000, 20, 'a']);
    const tail = new TailReduce(live, '5s', { value: 'sum' });
    // cutoff = 5000-5000 = 0, event at 0 has timestamp === cutoff, NOT evicted
    expect(tail.value().value).toBe(30);
    tail.dispose();
  });
});

// ── Count-based window ──────────────────────────────────────────

describe('TailReduce count-based', () => {
  it('keeps last N events', () => {
    const live = makeLive();
    live.push(
      [0, 10, 'a'],
      [1000, 20, 'a'],
      [2000, 30, 'a'],
      [3000, 40, 'a'],
      [4000, 50, 'a'],
    );
    const tail = new TailReduce(live, 3, { value: 'avg' });
    expect(tail.windowSize).toBe(3);
    expect(tail.value().value).toBe(40); // avg(30,40,50)
    tail.dispose();
  });

  it('evicts oldest when window exceeds count', () => {
    const live = makeLive();
    const tail = new TailReduce(live, 2, { value: 'sum' });
    live.push([0, 10, 'a'], [1000, 20, 'a']);
    expect(tail.value().value).toBe(30);
    live.push([2000, 30, 'a']);
    expect(tail.value().value).toBe(50); // 20+30
    expect(tail.windowSize).toBe(2);
    tail.dispose();
  });

  it('count of 1 always has the latest event', () => {
    const live = makeLive();
    const tail = new TailReduce(live, 1, { value: 'sum' });
    live.push([0, 10, 'a']);
    expect(tail.value().value).toBe(10);
    live.push([1000, 20, 'a']);
    expect(tail.value().value).toBe(20);
    tail.dispose();
  });

  it('handles fewer events than window size', () => {
    const live = makeLive();
    const tail = new TailReduce(live, 100, { value: 'sum' });
    live.push([0, 10, 'a'], [1000, 20, 'a']);
    expect(tail.value().value).toBe(30);
    expect(tail.windowSize).toBe(2);
    tail.dispose();
  });
});

// ── Multiple columns ────────────────────────────────────────────

describe('multiple columns', () => {
  it('reduces multiple columns independently', () => {
    const numSchema = [
      { name: 'time', kind: 'time' },
      { name: 'a', kind: 'number' },
      { name: 'b', kind: 'number' },
    ] as const;
    const live = new LiveSeries({ name: 'multi', schema: numSchema });
    live.push([0, 10, 100], [1000, 20, 200], [2000, 30, 300]);
    const tail = new TailReduce(live, '5s', { a: 'avg', b: 'max' });
    expect(tail.value().a).toBe(20);
    expect(tail.value().b).toBe(300);
    tail.dispose();
  });
});

// ── Subscriptions ───────────────────────────────────────────────

describe('subscriptions', () => {
  it('unsubscribe stops updates', () => {
    const live = makeLive();
    const tail = new TailReduce(live, '5s', { value: 'sum' });
    let count = 0;
    const unsub = tail.on('update', () => count++);
    live.push([0, 10, 'a']);
    expect(count).toBe(1);
    unsub();
    live.push([1000, 20, 'a']);
    expect(count).toBe(1);
    tail.dispose();
  });

  it('dispose stops receiving source events', () => {
    const live = makeLive();
    const tail = new TailReduce(live, '5s', { value: 'sum' });
    live.push([0, 10, 'a']);
    expect(tail.value().value).toBe(10);
    tail.dispose();
    live.push([1000, 20, 'a']);
    // Value doesn't change after dispose
    expect(tail.value().value).toBe(10);
  });
});

// ── Edge cases ──────────────────────────────────────────────────

describe('edge cases', () => {
  it('rejects unknown column', () => {
    const live = makeLive();
    expect(
      () => new TailReduce(live, '5s', { nonexistent: 'sum' } as any),
    ).toThrow(/unknown column/);
  });

  it('many rapid pushes with count window', () => {
    const live = makeLive();
    const tail = new TailReduce(live, 10, { value: 'sum' });
    for (let i = 0; i < 1000; i++) {
      live.push([i * 1000, i, `h${i % 5}`]);
    }
    // Last 10 events: 990..999, sum = 990+991+...+999 = 9945
    expect(tail.value().value).toBe(9945);
    expect(tail.windowSize).toBe(10);
    tail.dispose();
  });

  it('single event in window', () => {
    const live = makeLive();
    live.push([0, 42, 'a']);
    const tail = new TailReduce(live, '5s', { value: 'avg' });
    expect(tail.value().value).toBe(42);
    tail.dispose();
  });

  it('works with min reducer', () => {
    const live = makeLive();
    live.push([0, 30, 'a'], [1000, 10, 'a'], [2000, 50, 'a']);
    const tail = new TailReduce(live, 2, { value: 'min' });
    expect(tail.value().value).toBe(10); // min(10, 50)
    live.push([3000, 5, 'a']);
    expect(tail.value().value).toBe(5); // min(50, 5)
    tail.dispose();
  });
});
