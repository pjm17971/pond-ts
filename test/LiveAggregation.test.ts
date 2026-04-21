import { describe, expect, it, vi } from 'vitest';
import { LiveAggregation, LiveSeries, Sequence } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

function makeLive() {
  return new LiveSeries({ name: 'test', schema });
}

// ── Basic bucketing ─────────────────────────────────────────────

describe('LiveAggregation bucketing', () => {
  it('aggregates events into fixed buckets', () => {
    const live = makeLive();
    live.push(
      [0, 10, 'a'],
      [1000, 20, 'a'],
      [2000, 30, 'a'],
      [3000, 40, 'a'],
      [4000, 50, 'a'],
    );
    const agg = new LiveAggregation(live, Sequence.every('5s'), {
      value: 'avg',
    });
    // All in one open bucket [0, 5000)
    expect(agg.closedCount).toBe(0);
    expect(agg.hasOpenBucket).toBe(true);
    const snap = agg.snapshot();
    expect(snap.length).toBe(1);
    expect(snap.at(0)?.get('value')).toBe(30);
    agg.dispose();
  });

  it('closes bucket when event crosses boundary', () => {
    const live = makeLive();
    live.push([0, 10, 'a'], [2000, 20, 'a'], [4000, 30, 'a']);
    const agg = new LiveAggregation(live, Sequence.every('5s'), {
      value: 'sum',
    });
    expect(agg.closedCount).toBe(0);

    live.push([5000, 40, 'a']);
    expect(agg.closedCount).toBe(1);
    const closed = agg.closed();
    expect(closed.length).toBe(1);
    expect(closed.at(0)?.get('value')).toBe(60); // 10+20+30
    agg.dispose();
  });

  it('produces multiple closed buckets', () => {
    const live = makeLive();
    const agg = new LiveAggregation(live, Sequence.every('5s'), {
      value: 'sum',
    });

    // bucket [0, 5000): 10+20=30
    live.push([0, 10, 'a'], [2000, 20, 'a']);
    // bucket [5000, 10000): 30+40=70, closes [0,5000)
    live.push([5000, 30, 'a'], [7000, 40, 'a']);
    // bucket [10000, 15000): 50, closes [5000,10000)
    live.push([10000, 50, 'a']);

    expect(agg.closedCount).toBe(2);
    const closed = agg.closed();
    expect(closed.at(0)?.get('value')).toBe(30);
    expect(closed.at(1)?.get('value')).toBe(70);

    const snap = agg.snapshot();
    expect(snap.length).toBe(3);
    expect(snap.at(2)?.get('value')).toBe(50);
    agg.dispose();
  });

  it('snapshot includes open bucket', () => {
    const live = makeLive();
    live.push([0, 10, 'a']);
    const agg = new LiveAggregation(live, Sequence.every('5s'), {
      value: 'avg',
    });
    const snap = agg.snapshot();
    expect(snap.length).toBe(1);
    expect(snap.at(0)?.get('value')).toBe(10);

    const closed = agg.closed();
    expect(closed.length).toBe(0);
    agg.dispose();
  });

  it('empty source produces empty aggregation', () => {
    const live = makeLive();
    const agg = new LiveAggregation(live, Sequence.every('5s'), {
      value: 'sum',
    });
    expect(agg.closedCount).toBe(0);
    expect(agg.hasOpenBucket).toBe(false);
    expect(agg.snapshot().length).toBe(0);
    expect(agg.closed().length).toBe(0);
    agg.dispose();
  });
});

// ── Multiple reducers ───────────────────────────────────────────

describe('multiple reducers', () => {
  it('aggregates multiple columns', () => {
    const live = makeLive();
    live.push([0, 10, 'a'], [1000, 20, 'a'], [5000, 30, 'b']);
    const agg = new LiveAggregation(live, Sequence.every('5s'), {
      value: 'avg',
      host: 'keep',
    });
    const snap = agg.snapshot();
    expect(snap.length).toBe(2);
    expect(snap.at(0)?.get('value')).toBe(15);
    expect(snap.at(0)?.get('host')).toBe('a');
    expect(snap.at(1)?.get('value')).toBe(30);
    agg.dispose();
  });

  it('supports percentile reducers', () => {
    const live = makeLive();
    live.push(
      [0, 10, 'a'],
      [1000, 20, 'a'],
      [2000, 30, 'a'],
      [3000, 40, 'a'],
      [4000, 50, 'a'],
    );
    const agg = new LiveAggregation(live, Sequence.every('10s'), {
      value: 'p50',
    });
    const snap = agg.snapshot();
    expect(snap.at(0)?.get('value')).toBe(30);
    agg.dispose();
  });
});

// ── Subscriptions ───────────────────────────────────────────────

describe('subscriptions', () => {
  it('close fires when a bucket finalizes', () => {
    const live = makeLive();
    const agg = new LiveAggregation(live, Sequence.every('5s'), {
      value: 'sum',
    });
    const closed: number[] = [];
    agg.on('close', (event) => {
      closed.push(event.get('value') as number);
    });

    live.push([0, 10, 'a'], [2000, 20, 'a']);
    expect(closed).toEqual([]);

    live.push([5000, 30, 'a']);
    expect(closed).toEqual([30]); // 10+20

    agg.dispose();
  });

  it('update fires on every source event', () => {
    const live = makeLive();
    const agg = new LiveAggregation(live, Sequence.every('5s'), {
      value: 'sum',
    });
    let updateCount = 0;
    agg.on('update', () => updateCount++);

    live.push([0, 10, 'a'], [1000, 20, 'a']);
    expect(updateCount).toBe(2);
    agg.dispose();
  });

  it('unsubscribe stops listener', () => {
    const live = makeLive();
    const agg = new LiveAggregation(live, Sequence.every('5s'), {
      value: 'sum',
    });
    let count = 0;
    const unsub = agg.on('update', () => count++);
    live.push([0, 10, 'a']);
    expect(count).toBe(1);
    unsub();
    live.push([1000, 20, 'a']);
    expect(count).toBe(1);
    agg.dispose();
  });

  it('dispose stops receiving source events', () => {
    const live = makeLive();
    const agg = new LiveAggregation(live, Sequence.every('5s'), {
      value: 'sum',
    });
    let count = 0;
    agg.on('update', () => count++);
    live.push([0, 10, 'a']);
    expect(count).toBe(1);
    agg.dispose();
    live.push([1000, 20, 'a']);
    expect(count).toBe(1);
  });
});

// ── Interval keys ───────────────────────────────────────────────

describe('output series', () => {
  it('closed buckets have interval keys', () => {
    const live = makeLive();
    live.push([0, 10, 'a'], [5000, 20, 'a']);
    const agg = new LiveAggregation(live, Sequence.every('5s'), {
      value: 'sum',
    });
    const closed = agg.closed();
    expect(closed.at(0)?.begin()).toBe(0);
    expect(closed.at(0)?.end()).toBe(5000);
    agg.dispose();
  });

  it('snapshot is a valid TimeSeries for further operations', () => {
    const live = makeLive();
    live.push([0, 10, 'a'], [1000, 20, 'a'], [5000, 30, 'b'], [6000, 40, 'b']);
    const agg = new LiveAggregation(live, Sequence.every('5s'), {
      value: 'sum',
    });
    // Push one more to close the first two buckets
    live.push([10000, 50, 'c']);
    const snap = agg.snapshot();
    expect(snap.length).toBe(3);
    // Can call TimeSeries methods on the snapshot
    expect(snap.reduce('value', 'sum')).toBe(10 + 20 + 30 + 40 + 50);
    agg.dispose();
  });
});

// ── Processes existing events ───────────────────────────────────

describe('existing events', () => {
  it('processes events already in the source', () => {
    const live = makeLive();
    live.push([0, 10, 'a'], [2000, 20, 'a'], [5000, 30, 'a']);

    const agg = new LiveAggregation(live, Sequence.every('5s'), {
      value: 'sum',
    });
    // [0, 5000) was closed by the event at 5000
    expect(agg.closedCount).toBe(1);
    expect(agg.closed().at(0)?.get('value')).toBe(30);
    expect(agg.snapshot().at(1)?.get('value')).toBe(30);
    agg.dispose();
  });
});

// ── Edge cases ──────────────────────────────────────────────────

describe('edge cases', () => {
  it('gap between buckets creates separate closed entries', () => {
    const live = makeLive();
    live.push([0, 10, 'a']);
    const agg = new LiveAggregation(live, Sequence.every('5s'), {
      value: 'sum',
    });
    // Skip directly to bucket [15000, 20000)
    live.push([15000, 20, 'a']);
    // The [0, 5000) bucket closed
    expect(agg.closedCount).toBe(1);
    expect(agg.closed().at(0)?.get('value')).toBe(10);
    agg.dispose();
  });

  it('rejects unknown column', () => {
    const live = makeLive();
    expect(
      () =>
        new LiveAggregation(live, Sequence.every('5s'), {
          nonexistent: 'sum',
        } as any),
    ).toThrow(/unknown column/);
  });

  it('works with 1-second buckets', () => {
    const live = makeLive();
    const agg = new LiveAggregation(live, Sequence.every('1s'), {
      value: 'sum',
    });
    live.push([0, 1, 'a'], [500, 2, 'a'], [1000, 3, 'a'], [1500, 4, 'a']);
    expect(agg.closedCount).toBe(1);
    expect(agg.closed().at(0)?.get('value')).toBe(3); // 1+2
    agg.dispose();
  });
});
