/**
 * Cross-class `stats()` accessor tests. Pins the per-class field set
 * documented in PLAN.md "Queued: pipeline `stats()` accessor", and
 * verifies counters advance on every relevant event.
 *
 * Each describe block covers one accumulator/series class. The
 * harness style mirrors `live-buffer-as-window.test.ts` — small
 * helpers, explicit setup, post-microtask `flush()` for any
 * `LiveReduce` interaction.
 */
import { describe, expect, it } from 'vitest';
import {
  LiveAggregation,
  LiveSeries,
  Sequence,
  Trigger,
} from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

function makeLive(opts?: Partial<ConstructorParameters<typeof LiveSeries>[0]>) {
  return new LiveSeries({ name: 'test', schema, ...opts });
}

const flush = () => new Promise<void>((resolve) => queueMicrotask(resolve));

// ── LiveSeries ─────────────────────────────────────────────────

describe('LiveSeries.stats', () => {
  it('returns a record with the documented shape on a fresh source', () => {
    const live = makeLive();
    const s = live.stats();
    expect(s.ingested).toBe(0);
    expect(s.evicted).toBe(0);
    expect(s.rejected).toBe(0);
    expect(s.length).toBe(0);
    expect(s.earliestTs).toBeUndefined();
    expect(s.latestTs).toBeUndefined();
  });

  it('advances ingested + length, sets earliestTs/latestTs after push', () => {
    const live = makeLive();
    live.push([1000, 1, 'a']);
    live.push([2000, 2, 'a']);
    live.push([3000, 3, 'a']);
    const s = live.stats();
    expect(s.ingested).toBe(3);
    expect(s.evicted).toBe(0);
    expect(s.rejected).toBe(0);
    expect(s.length).toBe(3);
    expect(s.earliestTs).toBe(1000);
    expect(s.latestTs).toBe(3000);
  });

  it('counts evictions when retention removes events; ingested still grows', () => {
    const live = makeLive({ retention: { maxEvents: 2 } });
    live.push([1000, 1, 'a']);
    live.push([2000, 2, 'a']);
    live.push([3000, 3, 'a']);
    live.push([4000, 4, 'a']);
    const s = live.stats();
    expect(s.ingested).toBe(4);
    expect(s.evicted).toBe(2);
    expect(s.length).toBe(2);
    expect(s.earliestTs).toBe(3000);
    expect(s.latestTs).toBe(4000);
  });

  it('counts silent rejections under ordering: drop', () => {
    const live = makeLive({ ordering: 'drop' });
    live.push([2000, 2, 'a']);
    live.push([1000, 1, 'a']); // out-of-order: dropped
    live.push([3000, 3, 'a']);
    const s = live.stats();
    expect(s.ingested).toBe(2);
    expect(s.rejected).toBe(1);
    expect(s.length).toBe(2);
  });

  it('counters are cumulative — never decrease across pushes', () => {
    const live = makeLive({ retention: { maxEvents: 1 } });
    live.push([1000, 1, 'a']);
    expect(live.stats().ingested).toBe(1);
    live.push([2000, 2, 'a']);
    expect(live.stats().ingested).toBe(2);
    expect(live.stats().evicted).toBe(1);
    live.push([3000, 3, 'a']);
    expect(live.stats().ingested).toBe(3);
    expect(live.stats().evicted).toBe(2);
  });

  it('pushMany increments ingested by the batch size', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1, 'a'],
      [2000, 2, 'a'],
      [3000, 3, 'a'],
    ]);
    expect(live.stats().ingested).toBe(3);
  });
});

// ── LiveRollingAggregation ─────────────────────────────────────

describe('LiveRollingAggregation.stats', () => {
  it('returns the documented shape on a fresh source', () => {
    const live = makeLive();
    const rolling = live.rolling(3, { value: 'avg' });
    const s = rolling.stats();
    expect(s.eventsObserved).toBe(0);
    expect(s.evictions).toBe(0);
    expect(s.emissions).toBe(0);
    expect(s.windowSize).toBe(0);
  });

  it('eventsObserved advances per source event; emissions matches under Trigger.event', () => {
    const live = makeLive();
    const rolling = live.rolling(3, { value: 'avg' });
    live.push([1000, 1, 'a']);
    live.push([2000, 2, 'a']);
    live.push([3000, 3, 'a']);
    const s = rolling.stats();
    expect(s.eventsObserved).toBe(3);
    expect(s.emissions).toBe(3);
    expect(s.evictions).toBe(0);
    expect(s.windowSize).toBe(3);
  });

  it('evictions fire when count window slides past size', () => {
    const live = makeLive();
    const rolling = live.rolling(2, { value: 'avg' });
    live.push([1000, 1, 'a']);
    live.push([2000, 2, 'a']);
    live.push([3000, 3, 'a']); // evicts 1
    live.push([4000, 4, 'a']); // evicts 2
    const s = rolling.stats();
    expect(s.eventsObserved).toBe(4);
    expect(s.emissions).toBe(4);
    expect(s.evictions).toBe(2);
    expect(s.windowSize).toBe(2);
  });

  it('Trigger.count emits less often than eventsObserved', () => {
    const live = makeLive();
    const rolling = live.rolling(
      5,
      { value: 'avg' },
      { trigger: Trigger.count(3) },
    );
    for (let i = 1; i <= 10; i++) {
      live.push([i * 1000, i, 'a']);
    }
    const s = rolling.stats();
    expect(s.eventsObserved).toBe(10);
    expect(s.emissions).toBe(3); // floor(10 / 3)
  });
});

// ── LiveFusedRolling ───────────────────────────────────────────

describe('LiveFusedRolling.stats', () => {
  it('returns the documented shape with windowsCount on a fresh source', () => {
    const live = makeLive();
    const fused = live.rolling({
      '5s': { value_avg: { from: 'value', using: 'avg' } },
      '10s': { value_sum: { from: 'value', using: 'sum' } },
    });
    const s = fused.stats();
    expect(s.eventsObserved).toBe(0);
    expect(s.evictions).toBe(0);
    expect(s.emissions).toBe(0);
    expect(s.windowSize).toBe(0);
    expect(s.windowsCount).toBe(2);
  });

  it('eventsObserved + emissions + windowSize advance per source event', () => {
    const live = makeLive();
    const fused = live.rolling({
      '5s': { value: 'avg' },
    });
    live.push([1000, 1, 'a']);
    live.push([2000, 2, 'a']);
    live.push([3000, 3, 'a']);
    const s = fused.stats();
    expect(s.eventsObserved).toBe(3);
    expect(s.emissions).toBe(3);
    expect(s.windowSize).toBe(3);
    expect(s.windowsCount).toBe(1);
  });

  it('evictions count source events that fell out of every window', () => {
    const live = makeLive();
    const fused = live.rolling({
      '5s': { value: 'avg' },
    });
    // 10 events at 1s apart. With a 5s window, the 6th event (t=6000)
    // pushes the t=1000 event out. By t=10000 we should have 5 evictions.
    for (let i = 1; i <= 10; i++) {
      live.push([i * 1000, i, 'a']);
    }
    const s = fused.stats();
    expect(s.eventsObserved).toBe(10);
    expect(s.emissions).toBe(10);
    expect(s.evictions).toBeGreaterThan(0);
    // Live window should hold roughly 5-6 events (5s window at 1s spacing).
    expect(s.windowSize).toBeGreaterThan(0);
    expect(s.windowSize).toBeLessThanOrEqual(6);
  });
});

// ── LiveAggregation ────────────────────────────────────────────

describe('LiveAggregation.stats', () => {
  it('returns the documented shape on a fresh source', () => {
    const live = makeLive();
    const agg = live.aggregate(Sequence.every('1s'), { value: 'avg' });
    const s = agg.stats();
    expect(s.eventsObserved).toBe(0);
    expect(s.bucketsClosed).toBe(0);
    expect(s.openBuckets).toBe(0);
    expect(s.openBucketStart).toBeUndefined();
  });

  it('eventsObserved advances per ingested event; openBucketStart tracks earliest pending', () => {
    const live = makeLive();
    const agg = live.aggregate(Sequence.every('1s'), { value: 'avg' });
    live.push([1000, 1, 'a']);
    live.push([1500, 2, 'a']);
    const s = agg.stats();
    expect(s.eventsObserved).toBe(2);
    expect(s.bucketsClosed).toBe(0);
    expect(s.openBuckets).toBe(1);
    expect(s.openBucketStart).toBe(1000);
  });

  it('bucketsClosed advances and openBucketStart updates as watermark moves', () => {
    const live = makeLive();
    const agg = live.aggregate(Sequence.every('1s'), { value: 'avg' });
    live.push([1000, 1, 'a']); // bucket [1000, 2000)
    live.push([2500, 2, 'a']); // bucket [2000, 3000); closes [1000, 2000)
    const s = agg.stats();
    expect(s.eventsObserved).toBe(2);
    expect(s.bucketsClosed).toBe(1);
    expect(s.openBuckets).toBe(1);
    expect(s.openBucketStart).toBe(2000);
  });

  it('late events that fall outside the grace window do not increment eventsObserved', () => {
    // Construct LiveAggregation directly with an explicit grace
    // override, on top of a source that accepts late events. The
    // source accepts the stale push (reorder + 10s grace); the
    // aggregation drops it under the tighter 500ms grace before it
    // contributes to a bucket, so eventsObserved doesn't increment.
    // (LiveSeries.aggregate() doesn't currently accept options, so we
    // construct the LiveAggregation directly here.)
    const live = makeLive({ ordering: 'reorder', graceWindow: '10s' });
    const agg = new LiveAggregation(
      live,
      Sequence.every('1s'),
      { value: 'avg' },
      { grace: '500ms' },
    );
    live.push([1000, 1, 'a']); // bucket [1000, 2000)
    live.push([5000, 5, 'a']); // bucket [5000, 6000); closes [1000, 2000)
    expect(agg.stats().bucketsClosed).toBeGreaterThanOrEqual(1);
    const observedBefore = agg.stats().eventsObserved;
    live.push([1500, 999, 'a']);
    expect(agg.stats().eventsObserved).toBe(observedBefore);
  });
});

// ── LiveReduce ─────────────────────────────────────────────────

describe('LiveReduce.stats', () => {
  it('returns the documented shape on a fresh source', () => {
    const live = makeLive();
    const r = live.reduce({ value: 'avg' });
    const s = r.stats();
    expect(s.eventsObserved).toBe(0);
    expect(s.evictions).toBe(0);
    expect(s.emissions).toBe(0);
    expect(s.bufferSize).toBe(0);
  });

  it('eventsObserved + bufferSize advance per source push (after microtask)', async () => {
    const live = makeLive();
    const r = live.reduce({ value: 'avg' });
    // Synchronous pushes share one microtask flush — eventsObserved
    // counts every event but emissions collapses to one. Awaiting the
    // microtask between pushes is what splits them into separate
    // emissions (matches LiveReduce class JSDoc semantics).
    live.push([1000, 1, 'a']);
    await flush();
    live.push([2000, 2, 'a']);
    await flush();
    live.push([3000, 3, 'a']);
    await flush();
    const s = r.stats();
    expect(s.eventsObserved).toBe(3);
    expect(s.evictions).toBe(0);
    expect(s.bufferSize).toBe(3);
    expect(s.emissions).toBe(3);
  });

  it('synchronous pushes collapse to one deferred emission', async () => {
    const live = makeLive();
    const r = live.reduce({ value: 'count' });
    live.push([1000, 1, 'a']);
    live.push([2000, 2, 'a']);
    live.push([3000, 3, 'a']);
    await flush();
    expect(r.stats().eventsObserved).toBe(3);
    expect(r.stats().emissions).toBe(1);
  });

  it('emissions counts deferred microtask fires (one per pushMany under Trigger.event)', async () => {
    const live = makeLive();
    const r = live.reduce({ value: 'count' });
    live.pushMany([
      [1000, 1, 'a'],
      [2000, 2, 'a'],
      [3000, 3, 'a'],
    ]);
    await flush();
    expect(r.stats().eventsObserved).toBe(3);
    expect(r.stats().emissions).toBe(1); // one deferred emit per pushMany
  });

  it('evictions track source retention removes; bufferSize stays bounded', async () => {
    const live = makeLive({ retention: { maxEvents: 3 } });
    const r = live.reduce({ value: 'avg' });
    for (let i = 1; i <= 5; i++) {
      live.push([i * 1000, i, 'a']);
    }
    await flush();
    const s = r.stats();
    expect(s.eventsObserved).toBe(5);
    expect(s.evictions).toBe(2);
    expect(s.bufferSize).toBe(3);
  });
});

// ── LivePartitionedSeries ──────────────────────────────────────

describe('LivePartitionedSeries.stats', () => {
  it('returns the documented shape on a fresh partitioned view', () => {
    const live = makeLive();
    const partitioned = live.partitionBy('host');
    const s = partitioned.stats();
    expect(s.partitions).toBe(0);
    expect(s.eventsRouted).toBe(0);
  });

  it('partitions count is preseeded by declared groups', () => {
    const live = makeLive();
    const partitioned = live.partitionBy('host', { groups: ['a', 'b'] });
    expect(partitioned.stats().partitions).toBe(2);
    expect(partitioned.stats().eventsRouted).toBe(0);
  });

  it('eventsRouted advances per source push; partitions count grows on auto-spawn', () => {
    const live = makeLive();
    const partitioned = live.partitionBy('host');
    live.push([1000, 1, 'a']);
    live.push([2000, 2, 'a']);
    live.push([3000, 3, 'b']);
    const s = partitioned.stats();
    expect(s.partitions).toBe(2);
    expect(s.eventsRouted).toBe(3);
  });
});

// ── LivePartitionedSyncRolling ─────────────────────────────────

describe('LivePartitionedSyncRolling.stats', () => {
  it('returns the documented shape on a fresh sync rolling', () => {
    const live = makeLive();
    const sync = live
      .partitionBy('host')
      .rolling('5s', { value: 'avg' }, { trigger: Trigger.every('1s') });
    const s = sync.stats();
    expect(s.partitions).toBe(0);
    expect(s.eventsObserved).toBe(0);
    expect(s.emissions).toBe(0);
    expect(s.windowSize).toBe(0);
  });

  it('declared groups are pre-seeded as partitions', () => {
    const live = makeLive();
    const sync = live
      .partitionBy('host', { groups: ['a', 'b'] })
      .rolling('5s', { value: 'avg' }, { trigger: Trigger.every('1s') });
    expect(sync.stats().partitions).toBe(2);
  });

  it('eventsObserved advances per ingest; emissions = ticks × partitions', () => {
    const live = makeLive();
    const sync = live
      .partitionBy('host', { groups: ['a', 'b'] })
      .rolling('5s', { value: 'avg' }, { trigger: Trigger.every('1s') });
    // 4 events for 'a' at 1s spacing — 3 boundary crossings, each
    // fires 2 emits (one per known partition).
    live.push([1000, 1, 'a']); // first event, no emit
    live.push([2000, 2, 'a']); // boundary 1->2, fires 2 emits
    live.push([3000, 3, 'a']); // boundary 2->3, fires 2 emits
    live.push([4000, 4, 'b']); // boundary 3->4, fires 2 emits
    const s = sync.stats();
    expect(s.eventsObserved).toBe(4);
    expect(s.partitions).toBe(2);
    expect(s.emissions).toBe(6); // 3 ticks × 2 partitions
  });

  it('windowSize reports the max across partitions', () => {
    const live = makeLive();
    const sync = live
      .partitionBy('host')
      .rolling('5s', { value: 'avg' }, { trigger: Trigger.every('1s') });
    // Source is strict-ordered (default), so push events in time
    // order. After this sequence: partition 'a' has 4 events in its
    // 5s window, partition 'b' has 1.
    live.push([1000, 1, 'b']);
    live.push([2000, 1, 'a']);
    live.push([3000, 2, 'a']);
    live.push([4000, 3, 'a']);
    live.push([5000, 4, 'a']);
    expect(sync.stats().windowSize).toBe(4);
  });
});

// ── LivePartitionedFusedRolling ────────────────────────────────

describe('LivePartitionedFusedRolling.stats', () => {
  it('returns the documented shape with windowsCount', () => {
    const live = makeLive();
    const fused = live.partitionBy('host').rolling(
      {
        '5s': { value_avg: { from: 'value', using: 'avg' } },
        '10s': { value_sum: { from: 'value', using: 'sum' } },
      },
      { trigger: Trigger.every('1s') },
    );
    const s = fused.stats();
    expect(s.partitions).toBe(0);
    expect(s.eventsObserved).toBe(0);
    expect(s.emissions).toBe(0);
    expect(s.windowSize).toBe(0);
    expect(s.windowsCount).toBe(2);
  });

  it('counters advance across partitions and ticks', () => {
    const live = makeLive();
    const fused = live
      .partitionBy('host', { groups: ['a', 'b'] })
      .rolling({ '5s': { value: 'avg' } }, { trigger: Trigger.every('1s') });
    live.push([1000, 1, 'a']); // first event, no emit
    live.push([2000, 2, 'a']); // tick: 2 partitions emit
    live.push([3000, 3, 'b']); // tick: 2 partitions emit
    const s = fused.stats();
    expect(s.partitions).toBe(2);
    expect(s.eventsObserved).toBe(3);
    expect(s.emissions).toBe(4); // 2 ticks × 2 partitions
    expect(s.windowsCount).toBe(1);
  });
});

// ── Cross-class invariants ─────────────────────────────────────

describe('stats() cross-class invariants', () => {
  it('LiveSeries.stats values match what toTimeSeries would observe', () => {
    const live = makeLive();
    live.pushMany([
      [1000, 1, 'a'],
      [2000, 2, 'a'],
    ]);
    const s = live.stats();
    const ts = live.toTimeSeries();
    expect(s.length).toBe(ts.length);
    expect(s.earliestTs).toBe(ts.first()?.begin());
    expect(s.latestTs).toBe(ts.last()?.begin());
  });

  it('counters do not allocate per-event (smoke test on hot path)', () => {
    const live = makeLive();
    const rolling = live.rolling(100, { value: 'avg' });
    // Push enough events to surface allocation regressions if any
    // counter accidentally wraps in an object.
    for (let i = 0; i < 10_000; i++) {
      live.push([i, i, 'a']);
    }
    const s = rolling.stats();
    expect(s.eventsObserved).toBe(10_000);
    expect(s.emissions).toBe(10_000);
  });
});
