import { describe, expect, it } from 'vitest';
import {
  LivePartitionedSeries,
  LivePartitionedView,
  LiveSeries,
} from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number', required: false },
  { name: 'host', kind: 'string', required: false },
] as const;

function makeLive() {
  return new LiveSeries({ name: 'metrics', schema });
}

describe('LivePartitionedSeries chainable typed sugar', () => {
  describe('shape', () => {
    it('fill() returns a LivePartitionedView, not a LiveSeries', () => {
      const live = makeLive();
      const view = live.partitionBy('host').fill({ cpu: 'hold' });
      expect(view).toBeInstanceOf(LivePartitionedView);
    });

    it('chain step exposes schema captured from factory output', () => {
      const live = makeLive();
      // fill preserves the input schema
      const filled = live.partitionBy('host').fill({ cpu: 'hold' });
      expect(filled.schema).toEqual(schema);
    });

    it('rolling() narrows schema to the rolling output', () => {
      const live = makeLive();
      const rolled = live.partitionBy('host').rolling(10, { cpu: 'avg' });
      // Output schema has cpu as number (avg result)
      const cpuCol = rolled.schema.find((c) => c.name === 'cpu');
      expect(cpuCol?.kind).toBe('number');
    });
  });

  describe('per-partition correctness via chained sugar', () => {
    it('fill().collect() applies fill per partition (no cross-host hold)', () => {
      const live = makeLive();
      const collected = live
        .partitionBy('host')
        .fill({ cpu: 'hold' })
        .collect();

      live.push([0, 0.5, 'a']);
      live.push([0, 0.3, 'b']);
      live.push([60_000, undefined, 'a']);
      live.push([60_000, undefined, 'b']);

      expect(collected.length).toBe(4);
      const events = [...collected.toTimeSeries().events];
      const aMid = events.find(
        (e) => e.begin() === 60_000 && e.get('host') === 'a',
      );
      const bMid = events.find(
        (e) => e.begin() === 60_000 && e.get('host') === 'b',
      );
      // Each host's hold-fill picks its own previous value, not the
      // other host's.
      expect(aMid?.get('cpu')).toBe(0.5);
      expect(bMid?.get('cpu')).toBe(0.3);
    });

    it('fill().rolling().collect() chains multiple operators per partition', () => {
      // Note: rolling's output schema only retains columns named in
      // the mapping — to keep the partition column visible in the
      // unified buffer, include it explicitly with `'last'` (or
      // similar). This is per-LiveSeries.rolling semantics, not
      // partition-specific.
      const live = makeLive();
      const collected = live
        .partitionBy('host')
        .fill({ cpu: 'hold' })
        .rolling(2, { cpu: 'avg', host: 'last' })
        .collect();

      // Push events globally ordered by time.
      live.push([0, 0.4, 'a']);
      live.push([0, 1.0, 'b']);
      live.push([1, 0.6, 'a']); // a's window: [0.4, 0.6] avg=0.5
      live.push([1, 1.2, 'b']); // b's window: [1.0, 1.2] avg=1.1
      live.push([2, 0.8, 'a']); // a's window: [0.6, 0.8] avg=0.7
      live.push([2, 1.4, 'b']); // b's window: [1.2, 1.4] avg=1.3

      // Per-host rolling avg fires after the window has 2 events.
      const events = [...collected.toTimeSeries().events];
      const aLast = events.find(
        (e) => e.begin() === 2 && e.get('host') === 'a',
      );
      const bLast = events.find(
        (e) => e.begin() === 2 && e.get('host') === 'b',
      );
      expect(aLast?.get('cpu')).toBeCloseTo(0.7, 5);
      expect(bLast?.get('cpu')).toBeCloseTo(1.3, 5);
    });

    it('diff() chained returns per-partition deltas', () => {
      const live = makeLive();
      const collected = live.partitionBy('host').diff('cpu').collect();

      live.push([0, 0.5, 'a']);
      live.push([0, 1.0, 'b']);
      live.push([60_000, 0.7, 'a']);
      live.push([60_000, 1.3, 'b']);

      const events = [...collected.toTimeSeries().events];
      const aMid = events.find(
        (e) => e.begin() === 60_000 && e.get('host') === 'a',
      );
      const bMid = events.find(
        (e) => e.begin() === 60_000 && e.get('host') === 'b',
      );
      // Per-partition diff: each host's delta uses its own prev event.
      expect(aMid?.get('cpu')).toBeCloseTo(0.2, 5);
      expect(bMid?.get('cpu')).toBeCloseTo(0.3, 5);
    });
  });

  describe('chain composition equivalence', () => {
    it('chained sugar produces the same result as apply with the same factory', () => {
      const live1 = makeLive();
      const sugar = live1
        .partitionBy('host')
        .fill({ cpu: 'hold' })
        .rolling(2, { cpu: 'avg', host: 'last' })
        .collect();

      const live2 = makeLive();
      const apply = live2
        .partitionBy('host')
        .apply((sub) =>
          sub.fill({ cpu: 'hold' }).rolling(2, { cpu: 'avg', host: 'last' }),
        );

      const seq: Array<readonly [number, number | undefined, string]> = [
        [0, 0.4, 'a'],
        [0, 1.0, 'b'],
        [1, 0.6, 'a'],
        [1, 1.2, 'b'],
        [2, 0.8, 'a'],
        [2, 1.4, 'b'],
      ];
      for (const r of seq) live1.push(r as never);
      for (const r of seq) live2.push(r as never);

      const sugarEvents = [...sugar.toTimeSeries().events]
        .map((e) => [e.begin(), e.get('cpu'), e.get('host')])
        .sort();
      const applyEvents = [...apply.toTimeSeries().events]
        .map((e) => [e.begin(), e.get('cpu'), e.get('host')])
        .sort();

      expect(sugarEvents).toEqual(applyEvents);
    });
  });

  describe('LivePartitionedView.apply', () => {
    it('apply on a chained view composes with the existing factory', () => {
      const live = makeLive();
      // fill().apply(custom) — apply adds another transform on top
      const collected = live
        .partitionBy('host')
        .fill({ cpu: 'hold' })
        .apply((sub) => sub); // identity transform

      live.push([0, 0.5, 'a']);
      live.push([60_000, undefined, 'a']);

      expect(collected.length).toBe(2);
    });
  });

  describe('LivePartitionedView.toMap', () => {
    it('snapshots per-partition factory outputs as a Map', () => {
      const live = makeLive();
      const view = live.partitionBy('host').fill({ cpu: 'hold' });

      // Globally ordered by time
      live.push([0, 0.5, 'a']);
      live.push([0, 0.3, 'b']);
      live.push([60_000, undefined, 'a']);

      const m = view.toMap();
      expect(m.size).toBe(2);
      // Each entry is a LiveSource (specifically a LiveView wrapping
      // the partition's LiveSeries through the fill factory).
      expect(m.get('a')).toBeDefined();
      expect(m.get('b')).toBeDefined();
    });
  });

  describe('auto-spawn through chains', () => {
    it('new partitions appearing mid-stream propagate through the chain', () => {
      const live = makeLive();
      const collected = live
        .partitionBy('host')
        .fill({ cpu: 'hold' })
        .collect();

      // Only 'a' at first
      live.push([0, 0.5, 'a']);
      live.push([60_000, undefined, 'a']);
      // 'b' shows up later
      live.push([60_000, 0.3, 'b']);
      live.push([120_000, undefined, 'b']);

      expect(collected.length).toBe(4);
      const events = [...collected.toTimeSeries().events];
      const bMid = events.find(
        (e) => e.begin() === 120_000 && e.get('host') === 'b',
      );
      // 'b' got its own fill chain — undefined held against its own 0.3.
      expect(bMid?.get('cpu')).toBe(0.3);
    });
  });

  describe('typed groups propagate through chain', () => {
    it('partitionBy(col, { groups }).fill() narrows toMap key type at runtime', () => {
      const live = makeLive();
      const HOSTS = ['a', 'b'] as const;
      const view = live
        .partitionBy('host', { groups: HOSTS })
        .fill({ cpu: 'hold' });

      // Eagerly spawned + factory applied to each
      const m = view.toMap();
      expect(m.size).toBe(2);
      expect(m.has('a')).toBe(true);
      expect(m.has('b')).toBe(true);
    });
  });

  describe('dispose propagates through chained terminals', () => {
    it('disposing the leaf disconnects collected output of a chain', () => {
      const live = makeLive();
      const partitioned = live.partitionBy('host');
      const collected = partitioned.fill({ cpu: 'hold' }).collect();

      live.push([0, 0.5, 'a']);
      expect(collected.length).toBe(1);

      partitioned.dispose();
      live.push([60_000, 0.6, 'a']);
      expect(collected.length).toBe(1); // unchanged
    });
  });

  describe('LivePartitionedSeries also supports apply() (PR 1 path)', () => {
    it('apply() factory on the leaf still produces a LiveSeries (no chain)', () => {
      // Sanity: typed sugar is additive; apply() still works.
      const live = makeLive();
      const collected = live
        .partitionBy('host')
        .apply((sub) => sub.fill({ cpu: 'hold' }));

      live.push([0, 0.5, 'a']);
      live.push([60_000, undefined, 'a']);

      expect(collected.length).toBe(2);
      expect(collected).toBeInstanceOf(LiveSeries);
      // The result is a LiveSeries — not a LivePartitionedView.
      expect((collected as object) instanceof LivePartitionedView).toBe(false);
    });
  });
});
