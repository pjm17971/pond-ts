import { describe, expect, it } from 'vitest';
import { LivePartitionedSeries, LiveSeries } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number', required: false },
  { name: 'host', kind: 'string', required: false },
] as const;

function makeLive() {
  return new LiveSeries({ name: 'metrics', schema });
}

describe('LivePartitionedSeries', () => {
  describe('routing', () => {
    it('routes events to per-partition sub-buffers', () => {
      const live = makeLive();
      const partitioned = live.partitionBy('host');

      // Source ordering 'strict' — push globally ordered by time;
      // partition view routes each to its host bucket.
      live.push([0, 0.5, 'a']);
      live.push([0, 0.3, 'b']);
      live.push([60_000, 0.6, 'a']);
      live.push([60_000, 0.4, 'b']);

      const m = partitioned.toMap();
      expect(m.size).toBe(2);
      expect(m.get('a')?.length).toBe(2);
      expect(m.get('b')?.length).toBe(2);
    });

    it('replays existing source events into partitions on construction', () => {
      const live = makeLive();
      live.push([0, 0.5, 'a']);
      live.push([60_000, 0.6, 'a']);

      // Construct partitioned view AFTER events were pushed
      const partitioned = live.partitionBy('host');
      const m = partitioned.toMap();
      expect(m.get('a')?.length).toBe(2);
    });

    it('auto-spawns a new partition the first time a value is seen', () => {
      const live = makeLive();
      const partitioned = live.partitionBy('host');

      expect(partitioned.toMap().size).toBe(0);
      live.push([0, 0.5, 'a']);
      expect(partitioned.toMap().size).toBe(1);
      live.push([60_000, 0.4, 'b']);
      expect(partitioned.toMap().size).toBe(2);
    });

    it('treats undefined partition values via the leading-space sentinel', () => {
      const live = makeLive();
      const partitioned = live.partitionBy('host');

      live.push([0, 0.5, undefined]); // missing host
      live.push([60_000, 0.6, 'a']);

      const m = partitioned.toMap();
      expect(m.size).toBe(2);
      expect(m.has(' undefined')).toBe(true);
      expect(m.has('a')).toBe(true);
    });
  });

  describe('declared groups', () => {
    it('eagerly spawns declared groups even before events arrive', () => {
      const live = makeLive();
      const partitioned = live.partitionBy('host', {
        groups: ['a', 'b'] as const,
      });
      const m = partitioned.toMap();
      expect(m.size).toBe(2);
      expect(m.get('a')?.length).toBe(0);
      expect(m.get('b')?.length).toBe(0);
    });

    it('throws on a partition value not in declared groups', () => {
      const live = makeLive();
      const partitioned = live.partitionBy('host', {
        groups: ['a', 'b'] as const,
      });
      expect(() => live.push([0, 0.5, 'rogue'])).toThrow(
        /not in declared groups/,
      );
      void partitioned;
    });

    it('throws on empty groups array', () => {
      const live = makeLive();
      expect(() => live.partitionBy('host', { groups: [] as const })).toThrow(
        /cannot be empty/,
      );
    });

    it('throws on duplicate values in groups', () => {
      const live = makeLive();
      expect(() =>
        live.partitionBy('host', { groups: ['a', 'b', 'a'] as const }),
      ).toThrow(/duplicate value "a"/);
    });
  });

  describe('per-partition retention', () => {
    it('each partition enforces its own maxEvents independently', () => {
      const live = makeLive();
      const partitioned = live.partitionBy('host', {
        retention: { maxEvents: 2 },
      });

      // Push more than 2 events for host 'a' — its sub-buffer caps at 2.
      live.push([0, 0.5, 'a']);
      live.push([60_000, 0.6, 'a']);
      live.push([120_000, 0.7, 'a']);
      live.push([180_000, 0.8, 'a']);

      // Push only 1 event for host 'b'. Its sub-buffer should still
      // have it — host 'a' didn't squeeze it out.
      live.push([240_000, 0.3, 'b']);

      const m = partitioned.toMap();
      expect(m.get('a')?.length).toBe(2); // capped
      expect(m.get('b')?.length).toBe(1); // independent
      // 'a' kept the latest two events (maxEvents evicts oldest)
      expect(m.get('a')?.first()?.begin()).toBe(120_000);
      expect(m.get('a')?.last()?.begin()).toBe(180_000);
    });
  });

  describe('per-partition grace window', () => {
    it('late events are accepted within their own partition grace', () => {
      // Source ordering 'reorder' to allow out-of-order pushes.
      // Each per-partition LiveSeries gets its own graceWindow.
      const live = new LiveSeries({
        name: 'metrics',
        schema,
        ordering: 'reorder',
        graceWindow: '10m',
      });
      const partitioned = live.partitionBy('host', {
        ordering: 'reorder',
        graceWindow: '10m',
      });

      live.push([0, 0.5, 'a']);
      live.push([60_000, 0.6, 'a']);
      live.push([120_000, 0.7, 'a']);
      // Late event for 'a' at t=30_000 — within grace
      live.push([30_000, 0.55, 'a']);

      const m = partitioned.toMap();
      const aEvents = m.get('a')!;
      // The reorder mode should have inserted the late event at its
      // proper position. 'a' has 4 events.
      expect(aEvents.length).toBe(4);
      const times = [];
      for (let i = 0; i < aEvents.length; i++)
        times.push(aEvents.at(i)!.begin());
      expect(times).toEqual([0, 30_000, 60_000, 120_000]);
    });
  });

  describe('collect()', () => {
    it('collects events from all partitions into a unified LiveSeries', () => {
      const live = makeLive();
      const partitioned = live.partitionBy('host');
      const unified = partitioned.collect();

      const seen: Array<{ host: string; cpu: number }> = [];
      unified.on('event', (event) => {
        seen.push({
          host: event.get('host') as string,
          cpu: event.get('cpu') as number,
        });
      });

      live.push([0, 0.5, 'a']);
      live.push([0, 0.3, 'b']);
      live.push([60_000, 0.6, 'a']);
      live.push([60_000, 0.4, 'b']);

      expect(seen.length).toBe(4);
      expect(seen).toContainEqual({ host: 'a', cpu: 0.5 });
      expect(seen).toContainEqual({ host: 'b', cpu: 0.3 });
      expect(unified.length).toBe(4);
    });

    it('replays existing partition events into the unified buffer', () => {
      const live = makeLive();
      live.push([0, 0.5, 'a']);
      live.push([60_000, 0.6, 'a']);

      const partitioned = live.partitionBy('host');
      const unified = partitioned.collect();
      expect(unified.length).toBe(2);
    });

    it('subscribes to partitions spawned after collect()', () => {
      const live = makeLive();
      const partitioned = live.partitionBy('host');
      const unified = partitioned.collect();

      // No partitions yet, then events arrive for 'a' and 'b'.
      live.push([0, 0.5, 'a']);
      live.push([60_000, 0.4, 'b']);

      expect(unified.length).toBe(2);
      const hosts = new Set([
        unified.first()?.get('host'),
        unified.last()?.get('host'),
      ]);
      expect(hosts).toEqual(new Set(['a', 'b']));
    });
  });

  describe('apply() — per-partition operator factory', () => {
    it('applies a fill chain per partition', () => {
      const live = makeLive();
      const partitioned = live.partitionBy('host');
      const filled = partitioned.apply((sub) => sub.fill({ cpu: 'hold' }));

      // Globally ordered by time; interleave hosts at each timestamp.
      live.push([0, 0.5, 'a']);
      live.push([0, 0.3, 'b']);
      live.push([60_000, undefined, 'a']);
      live.push([60_000, undefined, 'b']);

      expect(filled.length).toBe(4);
      const events = [...filled.toTimeSeries().events];
      const aMid = events.find(
        (e) => e.begin() === 60_000 && e.get('host') === 'a',
      );
      const bMid = events.find(
        (e) => e.begin() === 60_000 && e.get('host') === 'b',
      );
      // Per-partition hold-fill: each host's undefined cpu is filled
      // from its own previous event (0.5 for 'a', 0.3 for 'b'),
      // not from the other host.
      expect(aMid?.get('cpu')).toBe(0.5);
      expect(bMid?.get('cpu')).toBe(0.3);
    });

    it('does NOT cross partition boundaries on hold-fill (hazard pinned)', () => {
      // The point of partitioning. Without per-partition scoping,
      // host 'b''s missing cpu at t=120k would hold from host 'a'@60k.
      const live = makeLive();

      // First: no partitioning — confirm the hazard would happen.
      const unscoped = live.fill({ cpu: 'hold' });
      live.push([0, 0.5, 'a']);
      live.push([60_000, 1.0, 'b']);
      live.push([120_000, undefined, 'a']);
      // Without partitioning, t=120k for 'a' would hold from b's 1.0.
      const aMidUnscoped = [...unscoped.toTimeSeries().events].find(
        (e) => e.begin() === 120_000,
      );
      expect(aMidUnscoped?.get('cpu')).toBe(1.0); // hazard

      // Now: with partitioning, t=120k for 'a' holds from a's own 0.5.
      const live2 = makeLive();
      const filled = live2
        .partitionBy('host')
        .apply((sub) => sub.fill({ cpu: 'hold' }));
      live2.push([0, 0.5, 'a']);
      live2.push([60_000, 1.0, 'b']);
      live2.push([120_000, undefined, 'a']);
      const aMidScoped = [...filled.toTimeSeries().events].find(
        (e) => e.begin() === 120_000 && e.get('host') === 'a',
      );
      expect(aMidScoped?.get('cpu')).toBe(0.5); // correct
    });

    it('chains multiple operators inside the factory (fill + diff)', () => {
      const live = makeLive();
      const partitioned = live.partitionBy('host');
      const out = partitioned.apply((sub) =>
        sub.fill({ cpu: 'hold' }).diff('cpu'),
      );

      live.push([0, 0.5, 'a']);
      live.push([60_000, 0.7, 'a']);
      live.push([120_000, 0.9, 'a']);

      expect(out.length).toBe(3);
      // diff first event is undefined; subsequent are deltas
      const events = [...out.toTimeSeries().events];
      expect(events[0]?.get('cpu')).toBeUndefined();
      expect(events[1]?.get('cpu')).toBeCloseTo(0.2, 5);
      expect(events[2]?.get('cpu')).toBeCloseTo(0.2, 5);
    });

    it('applies the factory to partitions spawned after apply()', () => {
      const live = makeLive();
      const partitioned = live.partitionBy('host');
      const filled = partitioned.apply((sub) => sub.fill({ cpu: 'hold' }));

      // No partitions when apply() ran; spawn 'b' mid-stream.
      // Globally ordered timestamps; 'b' first appears at t=60k.
      live.push([0, 0.5, 'a']);
      live.push([60_000, undefined, 'a']);
      live.push([60_000, 0.3, 'b']);
      live.push([120_000, undefined, 'b']);

      const events = [...filled.toTimeSeries().events];
      const bMid = events.find(
        (e) => e.begin() === 120_000 && e.get('host') === 'b',
      );
      expect(bMid?.get('cpu')).toBe(0.3); // 'b' got its own fill chain
    });
  });

  describe('dispose()', () => {
    it('unsubscribes from the source after dispose', () => {
      const live = makeLive();
      const partitioned = live.partitionBy('host');

      live.push([0, 0.5, 'a']);
      expect(partitioned.toMap().get('a')?.length).toBe(1);

      partitioned.dispose();

      // Events pushed after dispose should not reach partitions.
      live.push([60_000, 0.6, 'a']);
      expect(partitioned.toMap().get('a')?.length).toBe(1); // unchanged
    });

    it('disconnects collect() unified series after dispose', () => {
      const live = makeLive();
      const partitioned = live.partitionBy('host');
      const unified = partitioned.collect();

      live.push([0, 0.5, 'a']);
      expect(unified.length).toBe(1);

      partitioned.dispose();
      live.push([60_000, 0.6, 'a']);
      expect(unified.length).toBe(1); // unchanged
    });

    it('is safe to call multiple times', () => {
      const live = makeLive();
      const partitioned = live.partitionBy('host');
      partitioned.dispose();
      expect(() => partitioned.dispose()).not.toThrow();
    });
  });

  describe('headline dashboard chain', () => {
    it('partitionBy + apply(fill + rolling) produces per-host smoothed values', () => {
      const live = makeLive();
      const partitioned = live.partitionBy('host');
      const smoothed = partitioned.apply((sub) =>
        sub.fill({ cpu: 'hold' }).rolling(10, { cpu: 'avg' }),
      );

      // Source ordering is 'strict' — push events globally in time
      // order. The partition view routes each to its host's
      // sub-buffer where rolling state is per-host.
      live.push([0, 0.4, 'a']);
      live.push([0, 1.0, 'b']);
      live.push([1, 0.5, 'a']);
      live.push([1, 1.1, 'b']);
      live.push([2, 0.6, 'a']);
      live.push([2, 1.2, 'b']);

      // The smoothed series has rolling-avg events from each host.
      // Host 'a' values average around 0.5; host 'b' around 1.1.
      expect(smoothed.length).toBeGreaterThan(0);
      // Sanity: events from both hosts present.
      const events = [...smoothed.toTimeSeries().events];
      const aSeen = events.some((e) => {
        const v = e.get('cpu') as number | undefined;
        return v !== undefined && v < 0.7;
      });
      const bSeen = events.some((e) => {
        const v = e.get('cpu') as number | undefined;
        return v !== undefined && v > 0.9;
      });
      expect(aSeen).toBe(true);
      expect(bSeen).toBe(true);
    });
  });

  describe('LivePartitionedSeries instance', () => {
    it('exposes name, schema, by, and groups', () => {
      const live = makeLive();
      const partitioned = live.partitionBy('host', {
        groups: ['a', 'b'] as const,
      });
      expect(partitioned.name).toBe('metrics');
      expect(partitioned.schema).toEqual(schema);
      expect(partitioned.by).toBe('host');
      expect(partitioned.groups).toEqual(['a', 'b']);
    });

    it('throws on a column not in the schema', () => {
      const live = makeLive();
      expect(() =>
        // @ts-expect-error invalid column
        live.partitionBy('not_a_column'),
      ).toThrow(/not in schema/);
    });

    it('also supports direct construction via new LivePartitionedSeries', () => {
      const live = makeLive();
      const partitioned = new LivePartitionedSeries(live, 'host');
      live.push([0, 0.5, 'a']);
      expect(partitioned.toMap().get('a')?.length).toBe(1);
    });
  });
});
