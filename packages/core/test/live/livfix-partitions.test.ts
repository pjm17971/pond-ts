import { describe, expect, it } from 'vitest';

import { LiveSeries } from '../../src/live/live-series.js';

/* -------------------------------------------------------------------------- */
/* [PND-LIVFIX] — unbounded partitions (audit §4.3). Per-partition `maxAge`   */
/* was push-driven: a partition that goes quiet never sees another push, so   */
/* its retention never runs and its events live forever. Unknown options were */
/* silently ignored, so a JS caller passing `maxPartitions` got no signal.     */
/* -------------------------------------------------------------------------- */

const SCHEMA = [
  { name: 'time', kind: 'time' },
  { name: 'host', kind: 'string' },
  { name: 'value', kind: 'number' },
] as const;

const S = 1_000;

describe('[PND-LIVFIX] quiet partitions evict by age', () => {
  it('a partition with no new events is evicted when the source watermark passes its maxAge', () => {
    const live = new LiveSeries({ name: 'l', schema: SCHEMA });
    const parts = live.partitionBy('host', { retention: { maxAge: '10s' } });
    live.push([0, 'a', 1], [0, 'b', 1]);
    const a = parts.toMap().get('a')!;
    const b = parts.toMap().get('b')!;
    expect(a.length).toBe(1);
    // Only `b` keeps talking. Audit: `a` retained its event 9,999 s later.
    live.push([100 * S, 'b', 2]);
    expect(b.length).toBe(1); // b's own retention ran on its push
    expect(a.length).toBe(0); // and so did a's, against the shared clock
  });

  it("emits 'evict' for the quiet partition so derived state stays in sync", () => {
    const live = new LiveSeries({ name: 'l', schema: SCHEMA });
    const parts = live.partitionBy('host', { retention: { maxAge: '10s' } });
    live.push([0, 'a', 1], [0, 'b', 1]);
    const a = parts.toMap().get('a')!;
    let evicted = 0;
    a.on('evict', (e) => {
      evicted += e.length;
    });
    live.push([100 * S, 'b', 2]);
    expect(evicted).toBe(1);
  });

  it('does not evict within maxAge, and keeps maxEvents semantics per partition', () => {
    const live = new LiveSeries({ name: 'l', schema: SCHEMA });
    const parts = live.partitionBy('host', {
      retention: { maxAge: '10s', maxEvents: 2 },
    });
    live.push([0, 'a', 1], [1 * S, 'a', 2], [2 * S, 'a', 3]);
    live.push([5 * S, 'b', 1]);
    const a = parts.toMap().get('a')!;
    expect(a.length).toBe(2); // maxEvents, on its own push
    live.push([9 * S, 'b', 2]); // 9s after a's newest: inside 10s
    expect(a.length).toBe(2);
    live.push([13 * S, 'b', 3]); // a's newest (2s) is now 11s old
    expect(a.length).toBe(0);
  });

  it('sweeps are throttled to maxAge / 8 of watermark advance', () => {
    // maxAge 80 s → a sweep at most every 10 s of data time. The cost of
    // the throttle is bounded overstay; the point of pinning it is that
    // removing the throttle (sweeping every event) is O(partitions) per
    // event on a high-cardinality source.
    const live = new LiveSeries({ name: 'l', schema: SCHEMA });
    const parts = live.partitionBy('host', { retention: { maxAge: '80s' } });
    live.push([0, 'a', 1], [5 * S, 'c', 1], [6 * S, 'b', 1]);
    live.push([85 * S, 'b', 2]); // sweep: a (0s) is 85s old → evicted; c (5s) is 80s old → not yet
    expect(parts.toMap().get('a')!.length).toBe(0);
    expect(parts.toMap().get('c')!.length).toBe(1);
    live.push([90 * S, 'b', 3]); // c is now 85s old, but only 5s since the last sweep → deferred
    expect(parts.toMap().get('c')!.length).toBe(1);
    live.push([96 * S, 'b', 4]); // 11s since the last sweep → swept
    expect(parts.toMap().get('c')!.length).toBe(0);
  });

  it("a throwing 'evict' listener on one partition does not starve the sweep of the others", () => {
    const live = new LiveSeries({ name: 'l', schema: SCHEMA });
    const parts = live.partitionBy('host', { retention: { maxAge: '10s' } });
    live.push([0, 'a', 1], [0, 'b', 1], [0, 'c', 1]);
    parts
      .toMap()
      .get('a')!
      .on('evict', () => {
        throw new Error('a boom');
      });
    expect(() => live.push([100 * S, 'd', 1])).toThrow('a boom');
    for (const k of ['a', 'b', 'c']) {
      expect(parts.toMap().get(k)!.length, k).toBe(0);
    }
  });

  it('a partition without maxAge is never swept', () => {
    const live = new LiveSeries({ name: 'l', schema: SCHEMA });
    const parts = live.partitionBy('host', { retention: { maxEvents: 5 } });
    live.push([0, 'a', 1], [0, 'b', 1]);
    live.push([1_000_000 * S, 'b', 2]);
    expect(parts.toMap().get('a')!.length).toBe(1);
  });
});

describe('[PND-LIVFIX] partitionBy rejects unknown options', () => {
  it('names the unknown key and the known ones', () => {
    const live = new LiveSeries({ name: 'l', schema: SCHEMA });
    expect(() =>
      live.partitionBy('host', { maxPartitions: 10 } as never),
    ).toThrow(/maxPartitions.*groups.*retention/s);
  });
});
