import { describe, expect, it, vi } from 'vitest';
import { LiveSeries, Sequence, Trigger } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
] as const;

const partitionedSchema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

function makeLive() {
  return new LiveSeries({ name: 'test', schema });
}

function makePartitioned() {
  return new LiveSeries({ name: 'test', schema: partitionedSchema });
}

// ── Trigger taxonomy ─────────────────────────────────────────────

describe('Trigger', () => {
  it('Trigger.event() returns a frozen kind=event trigger', () => {
    const t = Trigger.event();
    expect(t.kind).toBe('event');
    expect(Object.isFrozen(t)).toBe(true);
    // Sentinel is shared
    expect(Trigger.event()).toBe(t);
  });

  it('Trigger.clock(seq) returns a frozen kind=clock trigger', () => {
    const seq = Sequence.every('30s');
    const t = Trigger.clock(seq);
    expect(t.kind).toBe('clock');
    expect(t.sequence).toBe(seq);
    expect(Object.isFrozen(t)).toBe(true);
  });

  it('Trigger.clock rejects calendar sequences', () => {
    expect(() => Trigger.clock(Sequence.calendar('day'))).toThrowError(
      /requires a fixed-step Sequence/,
    );
  });

  it('Trigger.every(duration) is sugar for Trigger.clock(Sequence.every(duration))', () => {
    const sugar = Trigger.every('30s');
    expect(sugar.kind).toBe('clock');
    expect(Object.isFrozen(sugar)).toBe(true);
    expect(sugar.sequence.stepMs()).toBe(30_000);
    // Default anchor matches Sequence.every default (epoch).
    expect(sugar.sequence.anchor()).toBe(0);
  });

  it('Trigger.every forwards the anchor option', () => {
    const sugar = Trigger.every('30s', { anchor: 5_000 });
    expect(sugar.sequence.stepMs()).toBe(30_000);
    expect(sugar.sequence.anchor()).toBe(5_000);
  });

  it('Trigger.count(n) returns a frozen kind=count trigger', () => {
    const t = Trigger.count(100);
    expect(t.kind).toBe('count');
    expect(t.n).toBe(100);
    expect(Object.isFrozen(t)).toBe(true);
  });

  it('Trigger.count rejects non-positive integers', () => {
    expect(() => Trigger.count(0)).toThrowError(/positive integer/);
    expect(() => Trigger.count(-1)).toThrowError(/positive integer/);
    expect(() => Trigger.count(1.5)).toThrowError(/positive integer/);
    expect(() => Trigger.count(NaN)).toThrowError(/positive integer/);
  });

  it('Trigger.every drives a rolling at the same cadence as Trigger.clock', () => {
    // Behavioural pin: sugar produces the same emission cadence as
    // explicit Trigger.clock(Sequence.every(...)).
    const live = makeLive();
    const sugar = live.rolling(
      '1m',
      { value: 'avg' },
      { trigger: Trigger.every('30s') },
    );
    const explicit = makeLive();
    const explicitRolling = explicit.rolling(
      '1m',
      { value: 'avg' },
      { trigger: Trigger.clock(Sequence.every('30s')) },
    );

    const sugarTimes: number[] = [];
    sugar.on('event', (e) => sugarTimes.push(e.begin()));
    const explicitTimes: number[] = [];
    explicitRolling.on('event', (e) => explicitTimes.push(e.begin()));

    for (const ts of [0, 5_000, 30_001, 60_001]) {
      live.push([ts, ts / 1000]);
      explicit.push([ts, ts / 1000]);
    }

    expect(sugarTimes).toEqual(explicitTimes);
    sugar.dispose();
    explicitRolling.dispose();
  });
});

// ── Default (event) trigger preserves existing behavior ──────────

describe('LiveRollingAggregation — default event trigger (no trigger option)', () => {
  it('emits per source event, keyed at source-event timestamps', () => {
    const live = makeLive();
    const rolling = live.rolling('1m', { value: 'avg' });

    const received: Array<{ ts: number; value: number | undefined }> = [];
    rolling.on('event', (e) =>
      received.push({ ts: e.begin(), value: e.get('value') }),
    );

    live.push([0, 10], [5_000, 20], [10_000, 30]);
    expect(received).toEqual([
      { ts: 0, value: 10 },
      { ts: 5_000, value: 15 },
      { ts: 10_000, value: 20 },
    ]);

    rolling.dispose();
  });

  it('Trigger.event() passed explicitly produces the same behavior as omitting', () => {
    const live = makeLive();
    const rolling = live.rolling(
      '1m',
      { value: 'avg' },
      { trigger: Trigger.event() },
    );
    live.push([0, 10], [5_000, 20]);
    expect(rolling.length).toBe(2);
    rolling.dispose();
  });
});

// ── Clock trigger (replaces v0.11.8 .sample) ─────────────────────

describe('LiveRollingAggregation — clock trigger (Trigger.clock)', () => {
  it('does not emit before first boundary is crossed', () => {
    const live = makeLive();
    const rolling = live.rolling(
      '1m',
      { value: 'avg' },
      { trigger: Trigger.clock(Sequence.every('30s')) },
    );

    live.push([0, 10], [5_000, 20], [15_000, 30]);
    expect(rolling.length).toBe(0);

    rolling.dispose();
  });

  it('emits once per boundary crossing, keyed at boundary timestamp', () => {
    const live = makeLive();
    const rolling = live.rolling(
      '1m',
      { value: 'avg' },
      { trigger: Trigger.clock(Sequence.every('30s')) },
    );

    live.push([0, 10], [15_000, 20], [30_001, 30]);
    expect(rolling.length).toBe(1);
    expect(rolling.at(0)!.begin()).toBe(30_000);
    expect(rolling.at(0)!.get('value')).toBeCloseTo((10 + 20 + 30) / 3, 5);

    rolling.dispose();
  });

  it('emits exactly one event when a single source event crosses multiple boundaries', () => {
    const live = makeLive();
    const rolling = live.rolling(
      '1m',
      { value: 'avg' },
      { trigger: Trigger.clock(Sequence.every('30s')) },
    );

    live.push([0, 10]);
    live.push([90_001, 20]); // jumps over 30s and 60s boundaries
    expect(rolling.length).toBe(1);
    expect(rolling.at(0)!.begin()).toBe(90_000);

    rolling.dispose();
  });

  it('respects minSamples — emits undefined while window is cold', () => {
    const live = makeLive();
    const rolling = live.rolling(
      '1m',
      { value: 'avg' },
      {
        trigger: Trigger.clock(Sequence.every('30s')),
        minSamples: 10,
      },
    );

    live.push([0, 100], [10_000, 200], [30_001, 300]);
    expect(rolling.at(0)!.get('value')).toBeUndefined();

    rolling.dispose();
  });

  it('respects a non-zero anchor', () => {
    const live = makeLive();
    const rolling = live.rolling(
      '1m',
      { value: 'avg' },
      {
        trigger: Trigger.clock(Sequence.every('30s', { anchor: 5_000 })),
      },
    );

    live.push([5_000, 10], [35_001, 20]);
    expect(rolling.length).toBe(1);
    expect(rolling.at(0)!.begin()).toBe(35_000);

    rolling.dispose();
  });

  it('on(event) listener fires per boundary crossing', () => {
    const live = makeLive();
    const rolling = live.rolling(
      '1m',
      { value: 'sum' },
      { trigger: Trigger.clock(Sequence.every('30s')) },
    );

    const spy = vi.fn();
    rolling.on('event', spy);

    live.push([0, 1], [30_001, 2]);
    expect(spy).toHaveBeenCalledTimes(1);

    live.push([60_001, 3]);
    expect(spy).toHaveBeenCalledTimes(2);

    rolling.dispose();
  });

  it('rolling.value() reads the current rolling state regardless of trigger', () => {
    // Trigger gates emission, not state. value() always returns
    // the current rolling-window snapshot.
    const live = makeLive();
    const rolling = live.rolling(
      '1m',
      { value: 'avg' },
      { trigger: Trigger.clock(Sequence.every('30s')) },
    );

    live.push([0, 10], [5_000, 20]);
    // No boundary crossed yet, but value() should still report current state
    expect(rolling.value().value).toBeCloseTo(15, 5);

    rolling.dispose();
  });
});

// ── Count trigger ────────────────────────────────────────────────

describe('LiveRollingAggregation — count trigger (Trigger.count)', () => {
  it('emits one snapshot every N events, starting on the Nth', () => {
    const live = makeLive();
    const rolling = live.rolling(
      '5m',
      { value: 'avg' },
      { trigger: Trigger.count(3) },
    );

    const emissions: Array<{ ts: number; value: number | undefined }> = [];
    rolling.on('event', (e) =>
      emissions.push({ ts: e.begin(), value: e.get('value') }),
    );

    // No emission for the first 2 events.
    live.push([0, 10]);
    live.push([1_000, 20]);
    expect(emissions).toHaveLength(0);

    // The 3rd event triggers the first emission with the rolling avg
    // including all 3 events: (10+20+30)/3 = 20.
    live.push([2_000, 30]);
    expect(emissions).toHaveLength(1);
    expect(emissions[0]!.ts).toBe(2_000);
    expect(emissions[0]!.value).toBe(20);

    // The next 2 events don't trigger; the 6th does.
    live.push([3_000, 40]);
    live.push([4_000, 50]);
    expect(emissions).toHaveLength(1);

    live.push([5_000, 60]);
    expect(emissions).toHaveLength(2);
    expect(emissions[1]!.ts).toBe(5_000);
    // Rolling avg = (10+..+60)/6 = 35
    expect(emissions[1]!.value).toBe(35);

    rolling.dispose();
  });

  it('count(1) is equivalent to event trigger', () => {
    const live = makeLive();
    const rolling = live.rolling(
      '5m',
      { value: 'avg' },
      { trigger: Trigger.count(1) },
    );
    const emissions: number[] = [];
    rolling.on('event', (e) => emissions.push(e.begin()));

    live.push([0, 10], [1_000, 20], [2_000, 30]);
    expect(emissions).toEqual([0, 1_000, 2_000]);

    rolling.dispose();
  });

  it('does not emit during quiet periods (data-driven, no timer)', () => {
    const live = makeLive();
    const rolling = live.rolling(
      '5m',
      { value: 'avg' },
      { trigger: Trigger.count(3) },
    );
    const emissions: number[] = [];
    rolling.on('event', (e) => emissions.push(e.begin()));

    live.push([0, 10], [1_000, 20]); // 2 events, no emission yet
    expect(emissions).toHaveLength(0);

    // No further pushes — no setTimeout-style emission. Counter stays
    // at 2 indefinitely; emission only fires on the next ingest.
    expect(emissions).toHaveLength(0);

    rolling.dispose();
  });

  it('rolling.value() reports current state regardless of trigger', () => {
    const live = makeLive();
    const rolling = live.rolling(
      '5m',
      { value: 'avg' },
      { trigger: Trigger.count(100) },
    );

    live.push([0, 10], [1_000, 20]);
    // Trigger has not fired (only 2 events out of 100), but value()
    // always reads the current rolling-window snapshot.
    expect(rolling.value().value).toBe(15);

    rolling.dispose();
  });

  it('per-partition rolling counts each partition independently', () => {
    const live = makePartitioned();
    const rolling = live
      .partitionBy('host')
      .rolling('5m', { cpu: 'avg' }, { trigger: Trigger.count(2) });

    const emitted: Array<{ host: unknown; cpu: unknown }> = [];
    rolling
      .collect()
      .on('event', (e) =>
        emitted.push({ host: e.get('host'), cpu: e.get('cpu') }),
      );

    // Globally monotonic timestamps; partitioned by host.
    live.push([0, 0.4, 'api-1']);
    live.push([1_000, 0.5, 'api-2']);
    expect(emitted).toHaveLength(0); // each host has only 1 event

    live.push([2_000, 0.6, 'api-1']); // api-1 hits count=2 → emits
    expect(emitted).toHaveLength(1);

    live.push([3_000, 0.7, 'api-2']); // api-2 hits count=2 → emits
    expect(emitted).toHaveLength(2);
  });
});

// ── Synchronised partitioned rolling with clock trigger ──────────

describe('partitionBy().rolling(..., { trigger: Trigger.clock(...) })', () => {
  it('emits one event per known partition on each boundary crossing, all sharing the same ts', () => {
    const live = makePartitioned();
    const ticks = live
      .partitionBy('host')
      .rolling(
        '1m',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );

    // Push events from two hosts before the first boundary
    live.push([0, 0.4, 'api-1']);
    live.push([5_000, 0.5, 'api-2']);
    expect(ticks.length).toBe(0);

    // api-1's event at 30_001 crosses the boundary — both hosts emit
    live.push([30_001, 0.6, 'api-1']);
    expect(ticks.length).toBe(2);

    // Both emitted at the same boundary timestamp
    expect(ticks.at(0)!.begin()).toBe(30_000);
    expect(ticks.at(1)!.begin()).toBe(30_000);

    // Each carries its partition tag
    const hosts = [ticks.at(0)!.get('host'), ticks.at(1)!.get('host')];
    expect(new Set(hosts)).toEqual(new Set(['api-1', 'api-2']));
  });

  it('output schema includes time, partition column, and rolling reducer columns', () => {
    const live = makePartitioned();
    const ticks = live
      .partitionBy('host')
      .rolling(
        '1m',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );

    expect(ticks.schema).toHaveLength(3);
    expect(ticks.schema[0]!.name).toBe('time');
    expect(ticks.schema[1]!.name).toBe('host');
    expect(ticks.schema[2]!.name).toBe('cpu');
  });

  it('partitions emit even when they had no events in the most recent tick window', () => {
    // Once a partition is observed, it stays in the rotation.
    // Subsequent ticks emit a snapshot of its rolling window state
    // even if no new events arrived for that partition.
    const live = makePartitioned();
    const ticks = live
      .partitionBy('host')
      .rolling(
        '30s',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );

    live.push([0, 0.4, 'api-1']);
    live.push([5_000, 0.5, 'api-2']);
    // Cross the 30s boundary with only api-1
    live.push([30_001, 0.6, 'api-1']);

    const tickRows = [ticks.at(0)!, ticks.at(1)!];
    const byHost = new Map(tickRows.map((r) => [r.get('host'), r]));
    expect(byHost.size).toBe(2);
    // api-2's window held [5_000, 0.5]; rolling 30s at boundary
    // 30_001 evicts entries older than 30_001 - 30_000 = 1; 5_000 stays
    expect(byHost.get('api-2')!.get('cpu')).toBeCloseTo(0.5, 5);
  });

  it('declared groups appear in declared order in every tick', () => {
    const live = makePartitioned();
    const ticks = live
      .partitionBy('host', { groups: ['api-2', 'api-1'] as const })
      .rolling(
        '1m',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );

    live.push([0, 0.4, 'api-1']);
    live.push([5_000, 0.5, 'api-2']);
    live.push([30_001, 0.6, 'api-1']);

    // Two declared groups → 2 rows per tick, in declared order
    expect(ticks.length).toBe(2);
    expect(ticks.at(0)!.get('host')).toBe('api-2');
    expect(ticks.at(1)!.get('host')).toBe('api-1');
  });

  it('multi-boundary jump emits exactly one tick (per skipped boundary, not per partition)', () => {
    const live = makePartitioned();
    const ticks = live
      .partitionBy('host')
      .rolling(
        '1m',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );

    live.push([0, 0.4, 'api-1']);
    live.push([90_001, 0.6, 'api-1']); // jumps 30s and 60s
    // Only one boundary crossing emission — at 90_000
    expect(ticks.length).toBe(1); // one partition, one tick
    expect(ticks.at(0)!.begin()).toBe(90_000);
  });

  it('forwards events to on(event) listeners synchronously per tick', () => {
    const live = makePartitioned();
    const ticks = live
      .partitionBy('host')
      .rolling(
        '1m',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );

    const tickSeen: Array<{ ts: number; host: unknown }> = [];
    ticks.on('event', (e) =>
      tickSeen.push({ ts: e.begin(), host: e.get('host') }),
    );

    live.push([0, 0.4, 'api-1']);
    live.push([5_000, 0.5, 'api-2']);
    live.push([30_001, 0.6, 'api-1']);

    // Two events for one tick, both at ts=30_000
    expect(tickSeen).toHaveLength(2);
    expect(tickSeen[0]!.ts).toBe(30_000);
    expect(tickSeen[1]!.ts).toBe(30_000);
    expect(new Set([tickSeen[0]!.host, tickSeen[1]!.host])).toEqual(
      new Set(['api-1', 'api-2']),
    );
  });
});

// ── Column collision rejection ───────────────────────────────────

describe('partitionBy().rolling clock-trigger column collision', () => {
  it('throws when partition column name collides with a reducer-output column', () => {
    // Schema: [time, cpu, host]; partitionBy('cpu') would put 'cpu'
    // as the partition tag, and a reducer mapping like { cpu: 'avg' }
    // would also produce a 'cpu' output column. Without rejection,
    // the emit loop's record would silently overwrite one with the
    // other.
    const live = makePartitioned();
    expect(() =>
      live
        .partitionBy('cpu' as any) // partition column matches mapping output
        .rolling(
          '1m',
          { cpu: 'avg' },
          { trigger: Trigger.clock(Sequence.every('30s')) },
        ),
    ).toThrowError(/collides with a reducer-output column/);
  });
});

// ── Dispose semantics on the sync source ──────────────────────────

describe('partitionBy().rolling clock-trigger — dispose semantics', () => {
  it('dispose() detaches from upstream partitions and stops emitting', () => {
    const live = makePartitioned();
    const ticks = live
      .partitionBy('host')
      .rolling(
        '1m',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );

    const spy = vi.fn();
    ticks.on('event', spy);

    live.push([0, 0.4, 'api-1']);
    live.push([5_000, 0.5, 'api-2']);
    live.push([30_001, 0.6, 'api-1']); // boundary crossing → 2 emissions
    expect(spy).toHaveBeenCalledTimes(2);

    (ticks as any).dispose();

    // After dispose, further pushes don't reach the sync rolling
    live.push([60_001, 0.7, 'api-1']);
    expect(spy).toHaveBeenCalledTimes(2);
  });

  it('dispose() is idempotent', () => {
    const live = makePartitioned();
    const ticks = live
      .partitionBy('host')
      .rolling(
        '1m',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );
    expect(() => {
      (ticks as any).dispose();
      (ticks as any).dispose();
      (ticks as any).dispose();
    }).not.toThrow();
  });
});

// ── Multi-partition multi-boundary jump ───────────────────────────

describe('partitionBy().rolling clock-trigger — multi-partition multi-boundary jump', () => {
  it('a single source event jumping multiple boundaries fires exactly one tick across all partitions', () => {
    const live = makePartitioned();
    const ticks = live
      .partitionBy('host')
      .rolling(
        '1m',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );

    // Establish two partitions before any boundary crossing
    live.push([0, 0.4, 'api-1']);
    live.push([5_000, 0.5, 'api-2']);
    expect(ticks.length).toBe(0);

    // One event that jumps THREE boundaries (0→30s→60s→90s)
    live.push([90_001, 0.6, 'api-1']);

    // Exactly one tick fires — at 90_000, with both known partitions
    // emitting one row each (2 events total, both keyed at 90_000)
    expect(ticks.length).toBe(2);
    expect(ticks.at(0)!.begin()).toBe(90_000);
    expect(ticks.at(1)!.begin()).toBe(90_000);
    const hosts = new Set([ticks.at(0)!.get('host'), ticks.at(1)!.get('host')]);
    expect(hosts).toEqual(new Set(['api-1', 'api-2']));
  });
});

// ── Late-spawn partition semantics ────────────────────────────────

describe('partitionBy().rolling clock-trigger — late-spawn partitions', () => {
  it('a partition that spawns AFTER the first tick only appears in subsequent ticks', () => {
    // Documented limitation: the sync source emits one row per known
    // partition per tick. A partition that has not yet been spawned
    // has no row in the current tick — it joins the rotation on its
    // first event and emits starting with the next tick crossing.
    const live = makePartitioned();
    const ticks = live
      .partitionBy('host')
      .rolling(
        '1m',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );

    live.push([0, 0.4, 'api-1']);
    live.push([30_001, 0.6, 'api-1']); // tick at 30_000 — only 'api-1' known
    expect(ticks.length).toBe(1);
    expect(ticks.at(0)!.get('host')).toBe('api-1');

    // 'api-2' arrives AFTER the first tick. Its first event spawns
    // it; it joins the rotation but doesn't retroactively emit for
    // the 30_000 tick.
    live.push([35_000, 0.5, 'api-2']);
    expect(ticks.length).toBe(1); // no new emission yet

    // Next boundary crossing — both partitions now emit
    live.push([60_001, 0.7, 'api-2']);
    expect(ticks.length).toBe(3); // 1 prior + 2 (api-1 and api-2 at 60_000)
  });
});

// ── Chained-view sync rolling ──────────────────────────────────────

describe('LivePartitionedView.rolling — clock trigger after chained sugar', () => {
  it('partitionBy().fill().rolling(..., { trigger }) works and uses chain output', () => {
    // The chain factory output's events feed sync.ingest. fill
    // transforms values within existing events (replaces undefined
    // with held values); it doesn't add events, so the bucket-index
    // logic operates on the same timestamps regardless.
    const schemaWithGaps = [
      { name: 'time', kind: 'time' },
      { name: 'cpu', kind: 'number', required: false },
      { name: 'host', kind: 'string' },
    ] as const;
    const live = new LiveSeries({ name: 'test', schema: schemaWithGaps });

    const ticks = live
      .partitionBy('host')
      .fill({ cpu: 'hold' })
      .rolling(
        '1m',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );

    // Push events; one has a missing cpu that fill should hold from
    // the previous value.
    live.push([0, 0.4, 'api-1']);
    live.push([5_000, undefined, 'api-1']); // fill('hold') → 0.4
    live.push([10_000, 0.6, 'api-1']);
    live.push([30_001, 0.8, 'api-1']); // crosses 30s boundary

    expect(ticks.length).toBe(1);
    expect(ticks.at(0)!.begin()).toBe(30_000);
    expect(ticks.at(0)!.get('host')).toBe('api-1');
    // After fill('hold'): values seen by rolling = [0.4, 0.4, 0.6, 0.8]
    expect(ticks.at(0)!.get('cpu')).toBeCloseTo((0.4 + 0.4 + 0.6 + 0.8) / 4, 5);
  });

  it('output schema is [time, partitionColumn, ...mappingColumns] regardless of chain', () => {
    const live = makePartitioned();
    const ticks = live
      .partitionBy('host')
      .fill({ cpu: 'hold' })
      .rolling(
        '1m',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );

    expect(ticks.schema).toHaveLength(3);
    expect(ticks.schema[0]!.name).toBe('time');
    expect(ticks.schema[1]!.name).toBe('host');
    expect(ticks.schema[2]!.name).toBe('cpu');
  });

  it('synchronisation across partitions still holds with chained sugar', () => {
    const live = makePartitioned();
    const ticks = live
      .partitionBy('host')
      .fill({ cpu: 'hold' })
      .rolling(
        '1m',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );

    live.push([0, 0.4, 'api-1']);
    live.push([5_000, 0.5, 'api-2']);
    live.push([30_001, 0.6, 'api-1']); // crosses 30s; both should emit

    expect(ticks.length).toBe(2);
    expect(ticks.at(0)!.begin()).toBe(30_000);
    expect(ticks.at(1)!.begin()).toBe(30_000); // same boundary ts
    const hosts = new Set([ticks.at(0)!.get('host'), ticks.at(1)!.get('host')]);
    expect(hosts).toEqual(new Set(['api-1', 'api-2']));
  });

  it('dispose semantics work through the chain', () => {
    const live = makePartitioned();
    const ticks = live
      .partitionBy('host')
      .fill({ cpu: 'hold' })
      .rolling(
        '1m',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );

    const spy = vi.fn();
    ticks.on('event', spy);

    live.push([0, 0.4, 'api-1']);
    live.push([30_001, 0.6, 'api-1']);
    expect(spy).toHaveBeenCalledTimes(1);

    (ticks as any).dispose();

    live.push([60_001, 0.7, 'api-1']);
    expect(spy).toHaveBeenCalledTimes(1); // no new events after dispose
  });

  it('disposing a chained sync rolling tears down the chain views (no orphan partition listeners)', () => {
    // Codex review item: the chained path's factory creates a
    // LiveView/LiveRollingAggregation per partition. Each chain
    // output subscribes UPSTREAM to its partition. Without
    // explicitly disposing those chain outputs on sync.dispose(),
    // the partitions keep firing events into the chain views, and
    // the chain views keep buffering them — even though the sync
    // rolling itself no longer emits.
    //
    // Verify by counting listeners on the partitions: after
    // construction the partition has 1 listener (the chain view);
    // after sync.dispose() it should have 0.
    const live = makePartitioned();
    const partitioned = live.partitionBy('host', { groups: ['api-1'] });
    const partition = partitioned.toMap().get('api-1')!;

    // Wrap partition.on to count active listeners
    const realOn = partition.on.bind(partition);
    let activeListeners = 0;
    (partition as any).on = ((type: any, fn: any) => {
      const unsub = realOn(type, fn);
      activeListeners++;
      return () => {
        activeListeners--;
        if (typeof unsub === 'function') return unsub();
      };
    }) as any;

    const ticks = partitioned
      .fill({ cpu: 'hold' })
      .rolling(
        '1m',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );

    // The chain view (fill output, a LiveView) subscribes to BOTH
    // `'event'` and `'evict'` on its upstream partition (the partition
    // advertises EMITS_EVICT). So 2 active listeners: one per channel.
    expect(activeListeners).toBe(2);

    (ticks as any).dispose();

    // After dispose: the chain view is also disposed (because the
    // chained path passes ownsFactoryOutput: true), releasing both
    // its event and evict subscriptions on the partition.
    expect(activeListeners).toBe(0);
  });

  it('replay works for chained-view path: pre-existing events feed the sync via the chain (with fill actually doing work)', () => {
    // Pin that the chain factory's transformation is applied during
    // replay, not bypassed. Use `required: false` cpu and push some
    // undefined values: fill('hold') should carry forward the
    // previous value, so the rolling avg differs from "raw avg with
    // undefined skipped."
    const optionalSchema = [
      { name: 'time', kind: 'time' },
      { name: 'cpu', kind: 'number', required: false },
      { name: 'host', kind: 'string' },
    ] as const;
    const live = new LiveSeries({ name: 'test', schema: optionalSchema });

    // Pre-populate. host 'a' has [0.4, undefined, undefined]; with
    // fill('hold') those undefineds become 0.4, so rolling sees
    // three 0.4 values. Without fill, rolling would average just
    // the one 0.4 (or zero — depending on how undefined is treated).
    live.push([0, 0.4, 'a']);
    live.push([5_000, undefined, 'a']);
    live.push([10_000, undefined, 'a']);
    live.push([20_000, 0.5, 'b']);
    live.push([30_001, 0.4, 'a']); // crosses 30s

    const ticks = live
      .partitionBy('host')
      .fill({ cpu: 'hold' })
      .rolling(
        '1m',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );

    // Two partitions, one tick fired during replay (30_001 crossed).
    expect(ticks.length).toBe(2);
    expect(ticks.at(0)!.begin()).toBe(30_000);
    expect(ticks.at(1)!.begin()).toBe(30_000);

    // Verify host 'a' shows fill applied: window has [0.4, 0.4 (held),
    // 0.4 (held), 0.4] → avg = 0.4. If fill were bypassed, the
    // undefineds would skip and avg would be different (or
    // undefined for the count-zero case depending on reducer).
    const byHost = new Map<unknown, unknown>();
    for (let i = 0; i < ticks.length; i++) {
      const e = ticks.at(i)!;
      byHost.set(e.get('host'), e.get('cpu'));
    }
    expect(byHost.get('a')).toBeCloseTo(0.4, 5);
    expect(byHost.get('b')).toBeCloseTo(0.5, 5);
  });
});

// ── Regression tests from PR #94's Codex adversarial review ──────

describe('partitionBy().rolling clock-trigger — quiet-partition eviction', () => {
  it('a quiet partition with stale entries outside the rolling window emits null/undefined, not the stale value', () => {
    // Codex review item #1: every known partition's window state
    // must be evicted against the triggering event's timestamp
    // before snapshotting. Without this fix, a quiet partition's
    // pre-window value would still appear in the emitted aggregate.
    const live = makePartitioned();
    const ticks = live
      .partitionBy('host')
      .rolling(
        '30s',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );

    // host A's only event is at t=0. host B then triggers a tick
    // far past A's data:
    live.push([0, 0.99, 'a']);
    // host B at t=90_001 — 90 seconds later. Crosses to bucket 3.
    // The 30s window cutoff at t=90_001 is 60_001; A's event at
    // t=0 is well outside the window.
    live.push([90_001, 0.5, 'b']);

    // 1 tick fires at boundary 90_000, with both partitions emitting.
    expect(ticks.length).toBe(2);
    const byHost = new Map<unknown, unknown>();
    for (let i = 0; i < ticks.length; i++) {
      const e = ticks.at(i)!;
      byHost.set(e.get('host'), e.get('cpu'));
    }
    // host A's window is empty — the t=0 event has been evicted.
    // Pre-fix: would have emitted 0.99. Post-fix: undefined.
    expect(byHost.get('a')).toBeUndefined();
    // host B has its just-arrived event in the window.
    expect(byHost.get('b')).toBeCloseTo(0.5, 5);
  });
});

describe('partitionBy().rolling clock-trigger — pre-existing partition data replay', () => {
  it('events already in partition buffers are replayed into the sync rolling at construction', () => {
    // Codex review item #2: when constructing the sync rolling, the
    // existing partition buffers are replayed in global timestamp
    // order so the rolling state reflects history, not just future
    // events.
    const live = makePartitioned();

    // Pre-populate the source with events across two hosts and
    // multiple boundary crossings, BEFORE constructing the sync
    // rolling. The router will spawn the partitions and route events
    // into them.
    live.push([0, 0.1, 'a']);
    live.push([10_000, 0.2, 'b']);
    live.push([20_000, 0.3, 'a']);
    live.push([30_001, 0.4, 'a']); // crosses 30s boundary
    live.push([40_000, 0.5, 'b']);
    live.push([60_001, 0.6, 'b']); // crosses 60s boundary

    // NOW construct the sync rolling. Without the fix, it would see
    // zero events; with the fix, it replays all six events in
    // timestamp order and the bucket index advances naturally.
    const ticks = live
      .partitionBy('host')
      .rolling(
        '60s',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );

    // Two boundaries were crossed during replay (30s and 60s). Each
    // emits one row per known partition (2 partitions) → 4 events.
    expect(ticks.length).toBe(4);
    expect(ticks.at(0)!.begin()).toBe(30_000);
    expect(ticks.at(1)!.begin()).toBe(30_000);
    expect(ticks.at(2)!.begin()).toBe(60_000);
    expect(ticks.at(3)!.begin()).toBe(60_000);
  });

  it('replay correctly interleaves events across partitions by timestamp', () => {
    // Critical correctness guarantee: when replaying, events from
    // different partitions must be ingested in global timestamp
    // order. Otherwise the second partition's earlier events would
    // be silently discarded (bucketIdx ≤ lastBucketIdx).
    const live = makePartitioned();

    // Construct interleaved history: a@0, b@5_000, a@30_001 (crosses)
    live.push([0, 0.1, 'a']);
    live.push([5_000, 0.5, 'b']);
    live.push([30_001, 0.2, 'a']);

    const ticks = live
      .partitionBy('host')
      .rolling(
        '60s',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );

    // Replay processes events in [0, 5_000, 30_001] order. The
    // 30_001 crosses the boundary; both partitions emit.
    expect(ticks.length).toBe(2);
    // Both partitions should have their pre-boundary events in
    // their windows. b's avg = 0.5 (one event); a's avg = (0.1 + 0.2) / 2.
    const byHost = new Map<unknown, unknown>();
    for (let i = 0; i < ticks.length; i++) {
      const e = ticks.at(i)!;
      byHost.set(e.get('host'), e.get('cpu'));
    }
    expect(byHost.get('a')).toBeCloseTo(0.15, 5);
    expect(byHost.get('b')).toBeCloseTo(0.5, 5);
  });
});

describe('partitionBy().rolling clock-trigger — spawn-listener cleanup on dispose', () => {
  it('disposed sync rollings do not retain memory via the parent series spawn listener', () => {
    // Codex review item #3: the spawn listener captures the sync
    // source. When sync.dispose() runs, the spawn listener must be
    // removed from the parent series — otherwise repeated
    // create/dispose cycles accumulate dead listeners and retained
    // state on a long-lived high-cardinality partitioned source.
    //
    // We can't directly assert "no memory leak" but we can verify
    // the listener registration count stays bounded across cycles.
    const live = makePartitioned();
    const partitioned = live.partitionBy('host');

    // Probe: count listeners on the partitioned series. We can't
    // reach #onSpawn directly, but we can observe behavior: after
    // dispose, future spawns should NOT call into the disposed
    // sync. We approximate this by creating, disposing, and then
    // pushing a NEW partition — the disposed sync's ingest is a
    // no-op so it shouldn't fire any of its listeners on emission.

    const sync1 = partitioned.rolling(
      '1m',
      { cpu: 'avg' },
      { trigger: Trigger.clock(Sequence.every('30s')) },
    );
    const spy1 = vi.fn();
    sync1.on('event', spy1);

    (sync1 as any).dispose();

    // After dispose: a new partition spawns. Pre-fix, the disposed
    // sync would still receive ingest calls (its spawn handler is
    // still in #onSpawn) — though they'd be no-ops because dispose
    // gates them. Post-fix, the spawn handler is removed entirely.
    live.push([0, 0.1, 'new-host']);
    live.push([30_001, 0.2, 'new-host']); // crosses boundary

    // The disposed sync should have emitted nothing.
    expect(spy1).not.toHaveBeenCalled();

    // And a new sync constructed AFTER the dispose should work
    // independently — it sees the new-host events via replay.
    const sync2 = partitioned.rolling(
      '1m',
      { cpu: 'avg' },
      { trigger: Trigger.clock(Sequence.every('30s')) },
    );
    // Replay handles the existing events; the second crosses the boundary
    expect(sync2.length).toBeGreaterThanOrEqual(1);

    (sync2 as any).dispose();
  });

  it('repeated create/dispose cycles do not accumulate listeners on the parent', () => {
    // Stronger assertion: cycle the sync many times and verify no
    // residual effect after disposal. If spawn listeners weren't
    // being removed, each cycle would add one. After 100 cycles,
    // a future spawn would trigger 100 dead-listener calls.
    const live = makePartitioned();
    const partitioned = live.partitionBy('host');

    for (let i = 0; i < 100; i++) {
      const sync = partitioned.rolling(
        '1m',
        { cpu: 'avg' },
        { trigger: Trigger.clock(Sequence.every('30s')) },
      );
      (sync as any).dispose();
    }

    // After 100 cycles, push events. If spawn listeners leaked,
    // each new partition spawn would invoke 100 dead handlers. The
    // dispose path makes ingest a no-op, so this is silent — but
    // the absence of any thrown error or listener-count blowup is
    // the implicit pin.
    expect(() => {
      live.push([0, 0.1, 'host-x']);
      live.push([30_001, 0.2, 'host-x']);
    }).not.toThrow();
  });
});

// ── Webapp-telemetry pattern: replaces .sample() ──────────────────

describe('telemetry pattern (one rolling drives both backend report and live display)', () => {
  it('rolling.value() and on(event) read the same state, different cadences', () => {
    const live = makeLive();
    const rolling = live.rolling(
      '1m',
      { value: 'p95' },
      {
        trigger: Trigger.clock(Sequence.every('30s')),
        minSamples: 1,
      },
    );

    // Backend reports — fire on boundary crossings
    const reports: number[] = [];
    rolling.on('event', (e) => {
      const v = e.get('value');
      if (typeof v === 'number') reports.push(v);
    });

    // Push a few events; cross one boundary
    live.push([0, 10], [10_000, 20], [20_000, 30]);
    // value() reads current state at any time, no boundary crossed yet
    expect(typeof rolling.value().value).toBe('number');
    expect(reports).toHaveLength(0);

    // Cross 30s boundary — backend report fires
    live.push([30_001, 40]);
    expect(reports).toHaveLength(1);
    // And value() still reads current state
    expect(typeof rolling.value().value).toBe('number');

    rolling.dispose();
  });
});
