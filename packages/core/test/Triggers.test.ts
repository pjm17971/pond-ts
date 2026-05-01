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

// ── Chained-view rejection ─────────────────────────────────────────

describe('LivePartitionedView.rolling rejects clock trigger', () => {
  it('clock trigger after a chained sugar method throws with a clear message', () => {
    const live = makePartitioned();
    expect(() =>
      live
        .partitionBy('host')
        .fill({ cpu: 'hold' })
        .rolling(
          '1m',
          { cpu: 'avg' },
          { trigger: Trigger.clock(Sequence.every('30s')) },
        ),
    ).toThrowError(/only supported directly after partitionBy/);
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
