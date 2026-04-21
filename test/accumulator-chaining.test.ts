import { describe, it, expect } from 'vitest';
import { LiveSeries, Sequence } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

function makeLive() {
  const live = new LiveSeries({ name: 'test', schema });
  return live;
}

function pushMinuteData(live: ReturnType<typeof makeLive>, values: number[]) {
  for (let i = 0; i < values.length; i++) {
    live.push([i * 60_000, values[i]!, 'api-1']);
  }
}

// ── LiveAggregation chaining ─────────────────────────────────────

describe('LiveAggregation chaining', () => {
  it('filter: removes buckets by predicate', () => {
    const live = makeLive();
    const agg = live.aggregate(Sequence.every('2m'), { value: 'avg' });

    // Push 4 events across 2 buckets: [0m,1m] avg=15, [2m,3m] avg=35
    live.push(
      [0, 10, 'a'],
      [60_000, 20, 'a'],
      [120_000, 30, 'a'],
      [180_000, 40, 'a'],
      [240_000, 50, 'a'], // closes both buckets
    );

    const filtered = agg.filter((e) => (e.get('value') as number) > 20);
    expect(filtered.length).toBe(1);
    expect(filtered.at(0)!.get('value')).toBe(35);
  });

  it('map: clamps negative values to zero', () => {
    const live = makeLive();
    const agg = live.aggregate(Sequence.every('1m'), { value: 'avg' });

    live.push([0, -10, 'a'], [60_000, 20, 'a'], [120_000, 0, 'a']);

    const clamped = agg.map((e) => {
      const val = e.get('value') as number;
      return val < 0 ? e.set('value', 0) : e;
    });

    expect(clamped.length).toBe(2);
    expect(clamped.at(0)!.get('value')).toBe(0);
    expect(clamped.at(1)!.get('value')).toBe(20);
  });

  it('fill: fills undefined aggregate values with zero', () => {
    const live = makeLive();
    const agg = live
      .aggregate(Sequence.every('1m'), { value: 'avg' })
      .fill('zero');

    live.push([0, 10, 'a'], [60_000, 20, 'a'], [120_000, 30, 'a']);
    expect(agg.length).toBe(2);
    expect(agg.at(0)!.get('value')).toBe(10);
  });

  it('diff: computes per-bucket differences', () => {
    const live = makeLive();
    const agg = live.aggregate(Sequence.every('1m'), { value: 'avg' });

    live.push(
      [0, 10, 'a'],
      [60_000, 30, 'a'],
      [120_000, 25, 'a'],
      [180_000, 0, 'a'],
    );

    const d = agg.diff('value');
    expect(d.length).toBe(3);
    expect(d.at(0)!.get('value')).toBeUndefined();
    expect(d.at(1)!.get('value')).toBe(20); // 30 - 10
    expect(d.at(2)!.get('value')).toBe(-5); // 25 - 30
  });

  it('rate: computes per-second rate between buckets', () => {
    const live = makeLive();
    const agg = live.aggregate(Sequence.every('1m'), { value: 'avg' });

    live.push([0, 100, 'a'], [60_000, 220, 'a'], [120_000, 0, 'a']);

    const r = agg.rate('value');
    expect(r.at(1)!.get('value')).toBeCloseTo(2); // 120 / 60s
  });

  it('select: narrows schema to specific columns', () => {
    const live = makeLive();
    const agg = live.aggregate(Sequence.every('1m'), {
      value: 'avg',
      host: 'first',
    });

    live.push([0, 10, 'a'], [60_000, 20, 'b'], [120_000, 0, 'c']);

    const selected = agg.select('value');
    expect(selected.schema.length).toBe(2); // time + value
    expect(selected.at(0)!.get('value')).toBe(10);
  });

  it('cumulative: running sum across buckets', () => {
    const live = makeLive();
    const agg = live.aggregate(Sequence.every('1m'), { value: 'avg' });

    live.push(
      [0, 10, 'a'],
      [60_000, 20, 'a'],
      [120_000, 30, 'a'],
      [180_000, 0, 'a'],
    );

    const c = agg.cumulative({ value: 'sum' });
    expect(c.at(0)!.get('value')).toBe(10);
    expect(c.at(1)!.get('value')).toBe(30);
    expect(c.at(2)!.get('value')).toBe(60);
  });

  it('incremental: new closed buckets flow through chained view', () => {
    const live = makeLive();
    const pipeline = agg(live).filter((e) => (e.get('value') as number) > 0);

    const received: number[] = [];
    pipeline.on('event', (event) => {
      received.push(event.get('value') as number);
    });

    live.push([0, 10, 'a']);
    expect(received).toEqual([]);

    live.push([60_000, -5, 'a']);
    // bucket [0, 60000) closes with avg=10, passes filter
    expect(received).toEqual([10]);

    live.push([120_000, 20, 'a']);
    // bucket [60000, 120000) closes with avg=-5, filtered out
    expect(received).toEqual([10]);
  });

  it('multi-stage: agg → diff → fill', () => {
    const live = makeLive();
    const pipeline = live
      .aggregate(Sequence.every('1m'), { value: 'avg' })
      .diff('value')
      .fill('zero');

    live.push(
      [0, 10, 'a'],
      [60_000, 30, 'a'],
      [120_000, 25, 'a'],
      [180_000, 0, 'a'],
    );

    expect(pipeline.at(0)!.get('value')).toBe(0); // first diff → undefined → fill zero
    expect(pipeline.at(1)!.get('value')).toBe(20);
    expect(pipeline.at(2)!.get('value')).toBe(-5);
  });
});

// ── LiveRollingAggregation chaining ──────────────────────────────

describe('LiveRollingAggregation chaining', () => {
  it('filter: removes rolling values by predicate', () => {
    const live = makeLive();
    const r = live.rolling(2, { value: 'avg' });

    live.push([0, 10, 'a'], [1000, 20, 'a'], [2000, 100, 'a']);

    const filtered = r.filter((e) => (e.get('value') as number) < 50);
    expect(filtered.length).toBe(2);
    expect(filtered.at(0)!.get('value')).toBe(10);
    expect(filtered.at(1)!.get('value')).toBe(15);
  });

  it('map: transforms rolling output', () => {
    const live = makeLive();
    const r = live.rolling(2, { value: 'avg' });

    live.push([0, 10, 'a'], [1000, 30, 'a']);

    const doubled = r.map((e) =>
      e.set('value', (e.get('value') as number) * 2),
    );
    expect(doubled.at(1)!.get('value')).toBe(40); // avg=20, doubled=40
  });

  it('diff: computes differences in rolling output', () => {
    const live = makeLive();
    const r = live.rolling(2, { value: 'avg' });

    live.push([0, 10, 'a'], [1000, 20, 'a'], [2000, 30, 'a']);

    const d = r.diff('value');
    // Rolling outputs: 10, 15, 25
    expect(d.at(0)!.get('value')).toBeUndefined();
    expect(d.at(1)!.get('value')).toBe(5);
    expect(d.at(2)!.get('value')).toBe(10);
  });

  it('fill: fills undefined rolling values', () => {
    const live = makeLive();
    const filled = live.rolling(2, { value: 'avg' }).fill('zero');

    live.push([0, 10, 'a']);
    expect(filled.at(0)!.get('value')).toBe(10);
  });

  it('incremental: new rolling values flow through chained view', () => {
    const live = makeLive();
    const pipeline = live
      .rolling(2, { value: 'avg' })
      .filter((e) => (e.get('value') as number) >= 15);

    live.push([0, 10, 'a']);
    expect(pipeline.length).toBe(0);

    live.push([1000, 20, 'a']);
    expect(pipeline.length).toBe(1);
    expect(pipeline.at(0)!.get('value')).toBe(15);
  });

  it('aggregate: re-aggregate rolling output into buckets', () => {
    const live = makeLive();
    const r = live
      .rolling('2s', { value: 'avg' })
      .aggregate(Sequence.every('2s'), { value: 'avg' });

    live.push(
      [0, 10, 'a'],
      [1000, 20, 'a'],
      [2000, 30, 'a'],
      [3000, 40, 'a'],
      [4000, 50, 'a'],
    );

    expect(r.closedCount).toBeGreaterThanOrEqual(1);
  });
});

// helper to keep the incremental test readable
function agg(live: ReturnType<typeof makeLive>) {
  return live.aggregate(Sequence.every('1m'), { value: 'avg' });
}
