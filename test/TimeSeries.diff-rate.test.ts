import { describe, expect, it } from 'vitest';
import { TimeSeries } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

function makeSeries() {
  return new TimeSeries({
    name: 'metrics',
    schema,
    rows: [
      [1000, 10, 'a'],
      [2000, 30, 'a'],
      [4000, 60, 'a'],
      [7000, 100, 'a'],
    ],
  });
}

const numericSchema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'mem', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

function makeMultiSeries() {
  return new TimeSeries({
    name: 'multi',
    schema: numericSchema,
    rows: [
      [1000, 10, 100, 'a'],
      [2000, 30, 150, 'a'],
      [4000, 60, 130, 'a'],
    ],
  });
}

describe('TimeSeries.diff', () => {
  it('computes differences for a single column', () => {
    const d = makeSeries().diff('value');
    expect(d.length).toBe(4);
    expect(d.at(0)?.get('value')).toBeUndefined();
    expect(d.at(1)?.get('value')).toBe(20);
    expect(d.at(2)?.get('value')).toBe(30);
    expect(d.at(3)?.get('value')).toBe(40);
  });

  it('preserves non-targeted columns', () => {
    const d = makeSeries().diff('value');
    expect(d.at(0)?.get('host')).toBe('a');
    expect(d.at(1)?.get('host')).toBe('a');
  });

  it('preserves event keys', () => {
    const d = makeSeries().diff('value');
    expect(d.at(0)?.begin()).toBe(1000);
    expect(d.at(1)?.begin()).toBe(2000);
    expect(d.at(2)?.begin()).toBe(4000);
    expect(d.at(3)?.begin()).toBe(7000);
  });

  it('drops first event with { drop: true }', () => {
    const d = makeSeries().diff('value', { drop: true });
    expect(d.length).toBe(3);
    expect(d.at(0)?.get('value')).toBe(20);
    expect(d.at(0)?.begin()).toBe(2000);
  });

  it('diffs multiple columns', () => {
    const d = makeMultiSeries().diff(['cpu', 'mem']);
    expect(d.length).toBe(3);
    expect(d.at(0)?.get('cpu')).toBeUndefined();
    expect(d.at(0)?.get('mem')).toBeUndefined();
    expect(d.at(1)?.get('cpu')).toBe(20);
    expect(d.at(1)?.get('mem')).toBe(50);
    expect(d.at(2)?.get('cpu')).toBe(30);
    expect(d.at(2)?.get('mem')).toBe(-20);
  });

  it('preserves non-targeted columns in multi-column diff', () => {
    const d = makeMultiSeries().diff(['cpu', 'mem']);
    expect(d.at(0)?.get('host')).toBe('a');
    expect(d.at(1)?.get('host')).toBe('a');
  });

  it('handles empty series', () => {
    const empty = new TimeSeries({ name: 'e', schema, rows: [] });
    const d = empty.diff('value');
    expect(d.length).toBe(0);
  });

  it('handles single-event series', () => {
    const single = new TimeSeries({
      name: 's',
      schema,
      rows: [[1000, 10, 'a']],
    });
    const d = single.diff('value');
    expect(d.length).toBe(1);
    expect(d.at(0)?.get('value')).toBeUndefined();
  });

  it('handles single-event series with drop', () => {
    const single = new TimeSeries({
      name: 's',
      schema,
      rows: [[1000, 10, 'a']],
    });
    const d = single.diff('value', { drop: true });
    expect(d.length).toBe(0);
  });

  it('handles negative differences', () => {
    const ts = new TimeSeries({
      name: 'dec',
      schema,
      rows: [
        [1000, 50, 'a'],
        [2000, 30, 'a'],
        [3000, 10, 'a'],
      ],
    });
    const d = ts.diff('value');
    expect(d.at(1)?.get('value')).toBe(-20);
    expect(d.at(2)?.get('value')).toBe(-20);
  });

  it('handles zero differences', () => {
    const ts = new TimeSeries({
      name: 'flat',
      schema,
      rows: [
        [1000, 10, 'a'],
        [2000, 10, 'a'],
      ],
    });
    const d = ts.diff('value');
    expect(d.at(1)?.get('value')).toBe(0);
  });
});

describe('TimeSeries.rate', () => {
  it('computes per-second rate of change', () => {
    const r = makeSeries().rate('value');
    expect(r.length).toBe(4);
    expect(r.at(0)?.get('value')).toBeUndefined();
    // t=1000→2000 (1s gap), value 10→30, rate = 20/1 = 20
    expect(r.at(1)?.get('value')).toBe(20);
    // t=2000→4000 (2s gap), value 30→60, rate = 30/2 = 15
    expect(r.at(2)?.get('value')).toBe(15);
    // t=4000→7000 (3s gap), value 60→100, rate = 40/3
    expect(r.at(3)?.get('value')).toBeCloseTo(40 / 3);
  });

  it('preserves non-targeted columns', () => {
    const r = makeSeries().rate('value');
    expect(r.at(1)?.get('host')).toBe('a');
  });

  it('drops first event with { drop: true }', () => {
    const r = makeSeries().rate('value', { drop: true });
    expect(r.length).toBe(3);
    expect(r.at(0)?.get('value')).toBe(20);
    expect(r.at(0)?.begin()).toBe(2000);
  });

  it('rates multiple columns', () => {
    const r = makeMultiSeries().rate(['cpu', 'mem']);
    expect(r.length).toBe(3);
    expect(r.at(0)?.get('cpu')).toBeUndefined();
    expect(r.at(0)?.get('mem')).toBeUndefined();
    // t=1000→2000 (1s), cpu 10→30 = 20/s, mem 100→150 = 50/s
    expect(r.at(1)?.get('cpu')).toBe(20);
    expect(r.at(1)?.get('mem')).toBe(50);
    // t=2000→4000 (2s), cpu 30→60 = 15/s, mem 150→130 = -10/s
    expect(r.at(2)?.get('cpu')).toBe(15);
    expect(r.at(2)?.get('mem')).toBe(-10);
  });

  it('handles zero time gap', () => {
    const ts = new TimeSeries({
      name: 'dup',
      schema,
      rows: [
        [1000, 10, 'a'],
        [1000, 20, 'a'],
      ],
    });
    const r = ts.rate('value');
    expect(r.at(1)?.get('value')).toBeUndefined();
  });

  it('handles empty series', () => {
    const empty = new TimeSeries({ name: 'e', schema, rows: [] });
    const r = empty.rate('value');
    expect(r.length).toBe(0);
  });

  it('handles single-event series', () => {
    const single = new TimeSeries({
      name: 's',
      schema,
      rows: [[1000, 10, 'a']],
    });
    const r = single.rate('value');
    expect(r.length).toBe(1);
    expect(r.at(0)?.get('value')).toBeUndefined();
  });

  it('composes with groupBy', () => {
    const ts = new TimeSeries({
      name: 'multi-host',
      schema,
      rows: [
        [1000, 10, 'a'],
        [1000, 100, 'b'],
        [2000, 30, 'a'],
        [2000, 200, 'b'],
      ],
    });
    const groups = ts.groupBy('host', (group) => group.rate('value'));
    const a = groups.get('a')!;
    const b = groups.get('b')!;
    expect(a.at(1)?.get('value')).toBe(20);
    expect(b.at(1)?.get('value')).toBe(100);
  });
});
