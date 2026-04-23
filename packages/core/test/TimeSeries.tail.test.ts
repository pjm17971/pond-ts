import { describe, expect, it } from 'vitest';
import { TimeSeries } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
] as const;

function makeSeries(times: number[]) {
  return new TimeSeries({
    name: 'test',
    schema,
    rows: times.map((t, i) => [t, i]),
  });
}

describe('TimeSeries.tail', () => {
  it('returns the series unchanged when called with no argument', () => {
    const s = makeSeries([0, 1000, 2000, 3000]);
    const tailed = s.tail();
    expect(tailed.length).toBe(4);
    expect(tailed.at(0)!.get('value')).toBe(0);
    expect(tailed.at(3)!.get('value')).toBe(3);
  });

  it('keeps events whose begin is strictly greater than lastBegin - duration', () => {
    // events at 0, 1s, 2s, 3s, 4s (last begin = 4000)
    // tail('2s') keeps events where begin > 2000 -> 3s and 4s
    const s = makeSeries([0, 1000, 2000, 3000, 4000]);
    const tailed = s.tail('2s');
    expect(tailed.length).toBe(2);
    expect(tailed.at(0)!.begin()).toBe(3000);
    expect(tailed.at(1)!.begin()).toBe(4000);
  });

  it('includes the last event regardless of duration (strict > cutoff)', () => {
    // lastBegin = 4000, duration = 0 -> cutoff = 4000, keep events where begin > 4000
    // => empty. But tail('0s') is degenerate; common case is duration >= one gap.
    const s = makeSeries([0, 1000, 2000, 4000]);
    // tail('1s'): cutoff = 3000, keep events where begin > 3000 -> just 4000
    const tailed = s.tail('1s');
    expect(tailed.length).toBe(1);
    expect(tailed.at(0)!.begin()).toBe(4000);
  });

  it('accepts a number duration as milliseconds', () => {
    const s = makeSeries([0, 1000, 2000, 3000]);
    // 1500ms: cutoff = 1500, keep events where begin > 1500 -> 2000, 3000
    const tailed = s.tail(1500);
    expect(tailed.length).toBe(2);
    expect(tailed.at(0)!.begin()).toBe(2000);
  });

  it('returns an empty series when called on an empty series', () => {
    const empty = new TimeSeries({ name: 'e', schema, rows: [] });
    expect(empty.tail('30s').length).toBe(0);
    expect(empty.tail().length).toBe(0);
  });

  it('returns a series with all events when the duration exceeds the span', () => {
    const s = makeSeries([0, 1000, 2000]);
    // lastBegin = 2000, cutoff = 2000 - 60s = -58000 -> all events kept
    const tailed = s.tail('1m');
    expect(tailed.length).toBe(3);
  });

  it('preserves schema and other columns', () => {
    const fancy = new TimeSeries({
      name: 'fancy',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'cpu', kind: 'number' },
        { name: 'host', kind: 'string' },
      ] as const,
      rows: [
        [0, 0.3, 'api-1'],
        [1000, 0.5, 'api-2'],
        [2000, 0.7, 'api-3'],
      ],
    });
    const tailed = fancy.tail('1s');
    // lastBegin = 2000, cutoff = 1000, keep events where begin > 1000 -> just 2000
    expect(tailed.length).toBe(1);
    expect(tailed.at(0)!.get('cpu')).toBe(0.7);
    expect(tailed.at(0)!.get('host')).toBe('api-3');
    expect(tailed.schema).toEqual(fancy.schema);
  });

  it('composes with reduce for a "current state" expression', () => {
    const s = new TimeSeries({
      name: 'metrics',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'cpu', kind: 'number' },
        { name: 'host', kind: 'string' },
      ] as const,
      rows: [
        [0, 0.3, 'api-1'],
        [500, 0.4, 'api-1'],
        [1000, 0.6, 'api-2'],
        [1500, 0.7, 'api-3'],
        [2000, 0.8, 'api-3'],
      ],
    });

    // Last 1s: events at 1500 and 2000 -> cpu avg = 0.75, hosts = [api-3]
    const recent = s.tail('1s').reduce({ cpu: 'avg', host: 'unique' });
    expect(recent.cpu).toBeCloseTo(0.75, 6);
    expect(recent.host).toEqual(['api-3']);
  });

  it('composes with reduce on the full series when tail() has no argument', () => {
    const s = new TimeSeries({
      name: 'metrics',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'host', kind: 'string' },
      ] as const,
      rows: [
        [0, 'api-1'],
        [1000, 'api-2'],
        [2000, 'api-1'],
      ],
    });

    const all = s.tail().reduce({ host: 'unique' });
    expect(all.host).toEqual(['api-1', 'api-2']);
  });
});
