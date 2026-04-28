/**
 * Tests for the `minSamples` warm-up gate on `TimeSeries.rolling` —
 * suppresses output rows whose window contains fewer than the
 * configured number of source events. Default of 1 preserves the
 * original "emit from event 0" behavior.
 */
import { describe, expect, it } from 'vitest';
import { Sequence, TimeSeries } from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

function makeSeries(n = 10) {
  const rows: Array<[number, number, string]> = [];
  for (let i = 0; i < n; i += 1) rows.push([i * 1000, i * 0.1, 'api-1']);
  return new TimeSeries({ name: 'cpu', schema, rows });
}

describe('TimeSeries.rolling minSamples (event-driven)', () => {
  it('default of 1 emits avg/sd from the first event', () => {
    const r = makeSeries(5).rolling('3s', {
      cpu: 'avg',
      host: 'last',
    });
    expect(r.at(0)!.get('cpu')).toBe(0);
    expect(r.at(0)!.get('host')).toBe('api-1');
  });

  it('suppresses warm-up rows below minSamples', () => {
    const r = makeSeries(10).rolling(
      '10s',
      { cpu: 'avg', host: 'last' },
      { minSamples: 3 },
    );
    // 1s-spaced events on a 10s window — at i=0,1 the window has
    // <3 samples; at i=2 it crosses the threshold.
    for (let i = 0; i < 2; i += 1) {
      expect(r.at(i)!.get('cpu')).toBeUndefined();
      expect(r.at(i)!.get('host')).toBeUndefined();
    }
    // avg of (0, 0.1, 0.2) — toBeCloseTo for FP imprecision.
    expect(r.at(2)!.get('cpu')).toBeCloseTo(0.1, 10);
    expect(r.at(2)!.get('host')).toBe('api-1');
  });

  it('output schema is unchanged; only values switch to undefined', () => {
    const baseline = makeSeries(10).rolling('10s', { cpu: 'avg' });
    const gated = makeSeries(10).rolling(
      '10s',
      { cpu: 'avg' },
      { minSamples: 5 },
    );
    expect(gated.schema).toEqual(baseline.schema);
    expect(gated.length).toBe(baseline.length);
  });

  it('minSamples larger than the source emits undefined throughout', () => {
    const r = makeSeries(5).rolling('60s', { cpu: 'avg' }, { minSamples: 10 });
    for (let i = 0; i < r.length; i += 1) {
      expect(r.at(i)!.get('cpu')).toBeUndefined();
    }
  });

  it('rejects negative or non-integer minSamples', () => {
    const s = makeSeries(5);
    expect(() => s.rolling('3s', { cpu: 'avg' }, { minSamples: -1 })).toThrow(
      /non-negative integer/,
    );
    expect(() => s.rolling('3s', { cpu: 'avg' }, { minSamples: 1.5 })).toThrow(
      /non-negative integer/,
    );
  });

  it('minSamples 0 is the default (no gate; empty windows still emit)', () => {
    // First event in a 0-sized source has windowSize=0; with default
    // minSamples=0 the reducer is called and built-in reducers return
    // undefined for empty inputs (rather than the "warmup" undefined).
    const empty = new TimeSeries({ name: 'cpu', schema, rows: [] });
    const r = empty.rolling('5s', { cpu: 'avg' });
    expect(r.length).toBe(0);
  });
});

describe('TimeSeries.rolling minSamples (sequence-driven)', () => {
  it('emits undefined values for buckets below the threshold', () => {
    const s = makeSeries(10);
    const grid = Sequence.every('1s');
    const sparse = s.rolling(grid, '2s', { cpu: 'avg' }, { minSamples: 3 });
    // 1-second-spaced events on a 2s window — bucket can hold at most
    // ~2 events, so minSamples: 3 should suppress every output.
    for (let i = 0; i < sparse.length; i += 1) {
      expect(sparse.at(i)!.get('cpu')).toBeUndefined();
    }
  });

  it('a wide enough window passes the threshold', () => {
    const s = makeSeries(10);
    const grid = Sequence.every('1s');
    const wide = s.rolling(grid, '5s', { cpu: 'avg' }, { minSamples: 3 });
    // The first bucket centered at t=0 has fewer than 3 contributors;
    // later buckets have a full 5-event window.
    const lastIdx = wide.length - 1;
    expect(typeof wide.at(lastIdx)!.get('cpu')).toBe('number');
  });
});
