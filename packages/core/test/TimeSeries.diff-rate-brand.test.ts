import { describe, expect, it } from 'vitest';
import { TimeSeries } from '../src/index.js';

// Regression: `diff` / `rate` / `pctChange` used to fail on series built
// via `#fromTrustedEvents` (filter, select, slice, etc.) because the
// internal helper was a JS-`#`-private method, which fails the brand
// check when the receiver was created via Object.create rather than the
// constructor. Fixed by making the helper TS-private (compile-only).

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'value', kind: 'number' },
] as const;

function makeSeries() {
  return new TimeSeries({
    name: 't',
    schema,
    rows: [
      [0, 1],
      [60_000, 2],
      [120_000, 3],
    ],
  });
}

describe('diff/rate/pctChange brand check', () => {
  it('diff() works on series built by filter()', () => {
    const out = makeSeries()
      .filter(() => true)
      .diff('value');
    // First event has undefined diff (no previous); subsequent are
    // 1 (2-1) and 1 (3-2). Pin the values to prove the impl actually
    // ran, not just that no error was thrown.
    expect(out.length).toBe(3);
    expect(out.at(0)?.get('value')).toBeUndefined();
    expect(out.at(1)?.get('value')).toBe(1);
    expect(out.at(2)?.get('value')).toBe(1);
  });

  it('rate() works on series built by select()', () => {
    // 60s gap between events; rate = delta / (gap-in-seconds) = 1/60.
    const out = makeSeries().select('value').rate('value');
    expect(out.length).toBe(3);
    expect(out.at(0)?.get('value')).toBeUndefined();
    expect(out.at(1)?.get('value')).toBeCloseTo(1 / 60, 5);
    expect(out.at(2)?.get('value')).toBeCloseTo(1 / 60, 5);
  });

  it('pctChange() works on series built by slice()', () => {
    // pctChange = (curr - prev) / prev: (2-1)/1 = 1, (3-2)/2 = 0.5
    const out = makeSeries().slice(0, 3).pctChange('value');
    expect(out.length).toBe(3);
    expect(out.at(0)?.get('value')).toBeUndefined();
    expect(out.at(1)?.get('value')).toBeCloseTo(1, 5);
    expect(out.at(2)?.get('value')).toBeCloseTo(0.5, 5);
  });

  it('chained: filter().diff().rate() works', () => {
    // diff produces [undef, 1, 1]; rate of that is [undef, undef, 0]
    // (rate needs two consecutive non-undef numeric values).
    const out = makeSeries()
      .filter(() => true)
      .diff('value')
      .rate('value');
    expect(out.length).toBe(3);
    expect(out.at(2)?.get('value')).toBe(0);
  });
});
