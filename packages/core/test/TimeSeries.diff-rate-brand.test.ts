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
    expect(out.length).toBe(3);
  });

  it('rate() works on series built by select()', () => {
    const out = makeSeries().select('value').rate('value');
    expect(out.length).toBe(3);
  });

  it('pctChange() works on series built by slice()', () => {
    const out = makeSeries().slice(0, 3).pctChange('value');
    expect(out.length).toBe(3);
  });

  it('chained: filter().diff().rate() works', () => {
    const out = makeSeries()
      .filter(() => true)
      .diff('value')
      .rate('value');
    expect(out.length).toBe(3);
  });
});
