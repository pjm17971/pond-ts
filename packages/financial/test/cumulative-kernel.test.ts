import { describe, expect, it } from 'vitest';
import { cumulativeValues } from '../src/kernels/cumulative.js';

/*
 * The accumulation half of OBV, tested on its own because it is SHARED: the
 * Accumulation/Distribution line and Price-Volume Trend accumulate the same
 * way, and an anchored VWAP is a ratio of two of these. What is worth
 * pinning is the gap rule — a running sum is the one shape where an interior
 * gap has no local answer — and that a leading gap shifts the start rather
 * than emptying the output.
 */

const arr = (...xs: number[]) => Float64Array.from(xs);
const read = (x: Float64Array) =>
  Array.from(x, (v) => (Number.isNaN(v) ? undefined : v));

describe('cumulativeValues', () => {
  it('is the running sum, signs included', () => {
    expect(read(cumulativeValues(arr(1, 2, 3, -4)))).toEqual([1, 3, 6, 2]);
  });

  it('starts at the first finite term when the head is missing', () => {
    // The chaining case: a source whose own warm-up leaves missing rows at
    // the head must delay the sum, not poison it.
    expect(read(cumulativeValues(arr(NaN, NaN, 2, 3)))).toEqual([
      undefined,
      undefined,
      2,
      5,
    ]);
  });

  it('propagates an INTERIOR gap to the end', () => {
    // Every value after an unknown term is a known sum plus an unknown.
    // Skipping the term would report a level silently off by it forever.
    expect(read(cumulativeValues(arr(1, 2, NaN, 3, 4)))).toEqual([
      1,
      3,
      undefined,
      undefined,
      undefined,
    ]);
  });

  it('handles an all-gap input and an empty input', () => {
    expect(read(cumulativeValues(arr(NaN, NaN)))).toEqual([
      undefined,
      undefined,
    ]);
    expect(read(cumulativeValues(arr()))).toEqual([]);
  });
});
