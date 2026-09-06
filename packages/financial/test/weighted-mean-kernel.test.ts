import { describe, expect, it } from 'vitest';
import {
  rollingWeightedMeanValues,
  symmetricWeightedValues,
} from '../src/kernels/weighted-mean.js';

/*
 * The kernel under VWAP (and a future VWMA), tested on its own. Every
 * fixture here has UNEQUAL weights — with equal weights a weighted mean is
 * the plain mean and the test cannot tell whether the weighting ran. The
 * gap cases are the ones `vwap`'s oracle cannot reach (its input is
 * gap-free): a gap on either side must drop the row from BOTH sums.
 */

const arr = (...xs: number[]) => Float64Array.from(xs);
const read = (x: Float64Array) =>
  Array.from(x, (v) => (Number.isNaN(v) ? undefined : v));

describe('rollingWeightedMeanValues', () => {
  it('weights, rather than averaging', () => {
    // (10·1 + 20·3) / 4 = 17.5; the plain mean is 15.
    const out = read(rollingWeightedMeanValues(arr(10, 20), arr(1, 3), 2));
    expect(out[0]).toBeUndefined();
    expect(out[1]).toBeCloseTo(17.5, 12);
  });

  it('slides: each window weights only its own rows', () => {
    const out = read(
      rollingWeightedMeanValues(arr(10, 20, 30), arr(1, 3, 6), 2),
    );
    expect(out[1]).toBeCloseTo(17.5, 12); // (10 + 60) / 4
    expect(out[2]).toBeCloseTo((60 + 180) / 9, 12); // bar 0 has left
  });

  it('is the value itself at period 1', () => {
    expect(
      read(rollingWeightedMeanValues(arr(10, 20, 30), arr(1, 5, 9), 1)),
    ).toEqual([10, 20, 30]);
  });

  it('is missing when the window has no weight, and resumes when it does', () => {
    // Nothing to weight by means no answer — and not the plain mean. (With
    // non-negative weights this is 0/0, which is NaN without a guard; the
    // test pins the behaviour, not a branch.)
    const out = read(
      rollingWeightedMeanValues(arr(10, 20, 30), arr(0, 0, 5), 2),
    );
    expect(out[1]).toBeUndefined();
    expect(out[2]).toBeCloseTo(30, 12); // (0·20 + 5·30) / 5
  });

  it('drops a row with a missing VALUE from the denominator too', () => {
    // The middle row carries almost all the weight. If its weight stayed in
    // the denominator the answer would be 40 / 102 ≈ 0.39; dropping the row
    // from both sums gives the weighted mean of the two rows that exist.
    const out = read(
      rollingWeightedMeanValues(arr(10, NaN, 30), arr(1, 100, 3), 3),
    );
    expect(out[2]).toBeCloseTo((10 + 90) / 4, 12);
  });

  it('drops a row with a missing WEIGHT from the numerator too', () => {
    const out = read(
      rollingWeightedMeanValues(arr(10, 20, 30), arr(1, NaN, 3), 3),
    );
    expect(out[2]).toBeCloseTo((10 + 90) / 4, 12);
  });

  it('is missing on a window with no finite row at all', () => {
    const out = read(
      rollingWeightedMeanValues(arr(NaN, NaN, 30), arr(1, 2, 3), 2),
    );
    expect(out[1]).toBeUndefined();
    expect(out[2]).toBeCloseTo(30, 12);
  });

  it('is all-missing when the period exceeds the rows', () => {
    expect(read(rollingWeightedMeanValues(arr(10, 20), arr(1, 3), 5))).toEqual([
      undefined,
      undefined,
    ]);
  });
});

/*
 * Ehlers' 4-bar symmetric filter — the Relative Vigor Index's smoother. The
 * thing worth pinning is that it is SYMMETRIC: a linear `wma(4)` has the
 * same width and the same normalisation and a completely different impulse
 * response, so a fixture that could not tell them apart would let the wrong
 * filter ship.
 */
describe('symmetricWeightedValues', () => {
  it('is (x + 2x₋₁ + 2x₋₂ + x₋₃)/6, hand-computed', () => {
    const out = read(symmetricWeightedValues(arr(1, 2, 3, 4, 5)));
    expect(out.slice(0, 3)).toEqual([undefined, undefined, undefined]);
    expect(out[3]).toBeCloseTo((4 + 2 * 3 + 2 * 2 + 1) / 6, 12); // 15/6
    expect(out[4]).toBeCloseTo((5 + 2 * 4 + 2 * 3 + 2) / 6, 12); // 21/6
  });

  it('answers a unit spike with 1, 2, 2, 1 — not wma(4)’s 4, 3, 2, 1', () => {
    const out = read(symmetricWeightedValues(arr(0, 0, 0, 6, 0, 0, 0, 0)));
    expect(out[3]).toBeCloseTo(1, 12);
    expect(out[4]).toBeCloseTo(2, 12);
    expect(out[5]).toBeCloseTo(2, 12);
    expect(out[6]).toBeCloseTo(1, 12);
    expect(out[7]).toBeCloseTo(0, 12);
  });

  it('returns a constant unchanged (the weights sum to 6, and it divides by 6)', () => {
    const out = read(symmetricWeightedValues(arr(7, 7, 7, 7, 7)));
    expect(out[3]).toBeCloseTo(7, 12);
    expect(out[4]).toBeCloseTo(7, 12);
  });

  it('blanks the gap bar and the three after it — a positional weight cannot skip a cell', () => {
    const out = read(symmetricWeightedValues(arr(1, 1, 1, NaN, 1, 1, 1, 1)));
    expect(out.slice(0, 7).every((x) => x === undefined)).toBe(true);
    expect(out[7]).toBeCloseTo(1, 12);
  });

  it('is all-missing on an input shorter than the 4-bar window', () => {
    expect(read(symmetricWeightedValues(arr(1, 2, 3)))).toEqual([
      undefined,
      undefined,
      undefined,
    ]);
    expect(read(symmetricWeightedValues(arr()))).toEqual([]);
  });
});
