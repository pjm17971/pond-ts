import { describe, expect, it } from 'vitest';
import { swingIndexValues } from '../src/kernels/swing-index.js';

/*
 * Tested directly for the reasons `true-range-kernel.test.ts` gives: the two
 * studies on this kernel (`swingIndex`, `accumulativeSwingIndex`) cannot
 * reach its degenerate inputs — an empty series never gets past
 * `withColumn`, and the three branches of `R` are easier to pin as bare
 * arrays than as bars.
 */

const arr = (...xs: number[]) => Float64Array.from(xs);
const read = (x: Float64Array) =>
  Array.from(x, (v) => (Number.isNaN(v) ? undefined : v));

describe('swingIndexValues', () => {
  it('returns an empty array for an empty input', () => {
    expect(swingIndexValues(arr(), arr(), arr(), arr(), 5)).toHaveLength(0);
  });

  it('a one-bar input is a single undefined — every term reads yesterday', () => {
    expect(
      read(swingIndexValues(arr(10), arr(12), arr(9), arr(11), 5)),
    ).toEqual([undefined]);
  });

  it('picks the D branch of R when the bar contains the previous close', () => {
    //   A = |14−11| = 3, B = |10−11| = 1, D = 4 → D is the largest
    //   R = 4 + 0.25·|11−10| = 4.25, K = 3, N = 3.25
    const v = read(
      swingIndexValues(arr(10, 11), arr(12, 14), arr(9, 10), arr(11, 13), 5),
    );
    expect(v[1]).toBeCloseTo((50 * 3.25 * 3) / (4.25 * 5), 12);
  });

  it('picks the A branch on a gap UP and the B branch on a gap DOWN', () => {
    // Up: A = 10, B = 9, D = 1 → R = 10 − 4.5 + 0.25 = 5.75, K = 10, N = 10.
    const up = read(
      swingIndexValues(arr(9, 19), arr(11, 20), arr(8, 19), arr(10, 19.5), 20),
    );
    expect(up[1]).toBeCloseTo((50 * 10 * 10) / (5.75 * 20), 12);
    // Down: A = 9, B = 10, D = 1 → R = 10 − 4.5 = 5.5, K = 10, N = −9.25.
    const down = read(
      swingIndexValues(arr(19, 9), arr(20, 10), arr(18, 9), arr(19, 9.5), 20),
    );
    expect(down[1]).toBeCloseTo((50 * -9.25 * 10) / (5.5 * 20), 12);
  });

  it('R = 0 reads undefined rather than dividing', () => {
    // Today's high, today's low, yesterday's close and yesterday's open are
    // all 10 — the only way R can be zero.
    const v = read(
      swingIndexValues(arr(10, 10), arr(11, 10), arr(9, 10), arr(10, 10), 5),
    );
    expect(v[1]).toBeUndefined();
  });

  it('a NaN in any input propagates rather than picking a branch', () => {
    // Every comparison against NaN is false, so the D branch is taken and R
    // is NaN — which is not `=== 0` and therefore falls to the division.
    const v = read(
      swingIndexValues(arr(10, 11), arr(12, NaN), arr(9, 10), arr(11, 13), 5),
    );
    expect(v[1]).toBeUndefined();
  });

  it('is linear in the prices and inversely linear in the limit', () => {
    const base = swingIndexValues(
      arr(10, 11),
      arr(12, 14),
      arr(9, 10),
      arr(11, 13),
      5,
    )[1]!;
    const halved = swingIndexValues(
      arr(10, 11),
      arr(12, 14),
      arr(9, 10),
      arr(11, 13),
      2.5,
    )[1]!;
    expect(halved).toBeCloseTo(base * 2, 12);
    const scaled = swingIndexValues(
      arr(20, 22),
      arr(24, 28),
      arr(18, 20),
      arr(22, 26),
      5,
    )[1]!;
    expect(scaled).toBeCloseTo(base * 2, 12);
  });
});
