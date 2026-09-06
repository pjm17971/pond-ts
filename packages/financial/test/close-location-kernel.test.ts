import { describe, expect, it } from 'vitest';
import {
  accumulationDistributionValues,
  clvValues,
} from '../src/kernels/close-location.js';

const arr = (...xs: number[]) => Float64Array.from(xs);
const read = (x: Float64Array) =>
  Array.from(x, (v) => (Number.isNaN(v) ? undefined : v));

describe('clvValues', () => {
  it('is +1 on the high, −1 on the low, 0 at the midpoint', () => {
    expect(
      read(clvValues(arr(12, 12, 12), arr(10, 10, 10), arr(12, 10, 11))),
    ).toEqual([1, -1, 0]);
  });

  it('grades the close’s position inside the range', () => {
    // Three quarters of the way up a range of 4: 2·(0.75) − 1 = +0.5.
    expect(read(clvValues(arr(14), arr(10), arr(13)))).toEqual([0.5]);
    expect(read(clvValues(arr(14), arr(10), arr(11)))).toEqual([-0.5]);
  });

  it('reads the LOCATION, not the direction: it never sees a previous bar', () => {
    // Two identical bars: whatever the close did between them, the value is
    // the same. (A study that wanted the direction has `signedVolumeValues`.)
    const out = read(clvValues(arr(14, 14), arr(10, 10), arr(13, 13)));
    expect(out[0]).toBe(out[1]);
  });

  it('is 0 on a flat bar — the numerator is exactly zero, and TA-Lib’s AD agrees', () => {
    // Unlike a stochastic's flat WINDOW, a flat bar's `(c − l) − (h − c)` is
    // forced to zero, so 0 is the value, not a convention. A missing close on
    // a flat bar is still a gap.
    expect(read(clvValues(arr(10, 14), arr(10, 10), arr(10, 13)))).toEqual([
      0, 0.5,
    ]);
    expect(read(clvValues(arr(10), arr(10), arr(NaN)))).toEqual([undefined]);
  });

  it('is missing when any of the three prices is', () => {
    expect(
      read(
        clvValues(
          arr(NaN, 14, 14, 14),
          arr(10, NaN, 10, 10),
          arr(13, 13, NaN, 13),
        ),
      ),
    ).toEqual([undefined, undefined, undefined, 0.5]);
  });

  it('does not clamp a close outside its own range', () => {
    // Reachable when a caller redirects `close` at a smoothed column; the
    // honest answer is outside [−1, +1], not a clipped one.
    expect(read(clvValues(arr(14), arr(10), arr(16)))).toEqual([2]);
  });
});

describe('accumulationDistributionValues', () => {
  it('accumulates CLV × volume from bar 0', () => {
    const out = accumulationDistributionValues(
      arr(12, 14, 16),
      arr(10, 12, 14),
      arr(11.5, 12.5, 16),
      arr(100, 200, 300),
    );
    expect(read(out)).toEqual([50, -50, 250]);
  });

  it('steps a leading gap over and propagates an interior one', () => {
    const lead = accumulationDistributionValues(
      arr(NaN, 14, 16),
      arr(10, 12, 14),
      arr(11.5, 12.5, 16),
      arr(100, 200, 300),
    );
    expect(read(lead)).toEqual([undefined, -100, 200]);

    const hole = accumulationDistributionValues(
      arr(12, 14, 16),
      arr(10, 12, 14),
      arr(11.5, 12.5, 16),
      arr(100, NaN, 300),
    );
    expect(read(hole)).toEqual([50, undefined, undefined]);
  });

  it('a flat bar adds 0 and the line carries on — as TA-Lib’s AD does', () => {
    const out = accumulationDistributionValues(
      arr(12, 13, 16),
      arr(10, 13, 14),
      arr(11.5, 13, 16),
      arr(100, 200, 300),
    );
    expect(read(out)).toEqual([50, 50, 350]);
  });

  it('is all-missing on all-missing input, length kept', () => {
    const out = accumulationDistributionValues(
      arr(NaN, NaN),
      arr(NaN, NaN),
      arr(NaN, NaN),
      arr(NaN, NaN),
    );
    expect(out).toHaveLength(2);
    expect(read(out)).toEqual([undefined, undefined]);
  });
});
