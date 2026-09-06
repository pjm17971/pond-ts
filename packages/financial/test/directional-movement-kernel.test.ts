import { describe, expect, it } from 'vitest';
import {
  directionalMovementValues,
  vortexMovementValues,
} from '../src/kernels/directional-movement.js';

/*
 * Tested directly, for the reason `trueRangeValues` is: the studies on these
 * kernels smooth them before reporting, so a study-level fixture cannot show
 * which branch of the DM split fired on which bar. Every case below is a bar
 * shape the split has to get right — an outside bar, an inside bar, an exact
 * tie — and each one distinguishes the shipped rule from a plausible wrong
 * one (`Math.max(up, 0)`, a non-strict comparison, a missing NaN guard).
 */

const arr = (...xs: number[]) => Float64Array.from(xs);
const read = (x: Float64Array) =>
  Array.from(x, (v) => (Number.isNaN(v) ? undefined : v));

describe('directionalMovementValues', () => {
  it('takes the up move when it is the larger excursion', () => {
    // High +3, low +1 → up 3 beats dn −1.
    const { plus, minus } = directionalMovementValues(arr(10, 13), arr(8, 9));
    expect(read(plus)[1]).toBeCloseTo(3, 12);
    expect(read(minus)[1]).toBeCloseTo(0, 12);
  });

  it('takes the down move when it is the larger excursion', () => {
    const { plus, minus } = directionalMovementValues(arr(13, 12), arr(9, 5));
    expect(read(plus)[1]).toBeCloseTo(0, 12); // up = −1
    expect(read(minus)[1]).toBeCloseTo(4, 12); // dn = 9 − 5
  });

  it('an OUTSIDE bar counts only its larger side, never both', () => {
    // High +2 and low −5: `Math.max(up, 0)` would report +2 here as well,
    // which would make +DI + −DI stop being the total directional movement.
    const { plus, minus } = directionalMovementValues(arr(10, 12), arr(8, 3));
    expect(read(plus)[1]).toBeCloseTo(0, 12);
    expect(read(minus)[1]).toBeCloseTo(5, 12);
  });

  it('an INSIDE bar moves in neither direction', () => {
    const { plus, minus } = directionalMovementValues(arr(20, 19), arr(10, 11));
    expect(read(plus)[1]).toBe(0);
    expect(read(minus)[1]).toBe(0);
  });

  it('an inside bar reports nothing even when one side "moved less"', () => {
    // The high fell 0.5 and the low rose 1, so `up > dn` holds with BOTH
    // negative — the one shape where dropping the `up > 0` sign guard would
    // report a NEGATIVE +DM. (A mutation run found this uncovered: the
    // mirror case on the −DM leg was, and this one was not.)
    const { plus, minus } = directionalMovementValues(
      arr(20, 19.5),
      arr(10, 11),
    );
    expect(read(plus)[1]).toBe(0);
    expect(read(minus)[1]).toBe(0);
    // …and the mirror: the low rose less than the high fell.
    const mirror = directionalMovementValues(arr(20, 19), arr(10, 10.5));
    expect(read(mirror.plus)[1]).toBe(0);
    expect(read(mirror.minus)[1]).toBe(0);
  });

  it('an exact tie is zero on BOTH legs', () => {
    // up = dn = 2. Both comparisons are strict, so neither leg fires — the
    // case a `>=` anywhere would break.
    const { plus, minus } = directionalMovementValues(arr(10, 12), arr(8, 6));
    expect(read(plus)[1]).toBe(0);
    expect(read(minus)[1]).toBe(0);
  });

  it('an unmoved bar is zero movement, not missing', () => {
    const { plus, minus } = directionalMovementValues(arr(10, 10), arr(8, 8));
    expect(read(plus)[1]).toBe(0);
    expect(read(minus)[1]).toBe(0);
  });

  it('a missing extreme is UNKNOWN movement, not zero — for two bars', () => {
    // The guard is the point: every comparison against NaN is false, so an
    // unguarded version would fall through to `else 0` and report a flat bar
    // where there is no answer. The gap costs its own bar and the next one.
    const { plus, minus } = directionalMovementValues(
      arr(10, 12, NaN, 15, 16),
      arr(8, 9, 10, 11, 12),
    );
    expect(read(plus)[1]).toBeCloseTo(2, 12);
    expect(read(plus)[2]).toBeUndefined();
    expect(read(plus)[3]).toBeUndefined();
    expect(read(plus)[4]).toBeCloseTo(1, 12);
    expect(read(minus)[2]).toBeUndefined();
    expect(read(minus)[3]).toBeUndefined();
  });

  it('has no movement on the first bar, and returns empty for an empty input', () => {
    const { plus, minus } = directionalMovementValues(arr(10, 11), arr(8, 9));
    expect(read(plus)[0]).toBeUndefined();
    expect(read(minus)[0]).toBeUndefined();
    const empty = directionalMovementValues(arr(), arr());
    expect(empty.plus).toHaveLength(0);
    expect(empty.minus).toHaveLength(0);
  });
});

describe('vortexMovementValues', () => {
  it('crosses the bars: |high − prevLow| and |low − prevHigh|', () => {
    const { plus, minus } = vortexMovementValues(arr(10, 13), arr(8, 9));
    expect(read(plus)[1]).toBeCloseTo(5, 12); // |13 − 8|
    expect(read(minus)[1]).toBeCloseTo(1, 12); // |9 − 10|
  });

  it('both legs are positive even on an inside bar — unlike +DM/−DM', () => {
    // The contrast the shared file exists for: this bar reports nothing to
    // Wilder's split and two real distances to the vortex.
    const inside = { high: arr(20, 19), low: arr(10, 11) };
    const dm = directionalMovementValues(inside.high, inside.low);
    const vm = vortexMovementValues(inside.high, inside.low);
    expect(read(dm.plus)[1]).toBe(0);
    expect(read(dm.minus)[1]).toBe(0);
    expect(read(vm.plus)[1]).toBeCloseTo(9, 12); // |19 − 10|
    expect(read(vm.minus)[1]).toBeCloseTo(9, 12); // |11 − 20|
  });

  it('the absolute value matters on a bar that gaps clear of its predecessor', () => {
    // Bar 1 sits entirely below bar 0, so `high − prevLow` is negative; the
    // published definition takes its magnitude.
    const { plus } = vortexMovementValues(arr(20, 12), arr(15, 10));
    expect(read(plus)[1]).toBeCloseTo(3, 12); // |12 − 15|
  });

  it('a missing extreme costs its own bar and the next, and bar 0 has none', () => {
    const { plus, minus } = vortexMovementValues(
      arr(10, 12, NaN, 15),
      arr(8, 9, 10, 11),
    );
    expect(read(plus)[0]).toBeUndefined();
    expect(read(plus)[2]).toBeUndefined(); // reads its own high
    expect(read(plus)[3]).toBeCloseTo(5, 12); // |15 − 10| — the low is intact
    expect(read(minus)[3]).toBeUndefined(); // reads bar 2's high
    const empty = vortexMovementValues(arr(), arr());
    expect(empty.plus).toHaveLength(0);
  });
});
