import { describe, expect, it } from 'vitest';
import { upDownLegValues } from '../src/kernels/up-down.js';

/*
 * The gain/loss split three studies share (`rsi`, `chandeMomentum`,
 * `intradayMomentumIndex`). Two of its three cases are the interesting ones
 * and neither is what the obvious one-liner gives: a bar that did not move
 * contributes a real 0 to both legs (so a flat window sums to 0/0 and reads
 * as NO ANSWER rather than as a reading of zero), and a bar whose change is
 * unknown contributes NaN to both (so the rolling kernels' array door blanks
 * the windows containing it rather than treating it as flat).
 *
 * `Math.max(d, 0)` gets the second right and needs a branch for nothing;
 * `d > 0 ? d : 0` gets the first right and silently maps an unknown change
 * to a flat bar. The kernel does both, which is why it is a kernel.
 */

const arr = (...xs: number[]) => Float64Array.from(xs);
const read = (x: Float64Array) =>
  Array.from(x, (v) => (Number.isNaN(v) ? undefined : v));

describe('upDownLegValues', () => {
  it('splits a change into its two non-negative legs', () => {
    const { up, down } = upDownLegValues(arr(2, -3, 0, 1.5, -0.5));
    expect(read(up)).toEqual([2, 0, 0, 1.5, 0]);
    expect(read(down)).toEqual([0, 3, 0, 0, 0.5]);
  });

  it('gives a flat bar an honest 0 on BOTH legs, not a gap', () => {
    const { up, down } = upDownLegValues(arr(0, 0));
    expect(read(up)).toEqual([0, 0]);
    expect(read(down)).toEqual([0, 0]);
  });

  it('gives an unknown change NaN on BOTH legs, not 0', () => {
    // The distinction the whole kernel exists for: `d > 0 ? d : 0` would
    // report a missing bar as flat, which reads downstream as a definite
    // "no movement" instead of "no answer".
    const { up, down } = upDownLegValues(arr(1, NaN, -1));
    expect(read(up)).toEqual([1, undefined, 0]);
    expect(read(down)).toEqual([0, undefined, 1]);
  });

  it('sums to |change| and differences to the change on every bar', () => {
    // up + down = |d| and up − down = d, which together say the split lost
    // nothing — the property both consumers rely on.
    const deltas = arr(3, -4, 0, 2.25, -0.75, 7);
    const { up, down } = upDownLegValues(deltas);
    for (let i = 0; i < deltas.length; i += 1) {
      expect(up[i]! + down[i]!).toBeCloseTo(Math.abs(deltas[i]!), 12);
      expect(up[i]! - down[i]!).toBeCloseTo(deltas[i]!, 12);
    }
  });

  it('returns fresh arrays of the input length, empty input included', () => {
    const { up, down } = upDownLegValues(arr());
    expect(up).toHaveLength(0);
    expect(down).toHaveLength(0);
    const legs = upDownLegValues(arr(1, 2));
    expect(legs.up).not.toBe(legs.down);
    expect(legs.up).toHaveLength(2);
  });
});
