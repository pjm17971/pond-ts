import { describe, expect, it } from 'vitest';
import { signedVolumeValues } from '../src/kernels/signed-volume.js';

/*
 * The per-bar term OBV accumulates, tested before the sum so that each rule
 * is pinned where it lives: the seed convention, the three signs, and which
 * bars a gap in each input costs. Through `obv` alone the last of these is
 * invisible — the running sum turns any interior gap into "missing from here
 * on", so a kernel that charged the wrong bar would pass every study test.
 */

const arr = (...xs: number[]) => Float64Array.from(xs);
const read = (x: Float64Array) =>
  Array.from(x, (v) => (Number.isNaN(v) ? undefined : v));

describe('signedVolumeValues', () => {
  it('seeds the first bar with its whole volume (TA-Lib convention)', () => {
    // Bar 0 has no previous close. TA-Lib counts it as accumulation; a
    // kernel that emitted NaN or 0 there would sit volume[0] below TA-Lib
    // for the whole series.
    expect(read(signedVolumeValues(arr(10, 11), arr(100, 200)))).toEqual([
      100, 200,
    ]);
  });

  it('carries +volume on an up-close, −volume on a down-close, 0 unchanged', () => {
    expect(
      read(signedVolumeValues(arr(10, 11, 11, 9), arr(100, 200, 300, 400))),
    ).toEqual([100, 200, 0, -400]);
  });

  it('shifts the seed past a leading gap in close', () => {
    expect(
      read(signedVolumeValues(arr(NaN, 11, 9), arr(100, 200, 300))),
    ).toEqual([undefined, 200, -300]);
  });

  it('shifts the seed past a leading gap in volume, and seeds on that bar', () => {
    // Close FALLS from bar 0 to bar 1. An implementation that skipped only
    // the volume gap and kept the direction would emit −200 at bar 1; the
    // seed is the bar's whole volume regardless, as TA-Lib's wrapper does
    // once it has stripped the leading NaN.
    expect(
      read(signedVolumeValues(arr(10, 9, 12), arr(NaN, 200, 300))),
    ).toEqual([undefined, 200, 300]);
  });

  it('a missing close costs its own bar AND the next', () => {
    // The next bar's sign is taken against the missing close.
    expect(
      read(
        signedVolumeValues(
          arr(10, 11, NaN, 12, 13),
          arr(100, 100, 100, 100, 100),
        ),
      ),
    ).toEqual([100, 100, undefined, undefined, 100]);
  });

  it('a missing volume costs only its own bar', () => {
    expect(
      read(signedVolumeValues(arr(10, 11, 12, 13), arr(100, NaN, 100, 100))),
    ).toEqual([100, undefined, 100, 100]);
  });

  it('a flat bar with missing volume is unknown, not zero', () => {
    // "Unknown times zero" is a rule nobody should have to know; documented
    // on the kernel as the one place this diverges from TA-Lib's arithmetic.
    expect(
      read(signedVolumeValues(arr(10, 10, 11), arr(100, NaN, 100))),
    ).toEqual([100, undefined, 100]);
  });

  it('handles an all-gap input and an empty input', () => {
    expect(read(signedVolumeValues(arr(NaN, NaN), arr(1, 2)))).toEqual([
      undefined,
      undefined,
    ]);
    expect(read(signedVolumeValues(arr(), arr()))).toEqual([]);
  });
});
