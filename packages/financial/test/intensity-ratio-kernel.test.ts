import { describe, expect, it } from 'vitest';
import { shinoharaTermsValues } from '../src/kernels/intensity-ratio.js';

/*
 * Tested directly, because the study can only see the four terms through two
 * rolling sums and a division — which is exactly the shape that hides a
 * swapped pair (`open − low` for `high − open` leaves a plausible-looking
 * ratio). Each term gets a bar where it is the only one that could produce
 * the value.
 */

const arr = (...xs: number[]) => Float64Array.from(xs);
const read = (x: Float64Array) =>
  Array.from(x, (v) => (Number.isNaN(v) ? undefined : v));

describe('shinoharaTermsValues', () => {
  //        open  high  low   close
  // bar 0: 100   104    99    102
  // bar 1: 103   107   101    105
  const open = arr(100, 103);
  const high = arr(104, 107);
  const low = arr(99, 101);
  const close = arr(102, 105);

  it('splits the strong pair around the bar’s OWN open', () => {
    const { strongUp, strongDown } = shinoharaTermsValues(
      open,
      high,
      low,
      close,
    );
    expect(read(strongUp)).toEqual([4, 4]);
    expect(read(strongDown)).toEqual([1, 2]);
  });

  it('splits the weak pair around the PREVIOUS close, so bar 0 is missing', () => {
    const { weakUp, weakDown } = shinoharaTermsValues(open, high, low, close);
    expect(read(weakUp)).toEqual([undefined, 5]); // 107 − 102
    expect(read(weakDown)).toEqual([undefined, 1]); // 102 − 101
  });

  it('goes negative on a gap, rather than clamping', () => {
    // Bar 1 opened above the previous close and never traded back to it, so
    // `prevClose − low` is negative. The study documents what that means.
    const { weakDown } = shinoharaTermsValues(
      arr(100, 110),
      arr(104, 114),
      arr(99, 109),
      arr(100, 112),
    );
    expect(read(weakDown)).toEqual([undefined, -9]);
  });

  it('propagates a missing input through the terms that read it', () => {
    const { strongUp, strongDown, weakUp, weakDown } = shinoharaTermsValues(
      arr(100, NaN, 100),
      arr(104, 107, 106),
      arr(99, 101, 100),
      arr(102, 105, 101),
    );
    // A missing OPEN blanks only the strong pair, and only its own bar.
    expect(read(strongUp)).toEqual([4, undefined, 6]);
    expect(read(strongDown)).toEqual([1, undefined, 0]);
    expect(read(weakUp)).toEqual([undefined, 5, 1]);
    expect(read(weakDown)).toEqual([undefined, 1, 5]);
  });

  it('an empty input gives four empty arrays', () => {
    const t = shinoharaTermsValues(arr(), arr(), arr(), arr());
    for (const v of [t.strongUp, t.strongDown, t.weakUp, t.weakDown]) {
      expect(v).toHaveLength(0);
    }
  });
});
