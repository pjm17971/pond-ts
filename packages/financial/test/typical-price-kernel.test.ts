import { describe, expect, it } from 'vitest';
import { typicalPriceValues } from '../src/kernels/typical-price.js';

const arr = (...xs: number[]) => Float64Array.from(xs);
const read = (x: Float64Array) =>
  Array.from(x, (v) => (Number.isNaN(v) ? undefined : v));

describe('typicalPriceValues', () => {
  it('is (high + low + close) / 3', () => {
    // Asymmetric bar, so a (high + low) / 2 midpoint (10.5) or a plain close
    // (10) would both read differently from the answer.
    expect(read(typicalPriceValues(arr(12), arr(9), arr(10)))).toEqual([
      31 / 3,
    ]);
  });

  it('is missing when any of the three prices is', () => {
    expect(
      read(
        typicalPriceValues(
          arr(NaN, 12, 12, 12),
          arr(9, NaN, 9, 9),
          arr(10, 10, NaN, 10),
        ),
      ),
    ).toEqual([undefined, undefined, undefined, 31 / 3]);
  });
});
