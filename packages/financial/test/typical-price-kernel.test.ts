import { describe, expect, it } from 'vitest';
import {
  barRangeValues,
  medianPriceValues,
  typicalPriceValues,
} from '../src/kernels/typical-price.js';

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

describe('medianPriceValues', () => {
  it('is (high + low) / 2, and is not the typical price', () => {
    // The same asymmetric bar: the midpoint is 10.5 where the typical price
    // is 31/3, so a caller reaching for the wrong one is visible.
    expect(read(medianPriceValues(arr(12), arr(9)))).toEqual([10.5]);
    expect(read(typicalPriceValues(arr(12), arr(9), arr(10)))).not.toEqual([
      10.5,
    ]);
  });

  it('is missing when either price is', () => {
    expect(read(medianPriceValues(arr(NaN, 12, 12), arr(9, NaN, 9)))).toEqual([
      undefined,
      undefined,
      10.5,
    ]);
  });

  it('is length-preserving and reads the bar it is given, not a neighbour', () => {
    const out = medianPriceValues(arr(12, 20, 30), arr(9, 10, 10));
    expect(out).toHaveLength(3);
    expect(read(out)).toEqual([10.5, 15, 20]);
  });
});

describe('barRangeValues', () => {
  it('is high − low, in that order and with that sign', () => {
    // The ORDER is the whole test. Both of this kernel's shipped consumers
    // (`chaikinVolatility`, `massIndex`) read only RATIOS of ranges, so they
    // are blind to a flipped sign — a `low − high` mutation survives every
    // study test in the package. The sign is part of the kernel's contract
    // (a negative range is how a caller learns their `high`/`low` options are
    // the wrong way round), so it is pinned here rather than nowhere.
    expect(read(barRangeValues(arr(12, 20, 9), arr(9, 10, 11)))).toEqual([
      3, 10, -2,
    ]);
  });

  it('is NOT the true range — it never reads the close', () => {
    // A bar that gapped away from the previous close covers ground its own
    // span does not show; this kernel deliberately does not know that, which
    // is the fork `chaikinVolatility`/`massIndex` take against `atr`.
    expect(read(barRangeValues(arr(12, 12), arr(11, 11)))).toEqual([1, 1]);
  });

  it('is missing when either price is, and is length-preserving', () => {
    const out = barRangeValues(arr(NaN, 12, 12), arr(9, NaN, 9));
    expect(out).toHaveLength(3);
    expect(read(out)).toEqual([undefined, undefined, 3]);
  });
});
