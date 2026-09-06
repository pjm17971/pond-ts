import { describe, expect, it } from 'vitest';
import { TimeSeries } from 'pond-ts';
import {
  rollingMeanAbsDevValues,
  rollingMeanValues,
} from '../src/kernels/rolling-mean.js';
import { sma } from '../src/index.js';

/*
 * The raw-array SMA a slow stochastic smooths with. What distinguishes it
 * from `sma()` over a scratch column is the missing-cell contract — a window
 * short of `period` finite values has no mean — so that is what these pin,
 * alongside the identity with `sma()` where the two must agree.
 */

const arr = (...xs: number[]) => Float64Array.from(xs);
const read = (x: Float64Array) =>
  Array.from(x, (v) => (Number.isNaN(v) ? undefined : v));

describe('rollingMeanValues', () => {
  it('is the trailing mean, undefined for the first period − 1 rows', () => {
    expect(read(rollingMeanValues(arr(1, 2, 3, 4, 5), 3))).toEqual([
      undefined,
      undefined,
      2,
      3,
      4,
    ]);
  });

  it('gives the same numbers sma() gives over the same values', () => {
    // Same arithmetic (the range-exact kernel `sma` runs on), so the two
    // must agree to the last bit on a gap-free input, not merely closely.
    const values = Array.from(
      { length: 40 },
      (_, i) => 100 + 6 * Math.sin(i / 2.5) + i * 0.1,
    );
    const viaStudy = sma(
      new TimeSeries({
        name: 'bars',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'close', kind: 'number' },
        ] as const,
        rows: values.map((c, i) => [i, c]) as Array<[number, number]>,
      }),
      { period: 7 },
    ).events.map((e) => e.data()['sma']);
    const viaKernel = read(rollingMeanValues(Float64Array.from(values), 7));
    expect(viaKernel).toEqual(viaStudy);
  });

  it('has no mean for a window that holds a missing value', () => {
    // NOT core's count-window `avg`, which averages the cells it has. An
    // average of three values one of which is unknown is unknown; the window
    // recovers once the gap has left it.
    expect(read(rollingMeanValues(arr(1, 2, NaN, 4, 5, 6, 7), 2))).toEqual([
      undefined,
      1.5,
      undefined,
      undefined,
      4.5,
      5.5,
      6.5,
    ]);
  });

  it('a leading gap shifts the start rather than shortening the window', () => {
    // The row-counting alternative would emit `3` at index 2 (one value in
    // a 2-row window). Here the mean waits for two.
    expect(read(rollingMeanValues(arr(NaN, NaN, 3, 4, 5), 2))).toEqual([
      undefined,
      undefined,
      undefined,
      3.5,
      4.5,
    ]);
  });

  it('is the identity at period 1, gaps included', () => {
    expect(read(rollingMeanValues(arr(3, NaN, 5), 1))).toEqual([
      3,
      undefined,
      5,
    ]);
  });

  it('is all-undefined when the period exceeds the length', () => {
    expect(read(rollingMeanValues(arr(1, 2, 3), 5))).toEqual([
      undefined,
      undefined,
      undefined,
    ]);
  });
});

/*
 * The mean ABSOLUTE deviation — CCI's denominator. It is not a moment, so
 * nothing in core produces it and it cannot ride the mean's accumulator;
 * what these pin is that it is the deviation about the window's own mean
 * (not a median, not the standard deviation) and that it inherits
 * `rollingMeanValues`' missing-cell rule exactly rather than carrying a
 * second copy of it.
 */
describe('rollingMeanAbsDevValues', () => {
  it('is the mean |x − windowMean|, hand-computed', () => {
    // {1,2,3}: mean 2, deviations 1,0,1 → 2/3. {2,3,4}: the same by
    // translation. {3,4,10}: mean 17/3, deviations 8/3, 5/3, 13/3 → 26/9.
    const v = read(rollingMeanAbsDevValues(arr(1, 2, 3, 4, 10), 3));
    expect(v[0]).toBeUndefined();
    expect(v[1]).toBeUndefined();
    expect(v[2]).toBeCloseTo(2 / 3, 12);
    expect(v[3]).toBeCloseTo(2 / 3, 12);
    expect(v[4]).toBeCloseTo(26 / 9, 12);
  });

  it('is NOT the standard deviation — it is smaller on a spread window', () => {
    // {1,2,9}: mean 4, mean absolute deviation (3+2+5)/3 = 10/3 ≈ 3.33,
    // population σ = √(38/3) ≈ 3.56. An implementation that squared would
    // land on the second number.
    const v = read(rollingMeanAbsDevValues(arr(1, 2, 9), 3));
    expect(v[2]).toBeCloseTo(10 / 3, 12);
    expect(v[2]).not.toBeCloseTo(Math.sqrt(38 / 3), 3);
  });

  it('is 0 on a flat window — a real 0, which is CCI’s guard case', () => {
    expect(read(rollingMeanAbsDevValues(arr(5, 5, 5, 5), 3))).toEqual([
      undefined,
      undefined,
      0,
      0,
    ]);
  });

  it('has no deviation for a window holding a missing value, and recovers', () => {
    // The same mask `rollingMeanValues` gives, by construction: this kernel
    // reads that mean and emits nothing wherever it is missing.
    const values = arr(1, 2, NaN, 4, 5, 6);
    const mad = read(rollingMeanAbsDevValues(values, 2));
    const mean = read(rollingMeanValues(values, 2));
    expect(mad.map((x) => x === undefined)).toEqual(
      mean.map((x) => x === undefined),
    );
    expect(mad).toEqual([undefined, 0.5, undefined, undefined, 0.5, 0.5]);
  });

  it('is 0 at period 1 (a window of one is its own mean), gaps kept', () => {
    expect(read(rollingMeanAbsDevValues(arr(3, NaN, 5), 1))).toEqual([
      0,
      undefined,
      0,
    ]);
  });

  it('is all-undefined when the period exceeds the length', () => {
    expect(read(rollingMeanAbsDevValues(arr(1, 2, 3), 5))).toEqual([
      undefined,
      undefined,
      undefined,
    ]);
  });
});
