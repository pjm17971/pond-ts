import { describe, expect, it } from 'vitest';
import { TimeSeries } from 'pond-ts';
import { rollingMeanValues } from '../src/kernels/rolling-mean.js';
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
