import { describe, expect, it } from 'vitest';
import { TimeSeries } from 'pond-ts';
import {
  highestLowestValues,
  percentOfRangeValues,
} from '../src/kernels/highest-lowest.js';

/*
 * Tested directly, on a fixture whose window extremes are NOT at the window's
 * edges: an implementation reading `high[i]` or `high[i − period + 1]` in
 * place of the max passes any test whose extremes happen to sit there, and a
 * monotonic series puts them there on every bar.
 */

const arr = (...xs: number[]) => Float64Array.from(xs);
const read = (x: Float64Array) =>
  Array.from(x, (v) => (Number.isNaN(v) ? undefined : v));

const ohlc = (rows: Array<[number | undefined, number | undefined]>) =>
  new TimeSeries({
    name: 'bars',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'high', kind: 'number', required: false },
      { name: 'low', kind: 'number', required: false },
    ] as const,
    rows: rows.map(([h, l], i) => [i, h, l]) as never,
  }) as unknown as TimeSeries<never>;

describe('highestLowestValues', () => {
  //            high  low       3-bar HH   3-bar LL
  //  0          12    8
  //  1          15   11
  //  2          14    9        15 (bar 1)  8 (bar 0)
  //  3          11    7        15 (bar 1)  7 (bar 3)
  //  4          13   10        14 (bar 2)  7 (bar 3)  <- LL interior
  //  5          16   12        16 (bar 5)  7 (bar 3)
  const bars = ohlc([
    [12, 8],
    [15, 11],
    [14, 9],
    [11, 7],
    [13, 10],
    [16, 12],
  ]);

  it('takes the extremes of the whole window, wherever they fall', () => {
    const { highest, lowest } = highestLowestValues(bars, 'high', 'low', 3);
    expect(read(highest)).toEqual([undefined, undefined, 15, 15, 14, 16]);
    expect(read(lowest)).toEqual([undefined, undefined, 8, 7, 7, 7]);
  });

  it('warms up over period − 1 rows, length-preserving', () => {
    const { highest } = highestLowestValues(bars, 'high', 'low', 5);
    expect(read(highest)).toEqual([
      undefined,
      undefined,
      undefined,
      undefined,
      15,
      16,
    ]);
  });

  it('skips a missing cell and takes the extreme of what the window holds', () => {
    // Core's reducer policy, inherited: bar 1's high is missing, so the
    // 3-bar HH at bar 2 is max(12, 14) = 14, not 15 and not undefined.
    const gappy = ohlc([
      [12, 8],
      [undefined, 11],
      [14, 9],
      [11, undefined],
      [13, 10],
    ]);
    const { highest, lowest } = highestLowestValues(gappy, 'high', 'low', 3);
    expect(read(highest)).toEqual([undefined, undefined, 14, 14, 14]);
    expect(read(lowest)).toEqual([undefined, undefined, 8, 9, 9]);
  });

  it('is undefined only when the window holds nothing', () => {
    const empty = ohlc([
      [undefined, undefined],
      [undefined, undefined],
      [undefined, undefined],
      [13, 10],
    ]);
    const { highest, lowest } = highestLowestValues(empty, 'high', 'low', 2);
    expect(read(highest)).toEqual([undefined, undefined, undefined, 13]);
    expect(read(lowest)).toEqual([undefined, undefined, undefined, 10]);
  });

  it('reads a misnamed column as all-missing', () => {
    const { highest } = highestLowestValues(bars, 'nope', 'low', 2);
    expect(read(highest).every((x) => x === undefined)).toBe(true);
  });
});

describe('percentOfRangeValues', () => {
  it('is 100 · (value − lowest) / (highest − lowest)', () => {
    const p = read(
      percentOfRangeValues(arr(15, 15, 14), arr(8, 7, 7), arr(12, 9, 12)),
    );
    expect(p[0]).toBeCloseTo(400 / 7, 12); // 57.14…
    expect(p[1]).toBeCloseTo(25, 12);
    expect(p[2]).toBeCloseTo(500 / 7, 12); // 71.43…
  });

  it('is 0 at the lowest and 100 at the highest', () => {
    const p = read(percentOfRangeValues(arr(15, 15), arr(8, 8), arr(8, 15)));
    expect(p).toEqual([0, 100]);
  });

  it('is undefined on a flat window — 0/0 has no position', () => {
    // Not 0 (TA-Lib's answer, which is also "at the very bottom") and not a
    // NaN-from-division that happens to read the same: an explicit rule.
    const p = read(percentOfRangeValues(arr(10, 15), arr(10, 8), arr(10, 12)));
    expect(p[0]).toBeUndefined();
    expect(p[1]).toBeDefined();
  });

  it('propagates a missing input', () => {
    const p = read(
      percentOfRangeValues(arr(NaN, 15, 15), arr(8, NaN, 8), arr(12, 12, NaN)),
    );
    expect(p).toEqual([undefined, undefined, undefined]);
  });

  it('does not clamp a value outside the range', () => {
    // A redirected `close` the range does not bound reads outside 0..100.
    const p = read(percentOfRangeValues(arr(15, 15), arr(8, 8), arr(22, 1)));
    expect(p[0]).toBeCloseTo(200, 12);
    expect(p[1]).toBeCloseTo(-100, 12);
  });
});
