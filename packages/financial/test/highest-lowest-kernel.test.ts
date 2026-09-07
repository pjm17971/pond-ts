import { describe, expect, it } from 'vitest';
import { TimeSeries } from 'pond-ts';
import {
  barsSinceExtremeValues,
  highestLowestValues,
  percentOfRangeValues,
  rollingExtremesValues,
  rollingBarExtremesValues,
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

describe('barsSinceExtremeValues', () => {
  /** The naive O(N·period) reference the deque has to agree with, written
   *  out so the two are independent implementations rather than one. */
  const naive = (values: Float64Array, period: number, mode: 'max' | 'min') =>
    Array.from(values, (_, i) => {
      if (i < period) return undefined;
      const w = Array.from(values.slice(i - period, i + 1));
      if (w.some((x) => !Number.isFinite(x))) return undefined;
      const best = mode === 'max' ? Math.max(...w) : Math.min(...w);
      let at = 0;
      for (let k = 0; k < w.length; k += 1) if (w[k] === best) at = k;
      return period - at;
    });

  it('counts back from the reported bar, 0 for today', () => {
    // Window of period + 1 = 4 bars. At bar 3 the max (14) is today's.
    const v = read(barsSinceExtremeValues(arr(10, 13, 11, 14, 12), 3, 'max'));
    expect(v[0]).toBeUndefined();
    expect(v[2]).toBeUndefined(); // warm-up is `period` rows, not period - 1
    expect(v[3]).toBe(0);
    expect(v[4]).toBe(1);
  });

  it('finds an extreme in the MIDDLE of the window, not at an edge', () => {
    // The failure mode the file header names: reading an edge instead of
    // scanning. The max (20) sits two in from the newest bar at bar 4.
    const v = read(barsSinceExtremeValues(arr(9, 12, 20, 15, 11), 4, 'max'));
    expect(v[4]).toBe(2);
    const w = read(barsSinceExtremeValues(arr(9, 12, 3, 15, 11), 4, 'min'));
    expect(w[4]).toBe(2);
  });

  it('gives a tie to the MOST RECENT bar', () => {
    // TA-Lib's rule (measured in the oracle generator). An implementation
    // that queued equal values behind the older one would say 3 here.
    const v = read(barsSinceExtremeValues(arr(10, 12, 11, 12, 10.5), 4, 'max'));
    expect(v[4]).toBe(1);
    const w = read(barsSinceExtremeValues(arr(10, 8, 11, 8, 10.5), 4, 'min'));
    expect(w[4]).toBe(1);
  });

  it('ages an extreme out of the window rather than holding it forever', () => {
    // 30 is the max until it leaves; the deque's front eviction is what makes
    // the later bars report the smaller, newer high.
    const v = read(
      barsSinceExtremeValues(arr(30, 10, 11, 12, 13, 14), 2, 'max'),
    );
    expect(v[2]).toBe(2); // 30 still in the 3-bar window
    expect(v[3]).toBe(0); // 30 gone; 12 is today's max
    expect(v[5]).toBe(0);
  });

  it('blanks a window holding a gap, then recovers exactly period + 1 later', () => {
    const v = read(
      barsSinceExtremeValues(arr(10, 11, NaN, 13, 14, 15, 16), 2, 'max'),
    );
    expect(v[1]).toBeUndefined(); // warm-up
    expect(v[2]).toBeUndefined();
    expect(v[3]).toBeUndefined();
    expect(v[4]).toBeUndefined(); // last window containing the gap
    expect(v[5]).toBe(0);
    expect(v[6]).toBe(0);
  });

  it('never picks a non-finite cell as the extreme', () => {
    // Once the gap has aged out, the standing extreme must still be right —
    // the deque must not have queued the NaN as a candidate.
    const v = read(barsSinceExtremeValues(arr(NaN, 50, 10, 11, 12), 2, 'max'));
    expect(v[2]).toBeUndefined(); // the window still holds the gap
    expect(v[3]).toBe(2); // window [50, 10, 11]
    expect(v[4]).toBe(0); // window [10, 11, 12]
  });

  it('agrees with the naive scan on a wavy series, both modes, with and without gaps', () => {
    const wavy = Float64Array.from({ length: 60 }, (_, i) =>
      Math.round((100 + 8 * Math.sin(i / 3.5) + 0.3 * i) * 4),
    );
    for (const period of [1, 2, 5, 13]) {
      for (const mode of ['max', 'min'] as const) {
        expect(
          read(barsSinceExtremeValues(wavy, period, mode)),
          `${mode} ${period}`,
        ).toEqual(naive(wavy, period, mode));
      }
    }
    const holed = Float64Array.from(wavy);
    holed[7] = NaN;
    holed[31] = NaN;
    for (const period of [2, 5, 13])
      expect(
        read(barsSinceExtremeValues(holed, period, 'max')),
        `holed ${period}`,
      ).toEqual(naive(holed, period, 'max'));
  });

  it('is all-missing when the window is longer than the input', () => {
    const v = read(barsSinceExtremeValues(arr(10, 11, 12), 5, 'max'));
    expect(v).toEqual([undefined, undefined, undefined]);
    expect(
      barsSinceExtremeValues(Float64Array.from([]), 3, 'min'),
    ).toHaveLength(0);
  });
});

describe('rollingExtremesValues — the STRICT array door', () => {
  it('takes both extremes over the window, hand-checked', () => {
    // Extremes deliberately away from the window edges: an implementation
    // reading the first or last cell passes a monotonic fixture.
    const v = arr(5, 9, 3, 7, 4, 8, 2, 6);
    const { highest, lowest } = rollingExtremesValues(v, 3);
    expect(read(highest)).toEqual([undefined, undefined, 9, 9, 7, 8, 8, 8]);
    expect(read(lowest)).toEqual([undefined, undefined, 3, 3, 3, 4, 2, 2]);
  });

  it('is STRICT — one missing cell blanks every window holding it', () => {
    // The contrast with `highestLowestValues`, which skips and answers over
    // whatever the window does hold. Here the gap costs `period` bars and
    // then the answer comes back.
    const v = arr(5, 9, NaN, 7, 4, 8, 2, 6);
    const { highest, lowest } = rollingExtremesValues(v, 3);
    expect(read(highest)).toEqual([
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      8,
      8,
      8,
    ]);
    expect(read(lowest)[5]).toBe(4);
  });

  it('a leading run of gaps just delays the first value', () => {
    // The `stochasticRsi` shape: the input is another study's output, so its
    // head is missing. The window must wait for `period` finite values, not
    // emit an extreme over two of them.
    const v = arr(NaN, NaN, NaN, 10, 12, 11, 15, 9);
    const { highest } = rollingExtremesValues(v, 3);
    expect(read(highest)).toEqual([
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      12,
      15,
      15,
    ]);
  });

  it('period 1 is the identity, gaps included', () => {
    const v = arr(5, NaN, 3);
    const { highest, lowest } = rollingExtremesValues(v, 1);
    expect(read(highest)).toEqual([5, undefined, 3]);
    expect(read(lowest)).toEqual([5, undefined, 3]);
  });

  it('a window longer than the input is all-missing, length kept', () => {
    const { highest, lowest } = rollingExtremesValues(arr(1, 2, 3), 5);
    expect(read(highest)).toEqual([undefined, undefined, undefined]);
    expect(lowest).toHaveLength(3);
  });

  it('repeated extremes and a flat run still read correctly', () => {
    // Non-strict eviction on both deques means an equal value displaces the
    // older candidate; the VALUE is unchanged either way, which is what this
    // pins (contrast `barsSinceExtremeValues`, where the tie moves the age).
    const v = arr(4, 4, 4, 4, 7, 4);
    const { highest, lowest } = rollingExtremesValues(v, 3);
    expect(read(highest)).toEqual([undefined, undefined, 4, 4, 7, 7]);
    expect(read(lowest)).toEqual([undefined, undefined, 4, 4, 4, 4]);
  });

  it('agrees with a brute-force scan on a long random-ish input', () => {
    // The deque is the whole point of the kernel, so it is checked against
    // the naive O(N·period) answer it replaces — including gaps.
    const n = 500;
    const period = 17;
    const v = new Float64Array(n);
    for (let i = 0; i < n; i += 1) {
      v[i] =
        i % 37 === 11
          ? NaN
          : 100 + 9 * Math.sin(i / 3.1) + 4 * Math.cos(i / 7.3);
    }
    const { highest, lowest } = rollingExtremesValues(v, period);
    for (let i = 0; i < n; i += 1) {
      if (i < period - 1) {
        expect(Number.isNaN(highest[i]!), `bar ${i}`).toBe(true);
        continue;
      }
      const window = Array.from(v.slice(i - period + 1, i + 1));
      if (window.some((x) => Number.isNaN(x))) {
        expect(Number.isNaN(highest[i]!), `bar ${i}`).toBe(true);
        expect(Number.isNaN(lowest[i]!), `bar ${i}`).toBe(true);
        continue;
      }
      expect(highest[i], `bar ${i}`).toBe(Math.max(...window));
      expect(lowest[i], `bar ${i}`).toBe(Math.min(...window));
    }
  });
});

describe('rollingBarExtremesValues', () => {
  const f = (xs: number[]) => Float64Array.from(xs);

  it('takes the max of `highs` against the min of `lows`, hand-computed', () => {
    const highs = f([12, 14, 13, 16, 15]);
    const lows = f([10, 11, 9, 12, 13]);
    const { highest, lowest } = rollingBarExtremesValues(highs, lows, 3);
    expect(Array.from(highest)).toEqual([NaN, NaN, 14, 16, 16]);
    expect(Array.from(lowest)).toEqual([NaN, NaN, 9, 9, 9]);
  });

  it('is the pair two `rollingExtremesValues` calls would give, on clean input', () => {
    // The door exists to halve the work, not to change the answer.
    const n = 300;
    const highs = new Float64Array(n);
    const lows = new Float64Array(n);
    for (let i = 0; i < n; i += 1) {
      const c = 100 + 9 * Math.sin(i / 3.1) + 4 * Math.cos(i / 7.3);
      highs[i] = c + 0.5;
      lows[i] = c - 0.7;
    }
    for (const period of [1, 2, 13, 52]) {
      const paired = rollingBarExtremesValues(highs, lows, period);
      expect(Array.from(paired.highest), `period ${period}`).toEqual(
        Array.from(rollingExtremesValues(highs, period).highest),
      );
      expect(Array.from(paired.lowest), `period ${period}`).toEqual(
        Array.from(rollingExtremesValues(lows, period).lowest),
      );
    }
  });

  it('blanks BOTH outputs when either array has a hole in the window', () => {
    // Stricter than a per-array rule, deliberately: a range whose top is
    // known and whose bottom is not is not a range.
    const highs = f([12, NaN, 13, 16, 15, 17]);
    const lows = f([10, 11, 9, NaN, 13, 14]);
    const { highest, lowest } = rollingBarExtremesValues(highs, lows, 2);
    // Windows holding bar 1 (a missing high) or bar 3 (a missing low) are
    // blank on both sides; bar 5's window (bars 4–5) is complete.
    expect(
      Array.from(highest).map((x) => (Number.isNaN(x) ? null : x)),
    ).toEqual([null, null, null, null, null, 17]);
    expect(Array.from(lowest).map((x) => (Number.isNaN(x) ? null : x))).toEqual(
      [null, null, null, null, null, 13],
    );
  });

  it('agrees with a brute-force scan on a long gappy input', () => {
    const n = 400;
    const period = 11;
    const highs = new Float64Array(n);
    const lows = new Float64Array(n);
    for (let i = 0; i < n; i += 1) {
      const c = 100 + 9 * Math.sin(i / 3.1) + 4 * Math.cos(i / 7.3);
      highs[i] = i % 31 === 7 ? NaN : c + 0.5;
      lows[i] = i % 43 === 19 ? NaN : c - 0.7;
    }
    const { highest, lowest } = rollingBarExtremesValues(highs, lows, period);
    for (let i = 0; i < n; i += 1) {
      if (i < period - 1) {
        expect(Number.isNaN(highest[i]!), `bar ${i}`).toBe(true);
        continue;
      }
      const hw = Array.from(highs.slice(i - period + 1, i + 1));
      const lw = Array.from(lows.slice(i - period + 1, i + 1));
      if ([...hw, ...lw].some((x) => Number.isNaN(x))) {
        expect(Number.isNaN(highest[i]!), `bar ${i}`).toBe(true);
        expect(Number.isNaN(lowest[i]!), `bar ${i}`).toBe(true);
        continue;
      }
      expect(highest[i], `bar ${i}`).toBe(Math.max(...hw));
      expect(lowest[i], `bar ${i}`).toBe(Math.min(...lw));
    }
  });

  it('reserves no more ring than the series is long', () => {
    // The #709 lesson: a period far longer than the input must not allocate
    // for a window it can never fill.
    const { highest, lowest } = rollingBarExtremesValues(
      f([1, 2, 3]),
      f([0, 1, 2]),
      5_000_000,
    );
    expect(Array.from(highest).every((x) => Number.isNaN(x))).toBe(true);
    expect(Array.from(lowest).every((x) => Number.isNaN(x))).toBe(true);
  });
});
