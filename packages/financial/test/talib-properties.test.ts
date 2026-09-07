/*
 * Property tests converted from TA-Lib's own Python test suite.
 *
 * Portions (the input arrays and expected values in `scale invariance`) are
 * derived from `tests/test_func.py` in TA-Lib/ta-lib-python
 * (https://github.com/TA-Lib/ta-lib-python), used under the BSD 2-Clause
 * License, which requires that redistributions of source retain the copyright
 * notice and disclaimer. The upstream LICENSE file names no individual
 * copyright holder, so attribution is to the project.
 *
 *   BSD 2-Clause License. Redistributions of source code must retain the
 *   above copyright notice, this list of conditions and the following
 *   disclaimer. THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
 *   CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES ARE
 *   DISCLAIMED. See https://github.com/TA-Lib/ta-lib-python/blob/master/LICENSE
 *
 * ## Why these, and not TA-Lib's value tables
 *
 * `study-oracle.test.ts` already checks our VALUES against TA-Lib, bar for
 * bar, on a generated input. Copying more value tables would duplicate it.
 *
 * What TA-Lib's suite has that ours does not is PROPERTY tests, and they
 * cover exactly the ground the oracle's input is designed to avoid — its
 * series is clean, never flat, and gap-free by construction. These are the
 * cases where an indicator is most likely to be quietly wrong:
 *
 *   - `test_RSI`      scale invariance at magnitudes near 1e-7
 *   - `test_EMAEMA`   a study over another study's output: length preserved,
 *                     warm-up composed
 *   - `test_input_allnans`  all-missing in, all-missing out
 *   - `test_input_lengths`  multi-input studies and mismatched inputs
 *
 * What the all-missing tests actually pin is that a study handed nothing
 * usable neither throws nor INVENTS a value. They do not, and cannot, pin
 * "no NaN leaks into a column": `withColumn` maps `NaN` to missing on its
 * typed door ([PND-WCNAN]), so a leaked `NaN` would arrive at any reader as
 * `undefined` and read as a pass. That guarantee belongs to core and is
 * tested there; claiming it here would be coverage this file does not have.
 *
 * The middle one is not hypothetical for us. `rsi(sma(...))` shipped in
 * review returning an entirely empty column, because a leading NaN landed in
 * the Wilder seed window and a recursion carries state forward forever. It
 * was caught by a chaining test written on a hunch; TA-Lib has had a standing
 * test for that shape for years. This file is the standing version.
 */
import { describe, expect, it } from 'vitest';
import { TimeSeries } from 'pond-ts';
import {
  MA_TYPES,
  atr,
  donchian,
  ema,
  historicalVolatility,
  macd,
  momentum,
  movingAverage,
  obv,
  vwap,
  rsi,
  sma,
  stochastic,
  williamsR,
  keltner,
  atrBands,
  qstick,
  trix,
  coppock,
  priceOscillator,
  disparityIndex,
  detrendedPriceOscillator,
  elderRay,
  awesomeOscillator,
  accumulationDistribution,
  chaikinOscillator,
  priceVolumeTrend,
  chaikinMoneyFlow,
  moneyFlowIndex,
  forceIndex,
  easeOfMovement,
  volumeOscillator,
  chandeMomentum,
  ultimateOscillator,
  commodityChannelIndex,
  intradayMomentumIndex,
  relativeVigorIndex,
  psychologicalLine,
  chaikinVolatility,
  massIndex,
  choppinessIndex,
  ulcerIndex,
  verticalHorizontalFilter,
  gopalakrishnanRangeIndex,
  relativeVolatilityIndex,
  correlation,
  beta,
  priceRelative,
  performanceIndex,
  directionalMovement,
  aroon,
  vortex,
  linearRegression,
  timeSeriesForecast,
  chandeForecastOscillator,
  centerOfGravity,
  parabolicSar,
  superTrend,
  atrTrailingStop,
  negativeVolumeIndex,
  positiveVolumeIndex,
  klinger,
} from '../src/index.js';

const closeSchema = [
  { name: 'time', kind: 'time' },
  { name: 'close', kind: 'number' },
] as const;

const bars = (closes: number[]) =>
  new TimeSeries({
    name: 'bars',
    schema: closeSchema,
    rows: closes.map((c, i) => [i, c]) as Array<[number, number]>,
  });

const col = (s: unknown, name: string): Array<number | undefined> => {
  const events = (
    s as { events: ReadonlyArray<{ data(): Record<string, unknown> }> }
  ).events;
  return events.map((e) => {
    const v = e.data()[name];
    return typeof v === 'number' ? v : undefined;
  });
};

/** First index holding a value, or -1. */
const firstValid = (v: Array<number | undefined>) =>
  v.findIndex((x) => x !== undefined);

describe('[talib] scale invariance', () => {
  // Verbatim from TA-Lib's `test_RSI`: values around 2.4e-7, and the same
  // series multiplied by 1e5. RSI is a ratio of averaged differences, so it
  // is scale-free in exact arithmetic — the test is whether float error at
  // denormal-adjacent magnitudes breaks that.
  const tiny = [
    0.00000024, 0.00000024, 0.00000024, 0.00000024, 0.00000024, 0.00000023,
    0.00000024, 0.00000024, 0.00000024, 0.00000024, 0.00000023, 0.00000024,
    0.00000023, 0.00000024, 0.00000023, 0.00000024, 0.00000024, 0.00000023,
    0.00000023, 0.00000023,
  ];
  // TA-Lib's own expected output for `RSI(a, 10)`.
  const expected = [
    33.333333333333329, 51.351351351351347, 39.491916859122398,
    51.84807024709005, 42.25953803191981, 52.101824405061215,
    52.101824405061215, 43.043664867691085, 43.043664867691085,
    43.043664867691085,
  ];

  it('rsi matches TA-Lib at magnitudes near 1e-7', () => {
    const v = col(rsi(bars(tiny), { period: 10 }), 'rsi');
    expect(v.slice(0, 10).every((x) => x === undefined)).toBe(true);
    for (let i = 10; i < 20; i += 1) {
      expect(v[i], `bar ${i}`).toBeCloseTo(expected[i - 10]!, 9);
    }
  });

  it('rsi is unchanged by scaling the input', () => {
    const base = col(rsi(bars(tiny), { period: 10 }), 'rsi');
    const scaled = col(
      rsi(bars(tiny.map((x) => x * 100000)), { period: 10 }),
      'rsi',
    );
    for (let i = 0; i < base.length; i += 1) {
      if (base[i] === undefined) expect(scaled[i], `bar ${i}`).toBeUndefined();
      else expect(scaled[i], `bar ${i}`).toBeCloseTo(base[i]!, 9);
    }
  });

  it('atr scales LINEARLY with the input', () => {
    // ATR is an absolute quantity in the price's units — it deliberately does
    // not normalise, so tripling every price triples it.
    const ohlc = (k: number) =>
      new TimeSeries({
        name: 'bars',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'high', kind: 'number' },
          { name: 'low', kind: 'number' },
          { name: 'close', kind: 'number' },
        ] as const,
        rows: Array.from({ length: 30 }, (_, i) => {
          const c = 100 + 8 * Math.sin(i / 3);
          return [i, (c + 1.2) * k, (c - 0.9) * k, c * k];
        }) as Array<[number, number, number, number]>,
      });
    const base = col(atr(ohlc(1), { period: 4 }), 'atr');
    const scaled = col(atr(ohlc(3), { period: 4 }), 'atr');
    for (let i = 0; i < base.length; i += 1) {
      if (base[i] === undefined) expect(scaled[i], `bar ${i}`).toBeUndefined();
      else expect(scaled[i]! / 3, `bar ${i}`).toBeCloseTo(base[i]!, 9);
    }
  });

  it('macd scales LINEARLY with the input, rather than being invariant', () => {
    // The companion property, and the reason scale invariance is a real
    // assertion rather than a truism: MACD is a difference of prices, so it
    // must scale with them. An implementation that normalised somewhere it
    // shouldn't would pass the RSI test and fail this one.
    const src = Array.from(
      { length: 40 },
      (_, i) => 100 + 10 * Math.sin(i / 4),
    );
    const k = 1000;
    const opts = { fastPeriod: 3, slowPeriod: 7, signalPeriod: 4 } as const;
    const base = col(macd(bars(src), opts), 'macdLine');
    const scaled = col(macd(bars(src.map((x) => x * k)), opts), 'macdLine');
    for (let i = 0; i < base.length; i += 1) {
      if (base[i] === undefined) expect(scaled[i], `bar ${i}`).toBeUndefined();
      else expect(scaled[i]! / k, `bar ${i}`).toBeCloseTo(base[i]!, 9);
    }
  });
});

describe('[talib] a study over another study composes its warm-up', () => {
  const rising = Array.from({ length: 30 }, (_, i) => 100 + i);

  it('ema of ema preserves length and composes the warm-up', () => {
    // TA-Lib's `test_EMAEMA`: EMA(EMA(x, 2), 2) is length-preserving and
    // first valid at index 2 — each pass costs `period - 1` rows.
    const once = ema(bars(rising), { period: 2, output: 'e1' });
    const twice = ema(once, { period: 2, column: 'e1', output: 'e2' });
    const v = col(twice, 'e2');
    expect(v).toHaveLength(rising.length);
    expect(firstValid(v)).toBe(2);
  });

  it('rsi over sma starts late rather than coming back empty', () => {
    // The regression this file exists for. A leading gap from the source
    // study's own warm-up must SHIFT the Wilder seed, not poison it — a
    // recursion carries state forward forever, so poisoning empties the
    // whole column rather than delaying it.
    //
    // The source OSCILLATES deliberately. A monotonic one (an earlier draft
    // used `100 + i`) sends every bar through RSI's `avgLoss === 0` branch,
    // so the whole test passes on a constant 100 and pins nothing but the
    // shape — mutating that branch to emit 0, or deleting it outright,
    // survived. With a source that falls as well as rises, the same test
    // also pins the values.
    const wavy = Array.from(
      { length: 30 },
      (_, i) => 100 + 6 * Math.sin(i / 2.5) + i * 0.1,
    );
    const src = sma(bars(wavy), { period: 3 });
    const v = col(rsi(src, { column: 'sma', period: 4, output: 'r' }), 'r');
    expect(v).toHaveLength(wavy.length);
    // sma(3) first valid at 2; rsi(4) needs 4 differences after that.
    expect(firstValid(v)).toBe(6);
    expect(v.slice(6).every((x) => x !== undefined)).toBe(true);
    // Strictly interior values: proof the arithmetic ran rather than the
    // no-losses shortcut, which is what a monotonic source hid.
    const defined = v.slice(6) as number[];
    expect(defined.some((x) => x > 0 && x < 100)).toBe(true);
    expect(new Set(defined).size).toBeGreaterThan(1);
  });

  it('atr over a study output composes its warm-up', () => {
    // ATR's close can be redirected at another study's output, whose warm-up
    // must shift the Wilder seed rather than poison it.
    const n = 30;
    const src = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'high', kind: 'number' },
        { name: 'low', kind: 'number' },
        { name: 'close', kind: 'number' },
      ] as const,
      rows: Array.from({ length: n }, (_, i) => {
        const c = 100 + 5 * Math.sin(i / 3);
        return [i, c + 1, c - 1, c];
      }) as Array<[number, number, number, number]>,
    });
    const smoothed = sma(src, { period: 3, output: 'sc' });
    const v = col(atr(smoothed, { period: 4, close: 'sc', output: 'a' }), 'a');
    expect(v).toHaveLength(n);
    // sma(3) first valid at 2, so TR first valid at 3, seed 4 bars on: 6.
    expect(firstValid(v)).toBe(6);
  });

  it('macd over sma composes too', () => {
    const src = sma(bars(rising), { period: 3 });
    const r = macd(src, {
      column: 'sma',
      fastPeriod: 2,
      slowPeriod: 5,
      signalPeriod: 3,
      prefix: 'm',
    });
    expect(col(r, 'mLine')).toHaveLength(rising.length);
    // sma(3) first valid at 2; the slow EMA needs 4 more; the signal 2 more.
    expect(firstValid(col(r, 'mLine'))).toBe(6);
    expect(firstValid(col(r, 'mSignal'))).toBe(8);
  });
});

describe("[talib] a multi-input study's inputs cannot disagree in length", () => {
  /*
   * TA-Lib's `test_input_lengths` exists because its functions take separate
   * arrays: `ATR(high, low, close)` with a short `low` is a caller error it
   * must detect and raise on. Converting it literally is impossible here, and
   * that is the finding rather than a gap — pond's multi-input studies read
   * COLUMNS OF ONE SERIES, so unequal lengths are unrepresentable. The check
   * that upstream needs at every call site is discharged by the type.
   *
   * What remains testable is the part that IS still a caller error: naming a
   * column that is not there.
   */
  const bar = (n: number) =>
    new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'high', kind: 'number' },
        { name: 'low', kind: 'number' },
        { name: 'close', kind: 'number' },
      ] as const,
      rows: Array.from({ length: n }, (_, i) => [i, 101, 99, 100]) as Array<
        [number, number, number, number]
      >,
    });

  it('atr over aligned columns needs no length check', () => {
    const v = col(atr(bar(12) as never, { period: 3 }), 'atr');
    expect(v).toHaveLength(12);
    expect(v[3]).toBeCloseTo(2, 12);
  });

  it('atr reads all-missing when an input column is misnamed', () => {
    const v = col(
      atr(bar(12) as never, { period: 3, low: 'nope' as never }),
      'atr',
    );
    expect(v).toHaveLength(12);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('[talib] all-missing input yields all-missing output', () => {
  // TA-Lib's `test_input_allnans`. A study must not invent a value, throw, or
  // leak NaN into a user-visible column when it is handed nothing usable.
  const allMissing = new TimeSeries({
    name: 'bars',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'close', kind: 'number', required: false },
    ] as const,
    rows: Array.from({ length: 20 }, (_, i) => [i, undefined]) as Array<
      [number, number | undefined]
    >,
  });

  it('rsi', () => {
    const v = col(rsi(allMissing as never, { period: 5 }), 'rsi');
    expect(v).toHaveLength(20);
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('macd', () => {
    const r = macd(allMissing as never, {
      fastPeriod: 2,
      slowPeriod: 5,
      signalPeriod: 3,
    });
    for (const name of ['macdLine', 'macdSignal', 'macdHist']) {
      const v = col(r, name);
      expect(v, name).toHaveLength(20);
      expect(
        v.every((x) => x === undefined),
        name,
      ).toBe(true);
    }
  });

  it('atr', () => {
    const allMissingBars = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'high', kind: 'number', required: false },
        { name: 'low', kind: 'number', required: false },
        { name: 'close', kind: 'number', required: false },
      ] as const,
      rows: Array.from({ length: 20 }, (_, i) => [
        i,
        undefined,
        undefined,
        undefined,
      ]) as Array<
        [number, number | undefined, number | undefined, number | undefined]
      >,
    });
    const v = col(atr(allMissingBars as never, { period: 5 }), 'atr');
    expect(v).toHaveLength(20);
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('sma and ema', () => {
    expect(
      col(sma(allMissing as never, { period: 5 }), 'sma').every(
        (x) => x === undefined,
      ),
    ).toBe(true);
    expect(
      col(ema(allMissing as never, { period: 5 }), 'ema').every(
        (x) => x === undefined,
      ),
    ).toBe(true);
  });

  it('momentum and historicalVolatility', () => {
    const m = col(momentum(allMissing as never, { period: 3 }), 'momentum');
    expect(m).toHaveLength(20);
    expect(m.every((x) => x === undefined)).toBe(true);
    const h = col(
      historicalVolatility(allMissing as never, { period: 3 }),
      'hv',
    );
    expect(h).toHaveLength(20);
    expect(h.every((x) => x === undefined)).toBe(true);
  });
});

describe('[talib] momentum and historical volatility: scale and composition', () => {
  // Oscillating: a geometric series has constant log returns (HV ≡ 0, which
  // is scale-invariant for the wrong reason), and a linear one has constant
  // momentum (a shifted lookback passes).
  const wavy = Array.from(
    { length: 40 },
    (_, i) => 100 + 8 * Math.sin(i / 3) + 3 * Math.sin(i / 1.3),
  );

  it('momentum scales LINEARLY with the input', () => {
    // A difference of prices is in the price's units, like ATR and MACD.
    const k = 1000;
    const base = col(momentum(bars(wavy), { period: 4 }), 'momentum');
    const scaled = col(
      momentum(bars(wavy.map((x) => x * k)), { period: 4 }),
      'momentum',
    );
    expect(firstValid(base)).toBe(4);
    for (let i = 0; i < base.length; i += 1) {
      if (base[i] === undefined) expect(scaled[i], `bar ${i}`).toBeUndefined();
      else expect(scaled[i]! / k, `bar ${i}`).toBeCloseTo(base[i]!, 9);
    }
  });

  it('historicalVolatility is INVARIANT under scaling the input', () => {
    // Log returns are ratios, so a constant factor cancels — at 1e5 and at
    // 1e-5, where the RSI test above showed float error can bite.
    const base = col(historicalVolatility(bars(wavy), { period: 6 }), 'hv');
    expect(firstValid(base)).toBe(6);
    expect(base.slice(6).every((x) => x! > 0)).toBe(true);
    for (const k of [1e5, 1e-5]) {
      const scaled = col(
        historicalVolatility(bars(wavy.map((x) => x * k)), { period: 6 }),
        'hv',
      );
      for (let i = 0; i < base.length; i += 1) {
        if (base[i] === undefined)
          expect(scaled[i], `×${k} bar ${i}`).toBeUndefined();
        else expect(scaled[i], `×${k} bar ${i}`).toBeCloseTo(base[i]!, 9);
      }
    }
  });

  it('momentum over sma composes its warm-up', () => {
    const src = sma(bars(wavy), { period: 3 });
    const v = col(
      momentum(src, { column: 'sma', period: 4, output: 'm' }),
      'm',
    );
    expect(v).toHaveLength(wavy.length);
    // sma(3) first valid at 2; a 4-bar difference of it needs 4 more.
    expect(firstValid(v)).toBe(6);
    expect(v.slice(6).every((x) => x !== undefined)).toBe(true);
  });

  it('historicalVolatility over sma composes its warm-up', () => {
    const src = sma(bars(wavy), { period: 3 });
    const v = col(
      historicalVolatility(src, { column: 'sma', period: 4, output: 'h' }),
      'h',
    );
    expect(v).toHaveLength(wavy.length);
    // sma(3) first valid at 2; 4 returns of it need 4 more bars.
    expect(firstValid(v)).toBe(6);
    expect(v.slice(6).every((x) => x !== undefined && x > 0)).toBe(true);
  });
});

/* ------------------------------------------------------------------------ */
/* The range-position studies: stochastic, williamsR, donchian.             */
/* ------------------------------------------------------------------------ */

const ohlcSchema = [
  { name: 'time', kind: 'time' },
  { name: 'high', kind: 'number' },
  { name: 'low', kind: 'number' },
  { name: 'close', kind: 'number' },
] as const;

/** Oscillating bars with genuine range variation, scaled by `k`. The
 *  half-widths vary so no window is flat and the extremes do not always sit
 *  on the window's edges. */
const rangeBars = (n: number, k = 1) =>
  new TimeSeries({
    name: 'bars',
    schema: ohlcSchema,
    rows: Array.from({ length: n }, (_, i) => {
      const c = 100 + 8 * Math.sin(i / 3) + 0.2 * i;
      return [
        i,
        (c + 0.4 + 0.6 * Math.abs(Math.sin(i / 2))) * k,
        (c - 0.3 - 0.5 * Math.abs(Math.cos(i / 2.5))) * k,
        c * k,
      ];
    }) as Array<[number, number, number, number]>,
  });

describe('[talib] scale behaviour of the range-position studies', () => {
  const K = 100000;

  it('stochastic is unchanged by scaling the input', () => {
    // A ratio of price differences, like RSI: scale-free in exact
    // arithmetic, and the test is that float error at 1e5× does not break
    // that.
    const opts = { kPeriod: 5, slowing: 3, dPeriod: 3 } as const;
    const base = stochastic(rangeBars(40), opts);
    const scaled = stochastic(rangeBars(40, K), opts);
    for (const name of ['stochK', 'stochD']) {
      const b = col(base, name);
      const s = col(scaled, name);
      expect(b.filter((x) => x !== undefined).length, name).toBeGreaterThan(30);
      for (let i = 0; i < b.length; i += 1) {
        if (b[i] === undefined) expect(s[i], `${name}[${i}]`).toBeUndefined();
        else expect(s[i], `${name}[${i}]`).toBeCloseTo(b[i]!, 9);
      }
    }
  });

  it('williamsR is unchanged by scaling the input', () => {
    const base = col(williamsR(rangeBars(40), { period: 5 }), 'williamsR');
    const scaled = col(williamsR(rangeBars(40, K), { period: 5 }), 'williamsR');
    for (let i = 0; i < base.length; i += 1) {
      if (base[i] === undefined) expect(scaled[i], `bar ${i}`).toBeUndefined();
      else expect(scaled[i], `bar ${i}`).toBeCloseTo(base[i]!, 9);
    }
  });

  it('donchian scales LINEARLY with the input, like atr', () => {
    // A channel is in the price's units; scaling every price scales it.
    const base = donchian(rangeBars(40), { period: 5 });
    const scaled = donchian(rangeBars(40, 3), { period: 5 });
    for (const name of ['dcUpper', 'dcLower', 'dcMiddle']) {
      const b = col(base, name);
      const s = col(scaled, name);
      for (let i = 0; i < b.length; i += 1) {
        if (b[i] === undefined) expect(s[i], `${name}[${i}]`).toBeUndefined();
        else expect(s[i]! / 3, `${name}[${i}]`).toBeCloseTo(b[i]!, 9);
      }
    }
  });
});

describe('[talib] the range-position studies compose over another study', () => {
  // `close` redirected at an SMA(6) of itself: defined from bar 5. The range
  // is still read off the raw high/low, so this is the shape where the
  // inputs warm up at different times.
  const smoothed = () => sma(rangeBars(40), { period: 6, output: 'sc' });

  it('stochastic starts late rather than coming back empty', () => {
    const r = stochastic(smoothed(), {
      kPeriod: 4,
      slowing: 2,
      dPeriod: 2,
      close: 'sc',
    });
    const k = col(r, 'stochK');
    const d = col(r, 'stochD');
    expect(k).toHaveLength(40);
    // fast %K first at 5 (the close), %K one bar on, %D one more.
    expect(firstValid(k)).toBe(6);
    expect(firstValid(d)).toBe(7);
    expect(k.slice(6).every((x) => x !== undefined)).toBe(true);
    expect(d.slice(7).every((x) => x !== undefined)).toBe(true);
    expect(new Set(k.slice(6)).size).toBeGreaterThan(1);
  });

  it('williamsR starts late rather than coming back empty', () => {
    const v = col(
      williamsR(smoothed(), { period: 4, close: 'sc' }),
      'williamsR',
    );
    expect(v).toHaveLength(40);
    expect(firstValid(v)).toBe(5);
    expect(v.slice(5).every((x) => x !== undefined)).toBe(true);
  });

  it('donchian over a smoothed high starts on its first bar (core count-window contract)', () => {
    // The edges compose on core's rolling reducers, whose window counts
    // ROWS, not values: the channel's upper edge appears on the smoothed
    // high's first bar with whatever the window holds by then — the same
    // contract `sma(sma(...))` and `rollingMax` already ship — rather than
    // `period − 1` bars after it. Pinned so a change to that contract is
    // noticed here.
    const src = sma(rangeBars(40), { period: 6, column: 'high', output: 'sh' });
    const r = donchian(src, { period: 4, high: 'sh' });
    expect(col(r, 'dcUpper')).toHaveLength(40);
    expect(firstValid(col(r, 'dcLower'))).toBe(3); // raw low: period − 1
    expect(firstValid(col(r, 'dcUpper'))).toBe(5); // the smoothed high's own start
    expect(firstValid(col(r, 'dcMiddle'))).toBe(5);
  });
});

describe('[talib] all-missing bars yield all-missing range-position studies', () => {
  const allMissingBars = new TimeSeries({
    name: 'bars',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'high', kind: 'number', required: false },
      { name: 'low', kind: 'number', required: false },
      { name: 'close', kind: 'number', required: false },
    ] as const,
    rows: Array.from({ length: 20 }, (_, i) => [
      i,
      undefined,
      undefined,
      undefined,
    ]) as Array<
      [number, number | undefined, number | undefined, number | undefined]
    >,
  });

  it('stochastic', () => {
    const r = stochastic(allMissingBars as never, { kPeriod: 3 });
    for (const name of ['stochK', 'stochD']) {
      const v = col(r, name);
      expect(v, name).toHaveLength(20);
      expect(
        v.every((x) => x === undefined),
        name,
      ).toBe(true);
    }
  });

  it('williamsR', () => {
    const v = col(
      williamsR(allMissingBars as never, { period: 3 }),
      'williamsR',
    );
    expect(v).toHaveLength(20);
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('donchian', () => {
    const r = donchian(allMissingBars as never, { period: 3 });
    for (const name of ['dcUpper', 'dcLower', 'dcMiddle']) {
      const v = col(r, name);
      expect(v, name).toHaveLength(20);
      expect(
        v.every((x) => x === undefined),
        name,
      ).toBe(true);
    }
  });
});

/* -------------------------------------------------------------------------- */
/* The volume studies. OBV and VWAP each have TWO scale properties, because   */
/* each has two inputs on different axes: OBV is a sum of volumes (linear in  */
/* volume, blind to the price level), VWAP is an average of prices (linear in */
/* price, blind to the volume level). Both halves are real assertions — an    */
/* implementation that weighted OBV by the size of the price change, or       */
/* normalised VWAP by total volume, would pass one and fail the other.        */
/* -------------------------------------------------------------------------- */

const ohlcvSchema = [
  { name: 'time', kind: 'time' },
  { name: 'high', kind: 'number' },
  { name: 'low', kind: 'number' },
  { name: 'close', kind: 'number' },
  { name: 'volume', kind: 'number' },
] as const;

/** 40 wavy bars with volume spikes; `px` scales every price, `vol` every
 *  volume. */
const volumeBars = (px: number, vol: number, n = 40) =>
  new TimeSeries({
    name: 'bars',
    schema: ohlcvSchema,
    rows: Array.from({ length: n }, (_, i) => {
      const c = 100 + 8 * Math.sin(i / 3) + 0.2 * i;
      const v = 1000 + 700 * Math.sin(i / 2.3) + (i % 7 === 3 ? 5000 : 0);
      return [i, (c + 1.1) * px, (c - 0.8) * px, c * px, v * vol];
    }) as Array<[number, number, number, number, number]>,
  });

/** `scaled` is `base` times `factor`, bar for bar, with the same gaps. */
const sameShape = (
  base: Array<number | undefined>,
  scaled: Array<number | undefined>,
  factor: number,
) => {
  expect(scaled).toHaveLength(base.length);
  // At least one defined value, or the loop below pins nothing.
  expect(base.some((x) => x !== undefined)).toBe(true);
  for (let i = 0; i < base.length; i += 1) {
    if (base[i] === undefined) expect(scaled[i], `bar ${i}`).toBeUndefined();
    else expect(scaled[i]! / factor, `bar ${i}`).toBeCloseTo(base[i]!, 9);
  }
};

describe('[talib] volume studies: scale behaviour', () => {
  it('obv scales LINEARLY with volume', () => {
    sameShape(
      col(obv(volumeBars(1, 1)), 'obv'),
      col(obv(volumeBars(1, 7)), 'obv'),
      7,
    );
  });

  it('obv is unchanged by scaling the price', () => {
    // Only the SIGN of each close change is read, and scaling preserves it.
    sameShape(
      col(obv(volumeBars(1, 1)), 'obv'),
      col(obv(volumeBars(1000, 1)), 'obv'),
      1,
    );
  });

  it('vwap scales LINEARLY with price', () => {
    sameShape(
      col(vwap(volumeBars(1, 1), { period: 5 }), 'vwap'),
      col(vwap(volumeBars(1000, 1), { period: 5 }), 'vwap'),
      1000,
    );
  });

  it('vwap is unchanged by scaling the volume', () => {
    // Volume is a WEIGHT: it appears in numerator and denominator alike.
    sameShape(
      col(vwap(volumeBars(1, 1), { period: 5 }), 'vwap'),
      col(vwap(volumeBars(1, 7), { period: 5 }), 'vwap'),
      1,
    );
  });
});

describe('[talib] volume studies over another study compose their warm-up', () => {
  it('obv over a smoothed close starts at its first value, not empty', () => {
    // A running sum carries forward forever, so a leading NaN in the seed
    // would empty the whole column — the rsi(sma(...)) regression, again.
    const smoothed = sma(volumeBars(1, 1), { period: 3, output: 'sc' });
    const v = col(obv(smoothed, { close: 'sc', output: 'o' }), 'o');
    expect(v).toHaveLength(40);
    expect(firstValid(v)).toBe(2); // sma(3) first valid at 2
    // Seeded with THAT bar's volume, then carried — proof the sum ran from
    // the shifted seed rather than from bar 0 with the head skipped.
    const vol = col(smoothed, 'volume');
    expect(v[2]).toBeCloseTo(vol[2]!, 9);
    expect(v.slice(2).every((x) => x !== undefined)).toBe(true);
    expect(new Set(v.slice(2)).size).toBeGreaterThan(1);
  });

  it('vwap over a smoothed close is a count window: it emits once it spans period rows', () => {
    // The same contract `sma(sma(...))` pins in study-missing-cells: a
    // count window emits once it spans `period` ROWS and averages whichever
    // are present. So vwap(4) over sma(3) is first valid at 3, computed from
    // the two rows the inner study has by then — not at 5.
    const smoothed = sma(volumeBars(1, 1), { period: 3, output: 'sc' });
    const v = col(vwap(smoothed, { period: 4, close: 'sc', output: 'w' }), 'w');
    expect(v).toHaveLength(40);
    expect(firstValid(v)).toBe(3);
    expect(v.slice(3).every((x) => x !== undefined)).toBe(true);
  });
});

describe('[talib] all-missing input yields all-missing output (volume studies)', () => {
  const allMissingBars = new TimeSeries({
    name: 'bars',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'high', kind: 'number', required: false },
      { name: 'low', kind: 'number', required: false },
      { name: 'close', kind: 'number', required: false },
      { name: 'volume', kind: 'number', required: false },
    ] as const,
    rows: Array.from({ length: 20 }, (_, i) => [
      i,
      undefined,
      undefined,
      undefined,
      undefined,
    ]) as Array<
      [
        number,
        number | undefined,
        number | undefined,
        number | undefined,
        number | undefined,
      ]
    >,
  });

  it('obv', () => {
    const v = col(obv(allMissingBars as never), 'obv');
    expect(v).toHaveLength(20);
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('vwap', () => {
    const v = col(vwap(allMissingBars as never, { period: 5 }), 'vwap');
    expect(v).toHaveLength(20);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

/*
 * The K2 moving-average engine. The oracle pins the ten types' VALUES against
 * TA-Lib and pandas on one clean input; what it cannot see is whether each is
 * the affine map a moving average is supposed to be. Every type here is
 * LINEAR — `MA(a·x + b) = a·MA(x) + b` — which is a real assertion and not a
 * tautology: a squared efficiency ratio (KAMA), a difference of EMAs
 * (DEMA/TEMA) or a de-lagged input (ZLEMA) each have a plausible wrong form
 * that breaks it.
 */
describe('[talib] the moving-average engine is linear in its input', () => {
  const wavy = [
    10, 13, 11, 15, 12, 16, 14, 18, 15, 19, 16, 20, 17, 21, 18, 22, 19, 23,
  ];
  const scale = 2.5;
  const offset = -40;

  for (const type of MA_TYPES) {
    it(`${type}: MA(a·x + b) = a·MA(x) + b`, () => {
      const base = col(movingAverage(bars(wavy), { period: 5, type }), 'ma');
      const mapped = col(
        movingAverage(bars(wavy.map((x) => x * scale + offset)), {
          period: 5,
          type,
        }),
        'ma',
      );
      expect(firstValid(mapped), `${type} warm-up`).toBe(firstValid(base));
      for (let i = 0; i < base.length; i += 1) {
        if (base[i] === undefined) {
          expect(mapped[i], `${type} bar ${i}`).toBeUndefined();
        } else {
          expect(mapped[i], `${type} bar ${i}`).toBeCloseTo(
            base[i]! * scale + offset,
            9,
          );
        }
      }
    });
  }

  it('every type returns the constant on a constant input', () => {
    // The degenerate case of linearity (a = 0), and the one place a weighting
    // bug hides: on a flat line every weighting scheme agrees, so this is a
    // NECESSARY condition and never a sufficient one. It is here because a
    // normaliser that did not match its weights fails it — a WMA divided by
    // `period` instead of `period(period+1)/2` reads 7.35 on a constant 21.
    const flat = Array.from({ length: 30 }, () => 21);
    for (const type of MA_TYPES) {
      const v = col(movingAverage(bars(flat), { period: 5, type }), 'ma');
      const seen = v.filter((x) => x !== undefined);
      expect(seen.length, type).toBeGreaterThan(0);
      for (const x of seen) expect(x, type).toBeCloseTo(21, 9);
    }
  });

  it('every type composes over another study’s output', () => {
    // The `rsi(sma(...))` shape: a study run over a column with a warm-up
    // head must start LATE, not come back empty.
    for (const type of MA_TYPES) {
      const stacked = movingAverage(
        sma(bars(wavy), { period: 3, output: 'sma3' }),
        { period: 4, type, column: 'sma3' as never, output: 'stacked' },
      );
      const v = col(stacked, 'stacked');
      expect(v, type).toHaveLength(wavy.length);
      expect(firstValid(v), type).toBeGreaterThan(0);
      expect(v.at(-1), type).not.toBeUndefined();
    }
  });

  it('every type reads all-missing over an all-missing input', () => {
    const empty = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'close', kind: 'number', required: false },
      ] as const,
      rows: Array.from({ length: 20 }, (_, i) => [i, undefined]) as never,
    });
    for (const type of MA_TYPES) {
      const v = col(movingAverage(empty as never, { period: 5, type }), 'ma');
      expect(v, type).toHaveLength(20);
      expect(
        v.every((x) => x === undefined),
        type,
      ).toBe(true);
    }
  });
});

/* -------------------------------------------------------------------------- */
/* The K2 consumers. Two families with OPPOSITE scale behaviour, and each     */
/* assertion is the one its family should satisfy: the channels (keltner,     */
/* atrBands) and QStick are in the units of the price and scale LINEARLY;     */
/* the rate studies (trix, coppock) are ratios and are INVARIANT. A study     */
/* that normalised by the price level would pass one family's test and fail   */
/* the other's, which is the point of asserting both.                          */
/* -------------------------------------------------------------------------- */

const k2OhlcvSchema = [
  { name: 'time', kind: 'time' },
  { name: 'open', kind: 'number' },
  { name: 'high', kind: 'number' },
  { name: 'low', kind: 'number' },
  { name: 'close', kind: 'number' },
] as const;

/** Wavy bars with a varying range and a body that changes sign. `k` scales
 *  every price; `shift` adds a constant to every price. */
const k2Bars = (n: number, k = 1, shift = 0) =>
  new TimeSeries({
    name: 'bars',
    schema: k2OhlcvSchema,
    rows: Array.from({ length: n }, (_, i) => {
      const c = 100 + 8 * Math.sin(i / 3) + 0.2 * i;
      const o = c - 0.5 * Math.cos(i / 1.7);
      return [
        i,
        o * k + shift,
        (c + 0.4 + 0.6 * Math.abs(Math.sin(i / 2))) * k + shift,
        (c - 0.5 - 0.6 * Math.abs(Math.cos(i / 2.5))) * k + shift,
        c * k + shift,
      ];
    }) as Array<[number, number, number, number, number]>,
  });

describe('[talib] the K2 channels are linear in price', () => {
  const K = 1000;

  it('keltner scales LINEARLY with the input', () => {
    const base = keltner(k2Bars(40), { period: 6, atrPeriod: 5 });
    const scaled = keltner(k2Bars(40, K), { period: 6, atrPeriod: 5 });
    for (const name of ['kcMiddle', 'kcUpper', 'kcLower']) {
      const b = col(base, name);
      const s = col(scaled, name);
      expect(b.filter((x) => x !== undefined).length, name).toBeGreaterThan(30);
      for (let i = 0; i < b.length; i += 1) {
        if (b[i] === undefined) expect(s[i], `${name}[${i}]`).toBeUndefined();
        else expect(s[i]! / K, `${name}[${i}]`).toBeCloseTo(b[i]!, 6);
      }
    }
  });

  it('keltner TRANSLATES with the input — a channel, not a normalised one', () => {
    // Adding a constant to every price moves the whole channel by it and
    // leaves its WIDTH alone (true range is a difference). A study that
    // divided by the price level would fail this while passing the scale
    // test above.
    const base = keltner(k2Bars(40), { period: 6, atrPeriod: 5 });
    const moved = keltner(k2Bars(40, 1, 50), { period: 6, atrPeriod: 5 });
    const b = col(base, 'kcUpper');
    const m = col(moved, 'kcUpper');
    const bw = col(base, 'kcUpper').map((x, i) =>
      x === undefined ? undefined : x - col(base, 'kcLower')[i]!,
    );
    const mw = col(moved, 'kcUpper').map((x, i) =>
      x === undefined ? undefined : x - col(moved, 'kcLower')[i]!,
    );
    for (let i = 0; i < b.length; i += 1) {
      if (b[i] === undefined) continue;
      expect(m[i]! - 50, `bar ${i}`).toBeCloseTo(b[i]!, 6);
      expect(mw[i], `width ${i}`).toBeCloseTo(bw[i]!, 6);
    }
  });

  it('atrBands scales LINEARLY with the input', () => {
    const base = atrBands(k2Bars(40), { period: 5 });
    const scaled = atrBands(k2Bars(40, K), { period: 5 });
    for (const name of ['atrbUpper', 'atrbLower']) {
      const b = col(base, name);
      const s = col(scaled, name);
      expect(b.filter((x) => x !== undefined).length, name).toBeGreaterThan(30);
      for (let i = 0; i < b.length; i += 1) {
        if (b[i] === undefined) expect(s[i], `${name}[${i}]`).toBeUndefined();
        else expect(s[i]! / K, `${name}[${i}]`).toBeCloseTo(b[i]!, 6);
      }
    }
  });

  it('qstick scales LINEARLY with the input and is unmoved by a shift', () => {
    // Both halves matter: the body is a DIFFERENCE of two prices, so it
    // scales with them and is blind to their common level.
    const base = col(qstick(k2Bars(40), { period: 5 }), 'qstick');
    const scaled = col(qstick(k2Bars(40, K), { period: 5 }), 'qstick');
    const moved = col(qstick(k2Bars(40, 1, 50), { period: 5 }), 'qstick');
    expect(base.filter((x) => x !== undefined).length).toBeGreaterThan(30);
    // Not flat, or "scales linearly" would hold vacuously.
    expect(new Set(base.slice(4)).size).toBeGreaterThan(20);
    expect(base.some((x) => x !== undefined && x > 0)).toBe(true);
    expect(base.some((x) => x !== undefined && x < 0)).toBe(true);
    for (let i = 0; i < base.length; i += 1) {
      if (base[i] === undefined) {
        expect(scaled[i], `bar ${i}`).toBeUndefined();
        continue;
      }
      expect(scaled[i]! / K, `bar ${i}`).toBeCloseTo(base[i]!, 6);
      expect(moved[i], `shift bar ${i}`).toBeCloseTo(base[i]!, 6);
    }
  });
});

describe('[talib] the K2 rate studies are scale-invariant', () => {
  const wavy = Array.from(
    { length: 60 },
    (_, i) => 100 + 8 * Math.sin(i / 3) + 3 * Math.sin(i / 1.3),
  );

  it('trix is UNCHANGED by scaling the input', () => {
    // A rate of change of a linear filter of the price: the constant factor
    // cancels. Checked at 1e5 and 1e-5, where the RSI test above showed
    // float error can bite.
    const base = trix(bars(wavy), { period: 4, signalPeriod: 3 });
    for (const k of [1e5, 1e-5]) {
      const scaled = trix(bars(wavy.map((x) => x * k)), {
        period: 4,
        signalPeriod: 3,
      });
      for (const name of ['trix', 'trixSignal']) {
        const b = col(base, name);
        const s = col(scaled, name);
        expect(b.filter((x) => x !== undefined).length, name).toBeGreaterThan(
          30,
        );
        for (let i = 0; i < b.length; i += 1) {
          if (b[i] === undefined)
            expect(s[i], `×${k} ${name}[${i}]`).toBeUndefined();
          else expect(s[i], `×${k} ${name}[${i}]`).toBeCloseTo(b[i]!, 9);
        }
      }
    }
    // Not identically zero, or invariance would hold for the wrong reason.
    expect(
      col(base, 'trix').some((x) => x !== undefined && Math.abs(x) > 0.01),
    ).toBe(true);
  });

  it('coppock is UNCHANGED by scaling the input', () => {
    const base = col(
      coppock(bars(wavy), { longPeriod: 8, shortPeriod: 5, wmaPeriod: 4 }),
      'coppock',
    );
    expect(base.filter((x) => x !== undefined).length).toBeGreaterThan(40);
    expect(base.some((x) => x !== undefined && Math.abs(x) > 0.01)).toBe(true);
    for (const k of [1e5, 1e-5]) {
      const scaled = col(
        coppock(bars(wavy.map((x) => x * k)), {
          longPeriod: 8,
          shortPeriod: 5,
          wmaPeriod: 4,
        }),
        'coppock',
      );
      for (let i = 0; i < base.length; i += 1) {
        if (base[i] === undefined)
          expect(scaled[i], `×${k} bar ${i}`).toBeUndefined();
        else expect(scaled[i], `×${k} bar ${i}`).toBeCloseTo(base[i]!, 9);
      }
    }
  });
});

describe('[talib] the K2 consumers compose over another study', () => {
  const wavy = Array.from(
    { length: 60 },
    (_, i) => 100 + 8 * Math.sin(i / 3) + 3 * Math.sin(i / 1.3),
  );

  it('trix over sma starts late rather than coming back empty', () => {
    const src = sma(bars(wavy), { period: 3, output: 'mid' });
    const r = trix(src, { period: 3, signalPeriod: 3, column: 'mid' });
    const line = col(r, 'trix');
    expect(line).toHaveLength(60);
    // sma(3) first valid at 2; three 3-bar EMAs cost 6 more, the rate of
    // change one more.
    expect(firstValid(line)).toBe(9);
    expect(line.slice(9).every((x) => x !== undefined)).toBe(true);
    expect(firstValid(col(r, 'trixSignal'))).toBe(11);
  });

  it('coppock over sma starts late rather than coming back empty', () => {
    const src = sma(bars(wavy), { period: 3, output: 'mid' });
    const v = col(
      coppock(src, {
        column: 'mid',
        longPeriod: 6,
        shortPeriod: 3,
        wmaPeriod: 2,
        output: 'c',
      }),
      'c',
    );
    expect(v).toHaveLength(60);
    // sma(3) at 2; the 6-bar rate of change 6 later; the 2-bar WMA 1 more.
    expect(firstValid(v)).toBe(9);
    expect(v.slice(9).every((x) => x !== undefined)).toBe(true);
  });

  it('atrBands over sma draws the band around the smoothed line', () => {
    const src = sma(k2Bars(40), { period: 6, output: 'mid' });
    const r = atrBands(src, { period: 5, column: 'mid' });
    const up = col(r, 'atrbUpper');
    expect(up).toHaveLength(40);
    // The ATR is defined from bar 5 and `mid` from bar 5 as well, so the
    // band starts there and never goes missing again.
    expect(firstValid(up)).toBe(5);
    expect(up.slice(5).every((x) => x !== undefined)).toBe(true);
  });

  it('qstick over a smoothed close composes its warm-up', () => {
    const src = sma(k2Bars(40), { period: 4, output: 'mid' });
    const v = col(qstick(src, { period: 3, close: 'mid' }), 'qstick');
    expect(v).toHaveLength(40);
    // `mid` first valid at 3; the body then needs 3 finite values.
    expect(firstValid(v)).toBe(5);
    expect(v.slice(5).every((x) => x !== undefined)).toBe(true);
  });
});

describe('[talib] all-missing input yields all-missing K2 consumers', () => {
  const allMissing = new TimeSeries({
    name: 'bars',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'open', kind: 'number', required: false },
      { name: 'high', kind: 'number', required: false },
      { name: 'low', kind: 'number', required: false },
      { name: 'close', kind: 'number', required: false },
    ] as const,
    rows: Array.from({ length: 20 }, (_, i) => [
      i,
      undefined,
      undefined,
      undefined,
      undefined,
    ]) as never,
  });

  const allBlank = (result: unknown, names: string[]) => {
    for (const name of names) {
      const v = col(result, name);
      expect(v, name).toHaveLength(20);
      expect(
        v.every((x) => x === undefined),
        name,
      ).toBe(true);
    }
  };

  it('keltner', () => {
    allBlank(keltner(allMissing, { period: 3, atrPeriod: 3 }), [
      'kcMiddle',
      'kcUpper',
      'kcLower',
    ]);
  });

  it('atrBands', () => {
    allBlank(atrBands(allMissing, { period: 3 }), ['atrbUpper', 'atrbLower']);
  });

  it('qstick', () => {
    allBlank(qstick(allMissing, { period: 3 }), ['qstick']);
  });

  it('trix', () => {
    allBlank(trix(allMissing, { period: 3, signalPeriod: 3 }), [
      'trix',
      'trixSignal',
    ]);
  });

  it('coppock', () => {
    allBlank(
      coppock(allMissing, { longPeriod: 4, shortPeriod: 2, wmaPeriod: 2 }),
      ['coppock'],
    );
  });
});

/* ==========================================================================
 * The K2 oscillators: scale, shift, composition, all-missing.
 *
 * Two families with opposite scale behaviour, which is what makes either
 * assertion worth writing: the PERCENT forms (`priceOscillator`'s default
 * mode, `disparityIndex`) normalise by their own denominator and are
 * INVARIANT; the difference forms (`priceOscillator({ mode: 'absolute' })`,
 * `detrendedPriceOscillator`, `elderRay`, `awesomeOscillator`) are in price
 * units and are LINEAR. An implementation that normalised where it should
 * not — or forgot to — passes one of these and fails the other.
 * ========================================================================== */

const oscBars = (rows: Array<[number, number, number]>) =>
  new TimeSeries({
    name: 'bars',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'high', kind: 'number' },
      { name: 'low', kind: 'number' },
      { name: 'close', kind: 'number' },
    ] as const,
    rows: rows.map(([h, l, c], i) => [i, h, l, c]) as Array<
      [number, number, number, number]
    >,
  });

/** Non-degenerate bars: never monotonic, varying range, close never on an
 *  extreme. `a` scales every price, `b` shifts every price. */
const oscRows = (a = 1, b = 0): Array<[number, number, number]> =>
  Array.from({ length: 40 }, (_, i) => {
    const c = 100 + 8 * Math.sin(i / 3.5) + 0.3 * i;
    const up = 0.4 + 0.7 * Math.abs(Math.sin(i / 2.3));
    const down = 0.4 + 0.7 * Math.abs(Math.cos(i / 1.9));
    return [(c + up) * a + b, (c - down) * a + b, c * a + b];
  });

/** Assert `f(scaled)` is `k ×` `f(base)` wherever base has a value, and
 *  missing on exactly the same rows. */
const expectLinear = (
  base: Array<number | undefined>,
  scaled: Array<number | undefined>,
  k: number,
) => {
  for (let i = 0; i < base.length; i += 1) {
    if (base[i] === undefined) expect(scaled[i], `bar ${i}`).toBeUndefined();
    else expect(scaled[i]! / k, `bar ${i}`).toBeCloseTo(base[i]!, 8);
  }
};

/** Assert two runs agree bar for bar (invariance). */
const expectSame = (
  base: Array<number | undefined>,
  other: Array<number | undefined>,
) => {
  for (let i = 0; i < base.length; i += 1) {
    if (base[i] === undefined) expect(other[i], `bar ${i}`).toBeUndefined();
    else expect(other[i], `bar ${i}`).toBeCloseTo(base[i]!, 8);
  }
};

describe('[talib] the K2 oscillators: scale and shift behaviour', () => {
  const K = 1000;

  it('priceOscillator PERCENT is unchanged by scaling the input', () => {
    const opts = { fastPeriod: 3, slowPeriod: 7, maType: 'ema' } as const;
    const base = col(priceOscillator(oscBars(oscRows()), opts), 'priceOsc');
    const scaled = col(priceOscillator(oscBars(oscRows(K)), opts), 'priceOsc');
    expectSame(base, scaled);
    expect(base.some((x) => x !== undefined && x !== 0)).toBe(true);
  });

  it('priceOscillator PERCENT is NOT shift-invariant (its denominator moves)', () => {
    // The companion assertion: adding a constant to every price changes the
    // denominator without changing the numerator, so a percent-of-price
    // reading must move. A study that quietly dropped the division would
    // pass the scale test above and fail this one.
    const opts = { fastPeriod: 3, slowPeriod: 7, maType: 'ema' } as const;
    const base = col(priceOscillator(oscBars(oscRows()), opts), 'priceOsc');
    const shifted = col(
      priceOscillator(oscBars(oscRows(1, 500)), opts),
      'priceOsc',
    );
    const bar = 30;
    expect(base[bar]).toBeDefined();
    expect(Math.abs(shifted[bar]! - base[bar]!)).toBeGreaterThan(1e-6);
  });

  it('priceOscillator ABSOLUTE scales LINEARLY and is shift-invariant', () => {
    const opts = {
      fastPeriod: 3,
      slowPeriod: 7,
      maType: 'ema',
      mode: 'absolute',
    } as const;
    const base = col(priceOscillator(oscBars(oscRows()), opts), 'priceOsc');
    expectLinear(
      base,
      col(priceOscillator(oscBars(oscRows(K)), opts), 'priceOsc'),
      K,
    );
    expectSame(
      base,
      col(priceOscillator(oscBars(oscRows(1, 500)), opts), 'priceOsc'),
    );
  });

  it('disparityIndex is unchanged by scaling, and is NOT shift-invariant', () => {
    const opts = { period: 5, maType: 'sma' } as const;
    const base = col(disparityIndex(oscBars(oscRows()), opts), 'disparity');
    expectSame(
      base,
      col(disparityIndex(oscBars(oscRows(K)), opts), 'disparity'),
    );
    const shifted = col(
      disparityIndex(oscBars(oscRows(1, 500)), opts),
      'disparity',
    );
    expect(base[30]).toBeDefined();
    expect(Math.abs(shifted[30]! - base[30]!)).toBeGreaterThan(1e-6);
  });

  it('detrendedPriceOscillator scales LINEARLY and is shift-invariant', () => {
    const opts = { period: 5, maType: 'sma' } as const;
    const base = col(detrendedPriceOscillator(oscBars(oscRows()), opts), 'dpo');
    expectLinear(
      base,
      col(detrendedPriceOscillator(oscBars(oscRows(K)), opts), 'dpo'),
      K,
    );
    expectSame(
      base,
      col(detrendedPriceOscillator(oscBars(oscRows(1, 500)), opts), 'dpo'),
    );
  });

  it('elderRay scales LINEARLY and is shift-invariant on both legs', () => {
    const opts = { period: 5 } as const;
    for (const name of ['elderBull', 'elderBear']) {
      const base = col(elderRay(oscBars(oscRows()), opts), name);
      expectLinear(base, col(elderRay(oscBars(oscRows(K)), opts), name), K);
      expectSame(base, col(elderRay(oscBars(oscRows(1, 500)), opts), name));
    }
  });

  it('awesomeOscillator scales LINEARLY and is shift-invariant', () => {
    const opts = { fastPeriod: 3, slowPeriod: 8 } as const;
    const base = col(awesomeOscillator(oscBars(oscRows()), opts), 'ao');
    expectLinear(
      base,
      col(awesomeOscillator(oscBars(oscRows(K)), opts), 'ao'),
      K,
    );
    expectSame(
      base,
      col(awesomeOscillator(oscBars(oscRows(1, 500)), opts), 'ao'),
    );
  });
});

describe('[talib] the K2 oscillators over another study compose their warm-up', () => {
  const wavy = Array.from(
    { length: 30 },
    (_, i) => 100 + 6 * Math.sin(i / 2.5) + i * 0.1,
  );

  it('priceOscillator over sma starts late rather than coming back empty', () => {
    const src = sma(bars(wavy), { period: 3 });
    const v = col(
      priceOscillator(src, {
        column: 'sma',
        fastPeriod: 2,
        slowPeriod: 3,
        maType: 'ema',
      }),
      'priceOsc',
    );
    expect(v).toHaveLength(wavy.length);
    // sma(3) first valid at 2; the slow EMA then needs 3 finite samples.
    expect(firstValid(v)).toBe(4);
    expect(v.slice(4).every((x) => x !== undefined)).toBe(true);
  });

  it('disparityIndex over sma starts late rather than coming back empty', () => {
    const src = sma(bars(wavy), { period: 3 });
    const v = col(
      disparityIndex(src, { column: 'sma', period: 3, maType: 'ema' }),
      'disparity',
    );
    expect(v).toHaveLength(wavy.length);
    expect(firstValid(v)).toBe(4);
    expect(v.slice(4).every((x) => x !== undefined)).toBe(true);
  });

  it('detrendedPriceOscillator over sma composes the warm-up AND the shift', () => {
    const src = sma(bars(wavy), { period: 3 });
    const v = col(
      detrendedPriceOscillator(src, {
        column: 'sma',
        period: 3,
        maType: 'ema',
      }),
      'dpo',
    );
    expect(v).toHaveLength(wavy.length);
    // sma(3) at 2, the EMA 2 more, then the 2-bar displacement.
    expect(firstValid(v)).toBe(6);
    expect(v.slice(6).every((x) => x !== undefined)).toBe(true);
  });

  it('elderRay over a smoothed close starts late rather than coming back empty', () => {
    const rows = Array.from({ length: 30 }, (_, i) => {
      const c = 100 + 5 * Math.sin(i / 3);
      return [c + 1.2, c - 0.9, c] as [number, number, number];
    });
    const smoothed = sma(oscBars(rows), { period: 3, output: 'sc' });
    const v = col(
      elderRay(smoothed, { period: 3, close: 'sc', prefix: 'e' }),
      'eBull',
    );
    expect(v).toHaveLength(30);
    // sma(3) first valid at 2, then the EMA needs 3 finite samples: 4.
    expect(firstValid(v)).toBe(4);
    expect(v.slice(4).every((x) => x !== undefined)).toBe(true);
  });

  it('awesomeOscillator over smoothed high/low starts late rather than empty', () => {
    const rows = Array.from({ length: 30 }, (_, i) => {
      const c = 100 + 5 * Math.sin(i / 3);
      return [c + 1.2, c - 0.9, c] as [number, number, number];
    });
    const sh = sma(oscBars(rows), { period: 3, column: 'high', output: 'sh' });
    const sl = sma(sh, { period: 3, column: 'low', output: 'sl' });
    const v = col(
      awesomeOscillator(sl, {
        fastPeriod: 2,
        slowPeriod: 3,
        high: 'sh',
        low: 'sl',
      }),
      'ao',
    );
    expect(v).toHaveLength(30);
    // The smoothed inputs are first valid at 2, so the derived median is
    // too; the slow leg then needs 3 finite values of it.
    expect(firstValid(v)).toBe(4);
    expect(v.slice(4).every((x) => x !== undefined)).toBe(true);
  });
});

describe('[talib] all-missing input yields all-missing K2 oscillators', () => {
  const allMissingBars = new TimeSeries({
    name: 'bars',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'high', kind: 'number', required: false },
      { name: 'low', kind: 'number', required: false },
      { name: 'close', kind: 'number', required: false },
    ] as const,
    rows: Array.from({ length: 20 }, (_, i) => [
      i,
      undefined,
      undefined,
      undefined,
    ]) as Array<
      [number, number | undefined, number | undefined, number | undefined]
    >,
  });
  const empty = (v: Array<number | undefined>) => {
    expect(v).toHaveLength(20);
    expect(v.every((x) => x === undefined)).toBe(true);
  };

  it('priceOscillator, in both modes', () => {
    empty(
      col(
        priceOscillator(allMissingBars as never, {
          fastPeriod: 2,
          slowPeriod: 5,
        }),
        'priceOsc',
      ),
    );
    empty(
      col(
        priceOscillator(allMissingBars as never, {
          fastPeriod: 2,
          slowPeriod: 5,
          mode: 'absolute',
        }),
        'priceOsc',
      ),
    );
  });

  it('disparityIndex and detrendedPriceOscillator', () => {
    empty(
      col(disparityIndex(allMissingBars as never, { period: 5 }), 'disparity'),
    );
    empty(
      col(
        detrendedPriceOscillator(allMissingBars as never, { period: 5 }),
        'dpo',
      ),
    );
  });

  it('elderRay and awesomeOscillator', () => {
    const r = elderRay(allMissingBars as never, { period: 5 });
    empty(col(r, 'elderBull'));
    empty(col(r, 'elderBear'));
    empty(
      col(
        awesomeOscillator(allMissingBars as never, {
          fastPeriod: 2,
          slowPeriod: 5,
        }),
        'ao',
      ),
    );
  });
});

/*
 * The volume & money-flow group (corpus §6.6). Its scale behaviour is the
 * least uniform in the package, which is exactly why it is worth pinning:
 * three of the eight are invariant in price, two are linear in it, and one
 * (ease of movement) is QUADRATIC in it — the distance and the range both
 * scale. Getting that wrong is a plausible bug that no value oracle on one
 * fixture would catch.
 */
/** {@link sameShape} with a RELATIVE tolerance. The shift-invariance and
 *  quadratic-scale checks below compare quantities in the 1e5 range whose
 *  operands round differently once every price moves, so agreement is exact
 *  to ~1e-15 relative and not to 1e-9 absolute; asserting the absolute form
 *  would be asserting the rounding, not the property. */
const sameShapeRelative = (
  base: Array<number | undefined>,
  scaled: Array<number | undefined>,
  factor: number,
) => {
  expect(scaled).toHaveLength(base.length);
  expect(base.some((x) => x !== undefined)).toBe(true);
  for (let i = 0; i < base.length; i += 1) {
    if (base[i] === undefined) {
      expect(scaled[i], `bar ${i}`).toBeUndefined();
      continue;
    }
    const expected = base[i]!;
    const actual = scaled[i]! / factor;
    expect(
      Math.abs(actual - expected) <= 1e-9 * Math.max(1, Math.abs(expected)),
      `bar ${i}: ${actual} vs ${expected}`,
    ).toBe(true);
  }
};

/** The same 40 wavy bars as {@link volumeBars}, with every PRICE shifted by
 *  `delta` and volume untouched — for the affine-invariance half. */
const shiftedVolumeBars = (delta: number, n = 40) =>
  new TimeSeries({
    name: 'bars',
    schema: ohlcvSchema,
    rows: Array.from({ length: n }, (_, i) => {
      const c = 100 + 8 * Math.sin(i / 3) + 0.2 * i;
      const v = 1000 + 700 * Math.sin(i / 2.3) + (i % 7 === 3 ? 5000 : 0);
      return [i, c + 1.1 + delta, c - 0.8 + delta, c + delta, v];
    }) as Array<[number, number, number, number, number]>,
  });

describe('[talib] the money-flow studies: scale and shift behaviour', () => {
  it('accumulationDistribution scales LINEARLY with volume', () => {
    sameShape(
      col(accumulationDistribution(volumeBars(1, 1)), 'ad'),
      col(accumulationDistribution(volumeBars(1, 7)), 'ad'),
      7,
    );
  });

  it('accumulationDistribution is invariant under ANY affine change of price', () => {
    // The close location is a ratio of differences: both a scale and a shift
    // cancel. (Scaling would survive a study that forgot to divide by the
    // range; shifting would not.)
    sameShape(
      col(accumulationDistribution(volumeBars(1, 1)), 'ad'),
      col(accumulationDistribution(volumeBars(1000, 1)), 'ad'),
      1,
    );
    sameShapeRelative(
      col(accumulationDistribution(volumeBars(1, 1)), 'ad'),
      col(accumulationDistribution(shiftedVolumeBars(500)), 'ad'),
      1,
    );
  });

  it('chaikinOscillator inherits both — linear in volume, invariant in price', () => {
    sameShape(
      col(chaikinOscillator(volumeBars(1, 1)), 'chaikinOsc'),
      col(chaikinOscillator(volumeBars(1, 7)), 'chaikinOsc'),
      7,
    );
    sameShapeRelative(
      col(chaikinOscillator(volumeBars(1, 1)), 'chaikinOsc'),
      col(chaikinOscillator(shiftedVolumeBars(500)), 'chaikinOsc'),
      1,
    );
  });

  it('priceVolumeTrend is linear in volume and scale-invariant in price', () => {
    sameShape(
      col(priceVolumeTrend(volumeBars(1, 1)), 'pvt'),
      col(priceVolumeTrend(volumeBars(1, 7)), 'pvt'),
      7,
    );
    sameShapeRelative(
      col(priceVolumeTrend(volumeBars(1, 1)), 'pvt'),
      col(priceVolumeTrend(volumeBars(1000, 1)), 'pvt'),
      1,
    );
  });

  it('priceVolumeTrend is NOT shift-invariant — the fraction has a base', () => {
    // Unlike A/D: a shift changes the denominator of every rate of change.
    // Asserted rather than left implicit, because "invariant in price" is
    // the wrong summary for this one.
    const base = col(priceVolumeTrend(volumeBars(1, 1)), 'pvt');
    const shifted = col(priceVolumeTrend(shiftedVolumeBars(500)), 'pvt');
    expect(shifted[39]).not.toBeCloseTo(base[39]!, 6);
  });

  it('chaikinMoneyFlow is invariant in volume AND in any affine price change', () => {
    sameShape(
      col(chaikinMoneyFlow(volumeBars(1, 1), { period: 10 }), 'cmf'),
      col(chaikinMoneyFlow(volumeBars(1, 7), { period: 10 }), 'cmf'),
      1,
    );
    sameShape(
      col(chaikinMoneyFlow(volumeBars(1, 1), { period: 10 }), 'cmf'),
      col(chaikinMoneyFlow(shiftedVolumeBars(500), { period: 10 }), 'cmf'),
      1,
    );
    for (const x of col(
      chaikinMoneyFlow(volumeBars(1, 1), { period: 10 }),
      'cmf',
    ))
      if (x !== undefined) expect(Math.abs(x)).toBeLessThanOrEqual(1);
  });

  it('moneyFlowIndex is invariant in price scale and in volume scale', () => {
    // A ratio of flows: both factors cancel. It is the `rsi` side of the
    // scale pair, and it stays inside 0..100.
    sameShape(
      col(moneyFlowIndex(volumeBars(1, 1), { period: 10 }), 'mfi'),
      col(moneyFlowIndex(volumeBars(1000, 1), { period: 10 }), 'mfi'),
      1,
    );
    sameShape(
      col(moneyFlowIndex(volumeBars(1, 1), { period: 10 }), 'mfi'),
      col(moneyFlowIndex(volumeBars(1, 7), { period: 10 }), 'mfi'),
      1,
    );
    for (const x of col(
      moneyFlowIndex(volumeBars(1, 1), { period: 10 }),
      'mfi',
    ))
      if (x !== undefined) {
        expect(x).toBeGreaterThanOrEqual(0);
        expect(x).toBeLessThanOrEqual(100);
      }
  });

  it('forceIndex is linear in price, linear in volume, and shift-invariant', () => {
    sameShapeRelative(
      col(forceIndex(volumeBars(1, 1), { period: 5 }), 'force'),
      col(forceIndex(volumeBars(1000, 1), { period: 5 }), 'force'),
      1000,
    );
    sameShape(
      col(forceIndex(volumeBars(1, 1), { period: 5 }), 'force'),
      col(forceIndex(volumeBars(1, 7), { period: 5 }), 'force'),
      7,
    );
    sameShapeRelative(
      col(forceIndex(volumeBars(1, 1), { period: 5 }), 'force'),
      col(forceIndex(shiftedVolumeBars(500), { period: 5 }), 'force'),
      1,
    );
  });

  it('easeOfMovement is QUADRATIC in price, inverse in volume, linear in scale', () => {
    // The distance moved scales with price and so does the bar's range, so
    // the reading scales with the SQUARE — the one study here that does.
    sameShapeRelative(
      col(easeOfMovement(volumeBars(1, 1), { period: 5 }), 'eom'),
      col(easeOfMovement(volumeBars(10, 1), { period: 5 }), 'eom'),
      100,
    );
    sameShapeRelative(
      col(easeOfMovement(volumeBars(1, 1), { period: 5 }), 'eom'),
      col(easeOfMovement(volumeBars(1, 7), { period: 5 }), 'eom'),
      1 / 7,
    );
    sameShapeRelative(
      col(easeOfMovement(volumeBars(1, 1), { period: 5 }), 'eom'),
      col(
        easeOfMovement(volumeBars(1, 1), { period: 5, scale: 300_000_000 }),
        'eom',
      ),
      3,
    );
    // Shifting every price leaves both the distance and the range alone.
    sameShapeRelative(
      col(easeOfMovement(volumeBars(1, 1), { period: 5 }), 'eom'),
      col(easeOfMovement(shiftedVolumeBars(500), { period: 5 }), 'eom'),
      1,
    );
  });

  it('volumeOscillator is invariant in volume and blind to price', () => {
    sameShape(
      col(volumeOscillator(volumeBars(1, 1)), 'volOsc'),
      col(volumeOscillator(volumeBars(1, 7)), 'volOsc'),
      1,
    );
    sameShape(
      col(volumeOscillator(volumeBars(1, 1)), 'volOsc'),
      col(volumeOscillator(volumeBars(1000, 1)), 'volOsc'),
      1,
    );
  });
});

describe('[talib] the money-flow studies over another study compose their warm-up', () => {
  it('priceVolumeTrend over a smoothed close starts at its first value, not empty', () => {
    // The running-sum seed shift — the rsi(sma(...)) regression shape. A
    // leading NaN in a cumulative study must move the seed, not empty it.
    const smoothed = sma(volumeBars(1, 1), { period: 3, output: 'sc' });
    const v = col(
      priceVolumeTrend(smoothed, { close: 'sc', output: 'p' }),
      'p',
    );
    expect(v).toHaveLength(40);
    expect(firstValid(v)).toBe(3); // sma(3) valid at 2; its first CHANGE at 3
    expect(v.slice(3).every((x) => x !== undefined)).toBe(true);
    expect(new Set(v.slice(3)).size).toBeGreaterThan(1);
  });

  it('forceIndex over a smoothed close composes rather than emptying', () => {
    const smoothed = sma(volumeBars(1, 1), { period: 3, output: 'sc' });
    const v = col(
      forceIndex(smoothed, { period: 4, close: 'sc', output: 'f' }),
      'f',
    );
    expect(v).toHaveLength(40);
    // The EMA array door steps over the NaN head and waits for 4 finite raw
    // forces: the first is at bar 3, so the fourth is at bar 6.
    expect(firstValid(v)).toBe(6);
    expect(v.slice(6).every((x) => x !== undefined)).toBe(true);
  });

  it('chaikinMoneyFlow over a smoothed close is a count window', () => {
    // The same contract sma(sma(...)) pins: a count window emits once it
    // spans `period` ROWS, computed from whichever are present.
    const smoothed = sma(volumeBars(1, 1), { period: 3, output: 'sc' });
    const v = col(
      chaikinMoneyFlow(smoothed, { period: 4, close: 'sc', output: 'c' }),
      'c',
    );
    expect(v).toHaveLength(40);
    expect(firstValid(v)).toBe(3);
    expect(v.slice(3).every((x) => x !== undefined)).toBe(true);
  });
});

describe('[talib] all-missing input yields all-missing money-flow studies', () => {
  const allMissingBars = new TimeSeries({
    name: 'bars',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'high', kind: 'number', required: false },
      { name: 'low', kind: 'number', required: false },
      { name: 'close', kind: 'number', required: false },
      { name: 'volume', kind: 'number', required: false },
    ] as const,
    rows: Array.from({ length: 20 }, (_, i) => [
      i,
      undefined,
      undefined,
      undefined,
      undefined,
    ]) as Array<
      [
        number,
        number | undefined,
        number | undefined,
        number | undefined,
        number | undefined,
      ]
    >,
  });
  const allMissing = (result: unknown, name: string) => {
    const v = col(result, name);
    expect(v).toHaveLength(20);
    expect(v.every((x) => x === undefined)).toBe(true);
  };

  it('accumulationDistribution', () => {
    allMissing(accumulationDistribution(allMissingBars as never), 'ad');
  });
  it('chaikinOscillator', () => {
    allMissing(chaikinOscillator(allMissingBars as never), 'chaikinOsc');
  });
  it('priceVolumeTrend', () => {
    allMissing(priceVolumeTrend(allMissingBars as never), 'pvt');
  });
  it('chaikinMoneyFlow', () => {
    allMissing(chaikinMoneyFlow(allMissingBars as never, { period: 5 }), 'cmf');
  });
  it('moneyFlowIndex', () => {
    allMissing(moneyFlowIndex(allMissingBars as never, { period: 5 }), 'mfi');
  });
  it('forceIndex', () => {
    allMissing(forceIndex(allMissingBars as never, { period: 5 }), 'force');
  });
  it('easeOfMovement', () => {
    allMissing(easeOfMovement(allMissingBars as never, { period: 5 }), 'eom');
  });
  it('volumeOscillator', () => {
    allMissing(volumeOscillator(allMissingBars as never), 'volOsc');
  });
});

/* ==========================================================================
 * The momentum tail (assessment §6.3).
 *
 * All six are RATIOS of quantities in price units — up moves against total
 * movement, buying pressure against true range, an excursion against its own
 * mean deviation — so unlike the price-unit family above (`elderRay`,
 * `awesomeOscillator`, `atrBands`) every one of them is invariant to BOTH a
 * scale factor and a constant shift. That pair is the assertion: an
 * implementation that dropped a normalisation would keep the shift
 * invariance and lose the scale one, and one that read a level where it
 * should read a change would do the reverse.
 * ========================================================================== */

const momBars = (rows: Array<[number, number, number, number]>) =>
  new TimeSeries({
    name: 'bars',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'open', kind: 'number' },
      { name: 'high', kind: 'number' },
      { name: 'low', kind: 'number' },
      { name: 'close', kind: 'number' },
    ] as const,
    rows: rows.map(([o, h, l, c], i) => [i, o, h, l, c]) as Array<
      [number, number, number, number, number]
    >,
  });

/** Non-degenerate OHLC bars: never monotonic, varying range, close never on
 *  an extreme, bodies changing sign. `a` scales every price, `b` shifts it. */
const momRows = (a = 1, b = 0): Array<[number, number, number, number]> =>
  Array.from({ length: 40 }, (_, i) => {
    const c = 100 + 8 * Math.sin(i / 3.5) + 0.3 * i;
    const o = c - 0.9 * Math.cos(i / 2.1);
    const up = 0.5 + 0.8 * Math.abs(Math.sin(i / 2.3));
    const down = 0.5 + 0.8 * Math.abs(Math.cos(i / 1.9));
    return [
      o * a + b,
      (Math.max(o, c) + up) * a + b,
      (Math.min(o, c) - down) * a + b,
      c * a + b,
    ];
  });

describe('[talib] the momentum tail is scale- AND shift-invariant', () => {
  const K = 1000;
  const SHIFT = 500;
  const cases: Array<[string, string, (s: never) => unknown]> = [
    ['chandeMomentum', 'cmo', (s) => chandeMomentum(s, { period: 5 })],
    [
      'ultimateOscillator',
      'uo',
      (s) =>
        ultimateOscillator(s, {
          shortPeriod: 3,
          mediumPeriod: 5,
          longPeriod: 9,
        }),
    ],
    [
      'commodityChannelIndex',
      'cci',
      (s) => commodityChannelIndex(s, { period: 5 }),
    ],
    [
      'intradayMomentumIndex',
      'imi',
      (s) => intradayMomentumIndex(s, { period: 5 }),
    ],
    ['relativeVigorIndex', 'rvi', (s) => relativeVigorIndex(s, { period: 4 })],
    [
      'relativeVigorIndex',
      'rviSignal',
      (s) => relativeVigorIndex(s, { period: 4 }),
    ],
    ['psychologicalLine', 'psy', (s) => psychologicalLine(s, { period: 5 })],
  ];

  for (const [name, column, run] of cases) {
    it(`${name} (${column}) is unchanged by scaling and by shifting`, () => {
      const base = col(run(momBars(momRows()) as never), column);
      expect(base.some((x) => x !== undefined)).toBe(true);
      expectSame(base, col(run(momBars(momRows(K)) as never), column));
      expectSame(base, col(run(momBars(momRows(1, SHIFT)) as never), column));
    });
  }

  it('the readings are not constant — the invariance above is not vacuous', () => {
    // expectSame passes trivially on a column that never varies, so pin that
    // each study actually moves over this input.
    const s = momBars(momRows()) as never;
    const spread = (v: Array<number | undefined>) => {
      const seen = v.filter((x) => x !== undefined) as number[];
      return Math.max(...seen) - Math.min(...seen);
    };
    expect(
      spread(col(chandeMomentum(s, { period: 5 }), 'cmo')),
    ).toBeGreaterThan(10);
    expect(
      spread(
        col(
          ultimateOscillator(s, {
            shortPeriod: 3,
            mediumPeriod: 5,
            longPeriod: 9,
          }),
          'uo',
        ),
      ),
    ).toBeGreaterThan(10);
    expect(
      spread(col(commodityChannelIndex(s, { period: 5 }), 'cci')),
    ).toBeGreaterThan(10);
    expect(
      spread(col(intradayMomentumIndex(s, { period: 5 }), 'imi')),
    ).toBeGreaterThan(10);
    expect(
      spread(col(relativeVigorIndex(s, { period: 4 }), 'rvi')),
    ).toBeGreaterThan(0.05);
    expect(
      spread(col(psychologicalLine(s, { period: 5 }), 'psy')),
    ).toBeGreaterThan(10);
  });

  it('the bounded ones stay in their bands', () => {
    const s = momBars(momRows()) as never;
    const within = (
      v: Array<number | undefined>,
      lo: number,
      hi: number,
      label: string,
    ) => {
      for (const x of v) {
        if (x === undefined) continue;
        expect(x, label).toBeGreaterThanOrEqual(lo - 1e-9);
        expect(x, label).toBeLessThanOrEqual(hi + 1e-9);
      }
    };
    within(col(chandeMomentum(s, { period: 5 }), 'cmo'), -100, 100, 'cmo');
    within(
      col(
        ultimateOscillator(s, {
          shortPeriod: 3,
          mediumPeriod: 5,
          longPeriod: 9,
        }),
        'uo',
      ),
      0,
      100,
      'uo',
    );
    within(col(intradayMomentumIndex(s, { period: 5 }), 'imi'), 0, 100, 'imi');
    within(col(psychologicalLine(s, { period: 5 }), 'psy'), 0, 100, 'psy');
  });
});

describe('[talib] the momentum tail over another study composes its warm-up', () => {
  const wavy = Array.from(
    { length: 30 },
    (_, i) => 100 + 6 * Math.sin(i / 2.5) + i * 0.1,
  );

  it('chandeMomentum over sma starts late rather than coming back empty', () => {
    const src = sma(bars(wavy), { period: 3 });
    const v = col(chandeMomentum(src, { column: 'sma', period: 3 }), 'cmo');
    expect(v).toHaveLength(wavy.length);
    // sma(3) first valid at 2, so its first CHANGE is at 3, and a 3-bar sum
    // of changes lands at 5.
    expect(firstValid(v)).toBe(5);
    expect(v.slice(5).every((x) => x !== undefined)).toBe(true);
  });

  it('psychologicalLine over sma starts late rather than coming back empty', () => {
    const src = sma(bars(wavy), { period: 3 });
    const v = col(psychologicalLine(src, { column: 'sma', period: 3 }), 'psy');
    expect(v).toHaveLength(wavy.length);
    expect(firstValid(v)).toBe(5);
    expect(v.slice(5).every((x) => x !== undefined)).toBe(true);
  });

  it('commodityChannelIndex over a smoothed bar starts late rather than empty', () => {
    const rows = momRows().slice(0, 30);
    const sh = sma(momBars(rows), { period: 3, column: 'high', output: 'sh' });
    const sl = sma(sh, { period: 3, column: 'low', output: 'sl' });
    const sc = sma(sl, { period: 3, column: 'close', output: 'sc' });
    const v = col(
      commodityChannelIndex(sc, {
        period: 3,
        high: 'sh',
        low: 'sl',
        close: 'sc',
      }),
      'cci',
    );
    expect(v).toHaveLength(30);
    // The three smoothed inputs are first valid at 2, so the typical price
    // is too; a 3-bar window of it lands at 4.
    expect(firstValid(v)).toBe(4);
    expect(v.slice(4).every((x) => x !== undefined)).toBe(true);
  });

  it('ultimateOscillator over a smoothed bar starts late rather than empty', () => {
    const rows = momRows().slice(0, 30);
    const sh = sma(momBars(rows), { period: 3, column: 'high', output: 'sh' });
    const sl = sma(sh, { period: 3, column: 'low', output: 'sl' });
    const sc = sma(sl, { period: 3, column: 'close', output: 'sc' });
    const v = col(
      ultimateOscillator(sc, {
        shortPeriod: 2,
        mediumPeriod: 3,
        longPeriod: 4,
        high: 'sh',
        low: 'sl',
        close: 'sc',
      }),
      'uo',
    );
    expect(v).toHaveLength(30);
    // Inputs first valid at 2; both legs read the previous close, so the
    // first defined BP/TR is at 3 and a 4-bar window of them at 6.
    expect(firstValid(v)).toBe(6);
    expect(v.slice(6).every((x) => x !== undefined)).toBe(true);
  });

  it('intradayMomentumIndex over a smoothed close composes its warm-up', () => {
    const rows = momRows().slice(0, 30);
    const sc = sma(momBars(rows), { period: 3, column: 'close', output: 'sc' });
    const v = col(intradayMomentumIndex(sc, { period: 3, close: 'sc' }), 'imi');
    expect(v).toHaveLength(30);
    // The body is defined from the smoothed close's first bar (2), and a
    // 3-bar window of it lands at 4.
    expect(firstValid(v)).toBe(4);
    expect(v.slice(4).every((x) => x !== undefined)).toBe(true);
  });

  it('relativeVigorIndex over a smoothed close composes its warm-up', () => {
    const rows = momRows().slice(0, 30);
    const sc = sma(momBars(rows), { period: 3, column: 'close', output: 'sc' });
    const r = relativeVigorIndex(sc, { period: 3, close: 'sc' });
    const v = col(r, 'rvi');
    expect(v).toHaveLength(30);
    // The body starts at 2, the 4-bar SWMA 3 later, the 3-bar sum 2 later.
    expect(firstValid(v)).toBe(7);
    expect(v.slice(7).every((x) => x !== undefined)).toBe(true);
    expect(firstValid(col(r, 'rviSignal'))).toBe(10);
  });
});

describe('[talib] all-missing input yields all-missing momentum-tail studies', () => {
  const allMissing = new TimeSeries({
    name: 'bars',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'open', kind: 'number', required: false },
      { name: 'high', kind: 'number', required: false },
      { name: 'low', kind: 'number', required: false },
      { name: 'close', kind: 'number', required: false },
    ] as const,
    rows: Array.from({ length: 20 }, (_, i) => [
      i,
      undefined,
      undefined,
      undefined,
      undefined,
    ]) as Array<
      [
        number,
        number | undefined,
        number | undefined,
        number | undefined,
        number | undefined,
      ]
    >,
  });
  const empty = (v: Array<number | undefined>) => {
    expect(v).toHaveLength(20);
    expect(v.every((x) => x === undefined)).toBe(true);
  };

  it('chandeMomentum, psychologicalLine and intradayMomentumIndex', () => {
    empty(col(chandeMomentum(allMissing as never, { period: 5 }), 'cmo'));
    empty(col(psychologicalLine(allMissing as never, { period: 5 }), 'psy'));
    empty(
      col(intradayMomentumIndex(allMissing as never, { period: 5 }), 'imi'),
    );
  });

  it('ultimateOscillator, commodityChannelIndex and relativeVigorIndex', () => {
    empty(
      col(
        ultimateOscillator(allMissing as never, {
          shortPeriod: 2,
          mediumPeriod: 3,
          longPeriod: 5,
        }),
        'uo',
      ),
    );
    empty(
      col(commodityChannelIndex(allMissing as never, { period: 5 }), 'cci'),
    );
    const r = relativeVigorIndex(allMissing as never, { period: 3 });
    empty(col(r, 'rvi'));
    empty(col(r, 'rviSignal'));
  });
});

describe('[talib] the directional group is scale- AND shift-invariant', () => {
  const K = 1000;
  const SHIFT = 500;
  const cases: Array<[string, string, (s: never) => unknown]> = [
    [
      'directionalMovement',
      'dmiPlusDi',
      (s) => directionalMovement(s, { period: 5 }),
    ],
    [
      'directionalMovement',
      'dmiMinusDi',
      (s) => directionalMovement(s, { period: 5 }),
    ],
    [
      'directionalMovement',
      'dmiDx',
      (s) => directionalMovement(s, { period: 5 }),
    ],
    [
      'directionalMovement',
      'dmiAdx',
      (s) => directionalMovement(s, { period: 5 }),
    ],
    [
      'directionalMovement',
      'dmiAdxr',
      (s) => directionalMovement(s, { period: 5 }),
    ],
    ['aroon', 'aroonUp', (s) => aroon(s, { period: 5 })],
    ['aroon', 'aroonDown', (s) => aroon(s, { period: 5 })],
    ['aroon', 'aroonOsc', (s) => aroon(s, { period: 5 })],
    ['vortex', 'viPlus', (s) => vortex(s, { period: 5 })],
    ['vortex', 'viMinus', (s) => vortex(s, { period: 5 })],
  ];

  for (const [name, column, run] of cases) {
    it(`${name} (${column}) is unchanged by scaling and by shifting`, () => {
      const base = col(run(momBars(momRows()) as never), column);
      expect(base.some((x) => x !== undefined)).toBe(true);
      expectSame(base, col(run(momBars(momRows(K)) as never), column));
      expectSame(base, col(run(momBars(momRows(1, SHIFT)) as never), column));
    });
  }

  it('aroon is invariant to any strictly increasing map of price, exactly — it reads positions', () => {
    // Stronger than the scale/shift invariance above, and asserted as
    // equality rather than a tolerance: the study never touches the SIZE of
    // an extreme, only where it sits, so a strictly increasing map of every
    // price leaves all three columns bit-identical.
    const base = aroon(momBars(momRows()) as never, { period: 5 });
    const warped = aroon(
      momBars(
        momRows().map(
          ([o, h, l, c]) =>
            [o ** 1.5, h ** 1.5, l ** 1.5, c ** 1.5] as [
              number,
              number,
              number,
              number,
            ],
        ),
      ) as never,
      { period: 5 },
    );
    for (const name of ['aroonUp', 'aroonDown', 'aroonOsc'])
      expect(col(warped, name), name).toEqual(col(base, name));
  });

  it('the readings are not constant — the invariance above is not vacuous', () => {
    const s = momBars(momRows()) as never;
    const spread = (v: Array<number | undefined>) => {
      const seen = v.filter((x) => x !== undefined) as number[];
      return Math.max(...seen) - Math.min(...seen);
    };
    const dm = directionalMovement(s, { period: 5 });
    for (const name of [
      'dmiPlusDi',
      'dmiMinusDi',
      'dmiDx',
      'dmiAdx',
      'dmiAdxr',
    ])
      expect(spread(col(dm, name)), name).toBeGreaterThan(10);
    const ar = aroon(s, { period: 5 });
    for (const name of ['aroonUp', 'aroonDown', 'aroonOsc'])
      expect(spread(col(ar, name)), name).toBeGreaterThan(20);
    const vi = vortex(s, { period: 5 });
    for (const name of ['viPlus', 'viMinus'])
      expect(spread(col(vi, name)), name).toBeGreaterThan(0.2);
  });

  it('the bounded ones stay in their bands, and vortex stays positive', () => {
    const s = momBars(momRows()) as never;
    const within = (
      v: Array<number | undefined>,
      lo: number,
      hi: number,
      label: string,
    ) => {
      expect(
        v.some((x) => x !== undefined),
        label,
      ).toBe(true);
      for (const x of v) {
        if (x === undefined) continue;
        expect(x, label).toBeGreaterThanOrEqual(lo - 1e-9);
        expect(x, label).toBeLessThanOrEqual(hi + 1e-9);
      }
    };
    // On CONSISTENT bars (prevClose inside the previous bar's range) each DM
    // leg is bounded by the true range, so both DI lines are — and DX, ADX
    // and ADXR are then bounded by construction.
    const dm = directionalMovement(s, { period: 5 });
    for (const name of [
      'dmiPlusDi',
      'dmiMinusDi',
      'dmiDx',
      'dmiAdx',
      'dmiAdxr',
    ])
      within(col(dm, name), 0, 100, name);
    const ar = aroon(s, { period: 5 });
    within(col(ar, 'aroonUp'), 0, 100, 'aroonUp');
    within(col(ar, 'aroonDown'), 0, 100, 'aroonDown');
    within(col(ar, 'aroonOsc'), -100, 100, 'aroonOsc');
    // The vortex is NOT bounded by 1 — only positive. Assert the sign, and
    // assert the fixture actually reaches past 1 so "bounded" cannot creep
    // in as an unstated assumption.
    const vi = vortex(s, { period: 5 });
    for (const name of ['viPlus', 'viMinus']) {
      const v = col(vi, name);
      within(v, 0, Number.POSITIVE_INFINITY, name);
      expect(
        v.some((x) => x !== undefined && x > 1),
        name,
      ).toBe(true);
    }
  });

  it('aroonOsc is exactly aroonUp − aroonDown', () => {
    const r = aroon(momBars(momRows()) as never, { period: 7 });
    const up = col(r, 'aroonUp');
    const down = col(r, 'aroonDown');
    const osc = col(r, 'aroonOsc');
    for (let i = 0; i < osc.length; i += 1) {
      if (up[i] === undefined) expect(osc[i], `bar ${i}`).toBeUndefined();
      else expect(osc[i], `bar ${i}`).toBeCloseTo(up[i]! - down[i]!, 12);
    }
  });
});

describe('[talib] the directional group over another study’s output', () => {
  it('directionalMovement over smoothed bars starts late rather than empty', () => {
    const rows = momRows().slice(0, 40);
    const sh = sma(momBars(rows), { period: 3, column: 'high', output: 'sh' });
    const sl = sma(sh, { period: 3, column: 'low', output: 'sl' });
    const sc = sma(sl, { period: 3, column: 'close', output: 'sc' });
    const r = directionalMovement(sc, {
      period: 3,
      high: 'sh',
      low: 'sl',
      close: 'sc',
    });
    const plus = col(r, 'dmiPlusDi');
    expect(plus).toHaveLength(40);
    // The inputs are first defined at 2; DM and TR at 3; the Wilder seed
    // steps over the leading gap and lands `period − 1` later, at 5.
    expect(firstValid(plus)).toBe(5);
    expect(plus.slice(5).every((x) => x !== undefined)).toBe(true);
    // ADX is a second Wilder smooth on top: 3 − 1 bars later again.
    expect(firstValid(col(r, 'dmiAdx'))).toBe(7);
    expect(firstValid(col(r, 'dmiAdxr'))).toBe(9);
  });

  it('aroon over a smoothed high/low composes its warm-up', () => {
    const rows = momRows().slice(0, 30);
    const sh = sma(momBars(rows), { period: 3, column: 'high', output: 'sh' });
    const sl = sma(sh, { period: 3, column: 'low', output: 'sl' });
    const v = col(aroon(sl, { period: 4, high: 'sh', low: 'sl' }), 'aroonUp');
    expect(v).toHaveLength(30);
    // Inputs first defined at 2; the window needs period + 1 = 5 of them.
    expect(firstValid(v)).toBe(6);
    expect(v.slice(6).every((x) => x !== undefined)).toBe(true);
  });

  it('vortex over smoothed bars composes its warm-up', () => {
    const rows = momRows().slice(0, 30);
    const sh = sma(momBars(rows), { period: 3, column: 'high', output: 'sh' });
    const sl = sma(sh, { period: 3, column: 'low', output: 'sl' });
    const sc = sma(sl, { period: 3, column: 'close', output: 'sc' });
    const v = col(
      vortex(sc, { period: 4, high: 'sh', low: 'sl', close: 'sc' }),
      'viPlus',
    );
    expect(v).toHaveLength(30);
    // Inputs at 2, the movement legs and TR at 3, a 4-bar sum of them at 6.
    expect(firstValid(v)).toBe(6);
    expect(v.slice(6).every((x) => x !== undefined)).toBe(true);
  });
});

describe('[talib] all-missing input yields all-missing directional studies', () => {
  const allMissing = new TimeSeries({
    name: 'bars',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'high', kind: 'number', required: false },
      { name: 'low', kind: 'number', required: false },
      { name: 'close', kind: 'number', required: false },
    ] as const,
    rows: Array.from({ length: 20 }, (_, i) => [
      i,
      undefined,
      undefined,
      undefined,
    ]) as Array<
      [number, number | undefined, number | undefined, number | undefined]
    >,
  });
  const empty = (v: Array<number | undefined>, label: string) => {
    expect(v, label).toHaveLength(20);
    expect(
      v.every((x) => x === undefined),
      label,
    ).toBe(true);
  };

  it('directionalMovement, aroon and vortex', () => {
    const dm = directionalMovement(allMissing as never, { period: 5 });
    for (const name of [
      'dmiPlusDi',
      'dmiMinusDi',
      'dmiDx',
      'dmiAdx',
      'dmiAdxr',
    ])
      empty(col(dm, name), name);
    const ar = aroon(allMissing as never, { period: 5 });
    for (const name of ['aroonUp', 'aroonDown', 'aroonOsc'])
      empty(col(ar, name), name);
    const vi = vortex(allMissing as never, { period: 5 });
    for (const name of ['viPlus', 'viMinus']) empty(col(vi, name), name);
  });
});

/* -------------------------------------------------------------------------- */
/* The volatility tail (assessment §6.5). Its property matrix is NOT uniform,  */
/* which is the reason it is worth writing out rather than looping over one    */
/* claim: five of the seven are invariant to both a scale factor and a shift,  */
/* `ulcerIndex` is scale-invariant only (it normalises by a price, so adding   */
/* a constant moves its base), and `gopalakrishnanRangeIndex` is the mirror —  */
/* shift-invariant, and scale-ADDITIVE by exactly ln(k)/ln(period). A test     */
/* that asserted the wrong half of that would pass vacuously.                  */
/* -------------------------------------------------------------------------- */

describe('[talib] the volatility tail: scale and shift, one claim per study', () => {
  const K = 1000;
  const SHIFT = 500;

  const bothInvariant: Array<[string, string, (s: never) => unknown]> = [
    [
      'chaikinVolatility',
      'chaikinVol',
      (s) => chaikinVolatility(s, { period: 4, rocPeriod: 3 }),
    ],
    ['massIndex', 'mass', (s) => massIndex(s, { emaPeriod: 4, sumPeriod: 6 })],
    ['choppinessIndex', 'chop', (s) => choppinessIndex(s, { period: 5 })],
    [
      'verticalHorizontalFilter',
      'vhf',
      (s) => verticalHorizontalFilter(s, { period: 6 }),
    ],
    [
      'relativeVolatilityIndex',
      'relVol',
      (s) => relativeVolatilityIndex(s, { period: 4, stdevPeriod: 3 }),
    ],
  ];

  for (const [name, column, run] of bothInvariant) {
    it(`${name} (${column}) is unchanged by scaling and by shifting`, () => {
      const base = col(run(momBars(momRows()) as never), column);
      expect(base.some((x) => x !== undefined)).toBe(true);
      expectSame(base, col(run(momBars(momRows(K)) as never), column));
      expectSame(base, col(run(momBars(momRows(1, SHIFT)) as never), column));
    });
  }

  it('ulcerIndex is scale-invariant but NOT shift-invariant', () => {
    const run = (s: never) => ulcerIndex(s, { period: 5 });
    const base = col(run(momBars(momRows()) as never), 'ulcer');
    expect(base.some((x) => x !== undefined)).toBe(true);
    expectSame(base, col(run(momBars(momRows(K)) as never), 'ulcer'));
    // Adding a constant raises the base of every percentage, so the reading
    // must SHRINK — asserted as a direction, not merely as "different".
    const shifted = col(run(momBars(momRows(1, SHIFT)) as never), 'ulcer');
    let compared = 0;
    for (let i = 0; i < base.length; i += 1) {
      if (base[i] === undefined || base[i] === 0) continue;
      expect(shifted[i]!, `bar ${i}`).toBeLessThan(base[i]!);
      compared += 1;
    }
    expect(compared).toBeGreaterThan(10);
  });

  it('gopalakrishnanRangeIndex is shift-invariant and scale-ADDITIVE', () => {
    const run = (s: never) => gopalakrishnanRangeIndex(s, { period: 5 });
    const base = col(run(momBars(momRows()) as never), 'gapo');
    expect(base.some((x) => x !== undefined)).toBe(true);
    expectSame(base, col(run(momBars(momRows(1, SHIFT)) as never), 'gapo'));
    // Scaling every price by K adds exactly ln(K)/ln(period) to the reading:
    // the study carries the units of the price, which is what separates it
    // from every other study in this batch.
    const scaled = col(run(momBars(momRows(K)) as never), 'gapo');
    const offset = Math.log(K) / Math.log(5);
    let compared = 0;
    for (let i = 0; i < base.length; i += 1) {
      if (base[i] === undefined) continue;
      expect(scaled[i]!, `bar ${i}`).toBeCloseTo(base[i]! + offset, 10);
      compared += 1;
    }
    expect(compared).toBeGreaterThan(10);
    // …and the offset is real: without it the two would not agree at all.
    expect(offset).toBeGreaterThan(4);
  });

  it('the readings are not constant — the claims above are not vacuous', () => {
    const s = momBars(momRows()) as never;
    const spread = (v: Array<number | undefined>) => {
      const seen = v.filter((x) => x !== undefined) as number[];
      return Math.max(...seen) - Math.min(...seen);
    };
    expect(
      spread(
        col(chaikinVolatility(s, { period: 4, rocPeriod: 3 }), 'chaikinVol'),
      ),
    ).toBeGreaterThan(10);
    expect(
      spread(col(massIndex(s, { emaPeriod: 4, sumPeriod: 6 }), 'mass')),
    ).toBeGreaterThan(0.05);
    expect(
      spread(col(choppinessIndex(s, { period: 5 }), 'chop')),
    ).toBeGreaterThan(10);
    expect(spread(col(ulcerIndex(s, { period: 5 }), 'ulcer'))).toBeGreaterThan(
      0.5,
    );
    expect(
      spread(col(verticalHorizontalFilter(s, { period: 6 }), 'vhf')),
    ).toBeGreaterThan(0.1);
    expect(
      spread(col(gopalakrishnanRangeIndex(s, { period: 5 }), 'gapo')),
    ).toBeGreaterThan(0.1);
    expect(
      spread(
        col(
          relativeVolatilityIndex(s, { period: 4, stdevPeriod: 3 }),
          'relVol',
        ),
      ),
    ).toBeGreaterThan(10);
  });

  it('the bounded ones stay in their bands', () => {
    const s = momBars(momRows()) as never;
    const within = (
      v: Array<number | undefined>,
      lo: number,
      hi: number,
      label: string,
    ) => {
      let seen = 0;
      for (const x of v) {
        if (x === undefined) continue;
        expect(x, label).toBeGreaterThanOrEqual(lo - 1e-9);
        expect(x, label).toBeLessThanOrEqual(hi + 1e-9);
        seen += 1;
      }
      expect(seen, `${label} had no readings to bound`).toBeGreaterThan(10);
    };
    within(col(choppinessIndex(s, { period: 5 }), 'chop'), 0, 100, 'chop');
    within(
      col(relativeVolatilityIndex(s, { period: 4, stdevPeriod: 3 }), 'relVol'),
      0,
      100,
      'relVol',
    );
    // VHF is a fraction in (0, 1]; ulcer is a non-negative percentage.
    within(col(verticalHorizontalFilter(s, { period: 6 }), 'vhf'), 0, 1, 'vhf');
    within(col(ulcerIndex(s, { period: 5 }), 'ulcer'), 0, 100, 'ulcer');
  });
});

describe('[talib] the volatility tail over another study composes its warm-up', () => {
  const wavy = Array.from(
    { length: 40 },
    (_, i) => 100 + 6 * Math.sin(i / 2.5) + i * 0.1,
  );

  it('ulcerIndex over sma starts late rather than coming back empty', () => {
    const src = sma(bars(wavy), { period: 3 });
    const v = col(ulcerIndex(src, { column: 'sma', period: 3 }), 'ulcer');
    expect(v).toHaveLength(wavy.length);
    // sma(3) first valid at 2 — but the PEAK reads the COLUMN door, whose
    // window counts rows and skips missing cells, so it emits from bar 2 over
    // one contributor (`rollingMax`'s documented contract, and `donchian`'s).
    // The first drawdown is therefore at 2 and the mean of squares — on the
    // ARRAY door, which waits for `period` finite values — at 4. The mixed
    // doors are why this is measured rather than derived from `2·period − 2`.
    expect(firstValid(v)).toBe(4);
    expect(v.slice(4).every((x) => x !== undefined)).toBe(true);
  });

  it('verticalHorizontalFilter over sma starts late rather than empty', () => {
    const src = sma(bars(wavy), { period: 3 });
    const v = col(
      verticalHorizontalFilter(src, { column: 'sma', period: 3 }),
      'vhf',
    );
    expect(v).toHaveLength(wavy.length);
    // sma(3) first valid at 2, its first change at 3, a 3-change sum at 5.
    expect(firstValid(v)).toBe(5);
    expect(v.slice(5).every((x) => x !== undefined)).toBe(true);
  });

  it('relativeVolatilityIndex over sma starts late rather than empty', () => {
    const src = sma(bars(wavy), { period: 3 });
    const v = col(
      relativeVolatilityIndex(src, {
        column: 'sma',
        period: 3,
        stdevPeriod: 3,
      }),
      'relVol',
    );
    expect(v).toHaveLength(wavy.length);
    // σ also reads the COLUMN door, so it emits from bar 2 (over one value,
    // where it is 0 — `rollingStdev`'s contract, the one `historicalVolatility`
    // flags). The first DIRECTION is at bar 3, so the legs start there and the
    // Wilder seed lands `period − 1` later, at 5.
    expect(firstValid(v)).toBe(5);
    expect(v.slice(5).every((x) => x !== undefined)).toBe(true);
  });

  it('the bar studies over a smoothed bar start late rather than empty', () => {
    const rows = momRows().slice(0, 30);
    const sh = sma(momBars(rows), { period: 3, column: 'high', output: 'sh' });
    const sl = sma(sh, { period: 3, column: 'low', output: 'sl' });
    const sc = sma(sl, { period: 3, column: 'close', output: 'sc' });
    const opts = { high: 'sh', low: 'sl', close: 'sc' } as const;

    // The smoothed inputs are first valid at 2, so the range is too.
    const cv = col(
      chaikinVolatility(sc, { ...opts, period: 2, rocPeriod: 2 }),
      'chaikinVol',
    );
    expect(cv).toHaveLength(30);
    expect(firstValid(cv)).toBe(5); // 2 + (period − 1) + rocPeriod

    const mi = col(
      massIndex(sc, { ...opts, emaPeriod: 2, sumPeriod: 3 }),
      'mass',
    );
    expect(firstValid(mi)).toBe(6); // 2 + 2·emaPeriod − 2 + sumPeriod − 1

    const ci = col(choppinessIndex(sc, { ...opts, period: 3 }), 'chop');
    expect(firstValid(ci)).toBe(5); // the first true range is at 3, +period−1

    const gp = col(
      gopalakrishnanRangeIndex(sc, { high: 'sh', low: 'sl', period: 3 }),
      'gapo',
    );
    // The extremes SKIP the warm-up rows rather than waiting for them, so
    // GAPO starts at its input's own first bar — the one study here that does.
    expect(firstValid(gp)).toBe(2);
  });
});

describe('[talib] all-missing input yields all-missing volatility-tail studies', () => {
  const allMissing = new TimeSeries({
    name: 'bars',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'high', kind: 'number', required: false },
      { name: 'low', kind: 'number', required: false },
      { name: 'close', kind: 'number', required: false },
    ] as const,
    rows: Array.from({ length: 20 }, (_, i) => [
      i,
      undefined,
      undefined,
      undefined,
    ]) as Array<
      [number, number | undefined, number | undefined, number | undefined]
    >,
  });
  const empty = (v: Array<number | undefined>) => {
    expect(v).toHaveLength(20);
    expect(v.every((x) => x === undefined)).toBe(true);
  };

  it('the bar studies: chaikinVolatility, massIndex, choppinessIndex, gapo', () => {
    empty(
      col(
        chaikinVolatility(allMissing as never, { period: 3, rocPeriod: 3 }),
        'chaikinVol',
      ),
    );
    empty(
      col(
        massIndex(allMissing as never, { emaPeriod: 3, sumPeriod: 4 }),
        'mass',
      ),
    );
    empty(col(choppinessIndex(allMissing as never, { period: 5 }), 'chop'));
    empty(
      col(gopalakrishnanRangeIndex(allMissing as never, { period: 5 }), 'gapo'),
    );
  });

  it('the column studies: ulcerIndex, verticalHorizontalFilter, relVol', () => {
    empty(col(ulcerIndex(allMissing as never, { period: 5 }), 'ulcer'));
    empty(
      col(verticalHorizontalFilter(allMissing as never, { period: 5 }), 'vhf'),
    );
    empty(
      col(
        relativeVolatilityIndex(allMissing as never, {
          period: 5,
          stdevPeriod: 3,
        }),
        'relVol',
      ),
    );
  });
});

/*
 * The regression family's property matrix, written out study by study.
 *
 * A loop over "these are all scale-invariant" would be wrong for five of
 * the eight columns here: `linregValue` / `linregIntercept` / `tsf` are
 * EQUIVARIANT to both scale and shift, `linregSlope` is equivariant to
 * scale and INVARIANT to shift, `linregAngle` is invariant to shift and
 * genuinely DEPENDENT on scale (TA-Lib applies no normalisation), `linregR2`
 * is invariant to both, and `cfo` / `cog` are scale-invariant but not
 * shift-invariant. Each claim is its own assertion.
 */
describe('[talib] the regression family: one property claim per column', () => {
  const wavy = Array.from(
    { length: 40 },
    (_, i) => 100 + 6 * Math.sin(i / 2.5) + i * 0.1,
  );
  const P = 6;
  const K = 2.5;
  const B = -40;
  const scaled = wavy.map((x) => x * K);
  const shifted = wavy.map((x) => x + B);
  const pairs = (
    a: Array<number | undefined>,
    b: Array<number | undefined>,
  ) => {
    expect(firstValid(b)).toBe(firstValid(a));
    let seen = 0;
    for (let i = 0; i < a.length; i += 1) {
      if (a[i] === undefined) {
        expect(b[i], `bar ${i}`).toBeUndefined();
      } else {
        seen += 1;
      }
    }
    expect(seen).toBeGreaterThan(20);
  };

  it('linregSlope: scales with the price, and is unmoved by a shift', () => {
    const base = col(
      linearRegression(bars(wavy), { period: P }),
      'linregSlope',
    );
    const up = col(
      linearRegression(bars(scaled), { period: P }),
      'linregSlope',
    );
    const over = col(
      linearRegression(bars(shifted), { period: P }),
      'linregSlope',
    );
    pairs(base, up);
    for (let i = 0; i < base.length; i += 1) {
      if (base[i] === undefined) continue;
      expect(up[i], `scaled ${i}`).toBeCloseTo(base[i]! * K, 9);
      expect(over[i], `shifted ${i}`).toBeCloseTo(base[i]!, 9);
    }
  });

  it('linregValue and linregIntercept: equivariant to BOTH (a·y + b)', () => {
    for (const name of ['linregValue', 'linregIntercept']) {
      const base = col(linearRegression(bars(wavy), { period: P }), name);
      const mapped = col(
        linearRegression(bars(wavy.map((x) => x * K + B)), { period: P }),
        name,
      );
      pairs(base, mapped);
      for (let i = 0; i < base.length; i += 1) {
        if (base[i] === undefined) continue;
        expect(mapped[i], `${name} bar ${i}`).toBeCloseTo(base[i]! * K + B, 8);
      }
    }
  });

  it('linregAngle: unmoved by a shift, and genuinely MOVED by a scale', () => {
    const base = col(
      linearRegression(bars(wavy), { period: P }),
      'linregAngle',
    );
    const over = col(
      linearRegression(bars(shifted), { period: P }),
      'linregAngle',
    );
    const up = col(
      linearRegression(bars(scaled), { period: P }),
      'linregAngle',
    );
    let moved = 0;
    for (let i = 0; i < base.length; i += 1) {
      if (base[i] === undefined) continue;
      expect(over[i], `shifted ${i}`).toBeCloseTo(base[i]!, 9);
      // atan(k·m) ≠ atan(m) for every non-zero slope: the angle carries the
      // price's UNITS. This is the assertion the docstring's warning rests
      // on, so it is a real inequality rather than "different somewhere".
      expect(up[i], `scaled ${i}`).toBeCloseTo(
        (Math.atan(Math.tan((base[i]! * Math.PI) / 180) * K) * 180) / Math.PI,
        8,
      );
      if (Math.abs(up[i]! - base[i]!) > 1) moved += 1;
    }
    expect(moved, 'scaling must move the angle on most bars').toBeGreaterThan(
      20,
    );
  });

  it('linregR2: invariant to both — the one column comparable across instruments', () => {
    const base = col(linearRegression(bars(wavy), { period: P }), 'linregR2');
    const mapped = col(
      linearRegression(bars(wavy.map((x) => x * K + B)), { period: P }),
      'linregR2',
    );
    pairs(base, mapped);
    for (let i = 0; i < base.length; i += 1) {
      if (base[i] === undefined) continue;
      expect(mapped[i], `bar ${i}`).toBeCloseTo(base[i]!, 9);
    }
  });

  it('tsf: equivariant to both, like any linear filter', () => {
    const base = col(timeSeriesForecast(bars(wavy), { period: P }), 'tsf');
    const mapped = col(
      timeSeriesForecast(bars(wavy.map((x) => x * K + B)), { period: P }),
      'tsf',
    );
    pairs(base, mapped);
    for (let i = 0; i < base.length; i += 1) {
      if (base[i] === undefined) continue;
      expect(mapped[i], `bar ${i}`).toBeCloseTo(base[i]! * K + B, 8);
    }
  });

  it('cfo: scale-INVARIANT, and shift-DEPENDENT (it divides by the price)', () => {
    const base = col(
      chandeForecastOscillator(bars(wavy), { period: P }),
      'cfo',
    );
    const up = col(
      chandeForecastOscillator(bars(scaled), { period: P }),
      'cfo',
    );
    const over = col(
      chandeForecastOscillator(bars(wavy.map((x) => x + 400)), { period: P }),
      'cfo',
    );
    pairs(base, up);
    let shrank = 0;
    for (let i = 0; i < base.length; i += 1) {
      if (base[i] === undefined) continue;
      expect(up[i], `scaled ${i}`).toBeCloseTo(base[i]!, 9);
      // Adding a constant moves the BASE of the percentage without moving
      // its numerator, so every reading shrinks toward zero. Asserted as a
      // direction, not merely "different" — a study that normalised by the
      // wrong thing would pass the weaker claim.
      expect(Math.abs(over[i]!), `shifted ${i}`).toBeLessThan(
        Math.abs(base[i]!) + 1e-12,
      );
      if (Math.abs(over[i]!) < Math.abs(base[i]!) * 0.5) shrank += 1;
    }
    expect(shrank, 'a +400 shift must visibly shrink cfo').toBeGreaterThan(20);
  });

  it('cog: scale-INVARIANT, and shift-DEPENDENT (a shift drags it to the middle)', () => {
    const base = col(centerOfGravity(bars(wavy), { period: P }), 'cog');
    const up = col(centerOfGravity(bars(scaled), { period: P }), 'cog');
    const over = col(
      centerOfGravity(bars(wavy.map((x) => x + 400)), { period: P }),
      'cog',
    );
    pairs(base, up);
    const middle = -(P + 1) / 2;
    let dragged = 0;
    for (let i = 0; i < base.length; i += 1) {
      if (base[i] === undefined) continue;
      expect(up[i], `scaled ${i}`).toBeCloseTo(base[i]!, 9);
      // A shift adds the same constant to every term of both sums, which
      // pulls the balance point toward the flat-window middle.
      expect(Math.abs(over[i]! - middle), `shifted ${i}`).toBeLessThanOrEqual(
        Math.abs(base[i]! - middle) + 1e-12,
      );
      if (Math.abs(over[i]! - middle) < Math.abs(base[i]! - middle) * 0.5) {
        dragged += 1;
      }
    }
    expect(
      dragged,
      'a +400 shift must drag cog toward the middle',
    ).toBeGreaterThan(20);
  });
});

describe('[talib] the regression family over another study composes its warm-up', () => {
  const wavy = Array.from(
    { length: 40 },
    (_, i) => 100 + 6 * Math.sin(i / 2.5) + i * 0.1,
  );

  it('linearRegression over sma starts late rather than coming back empty', () => {
    const src = sma(bars(wavy), { period: 3 });
    const r = linearRegression(src, { column: 'sma', period: 4 });
    for (const name of ['linregValue', 'linregSlope', 'linregR2']) {
      const v = col(r, name);
      expect(v, name).toHaveLength(wavy.length);
      // sma(3) first valid at 2; the strict window then needs 4 finite
      // values, so the fit lands at 2 + 4 − 1 = 5.
      expect(firstValid(v), name).toBe(5);
      expect(
        v.slice(5).every((x) => x !== undefined),
        name,
      ).toBe(true);
    }
  });

  it('timeSeriesForecast, cfo and cog over sma start late rather than empty', () => {
    const src = sma(bars(wavy), { period: 3 });
    const tsf = col(
      timeSeriesForecast(src, { column: 'sma', period: 4 }),
      'tsf',
    );
    expect(firstValid(tsf)).toBe(5);
    const cfo = col(
      chandeForecastOscillator(src, { column: 'sma', period: 4 }),
      'cfo',
    );
    expect(firstValid(cfo)).toBe(5);
    // CG's two halves are both strict-window array doors, so it composes the
    // same way — `wma` and `rollingMeanValues` agree on the mask.
    const cog = col(centerOfGravity(src, { column: 'sma', period: 4 }), 'cog');
    expect(firstValid(cog)).toBe(5);
    expect(cog.slice(5).every((x) => x !== undefined)).toBe(true);
  });
});

describe('[talib] all-missing input yields all-missing regression studies', () => {
  const allMissing = new TimeSeries({
    name: 'bars',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'close', kind: 'number', required: false },
    ] as const,
    rows: Array.from({ length: 20 }, (_, i) => [i, undefined]) as never,
  });
  const empty = (v: Array<number | undefined>) => {
    expect(v).toHaveLength(20);
    expect(v.every((x) => x === undefined)).toBe(true);
  };

  it('all five linearRegression columns, tsf, cfo and cog', () => {
    const r = linearRegression(allMissing as never, { period: 5 });
    for (const name of [
      'linregValue',
      'linregSlope',
      'linregIntercept',
      'linregAngle',
      'linregR2',
    ]) {
      empty(col(r, name));
    }
    empty(col(timeSeriesForecast(allMissing as never, { period: 5 }), 'tsf'));
    empty(
      col(chandeForecastOscillator(allMissing as never, { period: 5 }), 'cfo'),
    );
    empty(col(centerOfGravity(allMissing as never, { period: 5 }), 'cog'));
  });
});

/* -------------------------------------------------------------------------- */
/* The two-series family (corpus §6.7). Four studies, four DIFFERENT property  */
/* matrices — which is why they are written out one at a time rather than      */
/* looped over one claim. A loop would have asserted the wrong half for two of */
/* the four: `priceRelative` is scale-EQUIVARIANT (not invariant), and `beta`  */
/* is scale-invariant but NOT shift-invariant, where `correlation` is both.    */
/* -------------------------------------------------------------------------- */

const twoSeriesSchema = [
  { name: 'time', kind: 'time' },
  { name: 'close', kind: 'number' },
  { name: 'bench', kind: 'number' },
] as const;

/** A never-flat, never-monotonic pair whose two sides lead each other, with
 *  each side independently scalable and shiftable. */
const twoSeriesBars = (
  n = 60,
  kClose = 1,
  shiftClose = 0,
  kBench = 1,
  shiftBench = 0,
) =>
  new TimeSeries({
    name: 'bars',
    schema: twoSeriesSchema,
    rows: Array.from({ length: n }, (_, i) => [
      i,
      kClose * (100 + 6 * Math.sin(i / 4.1) + 0.15 * i) + shiftClose,
      kBench * (60 + 3.5 * Math.cos(i / 5.7) + 0.08 * i) + shiftBench,
    ]) as Array<[number, number, number]>,
  });

/** Assert two runs DISAGREE somewhere they both have values — the half of a
 *  property matrix that a "different numbers" test usually forgets. */
const expectMoved = (
  base: Array<number | undefined>,
  other: Array<number | undefined>,
) => {
  const moved = base.some(
    (b, i) =>
      b !== undefined &&
      other[i] !== undefined &&
      Math.abs(other[i]! - b) > 1e-6,
  );
  expect(moved).toBe(true);
};

describe('[talib] the two-series family: scale and shift behaviour', () => {
  const base = twoSeriesBars();

  it('correlation is invariant to an independent SCALE of either column', () => {
    const v = col(
      correlation(base, { benchmark: 'bench', period: 10 }),
      'corr',
    );
    expectSame(
      v,
      col(
        correlation(twoSeriesBars(60, 7.5), { benchmark: 'bench', period: 10 }),
        'corr',
      ),
    );
    expectSame(
      v,
      col(
        correlation(twoSeriesBars(60, 1, 0, 0.02), {
          benchmark: 'bench',
          period: 10,
        }),
        'corr',
      ),
    );
  });

  it('correlation is invariant to an independent SHIFT of either column too', () => {
    // This is the half that separates Pearson's r from a covariance: a
    // covariance is shift-invariant as well, but a study that had normalised
    // by the wrong thing (dividing by the MEANS rather than the standard
    // deviations, say) would fail here and pass the scale test.
    const v = col(
      correlation(base, { benchmark: 'bench', period: 10 }),
      'corr',
    );
    expectSame(
      v,
      col(
        correlation(twoSeriesBars(60, 1, 500), {
          benchmark: 'bench',
          period: 10,
        }),
        'corr',
      ),
    );
    expectSame(
      v,
      col(
        correlation(twoSeriesBars(60, 1, 0, 1, -40), {
          benchmark: 'bench',
          period: 10,
        }),
        'corr',
      ),
    );
  });

  it('beta is invariant to SCALING either column and MOVES when either is shifted', () => {
    const v = col(beta(base, { benchmark: 'bench', period: 10 }), 'beta');
    // Scaling a price series scales its returns by exactly one, so beta is
    // unchanged on either side.
    expectSame(
      v,
      col(
        beta(twoSeriesBars(60, 12), { benchmark: 'bench', period: 10 }),
        'beta',
      ),
    );
    expectSame(
      v,
      col(
        beta(twoSeriesBars(60, 1, 0, 0.3), { benchmark: 'bench', period: 10 }),
        'beta',
      ),
    );
    // Shifting does NOT cancel — it changes the base of every return — and
    // asserting that it moves is the point: a study that had (wrongly)
    // differenced instead of taking returns would pass the scale test above
    // and fail here in the other direction.
    expectMoved(
      v,
      col(
        beta(twoSeriesBars(60, 1, 400), { benchmark: 'bench', period: 10 }),
        'beta',
      ),
    );
    expectMoved(
      v,
      col(
        beta(twoSeriesBars(60, 1, 0, 1, 300), {
          benchmark: 'bench',
          period: 10,
        }),
        'beta',
      ),
    );
  });

  it('priceRelative is scale-EQUIVARIANT in column and inverse in benchmark', () => {
    const v = col(priceRelative(base, { benchmark: 'bench' }), 'priceRel');
    expectLinear(
      v,
      col(
        priceRelative(twoSeriesBars(60, 4), { benchmark: 'bench' }),
        'priceRel',
      ),
      4,
    );
    expectLinear(
      v,
      col(
        priceRelative(twoSeriesBars(60, 1, 0, 4), { benchmark: 'bench' }),
        'priceRel',
      ),
      1 / 4,
    );
  });

  it('performanceIndex is invariant to SCALING both columns and moves on a shift', () => {
    const v = col(
      performanceIndex(base, { benchmark: 'bench', period: 12 }),
      'perf',
    );
    expectSame(
      v,
      col(
        performanceIndex(twoSeriesBars(60, 9, 0, 0.05), {
          benchmark: 'bench',
          period: 12,
        }),
        'perf',
      ),
    );
    expectMoved(
      v,
      col(
        performanceIndex(twoSeriesBars(60, 1, 250), {
          benchmark: 'bench',
          period: 12,
        }),
        'perf',
      ),
    );
    expectMoved(
      v,
      col(
        performanceIndex(twoSeriesBars(60, 1, 0, 1, 250), {
          benchmark: 'bench',
          period: 12,
        }),
        'perf',
      ),
    );
  });
});

describe('[talib] the two-series family composes over another study', () => {
  it('each runs over an sma of the close against an sma of the benchmark', () => {
    // The composition case: the comparison column can itself be a study
    // output, warm-up and all. Length preserved, warm-ups COMPOSED (the
    // strict pair window waits for both), and the column is not empty —
    // the `rsi(sma(...))` failure mode this file exists for.
    const withSma = sma(twoSeriesBars(60), { period: 5, output: 'smaClose' });
    const both = sma(withSma, {
      period: 5,
      column: 'bench',
      output: 'smaBench',
    });

    const corr = col(
      correlation(both, {
        column: 'smaClose',
        benchmark: 'smaBench',
        period: 10,
      }),
      'corr',
    );
    expect(corr).toHaveLength(60);
    // Both inputs warm up at bar 4, so the first strict 10-bar pair window
    // ends at bar 13 — the inner warm-up plus the outer one, composed.
    expect(firstValid(corr)).toBe(13);
    expect(corr.slice(13).every((x) => typeof x === 'number')).toBe(true);

    const b = col(
      beta(both, { column: 'smaClose', benchmark: 'smaBench', period: 10 }),
      'beta',
    );
    expect(firstValid(b)).toBe(14); // one more: 10 returns need 11 values
    expect(b.slice(14).every((x) => typeof x === 'number')).toBe(true);

    const rel = col(
      priceRelative(both, { column: 'smaClose', benchmark: 'smaBench' }),
      'priceRel',
    );
    expect(firstValid(rel)).toBe(4);

    const perf = col(
      performanceIndex(both, {
        column: 'smaClose',
        benchmark: 'smaBench',
        period: 10,
      }),
      'perf',
    );
    expect(firstValid(perf)).toBe(14);
  });
});

describe('[talib] all-missing input yields all-missing two-series studies', () => {
  const allMissingPair = new TimeSeries({
    name: 'bars',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'close', kind: 'number', required: false },
      { name: 'bench', kind: 'number', required: false },
    ] as const,
    rows: Array.from({ length: 20 }, (_, i) => [
      i,
      undefined,
      undefined,
    ]) as Array<[number, number | undefined, number | undefined]>,
  });

  it('neither throws nor invents a value', () => {
    const emptyPair = (v: Array<number | undefined>) => {
      expect(v).toHaveLength(20);
      expect(v.every((x) => x === undefined)).toBe(true);
    };
    emptyPair(
      col(
        correlation(allMissingPair as never, {
          benchmark: 'bench' as never,
          period: 5,
        }),
        'corr',
      ),
    );
    emptyPair(
      col(
        beta(allMissingPair as never, {
          benchmark: 'bench' as never,
          period: 5,
        }),
        'beta',
      ),
    );
    emptyPair(
      col(
        priceRelative(allMissingPair as never, { benchmark: 'bench' as never }),
        'priceRel',
      ),
    );
    emptyPair(
      col(
        performanceIndex(allMissingPair as never, {
          benchmark: 'bench' as never,
          period: 5,
        }),
        'perf',
      ),
    );
  });
});

/* -------------------------------------------------------------------------- */
/* [PND-SFOLD] — the K6 state machines' scale and shift behaviour.             */
/*                                                                             */
/* Every one of these is worked out from the definition rather than copied     */
/* from a neighbour, because the five studies do NOT share an answer:          */
/*                                                                             */
/*  parabolicSar / superTrend / atrTrailingStop  price-EQUIVARIANT in both     */
/*      scale and shift (they are stop LEVELS, in the units of the price), and */
/*      the trend column is invariant under both.                              */
/*  negativeVolumeIndex / positiveVolumeIndex    price-scale INVARIANT (the    */
/*      term is a ratio) but NOT price-shift invariant (a shift moves the      */
/*      return's denominator); volume-scale AND volume-shift invariant (only   */
/*      the comparison is read).                                               */
/*  klinger                                      volume-EQUIVARIANT (the force */
/*      is linear in volume) and price-scale AND price-shift INVARIANT: dm/cm  */
/*      is a ratio of ranges, and the trend flag compares two HLC sums, which  */
/*      moves both sides equally under either transform.                       */
/* -------------------------------------------------------------------------- */

const k6PropSchema = [
  { name: 'time', kind: 'time' },
  { name: 'high', kind: 'number' },
  { name: 'low', kind: 'number' },
  { name: 'close', kind: 'number' },
  { name: 'volume', kind: 'number' },
] as const;

/** Wavy OHLCV bars. `k` scales every PRICE, `shift` adds to every price;
 *  `vk` scales every volume and `vShift` adds to it. */
const k6PropBars = (n = 50, k = 1, shift = 0, vk = 1, vShift = 0) =>
  new TimeSeries({
    name: 'bars',
    schema: k6PropSchema,
    rows: Array.from({ length: n }, (_, i) => {
      const c = 100 + 8 * Math.sin(i / 3.1) + 0.25 * i;
      return [
        i,
        (c + 0.4 + 0.6 * Math.abs(Math.sin(i / 2))) * k + shift,
        (c - 0.5 - 0.6 * Math.abs(Math.cos(i / 2.5))) * k + shift,
        c * k + shift,
        (1000 + 130 * ((i * 7) % 5) + 40 * (i % 3)) * vk + vShift,
      ];
    }) as Array<[number, number, number, number, number]>,
  });

const allMissingK6 = (n = 20) =>
  new TimeSeries({
    name: 'bars',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'high', kind: 'number', required: false },
      { name: 'low', kind: 'number', required: false },
      { name: 'close', kind: 'number', required: false },
      { name: 'volume', kind: 'number', required: false },
    ] as const,
    rows: Array.from({ length: n }, (_, i) => [
      i,
      undefined,
      undefined,
      undefined,
      undefined,
    ]) as never,
  });

describe('[talib] the K6 stop machines are price-equivariant', () => {
  const K = 1000;
  const SHIFT = 5000;

  it('parabolicSar scales LINEARLY and shifts by the same constant', () => {
    const base = col(parabolicSar(k6PropBars()), 'psar');
    expect(base.filter((x) => x !== undefined).length).toBeGreaterThan(40);
    expectLinear(base, col(parabolicSar(k6PropBars(50, K)), 'psar'), K);
    const shifted = col(parabolicSar(k6PropBars(50, 1, SHIFT)), 'psar');
    for (let i = 0; i < base.length; i += 1) {
      if (base[i] === undefined) expect(shifted[i], `bar ${i}`).toBeUndefined();
      else expect(shifted[i]! - SHIFT, `bar ${i}`).toBeCloseTo(base[i]!, 6);
    }
  });

  it('parabolicSar’s TREND column is invariant under both transforms', () => {
    const base = col(parabolicSar(k6PropBars()), 'psarTrend');
    expect(new Set(base.filter((x) => x !== undefined))).toEqual(
      new Set([1, -1]),
    );
    expectSame(base, col(parabolicSar(k6PropBars(50, K)), 'psarTrend'));
    expectSame(base, col(parabolicSar(k6PropBars(50, 1, SHIFT)), 'psarTrend'));
  });

  it('superTrend scales LINEARLY and shifts, trend unchanged', () => {
    const o = { period: 6, multiplier: 2 } as const;
    const base = col(superTrend(k6PropBars(), o), 'st');
    expect(base.filter((x) => x !== undefined).length).toBeGreaterThan(40);
    expectLinear(base, col(superTrend(k6PropBars(50, K), o), 'st'), K);
    const shifted = col(superTrend(k6PropBars(50, 1, SHIFT), o), 'st');
    for (let i = 0; i < base.length; i += 1) {
      if (base[i] === undefined) expect(shifted[i], `bar ${i}`).toBeUndefined();
      else expect(shifted[i]! - SHIFT, `bar ${i}`).toBeCloseTo(base[i]!, 6);
    }
    const trend = col(superTrend(k6PropBars(), o), 'stTrend');
    expectSame(trend, col(superTrend(k6PropBars(50, K), o), 'stTrend'));
    expectSame(trend, col(superTrend(k6PropBars(50, 1, SHIFT), o), 'stTrend'));
    expect(new Set(trend.filter((x) => x !== undefined))).toEqual(
      new Set([1, -1]),
    );
  });

  it('atrTrailingStop scales LINEARLY and shifts, trend unchanged', () => {
    const o = { period: 6, multiplier: 2 } as const;
    const base = col(atrTrailingStop(k6PropBars(), o), 'ats');
    expect(base.filter((x) => x !== undefined).length).toBeGreaterThan(40);
    expectLinear(base, col(atrTrailingStop(k6PropBars(50, K), o), 'ats'), K);
    const shifted = col(atrTrailingStop(k6PropBars(50, 1, SHIFT), o), 'ats');
    for (let i = 0; i < base.length; i += 1) {
      if (base[i] === undefined) expect(shifted[i], `bar ${i}`).toBeUndefined();
      else expect(shifted[i]! - SHIFT, `bar ${i}`).toBeCloseTo(base[i]!, 6);
    }
    const trend = col(atrTrailingStop(k6PropBars(), o), 'atsTrend');
    expectSame(trend, col(atrTrailingStop(k6PropBars(50, K), o), 'atsTrend'));
    expect(new Set(trend.filter((x) => x !== undefined))).toEqual(
      new Set([1, -1]),
    );
  });

  it('the stop machines are unchanged by scaling VOLUME — they never read it', () => {
    expectSame(
      col(parabolicSar(k6PropBars()), 'psar'),
      col(parabolicSar(k6PropBars(50, 1, 0, 77)), 'psar'),
    );
    expectSame(
      col(superTrend(k6PropBars(), { period: 6 }), 'st'),
      col(superTrend(k6PropBars(50, 1, 0, 77), { period: 6 }), 'st'),
    );
  });
});

describe('[talib] NVI / PVI are invariant in both price scale and volume', () => {
  const K = 1000;

  it('scaling every PRICE leaves both indices unchanged (the term is a ratio)', () => {
    for (const [name, run] of [
      ['nvi', negativeVolumeIndex],
      ['pvi', positiveVolumeIndex],
    ] as const) {
      const base = col(run(k6PropBars()), name);
      expect(base.filter((x) => x !== undefined).length).toBe(50);
      expect(new Set(base).size, name).toBeGreaterThan(5); // it actually moves
      expectSame(base, col(run(k6PropBars(50, K)), name));
    }
  });

  it('SHIFTING every price does NOT leave them unchanged (the denominator moves)', () => {
    // The companion assertion: a study that dropped the division by the
    // previous close would pass the scale test above and fail this one.
    const base = col(negativeVolumeIndex(k6PropBars()), 'nvi');
    const shifted = col(negativeVolumeIndex(k6PropBars(50, 1, 5000)), 'nvi');
    expect(shifted.at(-1)).not.toBeCloseTo(base.at(-1)!, 6);
  });

  it('scaling or shifting VOLUME leaves both unchanged (only the comparison is read)', () => {
    for (const [name, run] of [
      ['nvi', negativeVolumeIndex],
      ['pvi', positiveVolumeIndex],
    ] as const) {
      const base = col(run(k6PropBars()), name);
      expectSame(base, col(run(k6PropBars(50, 1, 0, 1e6)), name));
      expectSame(base, col(run(k6PropBars(50, 1, 0, 1, 1e6)), name));
    }
  });

  it('`start` scales the whole line and nothing else', () => {
    const base = col(negativeVolumeIndex(k6PropBars()), 'nvi');
    const based = col(negativeVolumeIndex(k6PropBars(), { start: 250 }), 'nvi');
    for (let i = 0; i < base.length; i += 1) {
      expect(based[i]! * 4, `bar ${i}`).toBeCloseTo(base[i]!, 6);
    }
  });

  it('composes over another study’s output: length kept, warm-up composed', () => {
    // The `rsi(sma(...))` shape. NVI's `column` is a price column, so an SMA
    // output is a legal source; the index must start at the SMA's first bar
    // rather than coming back empty.
    const chained = negativeVolumeIndex(
      sma(k6PropBars(), { period: 5, output: 'smaClose' }),
      { column: 'smaClose' },
    );
    const v = col(chained, 'nvi');
    expect(v).toHaveLength(50);
    expect(v.slice(0, 4).every((x) => x === undefined)).toBe(true);
    expect(v[4]).toBe(1000); // re-based on the SMA's first bar
    expect(v.filter((x) => x !== undefined).length).toBe(46);
  });
});

describe('[talib] klinger is linear in volume and invariant in price', () => {
  const o = { fastPeriod: 4, slowPeriod: 9, signalPeriod: 3 } as const;

  it('scaling VOLUME scales both columns by the same factor', () => {
    for (const name of ['kvo', 'kvoSignal']) {
      const base = col(klinger(k6PropBars(), o), name);
      expect(base.filter((x) => x !== undefined).length, name).toBeGreaterThan(
        35,
      );
      expectLinear(base, col(klinger(k6PropBars(50, 1, 0, 500), o), name), 500);
    }
  });

  it('scaling or shifting PRICE leaves both columns unchanged', () => {
    // dm/cm is a ratio of RANGES, so a scale cancels; and the trend flag
    // compares two `high + low + close` sums, which a scale (k > 0) and a
    // shift (+3c on both sides) both preserve.
    //
    // Compared RELATIVELY: the force is `volume x ... x 100`, so these
    // columns run to 1e5, where an absolute 1e-8 tolerance is below one ulp
    // of the arithmetic. The residual measured here is ~1.4e-13 relative,
    // i.e. the reassociation the transform forces, not a term that moved.
    const near = (
      base: Array<number | undefined>,
      other: Array<number | undefined>,
      label: string,
    ) => {
      for (let i = 0; i < base.length; i += 1) {
        if (base[i] === undefined) {
          expect(other[i], `${label}[${i}]`).toBeUndefined();
        } else {
          expect(
            Math.abs(other[i]! - base[i]!) / Math.max(1, Math.abs(base[i]!)),
            `${label}[${i}]`,
          ).toBeLessThan(1e-11);
        }
      }
    };
    for (const name of ['kvo', 'kvoSignal']) {
      const base = col(klinger(k6PropBars(), o), name);
      near(base, col(klinger(k6PropBars(50, 1000), o), name), `${name} scaled`);
      near(
        base,
        col(klinger(k6PropBars(50, 1, 5000), o), name),
        `${name} shifted`,
      );
    }
  });

  it('SHIFTING volume is NOT invariant — it moves the force, not just the flags', () => {
    // The companion assertion to the volume-scale one: the force multiplies
    // by volume, so adding a constant to every volume is not a no-op (unlike
    // NVI, which only compares them).
    const base = col(klinger(k6PropBars(), o), 'kvo');
    const shifted = col(klinger(k6PropBars(50, 1, 0, 1, 5000), o), 'kvo');
    expect(shifted.at(-1)).not.toBeCloseTo(base.at(-1)!, 3);
  });
});

describe('[talib] all-missing in, all-missing out for every K6 study', () => {
  it('none of the five throws, and none invents a value', () => {
    const empty = allMissingK6();
    for (const [name, out] of [
      ['psar', parabolicSar(empty as never)],
      ['psarTrend', parabolicSar(empty as never)],
      ['st', superTrend(empty as never, { period: 3 })],
      ['ats', atrTrailingStop(empty as never, { period: 3 })],
      ['nvi', negativeVolumeIndex(empty as never)],
      ['pvi', positiveVolumeIndex(empty as never)],
      ['kvo', klinger(empty as never, { fastPeriod: 2, slowPeriod: 4 })],
      ['kvoSignal', klinger(empty as never, { fastPeriod: 2, slowPeriod: 4 })],
    ] as const) {
      const v = col(out, name);
      expect(v, name).toHaveLength(20);
      expect(
        v.every((x) => x === undefined),
        name,
      ).toBe(true);
    }
  });
});
