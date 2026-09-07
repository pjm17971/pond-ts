import { describe, expect, it } from 'vitest';
import { TimeSeries } from 'pond-ts';
import {
  MA_TYPES,
  movingAverage,
  sma,
  ema,
  bollinger,
  zScore,
  percentChange,
  envelope,
  rsi,
  macd,
  momentum,
  historicalVolatility,
  obv,
  vwap,
  stochastic,
  williamsR,
  donchian,
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
  directionalMovement,
  aroon,
  vortex,
  correlation,
  beta,
  priceRelative,
  performanceIndex,
  linearRegression,
  timeSeriesForecast,
  chandeForecastOscillator,
  centerOfGravity,
  guppy,
  rainbow,
  rainbowOscillator,
  kst,
  priceMomentumOscillator,
  stochasticRsi,
  trueStrengthIndex,
  movingAverageDeviation,
  parabolicSar,
  superTrend,
  atrTrailingStop,
  negativeVolumeIndex,
  positiveVolumeIndex,
  klinger,
  typicalPrice,
  medianPrice,
  weightedClose,
  averagePrice,
  balanceOfPower,
  starcBands,
  highLowBands,
  bollingerBandwidth,
  bollingerPercentB,
  primeNumberBands,
  primeNumberOscillator,
  marketFacilitationIndex,
  stochasticMomentumIndex,
  fisherTransform,
  schaffTrendCycle,
  prettyGoodOscillator,
  swingIndex,
  twiggsMoneyFlow,
  tradeVolumeIndex,
  shinoharaIntensityRatio,
  elderImpulse,
  movingAverageCross,
  anchoredVwap,
  ichimoku,
  zigZag,
  sessionVwap,
  pivotPoints,
  TradingCalendar,
  generateSessions,
  accumulativeSwingIndex,
  randomWalkIndex,
  ravi,
  trendIntensityIndex,
  specialK,
} from '../src/index.js';

/* -------------------------------------------------------------------------- */
/* [PND-STUDYBOX] — studies compute over Float64Array with NaN-as-missing.     */
/*                                                                             */
/* The kernel used to hand studies `Array<number | undefined>` and each study  */
/* checked every input for `undefined` per cell. It now hands them a           */
/* `Float64Array` where NaN marks a gap, which propagates through arithmetic   */
/* on its own — so those per-cell checks are gone.                             */
/*                                                                             */
/* That is only safe if the OBSERVABLE result is unchanged, which is what      */
/* these pin. The oracle suite (test/study-oracle.test.ts) already checks the  */
/* numbers against pandas; this checks the thing the oracle fixtures don't     */
/* exercise — where the gaps are, and that they read back as `undefined`       */
/* rather than as NaN leaking into a user-visible column.                      */
/* -------------------------------------------------------------------------- */

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'close', kind: 'number' },
] as const;

const MINUTE = 60_000;

const bars = (closes: number[]) =>
  new TimeSeries({
    name: 'bars',
    schema,
    rows: closes.map((c, i) => [i * MINUTE, c] as const) as never,
  });

/** Structural read so a test can name any output column without the
 *  schema-narrowed `column()` overload rejecting a plain string. */
type ReadableSeries = {
  readonly length: number;
  column(name: string): { at(i: number): unknown } | undefined;
};

const cells = (s: unknown, name: string): Array<unknown> => {
  const r = s as ReadableSeries;
  const col = r.column(name);
  return Array.from({ length: r.length }, (_, i) => col?.at(i));
};

const nullCountOf = (s: unknown, name: string): number =>
  cells(s, name).filter((x) => x === undefined).length;

const rising = Array.from({ length: 30 }, (_, i) => 100 + i);

describe('[PND-STUDYBOX] warm-up heads read as undefined, not NaN', () => {
  it('sma warms up length-preservingly', () => {
    const out = sma(bars(rising), { period: 5 });
    const v = cells(out, 'sma');
    expect(out.length).toBe(30);
    for (let i = 0; i < 4; i += 1) {
      expect(v[i], `bar ${i}`).toBeUndefined();
    }
    expect(v[4]).toBeCloseTo(102, 10);
    // No NaN may reach a user-visible column — a NaN here would mean the
    // gap marker leaked through `withColumn` instead of becoming a gap.
    expect(v.some((x) => typeof x === 'number' && Number.isNaN(x))).toBe(false);
    expect(nullCountOf(out, 'sma')).toBe(4);
  });

  it('ema warms up length-preservingly', () => {
    const out = ema(bars(rising), { period: 5 });
    const v = cells(out, 'ema');
    expect(v.slice(0, 4).every((x) => x === undefined)).toBe(true);
    expect(typeof v[4]).toBe('number');
  });

  it('bollinger warms up on all three bands', () => {
    const out = bollinger(bars(rising), { period: 5 });
    for (const name of ['bbMiddle', 'bbUpper', 'bbLower']) {
      const v = cells(out as never, name);
      expect(
        v.slice(0, 4).every((x) => x === undefined),
        name,
      ).toBe(true);
      expect(typeof v[4], name).toBe('number');
      expect(nullCountOf(out, name), name).toBe(4);
    }
  });

  it('zScore warms up', () => {
    const out = zScore(bars(rising), { period: 5 });
    const v = cells(out, 'zscore');
    expect(v.slice(0, 4).every((x) => x === undefined)).toBe(true);
    expect(typeof v[4]).toBe('number');
  });

  it('envelope warms up on all three lines', () => {
    const out = envelope(bars(rising), { period: 5 });
    for (const name of ['envMiddle', 'envUpper', 'envLower']) {
      const v = cells(out as never, name);
      expect(
        v.slice(0, 4).every((x) => x === undefined),
        name,
      ).toBe(true);
      expect(typeof v[4], name).toBe('number');
    }
  });

  it('percentChange has no predecessor for the first bar', () => {
    const out = percentChange(bars(rising), {});
    const v = cells(out, 'pctChange');
    expect(v[0]).toBeUndefined();
    expect(v[1]).toBeCloseTo(1, 10);
  });

  it('macd warms up per column without leaking NaN', () => {
    const out = macd(bars(rising), {
      fastPeriod: 3,
      slowPeriod: 7,
      signalPeriod: 4,
    });
    // Line before signal — the per-column warm-up, not a single shared one.
    // (No `!isNaN` assertion here: `withColumn` maps NaN to missing on its
    // typed door, so such a check can never fire and would read as coverage
    // it isn't. What is worth pinning is WHERE the missing rows are.)
    expect(cells(out, 'macdLine')[5]).toBeUndefined();
    expect(cells(out, 'macdLine')[6]).toBeDefined();
    // signal = EMA(4) of the line, whose warm-up shifts past the line's own:
    // 6 + 4 - 1 = 9, not 8.
    expect(cells(out, 'macdSignal')[8]).toBeUndefined();
    expect(cells(out, 'macdSignal')[9]).toBeDefined();
  });

  it('rsi warms up length-preservingly', () => {
    // rsi is the first study to hand-roll its own NaN derivation (the
    // gain/loss split), so it is the most likely to leak one into a
    // user-visible column rather than reading back as `undefined`.
    const out = rsi(bars(rising), { period: 5 });
    const v = cells(out, 'rsi');
    expect(v.slice(0, 5).every((x) => x === undefined)).toBe(true);
    expect(typeof v[5]).toBe('number');
    expect(v.every((x) => !Number.isNaN(x as number))).toBe(true);
  });
});

describe('[PND-STUDYBOX] the study-specific guards still produce missing', () => {
  // A flat window has σ = 0, which has no meaningful band or z-score. These
  // are the only per-cell guards that survived the NaN-propagation
  // simplification, so they are the ones worth pinning.
  const flat = Array.from({ length: 10 }, () => 42);

  it('rsi emits missing on a flat window (0/0 has no relative strength)', () => {
    // And the value is `undefined`, not NaN — the deliberate delta from
    // TA-Lib, which reports 0 here and so cannot distinguish "no movement"
    // from "every bar fell".
    const out = rsi(bars(flat), { period: 3 });
    const v = cells(out, 'rsi');
    expect(v[3]).toBeUndefined();
    expect(v[9]).toBeUndefined();
    expect(v.every((x) => !Number.isNaN(x as number))).toBe(true);
  });

  it('bollinger emits missing bands where σ = 0', () => {
    const out = bollinger(bars(flat), { period: 5 });
    const upper = cells(out, 'bbUpper');
    const middle = cells(out, 'bbMiddle');
    // The centre line is defined on a flat window; the bands are not.
    expect(middle[9]).toBeCloseTo(42, 10);
    expect(upper[9]).toBeUndefined();
    expect(nullCountOf(out, 'bbUpper')).toBe(10);
  });

  it('zScore emits missing where σ = 0', () => {
    const out = zScore(bars(flat), { period: 5 });
    expect(cells(out, 'zscore')[9]).toBeUndefined();
    expect(nullCountOf(out, 'zscore')).toBe(10);
  });

  it('percentChange emits missing where the base is zero', () => {
    const out = percentChange(bars([0, 5, 0, 5]), {});
    const v = cells(out, 'pctChange');
    expect(v[0]).toBeUndefined(); // no predecessor
    expect(v[1]).toBeUndefined(); // base 0
    expect(v[2]).toBeCloseTo(-100, 10);
    expect(v[3]).toBeUndefined(); // base 0
  });
});

describe('[PND-STUDYBOX] gaps in the source propagate', () => {
  const gappySchema = [
    { name: 'time', kind: 'time' },
    { name: 'close', kind: 'number', required: false },
  ] as const;

  const gappy = new TimeSeries({
    name: 'bars',
    schema: gappySchema,
    rows: Array.from({ length: 20 }, (_, i) => [
      i * MINUTE,
      i === 10 ? undefined : 100 + i,
    ]) as never,
  });

  it('a missing source bar does not become a spurious number', () => {
    const out = sma(gappy, { period: 4 });
    const v = cells(out, 'sma');
    // A count window still advances over the gap, so the study stays
    // length-preserving; what matters is that no cell reads back NaN.
    expect(v.length).toBe(20);
    expect(v.some((x) => typeof x === 'number' && Number.isNaN(x))).toBe(false);
  });

  it('studies compose — one study over another’s output', () => {
    const stacked = sma(sma(bars(rising), { period: 5, output: 'sma5' }), {
      period: 3,
      column: 'sma5' as never,
      output: 'smaOfSma',
    });
    const v = cells(stacked, 'smaOfSma');
    // Warm-up is 4 bars, not 6. The outer window is a **count of rows**, not
    // of defined values, so once its span reaches the inner study's first
    // defined bar it has met `minSamples` and averages the one contributor
    // it can see. Verified byte-identical to the pre-[PND-STUDYBOX] build,
    // so this is the existing contract rather than something the typed
    // rewrite introduced.
    expect(v.slice(0, 4).every((x) => x === undefined)).toBe(true);
    expect(v[4]).toBeCloseTo(102, 10);
    expect(v[5]).toBeCloseTo(102.5, 10);
    expect(v.some((x) => typeof x === 'number' && Number.isNaN(x))).toBe(false);
  });
});

describe('[PND-STUDYBOX] the difference-built studies warm up by `period` rows', () => {
  // Both are built on differences, so they cost one bar more than a rolling
  // study of the same period. What is pinned is WHERE the missing rows are —
  // not "no NaN", which `withColumn` makes unfalsifiable.
  const wavy = Array.from(
    { length: 30 },
    (_, i) => 100 + 6 * Math.sin(i / 2.5),
  );

  it('momentum: the first `period` rows, then values', () => {
    const out = momentum(bars(wavy), { period: 5 });
    const v = cells(out, 'momentum');
    expect(v.slice(0, 5).every((x) => x === undefined)).toBe(true);
    expect(v[5]).toBeCloseTo(wavy[5]! - wavy[0]!, 10);
    expect(nullCountOf(out, 'momentum')).toBe(5);
  });

  it('historicalVolatility: the first `period` rows, then values', () => {
    const out = historicalVolatility(bars(wavy), { period: 5 });
    const v = cells(out, 'hv');
    expect(v.slice(0, 5).every((x) => x === undefined)).toBe(true);
    expect(typeof v[5]).toBe('number');
    expect(nullCountOf(out, 'hv')).toBe(5);
  });

  it('historicalVolatility: a non-positive price reads as missing, never as a number', () => {
    // With period 1 every window is a single return, so this pins the
    // FOOTPRINT: exactly the two returns that touch the bad price are gone.
    // (A zero price alone would pass without the guard — `ln(0)` is
    // `-Infinity`, which the kernel already skips. What the explicit guard
    // buys is the negative-price case, `ln(-4/-5)` being finite; that is
    // pinned in studies.test.ts.)
    const out = historicalVolatility(bars([100, 101, 0, 102, 103]), {
      period: 1,
      annualize: 1,
    });
    const v = cells(out, 'hv');
    expect(v[1]).toBe(0); // one return: σ of a single value
    expect(v[2]).toBeUndefined(); // return INTO the zero
    expect(v[3]).toBeUndefined(); // return OUT of it
    expect(v[4]).toBe(0);
    expect(nullCountOf(out, 'hv')).toBe(3); // bar 0 + the two above
  });
});

describe('[PND-STUDYBOX] range-position studies: where the missing rows are', () => {
  // No `!isNaN` assertions here — `withColumn` maps NaN to missing on its
  // typed door, so such a check can never fire. What is worth pinning is
  // WHERE the missing rows are, and how many.
  const ohlc = (rows: Array<[number, number, number]>) =>
    new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'high', kind: 'number' },
        { name: 'low', kind: 'number' },
        { name: 'close', kind: 'number' },
      ] as const,
      rows: rows.map(([h, l, c], i) => [i * MINUTE, h, l, c]) as Array<
        [number, number, number, number]
      >,
    });
  const wavy = ohlc(
    Array.from({ length: 30 }, (_, i) => {
      const c = 100 + 8 * Math.sin(i / 3) + 0.2 * i;
      return [
        c + 0.4 + 0.6 * Math.abs(Math.sin(i / 2)),
        c - 0.3 - 0.5 * Math.abs(Math.cos(i / 2.5)),
        c,
      ];
    }),
  );

  it('stochastic warms up %K and %D at different bars', () => {
    const out = stochastic(wavy, { kPeriod: 5, slowing: 3, dPeriod: 3 });
    const k = cells(out, 'stochK');
    const d = cells(out, 'stochD');
    // %K: kPeriod + slowing − 2 = 6 missing rows; %D: two more.
    expect(k.slice(0, 6).every((x) => x === undefined)).toBe(true);
    expect(typeof k[6]).toBe('number');
    expect(nullCountOf(out, 'stochK')).toBe(6);
    expect(d.slice(0, 8).every((x) => x === undefined)).toBe(true);
    expect(typeof d[8]).toBe('number');
    expect(nullCountOf(out, 'stochD')).toBe(8);
  });

  it('williamsR warms up over period − 1 rows', () => {
    const out = williamsR(wavy, { period: 5 });
    const v = cells(out, 'williamsR');
    expect(v.slice(0, 4).every((x) => x === undefined)).toBe(true);
    expect(typeof v[4]).toBe('number');
    expect(nullCountOf(out, 'williamsR')).toBe(4);
  });

  it('donchian warms up on all three lines', () => {
    const out = donchian(wavy, { period: 5 });
    for (const name of ['dcUpper', 'dcLower', 'dcMiddle']) {
      const v = cells(out, name);
      expect(
        v.slice(0, 4).every((x) => x === undefined),
        name,
      ).toBe(true);
      expect(typeof v[4], name).toBe('number');
      expect(nullCountOf(out, name), name).toBe(4);
    }
  });

  it('stochastic and williamsR emit missing on a flat window (0/0 has no position)', () => {
    // The deliberate delta from TA-Lib, which reports 0 for both — "at the
    // very bottom" for %K and "at the very top" for %R, on the same bar.
    const flat = ohlc(Array.from({ length: 10 }, () => [42, 42, 42]));
    const st = stochastic(flat, { kPeriod: 3, slowing: 2, dPeriod: 2 });
    expect(nullCountOf(st, 'stochK')).toBe(10);
    expect(nullCountOf(st, 'stochD')).toBe(10);
    expect(nullCountOf(williamsR(flat, { period: 3 }), 'williamsR')).toBe(10);
    // Donchian has no division: a flat window is a zero-width channel.
    const dc = donchian(flat, { period: 3 });
    expect(nullCountOf(dc, 'dcUpper')).toBe(2);
    expect(cells(dc, 'dcMiddle')[9]).toBe(42);
  });
});

describe('[PND-STUDYBOX] the volume studies', () => {
  const cvSchema = [
    { name: 'time', kind: 'time' },
    { name: 'high', kind: 'number', required: false },
    { name: 'low', kind: 'number', required: false },
    { name: 'close', kind: 'number', required: false },
    { name: 'volume', kind: 'number', required: false },
  ] as const;
  const cv = (
    closes: Array<number | undefined>,
    volumes: Array<number | undefined>,
  ) =>
    new TimeSeries({
      name: 'bars',
      schema: cvSchema,
      rows: closes.map((c, i) => [
        i * MINUTE,
        c === undefined ? undefined : c + 1,
        c === undefined ? undefined : c - 1,
        c,
        volumes[i],
      ]) as never,
    });
  const closes = [10, 11, 11, 9, 12, 12, 8];
  const volumes = [100, 200, 300, 400, 500, 600, 700];

  it('obv has no warm-up: bar 0 is its own volume', () => {
    const out = obv(cv(closes, volumes) as never);
    expect(cells(out, 'obv')[0]).toBe(100);
    expect(nullCountOf(out, 'obv')).toBe(0);
  });

  it('obv shifts for a leading gap and propagates an interior one', () => {
    // The running-sum asymmetry, same as Wilder's: the head is stepped
    // over, a hole is carried to the end. Where the missing rows ARE is
    // what is pinned — no `!isNaN` check, which could never fire.
    const lead = cells(
      obv(cv([undefined, ...closes.slice(1)], volumes) as never),
      'obv',
    );
    expect(lead[0]).toBeUndefined();
    expect(lead[1]).toBe(200);
    expect(lead[6]).toBe(-400);

    const hole = obv(
      cv(closes, [100, 200, 300, undefined, 500, 600, 700]) as never,
    );
    const h = cells(hole, 'obv');
    expect(h[2]).toBe(300);
    expect(h[3]).toBeUndefined();
    expect(nullCountOf(hole, 'obv')).toBe(4); // bars 3..6
  });

  it('vwap warms up length-preservingly', () => {
    const out = vwap(cv(closes, volumes) as never, { period: 3 });
    const v = cells(out, 'vwap');
    expect(v.slice(0, 2).every((x) => x === undefined)).toBe(true);
    expect(typeof v[2]).toBe('number');
    expect(nullCountOf(out, 'vwap')).toBe(2);
  });

  it('vwap emits missing where the window has no volume', () => {
    // Σvolume = 0 has nothing to weight by. It must read back as a gap —
    // not as the plain mean, and not as 0.
    const out = vwap(cv(closes, [0, 0, 0, 400, 500, 600, 700]) as never, {
      period: 3,
    });
    const v = cells(out, 'vwap');
    expect(v[2]).toBeUndefined(); // volumes 0, 0, 0
    expect(v[3]).toBeCloseTo(9, 10); // 0, 0, 400 — all the weight on bar 3
    expect(nullCountOf(out, 'vwap')).toBe(3); // two warm-up + one no-volume
  });
});

describe('[PND-STUDYBOX] the moving-average engine: where each type is missing', () => {
  // A wavy series, because a monotonic one makes every MA type agree — the
  // warm-up positions would still be pinned, but nothing else would be.
  const wavy = Array.from(
    { length: 24 },
    (_, i) => 100 + 6 * Math.sin(i / 2.5),
  );

  // Each type's first defined bar at period 4, from its definition:
  // n−1 for the window types, 2n−2 / 3n−3 for the double / triple EMA,
  // n−2+round(√n) for Hull, n for KAMA (it needs n differences), and
  // ⌊(n−1)/2⌋+n−1 for ZLEMA.
  const firstBar: Record<string, number> = {
    sma: 3,
    ema: 3,
    wma: 3,
    smma: 3,
    trima: 3,
    dema: 6,
    tema: 9,
    hull: 4,
    kama: 4,
    zlema: 4,
  };

  const gappy = () =>
    new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'close', kind: 'number', required: false },
      ] as const,
      rows: wavy.map((c, i) => [i * MINUTE, i === 10 ? undefined : c]) as never,
    });

  it('warms up exactly `firstBar` rows, per type, length-preservingly', () => {
    for (const type of MA_TYPES) {
      const out = movingAverage(bars(wavy), { period: 4, type });
      const v = cells(out, 'ma');
      expect(out.length, type).toBe(24);
      expect(nullCountOf(out, 'ma'), type).toBe(firstBar[type]!);
      expect(
        v.slice(0, firstBar[type]!).every((x) => x === undefined),
        type,
      ).toBe(true);
      expect(typeof v[firstBar[type]!], type).toBe('number');
    }
  });

  it('a gap at bar 10: the window types blank a run and come back', () => {
    // `wma` cannot skip a cell without reweighting the rest, so the windows
    // CONTAINING bar 10 read missing — bars 10–13 for a 4-bar WMA — and bar
    // 14 is a value again.
    const wma = cells(movingAverage(gappy(), { period: 4, type: 'wma' }), 'ma');
    expect(wma.slice(10, 14).every((x) => x === undefined)).toBe(true);
    expect(typeof wma[14]).toBe('number');

    // `sma` keeps `sma()`'s contract instead: the window still spans four
    // ROWS, so it averages the three finite ones and never goes missing.
    const sma4 = cells(
      movingAverage(gappy(), { period: 4, type: 'sma' }),
      'ma',
    );
    expect(typeof sma4[10]).toBe('number');

    // The ema family emits nothing on the gap bar itself and resumes.
    const emaCells = cells(
      movingAverage(gappy(), { period: 4, type: 'ema' }),
      'ma',
    );
    expect(emaCells[10]).toBeUndefined();
    expect(typeof emaCells[11]).toBe('number');
  });

  it('a gap at bar 10: smma and kama propagate it to the end', () => {
    for (const type of ['smma', 'kama'] as const) {
      const out = movingAverage(gappy(), { period: 4, type });
      const v = cells(out, 'ma');
      expect(typeof v[9], type).toBe('number');
      expect(
        v.slice(10).every((x) => x === undefined),
        type,
      ).toBe(true);
      // The head plus everything from the gap on: 24 rows less the run of
      // values between the warm-up and bar 10.
      expect(nullCountOf(out, 'ma'), type).toBe(24 - (10 - firstBar[type]!));
    }
  });
});

describe('[PND-STUDYBOX] the K2 channels: where the missing rows are', () => {
  // No `!isNaN` assertions — `withColumn` maps NaN to missing on its typed
  // door, so such a check can never fire. What is pinned is WHERE the missing
  // rows are, and how many.
  const gappyOhlc = (gapAt: number | undefined, column: 'high' | 'close') =>
    new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'high', kind: 'number', required: false },
        { name: 'low', kind: 'number', required: false },
        { name: 'close', kind: 'number', required: false },
      ] as const,
      rows: Array.from({ length: 30 }, (_, i) => {
        const c = 100 + 8 * Math.sin(i / 3) + 0.2 * i;
        const high = c + 0.4 + 0.6 * Math.abs(Math.sin(i / 2));
        const low = c - 0.3 - 0.5 * Math.abs(Math.cos(i / 2.5));
        const hide = i === gapAt;
        return [
          i * MINUTE,
          hide && column === 'high' ? undefined : high,
          low,
          hide && column === 'close' ? undefined : c,
        ];
      }) as never,
    });

  it('keltner warms up per column: centre at period − 1, bands at max(centre, ATR)', () => {
    const out = keltner(gappyOhlc(undefined, 'close'), {
      period: 4,
      atrPeriod: 6,
      maType: 'sma',
    });
    expect(nullCountOf(out, 'kcMiddle')).toBe(3);
    expect(nullCountOf(out, 'kcUpper')).toBe(6);
    expect(nullCountOf(out, 'kcLower')).toBe(6);
    expect(
      cells(out, 'kcMiddle')
        .slice(0, 3)
        .every((x) => x === undefined),
    ).toBe(true);
    expect(typeof cells(out, 'kcMiddle')[3]).toBe('number');
    expect(typeof cells(out, 'kcUpper')[6]).toBe('number');
  });

  it('keltner: a missing close blanks the centre for one bar and the bands to the end', () => {
    // The two halves genuinely differ, and this is the case that shows it.
    // The centre is an EMA of typical price: the gap bar has no typical
    // price, and the recursion then skips it and carries on. The ATR is
    // Wilder-smoothed TRUE RANGE, which reads the PREVIOUS close — so the
    // missing close costs the NEXT bar's true range, and a recursion never
    // gives that back.
    const out = keltner(gappyOhlc(15, 'close'), { period: 4, atrPeriod: 6 });
    const mid = cells(out, 'kcMiddle');
    const up = cells(out, 'kcUpper');
    expect(mid[14]).toBeDefined();
    expect(mid[15]).toBeUndefined();
    expect(typeof mid[16]).toBe('number'); // the ema recovers
    expect(typeof up[14]).toBe('number');
    expect(up.slice(15).every((x) => x === undefined)).toBe(true);
    // The head (6) plus bars 15..29.
    expect(nullCountOf(out, 'kcUpper')).toBe(6 + 15);
  });

  it('atrBands warms up over `period` rows — the ATR’s own off-by-one', () => {
    const out = atrBands(gappyOhlc(undefined, 'close'), { period: 5 });
    for (const name of ['atrbUpper', 'atrbLower']) {
      expect(
        cells(out, name)
          .slice(0, 5)
          .every((x) => x === undefined),
        name,
      ).toBe(true);
      expect(typeof cells(out, name)[5], name).toBe('number');
      expect(nullCountOf(out, name), name).toBe(5);
    }
  });

  it('atrBands: a missing close propagates to the end; a missing FIELD costs one bar', () => {
    const hole = atrBands(gappyOhlc(12, 'close'), { period: 5 });
    const up = cells(hole, 'atrbUpper');
    expect(typeof up[11]).toBe('number');
    expect(up.slice(12).every((x) => x === undefined)).toBe(true);
    expect(nullCountOf(hole, 'atrbUpper')).toBe(5 + 18); // head + 12..29

    // The field is a SEPARATE input, so gapping it alone leaves the ATR
    // intact and exactly one bar is lost — the asymmetry `column` exists for.
    const withField = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'high', kind: 'number' },
        { name: 'low', kind: 'number' },
        { name: 'close', kind: 'number' },
        { name: 'mid', kind: 'number', required: false },
      ] as const,
      rows: Array.from({ length: 30 }, (_, i) => {
        const c = 100 + 8 * Math.sin(i / 3) + 0.2 * i;
        return [
          i * MINUTE,
          c + 0.4 + 0.6 * Math.abs(Math.sin(i / 2)),
          c - 0.3 - 0.5 * Math.abs(Math.cos(i / 2.5)),
          c,
          i === 12 ? undefined : c,
        ];
      }) as never,
    });
    const field = atrBands(withField, { period: 5, column: 'mid' as never });
    const fu = cells(field, 'atrbUpper');
    expect(typeof fu[11]).toBe('number');
    expect(fu[12]).toBeUndefined();
    expect(typeof fu[13]).toBe('number');
    expect(nullCountOf(field, 'atrbUpper')).toBe(5 + 1);
  });
});

describe('[PND-STUDYBOX] the smoothed-rate studies: where the missing rows are', () => {
  const wavy = Array.from(
    { length: 40 },
    (_, i) => 100 + 6 * Math.sin(i / 4) + 0.1 * i,
  );

  const gappyClose = (gapAt: number | undefined) =>
    new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'close', kind: 'number', required: false },
      ] as const,
      rows: wavy.map((c, i) => [
        i * MINUTE,
        i === gapAt ? undefined : c,
      ]) as never,
    });

  const bodyBars = (gapAt: number | undefined) =>
    new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'open', kind: 'number', required: false },
        { name: 'close', kind: 'number', required: false },
      ] as const,
      rows: wavy.map((c, i) => [
        i * MINUTE,
        i === gapAt ? undefined : c - 0.4 * Math.cos(i / 1.7),
        c,
      ]) as never,
    });

  it('qstick: `period − 1` head, and a gap blanks only the windows holding it', () => {
    const clean = qstick(bodyBars(undefined), { period: 4 });
    expect(nullCountOf(clean, 'qstick')).toBe(3);
    expect(typeof cells(clean, 'qstick')[3]).toBe('number');

    // A missing open has no body, and the array door's SMA waits for
    // `period` finite VALUES — so the four windows containing bar 10 are
    // blank and bar 14 is a value again.
    const hole = qstick(bodyBars(10), { period: 4 });
    const v = cells(hole, 'qstick');
    expect(typeof v[9]).toBe('number');
    expect(v.slice(10, 14).every((x) => x === undefined)).toBe(true);
    expect(typeof v[14]).toBe('number');
    expect(nullCountOf(hole, 'qstick')).toBe(3 + 4);
  });

  it('trix: a 3·period − 2 head on both columns, offset by the signal', () => {
    const out = trix(gappyClose(undefined), { period: 3, signalPeriod: 4 });
    const line = cells(out, 'trix');
    expect(line.slice(0, 7).every((x) => x === undefined)).toBe(true);
    expect(typeof line[7]).toBe('number');
    expect(nullCountOf(out, 'trix')).toBe(7);
    expect(nullCountOf(out, 'trixSignal')).toBe(10);
    expect(typeof cells(out, 'trixSignal')[10]).toBe('number');
  });

  it('trix: an interior gap costs the bar and the one after it, then recovers', () => {
    // The `ema` family skips a missing bar (so the three stages resume), and
    // the rate of change reads a predecessor (so the bar AFTER the gap has
    // no base). Two bars, not a propagation to the end.
    const out = trix(gappyClose(20), { period: 3, signalPeriod: 4 });
    const line = cells(out, 'trix');
    expect(typeof line[19]).toBe('number');
    expect(line[20]).toBeUndefined();
    expect(line[21]).toBeUndefined();
    expect(typeof line[22]).toBe('number');
    expect(nullCountOf(out, 'trix')).toBe(7 + 2);
  });

  it('coppock: the head is max(long, short) + wma − 1', () => {
    const out = coppock(gappyClose(undefined), {
      longPeriod: 6,
      shortPeriod: 3,
      wmaPeriod: 2,
    });
    const v = cells(out, 'coppock');
    expect(v.slice(0, 7).every((x) => x === undefined)).toBe(true);
    expect(typeof v[7]).toBe('number');
    expect(nullCountOf(out, 'coppock')).toBe(7);
  });

  it('coppock: one gap blanks THREE windows — the bar and both look-backs', () => {
    // A missing bar costs its own rate of change and the ones that read it
    // as a base, `shortPeriod` and `longPeriod` bars later; the WMA then
    // masks each window containing one of the three.
    const out = coppock(gappyClose(12), {
      longPeriod: 6,
      shortPeriod: 3,
      wmaPeriod: 2,
    });
    const v = cells(out, 'coppock');
    for (const b of [12, 13, 15, 16, 18, 19]) {
      expect(v[b], `bar ${b}`).toBeUndefined();
    }
    for (const b of [11, 14, 17, 20]) {
      expect(typeof v[b], `bar ${b}`).toBe('number');
    }
    expect(nullCountOf(out, 'coppock')).toBe(7 + 6);
  });
});

describe('[PND-STUDYBOX] the K2 oscillators: where the missing rows are', () => {
  // No `!isNaN` assertions — `withColumn` maps NaN to missing on its typed
  // door, so such a check can never fire. What is worth pinning is WHERE the
  // missing rows are, which for these five is a warm-up plus whatever the
  // chosen `maType`'s interior-gap rule costs.
  const gapSchema = [
    { name: 'time', kind: 'time' },
    { name: 'high', kind: 'number', required: false },
    { name: 'low', kind: 'number', required: false },
    { name: 'close', kind: 'number', required: false },
  ] as const;
  /** Bars whose close (and therefore high/low) is missing on bar 2. */
  const gappy = (closes: Array<number | undefined>) =>
    new TimeSeries({
      name: 'bars',
      schema: gapSchema,
      rows: closes.map((c, i) => [
        i * MINUTE,
        c === undefined ? undefined : c + 1,
        c === undefined ? undefined : c - 1,
        c,
      ]) as never,
    });
  const holed = gappy([10, 12, undefined, 20, 22, 24, 26, 28]);
  const clean = gappy([10, 12, 14, 20, 22, 24, 26, 28]);

  it('priceOscillator warms up on the SLOW average and inherits its maType’s gap rule', () => {
    // `sma` on the column door keeps `sma()`'s row-counting window, so the
    // gap costs it nothing: the warm-up is the only missing run, and it is
    // the SLOW leg's (2 rows at period 3), not the fast one's.
    const smaOut = priceOscillator(holed, {
      fastPeriod: 2,
      slowPeriod: 3,
      maType: 'sma',
      mode: 'absolute',
    });
    expect(
      cells(smaOut, 'priceOsc')
        .slice(0, 2)
        .every((x) => x === undefined),
    ).toBe(true);
    expect(nullCountOf(smaOut, 'priceOsc')).toBe(2);

    // `ema` waits for `period` finite samples, so the gap pushes the slow
    // leg's first value one bar later — 3 missing rows, not 2, and the gap
    // bar itself has no value.
    const emaOut = priceOscillator(holed, {
      fastPeriod: 2,
      slowPeriod: 3,
      maType: 'ema',
      mode: 'absolute',
    });
    expect(nullCountOf(emaOut, 'priceOsc')).toBe(3);
    expect(cells(emaOut, 'priceOsc')[2]).toBeUndefined();
    expect(typeof cells(emaOut, 'priceOsc')[3]).toBe('number');
    // Without the hole the same call starts a bar earlier — the gap, not the
    // period, is what moved it.
    expect(
      nullCountOf(
        priceOscillator(clean, {
          fastPeriod: 2,
          slowPeriod: 3,
          maType: 'ema',
          mode: 'absolute',
        }),
        'priceOsc',
      ),
    ).toBe(2);

    // `wma` masks every window containing the gap (a positional weight
    // cannot skip a cell), so bars 2–4 go too: 5 missing rows.
    expect(
      nullCountOf(
        priceOscillator(holed, {
          fastPeriod: 2,
          slowPeriod: 3,
          maType: 'wma',
          mode: 'absolute',
        }),
        'priceOsc',
      ),
    ).toBe(5);
  });

  it('disparityIndex loses the gap bar itself even when the average survives it', () => {
    // `sma` averages around the hole, but the NUMERATOR reads the price on
    // that bar, so the reading is missing there regardless of the maType.
    const out = disparityIndex(holed, { period: 3, maType: 'sma' });
    const v = cells(out, 'disparity');
    expect(v.slice(0, 2).every((x) => x === undefined)).toBe(true);
    expect(v[2]).toBeUndefined();
    expect(typeof v[3]).toBe('number');
    expect(nullCountOf(out, 'disparity')).toBe(3);
  });

  it('detrendedPriceOscillator warms up over period − 1 + shift rows', () => {
    // period 3 → shift 2, so the first value lands on bar 4 even with no
    // gap; the sma leg then averages around the hole and nothing else is
    // lost.
    expect(
      nullCountOf(detrendedPriceOscillator(clean, { period: 3 }), 'dpo'),
    ).toBe(4);
    const out = detrendedPriceOscillator(holed, { period: 3 });
    expect(
      cells(out, 'dpo')
        .slice(0, 4)
        .every((x) => x === undefined),
    ).toBe(true);
    expect(typeof cells(out, 'dpo')[4]).toBe('number');
    expect(nullCountOf(out, 'dpo')).toBe(4);

    // With `wma` the masked windows shift the average's first value from bar
    // 2 to bar 5, and the displacement carries that to bar 7.
    const wmaOut = detrendedPriceOscillator(holed, {
      period: 3,
      maType: 'wma',
    });
    expect(nullCountOf(wmaOut, 'dpo')).toBe(7);
    expect(typeof cells(wmaOut, 'dpo')[7]).toBe('number');
  });

  it('elderRay loses only the gap bar, then the EMA carries on', () => {
    const out = elderRay(holed, { period: 2 });
    for (const name of ['elderBull', 'elderBear']) {
      const v = cells(out, name);
      expect(v[0], name).toBeUndefined(); // the EMA's own warm-up row
      expect(typeof v[1], name).toBe('number');
      expect(v[2], name).toBeUndefined(); // the gap bar
      expect(typeof v[3], name).toBe('number'); // the recursion skipped it
      expect(nullCountOf(out, name), name).toBe(2);
    }
  });

  it('awesomeOscillator masks every window containing the gap', () => {
    // The median price is a DERIVED array, so both legs wait for that many
    // finite VALUES: the slow leg's windows over bars 2, 3 and 4 all contain
    // the hole, and the first value lands on bar 5 instead of bar 2.
    const out = awesomeOscillator(holed, { fastPeriod: 2, slowPeriod: 3 });
    const v = cells(out, 'ao');
    expect(v.slice(0, 5).every((x) => x === undefined)).toBe(true);
    expect(typeof v[5]).toBe('number');
    expect(nullCountOf(out, 'ao')).toBe(5);
    // Without the hole the same call starts on bar 2.
    expect(
      nullCountOf(
        awesomeOscillator(clean, { fastPeriod: 2, slowPeriod: 3 }),
        'ao',
      ),
    ).toBe(2);
  });

  it('the percent forms report missing, not Infinity, on a zero average', () => {
    // A flat ZERO series: the denominators of both percent forms are 0 on
    // every bar they would otherwise emit, so the columns are entirely
    // missing rather than carrying ±Infinity into a chart's y-domain.
    const zeros = gappy([0, 0, 0, 0, 0, 0]);
    const osc = priceOscillator(zeros, { fastPeriod: 2, slowPeriod: 3 });
    expect(nullCountOf(osc, 'priceOsc')).toBe(6);
    const disp = disparityIndex(zeros, { period: 3 });
    expect(nullCountOf(disp, 'disparity')).toBe(6);
    // The absolute form has no denominator: it emits 0 from bar 2 on.
    const abs = priceOscillator(zeros, {
      fastPeriod: 2,
      slowPeriod: 3,
      mode: 'absolute',
      maType: 'sma',
      output: 'abs',
    });
    expect(nullCountOf(abs, 'abs')).toBe(2);
    expect(cells(abs, 'abs')[5]).toBe(0);
  });
});

describe('[PND-STUDYBOX] the volume & money-flow studies: where the missing rows are', () => {
  // No `!isNaN` assertions — `withColumn` maps NaN to missing on its typed
  // door, so such a check can never fire. What is pinned is WHERE the missing
  // rows are, and the split that matters for this group: the two CUMULATIVE
  // studies (A/D, PVT) stop at a gap forever, while the WINDOW ones (CMF,
  // MFI, EOM, volume oscillator) and the EMA one (force index) recover.
  const flowSchema = [
    { name: 'time', kind: 'time' },
    { name: 'high', kind: 'number', required: false },
    { name: 'low', kind: 'number', required: false },
    { name: 'close', kind: 'number', required: false },
    { name: 'volume', kind: 'number', required: false },
  ] as const;
  // The close must sit OFF the midpoint of its bar and at a VARYING place,
  // or the close location value is a constant (zero, at the midpoint) and
  // every A/D and CMF assertion below would hold for a study that read the
  // range and nothing else.
  const hiOf = (c: number, i: number) => c + 0.6 + 0.4 * (i % 3);
  const loOf = (c: number, i: number) => c - 0.9 - 0.2 * (i % 2);
  /** Bars from a close and a volume, either of which may be missing. `flat`
   *  names a bar to give a zero range (h === l === c). */
  const flowBars = (
    closes: Array<number | undefined>,
    volumes: Array<number | undefined>,
    flat = -1,
  ) =>
    new TimeSeries({
      name: 'bars',
      schema: flowSchema,
      rows: closes.map((c, i) => [
        i * MINUTE,
        c === undefined ? undefined : i === flat ? c : hiOf(c, i),
        c === undefined ? undefined : i === flat ? c : loOf(c, i),
        c,
        volumes[i],
      ]) as never,
    });
  const closes = [10, 12, 11, 13, 12, 14, 15, 16];
  const volumes = [100, 200, 300, 400, 500, 600, 700, 800];
  const clean = flowBars(closes, volumes);
  /** The gap is on bar 2 — early enough that a cumulative study loses the
   *  rest of the series. */
  const holed = flowBars([10, 12, undefined, 13, 12, 14, 15, 16], volumes);
  /** The same gap at bar 5, past every warm-up, so a study that RECOVERS
   *  shows a hole rather than a longer head. */
  const holedLate = flowBars([10, 12, 11, 13, 12, undefined, 15, 16], volumes);

  it('accumulationDistribution: no warm-up, and a gap ends the line', () => {
    expect(nullCountOf(accumulationDistribution(clean), 'ad')).toBe(0);
    const out = accumulationDistribution(holed);
    const v = cells(out, 'ad');
    expect(typeof v[1]).toBe('number');
    expect(v[2]).toBeUndefined();
    expect(nullCountOf(out, 'ad')).toBe(6); // bars 2..7 — never recovers
  });

  it('accumulationDistribution: a FLAT bar is not a gap — it adds 0, like TA-Lib', () => {
    const out = accumulationDistribution(flowBars(closes, volumes, 3));
    const ad = cells(out, 'ad');
    expect(ad[3]).toBe(ad[2]); // the flat bar's contribution is exactly 0
    expect(nullCountOf(out, 'ad')).toBe(0);
  });

  it('chaikinOscillator: the slow EMA’s warm-up, then the A/D line’s gap rule', () => {
    const out = chaikinOscillator(clean, { fastPeriod: 2, slowPeriod: 3 });
    expect(nullCountOf(out, 'chaikinOsc')).toBe(2); // slowPeriod − 1
    // The A/D line dies at bar 2, so every average of it dies with it.
    const gapped = chaikinOscillator(holed, { fastPeriod: 2, slowPeriod: 3 });
    expect(nullCountOf(gapped, 'chaikinOsc')).toBe(8);
  });

  it('priceVolumeTrend: bar 0 has no term, and a gap ends the line', () => {
    const out = priceVolumeTrend(clean);
    expect(cells(out, 'pvt')[0]).toBeUndefined();
    expect(nullCountOf(out, 'pvt')).toBe(1);
    const gapped = priceVolumeTrend(
      flowBars(closes, [100, 200, undefined, ...volumes.slice(3)]),
    );
    expect(typeof cells(gapped, 'pvt')[1]).toBe('number');
    expect(nullCountOf(gapped, 'pvt')).toBe(7); // bar 0 + bars 2..7
  });

  it('chaikinMoneyFlow: a `period − 1` head, and a gap changes windows without blanking them', () => {
    const out = chaikinMoneyFlow(clean, { period: 3 });
    expect(nullCountOf(out, 'cmf')).toBe(2);
    // A count window is emitted once it SPANS `period` rows and is computed
    // from whichever are present, so the gap costs no whole bar here — the
    // gap bar simply leaves both sums. What must not happen is a run of
    // missing rows, and the numbers must actually move.
    const gapped = chaikinMoneyFlow(holed, { period: 3 });
    expect(nullCountOf(gapped, 'cmf')).toBe(2);
    expect(cells(gapped, 'cmf')[2]).not.toBe(cells(out, 'cmf')[2]);
    // Bar 5's window is bars 3–5, past the gap: the same answer again.
    expect(cells(gapped, 'cmf')[5]).toBeCloseTo(
      cells(out, 'cmf')[5] as number,
      10,
    );
  });

  it('chaikinMoneyFlow: a window with no volume at all is missing', () => {
    const dead = flowBars(closes, [0, 0, 0, ...volumes.slice(3)]);
    const out = chaikinMoneyFlow(dead, { period: 3 });
    expect(cells(out, 'cmf')[2]).toBeUndefined(); // Σ volume = 0
    expect(typeof cells(out, 'cmf')[3]).toBe('number');
    expect(nullCountOf(out, 'cmf')).toBe(3); // two warm-up + the dead window
  });

  it('moneyFlowIndex: a `period` head (not `period − 1`), gap costs two bars’ windows', () => {
    const out = moneyFlowIndex(clean, { period: 3 });
    expect(nullCountOf(out, 'mfi')).toBe(3);
    expect(typeof cells(out, 'mfi')[3]).toBe('number');
    // The gap bar has no typical price, and neither has the DIRECTION of the
    // bar after it, so every window holding either is blank: bars 5, 6, 7.
    const gapped = moneyFlowIndex(holedLate, { period: 3 });
    const v = cells(gapped, 'mfi');
    expect(typeof v[4]).toBe('number');
    expect(v.slice(5).every((x) => x === undefined)).toBe(true);
    expect(nullCountOf(gapped, 'mfi')).toBe(6); // 3 warm-up + bars 5..7
  });

  it('moneyFlowIndex: a window with no flow in either direction is missing', () => {
    // A CONSTANT typical price: the ranges have to be constant too, or the
    // bars' typical prices move even though their closes do not.
    const flat = new TimeSeries({
      name: 'bars',
      schema: flowSchema,
      rows: closes.map((_, i) => [i * MINUTE, 11, 9, 10, volumes[i]]) as never,
    });
    expect(nullCountOf(moneyFlowIndex(flat, { period: 3 }), 'mfi')).toBe(8);
  });

  it('forceIndex: a `period` head, and a gap costs two bars before the EMA resumes', () => {
    const out = forceIndex(clean, { period: 2 });
    expect(nullCountOf(out, 'force')).toBe(2); // bars 0 and 1
    const gapped = forceIndex(holedLate, { period: 2 });
    const v = cells(gapped, 'force');
    expect(typeof v[4]).toBe('number');
    expect(v[5]).toBeUndefined(); // the gap bar
    expect(v[6]).toBeUndefined(); // its change reads the missing close
    expect(typeof v[7]).toBe('number'); // the recursion skipped, not stopped
    expect(nullCountOf(gapped, 'force')).toBe(4);
  });

  it('easeOfMovement: a `period` head; a flat bar and a zero-volume bar blank their windows', () => {
    const out = easeOfMovement(clean, { period: 2, scale: 1 });
    expect(nullCountOf(out, 'eom')).toBe(2); // bars 0 and 1
    // Bar 3 flat: its own 1-bar value is missing, and the sma windows
    // holding it (bars 3 and 4) go with it — then it recovers.
    const flatOut = easeOfMovement(flowBars(closes, volumes, 3), {
      period: 2,
      scale: 1,
    });
    expect(cells(flatOut, 'eom')[3]).toBeUndefined();
    expect(cells(flatOut, 'eom')[4]).toBeUndefined();
    expect(typeof cells(flatOut, 'eom')[5]).toBe('number');

    const dead = flowBars(closes, [100, 200, 300, 0, ...volumes.slice(4)]);
    const deadOut = easeOfMovement(dead, { period: 2, scale: 1 });
    expect(cells(deadOut, 'eom')[3]).toBeUndefined(); // not ±Infinity
    expect(typeof cells(deadOut, 'eom')[5]).toBe('number');
  });

  it('volumeOscillator: the slow average’s warm-up, and no value on a dead window', () => {
    const out = volumeOscillator(clean, { fastPeriod: 2, slowPeriod: 3 });
    expect(nullCountOf(out, 'volOsc')).toBe(2);
    const dead = flowBars(
      closes,
      closes.map(() => 0),
    );
    expect(
      nullCountOf(
        volumeOscillator(dead, { fastPeriod: 2, slowPeriod: 3 }),
        'volOsc',
      ),
    ).toBe(8); // 0/0 on every bar — missing, not Infinity
  });
});

describe('[PND-STUDYBOX] the momentum tail: where the missing rows are', () => {
  // No `!isNaN` assertions — `withColumn` maps NaN to missing on its typed
  // door, so such a check can never fire. What is worth pinning is WHERE the
  // missing rows are, and every one of these six is a WINDOW study, so the
  // shared claim is: a gap costs the windows that contain it and the study
  // then recovers. (Contrast `rsi`, whose Wilder recursion carries an
  // interior gap to the end of the series — pinned above.)
  const barSchema = [
    { name: 'time', kind: 'time' },
    { name: 'open', kind: 'number', required: false },
    { name: 'high', kind: 'number', required: false },
    { name: 'low', kind: 'number', required: false },
    { name: 'close', kind: 'number', required: false },
  ] as const;

  /** Bars built around each close; a missing close makes the whole bar
   *  missing, which is what a dropped tick actually looks like. */
  const momBars = (closes: Array<number | undefined>) =>
    new TimeSeries({
      name: 'bars',
      schema: barSchema,
      rows: closes.map((c, i) => [
        i * MINUTE,
        c === undefined ? undefined : c - 0.5,
        c === undefined ? undefined : c + 1,
        c === undefined ? undefined : c - 1,
        c,
      ]) as never,
    });

  const cleanCloses = [10, 12, 14, 20, 22, 24, 26, 28, 27, 29, 31, 30];
  const holedCloses = [...cleanCloses];
  holedCloses[2] = undefined as never;
  const clean = momBars(cleanCloses);
  const holed = momBars(holedCloses);

  it('chandeMomentum loses the gap bar, the bar after it, and their windows', () => {
    // A missing close costs TWO changes (its own and the next bar's), and at
    // period 2 the windows containing either run to bar 4 — so the first
    // reading moves from bar 2 to bar 5.
    expect(nullCountOf(chandeMomentum(clean, { period: 2 }), 'cmo')).toBe(2);
    const out = chandeMomentum(holed, { period: 2 });
    expect(
      cells(out, 'cmo')
        .slice(0, 5)
        .every((x) => x === undefined),
    ).toBe(true);
    expect(typeof cells(out, 'cmo')[5]).toBe('number');
    expect(nullCountOf(out, 'cmo')).toBe(5);
  });

  it('psychologicalLine loses exactly the same rows as chandeMomentum', () => {
    // Same input (a direction per bar), same array door — the two differ only
    // in what they do with the legs, so their masks must agree.
    expect(nullCountOf(psychologicalLine(clean, { period: 2 }), 'psy')).toBe(2);
    const out = psychologicalLine(holed, { period: 2 });
    expect(nullCountOf(out, 'psy')).toBe(5);
    expect(typeof cells(out, 'psy')[5]).toBe('number');
  });

  it('intradayMomentumIndex loses only the windows over the gap BAR', () => {
    // The body reads no previous bar, so the missing close costs one row of
    // input, not two: bars 2 and 3's windows go, and bar 4 is back.
    expect(
      nullCountOf(intradayMomentumIndex(clean, { period: 2 }), 'imi'),
    ).toBe(1);
    const out = intradayMomentumIndex(holed, { period: 2 });
    const v = cells(out, 'imi');
    expect(typeof v[1]).toBe('number');
    expect(v[2]).toBeUndefined();
    expect(v[3]).toBeUndefined();
    expect(typeof v[4]).toBe('number');
    expect(nullCountOf(out, 'imi')).toBe(3);
  });

  it('commodityChannelIndex blanks every window containing the gap', () => {
    expect(
      nullCountOf(commodityChannelIndex(clean, { period: 3 }), 'cci'),
    ).toBe(2);
    const out = commodityChannelIndex(holed, { period: 3 });
    expect(
      cells(out, 'cci')
        .slice(0, 5)
        .every((x) => x === undefined),
    ).toBe(true);
    expect(typeof cells(out, 'cci')[5]).toBe('number');
    expect(nullCountOf(out, 'cci')).toBe(5);
  });

  it('ultimateOscillator loses two bars of BP/TR and the windows over them', () => {
    // Both legs read the PREVIOUS close, so the gap bar and the one after it
    // have neither; at longPeriod 3 that reaches bar 5.
    const opts = { shortPeriod: 1, mediumPeriod: 2, longPeriod: 3 } as const;
    expect(nullCountOf(ultimateOscillator(clean, opts), 'uo')).toBe(3);
    const out = ultimateOscillator(holed, opts);
    expect(
      cells(out, 'uo')
        .slice(0, 6)
        .every((x) => x === undefined),
    ).toBe(true);
    expect(typeof cells(out, 'uo')[6]).toBe('number');
    expect(nullCountOf(out, 'uo')).toBe(6);
  });

  it('relativeVigorIndex pays the SWMA’s width as well as the window’s', () => {
    // A positional weight cannot skip a cell, so the gap blanks four bars of
    // each leg (the `wma` rule); the summation then blanks the windows over
    // those, and the signal is a fourth SWMA on top.
    const r = relativeVigorIndex(clean, { period: 2 });
    expect(nullCountOf(r, 'rvi')).toBe(4);
    expect(nullCountOf(r, 'rviSignal')).toBe(7);
    const out = relativeVigorIndex(holed, { period: 2 });
    expect(nullCountOf(out, 'rvi')).toBe(7);
    expect(typeof cells(out, 'rvi')[7]).toBe('number');
    expect(nullCountOf(out, 'rviSignal')).toBe(10);
    expect(typeof cells(out, 'rviSignal')[10]).toBe('number');
  });

  it('the zero-denominator guards report missing, not ±Infinity', () => {
    // Bars whose high and low both sit on the PREVIOUS close: true range is
    // then identically 0, while buying pressure is not on the bar the close
    // jumps. That combination is only reachable on inconsistent bars (a
    // close outside its own range — which is what a redirected `close`
    // option produces), and it is the only way to divide a non-zero
    // numerator by zero here, so it is what the guards are for.
    const jumps = [10, 10, 10, 10, 20, 20, 20, 20];
    const noRange = new TimeSeries({
      name: 'bars',
      schema: barSchema,
      rows: jumps.map((c, i) => [
        i * MINUTE,
        c - 2,
        jumps[i - 1] ?? c,
        jumps[i - 1] ?? c,
        c,
      ]) as never,
    });
    expect(
      nullCountOf(
        ultimateOscillator(noRange, {
          shortPeriod: 1,
          mediumPeriod: 2,
          longPeriod: 3,
        }),
        'uo',
      ),
    ).toBe(8);
    const r = relativeVigorIndex(noRange, { period: 2 });
    expect(nullCountOf(r, 'rvi')).toBe(8);
    expect(nullCountOf(r, 'rviSignal')).toBe(8);
    // CCI's guard is the flat-window one: a constant typical price has no
    // mean absolute deviation to divide by.
    const flat = momBars([12, 12, 12, 12, 12, 12]);
    expect(nullCountOf(commodityChannelIndex(flat, { period: 3 }), 'cci')).toBe(
      6,
    );
  });
});

describe('[PND-STUDYBOX] the directional group: which input kills which column', () => {
  // The group's three studies split on the WINDOW-vs-RECURSION line, and this
  // is where that shows: `aroon` and `vortex` lose a bounded run of rows and
  // recover, `directionalMovement` stacks two Wilder smooths and carries an
  // interior gap to the end of the series. And because each study reads three
  // (or two) inputs that enter at different points, WHICH column dies depends
  // on WHICH input has the hole — the thing no single "it goes missing" count
  // would pin.
  const hlcSchema = [
    { name: 'time', kind: 'time' },
    { name: 'high', kind: 'number', required: false },
    { name: 'low', kind: 'number', required: false },
    { name: 'close', kind: 'number', required: false },
  ] as const;

  /** Twelve non-degenerate bars, optionally holing one input at bar 4. */
  const dirBars = (holeIn?: 'high' | 'low' | 'close') =>
    new TimeSeries({
      name: 'bars',
      schema: hlcSchema,
      rows: Array.from({ length: 12 }, (_, i) => {
        const c = 100 + 6 * Math.sin(i / 2.2) + 0.4 * i;
        const h = c + 0.5 + 0.7 * Math.abs(Math.sin(i / 1.7));
        const l = c - 0.5 - 0.7 * Math.abs(Math.cos(i / 1.3));
        return [
          i * MINUTE,
          holeIn === 'high' && i === 4 ? undefined : h,
          holeIn === 'low' && i === 4 ? undefined : l,
          holeIn === 'close' && i === 4 ? undefined : c,
        ];
      }) as never,
    });

  /** The first index at or after `from` with no value. */
  const firstMissingFrom = (s: unknown, name: string, from: number) =>
    cells(s, name).findIndex((x, i) => i >= from && x === undefined);

  it('directionalMovement carries a gap in high or low to the end of the series', () => {
    for (const holeIn of ['high', 'low'] as const) {
      const out = directionalMovement(dirBars(holeIn), { period: 2 });
      for (const name of [
        'dmiPlusDi',
        'dmiMinusDi',
        'dmiDx',
        'dmiAdx',
        'dmiAdxr',
      ]) {
        const v = cells(out, name);
        // The DM split needs both bars, so the hole lands on bar 4 itself…
        expect(firstMissingFrom(out, name, 4), `${holeIn}/${name}`).toBe(4);
        // …and a recursion has no state to carry across it.
        expect(
          v.slice(4).every((x) => x === undefined),
          `${holeIn}/${name}`,
        ).toBe(true);
      }
      // Everything before the hole is intact — the gap costs the tail, not
      // the whole column.
      expect(
        cells(directionalMovement(dirBars(holeIn), { period: 2 }), 'dmiDx')[3],
      ).toBeDefined();
    }
  });

  it('directionalMovement loses a gap in CLOSE one bar later — the true range reads prevClose', () => {
    const out = directionalMovement(dirBars('close'), { period: 2 });
    for (const name of ['dmiPlusDi', 'dmiMinusDi', 'dmiDx']) {
      expect(cells(out, name)[4], name).toBeDefined();
      expect(firstMissingFrom(out, name, 4), name).toBe(5);
      expect(
        cells(out, name)
          .slice(5)
          .every((x) => x === undefined),
        name,
      ).toBe(true);
    }
    // ADX's own smooth is over DX, so it dies with it; ADXR reads ADX two
    // bars apart and so keeps the one bar where both ends exist.
    expect(cells(out, 'dmiAdx')[4]).toBeDefined();
    expect(firstMissingFrom(out, 'dmiAdx', 4)).toBe(5);
    expect(cells(out, 'dmiAdxr')[4]).toBeDefined();
  });

  it('aroon loses period + 1 bars of the leg whose input is holed, then recovers', () => {
    const holedHigh = aroon(dirBars('high'), { period: 2 });
    expect(nullCountOf(aroon(dirBars(), { period: 2 }), 'aroonUp')).toBe(2);
    // The window is period + 1 bars, so bars 4, 5 and 6 all contain the hole.
    expect(
      cells(holedHigh, 'aroonUp')
        .slice(4, 7)
        .every((x) => x === undefined),
    ).toBe(true);
    expect(cells(holedHigh, 'aroonUp')[7]).toBeDefined();
    // The low leg never reads the high, so it is untouched — and the
    // oscillator, which needs both, follows the dead one.
    expect(nullCountOf(holedHigh, 'aroonDown')).toBe(2);
    expect(nullCountOf(holedHigh, 'aroonUp')).toBe(5);
    expect(nullCountOf(holedHigh, 'aroonOsc')).toBe(5);
    // A missing close is invisible to Aroon: it reads high and low only.
    expect(nullCountOf(aroon(dirBars('close'), { period: 2 }), 'aroonUp')).toBe(
      2,
    );
  });

  it('vortex loses the windows over the gap — and the two legs lose DIFFERENT ones', () => {
    expect(nullCountOf(vortex(dirBars(), { period: 2 }), 'viPlus')).toBe(2);
    const out = vortex(dirBars('high'), { period: 2 });
    // +VM reads its own high (bar 4) and the true range dies with it, so the
    // windows at 4 and 5 go. −VM reads the PREVIOUS high, so bar 5's leg is
    // gone too and its windows reach bar 6.
    expect(
      cells(out, 'viPlus')
        .slice(4, 6)
        .every((x) => x === undefined),
    ).toBe(true);
    expect(cells(out, 'viPlus')[6]).toBeDefined();
    expect(
      cells(out, 'viMinus')
        .slice(4, 7)
        .every((x) => x === undefined),
    ).toBe(true);
    expect(cells(out, 'viMinus')[7]).toBeDefined();
    // A missing close costs only the true range, one bar later, and both legs
    // then lose the same rows.
    const holedClose = vortex(dirBars('close'), { period: 2 });
    expect(cells(holedClose, 'viPlus')[4]).toBeDefined();
    expect(
      cells(holedClose, 'viPlus')
        .slice(5, 7)
        .every((x) => x === undefined),
    ).toBe(true);
    expect(cells(holedClose, 'viPlus')[7]).toBeDefined();
    expect(nullCountOf(holedClose, 'viMinus')).toBe(
      nullCountOf(holedClose, 'viPlus'),
    );
  });
});

describe('[PND-STUDYBOX] the volatility tail: where the missing rows are', () => {
  // No `!isNaN` assertions — `withColumn` maps NaN to missing on its typed
  // door, so such a check can never fire. What is pinned is WHERE the missing
  // rows are, and this batch is not uniform about it: six of the seven are
  // windows and recover after a gap, `relativeVolatilityIndex` is a Wilder
  // recursion and carries one to the end, and `gopalakrishnanRangeIndex`
  // reads only core's rolling extremes, which SKIP a gap rather than blanking
  // the windows over it. Those three behaviours are the point of this block.
  const volSchema = [
    { name: 'time', kind: 'time' },
    { name: 'high', kind: 'number', required: false },
    { name: 'low', kind: 'number', required: false },
    { name: 'close', kind: 'number', required: false },
  ] as const;

  /** Bars built around each close; a missing close makes the whole bar
   *  missing, which is what a dropped tick actually looks like. */
  const volBars = (closes: Array<number | undefined>) =>
    new TimeSeries({
      name: 'bars',
      schema: volSchema,
      rows: closes.map((c, i) => [
        i * MINUTE,
        c === undefined ? undefined : c + 1,
        c === undefined ? undefined : c - 1,
        c,
      ]) as never,
    });

  const volCloses = [10, 12, 14, 20, 22, 24, 26, 28, 27, 29, 31, 30];
  const holedCloses = [...volCloses];
  holedCloses[4] = undefined as never;
  const clean = volBars(volCloses);
  const holed = volBars(holedCloses);

  it('chaikinVolatility loses the gap bar and the bar `rocPeriod` later', () => {
    // The EMA family SKIPS a gap rather than blanking every window over it, so
    // the only rows lost past the warm-up are the gap bar itself and the one
    // whose rate of change reads it as a predecessor.
    expect(
      nullCountOf(
        chaikinVolatility(clean, { period: 2, rocPeriod: 2 }),
        'chaikinVol',
      ),
    ).toBe(3); // period − 1 + rocPeriod
    const out = chaikinVolatility(holed, { period: 2, rocPeriod: 2 });
    const v = cells(out, 'chaikinVol');
    expect(typeof v[3]).toBe('number');
    expect(v[4]).toBeUndefined(); // the gap bar: no range
    expect(typeof v[5]).toBe('number'); // the EMA skipped it and carried on
    expect(v[6]).toBeUndefined(); // reads bar 4 as its base
    expect(typeof v[7]).toBe('number');
    expect(nullCountOf(out, 'chaikinVol')).toBe(5);
  });

  it('massIndex blanks the summation windows over the gap, then recovers', () => {
    // Both EMA stages skip the gap bar, but the SUM is a window kernel and
    // blanks every window holding the missing ratio.
    expect(
      nullCountOf(massIndex(clean, { emaPeriod: 2, sumPeriod: 2 }), 'mass'),
    ).toBe(3); // 2·emaPeriod + sumPeriod − 3
    const out = massIndex(holed, { emaPeriod: 2, sumPeriod: 2 });
    const v = cells(out, 'mass');
    expect(typeof v[3]).toBe('number');
    expect(v[4]).toBeUndefined();
    expect(v[5]).toBeUndefined(); // its 2-bar sum still holds bar 4
    expect(typeof v[6]).toBe('number');
    expect(nullCountOf(out, 'mass')).toBe(5);
  });

  it('choppinessIndex loses two bars of true range and the windows over them', () => {
    // True range reads the PREVIOUS close, so a missing bar costs its own row
    // and the next one's; the ΣTR window then blanks over both. The HH/LL half
    // would have skipped the gap — the sum is what sets the mask.
    expect(nullCountOf(choppinessIndex(clean, { period: 3 }), 'chop')).toBe(3);
    const out = choppinessIndex(holed, { period: 3 });
    const v = cells(out, 'chop');
    expect(typeof v[3]).toBe('number');
    // Bars 4 and 5 have no true range, so every 3-bar sum holding either is
    // blank: bars 4, 5, 6 and 7. Bar 8 is the first with three ranges again.
    expect(v.slice(4, 8).every((x) => x === undefined)).toBe(true);
    expect(typeof v[8]).toBe('number');
    expect(nullCountOf(out, 'chop')).toBe(7);
  });

  it('ulcerIndex blanks the averaging windows over the gap, not the peak', () => {
    // Core's rolling `max` skips a missing cell, so the peak stays defined;
    // the drawdown on the gap bar does not, and the mean-of-squares window
    // blanks over it.
    expect(nullCountOf(ulcerIndex(clean, { period: 3 }), 'ulcer')).toBe(4); // 2·3 − 2
    const out = ulcerIndex(holed, { period: 3 });
    const v = cells(out, 'ulcer');
    expect(v[4]).toBeUndefined(); // the gap bar has no drawdown
    expect(v[5]).toBeUndefined(); // its 3-bar mean-of-squares still holds it
    expect(v[6]).toBeUndefined();
    expect(typeof v[7]).toBe('number');
    expect(nullCountOf(out, 'ulcer')).toBe(7);
  });

  it('verticalHorizontalFilter blanks the path windows over the gap', () => {
    // Two changes go missing (the gap bar's own and the next bar's), and every
    // path-length window holding one of them goes with them; the range half
    // skips, as in choppinessIndex.
    expect(
      nullCountOf(verticalHorizontalFilter(clean, { period: 3 }), 'vhf'),
    ).toBe(3); // `period` rows
    const out = verticalHorizontalFilter(holed, { period: 3 });
    const v = cells(out, 'vhf');
    expect(typeof v[3]).toBe('number');
    expect(v.slice(4, 8).every((x) => x === undefined)).toBe(true);
    expect(typeof v[8]).toBe('number');
    expect(nullCountOf(out, 'vhf')).toBe(7);
  });

  it('gopalakrishnanRangeIndex SKIPS the gap — the one study here that does', () => {
    // It reads only core's rolling `max`/`min`, whose policy is to take the
    // extreme over the cells the window does hold. So no row is lost at all
    // past the warm-up, where every other study in this batch blanks. The
    // contrast is the point: this study has no averaging half to set a
    // stricter mask.
    expect(
      nullCountOf(gopalakrishnanRangeIndex(clean, { period: 3 }), 'gapo'),
    ).toBe(2); // period − 1
    const out = gopalakrishnanRangeIndex(holed, { period: 3 });
    expect(nullCountOf(out, 'gapo')).toBe(2);
    expect(typeof cells(out, 'gapo')[4]).toBe('number');
  });

  it('relativeVolatilityIndex carries an interior gap to the END, like rsi', () => {
    // The Wilder recursion has no state to carry across a hole, so unlike the
    // six window studies above this one never recovers. That asymmetry is
    // stated on the study and pinned here.
    expect(
      nullCountOf(
        relativeVolatilityIndex(clean, { period: 2, stdevPeriod: 2 }),
        'relVol',
      ),
    ).toBe(2); // stdevPeriod + period − 2
    const out = relativeVolatilityIndex(holed, {
      period: 2,
      stdevPeriod: 2,
    });
    const v = cells(out, 'relVol');
    expect(typeof v[3]).toBe('number');
    expect(v.slice(4).every((x) => x === undefined)).toBe(true);
    expect(nullCountOf(out, 'relVol')).toBe(volCloses.length - 2);
  });

  it('a LEADING gap shifts the start rather than emptying relativeVolatilityIndex', () => {
    // The other half of the Wilder rule: the seed steps over a leading run,
    // which is what makes the study composable over another study's warm-up.
    const late = volBars([undefined, undefined, ...volCloses.slice(2)]);
    const v = cells(
      relativeVolatilityIndex(late, { period: 2, stdevPeriod: 2 }),
      'relVol',
    );
    expect(v.slice(0, 4).every((x) => x === undefined)).toBe(true);
    expect(typeof v[4]).toBe('number');
    expect(v.slice(4).every((x) => typeof x === 'number')).toBe(true);
  });

  it('the zero-denominator guards report missing, not ±Infinity', () => {
    // Each of these would otherwise send an infinity to `withColumn`, which
    // rejects it outright — so "the study returns at all" is the assertion.
    // A constant high and low with a moving close: the span is zero and the
    // true range is not (it reads the close).
    const noSpan = new TimeSeries({
      name: 'bars',
      schema: volSchema,
      rows: volCloses.map((c, i) => [i * MINUTE, 5, 5, c]) as never,
    });
    expect(nullCountOf(choppinessIndex(noSpan, { period: 3 }), 'chop')).toBe(
      volCloses.length,
    );
    expect(
      nullCountOf(gopalakrishnanRangeIndex(noSpan, { period: 3 }), 'gapo'),
    ).toBe(volCloses.length);
    // A column whose rolling peak is exactly zero with a non-zero value under
    // it — reachable over another study's output, not over prices.
    const crossesZero = volBars([0, -5, -5, -5, -5, -5]);
    expect(nullCountOf(ulcerIndex(crossesZero, { period: 2 }), 'ulcer')).toBe(
      3,
    );
  });
});

describe('[PND-STUDYBOX] the regression family: WHERE the missing rows are', () => {
  // A positional regressor cannot skip a cell — `x` names a position in the
  // window, so a gap would re-index every bar after it. All four studies
  // here therefore run the STRICT window (`wma`'s rule): a gap blanks the
  // gap bar and the `period − 1` bars after it, and then they recover. That
  // uniformity is the point of this block — contrast the volatility tail
  // above, where three studies answered a gap three different ways.
  const regSchema = [
    { name: 'time', kind: 'time' },
    { name: 'close', kind: 'number', required: false },
  ] as const;

  const regBars = (closes: Array<number | undefined>) =>
    new TimeSeries({
      name: 'bars',
      schema: regSchema,
      rows: closes.map((c, i) => [i * MINUTE, c]) as never,
    });

  const regCloses = [10, 12, 11, 15, 14, 18, 17, 21, 20, 24, 23, 27];
  const holedCloses = [...regCloses];
  holedCloses[5] = undefined as never;
  const clean = regBars(regCloses);
  const holed = regBars(holedCloses);

  it('linearRegression: all five columns warm up together at period − 1', () => {
    const out = linearRegression(clean, { period: 3 });
    for (const name of [
      'linregValue',
      'linregSlope',
      'linregIntercept',
      'linregAngle',
      'linregR2',
    ]) {
      const v = cells(out, name);
      expect(
        v.slice(0, 2).every((x) => x === undefined),
        name,
      ).toBe(true);
      expect(typeof v[2], name).toBe('number');
      expect(nullCountOf(out, name), name).toBe(2);
      expect(
        v.some((x) => typeof x === 'number' && Number.isNaN(x)),
        name,
      ).toBe(false);
    }
  });

  it('linearRegression: an interior gap blanks bars 5–7 and then recovers', () => {
    const out = linearRegression(holed, { period: 3 });
    for (const name of ['linregValue', 'linregSlope', 'linregR2']) {
      const v = cells(out, name);
      expect(typeof v[4], `${name} before the gap`).toBe('number');
      // The gap bar and the two windows that still contain it.
      expect(v[5], `${name} bar 5`).toBeUndefined();
      expect(v[6], `${name} bar 6`).toBeUndefined();
      expect(v[7], `${name} bar 7`).toBeUndefined();
      expect(typeof v[8], `${name} recovers at 8`).toBe('number');
      // 2 warm-up + 3 gap rows.
      expect(nullCountOf(out, name), name).toBe(5);
    }
  });

  it('timeSeriesForecast, cfo and cog lose exactly the same rows', () => {
    for (const [label, out, name] of [
      ['tsf', timeSeriesForecast(holed, { period: 3 }), 'tsf'],
      ['cfo', chandeForecastOscillator(holed, { period: 3 }), 'cfo'],
      ['cog', centerOfGravity(holed, { period: 3 }), 'cog'],
    ] as const) {
      const v = cells(out, name);
      expect(typeof v[4], `${label} before the gap`).toBe('number');
      expect(
        v.slice(5, 8).every((x) => x === undefined),
        label,
      ).toBe(true);
      expect(typeof v[8], `${label} recovers at 8`).toBe('number');
      expect(nullCountOf(out, name), label).toBe(5);
    }
  });

  it('a LEADING gap shifts the start rather than emptying the fit', () => {
    const late = regBars([undefined, undefined, ...regCloses.slice(2)]);
    for (const [label, out, name] of [
      ['linreg', linearRegression(late, { period: 3 }), 'linregSlope'],
      ['tsf', timeSeriesForecast(late, { period: 3 }), 'tsf'],
      ['cfo', chandeForecastOscillator(late, { period: 3 }), 'cfo'],
      ['cog', centerOfGravity(late, { period: 3 }), 'cog'],
    ] as const) {
      const v = cells(out, name);
      expect(
        v.slice(0, 4).every((x) => x === undefined),
        label,
      ).toBe(true);
      expect(typeof v[4], label).toBe('number');
      expect(
        v.slice(4).every((x) => typeof x === 'number'),
        label,
      ).toBe(true);
    }
  });

  it('the zero-denominator guards report missing, not ±Infinity', () => {
    // Both divisions sit at their study's OUTPUT, so without the guards an
    // Infinity would reach `withColumn`, which rejects it outright — "the
    // study returns at all" is half the assertion here.
    const crossesZero = regBars([3, 2, 1, 0, -1, -2]);
    const cfo = cells(
      chandeForecastOscillator(crossesZero, { period: 3 }),
      'cfo',
    );
    expect(cfo[3]).toBeUndefined(); // the zero price
    expect(typeof cfo[4]).toBe('number'); // a negative price still reads
    // A window summing to exactly zero with a non-zero weighted sum.
    const sumsToZero = regBars([5, 1, -1, 4]);
    const cog = cells(centerOfGravity(sumsToZero, { period: 2 }), 'cog');
    expect(cog[2]).toBeUndefined();
    expect(typeof cog[3]).toBe('number');
  });
});

describe('the two-series family: where a gap lands (corpus §6.7)', () => {
  const pairSchema = [
    { name: 'time', kind: 'time' },
    { name: 'close', kind: 'number', required: false },
    { name: 'bench', kind: 'number', required: false },
  ] as const;

  const pairBars = (
    closes: Array<number | undefined>,
    bench: Array<number | undefined>,
  ) =>
    new TimeSeries({
      name: 'bars',
      schema: pairSchema,
      rows: closes.map((c, i) => [i * MINUTE, c, bench[i]]) as never,
    });

  const pc = [100, 102, 101, 104, 103, 107, 105, 108, 110, 109, 112, 111];
  const pb = [50, 51, 50.5, 51.5, 52, 52.5, 53.5, 53, 54, 55, 54.5, 56];
  const hole = (values: number[], at: number): Array<number | undefined> => {
    const copy: Array<number | undefined> = [...values];
    copy[at] = undefined;
    return copy;
  };

  it('correlation: an interior gap in EITHER column blanks the same `period` rows', () => {
    const clean = correlation(pairBars(pc, pb), {
      benchmark: 'bench',
      period: 3,
    });
    expect(nullCountOf(clean, 'corr')).toBe(2); // warm-up only

    // The window is STRICT — it is a statement about pairs — so a hole in the
    // benchmark costs exactly what a hole in the source costs.
    for (const holed of [
      pairBars(hole(pc, 5), pb),
      pairBars(pc, hole(pb, 5)),
    ]) {
      const v = cells(
        correlation(holed, { benchmark: 'bench', period: 3 }),
        'corr',
      );
      expect(v[4]).toBeDefined();
      expect(v[5]).toBeUndefined();
      expect(v[6]).toBeUndefined();
      expect(v[7]).toBeUndefined();
      expect(v[8]).toBeDefined(); // recovers as soon as the hole leaves
      expect(
        nullCountOf(
          correlation(holed, { benchmark: 'bench', period: 3 }),
          'corr',
        ),
      ).toBe(5);
    }
  });

  it('correlation: a LEADING gap shifts the start rather than emptying the column', () => {
    const late = pairBars(
      [undefined, undefined, ...pc.slice(2)],
      [undefined, undefined, ...pb.slice(2)],
    );
    const v = cells(
      correlation(late, { benchmark: 'bench', period: 3 }),
      'corr',
    );
    expect(v.slice(0, 4).every((x) => x === undefined)).toBe(true);
    expect(v[4]).toBeDefined();
    expect(v.slice(4).every((x) => typeof x === 'number')).toBe(true);
  });

  it('beta: a gap costs TWO returns, so it blanks `period + 1` rows', () => {
    // The one asymmetry against `correlation`: a missing PRICE takes both the
    // return into it and the return out of it, so the hole is one bar wider.
    // (A ZERO price, by contrast, takes only the return out — pinned in
    // studies.test.ts.)
    const holed = pairBars(hole(pc, 5), pb);
    const v = cells(beta(holed, { benchmark: 'bench', period: 3 }), 'beta');
    expect(v[4]).toBeDefined();
    expect(v.slice(5, 9).every((x) => x === undefined)).toBe(true);
    expect(v[9]).toBeDefined();
  });

  it('priceRelative: a gap blanks its own row and nothing else', () => {
    const v = cells(
      priceRelative(pairBars(pc, hole(pb, 5)), { benchmark: 'bench' }),
      'priceRel',
    );
    expect(v[5]).toBeUndefined();
    expect(v.filter((x) => x === undefined)).toHaveLength(1);
    expect(v[0]).toBeDefined(); // no warm-up at all
  });

  it('performanceIndex: a gap blanks its own row and the row `period` later', () => {
    const v = cells(
      performanceIndex(pairBars(pc, hole(pb, 5)), {
        benchmark: 'bench',
        period: 3,
      }),
      'perf',
    );
    // 3 warm-up rows, the gap bar itself, and the bar 3 later that looks back
    // at it — and nothing else.
    expect(v.slice(0, 3).every((x) => x === undefined)).toBe(true);
    expect(v[5]).toBeUndefined();
    expect(v[8]).toBeUndefined();
    expect(v[4]).toBeDefined();
    expect(v[6]).toBeDefined();
    expect(v[9]).toBeDefined();
    expect(
      nullCountOf(
        performanceIndex(pairBars(pc, hole(pb, 5)), {
          benchmark: 'bench',
          period: 3,
        }),
        'perf',
      ),
    ).toBe(5);
  });

  it('an all-missing benchmark gives an all-missing column, never a throw', () => {
    const empty = pc.map(() => undefined);
    for (const [name, out] of [
      [
        'corr',
        correlation(pairBars(pc, empty), { benchmark: 'bench', period: 3 }),
      ],
      ['beta', beta(pairBars(pc, empty), { benchmark: 'bench', period: 3 })],
      ['priceRel', priceRelative(pairBars(pc, empty), { benchmark: 'bench' })],
      [
        'perf',
        performanceIndex(pairBars(pc, empty), {
          benchmark: 'bench',
          period: 3,
        }),
      ],
    ] as const) {
      expect(nullCountOf(out, name), name).toBe(pc.length);
    }
  });
});

/* -------------------------------------------------------------------------- */
/* [PND-SFOLD] — where a K6 machine's gaps land.                               */
/*                                                                             */
/* The kernel's rule is RESET: an incomplete row is `undefined`, and the next   */
/* complete row starts a fresh run. These pin WHERE that shows, per study —    */
/* which is not the same answer for all five, because a study that reads a     */
/* Wilder-smoothed input inherits Wilder's propagate-to-the-end rule through   */
/* the front door and never gets to re-seed.                                   */
/* -------------------------------------------------------------------------- */
describe('[PND-SFOLD] the K6 state machines', () => {
  const k6Schema = [
    { name: 'time', kind: 'time' },
    { name: 'high', kind: 'number', required: false },
    { name: 'low', kind: 'number', required: false },
    { name: 'close', kind: 'number', required: false },
    { name: 'volume', kind: 'number', required: false },
  ] as const;

  /** Ten wavy bars; `hole` names a bar and a column to blank. */
  const k6Bars = (hole?: {
    at: number;
    column: 'high' | 'low' | 'close' | 'volume';
  }) =>
    new TimeSeries({
      name: 'bars',
      schema: k6Schema,
      rows: Array.from({ length: 10 }, (_, i) => {
        const c = 100 + 6 * Math.sin(i / 1.7) + 0.4 * i;
        const row: Array<number | undefined> = [
          i * MINUTE,
          c + 0.5 + 0.4 * ((i % 3) + 1),
          c - 0.5 - 0.3 * ((i % 2) + 1),
          c,
          1000 + 90 * ((i * 7) % 5),
        ];
        if (hole !== undefined && hole.at === i) {
          row[{ high: 1, low: 2, close: 3, volume: 4 }[hole.column]] =
            undefined;
        }
        return row;
      }) as never,
    });

  it('parabolicSar: a hole costs its own bar AND the re-seed bar, then recovers', () => {
    const clean = cells(parabolicSar(k6Bars()), 'psar');
    expect(clean[0]).toBeUndefined(); // the seed needs a predecessor
    expect(clean.slice(1).every((x) => x !== undefined)).toBe(true);

    const holed = parabolicSar(k6Bars({ at: 5, column: 'high' }));
    for (const name of ['psar', 'psarTrend']) {
      const v = cells(holed, name);
      expect(v[5], name).toBeUndefined(); // the incomplete bar
      expect(v[6], name).toBeUndefined(); // run 1 again: no predecessor yet
      expect(v[7], name).toBeDefined(); // re-seeded and running
      expect(nullCountOf(holed, name), name).toBe(3); // bars 0, 5, 6 — nothing else
    }
  });

  it('parabolicSar: a hole in the LOW costs the same two bars as one in the high', () => {
    const byHigh = cells(
      parabolicSar(k6Bars({ at: 4, column: 'high' })),
      'psar',
    ).map((x) => x === undefined);
    const byLow = cells(
      parabolicSar(k6Bars({ at: 4, column: 'low' })),
      'psar',
    ).map((x) => x === undefined);
    expect(byLow).toEqual(byHigh);
    // …and the close is not read at all, so blanking it changes nothing.
    expect(
      cells(parabolicSar(k6Bars({ at: 4, column: 'close' })), 'psar'),
    ).toEqual(cells(parabolicSar(k6Bars()), 'psar'));
  });

  it('superTrend: the ATR propagates, so a hole ends the study rather than resetting it', () => {
    const out = superTrend(k6Bars({ at: 5, column: 'close' }), { period: 2 });
    for (const name of ['st', 'stTrend']) {
      const v = cells(out, name);
      expect(
        v.slice(0, 2).every((x) => x === undefined),
        name,
      ).toBe(true);
      expect(v[2], name).toBeDefined();
      expect(v[4], name).toBeDefined();
      // From the gap on: the true range is unknown on bars 5 and 6, and
      // Wilder's recursion has no state to carry across the hole.
      expect(
        v.slice(5).every((x) => x === undefined),
        name,
      ).toBe(true);
    }
  });

  it('atrTrailingStop: same — the gap rule it shows is the ATR’s, not the fold’s', () => {
    const out = atrTrailingStop(k6Bars({ at: 6, column: 'low' }), {
      period: 2,
    });
    const v = cells(out, 'ats');
    expect(v[5]).toBeDefined();
    expect(v.slice(6).every((x) => x === undefined)).toBe(true);
    expect(
      cells(out, 'atsTrend')
        .slice(6)
        .every((x) => x === undefined),
    ).toBe(true);
  });

  it('negativeVolumeIndex: a hole blanks its own bar and RE-BASES the index', () => {
    // The visible half of the reset rule: unlike `obv`, which propagates to
    // the end, the index restarts from `start` on the next complete bar —
    // its level is an arbitrary base, so re-basing loses only the base.
    const out = negativeVolumeIndex(k6Bars({ at: 4, column: 'volume' }), {
      start: 1000,
    });
    const v = cells(out, 'nvi');
    expect(v[0]).toBe(1000);
    expect(v[3]).toBeDefined();
    expect(v[4]).toBeUndefined();
    expect(v[5]).toBe(1000); // re-based, not continued
    expect(nullCountOf(out, 'nvi')).toBe(1);
  });

  it('positiveVolumeIndex: a hole in the PRICE column re-bases it the same way', () => {
    const v = cells(
      positiveVolumeIndex(k6Bars({ at: 6, column: 'close' })),
      'pvi',
    );
    expect(v[6]).toBeUndefined();
    expect(v[7]).toBe(1000);
  });

  it('klinger: the force re-seeds, and both EMAs step over the hole', () => {
    // The volume force is undefined on the incomplete bar AND on the bar
    // after it (the trend comparison needs a predecessor), so the slow EMA —
    // which waits for `slowPeriod` finite values — starts two bars later than
    // it would on clean input.
    const clean = klinger(k6Bars(), {
      fastPeriod: 2,
      slowPeriod: 3,
      signalPeriod: 2,
    });
    expect(cells(clean, 'kvo')[3]).toBeDefined();
    expect(
      cells(clean, 'kvo')
        .slice(0, 3)
        .every((x) => x === undefined),
    ).toBe(true);

    const holed = klinger(k6Bars({ at: 2, column: 'volume' }), {
      fastPeriod: 2,
      slowPeriod: 3,
      signalPeriod: 2,
    });
    const v = cells(holed, 'kvo');
    expect(v.slice(0, 5).every((x) => x === undefined)).toBe(true);
    expect(v[5]).toBeDefined();
    expect(cells(holed, 'kvoSignal')[5]).toBeUndefined();
    expect(cells(holed, 'kvoSignal')[6]).toBeDefined();
  });

  it('an all-missing input gives an all-missing column, never a throw', () => {
    const empty = new TimeSeries({
      name: 'bars',
      schema: k6Schema,
      rows: Array.from({ length: 8 }, (_, i) => [
        i * MINUTE,
        undefined,
        undefined,
        undefined,
        undefined,
      ]) as never,
    });
    for (const [name, out] of [
      ['psar', parabolicSar(empty)],
      ['st', superTrend(empty, { period: 2 })],
      ['ats', atrTrailingStop(empty, { period: 2 })],
      ['nvi', negativeVolumeIndex(empty)],
      ['pvi', positiveVolumeIndex(empty)],
      ['kvo', klinger(empty, { fastPeriod: 2, slowPeriod: 3 })],
    ] as const) {
      expect(nullCountOf(out, name), name).toBe(8);
    }
  });

  it('no K6 column ever leaks a NaN to a reader', () => {
    const holed = k6Bars({ at: 5, column: 'high' });
    const outs = [
      ['psar', parabolicSar(holed)],
      ['psarTrend', parabolicSar(holed)],
      ['st', superTrend(holed, { period: 2 })],
      ['ats', atrTrailingStop(holed, { period: 2 })],
      ['nvi', negativeVolumeIndex(holed)],
      ['kvo', klinger(holed, { fastPeriod: 2, slowPeriod: 3 })],
    ] as const;
    for (const [name, out] of outs) {
      expect(
        cells(out, name).some((x) => typeof x === 'number' && Number.isNaN(x)),
        name,
      ).toBe(false);
    }
  });
});

describe('[PND-STUDYBOX] the moving-average stacks: where the missing rows are', () => {
  const wavy = Array.from(
    { length: 70 },
    (_, i) => 100 + 6 * Math.sin(i / 2.5) + 0.3 * i,
  );

  const gappy = (gapAt: number | undefined) =>
    new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'close', kind: 'number', required: false },
      ] as const,
      rows: wavy.map((c, i) => [
        i * MINUTE,
        i === gapAt ? undefined : c,
      ]) as never,
    });

  it('guppy: each of the twelve warms up at its own period − 1', () => {
    const out = guppy(gappy(undefined));
    expect(nullCountOf(out, 'gmmaS3')).toBe(2);
    expect(nullCountOf(out, 'gmmaS15')).toBe(14);
    expect(nullCountOf(out, 'gmmaL30')).toBe(29);
    expect(nullCountOf(out, 'gmmaL60')).toBe(59);
    expect(typeof cells(out, 'gmmaL60')[59]).toBe('number');
  });

  it('guppy: a leading gap shifts every ema column by one bar', () => {
    const out = guppy(gappy(0));
    // The `ema` recursion steps over a leading gap rather than seeding on it,
    // so each column starts one bar later and the gap bar itself is missing.
    expect(nullCountOf(out, 'gmmaS3')).toBe(3);
    expect(nullCountOf(out, 'gmmaL30')).toBe(30);
    expect(cells(out, 'gmmaS3')[2]).toBeUndefined();
    expect(typeof cells(out, 'gmmaS3')[3]).toBe('number');
  });

  it('guppy: an interior gap costs that bar only — the ema skip', () => {
    const out = guppy(gappy(65));
    for (const name of ['gmmaS3', 'gmmaS15', 'gmmaL30']) {
      const v = cells(out, name);
      expect(v[64], name).toBeDefined();
      expect(v[65], name).toBeUndefined();
      expect(v[66], name).toBeDefined();
    }
    // gmmaL60 starts at bar 59, so it has values on both sides of the hole.
    expect(nullCountOf(out, 'gmmaL60')).toBe(59 + 1);
  });

  it('rainbow: stage k warms up at k · (period − 1), all ten kept', () => {
    const out = rainbow(gappy(undefined), { period: 2 });
    expect(nullCountOf(out, 'rainbow1')).toBe(1);
    expect(nullCountOf(out, 'rainbow5')).toBe(5);
    expect(nullCountOf(out, 'rainbow10')).toBe(10);
    expect(typeof cells(out, 'rainbow10')[10]).toBe('number');
  });

  it('rainbow: a leading gap shifts EVERY stage, `sma` included', () => {
    // Unlike guppy's column door, the array door counts finite VALUES, so
    // there is no `sma` exception here.
    const out = rainbow(gappy(0), { period: 2 });
    expect(nullCountOf(out, 'rainbow1')).toBe(2);
    expect(nullCountOf(out, 'rainbow10')).toBe(11);
  });

  it('rainbow: an interior gap costs one more bar per stage', () => {
    const out = rainbow(gappy(40), { period: 2 });
    // Stage 1 loses the gap bar and the one after (its window holds it);
    // each further stage carries the hole one bar further along.
    expect(cells(out, 'rainbow1')[39]).toBeDefined();
    expect(cells(out, 'rainbow1')[40]).toBeUndefined();
    expect(cells(out, 'rainbow1')[41]).toBeUndefined();
    expect(cells(out, 'rainbow1')[42]).toBeDefined();
    expect(nullCountOf(out, 'rainbow1')).toBe(1 + 2);
    expect(nullCountOf(out, 'rainbow10')).toBe(10 + 11);
  });

  it('rainbowOscillator: one head for all three columns, one hole for all three', () => {
    const clean = rainbowOscillator(gappy(undefined), {
      period: 2,
      lookback: 10,
    });
    for (const name of ['rbo', 'rboUpper', 'rboLower']) {
      expect(nullCountOf(clean, name), name).toBe(10);
    }
    // A missing close costs the stack (every window and stage carrying it)
    // but NOT the range, which skips a missing cell — core's reducer policy.
    const hole = rainbowOscillator(gappy(40), { period: 2, lookback: 10 });
    for (const name of ['rbo', 'rboUpper', 'rboLower']) {
      expect(cells(hole, name)[39], name).toBeDefined();
      expect(cells(hole, name)[40], name).toBeUndefined();
      expect(cells(hole, name)[51], name).toBeDefined();
      expect(nullCountOf(hole, name), name).toBe(10 + 11);
    }
  });

  it('kst: a 44-bar head on the line, signalPeriod − 1 more on the signal', () => {
    const out = kst(gappy(undefined));
    expect(nullCountOf(out, 'kst')).toBe(44);
    expect(nullCountOf(out, 'kstSignal')).toBe(52);
    expect(typeof cells(out, 'kst')[44]).toBe('number');
    expect(typeof cells(out, 'kstSignal')[52]).toBe('number');
  });

  it('kst: one gap blanks the bar, four look-backs, and their windows', () => {
    // A missing close costs its own four rates of change and the four that
    // read it as a base (bars +10, +15, +20, +30); each smoothing window
    // holding one of those is then blank too. Nothing propagates to the end
    // — there is no recursion anywhere in the study.
    const out = kst(gappy(20));
    const v = cells(out, 'kst');
    // term 3 (ROC 20, smoothed 10) blanks bars 40..49; term 4 (ROC 30,
    // smoothed 15) blanks 50..64. Bar 65 is the first clean one again.
    expect(v[49]).toBeUndefined();
    expect(v[60]).toBeUndefined();
    expect(typeof v[65]).toBe('number');
    expect(nullCountOf(out, 'kst')).toBe(65);
  });

  it('pmo: heads at 54 and 63; an interior gap costs two bars, not the tail', () => {
    const long = Array.from(
      { length: 90 },
      (_, i) => 100 + 6 * Math.sin(i / 2.5) + 0.3 * i,
    );
    const withGap = (gapAt: number | undefined) =>
      new TimeSeries({
        name: 'bars',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'close', kind: 'number', required: false },
        ] as const,
        rows: long.map((c, i) => [
          i * MINUTE,
          i === gapAt ? undefined : c,
        ]) as never,
      });

    const clean = priceMomentumOscillator(withGap(undefined));
    expect(nullCountOf(clean, 'pmo')).toBe(54);
    expect(nullCountOf(clean, 'pmoSignal')).toBe(63);

    // The ROC reads a predecessor, so a hole costs its own bar and the next;
    // both recursions then SKIP it and carry on (the `ema` rule), so the
    // column comes back rather than dying at the gap. The two stages also
    // each consume one fewer sample, which pushes the head one bar later.
    const hole = priceMomentumOscillator(withGap(70));
    const v = cells(hole, 'pmo');
    expect(typeof v[69]).toBe('number');
    expect(v[70]).toBeUndefined();
    expect(v[71]).toBeUndefined();
    expect(typeof v[72]).toBe('number');
    expect(nullCountOf(hole, 'pmo')).toBe(54 + 2);
  });

  it('stochasticRsi: heads at 29 and 31; an interior gap kills the tail', () => {
    const clean = stochasticRsi(gappy(undefined));
    expect(nullCountOf(clean, 'stochRsiK')).toBe(29);
    expect(nullCountOf(clean, 'stochRsiD')).toBe(31);

    // The underlying RSI is a Wilder recursion, so a hole makes its state
    // unknown forever — this study inherits that rather than recovering the
    // way a pure window study does.
    const hole = stochasticRsi(gappy(40));
    const v = cells(hole, 'stochRsiK');
    expect(typeof v[39]).toBe('number');
    expect(v.slice(40).every((x) => x === undefined)).toBe(true);
  });

  it('tsi: heads compose; an interior gap costs two bars, then recovers', () => {
    const clean = trueStrengthIndex(gappy(undefined), {
      longPeriod: 6,
      shortPeriod: 3,
      signalPeriod: 4,
    });
    expect(nullCountOf(clean, 'tsi')).toBe(8);
    expect(nullCountOf(clean, 'tsiSignal')).toBe(11);

    // The difference reads a predecessor, so a hole costs its own bar and
    // the next; the EMA family then SKIPS it (not Wilder, so nothing
    // propagates to the end) and each stage's head shifts by one.
    const hole = trueStrengthIndex(gappy(40), {
      longPeriod: 6,
      shortPeriod: 3,
      signalPeriod: 4,
    });
    const v = cells(hole, 'tsi');
    expect(typeof v[39]).toBe('number');
    expect(v[40]).toBeUndefined();
    expect(v[41]).toBeUndefined();
    expect(typeof v[42]).toBe('number');
    expect(nullCountOf(hole, 'tsi')).toBe(8 + 2);
  });

  it('maDev: the average\u2019s head, and a gap costs the bar plus its windows', () => {
    const clean = movingAverageDeviation(gappy(undefined), { period: 5 });
    expect(nullCountOf(clean, 'maDev')).toBe(4);

    // `sma` counts ROWS, so the window averages what it has and only the
    // gap bar itself loses its price — one missing cell, not five.
    const hole = movingAverageDeviation(gappy(30), { period: 5 });
    expect(cells(hole, 'maDev')[30]).toBeUndefined();
    expect(typeof cells(hole, 'maDev')[31]).toBe('number');
    expect(nullCountOf(hole, 'maDev')).toBe(4 + 1);

    // `ema` skips the missing bar in the recursion but still loses that
    // bar's own price, and its head shifts by one.
    const emaHole = movingAverageDeviation(gappy(30), {
      period: 5,
      maType: 'ema',
    });
    expect(cells(emaHole, 'maDev')[30]).toBeUndefined();
    expect(typeof cells(emaHole, 'maDev')[31]).toBe('number');
  });

  it('guppy: with `type: sma` the window recovers and the head does not shift', () => {
    // `sma` keeps `sma()`'s row-counting window (the column door's documented
    // asymmetry), so a LEADING gap does not move the first value at all.
    const out = guppy(gappy(0), { type: 'sma' });
    expect(nullCountOf(out, 'gmmaS3')).toBe(2);
    expect(typeof cells(out, 'gmmaS3')[2]).toBe('number');
  });
});

describe('[PND-STUDYBOX] the momentum and trend leftovers: where the missing rows are', () => {
  const leftoverSchema = [
    { name: 'time', kind: 'time' },
    { name: 'open', kind: 'number', required: false },
    { name: 'high', kind: 'number', required: false },
    { name: 'low', kind: 'number', required: false },
    { name: 'close', kind: 'number', required: false },
  ] as const;

  /** Sixty wavy OHLC bars; `hole` names a bar and a column to blank. */
  const leftoverBars = (hole?: {
    at: number;
    column: 'open' | 'high' | 'low' | 'close';
  }) =>
    new TimeSeries({
      name: 'bars',
      schema: leftoverSchema,
      rows: Array.from({ length: 60 }, (_, i) => {
        const c = 100 + 7 * Math.sin(i / 3.1) + 0.25 * i;
        const o = c - 0.7 * Math.cos(i / 2.3);
        const row: Array<number | undefined> = [
          i * MINUTE,
          o,
          Math.max(o, c) + 0.4 + 0.5 * Math.abs(Math.sin(i / 2.1)),
          Math.min(o, c) - 0.4 - 0.5 * Math.abs(Math.cos(i / 1.7)),
          c,
        ];
        if (hole !== undefined && hole.at === i) {
          row[{ open: 1, high: 2, low: 3, close: 4 }[hole.column]] = undefined;
        }
        return row;
      }) as never,
    });

  /** The close-only view, for the four `column` studies. */
  const leftoverCloses = (gapAt?: number) =>
    new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'close', kind: 'number', required: false },
      ] as const,
      rows: Array.from({ length: 60 }, (_, i) => [
        i * MINUTE,
        i === gapAt ? undefined : 100 + 7 * Math.sin(i / 3.1) + 0.25 * i,
      ]) as never,
    });

  it('smi: the head is period + long + short − 3, the signal signalPeriod − 1 later', () => {
    const out = stochasticMomentumIndex(leftoverBars(), {
      period: 4,
      longPeriod: 5,
      shortPeriod: 3,
      signalPeriod: 4,
    });
    expect(nullCountOf(out, 'smi')).toBe(9);
    expect(nullCountOf(out, 'smiSignal')).toBe(12);
    expect(typeof cells(out, 'smi')[9]).toBe('number');
  });

  it('smi: a hole in the CLOSE costs one bar; one in the HIGH costs none', () => {
    const opts = {
      period: 4,
      longPeriod: 5,
      shortPeriod: 3,
      signalPeriod: 4,
    } as const;
    // The close feeds the numerator directly, and the EMAs skip the missing
    // cell rather than propagating it.
    const closeHole = stochasticMomentumIndex(
      leftoverBars({ at: 30, column: 'close' }),
      opts,
    );
    expect(cells(closeHole, 'smi')[30]).toBeUndefined();
    expect(typeof cells(closeHole, 'smi')[31]).toBe('number');
    // The extremes come from the SKIPPING door, so an absent high leaves the
    // range defined over the cells the window does hold — no bar is lost.
    const highHole = stochasticMomentumIndex(
      leftoverBars({ at: 30, column: 'high' }),
      opts,
    );
    expect(nullCountOf(highHole, 'smi')).toBe(9);
  });

  it('fisher: the head is period − 1, the signal one more; a hole RESETS the machine', () => {
    const clean = fisherTransform(leftoverBars(), { period: 5 });
    expect(nullCountOf(clean, 'fisher')).toBe(4);
    expect(nullCountOf(clean, 'fisherSignal')).toBe(5);
    // A missing low blanks the median price, so every strict 5-bar range
    // holding it is missing: bars 30…34. The machine then re-seeds on bar
    // 35, which means bar 35 has no TRIGGER either.
    const holed = fisherTransform(leftoverBars({ at: 30, column: 'low' }), {
      period: 5,
    });
    const line = cells(holed, 'fisher');
    expect(typeof line[29]).toBe('number');
    for (let i = 30; i <= 34; i += 1)
      expect(line[i], `bar ${i}`).toBeUndefined();
    expect(typeof line[35]).toBe('number');
    expect(cells(holed, 'fisherSignal')[35]).toBeUndefined();
    expect(typeof cells(holed, 'fisherSignal')[36]).toBe('number');
  });

  it('stc: the head is at least slow + 2·cycle − 3, and a hole re-seeds both folds', () => {
    const opts = { fastPeriod: 4, slowPeriod: 10, cyclePeriod: 4 } as const;
    const clean = schaffTrendCycle(leftoverCloses(), opts);
    const line = cells(clean, 'stc');
    for (let i = 0; i < 15; i += 1) expect(line[i], `bar ${i}`).toBeUndefined();
    expect(typeof line[15]).toBe('number');
    // A missing close blanks that bar's EMAs and therefore the MACD, and the
    // strict cycle windows holding it — but nothing propagates to the end.
    const holed = schaffTrendCycle(leftoverCloses(30), opts);
    expect(cells(holed, 'stc')[30]).toBeUndefined();
    expect(typeof cells(holed, 'stc')[45]).toBe('number');
  });

  it('pgo: the head is `period`, one past the average’s, and a hole costs two bars', () => {
    const clean = prettyGoodOscillator(leftoverBars(), { period: 6 });
    expect(nullCountOf(clean, 'pgo')).toBe(6);
    // A missing close costs EXACTLY two bars, and it is worth naming which
    // two because the obvious guess is wrong: the simple average comes from
    // the COLUMN door, which skips the missing cell and averages the rest,
    // so it costs nothing. Bar 30 goes because the numerator reads the close
    // directly; bar 31 goes because its true range reads bar 30's close as
    // `prevClose`, and the EMA emits nothing on a bar whose input is
    // missing. Bar 32 is back.
    const holed = prettyGoodOscillator(
      leftoverBars({ at: 30, column: 'close' }),
      { period: 6 },
    );
    const v = cells(holed, 'pgo');
    expect(typeof v[29]).toBe('number');
    expect(v[30]).toBeUndefined();
    expect(v[31]).toBeUndefined();
    expect(typeof v[32]).toBe('number');
  });

  it('si / asi: a one-bar head, and a hole costs that bar and the next', () => {
    const clean = swingIndex(leftoverBars(), { limit: 5 });
    expect(nullCountOf(clean, 'si')).toBe(1);
    const holed = swingIndex(leftoverBars({ at: 30, column: 'open' }), {
      limit: 5,
    });
    const v = cells(holed, 'si');
    expect(typeof v[29]).toBe('number');
    expect(v[30]).toBeUndefined();
    // Bar 31 reads bar 30's open as `prevOpen`.
    expect(v[31]).toBeUndefined();
    expect(typeof v[32]).toBe('number');
    // The ASI is a running sum, so the same hole ends it rather than
    // costing two bars — the cumulative rule OBV and the A/D line follow.
    const asi = cells(
      accumulativeSwingIndex(leftoverBars({ at: 30, column: 'open' }), {
        limit: 5,
      }),
      'asi',
    );
    expect(typeof asi[29]).toBe('number');
    for (let i = 30; i < 60; i += 1) expect(asi[i], `bar ${i}`).toBeUndefined();
  });

  it('rwi: the head is `period`, and a hole costs the bar plus `period` more', () => {
    const clean = randomWalkIndex(leftoverBars(), { period: 5 });
    expect(nullCountOf(clean, 'rwiHigh')).toBe(5);
    expect(nullCountOf(clean, 'rwiLow')).toBe(5);
    // The longest horizon's strict `period`-bar mean of true range is what
    // sets the width: a missing high blanks `TR[30]`, so every window
    // holding it — bars 30…34 — is missing, and bar 35's windows all start
    // at 31.
    const holed = randomWalkIndex(leftoverBars({ at: 30, column: 'high' }), {
      period: 5,
    });
    const v = cells(holed, 'rwiHigh');
    expect(typeof v[29]).toBe('number');
    for (let i = 30; i <= 34; i += 1) expect(v[i], `bar ${i}`).toBeUndefined();
    expect(typeof v[35]).toBe('number');
  });

  it('ravi: the head is max(short, long) − 1, and a hole costs no bar at all', () => {
    const opts = { shortPeriod: 4, longPeriod: 12 } as const;
    expect(nullCountOf(ravi(leftoverCloses(), opts), 'ravi')).toBe(11);
    // Both averages come from the COLUMN door, which SKIPS a missing cell
    // and averages the rest — so a hole leaves the reading defined
    // everywhere it was, on a window one contributor short. That is core's
    // reducer policy, stated here rather than assumed.
    const holed = ravi(leftoverCloses(30), opts);
    expect(nullCountOf(holed, 'ravi')).toBe(11);
    expect(typeof cells(holed, 'ravi')[30]).toBe('number');
  });

  it('tii: the head is maPeriod + period − 2, and a hole costs `period` bars', () => {
    const opts = { period: 5, maPeriod: 10 } as const;
    expect(
      nullCountOf(trendIntensityIndex(leftoverCloses(), opts), 'tii'),
    ).toBe(13);
    // The average skips the hole (column door) but the DEVIATION does not —
    // it reads the raw close — so the bar's deviation is missing and the
    // strict `period`-bar sums holding it are too: bars 30…34.
    const holed = trendIntensityIndex(leftoverCloses(30), opts);
    const v = cells(holed, 'tii');
    expect(typeof v[29]).toBe('number');
    for (let i = 30; i <= 34; i += 1) expect(v[i], `bar ${i}`).toBeUndefined();
    expect(typeof v[35]).toBe('number');
  });

  it('specialK: a 724-bar head, and a series shorter than it is all-missing', () => {
    const long = (gapAt?: number) =>
      new TimeSeries({
        name: 'bars',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'close', kind: 'number', required: false },
        ] as const,
        rows: Array.from({ length: 760 }, (_, i) => [
          i * MINUTE,
          i === gapAt ? undefined : 100 + 20 * Math.sin(i / 70) + 0.01 * i,
        ]) as never,
      });
    expect(nullCountOf(specialK(long()), 'specialK')).toBe(724);
    expect(nullCountOf(specialK(leftoverCloses()), 'specialK')).toBe(60);
    // One hole at bar 730 blanks that bar, the twelve bars that read it as a
    // look-back base, and every smoothing window holding one of those — a
    // wide hole (the longest smoothing is 195 bars) but a hole, not a tail.
    const holed = specialK(long(730));
    expect(typeof cells(holed, 'specialK')[729]).toBe('number');
    expect(cells(holed, 'specialK')[730]).toBeUndefined();
  });

  it('no leftover column ever leaks a NaN to a reader', () => {
    const out = trendIntensityIndex(
      ravi(
        randomWalkIndex(
          accumulativeSwingIndex(
            swingIndex(
              prettyGoodOscillator(
                fisherTransform(leftoverBars({ at: 20, column: 'close' }), {
                  period: 5,
                }),
                { period: 6 },
              ),
              { limit: 5 },
            ),
            { limit: 5 },
          ),
          { period: 5 },
        ),
        { shortPeriod: 4, longPeriod: 12 },
      ),
      { period: 5, maPeriod: 10 },
    );
    for (const name of [
      'fisher',
      'fisherSignal',
      'pgo',
      'si',
      'asi',
      'rwiHigh',
      'rwiLow',
      'ravi',
      'tii',
    ]) {
      for (const value of cells(out, name)) {
        expect(
          value === undefined ||
            (typeof value === 'number' && !Number.isNaN(value)),
          name,
        ).toBe(true);
      }
    }
  });
});

describe('[PND-STUDYBOX] the price transforms: a gap costs exactly its bar', () => {
  // No `!isNaN` assertions — `withColumn` maps NaN to missing on its typed
  // door, so such a check can never fire. What is pinned is WHERE the
  // missing rows are, and how many.
  const gappyOhlc = (
    gapAt: number | undefined,
    column: 'open' | 'high' | 'low' | 'close',
  ) =>
    new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'open', kind: 'number', required: false },
        { name: 'high', kind: 'number', required: false },
        { name: 'low', kind: 'number', required: false },
        { name: 'close', kind: 'number', required: false },
      ] as const,
      rows: Array.from({ length: 30 }, (_, i) => {
        const c = 100 + 8 * Math.sin(i / 3) + 0.2 * i;
        const hide = (name: typeof column) => i === gapAt && column === name;
        return [
          i * MINUTE,
          hide('open') ? undefined : c - 0.5 * Math.cos(i / 1.7),
          hide('high') ? undefined : c + 0.4 + 0.6 * Math.abs(Math.sin(i / 2)),
          hide('low') ? undefined : c - 0.5 - 0.6 * Math.abs(Math.cos(i / 2.5)),
          hide('close') ? undefined : c,
        ];
      }) as never,
    });

  it('all four are complete with no gaps — there is no warm-up head', () => {
    const src = gappyOhlc(undefined, 'close');
    expect(nullCountOf(typicalPrice(src), 'typicalPrice')).toBe(0);
    expect(nullCountOf(medianPrice(src), 'medianPrice')).toBe(0);
    expect(nullCountOf(weightedClose(src), 'weightedClose')).toBe(0);
    expect(nullCountOf(averagePrice(src), 'averagePrice')).toBe(0);
    expect(nullCountOf(balanceOfPower(src), 'bop')).toBe(0);
  });

  it('a missing close costs one bar on the three that read it', () => {
    const src = gappyOhlc(12, 'close');
    for (const [out, name] of [
      [typicalPrice(src), 'typicalPrice'],
      [weightedClose(src), 'weightedClose'],
      [averagePrice(src), 'averagePrice'],
      [balanceOfPower(src), 'bop'],
    ] as const) {
      expect(nullCountOf(out, name), name).toBe(1);
      expect(cells(out, name)[12], name).toBeUndefined();
      expect(typeof cells(out, name)[11], name).toBe('number');
      expect(typeof cells(out, name)[13], name).toBe('number');
    }
    // …and NONE on medianPrice, which does not read the close at all. That
    // asymmetry is the reason `close` sits on the shared options type as an
    // ignored field rather than being absent from it.
    expect(nullCountOf(medianPrice(src), 'medianPrice')).toBe(0);
  });

  it('a missing open costs one bar on averagePrice and bop, none elsewhere', () => {
    const src = gappyOhlc(7, 'open');
    expect(nullCountOf(averagePrice(src), 'averagePrice')).toBe(1);
    expect(cells(averagePrice(src), 'averagePrice')[7]).toBeUndefined();
    expect(nullCountOf(balanceOfPower(src), 'bop')).toBe(1);
    expect(cells(balanceOfPower(src), 'bop')[7]).toBeUndefined();
    expect(nullCountOf(typicalPrice(src), 'typicalPrice')).toBe(0);
    expect(nullCountOf(medianPrice(src), 'medianPrice')).toBe(0);
    expect(nullCountOf(weightedClose(src), 'weightedClose')).toBe(0);
  });

  it('a missing high costs one bar on all five', () => {
    const src = gappyOhlc(20, 'high');
    for (const [out, name] of [
      [typicalPrice(src), 'typicalPrice'],
      [medianPrice(src), 'medianPrice'],
      [weightedClose(src), 'weightedClose'],
      [averagePrice(src), 'averagePrice'],
      [balanceOfPower(src), 'bop'],
    ] as const) {
      expect(nullCountOf(out, name), name).toBe(1);
      expect(cells(out, name)[20], name).toBeUndefined();
    }
  });

  it('starcBands / highLowBands: where the missing rows are', () => {
    const clean = gappyOhlc(undefined, 'close');
    // STARC: centre at period − 1, bands at max(centre, ATR).
    const starc = starcBands(clean, {
      period: 4,
      atrPeriod: 6,
      maType: 'sma',
    });
    expect(nullCountOf(starc, 'starcMiddle')).toBe(3);
    expect(nullCountOf(starc, 'starcUpper')).toBe(6);
    expect(nullCountOf(starc, 'starcLower')).toBe(6);
    // High Low Bands: all three together, at the average's own bar.
    const hlb = highLowBands(clean, { period: 5, maType: 'sma' });
    for (const name of ['hlbMiddle', 'hlbUpper', 'hlbLower']) {
      expect(nullCountOf(hlb, name), name).toBe(4);
      expect(typeof cells(hlb, name)[4], name).toBe('number');
    }
  });

  it('starcBands: a missing close leaves the centre INTACT and stops the bands', () => {
    // The two halves take different doors and it shows. The centre is an
    // `sma` over a COLUMN, so it takes the K2 engine's column door — core's
    // count-window `avg` counts ROWS for `minSamples` and SKIPS a missing
    // cell, so the centre is drawn right through the gap (over `period − 1`
    // contributors on the windows that contain it). The ATR is Wilder over
    // TRUE range, which reads the PREVIOUS close: the gap costs bar 16's
    // true range, and a recursion never gives that back — so the bands are
    // defined ON bar 15 and blank from 16 to the end.
    const out = starcBands(gappyOhlc(15, 'close'), {
      period: 4,
      atrPeriod: 6,
      maType: 'sma',
    });
    const mid = cells(out, 'starcMiddle');
    const up = cells(out, 'starcUpper');
    expect(mid.slice(3).every((x) => typeof x === 'number')).toBe(true);
    expect(nullCountOf(out, 'starcMiddle')).toBe(3);
    expect(typeof up[15]).toBe('number');
    expect(up.slice(16).every((x) => x === undefined)).toBe(true);
    expect(nullCountOf(out, 'starcUpper')).toBe(6 + 14);
  });

  it('highLowBands: a missing HIGH blanks the window it falls in, and recovers', () => {
    // The centre smooths a DERIVED array (the median price), so it takes the
    // K2 engine's array door: a window type waits for `period` finite
    // values, blanking every window that contains the gap rather than
    // averaging a short one. Nothing propagates — there is no recursion.
    const out = highLowBands(gappyOhlc(12, 'high'), {
      period: 5,
      maType: 'sma',
    });
    for (const name of ['hlbMiddle', 'hlbUpper', 'hlbLower']) {
      const v = cells(out, name);
      expect(typeof v[11], name).toBe('number');
      expect(
        v.slice(12, 17).every((x) => x === undefined),
        name,
      ).toBe(true);
      expect(typeof v[17], name).toBe('number');
      expect(nullCountOf(out, name), name).toBe(4 + 5);
    }
    // A missing CLOSE costs it nothing at all — it never reads one, so only
    // the warm-up head is missing.
    expect(
      nullCountOf(
        highLowBands(gappyOhlc(12, 'close'), { period: 5, maType: 'sma' }),
        'hlbMiddle',
      ),
    ).toBe(4);
  });

  it('the prime studies: a gap costs one bar per column, and nothing else', () => {
    // No window and no recursion, so there is nothing for a gap to spread
    // into — the strictest form of "costs exactly its own bar".
    const clean = primeNumberBands(gappyOhlc(undefined, 'close'));
    expect(nullCountOf(clean, 'pnbUpper')).toBe(0);
    expect(nullCountOf(clean, 'pnbLower')).toBe(0);

    const hi = primeNumberBands(gappyOhlc(9, 'high'));
    expect(nullCountOf(hi, 'pnbUpper')).toBe(1);
    expect(cells(hi, 'pnbUpper')[9]).toBeUndefined();
    // …and the LOWER band is untouched: the two columns read different
    // inputs and neither shares the other's fate.
    expect(nullCountOf(hi, 'pnbLower')).toBe(0);

    const lo = primeNumberBands(gappyOhlc(9, 'low'));
    expect(nullCountOf(lo, 'pnbLower')).toBe(1);
    expect(nullCountOf(lo, 'pnbUpper')).toBe(0);

    const osc = primeNumberOscillator(gappyOhlc(20, 'close') as never);
    expect(nullCountOf(osc, 'pno')).toBe(1);
    expect(cells(osc, 'pno')[20]).toBeUndefined();
    expect(typeof cells(osc, 'pno')[21]).toBe('number');
  });

  it('marketFacilitationIndex: a gap in any of the three costs that bar', () => {
    const withVolume = (gapAt: number | undefined, column: string) =>
      new TimeSeries({
        name: 'bars',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'high', kind: 'number', required: false },
          { name: 'low', kind: 'number', required: false },
          { name: 'close', kind: 'number', required: false },
          { name: 'volume', kind: 'number', required: false },
        ] as const,
        rows: Array.from({ length: 20 }, (_, i) => {
          const c = 100 + 8 * Math.sin(i / 3) + 0.2 * i;
          const hide = (name: string) => i === gapAt && column === name;
          return [
            i * MINUTE,
            hide('high') ? undefined : c + 0.5,
            hide('low') ? undefined : c - 0.5,
            c,
            hide('volume') ? undefined : 1000 + 100 * (i % 5),
          ];
        }) as never,
      });

    expect(
      nullCountOf(marketFacilitationIndex(withVolume(undefined, '')), 'bwmfi'),
    ).toBe(0);
    for (const column of ['high', 'low', 'volume']) {
      const out = marketFacilitationIndex(withVolume(7, column));
      expect(nullCountOf(out, 'bwmfi'), column).toBe(1);
      expect(cells(out, 'bwmfi')[7], column).toBeUndefined();
      expect(typeof cells(out, 'bwmfi')[8], column).toBe('number');
    }
  });

  it('bollingerBandwidth / bollingerPercentB: a gap costs %B one bar and bandwidth none', () => {
    // The asymmetry between the two, and it is not obvious: both read the
    // SAME window statistics, which core's count-window reducers compute
    // over the contributors present (a missing cell is skipped, not fatal),
    // so BandWidth — which reads nothing else — is drawn right through the
    // gap. %B additionally reads the bar's OWN price in its numerator, so
    // it loses exactly that bar and recovers on the next.
    const clean = bollingerBandwidth(gappyOhlc(undefined, 'close'), {
      period: 5,
    });
    expect(nullCountOf(clean, 'bbWidth')).toBe(4);
    expect(typeof cells(clean, 'bbWidth')[4]).toBe('number');

    const width = bollingerBandwidth(gappyOhlc(12, 'close'), { period: 5 });
    expect(nullCountOf(width, 'bbWidth')).toBe(4);
    expect(typeof cells(width, 'bbWidth')[12]).toBe('number');

    const pb = bollingerPercentB(gappyOhlc(12, 'close'), { period: 5 });
    const v = cells(pb, 'percentB');
    expect(typeof v[11]).toBe('number');
    expect(v[12]).toBeUndefined();
    expect(typeof v[13]).toBe('number');
    expect(nullCountOf(pb, 'percentB')).toBe(4 + 1);
  });

  it('smoothed balanceOfPower warms up on the average, and a gap is the maType’s rule', () => {
    // The K2 ARRAY door: a window type waits for `period` finite VALUES, so
    // the gap bar and the window that contains it are both blank, and the
    // line recovers `period` bars later rather than propagating.
    const out = balanceOfPower(gappyOhlc(15, 'high'), {
      period: 5,
      maType: 'sma',
    });
    expect(nullCountOf(out, 'bop')).toBe(4 + 5);
    expect(
      cells(out, 'bop')
        .slice(0, 4)
        .every((x) => x === undefined),
    ).toBe(true);
    expect(typeof cells(out, 'bop')[4]).toBe('number');
    expect(
      cells(out, 'bop')
        .slice(15, 20)
        .every((x) => x === undefined),
    ).toBe(true);
    expect(typeof cells(out, 'bop')[20]).toBe('number');
  });
});

describe('[PND-STUDYBOX] the volume and miscellaneous leftovers: where the missing rows are', () => {
  /** Sixty wavy OHLCV bars; `hole` names a bar and a column to blank. */
  const volMiscBars = (hole?: {
    at: number;
    column: 'open' | 'high' | 'low' | 'close' | 'volume';
  }) =>
    new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'open', kind: 'number', required: false },
        { name: 'high', kind: 'number', required: false },
        { name: 'low', kind: 'number', required: false },
        { name: 'close', kind: 'number', required: false },
        { name: 'volume', kind: 'number', required: false },
      ] as const,
      rows: Array.from({ length: 60 }, (_, i) => {
        const c = 100 + 7 * Math.sin(i / 3.1) + 0.25 * i;
        const o = c - 0.7 * Math.cos(i / 2.3);
        const row: Array<number | undefined> = [
          i * MINUTE,
          o,
          Math.max(o, c) + 0.4 + 0.5 * Math.abs(Math.sin(i / 2.1)),
          Math.min(o, c) - 0.4 - 0.5 * Math.abs(Math.cos(i / 1.7)),
          c,
          1000 + 120 * ((i * 3) % 7),
        ];
        if (hole !== undefined && hole.at === i) {
          row[{ open: 1, high: 2, low: 3, close: 4, volume: 5 }[hole.column]] =
            undefined;
        }
        return row;
      }) as never,
    });

  it('tmf: the head is `period` — one past a window study’s — and a hole ends it', () => {
    // Bar 0 has no previous close, so it has no true range; the Wilder seed
    // therefore lands at bar `period` rather than `period − 1`.
    const clean = twiggsMoneyFlow(volMiscBars(), { period: 6 });
    expect(nullCountOf(clean, 'tmf')).toBe(6);
    expect(typeof cells(clean, 'tmf')[6]).toBe('number');

    // An interior hole in ANY of the four inputs ends the reading, because
    // Wilder's recursion has no state to carry across it — the `atr` rule,
    // and the difference from `chaikinMoneyFlow`, which recovers `period`
    // bars later.
    for (const column of ['high', 'low', 'close', 'volume'] as const) {
      const holed = twiggsMoneyFlow(volMiscBars({ at: 30, column }), {
        period: 6,
      });
      const v = cells(holed, 'tmf');
      expect(typeof v[29], column).toBe('number');
      expect(v[30], column).toBeUndefined();
      expect(v[59], column).toBeUndefined();
    }
  });

  it('tvi: no head at all, and an interior hole ENDS the index (obv\u2019s rule)', () => {
    const clean = tradeVolumeIndex(volMiscBars(), { minTick: 0.2 });
    expect(nullCountOf(clean, 'tvi')).toBe(0);
    expect(cells(clean, 'tvi')[0]).toBe(0);

    // A hole in either input it reads ends the level; a hole in one it does
    // not read (`open`, `high`, `low`) is invisible to it.
    for (const column of ['close', 'volume'] as const) {
      const holed = tradeVolumeIndex(volMiscBars({ at: 30, column }), {
        minTick: 0.2,
      });
      const v = cells(holed, 'tvi');
      expect(typeof v[29], column).toBe('number');
      expect(v[30], column).toBeUndefined();
      expect(v[59], column).toBeUndefined();
    }
    expect(
      nullCountOf(
        tradeVolumeIndex(volMiscBars({ at: 30, column: 'high' }), {
          minTick: 0.2,
        }),
        'tvi',
      ),
    ).toBe(0);
  });

  it('sir: the two columns have DIFFERENT heads, and different holes', () => {
    const clean = shinoharaIntensityRatio(volMiscBars(), { period: 6 });
    // Per-column warm-up: the strong pair reads one bar, the weak pair reads
    // the previous close as well.
    expect(nullCountOf(clean, 'sirStrong')).toBe(5);
    expect(nullCountOf(clean, 'sirWeak')).toBe(6);

    // A hole in `open` blanks the STRONG windows holding it and nothing
    // else; a hole in `close` blanks the WEAK windows holding the bar AFTER
    // it, and nothing else. Both are `rollingMeanValues`' rule (a window
    // with a gap emits nothing) plus which column each pair reads.
    const noOpen = shinoharaIntensityRatio(
      volMiscBars({ at: 30, column: 'open' }),
      { period: 6 },
    );
    const strong = cells(noOpen, 'sirStrong');
    expect(typeof strong[29]).toBe('number');
    for (let i = 30; i <= 35; i += 1)
      expect(strong[i], `bar ${i}`).toBeUndefined();
    expect(typeof strong[36]).toBe('number');
    expect(nullCountOf(noOpen, 'sirWeak')).toBe(6); // untouched

    const noClose = shinoharaIntensityRatio(
      volMiscBars({ at: 30, column: 'close' }),
      { period: 6 },
    );
    const weak = cells(noClose, 'sirWeak');
    expect(typeof weak[30]).toBe('number'); // reads bar 29's close, which is there
    for (let i = 31; i <= 36; i += 1)
      expect(weak[i], `bar ${i}`).toBeUndefined();
    expect(typeof weak[37]).toBe('number');
    expect(nullCountOf(noClose, 'sirStrong')).toBe(5); // untouched
  });

  it('impulse: the head is one bar past the slower input, and a hole costs two bars', () => {
    const opts = {
      emaPeriod: 6,
      fastPeriod: 3,
      slowPeriod: 7,
      signalPeriod: 4,
    } as const;
    const clean = elderImpulse(volMiscBars(), opts);
    // The histogram starts at slow + signal - 2 = 9, the EMA at 5, and the
    // verdict needs TWO of the slower: bar 10.
    expect(nullCountOf(clean, 'impulse')).toBe(10);
    expect(typeof cells(clean, 'impulse')[10]).toBe('number');

    // A missing close blanks its own bar and the next (the verdict compares
    // adjacent bars), then the column comes BACK — both recursions skip the
    // gap and carry on, which is `ema`'s rule and not Wilder's.
    const holed = elderImpulse(volMiscBars({ at: 30, column: 'close' }), opts);
    const v = cells(holed, 'impulse');
    expect(typeof v[29]).toBe('number');
    expect(v[30]).toBeUndefined();
    expect(v[31]).toBeUndefined();
    expect(typeof v[32]).toBe('number');
  });

  it('maCross: the head is `slowPeriod`, and what a gap costs is `maType`\u2019s', () => {
    const clean = movingAverageCross(volMiscBars(), {
      fastPeriod: 3,
      slowPeriod: 6,
    });
    // Bars 0..4 have no slow average; bar 5 has one but is the SEED, which
    // reports nothing. So the head is `slowPeriod`, one longer than the
    // average's own.
    expect(nullCountOf(clean, 'maCross')).toBe(6);
    expect(typeof cells(clean, 'maCross')[6]).toBe('number');

    // At the default `sma` a hole costs NOTHING: the K2 column door counts
    // rows, so both averages skip the missing cell and stay defined, and the
    // fold never sees an incomplete row. The reset never fires — the sharp
    // edge this test exists to pin.
    const asSma = movingAverageCross(volMiscBars({ at: 30, column: 'close' }), {
      fastPeriod: 3,
      slowPeriod: 6,
    });
    expect(nullCountOf(asSma, 'maCross')).toBe(6);
    expect(typeof cells(asSma, 'maCross')[30]).toBe('number');

    // With `ema` the averages blank the gap bar and recover, so the RESET is
    // what is visible: bar 30 is the hole, bar 31 the fresh seed.
    const asEma = movingAverageCross(volMiscBars({ at: 30, column: 'close' }), {
      fastPeriod: 3,
      slowPeriod: 6,
      maType: 'ema',
    });
    const e = cells(asEma, 'maCross');
    expect(typeof e[29]).toBe('number');
    expect(e[30]).toBeUndefined();
    expect(e[31]).toBeUndefined();
    expect(typeof e[32]).toBe('number');

    // With `wma` the ARRAY door waits for `period` finite values, so the slow
    // average is blank for a whole window and the seed lands at 36.
    const asWma = movingAverageCross(volMiscBars({ at: 30, column: 'close' }), {
      fastPeriod: 3,
      slowPeriod: 6,
      maType: 'wma',
    });
    const w = cells(asWma, 'maCross');
    for (let i = 30; i <= 36; i += 1) expect(w[i], `bar ${i}`).toBeUndefined();
    expect(typeof w[37]).toBe('number');

    // With `smma` (Wilder) the averages never come back, so neither does the
    // signal — the recursion's rule, inherited whole.
    const asSmma = movingAverageCross(
      volMiscBars({ at: 30, column: 'close' }),
      { fastPeriod: 3, slowPeriod: 6, maType: 'smma' },
    );
    const m = cells(asSmma, 'maCross');
    expect(typeof m[29]).toBe('number');
    for (let i = 30; i < 60; i += 1) expect(m[i], `bar ${i}`).toBeUndefined();
  });

  it('avwap: everything before the anchor is missing, and a hole ends the line', () => {
    // The anchor is a TIME, and `volMiscBars` keys bar `i` at `i` minutes.
    const clean = anchoredVwap(volMiscBars(), { anchor: 20 * MINUTE });
    expect(nullCountOf(clean, 'avwap')).toBe(20);
    expect(typeof cells(clean, 'avwap')[20]).toBe('number');

    // An interior hole in ANY of the four inputs ends the line — both sums
    // are `cumulativeValues`, so every later level is a known sum plus an
    // unknown. `obv`'s rule, not `negativeVolumeIndex`'s re-seed.
    for (const column of ['high', 'low', 'close', 'volume'] as const) {
      const holed = anchoredVwap(volMiscBars({ at: 30, column }), {
        anchor: 20 * MINUTE,
      });
      const v = cells(holed, 'avwap');
      expect(typeof v[29], column).toBe('number');
      expect(v[30], column).toBeUndefined();
      expect(v[59], column).toBeUndefined();
    }

    // A hole BEFORE the anchor is invisible: those bars were never part of
    // this VWAP.
    const early = anchoredVwap(volMiscBars({ at: 5, column: 'close' }), {
      anchor: 20 * MINUTE,
    });
    expect(nullCountOf(early, 'avwap')).toBe(20);
  });
});

/* -------------------------------------------------------------------------- */
/* The session-anchored pair — where their missing rows are.                   */
/*                                                                             */
/* Both blank the same two things for structural reasons (a bar in closed time */
/* has no session; the first session has no predecessor), and then differ on   */
/* what a HOLE does: the VWAP's running sums end that session's line, while    */
/* the pivots' max/min/last simply skip the missing cell.                      */
/* -------------------------------------------------------------------------- */
describe('the session-anchored studies place their gaps', () => {
  const RULES = {
    timeZone: 'America/New_York',
    open: '09:30',
    close: '16:00',
  } as const;
  const sessions = generateSessions(RULES, {
    from: '2024-01-08',
    to: '2024-01-09',
  });
  const cal = TradingCalendar.fromSessions(sessions);
  const PER = 6; // bars per session, on a 1-hour grid

  /** Six bars per session plus a bar stamped at session 1's close (closed
   *  time), 13 rows. `hole` blanks one cell. */
  const sessionBars = (hole?: {
    at: number;
    column: 'high' | 'low' | 'close' | 'volume';
  }) => {
    const times = [
      ...Array.from(
        { length: PER },
        (_, k) => sessions[0]!.open + k * 3_600_000,
      ),
      sessions[0]!.close,
      ...Array.from(
        { length: PER },
        (_, k) => sessions[1]!.open + k * 3_600_000,
      ),
    ];
    return new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'high', kind: 'number', required: false },
        { name: 'low', kind: 'number', required: false },
        { name: 'close', kind: 'number', required: false },
        { name: 'volume', kind: 'number', required: false },
      ] as const,
      rows: times.map((t, i) => {
        const c = 100 + 5 * Math.sin(i / 2.1) + 0.4 * i;
        const row: Array<number | undefined> = [
          t,
          c + 0.6 + 0.4 * Math.abs(Math.sin(i / 1.7)),
          c - 0.6 - 0.4 * Math.abs(Math.cos(i / 1.3)),
          c,
          900 + 130 * ((i * 3) % 7),
        ];
        if (hole !== undefined && hole.at === i) {
          row[{ high: 1, low: 2, close: 3, volume: 4 }[hole.column]] =
            undefined;
        }
        return row;
      }) as never,
    });
  };

  it('svwap: closed time is missing, and a hole ends THAT SESSION only', () => {
    const clean = sessionVwap(sessionBars() as never, { sessions: cal });
    // Exactly one missing row — the bar stamped at the close.
    expect(nullCountOf(clean, 'svwap')).toBe(1);
    expect(cells(clean, 'svwap')[PER]).toBeUndefined();

    for (const column of ['high', 'low', 'close', 'volume'] as const) {
      const holed = sessionVwap(sessionBars({ at: 2, column }) as never, {
        sessions: cal,
      });
      const v = cells(holed, 'svwap');
      expect(typeof v[1], column).toBe('number');
      // The hole ends session 1's line …
      expect(v[2], column).toBeUndefined();
      expect(v[PER - 1], column).toBeUndefined();
      // … and the RESET at the next open is the recovery, which is the whole
      // difference from `anchoredVwap`, where the caller must re-anchor.
      expect(typeof v[PER + 1], column).toBe('number');
    }
  });

  it('svwap: a hole on a session\u2019s FIRST bar shifts that session\u2019s start', () => {
    const holed = sessionVwap(
      sessionBars({ at: PER + 1, column: 'close' }) as never,
      {
        sessions: cal,
      },
    );
    const v = cells(holed, 'svwap');
    expect(v[PER + 1]).toBeUndefined();
    expect(typeof v[PER + 2]).toBe('number');
  });

  it('pivots: the first session and closed time are missing, together', () => {
    const r = pivotPoints(sessionBars() as never, { sessions: cal });
    for (const name of [
      'ppPivot',
      'ppR1',
      'ppR2',
      'ppR3',
      'ppS1',
      'ppS2',
      'ppS3',
    ]) {
      const v = cells(r, name);
      // Six bars of session 1 plus the closed-time bar.
      expect(nullCountOf(r, name), name).toBe(PER + 1);
      for (let i = 0; i <= PER; i += 1)
        expect(v[i], `${name}[${i}]`).toBeUndefined();
      for (let i = PER + 1; i < 13; i += 1)
        expect(typeof v[i], `${name}[${i}]`).toBe('number');
    }
  });

  it('pivots: a hole inside the previous session SKIPS the bar, it does not blank the session', () => {
    // `max` / `min` / `last` are order statistics over the cells that are
    // present — the opposite call from the VWAP's paired sums, and why the
    // two studies differ here is written out in the kernel.
    const holed = pivotPoints(sessionBars({ at: 2, column: 'high' }) as never, {
      sessions: cal,
    });
    expect(typeof cells(holed, 'ppPivot')[PER + 1]).toBe('number');
  });

  it('pivots: a column with NO present cell in the previous session blanks the ladder', () => {
    const times = [
      ...Array.from({ length: 2 }, (_, k) => sessions[0]!.open + k * 3_600_000),
      ...Array.from({ length: 2 }, (_, k) => sessions[1]!.open + k * 3_600_000),
    ];
    const allHighsMissing = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'high', kind: 'number', required: false },
        { name: 'low', kind: 'number', required: false },
        { name: 'close', kind: 'number', required: false },
      ] as const,
      rows: times.map((t, i) => [
        t,
        i < 2 ? undefined : 101 + i,
        99 + i,
        100 + i,
      ]) as never,
    });
    const r = pivotPoints(allHighsMissing as never, { sessions: cal });
    expect(nullCountOf(r, 'ppPivot')).toBe(4);
    expect(nullCountOf(r, 'ppS3')).toBe(4);
  });
});

describe('[PND-STUDYBOX] Ichimoku and ZigZag: where the missing rows land', () => {
  /** 60 swinging bars, optionally with one cell punched out. The swing is
   *  wide enough (±7 on ~100) that ZigZag confirms pivots on both sides of
   *  a hole at bar 30. */
  const swingBars = (hole?: { at: number; column: 'high' | 'low' | 'close' }) =>
    new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'high', kind: 'number', required: false },
        { name: 'low', kind: 'number', required: false },
        { name: 'close', kind: 'number', required: false },
      ] as const,
      rows: Array.from({ length: 60 }, (_, i) => {
        const c = 100 + 7 * Math.sin(i / 3.1) + 0.25 * i;
        const row: Array<number | undefined> = [
          i * MINUTE,
          c + 0.4 + 0.5 * Math.abs(Math.sin(i / 2.1)),
          c - 0.4 - 0.5 * Math.abs(Math.cos(i / 1.7)),
          c,
        ];
        if (hole !== undefined && hole.at === i) {
          row[{ high: 1, low: 2, close: 3 }[hole.column]] = undefined;
        }
        return row;
      }) as never,
    });

  const ichiOpts = {
    conversionPeriod: 4,
    basePeriod: 6,
    spanBPeriod: 9,
  } as const;

  it('ichimoku: a hole in `high` costs each line exactly its OWN window', () => {
    // `rollingExtremesValues` is the STRICT door, so a hole blanks precisely
    // the windows that contain it — 4 bars for Tenkan, 6 for Kijun, 9 for
    // Senkou B — and the line then comes back. Senkou A is the mean of the
    // first two and takes the longer of their two blanks.
    const clean = ichimoku(swingBars(), ichiOpts);
    expect(nullCountOf(clean, 'ichiTenkan')).toBe(3);
    expect(nullCountOf(clean, 'ichiKijun')).toBe(5);
    expect(nullCountOf(clean, 'ichiSenkouA')).toBe(5);
    expect(nullCountOf(clean, 'ichiSenkouB')).toBe(8);
    expect(nullCountOf(clean, 'ichiChikou')).toBe(0);

    const holed = ichimoku(swingBars({ at: 30, column: 'high' }), ichiOpts);
    for (const [name, span] of [
      ['ichiTenkan', 4],
      ['ichiKijun', 6],
      ['ichiSenkouA', 6],
      ['ichiSenkouB', 9],
    ] as const) {
      const v = cells(holed, name);
      expect(typeof v[29], name).toBe('number');
      for (let i = 30; i < 30 + span; i += 1) {
        expect(v[i], `${name} bar ${i}`).toBeUndefined();
      }
      expect(typeof v[30 + span], name).toBe('number');
    }
    // The close is a different input: Chikou never notices.
    expect(nullCountOf(holed, 'ichiChikou')).toBe(0);
  });

  it('ichimoku: a hole in `close` blanks Chikou alone', () => {
    const holed = ichimoku(swingBars({ at: 30, column: 'close' }), ichiOpts);
    expect(nullCountOf(holed, 'ichiChikou')).toBe(1);
    expect(cells(holed, 'ichiChikou')[30]).toBeUndefined();
    // The four windowed lines read `high` and `low` only.
    expect(nullCountOf(holed, 'ichiTenkan')).toBe(3);
    expect(nullCountOf(holed, 'ichiSenkouB')).toBe(8);
  });

  it('zigZag: a gap ends the leg, and no line is drawn across it', () => {
    const opts = { deviation: 3 } as const;
    const clean = zigZag(swingBars(), opts);
    const cleanLine = cells(clean, 'zzLine');
    // The control: on clean bars a leg spans bar 30.
    expect(typeof cleanLine[30]).toBe('number');

    const holed = zigZag(swingBars({ at: 30, column: 'low' }), opts);
    const pivots = cells(holed, 'zzPivot');
    const line = cells(holed, 'zzLine');
    const direction = cells(holed, 'zzDirection');

    // The gap bar itself reports nothing on any of the three.
    expect(pivots[30]).toBeUndefined();
    expect(line[30]).toBeUndefined();
    expect(direction[30]).toBeUndefined();

    // Both runs confirm pivots, so the machine really did re-seed.
    const bars = pivots
      .map((x, i) => (typeof x === 'number' ? i : -1))
      .filter((i) => i >= 0);
    expect(bars.some((i) => i < 30)).toBe(true);
    expect(bars.some((i) => i > 30)).toBe(true);

    // The FIRST run's line stops at its last confirmed pivot — the leg in
    // force at the hole was provisional and is discarded, not confirmed
    // against the bars on the far side.
    const lastBefore = Math.max(...bars.filter((i) => i < 30));
    for (let i = lastBefore + 1; i <= 30; i += 1) {
      expect(line[i], `bar ${i}`).toBeUndefined();
    }
    // …and the second run's line starts at ITS first pivot, never earlier.
    const firstAfter = Math.min(...bars.filter((i) => i > 30));
    for (let i = 31; i < firstAfter; i += 1) {
      expect(line[i], `bar ${i}`).toBeUndefined();
    }
    expect(typeof line[firstAfter]).toBe('number');
    // The direction column DOES cover the provisional leg on both sides —
    // its direction is the one thing about it that is known.
    expect(typeof direction[29]).toBe('number');
    expect(typeof direction[31]).toBe('number');
  });
});
