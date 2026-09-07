import { readFileSync } from 'node:fs';
import { describe, it, expect } from 'vitest';
import { TimeSeries } from 'pond-ts';
import {
  sma,
  ema,
  bollinger,
  rollingStdev,
  rollingMin,
  rollingMax,
  rollingPercentile,
  zScore,
  envelope,
  percentChange,
  rsi,
  macd,
  atr,
  obv,
  vwap,
  stochastic,
  williamsR,
  donchian,
  momentum,
  historicalVolatility,
  movingAverage,
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
  MA_TYPES,
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
} from '../src/index.js';

/** A close-only bar series at 1ms spacing (value = the close). */
const closeSchema = [
  { name: 'time', kind: 'time' },
  { name: 'close', kind: 'number' },
] as const;

function bars(closes: number[], times?: number[]) {
  return new TimeSeries({
    name: 'bars',
    schema: closeSchema,
    rows: closes.map((c, i) => [times ? times[i]! : i, c]) as Array<
      [number, number]
    >,
  });
}

/** Read a numeric column as (number | undefined)[] from any result series. */
function col(s: unknown, name: string): Array<number | undefined> {
  const events = (
    s as { events: ReadonlyArray<{ data(): Record<string, unknown> }> }
  ).events;
  return events.map((e) => {
    const v = e.data()[name];
    return typeof v === 'number' ? v : undefined;
  });
}

describe('sma', () => {
  it('averages the last `period` bars, warmup rows undefined, length kept', () => {
    const r = sma(bars([10, 11, 12, 13, 14]), { period: 3 });
    expect(r.length).toBe(5);
    expect(col(r, 'sma')).toEqual([undefined, undefined, 11, 12, 13]);
  });

  it('counts N bars across a time gap (a duration window would not)', () => {
    // A big gap between bars 2 and 3; the count window still averages 2 bars.
    const r = sma(bars([10, 20, 30, 40, 50], [0, 1, 2, 1_000_000, 1_000_001]), {
      period: 2,
    });
    expect(col(r, 'sma')[3]).toBe(35); // (30 + 40) / 2 — two bars, not time
  });

  it('honours column + output, and composes over another study output', () => {
    const once = sma(bars([10, 11, 12, 13, 14]), { period: 2, output: 'fast' });
    const twice = sma(once, { period: 2, column: 'fast', output: 'fastfast' });
    expect(col(twice, 'fast')[4]).toBe(13.5); // avg(13,14)
    // fastfast = avg of the last two `fast` values (a study over a study).
    expect(col(twice, 'fastfast')[4]).toBeCloseTo((12.5 + 13.5) / 2, 10);
  });

  it('stacks sma → ema → bollinger without a strict-intake crash on the warmup gaps', () => {
    // Regression: chaining a rolling study (sma, undefined warmup) into ema
    // (which rebuilds rows via strict intake) crashed while withColumn marked
    // the sma column required. All three now stack on one series. (Separate
    // `const` bindings — each study widens the schema type, so a reassigned
    // `let` wouldn't typecheck.)
    const withSma = sma(bars([10, 11, 12, 13, 14, 15]), { period: 3 });
    const withEma = ema(withSma, { period: 3 });
    const withBands = bollinger(withEma, { period: 3 });
    const last = withBands.events.at(-1)!.data();
    expect(typeof last.sma).toBe('number');
    expect(typeof last.ema).toBe('number');
    expect(typeof last.bbMiddle).toBe('number');
  });

  it('throws on a bad period or an output collision', () => {
    expect(() => sma(bars([1, 2]), { period: 0 })).toThrow(/positive integer/);
    expect(() => sma(bars([1, 2]), { period: 1, output: 'close' })).toThrow(
      /collides/,
    );
  });

  it('period larger than the series → every row undefined, length kept', () => {
    const r = sma(bars([10, 20, 30]), { period: 5 });
    expect(r.length).toBe(3);
    expect(col(r, 'sma')).toEqual([undefined, undefined, undefined]);
  });

  it('period 1 on a single bar is that bar', () => {
    const r = sma(bars([42]), { period: 1 });
    expect(col(r, 'sma')).toEqual([42]);
  });
});

describe('ema', () => {
  it('is a span EMA (span = period → α = 2/(period+1)) with a length-preserving warmup', () => {
    const src = bars([10, 11, 12, 13, 14, 15]);
    const r = ema(src, { period: 3 }); // span 3 → α 0.5
    // First period-1 rows masked; length preserved.
    expect(r.length).toBe(6);
    expect(col(r, 'ema')[0]).toBeUndefined();
    expect(col(r, 'ema')[1]).toBeUndefined();
    // Equal to the raw smooth('ema', { span:3, minSamples:3 }) tail.
    const ref = src.smooth('close', 'ema', {
      span: 3,
      minSamples: 3,
      output: 'e',
    });
    for (let i = 2; i < 6; i += 1) {
      expect(col(r, 'ema')[i]).toBeCloseTo(col(ref, 'e')[i] as number, 10);
    }
  });
});

describe('bollinger', () => {
  it('appends middle/upper/lower with the ±stdDev band', () => {
    const r = bollinger(bars([10, 20, 30, 25, 15]), { period: 3, stdDev: 2 });
    const mid = col(r, 'bbMiddle');
    const up = col(r, 'bbUpper');
    const lo = col(r, 'bbLower');
    expect(mid[0]).toBeUndefined(); // warmup
    expect(mid[2]).toBeCloseTo(20, 10); // avg(10,20,30)
    // population stdev of {10,20,30} = sqrt(200/3); band = mid ± 2σ.
    const sd = Math.sqrt(200 / 3);
    expect(up[2]).toBeCloseTo(20 + 2 * sd, 10);
    expect(lo[2]).toBeCloseTo(20 - 2 * sd, 10);
  });

  it('emits undefined bands on a flat (σ = 0) window, and honours prefix', () => {
    const r = bollinger(bars([5, 5, 5, 5]), { period: 3, prefix: 'band' });
    expect(col(r, 'bandMiddle')[2]).toBe(5);
    expect(col(r, 'bandUpper')[2]).toBeUndefined(); // σ = 0 → no band
    expect(col(r, 'bandLower')[2]).toBeUndefined();
  });

  it('throws on a non-positive stdDev', () => {
    expect(() => bollinger(bars([1, 2, 3]), { period: 2, stdDev: 0 })).toThrow(
      /stdDev/,
    );
  });
});

// Values for the studies below are cross-validated against pandas in
// study-oracle.test.ts; here we pin the behaviours the oracle doesn't cover
// (output naming, warm-up shape, σ=0 / edge handling, validation).
describe('rolling-stat family', () => {
  it('rollingStdev/Min/Max append their default columns, warmup undefined', () => {
    const src = bars([10, 12, 11, 15, 14]);
    expect(col(rollingStdev(src, { period: 3 }), 'stdev')[1]).toBeUndefined();
    expect(col(rollingMin(src, { period: 3 }), 'min')[2]).toBe(10);
    expect(col(rollingMax(src, { period: 3 }), 'max')[2]).toBe(12);
    expect(col(rollingMax(src, { period: 3 }), 'max')[3]).toBe(15);
  });

  it('rollingPercentile defaults output to p{q} and validates q', () => {
    const r = rollingPercentile(bars([1, 2, 3, 4, 5]), { period: 3, q: 50 });
    expect(col(r, 'p50')[2]).toBe(2); // median of [1,2,3]
    expect(() =>
      rollingPercentile(bars([1, 2]), { period: 2, q: 150 }),
    ).toThrow(/\[0, 100\]/);
  });
});

describe('zScore', () => {
  it('is (value - mean) / stdev, undefined on warmup and flat windows', () => {
    // window [10,20,30] → mean 20, pop stdev sqrt(200/3); z of 30 = 10/sd.
    const r = zScore(bars([10, 20, 30]), { period: 3 });
    expect(col(r, 'zscore')[1]).toBeUndefined();
    expect(col(r, 'zscore')[2]).toBeCloseTo(10 / Math.sqrt(200 / 3), 10);
    // flat window → σ = 0 → undefined (not ±Infinity).
    expect(col(zScore(bars([5, 5, 5]), { period: 3 }), 'zscore')[2]).toBe(
      undefined,
    );
  });
});

describe('envelope', () => {
  it('bands are middle × (1 ± percent/100); honours prefix', () => {
    const r = envelope(bars([10, 20, 30]), { period: 3, percent: 10 });
    expect(col(r, 'envMiddle')[2]).toBe(20);
    expect(col(r, 'envUpper')[2]).toBeCloseTo(22, 10); // 20 * 1.10
    expect(col(r, 'envLower')[2]).toBeCloseTo(18, 10); // 20 * 0.90
    const e = envelope(bars([10, 20, 30]), { period: 3, prefix: 'ma' });
    expect(col(e, 'maMiddle')[2]).toBe(20);
  });

  it('throws on a non-positive percent', () => {
    expect(() => envelope(bars([1, 2, 3]), { period: 2, percent: 0 })).toThrow(
      /percent/,
    );
  });
});

describe('percentChange', () => {
  it('is (v / v[-periods] - 1) * 100; first `periods` rows undefined', () => {
    const r = percentChange(bars([100, 110, 121]), { periods: 1 });
    expect(col(r, 'pctChange')[0]).toBeUndefined();
    expect(col(r, 'pctChange')[1]).toBeCloseTo(10, 10); // 110/100 - 1
    expect(col(r, 'pctChange')[2]).toBeCloseTo(10, 10); // 121/110 - 1
  });

  it('defaults periods to 1 and validates', () => {
    expect(col(percentChange(bars([100, 105])), 'pctChange')[1]).toBeCloseTo(
      5,
      10,
    );
    expect(() => percentChange(bars([1, 2]), { periods: 0 })).toThrow(
      /positive integer/,
    );
  });
});

describe('rsi', () => {
  it('needs period+1 bars: the first `period` rows are undefined', () => {
    // RSI is built from DIFFERENCES, so a period-bar average of them needs
    // period+1 bars. Off-by-one here is the classic RSI mistake.
    const r = rsi(bars([1, 2, 3, 4, 5, 6]), { period: 3 });
    const v = col(r, 'rsi');
    expect(v.slice(0, 3).every((x) => x === undefined)).toBe(true);
    expect(v[3]).toBeDefined();
    expect(v).toHaveLength(6); // length-preserving
  });

  it('is 100 when the window has no losses', () => {
    // Monotonically rising: avgLoss is 0, the ratio diverges, RSI's limit
    // is 100 — not a division by zero and not undefined.
    const v = col(rsi(bars([1, 2, 3, 4, 5, 6]), { period: 3 }), 'rsi');
    expect(v[3]).toBe(100);
    expect(v[5]).toBe(100);
  });

  it('is 0 when the window has no gains', () => {
    const v = col(rsi(bars([6, 5, 4, 3, 2, 1]), { period: 3 }), 'rsi');
    expect(v[3]).toBe(0);
  });

  it('is undefined on a perfectly flat window (no relative strength)', () => {
    // avgGain and avgLoss are both 0: the ratio is 0/0, which has no value
    // rather than a conventional one.
    const v = col(rsi(bars([5, 5, 5, 5, 5, 5]), { period: 3 }), 'rsi');
    expect(v[3]).toBeUndefined();
    expect(v[5]).toBeUndefined();
  });

  it('stays within 0..100', () => {
    const closes = Array.from(
      { length: 60 },
      (_, i) => 100 + 10 * Math.sin(i / 4) + i * 0.3,
    );
    for (const x of col(rsi(bars(closes), { period: 14 }), 'rsi')) {
      if (x !== undefined) {
        expect(x).toBeGreaterThanOrEqual(0);
        expect(x).toBeLessThanOrEqual(100);
      }
    }
  });

  it('defaults to period 14 and the `rsi` output name, and honours both', () => {
    const closes = Array.from({ length: 40 }, (_, i) => 100 + (i % 5) - 2);
    const def = rsi(bars(closes));
    expect(col(def, 'rsi')[13]).toBeUndefined(); // period 14 → 14 warm-up rows
    expect(col(def, 'rsi')[14]).toBeDefined();

    const named = rsi(bars(closes), { period: 5, output: 'momentum' });
    expect(col(named, 'momentum')[5]).toBeDefined();
    expect(col(named, 'rsi')[5]).toBeUndefined(); // no default column written
  });

  it('runs over any numeric column, including another study output', () => {
    // The uniform-shape rule: never hard-code `close`.
    const chained = rsi(
      sma(bars([1, 3, 2, 5, 4, 7, 6, 9, 8, 11]), { period: 2 }),
      {
        column: 'sma',
        period: 3,
        output: 'smaRsi',
      },
    );
    expect(col(chained, 'smaRsi')[9]).toBeDefined();
  });

  it('propagates an interior gap, but only shifts for a leading one', () => {
    // The asymmetry is inherent to a recursion and is documented as such: a
    // leading gap (a chained study's warm-up) moves the seed; an interior one
    // has no state to carry across and poisons what follows.
    const withHole = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'close', kind: 'number', required: false },
      ] as const,
      rows: [
        [0, 1],
        [1, 2],
        [2, 3],
        [3, 4],
        [4, undefined],
        [5, 6],
        [6, 7],
        [7, 8],
      ] as Array<[number, number | undefined]>,
    });
    const v = col(rsi(withHole as never, { period: 3 }), 'rsi');
    expect(v[3]).toBeDefined(); // seeded before the hole
    expect(v[7]).toBeUndefined(); // and never recovers after it
  });

  it('rejects a non-positive or fractional period, and a colliding output', () => {
    expect(() => rsi(bars([1, 2, 3]), { period: 0 })).toThrow(TypeError);
    expect(() => rsi(bars([1, 2, 3]), { period: 2.5 })).toThrow(TypeError);
    expect(() => rsi(bars([1, 2, 3]), { output: 'close' })).toThrow();
  });

  it('is all-undefined when the period exceeds the available differences', () => {
    const v = col(rsi(bars([1, 2, 3]), { period: 5 }), 'rsi');
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('macd', () => {
  const rising = Array.from({ length: 60 }, (_, i) => 100 + i);

  it('each column warms up when it can, not all at the slowest', () => {
    // The line is defined once the SLOW ema is (bar slow-1); the signal a
    // further signalPeriod-1 bars on. TA-Lib masks the line back to the
    // signal's start and throws those values away; we keep them.
    const r = macd(bars(rising), {
      fastPeriod: 3,
      slowPeriod: 7,
      signalPeriod: 4,
    });
    const line = col(r, 'macdLine');
    const signal = col(r, 'macdSignal');
    const hist = col(r, 'macdHist');

    expect(line[5]).toBeUndefined();
    expect(line[6]).toBeDefined(); // slowPeriod - 1
    expect(signal[8]).toBeUndefined();
    expect(signal[9]).toBeDefined(); // + signalPeriod - 1
    expect(hist[8]).toBeUndefined();
    expect(hist[9]).toBeDefined();
    expect(line).toHaveLength(60); // length-preserving
  });

  it('the line is exactly ema(fast) − ema(slow) from this package', () => {
    // The internal-consistency property that decided the seed convention: if
    // this ever fails, macd() and ema() have drifted apart and the reason for
    // NOT matching TA-Lib's seed has evaporated.
    const src = bars(rising);
    const f = col(ema(src, { period: 3, output: 'f' }), 'f');
    const sl = col(ema(src, { period: 7, output: 's' }), 's');
    const line = col(
      macd(src, { fastPeriod: 3, slowPeriod: 7, signalPeriod: 4 }),
      'macdLine',
    );
    for (let i = 0; i < rising.length; i += 1) {
      if (f[i] === undefined || sl[i] === undefined) {
        expect(line[i]).toBeUndefined();
      } else {
        expect(line[i]).toBeCloseTo(f[i]! - sl[i]!, 12);
      }
    }
  });

  it('the histogram is exactly line − signal', () => {
    const r = macd(bars(rising), {
      fastPeriod: 3,
      slowPeriod: 7,
      signalPeriod: 4,
    });
    const line = col(r, 'macdLine');
    const signal = col(r, 'macdSignal');
    const hist = col(r, 'macdHist');
    for (let i = 0; i < rising.length; i += 1) {
      if (signal[i] === undefined) expect(hist[i]).toBeUndefined();
      else expect(hist[i]).toBeCloseTo(line[i]! - signal[i]!, 12);
    }
  });

  it('honours a custom prefix and leaves no scratch column behind', () => {
    const r = macd(bars(rising), { prefix: 'x' });
    expect(col(r, 'xLine')[30]).toBeDefined();
    expect(col(r, 'xSignal')[40]).toBeDefined();
    expect(col(r, 'xHist')[40]).toBeDefined();
    // The signal EMA is taken over a scratch column; it must not survive.
    expect(
      (r as unknown as { column(n: string): unknown }).column('__macdLine__'),
    ).toBeUndefined();
  });

  it('rejects a fast period that is not shorter than the slow one', () => {
    expect(() =>
      macd(bars(rising), { fastPeriod: 26, slowPeriod: 26 }),
    ).toThrow(TypeError);
    expect(() =>
      macd(bars(rising), { fastPeriod: 30, slowPeriod: 26 }),
    ).toThrow(TypeError);
  });

  it('rejects non-positive or fractional periods, and a colliding prefix', () => {
    expect(() => macd(bars(rising), { signalPeriod: 0 })).toThrow(TypeError);
    expect(() => macd(bars(rising), { fastPeriod: 2.5 })).toThrow(TypeError);
    expect(() => macd(bars(rising), { prefix: 'clo' as never })).not.toThrow();
    const withLine = macd(bars(rising));
    expect(() => macd(withLine as never)).toThrow(); // macdLine already there
  });

  it('is all-undefined when the slow period exceeds the series', () => {
    const short = bars([1, 2, 3, 4, 5]);
    const r = macd(short, { fastPeriod: 2, slowPeriod: 9, signalPeriod: 3 });
    for (const name of ['macdLine', 'macdSignal', 'macdHist']) {
      const v = col(r, name);
      expect(v, name).toHaveLength(5);
      expect(
        v.every((x) => x === undefined),
        name,
      ).toBe(true);
    }
  });

  it('runs over a non-default column', () => {
    const r = macd(sma(bars(rising), { period: 2 }), {
      column: 'sma',
      fastPeriod: 3,
      slowPeriod: 7,
      signalPeriod: 4,
      prefix: 'smaMacd',
    });
    expect(col(r, 'smaMacdLine')[20]).toBeDefined();
    expect(col(r, 'smaMacdSignal')[20]).toBeDefined();
  });
});

describe('atr', () => {
  const ohlcSchema = [
    { name: 'time', kind: 'time' },
    { name: 'high', kind: 'number' },
    { name: 'low', kind: 'number' },
    { name: 'close', kind: 'number' },
  ] as const;

  const ohlc = (rows: Array<[number, number, number]>) =>
    new TimeSeries({
      name: 'bars',
      schema: ohlcSchema,
      rows: rows.map(([h, l, c], i) => [i, h, l, c]) as Array<
        [number, number, number, number]
      >,
    });

  /** n bars of a steady 2-wide range with no gaps: TR is exactly 2 every bar. */
  const steady = (n: number): Array<[number, number, number]> =>
    Array.from({ length: n }, () => [101, 99, 100]);

  it('warms up over `period` rows — one more than the average needs', () => {
    // True range needs a PREVIOUS close, so bar 0 has none and a period-bar
    // average of TR first lands on bar `period`, not `period - 1`.
    const v = col(atr(ohlc(steady(10)), { period: 3 }), 'atr');
    expect(v).toHaveLength(10);
    expect(v.slice(0, 3).every((x) => x === undefined)).toBe(true);
    expect(v[3]).toBeCloseTo(2, 12);
  });

  it('takes the widest of the three spans, not just high − low', () => {
    // Bar 1 gaps far above bar 0's close. The three spans are:
    //   high - low            = 119 - 117 =  2
    //   |high - prevClose|    = |119-100| = 19   <- widest
    //   |low  - prevClose|    = |117-100| = 17
    // An implementation using plain high-low range would report 2.
    const v = col(
      atr(
        ohlc([
          [101, 99, 100],
          [119, 117, 118],
          [119, 117, 118],
        ]),
        { period: 1 },
      ),
      'atr',
    );
    expect(v[1]).toBeCloseTo(19, 12);
  });

  it('is the units of the price, and scales with it', () => {
    // ATR does not normalise — deliberately. Doubling every price doubles it.
    const base = col(atr(ohlc(steady(10)), { period: 3 }), 'atr');
    const scaled = col(
      atr(ohlc(steady(10).map(([h, l, c]) => [h * 3, l * 3, c * 3])), {
        period: 3,
      }),
      'atr',
    );
    for (let i = 0; i < base.length; i += 1) {
      if (base[i] === undefined) expect(scaled[i]).toBeUndefined();
      else expect(scaled[i]! / 3).toBeCloseTo(base[i]!, 12);
    }
  });

  it('reads redirected high/low/close columns', () => {
    // The three-input analogue of the uniform `column` rule: never hard-code.
    const s = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'h2', kind: 'number' },
        { name: 'l2', kind: 'number' },
        { name: 'c2', kind: 'number' },
      ] as const,
      rows: Array.from({ length: 10 }, (_, i) => [i, 101, 99, 100]) as Array<
        [number, number, number, number]
      >,
    });
    // No `as never` on the series: the point of this test is that the real
    // schema flows through, so `high: 'h2'` is accepted by
    // `NumericColumnNameForSchema<S>` rather than needing a cast.
    const v = col(
      atr(s, { period: 3, high: 'h2', low: 'l2', close: 'c2' }),
      'atr',
    );
    expect(v[3]).toBeCloseTo(2, 12);
  });

  it('reads all-missing when a named column is absent', () => {
    // A misnamed column reads as all-missing rather than throwing — the same
    // answer `columnValues` gives any study pointed at a column that is not
    // there.
    const v = col(
      atr(ohlc(steady(10)), { period: 3, high: 'nope' as never }),
      'atr',
    );
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('rejects a bad period and a colliding output', () => {
    expect(() => atr(ohlc(steady(5)), { period: 0 })).toThrow(TypeError);
    expect(() => atr(ohlc(steady(5)), { period: 1.5 })).toThrow(TypeError);
    expect(() => atr(ohlc(steady(5)), { output: 'close' })).toThrow();
  });

  it('is all-undefined when the period exceeds the bars available', () => {
    const v = col(atr(ohlc(steady(3)), { period: 9 }), 'atr');
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('momentum', () => {
  // Oscillating on purpose: a linear series has constant momentum, so a
  // lookback shifted by one bar gives the same answer and pins nothing.
  const zig = [100, 103, 101, 106, 102];

  it('is v − v[−period]; the first `period` rows are undefined', () => {
    const v = col(momentum(bars(zig), { period: 2 }), 'momentum');
    expect(v).toHaveLength(5);
    expect(v[0]).toBeUndefined();
    expect(v[1]).toBeUndefined();
    expect(v[2]).toBeCloseTo(1, 12); // 101 − 100
    expect(v[3]).toBeCloseTo(3, 12); // 106 − 103
    expect(v[4]).toBeCloseTo(1, 12); // 102 − 101
  });

  it('defaults to period 10 and the `momentum` output name, and honours both', () => {
    const closes = Array.from({ length: 20 }, (_, i) => 100 + 7 * Math.sin(i));
    const def = momentum(bars(closes));
    const v = col(def, 'momentum');
    expect(v[9]).toBeUndefined();
    expect(v[10]).toBeCloseTo(closes[10]! - closes[0]!, 12);

    const named = momentum(bars(closes), { period: 3, output: 'mom3' });
    expect(col(named, 'mom3')[3]).toBeCloseTo(closes[3]! - closes[0]!, 12);
    expect(col(named, 'momentum')[3]).toBeUndefined(); // no default written
  });

  it('runs over any numeric column, including another study output', () => {
    const src = sma(bars(zig), { period: 2 });
    const m = col(
      momentum(src, { column: 'sma', period: 1, output: 'dm' }),
      'dm',
    );
    const s = col(src, 'sma');
    // sma(2) starts at 1, so the first difference of it lands on 2.
    expect(m[1]).toBeUndefined();
    expect(m[2]).toBeCloseTo(s[2]! - s[1]!, 12);
    expect(m[4]).toBeCloseTo(s[4]! - s[3]!, 12);
  });

  it('a missing cell at either end of the look-back makes the difference missing', () => {
    const withHole = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'close', kind: 'number', required: false },
      ] as const,
      rows: [
        [0, 100],
        [1, 103],
        [2, 101],
        [3, 106],
        [4, undefined],
        [5, 105],
        [6, 109],
        [7, 104],
      ] as Array<[number, number | undefined]>,
    });
    const v = col(momentum(withHole as never, { period: 2 }), 'momentum');
    expect(v[3]).toBeCloseTo(3, 12); // before the hole
    expect(v[4]).toBeUndefined(); // the hole itself
    expect(v[5]).toBeCloseTo(-1, 12); // 105 − 106, straddles it
    expect(v[6]).toBeUndefined(); // the hole is its predecessor
    expect(v[7]).toBeCloseTo(-1, 12); // recovered: 104 − 105
  });

  it('rejects a non-positive or fractional period, and a colliding output', () => {
    expect(() => momentum(bars(zig), { period: 0 })).toThrow(TypeError);
    expect(() => momentum(bars(zig), { period: 1.5 })).toThrow(TypeError);
    expect(() => momentum(bars(zig), { output: 'close' })).toThrow();
  });

  it('is all-undefined when the period reaches past the first bar', () => {
    const v = col(momentum(bars(zig), { period: 5 }), 'momentum');
    expect(v).toHaveLength(5);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('historicalVolatility', () => {
  // Oscillating on purpose: a geometric series has CONSTANT log returns and
  // therefore zero volatility, so a test on it cannot tell log from simple
  // returns, population from sample, or a right lookback from a shifted one.
  const six = [100, 102, 99, 103, 98, 104];
  // pandas: np.log(s).diff().rolling(3).std(ddof=0)
  const sixP3 = [
    0.029217496740224568, 0.038309335084548916, 0.04749393808548082,
  ];

  it('is the population σ of log returns over `period` bars, as a decimal', () => {
    const v = col(
      historicalVolatility(bars(six), { period: 3, annualize: 1 }),
      'hv',
    );
    expect(v).toHaveLength(6);
    expect(v[3]).toBeCloseTo(sixP3[0]!, 12);
    expect(v[4]).toBeCloseTo(sixP3[1]!, 12);
    expect(v[5]).toBeCloseTo(sixP3[2]!, 12);
  });

  it('needs period+1 bars: the first `period` rows are undefined', () => {
    // HV is a σ of RETURNS, so `period` of them need `period + 1` prices —
    // the same off-by-one RSI and ATR have. A rolling window over the
    // returns column counted by ROWS would land one bar early, over one
    // return too few.
    const v = col(
      historicalVolatility(bars(six), { period: 3, annualize: 1 }),
      'hv',
    );
    expect(v.slice(0, 3).every((x) => x === undefined)).toBe(true);
    expect(v[3]).toBeDefined();
    const v5 = col(historicalVolatility(bars(six), { period: 5 }), 'hv');
    expect(v5[4]).toBeUndefined();
    expect(v5[5]).toBeDefined();
  });

  it('annualises by √annualize, and defaults annualize to 252', () => {
    const raw = col(
      historicalVolatility(bars(six), { period: 3, annualize: 1 }),
      'hv',
    );
    const def = col(historicalVolatility(bars(six), { period: 3 }), 'hv');
    const hundred = col(
      historicalVolatility(bars(six), { period: 3, annualize: 100 }),
      'hv',
    );
    // pandas: ... * sqrt(252) — the annualised number, not just the ratio.
    expect(def[3]).toBeCloseTo(0.46381338183884735, 12);
    expect(def[5]).toBeCloseTo(raw[5]! * Math.sqrt(252), 12);
    expect(hundred[5]).toBeCloseTo(raw[5]! * 10, 12);
  });

  it('is a POPULATION σ (ddof = 0), the package convention', () => {
    // Two returns: the population σ is half their gap; a sample σ would be
    // 1/√2 of it — 41% larger.
    const v = col(
      historicalVolatility(bars(six), { period: 2, annualize: 1 }),
      'hv',
    );
    const r1 = Math.log(102 / 100);
    const r2 = Math.log(99 / 102);
    expect(v[2]).toBeCloseTo(Math.abs(r1 - r2) / 2, 12);
  });

  it('uses LOG returns, not simple returns', () => {
    // Up 100% then down 50%: log returns are ±ln 2 (σ = ln 2), simple
    // returns are +1 and −0.5 (σ = 0.75). Symmetry is the point of the log.
    const v = col(
      historicalVolatility(bars([100, 200, 100]), { period: 2, annualize: 1 }),
      'hv',
    );
    expect(v[2]).toBeCloseTo(Math.LN2, 12);
  });

  it('defaults to period 20 and the `hv` output name, and honours both', () => {
    const closes = Array.from({ length: 25 }, (_, i) => 100 + 5 * Math.sin(i));
    const def = historicalVolatility(bars(closes));
    expect(col(def, 'hv')[19]).toBeUndefined();
    expect(col(def, 'hv')[20]).toBeDefined();

    const named = historicalVolatility(bars(closes), {
      period: 3,
      output: 'vol',
    });
    expect(col(named, 'vol')[3]).toBeDefined();
    expect(col(named, 'hv')[3]).toBeUndefined(); // no default column written
  });

  it('a non-positive price has no log return', () => {
    // The guard is explicit: `Math.log(-4 / -5)` is a finite number that is
    // not a return, so a run of negative prices must read as missing, not
    // as a volatility.
    const neg = col(
      historicalVolatility(bars([-5, -4, -3, -2, -1]), {
        period: 2,
        annualize: 1,
      }),
      'hv',
    );
    expect(neg).toHaveLength(5);
    expect(neg.every((x) => x === undefined)).toBe(true);

    // A zero price kills the two returns that touch it (its own and the
    // next bar's). At period 2 the window that holds only those two has
    // nothing to compute from; once it has passed the study recovers.
    const zero = col(
      historicalVolatility(bars([100, 101, 0, 102, 103, 104]), {
        period: 2,
        annualize: 1,
      }),
      'hv',
    );
    expect(zero[3]).toBeUndefined();
    expect(zero[5]).toBeCloseTo(
      Math.abs(Math.log(103 / 102) - Math.log(104 / 103)) / 2,
      12,
    );
  });

  it('a leading gap shifts the start rather than shrinking the first window', () => {
    // Over sma(3): the source starts at bar 2, the first return at bar 3,
    // and the first FULL window of 4 returns ends on bar 6. A rows-counted
    // window would emit at bar 3 over a single return — a σ of 0, read as
    // "no volatility" where there is simply no data yet.
    const wavy = Array.from(
      { length: 30 },
      (_, i) => 100 + 6 * Math.sin(i / 2.5) + i * 0.1,
    );
    const src = sma(bars(wavy), { period: 3 });
    const v = col(
      historicalVolatility(src, { column: 'sma', period: 4, annualize: 1 }),
      'hv',
    );
    expect(v.slice(0, 6).every((x) => x === undefined)).toBe(true);
    expect(v[6]).toBeDefined();
    // And the value is the σ over exactly those four returns of the SMA.
    const s = col(src, 'sma') as number[];
    const r = [3, 4, 5, 6].map((i) => Math.log(s[i]! / s[i - 1]!));
    const mean = r.reduce((a, b) => a + b, 0) / 4;
    const sd = Math.sqrt(r.reduce((a, b) => a + (b - mean) ** 2, 0) / 4);
    expect(v[6]).toBeCloseTo(sd, 12);
  });

  it('rejects a bad period or annualize, and a colliding output', () => {
    expect(() => historicalVolatility(bars(six), { period: 0 })).toThrow(
      TypeError,
    );
    expect(() => historicalVolatility(bars(six), { period: 2.5 })).toThrow(
      TypeError,
    );
    expect(() => historicalVolatility(bars(six), { annualize: 0 })).toThrow(
      /annualize/,
    );
    expect(() => historicalVolatility(bars(six), { annualize: -252 })).toThrow(
      /annualize/,
    );
    expect(() =>
      historicalVolatility(bars(six), { annualize: Number.NaN }),
    ).toThrow(/annualize/);
    expect(() =>
      historicalVolatility(bars(six), { output: 'close' }),
    ).toThrow();
  });

  it('is all-undefined when the period exceeds the available returns', () => {
    // Three prices give two returns; a period of 3 needs a fourth price.
    const v = col(
      historicalVolatility(bars([100, 102, 99]), { period: 3 }),
      'hv',
    );
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
    expect(col(historicalVolatility(bars([100]), { period: 1 }), 'hv')).toEqual(
      [undefined],
    );
  });

  it('an interior bad price yields σ = 0 over the one surviving return', () => {
    // Found by the Layer-2 review of #686 and pinned so it is deliberate.
    // The bad price at bar 2 kills the returns ending at bars 2 and 3. At
    // period 2, the windows ending at bars 2 and 4 each hold ONE finite
    // return, whose σ is 0 — which reads as "no volatility", the opposite of
    // a corrupt price. That is the package's rolling contract (fewer
    // contributors, not a missing window) and a documented divergence from
    // pandas, which gives NaN there.
    const v = col(
      historicalVolatility(bars([100, 101, 0, 102, 103, 104]), {
        period: 2,
        annualize: 1,
      }),
      'hv',
    );
    expect(v[2]).toBe(0); // window = returns at bars 1, 2 → one finite
    expect(v[3]).toBeUndefined(); // returns at bars 2, 3 → none finite
    expect(v[4]).toBe(0); // returns at bars 3, 4 → one finite
    expect(v[5]).toBeGreaterThan(0); // both returns present again
  });
});

/*
 * The range-position studies share one fixture, chosen so that a 3-bar
 * window's extremes are NOT always at its edges (bar 2's HH is bar 1; bar 4's
 * LL is bar 3) and the close is never exactly ON an extreme — the two ways a
 * fixture goes value-blind for these studies.
 *
 *        high  low  close   3-bar HH  3-bar LL   fast %K        %R
 *   0     12    8    10
 *   1     15   11    13
 *   2     14    9    12        15        8       400/7=57.14   −300/7=−42.86
 *   3     11    7     9        15        7       200/8=25      −75
 *   4     13   10    12        14        7       500/7=71.43   −200/7=−28.57
 *   5     16   12    15        16        7       800/9=88.89   −100/9=−11.11
 */
const rangeSchema = [
  { name: 'time', kind: 'time' },
  { name: 'high', kind: 'number' },
  { name: 'low', kind: 'number' },
  { name: 'close', kind: 'number' },
] as const;

const rangeBars = (rows: Array<[number, number, number]>) =>
  new TimeSeries({
    name: 'bars',
    schema: rangeSchema,
    rows: rows.map(([h, l, c], i) => [i, h, l, c]) as Array<
      [number, number, number, number]
    >,
  });

const RANGE_FIXTURE: Array<[number, number, number]> = [
  [12, 8, 10],
  [15, 11, 13],
  [14, 9, 12],
  [11, 7, 9],
  [13, 10, 12],
  [16, 12, 15],
];
const FAST_K = [400 / 7, 25, 500 / 7, 800 / 9];

/** A longer oscillating bar series with genuine range variation. */
const wavyBars = (n: number): Array<[number, number, number]> =>
  Array.from({ length: n }, (_, i) => {
    const c = 100 + 8 * Math.sin(i / 3) + 0.2 * i;
    return [
      c + 0.4 + 0.6 * Math.abs(Math.sin(i / 2)),
      c - 0.3 - 0.5 * Math.abs(Math.cos(i / 2.5)),
      c,
    ];
  });

describe('stochastic', () => {
  it('computes fast %K, then smooths it twice, on the hand-worked fixture', () => {
    const r = stochastic(rangeBars(RANGE_FIXTURE), {
      kPeriod: 3,
      slowing: 2,
      dPeriod: 2,
    });
    const k = col(r, 'stochK');
    const d = col(r, 'stochD');
    expect(k).toHaveLength(6);
    // %K = 2-bar SMA of fast %K: first at kPeriod + slowing − 2 = 3.
    expect(k.slice(0, 3).every((x) => x === undefined)).toBe(true);
    expect(k[3]).toBeCloseTo((FAST_K[0]! + FAST_K[1]!) / 2, 12); // 41.07
    expect(k[4]).toBeCloseTo((FAST_K[1]! + FAST_K[2]!) / 2, 12); // 48.21
    expect(k[5]).toBeCloseTo((FAST_K[2]! + FAST_K[3]!) / 2, 12); // 80.16
    // %D = 2-bar SMA of %K: first at kPeriod + slowing + dPeriod − 3 = 4.
    expect(d.slice(0, 4).every((x) => x === undefined)).toBe(true);
    expect(d[4]).toBeCloseTo((k[3]! + k[4]!) / 2, 12); // 44.64
    expect(d[5]).toBeCloseTo((k[4]! + k[5]!) / 2, 12); // 64.19
  });

  it('slowing 1 is the fast stochastic: %K unsmoothed', () => {
    const r = stochastic(rangeBars(RANGE_FIXTURE), {
      kPeriod: 3,
      slowing: 1,
      dPeriod: 2,
    });
    const k = col(r, 'stochK');
    expect(k.slice(0, 2).every((x) => x === undefined)).toBe(true);
    for (let i = 2; i < 6; i += 1) {
      expect(k[i], `bar ${i}`).toBeCloseTo(FAST_K[i - 2]!, 12);
    }
    expect(col(r, 'stochD')[3]).toBeCloseTo((FAST_K[0]! + FAST_K[1]!) / 2, 12);
  });

  it('is undefined on a flat window, and the smoothing carries the gap until it leaves', () => {
    // Bars 0–3 have no range at all. fast %K is 0/0 there — no value, not
    // TA-Lib's 0 — and a %K window that contains such a bar has no average
    // either. Once the flat bars leave the windows, everything recovers.
    const flatThenMoving = rangeBars([
      [10, 10, 10],
      [10, 10, 10],
      [10, 10, 10],
      [10, 10, 10],
      [12, 9, 11], // fast %K 66.67
      [13, 10, 12], // 75
      [14, 11, 13], // 75
      [15, 12, 14], // 75
    ]);
    const r = stochastic(flatThenMoving, {
      kPeriod: 2,
      slowing: 2,
      dPeriod: 2,
    });
    const k = col(r, 'stochK');
    const d = col(r, 'stochD');
    expect(k.slice(0, 5).every((x) => x === undefined)).toBe(true); // 4's window holds bar 3
    expect(k[5]).toBeCloseTo((200 / 3 + 75) / 2, 12); // 70.83
    expect(k[6]).toBeCloseTo(75, 12);
    expect(d.slice(0, 6).every((x) => x === undefined)).toBe(true);
    expect(d[6]).toBeCloseTo((k[5]! + 75) / 2, 12);
    expect(d[7]).toBeCloseTo(75, 12);
  });

  it('an interior gap in close costs one fast %K and the windows over it, then recovers', () => {
    const withHole = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'high', kind: 'number' },
        { name: 'low', kind: 'number' },
        { name: 'close', kind: 'number', required: false },
      ] as const,
      rows: RANGE_FIXTURE.map(([h, l, c], i) => [
        i,
        h,
        l,
        i === 3 ? undefined : c,
      ]) as Array<[number, number, number, number | undefined]>,
    });
    const k = col(
      stochastic(withHole as never, { kPeriod: 2, slowing: 2, dPeriod: 1 }),
      'stochK',
    );
    expect(k[2]).toBeDefined();
    expect(k[3]).toBeUndefined(); // fast %K[3] has no close
    expect(k[4]).toBeUndefined(); // its window still holds bar 3
    expect(k[5]).toBeDefined(); // recovered — a window, not a recursion
  });

  it('stays within 0..100 when low ≤ close ≤ high', () => {
    const r = stochastic(rangeBars(wavyBars(60)), { kPeriod: 5 });
    let seen = 0;
    for (const name of ['stochK', 'stochD']) {
      for (const x of col(r, name)) {
        if (x === undefined) continue;
        seen += 1;
        expect(x).toBeGreaterThanOrEqual(0);
        expect(x).toBeLessThanOrEqual(100);
      }
    }
    expect(seen).toBeGreaterThan(100);
  });

  it('defaults to (14, 3, 3) and the `stoch` prefix, and honours a prefix', () => {
    const def = stochastic(rangeBars(wavyBars(40)));
    expect(col(def, 'stochK')[14]).toBeUndefined(); // 14 + 3 − 2 = 15
    expect(col(def, 'stochK')[15]).toBeDefined();
    expect(col(def, 'stochD')[16]).toBeUndefined(); // 14 + 3 + 3 − 3 = 17
    expect(col(def, 'stochD')[17]).toBeDefined();

    const named = stochastic(rangeBars(wavyBars(40)), { prefix: 'st' });
    expect(col(named, 'stK')[15]).toBeDefined();
    expect(col(named, 'stD')[17]).toBeDefined();
    expect(col(named, 'stochK')[15]).toBeUndefined(); // no default column written
  });

  it('reads redirected high/low/close columns', () => {
    const s = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'h2', kind: 'number' },
        { name: 'l2', kind: 'number' },
        { name: 'c2', kind: 'number' },
      ] as const,
      rows: RANGE_FIXTURE.map(([h, l, c], i) => [i, h, l, c]) as Array<
        [number, number, number, number]
      >,
    });
    // No cast on the series: the schema flows through the option types.
    const k = col(
      stochastic(s, {
        kPeriod: 3,
        slowing: 1,
        dPeriod: 1,
        high: 'h2',
        low: 'l2',
        close: 'c2',
      }),
      'stochK',
    );
    expect(k[2]).toBeCloseTo(FAST_K[0]!, 12);
  });

  it('reads all-missing when a named column is absent', () => {
    const k = col(
      stochastic(rangeBars(RANGE_FIXTURE), {
        kPeriod: 2,
        low: 'nope' as never,
      }),
      'stochK',
    );
    expect(k.every((x) => x === undefined)).toBe(true);
  });

  it('rejects bad periods and a colliding prefix', () => {
    const s = rangeBars(RANGE_FIXTURE);
    expect(() => stochastic(s, { kPeriod: 0 })).toThrow(TypeError);
    expect(() => stochastic(s, { slowing: 0 })).toThrow(TypeError);
    expect(() => stochastic(s, { dPeriod: 1.5 })).toThrow(TypeError);
    // `${prefix}K` collides with an existing column.
    const withK = s.withColumn('xK', new Float64Array(6));
    expect(() => stochastic(withK, { prefix: 'x' })).toThrow(/collides/);
  });

  it('is all-undefined when the period exceeds the bars available', () => {
    const r = stochastic(rangeBars(RANGE_FIXTURE), { kPeriod: 9 });
    expect(col(r, 'stochK')).toHaveLength(6);
    expect(col(r, 'stochK').every((x) => x === undefined)).toBe(true);
    expect(col(r, 'stochD').every((x) => x === undefined)).toBe(true);
  });
});

describe('williamsR', () => {
  it('is −100 · (HH − close) / (HH − LL) on the hand-worked fixture', () => {
    const v = col(
      williamsR(rangeBars(RANGE_FIXTURE), { period: 3 }),
      'williamsR',
    );
    expect(v).toHaveLength(6);
    expect(v.slice(0, 2).every((x) => x === undefined)).toBe(true);
    expect(v[2]).toBeCloseTo(-300 / 7, 12); // −42.86
    expect(v[3]).toBeCloseTo(-75, 12);
    expect(v[4]).toBeCloseTo(-200 / 7, 12); // −28.57
    expect(v[5]).toBeCloseTo(-100 / 9, 12); // −11.11
  });

  it('is the fast stochastic %K shifted down by 100, bar for bar', () => {
    // The identity both studies are built on; a sign or term error in either
    // one breaks it. (Both against the oracle rules out an error in both.)
    const bars = rangeBars(wavyBars(50));
    const r = col(williamsR(bars, { period: 6 }), 'williamsR');
    const k = col(stochastic(bars, { kPeriod: 6, slowing: 1 }), 'stochK');
    expect(r).toHaveLength(k.length);
    for (let i = 0; i < r.length; i += 1) {
      if (k[i] === undefined) expect(r[i], `bar ${i}`).toBeUndefined();
      else expect(r[i], `bar ${i}`).toBeCloseTo(k[i]! - 100, 12);
    }
  });

  it('stays within −100..0 when low ≤ close ≤ high', () => {
    const v = col(
      williamsR(rangeBars(wavyBars(60)), { period: 5 }),
      'williamsR',
    );
    const defined = v.filter((x): x is number => x !== undefined);
    expect(defined.length).toBe(56);
    for (const x of defined) {
      expect(x).toBeGreaterThanOrEqual(-100);
      expect(x).toBeLessThanOrEqual(0);
    }
    // And moves: a value-blind fixture would not.
    expect(new Set(defined).size).toBeGreaterThan(10);
  });

  it('is undefined on a flat window (0/0 has no position)', () => {
    const flat = rangeBars(Array.from({ length: 6 }, () => [10, 10, 10]));
    const v = col(williamsR(flat, { period: 3 }), 'williamsR');
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('defaults to period 14 and the `williamsR` output, and honours both', () => {
    const def = williamsR(rangeBars(wavyBars(30)));
    expect(col(def, 'williamsR')[12]).toBeUndefined();
    expect(col(def, 'williamsR')[13]).toBeDefined();
    const named = williamsR(rangeBars(wavyBars(30)), {
      period: 4,
      output: 'pr',
    });
    expect(col(named, 'pr')[3]).toBeDefined();
    expect(col(named, 'williamsR')[3]).toBeUndefined();
  });

  it('reads redirected columns, and all-missing when one is absent', () => {
    const s = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'h2', kind: 'number' },
        { name: 'l2', kind: 'number' },
        { name: 'c2', kind: 'number' },
      ] as const,
      rows: RANGE_FIXTURE.map(([h, l, c], i) => [i, h, l, c]) as Array<
        [number, number, number, number]
      >,
    });
    const v = col(
      williamsR(s, { period: 3, high: 'h2', low: 'l2', close: 'c2' }),
      'williamsR',
    );
    expect(v[3]).toBeCloseTo(-75, 12);
    const missing = col(
      williamsR(rangeBars(RANGE_FIXTURE), {
        period: 3,
        close: 'nope' as never,
      }),
      'williamsR',
    );
    expect(missing.every((x) => x === undefined)).toBe(true);
  });

  it('rejects a bad period and a colliding output', () => {
    const s = rangeBars(RANGE_FIXTURE);
    expect(() => williamsR(s, { period: 0 })).toThrow(TypeError);
    expect(() => williamsR(s, { period: 2.5 })).toThrow(TypeError);
    expect(() => williamsR(s, { output: 'close' })).toThrow(/collides/);
  });
});

describe('donchian', () => {
  it('is the window high, the window low, and their midpoint', () => {
    const r = donchian(rangeBars(RANGE_FIXTURE), { period: 3 });
    expect(col(r, 'dcUpper')).toEqual([undefined, undefined, 15, 15, 14, 16]);
    expect(col(r, 'dcLower')).toEqual([undefined, undefined, 8, 7, 7, 7]);
    expect(col(r, 'dcMiddle')).toEqual([
      undefined,
      undefined,
      11.5,
      11,
      10.5,
      11.5,
    ]);
  });

  it('edges are exactly rollingMax(high) and rollingMin(low)', () => {
    const bars = rangeBars(wavyBars(50));
    const r = donchian(bars, { period: 7 });
    expect(col(r, 'dcUpper')).toEqual(
      col(rollingMax(bars, { period: 7, column: 'high' }), 'max'),
    );
    expect(col(r, 'dcLower')).toEqual(
      col(rollingMin(bars, { period: 7, column: 'low' }), 'min'),
    );
  });

  it('brackets the close when low ≤ close ≤ high', () => {
    const bars = rangeBars(wavyBars(50));
    const r = donchian(bars, { period: 7 });
    const upper = col(r, 'dcUpper');
    const lower = col(r, 'dcLower');
    const close = col(bars, 'close');
    for (let i = 6; i < 50; i += 1) {
      expect(upper[i]!, `bar ${i}`).toBeGreaterThanOrEqual(close[i]!);
      expect(lower[i]!, `bar ${i}`).toBeLessThanOrEqual(close[i]!);
    }
  });

  it('skips a missing high rather than emptying the channel', () => {
    // Core's reducer policy, and the existing rollingMax contract: the edge
    // is the extreme of what the window holds.
    const gappy = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'high', kind: 'number', required: false },
        { name: 'low', kind: 'number' },
      ] as const,
      rows: RANGE_FIXTURE.map(([h, l], i) => [
        i,
        i === 1 ? undefined : h,
        l,
      ]) as Array<[number, number | undefined, number]>,
    });
    const upper = col(donchian(gappy as never, { period: 3 }), 'dcUpper');
    expect(upper[2]).toBe(14); // max(12, —, 14), not 15 and not undefined
    expect(upper[3]).toBe(14);
  });

  it('defaults to period 20 and the `dc` prefix, and honours a prefix', () => {
    const def = donchian(rangeBars(wavyBars(30)));
    expect(col(def, 'dcUpper')[18]).toBeUndefined();
    expect(col(def, 'dcUpper')[19]).toBeDefined();
    const named = donchian(rangeBars(wavyBars(30)), {
      period: 3,
      prefix: 'chan',
    });
    for (const name of ['chanUpper', 'chanLower', 'chanMiddle']) {
      expect(col(named, name)[2], name).toBeDefined();
    }
    expect(col(named, 'dcUpper')[2]).toBeUndefined();
  });

  it('reads redirected high/low columns', () => {
    const s = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'h2', kind: 'number' },
        { name: 'l2', kind: 'number' },
      ] as const,
      rows: RANGE_FIXTURE.map(([h, l], i) => [i, h, l]) as Array<
        [number, number, number]
      >,
    });
    const r = donchian(s, { period: 3, high: 'h2', low: 'l2' });
    expect(col(r, 'dcUpper')[4]).toBe(14);
    expect(col(r, 'dcLower')[4]).toBe(7);
  });

  it('rejects a bad period and a colliding prefix', () => {
    const s = rangeBars(RANGE_FIXTURE);
    expect(() => donchian(s, { period: 0 })).toThrow(TypeError);
    expect(() => donchian(s, { period: 1.5 })).toThrow(TypeError);
    const withMiddle = s.withColumn('xMiddle', new Float64Array(6));
    expect(() => donchian(withMiddle, { prefix: 'x' })).toThrow(/collides/);
  });

  it('is all-undefined when the period exceeds the bars available', () => {
    const r = donchian(rangeBars(RANGE_FIXTURE), { period: 9 });
    for (const name of ['dcUpper', 'dcLower', 'dcMiddle']) {
      expect(col(r, name), name).toHaveLength(6);
      expect(
        col(r, name).every((x) => x === undefined),
        name,
      ).toBe(true);
    }
  });
});

/** Bars with a volume column, for the volume studies. Row = [h, l, c, v]. */
const ohlcvSchema = [
  { name: 'time', kind: 'time' },
  { name: 'high', kind: 'number' },
  { name: 'low', kind: 'number' },
  { name: 'close', kind: 'number' },
  { name: 'volume', kind: 'number' },
] as const;

const ohlcv = (rows: Array<[number, number, number, number]>) =>
  new TimeSeries({
    name: 'bars',
    schema: ohlcvSchema,
    rows: rows.map(([h, l, c, v], i) => [i, h, l, c, v]) as Array<
      [number, number, number, number, number]
    >,
  });

/** Close/volume bars with a fixed 1-wide range around the close. */
const cv = (closes: number[], volumes: number[]) =>
  ohlcv(closes.map((c, i) => [c + 0.5, c - 0.5, c, volumes[i]!]));

/** Bars whose close (and/or volume) may be missing, for the gap cases. */
const cvGappy = (
  closes: Array<number | undefined>,
  volumes: Array<number | undefined>,
) =>
  new TimeSeries({
    name: 'bars',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'close', kind: 'number', required: false },
      { name: 'volume', kind: 'number', required: false },
    ] as const,
    rows: closes.map((c, i) => [i, c, volumes[i]]) as Array<
      [number, number | undefined, number | undefined]
    >,
  });

describe('obv', () => {
  // TA-Lib's own answer for this sequence (checked by running it): the seed
  // is volume[0]; up adds, down subtracts, unchanged adds nothing.
  const closes = [10, 11, 11, 9, 12, 12, 8];
  const volumes = [100, 200, 300, 400, 500, 600, 700];
  const expected = [100, 300, 300, -100, 400, 400, -300];

  it('accumulates signed volume from a volume[0] seed, exactly as TA-Lib', () => {
    const v = col(obv(cv(closes, volumes)), 'obv');
    expect(v).toEqual(expected);
  });

  it('has no warm-up and no period: defined from bar 0', () => {
    const v = col(obv(cv([5], [42])), 'obv');
    expect(v).toEqual([42]);
  });

  it('defaults to the `obv` output name, and honours a custom one', () => {
    const named = obv(cv(closes, volumes), { output: 'balance' });
    expect(col(named, 'balance')).toEqual(expected);
    expect(col(named, 'obv').every((x) => x === undefined)).toBe(true);
  });

  it('reads redirected close/volume columns', () => {
    const s = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'px', kind: 'number' },
        { name: 'qty', kind: 'number' },
      ] as const,
      rows: closes.map((c, i) => [i, c, volumes[i]!]) as Array<
        [number, number, number]
      >,
    });
    // No cast on the series: the schema flows through, so `close: 'px'` is
    // accepted by `NumericColumnNameForSchema<S>`.
    expect(col(obv(s, { close: 'px', volume: 'qty' }), 'obv')).toEqual(
      expected,
    );
  });

  it('reads all-missing when a named column is absent', () => {
    const v = col(obv(cv(closes, volumes), { volume: 'nope' as never }), 'obv');
    expect(v).toHaveLength(closes.length);
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('shifts the seed past a leading gap in volume', () => {
    // Close is present on bar 0 but volume is not: the seed moves to bar 1
    // and is bar 1's whole volume, whatever direction close took to get
    // there (it FELL — a direction-keeping implementation would say −200).
    const v = col(obv(cvGappy([10, 9, 12], [undefined, 200, 300])), 'obv');
    expect(v).toEqual([undefined, 200, 500]);
  });

  it('propagates an interior gap in close to the end', () => {
    // A running sum has no local answer for a gap: the level is unknown
    // from the unknown term on. TA-Lib (0.7.1, measured) reports
    // 300, 300, 300, 300, -400 from bar 2 on — 400 out at the gap bar and
    // 100 out thereafter against the gap-free -100, 400, 400, -300 — never
    // recovered, presented as a value.
    const v = col(
      obv(cvGappy([10, 11, 11, undefined, 12, 12, 8], volumes)),
      'obv',
    );
    expect(v.slice(0, 3)).toEqual([100, 300, 300]);
    expect(v.slice(3).every((x) => x === undefined)).toBe(true);
  });

  it('propagates an interior gap in volume to the end', () => {
    const v = col(
      obv(cvGappy(closes, [100, 200, 300, undefined, 500, 600, 700])),
      'obv',
    );
    expect(v.slice(0, 3)).toEqual([100, 300, 300]);
    expect(v.slice(3).every((x) => x === undefined)).toBe(true);
  });

  it('rejects a colliding output', () => {
    expect(() => obv(cv(closes, volumes), { output: 'close' })).toThrow();
  });
});

describe('vwap', () => {
  it('weights typical price by volume — not the mean of typical prices', () => {
    // tp = 10.5 on 1 unit, tp = 20 on 3 units:
    //   weighted  (10.5·1 + 20·3) / 4 = 17.625
    //   plain     (10.5 + 20) / 2     = 15.25
    const v = col(
      vwap(
        ohlcv([
          [12, 9, 10.5, 1],
          [22, 18, 20, 3],
        ]),
        { period: 2 },
      ),
      'vwap',
    );
    expect(v[0]).toBeUndefined();
    expect(v[1]).toBeCloseTo(17.625, 12);
  });

  it('warms up over `period − 1` rows, length-preserving', () => {
    const s = cv([10, 11, 12, 13, 14, 15], [1, 2, 3, 4, 5, 6]);
    const v = col(vwap(s, { period: 4 }), 'vwap');
    expect(v).toHaveLength(6);
    expect(v.slice(0, 3).every((x) => x === undefined)).toBe(true);
    // (10·1 + 11·2 + 12·3 + 13·4) / 10 = 12
    expect(v[3]).toBeCloseTo(12, 12);
    // Slid one bar: (11·2 + 12·3 + 13·4 + 14·5) / 14
    expect(v[4]).toBeCloseTo(180 / 14, 12);
  });

  it('is the typical price at period 1', () => {
    const v = col(
      vwap(
        ohlcv([
          [12, 9, 10, 7],
          [15, 12, 15, 1],
        ]),
        { period: 1 },
      ),
      'vwap',
    );
    expect(v[0]).toBeCloseTo(31 / 3, 12);
    expect(v[1]).toBeCloseTo(14, 12);
  });

  it('is undefined on a window with no volume, and resumes after it', () => {
    // Σvolume = 0: nothing to weight by. Not the plain mean, and not 0.
    const v = col(
      vwap(cv([10, 20, 30, 40], [0, 0, 5, 5]), { period: 2 }),
      'vwap',
    );
    expect(v[1]).toBeUndefined();
    expect(v[2]).toBeCloseTo(30, 12); // (0·20 + 5·30) / 5
    expect(v[3]).toBeCloseTo(35, 12);
  });

  it('drops a bar with a missing close from BOTH sums', () => {
    // The middle bar carries almost all the volume but has no price. Its
    // volume must leave the denominator with it, or the window reads
    // (10·1 + 30·3) / 104 ≈ 0.96 instead of the VWAP of the bars present.
    const s = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'high', kind: 'number' },
        { name: 'low', kind: 'number' },
        { name: 'close', kind: 'number', required: false },
        { name: 'volume', kind: 'number' },
      ] as const,
      rows: [
        [0, 10, 10, 10, 1],
        [1, 20, 20, undefined, 100],
        [2, 30, 30, 30, 3],
      ] as Array<[number, number, number, number | undefined, number]>,
    });
    const v = col(vwap(s as never, { period: 3 }), 'vwap');
    expect(v[2]).toBeCloseTo((10 + 90) / 4, 12);
  });

  it('defaults to the `vwap` output name, honours a custom one, and reads redirected columns', () => {
    const rows: Array<[number, number, number, number]> = [
      [12, 9, 10.5, 1],
      [22, 18, 20, 3],
    ];
    const named = vwap(ohlcv(rows), { period: 2, output: 'avgPx' });
    expect(col(named, 'avgPx')[1]).toBeCloseTo(17.625, 12);
    expect(col(named, 'vwap')[1]).toBeUndefined();

    const s = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'h2', kind: 'number' },
        { name: 'l2', kind: 'number' },
        { name: 'c2', kind: 'number' },
        { name: 'v2', kind: 'number' },
      ] as const,
      rows: rows.map(([h, l, c, v], i) => [i, h, l, c, v]) as Array<
        [number, number, number, number, number]
      >,
    });
    const v = col(
      vwap(s, { period: 2, high: 'h2', low: 'l2', close: 'c2', volume: 'v2' }),
      'vwap',
    );
    expect(v[1]).toBeCloseTo(17.625, 12);
  });

  it('reads all-missing when a named column is absent', () => {
    const v = col(
      vwap(cv([10, 11, 12], [1, 2, 3]), { period: 2, high: 'nope' as never }),
      'vwap',
    );
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('requires a period, rejects a bad one, and rejects a colliding output', () => {
    const s = cv([10, 11, 12], [1, 2, 3]);
    expect(() => vwap(s, {} as never)).toThrow(TypeError);
    expect(() => vwap(s, { period: 0 })).toThrow(TypeError);
    expect(() => vwap(s, { period: 1.5 })).toThrow(TypeError);
    expect(() => vwap(s, { period: 2, output: 'close' })).toThrow();
  });

  it('is all-undefined when the period exceeds the bars available', () => {
    const v = col(vwap(cv([10, 11, 12], [1, 2, 3]), { period: 9 }), 'vwap');
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

/* -------------------------------------------------------------------------- */
/* The K2 consumers: channels (keltner, atrBands) and smoothed rates          */
/* (qstick, trix, coppock).                                                    */
/* -------------------------------------------------------------------------- */

const k2OhlcSchema = [
  { name: 'time', kind: 'time' },
  { name: 'high', kind: 'number' },
  { name: 'low', kind: 'number' },
  { name: 'close', kind: 'number' },
] as const;

const k2Bars = (rows: Array<[number, number, number]>) =>
  new TimeSeries({
    name: 'bars',
    schema: k2OhlcSchema,
    rows: rows.map(([h, l, c], i) => [i, h, l, c]) as Array<
      [number, number, number, number]
    >,
  });

/** `n` bars of a steady 2-wide range around 100: typical price is exactly
 *  100 and true range exactly 2 on every bar but the first, so every value
 *  below can be written out by hand. (The oracle covers the varied case bar
 *  for bar; this fixture exists so the assembly can be checked without a
 *  reference — the same trick the `atr` block already uses.) */
const k2Steady = (n: number) =>
  k2Bars(Array.from({ length: n }, () => [101, 99, 100]));

/** Wavy OHLC bars: the range varies, the close is never on an extreme, and
 *  the bar-to-bar moves are large enough that the gap terms of true range
 *  win on some bars. */
const k2Wavy = (n: number) =>
  k2Bars(
    Array.from({ length: n }, (_, i) => {
      const c = 100 + 8 * Math.sin(i / 3) + 0.2 * i;
      return [
        c + 0.4 + 0.6 * Math.abs(Math.sin(i / 2)),
        c - 0.3 - 0.5 * Math.abs(Math.cos(i / 2.5)),
        c,
      ] as [number, number, number];
    }),
  );

/** A wavy close-only series — the input the rate studies read. */
const k2Closes = (n: number) =>
  Array.from({ length: n }, (_, i) => 100 + 6 * Math.sin(i / 4) + 0.1 * i);

describe('keltner', () => {
  it('is MA(typical price) ± multiplier × ATR, hand-checked', () => {
    // Typical price is (101 + 99 + 100)/3 = 100 on every bar, so a 3-bar SMA
    // of it is 100 from bar 2. True range is max(2, |101−100|, |99−100|) = 2
    // from bar 1 (bar 0 has no previous close), so ATR(3) = 2 from bar 3.
    // The bands are then 100 ± 2 × 2.
    const r = keltner(k2Steady(8), {
      period: 3,
      atrPeriod: 3,
      multiplier: 2,
      maType: 'sma',
    });
    expect(col(r, 'kcMiddle')).toEqual([
      undefined,
      undefined,
      100,
      100,
      100,
      100,
      100,
      100,
    ]);
    expect(col(r, 'kcUpper')).toEqual([
      undefined,
      undefined,
      undefined,
      104,
      104,
      104,
      104,
      104,
    ]);
    expect(col(r, 'kcLower')).toEqual([
      undefined,
      undefined,
      undefined,
      96,
      96,
      96,
      96,
      96,
    ]);
  });

  it('warms up per column: the centre where it is defined, the bands at max(centre, ATR)', () => {
    // The `macd` rule, and the reason it is worth a test: the centre keeps
    // three real values (bars 2, 3, 4) that a study masking everything back
    // to the slowest input would throw away.
    const r = keltner(k2Steady(12), {
      period: 3,
      atrPeriod: 5,
      maType: 'sma',
    });
    const mid = col(r, 'kcMiddle');
    const up = col(r, 'kcUpper');
    expect(mid.findIndex((x) => x !== undefined)).toBe(2);
    expect(up.findIndex((x) => x !== undefined)).toBe(5);
    expect(mid.slice(2, 5).every((x) => x === 100)).toBe(true);
  });

  it('the band half-width is exactly `multiplier` × the shipped atr()', () => {
    // Not "close to": `keltner` and `atr` call the same `atrValues` kernel,
    // so the half-width is the multiplier times the same double.
    const source = k2Wavy(40);
    const k = keltner(source, { period: 6, atrPeriod: 7, multiplier: 2.5 });
    const a = col(atr(source, { period: 7 }), 'atr');
    const mid = col(k, 'kcMiddle');
    const up = col(k, 'kcUpper');
    const lo = col(k, 'kcLower');
    let checked = 0;
    for (let i = 0; i < a.length; i += 1) {
      if (mid[i] === undefined || a[i] === undefined) continue;
      expect(up[i], `bar ${i}`).toBe(mid[i]! + 2.5 * a[i]!);
      expect(lo[i], `bar ${i}`).toBe(mid[i]! - 2.5 * a[i]!);
      checked += 1;
    }
    expect(checked).toBeGreaterThan(30);
  });

  it('the default IS the modern variant: EMA(20) of typical price ± 2 × ATR(10)', () => {
    // The pinned variant, asserted as an equality against the explicit call
    // rather than left to the docstring.
    const source = k2Wavy(60);
    const defaults = keltner(source);
    const explicit = keltner(source, {
      period: 20,
      atrPeriod: 10,
      multiplier: 2,
      maType: 'ema',
      prefix: 'x',
    });
    for (const suffix of ['Middle', 'Upper', 'Lower']) {
      expect(col(defaults, `kc${suffix}`), suffix).toEqual(
        col(explicit, `x${suffix}`),
      );
    }
    // …and the centre is genuinely an EMA, not an SMA.
    const smaCentre = keltner(source, { maType: 'sma', prefix: 'y' });
    expect(col(defaults, 'kcMiddle')).not.toEqual(col(smaCentre, 'yMiddle'));
  });

  it('reads redirected high/low/close and a custom prefix', () => {
    const s = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'h2', kind: 'number' },
        { name: 'l2', kind: 'number' },
        { name: 'c2', kind: 'number' },
      ] as const,
      rows: Array.from({ length: 8 }, (_, i) => [i, 101, 99, 100]) as Array<
        [number, number, number, number]
      >,
    });
    const r = keltner(s, {
      period: 3,
      atrPeriod: 3,
      maType: 'sma',
      high: 'h2',
      low: 'l2',
      close: 'c2',
      prefix: 'band',
    });
    expect(col(r, 'bandMiddle')[7]).toBe(100);
    expect(col(r, 'bandUpper')[7]).toBe(104);
  });

  it('reads all-missing when a named column is absent', () => {
    const r = keltner(k2Steady(10), {
      period: 3,
      atrPeriod: 3,
      high: 'nope' as never,
    });
    for (const name of ['kcMiddle', 'kcUpper', 'kcLower']) {
      expect(
        col(r, name).every((x) => x === undefined),
        name,
      ).toBe(true);
    }
  });

  it('rejects bad periods, a bad multiplier, an unknown maType and a collision', () => {
    expect(() => keltner(k2Steady(5), { period: 0 })).toThrow(TypeError);
    expect(() => keltner(k2Steady(5), { atrPeriod: 1.5 })).toThrow(TypeError);
    expect(() => keltner(k2Steady(5), { multiplier: 0 })).toThrow(TypeError);
    expect(() => keltner(k2Steady(5), { multiplier: -2 })).toThrow(TypeError);
    expect(() => keltner(k2Steady(5), { maType: 'wilder' as never })).toThrow(
      TypeError,
    );
    // Running it twice with the same prefix collides on `kcMiddle`.
    expect(() => keltner(keltner(k2Steady(30)))).toThrow(TypeError);
  });

  it('is all-undefined when the periods exceed the bars available', () => {
    const r = keltner(k2Steady(4), { period: 9, atrPeriod: 9 });
    expect(col(r, 'kcMiddle')).toHaveLength(4);
    expect(col(r, 'kcUpper').every((x) => x === undefined)).toBe(true);
  });
});

describe('atrBands', () => {
  it('is close ± multiplier × ATR, hand-checked', () => {
    // True range is 2 on every bar but the first, so ATR(3) = 2 from bar 3;
    // the close is 100 throughout, so the bands are 100 ± 2 × 2.
    const r = atrBands(k2Steady(6), { period: 3, multiplier: 2 });
    expect(col(r, 'atrbUpper')).toEqual([
      undefined,
      undefined,
      undefined,
      104,
      104,
      104,
    ]);
    expect(col(r, 'atrbLower')).toEqual([
      undefined,
      undefined,
      undefined,
      96,
      96,
      96,
    ]);
  });

  it('appends TWO columns — there is no middle, the middle is the field', () => {
    const r = atrBands(k2Steady(6), { period: 3 });
    const names = (
      r as unknown as { schema: ReadonlyArray<{ name: string }> }
    ).schema.map((c) => c.name);
    expect(names).toEqual([
      'time',
      'high',
      'low',
      'close',
      'atrbUpper',
      'atrbLower',
    ]);
  });

  it('`upper − column` is EXACTLY multiplier × the shipped atr()', () => {
    // The claim the study makes, checked bit-for-bit rather than to a
    // tolerance: both call the same `atrValues` kernel, so `upper` is the
    // same `close + m × atr` expression evaluated once.
    const source = k2Wavy(40);
    const r = atrBands(source, { period: 9, multiplier: 1.5 });
    const a = col(atr(source, { period: 9 }), 'atr');
    const closes = col(source, 'close');
    const up = col(r, 'atrbUpper');
    const lo = col(r, 'atrbLower');
    let checked = 0;
    for (let i = 0; i < a.length; i += 1) {
      if (a[i] === undefined) {
        expect(up[i], `bar ${i}`).toBeUndefined();
        continue;
      }
      expect(up[i], `bar ${i}`).toBe(closes[i]! + 1.5 * a[i]!);
      expect(lo[i], `bar ${i}`).toBe(closes[i]! - 1.5 * a[i]!);
      checked += 1;
    }
    expect(checked).toBeGreaterThan(30);
  });

  it('draws the bands around `column`, which need not be the ATR’s close', () => {
    // The reason `column` is separate from `close`: bands around a smoothed
    // line, with the volatility still measured off the raw bars.
    const source = k2Wavy(40);
    const smoothed = sma(source, { period: 5, output: 'mid' });
    const r = atrBands(smoothed, { period: 9, column: 'mid' });
    const mid = col(smoothed, 'mid');
    const a = col(atr(source, { period: 9 }), 'atr');
    expect(col(r, 'atrbUpper')[20]).toBe(mid[20]! + 2 * a[20]!);
    // The band starts where BOTH are defined — the ATR at 9, `mid` at 4.
    expect(col(r, 'atrbUpper').findIndex((x) => x !== undefined)).toBe(9);
  });

  it('reads all-missing when a named column is absent', () => {
    const r = atrBands(k2Steady(10), { period: 3, low: 'nope' as never });
    expect(col(r, 'atrbUpper').every((x) => x === undefined)).toBe(true);
    const f = atrBands(k2Steady(10), { period: 3, column: 'nope' as never });
    expect(col(f, 'atrbLower').every((x) => x === undefined)).toBe(true);
  });

  it('rejects a bad period, a bad multiplier and a collision', () => {
    expect(() => atrBands(k2Steady(5), { period: 0 })).toThrow(TypeError);
    expect(() => atrBands(k2Steady(5), { multiplier: 0 })).toThrow(TypeError);
    expect(() => atrBands(k2Steady(5), { multiplier: Number.NaN })).toThrow(
      TypeError,
    );
    expect(() => atrBands(atrBands(k2Steady(20)))).toThrow(TypeError);
  });
});

describe('qstick', () => {
  const bodySchema = [
    { name: 'time', kind: 'time' },
    { name: 'open', kind: 'number' },
    { name: 'close', kind: 'number' },
  ] as const;

  const bodyBars = (pairs: Array<[number, number]>) =>
    new TimeSeries({
      name: 'bars',
      schema: bodySchema,
      rows: pairs.map(([o, c], i) => [i, o, c]) as Array<
        [number, number, number]
      >,
    });

  it('averages close − open, hand-checked, and changes sign', () => {
    // Bodies: +2, −1, +3, 0, −4. A 2-bar SMA of them is
    // undefined, 0.5, 1, 1.5, −2.
    const r = qstick(
      bodyBars([
        [10, 12],
        [12, 11],
        [11, 14],
        [14, 14],
        [14, 10],
      ]),
      { period: 2 },
    );
    expect(col(r, 'qstick')).toEqual([undefined, 0.5, 1, 1.5, -2]);
  });

  it('a doji contributes an honest 0, not a gap', () => {
    const r = qstick(
      bodyBars([
        [10, 10],
        [10, 10],
        [10, 10],
      ]),
      { period: 2 },
    );
    expect(col(r, 'qstick')).toEqual([undefined, 0, 0]);
  });

  it('honours maType — and the types genuinely differ', () => {
    const pairs = Array.from({ length: 30 }, (_, i) => {
      const c = 100 + 5 * Math.sin(i / 2.5);
      return [c - 0.4 * Math.cos(i / 1.7), c] as [number, number];
    });
    const simple = qstick(bodyBars(pairs), { period: 5, maType: 'sma' });
    const exp = qstick(bodyBars(pairs), {
      period: 5,
      maType: 'ema',
      output: 'q2',
    });
    expect(col(simple, 'qstick')[20]).not.toBe(col(exp, 'q2')[20]);
    // A wma waits for a full window of finite values, like the sma here.
    const w = qstick(bodyBars(pairs), {
      period: 5,
      maType: 'wma',
      output: 'q3',
    });
    expect(col(w, 'q3').findIndex((x) => x !== undefined)).toBe(4);
  });

  it('reads redirected open/close columns and a custom output', () => {
    const s = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'o2', kind: 'number' },
        { name: 'c2', kind: 'number' },
      ] as const,
      rows: [
        [0, 10, 12],
        [1, 12, 11],
        [2, 11, 14],
      ] as Array<[number, number, number]>,
    });
    const r = qstick(s, { period: 2, open: 'o2', close: 'c2', output: 'body' });
    expect(col(r, 'body')).toEqual([undefined, 0.5, 1]);
  });

  it('reads all-missing when `open` is absent (the new bar input)', () => {
    // The likely misconfiguration for this study — a series with no open.
    const r = qstick(bars([10, 11, 12, 13]) as never, { period: 2 });
    expect(col(r, 'qstick').every((x) => x === undefined)).toBe(true);
  });

  it('rejects a bad period, an unknown maType and a collision', () => {
    const b = bodyBars([
      [10, 12],
      [12, 11],
    ]);
    expect(() => qstick(b, { period: 0 })).toThrow(TypeError);
    expect(() => qstick(b, { period: 2.5 })).toThrow(TypeError);
    expect(() => qstick(b, { maType: 'rma' as never })).toThrow(TypeError);
    expect(() => qstick(b, { output: 'close' })).toThrow(TypeError);
  });
});

describe('trix', () => {
  it('is 0 on a flat series — three EMAs of a constant are the constant', () => {
    // A hand-checkable value: T[i] = T[i−1] = 100, so the rate of change is
    // exactly 0, and the signal EMA of zeros is 0.
    const r = trix(bars(Array.from({ length: 20 }, () => 100)), {
      period: 2,
      signalPeriod: 2,
    });
    const line = col(r, 'trix');
    expect(line.slice(0, 4).every((x) => x === undefined)).toBe(true);
    expect(line.slice(4).every((x) => x === 0)).toBe(true);
    expect(
      col(r, 'trixSignal')
        .slice(5)
        .every((x) => x === 0),
    ).toBe(true);
  });

  it('warms up at 3·period − 2, and the signal `signalPeriod − 1` later', () => {
    // Three chained EMAs each cost period − 1, and the rate of change costs
    // one more bar: TA-Lib's TRIX lookback exactly.
    const r = trix(bars(k2Closes(60)), { period: 4, signalPeriod: 3 });
    expect(col(r, 'trix').findIndex((x) => x !== undefined)).toBe(10);
    expect(col(r, 'trixSignal').findIndex((x) => x !== undefined)).toBe(12);
  });

  it('is NOT the rate of change of `tema` — the chain, not the combination', () => {
    // `tema` is 3·EMA − 3·EMA² + EMA³ over the same three stages; TRIX wants
    // EMA³ alone. Pinned because reaching for `tema` here would compile, run,
    // and be a different indicator.
    const closes = k2Closes(60);
    const line = col(trix(bars(closes), { period: 4 }), 'trix');
    const temaRoc = percentChange(
      movingAverage(bars(closes), { period: 4, type: 'tema', output: 't' }),
      { column: 't' as never, periods: 1, output: 'r' },
    );
    expect(col(temaRoc, 'r')[40]).toBeDefined();
    expect(line[40]).not.toBeCloseTo(col(temaRoc, 'r')[40]!, 6);
  });

  it('a zero previous value reads as missing rather than blowing up', () => {
    // The zero-base guard, inherited from `percentChange`. Without it the
    // ratio at bar 12 is `1.48 / 0 = Infinity`, which `withColumn` rejects
    // outright — so an unguarded build fails this as an exception, not as a
    // wrong number.
    const closes = [...Array.from({ length: 12 }, () => 0), 5, 5, 5, 5, 5, 5];
    const r = trix(bars(closes), { period: 2, signalPeriod: 2 });
    const line = col(r, 'trix');
    expect(line[11]).toBeUndefined(); // flat at zero: 0/0 is not a value
    expect(line[12]).toBeUndefined(); // the bar whose predecessor was 0
    expect(line[13]).toBeGreaterThan(0);
  });

  it('honours column and prefix', () => {
    const src = sma(bars(k2Closes(60)), { period: 3, output: 'mid' });
    const r = trix(src, { period: 3, column: 'mid', prefix: 'tx' });
    expect(col(r, 'tx').filter((x) => x !== undefined).length).toBeGreaterThan(
      30,
    );
    expect(
      col(r, 'txSignal').filter((x) => x !== undefined).length,
    ).toBeGreaterThan(20);
  });

  it('rejects bad periods and a collision', () => {
    const b = bars([1, 2, 3, 4, 5]);
    expect(() => trix(b, { period: 0 })).toThrow(TypeError);
    expect(() => trix(b, { signalPeriod: -1 })).toThrow(TypeError);
    // The line takes the prefix itself as its name, so this collides on
    // `close` directly.
    expect(() => trix(b, { prefix: 'close' })).toThrow(TypeError);
  });

  it('is all-undefined when the period exceeds the bars available', () => {
    const r = trix(bars([1, 2, 3, 4, 5]), { period: 4 });
    expect(col(r, 'trix')).toHaveLength(5);
    expect(col(r, 'trix').every((x) => x === undefined)).toBe(true);
  });
});

describe('coppock', () => {
  it('is the WMA of two percent rates of change, hand-checked', () => {
    // A geometric series makes every rate of change constant, so the whole
    // curve is one number that can be written down: ROC(2) = 21%, ROC(1) =
    // 10%, sum 31, and a weighted average of a constant is that constant.
    const geo = Array.from({ length: 8 }, (_, i) => 100 * 1.1 ** i);
    const v = col(
      coppock(bars(geo), { longPeriod: 2, shortPeriod: 1, wmaPeriod: 2 }),
      'coppock',
    );
    expect(v.slice(0, 3).every((x) => x === undefined)).toBe(true);
    for (let i = 3; i < 8; i += 1) expect(v[i], `bar ${i}`).toBeCloseTo(31, 9);
  });

  it('warms up at max(long, short) + wma − 1', () => {
    const closes = k2Closes(60);
    const r = coppock(bars(closes));
    expect(col(r, 'coppock').findIndex((x) => x !== undefined)).toBe(23);
    const short = coppock(bars(closes), {
      longPeriod: 6,
      shortPeriod: 3,
      wmaPeriod: 4,
      output: 'c2',
    });
    expect(col(short, 'c2').findIndex((x) => x !== undefined)).toBe(9);
  });

  it('is symmetric in longPeriod and shortPeriod', () => {
    // The two rates of change are ADDED, so no ordering is enforced (unlike
    // macd, which subtracts and therefore rejects fast >= slow).
    const closes = k2Closes(40);
    const a = col(
      coppock(bars(closes), { longPeriod: 9, shortPeriod: 4 }),
      'coppock',
    );
    const b = col(
      coppock(bars(closes), { longPeriod: 4, shortPeriod: 9 }),
      'coppock',
    );
    expect(a).toEqual(b);
    expect(a.filter((x) => x !== undefined).length).toBeGreaterThan(20);
  });

  it('the average is WEIGHTED — the unsmoothed sum differs from it', () => {
    // `wmaPeriod: 1` is the raw sum of the two rates of change (a one-bar
    // weighted average is the value itself); the 6-bar weighted average of a
    // curving input must differ from it.
    const closes = k2Closes(40);
    const weighted = col(coppock(bars(closes), { wmaPeriod: 6 }), 'coppock');
    const raw = col(
      coppock(bars(closes), { wmaPeriod: 1, output: 'raw' }),
      'raw',
    );
    expect(weighted[30]).toBeDefined();
    expect(weighted[30]).not.toBeCloseTo(raw[30]!, 6);
  });

  it('a zero base reads as missing', () => {
    const v = col(
      coppock(bars([0, 1, 2, 3, 4, 5, 6, 7]), {
        longPeriod: 2,
        shortPeriod: 1,
        wmaPeriod: 1,
      }),
      'coppock',
    );
    expect(v[2]).toBeUndefined(); // ROC(2) at bar 2 reads the 0 at bar 0
    expect(v[3]).toBeCloseTo((3 / 1 - 1) * 100 + (3 / 2 - 1) * 100, 9);
  });

  it('rejects bad periods and a collision', () => {
    const b = bars([1, 2, 3, 4, 5]);
    expect(() => coppock(b, { longPeriod: 0 })).toThrow(TypeError);
    expect(() => coppock(b, { shortPeriod: 1.5 })).toThrow(TypeError);
    expect(() => coppock(b, { wmaPeriod: -2 })).toThrow(TypeError);
    expect(() => coppock(b, { output: 'close' })).toThrow(TypeError);
  });
});

describe('the K2 consumers pin their published defaults', () => {
  // Every default in this batch is a documented convention (Wilder's 14,
  // Chande's 8, Hutson's 15 / ChartIQ's 9, Coppock's 14/11/10), so each is
  // asserted as an equality against the explicit call. Without these, a
  // mutation of any default changes no test — measured: four of them
  // survived the first mutation run.
  const source = k2Wavy(80);
  const closes = bars(k2Closes(80));

  it('atrBands defaults to period 14, multiplier 2', () => {
    const d = atrBands(source);
    const e = atrBands(source, { period: 14, multiplier: 2, prefix: 'z' });
    expect(col(d, 'atrbUpper')).toEqual(col(e, 'zUpper'));
    expect(col(d, 'atrbLower')).toEqual(col(e, 'zLower'));
    // …and the default is not some OTHER period that happens to agree.
    expect(col(d, 'atrbUpper')).not.toEqual(
      col(atrBands(source, { period: 20, prefix: 'w' }), 'wUpper'),
    );
  });

  it('qstick defaults to period 8, maType sma', () => {
    const bodyBars = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'open', kind: 'number' },
        { name: 'close', kind: 'number' },
      ] as const,
      rows: k2Closes(80).map((c, i) => [
        i,
        c - 0.5 * Math.cos(i / 1.7),
        c,
      ]) as Array<[number, number, number]>,
    });
    const d = qstick(bodyBars);
    const e = qstick(bodyBars, { period: 8, maType: 'sma', output: 'z' });
    expect(col(d, 'qstick')).toEqual(col(e, 'z'));
    expect(col(d, 'qstick')).not.toEqual(
      col(qstick(bodyBars, { period: 5, output: 'w' }), 'w'),
    );
  });

  it('trix defaults to period 15, signalPeriod 9', () => {
    const d = trix(closes);
    const e = trix(closes, { period: 15, signalPeriod: 9, prefix: 'z' });
    expect(col(d, 'trix')).toEqual(col(e, 'z'));
    expect(col(d, 'trixSignal')).toEqual(col(e, 'zSignal'));
    // The signal default is the one a wrong value hides in: the line is
    // unchanged by it, so it needs its own inequality.
    expect(col(d, 'trixSignal')).not.toEqual(
      col(trix(closes, { signalPeriod: 5, prefix: 'w' }), 'wSignal'),
    );
  });

  it('coppock defaults to 14 / 11 / 10', () => {
    const d = coppock(closes);
    const e = coppock(closes, {
      longPeriod: 14,
      shortPeriod: 11,
      wmaPeriod: 10,
      output: 'z',
    });
    expect(col(d, 'coppock')).toEqual(col(e, 'z'));
    expect(col(d, 'coppock')).not.toEqual(
      col(coppock(closes, { wmaPeriod: 5, output: 'w' }), 'w'),
    );
  });

  it('keltner defaults to 20 / 10 / 2 / ema', () => {
    // (The modern-variant test above asserts the same equality; this one
    // adds the inequalities that pin each default individually.)
    const d = col(keltner(source), 'kcUpper');
    expect(d).not.toEqual(
      col(keltner(source, { period: 10, prefix: 'a' }), 'aUpper'),
    );
    expect(d).not.toEqual(
      col(keltner(source, { atrPeriod: 20, prefix: 'b' }), 'bUpper'),
    );
    expect(d).not.toEqual(
      col(keltner(source, { multiplier: 3, prefix: 'c' }), 'cUpper'),
    );
    expect(d).not.toEqual(
      col(keltner(source, { maType: 'sma', prefix: 'd' }), 'dUpper'),
    );
  });
});

describe('atrBands — a redirected close moves the bands with it', () => {
  // Layer-2 review of #696: `column` used to default to the schema's
  // `close` independently of the `close` option, so
  // `atrBands({ close: 'c2' })` drew bands from c2's true range around
  // the OTHER close. `column` now defaults to whatever `close` resolves to.
  const series = new TimeSeries({
    name: 'two-closes',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'high', kind: 'number' },
      { name: 'low', kind: 'number' },
      { name: 'close', kind: 'number' },
      { name: 'c2', kind: 'number' },
    ] as const,
    rows: Array.from({ length: 12 }, (_, i) => {
      const c = 100 + 5 * Math.sin(i / 2);
      return [i, c + 1.5, c - 1.2, c, c + 10 + Math.cos(i)];
    }) as Array<[number, number, number, number, number]>,
  });
  it('centres on the redirected close by default', () => {
    const out = atrBands(series, { period: 3, multiplier: 2, close: 'c2' });
    const upper = col(out, 'atrbUpper');
    const a = col(atr(series, { period: 3, close: 'c2' }), 'atr');
    const c2 = col(series, 'c2');
    for (let i = 0; i < 12; i += 1) {
      if (upper[i] === undefined) continue;
      expect(upper[i]! - c2[i]!, `bar ${i}`).toBeCloseTo(2 * a[i]!, 12);
    }
    expect(upper.filter((v) => v !== undefined).length).toBeGreaterThan(0);
  });
  it('an explicit column still wins', () => {
    const out = atrBands(series, {
      period: 3,
      multiplier: 2,
      close: 'c2',
      column: 'close',
    });
    const upper = col(out, 'atrbUpper');
    const a = col(atr(series, { period: 3, close: 'c2' }), 'atr');
    const c = col(series, 'close');
    const i = 11;
    expect(upper[i]! - c[i]!).toBeCloseTo(2 * a[i]!, 12);
  });
});

/* ========================================================================== */
/* The K2 consumers: price-vs-moving-average oscillators.                     */
/* ========================================================================== */

/** High/low/close bars for the studies that read a whole bar. */
const hlc = (rows: Array<[number, number, number]>) =>
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

describe('priceOscillator', () => {
  // sma(2) = [_, 11, 13, 17, 21]; sma(3) = [_, _, 12, 46/3, 56/3].
  const src = [10, 12, 14, 20, 22];

  it('absolute mode is MA(fast) − MA(slow), hand-computed', () => {
    const v = col(
      priceOscillator(bars(src), {
        fastPeriod: 2,
        slowPeriod: 3,
        maType: 'sma',
        mode: 'absolute',
      }),
      'priceOsc',
    );
    expect(v).toHaveLength(5);
    expect(v[0]).toBeUndefined();
    expect(v[1]).toBeUndefined(); // the SLOW average's warm-up, not the fast one
    expect(v[2]).toBeCloseTo(13 - 12, 12);
    expect(v[3]).toBeCloseTo(17 - 46 / 3, 12);
    expect(v[4]).toBeCloseTo(21 - 56 / 3, 12);
  });

  it('percent mode is 100·(fast − slow)/slow, hand-computed, and is the default', () => {
    const v = col(
      priceOscillator(bars(src), {
        fastPeriod: 2,
        slowPeriod: 3,
        maType: 'sma',
      }),
      'priceOsc',
    );
    // `mode` omitted here: percent is the default (see the docstring for why).
    expect(v[2]).toBeCloseTo((100 * (13 - 12)) / 12, 12);
    expect(v[3]).toBeCloseTo((100 * (17 - 46 / 3)) / (46 / 3), 12);
    expect(v[4]).toBeCloseTo(12.5, 12); // (21 − 56/3) / (56/3) = 0.125
  });

  it('at the defaults with mode absolute it IS macd’s line, bar for bar', () => {
    // The step-0 cross-check, and the reason the default mode is `percent`:
    // the absolute form at 12/26/ema is a column this package already ships.
    const wavy = Array.from(
      { length: 60 },
      (_, i) => 100 + 9 * Math.sin(i / 5) + 0.2 * i,
    );
    const osc = col(
      priceOscillator(bars(wavy), { mode: 'absolute' }),
      'priceOsc',
    );
    const line = col(macd(bars(wavy)), 'macdLine');
    for (let i = 0; i < wavy.length; i += 1) {
      if (line[i] === undefined) expect(osc[i], `bar ${i}`).toBeUndefined();
      else expect(osc[i], `bar ${i}`).toBe(line[i]!); // the same kernel calls: bit-exact
    }
    // …and the default (percent) is NOT that column, or the default call
    // would be a rename of macdLine.
    expect(col(priceOscillator(bars(wavy)), 'priceOsc')[40]).not.toBeCloseTo(
      line[40]!,
      3,
    );
  });

  it('reports no value rather than Infinity when the slow average is zero', () => {
    // sma(3) is exactly 0 on bar 2 here; the percent form is a division by
    // zero there and the absolute form is unaffected.
    const zeroed = [-1, 0, 1, 2, 3];
    const pct = col(
      priceOscillator(bars(zeroed), {
        fastPeriod: 2,
        slowPeriod: 3,
        maType: 'sma',
      }),
      'priceOsc',
    );
    expect(pct[2]).toBeUndefined();
    expect(pct[3]).toBeCloseTo(50, 12); // 100·(1.5 − 1)/1
    expect(pct[4]).toBeCloseTo(25, 12); // 100·(2.5 − 2)/2
    const abs = col(
      priceOscillator(bars(zeroed), {
        fastPeriod: 2,
        slowPeriod: 3,
        maType: 'sma',
        mode: 'absolute',
      }),
      'priceOsc',
    );
    expect(abs[2]).toBeCloseTo(0.5, 12);
  });

  it('honours output, column, and every maType in the menu', () => {
    const withSma = sma(bars(src), { period: 2 });
    const r = priceOscillator(withSma, {
      column: 'sma',
      fastPeriod: 2,
      slowPeriod: 3,
      maType: 'wma',
      output: 'osc',
    });
    expect(col(r, 'osc')).toHaveLength(5);
    for (const maType of MA_TYPES) {
      expect(() =>
        priceOscillator(bars(src), { maType, fastPeriod: 2, slowPeriod: 3 }),
      ).not.toThrow();
    }
  });

  it('rejects bad periods, a swapped fast/slow pair, a bad mode or maType, and a colliding output', () => {
    expect(() => priceOscillator(bars(src), { fastPeriod: 0 })).toThrow(
      TypeError,
    );
    expect(() => priceOscillator(bars(src), { slowPeriod: 2.5 })).toThrow(
      TypeError,
    );
    expect(() =>
      priceOscillator(bars(src), { fastPeriod: 26, slowPeriod: 26 }),
    ).toThrow(/shorter/);
    expect(() =>
      priceOscillator(bars(src), { fastPeriod: 30, slowPeriod: 26 }),
    ).toThrow(/shorter/);
    expect(() =>
      priceOscillator(bars(src), { maType: 'nope' as never }),
    ).toThrow(/unknown moving-average type/);
    expect(() => priceOscillator(bars(src), { mode: 'pct' as never })).toThrow(
      /mode must be/,
    );
    expect(() => priceOscillator(bars(src), { output: 'close' })).toThrow(
      /collides/,
    );
  });

  it('is all-undefined when the slow period exceeds the series, length kept', () => {
    const v = col(
      priceOscillator(bars([1, 2, 3]), { fastPeriod: 2, slowPeriod: 9 }),
      'priceOsc',
    );
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('disparityIndex', () => {
  const src = [10, 12, 14, 20, 22]; // sma(3) = [_, _, 12, 46/3, 56/3]

  it('is 100·(price − MA)/MA, hand-computed', () => {
    const v = col(
      disparityIndex(bars(src), { period: 3, maType: 'sma' }),
      'disparity',
    );
    expect(v).toHaveLength(5);
    expect(v[0]).toBeUndefined();
    expect(v[1]).toBeUndefined();
    expect(v[2]).toBeCloseTo((100 * (14 - 12)) / 12, 12);
    expect(v[3]).toBeCloseTo((100 * (20 - 46 / 3)) / (46 / 3), 12);
    expect(v[4]).toBeCloseTo((100 * (22 - 56 / 3)) / (56 / 3), 12);
  });

  it('is zero exactly where price sits on its own average', () => {
    // A flat series is its own SMA, so every reading is 0 — not `undefined`
    // (there is no division by zero here) and not a missing row.
    const v = col(
      disparityIndex(bars([7, 7, 7, 7]), { period: 3 }),
      'disparity',
    );
    expect(v[2]).toBe(0);
    expect(v[3]).toBe(0);
  });

  it('reports no value rather than Infinity when the average is zero', () => {
    const v = col(
      disparityIndex(bars([-1, 0, 1, 2, 3]), { period: 3, maType: 'sma' }),
      'disparity',
    );
    expect(v[2]).toBeUndefined(); // sma(3) = 0 on this bar
    expect(v[3]).toBeCloseTo(100, 12); // 100·(2 − 1)/1
    expect(v[4]).toBeCloseTo(50, 12); // 100·(3 − 2)/2
  });

  it('defaults to period 14 / sma, and honours column, maType and output', () => {
    // The warm-up alone does NOT pin the default maType — sma and ema share
    // it — so the VALUES are compared both ways. (A mutation flipping the
    // default from 'sma' to 'ema' survived the warm-up-only version.)
    const long = Array.from(
      { length: 20 },
      (_, i) => 100 + 5 * Math.sin(i / 2) + i,
    );
    const d = col(disparityIndex(bars(long)), 'disparity');
    expect(d.slice(0, 13).every((x) => x === undefined)).toBe(true);
    expect(d[13]).toBeDefined();
    expect(d).toEqual(
      col(
        disparityIndex(bars(long), { period: 14, maType: 'sma' }),
        'disparity',
      ),
    );
    expect(d[19]).not.toBeCloseTo(
      col(
        disparityIndex(bars(long), { period: 14, maType: 'ema' }),
        'disparity',
      )[19]!,
      6,
    );
    const r = disparityIndex(sma(bars(long), { period: 2 }), {
      column: 'sma',
      period: 3,
      maType: 'ema',
      output: 'dev',
    });
    expect(col(r, 'dev')).toHaveLength(20);
  });

  it('rejects a bad period, an unknown maType, and a colliding output', () => {
    expect(() => disparityIndex(bars(src), { period: 0 })).toThrow(TypeError);
    expect(() => disparityIndex(bars(src), { period: 1.5 })).toThrow(TypeError);
    expect(() =>
      disparityIndex(bars(src), { maType: 'nope' as never }),
    ).toThrow(/unknown moving-average type/);
    expect(() => disparityIndex(bars(src), { output: 'close' })).toThrow(
      /collides/,
    );
  });

  it('is all-undefined when the period exceeds the series, length kept', () => {
    const v = col(disparityIndex(bars([1, 2, 3]), { period: 9 }), 'disparity');
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('detrendedPriceOscillator', () => {
  // period 3 → shift = ⌊3/2⌋ + 1 = 2; sma(3) = [_, _, 12, 46/3, 56/3, 22].
  const src = [10, 12, 14, 20, 22, 24];

  it('is price − MA displaced ⌊period/2⌋+1 bars back, hand-computed', () => {
    const v = col(
      detrendedPriceOscillator(bars(src), { period: 3, maType: 'sma' }),
      'dpo',
    );
    expect(v).toHaveLength(6);
    // Warm-up is the MA's (2 rows) plus the 2-bar displacement.
    expect(v.slice(0, 4).every((x) => x === undefined)).toBe(true);
    expect(v[4]).toBeCloseTo(22 - 12, 12); // close[4] − sma[2]
    expect(v[5]).toBeCloseTo(24 - 46 / 3, 12); // close[5] − sma[3]
  });

  it('the displacement floors on an odd period', () => {
    // period 5 → shift 3, first valid at 4 + 3 = 7; period 6 → shift 4,
    // first valid at 5 + 4 = 9. Flooring is what makes the odd case land a
    // bar earlier than the even one above it rather than half-way between.
    const long = Array.from({ length: 20 }, (_, i) => 100 + i * i * 0.05);
    const odd = col(detrendedPriceOscillator(bars(long), { period: 5 }), 'dpo');
    expect(odd.slice(0, 7).every((x) => x === undefined)).toBe(true);
    expect(odd[7]).toBeDefined();
    const even = col(
      detrendedPriceOscillator(bars(long), { period: 6 }),
      'dpo',
    );
    expect(even.slice(0, 9).every((x) => x === undefined)).toBe(true);
    expect(even[9]).toBeDefined();
  });

  it('is the displaced average, not the current one', () => {
    // The bug this study can have. `close − MA[i]` and `close − MA[i−shift]`
    // are different series; on a series that turns, they differ in SIGN on
    // at least one bar, which no tolerance can explain away.
    const long = Array.from(
      { length: 30 },
      (_, i) => 100 + 6 * Math.sin(i / 4),
    );
    const dpo = col(detrendedPriceOscillator(bars(long), { period: 5 }), 'dpo');
    const withMa = sma(bars(long), { period: 5, output: 'm' });
    const undisplaced = col(withMa, 'm').map((m, i) =>
      m === undefined ? undefined : long[i]! - m,
    );
    const disagrees = dpo.some(
      (x, i) =>
        x !== undefined &&
        undisplaced[i] !== undefined &&
        Math.sign(x) !== Math.sign(undisplaced[i]!),
    );
    expect(disagrees).toBe(true);
  });

  it('defaults to period 20 / sma, and honours column, maType and output', () => {
    const long = Array.from({ length: 40 }, (_, i) => 100 + i);
    const v = col(detrendedPriceOscillator(bars(long)), 'dpo');
    expect(v.slice(0, 30).every((x) => x === undefined)).toBe(true); // 19 + 11
    expect(v[30]).toBeDefined();
    const r = detrendedPriceOscillator(sma(bars(long), { period: 2 }), {
      column: 'sma',
      period: 4,
      maType: 'ema',
      output: 'detrended',
    });
    expect(col(r, 'detrended')).toHaveLength(40);
  });

  it('rejects a bad period, an unknown maType, and a colliding output', () => {
    expect(() => detrendedPriceOscillator(bars(src), { period: 0 })).toThrow(
      TypeError,
    );
    expect(() => detrendedPriceOscillator(bars(src), { period: 2.5 })).toThrow(
      TypeError,
    );
    expect(() =>
      detrendedPriceOscillator(bars(src), { maType: 'nope' as never }),
    ).toThrow(/unknown moving-average type/);
    expect(() =>
      detrendedPriceOscillator(bars(src), { output: 'close' }),
    ).toThrow(/collides/);
  });

  it('is all-undefined when period − 1 + shift exceeds the series', () => {
    const v = col(
      detrendedPriceOscillator(bars([1, 2, 3, 4]), { period: 3 }),
      'dpo',
    );
    expect(v).toHaveLength(4);
    expect(v.every((x) => x === undefined)).toBe(true); // first valid would be 4
  });
});

describe('elderRay', () => {
  // period 2 → α = 2/3. ema = [10 (masked), 34/3, …].
  const src: Array<[number, number, number]> = [
    [11, 9, 10],
    [13, 11, 12],
    [15, 13, 14],
  ];

  it('is high − EMA(close) and low − EMA(close), hand-computed', () => {
    const r = elderRay(hlc(src), { period: 2 });
    const bull = col(r, 'elderBull');
    const bear = col(r, 'elderBear');
    expect(bull).toHaveLength(3);
    expect(bull[0]).toBeUndefined();
    expect(bear[0]).toBeUndefined();
    const ema1 = (2 / 3) * 12 + (1 / 3) * 10;
    const ema2 = (2 / 3) * 14 + (1 / 3) * ema1;
    expect(bull[1]).toBeCloseTo(13 - ema1, 12);
    expect(bear[1]).toBeCloseTo(11 - ema1, 12);
    expect(bull[2]).toBeCloseTo(15 - ema2, 12);
    expect(bear[2]).toBeCloseTo(13 - ema2, 12);
  });

  it('the two legs differ by exactly the bar’s range', () => {
    const r = elderRay(hlc(src), { period: 2 });
    const bull = col(r, 'elderBull');
    const bear = col(r, 'elderBear');
    for (let i = 1; i < 3; i += 1) {
      expect(bull[i]! - bear[i]!).toBeCloseTo(src[i]![0] - src[i]![1], 12);
    }
  });

  it('uses the SAME EMA the package ships, not a private one', () => {
    const rows = Array.from({ length: 20 }, (_, i) => {
      const c = 100 + 5 * Math.sin(i / 3);
      return [c + 1.5, c - 1.2, c] as [number, number, number];
    });
    const bull = col(elderRay(hlc(rows), { period: 4 }), 'elderBull');
    const reference = col(ema(hlc(rows), { period: 4, output: 'e' }), 'e');
    for (let i = 0; i < rows.length; i += 1) {
      if (reference[i] === undefined) expect(bull[i]).toBeUndefined();
      else
        expect(bull[i], `bar ${i}`).toBeCloseTo(
          rows[i]![0] - reference[i]!,
          12,
        );
    }
  });

  it('defaults to period 13 and the `elder` prefix; honours a custom prefix', () => {
    const rows = Array.from(
      { length: 20 },
      (_, i) => [100 + i + 1, 100 + i - 1, 100 + i] as [number, number, number],
    );
    const d = elderRay(hlc(rows));
    expect(
      col(d, 'elderBull')
        .slice(0, 12)
        .every((x) => x === undefined),
    ).toBe(true);
    expect(col(d, 'elderBull')[12]).toBeDefined();
    const named = elderRay(hlc(rows), {
      period: 3,
      prefix: 'ray',
      high: 'high',
      low: 'low',
      close: 'close',
    });
    expect(col(named, 'rayBull')[5]).toBeDefined();
    expect(col(named, 'rayBear')[5]).toBeDefined();
  });

  it('reads all-missing when an input column is misnamed', () => {
    const v = col(
      elderRay(hlc(src), { period: 2, high: 'nope' as never }),
      'elderBull',
    );
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('rejects a bad period and a colliding prefix', () => {
    expect(() => elderRay(hlc(src), { period: 0 })).toThrow(TypeError);
    expect(() => elderRay(hlc(src), { period: 1.5 })).toThrow(TypeError);
    const once = elderRay(hlc(src), { period: 2 });
    expect(() => elderRay(once as never, { period: 2 })).toThrow(/collides/);
  });

  it('is all-undefined when the period exceeds the series, length kept', () => {
    const r = elderRay(hlc(src), { period: 9 });
    for (const name of ['elderBull', 'elderBear']) {
      expect(col(r, name), name).toHaveLength(3);
      expect(
        col(r, name).every((x) => x === undefined),
        name,
      ).toBe(true);
    }
  });
});

describe('awesomeOscillator', () => {
  // Median prices 4, 6, 8, 14, 18 → sma(2) = [_, 5, 7, 11, 16];
  // sma(3) = [_, _, 6, 28/3, 40/3].
  const medians = [4, 6, 8, 14, 18];
  const src = medians.map(
    (m) => [m + 2, m - 2, m - 2] as [number, number, number],
  );

  it('is SMA(fast) − SMA(slow) of the median price, hand-computed', () => {
    const v = col(
      awesomeOscillator(hlc(src), { fastPeriod: 2, slowPeriod: 3 }),
      'ao',
    );
    expect(v).toHaveLength(5);
    expect(v[0]).toBeUndefined();
    expect(v[1]).toBeUndefined(); // the slow leg's warm-up
    expect(v[2]).toBeCloseTo(7 - 6, 12);
    expect(v[3]).toBeCloseTo(11 - 28 / 3, 12);
    expect(v[4]).toBeCloseTo(16 - 40 / 3, 12);
  });

  it('reads the median price, not the close', () => {
    // The closes sit on each bar's LOW and the half-range VARIES, so a study
    // reading the close differs from this one by more than a constant (which
    // would cancel in the difference of the two legs).
    const rows: Array<[number, number, number]> = Array.from(
      { length: 12 },
      (_, i) => {
        const m = 100 + 3 * Math.sin(i / 2) + i * 0.4;
        const halfRange = 1 + 0.8 * Math.abs(Math.cos(i / 1.7));
        return [m + halfRange, m - halfRange, m - halfRange];
      },
    );
    const ao = col(
      awesomeOscillator(hlc(rows), { fastPeriod: 2, slowPeriod: 4 }),
      'ao',
    );
    const onClose = col(
      priceOscillator(hlc(rows), {
        fastPeriod: 2,
        slowPeriod: 4,
        maType: 'sma',
        mode: 'absolute',
        output: 'osc',
      }),
      'osc',
    );
    expect(ao[11]).not.toBeCloseTo(onClose[11]!, 6);
  });

  it('defaults to 5/34 and the `ao` column', () => {
    const rows = Array.from({ length: 40 }, (_, i) => {
      const c = 100 + 7 * Math.sin(i / 4) + 0.3 * i;
      return [c + 1, c - 1, c] as [number, number, number];
    });
    const v = col(awesomeOscillator(hlc(rows)), 'ao');
    expect(v.slice(0, 33).every((x) => x === undefined)).toBe(true);
    expect(v[33]).toBeDefined();
    // The warm-up is the SLOW leg's, so it pins `slowPeriod` and says nothing
    // about `fastPeriod` — the values have to (a mutation moving the default
    // fast period from 5 to 4 survived the warm-up-only version).
    expect(v).toEqual(
      col(
        awesomeOscillator(hlc(rows), { fastPeriod: 5, slowPeriod: 34 }),
        'ao',
      ),
    );
    expect(v[39]).not.toBeCloseTo(
      col(awesomeOscillator(hlc(rows), { fastPeriod: 4 }), 'ao')[39]!,
      6,
    );
  });

  it('honours output and the high/low column names', () => {
    const r = awesomeOscillator(hlc(src), {
      fastPeriod: 2,
      slowPeriod: 3,
      high: 'high',
      low: 'low',
      output: 'awesome',
    });
    expect(col(r, 'awesome')[4]).toBeCloseTo(16 - 40 / 3, 12);
  });

  it('reads all-missing when an input column is misnamed', () => {
    const v = col(
      awesomeOscillator(hlc(src), {
        fastPeriod: 2,
        slowPeriod: 3,
        low: 'nope' as never,
      }),
      'ao',
    );
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('rejects bad periods, a swapped fast/slow pair, and a colliding output', () => {
    expect(() => awesomeOscillator(hlc(src), { fastPeriod: 0 })).toThrow(
      TypeError,
    );
    expect(() => awesomeOscillator(hlc(src), { slowPeriod: 2.5 })).toThrow(
      TypeError,
    );
    expect(() =>
      awesomeOscillator(hlc(src), { fastPeriod: 5, slowPeriod: 5 }),
    ).toThrow(/shorter/);
    expect(() =>
      awesomeOscillator(hlc(src), { fastPeriod: 34, slowPeriod: 5 }),
    ).toThrow(/shorter/);
    expect(() => awesomeOscillator(hlc(src), { output: 'close' })).toThrow(
      /collides/,
    );
  });

  it('is all-undefined when the slow period exceeds the series, length kept', () => {
    const v = col(awesomeOscillator(hlc(src), { slowPeriod: 9 }), 'ao');
    expect(v).toHaveLength(5);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

/* -------------------------------------------------------------------------- */
/* Volume & money flow (corpus §6.6).                                          */
/*                                                                             */
/* Hand-computed values on four-bar fixtures. The bars are deliberately         */
/* non-degenerate: the close sits at 0.5 / −0.5 / 1 / 0 of the range across     */
/* the four, so a study that read the close's SIGN rather than its LOCATION,    */
/* or dropped the volume weighting, gets different numbers on every bar.        */
/* -------------------------------------------------------------------------- */

/** Bars whose high/low/close/volume may each be missing — the gap cases for
 *  the four-input volume studies. Row = [h, l, c, v]. */
const ohlcvGappy = (
  rows: Array<
    [
      number | undefined,
      number | undefined,
      number | undefined,
      number | undefined,
    ]
  >,
) =>
  new TimeSeries({
    name: 'bars',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'high', kind: 'number', required: false },
      { name: 'low', kind: 'number', required: false },
      { name: 'close', kind: 'number', required: false },
      { name: 'volume', kind: 'number', required: false },
    ] as const,
    rows: rows.map(([h, l, c, v], i) => [i, h, l, c, v]) as Array<
      [
        number,
        number | undefined,
        number | undefined,
        number | undefined,
        number | undefined,
      ]
    >,
  });

/** Four bars whose close locations are +0.5, −0.5, +1 and 0 of the range.
 *  CLV·volume = 50, −100, 300, 0, so the A/D line is 50, −50, 250, 250. */
const adBars: Array<[number, number, number, number]> = [
  [12, 10, 11.5, 100],
  [14, 12, 12.5, 200],
  [16, 14, 16, 300],
  [18, 16, 17, 400],
];

describe('accumulationDistribution', () => {
  it('accumulates CLV × volume, hand-computed', () => {
    const v = col(accumulationDistribution(ohlcv(adBars)), 'ad');
    expect(v).toEqual([50, -50, 250, 250]);
  });

  it('has no warm-up and no period: defined from bar 0', () => {
    expect(
      col(accumulationDistribution(ohlcv([[2, 0, 1.5, 8]])), 'ad'),
    ).toEqual([4]); // clv = ((1.5 − 0) − (2 − 1.5)) / 2 = 0.5, on 8 lots
  });

  it('reads the close’s LOCATION, not the sign of its change', () => {
    // Bar 2 closes ON its high after an up move, bar 3 mid-range after
    // another. OBV would add the whole volume to both; A/D adds all of bar
    // 2's and none of bar 3's.
    const v = col(accumulationDistribution(ohlcv(adBars)), 'ad');
    expect(v[2]! - v[1]!).toBe(300); // clv = +1 → the whole 300
    expect(v[3]! - v[2]!).toBe(0); // clv = 0 → nothing, on an up close
  });

  it('defaults to the `ad` output name, and honours a custom one', () => {
    const named = accumulationDistribution(ohlcv(adBars), { output: 'accum' });
    expect(col(named, 'accum')).toEqual([50, -50, 250, 250]);
    expect(col(named, 'ad').every((x) => x === undefined)).toBe(true);
  });

  it('reads redirected high/low/close/volume columns', () => {
    const s = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'hi', kind: 'number' },
        { name: 'lo', kind: 'number' },
        { name: 'px', kind: 'number' },
        { name: 'qty', kind: 'number' },
      ] as const,
      rows: adBars.map(([h, l, c, v], i) => [i, h, l, c, v]) as Array<
        [number, number, number, number, number]
      >,
    });
    expect(
      col(
        accumulationDistribution(s, {
          high: 'hi',
          low: 'lo',
          close: 'px',
          volume: 'qty',
        }),
        'ad',
      ),
    ).toEqual([50, -50, 250, 250]);
  });

  it('reads all-missing when a named column is absent', () => {
    const v = col(
      accumulationDistribution(ohlcv(adBars), { volume: 'nope' as never }),
      'ad',
    );
    expect(v).toHaveLength(4);
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('shifts the seed past a leading gap, and propagates an interior one', () => {
    const lead = col(
      accumulationDistribution(
        ohlcvGappy([
          [12, 10, undefined, 100],
          [14, 12, 12.5, 200],
          [16, 14, 16, 300],
          [18, 16, 17, 400],
        ]),
      ),
      'ad',
    );
    expect(lead).toEqual([undefined, -100, 200, 200]);

    const hole = col(
      accumulationDistribution(
        ohlcvGappy([
          [12, 10, 11.5, 100],
          [14, 12, 12.5, 200],
          [16, 14, 16, undefined],
          [18, 16, 17, 400],
        ]),
      ),
      'ad',
    );
    expect(hole.slice(0, 2)).toEqual([50, -50]);
    expect(hole.slice(2)).toEqual([undefined, undefined]);
  });

  it('a flat bar contributes 0 and the line carries on — matching TA-Lib', () => {
    // TA-Lib's AD folds a zero-range bar in as a ZERO contribution and
    // carries on (measured 0.7.1); so does this, because the close
    // location's numerator is exactly zero on a flat bar.
    const v = col(
      accumulationDistribution(
        ohlcv([
          [12, 10, 11.5, 100],
          [14, 12, 12.5, 200],
          [15, 15, 15, 300],
          [18, 16, 17, 400],
        ]),
      ),
      'ad',
    );
    // bar 3: (17−16) − (18−17) = 0, so it adds 0 as well.
    expect(v).toEqual([50, -50, -50, -50]);
  });

  it('rejects a colliding output', () => {
    expect(() =>
      accumulationDistribution(ohlcv(adBars), { output: 'close' }),
    ).toThrow(/collides/);
  });
});

describe('chaikinOscillator', () => {
  it('is the difference of two EMAs of the A/D line, hand-computed', () => {
    // A/D = [50, −50, 250, 250]. fast 1 is the line itself (α = 1); slow 2
    // is α = 2/3 seeded on the first sample, emitted from its second.
    const v = col(
      chaikinOscillator(ohlcv(adBars), { fastPeriod: 1, slowPeriod: 2 }),
      'chaikinOsc',
    );
    const slow1 = (2 / 3) * -50 + (1 / 3) * 50;
    const slow2 = (2 / 3) * 250 + (1 / 3) * slow1;
    const slow3 = (2 / 3) * 250 + (1 / 3) * slow2;
    expect(v[0]).toBeUndefined();
    expect(v[1]).toBeCloseTo(-50 - slow1, 12);
    expect(v[2]).toBeCloseTo(250 - slow2, 12);
    expect(v[3]).toBeCloseTo(250 - slow3, 12);
  });

  it('smooths the SAME A/D array the standalone study appends', () => {
    const rows = Array.from({ length: 30 }, (_, i) => {
      const c = 100 + 6 * Math.sin(i / 3);
      return [
        c + 1.2,
        c - 0.9,
        c + 0.3 * Math.cos(i / 2),
        900 + 300 * (i % 5),
      ] as [number, number, number, number];
    });
    const line = col(accumulationDistribution(ohlcv(rows)), 'ad');
    const osc = col(
      chaikinOscillator(ohlcv(rows), { fastPeriod: 3, slowPeriod: 10 }),
      'chaikinOsc',
    );
    // Rebuild the oscillator from the appended line via the shipped `ema`
    // study, which is the same first-sample-seeded average.
    const asSeries = new TimeSeries({
      name: 'ad',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'ad', kind: 'number' },
      ] as const,
      rows: line.map((x, i) => [i, x!]) as Array<[number, number]>,
    });
    const fast = col(
      ema(asSeries, { period: 3, column: 'ad', output: 'f' }),
      'f',
    );
    const slow = col(
      ema(asSeries, { period: 10, column: 'ad', output: 's' }),
      's',
    );
    for (let i = 0; i < rows.length; i += 1) {
      if (slow[i] === undefined) expect(osc[i], `bar ${i}`).toBeUndefined();
      else expect(osc[i]!, `bar ${i}`).toBeCloseTo(fast[i]! - slow[i]!, 9);
    }
  });

  it('defaults to 3 / 10 and the `chaikinOsc` column', () => {
    const rows = Array.from({ length: 24 }, (_, i) => {
      const c = 100 + 5 * Math.sin(i / 4);
      return [c + 1, c - 1, c + 0.4 * Math.cos(i), 1000 + 200 * (i % 7)] as [
        number,
        number,
        number,
        number,
      ];
    });
    const v = col(chaikinOscillator(ohlcv(rows)), 'chaikinOsc');
    expect(v.slice(0, 9).every((x) => x === undefined)).toBe(true);
    expect(v[9]).toBeDefined();
    // The warm-up pins only `slowPeriod`; the values have to pin `fastPeriod`.
    expect(v).toEqual(
      col(
        chaikinOscillator(ohlcv(rows), { fastPeriod: 3, slowPeriod: 10 }),
        'chaikinOsc',
      ),
    );
    expect(v[23]).not.toBeCloseTo(
      col(chaikinOscillator(ohlcv(rows), { fastPeriod: 4 }), 'chaikinOsc')[23]!,
      6,
    );
  });

  it('honours output and the redirected columns', () => {
    const r = chaikinOscillator(ohlcv(adBars), {
      fastPeriod: 1,
      slowPeriod: 2,
      high: 'high',
      volume: 'volume',
      output: 'co',
    });
    expect(col(r, 'co')[1]).toBeCloseTo(
      -50 - ((2 / 3) * -50 + (1 / 3) * 50),
      12,
    );
  });

  it('reads all-missing when an input column is misnamed', () => {
    const v = col(
      chaikinOscillator(ohlcv(adBars), {
        fastPeriod: 1,
        slowPeriod: 2,
        low: 'nope' as never,
      }),
      'chaikinOsc',
    );
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('rejects bad periods, a swapped pair, and a colliding output', () => {
    expect(() => chaikinOscillator(ohlcv(adBars), { fastPeriod: 0 })).toThrow(
      TypeError,
    );
    expect(() => chaikinOscillator(ohlcv(adBars), { slowPeriod: 2.5 })).toThrow(
      TypeError,
    );
    expect(() =>
      chaikinOscillator(ohlcv(adBars), { fastPeriod: 10, slowPeriod: 3 }),
    ).toThrow(/shorter/);
    expect(() =>
      chaikinOscillator(ohlcv(adBars), { fastPeriod: 5, slowPeriod: 5 }),
    ).toThrow(/shorter/);
    expect(() => chaikinOscillator(ohlcv(adBars), { output: 'low' })).toThrow(
      /collides/,
    );
  });

  it('is all-undefined when the slow period exceeds the series, length kept', () => {
    const v = col(
      chaikinOscillator(ohlcv(adBars), { fastPeriod: 2, slowPeriod: 9 }),
      'chaikinOsc',
    );
    expect(v).toHaveLength(4);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('priceVolumeTrend', () => {
  const closes = [10, 11, 11, 9];
  const volumes = [100, 200, 300, 400];

  it('accumulates the FRACTIONAL change times volume, hand-computed', () => {
    const v = col(priceVolumeTrend(cv(closes, volumes)), 'pvt');
    expect(v[0]).toBeUndefined();
    expect(v[1]).toBeCloseTo(20, 12); // 0.1 × 200
    expect(v[2]).toBeCloseTo(20, 12); // unchanged close adds nothing
    expect(v[3]).toBeCloseTo(20 + (-2 / 11) * 400, 12);
  });

  it('is not the percent form: a 10% bar on 200 lots adds 20, not 2000', () => {
    // The kernel it composes on returns a PERCENT; PVT's definition is the
    // fraction, and the /100 is what this pins.
    const v = col(priceVolumeTrend(cv([10, 11], [100, 200])), 'pvt');
    expect(v[1]).toBeCloseTo(20, 12);
  });

  it('bar 0 is undefined, not 0 — and the later levels are unchanged by that', () => {
    const v = col(priceVolumeTrend(cv(closes, volumes)), 'pvt');
    expect(v[0]).toBeUndefined();
    // A seed-at-zero implementation would agree from bar 1 on; that is why
    // declining to invent the seed costs nothing.
    expect(v[1]).toBeCloseTo(20, 12);
  });

  it('defaults to `pvt`, honours output and redirected columns', () => {
    const named = priceVolumeTrend(cv(closes, volumes), {
      close: 'close',
      volume: 'volume',
      output: 'trend',
    });
    expect(col(named, 'trend')[1]).toBeCloseTo(20, 12);
    expect(col(named, 'pvt').every((x) => x === undefined)).toBe(true);
  });

  it('reads all-missing when a named column is absent', () => {
    const v = col(
      priceVolumeTrend(cv(closes, volumes), { close: 'nope' as never }),
      'pvt',
    );
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('shifts the seed past a leading gap and propagates an interior one', () => {
    const lead = col(
      priceVolumeTrend(cvGappy([undefined, 10, 11, 11], volumes)),
      'pvt',
    );
    expect(lead[0]).toBeUndefined();
    expect(lead[1]).toBeUndefined(); // no base for bar 1's change either
    expect(lead[2]).toBeCloseTo(30, 12); // 0.1 × 300
    expect(lead[3]).toBeCloseTo(30, 12);

    const hole = col(
      priceVolumeTrend(cvGappy(closes, [100, 200, undefined, 400])),
      'pvt',
    );
    expect(hole[1]).toBeCloseTo(20, 12);
    expect(hole.slice(2)).toEqual([undefined, undefined]);
  });

  it('stops at a zero base rather than reporting Infinity', () => {
    const v = col(priceVolumeTrend(cv([1, 0, 5, 6], volumes)), 'pvt');
    expect(v[1]).toBeCloseTo(-200, 12); // (0/1 − 1) × 200
    expect(v.slice(2)).toEqual([undefined, undefined]);
  });

  it('rejects a colliding output', () => {
    expect(() =>
      priceVolumeTrend(cv(closes, volumes), { output: 'volume' }),
    ).toThrow(/collides/);
  });
});

describe('chaikinMoneyFlow', () => {
  it('is Σ(CLV × volume) / Σ volume over the window, hand-computed', () => {
    const v = col(chaikinMoneyFlow(ohlcv(adBars), { period: 2 }), 'cmf');
    expect(v[0]).toBeUndefined();
    expect(v[1]).toBeCloseTo(-50 / 300, 12);
    expect(v[2]).toBeCloseTo(200 / 500, 12);
    expect(v[3]).toBeCloseTo(300 / 700, 12);
  });

  it('is the volume-WEIGHTED mean of CLV, not the plain one', () => {
    // Both bars close at the same two locations; only the weights differ.
    const weighted = col(
      chaikinMoneyFlow(
        ohlcv([
          [12, 10, 12, 1],
          [14, 12, 12, 99],
        ]),
        { period: 2 },
      ),
      'cmf',
    );
    // clv = +1 on one lot, −1 on ninety-nine: (1 − 99)/100.
    expect(weighted[1]).toBeCloseTo(-0.98, 12);
  });

  it('emits missing on a window with no volume, and recovers after it', () => {
    const v = col(
      chaikinMoneyFlow(
        ohlcv([
          [12, 10, 11.5, 0],
          [14, 12, 12.5, 0],
          [16, 14, 16, 300],
          [18, 16, 17, 400],
        ]),
        { period: 2 },
      ),
      'cmf',
    );
    expect(v[1]).toBeUndefined();
    expect(v[2]).toBeCloseTo(1, 12); // only bar 2 has weight; clv = +1
    expect(v[3]).toBeCloseTo(300 / 700, 12);
  });

  it('a flat bar adds 0 to the numerator and its volume to the denominator', () => {
    // The conventional CMF: a bar that reported no direction dilutes the
    // reading by however much traded in it.
    const v = col(
      chaikinMoneyFlow(
        ohlcv([
          [12, 10, 11.5, 100],
          [15, 15, 15, 200],
          [16, 14, 16, 300],
          [18, 16, 17, 400],
        ]),
        { period: 2 },
      ),
      'cmf',
    );
    expect(v[1]).toBeCloseTo(50 / 300, 12); // (50 + 0) / (100 + 200)
    expect(v[2]).toBeCloseTo(300 / 500, 12); // (0 + 300) / (200 + 300)
    expect(v[3]).toBeCloseTo(300 / 700, 12); // bar 3's CLV is 0 too
  });

  it('defaults to period 20 and the `cmf` column', () => {
    const rows = Array.from({ length: 25 }, (_, i) => {
      const c = 100 + 4 * Math.sin(i / 3);
      return [c + 1, c - 1, c + 0.5 * Math.cos(i), 800 + 100 * (i % 6)] as [
        number,
        number,
        number,
        number,
      ];
    });
    const v = col(chaikinMoneyFlow(ohlcv(rows)), 'cmf');
    expect(v.slice(0, 19).every((x) => x === undefined)).toBe(true);
    expect(v[19]).toBeDefined();
    expect(v).toEqual(
      col(chaikinMoneyFlow(ohlcv(rows), { period: 20 }), 'cmf'),
    );
  });

  it('stays inside [−1, +1] — a weighted mean of a bounded quantity', () => {
    // Bars at both extremes of the range, on wildly different volumes: the
    // mean can reach the bounds but must never pass them.
    const extremes: Array<[number, number, number, number]> = [
      [12, 10, 12, 5000],
      [14, 12, 12, 10],
      [16, 14, 16, 7000],
      [18, 16, 16, 3],
    ];
    const w = col(chaikinMoneyFlow(ohlcv(extremes), { period: 3 }), 'cmf');
    expect(w.filter((x) => x !== undefined)).not.toHaveLength(0);
    for (const x of w)
      if (x !== undefined) expect(Math.abs(x)).toBeLessThanOrEqual(1);
  });

  it('honours output and redirected columns; reads all-missing when misnamed', () => {
    const named = chaikinMoneyFlow(ohlcv(adBars), {
      period: 2,
      close: 'close',
      output: 'flow',
    });
    expect(col(named, 'flow')[1]).toBeCloseTo(-50 / 300, 12);
    const v = col(
      chaikinMoneyFlow(ohlcv(adBars), { period: 2, high: 'nope' as never }),
      'cmf',
    );
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('rejects a bad period and a colliding output', () => {
    expect(() => chaikinMoneyFlow(ohlcv(adBars), { period: 0 })).toThrow(
      TypeError,
    );
    expect(() => chaikinMoneyFlow(ohlcv(adBars), { period: 1.5 })).toThrow(
      TypeError,
    );
    expect(() => chaikinMoneyFlow(ohlcv(adBars), { output: 'high' })).toThrow(
      /collides/,
    );
  });

  it('is all-undefined when the period exceeds the series, length kept', () => {
    const v = col(chaikinMoneyFlow(ohlcv(adBars), { period: 9 }), 'cmf');
    expect(v).toHaveLength(4);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('moneyFlowIndex', () => {
  // h = c + 1, l = c − 1 makes the typical price exactly the close, so the
  // flows are hand-computable: 10, 12, 11, 13 on 100, 200, 300, 400.
  const mfiBars: Array<[number, number, number, number]> = [
    [11, 9, 10, 100],
    [13, 11, 12, 200],
    [12, 10, 11, 300],
    [14, 12, 13, 400],
  ];

  it('is the RSI form on raw money flow, hand-computed', () => {
    const v = col(moneyFlowIndex(ohlcv(mfiBars), { period: 2 }), 'mfi');
    expect(v.slice(0, 2)).toEqual([undefined, undefined]);
    // window {1,2}: up 12·200 = 2400, down 11·300 = 3300
    expect(v[2]).toBeCloseTo((100 * 2400) / 5700, 12);
    // window {2,3}: up 13·400 = 5200, down 3300
    expect(v[3]).toBeCloseTo((100 * 5200) / 8500, 12);
  });

  it('warms up over `period` rows — the first bar has no previous typical price', () => {
    const rows = Array.from(
      { length: 12 },
      (_, i) =>
        [102 + i, 98 + i, 100 + i, 500 + 10 * i] as [
          number,
          number,
          number,
          number,
        ],
    );
    const v = col(moneyFlowIndex(ohlcv(rows), { period: 5 }), 'mfi');
    expect(v.slice(0, 5).every((x) => x === undefined)).toBe(true);
    expect(v[5]).toBeCloseTo(100, 12); // every bar up
  });

  it('is 100 on an all-up window and 0 on an all-down one', () => {
    const up = Array.from(
      { length: 6 },
      (_, i) => [i + 2, i, i + 1, 100] as [number, number, number, number],
    );
    const down = [...up].reverse();
    expect(col(moneyFlowIndex(ohlcv(up), { period: 3 }), 'mfi')[5]).toBeCloseTo(
      100,
      12,
    );
    expect(
      col(moneyFlowIndex(ohlcv(down), { period: 3 }), 'mfi')[5],
    ).toBeCloseTo(0, 12);
  });

  it('emits missing on a window with no flow — where TA-Lib emits 0', () => {
    // Flat typical price: nothing rose and nothing fell. TA-Lib reports 0,
    // its most bearish reading, for a window that showed no direction
    // (measured 0.7.1). This reports no value, the `rsi` flat-window rule.
    const flat = Array.from(
      { length: 6 },
      () => [11, 9, 10, 100] as [number, number, number, number],
    );
    const v = col(moneyFlowIndex(ohlcv(flat), { period: 3 }), 'mfi');
    expect(v.every((x) => x === undefined)).toBe(true);

    // Zero volume is the other way to reach it.
    const noVolume = Array.from(
      { length: 6 },
      (_, i) => [i + 2, i, i + 1, 0] as [number, number, number, number],
    );
    expect(
      col(moneyFlowIndex(ohlcv(noVolume), { period: 3 }), 'mfi').every(
        (x) => x === undefined,
      ),
    ).toBe(true);
  });

  it('an unchanged typical price counts for neither side', () => {
    // Bar 2 repeats bar 1's typical price on a big volume; it must not move
    // the reading, which stays the two-sided answer of bars 1 and 3.
    const v = col(
      moneyFlowIndex(
        ohlcv([
          [11, 9, 10, 100],
          [13, 11, 12, 200],
          [13, 11, 12, 9000],
          [12, 10, 11, 300],
        ]),
        { period: 3 },
      ),
      'mfi',
    );
    expect(v[3]).toBeCloseTo((100 * 2400) / (2400 + 3300), 12);
  });

  it('weights by volume: the same price path on different volume differs', () => {
    const light: Array<[number, number, number, number]> = [
      [11, 9, 10, 100],
      [13, 11, 12, 100],
      [12, 10, 11, 100],
    ];
    const heavyDown: Array<[number, number, number, number]> = [
      [11, 9, 10, 100],
      [13, 11, 12, 100],
      [12, 10, 11, 900],
    ];
    const a = col(moneyFlowIndex(ohlcv(light), { period: 2 }), 'mfi')[2]!;
    const b = col(moneyFlowIndex(ohlcv(heavyDown), { period: 2 }), 'mfi')[2]!;
    expect(b).toBeLessThan(a);
  });

  it('defaults to period 14 and the `mfi` column; honours output', () => {
    const rows = Array.from({ length: 20 }, (_, i) => {
      const c = 100 + 5 * Math.sin(i / 3);
      return [c + 1, c - 1, c, 900 + 100 * (i % 4)] as [
        number,
        number,
        number,
        number,
      ];
    });
    const v = col(moneyFlowIndex(ohlcv(rows)), 'mfi');
    expect(v.slice(0, 14).every((x) => x === undefined)).toBe(true);
    expect(v[14]).toBeDefined();
    expect(v).toEqual(col(moneyFlowIndex(ohlcv(rows), { period: 14 }), 'mfi'));
    expect(
      col(moneyFlowIndex(ohlcv(rows), { period: 14, output: 'flow' }), 'flow'),
    ).toEqual(v);
  });

  it('reads all-missing when an input column is misnamed', () => {
    const v = col(
      moneyFlowIndex(ohlcv(mfiBars), { period: 2, low: 'nope' as never }),
      'mfi',
    );
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('rejects a bad period and a colliding output', () => {
    expect(() => moneyFlowIndex(ohlcv(mfiBars), { period: 0 })).toThrow(
      TypeError,
    );
    expect(() => moneyFlowIndex(ohlcv(mfiBars), { period: 2.5 })).toThrow(
      TypeError,
    );
    expect(() => moneyFlowIndex(ohlcv(mfiBars), { output: 'volume' })).toThrow(
      /collides/,
    );
  });

  it('is all-undefined when the period exceeds the series, length kept', () => {
    const v = col(moneyFlowIndex(ohlcv(mfiBars), { period: 9 }), 'mfi');
    expect(v).toHaveLength(4);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('forceIndex', () => {
  const closes = [10, 12, 11, 13];
  const volumes = [100, 200, 300, 400];

  it('at period 1 is the raw force, Δclose × volume', () => {
    const v = col(forceIndex(cv(closes, volumes), { period: 1 }), 'force');
    expect(v).toEqual([undefined, 400, -300, 800]);
  });

  it('at period 2 is the EMA of that raw force, hand-computed', () => {
    const v = col(forceIndex(cv(closes, volumes), { period: 2 }), 'force');
    const e2 = (2 / 3) * -300 + (1 / 3) * 400;
    const e3 = (2 / 3) * 800 + (1 / 3) * e2;
    expect(v[0]).toBeUndefined();
    expect(v[1]).toBeUndefined(); // one raw value so far
    expect(v[2]).toBeCloseTo(e2, 12);
    expect(v[3]).toBeCloseTo(e3, 12);
  });

  it('warms up over `period` rows, not `period − 1`', () => {
    const rows = Array.from({ length: 12 }, (_, i) => 100 + i);
    const v = col(
      forceIndex(
        cv(
          rows,
          rows.map(() => 500),
        ),
        { period: 4 },
      ),
      'force',
    );
    expect(v.slice(0, 4).every((x) => x === undefined)).toBe(true);
    expect(v[4]).toBeDefined();
  });

  it('uses the SAME EMA the package ships', () => {
    const closesLong = Array.from(
      { length: 20 },
      (_, i) => 100 + 5 * Math.sin(i / 3),
    );
    const vols = closesLong.map((_, i) => 800 + 90 * (i % 5));
    const force = col(forceIndex(cv(closesLong, vols), { period: 4 }), 'force');
    const raw = closesLong.map((c, i) =>
      i === 0 ? undefined : (c - closesLong[i - 1]!) * vols[i]!,
    );
    const rawSeries = new TimeSeries({
      name: 'raw',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'raw', kind: 'number', required: false },
      ] as const,
      rows: raw.map((x, i) => [i, x]) as Array<[number, number | undefined]>,
    });
    const reference = col(
      ema(rawSeries, { period: 4, column: 'raw', output: 'e' }),
      'e',
    );
    for (let i = 0; i < closesLong.length; i += 1) {
      if (reference[i] === undefined)
        expect(force[i], `bar ${i}`).toBeUndefined();
      else expect(force[i]!, `bar ${i}`).toBeCloseTo(reference[i]!, 9);
    }
  });

  it('defaults to Elder’s 13 and the `force` column; honours output', () => {
    const rows = Array.from({ length: 20 }, (_, i) => 100 + i * 0.5);
    const vols = rows.map(() => 700);
    const v = col(forceIndex(cv(rows, vols)), 'force');
    expect(v.slice(0, 13).every((x) => x === undefined)).toBe(true);
    expect(v[13]).toBeDefined();
    expect(v).toEqual(col(forceIndex(cv(rows, vols), { period: 13 }), 'force'));
    expect(
      col(forceIndex(cv(rows, vols), { output: 'elderForce' }), 'elderForce'),
    ).toEqual(v);
  });

  it('a gap costs two bars, then the EMA carries on', () => {
    const v = col(
      forceIndex(
        cvGappy(
          [10, 12, undefined, 13, 14, 15, 16],
          volumes.concat([500, 600, 700]),
        ),
        {
          period: 2,
        },
      ),
      'force',
    );
    expect(v[2]).toBeUndefined();
    expect(v[3]).toBeUndefined(); // its change reads the missing close
    expect(v[4]).toBeDefined();
  });

  it('reads all-missing when a named column is absent', () => {
    const v = col(
      forceIndex(cv(closes, volumes), { volume: 'nope' as never, period: 2 }),
      'force',
    );
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('rejects a bad period and a colliding output', () => {
    expect(() => forceIndex(cv(closes, volumes), { period: 0 })).toThrow(
      TypeError,
    );
    expect(() => forceIndex(cv(closes, volumes), { period: 1.5 })).toThrow(
      TypeError,
    );
    expect(() => forceIndex(cv(closes, volumes), { output: 'close' })).toThrow(
      /collides/,
    );
  });

  it('is all-undefined when the period exceeds the series, length kept', () => {
    const v = col(forceIndex(cv(closes, volumes), { period: 9 }), 'force');
    expect(v).toHaveLength(4);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('easeOfMovement', () => {
  // mid = 11, 13, 14; range = 2, 2, 4; volume = 100, 200, 400.
  const eomBars: Array<[number, number, number, number]> = [
    [12, 10, 11, 100],
    [14, 12, 13, 200],
    [16, 12, 14, 400],
  ];

  it('is the midpoint move divided by the box ratio, hand-computed', () => {
    const v = col(
      easeOfMovement(ohlcv(eomBars), { period: 1, scale: 1 }),
      'eom',
    );
    expect(v[0]).toBeUndefined();
    expect(v[1]).toBeCloseTo((2 * 2) / 200, 12); // 0.02
    expect(v[2]).toBeCloseTo((1 * 4) / 400, 12); // 0.01
  });

  it('defaults the scale to 100,000,000 (StockCharts / ChartIQ)', () => {
    const v = col(easeOfMovement(ohlcv(eomBars), { period: 1 }), 'eom');
    expect(v[1]).toBeCloseTo(0.02 * 100_000_000, 6);
    expect(v[2]).toBeCloseTo(0.01 * 100_000_000, 6);
  });

  it('smooths with the chosen MA over `period` bars', () => {
    const v = col(
      easeOfMovement(ohlcv(eomBars), { period: 2, scale: 1 }),
      'eom',
    );
    expect(v.slice(0, 2).every((x) => x === undefined)).toBe(true);
    expect(v[2]).toBeCloseTo((0.02 + 0.01) / 2, 12);
  });

  it('takes any MaType and routes to the shared engine', () => {
    const rows = Array.from({ length: 16 }, (_, i) => {
      const c = 100 + 4 * Math.sin(i / 2);
      return [c + 1 + 0.3 * (i % 3), c - 1, c, 900 + 200 * (i % 5)] as [
        number,
        number,
        number,
        number,
      ];
    });
    const bySma = col(
      easeOfMovement(ohlcv(rows), { period: 4, maType: 'sma' }),
      'eom',
    );
    const byEma = col(
      easeOfMovement(ohlcv(rows), { period: 4, maType: 'ema' }),
      'eom',
    );
    expect(bySma[15]).not.toBeCloseTo(byEma[15]!, 6);
    expect(() =>
      easeOfMovement(ohlcv(rows), { maType: 'nope' as never }),
    ).toThrow(/unknown moving-average type/);
  });

  it('emits missing for a flat bar and for a bar with no volume', () => {
    const flat = col(
      easeOfMovement(
        ohlcv([
          [12, 10, 11, 100],
          [13, 13, 13, 200],
          [16, 12, 14, 400],
        ]),
        { period: 1, scale: 1 },
      ),
      'eom',
    );
    expect(flat[1]).toBeUndefined(); // the box ratio divides by the range
    expect(flat[2]).toBeDefined();

    const noVolume = col(
      easeOfMovement(
        ohlcv([
          [12, 10, 11, 100],
          [14, 12, 13, 0],
          [16, 12, 14, 400],
        ]),
        { period: 1, scale: 1 },
      ),
      'eom',
    );
    expect(noVolume[1]).toBeUndefined(); // and not ±Infinity
    expect(noVolume[2]).toBeDefined();
  });

  it('guards the zero-volume bar BEFORE the smoother — `smma` would carry an Infinity', () => {
    // Every window MA type masks a non-finite cell, so on `sma` the guard is
    // invisible. `smma` is Wilder's recursion: it CARRIES what it is given,
    // and an unguarded x/0 would reach `withColumn` as ±Infinity, which it
    // rejects loudly. Measured: with the guard, the zero-volume bar poisons
    // the smma seed and the column is empty (the documented smma gap rule);
    // the `sma` and `ema` columns over the same bars recover.
    const rows: Array<[number, number, number, number]> = [
      [12, 10, 11, 100],
      [14, 12, 13, 200],
      [16, 12, 14, 0],
      [18, 14, 16, 400],
      [20, 16, 18, 500],
      [22, 18, 20, 600],
    ];
    const bySmma = col(
      easeOfMovement(ohlcv(rows), { period: 2, maType: 'smma', scale: 1 }),
      'eom',
    );
    expect(bySmma).toHaveLength(6);
    expect(bySmma.every((x) => x === undefined)).toBe(true);
    const bySma = col(
      easeOfMovement(ohlcv(rows), { period: 2, maType: 'sma', scale: 1 }),
      'eom',
    );
    expect(bySma[4]).toBeCloseTo(0.018, 12);
  });

  it('defaults to period 14 / sma and the `eom` column; honours output', () => {
    const rows = Array.from({ length: 20 }, (_, i) => {
      const c = 100 + i;
      return [c + 1, c - 1, c, 1000] as [number, number, number, number];
    });
    const v = col(easeOfMovement(ohlcv(rows)), 'eom');
    expect(v.slice(0, 14).every((x) => x === undefined)).toBe(true);
    expect(v[14]).toBeDefined();
    expect(v).toEqual(
      col(easeOfMovement(ohlcv(rows), { period: 14, maType: 'sma' }), 'eom'),
    );
    expect(col(easeOfMovement(ohlcv(rows), { output: 'emv' }), 'emv')).toEqual(
      v,
    );
  });

  it('reads all-missing when an input column is misnamed', () => {
    const v = col(
      easeOfMovement(ohlcv(eomBars), { period: 1, volume: 'nope' as never }),
      'eom',
    );
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('rejects a bad period, a bad scale, and a colliding output', () => {
    expect(() => easeOfMovement(ohlcv(eomBars), { period: 0 })).toThrow(
      TypeError,
    );
    expect(() => easeOfMovement(ohlcv(eomBars), { period: 1.5 })).toThrow(
      TypeError,
    );
    expect(() => easeOfMovement(ohlcv(eomBars), { scale: 0 })).toThrow(
      /scale must be a positive/,
    );
    expect(() => easeOfMovement(ohlcv(eomBars), { scale: -1 })).toThrow(
      /scale must be a positive/,
    );
    expect(() =>
      easeOfMovement(ohlcv(eomBars), { scale: Number.POSITIVE_INFINITY }),
    ).toThrow(/scale must be a positive/);
    expect(() => easeOfMovement(ohlcv(eomBars), { output: 'high' })).toThrow(
      /collides/,
    );
  });

  it('is all-undefined when the period exceeds the series, length kept', () => {
    const v = col(easeOfMovement(ohlcv(eomBars), { period: 9 }), 'eom');
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('volumeOscillator', () => {
  const volumes = [10, 20, 30, 40, 50];
  const bars5 = volumes.map(
    (v, i) => [102 + i, 98 + i, 100 + i, v] as [number, number, number, number],
  );

  it('is the percent spread of two volume MAs, hand-computed', () => {
    const v = col(
      volumeOscillator(ohlcv(bars5), { fastPeriod: 1, slowPeriod: 2 }),
      'volOsc',
    );
    expect(v[0]).toBeUndefined();
    expect(v[1]).toBeCloseTo((100 * (20 - 15)) / 15, 12);
    expect(v[2]).toBeCloseTo((100 * (30 - 25)) / 25, 12);
    expect(v[4]).toBeCloseTo((100 * (50 - 45)) / 45, 12);
  });

  it('IS priceOscillator over the volume column — the alias, pinned', () => {
    const rows = Array.from({ length: 30 }, (_, i) => {
      const c = 100 + 5 * Math.sin(i / 3);
      return [
        c + 1,
        c - 1,
        c,
        900 + 400 * Math.sin(i / 2.2) + (i % 7 === 3 ? 4000 : 0),
      ] as [number, number, number, number];
    });
    const alias = col(volumeOscillator(ohlcv(rows)), 'volOsc');
    const explicit = col(
      priceOscillator(ohlcv(rows), {
        column: 'volume',
        mode: 'percent',
        maType: 'sma',
        fastPeriod: 5,
        slowPeriod: 10,
        output: 'volOsc',
      }),
      'volOsc',
    );
    expect(alias).toEqual(explicit);
  });

  it('defaults to 5 / 10 / sma — not priceOscillator’s 12 / 26 / ema', () => {
    const rows = Array.from({ length: 30 }, (_, i) => {
      const c = 100 + i * 0.3;
      return [c + 1, c - 1, c, 1000 + 300 * Math.sin(i / 2)] as [
        number,
        number,
        number,
        number,
      ];
    });
    const v = col(volumeOscillator(ohlcv(rows)), 'volOsc');
    expect(v.slice(0, 9).every((x) => x === undefined)).toBe(true);
    expect(v[9]).toBeDefined();
    expect(v[29]).not.toBeCloseTo(
      col(volumeOscillator(ohlcv(rows), { fastPeriod: 4 }), 'volOsc')[29]!,
      6,
    );
    expect(v[29]).not.toBeCloseTo(
      col(volumeOscillator(ohlcv(rows), { maType: 'ema' }), 'volOsc')[29]!,
      6,
    );
  });

  it('reads the volume column, and a redirected one', () => {
    const s = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'close', kind: 'number' },
        { name: 'qty', kind: 'number' },
      ] as const,
      rows: volumes.map((v, i) => [i, 100 + i, v]) as Array<
        [number, number, number]
      >,
    });
    const v = col(
      volumeOscillator(s, { fastPeriod: 1, slowPeriod: 2, volume: 'qty' }),
      'volOsc',
    );
    expect(v[1]).toBeCloseTo((100 * (20 - 15)) / 15, 12);
  });

  it('emits missing, not Infinity, on a window of zero-volume bars', () => {
    const dead = Array.from(
      { length: 4 },
      (_, i) =>
        [102 + i, 98 + i, 100 + i, 0] as [number, number, number, number],
    );
    const v = col(
      volumeOscillator(ohlcv(dead), { fastPeriod: 1, slowPeriod: 2 }),
      'volOsc',
    );
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('honours output; rejects a swapped pair and a colliding output', () => {
    expect(
      col(
        volumeOscillator(ohlcv(bars5), {
          fastPeriod: 1,
          slowPeriod: 2,
          output: 'vo',
        }),
        'vo',
      )[1],
    ).toBeCloseTo((100 * (20 - 15)) / 15, 12);
    expect(() =>
      volumeOscillator(ohlcv(bars5), { fastPeriod: 10, slowPeriod: 5 }),
    ).toThrow(/shorter/);
    expect(() => volumeOscillator(ohlcv(bars5), { fastPeriod: 0 })).toThrow(
      TypeError,
    );
    // The error names THIS study, not the one it delegates to.
    expect(() =>
      volumeOscillator(ohlcv(bars5), { fastPeriod: 10, slowPeriod: 5 }),
    ).toThrow(/^volumeOscillator fastPeriod/);
    expect(() => volumeOscillator(ohlcv(bars5), { output: 'close' })).toThrow(
      /collides/,
    );
  });

  it('is all-undefined when the slow period exceeds the series, length kept', () => {
    const v = col(
      volumeOscillator(ohlcv(bars5), { fastPeriod: 2, slowPeriod: 9 }),
      'volOsc',
    );
    expect(v).toHaveLength(5);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('volumeRateOfChange is percentChange over volume — no second study', () => {
  it('percentChange({ column: volume }) IS the VROC definition', () => {
    // Corpus §6.6 lists "Volume Rate of Change" as `(V / V[−n] − 1) × 100`,
    // which is exactly `percentChange`'s formula (itself cross-checked
    // against TA-Lib's ROC). Shipping a `volumeRateOfChange` study would be
    // a rename with a different default column — step 0 of the studies
    // README. This test is the recipe, and the guard that it stays true.
    const volumes = [100, 120, 90, 150, 200];
    const s = ohlcv(
      volumes.map(
        (v, i) =>
          [102 + i, 98 + i, 100 + i, v] as [number, number, number, number],
      ),
    );
    const vroc = col(
      percentChange(s, { column: 'volume', periods: 2, output: 'vroc' }),
      'vroc',
    );
    expect(vroc.slice(0, 2).every((x) => x === undefined)).toBe(true);
    expect(vroc[2]).toBeCloseTo(-10, 12); // 90 / 100 − 1
    expect(vroc[3]).toBeCloseTo(25, 12); // 150 / 120 − 1
    expect(vroc[4]).toBeCloseTo((200 / 90 - 1) * 100, 12);
  });
});

/* ========================================================================== */
/* The momentum tail (assessment §6.3): CMO, Ultimate, CCI, IMI, RVI, PSY.    */
/*                                                                            */
/* The oracle pins the VALUES against TA-Lib (Ultimate, CCI) or pandas (the   */
/* other four). What is pinned here is what it cannot see: the hand-computed  */
/* arithmetic on a tiny fixture, the definition forks (Chande vs TA-Lib's     */
/* Wilder-smoothed CMO, the symmetric vs linear 4-bar filter), the            */
/* zero-denominator guards, the defaults, and validation.                     */
/* ========================================================================== */

/** Open/high/low/close bars — the four-input shape `relativeVigorIndex`
 *  reads, and the open/close pair `intradayMomentumIndex` does. */
const ohlcBars = (rows: Array<[number, number, number, number]>) =>
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

/** A non-degenerate wavy close series: never monotonic, never flat. */
const wavyCloses = Array.from(
  { length: 40 },
  (_, i) => 100 + 8 * Math.sin(i / 3.5) + 0.3 * i,
);

describe('chandeMomentum', () => {
  it('is 100·(Σup − Σdown)/(Σup + Σdown), hand-computed', () => {
    // Changes: _, +2, −1, +3, −1.
    const v = col(
      chandeMomentum(bars([10, 12, 11, 14, 13]), { period: 2 }),
      'cmo',
    );
    expect(v).toHaveLength(5);
    // Warm-up is `period` rows, not period − 1: the study reads differences.
    expect(v[0]).toBeUndefined();
    expect(v[1]).toBeUndefined();
    expect(v[2]).toBeCloseTo((100 * (2 - 1)) / (2 + 1), 12); // {+2, −1}
    expect(v[3]).toBeCloseTo((100 * (3 - 1)) / (3 + 1), 12); // {−1, +3}
    expect(v[4]).toBeCloseTo((100 * (3 - 1)) / (3 + 1), 12); // {+3, −1}
  });

  it('saturates at ±100 on one-sided windows, and an unchanged bar counts as neither', () => {
    expect(
      col(chandeMomentum(bars([1, 2, 3, 4]), { period: 2 }), 'cmo')[3],
    ).toBe(100);
    expect(
      col(chandeMomentum(bars([4, 3, 2, 1]), { period: 2 }), 'cmo')[3],
    ).toBe(-100);
    // One flat bar among gains does not count as a down bar; it just shrinks
    // the up sum's share of nothing — the reading stays +100.
    expect(
      col(chandeMomentum(bars([1, 2, 2, 3]), { period: 2 }), 'cmo')[3],
    ).toBe(100);
  });

  it('a perfectly flat window is undefined, not 0', () => {
    const v = col(chandeMomentum(bars([5, 5, 5, 5, 5]), { period: 2 }), 'cmo');
    expect(v).toHaveLength(5);
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('is Chande’s UNSMOOTHED form, not TA-Lib’s (which is 2·rsi − 100)', () => {
    // The definition fork this study exists to get right. TA-Lib smooths the
    // two legs with Wilder's recursion, which makes its CMO an affine
    // restatement of the shipped `rsi` — so ours must NOT equal 2·rsi − 100.
    const src = bars(wavyCloses);
    const ours = col(chandeMomentum(src, { period: 5 }), 'cmo');
    const wilder = col(rsi(src, { period: 5 }), 'rsi');
    let worst = 0;
    for (let i = 0; i < wavyCloses.length; i += 1) {
      if (ours[i] === undefined || wilder[i] === undefined) continue;
      worst = Math.max(worst, Math.abs(ours[i]! - (2 * wilder[i]! - 100)));
    }
    // Measured on this series (against TA-Lib's own RSI): the two definitions
    // are 80.58 points apart at their widest, on a scale spanning 200. A
    // version that smoothed the legs would collapse this to ~0.
    expect(worst).toBeGreaterThan(10);
  });

  it('recovers after an interior gap (a window forgets; rsi’s recursion does not)', () => {
    const gappy = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'close', kind: 'number', required: false },
      ] as const,
      rows: [10, 12, undefined, 14, 13, 15, 16, 18].map((c, i) => [
        i,
        c,
      ]) as never,
    });
    const v = col(chandeMomentum(gappy as never, { period: 2 }), 'cmo');
    // The gap costs its own bar and the two after it (both windows contain a
    // missing change); bar 5 is the first with a full window of changes again.
    expect(v.slice(0, 5).every((x) => x === undefined)).toBe(true);
    expect(v[5]).toBeDefined();
  });

  it('defaults to period 14 and the `cmo` column; honours column and output', () => {
    const v = col(chandeMomentum(bars(wavyCloses)), 'cmo');
    expect(v.slice(0, 14).every((x) => x === undefined)).toBe(true);
    expect(v[14]).toBeDefined();
    expect(v).toEqual(
      col(chandeMomentum(bars(wavyCloses), { period: 14 }), 'cmo'),
    );
    const renamed = chandeMomentum(
      sma(bars(wavyCloses), { period: 3, output: 'fast' }),
      { period: 4, column: 'fast', output: 'cmoFast' },
    );
    expect(col(renamed, 'cmoFast')[20]).toBeDefined();
  });

  it('rejects a bad period and a colliding output; a misnamed column reads empty', () => {
    expect(() => chandeMomentum(bars([1, 2, 3]), { period: 0 })).toThrow(
      TypeError,
    );
    expect(() => chandeMomentum(bars([1, 2, 3]), { period: 1.5 })).toThrow(
      TypeError,
    );
    expect(() => chandeMomentum(bars([1, 2, 3]), { output: 'close' })).toThrow(
      /collides/,
    );
    expect(
      col(
        chandeMomentum(bars([1, 2, 3]), { period: 2, column: 'nope' as never }),
        'cmo',
      ).every((x) => x === undefined),
    ).toBe(true);
  });

  it('is all-undefined when the period exceeds the series, length kept', () => {
    const v = col(chandeMomentum(bars([10, 12, 11]), { period: 5 }), 'cmo');
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('ultimateOscillator', () => {
  // h, l, c per bar. TR and BP both read the PREVIOUS close:
  //   bar 1: TR = 13 − 10 = 3, BP = 12 − 10 = 2
  //   bar 2: TR = 13 − 11 = 2, BP = 11 − 11 = 0
  //   bar 3: TR = 14 − 11 = 3, BP = 14 − 11 = 3
  const src: Array<[number, number, number]> = [
    [11, 9, 10],
    [13, 10, 12],
    [13, 11, 11],
    [14, 12, 14],
  ];

  it('is the 4/2/1-weighted blend of three BP/TR ratios, hand-computed', () => {
    const v = col(
      ultimateOscillator(hlc(src), {
        shortPeriod: 1,
        mediumPeriod: 2,
        longPeriod: 3,
      }),
      'uo',
    );
    expect(v).toHaveLength(4);
    // Warm-up is the LONGEST period, and it is `longPeriod` rows rather than
    // `longPeriod − 1` because bar 0 has no previous close.
    expect(v.slice(0, 3).every((x) => x === undefined)).toBe(true);
    const a1 = 3 / 3;
    const a2 = (0 + 3) / (2 + 3);
    const a3 = (2 + 0 + 3) / (3 + 2 + 3);
    expect(v[3]).toBeCloseTo((100 * (4 * a1 + 2 * a2 + a3)) / 7, 12);
  });

  it('a window of zero true range is undefined, not ±Infinity', () => {
    // Every bar has no range, so TR sums to 0 — but the last close jumps, so
    // buying pressure does not. Without the guard this is a division by zero.
    const flatRange: Array<[number, number, number]> = [
      [10, 10, 10],
      [10, 10, 10],
      [10, 10, 10],
      [10, 10, 20],
    ];
    const v = col(
      ultimateOscillator(hlc(flatRange), {
        shortPeriod: 1,
        mediumPeriod: 2,
        longPeriod: 3,
      }),
      'uo',
    );
    expect(v).toHaveLength(4);
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('any one leg being undefined makes the reading undefined', () => {
    // The two shorter legs are ready well before the longest one; the study
    // must wait for all three rather than emitting a partial blend.
    const rows = Array.from({ length: 12 }, (_, i) => {
      const c = 100 + 4 * Math.sin(i / 2.2) + 0.2 * i;
      return [c + 0.6, c - 0.5, c] as [number, number, number];
    });
    const v = col(
      ultimateOscillator(hlc(rows), {
        shortPeriod: 2,
        mediumPeriod: 4,
        longPeriod: 9,
      }),
      'uo',
    );
    expect(v.slice(0, 9).every((x) => x === undefined)).toBe(true);
    expect(v[9]).toBeDefined();
  });

  it('defaults to 7/14/28 and the `uo` column; honours the bar columns and output', () => {
    const rows = Array.from({ length: 34 }, (_, i) => {
      const c = 100 + 6 * Math.sin(i / 4) + 0.25 * i;
      return [c + 0.8, c - 0.7, c] as [number, number, number];
    });
    const v = col(ultimateOscillator(hlc(rows)), 'uo');
    expect(v.slice(0, 28).every((x) => x === undefined)).toBe(true);
    expect(v[28]).toBeDefined();
    expect(v).toEqual(
      col(
        ultimateOscillator(hlc(rows), {
          shortPeriod: 7,
          mediumPeriod: 14,
          longPeriod: 28,
        }),
        'uo',
      ),
    );
    // The weights are positional, so a different short period must move it.
    expect(v[33]).not.toBeCloseTo(
      col(ultimateOscillator(hlc(rows), { shortPeriod: 5 }), 'uo')[33]!,
      6,
    );
    const named = ultimateOscillator(hlc(src), {
      shortPeriod: 1,
      mediumPeriod: 2,
      longPeriod: 3,
      high: 'high',
      low: 'low',
      close: 'close',
      output: 'ultimate',
    });
    expect(col(named, 'ultimate')[3]).toBeDefined();
  });

  it('rejects bad periods, a non-increasing triple, and a colliding output', () => {
    expect(() => ultimateOscillator(hlc(src), { shortPeriod: 0 })).toThrow(
      TypeError,
    );
    expect(() => ultimateOscillator(hlc(src), { mediumPeriod: 2.5 })).toThrow(
      TypeError,
    );
    expect(() =>
      ultimateOscillator(hlc(src), {
        shortPeriod: 14,
        mediumPeriod: 7,
        longPeriod: 28,
      }),
    ).toThrow(/strictly increasing/);
    expect(() =>
      ultimateOscillator(hlc(src), {
        shortPeriod: 7,
        mediumPeriod: 14,
        longPeriod: 14,
      }),
    ).toThrow(/strictly increasing/);
    expect(() => ultimateOscillator(hlc(src), { output: 'close' })).toThrow(
      /collides/,
    );
  });

  it('reads all-missing when an input column is misnamed, length kept', () => {
    const v = col(
      ultimateOscillator(hlc(src), {
        shortPeriod: 1,
        mediumPeriod: 2,
        longPeriod: 3,
        low: 'nope' as never,
      }),
      'uo',
    );
    expect(v).toHaveLength(4);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('commodityChannelIndex', () => {
  // Typical prices 10, 13, 16, 13 (built as close ± 1 around each).
  const src = [10, 13, 16, 13].map(
    (tp) => [tp + 1, tp - 1, tp] as [number, number, number],
  );

  it('is (tp − SMA)/(0.015 · meanAbsDev), hand-computed', () => {
    const v = col(commodityChannelIndex(hlc(src), { period: 3 }), 'cci');
    expect(v).toHaveLength(4);
    expect(v[0]).toBeUndefined();
    expect(v[1]).toBeUndefined();
    // {10,13,16}: mean 13, mad (3+0+3)/3 = 2 → (16−13)/(0.015·2) = 100.
    expect(v[2]).toBeCloseTo(100, 10);
    // {13,16,13}: mean 14, mad (1+2+1)/3 = 4/3 → (13−14)/(0.015·4/3) = −50.
    expect(v[3]).toBeCloseTo(-50, 10);
  });

  it('uses the MEAN ABSOLUTE deviation, not the standard deviation', () => {
    // On the window above the population stdev is √6 ≈ 2.449, not 2, so a
    // z-score-shaped implementation lands at ≈81.6 rather than 100.
    const v = col(commodityChannelIndex(hlc(src), { period: 3 }), 'cci');
    const zLike = 3 / (0.015 * Math.sqrt(6));
    expect(v[2]).not.toBeCloseTo(zLike, 3);
  });

  it('a window with zero deviation is undefined, not TA-Lib’s 0', () => {
    const flat = [12, 12, 12, 12].map(
      (tp) => [tp + 1, tp - 1, tp] as [number, number, number],
    );
    const v = col(commodityChannelIndex(hlc(flat), { period: 3 }), 'cci');
    expect(v).toHaveLength(4);
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('reads the typical price, not the close', () => {
    // The closes sit on each bar's low with a VARYING half-range, so a study
    // reading the close differs from this one by more than a constant.
    const rows: Array<[number, number, number]> = Array.from(
      { length: 24 },
      (_, i) => {
        const m = 100 + 5 * Math.sin(i / 2.5) + 0.3 * i;
        const halfRange = 1 + 0.9 * Math.abs(Math.cos(i / 1.7));
        return [m + halfRange, m - halfRange, m - halfRange];
      },
    );
    const onTypical = col(
      commodityChannelIndex(hlc(rows), { period: 5 }),
      'cci',
    );
    // Redirecting high and low at the close makes tp === close exactly.
    const onClose = col(
      commodityChannelIndex(hlc(rows), {
        period: 5,
        high: 'close',
        low: 'close',
        output: 'cciClose',
      }),
      'cciClose',
    );
    expect(onTypical[20]).not.toBeCloseTo(onClose[20]!, 3);
  });

  it('defaults to period 20 and the `cci` column', () => {
    const rows = Array.from({ length: 26 }, (_, i) => {
      const c = 100 + 7 * Math.sin(i / 4) + 0.2 * i;
      return [c + 0.9, c - 0.8, c] as [number, number, number];
    });
    const v = col(commodityChannelIndex(hlc(rows)), 'cci');
    expect(v.slice(0, 19).every((x) => x === undefined)).toBe(true);
    expect(v[19]).toBeDefined();
    expect(v).toEqual(
      col(commodityChannelIndex(hlc(rows), { period: 20 }), 'cci'),
    );
  });

  it('rejects a bad period and a colliding output; a misnamed input reads empty', () => {
    expect(() => commodityChannelIndex(hlc(src), { period: 0 })).toThrow(
      TypeError,
    );
    expect(() => commodityChannelIndex(hlc(src), { period: 2.5 })).toThrow(
      TypeError,
    );
    expect(() => commodityChannelIndex(hlc(src), { output: 'close' })).toThrow(
      /collides/,
    );
    expect(
      col(
        commodityChannelIndex(hlc(src), { period: 3, high: 'nope' as never }),
        'cci',
      ).every((x) => x === undefined),
    ).toBe(true);
  });

  it('is all-undefined when the period exceeds the series, length kept', () => {
    const v = col(commodityChannelIndex(hlc(src), { period: 9 }), 'cci');
    expect(v).toHaveLength(4);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('intradayMomentumIndex', () => {
  // Bodies (close − open): +2, −1, +3, 0.
  const src: Array<[number, number, number, number]> = [
    [10, 13, 9, 12],
    [12, 13, 10, 11],
    [11, 15, 10, 14],
    [14, 15, 13, 14],
  ];

  it('is 100·Σgain/(Σgain + Σloss) over the candle body, hand-computed', () => {
    const v = col(intradayMomentumIndex(ohlcBars(src), { period: 2 }), 'imi');
    expect(v).toHaveLength(4);
    // Warm-up is period − 1: a body needs no previous bar.
    expect(v[0]).toBeUndefined();
    expect(v[1]).toBeCloseTo((100 * 2) / 3, 12); // {+2, −1}
    expect(v[2]).toBeCloseTo(75, 12); // {−1, +3}
    expect(v[3]).toBeCloseTo(100, 12); // {+3, 0} — the doji adds nothing
  });

  it('an all-doji window is undefined, not 0 or 50', () => {
    const dojis: Array<[number, number, number, number]> = [
      [10, 11, 9, 10],
      [10, 11, 9, 10],
      [10, 11, 9, 10],
    ];
    const v = col(intradayMomentumIndex(ohlcBars(dojis), { period: 2 }), 'imi');
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('is (chandeMomentum + 100)/2 when the open is the previous close', () => {
    // Same form, different input: on a tape with no overnight gaps the candle
    // body IS the close-to-close change, and the two studies coincide up to
    // the affine map. This pins both formulas against each other.
    const rows = wavyCloses.map(
      (c, i) =>
        [
          i === 0 ? c - 0.5 : wavyCloses[i - 1]!,
          Math.max(c, wavyCloses[i - 1] ?? c) + 1,
          Math.min(c, wavyCloses[i - 1] ?? c) - 1,
          c,
        ] as [number, number, number, number],
    );
    const series = ohlcBars(rows);
    const imi = col(intradayMomentumIndex(series, { period: 5 }), 'imi');
    const cmo = col(chandeMomentum(series, { period: 5 }), 'cmo');
    for (let i = 5; i < rows.length; i += 1) {
      expect(imi[i], `bar ${i}`).toBeCloseTo((cmo[i]! + 100) / 2, 9);
    }
    // …and NOT before bar `period`: imi warms up one row earlier.
    expect(imi[4]).toBeDefined();
    expect(cmo[4]).toBeUndefined();
  });

  it('defaults to period 14 and the `imi` column; honours open/close and output', () => {
    const rows = Array.from({ length: 20 }, (_, i) => {
      const c = 100 + 5 * Math.sin(i / 3);
      const o = c - 0.6 * Math.cos(i / 1.9);
      return [o, Math.max(o, c) + 0.5, Math.min(o, c) - 0.5, c] as [
        number,
        number,
        number,
        number,
      ];
    });
    const v = col(intradayMomentumIndex(ohlcBars(rows)), 'imi');
    expect(v.slice(0, 13).every((x) => x === undefined)).toBe(true);
    expect(v[13]).toBeDefined();
    const named = intradayMomentumIndex(ohlcBars(rows), {
      period: 4,
      open: 'open',
      close: 'close',
      output: 'chandeImi',
    });
    expect(col(named, 'chandeImi')[10]).toBeDefined();
  });

  it('rejects a bad period and a colliding output; a series with no open reads empty', () => {
    expect(() => intradayMomentumIndex(ohlcBars(src), { period: 0 })).toThrow(
      TypeError,
    );
    expect(() => intradayMomentumIndex(ohlcBars(src), { period: 1.5 })).toThrow(
      TypeError,
    );
    expect(() =>
      intradayMomentumIndex(ohlcBars(src), { output: 'close' }),
    ).toThrow(/collides/);
    // The likely misconfiguration: a close-only series.
    expect(
      col(
        intradayMomentumIndex(bars([10, 11, 12, 13]) as never, { period: 2 }),
        'imi',
      ).every((x) => x === undefined),
    ).toBe(true);
  });
});

describe('relativeVigorIndex', () => {
  /** Bars with the given bodies (close − open) and a constant 30-wide range. */
  const vigorBars = (bodies: number[]) =>
    ohlcBars(
      bodies.map(
        (b) => [100 - b, 115, 85, 100] as [number, number, number, number],
      ),
    );

  it('is ΣSWMA(body)/ΣSWMA(range) with a SWMA signal, hand-computed', () => {
    // Bodies 0,0,0,6,0,0,12,0 → SWMA (1,2,2,1)/6 gives num = 1,2,2,3,4 from
    // bar 3; the range is a flat 30, so its SWMA is 30 on every emitted bar.
    const r = relativeVigorIndex(vigorBars([0, 0, 0, 6, 0, 0, 12, 0]), {
      period: 2,
    });
    const v = col(r, 'rvi');
    const sig = col(r, 'rviSignal');
    expect(v).toHaveLength(8);
    // Per-column warm-up: the index at period + 2, the signal three later.
    expect(v.slice(0, 4).every((x) => x === undefined)).toBe(true);
    expect(sig.slice(0, 7).every((x) => x === undefined)).toBe(true);
    expect(v[4]).toBeCloseTo(3 / 60, 12); // (num 2 + num 1) / (30 + 30)
    expect(v[5]).toBeCloseTo(4 / 60, 12);
    expect(v[6]).toBeCloseTo(5 / 60, 12);
    expect(v[7]).toBeCloseTo(7 / 60, 12);
    // signal = SWMA of the index over its own last four values.
    expect(sig[7]).toBeCloseTo(7 / 90, 12);
  });

  it('smooths with the SYMMETRIC (1,2,2,1)/6 weights, not a linear wma(4)', () => {
    // A single unit body spike: the symmetric filter answers 1, 2, 2, 1 (over
    // 6); a linear wma(4) would answer 4, 3, 2, 1 (over 10) — the response is
    // the filter, so this is what tells the two apart.
    const r = relativeVigorIndex(vigorBars([0, 0, 0, 6, 0, 0, 0, 0]), {
      period: 1,
    });
    const v = col(r, 'rvi');
    expect(v[3]).toBeCloseTo(1 / 30, 12);
    expect(v[4]).toBeCloseTo(2 / 30, 12);
    expect(v[5]).toBeCloseTo(2 / 30, 12);
    expect(v[6]).toBeCloseTo(1 / 30, 12);
    expect(v[7]).toBeCloseTo(0, 12);
  });

  it('a window of zero range is undefined on both columns, not ±Infinity', () => {
    const noRange: Array<[number, number, number, number]> = Array.from(
      { length: 10 },
      () => [98, 100, 100, 100],
    );
    const r = relativeVigorIndex(ohlcBars(noRange), { period: 2 });
    for (const name of ['rvi', 'rviSignal']) {
      expect(col(r, name), name).toHaveLength(10);
      expect(
        col(r, name).every((x) => x === undefined),
        name,
      ).toBe(true);
    }
  });

  it('defaults to period 10 and the `rvi` prefix; honours a custom prefix', () => {
    const rows = Array.from({ length: 24 }, (_, i) => {
      const c = 100 + 6 * Math.sin(i / 3) + 0.2 * i;
      const o = c - 0.7 * Math.cos(i / 2.1);
      return [o, Math.max(o, c) + 0.9, Math.min(o, c) - 0.8, c] as [
        number,
        number,
        number,
        number,
      ];
    });
    const d = relativeVigorIndex(ohlcBars(rows));
    expect(
      col(d, 'rvi')
        .slice(0, 12)
        .every((x) => x === undefined),
    ).toBe(true);
    expect(col(d, 'rvi')[12]).toBeDefined();
    expect(
      col(d, 'rviSignal')
        .slice(0, 15)
        .every((x) => x === undefined),
    ).toBe(true);
    expect(col(d, 'rviSignal')[15]).toBeDefined();
    const named = relativeVigorIndex(ohlcBars(rows), {
      period: 3,
      prefix: 'vigor',
      open: 'open',
      high: 'high',
      low: 'low',
      close: 'close',
    });
    expect(col(named, 'vigor')[10]).toBeDefined();
    expect(col(named, 'vigorSignal')[10]).toBeDefined();
  });

  it('rejects a bad period and a colliding prefix; a misnamed input reads empty', () => {
    const b = vigorBars([1, 2, 3, 4, 5, 6, 7, 8]);
    expect(() => relativeVigorIndex(b, { period: 0 })).toThrow(TypeError);
    expect(() => relativeVigorIndex(b, { period: 2.5 })).toThrow(TypeError);
    expect(() => relativeVigorIndex(b, { prefix: 'close' })).toThrow(
      /collides/,
    );
    const once = relativeVigorIndex(b, { period: 2 });
    expect(() => relativeVigorIndex(once as never, { period: 2 })).toThrow(
      /collides/,
    );
    expect(
      col(
        relativeVigorIndex(b, { period: 2, high: 'nope' as never }),
        'rvi',
      ).every((x) => x === undefined),
    ).toBe(true);
  });

  it('is all-undefined when the period exceeds the series, length kept', () => {
    const r = relativeVigorIndex(vigorBars([1, 2, 3, 4]), { period: 9 });
    for (const name of ['rvi', 'rviSignal']) {
      expect(col(r, name), name).toHaveLength(4);
      expect(
        col(r, name).every((x) => x === undefined),
        name,
      ).toBe(true);
    }
  });
});

describe('psychologicalLine', () => {
  it('is the percent of up closes, hand-computed, with a flat bar NOT up', () => {
    // Directions: _, up, up, down, flat.
    const v = col(
      psychologicalLine(bars([10, 11, 12, 11, 11]), { period: 2 }),
      'psy',
    );
    expect(v).toHaveLength(5);
    // Warm-up is `period` rows: bar 0 has no direction.
    expect(v[0]).toBeUndefined();
    expect(v[1]).toBeUndefined();
    expect(v[2]).toBe(100); // {up, up}
    expect(v[3]).toBe(50); // {up, down}
    expect(v[4]).toBe(0); // {down, flat} — an unchanged close is not an up bar
  });

  it('takes only the period + 1 quantised levels, and reaches both ends', () => {
    const v = col(psychologicalLine(bars(wavyCloses), { period: 4 }), 'psy');
    const seen = new Set(v.filter((x) => x !== undefined));
    for (const x of seen)
      expect((x! * 4) / 100).toBeCloseTo(Math.round((x! * 4) / 100), 12);
    expect(Math.max(...seen)).toBe(100);
    expect(Math.min(...seen)).toBe(0);
  });

  it('a bar with no close costs its own direction AND the next bar’s', () => {
    const gappy = new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'close', kind: 'number', required: false },
      ] as const,
      rows: [10, 11, undefined, 13, 14, 15, 16].map((c, i) => [i, c]) as never,
    });
    const v = col(psychologicalLine(gappy as never, { period: 2 }), 'psy');
    // Bar 2's own close is missing and bar 3's predecessor is, so NEITHER has
    // a direction; every window covering either is missing, which carries the
    // hole to bar 4 as well. Bar 5 is the first with two directed bars again.
    expect(v.slice(0, 5).every((x) => x === undefined)).toBe(true);
    expect(v[5]).toBe(100);
  });

  it('defaults to period 12 and the `psy` column; honours column and output', () => {
    const v = col(psychologicalLine(bars(wavyCloses)), 'psy');
    expect(v.slice(0, 12).every((x) => x === undefined)).toBe(true);
    expect(v[12]).toBeDefined();
    expect(v).toEqual(
      col(psychologicalLine(bars(wavyCloses), { period: 12 }), 'psy'),
    );
    const renamed = psychologicalLine(
      sma(bars(wavyCloses), { period: 3, output: 'fast' }),
      { period: 4, column: 'fast', output: 'psyFast' },
    );
    expect(col(renamed, 'psyFast')[20]).toBeDefined();
  });

  it('rejects a bad period and a colliding output; a misnamed column reads empty', () => {
    expect(() => psychologicalLine(bars([1, 2, 3]), { period: 0 })).toThrow(
      TypeError,
    );
    expect(() => psychologicalLine(bars([1, 2, 3]), { period: 1.5 })).toThrow(
      TypeError,
    );
    expect(() =>
      psychologicalLine(bars([1, 2, 3]), { output: 'close' }),
    ).toThrow(/collides/);
    expect(
      col(
        psychologicalLine(bars([1, 2, 3]), {
          period: 2,
          column: 'nope' as never,
        }),
        'psy',
      ).every((x) => x === undefined),
    ).toBe(true);
  });

  it('is all-undefined when the period exceeds the series, length kept', () => {
    const v = col(psychologicalLine(bars([10, 12, 11]), { period: 5 }), 'psy');
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

/* -------------------------------------------------------------------------- */
/* The Wilder directional group (corpus §6.4).                                 */
/* -------------------------------------------------------------------------- */

/** High/low/close bars at 1ms spacing. */
const hlcBars = (rows: Array<[number, number, number]>) =>
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

/** Non-degenerate OHLC bars: never monotonic, varying range, close never on
 *  an extreme. Long enough for the default ADXR warm-up (3·14 − 2 = 40). */
const wavyOhlc = (length = 60) =>
  ohlcBars(
    Array.from({ length }, (_, i) => {
      const c = 100 + 8 * Math.sin(i / 3.5) + 0.3 * i;
      const o = c - 0.9 * Math.cos(i / 2.1);
      return [
        o,
        Math.max(o, c) + 0.5 + 0.8 * Math.abs(Math.sin(i / 2.3)),
        Math.min(o, c) - 0.5 - 0.8 * Math.abs(Math.cos(i / 1.9)),
        c,
      ] as [number, number, number, number];
    }),
  );

/** The worked fixture `directionalMovement` and `vortex` are hand-computed
 *  on. Per bar (h, l, c), with the derivations spelled out:
 *
 *  i  h     l     c    +DM  −DM  TR   +VM  −VM
 *  0  10    8     9     –    –    –    –    –
 *  1  12    9     11    2    0    3    4    1
 *  2  11    7     8     0    2    4    2    5
 *  3  13    10    12    2    0    5    6    1
 *  4  13.5  10.5  13    0.5  0    3    3.5  2.5
 */
const workedBars = () =>
  hlcBars([
    [10, 8, 9],
    [12, 9, 11],
    [11, 7, 8],
    [13, 10, 12],
    [13.5, 10.5, 13],
  ]);

describe('directionalMovement', () => {
  it('is Wilder’s DMS hand-computed, with a per-column warm-up', () => {
    const r = directionalMovement(workedBars(), { period: 2 });
    const plus = col(r, 'dmiPlusDi');
    const minus = col(r, 'dmiMinusDi');
    const dx = col(r, 'dmiDx');
    const adx = col(r, 'dmiAdx');
    const adxr = col(r, 'dmiAdxr');
    expect(plus).toHaveLength(5);

    // Wilder(+DM) seeds on the mean of bars 1..2: (2+0)/2 = 1, then
    // (1·1 + 2)/2 = 1.5 and (1.5·1 + 0.5)/2 = 1.
    // Wilder(−DM): (0+2)/2 = 1, then 0.5, then 0.25.
    // Wilder(TR) — this is `atr()`: (3+4)/2 = 3.5, then 4.25, then 3.625.
    expect(plus[0]).toBeUndefined();
    expect(plus[1]).toBeUndefined();
    expect(plus[2]).toBeCloseTo((100 * 1) / 3.5, 12);
    expect(plus[3]).toBeCloseTo((100 * 1.5) / 4.25, 12);
    expect(plus[4]).toBeCloseTo((100 * 1) / 3.625, 12);
    expect(minus[2]).toBeCloseTo((100 * 1) / 3.5, 12);
    expect(minus[3]).toBeCloseTo((100 * 0.5) / 4.25, 12);
    expect(minus[4]).toBeCloseTo((100 * 0.25) / 3.625, 12);

    // DX = 100·|+DI − −DI| / (+DI + −DI): the ranges cancel, so it is
    // 100·|1 − 1|/2 = 0, then 100·1/2 = 50, then 100·0.75/1.25 = 60.
    expect(dx[1]).toBeUndefined();
    expect(dx[2]).toBeCloseTo(0, 12);
    expect(dx[3]).toBeCloseTo(50, 12);
    expect(dx[4]).toBeCloseTo(60, 12);

    // ADX seeds on the mean of the first `period` DXs — bar 2·2 − 1 = 3.
    expect(adx[2]).toBeUndefined();
    expect(adx[3]).toBeCloseTo((0 + 50) / 2, 12);
    expect(adx[4]).toBeCloseTo((25 * 1 + 60) / 2, 12);

    // ADXR averages ADX with the ADX `period − 1` bars back — bar 3·2 − 2.
    expect(adxr[3]).toBeUndefined();
    expect(adxr[4]).toBeCloseTo((42.5 + 25) / 2, 12);
  });

  it('warms up at period / 2·period − 1 / 3·period − 2 at the defaults', () => {
    const r = directionalMovement(wavyOhlc());
    for (const [name, first] of [
      ['dmiPlusDi', 14],
      ['dmiMinusDi', 14],
      ['dmiDx', 14],
      ['dmiAdx', 27],
      ['dmiAdxr', 40],
    ] as const) {
      const v = col(r, name);
      expect(
        v.slice(0, first).every((x) => x === undefined),
        name,
      ).toBe(true);
      expect(v[first], name).toBeDefined();
    }
  });

  it('the DI denominator is `atr()`’s own array', () => {
    // Bars that step up by 2 with a 2-wide range and a 3-wide TRUE range
    // (the previous close sits below the low): +DM = 2 and TR = 3 on every
    // bar, so the Wilder smooths are constants and +DI is exactly 200/3.
    // A denominator that used the bar range instead would read 100.
    const stepping = hlcBars(
      Array.from({ length: 8 }, (_, i) => [10 + 2 * i, 8 + 2 * i, 9 + 2 * i]),
    );
    const r = directionalMovement(atr(stepping, { period: 3 }), { period: 3 });
    const plus = col(r, 'dmiPlusDi');
    const a = col(atr(stepping, { period: 3 }), 'atr');
    for (let i = 3; i < 8; i += 1) {
      expect(a[i], `atr[${i}]`).toBeCloseTo(3, 12);
      expect(plus[i], `dmiPlusDi[${i}]`).toBeCloseTo(200 / 3, 12);
      expect(col(r, 'dmiMinusDi')[i], `dmiMinusDi[${i}]`).toBeCloseTo(0, 12);
      expect(col(r, 'dmiDx')[i], `dmiDx[${i}]`).toBeCloseTo(100, 12);
    }
    // …and the DI pair starts exactly where `atr` does — the shared seed.
    expect(plus.findIndex((x) => x !== undefined)).toBe(
      a.findIndex((x) => x !== undefined),
    );
  });

  it('a run of inside bars gives DX = 0, not undefined', () => {
    // Each bar is strictly inside its predecessor, so both DM legs are zero
    // on every bar while the true range is not. `+DI + −DI = 0` then forces
    // the numerator to zero as well — a value, not a missing cell.
    const inside = hlcBars([
      [20, 10, 15],
      [19, 11, 15],
      [18, 12, 15],
      [17, 13, 15],
    ]);
    const r = directionalMovement(inside, { period: 2 });
    expect(col(r, 'dmiPlusDi')[2]).toBe(0);
    expect(col(r, 'dmiMinusDi')[2]).toBe(0);
    expect(col(r, 'dmiDx')[2]).toBe(0);
    expect(col(r, 'dmiDx')[3]).toBe(0);
    expect(col(r, 'dmiAdx')[3]).toBe(0);
  });

  it('a zero true range is undefined, not zero — the 0/0 is genuine', () => {
    // Every bar identical: no range, no movement, so the DI ratio is 0/0 and
    // there is no honest reading. (Contrast the inside-bar case above.)
    const frozen = hlcBars([
      [10, 10, 10],
      [10, 10, 10],
      [10, 10, 10],
      [10, 10, 10],
    ]);
    const r = directionalMovement(frozen, { period: 2 });
    for (const name of [
      'dmiPlusDi',
      'dmiMinusDi',
      'dmiDx',
      'dmiAdx',
      'dmiAdxr',
    ])
      expect(
        col(r, name).every((x) => x === undefined),
        name,
      ).toBe(true);
  });

  it('honours the prefix and the three input names; a misnamed input reads empty', () => {
    const named = directionalMovement(wavyOhlc(), {
      period: 5,
      prefix: 'wilder',
      high: 'high',
      low: 'low',
      close: 'close',
    });
    for (const name of [
      'wilderPlusDi',
      'wilderMinusDi',
      'wilderDx',
      'wilderAdx',
    ])
      expect(col(named, name)[30], name).toBeDefined();
    expect(
      col(
        directionalMovement(wavyOhlc(), { period: 5, high: 'nope' as never }),
        'dmiPlusDi',
      ).every((x) => x === undefined),
    ).toBe(true);
  });

  it('rejects a bad period and a colliding prefix column', () => {
    const b = workedBars();
    expect(() => directionalMovement(b, { period: 0 })).toThrow(TypeError);
    expect(() => directionalMovement(b, { period: 2.5 })).toThrow(TypeError);
    const once = directionalMovement(b, { period: 2 });
    expect(() => directionalMovement(once as never, { period: 2 })).toThrow(
      /collides/,
    );
  });

  it('is all-undefined when the period exceeds the series, length kept', () => {
    const r = directionalMovement(workedBars(), { period: 9 });
    for (const name of [
      'dmiPlusDi',
      'dmiMinusDi',
      'dmiDx',
      'dmiAdx',
      'dmiAdxr',
    ]) {
      expect(col(r, name), name).toHaveLength(5);
      expect(
        col(r, name).every((x) => x === undefined),
        name,
      ).toBe(true);
    }
  });
});

describe('aroon', () => {
  it('is 100·(period − age)/period over a period + 1 bar window, hand-computed', () => {
    // Highs 10, 12, 11, 12, 10.5, 10.2, 10.1 at period 4. The window at bar 4
    // is [10, 12, 11, 12, 10.5]: the newest 12 is 1 bar back, so 100·3/4 = 75.
    const r = aroon(
      hlcBars([
        [10, 9, 9.5],
        [12, 9.5, 11],
        [11, 9.2, 10],
        [12, 9.5, 11.5],
        [10.5, 9.8, 10],
        [10.2, 9.9, 10],
        [10.1, 9.7, 9.9],
      ]),
      { period: 4 },
    );
    const up = col(r, 'aroonUp');
    const down = col(r, 'aroonDown');
    const osc = col(r, 'aroonOsc');
    expect(up).toHaveLength(7);
    // Warm-up is `period` rows: the window holds period + 1 bars.
    expect(up.slice(0, 4).every((x) => x === undefined)).toBe(true);
    expect(up[4]).toBe(75); // tie on 12 — the NEWEST bar wins (else 25)
    expect(up[5]).toBe(50);
    expect(up[6]).toBe(25);
    // Lows 9, 9.5, 9.2, 9.5, 9.8, 9.9, 9.7: the low at bar 0 is the oldest
    // bar in bar 4's window, so aroonDown is 0 there.
    expect(down[4]).toBe(0);
    expect(down[5]).toBe(25);
    expect(down[6]).toBe(0);
    expect(osc[4]).toBe(75);
    expect(osc[5]).toBe(25);
    expect(osc[6]).toBe(25);
  });

  it('reaches 100 on a fresh extreme and 0 on the oldest bar', () => {
    const rising = aroon(
      hlcBars(
        Array.from({ length: 8 }, (_, i) => [10 + i, 8 + i, 9 + i]) as Array<
          [number, number, number]
        >,
      ),
      { period: 3 },
    );
    // Every bar makes a new high AND a new low: up pinned at 100, down at 0.
    expect(col(rising, 'aroonUp').slice(3)).toEqual([100, 100, 100, 100, 100]);
    expect(col(rising, 'aroonDown').slice(3)).toEqual([0, 0, 0, 0, 0]);
    expect(col(rising, 'aroonOsc')[7]).toBe(100);
  });

  it('defaults to period 25 and the `aroon` prefix', () => {
    const r = aroon(wavyOhlc());
    const up = col(r, 'aroonUp');
    expect(up.slice(0, 25).every((x) => x === undefined)).toBe(true);
    expect(up[25]).toBeDefined();
    expect(up).toEqual(col(aroon(wavyOhlc(), { period: 25 }), 'aroonUp'));
  });

  it('honours the prefix and input names; a misnamed input reads empty', () => {
    const named = aroon(wavyOhlc(), {
      period: 5,
      prefix: 'ar',
      high: 'high',
      low: 'low',
    });
    expect(col(named, 'arUp')[10]).toBeDefined();
    expect(col(named, 'arDown')[10]).toBeDefined();
    expect(col(named, 'arOsc')[10]).toBeDefined();
    const badHigh = aroon(wavyOhlc(), { period: 5, high: 'nope' as never });
    // Only the leg reading the missing column goes empty — the other still
    // reports, and the oscillator (which needs both) does not.
    expect(col(badHigh, 'aroonUp').every((x) => x === undefined)).toBe(true);
    expect(col(badHigh, 'aroonDown')[10]).toBeDefined();
    expect(col(badHigh, 'aroonOsc').every((x) => x === undefined)).toBe(true);
  });

  it('rejects a bad period and a colliding prefix column', () => {
    const b = workedBars();
    expect(() => aroon(b, { period: 0 })).toThrow(TypeError);
    expect(() => aroon(b, { period: 3.5 })).toThrow(TypeError);
    const once = aroon(b, { period: 2 });
    expect(() => aroon(once as never, { period: 2 })).toThrow(/collides/);
  });

  it('is all-undefined when the period exceeds the series, length kept', () => {
    const r = aroon(workedBars(), { period: 9 });
    for (const name of ['aroonUp', 'aroonDown', 'aroonOsc']) {
      expect(col(r, name), name).toHaveLength(5);
      expect(
        col(r, name).every((x) => x === undefined),
        name,
      ).toBe(true);
    }
  });
});

describe('vortex', () => {
  it('is Σ|H − prevL| / ΣTR and Σ|L − prevH| / ΣTR, hand-computed', () => {
    // On the worked fixture at period 2 (see the table above):
    //   bar 2: Σ+VM = 4+2 = 6, Σ−VM = 1+5 = 6, ΣTR = 3+4 = 7
    //   bar 3: Σ+VM = 2+6 = 8, Σ−VM = 5+1 = 6, ΣTR = 4+5 = 9
    //   bar 4: Σ+VM = 6+3.5 = 9.5, Σ−VM = 1+2.5 = 3.5, ΣTR = 5+3 = 8
    const r = vortex(workedBars(), { period: 2 });
    const plus = col(r, 'viPlus');
    const minus = col(r, 'viMinus');
    expect(plus).toHaveLength(5);
    expect(plus[0]).toBeUndefined();
    expect(plus[1]).toBeUndefined(); // both legs read the previous bar
    expect(plus[2]).toBeCloseTo(6 / 7, 12);
    expect(plus[3]).toBeCloseTo(8 / 9, 12);
    expect(plus[4]).toBeCloseTo(9.5 / 8, 12);
    expect(minus[2]).toBeCloseTo(6 / 7, 12);
    expect(minus[3]).toBeCloseTo(6 / 9, 12);
    expect(minus[4]).toBeCloseTo(3.5 / 8, 12);
  });

  it('a zero total true range is undefined, not ±Infinity', () => {
    // Bars 1 and 2 sit entirely on the previous close, so their true range is
    // exactly 0 — while +VM on bar 1 is |10 − 8| = 2, a NON-zero numerator
    // over a zero denominator. This is the case `dmiDx`'s forced-zero
    // reasoning does NOT cover, which is why the guard is here and not there.
    const halted = hlcBars([
      [12, 8, 10],
      [10, 10, 10],
      [10, 10, 10],
    ]);
    const r = vortex(halted, { period: 2 });
    expect(col(r, 'viPlus')[2]).toBeUndefined();
    expect(col(r, 'viMinus')[2]).toBeUndefined();
  });

  it('defaults to period 14 and the `vi` prefix; honours the input names', () => {
    const r = vortex(wavyOhlc());
    expect(
      col(r, 'viPlus')
        .slice(0, 14)
        .every((x) => x === undefined),
    ).toBe(true);
    expect(col(r, 'viPlus')[14]).toBeDefined();
    const named = vortex(wavyOhlc(), {
      period: 5,
      prefix: 'vtx',
      high: 'high',
      low: 'low',
      close: 'close',
    });
    expect(col(named, 'vtxPlus')[20]).toBeDefined();
    expect(col(named, 'vtxMinus')[20]).toBeDefined();
    expect(
      col(
        vortex(wavyOhlc(), { period: 5, low: 'nope' as never }),
        'viPlus',
      ).every((x) => x === undefined),
    ).toBe(true);
  });

  it('rejects a bad period and a colliding prefix column', () => {
    const b = workedBars();
    expect(() => vortex(b, { period: 0 })).toThrow(TypeError);
    expect(() => vortex(b, { period: 1.5 })).toThrow(TypeError);
    const once = vortex(b, { period: 2 });
    expect(() => vortex(once as never, { period: 2 })).toThrow(/collides/);
  });

  it('is all-undefined when the period exceeds the series, length kept', () => {
    const r = vortex(workedBars(), { period: 9 });
    for (const name of ['viPlus', 'viMinus']) {
      expect(col(r, name), name).toHaveLength(5);
      expect(
        col(r, name).every((x) => x === undefined),
        name,
      ).toBe(true);
    }
  });
});

/* ========================================================================== */
/* The volatility tail (assessment §6.5): Chaikin Volatility, Mass Index,     */
/* Choppiness, Ulcer, VHF, GAPO and the Relative VOLATILITY Index.            */
/*                                                                            */
/* None of the seven has a TA-Lib function, so the oracle is a pandas         */
/* replication in every case and it pins the VALUES. What is pinned here is   */
/* what a smooth 80-bar fixture cannot show: hand-computed arithmetic on a    */
/* handful of bars, the zero-denominator guards (every one of which would     */
/* otherwise throw from `withColumn`, since ±Infinity is not a value a        */
/* numeric column accepts), the definition forks, the defaults and validation.*/
/* ========================================================================== */

/** Non-degenerate OHLC bars for the volatility tail: 60 rows, never monotonic,
 *  the range varying bar to bar, the close never sitting on an extreme, and
 *  long enough for the batch's longest default (`verticalHorizontalFilter`'s
 *  28 and `massIndex`'s 40-bar warm-up). */
const volatilityRows = (): Array<[number, number, number, number]> =>
  Array.from({ length: 60 }, (_, i) => {
    const c = 100 + 7 * Math.sin(i / 4.1) + 0.25 * i;
    const o = c - 0.6 * Math.cos(i / 2.7);
    const up = 0.4 + 0.9 * Math.abs(Math.sin(i / 2.9));
    const down = 0.4 + 0.9 * Math.abs(Math.cos(i / 2.2));
    return [o, Math.max(o, c) + up, Math.min(o, c) - down, c];
  });

/** Bars whose plain range (`high − low`) is exactly `r`, with a flat close —
 *  the input `chaikinVolatility` and `massIndex` reduce to. */
const flatCloseRangeBars = (ranges: number[]) =>
  ohlcBars(ranges.map((r) => [100, 100 + r, 100, 100]));

describe('chaikinVolatility', () => {
  it('is the percent rate of change of an EMA of the range, hand-computed', () => {
    // `period: 1` makes α = 1, so the EMA is the identity and the arithmetic
    // is visible: 100·(range[i]/range[i−2] − 1).
    const v = col(
      chaikinVolatility(flatCloseRangeBars([2, 4, 3, 6, 1]), {
        period: 1,
        rocPeriod: 2,
      }),
      'chaikinVol',
    );
    expect(v).toHaveLength(5);
    expect(v.slice(0, 2).every((x) => x === undefined)).toBe(true);
    expect(v[2]).toBeCloseTo(50, 12); // 3 / 2
    expect(v[3]).toBeCloseTo(50, 12); // 6 / 4
    expect(v[4]).toBeCloseTo((100 * (1 - 3)) / 3, 12); // 1 / 3
  });

  it('smooths with pond’s EMA seed and reads the SMOOTHED array’s warm-up', () => {
    // period 3 → α = 1/2. Ranges 4,4,4,8 give E = 4,4,4,6, emitted from bar 2
    // (the array door's `period` finite values), so a 1-bar rate of change
    // first exists on bar 3 and is 100·(6/4 − 1).
    const v = col(
      chaikinVolatility(flatCloseRangeBars([4, 4, 4, 8]), {
        period: 3,
        rocPeriod: 1,
      }),
      'chaikinVol',
    );
    expect(v.slice(0, 3).every((x) => x === undefined)).toBe(true);
    expect(v[3]).toBeCloseTo(50, 12);
  });

  it('a zero base is undefined, not ±Infinity (percentChange’s rule)', () => {
    // A halted instrument: no range at all, then a jump. Without the kernel's
    // `=== 0` guard the last bar would be Infinity, which `withColumn` rejects
    // outright — so this asserts the study returns at all.
    const v = col(
      chaikinVolatility(flatCloseRangeBars([0, 0, 0, 4]), {
        period: 1,
        rocPeriod: 1,
      }),
      'chaikinVol',
    );
    expect(v).toHaveLength(4);
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('defaults to 10/10 and the `chaikinVol` column; honours the options', () => {
    const b = ohlcBars(volatilityRows());
    const v = col(chaikinVolatility(b), 'chaikinVol');
    expect(v.slice(0, 19).every((x) => x === undefined)).toBe(true);
    expect(v[19]).toBeDefined();
    expect(v).toEqual(
      col(chaikinVolatility(b, { period: 10, rocPeriod: 10 }), 'chaikinVol'),
    );
    const named = chaikinVolatility(b, {
      period: 3,
      rocPeriod: 2,
      high: 'high',
      low: 'low',
      output: 'cv',
    });
    expect(col(named, 'cv')[4]).toBeDefined();
  });

  it('rejects bad periods and a colliding output; a misnamed input reads empty', () => {
    const b = flatCloseRangeBars([1, 2, 3, 4, 5, 6]);
    expect(() => chaikinVolatility(b, { period: 0 })).toThrow(TypeError);
    expect(() => chaikinVolatility(b, { rocPeriod: 1.5 })).toThrow(TypeError);
    expect(() => chaikinVolatility(b, { output: 'close' })).toThrow(/collides/);
    expect(
      col(
        chaikinVolatility(b, {
          period: 2,
          rocPeriod: 1,
          high: 'nope' as never,
        }),
        'chaikinVol',
      ).every((x) => x === undefined),
    ).toBe(true);
  });

  it('is all-undefined when the periods exceed the series, length kept', () => {
    const v = col(
      chaikinVolatility(flatCloseRangeBars([1, 2, 3]), {
        period: 3,
        rocPeriod: 5,
      }),
      'chaikinVol',
    );
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('massIndex', () => {
  it('is Σ EMA(range)/EMA(EMA(range)), hand-computed on the TRIX warm-up', () => {
    // emaPeriod 3 → α = 1/2. Ranges 4,4,4,8,8,8:
    //   E1 = 4,4,4,6,7,7.5   (emitted from bar 2)
    //   E2 = _,_,4,5,6,6.75  (seeded on E1's first emitted value, bar 2,
    //                         emitted from bar 4 = 2·emaPeriod − 2)
    // so the ratio starts at bar 4 and a 2-bar SUM of it at bar 5.
    const v = col(
      massIndex(flatCloseRangeBars([4, 4, 4, 8, 8, 8]), {
        emaPeriod: 3,
        sumPeriod: 2,
      }),
      'mass',
    );
    expect(v).toHaveLength(6);
    expect(v.slice(0, 5).every((x) => x === undefined)).toBe(true);
    expect(v[5]).toBeCloseTo(7 / 6 + 7.5 / 6.75, 12);
  });

  it('a SUM, not a mean — a steady market reads ≈ sumPeriod', () => {
    // A constant range makes both EMAs equal, so every ratio is exactly 1 and
    // the sum is exactly `sumPeriod`. The mean version would read 1.
    const v = col(
      massIndex(flatCloseRangeBars(Array.from({ length: 20 }, () => 5)), {
        emaPeriod: 3,
        sumPeriod: 4,
      }),
      'mass',
    );
    expect(v[19]).toBeCloseTo(4, 12);
  });

  it('a double EMA that lands on zero is undefined, not ±Infinity', () => {
    // Reachable only on CROSSING inputs (high below low on some bars, above on
    // others), which is what a redirected pair produces. At emaPeriod 3 the
    // ranges −1,−1,−1,−1,3 drive E1 to −1,−1,−1,−1,1 and E2 to exactly 0 on
    // bar 4 — a non-zero numerator over a zero denominator. Without the guard
    // `withColumn` rejects the Infinity and the study throws.
    const crossing = ohlcBars([
      [100, 100, 101, 100],
      [100, 100, 101, 100],
      [100, 100, 101, 100],
      [100, 100, 101, 100],
      [100, 103, 100, 100],
      [100, 100, 101, 100],
    ]);
    const v = col(massIndex(crossing, { emaPeriod: 3, sumPeriod: 1 }), 'mass');
    expect(v).toHaveLength(6);
    expect(v[4]).toBeUndefined();
  });

  it('defaults to 9/25 and the `mass` column; honours the options', () => {
    const b = ohlcBars(volatilityRows());
    const v = col(massIndex(b), 'mass');
    expect(v.slice(0, 40).every((x) => x === undefined)).toBe(true);
    expect(v[40]).toBeDefined();
    expect(v).toEqual(
      col(massIndex(b, { emaPeriod: 9, sumPeriod: 25 }), 'mass'),
    );
    const named = massIndex(b, {
      emaPeriod: 3,
      sumPeriod: 4,
      high: 'high',
      low: 'low',
      output: 'mi',
    });
    expect(col(named, 'mi')[7]).toBeDefined();
  });

  it('rejects bad periods and a colliding output; a misnamed input reads empty', () => {
    const b = flatCloseRangeBars([1, 2, 3, 4, 5, 6]);
    expect(() => massIndex(b, { emaPeriod: 0 })).toThrow(TypeError);
    expect(() => massIndex(b, { sumPeriod: 2.5 })).toThrow(TypeError);
    expect(() => massIndex(b, { output: 'high' })).toThrow(/collides/);
    expect(
      col(
        massIndex(b, { emaPeriod: 2, sumPeriod: 2, low: 'nope' as never }),
        'mass',
      ).every((x) => x === undefined),
    ).toBe(true);
  });

  it('is all-undefined when the periods exceed the series, length kept', () => {
    const v = col(
      massIndex(flatCloseRangeBars([1, 2, 3]), { emaPeriod: 3, sumPeriod: 3 }),
      'mass',
    );
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('choppinessIndex', () => {
  it('is 100·log10(ΣTR/(HH−LL))/log10(period), hand-computed', () => {
    // Bars (h,l,c): (10,8,9), (11,9,10), (12,10,11).
    // TR[1] = max(2, |11−9|, |9−9|) = 2; TR[2] = max(2, |12−10|, |10−10|) = 2.
    // At bar 2, period 2: ΣTR = 4, HH = 12, LL = 9 → 100·log10(4/3)/log10(2).
    const v = col(
      choppinessIndex(
        ohlcBars([
          [9, 10, 8, 9],
          [10, 11, 9, 10],
          [11, 12, 10, 11],
        ]),
        { period: 2 },
      ),
      'chop',
    );
    expect(v).toHaveLength(3);
    // Warm-up is `period` rows, not period − 1: TR[0] does not exist.
    expect(v.slice(0, 2).every((x) => x === undefined)).toBe(true);
    expect(v[2]).toBeCloseTo((100 * Math.log10(4 / 3)) / Math.log10(2), 12);
  });

  it('a flat window is undefined — not 0 (“trending”) and not 100 (“choppy”)', () => {
    const flat = ohlcBars(
      Array.from(
        { length: 6 },
        () => [10, 10, 10, 10] as [number, number, number, number],
      ),
    );
    const v = col(choppinessIndex(flat, { period: 3 }), 'chop');
    expect(v).toHaveLength(6);
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('a zero HH−LL span with a non-zero ΣTR is undefined, not ±Infinity', () => {
    // A constant high and low with a moving close — an inconsistent bar, which
    // is what a redirected `close` produces. True range is non-zero (it reads
    // the close), the span is exactly zero. Without the guard the ratio is
    // Infinity and `withColumn` rejects it.
    const v = col(
      choppinessIndex(
        ohlcBars([
          [5, 5, 5, 1],
          [5, 5, 5, 2],
          [5, 5, 5, 3],
          [5, 5, 5, 4],
        ]),
        { period: 2 },
      ),
      'chop',
    );
    expect(v).toHaveLength(4);
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('a zero ΣTR with a non-zero span is undefined, not −Infinity', () => {
    // The other half of the guard, and it needs its own input: bars whose high
    // and low both sit on the PREVIOUS close have no true range at all, while
    // their highs still differ from one another, so the span is positive.
    // `log10(0)` would be −Infinity.
    const v = col(
      choppinessIndex(
        ohlcBars([
          [1, 1, 1, 1],
          [1, 1, 1, 2],
          [2, 2, 2, 3],
          [3, 3, 3, 4],
        ]),
        { period: 2 },
      ),
      'chop',
    );
    expect(v).toHaveLength(4);
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('rejects period 1 — log10(1) is zero, so there is no reading', () => {
    const b = ohlcBars(volatilityRows());
    expect(() => choppinessIndex(b, { period: 1 })).toThrow(/at least 2/);
    expect(() => choppinessIndex(b, { period: 0 })).toThrow(TypeError);
    expect(() => choppinessIndex(b, { period: 2.5 })).toThrow(TypeError);
  });

  it('defaults to period 14 and the `chop` column; honours the options', () => {
    const b = ohlcBars(volatilityRows());
    const v = col(choppinessIndex(b), 'chop');
    expect(v.slice(0, 14).every((x) => x === undefined)).toBe(true);
    expect(v[14]).toBeDefined();
    expect(v).toEqual(col(choppinessIndex(b, { period: 14 }), 'chop'));
    const named = choppinessIndex(b, {
      period: 4,
      high: 'high',
      low: 'low',
      close: 'close',
      output: 'ci',
    });
    expect(col(named, 'ci')[4]).toBeDefined();
    expect(() => choppinessIndex(b, { output: 'close' })).toThrow(/collides/);
    expect(
      col(
        choppinessIndex(b, { period: 3, high: 'nope' as never }),
        'chop',
      ).every((x) => x === undefined),
    ).toBe(true);
  });

  it('is all-undefined when the period exceeds the series, length kept', () => {
    const v = col(
      choppinessIndex(ohlcBars(volatilityRows().slice(0, 4)), { period: 9 }),
      'chop',
    );
    expect(v).toHaveLength(4);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('ulcerIndex', () => {
  it('is the RMS percent drawdown from the rolling peak, hand-computed', () => {
    // Closes 10, 12, 9, 9 at period 2. Peaks: _, 12, 12, 9.
    // Drawdowns: _, 0, 100·(9−12)/12 = −25, 0.
    // Mean of squares over 2: bar 2 → (0 + 625)/2, bar 3 → (625 + 0)/2.
    const v = col(ulcerIndex(bars([10, 12, 9, 9]), { period: 2 }), 'ulcer');
    expect(v).toHaveLength(4);
    // Warm-up is 2·period − 2: the peak costs one window, the mean another.
    expect(v.slice(0, 2).every((x) => x === undefined)).toBe(true);
    expect(v[2]).toBeCloseTo(Math.sqrt(625 / 2), 12);
    expect(v[3]).toBeCloseTo(Math.sqrt(625 / 2), 12);
  });

  it('a window at new highs reads EXACTLY zero, not the accumulator’s residue', () => {
    // The reading a caller looks for, and the one an incremental mean cannot
    // produce on its own once the square root has amplified its residue — see
    // the docstring's measured 1.6e-9. `toBe(0)` is the point of this test.
    const v = col(ulcerIndex(bars([10, 11, 12, 13]), { period: 2 }), 'ulcer');
    expect(v[2]).toBe(0);
    expect(v[3]).toBe(0);
  });

  it('a zero rolling peak is undefined, not ±Infinity', () => {
    // Reachable only over a column that crosses zero (another study's output).
    // Bar 1's window peaks at 0 with a value of −5, so the numerator is NOT
    // forced to zero and the guard is what stops an Infinity reaching
    // `withColumn`.
    const v = col(ulcerIndex(bars([0, -5, -5, -5]), { period: 2 }), 'ulcer');
    expect(v).toHaveLength(4);
    expect(v[2]).toBeUndefined();
    expect(v[3]).toBe(0); // the peak is −5 by then: a defined, if unusual, base
  });

  it('defaults to period 14 and the `ulcer` column; honours column and output', () => {
    const b = bars(wavyCloses);
    const v = col(ulcerIndex(b), 'ulcer');
    expect(v.slice(0, 26).every((x) => x === undefined)).toBe(true);
    expect(v[26]).toBeDefined();
    expect(v).toEqual(col(ulcerIndex(b, { period: 14 }), 'ulcer'));
    const renamed = ulcerIndex(sma(b, { period: 3, output: 'fast' }), {
      period: 4,
      column: 'fast',
      output: 'ulcerFast',
    });
    expect(col(renamed, 'ulcerFast')[20]).toBeDefined();
  });

  it('rejects a bad period and a colliding output; a misnamed column throws', () => {
    const b = bars(wavyCloses);
    expect(() => ulcerIndex(b, { period: 0 })).toThrow(TypeError);
    expect(() => ulcerIndex(b, { period: 1.5 })).toThrow(TypeError);
    expect(() => ulcerIndex(b, { output: 'close' })).toThrow(/collides/);
    // The `rollingStdev` door: a single-column study on `rollingValues`
    // throws rather than reading empty (contrast the bar studies above).
    expect(() => ulcerIndex(b, { period: 3, column: 'nope' as never })).toThrow(
      /nope/,
    );
  });

  it('is all-undefined when the period exceeds the series, length kept', () => {
    const v = col(ulcerIndex(bars([10, 12, 11]), { period: 5 }), 'ulcer');
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('verticalHorizontalFilter', () => {
  it('is (HH − LL) / Σ|Δ|, hand-computed, with the period-row warm-up', () => {
    // Closes 10, 12, 11, 15 at period 2. |Δ| = _, 2, 1, 4.
    // Bar 2: range over bars 1–2 = 12 − 11 = 1, path = 2 + 1 = 3.
    // Bar 3: range = 15 − 11 = 4, path = 1 + 4 = 5.
    const v = col(
      verticalHorizontalFilter(bars([10, 12, 11, 15]), { period: 2 }),
      'vhf',
    );
    expect(v).toHaveLength(4);
    // `period` rows, not period − 1: 2 changes need 3 closes.
    expect(v.slice(0, 2).every((x) => x === undefined)).toBe(true);
    expect(v[2]).toBeCloseTo(1 / 3, 12);
    expect(v[3]).toBeCloseTo(4 / 5, 12);
  });

  it('reaches 1 when the window has no retracement at all', () => {
    // Closes 5, 5, 6, 7: at bar 2 the path is 0 + 1 and the range is 1.
    const v = col(
      verticalHorizontalFilter(bars([5, 5, 6, 7]), { period: 2 }),
      'vhf',
    );
    expect(v[2]).toBe(1);
    expect(v[3]).toBeCloseTo(0.5, 12); // range 1, path 1 + 1
  });

  it('a window with no movement is undefined (0/0), with no guard needed', () => {
    // Both halves read the same column, so a zero path forces a zero range —
    // there is no input that puts a non-zero numerator over a zero
    // denominator, which is why this study has no explicit guard.
    const v = col(
      verticalHorizontalFilter(bars([5, 5, 5, 5]), { period: 2 }),
      'vhf',
    );
    expect(v).toHaveLength(4);
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('defaults to period 28 and the `vhf` column; honours column and output', () => {
    const b = bars(wavyCloses);
    const v = col(verticalHorizontalFilter(b), 'vhf');
    expect(v.slice(0, 28).every((x) => x === undefined)).toBe(true);
    expect(v[28]).toBeDefined();
    expect(v).toEqual(col(verticalHorizontalFilter(b, { period: 28 }), 'vhf'));
    const renamed = verticalHorizontalFilter(
      sma(b, { period: 3, output: 'fast' }),
      { period: 4, column: 'fast', output: 'vhfFast' },
    );
    expect(col(renamed, 'vhfFast')[20]).toBeDefined();
  });

  it('rejects a bad period and a colliding output; a misnamed column throws', () => {
    const b = bars(wavyCloses);
    expect(() => verticalHorizontalFilter(b, { period: 0 })).toThrow(TypeError);
    expect(() => verticalHorizontalFilter(b, { period: 1.5 })).toThrow(
      TypeError,
    );
    expect(() => verticalHorizontalFilter(b, { output: 'close' })).toThrow(
      /collides/,
    );
    expect(() =>
      verticalHorizontalFilter(b, { period: 3, column: 'nope' as never }),
    ).toThrow(/nope/);
  });

  it('is all-undefined when the period exceeds the series, length kept', () => {
    const v = col(
      verticalHorizontalFilter(bars([10, 12, 11]), { period: 5 }),
      'vhf',
    );
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('gopalakrishnanRangeIndex', () => {
  it('is ln(HH − LL)/ln(period), hand-computed', () => {
    // Highs 12, 14, 13; lows 10, 11, 9; period 2.
    // Bar 1: HH 14, LL 10 → ln(4)/ln(2) = 2 exactly.
    // Bar 2: HH 14, LL 9  → ln(5)/ln(2).
    const v = col(
      gopalakrishnanRangeIndex(
        ohlcBars([
          [11, 12, 10, 11],
          [12, 14, 11, 12],
          [11, 13, 9, 11],
        ]),
        { period: 2 },
      ),
      'gapo',
    );
    expect(v).toHaveLength(3);
    expect(v[0]).toBeUndefined();
    expect(v[1]).toBe(2);
    expect(v[2]).toBeCloseTo(Math.log(5) / Math.log(2), 12);
  });

  it('is the same number in any log base — log10/log10 agrees with ln/ln', () => {
    // `log_b(x)/log_b(n)` is `log_n(x)` for every base, which is why neither
    // this study nor `choppinessIndex` takes a base option.
    const rows = volatilityRows();
    const period = 6;
    const v = col(gopalakrishnanRangeIndex(ohlcBars(rows), { period }), 'gapo');
    for (let i = period - 1; i < rows.length; i += 1) {
      let hh = -Infinity;
      let ll = Infinity;
      for (let j = i - period + 1; j <= i; j += 1) {
        hh = Math.max(hh, rows[j]![1]);
        ll = Math.min(ll, rows[j]![2]);
      }
      expect(v[i]!, `bar ${i}`).toBeCloseTo(
        Math.log10(hh - ll) / Math.log10(period),
        12,
      );
    }
  });

  it('a flat window is undefined — ln(0) would be −Infinity', () => {
    const flat = ohlcBars(
      Array.from(
        { length: 5 },
        () => [7, 7, 7, 7] as [number, number, number, number],
      ),
    );
    const v = col(gopalakrishnanRangeIndex(flat, { period: 2 }), 'gapo');
    expect(v).toHaveLength(5);
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('rejects period 1 — ln(1) is zero, so there is no reading', () => {
    const b = ohlcBars(volatilityRows());
    expect(() => gopalakrishnanRangeIndex(b, { period: 1 })).toThrow(
      /at least 2/,
    );
    expect(() => gopalakrishnanRangeIndex(b, { period: 0 })).toThrow(TypeError);
  });

  it('defaults to period 10 and the `gapo` column; honours the options', () => {
    const b = ohlcBars(volatilityRows());
    const v = col(gopalakrishnanRangeIndex(b), 'gapo');
    expect(v.slice(0, 9).every((x) => x === undefined)).toBe(true);
    expect(v[9]).toBeDefined();
    expect(v).toEqual(col(gopalakrishnanRangeIndex(b, { period: 10 }), 'gapo'));
    const named = gopalakrishnanRangeIndex(b, {
      period: 3,
      high: 'high',
      low: 'low',
      output: 'gp',
    });
    expect(col(named, 'gp')[2]).toBeDefined();
    expect(() => gopalakrishnanRangeIndex(b, { output: 'high' })).toThrow(
      /collides/,
    );
    expect(
      col(
        gopalakrishnanRangeIndex(b, { period: 3, low: 'nope' as never }),
        'gapo',
      ).every((x) => x === undefined),
    ).toBe(true);
  });

  it('is all-undefined when the period exceeds the series, length kept', () => {
    const v = col(
      gopalakrishnanRangeIndex(ohlcBars(volatilityRows().slice(0, 3)), {
        period: 9,
      }),
      'gapo',
    );
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('relativeVolatilityIndex', () => {
  it('is RSI’s form on σ, Wilder-smoothed, hand-computed', () => {
    // Closes 10, 12, 11, 13, 14 at stdevPeriod 2 / period 2.
    // σ  = _, 1, 0.5, 1, 0.5     Δ = _, +2, −1, +2, +1
    // up = _, 1, 0,   1, 0.5     dn = _, 0, 0.5, 0, 0
    // Wilder seeds on bar 2 (the first σ is bar 1, plus period − 1):
    //   U = 0.5, 0.75, 0.625     D = 0.25, 0.125, 0.0625
    const v = col(
      relativeVolatilityIndex(bars([10, 12, 11, 13, 14]), {
        period: 2,
        stdevPeriod: 2,
      }),
      'relVol',
    );
    expect(v).toHaveLength(5);
    // Warm-up is stdevPeriod + period − 2.
    expect(v.slice(0, 2).every((x) => x === undefined)).toBe(true);
    expect(v[2]).toBeCloseTo((100 * 0.5) / 0.75, 12);
    expect(v[3]).toBeCloseTo((100 * 0.75) / 0.875, 12);
    expect(v[4]).toBeCloseTo((100 * 0.625) / 0.6875, 12);
  });

  it('an unchanged close counts as a DOWN bar (Dorsey), unlike rsi’s split', () => {
    // Closes 10, 11, 12, 12, 13, 14: every move is up except one flat bar. If
    // a flat bar counted for neither leg — the `upDownLegValues` rule `rsi`
    // uses — the down leg would be zero throughout and every reading would be
    // exactly 100. It is not, which is the whole of the delta.
    const v = col(
      relativeVolatilityIndex(bars([10, 11, 12, 12, 13, 14]), {
        period: 2,
        stdevPeriod: 3,
      }),
      'relVol',
    );
    expect(v.slice(0, 3).every((x) => x === undefined)).toBe(true);
    expect(v[3]!).toBeLessThan(100);
    expect(v[3]).toBeCloseTo(63.39745962155614, 9);
    expect(v[5]!).toBeLessThan(100);
  });

  it('a flat window is undefined (0/0) — every σ in the smoother is zero', () => {
    const v = col(
      relativeVolatilityIndex(bars([5, 5, 5, 5, 5, 5]), {
        period: 2,
        stdevPeriod: 2,
      }),
      'relVol',
    );
    expect(v).toHaveLength(6);
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('defaults to 14/10 and `relVol`; it coexists with relativeVigorIndex', () => {
    const b = bars(wavyCloses);
    const v = col(relativeVolatilityIndex(b), 'relVol');
    expect(v.slice(0, 22).every((x) => x === undefined)).toBe(true);
    expect(v[22]).toBeDefined();
    expect(v).toEqual(
      col(
        relativeVolatilityIndex(b, { period: 14, stdevPeriod: 10 }),
        'relVol',
      ),
    );
    // The reason the default is not `rvi`: both studies on one series.
    const both = relativeVolatilityIndex(
      relativeVigorIndex(ohlcBars(volatilityRows()), { period: 4 }),
      { period: 4, stdevPeriod: 3 },
    );
    expect(col(both, 'rvi')[10]).toBeDefined();
    expect(col(both, 'relVol')[10]).toBeDefined();
  });

  it('rejects bad periods and a colliding output; a misnamed column throws', () => {
    const b = bars(wavyCloses);
    expect(() => relativeVolatilityIndex(b, { period: 0 })).toThrow(TypeError);
    expect(() => relativeVolatilityIndex(b, { stdevPeriod: 1.5 })).toThrow(
      TypeError,
    );
    expect(() => relativeVolatilityIndex(b, { output: 'close' })).toThrow(
      /collides/,
    );
    // A misnamed column reads EMPTY here, where `ulcerIndex` and
    // `verticalHorizontalFilter` throw on the same `rollingValues` door — the
    // kernel's answer depends on the reducer, not on the study. `stdev` (and
    // `avg`) take the range-exact path, which reads a missing column as
    // all-`NaN`; `max`/`min` fall through to core's sweep, which rejects the
    // name. Pinned in both directions so the split is chosen, not incidental.
    expect(
      col(
        relativeVolatilityIndex(b, { period: 3, column: 'nope' as never }),
        'relVol',
      ).every((x) => x === undefined),
    ).toBe(true);
  });

  it('a stdevPeriod of 1 makes every σ zero, so the column is empty', () => {
    const v = col(
      relativeVolatilityIndex(bars(wavyCloses), {
        period: 3,
        stdevPeriod: 1,
      }),
      'relVol',
    );
    expect(v).toHaveLength(wavyCloses.length);
    expect(v.every((x) => x === undefined)).toBe(true);
  });

  it('is all-undefined when the periods exceed the series, length kept', () => {
    const v = col(
      relativeVolatilityIndex(bars([10, 12, 11]), {
        period: 5,
        stdevPeriod: 3,
      }),
      'relVol',
    );
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('linearRegression', () => {
  // Closes 10, 12, 11, 15 at period 3, x = 0, 1, 2 over the window.
  //   bar 2: Σy = 33, Σxy = 34, num = 3·34 − 3·33 = 3, den = 3·5 − 3² = 6
  //          slope = 0.5, intercept = (33 − 0.5·3)/3 = 10.5,
  //          value = 10.5 + 0.5·2 = 11.5, r² = 3²/(6·(3·365 − 33²)) = 0.25
  //   bar 3: Σy = 38, Σxy = 41, num = 9, slope = 1.5,
  //          intercept = (38 − 4.5)/3 = 67/6, value = 67/6 + 3 = 85/6,
  //          r² = 81/(6·(3·490 − 38²)) = 81/156
  const hand = () => linearRegression(bars([10, 12, 11, 15]), { period: 3 });

  it('is the OLS fit of the window against the bar index, hand-computed', () => {
    const r = hand();
    expect(col(r, 'linregSlope')).toEqual([undefined, undefined, 0.5, 1.5]);
    expect(col(r, 'linregIntercept')[2]).toBeCloseTo(10.5, 12);
    expect(col(r, 'linregIntercept')[3]).toBeCloseTo(67 / 6, 12);
    expect(col(r, 'linregValue')[2]).toBeCloseTo(11.5, 12);
    expect(col(r, 'linregValue')[3]).toBeCloseTo(85 / 6, 12);
    expect(col(r, 'linregR2')[2]).toBeCloseTo(0.25, 12);
    expect(col(r, 'linregR2')[3]).toBeCloseTo(81 / 156, 12);
  });

  it('appends five columns that all warm up on bar period − 1', () => {
    const r = linearRegression(bars(wavyCloses));
    for (const name of [
      'linregValue',
      'linregSlope',
      'linregIntercept',
      'linregAngle',
      'linregR2',
    ]) {
      const v = col(r, name);
      expect(v, name).toHaveLength(wavyCloses.length);
      expect(
        v.slice(0, 13).every((x) => x === undefined),
        name,
      ).toBe(true);
      expect(v[13], name).toBeDefined();
    }
  });

  it('the angle is degrees, and the value is the intercept `period − 1` bars on', () => {
    const r = hand();
    const slope = col(r, 'linregSlope');
    const angle = col(r, 'linregAngle');
    const value = col(r, 'linregValue');
    const intercept = col(r, 'linregIntercept');
    for (const i of [2, 3]) {
      expect(angle[i]).toBeCloseTo((Math.atan(slope[i]!) * 180) / Math.PI, 12);
      expect(value[i]).toBeCloseTo(intercept[i]! + slope[i]! * 2, 12);
    }
    // 0.5 price units per bar reads as 26.57°, not 45° — the angle carries
    // the price's units (see the study's docstring).
    expect(angle[2]).toBeCloseTo(26.565051177077986, 9);
  });

  it('an exactly straight window reads slope = b, R² = 1 and the line back', () => {
    // p[t] = 40 + 0.25·t. The shifted frame makes every accumulator exact on
    // a line, so these are `toBe`-exact rather than close-to (measured over
    // 1000 bars at periods 5, 14 and 200: zero error on all three).
    const line = Array.from({ length: 30 }, (_, i) => 40 + 0.25 * i);
    const r = linearRegression(bars(line), { period: 5 });
    for (let i = 4; i < 30; i += 1) {
      expect(col(r, 'linregSlope')[i], `slope ${i}`).toBe(0.25);
      expect(col(r, 'linregR2')[i], `r2 ${i}`).toBe(1);
      expect(col(r, 'linregValue')[i], `value ${i}`).toBe(line[i]);
    }
  });

  it('a flat window: slope EXACTLY 0, R² undefined — not a residue ratio', () => {
    // The window [3, 5] is flat at 193.5. Without the kernel's change count
    // the accumulator residue reads slope = −2.1e-14 and, far worse,
    // R² = −13.5 — outside [0, 1] entirely. Measured; this is the test that
    // kills that mutation.
    const r = linearRegression(
      bars([186.6, 154.81, 103.74, 193.5, 193.5, 193.5, 193.5]),
      { period: 3 },
    );
    expect(col(r, 'linregSlope')[5]).toBe(0);
    expect(col(r, 'linregSlope')[6]).toBe(0);
    expect(col(r, 'linregIntercept')[5]).toBe(193.5);
    expect(col(r, 'linregValue')[5]).toBe(193.5);
    expect(col(r, 'linregR2')[5]).toBeUndefined();
    expect(col(r, 'linregR2')[6]).toBeUndefined();
  });

  it('R² stays inside [0, 1] on a non-degenerate series', () => {
    const v = col(
      linearRegression(bars(wavyCloses), { period: 5 }),
      'linregR2',
    );
    const seen = v.filter((x) => x !== undefined);
    expect(seen.length).toBeGreaterThan(20);
    for (const x of seen) {
      expect(x).toBeGreaterThanOrEqual(0);
      expect(x).toBeLessThanOrEqual(1);
    }
  });

  it('defaults to period 14 and the `linreg` prefix; honours column and prefix', () => {
    const b = bars(wavyCloses);
    expect(col(linearRegression(b), 'linregSlope')).toEqual(
      col(linearRegression(b, { period: 14 }), 'linregSlope'),
    );
    const renamed = linearRegression(sma(b, { period: 3, output: 'fast' }), {
      period: 4,
      column: 'fast',
      prefix: 'fit',
    });
    expect(col(renamed, 'fitSlope')[20]).toBeDefined();
    expect(col(renamed, 'fitR2')[20]).toBeDefined();
  });

  it('rejects period < 2, a bad period and a colliding output', () => {
    const b = bars(wavyCloses);
    expect(() => linearRegression(b, { period: 0 })).toThrow(TypeError);
    expect(() => linearRegression(b, { period: 1.5 })).toThrow(TypeError);
    // One point does not determine a line — the denominator is 0 at n = 1.
    // The message names the STUDY, not the kernel underneath it.
    expect(() => linearRegression(b, { period: 1 })).toThrow(
      /linearRegression period must be at least 2/,
    );
    // A prefix family collides on its own appended names, so running it
    // twice under one prefix is the case that has to throw.
    expect(() => linearRegression(linearRegression(b))).toThrow(/collides/);
  });

  it('reads all-missing for a misnamed column, and when period > length', () => {
    expect(
      col(
        linearRegression(bars(wavyCloses), {
          period: 3,
          column: 'nope' as never,
        }),
        'linregSlope',
      ).every((x) => x === undefined),
    ).toBe(true);
    const v = col(
      linearRegression(bars([10, 12, 11]), { period: 5 }),
      'linregValue',
    );
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('timeSeriesForecast', () => {
  it('projects the fit one bar PAST the window, hand-computed', () => {
    // Same fit as linearRegression's hand case: bar 2 slope 0.5, intercept
    // 10.5 → 10.5 + 0.5·3 = 12; bar 3 slope 1.5, intercept 67/6 → 67/6 + 4.5.
    const v = col(
      timeSeriesForecast(bars([10, 12, 11, 15]), { period: 3 }),
      'tsf',
    );
    expect(v).toHaveLength(4);
    expect(v.slice(0, 2).every((x) => x === undefined)).toBe(true);
    expect(v[2]).toBeCloseTo(12, 12);
    expect(v[3]).toBeCloseTo(67 / 6 + 4.5, 12);
  });

  it('is exactly one slope past linearRegression’s in-window endpoint', () => {
    const b = bars(wavyCloses);
    const fit = linearRegression(b, { period: 6 });
    const value = col(fit, 'linregValue');
    const slope = col(fit, 'linregSlope');
    const tsf = col(timeSeriesForecast(b, { period: 6 }), 'tsf');
    for (let i = 5; i < wavyCloses.length; i += 1) {
      expect(tsf[i], `bar ${i}`).toBeCloseTo(value[i]! + slope[i]!, 12);
    }
  });

  it('reads an exact line’s NEXT value back, exactly', () => {
    const line = Array.from({ length: 30 }, (_, i) => 40 + 0.25 * i);
    const v = col(timeSeriesForecast(bars(line), { period: 5 }), 'tsf');
    for (let i = 4; i < 30; i += 1) {
      expect(v[i], `bar ${i}`).toBe(40 + 0.25 * (i + 1));
    }
  });

  it('a flat window forecasts the flat level, exactly', () => {
    const v = col(
      timeSeriesForecast(bars([9, 4, 7, 7, 7, 7]), { period: 3 }),
      'tsf',
    );
    expect(v[4]).toBe(7);
    expect(v[5]).toBe(7);
  });

  it('defaults to period 14 and the `tsf` column; honours column and output', () => {
    const b = bars(wavyCloses);
    const v = col(timeSeriesForecast(b), 'tsf');
    expect(v.slice(0, 13).every((x) => x === undefined)).toBe(true);
    expect(v[13]).toBeDefined();
    expect(v).toEqual(col(timeSeriesForecast(b, { period: 14 }), 'tsf'));
    const renamed = timeSeriesForecast(sma(b, { period: 3, output: 'fast' }), {
      period: 4,
      column: 'fast',
      output: 'tsfFast',
    });
    expect(col(renamed, 'tsfFast')[20]).toBeDefined();
  });

  it('rejects period < 2, a bad period and a colliding output', () => {
    const b = bars(wavyCloses);
    expect(() => timeSeriesForecast(b, { period: 0 })).toThrow(TypeError);
    expect(() => timeSeriesForecast(b, { period: 1 })).toThrow(
      /timeSeriesForecast period must be at least 2/,
    );
    expect(() => timeSeriesForecast(b, { output: 'close' })).toThrow(
      /collides/,
    );
  });

  it('is all-undefined when the period exceeds the series, length kept', () => {
    const v = col(timeSeriesForecast(bars([10, 12, 11]), { period: 5 }), 'tsf');
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('chandeForecastOscillator', () => {
  it('is 100·(price − TSF)/price, hand-computed', () => {
    // Same fit again: bar 2 TSF 12 against a close of 11, bar 3 TSF 67/6+4.5
    // against 15.
    const v = col(
      chandeForecastOscillator(bars([10, 12, 11, 15]), { period: 3 }),
      'cfo',
    );
    expect(v).toHaveLength(4);
    expect(v.slice(0, 2).every((x) => x === undefined)).toBe(true);
    expect(v[2]).toBeCloseTo((100 * (11 - 12)) / 11, 12);
    expect(v[3]).toBeCloseTo((100 * (15 - (67 / 6 + 4.5))) / 15, 12);
  });

  it('divides by the PRICE, not by the forecast', () => {
    // The two agree only where the numerator is zero, so one bar pins it.
    const b = bars([10, 12, 11, 15]);
    const cfo = col(chandeForecastOscillator(b, { period: 3 }), 'cfo')[2]!;
    const byForecast = (100 * (11 - 12)) / 12;
    expect(cfo).not.toBeCloseTo(byForecast, 6);
  });

  it('a flat window reads exactly 0 — the price IS its forecast', () => {
    const v = col(
      chandeForecastOscillator(bars([9, 4, 7, 7, 7, 7]), { period: 3 }),
      'cfo',
    );
    expect(v[4]).toBe(0);
    expect(v[5]).toBe(0);
  });

  it('a zero price reads undefined rather than ±Infinity', () => {
    // Reachable only over a column that crosses zero. The forecast at bar 3
    // is not zero, so the numerator is NOT forced to zero with the
    // denominator — the guard is what stops an Infinity reaching withColumn.
    const v = col(
      chandeForecastOscillator(bars([3, 2, 1, 0, -1, -2]), { period: 3 }),
      'cfo',
    );
    expect(v).toHaveLength(6);
    expect(v[3]).toBeUndefined();
    // A NEGATIVE price still produces a number (the `percentChange` rule).
    expect(v[4]).toBeDefined();
    expect(v[5]).toBeDefined();
  });

  it('defaults to period 14 and the `cfo` column; honours column and output', () => {
    const b = bars(wavyCloses);
    const v = col(chandeForecastOscillator(b), 'cfo');
    expect(v.slice(0, 13).every((x) => x === undefined)).toBe(true);
    expect(v[13]).toBeDefined();
    expect(v).toEqual(col(chandeForecastOscillator(b, { period: 14 }), 'cfo'));
    const renamed = chandeForecastOscillator(
      sma(b, { period: 3, output: 'fast' }),
      { period: 4, column: 'fast', output: 'cfoFast' },
    );
    expect(col(renamed, 'cfoFast')[20]).toBeDefined();
  });

  it('rejects period < 2, a bad period and a colliding output', () => {
    const b = bars(wavyCloses);
    expect(() => chandeForecastOscillator(b, { period: 0 })).toThrow(TypeError);
    expect(() => chandeForecastOscillator(b, { period: 1 })).toThrow(
      /chandeForecastOscillator period must be at least 2/,
    );
    expect(() => chandeForecastOscillator(b, { output: 'close' })).toThrow(
      /collides/,
    );
  });

  it('is all-undefined when the period exceeds the series, length kept', () => {
    const v = col(
      chandeForecastOscillator(bars([10, 12, 11]), { period: 5 }),
      'cfo',
    );
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('centerOfGravity', () => {
  it('is the position-weighted balance point, hand-computed', () => {
    // Closes 10, 12, 11, 15 at period 3. The NEWEST bar carries weight 1.
    //   bar 2: −(1·11 + 2·12 + 3·10)/(11 + 12 + 10) = −65/33
    //   bar 3: −(1·15 + 2·11 + 3·12)/(15 + 11 + 12) = −73/38
    const v = col(
      centerOfGravity(bars([10, 12, 11, 15]), { period: 3 }),
      'cog',
    );
    expect(v).toHaveLength(4);
    expect(v.slice(0, 2).every((x) => x === undefined)).toBe(true);
    expect(v[2]).toBeCloseTo(-65 / 33, 12);
    expect(v[3]).toBeCloseTo(-73 / 38, 12);
  });

  it('matches the naive O(N·period) definition it is an identity for', () => {
    // The study ships `(n+1)·(wma/(2·sma) − 1)` rather than the two sums;
    // this is the test that lets a future editor check the shortcut instead
    // of trusting it.
    const naive = (values: number[], period: number) =>
      values.map((_, t) => {
        if (t < period - 1) return undefined;
        let num = 0;
        let den = 0;
        for (let k = 0; k < period; k += 1) {
          num += (k + 1) * values[t - k]!;
          den += values[t - k]!;
        }
        return -num / den;
      });
    for (const period of [2, 3, 10, 20]) {
      const v = col(centerOfGravity(bars(wavyCloses), { period }), 'cog');
      const want = naive(wavyCloses, period);
      for (let i = 0; i < wavyCloses.length; i += 1) {
        if (want[i] === undefined) {
          expect(v[i], `period ${period} bar ${i}`).toBeUndefined();
        } else {
          expect(v[i], `period ${period} bar ${i}`).toBeCloseTo(want[i]!, 11);
        }
      }
    }
  });

  it('a flat window balances exactly in the middle, at −(period + 1)/2', () => {
    const v = col(
      centerOfGravity(bars([9, 4, 7, 7, 7, 7]), { period: 3 }),
      'cog',
    );
    expect(v[4]).toBeCloseTo(-2, 12);
    expect(v[5]).toBeCloseTo(-2, 12);
  });

  it('stays inside [−period, −1] and moves UP as the newest bars get heavier', () => {
    const period = 5;
    const v = col(centerOfGravity(bars(wavyCloses), { period }), 'cog');
    const seen = v.filter((x) => x !== undefined);
    expect(seen.length).toBeGreaterThan(20);
    for (const x of seen) {
      expect(x).toBeGreaterThanOrEqual(-period);
      expect(x).toBeLessThanOrEqual(-1);
    }
    // A rising window puts more weight on the newest (lightest) bars, so the
    // balance point sits ABOVE the flat −(period+1)/2 = −3; a falling one
    // below it.
    const rising = col(
      centerOfGravity(bars([1, 2, 3, 4, 9]), { period }),
      'cog',
    );
    const falling = col(
      centerOfGravity(bars([9, 4, 3, 2, 1]), { period }),
      'cog',
    );
    expect(rising[4]!).toBeGreaterThan(-3);
    expect(falling[4]!).toBeLessThan(-3);
  });

  it('a zero-sum window reads undefined rather than ±Infinity', () => {
    // [1, −1] sums to 0 with a weighted sum of −1, so the numerator is NOT
    // forced to zero with the denominator and the guard is load-bearing.
    const v = col(centerOfGravity(bars([5, 1, -1, 4]), { period: 2 }), 'cog');
    expect(v).toHaveLength(4);
    expect(v[2]).toBeUndefined();
    expect(v[1]).toBeDefined();
    expect(v[3]).toBeDefined();
  });

  it('defaults to period 10 and the `cog` column; honours column and output', () => {
    const b = bars(wavyCloses);
    const v = col(centerOfGravity(b), 'cog');
    expect(v.slice(0, 9).every((x) => x === undefined)).toBe(true);
    expect(v[9]).toBeDefined();
    expect(v).toEqual(col(centerOfGravity(b, { period: 10 }), 'cog'));
    const renamed = centerOfGravity(sma(b, { period: 3, output: 'fast' }), {
      period: 4,
      column: 'fast',
      output: 'cogFast',
    });
    expect(col(renamed, 'cogFast')[20]).toBeDefined();
  });

  it('accepts period 1 — a one-bar balance point is −1, not a 0/0', () => {
    // Unlike the three regression studies: CG takes a moment, not a fit, so
    // there is nothing degenerate about a single bar.
    const v = col(centerOfGravity(bars([10, 12, 11]), { period: 1 }), 'cog');
    expect(v).toEqual([-1, -1, -1]);
  });

  it('rejects a bad period and a colliding output', () => {
    const b = bars(wavyCloses);
    expect(() => centerOfGravity(b, { period: 0 })).toThrow(TypeError);
    expect(() => centerOfGravity(b, { period: 1.5 })).toThrow(TypeError);
    expect(() => centerOfGravity(b, { output: 'close' })).toThrow(/collides/);
  });

  it('is all-undefined when the period exceeds the series, length kept', () => {
    const v = col(centerOfGravity(bars([10, 12, 11]), { period: 5 }), 'cog');
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

/* -------------------------------------------------------------------------- */
/* The two-series / comparison family (corpus §6.7, kernel K8).                */
/*                                                                             */
/* The comparison series is a COLUMN on the same series, so every fixture here */
/* is a two-column bar series — the shape a consumer gets from `align` +       */
/* `TimeSeries.joinMany`, which the first test builds for real.                */
/* -------------------------------------------------------------------------- */

const pairSchema = [
  { name: 'time', kind: 'time' },
  { name: 'close', kind: 'number' },
  { name: 'bench', kind: 'number' },
] as const;

function pairBars(closes: number[], bench: number[]) {
  return new TimeSeries({
    name: 'bars',
    schema: pairSchema,
    rows: closes.map((c, i) => [i, c, bench[i]!]) as Array<
      [number, number, number]
    >,
  });
}

/** Never monotonic, never flat, and the two sides lead each other in places
 *  so the correlation crosses zero rather than sitting at one corner. */
const pairCloses = [100, 102, 101, 104, 103, 107, 105, 108, 110, 109, 112, 111];
const pairBench = [50, 51, 50.5, 51.5, 52, 52.5, 53.5, 53, 54, 55, 54.5, 56];

/** A straightforward two-pass beta over the last `period` one-bar returns —
 *  written out here so the study's answer is checked against arithmetic a
 *  reader can follow, not against a second copy of the kernel. */
function betaReference(
  closes: number[],
  bench: number[],
  period: number,
  at: number,
): number {
  const rx: number[] = [];
  const ry: number[] = [];
  for (let i = at - period + 1; i <= at; i += 1) {
    rx.push(closes[i]! / closes[i - 1]! - 1);
    ry.push(bench[i]! / bench[i - 1]! - 1);
  }
  const mx = rx.reduce((a, b) => a + b, 0) / period;
  const my = ry.reduce((a, b) => a + b, 0) / period;
  let cov = 0;
  let vy = 0;
  for (let i = 0; i < period; i += 1) {
    cov += (rx[i]! - mx) * (ry[i]! - my);
    vy += (ry[i]! - my) * (ry[i]! - my);
  }
  return cov / vy;
}

describe('the two-series family: joining the benchmark in', () => {
  it('the documented recipe works: two series, align-free join, then the study', () => {
    // This is the recipe every docstring in the family points at, run for
    // real. It is the argument for the design: the study never sees two
    // series, because core has already made them one row.
    const barsOnly = bars(pairCloses);
    const spy = new TimeSeries({
      name: 'spy',
      schema: closeSchema,
      rows: pairBench.map((c, i) => [i, c]) as Array<[number, number]>,
    });

    const wide = TimeSeries.joinMany([barsOnly, spy.rename({ close: 'spy' })], {
      type: 'inner',
    });
    expect(wide.length).toBe(pairCloses.length);

    const withCorr = correlation(wide, {
      column: 'close',
      benchmark: 'spy',
      period: 4,
    });
    const withBeta = beta(withCorr, {
      column: 'close',
      benchmark: 'spy',
      period: 4,
    });
    const both = priceRelative(withBeta, {
      column: 'close',
      benchmark: 'spy',
    });

    // Same numbers as running the same studies over a hand-built wide series
    // — the join is transport, not semantics.
    const direct = pairBars(pairCloses, pairBench);
    expect(col(both, 'corr')).toEqual(
      col(correlation(direct, { benchmark: 'bench', period: 4 }), 'corr'),
    );
    expect(col(both, 'beta')).toEqual(
      col(beta(direct, { benchmark: 'bench', period: 4 }), 'beta'),
    );
    expect(col(both, 'priceRel')).toEqual(
      col(priceRelative(direct, { benchmark: 'bench' }), 'priceRel'),
    );
  });

  it('an OUTER join leaves one-sided rows, and the strict window blanks them', () => {
    // The other half of the recipe: what a join type means for the study. An
    // outer join over two different bar clocks produces rows with only one
    // side present; a bivariate moment cannot use them, so they and the
    // windows over them read `undefined` rather than being averaged around.
    const barsOnly = bars(pairCloses);
    const sparse = new TimeSeries({
      name: 'spy',
      schema: closeSchema,
      // Only the even bars, so every odd row of the join is one-sided.
      rows: pairBench
        .map((c, i) => [i, c] as [number, number])
        .filter((_, i) => i % 2 === 0),
    });
    const wide = TimeSeries.joinMany(
      [barsOnly, sparse.rename({ close: 'spy' })],
      { type: 'outer' },
    );
    const v = col(
      correlation(wide, { column: 'close', benchmark: 'spy', period: 3 }),
      'corr',
    );
    expect(v).toHaveLength(pairCloses.length);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('correlation', () => {
  it('is Pearson r of the two columns, warm-up period − 1, length kept', () => {
    const r = correlation(pairBars(pairCloses, pairBench), {
      benchmark: 'bench',
      period: 4,
    });
    const v = col(r, 'corr');
    expect(v).toHaveLength(pairCloses.length);
    expect(v.slice(0, 3).every((x) => x === undefined)).toBe(true);
    expect(typeof v[3]).toBe('number');
    // Bars 0..3: close 100,102,101,104 (mean 101.75); bench 50,51,50.5,51.5
    // (mean 50.75). cov = (−1.75·−0.75 + 0.25·0.25 + −0.75·−0.25 +
    // 2.25·0.75)/4 = (1.3125 + 0.0625 + 0.1875 + 1.6875)/4 = 0.8125.
    // varX = (3.0625+0.0625+0.5625+5.0625)/4 = 2.1875,
    // varY = (0.5625+0.0625+0.0625+0.5625)/4 = 0.3125.
    expect(v[3]!).toBeCloseTo(0.8125 / Math.sqrt(2.1875 * 0.3125), 12);
    expect(Math.abs(v[3]!)).toBeLessThanOrEqual(1);
  });

  it('defaults to period 30, column close and output corr', () => {
    const long = Array.from(
      { length: 40 },
      (_, i) => 100 + Math.sin(i / 3) * 5,
    );
    const other = Array.from(
      { length: 40 },
      (_, i) => 60 + Math.cos(i / 4) * 3,
    );
    const v = col(
      correlation(pairBars(long, other), { benchmark: 'bench' }),
      'corr',
    );
    expect(v.slice(0, 29).every((x) => x === undefined)).toBe(true);
    expect(typeof v[29]).toBe('number');
  });

  it('reads exactly +1 on an affine benchmark and −1 on a negated one', () => {
    // The definition of Pearson's r: invariant to an independent scale AND
    // shift of either column, so an affine pair is perfectly correlated.
    const affine = pairCloses.map((c) => 2 * c + 5);
    expect(
      col(
        correlation(pairBars(pairCloses, affine), {
          benchmark: 'bench',
          period: 5,
        }),
        'corr',
      ).slice(4),
    ).toEqual(Array.from({ length: 8 }, () => 1));
    // The negated pair is NOT bit-exact at the kernel: this input's raw
    // ratio reads `-1.0000000000000002` on one bar. Since #707 the study
    // pins |r| to 1 — the kernel now rebuilds any window whose moments
    // could overshoot materially, so what is left is last-ulp rounding,
    // and the pin is asserted here as the bound holding on every bar.
    const negated = pairCloses.map((c) => -3 * c + 1000);
    const negatedCorr = col(
      correlation(pairBars(pairCloses, negated), {
        benchmark: 'bench',
        period: 5,
      }),
      'corr',
    ).slice(4);
    for (const r of negatedCorr) expect(r!).toBeCloseTo(-1, 14);
    expect(negatedCorr.every((r) => r! >= -1 && r! <= 1)).toBe(true);
    expect(negatedCorr.some((r) => r === -1)).toBe(true);
  });

  it('a flat window on either side is undefined, not 0 (TA-Lib says 0)', () => {
    // A genuine 0/0 — there is no correlation to report. Deliberate delta
    // from `talib.CORREL`, which substitutes 0.0 for its zero denominator
    // (measured). No guard exists: the covariance of a flat window is
    // exactly 0, so the division is already 0/0.
    const flat = pairCloses.map(() => 7);
    expect(
      col(
        correlation(pairBars(pairCloses, flat), {
          benchmark: 'bench',
          period: 4,
        }),
        'corr',
      ).every((x) => x === undefined),
    ).toBe(true);
    expect(
      col(
        correlation(pairBars(flat, pairCloses), {
          benchmark: 'bench',
          period: 4,
        }),
        'corr',
      ).every((x) => x === undefined),
    ).toBe(true);
  });

  it('validates its options', () => {
    const b = pairBars(pairCloses, pairBench);
    expect(() => correlation(b, { benchmark: 'bench', period: 0 })).toThrow(
      /positive integer/,
    );
    expect(() => correlation(b, { benchmark: 'bench', period: 1 })).toThrow(
      /at least 2/,
    );
    expect(() =>
      correlation(b, { benchmark: 'bench', output: 'close' as never }),
    ).toThrow(/collides/);
    // A benchmark that is the source column is a mistake, not a reading.
    expect(() => correlation(b, { benchmark: 'close' })).toThrow(/same column/);
    // A misnamed column THROWS in this family rather than reading empty —
    // the deliberate resolution of the reducer-dependent split the volatility
    // tail recorded, chosen here because these studies read `columnValues`
    // directly and inherit neither door's answer.
    expect(() => correlation(b, { benchmark: 'spy' as never })).toThrow(
      /benchmark column 'spy' is not on the series/,
    );
    // A benchmark that exists but is not numeric throws too — otherwise it
    // would read all-NaN, the silent-empty outcome the throw exists to stop.
    const labelled = new TimeSeries({
      name: 'bars',
      schema: [...pairSchema, { name: 'label', kind: 'string' }] as const,
      rows: pairCloses.map(
        (c, i) =>
          [i, c, pairBench[i]!, 'spy'] as [number, number, number, string],
      ),
    });
    expect(() =>
      correlation(labelled, { benchmark: 'label' as never, period: 3 }),
    ).toThrow(
      /benchmark column 'label' is a string column, not a number column/,
    );
    // A misnamed `column` has a default and reads all-missing, as in every
    // other study; only the required `benchmark` throws.
    const typo = correlation(b, {
      benchmark: 'bench',
      column: 'nope' as never,
    });
    expect(col(typo, 'corr').every((v) => v === undefined)).toBe(true);
  });

  it('period longer than the series is all-undefined, length kept', () => {
    const v = col(
      correlation(pairBars([1, 2, 3], [3, 2, 1]), {
        benchmark: 'bench',
        period: 5,
      }),
      'corr',
    );
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('correlation on a column that freezes mid-series', () => {
  it('reads undefined on the flat windows and finite elsewhere — never throws', () => {
    // The Layer-2 repro for #706: a tick-frozen price that steps every 97
    // bars, against a moving benchmark. Before the kernel's change counter
    // this threw `withColumn 'corr': index 126 is -Infinity`.
    const n = 600;
    const closes = Array.from(
      { length: n },
      (_, i) => 100 + Math.floor(i / 97),
    );
    const bench = Array.from(
      { length: n },
      (_, i) => 50 + 3 * Math.sin(i / 5) + i * 0.001,
    );
    const r = correlation(pairBars(closes, bench), {
      benchmark: 'bench',
      period: 30,
    });
    const v = col(r, 'corr');
    let flat = 0;
    let moving = 0;
    for (let i = 29; i < n; i += 1) {
      const isFlat = Math.floor((i - 29) / 97) === Math.floor(i / 97);
      if (isFlat) {
        flat += 1;
        expect(v[i], `corr[${i}]`).toBeUndefined();
      } else {
        moving += 1;
        expect(Number.isFinite(v[i]!), `corr[${i}]`).toBe(true);
        expect(Math.abs(v[i]!)).toBeLessThanOrEqual(1 + 1e-12);
      }
    }
    expect(flat).toBeGreaterThan(300);
    expect(moving).toBeGreaterThan(100);
  });
});

describe('beta', () => {
  it('is the slope of the returns, first valid at bar `period`', () => {
    const r = beta(pairBars(pairCloses, pairBench), {
      benchmark: 'bench',
      period: 4,
    });
    const v = col(r, 'beta');
    expect(v).toHaveLength(pairCloses.length);
    // `period` returns need `period + 1` prices, so the warm-up is `period`
    // rows and not `period − 1` — one later than a plain window study.
    expect(v.slice(0, 4).every((x) => x === undefined)).toBe(true);
    expect(v[4]).toBeCloseTo(betaReference(pairCloses, pairBench, 4, 4), 10);
    expect(v[9]).toBeCloseTo(betaReference(pairCloses, pairBench, 4, 9), 10);
  });

  it('defaults to period 5, column close and output beta', () => {
    const v = col(
      beta(pairBars(pairCloses, pairBench), { benchmark: 'bench' }),
      'beta',
    );
    expect(v.slice(0, 5).every((x) => x === undefined)).toBe(true);
    expect(typeof v[5]).toBe('number');
  });

  it('is exactly 1 against a SCALED benchmark and not 1 against an affine one', () => {
    // A pure scale is return-preserving, so beta is exactly 1. An affine
    // transform is NOT — `Δx/(kx + c)` is not `Δx/x` — which is the trap
    // `correlation` (exactly +1 on the same pair) hides.
    const scaled = pairCloses.map((c) => 3.5 * c);
    expect(
      col(
        beta(pairBars(pairCloses, scaled), { benchmark: 'bench', period: 4 }),
        'beta',
      ).slice(4),
    ).toEqual(Array.from({ length: 8 }, () => 1));

    const affine = pairCloses.map((c) => 2 * c + 5);
    const v = col(
      beta(pairBars(pairCloses, affine), { benchmark: 'bench', period: 4 }),
      'beta',
    );
    expect(v[4]!).toBeCloseTo(betaReference(pairCloses, affine, 4, 4), 10);
    expect(v[4]!).not.toBeCloseTo(1, 3);
    expect(v[4]!).toBeGreaterThan(1); // the benchmark's returns are SMALLER
    // …while the correlation of the very same pair is exactly 1.
    expect(
      col(
        correlation(pairBars(pairCloses, affine), {
          benchmark: 'bench',
          period: 4,
        }),
        'corr',
      )[4],
    ).toBe(1);
  });

  it('a flat benchmark window is undefined, not 0 (TA-Lib says 0)', () => {
    const flat = pairCloses.map(() => 7);
    expect(
      col(
        beta(pairBars(pairCloses, flat), { benchmark: 'bench', period: 4 }),
        'beta',
      ).every((x) => x === undefined),
    ).toBe(true);
  });

  it('a zero price is a missing return, so it blanks `period` readings', () => {
    // TA-Lib substitutes a return of 0 for a zero base; `percentChangeValues`
    // marks it missing, and the strict window carries that through. Only
    // reachable when `column` is another study's output.
    const holed = [...pairCloses];
    holed[5] = 0;
    const v = col(
      beta(pairBars(holed, pairBench), { benchmark: 'bench', period: 3 }),
      'beta',
    );
    // Only ONE return is unusable, and it is the one AFTER the zero: the
    // return INTO a zero price is a legitimate −100%, the return out of it
    // divides by zero. So the three windows ending at bars 6, 7 and 8 blank
    // and bar 5 — whose window ends on the −100% — still reads.
    expect(typeof v[5]).toBe('number');
    expect(v.slice(6, 9).every((x) => x === undefined)).toBe(true);
    expect(typeof v[9]).toBe('number');
  });

  it('validates its options', () => {
    const b = pairBars(pairCloses, pairBench);
    expect(() => beta(b, { benchmark: 'bench', period: 1 })).toThrow(
      /at least 2/,
    );
    expect(() => beta(b, { benchmark: 'bench', period: 2.5 })).toThrow(
      /positive integer/,
    );
    expect(() => beta(b, { benchmark: 'close' })).toThrow(/same column/);
    expect(() => beta(b, { benchmark: 'nope' as never })).toThrow(
      /is not on the series/,
    );
    expect(() =>
      beta(b, { benchmark: 'bench', output: 'bench' as never }),
    ).toThrow(/collides/);
  });
});

describe('correlation at magnitudes where a product of variances underflows', () => {
  it('reads the exact value at 1e-90, not a laundered −1 (second-pass review of #707)', () => {
    // Kernel moments are exactly right here; `sqrt(vx · vy)` underflows to
    // 0 and `cov / 0` is −Infinity, which a bare ±1 pin then turned into a
    // plausible −1. The study falls back to the separate roots exactly
    // there, and pins only rounding-sized overshoot. Exact corr of the
    // first window is −0.327.
    const closes = [1e-90, 2e-90, 4e-90, 3e-90, 1e-90, 5e-90];
    const bench = [3e-90, 1e-90, 2e-90, 4e-90, 2e-90, 1e-90];
    const r = col(
      correlation(pairBars(closes, bench), { benchmark: 'bench', period: 3 }),
      'corr',
    );
    expect(r[2]).toBeCloseTo(-0.3273268353539886, 12);
    for (let i = 2; i < closes.length; i += 1) {
      expect(Number.isFinite(r[i]!), `corr[${i}] finite`).toBe(true);
      expect(Math.abs(r[i]!)).toBeLessThanOrEqual(1);
    }
    // And the overflow side: |price| ≈ 1e80 read `−0` before.
    const big = col(
      correlation(
        pairBars(
          closes.map((v) => v * 1e170),
          bench.map((v) => v * 1e170),
        ),
        { benchmark: 'bench', period: 3 },
      ),
      'corr',
    );
    expect(big[2]).toBeCloseTo(-0.3273268353539886, 12);
  });
});

describe('beta on a benchmark that freezes mid-series', () => {
  it('reads undefined where the benchmark returns are all zero and finite elsewhere — never throws', () => {
    // The Layer-2 repro for #706: a stale (forward-filled) benchmark that
    // steps every 61 bars, `period 5`. Before the change counter this threw
    // `withColumn 'beta': index 127 is Infinity`.
    const n = 400;
    const closes = Array.from(
      { length: n },
      (_, i) => 100 + 4 * Math.sin(i / 3) + i * 0.02,
    );
    const bench = Array.from(
      { length: n },
      (_, i) => 50 + Math.floor(i / 61) * 0.5,
    );
    const r = beta(pairBars(closes, bench), { benchmark: 'bench', period: 5 });
    const v = col(r, 'beta');
    let flat = 0;
    let moving = 0;
    // Bar i's window holds the returns at bars i−4 … i; return k is non-zero
    // only when the benchmark stepped at bar k (k % 61 === 0, k > 0).
    for (let i = 5; i < n; i += 1) {
      let steps = 0;
      for (let k = i - 4; k <= i; k += 1) if (k % 61 === 0) steps += 1;
      if (steps === 0) {
        flat += 1;
        expect(v[i], `beta[${i}]`).toBeUndefined();
      } else {
        moving += 1;
        expect(Number.isFinite(v[i]!), `beta[${i}]`).toBe(true);
      }
    }
    expect(flat).toBeGreaterThan(300);
    expect(moving).toBeGreaterThan(20);
  });
});

describe('priceRelative', () => {
  it('is the ratio, on every bar, with no warm-up at all', () => {
    const v = col(
      priceRelative(pairBars(pairCloses, pairBench), { benchmark: 'bench' }),
      'priceRel',
    );
    expect(v).toHaveLength(pairCloses.length);
    expect(v[0]).toBe(2); // 100 / 50 — bar 0 emits; it reads one row
    expect(v[1]).toBeCloseTo(102 / 51, 12);
    expect(v.every((x) => typeof x === 'number')).toBe(true);
  });

  it('is scale-EQUIVARIANT in column and inverse in benchmark', () => {
    const base = col(
      priceRelative(pairBars(pairCloses, pairBench), { benchmark: 'bench' }),
      'priceRel',
    );
    const scaledColumn = col(
      priceRelative(
        pairBars(
          pairCloses.map((c) => 3 * c),
          pairBench,
        ),
        {
          benchmark: 'bench',
        },
      ),
      'priceRel',
    );
    const scaledBench = col(
      priceRelative(
        pairBars(
          pairCloses,
          pairBench.map((c) => 3 * c),
        ),
        {
          benchmark: 'bench',
        },
      ),
      'priceRel',
    );
    for (let i = 0; i < base.length; i += 1) {
      expect(scaledColumn[i]!).toBeCloseTo(base[i]! * 3, 10);
      expect(scaledBench[i]!).toBeCloseTo(base[i]! / 3, 10);
    }
  });

  it('a zero benchmark is undefined — a LIVE guard at the output', () => {
    // Without it this is `Infinity`, which `withColumn` rejects outright, so
    // "the study returns at all" is half the assertion and "only that row is
    // missing" is the other half.
    const holed = [...pairBench];
    holed[3] = 0;
    const v = col(
      priceRelative(pairBars(pairCloses, holed), { benchmark: 'bench' }),
      'priceRel',
    );
    expect(v[3]).toBeUndefined();
    expect(v.filter((x) => x === undefined)).toHaveLength(1);
    // A NEGATIVE benchmark still produces a number (`=== 0`, not `<= 0`).
    const negative = [...pairBench];
    negative[3] = -25;
    expect(
      col(
        priceRelative(pairBars(pairCloses, negative), { benchmark: 'bench' }),
        'priceRel',
      )[3],
    ).toBeCloseTo(104 / -25, 12);
  });

  it('validates its options', () => {
    const b = pairBars(pairCloses, pairBench);
    expect(() => priceRelative(b, { benchmark: 'close' })).toThrow(
      /same column/,
    );
    expect(() => priceRelative(b, { benchmark: 'nope' as never })).toThrow(
      /is not on the series/,
    );
    expect(() =>
      priceRelative(b, { benchmark: 'bench', output: 'close' as never }),
    ).toThrow(/collides/);
  });
});

describe('performanceIndex', () => {
  it('is the ratio of the two period-bar growths, warm-up `period`', () => {
    const v = col(
      performanceIndex(pairBars(pairCloses, pairBench), {
        benchmark: 'bench',
        period: 4,
      }),
      'perf',
    );
    expect(v).toHaveLength(pairCloses.length);
    expect(v.slice(0, 4).every((x) => x === undefined)).toBe(true);
    // Bar 4: close 103/100, bench 52/50. Bar 11: close 111/108, bench 56/53.
    expect(v[4]!).toBeCloseTo(103 / 100 / (52 / 50), 12);
    expect(v[11]!).toBeCloseTo(111 / 108 / (56 / 53), 12);
  });

  it('defaults to period 20 and output perf', () => {
    const long = Array.from({ length: 30 }, (_, i) => 100 + i * 0.7);
    const other = Array.from({ length: 30 }, (_, i) => 40 + i * 0.2);
    const v = col(
      performanceIndex(pairBars(long, other), { benchmark: 'bench' }),
      'perf',
    );
    expect(v.slice(0, 20).every((x) => x === undefined)).toBe(true);
    expect(typeof v[20]).toBe('number');
  });

  it('(perf − 1) × 100 IS percentChange(priceRelative, period)', () => {
    // The identity that makes this a normalisation of the price relative
    // rather than new math — the `benchmark` terms cancel exactly.
    const withRatio = priceRelative(pairBars(pairCloses, pairBench), {
      benchmark: 'bench',
    });
    const viaRoc = col(
      percentChange(withRatio, { column: 'priceRel', periods: 4 }),
      'pctChange',
    );
    const direct = col(
      performanceIndex(pairBars(pairCloses, pairBench), {
        benchmark: 'bench',
        period: 4,
      }),
      'perf',
    );
    for (let i = 4; i < direct.length; i += 1) {
      expect((direct[i]! - 1) * 100).toBeCloseTo(viaRoc[i]!, 9);
    }
  });

  it('all three zero guards are live and report missing, not ±Infinity', () => {
    // Each of these would send a non-finite value (or a silently wrong 0) to
    // `withColumn`; each has its own row.
    const zeroBase = [...pairCloses];
    zeroBase[2] = 0; // column's look-back base
    expect(
      col(
        performanceIndex(pairBars(zeroBase, pairBench), {
          benchmark: 'bench',
          period: 3,
        }),
        'perf',
      )[5],
    ).toBeUndefined();

    const zeroBenchBase = [...pairBench];
    zeroBenchBase[2] = 0; // benchmark's look-back base → growth of 0 → ∞
    expect(
      col(
        performanceIndex(pairBars(pairCloses, zeroBenchBase), {
          benchmark: 'bench',
          period: 3,
        }),
        'perf',
      )[5],
    ).toBeUndefined();

    const zeroBenchNow = [...pairBench];
    zeroBenchNow[5] = 0; // benchmark now → growth of ∞ → a silent 0
    expect(
      col(
        performanceIndex(pairBars(pairCloses, zeroBenchNow), {
          benchmark: 'bench',
          period: 3,
        }),
        'perf',
      )[5],
    ).toBeUndefined();
  });

  it('validates its options', () => {
    const b = pairBars(pairCloses, pairBench);
    expect(() =>
      performanceIndex(b, { benchmark: 'bench', period: 0 }),
    ).toThrow(/positive integer/);
    expect(() => performanceIndex(b, { benchmark: 'close' })).toThrow(
      /same column/,
    );
    expect(() => performanceIndex(b, { benchmark: 'nope' as never })).toThrow(
      /is not on the series/,
    );
    expect(() =>
      performanceIndex(b, { benchmark: 'bench', output: 'bench' as never }),
    ).toThrow(/collides/);
  });

  it('period longer than the series is all-undefined, length kept', () => {
    const v = col(
      performanceIndex(pairBars([1, 2, 3], [3, 2, 1]), {
        benchmark: 'bench',
        period: 5,
      }),
      'perf',
    );
    expect(v).toHaveLength(3);
    expect(v.every((x) => x === undefined)).toBe(true);
  });
});

describe('the two-series oracle cases are in the fixture', () => {
  it('has a case for each of the four studies (bump when adding cases)', () => {
    const fixture = JSON.parse(
      readFileSync(
        new URL('./fixtures/study-oracle.json', import.meta.url),
        'utf8',
      ),
    ) as { cases: Array<{ study: string }> };
    // 119 cases before this batch + 7. A case that silently disappears takes
    // its study's only value check with it, and nothing else would notice.
    expect(fixture.cases).toHaveLength(134);
    const counts = new Map<string, number>();
    for (const c of fixture.cases) {
      counts.set(c.study, (counts.get(c.study) ?? 0) + 1);
    }
    expect(counts.get('correlation')).toBe(2);
    expect(counts.get('beta')).toBe(2);
    expect(counts.get('priceRelative')).toBe(1);
    expect(counts.get('performanceIndex')).toBe(2);
  });
});
