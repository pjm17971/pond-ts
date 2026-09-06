import { describe, it, expect } from 'vitest';
import { TimeSeries } from 'pond-ts';
import {
  MA_TYPES,
  priceOscillator,
  disparityIndex,
  detrendedPriceOscillator,
  elderRay,
  awesomeOscillator,
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
      else expect(osc[i], `bar ${i}`).toBeCloseTo(line[i]!, 12);
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
