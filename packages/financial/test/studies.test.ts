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
  stochastic,
  williamsR,
  donchian,
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
