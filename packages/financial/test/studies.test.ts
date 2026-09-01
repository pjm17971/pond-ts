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
    // from the unknown term on. TA-Lib would report 300, 300, 800, 800, 100
    // here — a level 400 out, never recovered.
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
