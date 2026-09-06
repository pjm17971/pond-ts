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
  directionalMovement,
  aroon,
  vortex,
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
      for (const name of ['dmPlusDi', 'dmMinusDi', 'dmDx', 'dmAdx', 'dmAdxr']) {
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
        cells(directionalMovement(dirBars(holeIn), { period: 2 }), 'dmDx')[3],
      ).toBeDefined();
    }
  });

  it('directionalMovement loses a gap in CLOSE one bar later — the true range reads prevClose', () => {
    const out = directionalMovement(dirBars('close'), { period: 2 });
    for (const name of ['dmPlusDi', 'dmMinusDi', 'dmDx']) {
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
    expect(cells(out, 'dmAdx')[4]).toBeDefined();
    expect(firstMissingFrom(out, 'dmAdx', 4)).toBe(5);
    expect(cells(out, 'dmAdxr')[4]).toBeDefined();
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
