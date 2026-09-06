/**
 * The opt-in fluent surface: `import '@pond-ts/financial/fluent'` mounts the
 * studies as chainable `TimeSeries` methods. This file imports it for its side
 * effect, so `series.sma().ema().bollinger()` both type-checks (the `declare
 * module` merge) and runs (the prototype mount). The methods delegate to the
 * standalone functions, so values must match them exactly.
 */
import { describe, it, expect } from 'vitest';
import { TimeSeries } from 'pond-ts';
import {
  MA_TYPES,
  movingAverage,
  sma,
  ema,
  bollinger,
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
} from '../src/index.js';
import '../src/fluent.js';

const closeSchema = [
  { name: 'time', kind: 'time' },
  { name: 'close', kind: 'number' },
] as const;

function bars() {
  const closes = [10, 11, 12, 13, 14, 15, 16, 17];
  return new TimeSeries({
    name: 'bars',
    schema: closeSchema,
    rows: closes.map((c, i) => [i, c]) as Array<[number, number]>,
  });
}

function col(s: unknown, name: string): Array<number | undefined> {
  const events = (
    s as { events: ReadonlyArray<{ data(): Record<string, unknown> }> }
  ).events;
  return events.map((e) => {
    const v = e.data()[name];
    return typeof v === 'number' ? v : undefined;
  });
}

describe('fluent studies (opt-in prototype augmentation)', () => {
  it('chains metric.sma().ema().bollinger() into one series', () => {
    const study = bars()
      .sma({ period: 3 })
      .ema({ period: 3 })
      .bollinger({ period: 3, stdDev: 2 });
    const last = study.events.at(-1)!.data();
    expect(typeof last.sma).toBe('number');
    expect(typeof last.ema).toBe('number');
    expect(typeof last.bbMiddle).toBe('number');
    expect(typeof last.bbUpper).toBe('number');
    expect(typeof last.bbLower).toBe('number');
  });

  it('interleaves with core methods in the same chain', () => {
    // core `.smooth` then the augmented `.sma`, one chain.
    const study = bars()
      .smooth('close', 'ema', { alpha: 0.5, output: 'e' }) // core method
      .sma({ period: 3, column: 'e', output: 'ma' }); // fluent study
    expect(col(study, 'e')).toHaveLength(8);
    expect(typeof study.events.at(-1)!.data().ma).toBe('number');
    // plain fluent value check: sma(3) at index 4 = avg(close 12,13,14) = 13.
    expect(col(bars().sma({ period: 3, output: 'ma' }), 'ma')[4]).toBe(13);
  });

  it('mounts the whole study set as chainable methods', () => {
    const study = bars()
      .rollingStdev({ period: 3 })
      .rollingMin({ period: 3 })
      .rollingMax({ period: 3 })
      .rollingPercentile({ period: 3, q: 90 })
      .zScore({ period: 3 })
      .envelope({ period: 3, percent: 2 })
      .percentChange({ periods: 1 });
    const last = study.events.at(-1)!.data();
    for (const c of [
      'stdev',
      'min',
      'max',
      'p90',
      'zscore',
      'envMiddle',
      'pctChange',
    ]) {
      expect(typeof last[c], c).toBe('number');
    }
  });

  it('is exactly the standalone functions bound to the series', () => {
    const fluent = bars()
      .sma({ period: 3 })
      .ema({ period: 4, output: 'e' })
      .bollinger({ period: 3 });
    const functional = bollinger(
      ema(sma(bars(), { period: 3 }), { period: 4, output: 'e' }),
      { period: 3 },
    );
    for (const c of ['sma', 'e', 'bbMiddle', 'bbUpper', 'bbLower']) {
      expect(col(fluent, c)).toEqual(col(functional, c));
    }
  });

  it('.movingAverage() is the standalone study, for every type', () => {
    // Chained twice with different `type`s and `output`s, which is the shape
    // a "MA Type" study actually gets used in (fast vs slow line).
    const chained = bars()
      .movingAverage({ period: 3, type: 'wma', output: 'fast' })
      .movingAverage({ period: 4, type: 'hull', output: 'slow' });
    const functional = movingAverage(
      movingAverage(bars(), { period: 3, type: 'wma', output: 'fast' }),
      { period: 4, type: 'hull', output: 'slow' },
    );
    expect(col(chained, 'fast')).toEqual(col(functional, 'fast'));
    expect(col(chained, 'slow')).toEqual(col(functional, 'slow'));

    for (const type of MA_TYPES) {
      expect(
        col(bars().movingAverage({ period: 3, type }), 'ma'),
        type,
      ).toEqual(col(movingAverage(bars(), { period: 3, type }), 'ma'));
    }
  });

  it('mounts momentum and historicalVolatility', () => {
    const study = bars()
      .momentum({ period: 2 })
      .historicalVolatility({ period: 3, annualize: 1, output: 'vol' });
    const last = study.events.at(-1)!.data();
    expect(typeof last.momentum).toBe('number');
    expect(typeof last.vol).toBe('number');
    expect(col(study, 'momentum')).toEqual(
      col(momentum(bars(), { period: 2 }), 'momentum'),
    );
    expect(col(study, 'vol')).toEqual(
      col(
        historicalVolatility(bars(), {
          period: 3,
          annualize: 1,
          output: 'vol',
        }),
        'vol',
      ),
    );
  });

  it('mounts the range-position studies, exactly as the standalone functions', () => {
    const ohlc = () =>
      new TimeSeries({
        name: 'bars',
        schema: [
          { name: 'time', kind: 'time' },
          { name: 'high', kind: 'number' },
          { name: 'low', kind: 'number' },
          { name: 'close', kind: 'number' },
        ] as const,
        rows: Array.from({ length: 12 }, (_, i) => {
          const c = 100 + 5 * Math.sin(i / 2);
          return [i, c + 1 + 0.5 * Math.abs(Math.cos(i)), c - 1, c];
        }) as Array<[number, number, number, number]>,
      });
    const fluent = ohlc()
      .stochastic({ kPeriod: 3, slowing: 2, dPeriod: 2 })
      .williamsR({ period: 3 })
      .donchian({ period: 3 });
    const functional = donchian(
      williamsR(stochastic(ohlc(), { kPeriod: 3, slowing: 2, dPeriod: 2 }), {
        period: 3,
      }),
      { period: 3 },
    );
    const last = fluent.events.at(-1)!.data() as Record<string, unknown>;
    for (const c of [
      'stochK',
      'stochD',
      'williamsR',
      'dcUpper',
      'dcLower',
      'dcMiddle',
    ]) {
      expect(typeof last[c], c).toBe('number');
      expect(col(fluent, c), c).toEqual(col(functional, c));
    }
  });
});

describe('fluent volume studies', () => {
  const ohlcv = () =>
    new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'high', kind: 'number' },
        { name: 'low', kind: 'number' },
        { name: 'close', kind: 'number' },
        { name: 'volume', kind: 'number' },
      ] as const,
      rows: [10, 11, 11, 9, 12, 12, 8].map((c, i) => [
        i,
        c + 1,
        c - 1,
        c,
        100 * (i + 1),
      ]) as Array<[number, number, number, number, number]>,
    });

  it('.obv() and .vwap() chain and match the standalone functions', () => {
    const fluent = ohlcv().obv().vwap({ period: 3, output: 'w' });
    const functional = vwap(obv(ohlcv()), { period: 3, output: 'w' });
    expect(col(fluent, 'obv')).toEqual([100, 300, 300, -100, 400, 400, -300]);
    for (const c of ['obv', 'w']) {
      expect(col(fluent, c)).toEqual(col(functional, c));
    }
  });
});

describe('fluent K2 consumers (channels and smoothed rates)', () => {
  const ohlc = () =>
    new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'open', kind: 'number' },
        { name: 'high', kind: 'number' },
        { name: 'low', kind: 'number' },
        { name: 'close', kind: 'number' },
      ] as const,
      rows: Array.from({ length: 40 }, (_, i) => {
        const c = 100 + 8 * Math.sin(i / 3) + 0.2 * i;
        return [
          i,
          c - 0.5 * Math.cos(i / 1.7),
          c + 0.4 + 0.6 * Math.abs(Math.sin(i / 2)),
          c - 0.5 - 0.6 * Math.abs(Math.cos(i / 2.5)),
          c,
        ];
      }) as Array<[number, number, number, number, number]>,
    });

  it('.keltner().atrBands().qstick().trix().coppock() chain and match the standalone functions', () => {
    const fluent = ohlc()
      .keltner({ period: 6, atrPeriod: 5 })
      .atrBands({ period: 5 })
      .qstick({ period: 4 })
      .trix({ period: 3, signalPeriod: 3 })
      .coppock({ longPeriod: 6, shortPeriod: 3, wmaPeriod: 2 });
    const functional = coppock(
      trix(
        qstick(
          atrBands(keltner(ohlc(), { period: 6, atrPeriod: 5 }), { period: 5 }),
          { period: 4 },
        ),
        { period: 3, signalPeriod: 3 },
      ),
      { longPeriod: 6, shortPeriod: 3, wmaPeriod: 2 },
    );
    const last = fluent.events.at(-1)!.data() as Record<string, unknown>;
    for (const c of [
      'kcMiddle',
      'kcUpper',
      'kcLower',
      'atrbUpper',
      'atrbLower',
      'qstick',
      'trix',
      'trixSignal',
      'coppock',
    ]) {
      expect(typeof last[c], c).toBe('number');
      expect(col(fluent, c), c).toEqual(col(functional, c));
    }
  });

  it('the fluent methods take no options at all (every period has a default)', () => {
    const study = ohlc().keltner().atrBands().qstick().trix().coppock();
    // 40 bars is not enough for the default TRIX (3 × 15 − 2 = 43), so what
    // is pinned here is that the no-argument calls run, stay
    // length-preserving, and warm up where their defaults say.
    expect(study.length).toBe(40);
    expect(col(study, 'kcMiddle').filter((x) => x !== undefined).length).toBe(
      21,
    );
    expect(col(study, 'trix').every((x) => x === undefined)).toBe(true);
  });
});

describe('fluent K2 oscillators', () => {
  const oscBars = () =>
    new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'high', kind: 'number' },
        { name: 'low', kind: 'number' },
        { name: 'close', kind: 'number' },
      ] as const,
      rows: Array.from({ length: 24 }, (_, i) => {
        const c = 100 + 6 * Math.sin(i / 3) + 0.2 * i;
        return [
          i,
          c + 0.4 + 0.6 * Math.abs(Math.sin(i / 2)),
          c - 0.4 - 0.6 * Math.abs(Math.cos(i / 2.4)),
          c,
        ];
      }) as Array<[number, number, number, number]>,
    });

  it('chains all five and matches the standalone functions bar for bar', () => {
    const fluent = oscBars()
      .priceOscillator({ fastPeriod: 3, slowPeriod: 7 })
      .disparityIndex({ period: 5 })
      .detrendedPriceOscillator({ period: 5 })
      .elderRay({ period: 4 })
      .awesomeOscillator({ fastPeriod: 3, slowPeriod: 8 });
    const functional = awesomeOscillator(
      elderRay(
        detrendedPriceOscillator(
          disparityIndex(
            priceOscillator(oscBars(), { fastPeriod: 3, slowPeriod: 7 }),
            { period: 5 },
          ),
          { period: 5 },
        ),
        { period: 4 },
      ),
      { fastPeriod: 3, slowPeriod: 8 },
    );
    const last = fluent.events.at(-1)!.data() as Record<string, unknown>;
    for (const c of [
      'priceOsc',
      'disparity',
      'dpo',
      'elderBull',
      'elderBear',
      'ao',
    ]) {
      expect(typeof last[c], c).toBe('number');
      expect(col(fluent, c), c).toEqual(col(functional, c));
    }
  });

  it('passes `mode` and `maType` through, not just the periods', () => {
    // A mount that dropped the options object entirely would still produce
    // numbers; these two knobs are the ones that change the answer.
    const pct = oscBars().priceOscillator({ fastPeriod: 3, slowPeriod: 7 });
    const abs = oscBars().priceOscillator({
      fastPeriod: 3,
      slowPeriod: 7,
      mode: 'absolute',
      output: 'abs',
    });
    expect(col(pct, 'priceOsc')[20]).not.toBeCloseTo(col(abs, 'abs')[20]!, 6);
    const smaD = oscBars().disparityIndex({ period: 5, maType: 'sma' });
    const emaD = oscBars().disparityIndex({
      period: 5,
      maType: 'ema',
      output: 'd2',
    });
    expect(col(smaD, 'disparity')[20]).not.toBeCloseTo(col(emaD, 'd2')[20]!, 6);
  });
});
