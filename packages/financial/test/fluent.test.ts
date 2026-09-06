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

describe('fluent volume & money-flow studies', () => {
  const flowBars = () =>
    new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'high', kind: 'number' },
        { name: 'low', kind: 'number' },
        { name: 'close', kind: 'number' },
        { name: 'volume', kind: 'number' },
      ] as const,
      rows: Array.from({ length: 30 }, (_, i) => {
        const c = 100 + 6 * Math.sin(i / 3) + 0.2 * i;
        return [
          i,
          c + 0.4 + 0.6 * Math.abs(Math.sin(i / 2)),
          c - 0.4 - 0.6 * Math.abs(Math.cos(i / 2.4)),
          c,
          1000 + 500 * Math.sin(i / 2.3) + (i % 7 === 3 ? 6000 : 0),
        ];
      }) as Array<[number, number, number, number, number]>,
    });

  it('chains all eight and matches the standalone functions bar for bar', () => {
    const fluent = flowBars()
      .accumulationDistribution()
      .chaikinOscillator({ fastPeriod: 3, slowPeriod: 8 })
      .priceVolumeTrend()
      .chaikinMoneyFlow({ period: 5 })
      .moneyFlowIndex({ period: 6 })
      .forceIndex({ period: 4 })
      .easeOfMovement({ period: 5 })
      .volumeOscillator({ fastPeriod: 3, slowPeriod: 7 });
    const functional = volumeOscillator(
      easeOfMovement(
        forceIndex(
          moneyFlowIndex(
            chaikinMoneyFlow(
              priceVolumeTrend(
                chaikinOscillator(accumulationDistribution(flowBars()), {
                  fastPeriod: 3,
                  slowPeriod: 8,
                }),
              ),
              { period: 5 },
            ),
            { period: 6 },
          ),
          { period: 4 },
        ),
        { period: 5 },
      ),
      { fastPeriod: 3, slowPeriod: 7 },
    );
    const last = fluent.events.at(-1)!.data() as Record<string, unknown>;
    for (const c of [
      'ad',
      'chaikinOsc',
      'pvt',
      'cmf',
      'mfi',
      'force',
      'eom',
      'volOsc',
    ]) {
      expect(typeof last[c], c).toBe('number');
      expect(col(fluent, c), c).toEqual(col(functional, c));
    }
  });

  it('passes the options through, not just the periods', () => {
    // A mount that dropped the options object would still produce numbers;
    // `maType` and `scale` are knobs that change the answer.
    const bySma = flowBars().easeOfMovement({ period: 4, maType: 'sma' });
    const byEma = flowBars().easeOfMovement({
      period: 4,
      maType: 'ema',
      output: 'e2',
    });
    expect(col(bySma, 'eom')[29]).not.toBeCloseTo(col(byEma, 'e2')[29]!, 6);
    const scaled = flowBars().easeOfMovement({ period: 4, scale: 1 });
    expect(col(scaled, 'eom')[29]).toBeCloseTo(
      col(bySma, 'eom')[29]! / 100_000_000,
      12,
    );
  });
});

describe('fluent momentum tail', () => {
  const momBars = () =>
    new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'open', kind: 'number' },
        { name: 'high', kind: 'number' },
        { name: 'low', kind: 'number' },
        { name: 'close', kind: 'number' },
      ] as const,
      rows: Array.from({ length: 30 }, (_, i) => {
        const c = 100 + 6 * Math.sin(i / 3) + 0.2 * i;
        const o = c - 0.8 * Math.cos(i / 2.1);
        return [
          i,
          o,
          Math.max(o, c) + 0.4 + 0.6 * Math.abs(Math.sin(i / 2)),
          Math.min(o, c) - 0.4 - 0.6 * Math.abs(Math.cos(i / 2.4)),
          c,
        ];
      }) as Array<[number, number, number, number, number]>,
    });

  it('chains all six and matches the standalone functions bar for bar', () => {
    const fluent = momBars()
      .chandeMomentum({ period: 5 })
      .ultimateOscillator({ shortPeriod: 3, mediumPeriod: 5, longPeriod: 9 })
      .commodityChannelIndex({ period: 5 })
      .intradayMomentumIndex({ period: 5 })
      .relativeVigorIndex({ period: 4 })
      .psychologicalLine({ period: 5 });
    const functional = psychologicalLine(
      relativeVigorIndex(
        intradayMomentumIndex(
          commodityChannelIndex(
            ultimateOscillator(chandeMomentum(momBars(), { period: 5 }), {
              shortPeriod: 3,
              mediumPeriod: 5,
              longPeriod: 9,
            }),
            { period: 5 },
          ),
          { period: 5 },
        ),
        { period: 4 },
      ),
      { period: 5 },
    );
    const last = fluent.events.at(-1)!.data() as Record<string, unknown>;
    for (const c of ['cmo', 'uo', 'cci', 'imi', 'rvi', 'rviSignal', 'psy']) {
      expect(typeof last[c], c).toBe('number');
      expect(col(fluent, c), c).toEqual(col(functional, c));
    }
  });

  it('passes the periods through, not just the defaults', () => {
    // A mount that dropped the options object would still produce numbers;
    // the period is the knob that changes the answer.
    const short = momBars().chandeMomentum({ period: 3 });
    const long = momBars().chandeMomentum({ period: 9, output: 'cmo9' });
    expect(col(short, 'cmo')[20]).not.toBeCloseTo(col(long, 'cmo9')[20]!, 6);
    // …and the positional weights: a different short period must move UO.
    const fast = momBars().ultimateOscillator({
      shortPeriod: 2,
      mediumPeriod: 5,
      longPeriod: 9,
    });
    const slow = momBars().ultimateOscillator({
      shortPeriod: 4,
      mediumPeriod: 5,
      longPeriod: 9,
      output: 'uo2',
    });
    expect(col(fast, 'uo')[25]).not.toBeCloseTo(col(slow, 'uo2')[25]!, 6);
  });
});

describe('fluent: the Wilder directional group', () => {
  const dirBars = () =>
    new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'high', kind: 'number' },
        { name: 'low', kind: 'number' },
        { name: 'close', kind: 'number' },
      ] as const,
      rows: Array.from({ length: 60 }, (_, i) => {
        const c = 100 + 6 * Math.sin(i / 3) + 0.2 * i;
        return [
          i,
          c + 0.4 + 0.6 * Math.abs(Math.sin(i / 2)),
          c - 0.4 - 0.6 * Math.abs(Math.cos(i / 2.4)),
          c,
        ];
      }) as Array<[number, number, number, number]>,
    });

  it('chains all three and matches the standalone functions bar for bar', () => {
    const fluent = dirBars()
      .directionalMovement({ period: 6 })
      .aroon({ period: 10 })
      .vortex({ period: 6 });
    const functional = vortex(
      aroon(directionalMovement(dirBars(), { period: 6 }), { period: 10 }),
      { period: 6 },
    );
    const last = fluent.events.at(-1)!.data() as Record<string, unknown>;
    for (const c of [
      'dmiPlusDi',
      'dmiMinusDi',
      'dmiDx',
      'dmiAdx',
      'dmiAdxr',
      'aroonUp',
      'aroonDown',
      'aroonOsc',
      'viPlus',
      'viMinus',
    ]) {
      expect(typeof last[c], c).toBe('number');
      expect(col(fluent, c), c).toEqual(col(functional, c));
    }
  });

  it('passes the periods through, not just the defaults', () => {
    // A mount that dropped the options object would still produce numbers;
    // the period is the knob that changes the answer.
    const short = dirBars().directionalMovement({ period: 4 });
    const long = dirBars().directionalMovement({ period: 12, prefix: 'dm12' });
    expect(col(short, 'dmiAdx')[50]).not.toBeCloseTo(
      col(long, 'dm12Adx')[50]!,
      6,
    );
    const fastAroon = dirBars().aroon({ period: 4 });
    const slowAroon = dirBars().aroon({ period: 20, prefix: 'ar20' });
    expect(col(fastAroon, 'aroonUp')[50]).not.toBe(
      col(slowAroon, 'ar20Up')[50],
    );
    const fastVi = dirBars().vortex({ period: 4 });
    const slowVi = dirBars().vortex({ period: 20, prefix: 'vi20' });
    expect(col(fastVi, 'viPlus')[50]).not.toBeCloseTo(
      col(slowVi, 'vi20Plus')[50]!,
      6,
    );
  });
});

describe('fluent volatility tail', () => {
  const volBars = () =>
    new TimeSeries({
      name: 'bars',
      schema: [
        { name: 'time', kind: 'time' },
        { name: 'high', kind: 'number' },
        { name: 'low', kind: 'number' },
        { name: 'close', kind: 'number' },
      ] as const,
      rows: Array.from({ length: 40 }, (_, i) => {
        const c = 100 + 7 * Math.sin(i / 4.1) + 0.25 * i;
        return [
          i,
          c + 0.4 + 0.9 * Math.abs(Math.sin(i / 2.9)),
          c - 0.4 - 0.9 * Math.abs(Math.cos(i / 2.2)),
          c,
        ];
      }) as Array<[number, number, number, number]>,
    });

  it('chains all seven and matches the standalone functions bar for bar', () => {
    const fluent = volBars()
      .chaikinVolatility({ period: 4, rocPeriod: 3 })
      .massIndex({ emaPeriod: 4, sumPeriod: 6 })
      .choppinessIndex({ period: 5 })
      .ulcerIndex({ period: 5 })
      .verticalHorizontalFilter({ period: 6 })
      .gopalakrishnanRangeIndex({ period: 5 })
      .relativeVolatilityIndex({ period: 4, stdevPeriod: 3 });
    const functional = relativeVolatilityIndex(
      gopalakrishnanRangeIndex(
        verticalHorizontalFilter(
          ulcerIndex(
            choppinessIndex(
              massIndex(
                chaikinVolatility(volBars(), { period: 4, rocPeriod: 3 }),
                { emaPeriod: 4, sumPeriod: 6 },
              ),
              { period: 5 },
            ),
            { period: 5 },
          ),
          { period: 6 },
        ),
        { period: 5 },
      ),
      { period: 4, stdevPeriod: 3 },
    );
    const last = fluent.events.at(-1)!.data() as Record<string, unknown>;
    for (const c of [
      'chaikinVol',
      'mass',
      'chop',
      'ulcer',
      'vhf',
      'gapo',
      'relVol',
    ]) {
      expect(typeof last[c], c).toBe('number');
      expect(col(fluent, c), c).toEqual(col(functional, c));
    }
  });

  it('passes the periods through, not just the defaults', () => {
    // A mount that dropped the options object would still produce numbers;
    // the periods are the knobs that change the answers.
    const shortChop = volBars().choppinessIndex({ period: 5 });
    const longChop = volBars().choppinessIndex({
      period: 20,
      output: 'chop20',
    });
    expect(col(shortChop, 'chop')[30]).not.toBeCloseTo(
      col(longChop, 'chop20')[30]!,
      6,
    );
    // Both of `chaikinVolatility`'s periods must matter independently.
    const base = volBars().chaikinVolatility({ period: 4, rocPeriod: 3 });
    const slower = volBars().chaikinVolatility({
      period: 9,
      rocPeriod: 3,
      output: 'cv2',
    });
    const further = volBars().chaikinVolatility({
      period: 4,
      rocPeriod: 8,
      output: 'cv3',
    });
    expect(col(base, 'chaikinVol')[30]).not.toBeCloseTo(
      col(slower, 'cv2')[30]!,
      6,
    );
    expect(col(base, 'chaikinVol')[30]).not.toBeCloseTo(
      col(further, 'cv3')[30]!,
      6,
    );
    // …and `relativeVolatilityIndex`'s two lengths likewise.
    const rv = volBars().relativeVolatilityIndex({
      period: 4,
      stdevPeriod: 3,
    });
    const rv2 = volBars().relativeVolatilityIndex({
      period: 10,
      stdevPeriod: 3,
      output: 'relVol2',
    });
    expect(col(rv, 'relVol')[35]).not.toBeCloseTo(col(rv2, 'relVol2')[35]!, 6);
  });
});
