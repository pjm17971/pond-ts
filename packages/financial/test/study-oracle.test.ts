/**
 * Cross-validation: every shipped study is checked against a **pandas** oracle.
 *
 * `scripts/oracle/generate.py` computes reference values with pandas (an
 * independent implementation, conventions pinned to match ours — see that file)
 * and commits them to `fixtures/study-oracle.json`. Here we run our TypeScript
 * studies over the *same* input and assert bar-for-bar agreement. CI needs no
 * Python — the JSON is the committed oracle; regenerate it only when a study's
 * definition changes (and expect the diff to be reviewed).
 */
import { readFileSync } from 'node:fs';
import { describe, it, expect } from 'vitest';
import { TimeSeries } from 'pond-ts';
import type { MaType } from '../src/index.js';
import {
  sma,
  ema,
  movingAverage,
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
  momentum,
  historicalVolatility,
  stochastic,
  williamsR,
  donchian,
  obv,
  vwap,
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
  accumulativeSwingIndex,
  randomWalkIndex,
  ravi,
  trendIntensityIndex,
  specialK,
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
} from '../src/index.js';
import type { PivotMethod, PriceOscillatorMode } from '../src/index.js';

interface OracleCase {
  study: string;
  params: {
    period?: number;
    stdDev?: number;
    q?: number;
    percent?: number;
    periods?: number;
    type?: MaType;
    maType?: MaType;
    fastPeriod?: number;
    slowPeriod?: number;
    signalPeriod?: number;
    annualize?: number;
    kPeriod?: number;
    slowing?: number;
    dPeriod?: number;
    atrPeriod?: number;
    multiplier?: number;
    longPeriod?: number;
    shortPeriod?: number;
    wmaPeriod?: number;
    mode?: PriceOscillatorMode;
    mediumPeriod?: number;
    rocPeriod?: number;
    emaPeriod?: number;
    sumPeriod?: number;
    stdevPeriod?: number;
    lookback?: number;
    rsiPeriod?: number;
    stochPeriod?: number;
    benchmark?: string;
    step?: number;
    maxStep?: number;
    maPeriod?: number;
    conversionPeriod?: number;
    basePeriod?: number;
    spanBPeriod?: number;
    displacement?: number;
    deviation?: number;
    high?: string;
    low?: string;
    cyclePeriod?: number;
    limit?: number;
    minTick?: number;
    anchor?: number;
    method?: PivotMethod;
  };
  /** Which input the case was generated over. Absent means the 80-bar
   *  OHLCV fixture; `'long'` means the 900-bar close-only one, which the
   *  three studies whose warm-up does not fit in 80 bars need (Special K
   *  reaches bar 724) and which two more use because the short fixture
   *  cannot exercise their reading (see the generator); `'session'` means
   *  the SAME 80 bars keyed onto a real trading-session clock, which the two
   *  session-anchored studies need (80 milliseconds is not a trading week). */
  input?: 'long' | 'session';
  expected: Record<string, Array<number | null>>;
}
interface Oracle {
  meta: { oracle: string };
  input: {
    closes: number[];
    longCloses: number[];
    opens: number[];
    highs: number[];
    lows: number[];
    volumes: number[];
    benchmarks: number[];
    sessionTimes: number[];
  };
  cases: OracleCase[];
}

const oracle = JSON.parse(
  readFileSync(
    new URL('./fixtures/study-oracle.json', import.meta.url),
    'utf8',
  ),
) as Oracle;

/** Build the close series the oracle computed over (index as the time key). */
function series(): TimeSeries<never> {
  return new TimeSeries({
    name: 'oracle',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'close', kind: 'number' },
    ],
    rows: oracle.input.closes.map((c, i) => [i, c]),
  }) as unknown as TimeSeries<never>;
}

/** The 900-bar close-only series the long cases were generated over.
 *  Separate from `closes` so that adding it did not recompute every existing
 *  case; see the generator's note. */
function longSeries(): TimeSeries<never> {
  return new TimeSeries({
    name: 'oracle',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'close', kind: 'number' },
    ],
    rows: oracle.input.longCloses.map((c, i) => [i, c]),
  }) as unknown as TimeSeries<never>;
}

/** The same bars with open/high/low/volume, for the studies that read a
 *  whole bar. Kept separate so the close-only cases stay on exactly the
 *  series they were generated against. The volume and open columns ride
 *  along on every bar study — an extra column is invisible to one that does
 *  not name it. */
function ohlcSeries(): TimeSeries<never> {
  return new TimeSeries({
    name: 'oracle',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'open', kind: 'number' },
      { name: 'high', kind: 'number' },
      { name: 'low', kind: 'number' },
      { name: 'close', kind: 'number' },
      { name: 'volume', kind: 'number' },
    ],
    rows: oracle.input.closes.map((c, i) => [
      i,
      oracle.input.opens[i]!,
      oracle.input.highs[i]!,
      oracle.input.lows[i]!,
      c,
      oracle.input.volumes[i]!,
    ]),
  }) as unknown as TimeSeries<never>;
}

/** The closes with the oracle's modelled BENCHMARK column beside them — the
 *  already-joined wide series the two-series family reads. In real use the
 *  benchmark arrives via `align` + `TimeSeries.joinMany`; the studies only
 *  ever see the result, which is the whole point of naming the comparison
 *  series with a column rather than a second `TimeSeries`. */
function benchmarkSeries(): TimeSeries<never> {
  return new TimeSeries({
    name: 'oracle',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'close', kind: 'number' },
      { name: 'bench', kind: 'number' },
    ],
    rows: oracle.input.closes.map((c, i) => [
      i,
      c,
      oracle.input.benchmarks[i]!,
    ]),
  }) as unknown as TimeSeries<never>;
}

/** The same 80 OHLCV bars keyed onto the generator's trading-session clock —
 *  30-minute bars on a 09:30–16:00 America/New_York grid over six sessions,
 *  plus the two bars that fall in no session. Only the two session-anchored
 *  studies read it. */
function sessionSeries(): TimeSeries<never> {
  return new TimeSeries({
    name: 'oracle',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'open', kind: 'number' },
      { name: 'high', kind: 'number' },
      { name: 'low', kind: 'number' },
      { name: 'close', kind: 'number' },
      { name: 'volume', kind: 'number' },
    ],
    rows: oracle.input.closes.map((c, i) => [
      oracle.input.sessionTimes[i]!,
      oracle.input.opens[i]!,
      oracle.input.highs[i]!,
      oracle.input.lows[i]!,
      c,
      oracle.input.volumes[i]!,
    ]),
  }) as unknown as TimeSeries<never>;
}

/** The calendar the generator laid the session clock out on, rebuilt from the
 *  same rules rather than from a table in the fixture — so a disagreement
 *  between our Temporal session generation and Python's `zoneinfo` fails these
 *  cases instead of hiding behind a shared input. */
function sessionCalendar(): TradingCalendar {
  return TradingCalendar.fromSessions(
    generateSessions(
      {
        timeZone: 'America/New_York',
        open: '09:30',
        close: '16:00',
        holidays: ['2024-01-15'],
      },
      { from: '2024-01-08', to: '2024-01-16' },
    ),
  );
}

function run(c: OracleCase): unknown {
  const p = c.params;
  // The close-only door: the 900-bar input for a case marked `'long'`, the
  // 80-bar one otherwise. Only the close-only studies have long cases.
  const source = () => (c.input === 'long' ? longSeries() : series());
  switch (c.study) {
    case 'sma':
      return sma(series(), p as { period: number });
    case 'ema':
      return ema(series(), p as { period: number });
    case 'movingAverage':
      return movingAverage(series(), p as { period: number; type?: MaType });
    case 'bollinger':
      return bollinger(series(), p as { period: number; stdDev?: number });
    case 'rollingStdev':
      return rollingStdev(series(), p as { period: number });
    case 'rollingMin':
      return rollingMin(series(), p as { period: number });
    case 'rollingMax':
      return rollingMax(series(), p as { period: number });
    case 'rollingPercentile':
      return rollingPercentile(series(), p as { period: number; q: number });
    case 'zScore':
      return zScore(series(), p as { period: number });
    case 'envelope':
      return envelope(
        series(),
        p as { period: number; percent?: number; maType?: MaType },
      );
    case 'percentChange':
      return percentChange(series(), p as { periods?: number });
    case 'rsi':
      return rsi(series(), p as { period?: number });
    case 'atr':
      return atr(ohlcSeries(), p as { period?: number });
    case 'obv':
      return obv(ohlcSeries());
    case 'vwap':
      return vwap(ohlcSeries(), p as { period: number });
    case 'macd':
      return macd(
        series(),
        p as {
          fastPeriod?: number;
          slowPeriod?: number;
          signalPeriod?: number;
        },
      );
    case 'momentum':
      return momentum(series(), p as { period?: number });
    case 'historicalVolatility':
      return historicalVolatility(
        series(),
        p as { period?: number; annualize?: number },
      );
    case 'stochastic':
      return stochastic(
        ohlcSeries(),
        p as { kPeriod?: number; slowing?: number; dPeriod?: number },
      );
    case 'williamsR':
      return williamsR(ohlcSeries(), p as { period?: number });
    case 'donchian':
      return donchian(ohlcSeries(), p as { period?: number });
    case 'keltner':
      return keltner(
        ohlcSeries(),
        p as {
          period?: number;
          atrPeriod?: number;
          multiplier?: number;
          maType?: MaType;
        },
      );
    case 'atrBands':
      return atrBands(
        ohlcSeries(),
        p as { period?: number; multiplier?: number },
      );
    case 'qstick':
      return qstick(ohlcSeries(), p as { period?: number; maType?: MaType });
    case 'trix':
      return trix(series(), p as { period?: number; signalPeriod?: number });
    case 'coppock':
      return coppock(
        series(),
        p as { longPeriod?: number; shortPeriod?: number; wmaPeriod?: number },
      );
    case 'priceOscillator':
      return priceOscillator(
        series(),
        p as {
          fastPeriod?: number;
          slowPeriod?: number;
          maType?: MaType;
          mode?: PriceOscillatorMode;
        },
      );
    case 'disparityIndex':
      return disparityIndex(
        series(),
        p as { period?: number; maType?: MaType },
      );
    case 'detrendedPriceOscillator':
      return detrendedPriceOscillator(
        series(),
        p as { period?: number; maType?: MaType },
      );
    case 'elderRay':
      return elderRay(ohlcSeries(), p as { period?: number });
    case 'awesomeOscillator':
      return awesomeOscillator(
        ohlcSeries(),
        p as { fastPeriod?: number; slowPeriod?: number },
      );
    case 'accumulationDistribution':
      return accumulationDistribution(ohlcSeries());
    case 'chaikinOscillator':
      return chaikinOscillator(
        ohlcSeries(),
        p as { fastPeriod?: number; slowPeriod?: number },
      );
    case 'priceVolumeTrend':
      return priceVolumeTrend(ohlcSeries());
    case 'chaikinMoneyFlow':
      return chaikinMoneyFlow(ohlcSeries(), p as { period?: number });
    case 'moneyFlowIndex':
      return moneyFlowIndex(ohlcSeries(), p as { period?: number });
    case 'forceIndex':
      return forceIndex(ohlcSeries(), p as { period?: number });
    case 'easeOfMovement':
      return easeOfMovement(
        ohlcSeries(),
        p as { period?: number; maType?: MaType },
      );
    case 'volumeOscillator':
      return volumeOscillator(
        ohlcSeries(),
        p as { fastPeriod?: number; slowPeriod?: number; maType?: MaType },
      );
    case 'chandeMomentum':
      return chandeMomentum(series(), p as { period?: number });
    case 'ultimateOscillator':
      return ultimateOscillator(
        ohlcSeries(),
        p as {
          shortPeriod?: number;
          mediumPeriod?: number;
          longPeriod?: number;
        },
      );
    case 'commodityChannelIndex':
      return commodityChannelIndex(ohlcSeries(), p as { period?: number });
    case 'intradayMomentumIndex':
      return intradayMomentumIndex(ohlcSeries(), p as { period?: number });
    case 'relativeVigorIndex':
      return relativeVigorIndex(ohlcSeries(), p as { period?: number });
    case 'psychologicalLine':
      return psychologicalLine(series(), p as { period?: number });
    case 'chaikinVolatility':
      return chaikinVolatility(
        ohlcSeries(),
        p as { period?: number; rocPeriod?: number },
      );
    case 'massIndex':
      return massIndex(
        ohlcSeries(),
        p as { emaPeriod?: number; sumPeriod?: number },
      );
    case 'choppinessIndex':
      return choppinessIndex(ohlcSeries(), p as { period?: number });
    case 'ulcerIndex':
      return ulcerIndex(series(), p as { period?: number });
    case 'verticalHorizontalFilter':
      return verticalHorizontalFilter(series(), p as { period?: number });
    case 'gopalakrishnanRangeIndex':
      return gopalakrishnanRangeIndex(ohlcSeries(), p as { period?: number });
    case 'relativeVolatilityIndex':
      return relativeVolatilityIndex(
        series(),
        p as { period?: number; stdevPeriod?: number },
      );
    case 'directionalMovement':
      return directionalMovement(ohlcSeries(), p as { period?: number });
    case 'aroon':
      return aroon(ohlcSeries(), p as { period?: number });
    case 'vortex':
      return vortex(ohlcSeries(), p as { period?: number });
    case 'correlation':
      return correlation(
        benchmarkSeries(),
        p as { period?: number; benchmark: never },
      );
    case 'beta':
      return beta(
        benchmarkSeries(),
        p as { period?: number; benchmark: never },
      );
    case 'priceRelative':
      return priceRelative(benchmarkSeries(), p as { benchmark: never });
    case 'performanceIndex':
      return performanceIndex(
        benchmarkSeries(),
        p as { period?: number; benchmark: never },
      );
    case 'linearRegression':
      return linearRegression(series(), p as { period?: number });
    case 'timeSeriesForecast':
      return timeSeriesForecast(series(), p as { period?: number });
    case 'chandeForecastOscillator':
      return chandeForecastOscillator(series(), p as { period?: number });
    case 'guppy':
      return guppy(series(), p as { type?: MaType });
    case 'rainbow':
      return rainbow(series(), p as { period?: number; type?: MaType });
    case 'movingAverageDeviation':
      return movingAverageDeviation(
        series(),
        p as { period?: number; maType?: MaType },
      );
    case 'trueStrengthIndex':
      return trueStrengthIndex(
        series(),
        p as {
          longPeriod?: number;
          shortPeriod?: number;
          signalPeriod?: number;
        },
      );
    case 'stochasticRsi':
      return stochasticRsi(
        series(),
        p as {
          rsiPeriod?: number;
          stochPeriod?: number;
          kPeriod?: number;
          dPeriod?: number;
        },
      );
    case 'priceMomentumOscillator':
      return priceMomentumOscillator(series(), p as Record<string, never>);
    case 'typicalPrice':
      return typicalPrice(ohlcSeries(), p as Record<string, never>);
    case 'medianPrice':
      return medianPrice(ohlcSeries(), p as Record<string, never>);
    case 'weightedClose':
      return weightedClose(ohlcSeries(), p as Record<string, never>);
    case 'averagePrice':
      return averagePrice(ohlcSeries(), p as Record<string, never>);
    case 'balanceOfPower':
      return balanceOfPower(
        ohlcSeries(),
        p as { period?: number; maType?: MaType },
      );
    case 'starcBands':
      return starcBands(
        ohlcSeries(),
        p as {
          period?: number;
          atrPeriod?: number;
          multiplier?: number;
          maType?: MaType;
        },
      );
    case 'highLowBands':
      return highLowBands(
        ohlcSeries(),
        p as { period?: number; percent?: number; maType?: MaType },
      );
    case 'bollingerBandwidth':
      return bollingerBandwidth(
        series(),
        p as { period?: number; stdDev?: number },
      );
    case 'bollingerPercentB':
      return bollingerPercentB(
        series(),
        p as { period?: number; stdDev?: number },
      );
    case 'primeNumberBands':
      return primeNumberBands(ohlcSeries(), p as Record<string, never>);
    case 'primeNumberOscillator':
      return primeNumberOscillator(series(), p as Record<string, never>);
    case 'marketFacilitationIndex':
      return marketFacilitationIndex(ohlcSeries(), p as Record<string, never>);
    case 'kst':
      return kst(series(), p as { signalPeriod?: number });
    case 'rainbowOscillator':
      return rainbowOscillator(
        series(),
        p as { period?: number; lookback?: number; type?: MaType },
      );
    case 'centerOfGravity':
      return centerOfGravity(series(), p as { period?: number });
    case 'parabolicSar':
      return parabolicSar(
        ohlcSeries(),
        p as { step?: number; maxStep?: number },
      );
    case 'superTrend':
      return superTrend(
        ohlcSeries(),
        p as { period?: number; multiplier?: number },
      );
    case 'atrTrailingStop':
      return atrTrailingStop(
        ohlcSeries(),
        p as { period?: number; multiplier?: number },
      );
    case 'negativeVolumeIndex':
      return negativeVolumeIndex(ohlcSeries(), p as Record<string, never>);
    case 'positiveVolumeIndex':
      return positiveVolumeIndex(ohlcSeries(), p as Record<string, never>);
    case 'ichimoku':
      return ichimoku(
        ohlcSeries(),
        p as {
          conversionPeriod?: number;
          basePeriod?: number;
          spanBPeriod?: number;
          displacement?: number;
        },
      );
    case 'zigZag':
      // The long cases are close-only, so they name `high` and `low` as the
      // close — which is the documented close-based recipe, not a mode.
      return zigZag(
        c.input === 'long' ? longSeries() : ohlcSeries(),
        p as { deviation?: number; high?: never; low?: never },
      );
    case 'klinger':
      return klinger(
        ohlcSeries(),
        p as {
          fastPeriod?: number;
          slowPeriod?: number;
          signalPeriod?: number;
        },
      );
    case 'stochasticMomentumIndex':
      return stochasticMomentumIndex(
        ohlcSeries(),
        p as {
          period?: number;
          longPeriod?: number;
          shortPeriod?: number;
          signalPeriod?: number;
        },
      );
    case 'fisherTransform':
      return fisherTransform(ohlcSeries(), p as { period?: number });
    case 'schaffTrendCycle':
      return schaffTrendCycle(
        source(),
        p as {
          fastPeriod?: number;
          slowPeriod?: number;
          cyclePeriod?: number;
        },
      );
    case 'prettyGoodOscillator':
      return prettyGoodOscillator(ohlcSeries(), p as { period?: number });
    case 'swingIndex':
      return swingIndex(ohlcSeries(), p as { limit: number });
    case 'accumulativeSwingIndex':
      return accumulativeSwingIndex(ohlcSeries(), p as { limit: number });
    case 'randomWalkIndex':
      return randomWalkIndex(ohlcSeries(), p as { period?: number });
    case 'ravi':
      return ravi(source(), p as { shortPeriod?: number; longPeriod?: number });
    case 'trendIntensityIndex':
      return trendIntensityIndex(
        source(),
        p as { period?: number; maPeriod?: number },
      );
    case 'specialK':
      return specialK(source(), p as Record<string, never>);
    case 'twiggsMoneyFlow':
      return twiggsMoneyFlow(ohlcSeries(), p as { period?: number });
    case 'tradeVolumeIndex':
      return tradeVolumeIndex(ohlcSeries(), p as { minTick: number });
    case 'shinoharaIntensityRatio':
      return shinoharaIntensityRatio(ohlcSeries(), p as { period?: number });
    case 'anchoredVwap':
      return anchoredVwap(ohlcSeries(), p as { anchor: number });
    case 'sessionVwap':
      return sessionVwap(sessionSeries(), { sessions: sessionCalendar() });
    case 'pivotPoints':
      return pivotPoints(sessionSeries(), {
        sessions: sessionCalendar(),
        ...(p as { method?: PivotMethod }),
      });
    case 'movingAverageCross':
      return movingAverageCross(
        source(),
        p as { fastPeriod?: number; slowPeriod?: number; maType?: MaType },
      );
    case 'elderImpulse':
      return elderImpulse(
        series(),
        p as {
          emaPeriod?: number;
          fastPeriod?: number;
          slowPeriod?: number;
          signalPeriod?: number;
        },
      );
    default:
      // A fixture case whose study has no dispatch here must fail loudly, not
      // silently skip — the guard for future fan-out studies.
      throw new Error(`no dispatch for oracle study '${String(c.study)}'`);
  }
}

/** Read an appended column as (number | null)[] — null for a missing cell, so
 *  it lines up with the oracle's JSON `null`. */
function colValues(result: unknown, name: string): Array<number | null> {
  const events = (
    result as { events: ReadonlyArray<{ data(): Record<string, unknown> }> }
  ).events;
  return events.map((e) => {
    const v = e.data()[name];
    return typeof v === 'number' ? v : null;
  });
}

describe(`studies match the ${oracle.meta.oracle} oracle`, () => {
  for (const c of oracle.cases) {
    const label = `${c.study}(${JSON.stringify(c.params)})`;
    it(`${label} agrees bar-for-bar`, () => {
      const result = run(c);
      for (const [column, expected] of Object.entries(c.expected)) {
        const actual = colValues(result, column);
        expect(actual).toHaveLength(expected.length);
        for (let i = 0; i < expected.length; i += 1) {
          const exp = expected[i]!;
          if (exp === null) {
            expect(actual[i], `${column}[${i}] should be missing`).toBeNull();
          } else {
            // pandas + our incremental reducer are both IEEE754 doubles but sum
            // in a different order; 1e-9 absolute is comfortably inside that.
            expect(actual[i], `${column}[${i}]`).toBeCloseTo(exp, 9);
          }
        }
      }
    });
  }
});
