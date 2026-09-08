import type { SeriesSchema } from 'pond-ts';
import type { StudyDescriptor } from './types.js';
import { defineStudy } from './define.js';
import { MA_TYPES } from '../kernels/moving-average.js';
import { rsi } from '../studies/rsi.js';
import type { RsiOptions } from '../studies/rsi.js';
import { stochastic } from '../studies/stochastic.js';
import type { StochasticOptions } from '../studies/stochastic.js';
import { stochasticMomentumIndex } from '../studies/stochastic-momentum-index.js';
import type { StochasticMomentumIndexOptions } from '../studies/stochastic-momentum-index.js';
import { stochasticRsi } from '../studies/stochastic-rsi.js';
import type { StochasticRsiOptions } from '../studies/stochastic-rsi.js';
import { williamsR } from '../studies/williams-r.js';
import type { WilliamsROptions } from '../studies/williams-r.js';
import { chandeMomentum } from '../studies/chande-momentum.js';
import type { ChandeMomentumOptions } from '../studies/chande-momentum.js';
import { momentum } from '../studies/momentum.js';
import type { MomentumOptions } from '../studies/momentum.js';
import { percentChange } from '../studies/percent-change.js';
import type { PercentChangeOptions } from '../studies/percent-change.js';
import { intradayMomentumIndex } from '../studies/intraday-momentum-index.js';
import type { IntradayMomentumIndexOptions } from '../studies/intraday-momentum-index.js';
import { awesomeOscillator } from '../studies/awesome-oscillator.js';
import type { AwesomeOscillatorOptions } from '../studies/awesome-oscillator.js';
import { ultimateOscillator } from '../studies/ultimate-oscillator.js';
import type { UltimateOscillatorOptions } from '../studies/ultimate-oscillator.js';
import { commodityChannelIndex } from '../studies/commodity-channel-index.js';
import type { CommodityChannelIndexOptions } from '../studies/commodity-channel-index.js';
import { fisherTransform } from '../studies/fisher-transform.js';
import type { FisherTransformOptions } from '../studies/fisher-transform.js';
import { psychologicalLine } from '../studies/psychological-line.js';
import type { PsychologicalLineOptions } from '../studies/psychological-line.js';
import { prettyGoodOscillator } from '../studies/pretty-good-oscillator.js';
import type { PrettyGoodOscillatorOptions } from '../studies/pretty-good-oscillator.js';
import { primeNumberOscillator } from '../studies/prime-number.js';
import type { PrimeNumberOscillatorOptions } from '../studies/prime-number.js';
import { qstick } from '../studies/qstick.js';
import type { QstickOptions } from '../studies/qstick.js';
import { relativeVigorIndex } from '../studies/relative-vigor-index.js';
import type { RelativeVigorIndexOptions } from '../studies/relative-vigor-index.js';
import { centerOfGravity } from '../studies/center-of-gravity.js';
import type { CenterOfGravityOptions } from '../studies/center-of-gravity.js';
import { chandeForecastOscillator } from '../studies/chande-forecast-oscillator.js';
import type { ChandeForecastOscillatorOptions } from '../studies/chande-forecast-oscillator.js';
import { trueStrengthIndex } from '../studies/true-strength-index.js';
import type { TrueStrengthIndexOptions } from '../studies/true-strength-index.js';

/** The `'momentum'` family — see `types.ts` for the family list. */
export const MOMENTUM_STUDIES: readonly StudyDescriptor[] = [
  defineStudy<RsiOptions<SeriesSchema, string>>({
    name: 'rsi',
    family: 'momentum',
    summary: "Wilder's relative strength index, bounded 0..100",
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', default: 14, min: 1, suggest: [5, 30] },
    },
    naming: { output: 'rsi' },
    outputs: [{ id: '', unit: 'percent' }],
    run: rsi,
  }),
  defineStudy<StochasticOptions<SeriesSchema, string>>({
    name: 'stochastic',
    family: 'momentum',
    summary: "Lane's stochastic — where the close sits in its recent range",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      kPeriod: { kind: 'integer', default: 14, min: 1, suggest: [5, 30] },
      // `1` is the *fast* stochastic (%K unsmoothed), so the useful range
      // starts at the minimum rather than above it.
      slowing: { kind: 'integer', default: 3, min: 1, suggest: [1, 10] },
      dPeriod: { kind: 'integer', default: 3, min: 1, suggest: [1, 10] },
    },
    naming: { prefix: 'stoch' },
    outputs: [
      { id: 'K', unit: 'percent' },
      { id: 'D', unit: 'percent' },
    ],
    run: stochastic,
  }),
  defineStudy<StochasticMomentumIndexOptions<SeriesSchema, string>>({
    name: 'stochasticMomentumIndex',
    family: 'momentum',
    summary: "Blau's SMI — the close against the range midpoint, smoothed",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      period: { kind: 'integer', default: 13, min: 1, suggest: [5, 30] },
      longPeriod: { kind: 'integer', default: 25, min: 1, suggest: [3, 50] },
      shortPeriod: { kind: 'integer', default: 2, min: 1, suggest: [1, 10] },
      signalPeriod: { kind: 'integer', default: 3, min: 1, suggest: [1, 15] },
    },
    naming: { prefix: 'smi' },
    // The line is named `${prefix}` itself (the `trix` / `trueStrengthIndex`
    // shape), so its suffix is empty and only the signal carries one.
    outputs: [
      { id: '', unit: 'percent' },
      { id: 'Signal', unit: 'percent' },
    ],
    run: stochasticMomentumIndex,
  }),
  defineStudy<StochasticRsiOptions<SeriesSchema, string>>({
    name: 'stochasticRsi',
    family: 'momentum',
    summary: "Chande & Kroll's stochastic of the RSI's own range",
    inputs: { column: { default: 'close' } },
    params: {
      rsiPeriod: { kind: 'integer', default: 14, min: 1, suggest: [5, 30] },
      stochPeriod: { kind: 'integer', default: 14, min: 1, suggest: [5, 30] },
      // `1` leaves %K as the raw range position (the "fast" Stochastic RSI).
      kPeriod: { kind: 'integer', default: 3, min: 1, suggest: [1, 10] },
      dPeriod: { kind: 'integer', default: 3, min: 1, suggest: [1, 10] },
    },
    naming: { prefix: 'stochRsi' },
    outputs: [
      { id: 'K', unit: 'percent' },
      { id: 'D', unit: 'percent' },
    ],
    run: stochasticRsi,
  }),
  defineStudy<WilliamsROptions<SeriesSchema, string>>({
    name: 'williamsR',
    family: 'momentum',
    summary: 'Williams %R — the close below the recent high, −100..0',
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      period: { kind: 'integer', default: 14, min: 1, suggest: [5, 30] },
    },
    naming: { output: 'williamsR' },
    outputs: [{ id: '', unit: 'percent' }],
    run: williamsR,
  }),
  defineStudy<ChandeMomentumOptions<SeriesSchema, string>>({
    name: 'chandeMomentum',
    family: 'momentum',
    summary: "Chande's momentum oscillator — unsmoothed up/down sums",
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', default: 14, min: 1, suggest: [5, 30] },
    },
    naming: { output: 'cmo' },
    outputs: [{ id: '', unit: 'percent' }],
    run: chandeMomentum,
  }),
  defineStudy<MomentumOptions<SeriesSchema, string>>({
    name: 'momentum',
    family: 'momentum',
    summary: 'The change from n bars ago, in the price’s own units',
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', default: 10, min: 1, suggest: [1, 50] },
    },
    naming: { output: 'momentum' },
    outputs: [{ id: '', unit: 'delta' }],
    run: momentum,
  }),
  defineStudy<PercentChangeOptions<SeriesSchema, string>>({
    name: 'percentChange',
    family: 'momentum',
    summary: 'Percent change from n bars ago — the rate of change',
    inputs: { column: { default: 'close' } },
    params: {
      periods: { kind: 'integer', default: 1, min: 1, suggest: [1, 20] },
    },
    naming: { output: 'pctChange' },
    outputs: [{ id: '', unit: 'percent' }],
    run: percentChange,
  }),
  defineStudy<IntradayMomentumIndexOptions<SeriesSchema, string>>({
    name: 'intradayMomentumIndex',
    family: 'momentum',
    summary: "Chande's IMI — RSI's question asked of the candle body",
    inputs: { open: { default: 'open' }, close: { default: 'close' } },
    params: {
      period: { kind: 'integer', default: 14, min: 1, suggest: [5, 30] },
    },
    naming: { output: 'imi' },
    outputs: [{ id: '', unit: 'percent' }],
    run: intradayMomentumIndex,
  }),
  defineStudy<AwesomeOscillatorOptions<SeriesSchema, string>>({
    name: 'awesomeOscillator',
    family: 'momentum',
    summary: "Bill Williams' AO — fast minus slow SMA of the median price",
    inputs: { high: { default: 'high' }, low: { default: 'low' } },
    params: {
      fastPeriod: { kind: 'integer', default: 5, min: 1, suggest: [2, 20] },
      slowPeriod: { kind: 'integer', default: 34, min: 1, suggest: [10, 60] },
    },
    naming: { output: 'ao' },
    outputs: [{ id: '', unit: 'delta' }],
    run: awesomeOscillator,
  }),
  defineStudy<UltimateOscillatorOptions<SeriesSchema, string>>({
    name: 'ultimateOscillator',
    family: 'momentum',
    summary: "Williams' three-horizon buying pressure, weighted 4/2/1",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      shortPeriod: { kind: 'integer', default: 7, min: 1, suggest: [3, 15] },
      mediumPeriod: { kind: 'integer', default: 14, min: 1, suggest: [5, 30] },
      longPeriod: { kind: 'integer', default: 28, min: 1, suggest: [10, 60] },
    },
    naming: { output: 'uo' },
    outputs: [{ id: '', unit: 'percent' }],
    run: ultimateOscillator,
  }),
  defineStudy<CommodityChannelIndexOptions<SeriesSchema, string>>({
    name: 'commodityChannelIndex',
    family: 'momentum',
    summary: "Lambert's CCI — typical price in mean absolute deviations",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      period: { kind: 'integer', default: 20, min: 1, suggest: [10, 50] },
    },
    naming: { output: 'cci' },
    // Unbounded — ±100 is a conventional band, not a limit.
    outputs: [{ id: '', unit: 'ratio' }],
    run: commodityChannelIndex,
  }),
  defineStudy<FisherTransformOptions<SeriesSchema, string>>({
    name: 'fisherTransform',
    family: 'momentum',
    summary: "Ehlers' Fisher transform of the normalised median price",
    inputs: { high: { default: 'high' }, low: { default: 'low' } },
    params: {
      period: { kind: 'integer', default: 10, min: 1, suggest: [5, 30] },
    },
    naming: { prefix: 'fisher' },
    // The line is named `${prefix}` itself; the signal is it delayed a bar.
    outputs: [
      { id: '', unit: 'ratio' },
      { id: 'Signal', unit: 'ratio' },
    ],
    run: fisherTransform,
  }),
  defineStudy<PsychologicalLineOptions<SeriesSchema, string>>({
    name: 'psychologicalLine',
    family: 'momentum',
    summary: 'The percentage of the last n bars that closed up',
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', default: 12, min: 1, suggest: [5, 30] },
    },
    naming: { output: 'psy' },
    outputs: [{ id: '', unit: 'percent' }],
    run: psychologicalLine,
  }),
  defineStudy<PrettyGoodOscillatorOptions<SeriesSchema, string>>({
    name: 'prettyGoodOscillator',
    family: 'momentum',
    summary: "Johnson's PGO — distance from the SMA in average true ranges",
    // `column` defaults to whatever `close` resolves to, so redirecting
    // `close` moves both halves together; with `close` at its own default
    // that is `'close'`.
    inputs: {
      column: { default: 'close' },
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      period: { kind: 'integer', default: 14, min: 1, suggest: [5, 50] },
    },
    naming: { output: 'pgo' },
    outputs: [{ id: '', unit: 'ratio' }],
    run: prettyGoodOscillator,
  }),
  defineStudy<PrimeNumberOscillatorOptions<SeriesSchema, string>>({
    name: 'primeNumberOscillator',
    family: 'momentum',
    summary: 'The distance from the price to the prime nearest it',
    inputs: { column: { default: 'close' } },
    params: {},
    naming: { output: 'pno' },
    outputs: [{ id: '', unit: 'delta' }],
    run: primeNumberOscillator,
  }),
  defineStudy<QstickOptions<SeriesSchema, string>>({
    name: 'qstick',
    family: 'momentum',
    summary: "Chande's QStick — a moving average of the candle body",
    inputs: { open: { default: 'open' }, close: { default: 'close' } },
    params: {
      period: { kind: 'integer', default: 8, min: 1, suggest: [2, 30] },
      maType: { kind: 'enum', default: 'sma', of: MA_TYPES },
    },
    naming: { output: 'qstick' },
    outputs: [{ id: '', unit: 'delta' }],
    run: qstick,
  }),
  defineStudy<RelativeVigorIndexOptions<SeriesSchema, string>>({
    name: 'relativeVigorIndex',
    family: 'momentum',
    summary: "Ehlers' RVI — the body as a fraction of the range, plus signal",
    inputs: {
      open: { default: 'open' },
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      period: { kind: 'integer', default: 10, min: 1, suggest: [4, 30] },
    },
    naming: { prefix: 'rvi' },
    // The line is named `${prefix}` itself; the signal is a fourth SWMA.
    outputs: [
      { id: '', unit: 'ratio' },
      { id: 'Signal', unit: 'ratio' },
    ],
    run: relativeVigorIndex,
  }),
  defineStudy<CenterOfGravityOptions<SeriesSchema, string>>({
    name: 'centerOfGravity',
    family: 'momentum',
    summary: "Ehlers' CG — where the window's price mass balances, in bars",
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', default: 10, min: 1, suggest: [5, 30] },
    },
    naming: { output: 'cog' },
    // A bar offset, not a price: the uncentred form reads `−period … −1`
    // on a positive source, and the study is scale-invariant.
    outputs: [{ id: '', unit: 'bars' }],
    run: centerOfGravity,
  }),
  defineStudy<ChandeForecastOscillatorOptions<SeriesSchema, string>>({
    name: 'chandeForecastOscillator',
    family: 'momentum',
    summary: "Chande's CFO — the price against its own forecast, in percent",
    inputs: { column: { default: 'close' } },
    params: {
      // At least 2: a one-bar regression window has no slope.
      period: { kind: 'integer', default: 14, min: 2, suggest: [5, 50] },
    },
    naming: { output: 'cfo' },
    outputs: [{ id: '', unit: 'percent' }],
    run: chandeForecastOscillator,
  }),
  defineStudy<TrueStrengthIndexOptions<SeriesSchema, string>>({
    name: 'trueStrengthIndex',
    family: 'momentum',
    summary: "Blau's TSI — double-smoothed net change over total change",
    inputs: { column: { default: 'close' } },
    params: {
      longPeriod: { kind: 'integer', default: 25, min: 1, suggest: [10, 50] },
      shortPeriod: { kind: 'integer', default: 13, min: 1, suggest: [5, 30] },
      signalPeriod: { kind: 'integer', default: 7, min: 1, suggest: [3, 20] },
    },
    naming: { prefix: 'tsi' },
    // The line is named `${prefix}` itself (the `trix` shape).
    outputs: [
      { id: '', unit: 'percent' },
      { id: 'Signal', unit: 'percent' },
    ],
    run: trueStrengthIndex,
  }),
];
