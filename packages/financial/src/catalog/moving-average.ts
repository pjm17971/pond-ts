import type { SeriesSchema } from 'pond-ts';
import type { StudyDescriptor } from './types.js';
import { defineStudy } from './define.js';
import { ema, movingAverage, sma } from '../studies/moving-average.js';
import type {
  MovingAverageOptions,
  MovingAverageTypeOptions,
} from '../studies/moving-average.js';
import { macd } from '../studies/macd.js';
import type { MacdOptions } from '../studies/macd.js';
import { movingAverageCross } from '../studies/moving-average-cross.js';
import type { MovingAverageCrossOptions } from '../studies/moving-average-cross.js';
import { movingAverageDeviation } from '../studies/moving-average-deviation.js';
import type { MovingAverageDeviationOptions } from '../studies/moving-average-deviation.js';
import { guppy } from '../studies/guppy.js';
import type { GuppyOptions } from '../studies/guppy.js';
import { rainbow, rainbowOscillator } from '../studies/rainbow.js';
import type {
  RainbowOptions,
  RainbowOscillatorOptions,
} from '../studies/rainbow.js';
import { trix } from '../studies/trix.js';
import type { TrixOptions } from '../studies/trix.js';
import { schaffTrendCycle } from '../studies/schaff-trend-cycle.js';
import type { SchaffTrendCycleOptions } from '../studies/schaff-trend-cycle.js';
import { detrendedPriceOscillator } from '../studies/detrended-price-oscillator.js';
import type { DetrendedPriceOscillatorOptions } from '../studies/detrended-price-oscillator.js';
import { disparityIndex } from '../studies/disparity-index.js';
import type { DisparityIndexOptions } from '../studies/disparity-index.js';
import { coppock } from '../studies/coppock.js';
import type { CoppockOptions } from '../studies/coppock.js';
import { kst } from '../studies/kst.js';
import type { KstOptions } from '../studies/kst.js';
import { specialK } from '../studies/special-k.js';
import type { SpecialKOptions } from '../studies/special-k.js';
import { priceOscillator } from '../studies/price-oscillator.js';
import type { PriceOscillatorOptions } from '../studies/price-oscillator.js';
import { priceMomentumOscillator } from '../studies/price-momentum-oscillator.js';
import type { PriceMomentumOscillatorOptions } from '../studies/price-momentum-oscillator.js';
import { MA_TYPES } from '../kernels/moving-average.js';

/** The `'moving-average'` family — see `types.ts` for the family list. */
export const MOVING_AVERAGE_STUDIES: readonly StudyDescriptor[] = [
  defineStudy<MovingAverageOptions<SeriesSchema, string>>()({
    name: 'sma',
    family: 'moving-average',
    summary: 'Simple moving average of a column',
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', example: 20, min: 1, suggest: [5, 200] },
    },
    naming: { output: 'sma' },
    outputs: [{ id: '', unit: 'inherit' }],
    run: sma,
  }),
  defineStudy<MovingAverageOptions<SeriesSchema, string>>()({
    name: 'ema',
    family: 'moving-average',
    summary: 'Exponential moving average, span convention (α = 2/(period+1))',
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', example: 20, min: 1, suggest: [5, 200] },
    },
    naming: { output: 'ema' },
    outputs: [{ id: '', unit: 'inherit' }],
    run: ema,
  }),
  defineStudy<MovingAverageTypeOptions<SeriesSchema, string>>()({
    name: 'movingAverage',
    family: 'moving-average',
    summary: 'Moving average of a selectable type (the shared MaType menu)',
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', example: 20, min: 1, suggest: [5, 200] },
      type: { kind: 'enum', default: 'sma', of: MA_TYPES },
    },
    naming: { output: 'ma' },
    outputs: [{ id: '', unit: 'inherit' }],
    run: movingAverage,
  }),
  defineStudy<MacdOptions<SeriesSchema, string>>()({
    name: 'macd',
    family: 'moving-average',
    summary: "Appel's MACD — a fast/slow EMA spread with its signal line",
    inputs: { column: { default: 'close' } },
    params: {
      fastPeriod: { kind: 'integer', default: 12, min: 1, suggest: [5, 20] },
      slowPeriod: { kind: 'integer', default: 26, suggest: [20, 60] },
      signalPeriod: { kind: 'integer', default: 9, min: 1, suggest: [3, 20] },
    },
    naming: { prefix: 'macd' },
    // All three are the source's units but zero-centred — a spread of two
    // EMAs, that spread's own EMA, and their difference — so none of them
    // belongs on the price axis.
    outputs: [
      { id: 'Line', unit: 'delta' },
      { id: 'Signal', unit: 'delta' },
      { id: 'Hist', unit: 'delta' },
    ],
    run: macd,
  }),
  defineStudy<MovingAverageCrossOptions<SeriesSchema, string>>()({
    name: 'movingAverageCross',
    family: 'moving-average',
    summary: 'Fast/slow moving-average crossings as a +1/−1 event column',
    inputs: { column: { default: 'close' } },
    params: {
      fastPeriod: { kind: 'integer', default: 10, min: 1, suggest: [5, 25] },
      slowPeriod: { kind: 'integer', default: 30, suggest: [15, 200] },
      maType: { kind: 'enum', default: 'sma', of: MA_TYPES },
    },
    naming: { output: 'maCross' },
    // ONE column, and it is the event: the two averages are `movingAverage`'s
    // business and are never appended here (see the study's docstring).
    outputs: [{ id: '', unit: 'signal' }],
    run: movingAverageCross,
  }),
  defineStudy<MovingAverageDeviationOptions<SeriesSchema, string>>()({
    name: 'movingAverageDeviation',
    family: 'moving-average',
    summary: 'How far price sits from its moving average, in price units',
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', default: 20, min: 1, suggest: [5, 100] },
      maType: { kind: 'enum', default: 'sma', of: MA_TYPES },
    },
    naming: { output: 'maDev' },
    // `price − MA`: price units, but zero-centred, so it is not a level to
    // draw on the price axis. The percent half of the pair is
    // `disparityIndex`.
    outputs: [{ id: '', unit: 'delta' }],
    run: movingAverageDeviation,
  }),
  defineStudy<GuppyOptions<SeriesSchema, string>>()({
    name: 'guppy',
    family: 'moving-average',
    summary: "Guppy's GMMA — the fixed twelve-average trader/investor ribbon",
    inputs: { column: { default: 'close' } },
    params: { type: { kind: 'enum', default: 'ema', of: MA_TYPES } },
    naming: { prefix: 'gmma' },
    // Twelve columns, in the chain's order: the six-deep short ("trader")
    // ribbon, then the six-deep long ("investor") one. The periods are the
    // study, not an option (`GUPPY_SHORT_PERIODS` / `GUPPY_LONG_PERIODS`).
    outputs: [
      { id: 'S3', unit: 'inherit' },
      { id: 'S5', unit: 'inherit' },
      { id: 'S8', unit: 'inherit' },
      { id: 'S10', unit: 'inherit' },
      { id: 'S12', unit: 'inherit' },
      { id: 'S15', unit: 'inherit' },
      { id: 'L30', unit: 'inherit' },
      { id: 'L35', unit: 'inherit' },
      { id: 'L40', unit: 'inherit' },
      { id: 'L45', unit: 'inherit' },
      { id: 'L50', unit: 'inherit' },
      { id: 'L60', unit: 'inherit' },
    ],
    run: guppy,
  }),
  defineStudy<RainbowOptions<SeriesSchema, string>>()({
    name: 'rainbow',
    family: 'moving-average',
    summary: "Widner's Rainbow — ten recursively smoothed moving averages",
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', default: 2, min: 1, suggest: [2, 10] },
      type: { kind: 'enum', default: 'sma', of: MA_TYPES },
    },
    naming: { prefix: 'rainbow' },
    // Ten columns, in the chain's order — stage `k` smooths stage `k − 1`,
    // so every one is a level in the source's own units.
    outputs: [
      { id: '1', unit: 'inherit' },
      { id: '2', unit: 'inherit' },
      { id: '3', unit: 'inherit' },
      { id: '4', unit: 'inherit' },
      { id: '5', unit: 'inherit' },
      { id: '6', unit: 'inherit' },
      { id: '7', unit: 'inherit' },
      { id: '8', unit: 'inherit' },
      { id: '9', unit: 'inherit' },
      { id: '10', unit: 'inherit' },
    ],
    run: rainbow,
  }),
  defineStudy<RainbowOscillatorOptions<SeriesSchema, string>>()({
    name: 'rainbowOscillator',
    family: 'moving-average',
    summary: "ChartIQ's rainbow oscillator, banded by the stack's own width",
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', default: 2, min: 1, suggest: [2, 10] },
      lookback: { kind: 'integer', default: 10, min: 1, suggest: [5, 50] },
      // Spelled `type`, not `maType`, by the study's own named exception: it
      // IS `rainbow`'s `type`, passed through to the same ten-stage stack.
      type: { kind: 'enum', default: 'sma', of: MA_TYPES },
    },
    naming: { prefix: 'rbo' },
    // The line takes the bare prefix (`rbo`), hence the empty id; the two
    // mirrored bands keep the family suffix.
    outputs: [
      { id: '', unit: 'percent' },
      { id: 'Upper', unit: 'percent' },
      { id: 'Lower', unit: 'percent' },
    ],
    run: rainbowOscillator,
  }),
  defineStudy<TrixOptions<SeriesSchema, string>>()({
    name: 'trix',
    family: 'moving-average',
    summary: "Hutson's TRIX — percent rate of change of a triple EMA",
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', default: 15, min: 1, suggest: [5, 40] },
      signalPeriod: { kind: 'integer', default: 9, min: 1, suggest: [3, 20] },
    },
    naming: { prefix: 'trix' },
    // The line takes the bare prefix (`trix`), hence the empty id — the
    // study's deliberate departure from `macd`'s `macdLine`.
    outputs: [
      { id: '', unit: 'percent' },
      { id: 'Signal', unit: 'percent' },
    ],
    run: trix,
  }),
  defineStudy<SchaffTrendCycleOptions<SeriesSchema, string>>()({
    name: 'schaffTrendCycle',
    family: 'moving-average',
    summary: "Schaff's double stochastic of a MACD, bounded 0…100",
    inputs: { column: { default: 'close' } },
    params: {
      fastPeriod: { kind: 'integer', default: 23, min: 1, suggest: [10, 40] },
      slowPeriod: { kind: 'integer', default: 50, suggest: [30, 100] },
      cyclePeriod: { kind: 'integer', default: 10, min: 1, suggest: [5, 30] },
    },
    naming: { output: 'stc' },
    outputs: [{ id: '', unit: 'percent' }],
    run: schaffTrendCycle,
  }),
  defineStudy<DetrendedPriceOscillatorOptions<SeriesSchema, string>>()({
    name: 'detrendedPriceOscillator',
    family: 'moving-average',
    summary: 'Price less a moving average displaced ⌊period/2⌋+1 bars back',
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', default: 20, min: 1, suggest: [10, 50] },
      maType: { kind: 'enum', default: 'sma', of: MA_TYPES },
    },
    naming: { output: 'dpo' },
    // Price units, zero-centred — a detrended residual, not a level.
    outputs: [{ id: '', unit: 'delta' }],
    run: detrendedPriceOscillator,
  }),
  defineStudy<DisparityIndexOptions<SeriesSchema, string>>()({
    name: 'disparityIndex',
    family: 'moving-average',
    summary: "Nison's disparity — price against its moving average, in percent",
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', default: 14, min: 1, suggest: [5, 50] },
      maType: { kind: 'enum', default: 'sma', of: MA_TYPES },
    },
    naming: { output: 'disparity' },
    outputs: [{ id: '', unit: 'percent' }],
    run: disparityIndex,
  }),
  defineStudy<CoppockOptions<SeriesSchema, string>>()({
    name: 'coppock',
    family: 'moving-average',
    summary: 'Coppock Curve — a WMA of two percent rates of change, summed',
    inputs: { column: { default: 'close' } },
    params: {
      longPeriod: { kind: 'integer', default: 14, min: 1, suggest: [8, 24] },
      shortPeriod: { kind: 'integer', default: 11, min: 1, suggest: [6, 20] },
      wmaPeriod: { kind: 'integer', default: 10, min: 1, suggest: [5, 20] },
    },
    naming: { output: 'coppock' },
    outputs: [{ id: '', unit: 'percent' }],
    run: coppock,
  }),
  defineStudy<KstOptions<SeriesSchema, string>>()({
    name: 'kst',
    family: 'moving-average',
    summary: "Pring's Know Sure Thing — four weighted smoothed percent ROCs",
    inputs: { column: { default: 'close' } },
    params: {
      signalPeriod: { kind: 'integer', default: 9, min: 1, suggest: [3, 20] },
    },
    naming: { prefix: 'kst' },
    // The line takes the bare prefix (`kst`), hence the empty id — the `trix`
    // shape; the signal keeps the family suffix.
    outputs: [
      { id: '', unit: 'percent' },
      { id: 'Signal', unit: 'percent' },
    ],
    run: kst,
  }),
  defineStudy<SpecialKOptions<SeriesSchema, string>>()({
    name: 'specialK',
    family: 'moving-average',
    summary: "Pring's Special K — the KST extended to twelve terms",
    inputs: { column: { default: 'close' } },
    // The thirty-six constants ARE the study: no numeric or menu options at
    // all (and the warm-up is 724 bars).
    params: {},
    naming: { output: 'specialK' },
    outputs: [{ id: '', unit: 'percent' }],
    run: specialK,
  }),
  defineStudy<PriceOscillatorOptions<SeriesSchema, string>>()({
    name: 'priceOscillator',
    family: 'moving-average',
    summary: 'Fast/slow MA spread, as a percent (PPO) or in points (APO)',
    inputs: { column: { default: 'close' } },
    params: {
      fastPeriod: { kind: 'integer', default: 12, min: 1, suggest: [5, 20] },
      slowPeriod: { kind: 'integer', default: 26, suggest: [20, 60] },
      maType: { kind: 'enum', default: 'ema', of: MA_TYPES },
      // `PriceOscillatorMode` has no runtime constant, so the union is
      // listed literally.
      mode: { kind: 'enum', default: 'percent', of: ['percent', 'absolute'] },
    },
    naming: { output: 'priceOsc' },
    // The unit is the DEFAULT mode's: `'percent'` is `100·(fast − slow)/slow`.
    // Under `mode: 'absolute'` the same column is `fast − slow` in the
    // source's own units, i.e. `'delta'`.
    outputs: [{ id: '', unit: 'percent' }],
    run: priceOscillator,
  }),
  defineStudy<PriceMomentumOscillatorOptions<SeriesSchema, string>>()({
    name: 'priceMomentumOscillator',
    family: 'moving-average',
    summary: "DecisionPoint's PMO — a double-smoothed 1-bar percent ROC",
    inputs: { column: { default: 'close' } },
    // 35 / 20 / 10 and the ×10 are DecisionPoint's: no options at all.
    params: {},
    naming: { prefix: 'pmo' },
    // The line takes the bare prefix (`pmo`), hence the empty id — the `trix`
    // shape; the signal keeps the family suffix.
    outputs: [
      { id: '', unit: 'percent' },
      { id: 'Signal', unit: 'percent' },
    ],
    run: priceMomentumOscillator,
  }),
];
