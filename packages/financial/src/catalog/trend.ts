import type { SeriesSchema } from 'pond-ts';
import type { StudyDescriptor } from './types.js';
import { defineStudy } from './define.js';
import { directionalMovement } from '../studies/directional-movement.js';
import type { DirectionalMovementOptions } from '../studies/directional-movement.js';
import { aroon } from '../studies/aroon.js';
import type { AroonOptions } from '../studies/aroon.js';
import { vortex } from '../studies/vortex.js';
import type { VortexOptions } from '../studies/vortex.js';
import { verticalHorizontalFilter } from '../studies/vertical-horizontal-filter.js';
import type { VerticalHorizontalFilterOptions } from '../studies/vertical-horizontal-filter.js';
import { randomWalkIndex } from '../studies/random-walk-index.js';
import type { RandomWalkIndexOptions } from '../studies/random-walk-index.js';
import { superTrend } from '../studies/super-trend.js';
import type { SuperTrendOptions } from '../studies/super-trend.js';
import { parabolicSar } from '../studies/parabolic-sar.js';
import type { ParabolicSarOptions } from '../studies/parabolic-sar.js';
import { atrTrailingStop } from '../studies/atr-trailing-stop.js';
import type { AtrTrailingStopOptions } from '../studies/atr-trailing-stop.js';
import { zigZag } from '../studies/zig-zag.js';
import type { ZigZagOptions } from '../studies/zig-zag.js';
import { ichimoku } from '../studies/ichimoku.js';
import type { IchimokuOptions } from '../studies/ichimoku.js';
import { elderImpulse } from '../studies/elder-impulse.js';
import type { ElderImpulseOptions } from '../studies/elder-impulse.js';
import { elderRay } from '../studies/elder-ray.js';
import type { ElderRayOptions } from '../studies/elder-ray.js';
import { trendIntensityIndex } from '../studies/trend-intensity-index.js';
import type { TrendIntensityIndexOptions } from '../studies/trend-intensity-index.js';
import { ravi } from '../studies/ravi.js';
import type { RaviOptions } from '../studies/ravi.js';
import { swingIndex, accumulativeSwingIndex } from '../studies/swing-index.js';
import type { SwingIndexOptions } from '../studies/swing-index.js';

/** The `'trend'` family — see `types.ts` for the family list. */
export const TREND_STUDIES: readonly StudyDescriptor[] = [
  defineStudy<DirectionalMovementOptions<SeriesSchema, string>>({
    name: 'directionalMovement',
    family: 'trend',
    summary: "Wilder's directional movement system — +DI/−DI, DX, ADX, ADXR",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      period: { kind: 'integer', default: 14, min: 1, suggest: [5, 50] },
    },
    naming: { prefix: 'dmi' },
    // All five are bounded 0..100 — two ratios of the same true-range total,
    // their normalised difference, and two averages of that.
    outputs: [
      { id: 'PlusDi', unit: 'percent' },
      { id: 'MinusDi', unit: 'percent' },
      { id: 'Dx', unit: 'percent' },
      { id: 'Adx', unit: 'percent' },
      { id: 'Adxr', unit: 'percent' },
    ],
    run: directionalMovement,
  }),
  defineStudy<AroonOptions<SeriesSchema, string>>({
    name: 'aroon',
    family: 'trend',
    summary: "Chande's Aroon — how recently the window's extremes printed",
    inputs: { high: { default: 'high' }, low: { default: 'low' } },
    params: {
      period: { kind: 'integer', default: 25, min: 1, suggest: [10, 50] },
    },
    naming: { prefix: 'aroon' },
    // `Up`/`Down` are 0..100; the oscillator is their difference, −100..100 —
    // still the percent scale, not a level and not dimensionless.
    outputs: [
      { id: 'Up', unit: 'percent' },
      { id: 'Down', unit: 'percent' },
      { id: 'Osc', unit: 'percent' },
    ],
    run: aroon,
  }),
  defineStudy<VortexOptions<SeriesSchema, string>>({
    name: 'vortex',
    family: 'trend',
    summary: "Vortex Indicator — Botes & Siepman's crossed movement pair",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      period: { kind: 'integer', default: 14, min: 1, suggest: [5, 50] },
    },
    naming: { prefix: 'vi' },
    // Non-negative and NOT bounded by 1 (the docstring is explicit): a
    // dimensionless ratio of price differences, so `'ratio'`.
    outputs: [
      { id: 'Plus', unit: 'ratio' },
      { id: 'Minus', unit: 'ratio' },
    ],
    run: vortex,
  }),
  defineStudy<VerticalHorizontalFilterOptions<SeriesSchema, string>>({
    name: 'verticalHorizontalFilter',
    family: 'trend',
    summary: "Adam White's VHF — net movement over total movement",
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', default: 28, min: 1, suggest: [10, 50] },
    },
    naming: { output: 'vhf' },
    // A fraction in (0, 1] — a ratio of two price distances, not a percent
    // (the study deliberately does not multiply by 100).
    outputs: [{ id: '', unit: 'ratio' }],
    run: verticalHorizontalFilter,
  }),
  defineStudy<RandomWalkIndexOptions<SeriesSchema, string>>({
    name: 'randomWalkIndex',
    family: 'trend',
    summary: "Poulos' RWI — the move in units of a random walk's reach",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      // The horizons are `2 … period`, so the study rejects `period < 2`
      // over and above `assertPeriod`.
      period: { kind: 'integer', default: 14, min: 2, suggest: [5, 40] },
    },
    naming: { prefix: 'rwi' },
    // Dimensionless and unbounded in both directions (it goes negative).
    outputs: [
      { id: 'High', unit: 'ratio' },
      { id: 'Low', unit: 'ratio' },
    ],
    run: randomWalkIndex,
  }),
  defineStudy<SuperTrendOptions<SeriesSchema, string>>({
    name: 'superTrend',
    family: 'trend',
    summary: 'SuperTrend — a ratcheting ATR band that flips with the close',
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      period: { kind: 'integer', default: 10, min: 1, suggest: [5, 30] },
      // A width in ATRs, validated as a positive finite number, so the
      // infimum is 0 and 0 itself is still rejected.
      multiplier: { kind: 'number', default: 3, min: 0, suggest: [1, 5] },
    },
    naming: { prefix: 'st' },
    // The line column is the BARE prefix (`st`), hence the empty id; the
    // side follows it.
    outputs: [
      { id: '', unit: 'inherit' },
      { id: 'Trend', unit: 'signal' },
    ],
    run: superTrend,
  }),
  defineStudy<ParabolicSarOptions<SeriesSchema, string>>({
    name: 'parabolicSar',
    family: 'trend',
    summary: "Wilder's parabolic stop-and-reverse",
    inputs: { high: { default: 'high' }, low: { default: 'low' } },
    params: {
      // Both are acceleration factors, validated as positive finite numbers
      // (and `maxStep >= step`), not bar counts.
      step: { kind: 'number', default: 0.02, min: 0, suggest: [0.01, 0.05] },
      maxStep: { kind: 'number', default: 0.2, min: 0, suggest: [0.1, 0.5] },
    },
    naming: { prefix: 'psar' },
    // The stop column is the BARE prefix (`psar`), hence the empty id.
    outputs: [
      { id: '', unit: 'inherit' },
      { id: 'Trend', unit: 'signal' },
    ],
    run: parabolicSar,
  }),
  defineStudy<AtrTrailingStopOptions<SeriesSchema, string>>({
    name: 'atrTrailingStop',
    family: 'trend',
    summary: "Vervoort's close-anchored ratcheting ATR stop",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      period: { kind: 'integer', default: 14, min: 1, suggest: [5, 50] },
      multiplier: { kind: 'number', default: 3, min: 0, suggest: [1, 5] },
    },
    naming: { prefix: 'ats' },
    // The stop column is the BARE prefix (`ats`), hence the empty id.
    outputs: [
      { id: '', unit: 'inherit' },
      { id: 'Trend', unit: 'signal' },
    ],
    run: atrTrailingStop,
  }),
  defineStudy<ZigZagOptions<SeriesSchema, string>>({
    name: 'zigZag',
    family: 'trend',
    summary: 'ZigZag — percent-reversal pivots and the line joining them',
    inputs: { high: { default: 'high' }, low: { default: 'low' } },
    params: {
      // A percent of the leg's extreme, validated as a positive finite
      // number rather than by `assertPeriod`.
      deviation: { kind: 'number', default: 5, min: 0, suggest: [1, 20] },
    },
    naming: { prefix: 'zz' },
    // `Pivot` sits on the extreme's own bar and `Line` interpolates between
    // pivots — both price levels; `Direction` is the leg's `+1`/`−1`.
    outputs: [
      { id: 'Pivot', unit: 'inherit' },
      { id: 'Direction', unit: 'signal' },
      { id: 'Line', unit: 'inherit' },
    ],
    run: zigZag,
  }),
  defineStudy<IchimokuOptions<SeriesSchema, string>>({
    name: 'ichimoku',
    family: 'trend',
    summary: "Ichimoku Kinko Hyo — Hosoda's five lines",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      conversionPeriod: {
        kind: 'integer',
        default: 9,
        min: 1,
        suggest: [5, 20],
      },
      basePeriod: { kind: 'integer', default: 26, min: 1, suggest: [10, 60] },
      spanBPeriod: { kind: 'integer', default: 52, min: 1, suggest: [26, 120] },
      // Metadata: `displacement` SHIFTS NOTHING in any appended column —
      // every column is keyed to the bar it is computed from. It is
      // validated here and echoed by `ichimokuOffsets`, which is what a
      // chart applies as an x-offset.
      displacement: { kind: 'integer', default: 26, min: 1, suggest: [10, 60] },
    },
    naming: { prefix: 'ichi' },
    // All five are levels in the price's own units (the Chikou span IS the
    // close), so all five overlay the price axis.
    outputs: [
      { id: 'Tenkan', unit: 'inherit' },
      { id: 'Kijun', unit: 'inherit' },
      { id: 'SenkouA', unit: 'inherit' },
      { id: 'SenkouB', unit: 'inherit' },
      { id: 'Chikou', unit: 'inherit' },
    ],
    run: ichimoku,
  }),
  defineStudy<ElderImpulseOptions<SeriesSchema, string>>({
    name: 'elderImpulse',
    family: 'trend',
    summary: "Elder's impulse system — EMA and MACD histogram agreeing",
    inputs: { column: { default: 'close' } },
    params: {
      emaPeriod: { kind: 'integer', default: 13, min: 1, suggest: [5, 30] },
      fastPeriod: { kind: 'integer', default: 12, min: 1, suggest: [5, 20] },
      slowPeriod: { kind: 'integer', default: 26, min: 1, suggest: [15, 50] },
      signalPeriod: { kind: 'integer', default: 9, min: 1, suggest: [5, 20] },
    },
    naming: { output: 'impulse' },
    outputs: [{ id: '', unit: 'signal' }],
    run: elderImpulse,
  }),
  defineStudy<ElderRayOptions<SeriesSchema, string>>({
    name: 'elderRay',
    family: 'trend',
    summary: "Elder Ray — the bar's high and low against an EMA",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      period: { kind: 'integer', default: 13, min: 1, suggest: [5, 30] },
    },
    naming: { prefix: 'elder' },
    // Price units, but a zero-centred difference from the EMA — its own
    // axis, not the price axis.
    outputs: [
      { id: 'Bull', unit: 'delta' },
      { id: 'Bear', unit: 'delta' },
    ],
    run: elderRay,
  }),
  defineStudy<TrendIntensityIndexOptions<SeriesSchema, string>>({
    name: 'trendIntensityIndex',
    family: 'trend',
    summary: "M. H. Pee's trend intensity index — deviations above an SMA",
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', default: 30, min: 1, suggest: [10, 60] },
      maPeriod: { kind: 'integer', default: 60, min: 1, suggest: [20, 120] },
    },
    naming: { output: 'tii' },
    outputs: [{ id: '', unit: 'percent' }],
    run: trendIntensityIndex,
  }),
  defineStudy<RaviOptions<SeriesSchema, string>>({
    name: 'ravi',
    family: 'trend',
    summary: "Chande's RAVI — the two SMAs' gap as a percent of the slow one",
    inputs: { column: { default: 'close' } },
    params: {
      shortPeriod: { kind: 'integer', default: 7, min: 1, suggest: [3, 20] },
      longPeriod: { kind: 'integer', default: 65, min: 1, suggest: [20, 130] },
    },
    naming: { output: 'ravi' },
    outputs: [{ id: '', unit: 'percent' }],
    run: ravi,
  }),
  defineStudy<SwingIndexOptions<SeriesSchema, string>>({
    name: 'swingIndex',
    family: 'trend',
    summary: "Wilder's swing index — one bar's move against the limit move",
    inputs: {
      open: { default: 'open' },
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      // The instrument's limit move `T`, in the price's own units — a fact
      // about the contract, so there is no default and no useful suggested
      // range (the `tradeVolumeIndex` `minTick` shape). The example is sized
      // for ordinary ~100-priced bars.
      limit: { kind: 'number', example: 5, min: 0 },
    },
    naming: { output: 'si' },
    // Bounded −100…100 and zero-centred, but it scales with price unless
    // `limit` scales with it — a signed reading in Wilder's own units, not a
    // dimensionless ratio and not a level.
    outputs: [{ id: '', unit: 'delta' }],
    run: swingIndex,
  }),
  defineStudy<SwingIndexOptions<SeriesSchema, string>>({
    name: 'accumulativeSwingIndex',
    family: 'trend',
    summary: "Wilder's accumulative swing index — the running swing total",
    inputs: {
      open: { default: 'open' },
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      limit: { kind: 'number', example: 5, min: 0 },
    },
    naming: { output: 'asi' },
    // A cumulative level starting at the first swing's value — an arbitrary
    // origin, which is exactly what `'index'` names.
    outputs: [{ id: '', unit: 'index' }],
    run: accumulativeSwingIndex,
  }),
];
