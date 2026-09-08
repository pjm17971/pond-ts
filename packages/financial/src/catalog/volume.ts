import type { SeriesSchema } from 'pond-ts';
import type { StudyDescriptor } from './types.js';
import { defineStudy } from './define.js';
import { MA_TYPES } from '../kernels/moving-average.js';
import { obv } from '../studies/obv.js';
import type { ObvOptions } from '../studies/obv.js';
import { accumulationDistribution } from '../studies/accumulation-distribution.js';
import type { AccumulationDistributionOptions } from '../studies/accumulation-distribution.js';
import { priceVolumeTrend } from '../studies/price-volume-trend.js';
import type { PriceVolumeTrendOptions } from '../studies/price-volume-trend.js';
import { chaikinMoneyFlow } from '../studies/chaikin-money-flow.js';
import type { ChaikinMoneyFlowOptions } from '../studies/chaikin-money-flow.js';
import { chaikinOscillator } from '../studies/chaikin-oscillator.js';
import type { ChaikinOscillatorOptions } from '../studies/chaikin-oscillator.js';
import { moneyFlowIndex } from '../studies/money-flow-index.js';
import type { MoneyFlowIndexOptions } from '../studies/money-flow-index.js';
import { twiggsMoneyFlow } from '../studies/twiggs-money-flow.js';
import type { TwiggsMoneyFlowOptions } from '../studies/twiggs-money-flow.js';
import { forceIndex } from '../studies/force-index.js';
import type { ForceIndexOptions } from '../studies/force-index.js';
import { easeOfMovement } from '../studies/ease-of-movement.js';
import type { EaseOfMovementOptions } from '../studies/ease-of-movement.js';
import { klinger } from '../studies/klinger.js';
import type { KlingerOptions } from '../studies/klinger.js';
import { volumeOscillator } from '../studies/volume-oscillator.js';
import type { VolumeOscillatorOptions } from '../studies/volume-oscillator.js';
import { tradeVolumeIndex } from '../studies/trade-volume-index.js';
import type { TradeVolumeIndexOptions } from '../studies/trade-volume-index.js';
import {
  negativeVolumeIndex,
  positiveVolumeIndex,
} from '../studies/volume-index.js';
import type { VolumeIndexOptions } from '../studies/volume-index.js';
import { marketFacilitationIndex } from '../studies/market-facilitation-index.js';
import type { MarketFacilitationIndexOptions } from '../studies/market-facilitation-index.js';
import { shinoharaIntensityRatio } from '../studies/shinohara-intensity-ratio.js';
import type { ShinoharaIntensityRatioOptions } from '../studies/shinohara-intensity-ratio.js';
import { vwap } from '../studies/vwap.js';
import type { VwapOptions } from '../studies/vwap.js';

import { anchoredVwap } from '../studies/anchored-vwap.js';
import type { AnchoredVwapOptions } from '../studies/anchored-vwap.js';
/** The `'volume'` family — see `types.ts` for the family list. */
export const VOLUME_STUDIES: readonly StudyDescriptor[] = [
  defineStudy<ObvOptions<SeriesSchema, string>>({
    name: 'obv',
    family: 'volume',
    summary: "Granville's running total of signed volume",
    inputs: { close: { default: 'close' }, volume: { default: 'volume' } },
    params: {},
    naming: { output: 'obv' },
    outputs: [{ id: '', unit: 'volume' }],
    run: obv,
  }),
  defineStudy<AccumulationDistributionOptions<SeriesSchema, string>>({
    name: 'accumulationDistribution',
    family: 'volume',
    summary:
      "Chaikin's A/D line — volume signed by the close's place in the bar",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
      volume: { default: 'volume' },
    },
    params: {},
    naming: { output: 'ad' },
    outputs: [{ id: '', unit: 'volume' }],
    run: accumulationDistribution,
  }),
  defineStudy<PriceVolumeTrendOptions<SeriesSchema, string>>({
    name: 'priceVolumeTrend',
    family: 'volume',
    summary: 'Running total of volume scaled by fractional price change',
    inputs: { close: { default: 'close' }, volume: { default: 'volume' } },
    params: {},
    naming: { output: 'pvt' },
    outputs: [{ id: '', unit: 'volume' }],
    run: priceVolumeTrend,
  }),
  defineStudy<ChaikinMoneyFlowOptions<SeriesSchema, string>>({
    name: 'chaikinMoneyFlow',
    family: 'volume',
    summary: "Chaikin's A/D term as a volume-weighted mean over a window",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
      volume: { default: 'volume' },
    },
    params: {
      period: { kind: 'integer', default: 20, min: 1, suggest: [10, 50] },
    },
    naming: { output: 'cmf' },
    // Bounded −1..+1 (a volume-weighted mean of the close location).
    outputs: [{ id: '', unit: 'ratio' }],
    run: chaikinMoneyFlow,
  }),
  defineStudy<ChaikinOscillatorOptions<SeriesSchema, string>>({
    name: 'chaikinOscillator',
    family: 'volume',
    summary: 'MACD of the Accumulation/Distribution line',
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
      volume: { default: 'volume' },
    },
    params: {
      fastPeriod: { kind: 'integer', default: 3, min: 1, suggest: [2, 10] },
      slowPeriod: { kind: 'integer', default: 10, min: 1, suggest: [5, 30] },
    },
    naming: { output: 'chaikinOsc' },
    outputs: [{ id: '', unit: 'volume' }],
    run: chaikinOscillator,
  }),
  defineStudy<MoneyFlowIndexOptions<SeriesSchema, string>>({
    name: 'moneyFlowIndex',
    family: 'volume',
    summary: 'RSI computed on money flow instead of price',
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
      volume: { default: 'volume' },
    },
    params: {
      period: { kind: 'integer', default: 14, min: 1, suggest: [5, 30] },
    },
    naming: { output: 'mfi' },
    outputs: [{ id: '', unit: 'percent' }],
    run: moneyFlowIndex,
  }),
  defineStudy<TwiggsMoneyFlowOptions<SeriesSchema, string>>({
    name: 'twiggsMoneyFlow',
    family: 'volume',
    summary: "Twiggs' money flow — true range, Wilder-smoothed",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
      volume: { default: 'volume' },
    },
    params: {
      period: { kind: 'integer', default: 21, min: 1, suggest: [10, 50] },
    },
    naming: { output: 'tmf' },
    outputs: [{ id: '', unit: 'ratio' }],
    run: twiggsMoneyFlow,
  }),
  defineStudy<ForceIndexOptions<SeriesSchema, string>>({
    name: 'forceIndex',
    family: 'volume',
    summary: "Elder's force index — price change times volume, EMA-smoothed",
    inputs: { close: { default: 'close' }, volume: { default: 'volume' } },
    params: {
      // `1` is the raw, unsmoothed force — the low end of the useful range.
      period: { kind: 'integer', default: 13, min: 1, suggest: [1, 40] },
    },
    naming: { output: 'force' },
    outputs: [{ id: '', unit: 'volume' }],
    run: forceIndex,
  }),
  defineStudy<EaseOfMovementOptions<SeriesSchema, string>>({
    name: 'easeOfMovement',
    family: 'volume',
    summary: "Arms' ease of movement — midpoint travel per unit of volume",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      volume: { default: 'volume' },
    },
    params: {
      period: { kind: 'integer', default: 14, min: 1, suggest: [5, 30] },
      maType: { kind: 'enum', default: 'sma', of: MA_TYPES },
      // A pure scale factor on the box ratio; the study rejects `<= 0`.
      scale: {
        kind: 'number',
        default: 100_000_000,
        min: 0,
        suggest: [1_000_000, 1_000_000_000],
      },
    },
    naming: { output: 'eom' },
    outputs: [{ id: '', unit: 'ratio' }],
    run: easeOfMovement,
  }),
  defineStudy<KlingerOptions<SeriesSchema, string>>({
    name: 'klinger',
    family: 'volume',
    summary: "Klinger's volume oscillator with its signal line",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
      volume: { default: 'volume' },
    },
    params: {
      fastPeriod: { kind: 'integer', default: 34, min: 1, suggest: [10, 60] },
      slowPeriod: { kind: 'integer', default: 55, min: 1, suggest: [20, 100] },
      signalPeriod: { kind: 'integer', default: 13, min: 1, suggest: [5, 30] },
    },
    naming: { prefix: 'kvo' },
    // The oscillator column is the BARE prefix (`kvo`), the signal is
    // `kvoSignal` — hence the empty first id (see `macd`, which suffixes
    // `Line` instead; `klinger` does not).
    outputs: [
      { id: '', unit: 'volume' },
      { id: 'Signal', unit: 'volume' },
    ],
    run: klinger,
  }),
  defineStudy<VolumeOscillatorOptions<SeriesSchema, string>>({
    name: 'volumeOscillator',
    family: 'volume',
    summary: 'Fast minus slow moving average of volume, as a percent',
    inputs: { volume: { default: 'volume' } },
    params: {
      fastPeriod: { kind: 'integer', default: 5, min: 1, suggest: [2, 20] },
      slowPeriod: { kind: 'integer', default: 10, min: 1, suggest: [5, 50] },
      maType: { kind: 'enum', default: 'sma', of: MA_TYPES },
    },
    naming: { output: 'volOsc' },
    // `priceOscillator` in `'percent'` mode: `100 · (fast − slow) / slow`.
    outputs: [{ id: '', unit: 'percent' }],
    run: volumeOscillator,
  }),
  defineStudy<TradeVolumeIndexOptions<SeriesSchema, string>>({
    name: 'tradeVolumeIndex',
    family: 'volume',
    summary: 'Tick-direction volume accumulation (needs the instrument tick)',
    inputs: { column: { default: 'close' }, volume: { default: 'volume' } },
    params: { minTick: { kind: 'number', example: 0.01, min: 0 } },
    naming: { output: 'tvi' },
    outputs: [{ id: '', unit: 'volume' }],
    run: tradeVolumeIndex,
  }),
  defineStudy<VolumeIndexOptions<SeriesSchema, string>>({
    name: 'negativeVolumeIndex',
    family: 'volume',
    summary: "Fosback's index compounding returns on lower-volume bars",
    inputs: { column: { default: 'close' }, volume: { default: 'volume' } },
    params: {
      // The index's arbitrary base; the study rejects `<= 0`.
      start: {
        kind: 'number',
        default: 1000,
        min: 0,
        suggest: [100, 10_000],
      },
    },
    naming: { output: 'nvi' },
    outputs: [{ id: '', unit: 'index' }],
    run: negativeVolumeIndex,
  }),
  defineStudy<VolumeIndexOptions<SeriesSchema, string>>({
    name: 'positiveVolumeIndex',
    family: 'volume',
    summary: "Fosback's index compounding returns on higher-volume bars",
    inputs: { column: { default: 'close' }, volume: { default: 'volume' } },
    params: {
      start: {
        kind: 'number',
        default: 1000,
        min: 0,
        suggest: [100, 10_000],
      },
    },
    naming: { output: 'pvi' },
    outputs: [{ id: '', unit: 'index' }],
    run: positiveVolumeIndex,
  }),
  defineStudy<MarketFacilitationIndexOptions<SeriesSchema, string>>({
    name: 'marketFacilitationIndex',
    family: 'volume',
    summary: "Bill Williams' price movement per unit of volume",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      volume: { default: 'volume' },
    },
    params: {},
    // `'bwmfi'`, not `'mfi'` — `moneyFlowIndex` already has that name.
    naming: { output: 'bwmfi' },
    outputs: [{ id: '', unit: 'ratio' }],
    run: marketFacilitationIndex,
  }),
  defineStudy<ShinoharaIntensityRatioOptions<SeriesSchema, string>>({
    name: 'shinoharaIntensityRatio',
    family: 'volume',
    summary: "Shinohara's strong and weak intensity ratios",
    inputs: {
      open: { default: 'open' },
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      period: { kind: 'integer', default: 26, min: 1, suggest: [10, 60] },
    },
    naming: { prefix: 'sir' },
    outputs: [
      { id: 'Strong', unit: 'ratio' },
      { id: 'Weak', unit: 'ratio' },
    ],
    run: shinoharaIntensityRatio,
  }),
  defineStudy<VwapOptions<SeriesSchema, string>>({
    name: 'vwap',
    family: 'volume',
    summary: 'Volume-weighted average price over a trailing window',
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
      volume: { default: 'volume' },
    },
    // Required: VWAP has no conventional length (see the study docstring).
    params: {
      period: { kind: 'integer', example: 20, min: 1, suggest: [5, 200] },
    },
    naming: { output: 'vwap' },
    outputs: [{ id: '', unit: 'inherit' }],
    run: vwap,
  }),
  defineStudy<AnchoredVwapOptions<SeriesSchema, string>>({
    name: 'anchoredVwap',
    family: 'volume',
    summary: 'VWAP accumulated from a user-chosen bar onward',
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
      volume: { default: 'volume' },
    },
    params: {},
    naming: { output: 'avwap' },
    outputs: [{ id: '', unit: 'inherit' }],
    // `anchor` is an instant (Date | epoch ms), supplied by the consumer.
    anchor: 'time',
    run: anchoredVwap,
  }),
];
