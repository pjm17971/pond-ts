import type { SeriesSchema } from 'pond-ts';
import type { StudyDescriptor } from './types.js';
import { defineStudy } from './define.js';
import { atr } from '../studies/atr.js';
import type { AtrOptions } from '../studies/atr.js';
import { historicalVolatility } from '../studies/volatility.js';
import type { HistoricalVolatilityOptions } from '../studies/volatility.js';
import { chaikinVolatility } from '../studies/chaikin-volatility.js';
import type { ChaikinVolatilityOptions } from '../studies/chaikin-volatility.js';
import { massIndex } from '../studies/mass-index.js';
import type { MassIndexOptions } from '../studies/mass-index.js';
import { choppinessIndex } from '../studies/choppiness-index.js';
import type { ChoppinessIndexOptions } from '../studies/choppiness-index.js';
import { gopalakrishnanRangeIndex } from '../studies/gopalakrishnan-range-index.js';
import type { GopalakrishnanRangeIndexOptions } from '../studies/gopalakrishnan-range-index.js';
import { ulcerIndex } from '../studies/ulcer-index.js';
import type { UlcerIndexOptions } from '../studies/ulcer-index.js';
import { relativeVolatilityIndex } from '../studies/relative-volatility-index.js';
import type { RelativeVolatilityIndexOptions } from '../studies/relative-volatility-index.js';

/** The `'volatility'` family — see `types.ts` for the family list. */
export const VOLATILITY_STUDIES: readonly StudyDescriptor[] = [
  defineStudy<AtrOptions<SeriesSchema, string>>({
    name: 'atr',
    family: 'volatility',
    summary: "Wilder's average true range",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      period: { kind: 'integer', default: 14, min: 1, suggest: [5, 50] },
    },
    naming: { output: 'atr' },
    outputs: [{ id: '', unit: 'delta' }],
    run: atr,
  }),
  defineStudy<HistoricalVolatilityOptions<SeriesSchema, string>>({
    name: 'historicalVolatility',
    family: 'volatility',
    summary: 'Annualised σ of log returns, as a decimal',
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', default: 20, min: 1, suggest: [10, 60] },
      // Bars per year, applied as √annualize. Validated positive-finite, so
      // `min` is the infimum: the study still rejects exactly `0`. The useful
      // range spans the calendar conventions (`1` = raw per-bar σ, `252`
      // trading days, `365` a 7-day market); intraday bar counts run far
      // above it.
      annualize: { kind: 'number', default: 252, min: 0, suggest: [1, 365] },
    },
    naming: { output: 'hv' },
    outputs: [{ id: '', unit: 'ratio' }],
    run: historicalVolatility,
  }),
  defineStudy<ChaikinVolatilityOptions<SeriesSchema, string>>({
    name: 'chaikinVolatility',
    family: 'volatility',
    summary: "Chaikin's percent rate of change of a smoothed bar range",
    inputs: { high: { default: 'high' }, low: { default: 'low' } },
    params: {
      period: { kind: 'integer', default: 10, min: 1, suggest: [5, 30] },
      rocPeriod: { kind: 'integer', default: 10, min: 1, suggest: [5, 30] },
    },
    naming: { output: 'chaikinVol' },
    outputs: [{ id: '', unit: 'percent' }],
    run: chaikinVolatility,
  }),
  defineStudy<MassIndexOptions<SeriesSchema, string>>({
    name: 'massIndex',
    family: 'volatility',
    summary: "Dorsey's range-expansion sum — the reversal bulge",
    inputs: { high: { default: 'high' }, low: { default: 'low' } },
    params: {
      emaPeriod: { kind: 'integer', default: 9, min: 1, suggest: [5, 20] },
      sumPeriod: { kind: 'integer', default: 25, min: 1, suggest: [10, 50] },
    },
    naming: { output: 'mass' },
    // A sum of EMA ratios: dimensionless and unbounded, sitting near
    // `sumPeriod` rather than near zero — not an axis the price can share.
    outputs: [{ id: '', unit: 'ratio' }],
    run: massIndex,
  }),
  defineStudy<ChoppinessIndexOptions<SeriesSchema, string>>({
    name: 'choppinessIndex',
    family: 'volatility',
    summary: "Dreiss' Choppiness Index — trending against ranging, 0..100",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      // At least 2: the reading is normalised by log10(period), zero at 1.
      period: { kind: 'integer', default: 14, min: 2, suggest: [5, 30] },
    },
    naming: { output: 'chop' },
    outputs: [{ id: '', unit: 'percent' }],
    run: choppinessIndex,
  }),
  defineStudy<GopalakrishnanRangeIndexOptions<SeriesSchema, string>>({
    name: 'gopalakrishnanRangeIndex',
    family: 'volatility',
    summary: "GAPO — the window's high-low range, logged in base period",
    inputs: { high: { default: 'high' }, low: { default: 'low' } },
    params: {
      // At least 2: the reading is normalised by ln(period), zero at 1.
      period: { kind: 'integer', default: 10, min: 2, suggest: [5, 30] },
    },
    naming: { output: 'gapo' },
    // A logarithm of a range over a logarithm of a bar count: a bare number,
    // read comparatively against its own history rather than on a price axis.
    outputs: [{ id: '', unit: 'ratio' }],
    run: gopalakrishnanRangeIndex,
  }),
  defineStudy<UlcerIndexOptions<SeriesSchema, string>>({
    name: 'ulcerIndex',
    family: 'volatility',
    summary: "Martin's RMS percent drawdown from the rolling peak",
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', default: 14, min: 1, suggest: [5, 30] },
    },
    naming: { output: 'ulcer' },
    outputs: [{ id: '', unit: 'percent' }],
    run: ulcerIndex,
  }),
  defineStudy<RelativeVolatilityIndexOptions<SeriesSchema, string>>({
    name: 'relativeVolatilityIndex',
    family: 'volatility',
    summary: "Dorsey's RSI form applied to σ instead of price",
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', default: 14, min: 1, suggest: [5, 30] },
      stdevPeriod: { kind: 'integer', default: 10, min: 1, suggest: [5, 30] },
    },
    naming: { output: 'relVol' },
    outputs: [{ id: '', unit: 'percent' }],
    run: relativeVolatilityIndex,
  }),
];
