import type { SeriesSchema } from 'pond-ts';
import type { StudyDescriptor } from './types.js';
import { defineStudy } from './define.js';
import { linearRegression } from '../studies/linear-regression.js';
import type { LinearRegressionOptions } from '../studies/linear-regression.js';
import { timeSeriesForecast } from '../studies/time-series-forecast.js';
import type { TimeSeriesForecastOptions } from '../studies/time-series-forecast.js';
import { beta } from '../studies/beta.js';
import type { BetaOptions } from '../studies/beta.js';
import { correlation } from '../studies/correlation.js';
import type { CorrelationOptions } from '../studies/correlation.js';
import { priceRelative } from '../studies/price-relative.js';
import type { PriceRelativeOptions } from '../studies/price-relative.js';
import { performanceIndex } from '../studies/performance-index.js';
import type { PerformanceIndexOptions } from '../studies/performance-index.js';
import { zScore } from '../studies/z-score.js';
import type { ZScoreOptions } from '../studies/z-score.js';
import {
  rollingMax,
  rollingMin,
  rollingPercentile,
  rollingStdev,
} from '../studies/rolling-stat.js';
import type {
  RollingPercentileOptions,
  RollingStatOptions,
} from '../studies/rolling-stat.js';

/** The `'statistical'` family — see `types.ts` for the family list. */
export const STATISTICAL_STUDIES: readonly StudyDescriptor[] = [
  defineStudy<LinearRegressionOptions<SeriesSchema, string>>()({
    name: 'linearRegression',
    family: 'statistical',
    summary: 'Rolling OLS fit against the bar index, with slope, angle and R²',
    inputs: { column: { default: 'close' } },
    params: {
      // At least 2: a one-bar window has no slope.
      period: { kind: 'integer', default: 14, min: 2, suggest: [5, 60] },
    },
    naming: { prefix: 'linreg' },
    outputs: [
      // The fit read at the window's LAST bar (TA-Lib `LINEARREG`) — a level.
      { id: 'Value', unit: 'inherit' },
      // Price units PER BAR, so a rate rather than a level on the price axis.
      { id: 'Slope', unit: 'delta' },
      // The fit at the window's FIRST bar — a level in the source's units.
      { id: 'Intercept', unit: 'inherit' },
      // atan(slope) in degrees: a bare number, bounded ±90.
      { id: 'Angle', unit: 'ratio' },
      { id: 'R2', unit: 'ratio' },
    ],
    run: linearRegression,
  }),
  defineStudy<TimeSeriesForecastOptions<SeriesSchema, string>>()({
    name: 'timeSeriesForecast',
    family: 'statistical',
    summary: 'The rolling OLS fit projected one bar past the window',
    inputs: { column: { default: 'close' } },
    params: {
      // At least 2: a one-bar window has no slope.
      period: { kind: 'integer', default: 14, min: 2, suggest: [5, 60] },
    },
    naming: { output: 'tsf' },
    outputs: [{ id: '', unit: 'inherit' }],
    run: timeSeriesForecast,
  }),
  defineStudy<BetaOptions<SeriesSchema, string>>()({
    name: 'beta',
    family: 'statistical',
    summary: "Slope of a column's one-bar returns on a benchmark's",
    inputs: { column: { default: 'close' }, benchmark: {} },
    params: {
      // At least 2: a single return has no variance.
      period: { kind: 'integer', default: 5, min: 2, suggest: [5, 60] },
    },
    naming: { output: 'beta' },
    outputs: [{ id: '', unit: 'ratio' }],
    run: beta,
  }),
  defineStudy<CorrelationOptions<SeriesSchema, string>>()({
    name: 'correlation',
    family: 'statistical',
    summary: 'Rolling Pearson correlation against a benchmark column',
    inputs: { column: { default: 'close' }, benchmark: {} },
    params: {
      period: { kind: 'integer', default: 30, min: 2, suggest: [10, 120] },
    },
    naming: { output: 'corr' },
    outputs: [{ id: '', unit: 'ratio' }],
    run: correlation,
  }),
  defineStudy<PriceRelativeOptions<SeriesSchema, string>>()({
    name: 'priceRelative',
    family: 'statistical',
    summary: 'Ratio of a column to a benchmark column, with no look-back',
    inputs: { column: { default: 'close' }, benchmark: {} },
    params: {},
    naming: { output: 'priceRel' },
    outputs: [{ id: '', unit: 'ratio' }],
    run: priceRelative,
  }),
  defineStudy<PerformanceIndexOptions<SeriesSchema, string>>()({
    name: 'performanceIndex',
    family: 'statistical',
    summary: "Each side's own period-bar growth, divided — 1 is parity",
    inputs: { column: { default: 'close' }, benchmark: {} },
    params: {
      period: { kind: 'integer', default: 20, min: 1, suggest: [10, 60] },
    },
    naming: { output: 'perf' },
    // A ratio of two growth ratios, oscillating around 1 — dimensionless.
    outputs: [{ id: '', unit: 'ratio' }],
    run: performanceIndex,
  }),
  defineStudy<ZScoreOptions<SeriesSchema, string>>()({
    name: 'zScore',
    family: 'statistical',
    summary: 'Deviation from the rolling mean, in standard deviations',
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', example: 20, min: 1, suggest: [10, 60] },
    },
    naming: { output: 'zscore' },
    outputs: [{ id: '', unit: 'ratio' }],
    run: zScore,
  }),
  defineStudy<RollingStatOptions<SeriesSchema, string>>()({
    name: 'rollingStdev',
    family: 'statistical',
    summary: 'Rolling population standard deviation (ddof = 0)',
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', example: 20, min: 1, suggest: [5, 100] },
    },
    naming: { output: 'stdev' },
    // A spread in the source's units, not a level on its axis.
    outputs: [{ id: '', unit: 'delta' }],
    run: rollingStdev,
  }),
  defineStudy<RollingStatOptions<SeriesSchema, string>>()({
    name: 'rollingMin',
    family: 'statistical',
    summary: 'Rolling minimum over the last period bars',
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', example: 20, min: 1, suggest: [5, 100] },
    },
    naming: { output: 'min' },
    outputs: [{ id: '', unit: 'inherit' }],
    run: rollingMin,
  }),
  defineStudy<RollingStatOptions<SeriesSchema, string>>()({
    name: 'rollingMax',
    family: 'statistical',
    summary: 'Rolling maximum over the last period bars',
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', example: 20, min: 1, suggest: [5, 100] },
    },
    naming: { output: 'max' },
    outputs: [{ id: '', unit: 'inherit' }],
    run: rollingMax,
  }),
  defineStudy<RollingPercentileOptions<SeriesSchema, string>>()({
    name: 'rollingPercentile',
    family: 'statistical',
    summary: 'Rolling q-th percentile, linearly interpolated',
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', example: 20, min: 1, suggest: [5, 100] },
      // Required, and validated into `[0, 100]` at both ends.
      q: { kind: 'number', example: 90, min: 0, max: 100, suggest: [5, 95] },
    },
    // The only default that is not a constant: the study names its column
    // `p${q}`, so the default follows `q`'s example above.
    naming: { output: 'p90' },
    outputs: [{ id: '', unit: 'inherit' }],
    run: rollingPercentile,
  }),
];
