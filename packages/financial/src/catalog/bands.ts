import type { SeriesSchema } from 'pond-ts';
import type { StudyDescriptor } from './types.js';
import { defineStudy } from './define.js';
import { MA_TYPES } from '../kernels/moving-average.js';
import { bollinger } from '../studies/bollinger.js';
import type { BollingerOptions } from '../studies/bollinger.js';
import {
  bollingerBandwidth,
  bollingerPercentB,
} from '../studies/bollinger-derived.js';
import type { BollingerDerivedOptions } from '../studies/bollinger-derived.js';
import { keltner } from '../studies/keltner.js';
import type { KeltnerOptions } from '../studies/keltner.js';
import { starcBands } from '../studies/starc-bands.js';
import type { StarcBandsOptions } from '../studies/starc-bands.js';
import { atrBands } from '../studies/atr-bands.js';
import type { AtrBandsOptions } from '../studies/atr-bands.js';
import { donchian } from '../studies/donchian.js';
import type { DonchianOptions } from '../studies/donchian.js';
import { highLowBands } from '../studies/high-low-bands.js';
import type { HighLowBandsOptions } from '../studies/high-low-bands.js';
import { primeNumberBands } from '../studies/prime-number.js';
import type { PrimeNumberBandsOptions } from '../studies/prime-number.js';
import { envelope } from '../studies/envelope.js';
import type { EnvelopeOptions } from '../studies/envelope.js';

/** The `'bands'` family — see `types.ts` for the family list. */
export const BANDS_STUDIES: readonly StudyDescriptor[] = [
  defineStudy<BollingerOptions<SeriesSchema, string>>({
    name: 'bollinger',
    family: 'bands',
    summary: 'Bollinger Bands — an SMA middle band with bands at ±k·σ',
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', example: 20, min: 1, suggest: [10, 50] },
      stdDev: { kind: 'number', default: 2, min: 0, suggest: [1, 3] },
    },
    naming: { prefix: 'bb' },
    outputs: [
      { id: 'Middle', unit: 'inherit' },
      { id: 'Upper', unit: 'inherit' },
      { id: 'Lower', unit: 'inherit' },
    ],
    run: bollinger,
  }),

  defineStudy<BollingerDerivedOptions<SeriesSchema, string>>({
    name: 'bollingerBandwidth',
    family: 'bands',
    summary: "Bollinger BandWidth — the channel's width in percent of price",
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', default: 20, min: 1, suggest: [10, 50] },
      stdDev: { kind: 'number', default: 2, min: 0, suggest: [1, 3] },
    },
    naming: { output: 'bbWidth' },
    // `100 × (upper − lower) / middle` — a percent of the centre, not a level.
    outputs: [{ id: '', unit: 'percent' }],
    run: bollingerBandwidth,
  }),

  defineStudy<BollingerDerivedOptions<SeriesSchema, string>>({
    name: 'bollingerPercentB',
    family: 'bands',
    summary: 'Bollinger %B — where price sits inside its own channel',
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', default: 20, min: 1, suggest: [10, 50] },
      stdDev: { kind: 'number', default: 2, min: 0, suggest: [1, 3] },
    },
    naming: { output: 'percentB' },
    // A position in the channel: `1` on the upper band, `0` on the lower —
    // a decimal, not ×100, and unbounded outside the bands.
    outputs: [{ id: '', unit: 'ratio' }],
    run: bollingerPercentB,
  }),

  defineStudy<KeltnerOptions<SeriesSchema, string>>({
    name: 'keltner',
    family: 'bands',
    summary: 'Keltner Channel — an EMA of typical price with bands at ±k·ATR',
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      period: { kind: 'integer', default: 20, min: 1, suggest: [10, 50] },
      atrPeriod: { kind: 'integer', default: 10, min: 1, suggest: [5, 30] },
      multiplier: { kind: 'number', default: 2, min: 0, suggest: [1, 4] },
      maType: { kind: 'enum', default: 'ema', of: MA_TYPES },
    },
    naming: { prefix: 'kc' },
    outputs: [
      { id: 'Middle', unit: 'inherit' },
      { id: 'Upper', unit: 'inherit' },
      { id: 'Lower', unit: 'inherit' },
    ],
    run: keltner,
  }),

  defineStudy<StarcBandsOptions<SeriesSchema, string>>({
    name: 'starcBands',
    family: 'bands',
    summary: 'STARC Bands (Stoller) — an MA of close with bands at ±k·ATR',
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      period: { kind: 'integer', default: 20, min: 1, suggest: [10, 50] },
      atrPeriod: { kind: 'integer', default: 15, min: 1, suggest: [5, 30] },
      multiplier: { kind: 'number', default: 2, min: 0, suggest: [1, 4] },
      maType: { kind: 'enum', default: 'sma', of: MA_TYPES },
    },
    naming: { prefix: 'starc' },
    outputs: [
      { id: 'Middle', unit: 'inherit' },
      { id: 'Upper', unit: 'inherit' },
      { id: 'Lower', unit: 'inherit' },
    ],
    run: starcBands,
  }),

  defineStudy<AtrBandsOptions<SeriesSchema, string>>({
    name: 'atrBands',
    family: 'bands',
    summary: 'A field with volatility bands at ±k·ATR either side of it',
    inputs: {
      // What the bands are drawn around; defaults to whatever `close`
      // resolves to, which is `'close'` unless that is redirected too.
      column: { default: 'close' },
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      period: { kind: 'integer', default: 14, min: 1, suggest: [5, 50] },
      multiplier: { kind: 'number', default: 2, min: 0, suggest: [1, 4] },
    },
    naming: { prefix: 'atrb' },
    // Two columns and no middle: the middle is `column`, already on the series.
    outputs: [
      { id: 'Upper', unit: 'inherit' },
      { id: 'Lower', unit: 'inherit' },
    ],
    run: atrBands,
  }),

  defineStudy<DonchianOptions<SeriesSchema, string>>({
    name: 'donchian',
    family: 'bands',
    summary: "Donchian channel — the window's highest high and lowest low",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
    },
    params: {
      period: { kind: 'integer', default: 20, min: 1, suggest: [10, 55] },
    },
    naming: { prefix: 'dc' },
    // Upper, then Lower, then the midpoint — the study's own append order.
    outputs: [
      { id: 'Upper', unit: 'inherit' },
      { id: 'Lower', unit: 'inherit' },
      { id: 'Middle', unit: 'inherit' },
    ],
    run: donchian,
  }),

  defineStudy<HighLowBandsOptions<SeriesSchema, string>>({
    name: 'highLowBands',
    family: 'bands',
    summary: 'High Low Bands — a smoothed median price ±percent',
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
    },
    params: {
      period: { kind: 'integer', default: 10, min: 1, suggest: [5, 30] },
      percent: { kind: 'number', default: 1, min: 0, suggest: [0.5, 5] },
      maType: { kind: 'enum', default: 'trima', of: MA_TYPES },
    },
    naming: { prefix: 'hlb' },
    outputs: [
      { id: 'Middle', unit: 'inherit' },
      { id: 'Upper', unit: 'inherit' },
      { id: 'Lower', unit: 'inherit' },
    ],
    run: highLowBands,
  }),

  defineStudy<PrimeNumberBandsOptions<SeriesSchema, string>>({
    name: 'primeNumberBands',
    family: 'bands',
    summary: "The primes immediately bracketing each bar's high and low",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
    },
    params: {},
    naming: { prefix: 'pnb' },
    // A step function of the price level — still a level on the price axis.
    outputs: [
      { id: 'Upper', unit: 'inherit' },
      { id: 'Lower', unit: 'inherit' },
    ],
    run: primeNumberBands,
  }),

  defineStudy<EnvelopeOptions<SeriesSchema, string>>({
    name: 'envelope',
    family: 'bands',
    summary: 'Moving-average envelope — bands at ±percent of the centre line',
    inputs: { column: { default: 'close' } },
    params: {
      period: { kind: 'integer', example: 20, min: 1, suggest: [10, 50] },
      percent: { kind: 'number', default: 2.5, min: 0, suggest: [0.5, 10] },
      maType: { kind: 'enum', default: 'sma', of: MA_TYPES },
    },
    naming: { prefix: 'env' },
    outputs: [
      { id: 'Middle', unit: 'inherit' },
      { id: 'Upper', unit: 'inherit' },
      { id: 'Lower', unit: 'inherit' },
    ],
    run: envelope,
  }),
];
