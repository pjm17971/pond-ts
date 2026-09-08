import type { SeriesSchema } from 'pond-ts';
import type { StudyDescriptor } from './types.js';
import { defineStudy } from './define.js';
import {
  averagePrice,
  medianPrice,
  typicalPrice,
  weightedClose,
} from '../studies/price-transform.js';
import { balanceOfPower } from '../studies/balance-of-power.js';
import type { BalanceOfPowerOptions } from '../studies/balance-of-power.js';
import { MA_TYPES } from '../kernels/moving-average.js';
import type {
  AveragePriceOptions,
  PriceTransformOptions,
} from '../studies/price-transform.js';

/** The `'price'` family — see `types.ts` for the family list. */
export const PRICE_STUDIES: readonly StudyDescriptor[] = [
  defineStudy<PriceTransformOptions<SeriesSchema, string>>({
    name: 'typicalPrice',
    family: 'price',
    summary: 'Typical price — (high + low + close) / 3',
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {},
    naming: { output: 'typicalPrice' },
    outputs: [{ id: '', unit: 'inherit' }],
    run: typicalPrice,
  }),

  defineStudy<PriceTransformOptions<SeriesSchema, string>>({
    name: 'medianPrice',
    family: 'price',
    summary: 'Median price — (high + low) / 2',
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      // On the shared options type and accepted, but never read — the family
      // has one shape (see `PriceTransformOptions`).
      close: { default: 'close' },
    },
    params: {},
    naming: { output: 'medianPrice' },
    outputs: [{ id: '', unit: 'inherit' }],
    run: medianPrice,
  }),

  defineStudy<PriceTransformOptions<SeriesSchema, string>>({
    name: 'weightedClose',
    family: 'price',
    summary: 'Weighted close — (high + low + 2·close) / 4',
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {},
    naming: { output: 'weightedClose' },
    outputs: [{ id: '', unit: 'inherit' }],
    run: weightedClose,
  }),

  defineStudy<AveragePriceOptions<SeriesSchema, string>>({
    name: 'averagePrice',
    family: 'price',
    summary: 'Average price — (open + high + low + close) / 4',
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
      // The one transform that reads the open; declared last because the
      // options type adds it to the shared three.
      open: { default: 'open' },
    },
    params: {},
    naming: { output: 'averagePrice' },
    outputs: [{ id: '', unit: 'inherit' }],
    run: averagePrice,
  }),
  defineStudy<BalanceOfPowerOptions<SeriesSchema, string>>({
    name: 'balanceOfPower',
    family: 'price',
    summary:
      "TA-Lib's per-bar (close − open)/(high − low), optionally smoothed",
    inputs: {
      open: { default: 'open' },
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      // Omitted is not a default: absent is the raw per-bar ratio, present
      // smooths it — hence `optional` with an `example` rather than a
      // `default`.
      period: {
        kind: 'integer',
        optional: true,
        example: 14,
        min: 1,
        suggest: [5, 30],
      },
      // Only legal alongside `period` (the study throws otherwise).
      maType: {
        kind: 'enum',
        default: 'sma',
        of: MA_TYPES,
        requires: 'period',
      },
    },
    naming: { output: 'bop' },
    outputs: [{ id: '', unit: 'ratio' }],
    run: balanceOfPower,
  }),
];
