import type { SeriesSchema } from 'pond-ts';
import type { StudyDescriptor } from './types.js';
import { defineStudy } from './define.js';
import { atr } from '../studies/atr.js';
import type { AtrOptions } from '../studies/atr.js';

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
];
