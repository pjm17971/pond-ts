import type { SeriesSchema } from 'pond-ts';
import type { StudyDescriptor } from './types.js';
import { defineStudy } from './define.js';
import { bollinger } from '../studies/bollinger.js';
import type { BollingerOptions } from '../studies/bollinger.js';

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
];
