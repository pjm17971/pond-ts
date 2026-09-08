import type { SeriesSchema } from 'pond-ts';
import type { StudyDescriptor } from './types.js';
import { defineStudy } from './define.js';
import { correlation } from '../studies/correlation.js';
import type { CorrelationOptions } from '../studies/correlation.js';

/** The `'statistical'` family — see `types.ts` for the family list. */
export const STATISTICAL_STUDIES: readonly StudyDescriptor[] = [
  defineStudy<CorrelationOptions<SeriesSchema, string>>({
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
];
