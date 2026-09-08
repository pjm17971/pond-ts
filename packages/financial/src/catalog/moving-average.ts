import type { SeriesSchema } from 'pond-ts';
import type { StudyDescriptor } from './types.js';
import { defineStudy } from './define.js';
import { movingAverage, sma } from '../studies/moving-average.js';
import type {
  MovingAverageOptions,
  MovingAverageTypeOptions,
} from '../studies/moving-average.js';
import { MA_TYPES } from '../kernels/moving-average.js';

/** The `'moving-average'` family — see `types.ts` for the family list. */
export const MOVING_AVERAGE_STUDIES: readonly StudyDescriptor[] = [
  defineStudy<MovingAverageOptions<SeriesSchema, string>>({
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
  defineStudy<MovingAverageTypeOptions<SeriesSchema, string>>({
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
];
