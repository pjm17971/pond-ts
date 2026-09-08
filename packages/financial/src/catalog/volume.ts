import type { SeriesSchema } from 'pond-ts';
import type { StudyDescriptor } from './types.js';
import { defineStudy } from './define.js';
import { tradeVolumeIndex } from '../studies/trade-volume-index.js';
import type { TradeVolumeIndexOptions } from '../studies/trade-volume-index.js';

/** The `'volume'` family — see `types.ts` for the family list. */
export const VOLUME_STUDIES: readonly StudyDescriptor[] = [
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
];
