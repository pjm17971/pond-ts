import type { SeriesSchema } from 'pond-ts';
import type { StudyDescriptor } from './types.js';
import { defineStudy } from './define.js';
import { sessionVwap } from '../studies/session-vwap.js';
import type { SessionVwapOptions } from '../studies/session-vwap.js';
import { pivotPoints } from '../studies/pivot-points.js';
import type { PivotPointsOptions } from '../studies/pivot-points.js';
import { PIVOT_METHODS } from '../kernels/pivot.js';
import type { PivotMethod } from '../kernels/pivot.js';

/** The `'session'` family — see `types.ts` for the family list. */
export const SESSION_STUDIES: readonly StudyDescriptor[] = [
  defineStudy<SessionVwapOptions<SeriesSchema, string>>({
    name: 'sessionVwap',
    family: 'session',
    summary: "Volume-weighted average price, reset at each session's open",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
      volume: { default: 'volume' },
    },
    params: {},
    naming: { output: 'svwap' },
    outputs: [{ id: '', unit: 'inherit' }],
    anchor: 'session',
    run: sessionVwap,
  }),

  defineStudy<PivotPointsOptions<SeriesSchema, string, PivotMethod>>({
    name: 'pivotPoints',
    family: 'session',
    summary: "The prior session's pivot with its support and resistance levels",
    inputs: {
      high: { default: 'high' },
      low: { default: 'low' },
      close: { default: 'close' },
    },
    params: {
      method: { kind: 'enum', default: 'standard', of: PIVOT_METHODS },
    },
    naming: { prefix: 'pp' },
    // The default method's columns; `'camarilla'` adds `R4` after `R3` and
    // `S4` after `S3` (see the study docstring).
    outputs: [
      { id: 'Pivot', unit: 'inherit' },
      { id: 'R1', unit: 'inherit' },
      { id: 'R2', unit: 'inherit' },
      { id: 'R3', unit: 'inherit' },
      { id: 'S1', unit: 'inherit' },
      { id: 'S2', unit: 'inherit' },
      { id: 'S3', unit: 'inherit' },
    ],
    anchor: 'session',
    run: pivotPoints,
  }),
];
