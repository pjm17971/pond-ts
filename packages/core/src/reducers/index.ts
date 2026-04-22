export type {
  ReducerDef,
  AggregateBucketState,
  RollingReducerState,
} from './types.js';

import type { ReducerDef } from './types.js';
import { parsePercentile, percentileReducer } from './percentile.js';

import { count } from './count.js';
import { sum } from './sum.js';
import { avg } from './avg.js';
import { min } from './min.js';
import { max } from './max.js';
import { first } from './first.js';
import { last } from './last.js';
import { median } from './median.js';
import { stdev } from './stdev.js';
import { difference } from './difference.js';
import { keep } from './keep.js';
import { unique } from './unique.js';

const registry: Record<string, ReducerDef> = {
  count,
  sum,
  avg,
  min,
  max,
  first,
  last,
  median,
  stdev,
  difference,
  keep,
  unique,
};

export function resolveReducer(operation: string): ReducerDef {
  const r = registry[operation];
  if (r) return r;
  const q = parsePercentile(operation);
  if (q !== undefined) return percentileReducer(q);
  throw new TypeError(`unsupported aggregate reducer: ${operation}`);
}
