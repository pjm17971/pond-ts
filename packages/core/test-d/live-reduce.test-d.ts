/**
 * Type-level tests for `LiveSeries.reduce(mapping)`. Verifies that
 * the output schema narrows the same way `LiveRollingAggregation`'s
 * does — `event.get(col)` resolves to the right type per reducer.
 */
import {
  LiveSeries,
  type LiveReduce,
  type EventForSchema,
} from '../src/index.js';

const schema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

const live = new LiveSeries({ name: 'metrics', schema });

// AggregateMap form: `{ cpu: 'avg' }` produces output column `cpu`
// with kind 'number'.
const r1 = live.reduce({ cpu: 'avg' });
type R1 = typeof r1;
declare const e1: EventForSchema<R1['schema']>;
const v1: number | undefined = e1.get('cpu');
void v1;

// AggregateOutputMap form: explicit alias + reducer.
const r2 = live.reduce({
  cpu_avg: { from: 'cpu', using: 'avg' },
  cpu_max: { from: 'cpu', using: 'max' },
  n: { from: 'cpu', using: 'count' },
});
declare const e2: EventForSchema<(typeof r2)['schema']>;
const v2_avg: number | undefined = e2.get('cpu_avg');
const v2_max: number | undefined = e2.get('cpu_max');
const v2_n: number | undefined = e2.get('n');
void v2_avg;
void v2_max;
void v2_n;

// Returns LiveReduce<S, Out> — composable as a LiveSource.
const _typeCheck: LiveReduce<typeof schema, R1['schema']> = r1;
void _typeCheck;
