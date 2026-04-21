import { TimeSeries } from 'pond-ts';
import type { LiveSource, SeriesSchema } from 'pond-ts';

/**
 * Build a `TimeSeries` snapshot from any `LiveSource`.
 *
 * Checks for dedicated snapshot methods first (`toTimeSeries`, `snapshot`),
 * then falls back to iterating `at()` + `length` for sources like
 * `LiveRollingAggregation` that lack a built-in snapshot.
 */
export function takeSnapshot<S extends SeriesSchema>(
  source: LiveSource<S>,
): TimeSeries<S> | null {
  if (source.length === 0) return null;

  // LiveSeries, LiveView
  if (
    'toTimeSeries' in source &&
    typeof (source as Record<string, unknown>).toTimeSeries === 'function'
  ) {
    return (
      source as unknown as { toTimeSeries(): TimeSeries<S> }
    ).toTimeSeries();
  }

  // LiveAggregation
  if (
    'snapshot' in source &&
    typeof (source as Record<string, unknown>).snapshot === 'function'
  ) {
    return (source as unknown as { snapshot(): TimeSeries<S> }).snapshot();
  }

  // Fallback: reconstruct rows from LiveSource interface
  const schema = source.schema;
  const rows: unknown[][] = [];
  for (let i = 0; i < source.length; i++) {
    const event = source.at(i);
    if (!event) continue;
    const row: unknown[] = [event.key()];
    for (let col = 1; col < schema.length; col++) {
      row.push(event.get((schema[col] as { name: string }).name));
    }
    rows.push(row);
  }

  if (rows.length === 0) return null;

  return new TimeSeries({
    name: source.name,
    schema,
    rows: rows as any,
  });
}
