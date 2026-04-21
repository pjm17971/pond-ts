import { useMemo } from 'react';
import { TimeSeries } from 'pond-ts';
import type { SeriesSchema } from 'pond-ts';

/**
 * Memoized `TimeSeries.fromJSON(...)` for static or fetched data.
 *
 * Re-parses only when `key` changes. If no `key` is provided, the input is
 * serialized via `JSON.stringify` as the cache key — fine for small to
 * moderate payloads. For large datasets, pass an explicit `key` (e.g. a fetch
 * URL or ETag) to avoid the serialization cost.
 */
export function useTimeSeries<
  S extends SeriesSchema,
  I extends Parameters<typeof TimeSeries.fromJSON<S>>[0],
>(input: I, key?: string): TimeSeries<S> {
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const cacheKey = key ?? JSON.stringify(input);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  return useMemo(() => TimeSeries.fromJSON<S>(input), [cacheKey]);
}
