import type { ScalarValue } from '../types.js';

/**
 * Incremental state for a single column within one aggregation bucket.
 * Created fresh per bucket. Values are fed in via `add()` as events are
 * assigned to the bucket; `snapshot()` reads the current result without
 * consuming the state.
 */
export type AggregateBucketState = {
  add(value: ScalarValue | undefined): void;
  snapshot(): ScalarValue | undefined;
};

/**
 * Incremental state for a single column within a sliding rolling window.
 * Unlike `AggregateBucketState`, this must also support `remove()` so
 * values can leave the window efficiently. `index` is a monotonically
 * increasing event counter used to identify which value to evict.
 */
export type RollingReducerState = {
  add(index: number, value: ScalarValue | undefined): void;
  remove(index: number, value: ScalarValue | undefined): void;
  snapshot(): ScalarValue | undefined;
};

/**
 * Complete definition of a named reducer. Every built-in reducer (sum,
 * avg, median, etc.) implements this interface. Each definition provides
 * three capabilities:
 *
 * - `reduce` — batch reduction over a materialized array of values.
 *   Used by `TimeSeries.reduce()`.
 *
 * - `bucketState` — factory for incremental bucket state. Used by
 *   `TimeSeries.aggregate()` where events stream into buckets one at a
 *   time. Only needs `add` + `snapshot` (values never leave a bucket).
 *
 * - `rollingState` — factory for incremental sliding-window state. Used
 *   by `TimeSeries.rolling()` where events both enter and leave the
 *   window. Must support `add`, `remove`, and `snapshot`.
 *
 * `outputKind` tells the aggregate schema builder whether the reducer
 * always produces a number (`'number'`) or preserves the source column
 * type (`'source'`). Numeric reducers like sum and avg use `'number'`;
 * type-agnostic reducers like first, last, and keep use `'source'`.
 */
export type ReducerDef = {
  outputKind: 'number' | 'source';

  /**
   * Batch reduce over a complete value array. `defined` contains all
   * non-undefined values (any type); `numeric` contains only the number
   * values. Both are pre-filtered from the raw event data — reducers do
   * not need to filter themselves.
   */
  reduce(
    defined: ReadonlyArray<ScalarValue>,
    numeric: ReadonlyArray<number>,
  ): ScalarValue | undefined;

  /** Return a fresh incremental state for one aggregation bucket. */
  bucketState(): AggregateBucketState;

  /** Return a fresh incremental state for one rolling window column. */
  rollingState(): RollingReducerState;
};
