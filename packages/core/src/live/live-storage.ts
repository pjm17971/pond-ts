/**
 * `LiveStorage<S>` — the private storage-strategy layer behind
 * `LiveSeries`.
 *
 * `LiveSeries` owns every public-facing semantic: ordering policy
 * (`strict` / `drop` / `reorder`), grace-window checks, `stats()`
 * counters, the `event → retention → batch → evict` listener
 * ordering, and the rejected-counter. A `LiveStorage` implementation
 * owns only the **mechanics** of holding the buffer: how rows are
 * stored, how a point is read back, how the oldest N rows are
 * evicted, and how a snapshot is produced.
 *
 * Two implementations:
 *
 * - {@link EventArrayLiveStorage} — the row-oriented `Event[]`
 *   backing. Supports `insertSortedTrusted` (the `reorder` mode's
 *   sorted mid-stream insertion). This is the only backing used in
 *   the first storage-strategy PR (behavior-preserving extraction).
 *
 * - `RingLiveStorage` (added in the follow-up) — `ColumnarRingBuffer`
 *   backing for the append-only `strict` / `drop` modes. Skips the
 *   long-lived `Event` retention that drives GC pressure at high
 *   ingest rates. Does NOT support `insertSortedTrusted` (an
 *   append-only ring cannot splice mid-stream); `LiveSeries` only
 *   routes `reorder` mode to the array backing, so the ring backing
 *   never sees that call.
 *
 * The interface is intentionally small. Anything that can be
 * expressed in terms of `length` + `at(i)` + `keyAt(i)` lives in
 * `LiveSeries` (e.g. `find` / `some` / `every` / `bisect`), so the
 * storage surface stays minimal and each implementation stays
 * coherent.
 *
 * Framework-internal; not exported from `packages/core/src/index.ts`.
 */

import type { EventKey } from '../core/temporal.js';
import type {
  EventForSchema,
  RowForSchema,
  SeriesSchema,
} from '../schema/index.js';
import { Event } from '../core/event.js';
import { Time } from '../core/time.js';
import { TimeRange } from '../core/time-range.js';
import { TimeSeries } from '../batch/time-series.js';
import { ColumnarRingBuffer } from '../columnar/ring-buffer.js';
import { MAX_COLUMN_LENGTH } from '../columnar/validity.js';
import type { ColumnSchema } from '../columnar/types.js';

/**
 * Comparator used to order the live buffer. Delegates to
 * `EventKey.compare`, which orders:
 *   - Time / TimeRange: by begin / end / type
 *   - Interval: by begin / end / value (so two intervals with the
 *     same span but different values get a stable order)
 *
 * Must match the comparator the Tier 2 query primitives (`bisect`,
 * `includesKey`, `atOrBefore`, `atOrAfter`) use to search the
 * buffer — otherwise interval-keyed series can hold same-span
 * intervals in arrival order while bisect expects value-ascending
 * order, producing false-negative `includesKey` results. Codex
 * caught this on PR #125 review; the comparator now lives here so
 * both `LiveSeries` and the storage backings share one definition.
 */
export function compareKeys(a: EventKey, b: EventKey): number {
  return a.compare(b);
}

/**
 * Shared snapshot builder — both storage backings produce a
 * `TimeSeries<S>` the same way: walk the events into normalized
 * rows and run them through the public `TimeSeries` constructor.
 *
 * The ring backing materializes its events first (it stores
 * columns, not `Event`s); the array backing already holds them.
 * A future follow-up can give the ring a fast snapshot path
 * (`ColumnarRingBuffer.snapshot()` → a trusted `TimeSeries`
 * factory) — deferred so this PR doesn't widen the `TimeSeries`
 * surface. Both backings share this row-rebuild today.
 */
function buildSnapshot<S extends SeriesSchema>(
  name: string,
  schema: S,
  events: ReadonlyArray<EventForSchema<S>>,
): TimeSeries<S> {
  const rows = events.map((event) => {
    const row: unknown[] = [event.key()];
    for (let col = 1; col < schema.length; col += 1) {
      row.push(event.get((schema[col] as { name: string }).name));
    }
    return row;
  });
  return new TimeSeries({ name, schema, rows: rows as RowForSchema<S>[] });
}

/**
 * Private storage strategy behind `LiveSeries`. See the module
 * docstring for the layering contract.
 */
export interface LiveStorage<S extends SeriesSchema> {
  /** Current row count. */
  readonly length: number;

  /**
   * Event at logical index `i` (0-based, oldest first). Returns
   * `undefined` for any out-of-range index (negative or `>= length`).
   * The caller (`LiveSeries`) normalizes negative indices before
   * calling.
   */
  at(index: number): EventForSchema<S> | undefined;

  /**
   * Key at logical index `i`, for binary search. Returns `undefined`
   * for out-of-range indices.
   */
  keyAt(index: number): EventKey | undefined;

  /**
   * Begin timestamp (ms) at logical index `i`. Cheaper than `keyAt`
   * for the `maxAge` retention walk and the ordering comparison —
   * reads a primitive without materializing an `EventKey`. Returns
   * `undefined` for out-of-range indices.
   */
  beginAt(index: number): number | undefined;

  /** Last event (logical index `length - 1`), or `undefined` when empty. */
  last(): EventForSchema<S> | undefined;

  /**
   * Append an event at the tail. The caller guarantees the event's
   * key is `>=` the current last key (ordering policy is enforced by
   * `LiveSeries` before this is called).
   */
  appendTrusted(event: EventForSchema<S>): void;

  /**
   * Insert an event at its sorted position (the `reorder` mode's
   * mid-stream insertion). Only the array backing supports this; the
   * ring backing throws, since `LiveSeries` never routes `reorder`
   * mode to it.
   */
  insertSortedTrusted(event: EventForSchema<S>): void;

  /**
   * Drop the oldest `n` rows and return them as materialized events
   * (for the `evict` listener). `n` is computed by `LiveSeries`'s
   * retention policy and is always `<= length`. Use this only when an
   * `'evict'` listener will consume the result — otherwise prefer
   * {@link dropPrefix}, which skips materialization.
   */
  evictPrefix(n: number): ReadonlyArray<EventForSchema<S>>;

  /**
   * Drop the oldest `n` rows WITHOUT materializing them. The
   * retention hot path with no `'evict'` listener uses this — on the
   * ring backing, materializing evicted events (Time + data dict +
   * Event per row) just to discard them was the dominant cost. `n`
   * is always `<= length`.
   */
  dropPrefix(n: number): void;

  /**
   * Empty the buffer and return all events that were in it (for the
   * `evict` listener fired by `LiveSeries.clear()`).
   */
  clear(): ReadonlyArray<EventForSchema<S>>;

  /** Immutable snapshot of the current buffer as a `TimeSeries<S>`. */
  snapshot(name: string): TimeSeries<S>;
}

/**
 * `Event[]`-backed storage — the row-oriented backing that
 * `LiveSeries` used before the storage-strategy extraction. Supports
 * sorted mid-stream insertion (`reorder` mode).
 *
 * This is a behavior-preserving extraction: the array, the
 * binary-search-splice insertion, and the prefix-eviction all match
 * the pre-extraction `LiveSeries` internals exactly.
 */
export class EventArrayLiveStorage<
  S extends SeriesSchema,
> implements LiveStorage<S> {
  readonly #schema: S;
  #events: EventForSchema<S>[] = [];

  constructor(schema: S) {
    this.#schema = schema;
  }

  get length(): number {
    return this.#events.length;
  }

  at(index: number): EventForSchema<S> | undefined {
    return this.#events[index];
  }

  keyAt(index: number): EventKey | undefined {
    const event = this.#events[index];
    return event ? event.key() : undefined;
  }

  beginAt(index: number): number | undefined {
    const event = this.#events[index];
    return event ? event.begin() : undefined;
  }

  last(): EventForSchema<S> | undefined {
    return this.#events[this.#events.length - 1];
  }

  appendTrusted(event: EventForSchema<S>): void {
    this.#events.push(event);
  }

  insertSortedTrusted(event: EventForSchema<S>): void {
    // Binary search for the sorted insertion point (rightmost
    // position keeping the buffer non-decreasing by key). Matches
    // the pre-extraction `#insert` reorder branch.
    let lo = 0;
    let hi = this.#events.length;
    const key = event.key();
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (compareKeys(this.#events[mid]!.key(), key) <= 0) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    this.#events.splice(lo, 0, event);
  }

  evictPrefix(n: number): ReadonlyArray<EventForSchema<S>> {
    if (n <= 0) return [];
    return this.#events.splice(0, n);
  }

  dropPrefix(n: number): void {
    if (n <= 0) return;
    this.#events.splice(0, n);
  }

  clear(): ReadonlyArray<EventForSchema<S>> {
    const evicted = this.#events;
    this.#events = [];
    return evicted;
  }

  snapshot(name: string): TimeSeries<S> {
    return buildSnapshot(name, this.#schema, this.#events);
  }
}

/**
 * `ColumnarRingBuffer`-backed storage — the append-only backing for
 * `LiveSeries`'s `strict` / `drop` ordering modes. The structural
 * fix for the GC pressure that long-lived `Event` retention drives
 * at high ingest rates: pushed events are decomposed into the ring's
 * typed-array columns and then become garbage (no array retains
 * them), instead of living in the buffer until eviction.
 *
 * `Event`s are materialized lazily on `at(i)` and cached for
 * reference stability (`at(i) === at(i)`). The cache is remapped on
 * eviction so identity survives the logical-index shift.
 *
 * **Does NOT support `insertSortedTrusted`** — an append-only ring
 * cannot splice mid-stream. `LiveSeries` only selects this backing
 * for `strict` / `drop` modes (never `reorder`), so the throw is a
 * defensive internal-error guard, not a reachable path.
 *
 * **Scope:** `time` / `timeRange` keys only. Interval-keyed series
 * keep the array backing (the ring's interval label-kind inference
 * is deferred); `LiveSeries` routes accordingly.
 */
export class RingLiveStorage<S extends SeriesSchema> implements LiveStorage<S> {
  readonly #schema: S;
  readonly #keyKind: 'time' | 'timeRange';
  readonly #valueNames: ReadonlyArray<string>;
  readonly #ring: ColumnarRingBuffer<ColumnSchema>;
  /** Lazy event materialization cache, keyed by logical index. */
  #cache = new Map<number, EventForSchema<S>>();
  /** Reused positional value buffer for the trusted per-row append. */
  readonly #valueScratch: unknown[];

  constructor(schema: S) {
    this.#schema = schema;
    const keyKind = schema[0].kind;
    if (keyKind !== 'time' && keyKind !== 'timeRange') {
      // LiveSeries only routes time / timeRange keys here. Interval
      // keys keep the array backing (label-kind inference deferred).
      throw new Error(
        `RingLiveStorage: unsupported key kind '${keyKind}' (expected 'time' | 'timeRange')`,
      );
    }
    this.#keyKind = keyKind;
    this.#valueNames = schema.slice(1).map((c) => c.name);
    this.#valueScratch = new Array<unknown>(this.#valueNames.length);
    // Retention is decoupled from `LiveSeries`'s `maxEvents` —
    // `LiveSeries` orchestrates retention via `evictPrefix`, so the
    // ring's own cap is a high backstop that never fires in normal
    // operation (only a single push larger than MAX_COLUMN_LENGTH
    // would trip it, well past any realistic heap). lazyGrowth keeps
    // capacity tracking the actual length, not the backstop.
    this.#ring = new ColumnarRingBuffer(schema as unknown as ColumnSchema, {
      retention: MAX_COLUMN_LENGTH,
      lazyGrowth: true,
    });
  }

  get length(): number {
    return this.#ring.length;
  }

  at(index: number): EventForSchema<S> | undefined {
    if (index < 0 || index >= this.#ring.length) return undefined;
    let event = this.#cache.get(index);
    if (event === undefined) {
      event = this.#materializeAt(index);
      this.#cache.set(index, event);
    }
    return event;
  }

  keyAt(index: number): EventKey | undefined {
    if (index < 0 || index >= this.#ring.length) return undefined;
    const cached = this.#cache.get(index);
    if (cached !== undefined) return cached.key();
    return this.#keyAt(index);
  }

  beginAt(index: number): number | undefined {
    if (index < 0 || index >= this.#ring.length) return undefined;
    return this.#ring._beginAt(index);
  }

  last(): EventForSchema<S> | undefined {
    return this.at(this.#ring.length - 1);
  }

  appendTrusted(event: EventForSchema<S>): void {
    const before = this.#ring.length;
    for (let c = 0; c < this.#valueNames.length; c += 1) {
      this.#valueScratch[c] = event.get(this.#valueNames[c]!);
    }
    const begin = event.begin();
    const end = this.#keyKind === 'timeRange' ? event.end() : begin;
    this.#ring._appendRowTrusted(begin, end, undefined, this.#valueScratch);
    // Normally length grows by exactly 1 and no logical index shifts,
    // so the cache stays valid untouched. If the ring hit its
    // MAX_COLUMN_LENGTH backstop and evicted from the head, remap the
    // cache by the eviction count to preserve identity. (Effectively
    // unreachable in normal operation; correctness backstop only.)
    const evicted = before + 1 - this.#ring.length;
    if (evicted > 0) this.#shiftCacheBy(evicted);
  }

  insertSortedTrusted(_event: EventForSchema<S>): void {
    throw new Error(
      'RingLiveStorage.insertSortedTrusted: append-only ring cannot ' +
        'splice mid-stream; reorder mode must use EventArrayLiveStorage ' +
        '(internal routing error)',
    );
  }

  evictPrefix(n: number): ReadonlyArray<EventForSchema<S>> {
    if (n <= 0) return [];
    const evicted: EventForSchema<S>[] = new Array(n);
    for (let i = 0; i < n; i += 1) {
      evicted[i] = this.at(i)!;
    }
    this.#ring.evictPrefix(n);
    this.#shiftCacheBy(n);
    return evicted;
  }

  dropPrefix(n: number): void {
    if (n <= 0) return;
    // The retention hot path with no 'evict' listener: advance the
    // ring head and remap the cache, but DON'T materialize the
    // evicted events (Time + data dict + Event per row) just to throw
    // them away. This was the dominant cost in the first Step 7 bench.
    this.#ring.evictPrefix(n);
    this.#shiftCacheBy(n);
  }

  clear(): ReadonlyArray<EventForSchema<S>> {
    const len = this.#ring.length;
    if (len === 0) return [];
    const all: EventForSchema<S>[] = new Array(len);
    for (let i = 0; i < len; i += 1) all[i] = this.at(i)!;
    this.#ring.evictPrefix(len);
    this.#cache.clear();
    return all;
  }

  snapshot(name: string): TimeSeries<S> {
    const len = this.#ring.length;
    const events: EventForSchema<S>[] = new Array(len);
    for (let i = 0; i < len; i += 1) events[i] = this.at(i)!;
    return buildSnapshot(name, this.#schema, events);
  }

  /** Build the key at logical index `i` from the ring's key buffers. */
  #keyAt(i: number): EventKey {
    const begin = this.#ring._beginAt(i);
    if (this.#keyKind === 'time') {
      return new Time(begin) as unknown as EventKey;
    }
    return new TimeRange({
      start: begin,
      end: this.#ring._endAt(i),
    }) as unknown as EventKey;
  }

  /** Materialize a fresh `Event` at logical index `i` from the ring. */
  #materializeAt(i: number): EventForSchema<S> {
    const begin = this.#ring._beginAt(i);
    const key =
      this.#keyKind === 'time'
        ? new Time(begin)
        : new TimeRange({ start: begin, end: this.#ring._endAt(i) });
    const data: Record<string, unknown> = {};
    for (let c = 0; c < this.#valueNames.length; c += 1) {
      const name = this.#valueNames[c]!;
      data[name] = this.#ring._valueAt(i, name);
    }
    return new Event(key, data) as unknown as EventForSchema<S>;
  }

  /**
   * Drop cache entries for the `n` evicted head rows and shift the
   * survivors down by `n` (logical index `k` → `k - n`). Preserves
   * `at(i) === at(i)` across an eviction.
   */
  #shiftCacheBy(n: number): void {
    if (this.#cache.size === 0) return;
    const next = new Map<number, EventForSchema<S>>();
    for (const [index, event] of this.#cache) {
      if (index >= n) next.set(index - n, event);
    }
    this.#cache = next;
  }
}
