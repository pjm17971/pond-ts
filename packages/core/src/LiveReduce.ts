import {
  normalizeAggregateColumns,
  type AggregateColumnSpec,
} from './aggregate-columns.js';
import { Event } from './Event.js';
import { Time } from './Time.js';
import { rollingStateFor, type RollingReducerState } from './reducers/index.js';
import {
  bucketIndexFor,
  boundaryTimestampFor,
  type Trigger,
  type ClockTrigger,
} from './triggers.js';
import {
  EMITS_EVICT,
  type AggregateMap,
  type AggregateOutputMap,
  type ColumnValue,
  type EventForSchema,
  type LiveSource,
  type SeriesSchema,
} from './types.js';
import type { LiveRollingOptions } from './LiveRollingAggregation.js';

type EventListener = (event: any) => void;

/**
 * "Reduce over the whole current buffer." `LiveReduce` is the
 * streaming counterpart to batch `series.reduce(mapping)` — same
 * mapping shape, but reactive: every push to the source updates
 * the reducer state via `add`, every retention eviction updates
 * via `remove`. The snapshot at any moment is the reduction over
 * "what's currently retained."
 *
 * **Why not sugar over `LiveFusedRolling`?** The fused-rolling
 * primitive requires a time-based window and maintains its own
 * deque. `LiveReduce`'s "window" is *whatever's in the source's
 * buffer right now* — driven by retention, not a duration. That
 * works for any retention shape (`maxAge`, `maxEvents`, both,
 * neither) without forcing a sentinel resolution.
 *
 * **Output stream-shape: a `LiveSource<Out>`.** One emitted event
 * per trigger fire, keyed at the latest source event's key. The
 * output schema is `[time, ...mappingColumns]` — same shape as
 * `LiveRollingAggregation` for consistency. Composes with the rest
 * of the live operator surface.
 *
 * **Trigger semantics match `LiveRollingAggregation`'s.** The
 * trigger fires on each source `'event'` ingest (default
 * `Trigger.event()`), per-N-events (`Trigger.count(n)`), or per
 * data-clock boundary crossing (`Trigger.every(...)`). Source
 * eviction is independent — it drives reducer-state removes but
 * does not itself fire the trigger.
 *
 * **Retention lag.** A push that triggers retention fires
 * `'event'` for the new event THEN `'evict'` for the dropped
 * events. If the trigger fires per-event, the snapshot may
 * briefly include events about to be evicted (between `'event'`
 * and `'evict'` callbacks). Acceptable for typical use; for
 * post-retention precision use a clock trigger with cadence
 * longer than the retention overflow rate.
 *
 * **Construction-time replay.** Sources with existing buffer
 * content at construction (e.g. a `LiveSeries` constructed from
 * a snapshot) replay each existing event through `#ingest`. With
 * `Trigger.event()` (the default) this emits N immediate output
 * events — same as `LiveRollingAggregation`'s replay shape. Use
 * a clock or count trigger if you want to suppress the
 * construction-time burst.
 *
 * **Source contract — `EMITS_EVICT` is load-bearing.** This
 * class's reducer state stays in sync with the source's current
 * buffer because it removes events as the source evicts them.
 * The `'evict'` subscription is gated on the `EMITS_EVICT`
 * symbol marker. Sources that *evict internally* but do NOT emit
 * `'evict'` would cause `LiveReduce`'s state to grow without
 * bound (no removes ever fire). Today every pond LiveSource that
 * evicts also marks itself with `EMITS_EVICT` (`LiveSeries`,
 * `LiveView` with eviction); future LiveSource implementations
 * must preserve this contract.
 *
 * Public API: constructed via `live.reduce(mapping, opts?)` on
 * `LiveSeries` / `LiveView`. User code doesn't import this class
 * directly.
 */
export class LiveReduce<
  S extends SeriesSchema,
  Out extends SeriesSchema = SeriesSchema,
> implements LiveSource<Out> {
  readonly name: string;
  readonly schema: Out;

  readonly #columns: AggregateColumnSpec[];
  readonly #states: RollingReducerState[];

  /**
   * Map from source `Event` reference → absolute index used in the
   * reducer state. Set on `'event'` (add), looked up on `'evict'`
   * (remove). WeakMap so the source's eviction releases the
   * reference.
   */
  readonly #eventToAbsIdx: WeakMap<EventForSchema<S>, number>;
  #nextAbsIdx: number;

  readonly #trigger: Trigger;
  #lastClockBucketIdx: number | undefined;
  #countSinceLastEmit: number;

  readonly #outputEvents: EventForSchema<Out>[];
  readonly #onEvent: Set<EventListener>;
  readonly #unsubscribeEvent: () => void;
  readonly #unsubscribeEvict: (() => void) | undefined;
  #disposed: boolean;

  constructor(
    source: LiveSource<S>,
    mapping: AggregateMap<S> | AggregateOutputMap<S>,
    options: LiveRollingOptions = {},
  ) {
    this.name = source.name;
    this.#trigger = options.trigger ?? { kind: 'event' };
    this.#lastClockBucketIdx = undefined;
    this.#countSinceLastEmit = 0;
    this.#nextAbsIdx = 0;
    this.#eventToAbsIdx = new WeakMap();
    this.#outputEvents = [];
    this.#onEvent = new Set();
    this.#disposed = false;

    // Reuse the same column-normalization helper as the rest of
    // the live aggregation surface; keeps `LiveReduce`'s reducer
    // semantics identical to `aggregate` / `rolling`.
    this.#columns = normalizeAggregateColumns(
      source.schema,
      mapping as AggregateMap<SeriesSchema> | AggregateOutputMap<SeriesSchema>,
    );
    this.#states = this.#columns.map((c) => rollingStateFor(c.reducer));

    // Output schema: source's first (time/keyed) column + each
    // reducer's output column. Matches LiveRollingAggregation's
    // schema shape for consistency.
    this.schema = Object.freeze([
      source.schema[0],
      ...this.#columns.map((c) => ({
        name: c.output,
        kind: c.kind,
        required: false,
      })),
    ]) as unknown as Out;

    // Replay existing buffer events through the same ingest path
    // so `LiveReduce` over a non-empty source matches the
    // streaming-from-construction shape.
    for (let i = 0; i < source.length; i++) {
      this.#ingest(source.at(i)!);
    }

    // Subscribe to source for forward events. The 'event' callback
    // fires per source event (post-insert, pre-retention); 'evict'
    // fires after retention has run, with the dropped events. Both
    // are wired so reducer state stays in sync with the source's
    // current buffer.
    this.#unsubscribeEvent = source.on('event', (event) => {
      this.#ingest(event);
    });
    // Same duck-typing pattern as LiveView's evict subscription:
    // `LiveSource.on()` only declares `'event'`, but sources marked
    // with `EMITS_EVICT` also support `'evict'`. Other LiveSource
    // impls (LiveAggregation, LiveRollingAggregation) silently route
    // unknown event types to other listener sets, so we must guard.
    if (EMITS_EVICT in source) {
      this.#unsubscribeEvict = (source as any).on(
        'evict',
        (evicted: ReadonlyArray<EventForSchema<S>>) => {
          for (const ev of evicted) this.#evictOne(ev);
        },
      );
    }
  }

  // ── LiveSource<Out> contract ────────────────────────────────

  get length(): number {
    return this.#outputEvents.length;
  }

  at(index: number): EventForSchema<Out> | undefined {
    if (index < 0) index = this.#outputEvents.length + index;
    return this.#outputEvents[index];
  }

  /**
   * Read the current reducer snapshot — every output column's
   * current value, computed over the source's current buffer. Cheap
   * O(reducers) — each reducer's `snapshot()` is O(1) for built-ins.
   */
  value(): Record<string, ColumnValue | undefined> {
    const result: Record<string, ColumnValue | undefined> = {};
    for (let i = 0; i < this.#columns.length; i++) {
      result[this.#columns[i]!.output] = this.#states[i]!.snapshot();
    }
    return result;
  }

  on(type: 'event', fn: EventListener): () => void {
    if (type !== 'event') {
      throw new TypeError(
        `LiveReduce.on: unsupported event type '${String(type)}'`,
      );
    }
    this.#onEvent.add(fn);
    return () => {
      this.#onEvent.delete(fn);
    };
  }

  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.#unsubscribeEvent();
    this.#unsubscribeEvict?.();
  }

  // ── Private ─────────────────────────────────────────────────

  #ingest(event: EventForSchema<S>): void {
    if (this.#disposed) return;
    const absIdx = this.#nextAbsIdx++;
    this.#eventToAbsIdx.set(event, absIdx);
    const data = event.data() as Record<string, ColumnValue | undefined>;
    for (let i = 0; i < this.#columns.length; i++) {
      this.#states[i]!.add(absIdx, data[this.#columns[i]!.source]);
    }

    // Trigger emission. Source eviction happens AFTER this
    // callback returns; the emitted snapshot may briefly include
    // events about to be evicted. See class JSDoc.
    switch (this.#trigger.kind) {
      case 'event':
        this.#emitEvent(event.key());
        return;
      case 'clock':
        this.#emitClock(event.begin(), this.#trigger);
        return;
      case 'count':
        this.#emitCount(event.key(), this.#trigger.n);
        return;
    }
  }

  #evictOne(event: EventForSchema<S>): void {
    if (this.#disposed) return;
    const absIdx = this.#eventToAbsIdx.get(event);
    if (absIdx === undefined) return; // event predated this LiveReduce
    this.#eventToAbsIdx.delete(event);
    const data = event.data() as Record<string, ColumnValue | undefined>;
    for (let i = 0; i < this.#columns.length; i++) {
      this.#states[i]!.remove(absIdx, data[this.#columns[i]!.source]);
    }
    // Eviction does NOT fire the trigger — only ingest does. This
    // matches `LiveRollingAggregation`'s pattern, where evictions
    // are silent state updates.
  }

  #emitCount(key: any, n: number): void {
    this.#countSinceLastEmit++;
    if (this.#countSinceLastEmit < n) return;
    this.#countSinceLastEmit = 0;
    this.#emitEvent(key);
  }

  #emitEvent(key: any): void {
    const record: Record<string, ColumnValue | undefined> = {};
    for (let i = 0; i < this.#columns.length; i++) {
      record[this.#columns[i]!.output] = this.#states[i]!.snapshot();
    }
    const outputEvent = new Event(
      key,
      record,
    ) as unknown as EventForSchema<Out>;
    this.#outputEvents.push(outputEvent);
    for (const fn of this.#onEvent) fn(outputEvent);
  }

  #emitClock(eventTs: number, trigger: ClockTrigger): void {
    const bucketIdx = bucketIndexFor(trigger, eventTs);
    if (this.#lastClockBucketIdx === undefined) {
      this.#lastClockBucketIdx = bucketIdx;
      return;
    }
    if (bucketIdx > this.#lastClockBucketIdx) {
      const boundaryMs = boundaryTimestampFor(trigger, bucketIdx);
      this.#emitEvent(new Time(boundaryMs));
      this.#lastClockBucketIdx = bucketIdx;
    }
  }
}
