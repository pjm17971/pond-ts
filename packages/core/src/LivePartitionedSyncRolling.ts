import { Event } from './Event.js';
import { Time } from './Time.js';
import { resolveReducer, type RollingReducerState } from './reducers/index.js';
import {
  bucketIndexFor,
  boundaryTimestampFor,
  type ClockTrigger,
} from './triggers.js';
import type { RollingWindow } from './LiveRollingAggregation.js';
import { parseDuration } from './utils/duration.js';
import type {
  AggregateMap,
  ColumnValue,
  EventForSchema,
  LiveSource,
  SeriesSchema,
} from './types.js';

type ColumnSpec = {
  source: string;
  reducer: string;
  kind: string;
};

type WindowEntry = {
  index: number;
  timestamp: number;
  values: (ColumnValue | undefined)[];
};

type PartitionState = {
  states: RollingReducerState[];
  entries: WindowEntry[];
  nextIndex: number;
};

type EventListener = (event: any) => void;

/**
 * A `LiveSource<Out>` produced by `LivePartitionedSeries.rolling(window, mapping, { trigger: Trigger.clock(...) })`.
 * Maintains a rolling-window aggregation per partition and emits a
 * **synchronised burst of events on every clock-trigger boundary
 * crossing**: when any partition's event crosses the boundary, every
 * known partition's rolling-window snapshot fires at the same instant.
 *
 * Output schema is `[time, <partitionColumn>, ...mappingColumns]` —
 * the partition column is added automatically so each emitted row
 * carries the partition tag for downstream consumers to rebucket on.
 *
 * **Internal** — no public class name. The public API surface is
 * `LiveSource<Out>`. Constructed via `LivePartitionedSeries.rolling`'s
 * trigger-bearing overload; user code never imports this class.
 */
export class LivePartitionedSyncRolling<
  S extends SeriesSchema,
  K extends string,
  Out extends SeriesSchema,
> implements LiveSource<Out> {
  readonly name: string;
  readonly schema: Out;

  readonly #byColumn: string;
  readonly #columns: ColumnSpec[];
  readonly #trigger: ClockTrigger;
  readonly #windowMs: number | undefined;
  readonly #windowCount: number | undefined;
  readonly #minSamples: number;

  readonly #partitionStates: Map<K, PartitionState>;
  /**
   * Partition keys in observation order — used as the stable iteration
   * order when emitting per-tick frames. If `groups` was provided
   * upstream, those keys are pre-seeded in declared order so emission
   * is deterministic across runs even before any events arrive.
   */
  readonly #partitionOrder: K[];
  #lastBucketIdx: number | undefined;

  readonly #outputEvents: EventForSchema<Out>[];
  readonly #onEvent: Set<EventListener>;

  constructor(
    upstreamName: string,
    upstreamSchema: S,
    byColumn: string,
    window: RollingWindow,
    mapping: AggregateMap<S>,
    trigger: ClockTrigger,
    options: { minSamples?: number; declaredGroups?: ReadonlyArray<K> } = {},
  ) {
    this.name = upstreamName;
    this.#byColumn = byColumn;
    this.#trigger = trigger;
    this.#minSamples = options.minSamples ?? 0;
    if (!Number.isInteger(this.#minSamples) || this.#minSamples < 0) {
      throw new TypeError(
        'rolling minSamples must be a non-negative integer (default 0)',
      );
    }

    if (typeof window === 'number' && Number.isInteger(window) && window > 0) {
      this.#windowMs = undefined;
      this.#windowCount = window;
    } else {
      this.#windowMs =
        typeof window === 'string' ? parseDuration(window) : undefined;
      if (this.#windowMs === undefined && typeof window === 'number') {
        throw new TypeError(
          'window must be a positive integer (event count) or duration string',
        );
      }
      this.#windowCount = undefined;
    }

    // Resolve the rolling output columns from `mapping`.
    const colsByName = new Map(
      upstreamSchema.slice(1).map((c) => [c.name, c] as const),
    );
    this.#columns = [];
    for (const [name, reducer] of Object.entries(
      mapping as Record<string, string>,
    )) {
      const col = colsByName.get(name);
      if (!col) throw new TypeError(`unknown column '${name}'`);
      const outputKind = resolveReducer(reducer).outputKind;
      const kind =
        outputKind === 'number'
          ? 'number'
          : outputKind === 'array'
            ? 'array'
            : col.kind;
      this.#columns.push({ source: name, reducer, kind });
    }

    // Locate the partition column's kind in the upstream schema.
    const byCol = upstreamSchema.find((c) => c.name === byColumn);
    if (!byCol) {
      throw new TypeError(
        `LivePartitionedSyncRolling: column '${byColumn}' not in upstream schema`,
      );
    }

    // Output schema: [time, <byColumn>, ...mappingColumns].
    this.schema = Object.freeze([
      { name: 'time', kind: 'time' },
      { name: byColumn, kind: byCol.kind, required: false },
      ...this.#columns.map((c) => ({
        name: c.source,
        kind: c.kind,
        required: false,
      })),
    ]) as unknown as Out;

    this.#partitionStates = new Map();
    this.#partitionOrder = [];
    this.#lastBucketIdx = undefined;
    this.#outputEvents = [];
    this.#onEvent = new Set();

    if (options.declaredGroups) {
      for (const k of options.declaredGroups) {
        this.#ensurePartition(k);
      }
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

  on(type: 'event', fn: EventListener): () => void {
    if (type !== 'event') {
      throw new TypeError(
        `LivePartitionedSyncRolling.on: unsupported event type '${String(type)}'`,
      );
    }
    this.#onEvent.add(fn);
    return () => {
      this.#onEvent.delete(fn);
    };
  }

  // ── Wiring entry point ──────────────────────────────────────

  /**
   * Called by `LivePartitionedSeries` for each event arriving on a
   * partition's `LiveSource`. Updates that partition's rolling window
   * state and, if the bucket index advances, emits a synchronised
   * burst of one event per known partition at the new boundary
   * timestamp.
   */
  ingest(partitionKey: K, event: EventForSchema<S>): void {
    const state = this.#ensurePartition(partitionKey);
    const data = event.data() as Record<string, ColumnValue | undefined>;
    const values = this.#columns.map((c) => data[c.source]);
    const index = state.nextIndex++;
    const ts = event.begin();
    const entry: WindowEntry = { index, timestamp: ts, values };

    for (let i = 0; i < this.#columns.length; i++) {
      state.states[i]!.add(index, values[i]);
    }
    state.entries.push(entry);
    this.#evictPartition(state, ts);

    const bucketIdx = bucketIndexFor(this.#trigger, ts);
    if (this.#lastBucketIdx === undefined) {
      // First event — establish the starting bucket; no emission yet.
      this.#lastBucketIdx = bucketIdx;
      return;
    }
    if (bucketIdx > this.#lastBucketIdx) {
      this.#emitTick(bucketIdx);
      this.#lastBucketIdx = bucketIdx;
    }
  }

  // ── Internal ────────────────────────────────────────────────

  #ensurePartition(key: K): PartitionState {
    let state = this.#partitionStates.get(key);
    if (state) return state;
    state = {
      states: this.#columns.map((c) =>
        resolveReducer(c.reducer).rollingState(),
      ),
      entries: [],
      nextIndex: 0,
    };
    this.#partitionStates.set(key, state);
    this.#partitionOrder.push(key);
    return state;
  }

  #evictPartition(state: PartitionState, latestTs: number): void {
    if (this.#windowMs !== undefined) {
      const cutoff = latestTs - this.#windowMs;
      while (state.entries.length > 0 && state.entries[0]!.timestamp < cutoff) {
        const entry = state.entries.shift()!;
        for (let i = 0; i < this.#columns.length; i++) {
          state.states[i]!.remove(entry.index, entry.values[i]);
        }
      }
    }
    if (this.#windowCount !== undefined) {
      while (state.entries.length > this.#windowCount) {
        const entry = state.entries.shift()!;
        for (let i = 0; i < this.#columns.length; i++) {
          state.states[i]!.remove(entry.index, entry.values[i]);
        }
      }
    }
  }

  /**
   * Walk every known partition (in observation / declared-groups
   * order), emit one row per partition keyed at the new bucket's
   * boundary timestamp. All emitted events share the same `ts`.
   *
   * Hot path: hoists invariants (column count, listener iterable,
   * byColumn name) out of the per-partition loop, uses an indexed
   * for over `partitionOrder` (cheaper than for-of), and constructs
   * the record object via a plain assignment rather than a computed-
   * property literal (which V8 deopts at scale).
   */
  #emitTick(bucketIdx: number): void {
    const boundaryMs = boundaryTimestampFor(this.#trigger, bucketIdx);
    const time = new Time(boundaryMs);
    const order = this.#partitionOrder;
    const states = this.#partitionStates;
    const cols = this.#columns;
    const colsLen = cols.length;
    const byCol = this.#byColumn;
    const minSamples = this.#minSamples;
    const out = this.#outputEvents;
    const listeners = this.#onEvent;
    const orderLen = order.length;

    for (let p = 0; p < orderLen; p++) {
      const key = order[p]!;
      const state = states.get(key)!;
      const warmup = state.entries.length < minSamples;
      const record: Record<string, ColumnValue | undefined> = {};
      record[byCol] = key;
      for (let i = 0; i < colsLen; i++) {
        record[cols[i]!.source] = warmup
          ? undefined
          : state.states[i]!.snapshot();
      }
      const evt = new Event(time, record) as unknown as EventForSchema<Out>;
      out.push(evt);
      if (listeners.size > 0) {
        for (const fn of listeners) fn(evt);
      }
    }
  }
}
