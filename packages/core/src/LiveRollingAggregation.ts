import { Event } from './Event.js';
import { Time } from './Time.js';
import { LiveAggregation } from './LiveAggregation.js';
import {
  LiveView,
  makeDiffView,
  makeFillView,
  makeCumulativeView,
  type LiveFillMapping,
  type LiveFillStrategy,
} from './LiveView.js';
import { resolveReducer, type RollingReducerState } from './reducers/index.js';
import type { Sequence } from './Sequence.js';
import {
  bucketIndexFor,
  boundaryTimestampFor,
  type Trigger,
  type ClockTrigger,
} from './triggers.js';
import type {
  AggregateMap,
  DiffSchema,
  EventDataForSchema,
  EventForSchema,
  LiveSource,
  NumericColumnNameForSchema,
  SelectSchema,
  SeriesSchema,
  ColumnValue,
} from './types.js';

import type { DurationInput } from './utils/duration.js';
import { parseDuration } from './utils/duration.js';

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

type UpdateListener = (value: Record<string, ColumnValue | undefined>) => void;
type EventListener = (event: any) => void;

export type RollingWindow = DurationInput | number;

export type LiveRollingOptions = {
  /**
   * Suppress output until the window contains at least this many
   * source events; below the threshold every reducer column emits
   * `undefined`. Defaults to `0` (no gate). For count-based windows
   * (`window: number`), `minSamples > window` means the gate never
   * opens — output is `undefined` forever.
   */
  minSamples?: number;

  /**
   * Emission cadence. Defaults to `Trigger.event()` — emits one
   * snapshot per source event push (the historical behavior).
   *
   * Pass `Trigger.clock(Sequence.every('30s'))` to switch to
   * sequence-triggered emission: one snapshot fires when a source
   * event crosses an epoch-aligned boundary of the sequence; output
   * timestamps are the boundary instants. If no events arrive during
   * an interval, no event is emitted (data-driven, not wall-clock-
   * driven).
   *
   * For partitioned rollings (`live.partitionBy(col).rolling(...)`),
   * a clock trigger emits **synchronised across partitions**: when
   * any partition's event crosses the boundary, every partition's
   * rolling-window snapshot fires at the same instant. See
   * {@link Trigger} for the full trigger taxonomy.
   *
   * @experimental Trigger types beyond `clock` and `event` are
   *   reserved for future expansion (`count`, `custom`, compound
   *   triggers). The current API surface is locked at these two for
   *   the experimental release.
   */
  trigger?: Trigger;
};

export class LiveRollingAggregation<
  S extends SeriesSchema,
  Out extends SeriesSchema = SeriesSchema,
> {
  readonly name: string;
  readonly schema: Out;

  readonly #columns: ColumnSpec[];
  readonly #states: RollingReducerState[];
  readonly #entries: WindowEntry[];

  readonly #windowMs: number | undefined;
  readonly #windowCount: number | undefined;
  readonly #minSamples: number;
  #nextIndex: number;

  /**
   * The configured trigger. Stored as a strict union; emission paths
   * dispatch on `kind`.
   */
  readonly #trigger: Trigger;
  /**
   * For clock triggers: the bucket index of the most recently
   * crossed boundary. Undefined until the first event is ingested
   * (the first event establishes the starting bucket; emission begins
   * on the next crossing).
   */
  #lastClockBucketIdx: number | undefined;

  readonly #outputEvents: any[];
  readonly #onUpdate: Set<UpdateListener>;
  readonly #onEvent: Set<EventListener>;
  readonly #unsubscribe: () => void;

  constructor(
    source: LiveSource<S>,
    window: RollingWindow,
    mapping: AggregateMap<S>,
    options: LiveRollingOptions = {},
  ) {
    this.name = source.name;
    const minSamples = options.minSamples ?? 0;
    if (!Number.isInteger(minSamples) || minSamples < 0) {
      throw new TypeError(
        'rolling minSamples must be a non-negative integer (default 0)',
      );
    }
    this.#minSamples = minSamples;
    this.#trigger = options.trigger ?? { kind: 'event' };
    this.#lastClockBucketIdx = undefined;

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

    const colsByName = new Map(
      source.schema.slice(1).map((c) => [c.name, c] as const),
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

    this.schema = Object.freeze([
      source.schema[0],
      ...this.#columns.map((c) => ({
        name: c.source,
        kind: c.kind,
        required: false,
      })),
    ]) as unknown as Out;

    this.#states = this.#columns.map((c) =>
      resolveReducer(c.reducer).rollingState(),
    );
    this.#entries = [];
    this.#nextIndex = 0;
    this.#outputEvents = [];
    this.#onUpdate = new Set();
    this.#onEvent = new Set();

    for (let i = 0; i < source.length; i++) {
      this.#ingest(source.at(i)!);
    }

    this.#unsubscribe = source.on('event', (event) => {
      this.#ingest(event);
      const val = this.value();
      for (const fn of this.#onUpdate) fn(val);
    });
  }

  get length(): number {
    return this.#outputEvents.length;
  }

  at(index: number): EventForSchema<Out> | undefined {
    if (index < 0) index = this.#outputEvents.length + index;
    return this.#outputEvents[index];
  }

  value(): Record<string, ColumnValue | undefined> {
    const result: Record<string, ColumnValue | undefined> = {};
    const warmup = this.#entries.length < this.#minSamples;
    for (let i = 0; i < this.#columns.length; i++) {
      result[this.#columns[i]!.source] = warmup
        ? undefined
        : this.#states[i]!.snapshot();
    }
    return result;
  }

  get windowSize(): number {
    return this.#entries.length;
  }

  on(type: 'event', fn: EventListener): () => void;
  on(type: 'update', fn: UpdateListener): this;
  on(
    type: 'event' | 'update',
    fn: EventListener | UpdateListener,
  ): this | (() => void) {
    if (type === 'event') {
      this.#onEvent.add(fn as EventListener);
      return () => {
        this.#onEvent.delete(fn as EventListener);
      };
    }
    this.#onUpdate.add(fn as UpdateListener);
    return this;
  }

  // ── View transforms ─────────────────────────────────────────

  filter(predicate: (event: EventForSchema<Out>) => boolean): LiveView<Out> {
    return new LiveView(this as any, (event: any) =>
      predicate(event) ? event : undefined,
    );
  }

  map(fn: (event: EventForSchema<Out>) => EventForSchema<Out>): LiveView<Out> {
    return new LiveView(this as any, fn as any);
  }

  select<const Keys extends readonly (keyof EventDataForSchema<Out>)[]>(
    ...keys: Keys
  ): LiveView<SelectSchema<Out, Keys[number] & string>> {
    const newSchema = Object.freeze([
      this.schema[0]!,
      ...this.schema.slice(1).filter((c) => keys.includes(c.name as any)),
    ]) as unknown as SelectSchema<Out, Keys[number] & string>;

    return new LiveView(this as any, (event: any) => event.select(...keys), {
      schema: newSchema as any,
    }) as any;
  }

  window(size: RollingWindow): LiveView<Out> {
    return new LiveView(this as any, (event: any) => event).window(size) as any;
  }

  diff<const Target extends NumericColumnNameForSchema<Out>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): LiveView<DiffSchema<Out, Target>> {
    return makeDiffView(this as any, 'diff', columns, options);
  }

  rate<const Target extends NumericColumnNameForSchema<Out>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): LiveView<DiffSchema<Out, Target>> {
    return makeDiffView(this as any, 'rate', columns, options);
  }

  pctChange<const Target extends NumericColumnNameForSchema<Out>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): LiveView<DiffSchema<Out, Target>> {
    return makeDiffView(this as any, 'pctChange', columns, options);
  }

  fill(
    strategy: LiveFillStrategy | LiveFillMapping<Out>,
    options?: { limit?: number },
  ): LiveView<Out> {
    return makeFillView(this as any, strategy, options);
  }

  cumulative<const Targets extends NumericColumnNameForSchema<Out>>(spec: {
    [K in Targets]:
      | 'sum'
      | 'max'
      | 'min'
      | 'count'
      | ((acc: number, value: number) => number);
  }): LiveView<DiffSchema<Out, Targets>> {
    return makeCumulativeView(this as any, spec);
  }

  aggregate<const M extends AggregateMap<Out>>(
    sequence: Sequence,
    mapping: M,
  ): LiveAggregation<Out> {
    return new LiveAggregation(this as any, sequence, mapping as any);
  }

  dispose(): void {
    this.#unsubscribe();
  }

  // ── Private ─────────────────────────────────────────────────

  #ingest(event: EventForSchema<S>): void {
    const data = event.data() as Record<string, ColumnValue | undefined>;
    const values = this.#columns.map((c) => data[c.source]);
    const index = this.#nextIndex++;
    const entry: WindowEntry = {
      index,
      timestamp: event.begin(),
      values,
    };

    for (let i = 0; i < this.#columns.length; i++) {
      this.#states[i]!.add(index, values[i]);
    }
    this.#entries.push(entry);

    this.#evict(event.begin());

    // Emission is gated by the configured trigger.
    switch (this.#trigger.kind) {
      case 'event':
        this.#emitEvent(event.key());
        return;
      case 'clock':
        this.#emitClock(event.begin(), this.#trigger);
        return;
    }
  }

  /**
   * Emit one output event keyed at `key`, carrying the current
   * rolling-window snapshot. Used by Trigger.event() (the default).
   */
  #emitEvent(key: any): void {
    const warmup = this.#entries.length < this.#minSamples;
    const record: Record<string, ColumnValue | undefined> = {};
    for (let i = 0; i < this.#columns.length; i++) {
      record[this.#columns[i]!.source] = warmup
        ? undefined
        : this.#states[i]!.snapshot();
    }
    const outputEvent = new Event(key, record);
    this.#outputEvents.push(outputEvent);
    for (const fn of this.#onEvent) fn(outputEvent);
  }

  /**
   * Clock-triggered emission: fire one output event at the new
   * bucket's start timestamp when an incoming event crosses an
   * epoch-aligned boundary. The first event ingested establishes
   * the starting bucket; emission begins on the next crossing.
   * A single event jumping multiple boundaries fires exactly one
   * event at the new bucket's start, not one per skipped boundary.
   */
  #emitClock(eventTs: number, trigger: ClockTrigger): void {
    const bucketIdx = bucketIndexFor(trigger, eventTs);

    if (this.#lastClockBucketIdx === undefined) {
      // First event — record the starting bucket; no emission yet.
      this.#lastClockBucketIdx = bucketIdx;
      return;
    }

    if (bucketIdx > this.#lastClockBucketIdx) {
      const boundaryMs = boundaryTimestampFor(trigger, bucketIdx);
      this.#emitEvent(new Time(boundaryMs));
      this.#lastClockBucketIdx = bucketIdx;
    }
  }

  #evict(latestTimestamp: number): void {
    if (this.#windowMs !== undefined) {
      const cutoff = latestTimestamp - this.#windowMs;
      while (this.#entries.length > 0 && this.#entries[0]!.timestamp < cutoff) {
        this.#removeFirst();
      }
    }

    if (this.#windowCount !== undefined) {
      while (this.#entries.length > this.#windowCount) {
        this.#removeFirst();
      }
    }
  }

  #removeFirst(): void {
    const entry = this.#entries.shift()!;
    for (let i = 0; i < this.#columns.length; i++) {
      this.#states[i]!.remove(entry.index, entry.values[i]);
    }
  }
}
