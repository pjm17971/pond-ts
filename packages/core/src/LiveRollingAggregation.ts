import { Event } from './Event.js';
import { LiveAggregation } from './LiveAggregation.js';
import { LiveSequenceRollingAggregation } from './LiveSequenceRollingAggregation.js';
import {
  LiveView,
  makeDiffView,
  makeFillView,
  makeCumulativeView,
  type LiveFillMapping,
  type LiveFillStrategy,
} from './LiveView.js';
import { resolveReducer, type RollingReducerState } from './reducers/index.js';
import { Sequence } from './Sequence.js';
import type { SequenceInterval } from './LiveSequenceRollingAggregation.js';
import type {
  AggregateMap,
  DiffSchema,
  EventDataForSchema,
  EventForSchema,
  LiveSource,
  NumericColumnNameForSchema,
  RollingSchema,
  ColumnValue,
  SelectSchema,
  SeriesSchema,
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

  /**
   * Returns a live source that emits one event per `interval` of event
   * time, carrying the current rolling-window aggregate at the moment each
   * interval boundary is crossed.
   *
   * Emission is **data-driven**: if no source events arrive during an
   * interval, no event is emitted for that interval. Output timestamps are
   * epoch-aligned to the given interval duration.
   *
   * @example
   * ```ts
   * const reported = rolling.sequence('30s');
   * reported.on('event', event => {
   *   fetch('/api/telemetry', { method: 'POST', body: JSON.stringify(event.data()) });
   * });
   * ```
   */
  sequence(interval: SequenceInterval): LiveSequenceRollingAggregation<S, Out> {
    return new LiveSequenceRollingAggregation(this, interval);
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

    const warmup = this.#entries.length < this.#minSamples;
    const record: Record<string, ColumnValue | undefined> = {};
    for (let i = 0; i < this.#columns.length; i++) {
      record[this.#columns[i]!.source] = warmup
        ? undefined
        : this.#states[i]!.snapshot();
    }
    const outputEvent = new Event(event.key(), record);
    this.#outputEvents.push(outputEvent);
    for (const fn of this.#onEvent) fn(outputEvent);
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
