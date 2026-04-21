import { Event } from './Event.js';
import { Interval } from './Interval.js';
import { LiveAggregation } from './LiveAggregation.js';
import {
  LiveView,
  makeDiffView,
  makeFillView,
  makeCumulativeView,
  type LiveFillMapping,
  type LiveFillStrategy,
} from './LiveView.js';
import { Time } from './Time.js';
import { TimeRange } from './TimeRange.js';
import { Rolling, type RollingWindow } from './Rolling.js';
import { TimeSeries } from './TimeSeries.js';
import { ValidationError } from './errors.js';
import type { EventKey, IntervalInput, TimeRangeInput } from './temporal.js';
import type { Sequence } from './Sequence.js';
import type {
  AggregateMap,
  DiffSchema,
  EventDataForSchema,
  EventForSchema,
  FirstColKind,
  NumericColumnNameForSchema,
  RowForSchema,
  SelectSchema,
  SeriesSchema,
} from './types.js';

// ── Duration parsing (shared with Sequence.ts / TimeSeries.ts) ──

type DurationInput = number | `${number}${'ms' | 's' | 'm' | 'h' | 'd'}`;

function parseDuration(value: DurationInput): number {
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || value <= 0) {
      throw new TypeError('duration must be a positive finite number');
    }
    return value;
  }
  const match = /^(\d+)(ms|s|m|h|d)$/.exec(value);
  if (!match) throw new TypeError(`unsupported duration '${value}'`);
  const amount = Number(match[1]);
  const unit = match[2];
  const multiplier =
    unit === 'ms'
      ? 1
      : unit === 's'
        ? 1_000
        : unit === 'm'
          ? 60_000
          : unit === 'h'
            ? 3_600_000
            : 86_400_000;
  return amount * multiplier;
}

// ── Single-row validation ───────────────────────────────────────

const FIRST_COL_KINDS: ReadonlySet<string> = new Set([
  'time',
  'interval',
  'timeRange',
]);

function validateSchema(schema: SeriesSchema): void {
  if (!schema.length) {
    throw new ValidationError('schema must have at least one column');
  }
  if (!FIRST_COL_KINDS.has(schema[0]!.kind)) {
    throw new ValidationError(
      'first column must be one of: time, interval, timeRange',
    );
  }
  for (let col = 1; col < schema.length; col++) {
    const kind = schema[col]!.kind;
    if (kind !== 'number' && kind !== 'string' && kind !== 'boolean') {
      throw new ValidationError(
        `column ${col} has unsupported value kind '${kind}'`,
      );
    }
  }
}

function normalizeKey(
  kind: FirstColKind,
  value: unknown,
): Time | TimeRange | Interval {
  switch (kind) {
    case 'time':
      return value instanceof Time ? value : new Time(value as number | Date);
    case 'timeRange':
      return value instanceof TimeRange
        ? value
        : new TimeRange(value as TimeRangeInput);
    case 'interval':
      return value instanceof Interval
        ? value
        : new Interval(value as IntervalInput);
  }
}

function assertCellKind(kind: string, value: unknown, name: string): void {
  if (value === undefined) return;
  switch (kind) {
    case 'number':
      if (typeof value !== 'number' || !Number.isFinite(value))
        throw new ValidationError(`'${name}': expected finite number`);
      return;
    case 'string':
      if (typeof value !== 'string')
        throw new ValidationError(`'${name}': expected string`);
      return;
    case 'boolean':
      if (typeof value !== 'boolean')
        throw new ValidationError(`'${name}': expected boolean`);
      return;
  }
}

function compareKeys(a: EventKey, b: EventKey): number {
  return a.begin() !== b.begin() ? a.begin() - b.begin() : a.end() - b.end();
}

function estimateEventBytes(event: Event<EventKey, unknown>): number {
  let bytes = 64; // base overhead (object, key, data refs)
  const data = event.data() as Record<string, unknown>;
  for (const v of Object.values(data)) {
    if (typeof v === 'number') bytes += 8;
    else if (typeof v === 'string') bytes += 2 * v.length;
    else if (typeof v === 'boolean') bytes += 4;
  }
  return bytes;
}

// ── Types ───────────────────────────────────────────────────────

export type OrderingMode = 'strict' | 'drop' | 'reorder';

export type RetentionPolicy = {
  maxEvents?: number;
  maxAge?: DurationInput;
  maxBytes?: number;
};

export type LiveSeriesOptions<S extends SeriesSchema> = {
  name: string;
  schema: S;
  ordering?: OrderingMode;
  graceWindow?: DurationInput;
  retention?: RetentionPolicy;
};

type EventListener<S extends SeriesSchema> = (event: EventForSchema<S>) => void;
type BatchListener<S extends SeriesSchema> = (
  events: ReadonlyArray<EventForSchema<S>>,
) => void;
type EvictListener<S extends SeriesSchema> = (
  events: ReadonlyArray<EventForSchema<S>>,
) => void;

// ── LiveSeries ──────────────────────────────────────────────────

export class LiveSeries<S extends SeriesSchema> {
  readonly name: string;
  readonly schema: S;

  readonly #ordering: OrderingMode;
  readonly #graceWindowMs: number;
  readonly #maxEvents: number;
  readonly #maxAgeMs: number;
  readonly #maxBytes: number;

  #events: EventForSchema<S>[];
  #byteEstimate: number;

  readonly #onEvent: Set<EventListener<S>>;
  readonly #onBatch: Set<BatchListener<S>>;
  readonly #onEvict: Set<EvictListener<S>>;

  constructor(options: LiveSeriesOptions<S>) {
    this.name = options.name;
    this.schema = Object.freeze([...options.schema]) as unknown as S;

    this.#ordering = options.ordering ?? 'strict';
    this.#graceWindowMs = options.graceWindow
      ? parseDuration(options.graceWindow)
      : Infinity;

    if (this.#ordering !== 'reorder' && options.graceWindow !== undefined) {
      throw new ValidationError(
        'graceWindow is only valid with ordering: "reorder"',
      );
    }

    const ret = options.retention ?? {};
    this.#maxEvents = ret.maxEvents ?? Infinity;
    this.#maxAgeMs = ret.maxAge ? parseDuration(ret.maxAge) : Infinity;
    this.#maxBytes = ret.maxBytes ?? Infinity;

    this.#events = [];
    this.#byteEstimate = 0;
    this.#onEvent = new Set();
    this.#onBatch = new Set();
    this.#onEvict = new Set();

    validateSchema(this.schema);
  }

  get length(): number {
    return this.#events.length;
  }

  get graceWindowMs(): number {
    return this.#graceWindowMs;
  }

  at(index: number): EventForSchema<S> | undefined {
    if (index < 0) index = this.#events.length + index;
    return this.#events[index];
  }

  first(): EventForSchema<S> | undefined {
    return this.#events[0];
  }

  last(): EventForSchema<S> | undefined {
    return this.#events[this.#events.length - 1];
  }

  push(...rows: RowForSchema<S>[]): void {
    if (rows.length === 0) return;

    const added: EventForSchema<S>[] = [];

    for (const row of rows) {
      const event = this.#validateRow(row);
      if (this.#insert(event)) {
        added.push(event);
        this.#byteEstimate += estimateEventBytes(event as any);
        for (const fn of this.#onEvent) fn(event);
      }
    }

    if (added.length === 0) return;

    const evicted = this.#applyRetention();

    for (const fn of this.#onBatch) fn(added);
    if (evicted.length > 0) {
      for (const fn of this.#onEvict) fn(evicted);
    }
  }

  clear(): void {
    const evicted = this.#events;
    this.#events = [];
    this.#byteEstimate = 0;
    if (evicted.length > 0) {
      for (const fn of this.#onEvict) fn(evicted);
    }
  }

  toTimeSeries(name?: string): TimeSeries<S> {
    const rows = this.#events.map((event) => {
      const row: unknown[] = [event.key()];
      for (let col = 1; col < this.schema.length; col++) {
        row.push(event.get((this.schema[col] as any).name));
      }
      return row;
    });
    return new TimeSeries({
      name: name ?? this.name,
      schema: this.schema,
      rows: rows as RowForSchema<S>[],
    });
  }

  filter(predicate: (event: EventForSchema<S>) => boolean): LiveView<S> {
    return new LiveView(this, (event: EventForSchema<S>) =>
      predicate(event) ? event : undefined,
    );
  }

  map(fn: (event: EventForSchema<S>) => EventForSchema<S>): LiveView<S> {
    return new LiveView(this, fn);
  }

  select<const Keys extends readonly (keyof EventDataForSchema<S>)[]>(
    ...keys: Keys
  ): LiveView<SelectSchema<S, Keys[number] & string>> {
    const newSchema = Object.freeze([
      this.schema[0]!,
      ...this.schema.slice(1).filter((c) => keys.includes(c.name as any)),
    ]) as unknown as SelectSchema<S, Keys[number] & string>;

    return new LiveView(this, (event: any) => event.select(...keys), {
      schema: newSchema,
    });
  }

  window(size: RollingWindow): LiveView<S> {
    if (typeof size === 'number' && Number.isInteger(size) && size > 0) {
      const count = size;
      return new LiveView(this, (event: EventForSchema<S>) => event, {
        evict: (events: readonly EventForSchema<S>[]) =>
          Math.max(0, events.length - count),
      });
    }
    if (typeof size === 'string') {
      const ms = parseDuration(size);
      return new LiveView(this, (event: EventForSchema<S>) => event, {
        evict: (events: readonly EventForSchema<S>[]) => {
          if (events.length === 0) return 0;
          const cutoff = events[events.length - 1]!.begin() - ms;
          let i = 0;
          while (i < events.length && events[i]!.begin() < cutoff) i++;
          return i;
        },
      });
    }
    throw new TypeError(
      'window must be a positive integer (event count) or duration string',
    );
  }

  aggregate(sequence: Sequence, mapping: AggregateMap<S>): LiveAggregation<S> {
    return new LiveAggregation(this, sequence, mapping);
  }

  rolling(window: RollingWindow, mapping: AggregateMap<S>): Rolling<S> {
    return new Rolling(this, window, mapping);
  }

  diff<const Target extends NumericColumnNameForSchema<S>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): LiveView<DiffSchema<S, Target>> {
    return makeDiffView(this, 'diff', columns, options);
  }

  rate<const Target extends NumericColumnNameForSchema<S>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): LiveView<DiffSchema<S, Target>> {
    return makeDiffView(this, 'rate', columns, options);
  }

  pctChange<const Target extends NumericColumnNameForSchema<S>>(
    columns: Target | readonly Target[],
    options?: { drop?: boolean },
  ): LiveView<DiffSchema<S, Target>> {
    return makeDiffView(this, 'pctChange', columns, options);
  }

  fill(
    strategy: LiveFillStrategy | LiveFillMapping<S>,
    options?: { limit?: number },
  ): LiveView<S> {
    return makeFillView(this, strategy, options);
  }

  cumulative<const Targets extends NumericColumnNameForSchema<S>>(spec: {
    [K in Targets]:
      | 'sum'
      | 'max'
      | 'min'
      | 'count'
      | ((acc: number, value: number) => number);
  }): LiveView<DiffSchema<S, Targets>> {
    return makeCumulativeView(this, spec);
  }

  on(type: 'event', fn: EventListener<S>): () => void;
  on(type: 'batch', fn: BatchListener<S>): () => void;
  on(type: 'evict', fn: EvictListener<S>): () => void;
  on(
    type: 'event' | 'batch' | 'evict',
    fn: EventListener<S> | BatchListener<S> | EvictListener<S>,
  ): () => void {
    const set =
      type === 'event'
        ? this.#onEvent
        : type === 'batch'
          ? this.#onBatch
          : this.#onEvict;
    set.add(fn as any);
    return () => {
      set.delete(fn as any);
    };
  }

  // ── Private ─────────────────────────────────────────────────

  #validateRow(row: RowForSchema<S>): EventForSchema<S> {
    const arr = row as unknown[];
    if (arr.length !== this.schema.length) {
      throw new ValidationError(
        `expected ${this.schema.length} values, got ${arr.length}`,
      );
    }

    const keyDef = this.schema[0]!;
    const key = normalizeKey(keyDef.kind as FirstColKind, arr[0]);
    const data: Record<string, unknown> = {};

    for (let col = 1; col < this.schema.length; col++) {
      const def = this.schema[col]!;
      const value = arr[col];
      if (value === undefined) {
        if (def.required !== false) {
          throw new ValidationError(`column '${def.name}' is required`);
        }
        data[def.name] = undefined;
        continue;
      }
      assertCellKind(def.kind, value, def.name);
      data[def.name] = value;
    }

    return new Event(key, data) as unknown as EventForSchema<S>;
  }

  #insert(event: EventForSchema<S>): boolean {
    const last = this.#events[this.#events.length - 1];

    if (!last || compareKeys(last.key(), event.key()) <= 0) {
      this.#events.push(event);
      return true;
    }

    switch (this.#ordering) {
      case 'strict':
        throw new ValidationError(
          `out-of-order event: timestamp ${event.begin()} is before latest ${last.begin()}`,
        );

      case 'drop':
        return false;

      case 'reorder': {
        if (
          this.#graceWindowMs !== Infinity &&
          last.begin() - event.begin() > this.#graceWindowMs
        ) {
          throw new ValidationError(
            `event at ${event.begin()} is outside grace window ` +
              `(latest: ${last.begin()}, grace: ${this.#graceWindowMs}ms)`,
          );
        }
        let lo = 0;
        let hi = this.#events.length;
        while (lo < hi) {
          const mid = (lo + hi) >>> 1;
          if (compareKeys(this.#events[mid]!.key(), event.key()) <= 0) {
            lo = mid + 1;
          } else {
            hi = mid;
          }
        }
        this.#events.splice(lo, 0, event);
        return true;
      }
    }
  }

  #applyRetention(): EventForSchema<S>[] {
    let evictCount = 0;

    if (this.#events.length > this.#maxEvents) {
      evictCount = this.#events.length - this.#maxEvents;
    }

    if (this.#maxAgeMs !== Infinity && this.#events.length > 0) {
      const latest = this.#events[this.#events.length - 1]!;
      const cutoff = latest.begin() - this.#maxAgeMs;
      let i = evictCount;
      while (i < this.#events.length && this.#events[i]!.begin() < cutoff) {
        i++;
      }
      evictCount = Math.max(evictCount, i);
    }

    if (this.#maxBytes !== Infinity && this.#byteEstimate > this.#maxBytes) {
      let i = evictCount;
      let freed = 0;
      for (let j = 0; j < evictCount; j++) {
        freed += estimateEventBytes(this.#events[j] as any);
      }
      while (
        i < this.#events.length &&
        this.#byteEstimate - freed > this.#maxBytes
      ) {
        freed += estimateEventBytes(this.#events[i] as any);
        i++;
      }
      evictCount = Math.max(evictCount, i);
    }

    if (evictCount === 0) return [];

    const evicted = this.#events.splice(0, evictCount);
    for (const e of evicted) {
      this.#byteEstimate -= estimateEventBytes(e as any);
    }
    return evicted;
  }
}
