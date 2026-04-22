import { Event } from './Event.js';
import { Interval } from './Interval.js';
import {
  LiveView,
  makeDiffView,
  makeFillView,
  makeCumulativeView,
  type LiveFillMapping,
  type LiveFillStrategy,
} from './LiveView.js';
import {
  LiveRollingAggregation,
  type RollingWindow,
} from './LiveRollingAggregation.js';
import { TimeSeries } from './TimeSeries.js';
import { resolveReducer, type AggregateBucketState } from './reducers/index.js';
import type { Sequence } from './Sequence.js';
import type {
  AggregateMap,
  AggregateSchema,
  DiffSchema,
  EventDataForSchema,
  EventForSchema,
  LiveSource,
  NumericColumnNameForSchema,
  ScalarValue,
  SelectSchema,
  SeriesSchema,
  ValueColumnsForSchema,
} from './types.js';

import { parseDuration } from './utils/duration.js';
import type { DurationInput } from './utils/duration.js';

type ColumnSpec = {
  output: string;
  source: string;
  reducer: string;
  kind: string;
};

type PendingBucket = {
  start: number;
  end: number;
  states: AggregateBucketState[];
};

type ClosedEvent = Event<Interval, Record<string, ScalarValue | undefined>>;

type BucketListener = (event: ClosedEvent) => void;
type CloseListener = (event: ClosedEvent) => void;
type UpdateListener = () => void;

export type LiveAggregationOptions = {
  grace?: DurationInput | `${number}${'ms' | 's' | 'm' | 'h' | 'd'}`;
};

export class LiveAggregation<
  S extends SeriesSchema,
  Out extends SeriesSchema = SeriesSchema,
> {
  readonly name: string;
  readonly schema: Out;

  readonly #columns: ColumnSpec[];
  readonly #stepMs: number;
  readonly #anchorMs: number;
  readonly #graceMs: number;

  readonly #pending: Map<number, PendingBucket>;
  #watermark: number;
  readonly #closedEvents: ClosedEvent[];

  readonly #onBucket: Set<BucketListener>;
  readonly #onClose: Set<CloseListener>;
  readonly #onUpdate: Set<UpdateListener>;
  readonly #unsubscribe: () => void;

  constructor(
    source: LiveSource<S>,
    sequence: Sequence,
    mapping: AggregateMap<S>,
    options?: LiveAggregationOptions,
  ) {
    this.name = source.name;
    this.#stepMs = sequence.stepMs();
    this.#anchorMs = sequence.anchor();
    const sourceGraceMs =
      'graceWindowMs' in source &&
      typeof (source as any).graceWindowMs === 'number' &&
      isFinite((source as any).graceWindowMs)
        ? (source as any).graceWindowMs
        : 0;
    this.#graceMs = options?.grace
      ? parseDuration(options.grace as any)
      : sourceGraceMs;

    const colsByName = new Map(
      source.schema.slice(1).map((c) => [c.name, c] as const),
    );
    this.#columns = [];
    for (const [name, reducer] of Object.entries(
      mapping as Record<string, string>,
    )) {
      const col = colsByName.get(name);
      if (!col) throw new TypeError(`unknown column '${name}'`);
      const kind =
        resolveReducer(reducer).outputKind === 'number' ? 'number' : col.kind;
      this.#columns.push({ output: name, source: name, reducer, kind });
    }

    this.schema = Object.freeze([
      { name: 'time', kind: 'interval' },
      ...this.#columns.map((c) => ({
        name: c.output,
        kind: c.kind,
        required: false,
      })),
    ]) as unknown as Out;

    this.#pending = new Map();
    this.#watermark = -Infinity;
    this.#closedEvents = [];
    this.#onBucket = new Set();
    this.#onClose = new Set();
    this.#onUpdate = new Set();

    for (let i = 0; i < source.length; i++) {
      this.#ingest(source.at(i)!);
    }

    this.#unsubscribe = source.on('event', (event) => {
      this.#ingest(event);
      for (const fn of this.#onUpdate) fn();
    });
  }

  get length(): number {
    return this.#closedEvents.length;
  }

  get closedCount(): number {
    return this.#closedEvents.length;
  }

  get hasOpenBucket(): boolean {
    return this.#pending.size > 0;
  }

  at(index: number): EventForSchema<Out> | undefined {
    if (index < 0) index = this.#closedEvents.length + index;
    return this.#closedEvents[index] as EventForSchema<Out> | undefined;
  }

  closed(): TimeSeries<Out> {
    return this.#buildSeries(false);
  }

  snapshot(): TimeSeries<Out> {
    return this.#buildSeries(true);
  }

  on(type: 'event', fn: CloseListener): () => void;
  on(type: 'bucket', fn: BucketListener): this;
  on(type: 'close', fn: CloseListener): this;
  on(type: 'update', fn: UpdateListener): this;
  on(
    type: 'event' | 'bucket' | 'close' | 'update',
    fn: BucketListener | CloseListener | UpdateListener,
  ): this | (() => void) {
    if (type === 'event' || type === 'close') {
      this.#onClose.add(fn as CloseListener);
      if (type === 'event')
        return () => {
          this.#onClose.delete(fn as CloseListener);
        };
      return this;
    }
    if (type === 'bucket') {
      this.#onBucket.add(fn as BucketListener);
      return this;
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

  rolling(
    windowSize: RollingWindow,
    mapping: AggregateMap<Out>,
  ): LiveRollingAggregation<Out> {
    return new LiveRollingAggregation(this as any, windowSize, mapping);
  }

  dispose(): void {
    this.#unsubscribe();
  }

  // ── Private ─────────────────────────────────────────────────

  #bucketFor(timestamp: number): { start: number; end: number } {
    const start =
      Math.floor((timestamp - this.#anchorMs) / this.#stepMs) * this.#stepMs +
      this.#anchorMs;
    return { start, end: start + this.#stepMs };
  }

  #ingest(event: EventForSchema<S>): void {
    const bucket = this.#bucketFor(event.begin());
    const ts = event.begin();

    if (ts > this.#watermark) this.#watermark = ts;

    const closeCutoff = this.#watermark - this.#graceMs;

    if (bucket.end <= closeCutoff) return;

    let pending = this.#pending.get(bucket.start);
    if (!pending) {
      pending = {
        start: bucket.start,
        end: bucket.end,
        states: this.#columns.map((c) =>
          resolveReducer(c.reducer).bucketState(),
        ),
      };
      this.#pending.set(bucket.start, pending);
    }

    const data = event.data() as Record<string, ScalarValue | undefined>;
    for (let i = 0; i < this.#columns.length; i++) {
      pending.states[i]!.add(data[this.#columns[i]!.source]);
    }

    if (this.#onBucket.size > 0) {
      const interval = new Interval({
        value: pending.start,
        start: pending.start,
        end: pending.end,
      });
      const record: Record<string, ScalarValue | undefined> = {};
      for (let i = 0; i < this.#columns.length; i++) {
        record[this.#columns[i]!.output] = pending.states[i]!.snapshot();
      }
      const evt = new Event(interval, record);
      for (const fn of this.#onBucket) fn(evt);
    }

    this.#closeEligible(closeCutoff);
  }

  #closeEligible(closeCutoff: number): void {
    const eligible: number[] = [];
    for (const [start, b] of this.#pending) {
      if (b.end <= closeCutoff) eligible.push(start);
    }
    if (eligible.length === 0) return;
    eligible.sort((a, b) => a - b);
    for (const start of eligible) {
      const b = this.#pending.get(start)!;
      this.#pending.delete(start);
      this.#finalizeBucket(b);
    }
  }

  #finalizeBucket(bucket: PendingBucket): void {
    const interval = new Interval({
      value: bucket.start,
      start: bucket.start,
      end: bucket.end,
    });
    const record: Record<string, ScalarValue | undefined> = {};
    for (let i = 0; i < this.#columns.length; i++) {
      record[this.#columns[i]!.output] = bucket.states[i]!.snapshot();
    }
    const evt = new Event(interval, record);
    this.#closedEvents.push(evt);
    for (const fn of this.#onClose) fn(evt);
  }

  #buildSeries(includeOpen: boolean): TimeSeries<Out> {
    const rows: unknown[][] = this.#closedEvents.map((event) => {
      const data = event.data() as Record<string, ScalarValue | undefined>;
      return [event.key(), ...this.#columns.map((c) => data[c.output])];
    });

    if (includeOpen) {
      const pendingBuckets = [...this.#pending.values()].sort(
        (a, b) => a.start - b.start,
      );
      for (const b of pendingBuckets) {
        rows.push([
          new Interval({
            value: b.start,
            start: b.start,
            end: b.end,
          }),
          ...b.states.map((s) => s.snapshot()),
        ]);
      }
    }

    return new TimeSeries({
      name: this.name,
      schema: this.schema,
      rows: rows as any,
    });
  }
}
