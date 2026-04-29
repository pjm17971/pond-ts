import { Event } from './Event.js';
import { Time } from './Time.js';
import { LiveAggregation } from './LiveAggregation.js';
import {
  LiveRollingAggregation,
  type LiveRollingOptions,
  type RollingWindow,
} from './LiveRollingAggregation.js';
import {
  LiveView,
  makeDiffView,
  makeFillView,
  makeCumulativeView,
  type LiveFillMapping,
  type LiveFillStrategy,
} from './LiveView.js';
import type { Sequence } from './Sequence.js';
import type {
  AggregateMap,
  DiffSchema,
  EventDataForSchema,
  EventForSchema,
  NumericColumnNameForSchema,
  ColumnValue,
  SelectSchema,
  SeriesSchema,
} from './types.js';
import type { DurationInput } from './utils/duration.js';
import { parseDuration } from './utils/duration.js';

type EventListener = (event: any) => void;

/**
 * A live source that emits one event per sequence interval, driven by
 * source-event timestamps crossing interval boundaries.
 *
 * Each emitted event carries a snapshot of the rolling-window aggregate
 * at the moment the boundary was crossed. The output is time-keyed at
 * epoch-aligned interval boundaries (e.g. every 30 s → timestamps at
 * 0, 30 000, 60 000, … ms).
 *
 * **Emission is data-driven.** If no source events arrive during an
 * interval, no event is emitted for that interval. This is consistent
 * with the rest of pond-ts: data is the clock.
 *
 * Created via `rolling.sequence(interval)`.
 *
 * @example
 * ```ts
 * const timings = new LiveSeries<TimingSchema>();
 * const rolling = timings.rolling('1m', {
 *   p50: { from: 'latency', using: 'p50' },
 *   p95: { from: 'latency', using: 'p95' },
 * });
 *
 * // Emits once per 30 s of event time with trailing 1-minute percentiles
 * const reported = rolling.sequence('30s');
 * reported.on('event', event => {
 *   fetch('/api/telemetry', {
 *     method: 'POST',
 *     body: JSON.stringify(event.data()),
 *   });
 * });
 * ```
 */
export class LiveSequenceRollingAggregation<
  S extends SeriesSchema,
  Out extends SeriesSchema = SeriesSchema,
> {
  readonly name: string;
  readonly schema: Out;

  readonly #rolling: LiveRollingAggregation<S, Out>;
  readonly #intervalMs: number;
  readonly #outputEvents: EventForSchema<Out>[];
  readonly #onEvent: Set<EventListener>;
  readonly #unsubscribe: () => void;

  #lastBucketIdx: number | undefined;

  constructor(
    rolling: LiveRollingAggregation<S, Out>,
    interval: DurationInput,
  ) {
    this.name = rolling.name;
    this.schema = rolling.schema;
    this.#rolling = rolling;
    this.#intervalMs = parseDuration(interval);
    this.#outputEvents = [];
    this.#onEvent = new Set();
    this.#lastBucketIdx = undefined;

    this.#unsubscribe = rolling.on('event', (event) => {
      this.#check(event);
    });
  }

  get length(): number {
    return this.#outputEvents.length;
  }

  at(index: number): EventForSchema<Out> | undefined {
    if (index < 0) index = this.#outputEvents.length + index;
    return this.#outputEvents[index];
  }

  on(type: 'event', fn: EventListener): () => void {
    this.#onEvent.add(fn);
    return () => {
      this.#onEvent.delete(fn);
    };
  }

  /**
   * Disconnect from the upstream rolling aggregation. No further events
   * will be emitted after this call.
   */
  dispose(): void {
    this.#unsubscribe();
  }

  // ── View transforms (mirrors LiveRollingAggregation) ────────────

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
    options?: LiveRollingOptions,
  ): LiveRollingAggregation<Out> {
    return new LiveRollingAggregation(this as any, windowSize, mapping, options);
  }

  aggregate<const M extends AggregateMap<Out>>(
    sequence: Sequence,
    mapping: M,
  ): LiveAggregation<Out> {
    return new LiveAggregation(this as any, sequence, mapping as any);
  }

  // ── Private ─────────────────────────────────────────────────────

  #check(event: EventForSchema<Out>): void {
    const ts = event.begin();
    const bucketIdx = Math.floor(ts / this.#intervalMs);

    if (this.#lastBucketIdx === undefined) {
      // First event — record which bucket we're in; no emission yet.
      this.#lastBucketIdx = bucketIdx;
      return;
    }

    if (bucketIdx > this.#lastBucketIdx) {
      // Crossed one or more bucket boundaries.
      // Emit once with the current rolling snapshot, timestamped at the
      // start of the new bucket (= end of the just-closed bucket).
      const boundaryMs = bucketIdx * this.#intervalMs;
      const snap = this.#rolling.value() as Record<
        string,
        ColumnValue | undefined
      >;
      const outputEvent = new Event(
        new Time(boundaryMs),
        snap,
      ) as unknown as EventForSchema<Out>;
      this.#outputEvents.push(outputEvent);
      for (const fn of this.#onEvent) fn(outputEvent);
      this.#lastBucketIdx = bucketIdx;
    }
  }
}
