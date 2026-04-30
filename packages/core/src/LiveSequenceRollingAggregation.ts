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
import { Sequence } from './Sequence.js';
import type {
  AggregateMap,
  DiffSchema,
  EventDataForSchema,
  EventForSchema,
  NumericColumnNameForSchema,
  ColumnValue,
  RollingSchema,
  SelectSchema,
  SeriesSchema,
} from './types.js';

type EventListener = (event: any) => void;

/**
 * A live source that emits one event per sequence interval, driven by
 * source-event timestamps crossing interval boundaries.
 *
 * Each emitted event carries a snapshot of the rolling-window aggregate
 * at the moment the boundary was crossed. The output is time-keyed at
 * the sequence's epoch-aligned interval boundaries (e.g.
 * `Sequence.every('30s')` → timestamps at 0, 30 000, 60 000, … ms).
 *
 * **Emission is data-driven.** If no source events arrive during an
 * interval, no event is emitted for that interval. This is consistent
 * with the rest of pond-ts: data is the clock.
 *
 * **One emission per crossing, not per skipped interval.** If a source
 * event jumps multiple interval boundaries at once (e.g. an event at
 * 90 001 ms after a gap since 0 ms, with a 30 s sequence), exactly one
 * event is emitted — at the start of the new bucket (90 000 ms). The
 * 30 s and 60 s intervals had no data, so they produce no output.
 *
 * **Snapshot timing.** The rolling-window snapshot is read after the
 * boundary-crossing event has been ingested by the rolling window. The
 * emitted value therefore includes that event's contribution to the
 * trailing aggregate.
 *
 * Created by passing a `Sequence` as the first argument to
 * `live.rolling(...)`, mirroring the batch API
 * `series.rolling(Sequence.every('30s'), '1m', mapping)`.
 *
 * @example
 * ```ts
 * const timings = new LiveSeries<TimingSchema>();
 *
 * // Emits once per 30 s of event time with trailing 1-minute percentiles
 * const reported = timings.rolling(
 *   Sequence.every('30s'),
 *   '1m',
 *   {
 *     p50: { from: 'latency', using: 'p50' },
 *     p95: { from: 'latency', using: 'p95' },
 *   },
 * );
 *
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
  readonly #ownsRolling: boolean;
  readonly #stepMs: number;
  readonly #anchorMs: number;
  readonly #outputEvents: EventForSchema<Out>[];
  readonly #onEvent: Set<EventListener>;
  readonly #unsubscribe: () => void;

  #lastBucketIdx: number | undefined;

  /**
   * Internal — use `live.rolling(Sequence, window, mapping)` instead.
   *
   * @param rolling The upstream rolling aggregation to snapshot at boundary crossings.
   * @param sequence The fixed-step Sequence whose boundaries trigger emission.
   * @param ownsRolling When `true`, `dispose()` also disposes the upstream rolling.
   *   Set by the `live.rolling(Sequence, ...)` overload, which constructs the
   *   rolling internally and is solely responsible for its lifecycle.
   */
  constructor(
    rolling: LiveRollingAggregation<S, Out>,
    sequence: Sequence,
    ownsRolling: boolean = false,
  ) {
    this.name = rolling.name;
    this.schema = rolling.schema;
    this.#rolling = rolling;
    this.#ownsRolling = ownsRolling;
    this.#stepMs = sequence.stepMs();
    this.#anchorMs = sequence.anchor();
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
   * will be emitted after this call. When this instance was created via
   * the `live.rolling(Sequence, window, mapping)` overload (which constructs
   * the upstream rolling internally), `dispose()` also disposes that
   * upstream rolling — there is no other reference to it, so leaving it
   * subscribed to the source would leak.
   */
  dispose(): void {
    this.#unsubscribe();
    if (this.#ownsRolling) {
      this.#rolling.dispose();
    }
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

  rolling<const M extends AggregateMap<Out>>(
    window: RollingWindow,
    mapping: M,
    options?: LiveRollingOptions,
  ): LiveRollingAggregation<Out, RollingSchema<Out, M>>;
  rolling<const M extends AggregateMap<Out>>(
    sequence: Sequence,
    window: RollingWindow,
    mapping: M,
    options?: LiveRollingOptions,
  ): LiveSequenceRollingAggregation<Out, RollingSchema<Out, M>>;
  rolling<const M extends AggregateMap<Out>>(
    sequenceOrWindow: Sequence | RollingWindow,
    windowOrMapping: RollingWindow | M,
    mappingOrOptions?: M | LiveRollingOptions,
    options?: LiveRollingOptions,
  ):
    | LiveRollingAggregation<Out, RollingSchema<Out, M>>
    | LiveSequenceRollingAggregation<Out, RollingSchema<Out, M>> {
    if (sequenceOrWindow instanceof Sequence) {
      const r = new LiveRollingAggregation(
        this as any,
        windowOrMapping as RollingWindow,
        mappingOrOptions as AggregateMap<Out>,
        options,
      );
      return new LiveSequenceRollingAggregation(
        r,
        sequenceOrWindow,
        true,
      ) as any;
    }
    return new LiveRollingAggregation(
      this as any,
      sequenceOrWindow,
      windowOrMapping as AggregateMap<Out>,
      mappingOrOptions as LiveRollingOptions | undefined,
    );
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
    const bucketIdx = Math.floor((ts - this.#anchorMs) / this.#stepMs);

    if (this.#lastBucketIdx === undefined) {
      // First event — record which bucket we're in; no emission yet.
      this.#lastBucketIdx = bucketIdx;
      return;
    }

    if (bucketIdx > this.#lastBucketIdx) {
      // Crossed one or more bucket boundaries.
      // Emit once with the current rolling snapshot, timestamped at the
      // start of the new bucket (= end of the just-closed bucket).
      const boundaryMs = this.#anchorMs + bucketIdx * this.#stepMs;
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
