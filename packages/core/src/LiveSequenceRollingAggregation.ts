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
 * A live source that emits one snapshot of a `LiveRollingAggregation` each
 * time a source event crosses an epoch-aligned boundary of the configured
 * `Sequence`. Output events are time-keyed at the boundary timestamp and
 * carry the rolling-window aggregate as it stood the moment the
 * boundary-crossing event was ingested.
 *
 * **Created via `rolling.sample(sequence)`.** The user retains the
 * underlying `LiveRollingAggregation` reference, so the rolling state is
 * available for direct reads (`rolling.value()`) and for reuse — e.g.
 * driving an in-app live display alongside a sampled backend report.
 *
 * **Emission is data-driven.** If no source events arrive during an
 * interval, no event is emitted for that interval. Consistent with the
 * rest of pond-ts: data is the clock.
 *
 * **One emission per crossing, not per skipped interval.** If a source
 * event jumps multiple boundaries at once (e.g. an event at 90 001 ms
 * after a gap since 0 ms with a 30 s sequence), exactly one event is
 * emitted — at the start of the new bucket (90 000 ms). The 30 s and
 * 60 s intervals had no data and produce no output.
 *
 * **Snapshot timing.** The rolling-window snapshot is read after the
 * boundary-crossing event has been ingested by the rolling. The emitted
 * value therefore includes that event's contribution to the trailing
 * aggregate.
 *
 * **Dispose independence.** `sample.dispose()` only detaches this
 * sampler from its upstream rolling — it does NOT dispose the rolling
 * itself. The rolling's lifetime is the user's responsibility (call
 * `rolling.dispose()` when done with it). This lets one rolling drive
 * multiple downstream consumers (e.g. one `.sample('30s')` for the
 * backend, plus direct `rolling.value()` reads for the UI) without
 * coupling their lifetimes.
 *
 * @example
 * ```ts
 * const timings = new LiveSeries<TimingSchema>();
 *
 * const rolling = timings.rolling('1m', {
 *   p50: { from: 'latency', using: 'p50' },
 *   p95: { from: 'latency', using: 'p95' },
 * });
 *
 * // Backend report every 30 s of event time
 * const reported = rolling.sample(Sequence.every('30s'));
 * reported.on('event', e =>
 *   fetch('/api/telemetry', { method: 'POST', body: JSON.stringify(e.data()) }),
 * );
 *
 * // In-app display reads the same rolling state continuously
 * setInterval(() => render(rolling.value()), 1_000);
 * ```
 */
export class LiveSequenceRollingAggregation<
  S extends SeriesSchema,
  Out extends SeriesSchema = SeriesSchema,
> {
  readonly name: string;
  readonly schema: Out;

  readonly #rolling: LiveRollingAggregation<S, Out>;
  readonly #stepMs: number;
  readonly #anchorMs: number;
  readonly #outputEvents: EventForSchema<Out>[];
  readonly #onEvent: Set<EventListener>;
  readonly #unsubscribe: () => void;

  #lastBucketIdx: number | undefined;

  /** Internal — use `rolling.sample(sequence)` instead. */
  constructor(rolling: LiveRollingAggregation<S, Out>, sequence: Sequence) {
    this.name = rolling.name;
    this.schema = rolling.schema;
    this.#rolling = rolling;
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
   * Detach this sampler from its upstream rolling. The upstream rolling
   * is not disposed — call `rolling.dispose()` separately when done.
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

  rolling<const M extends AggregateMap<Out>>(
    window: RollingWindow,
    mapping: M,
    options?: LiveRollingOptions,
  ): LiveRollingAggregation<Out, RollingSchema<Out, M>> {
    return new LiveRollingAggregation(
      this as any,
      window,
      mapping as AggregateMap<Out>,
      options,
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
