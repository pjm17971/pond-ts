import { Event } from './Event.js';
import { Interval } from './Interval.js';
import { TimeSeries } from './TimeSeries.js';
import type { LiveSeries } from './LiveSeries.js';
import { resolveReducer, type AggregateBucketState } from './reducers/index.js';
import type { Sequence } from './Sequence.js';
import type {
  AggregateMap,
  EventForSchema,
  ScalarValue,
  SeriesSchema,
} from './types.js';

type ColumnSpec = {
  output: string;
  source: string;
  reducer: string;
  kind: string;
};

type ClosedBucket = {
  start: number;
  end: number;
  values: (ScalarValue | undefined)[];
};

type OpenBucket = {
  start: number;
  end: number;
  states: AggregateBucketState[];
};

type CloseListener = (
  event: Event<Interval, Record<string, ScalarValue | undefined>>,
) => void;
type UpdateListener = () => void;

export class LiveAggregation<S extends SeriesSchema> {
  readonly #source: LiveSeries<S>;
  readonly #columns: ColumnSpec[];
  readonly #resultSchema: SeriesSchema;
  readonly #stepMs: number;
  readonly #anchorMs: number;

  #closed: ClosedBucket[];
  #open: OpenBucket | undefined;

  readonly #onClose: Set<CloseListener>;
  readonly #onUpdate: Set<UpdateListener>;
  readonly #unsubscribe: () => void;

  constructor(
    source: LiveSeries<S>,
    sequence: Sequence,
    mapping: AggregateMap<S>,
  ) {
    this.#source = source;
    this.#stepMs = sequence.stepMs();
    this.#anchorMs = sequence.anchor();

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

    this.#resultSchema = Object.freeze([
      { name: 'time', kind: 'interval' },
      ...this.#columns.map((c) => ({
        name: c.output,
        kind: c.kind,
        required: false,
      })),
    ]) as unknown as SeriesSchema;

    this.#closed = [];
    this.#open = undefined;
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

  get closedCount(): number {
    return this.#closed.length;
  }

  get hasOpenBucket(): boolean {
    return this.#open !== undefined;
  }

  closed(): TimeSeries<SeriesSchema> {
    return this.#buildSeries(false);
  }

  snapshot(): TimeSeries<SeriesSchema> {
    return this.#buildSeries(true);
  }

  on(type: 'close', fn: CloseListener): this;
  on(type: 'update', fn: UpdateListener): this;
  on(type: 'close' | 'update', fn: CloseListener | UpdateListener): this {
    const set =
      type === 'close'
        ? (this.#onClose as Set<any>)
        : (this.#onUpdate as Set<any>);
    set.add(fn);
    return this;
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

    if (this.#open && bucket.start > this.#open.start) {
      this.#closeBucket();
    }

    if (!this.#open || bucket.start !== this.#open.start) {
      this.#open = {
        start: bucket.start,
        end: bucket.end,
        states: this.#columns.map((c) =>
          resolveReducer(c.reducer).bucketState(),
        ),
      };
    }

    const data = event.data() as Record<string, ScalarValue | undefined>;
    for (let i = 0; i < this.#columns.length; i++) {
      this.#open.states[i]!.add(data[this.#columns[i]!.source]);
    }
  }

  #closeBucket(): void {
    if (!this.#open) return;
    const values = this.#open.states.map((s) => s.snapshot());
    this.#closed.push({
      start: this.#open.start,
      end: this.#open.end,
      values,
    });

    const interval = new Interval({
      value: this.#open.start,
      start: this.#open.start,
      end: this.#open.end,
    });
    const record: Record<string, ScalarValue | undefined> = {};
    for (let i = 0; i < this.#columns.length; i++) {
      record[this.#columns[i]!.output] = values[i];
    }
    const evt = new Event(interval, record);
    for (const fn of this.#onClose) fn(evt);

    this.#open = undefined;
  }

  #buildSeries(includeOpen: boolean): TimeSeries<SeriesSchema> {
    const rows: unknown[][] = this.#closed.map((b) => [
      new Interval({ value: b.start, start: b.start, end: b.end }),
      ...b.values,
    ]);

    if (includeOpen && this.#open) {
      rows.push([
        new Interval({
          value: this.#open.start,
          start: this.#open.start,
          end: this.#open.end,
        }),
        ...this.#open.states.map((s) => s.snapshot()),
      ]);
    }

    return new TimeSeries({
      name: this.#source.name,
      schema: this.#resultSchema,
      rows: rows as any,
    });
  }
}
