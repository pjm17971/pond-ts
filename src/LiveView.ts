import { LiveAggregation } from './LiveAggregation.js';
import { TailReduce, type TailReduceWindow } from './TailReduce.js';
import { TimeSeries } from './TimeSeries.js';
import type { Sequence } from './Sequence.js';
import type {
  AggregateMap,
  EventDataForSchema,
  EventForSchema,
  LiveSource,
  RowForSchema,
  SelectSchema,
  SeriesSchema,
} from './types.js';

type DurationInput = `${number}${'ms' | 's' | 'm' | 'h' | 'd'}`;

function parseDuration(value: DurationInput): number {
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

type EventListener<S extends SeriesSchema> = (event: EventForSchema<S>) => void;

type ViewOptions<S extends SeriesSchema> = {
  schema?: S;
  evict?: (events: readonly EventForSchema<S>[]) => number;
};

export class LiveView<S extends SeriesSchema> implements LiveSource<S> {
  readonly name: string;
  readonly schema: S;

  readonly #events: EventForSchema<S>[];
  readonly #process: (event: any) => EventForSchema<S> | undefined;
  readonly #evict:
    | ((events: readonly EventForSchema<S>[]) => number)
    | undefined;
  readonly #onEvent: Set<EventListener<S>>;
  readonly #unsubscribe: () => void;

  constructor(
    source: LiveSource<any>,
    process: (event: any) => EventForSchema<S> | undefined,
    options?: ViewOptions<S>,
  ) {
    this.name = source.name;
    this.schema = options?.schema ?? (source.schema as unknown as S);
    this.#events = [];
    this.#process = process;
    this.#evict = options?.evict;
    this.#onEvent = new Set();

    for (let i = 0; i < source.length; i++) {
      const result = this.#process(source.at(i)!);
      if (result !== undefined) this.#events.push(result);
    }
    this.#applyEviction();

    this.#unsubscribe = source.on('event', (event) => {
      const result = this.#process(event);
      if (result !== undefined) {
        this.#events.push(result);
        this.#applyEviction();
        for (const fn of this.#onEvent) fn(result);
      }
    });
  }

  get length(): number {
    return this.#events.length;
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

  window(size: TailReduceWindow): LiveView<S> {
    if (typeof size === 'number' && Number.isInteger(size) && size > 0) {
      const count = size;
      return new LiveView(this, (event: EventForSchema<S>) => event, {
        evict: (events) => Math.max(0, events.length - count),
      });
    }
    if (typeof size === 'string') {
      const ms = parseDuration(size as DurationInput);
      return new LiveView(this, (event: EventForSchema<S>) => event, {
        evict: (events) => {
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

  tail(window: TailReduceWindow, mapping: AggregateMap<S>): TailReduce<S> {
    return new TailReduce(this, window, mapping);
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

  on(type: 'event', fn: EventListener<S>): () => void {
    this.#onEvent.add(fn);
    return () => {
      this.#onEvent.delete(fn);
    };
  }

  dispose(): void {
    this.#unsubscribe();
  }

  #applyEviction(): void {
    if (!this.#evict) return;
    const count = this.#evict(this.#events);
    if (count > 0) this.#events.splice(0, count);
  }
}
