import type { LiveSeries } from './LiveSeries.js';
import { resolveReducer, type RollingReducerState } from './reducers/index.js';
import type {
  AggregateMap,
  EventForSchema,
  ScalarValue,
  SeriesSchema,
} from './types.js';

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

type ColumnSpec = {
  source: string;
  reducer: string;
};

type WindowEntry = {
  index: number;
  timestamp: number;
  values: (ScalarValue | undefined)[];
};

type UpdateListener = (value: Record<string, ScalarValue | undefined>) => void;

export type TailReduceWindow = DurationInput | number;

export class TailReduce<S extends SeriesSchema> {
  readonly #columns: ColumnSpec[];
  readonly #states: RollingReducerState[];
  readonly #entries: WindowEntry[];

  readonly #windowMs: number | undefined;
  readonly #windowCount: number | undefined;
  #nextIndex: number;

  readonly #onUpdate: Set<UpdateListener>;
  readonly #unsubscribe: () => void;

  constructor(
    source: LiveSeries<S>,
    window: TailReduceWindow,
    mapping: AggregateMap<S>,
  ) {
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
      if (!colsByName.has(name))
        throw new TypeError(`unknown column '${name}'`);
      this.#columns.push({ source: name, reducer });
    }

    this.#states = this.#columns.map((c) =>
      resolveReducer(c.reducer).rollingState(),
    );
    this.#entries = [];
    this.#nextIndex = 0;
    this.#onUpdate = new Set();

    for (let i = 0; i < source.length; i++) {
      this.#ingest(source.at(i)!);
    }

    this.#unsubscribe = source.on('event', (event) => {
      this.#ingest(event);
      const val = this.value();
      for (const fn of this.#onUpdate) fn(val);
    });
  }

  value(): Record<string, ScalarValue | undefined> {
    const result: Record<string, ScalarValue | undefined> = {};
    for (let i = 0; i < this.#columns.length; i++) {
      result[this.#columns[i]!.source] = this.#states[i]!.snapshot();
    }
    return result;
  }

  get windowSize(): number {
    return this.#entries.length;
  }

  on(type: 'update', fn: UpdateListener): this {
    this.#onUpdate.add(fn);
    return this;
  }

  dispose(): void {
    this.#unsubscribe();
  }

  // ── Private ─────────────────────────────────────────────────

  #ingest(event: EventForSchema<S>): void {
    const data = event.data() as Record<string, ScalarValue | undefined>;
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
