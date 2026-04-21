import { Event } from './Event.js';
import { LiveAggregation } from './LiveAggregation.js';
import { Rolling, type RollingWindow } from './Rolling.js';
import { TimeSeries } from './TimeSeries.js';
import type { Sequence } from './Sequence.js';
import type {
  AggregateMap,
  DiffSchema,
  EventDataForSchema,
  EventForSchema,
  LiveSource,
  NumericColumnNameForSchema,
  RowForSchema,
  ScalarValue,
  SelectSchema,
  SeriesSchema,
  ValueColumnsForSchema,
} from './types.js';

export type LiveFillStrategy = 'hold' | 'zero';

export type LiveFillMapping<S extends SeriesSchema> = {
  [K in ValueColumnsForSchema<S>[number]['name']]?:
    | LiveFillStrategy
    | ScalarValue;
};

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

  window(size: RollingWindow): LiveView<S> {
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

// ── Factory functions for stateful live views ────────────────────

export function makeDiffView<
  S extends SeriesSchema,
  Target extends NumericColumnNameForSchema<S>,
>(
  source: LiveSource<S>,
  mode: 'diff' | 'rate' | 'pctChange',
  columns: Target | readonly Target[],
  options?: { drop?: boolean },
): LiveView<DiffSchema<S, Target>> {
  type OutSchema = DiffSchema<S, Target>;
  const cols = (
    typeof columns === 'string' ? [columns] : [...columns]
  ) as string[];
  const drop = options?.drop === true;

  if (cols.length === 0) {
    throw new Error(`${mode}() requires at least one column name`);
  }

  const targetSet = new Set<string>(cols);
  const outSchema = Object.freeze(
    source.schema.map((col, i) => {
      if (i === 0) return col;
      if (targetSet.has(col.name)) {
        return { ...col, kind: 'number' as const, required: false as const };
      }
      return col;
    }),
  ) as unknown as OutSchema;

  let prevData: Record<string, unknown> | undefined;
  let prevTime: number | undefined;

  const process = (event: any): any => {
    const data = event.data() as Record<string, unknown>;

    if (prevData === undefined) {
      prevData = data;
      prevTime = event.begin();
      if (drop) return undefined;
      const firstData = { ...data };
      for (const col of cols) {
        firstData[col] = undefined;
      }
      return new Event(event.key(), firstData);
    }

    const outData = { ...data };
    const dt = mode === 'rate' ? (event.begin() - prevTime!) / 1000 : undefined;

    for (const col of cols) {
      const prevVal = prevData[col];
      const currVal = outData[col];

      if (typeof currVal === 'number' && typeof prevVal === 'number') {
        const delta = currVal - prevVal;
        if (mode === 'pctChange') {
          outData[col] = prevVal !== 0 ? delta / prevVal : undefined;
        } else if (mode === 'rate') {
          outData[col] = dt !== 0 ? delta / dt! : undefined;
        } else {
          outData[col] = delta;
        }
      } else {
        outData[col] = undefined;
      }
    }

    prevData = data;
    prevTime = event.begin();

    return new Event(event.key(), outData);
  };

  return new LiveView(source as any, process, {
    schema: outSchema as any,
  }) as unknown as LiveView<OutSchema>;
}

export function makeFillView<S extends SeriesSchema>(
  source: LiveSource<S>,
  strategy: LiveFillStrategy | LiveFillMapping<S>,
  options?: { limit?: number },
): LiveView<S> {
  type Spec =
    | { mode: 'hold' }
    | { mode: 'zero' }
    | { mode: 'literal'; value: ScalarValue };

  const colNames = source.schema.slice(1).map((c) => c.name);
  const specs = new Map<string, Spec>();

  if (typeof strategy === 'string') {
    if (strategy !== 'hold' && strategy !== 'zero') {
      throw new Error(
        `live fill strategy '${strategy}' is not supported (bfill and linear require future values)`,
      );
    }
    for (const name of colNames) {
      specs.set(name, { mode: strategy });
    }
  } else {
    for (const [name, spec] of Object.entries(strategy)) {
      if (spec === 'hold' || spec === 'zero') {
        specs.set(name, { mode: spec });
      } else if (spec === 'bfill' || spec === 'linear') {
        throw new Error(
          `live fill strategy '${spec}' is not supported (bfill and linear require future values)`,
        );
      } else {
        specs.set(name, { mode: 'literal', value: spec as ScalarValue });
      }
    }
  }

  const limit = options?.limit;
  const state = new Map<
    string,
    { lastKnown: ScalarValue | undefined; consecutive: number }
  >();
  for (const [name] of specs) {
    state.set(name, { lastKnown: undefined, consecutive: 0 });
  }

  const process = (event: any): any => {
    const data = event.data() as Record<string, unknown>;
    let outData: Record<string, unknown> | undefined;

    for (const [name, spec] of specs) {
      const s = state.get(name)!;
      const value = data[name];

      if (value !== undefined) {
        s.lastKnown = value as ScalarValue;
        s.consecutive = 0;
      } else {
        s.consecutive++;
        if (limit !== undefined && s.consecutive > limit) continue;

        let fillValue: ScalarValue | undefined;
        switch (spec.mode) {
          case 'hold':
            fillValue = s.lastKnown;
            break;
          case 'zero':
            fillValue = 0;
            break;
          case 'literal':
            fillValue = spec.value;
            break;
        }

        if (fillValue !== undefined) {
          if (!outData) outData = { ...data };
          outData[name] = fillValue;
        }
      }
    }

    return outData ? new Event(event.key(), outData) : event;
  };

  return new LiveView(source as any, process) as unknown as LiveView<S>;
}

export function makeCumulativeView<
  S extends SeriesSchema,
  Targets extends NumericColumnNameForSchema<S>,
>(
  source: LiveSource<S>,
  spec: {
    [K in Targets]:
      | 'sum'
      | 'max'
      | 'min'
      | 'count'
      | ((acc: number, value: number) => number);
  },
): LiveView<DiffSchema<S, Targets>> {
  type OutSchema = DiffSchema<S, Targets>;

  const entries = Object.entries(spec) as [
    string,
    'sum' | 'max' | 'min' | 'count' | ((acc: number, value: number) => number),
  ][];

  if (entries.length === 0) {
    throw new Error('cumulative() requires at least one column');
  }

  const targetSet = new Set<string>(entries.map(([name]) => name));
  const outSchema = Object.freeze(
    source.schema.map((col, i) => {
      if (i === 0) return col;
      if (targetSet.has(col.name)) {
        return { ...col, kind: 'number' as const, required: false as const };
      }
      return col;
    }),
  ) as unknown as OutSchema;

  const accState = new Map<
    string,
    {
      acc: number | undefined;
      apply: (acc: number | undefined, value: number) => number;
    }
  >();

  for (const [name, reducer] of entries) {
    if (typeof reducer === 'function') {
      const fn = reducer;
      accState.set(name, {
        acc: undefined,
        apply: (acc, v) => (acc === undefined ? v : fn(acc, v)),
      });
    } else {
      switch (reducer) {
        case 'sum':
          accState.set(name, {
            acc: undefined,
            apply: (acc, v) => (acc ?? 0) + v,
          });
          break;
        case 'count':
          accState.set(name, {
            acc: undefined,
            apply: (acc) => (acc ?? 0) + 1,
          });
          break;
        case 'max':
          accState.set(name, {
            acc: undefined,
            apply: (acc, v) => (acc === undefined || v > acc ? v : acc),
          });
          break;
        case 'min':
          accState.set(name, {
            acc: undefined,
            apply: (acc, v) => (acc === undefined || v < acc ? v : acc),
          });
          break;
      }
    }
  }

  const process = (event: any): any => {
    const data = { ...(event.data() as Record<string, unknown>) };
    for (const [name, s] of accState) {
      const raw = data[name];
      if (typeof raw === 'number') {
        s.acc = s.apply(s.acc, raw);
        data[name] = s.acc;
      } else {
        data[name] = s.acc;
      }
    }
    return new Event(event.key(), data);
  };

  return new LiveView(source as any, process, {
    schema: outSchema as any,
  }) as unknown as LiveView<OutSchema>;
}
