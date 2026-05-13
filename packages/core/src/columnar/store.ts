/**
 * `ColumnarStore<S>` — the framework's primary read-only data
 * container.
 *
 * Composes a `KeyColumn` (one of `TimeKeyColumn`,
 * `TimeRangeKeyColumn`, `IntervalKeyColumn`) with a
 * `ReadonlyMap<columnName, Column>` of value columns and a
 * declared `schema`. Provides:
 *
 * - Direct typed-buffer access: `keyAt(i)`, `beginAt(i)`, `endAt(i)`.
 * - Lazy event materialization: `eventAt(i)` with a per-row
 *   `Map<number, Event>` cache so repeated reads return the same
 *   `Event` reference (the framework's reference-stability
 *   contract for `series.at(i)`).
 * - Full materialization: `toEvents()` reuses the per-row cache,
 *   pinning `store.toEvents() === store.toEvents()` and
 *   `store.eventAt(i) === store.toEvents()[i]`.
 * - Event-shaped iteration: `Symbol.iterator` yields `Event`
 *   instances reusing the cache.
 *
 * Step-1d scope: the core read-only shape and event materialization.
 * Full intake paths (`fromValidatedRows`, `fromTrustedEvents`,
 * `fromBuilders`) and the store-native export plumbing
 * (`toJSON`, `toPoints`) land in subsequent sub-steps. This file
 * exposes a minimal `fromTrustedStore` factory accepting
 * pre-built columns.
 *
 * Framework-internal; not exported from `packages/core/src/index.ts`.
 */

import { Event } from '../Event.js';
import type { Interval } from '../Interval.js';
import type { Time } from '../Time.js';
import type { TimeRange } from '../TimeRange.js';
import type { SeriesSchema } from '../types.js';
import type { Column } from './column.js';
import type { KeyColumn } from './key-column.js';

/**
 * Runtime row-data shape — a record keyed by column name. The
 * generic `EventForSchema<S>` type plumbing in `types.ts` narrows
 * this further; for 1d the substrate stays loosely typed at the
 * record level and tightens at the `TimeSeries` integration
 * boundary in step 2.
 */
export type ColumnarRowData = Readonly<Record<string, unknown>>;

/**
 * Event materialized from a `ColumnarStore`. Statically widened
 * to `Event<EventKey, ColumnarRowData>` — the substrate doesn't
 * (yet) carry the schema-specific generic narrowing. Step 2
 * tightens this through `TimeSeries`'s integration types.
 */
export type ColumnarEvent = Event<Time | TimeRange | Interval, ColumnarRowData>;

/** Options accepted by `fromTrustedStore`. */
export interface FromTrustedStoreOptions {
  /**
   * Pre-populated event cache. When supplied, the store inherits
   * its entries — preserving event-identity contracts across
   * derivations like `TimeSeries.concat` (step-2 use case).
   *
   * The store treats this map as owned: it may add entries during
   * later `eventAt` calls. Callers should not mutate the map after
   * passing it in.
   */
  eventCache?: Map<number, ColumnarEvent>;
}

/**
 * Primary read-only columnar store. Construction goes through the
 * named factories below; the constructor is private to ensure the
 * column / key / schema shape is consistent.
 */
export class ColumnarStore<S extends SeriesSchema = SeriesSchema> {
  readonly schema: S;
  readonly length: number;
  readonly keys: KeyColumn;
  readonly columns: ReadonlyMap<string, Column>;
  readonly #eventCache: Map<number, ColumnarEvent>;
  // Lazy full-materialization snapshot. Built on first `toEvents()`
  // call and pinned thereafter — `toEvents() === toEvents()` and
  // `eventAt(i) === toEvents()[i]` both come from this cache.
  #eventsArray?: ReadonlyArray<ColumnarEvent>;

  private constructor(
    schema: S,
    keys: KeyColumn,
    columns: ReadonlyMap<string, Column>,
    eventCache: Map<number, ColumnarEvent>,
  ) {
    this.schema = schema;
    this.keys = keys;
    this.length = keys.length;
    this.columns = columns;
    this.#eventCache = eventCache;
  }

  /**
   * Trusted-construction factory. Accepts a pre-built key column,
   * a `ReadonlyMap` of value columns keyed by column name, and a
   * declaring `schema`. Validates the structural invariants:
   *
   * - Every value column's `length` matches `keys.length`.
   * - Every schema column (after `schema[0]`, the key column)
   *   is present in `columns` with a matching `kind`.
   *
   * No row-shaped validation; that's the row-intake factory's job
   * (sub-step 1e).
   */
  static fromTrustedStore<S extends SeriesSchema>(
    schema: S,
    keys: KeyColumn,
    columns: ReadonlyMap<string, Column>,
    options?: FromTrustedStoreOptions,
  ): ColumnarStore<S> {
    const expectedLength = keys.length;
    // Validate that every schema value column is present with the
    // declared kind.
    for (let i = 1; i < schema.length; i += 1) {
      const def = schema[i]!;
      const col = columns.get(def.name);
      if (col === undefined) {
        throw new RangeError(
          `ColumnarStore: schema column '${def.name}' is not present in the columns map`,
        );
      }
      if (col.length !== expectedLength) {
        throw new RangeError(
          `ColumnarStore: column '${def.name}' length ${col.length} does not match keys.length ${expectedLength}`,
        );
      }
      if (col.kind !== def.kind) {
        throw new RangeError(
          `ColumnarStore: column '${def.name}' kind is '${col.kind}' but schema declares '${def.kind}'`,
        );
      }
    }
    // Validate the key column's kind matches schema[0].
    const firstDef = schema[0]!;
    if (keys.kind !== firstDef.kind) {
      throw new RangeError(
        `ColumnarStore: key column kind '${keys.kind}' does not match schema[0].kind '${firstDef.kind}'`,
      );
    }
    const eventCache =
      options?.eventCache !== undefined
        ? options.eventCache
        : new Map<number, ColumnarEvent>();
    return new ColumnarStore<S>(schema, keys, columns, eventCache);
  }

  /** Direct buffer read; defers to the key column. */
  keyAt(i: number): Time | TimeRange | Interval {
    return this.keys.keyAt(i) as Time | TimeRange | Interval;
  }

  beginAt(i: number): number {
    return this.keys.beginAt(i);
  }

  endAt(i: number): number {
    return this.keys.endAt(i);
  }

  /**
   * Returns the value at `(rowIndex, columnName)` directly from the
   * column. Bypasses the row-materialization cache; cheap repeated
   * access for hot operator paths.
   */
  valueAt(rowIndex: number, columnName: string): unknown {
    const col = this.columns.get(columnName);
    if (col === undefined) {
      throw new RangeError(
        `ColumnarStore.valueAt: column '${columnName}' not present`,
      );
    }
    return col.read(rowIndex);
  }

  /**
   * Materializes the row at index `i` as an `Event` instance.
   * Lazily built and cached — `eventAt(i) === eventAt(i)` holds for
   * the column's lifetime.
   */
  eventAt(i: number): ColumnarEvent {
    if (i < 0 || i >= this.length) {
      throw new RangeError(
        `ColumnarStore.eventAt out of range: ${i} not in [0, ${this.length})`,
      );
    }
    let cached = this.#eventCache.get(i);
    if (cached === undefined) {
      const key = this.keys.keyAt(i);
      const data = this.#buildRowData(i);
      cached = new Event(key, data) as ColumnarEvent;
      this.#eventCache.set(i, cached);
    }
    return cached;
  }

  /**
   * Returns the full row-shaped event array. Built on first call
   * and cached — `toEvents() === toEvents()` holds across calls.
   * The array reuses the per-row `eventAt` cache, so
   * `eventAt(i) === toEvents()[i]` for every valid `i`.
   */
  toEvents(): ReadonlyArray<ColumnarEvent> {
    if (this.#eventsArray !== undefined) return this.#eventsArray;
    const events = new Array<ColumnarEvent>(this.length);
    for (let i = 0; i < this.length; i += 1) {
      events[i] = this.eventAt(i);
    }
    this.#eventsArray = events;
    return events;
  }

  /**
   * Event-shaped iteration. Yields `Event` instances from the
   * per-row cache — same identity as `eventAt(i)`. Pins the
   * `for (const ev of store) { ... }` pattern as a public API
   * invariant.
   */
  *[Symbol.iterator](): IterableIterator<ColumnarEvent> {
    for (let i = 0; i < this.length; i += 1) {
      yield this.eventAt(i);
    }
  }

  /**
   * Returns row-shaped tuples `[key, ...values]`. The key is the
   * `begin` timestamp for `time` keys; for `timeRange` and
   * `interval` keys the row format is the same `[begin, ...values]`
   * (the full key is recoverable via `keyAt(i)`).
   *
   * Each call rebuilds the array — `toRows() !== toRows()` — so
   * row-shape consumers that want stable references should cache
   * the result themselves. This trade keeps the columnar store
   * free of an extra cache; the row format is a transient
   * boundary representation, not a long-lived view.
   */
  toRows(): ReadonlyArray<ReadonlyArray<unknown>> {
    const rows = new Array<ReadonlyArray<unknown>>(this.length);
    const colNames: string[] = [];
    for (let i = 1; i < this.schema.length; i += 1) {
      colNames.push(this.schema[i]!.name);
    }
    for (let i = 0; i < this.length; i += 1) {
      const row: unknown[] = new Array(colNames.length + 1);
      row[0] = this.keys.beginAt(i);
      for (let c = 0; c < colNames.length; c += 1) {
        row[c + 1] = this.columns.get(colNames[c]!)!.read(i);
      }
      rows[i] = row;
    }
    return rows;
  }

  /**
   * Returns row-shaped objects keyed by column name. The key is
   * exposed as a `time` / `begin` / `end` field depending on the
   * key kind. Same identity contract as `toRows()` — each call
   * rebuilds.
   */
  toObjects(): ReadonlyArray<Readonly<Record<string, unknown>>> {
    const rows = new Array<Readonly<Record<string, unknown>>>(this.length);
    const colNames: string[] = [];
    for (let i = 1; i < this.schema.length; i += 1) {
      colNames.push(this.schema[i]!.name);
    }
    const keyField = this.schema[0]!.name;
    for (let i = 0; i < this.length; i += 1) {
      const row: Record<string, unknown> = {};
      row[keyField] = this.keys.beginAt(i);
      if (this.keys.kind === 'timeRange' || this.keys.kind === 'interval') {
        row.end = this.keys.endAt(i);
      }
      for (let c = 0; c < colNames.length; c += 1) {
        const name = colNames[c]!;
        row[name] = this.columns.get(name)!.read(i);
      }
      rows[i] = Object.freeze(row);
    }
    return rows;
  }

  // Builds the row-data object that gets fed into the `Event`
  // constructor. Each row's data is a frozen `Record<colName, value>`
  // covering every value column in the schema.
  #buildRowData(i: number): ColumnarRowData {
    const data: Record<string, unknown> = {};
    for (let c = 1; c < this.schema.length; c += 1) {
      const name = this.schema[c]!.name;
      const col = this.columns.get(name)!;
      data[name] = col.read(i);
    }
    // Event's constructor already shallow-freezes the data object, so
    // we don't need to freeze it here.
    return data as ColumnarRowData;
  }
}
