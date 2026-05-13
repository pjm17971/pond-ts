import { describe, expect, it } from 'vitest';

import { Event } from '../../src/Event.js';
import { Interval } from '../../src/Interval.js';
import { Time } from '../../src/Time.js';
import { TimeRange } from '../../src/TimeRange.js';
import {
  ColumnarStore,
  Float64Column,
  IntervalKeyColumn,
  TimeKeyColumn,
  TimeRangeKeyColumn,
  stringColumnFromArray,
  timeKeyColumnFromArray,
  timeRangeKeyColumnFromPairs,
} from '../../src/columnar/index.js';

/* -------------------------------------------------------------------------- */
/* Construction & schema validation                                           */
/* -------------------------------------------------------------------------- */

function makeBasicStore() {
  const schema = [
    { name: 'time', kind: 'time' },
    { name: 'value', kind: 'number' },
    { name: 'load', kind: 'number' },
  ] as const;
  const keys = timeKeyColumnFromArray([1000, 2000, 3000]);
  const value = new Float64Column(Float64Array.of(10, 20, 30), 3);
  const load = new Float64Column(Float64Array.of(0.5, 0.75, 0.9), 3);
  const columns = new Map([
    ['value', value],
    ['load', load],
  ]);
  return {
    schema,
    keys,
    columns,
    store: ColumnarStore.fromTrustedStore(schema, keys, columns),
  };
}

describe('ColumnarStore.fromTrustedStore', () => {
  it('builds a typed store from a schema + keys + columns map', () => {
    const { store } = makeBasicStore();
    expect(store.length).toBe(3);
    expect(store.schema[0]!.name).toBe('time');
    expect(store.columns.size).toBe(2);
  });

  it('rejects column-length mismatch', () => {
    const schema = [
      { name: 'time', kind: 'time' },
      { name: 'value', kind: 'number' },
    ] as const;
    const keys = timeKeyColumnFromArray([1, 2, 3]);
    const wrong = new Float64Column(Float64Array.of(10, 20), 2);
    expect(() =>
      ColumnarStore.fromTrustedStore(schema, keys, new Map([['value', wrong]])),
    ).toThrow(/length 2 does not match keys.length 3/);
  });

  it('rejects missing schema-declared column', () => {
    const schema = [
      { name: 'time', kind: 'time' },
      { name: 'value', kind: 'number' },
    ] as const;
    const keys = timeKeyColumnFromArray([1, 2]);
    expect(() =>
      ColumnarStore.fromTrustedStore(schema, keys, new Map()),
    ).toThrow(/'value' is not present/);
  });

  it('rejects column kind mismatch', () => {
    const schema = [
      { name: 'time', kind: 'time' },
      { name: 'value', kind: 'string' },
    ] as const;
    const keys = timeKeyColumnFromArray([1, 2]);
    const numCol = new Float64Column(Float64Array.of(1, 2), 2);
    expect(() =>
      ColumnarStore.fromTrustedStore(
        schema,
        keys,
        new Map([['value', numCol]]),
      ),
    ).toThrow(/kind is 'number' but schema declares 'string'/);
  });

  it('rejects key-column kind mismatch with schema[0]', () => {
    const schema = [
      { name: 'tr', kind: 'timeRange' },
      { name: 'v', kind: 'number' },
    ] as const;
    // Schema wants timeRange; pass a Time key column.
    const keys = timeKeyColumnFromArray([1, 2]);
    const v = new Float64Column(Float64Array.of(10, 20), 2);
    expect(() =>
      ColumnarStore.fromTrustedStore(schema, keys, new Map([['v', v]])),
    ).toThrow(
      /key column kind 'time' does not match schema\[0\].kind 'timeRange'/,
    );
  });
});

/* -------------------------------------------------------------------------- */
/* Direct accessors (keyAt / beginAt / endAt / valueAt)                       */
/* -------------------------------------------------------------------------- */

describe('Direct accessors', () => {
  it('keyAt returns a Time instance for time-keyed stores', () => {
    const { store } = makeBasicStore();
    expect(store.keyAt(0)).toBeInstanceOf(Time);
    expect(store.keyAt(0).begin()).toBe(1000);
  });

  it('beginAt / endAt delegate to the key column', () => {
    const { store } = makeBasicStore();
    expect(store.beginAt(1)).toBe(2000);
    expect(store.endAt(1)).toBe(2000); // time keys: end === begin
  });

  it('valueAt reads through the named column', () => {
    const { store } = makeBasicStore();
    expect(store.valueAt(0, 'value')).toBe(10);
    expect(store.valueAt(2, 'load')).toBe(0.9);
  });

  it('valueAt throws on unknown column name', () => {
    const { store } = makeBasicStore();
    expect(() => store.valueAt(0, 'missing')).toThrow(/'missing' not present/);
  });
});

/* -------------------------------------------------------------------------- */
/* Public API invariants (the five from the RFC)                              */
/* -------------------------------------------------------------------------- */

describe('Public API invariants', () => {
  it('Invariant 1: toEvents() === toEvents() (events array identity)', () => {
    const { store } = makeBasicStore();
    expect(store.toEvents()).toBe(store.toEvents());
  });

  it('Invariant 2: at(i) reference stability — eventAt(i) === eventAt(i)', () => {
    const { store } = makeBasicStore();
    expect(store.eventAt(0)).toBe(store.eventAt(0));
    expect(store.eventAt(2)).toBe(store.eventAt(2));
  });

  it('Invariant 3: at(i) ↔ events consistency — eventAt(i) === toEvents()[i]', () => {
    const { store } = makeBasicStore();
    const events = store.toEvents();
    for (let i = 0; i < store.length; i += 1) {
      expect(store.eventAt(i)).toBe(events[i]);
    }
  });

  it('Invariant 4 (mechanism only — concat-identity test lands in step 2): eventCache pre-population inherits a row-specific event reference', () => {
    // The full TimeSeries.concat event-identity contract lands in
    // TimeSeries integration (step 2). At this layer, pin only the
    // lower-level mechanism: a pre-built cache whose entries match
    // the column's key data is inherited by the new store, so
    // `storeB.eventAt(1)` returns the same Event reference as the
    // pre-built entry.
    const { schema, keys, columns, store: storeA } = makeBasicStore();
    const eventA = storeA.eventAt(1);
    const sharedCache = new Map<
      number,
      Event<Time | TimeRange | Interval, Readonly<Record<string, unknown>>>
    >();
    sharedCache.set(1, eventA);
    const storeB = ColumnarStore.fromTrustedStore(schema, keys, columns, {
      eventCache: sharedCache,
    });
    expect(storeB.eventAt(1)).toBe(eventA);
  });

  it('Invariant 5: Symbol.iterator yields cached Event references', () => {
    const { store } = makeBasicStore();
    const fromIter: Event<
      Time | TimeRange | Interval,
      Readonly<Record<string, unknown>>
    >[] = [];
    for (const ev of store) {
      fromIter.push(ev);
    }
    expect(fromIter.length).toBe(3);
    // Each yielded event matches the same eventAt reference.
    for (let i = 0; i < store.length; i += 1) {
      expect(fromIter[i]).toBe(store.eventAt(i));
    }
  });
});

/* -------------------------------------------------------------------------- */
/* eventAt — content + bounds                                                 */
/* -------------------------------------------------------------------------- */

describe('eventAt', () => {
  it('materializes events with the correct row data', () => {
    const { store } = makeBasicStore();
    const ev = store.eventAt(1);
    expect(ev).toBeInstanceOf(Event);
    expect(ev.key()).toBeInstanceOf(Time);
    expect(ev.key().begin()).toBe(2000);
    expect(ev.data().value).toBe(20);
    expect(ev.data().load).toBe(0.75);
  });

  it('throws on out-of-range index', () => {
    const { store } = makeBasicStore();
    expect(() => store.eventAt(-1)).toThrow(RangeError);
    expect(() => store.eventAt(3)).toThrow(RangeError);
  });

  it('event data is frozen', () => {
    const { store } = makeBasicStore();
    const ev = store.eventAt(0);
    expect(Object.isFrozen(ev.data())).toBe(true);
  });
});

/* -------------------------------------------------------------------------- */
/* Native exports                                                              */
/* -------------------------------------------------------------------------- */

describe('toRows', () => {
  it('produces tuple-shaped rows [begin, ...values]', () => {
    const { store } = makeBasicStore();
    const rows = store.toRows();
    expect(rows.length).toBe(3);
    expect(rows[0]).toEqual([1000, 10, 0.5]);
    expect(rows[1]).toEqual([2000, 20, 0.75]);
    expect(rows[2]).toEqual([3000, 30, 0.9]);
  });

  it('handles invalid value-column cells (returns undefined)', () => {
    const schema = [
      { name: 'time', kind: 'time' },
      { name: 'value', kind: 'string' },
    ] as const;
    const keys = timeKeyColumnFromArray([1, 2, 3]);
    const value = stringColumnFromArray(['a', undefined, 'b']);
    const store = ColumnarStore.fromTrustedStore(
      schema,
      keys,
      new Map([['value', value]]),
    );
    const rows = store.toRows();
    expect(rows[1]).toEqual([2, undefined]);
  });
});

describe('toObjects', () => {
  it('produces object-shaped rows keyed by column name', () => {
    const { store } = makeBasicStore();
    const objs = store.toObjects();
    expect(objs.length).toBe(3);
    expect(objs[0]).toEqual({ time: 1000, value: 10, load: 0.5 });
    expect(objs[2]).toEqual({ time: 3000, value: 30, load: 0.9 });
  });

  it('includes end field for timeRange-keyed stores', () => {
    const schema = [
      { name: 'tr', kind: 'timeRange' },
      { name: 'v', kind: 'number' },
    ] as const;
    const keys = timeRangeKeyColumnFromPairs([
      [0, 10],
      [10, 20],
    ]);
    const v = new Float64Column(Float64Array.of(100, 200), 2);
    const store = ColumnarStore.fromTrustedStore(
      schema,
      keys,
      new Map([['v', v]]),
    );
    const objs = store.toObjects();
    expect(objs[0]).toEqual({ tr: 0, end: 10, v: 100 });
    expect(objs[1]).toEqual({ tr: 10, end: 20, v: 200 });
  });

  it('row objects are frozen', () => {
    const { store } = makeBasicStore();
    const objs = store.toObjects();
    expect(Object.isFrozen(objs[0])).toBe(true);
  });
});

/* -------------------------------------------------------------------------- */
/* Interval-keyed stores                                                       */
/* -------------------------------------------------------------------------- */

describe('Interval-keyed stores', () => {
  it('eventAt produces Interval keys with the right label', () => {
    const schema = [
      { name: 'bucket', kind: 'interval' },
      { name: 'count', kind: 'number' },
    ] as const;
    const begin = Float64Array.of(0, 86_400_000);
    const end = Float64Array.of(86_400_000, 172_800_000);
    const labels = stringColumnFromArray(['day-1', 'day-2'], {
      forceDict: true,
    });
    const keys = new IntervalKeyColumn(begin, end, labels, 2);
    const counts = new Float64Column(Float64Array.of(42, 99), 2);
    const store = ColumnarStore.fromTrustedStore(
      schema,
      keys,
      new Map([['count', counts]]),
    );
    const ev = store.eventAt(1);
    expect(ev.key()).toBeInstanceOf(Interval);
    expect((ev.key() as Interval).value).toBe('day-2');
    expect(ev.data().count).toBe(99);
  });
});

/* -------------------------------------------------------------------------- */
/* Independence test — REAL one. Pins the framework boundary.                  */
/* -------------------------------------------------------------------------- */

describe('Framework independence (cross-module assertion)', () => {
  // The 1a/1b smoke tests proved barrel exports resolve. This test
  // proves the framework module graph doesn't pull in TimeSeries,
  // LiveSeries, or any operator — the boundary contract from the
  // framework README.
  it('packages/core/src/columnar/*.ts does not import from TimeSeries / LiveSeries / operators', async () => {
    const { readFileSync, readdirSync } = await import('node:fs');
    const { fileURLToPath } = await import('node:url');
    const { dirname, resolve } = await import('node:path');
    // Resolve relative to this test file's location — robust to cwd
    // (the previous cwd-relative + try/catch fallback was brittle).
    const here = dirname(fileURLToPath(import.meta.url));
    const columnarDir = resolve(here, '../../src/columnar');
    const files = readdirSync(columnarDir).filter((f) => f.endsWith('.ts'));
    // Forbidden upstream modules. Each entry can be a file (`X` → caught
    // by `from '../X.js'`) or a directory (`X/` → caught by both
    // `from '../X.js'` and `from '../X/...`'`). The `from '../X'` bare
    // form catches imports without `.js` (e.g., `from '../reducers';`).
    const forbiddenFiles = [
      'TimeSeries',
      'LiveSeries',
      'PartitionedTimeSeries',
      'LivePartitionedSeries',
      'LiveAggregation',
      'LiveRollingAggregation',
      'LiveFusedRolling',
      'LiveView',
      'LiveReduce',
      'LivePartitionedFusedRolling',
      'LivePartitionedSyncRolling',
    ];
    const forbiddenDirs = ['reducers'];

    for (const f of files) {
      const content = readFileSync(resolve(columnarDir, f), 'utf8');
      for (const banned of forbiddenFiles) {
        // Match `from '../X.js'` exactly — full file path with .js
        // suffix. Avoids false-positives on prefix collisions like
        // `from '../TimeSeriesBase.js'`.
        expect(
          content.includes(`from '../${banned}.js'`),
          `${f} imports forbidden module: ../${banned}.js`,
        ).toBe(false);
        // Bare imports without .js (e.g., `from '../TimeSeries'`).
        expect(
          content.includes(`from '../${banned}';`),
          `${f} imports forbidden module: ../${banned}`,
        ).toBe(false);
      }
      for (const banned of forbiddenDirs) {
        // Subdirectory imports: `from '../reducers/X.js'`,
        // `from '../reducers/index.js'`, or bare `from '../reducers';`.
        expect(
          content.includes(`from '../${banned}/`),
          `${f} imports forbidden subdirectory: ../${banned}/`,
        ).toBe(false);
        expect(
          content.includes(`from '../${banned}.js'`),
          `${f} imports forbidden module: ../${banned}.js`,
        ).toBe(false);
        expect(
          content.includes(`from '../${banned}';`),
          `${f} imports forbidden module: ../${banned}`,
        ).toBe(false);
      }
    }
  });

  it('a store built end-to-end without importing from TimeSeries / LiveSeries works', async () => {
    // This test only uses imports from src/columnar/ and src/Event.js
    // (which is a foundational type below the framework). No TimeSeries
    // / LiveSeries imports. If those leaked into the framework files,
    // they would have been caught by the import-string assertion above.
    const { ColumnarStore, Float64Column, timeKeyColumnFromArray } =
      await import('../../src/columnar/index.js');

    const schema = [
      { name: 'time', kind: 'time' },
      { name: 'temperature', kind: 'number' },
    ] as const;
    const keys = timeKeyColumnFromArray([0, 1000, 2000]);
    const temp = new Float64Column(Float64Array.of(20, 21, 22), 3);
    const store = ColumnarStore.fromTrustedStore(
      schema,
      keys,
      new Map([['temperature', temp]]),
    );

    expect(store.length).toBe(3);
    expect(store.eventAt(1).data().temperature).toBe(21);
    expect(store.toRows()[2]).toEqual([2000, 22]);
  });
});

/* -------------------------------------------------------------------------- */
/* L2-review regressions: defensive ownership, eventCache validation, etc.    */
/* -------------------------------------------------------------------------- */

describe('Defensive ownership of columns map (Codex round-2 inherited pattern)', () => {
  it('mutating the source columns map after construction does not affect the store', () => {
    const schema = [
      { name: 'time', kind: 'time' },
      { name: 'value', kind: 'number' },
    ] as const;
    const keys = timeKeyColumnFromArray([1, 2, 3]);
    const value = new Float64Column(Float64Array.of(10, 20, 30), 3);
    const sourceMap = new Map([['value', value]]);
    const store = ColumnarStore.fromTrustedStore(schema, keys, sourceMap);

    // Attempt to mutate the source map.
    sourceMap.delete('value');
    sourceMap.set('rogue', new Float64Column(Float64Array.of(0, 0, 0), 3));

    // Store is unaffected.
    expect(store.valueAt(1, 'value')).toBe(20);
    expect(store.columns.has('value')).toBe(true);
    expect(store.columns.has('rogue')).toBe(false);
  });
});

describe('eventCache validation (L2-flagged poisoning hole)', () => {
  function makeSchemaAndKeys() {
    const schema = [
      { name: 'time', kind: 'time' },
      { name: 'value', kind: 'number' },
    ] as const;
    const keys = timeKeyColumnFromArray([1, 2, 3]);
    const value = new Float64Column(Float64Array.of(10, 20, 30), 3);
    return { schema, keys, columns: new Map([['value', value]]) };
  }

  it('rejects cache entries whose key.begin disagrees with column key.begin', () => {
    const { schema, keys, columns } = makeSchemaAndKeys();
    const poisoned = new Map<number, ColumnarEvent>();
    poisoned.set(1, new Event(new Time(99999), { value: 20 }) as ColumnarEvent);
    expect(() =>
      ColumnarStore.fromTrustedStore(schema, keys, columns, {
        eventCache: poisoned,
      }),
    ).toThrow(/key\.begin\(\).*disagree/);
  });

  it('rejects cache entries with out-of-range row index', () => {
    const { schema, keys, columns } = makeSchemaAndKeys();
    const poisoned = new Map<number, ColumnarEvent>();
    poisoned.set(99, new Event(new Time(1), { value: 10 }) as ColumnarEvent);
    expect(() =>
      ColumnarStore.fromTrustedStore(schema, keys, columns, {
        eventCache: poisoned,
      }),
    ).toThrow(/out-of-range row index 99/);
  });

  it('accepts cache entries whose key matches the column key', () => {
    const { schema, keys, columns } = makeSchemaAndKeys();
    const goodCache = new Map<number, ColumnarEvent>();
    const ev = new Event(new Time(2), { value: 20 }) as ColumnarEvent;
    goodCache.set(1, ev);
    const store = ColumnarStore.fromTrustedStore(schema, keys, columns, {
      eventCache: goodCache,
    });
    expect(store.eventAt(1)).toBe(ev);
  });

  it('defensively owns the cache — mutating the source map after construction does not affect the store', () => {
    const { schema, keys, columns } = makeSchemaAndKeys();
    const ev = new Event(new Time(2), { value: 20 }) as ColumnarEvent;
    const sourceCache = new Map<number, ColumnarEvent>();
    sourceCache.set(1, ev);
    const store = ColumnarStore.fromTrustedStore(schema, keys, columns, {
      eventCache: sourceCache,
    });
    // Inject a poisoned event into the source map AFTER construction.
    sourceCache.set(
      0,
      new Event(new Time(99999), { value: 10 }) as ColumnarEvent,
    );
    // Store ignores the poisoning — the cache was copied at construction.
    const ev0 = store.eventAt(0);
    expect(ev0.key().begin()).toBe(1); // matches the column, not the poison
  });
});

describe('Schema validation edge cases (L2)', () => {
  it('rejects duplicate schema column names', () => {
    const schema = [
      { name: 'time', kind: 'time' },
      { name: 'dup', kind: 'number' },
      { name: 'dup', kind: 'string' },
    ] as const;
    const keys = timeKeyColumnFromArray([1, 2]);
    const value = new Float64Column(Float64Array.of(1, 2), 2);
    expect(() =>
      ColumnarStore.fromTrustedStore(schema, keys, new Map([['dup', value]])),
    ).toThrow(/duplicate schema column name 'dup'/);
  });

  it('rejects extra columns not in the schema', () => {
    const schema = [
      { name: 'time', kind: 'time' },
      { name: 'value', kind: 'number' },
    ] as const;
    const keys = timeKeyColumnFromArray([1, 2]);
    const value = new Float64Column(Float64Array.of(1, 2), 2);
    const extra = new Float64Column(Float64Array.of(0, 0), 2);
    const columns = new Map([
      ['value', value],
      ['rogue', extra],
    ]);
    expect(() => ColumnarStore.fromTrustedStore(schema, keys, columns)).toThrow(
      /'rogue' which is not declared in the schema/,
    );
  });
});

describe('valueAt consistency (L2)', () => {
  it('throws on out-of-range rowIndex (matching eventAt)', () => {
    const { store } = makeBasicStore();
    expect(() => store.valueAt(-1, 'value')).toThrow(/out of range/);
    expect(() => store.valueAt(99, 'value')).toThrow(/out of range/);
  });
});

describe('Edge cases (L2 coverage gaps)', () => {
  it('zero-length store: toEvents() === toEvents() and length is 0', () => {
    const schema = [
      { name: 'time', kind: 'time' },
      { name: 'value', kind: 'number' },
    ] as const;
    const keys = timeKeyColumnFromArray([]);
    const value = new Float64Column(new Float64Array(0), 0);
    const store = ColumnarStore.fromTrustedStore(
      schema,
      keys,
      new Map([['value', value]]),
    );
    expect(store.length).toBe(0);
    expect(store.toEvents()).toBe(store.toEvents());
    expect(store.toEvents().length).toBe(0);
  });

  it('key-only schema: store works with empty columns map', () => {
    const schema = [{ name: 'time', kind: 'time' }] as const;
    const keys = timeKeyColumnFromArray([1, 2, 3]);
    const store = ColumnarStore.fromTrustedStore(schema, keys, new Map());
    expect(store.length).toBe(3);
    const ev = store.eventAt(1);
    expect(ev.key().begin()).toBe(2);
    expect(Object.keys(ev.data())).toEqual([]);
    expect(store.toRows()[1]).toEqual([2]);
  });

  it('toRows() rebuilds on each call (documented contract)', () => {
    const { store } = makeBasicStore();
    expect(store.toRows()).not.toBe(store.toRows());
  });

  it('toRows for timeRange-keyed store includes the end timestamp', () => {
    const schema = [
      { name: 'tr', kind: 'timeRange' },
      { name: 'v', kind: 'number' },
    ] as const;
    const keys = timeRangeKeyColumnFromPairs([
      [0, 10],
      [10, 25],
    ]);
    const v = new Float64Column(Float64Array.of(100, 200), 2);
    const store = ColumnarStore.fromTrustedStore(
      schema,
      keys,
      new Map([['v', v]]),
    );
    const rows = store.toRows();
    expect(rows[0]).toEqual([0, 10, 100]);
    expect(rows[1]).toEqual([10, 25, 200]);
  });

  it('toObjects() rebuilds on each call (documented contract)', () => {
    const { store } = makeBasicStore();
    expect(store.toObjects()).not.toBe(store.toObjects());
  });
});
