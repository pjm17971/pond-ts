import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { TimeSeries } from 'pond-ts';
import type { SeriesSchema } from 'pond-ts';
import {
  STUDIES,
  STUDY_FAMILIES,
  studyDescriptor,
} from '../src/catalog/index.js';
import type { StudyDescriptor } from '../src/catalog/index.js';

/*
 * The runtime half of the catalog's drift guard (the compile-time half is
 * `defineStudy`). Every descriptor is run against its study:
 *
 * - the columns it appends are exactly the ones the descriptor names;
 * - passing every default explicitly changes nothing, so the defaults the
 *   descriptor states are the ones the study uses;
 * - every menu value runs; a value below a declared `min` throws;
 * - and the catalog is exactly the set of fluent methods, so a study
 *   added to the package without a descriptor fails here.
 */

// A deterministic 120-bar OHLCV fixture with a session-id column stepping
// every 20 bars (for the session-anchored studies' column door).
const schema = [
  { name: 'time', kind: 'time' },
  { name: 'open', kind: 'number' },
  { name: 'high', kind: 'number' },
  { name: 'low', kind: 'number' },
  { name: 'close', kind: 'number' },
  { name: 'volume', kind: 'number' },
  { name: 'sess', kind: 'number' },
] as const;

function fixture(): TimeSeries<typeof schema> {
  let seed = 12345;
  const next = () => {
    seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
    return seed / 2 ** 32;
  };
  const rows: Array<[number, number, number, number, number, number, number]> =
    [];
  let close = 100;
  for (let i = 0; i < 120; i += 1) {
    const open = close;
    close = Math.max(1, open + (next() - 0.5) * 4);
    const high = Math.max(open, close) + next() * 2;
    const low = Math.min(open, close) - next() * 2;
    const volume = Math.round(1000 + next() * 9000);
    rows.push([i * 60_000, open, high, low, close, volume, Math.floor(i / 20)]);
  }
  return new TimeSeries({ name: 'bars', schema, rows });
}

const bars = fixture() as unknown as TimeSeries<SeriesSchema>;

function columnNames(s: TimeSeries<SeriesSchema>): string[] {
  return (s.schema as ReadonlyArray<{ name: string }>).map((c) => c.name);
}

function columnValues(
  s: TimeSeries<SeriesSchema>,
  name: string,
): Array<number | undefined> {
  return (
    s as unknown as {
      events: ReadonlyArray<{ data(): Record<string, unknown> }>;
    }
  ).events.map((e) => {
    const v = e.data()[name];
    return typeof v === 'number' ? v : undefined;
  });
}

/** The smallest options that make the study runnable: required things only. */
function minimalOptions(d: StudyDescriptor): Record<string, unknown> {
  const o: Record<string, unknown> = {};
  for (const input of d.inputs) {
    // A required column (a `benchmark`) must differ from the defaulted
    // source: `correlation` rejects a column correlated with itself.
    if (input.default === undefined) o[input.role] = 'open';
  }
  for (const [name, p] of Object.entries(d.params)) {
    if (p.default === undefined) o[name] = p.example;
  }
  if (d.anchor === 'session') o['session'] = 'sess';
  return o;
}

/** Every default stated explicitly, on top of the minimal options. */
function explicitOptions(d: StudyDescriptor): Record<string, unknown> {
  const o = minimalOptions(d);
  for (const input of d.inputs) {
    if (input.default !== undefined) o[input.role] = input.default;
  }
  for (const [name, p] of Object.entries(d.params)) {
    if (p.default !== undefined) o[name] = p.default;
  }
  o[d.naming.kind] = d.naming.default;
  return o;
}

function expectedColumns(d: StudyDescriptor): string[] {
  return d.naming.kind === 'output'
    ? [d.naming.default]
    : d.outputs.map((out) => `${d.naming.default}${out.id}`);
}

describe('study catalog', () => {
  it('names are unique and every family is a known one', () => {
    const names = STUDIES.map((d) => d.name);
    expect(new Set(names).size).toBe(names.length);
    for (const d of STUDIES) expect(STUDY_FAMILIES).toContain(d.family);
  });

  it('is exactly the set of fluent methods', () => {
    const fluent = readFileSync(
      fileURLToPath(new URL('../src/fluent.ts', import.meta.url)),
      'utf8',
    );
    const methods = [...fluent.matchAll(/^proto\.(\w+) = /gm)].map(
      (m) => m[1]!,
    );
    expect(methods.length).toBeGreaterThan(0);
    const catalog = STUDIES.map((d) => d.name).sort();
    expect(catalog).toEqual([...methods].sort());
  });

  it('studyDescriptor looks a study up by name', () => {
    expect(studyDescriptor('atr')?.family).toBe('volatility');
    expect(studyDescriptor('nope')).toBeUndefined();
  });

  describe.each(STUDIES.map((d) => [d.name, d] as const))('%s', (_, d) => {
    it('has a well-formed shape', () => {
      expect(d.summary.length).toBeGreaterThan(0);
      expect(d.naming.default.length).toBeGreaterThan(0);
      expect(d.outputs.length).toBeGreaterThan(0);
      if (d.naming.kind === 'output') {
        expect(d.outputs.map((o) => o.id)).toEqual(['']);
      } else {
        // Suffixes are unique; at most one output is the bare prefix ('').
        const ids = d.outputs.map((o) => o.id);
        expect(new Set(ids).size).toBe(ids.length);
        expect(ids.filter((id) => id === '').length).toBeLessThanOrEqual(1);
      }
      for (const p of Object.values(d.params)) {
        // Optional ⇒ default; required ⇒ example. Never both, never neither.
        expect((p.default === undefined) !== (p.example === undefined)).toBe(
          true,
        );
        if (p.kind === 'enum') {
          expect(p.of.length).toBeGreaterThan(0);
          if (p.default !== undefined) expect(p.of).toContain(p.default);
        } else if (p.suggest !== undefined) {
          const [lo, hi] = p.suggest;
          expect(lo).toBeLessThanOrEqual(hi);
          if (p.min !== undefined) expect(lo).toBeGreaterThanOrEqual(p.min);
          if (p.max !== undefined) expect(hi).toBeLessThanOrEqual(p.max);
        }
      }
    });

    it('appends exactly the columns it declares', () => {
      const before = columnNames(bars);
      const after = columnNames(d.run(bars, minimalOptions(d)));
      expect(after.slice(0, before.length)).toEqual(before);
      expect(after.slice(before.length)).toEqual(expectedColumns(d));
    });

    it('states the defaults the study actually uses', () => {
      const implicit = d.run(bars, minimalOptions(d));
      const explicit = d.run(bars, explicitOptions(d));
      for (const name of expectedColumns(d)) {
        expect(columnValues(explicit, name)).toEqual(
          columnValues(implicit, name),
        );
      }
    });

    it('runs on every menu value, and rejects a value below a declared min', () => {
      for (const [name, p] of Object.entries(d.params)) {
        if (p.kind === 'enum') {
          for (const value of p.of) {
            expect(() =>
              d.run(bars, { ...minimalOptions(d), [name]: value }),
            ).not.toThrow();
          }
        } else if (p.min !== undefined) {
          expect(() =>
            d.run(bars, { ...minimalOptions(d), [name]: p.min! - 1 }),
          ).toThrow();
        }
      }
    });
  });
});
