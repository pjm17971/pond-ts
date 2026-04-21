import type { ScalarValue } from '../types.js';
import type { RollingReducerState } from './types.js';

type RollingWindowEntry<T> = { index: number; value: T };

function compactEntries<T>(
  entries: RollingWindowEntry<T>[],
  head: number,
): [RollingWindowEntry<T>[], number] {
  if (head > 0 && head * 2 >= entries.length) return [entries.slice(head), 0];
  return [entries, head];
}

export function rollingMonotoneDeque(
  compare: (a: number, b: number) => boolean,
): RollingReducerState {
  let entries: RollingWindowEntry<number>[] = [];
  let head = 0;
  return {
    add(index, value) {
      if (typeof value !== 'number') return;
      while (entries.length > head) {
        if (compare(entries[entries.length - 1]!.value, value)) break;
        entries.pop();
      }
      entries.push({ index, value });
    },
    remove(index, value) {
      if (typeof value !== 'number') return;
      if (entries[head]?.index === index) {
        head += 1;
        [entries, head] = compactEntries(entries, head);
      }
    },
    snapshot() {
      return entries[head]?.value;
    },
  };
}

export function rollingOrderedEntries(
  pick: (
    entries: RollingWindowEntry<ScalarValue>[],
    head: number,
  ) => ScalarValue | undefined,
): RollingReducerState {
  let entries: RollingWindowEntry<ScalarValue>[] = [];
  let head = 0;
  return {
    add(index, value) {
      if (value !== undefined) entries.push({ index, value });
    },
    remove(index, value) {
      if (value === undefined) return;
      if (entries[head]?.index === index) {
        head += 1;
        [entries, head] = compactEntries(entries, head);
      }
    },
    snapshot() {
      return pick(entries, head);
    },
  };
}

export function rollingSortedArray(): {
  sorted: number[];
  add(index: number, value: ScalarValue | undefined): void;
  remove(index: number, value: ScalarValue | undefined): void;
} {
  const sorted: number[] = [];
  function bisect(v: number): number {
    let lo = 0;
    let hi = sorted.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (sorted[mid]! < v) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }
  return {
    sorted,
    add(_index, value) {
      if (typeof value !== 'number') return;
      sorted.splice(bisect(value), 0, value);
    },
    remove(_index, value) {
      if (typeof value !== 'number') return;
      const pos = bisect(value);
      if (pos < sorted.length && sorted[pos] === value) sorted.splice(pos, 1);
    },
  };
}
