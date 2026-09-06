import type { ColumnValue } from '../schema/index.js';
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
    entries: RollingWindowEntry<ColumnValue>[],
    head: number,
  ) => ColumnValue | undefined,
): RollingReducerState {
  let entries: RollingWindowEntry<ColumnValue>[] = [];
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

/**
 * [PND-LIVFIX] Ordered entries that tolerate removal of ANY index, not only
 * the head. `rollingOrderedEntries` above is a forward-sliding window: it
 * assumes evictions arrive oldest-added-first, which holds for append-only
 * sources but not for a `reorder` source with retention, whose sorted-prefix
 * eviction can drop a middle or the newest arrival — and then `first` /
 * `last` kept reporting an evicted value. Entries live in a `Map` keyed by
 * index (insertion order = arrival order) so any index removes in O(1);
 * `first` is the map's first value and `last` is a tracked tail index,
 * re-derived by a scan only when the tail itself is evicted (rare).
 */
export function rollingIndexedEntries(
  pick: 'first' | 'last',
): RollingReducerState {
  const items = new Map<number, ColumnValue>();
  let tail: number | undefined;
  return {
    add(index, value) {
      if (value === undefined) return;
      items.set(index, value);
      if (tail === undefined || index > tail) tail = index;
    },
    remove(index, value) {
      if (value === undefined) return;
      if (!items.delete(index)) return;
      if (index === tail) {
        tail = undefined;
        for (const k of items.keys())
          if (tail === undefined || k > tail) tail = k;
      }
    },
    snapshot() {
      if (items.size === 0) return undefined;
      if (pick === 'first') return items.values().next().value;
      return tail === undefined ? undefined : items.get(tail);
    },
  };
}

/**
 * [PND-LIVFIX] `min` / `max` over a sorted array — removal by value, so the
 * extreme is exact whatever order evictions arrive in. O(n) per insert /
 * remove (a `splice`), against the monotone deque's amortised O(1); it is
 * selected only for sources whose eviction order is not arrival order
 * (`reorder` + retention), where the deque is wrong rather than slow.
 */
export function rollingSortedExtreme(pick: 'min' | 'max'): RollingReducerState {
  const state = rollingSortedArray();
  return {
    add: state.add,
    remove: state.remove,
    snapshot() {
      const s = state.sorted;
      if (s.length === 0) return undefined;
      return pick === 'min' ? s[0] : s[s.length - 1];
    },
  };
}

export function rollingSortedArray(): {
  sorted: number[];
  add(index: number, value: ColumnValue | undefined): void;
  remove(index: number, value: ColumnValue | undefined): void;
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
