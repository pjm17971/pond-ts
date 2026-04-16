# Performance & weakness audit

## Critical: O(N²) in hot paths — FIXED

All five critical hot paths have been optimized. See the `PERF_*.md` files for
detailed before/after benchmarks.

### 1. `aggregate()` — was O(N × B), now O(N + B) ✅

Fixed via single-pass bucketing for time-keyed point series. 16k events:
91.15 ms → 0.53 ms (**172x**). See [PERF_AGGREGATE.md](PERF_AGGREGATE.md).

### 2. `rolling()` (event-driven) — was O(N²), now O(N) ✅

Fixed via incremental sliding-window reducers. 4k events: 366.25 ms → 2.01 ms
(**182x**). See [PERF_ROLLING.md](PERF_ROLLING.md).

### 3. `rolling()` (sequence-driven) — fixed alongside #2 ✅

Same sliding-window approach applied to sequence-driven rolling.

### 4. `smooth('movingAverage')` — was O(N²), now O(N) ✅

Fixed via sliding deque over sorted anchors. 4k events: 47.01 ms → 3.08 ms
(**15x**). See [PERF_SMOOTH.md](PERF_SMOOTH.md).

### 5. `smooth('loess')` — was O(N² log N), now ~O(N²) ✅

Fixed by precomputing defined points and avoiding full distance sort per sample.
1.6k events: 253.79 ms → 34.05 ms (**7.5x**). See
[PERF_LOESS.md](PERF_LOESS.md).

Landed in commits `05a7af3` and `60b2f07`.

---

## Significant: re-validation on every derived series — FIXED

### 6. Internal pre-validated constructor path ✅

### 7. `toRows()` → constructor round-trip eliminated ✅

An internal constructor path now accepts pre-validated events directly, skipping
cell type-checking, key normalization, and sorted-order validation for
order-preserving derived transforms (`filter`, `select`, `rename`, `collapse`,
`map`, `slice`, `within`, `before`, `after`, `trim`, `overlapping`,
`containedBy`, `asTime`, `asTimeRange`, `asInterval`).

8k events through a chained `filter → select → rename → collapse → map`
derivation: 14.15 ms → 5.55 ms (**2.5x**). See
[PERF_DERIVED_CONSTRUCTION.md](PERF_DERIVED_CONSTRUCTION.md).

Landed in commit `2ef6265`.

---

## Moderate: missed binary search opportunities — FIXED

### 8. `includesKey()` — was O(N), now O(log N) ✅

Fixed to use existing `bisect()`. 8k events with repeated lookups: 1015.50 ms →
1.24 ms (**819x**). See [PERF_INCLUDES_KEY.md](PERF_INCLUDES_KEY.md).

### 9. `#alignLinearAt()` — was O(N) + O(log N), now forward cursor ✅

Fixed by replacing repeated exact-match scan with a forward cursor. 4k events:
491.26 ms → 3.67 ms (**134x**). See
[PERF_ALIGN_LINEAR.md](PERF_ALIGN_LINEAR.md).

---

## Moderate: unnecessary allocation in tight loops

### 10. `Time.overlaps/contains/isBefore/isAfter` allocate a `TimeRange`

`Time.ts:58-75`

```ts
overlaps(other: TemporalLike): boolean {
  return this.timeRange().overlaps(other);
}
```

Every temporal comparison on a `Time` key creates a throwaway `TimeRange`. For a
point, `overlaps` is just `timestamp >= other.begin() && timestamp <= other.end()`.
This matters because these are called inside inner loops of `filter`, `within`,
`overlapping`, `trim`, etc.

Same issue on `Interval` — all four temporal methods delegate through
`this.timeRange()` (`Interval.ts:124-141`), allocating a `TimeRange` each time.
`Interval` already has `start` and `endMs` fields — the comparison is direct
arithmetic.

### 11. `Object.freeze` on every `Event`

`Event.ts:49-51`

```ts
this.#data = Object.freeze({ ...data }) as Readonly<D>;
Object.freeze(this);
```

Every event construction spreads the data into a new object, freezes it, and
freezes the event. With private fields (`#key`, `#data`) the instance is already
effectively immutable. The freeze adds measurable overhead when constructing
thousands of events (which happens on every derived series due to issue #6).

### 12. `rows` getter does full materialization

`TimeSeries.ts:721-725`

```ts
get rows(): ReadonlyArray<NormalizedRowForSchema<S>> {
  return toRows(this.schema, this.events) as ...;
}
```

This is a getter that allocates N frozen arrays on every access. Callers may
reasonably expect a property to be cheap. Either cache the result lazily or make
it a method.

---

## Minor: algorithmic inefficiencies

### 13. `aggregateValues` filters twice

`TimeSeries.ts:461-465`

```ts
const defined = values.filter((value) => value !== undefined);
const numeric = defined.filter((value) => typeof value === 'number');
```

Two passes for `sum`/`avg`/`min`/`max` when one would do. Small per-call, but
called once per column per bucket/event in aggregate/rolling.

### 14. `compareEventKeys` uses `localeCompare` for tiebreaking

`temporal.ts:53`

```ts
return left.type().localeCompare(right.type());
```

`localeCompare` is ~10x slower than `<`/`>` comparison. This is the comparator
used in sorted order validation and bisect — it runs on every pair. Since the
values are fixed strings (`'time'`, `'timeRange'`, `'interval'`), a simple `<`
suffices.

### 15. `joinMany` does repeated pairwise joins

`TimeSeries.ts:667-688`

```ts
for (const next of rest) {
  joined = joined.join(next) ...;
}
```

Each pairwise join creates a new `TimeSeries` (re-validation, row round-trip).
An N-way sorted merge would be one pass producing one output.

### 16. `parseDurationInput` is duplicated

Exists in both `TimeSeries.ts:492-520` and `Sequence.ts:72-100`. Not a
performance issue, but a maintenance risk.

---

## Summary

| # | Method(s) | Was | Now | Status |
|---|-----------|-----|-----|--------|
| 1 | `aggregate` | O(N × B) | O(N + B) | ✅ **172x** at 16k |
| 2 | `rolling` (event) | O(N²) | O(N) | ✅ **182x** at 4k |
| 3 | `rolling` (sequence) | O(N × B) | O(N + B) | ✅ fixed with #2 |
| 4 | `smooth('movingAverage')` | O(N²) | O(N) | ✅ **15x** at 4k |
| 5 | `smooth('loess')` | O(N² log N) | ~O(N²) | ✅ **7.5x** at 1.6k |
| 6 | All derived series | O(N) re-validation | O(1) | ✅ **2.5x** at 8k |
| 7 | All derived series | rows→events round-trip | 0 passes | ✅ fixed with #6 |
| 8 | `includesKey` | O(N) | O(log N) | ✅ **819x** at 8k |
| 9 | `#alignLinearAt` | O(N) + O(log N) | forward cursor | ✅ **134x** at 4k |
| 10 | `Time`/`Interval` comparisons | 1 alloc/call | 0 | open |
| 11 | `Event` constructor | freeze × 2 | skip freeze | open |
| 12 | `rows` getter | O(N) per access | cached or method | open |
| 13 | `aggregateValues` | 2 filter passes | 1 pass | open |
| 14 | `compareEventKeys` | `localeCompare` | `<` compare | open |
| 15 | `joinMany` | K pairwise joins | 1 N-way merge | open |
| 16 | `parseDurationInput` | duplicated | shared util | open |

All high and medium-impact items (#1–9) are resolved. The remaining open items
(#10–16) are lower-priority constant-factor improvements that can be addressed
incrementally.
