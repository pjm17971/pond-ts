# Performance & weakness audit

## Critical: O(N²) in hot paths

### 1. `aggregate()` — O(N × B) full scan per bucket

`TimeSeries.ts:1092`

```ts
const resultRows = buckets.map((bucket) => {
  const contributors = this.events.filter((event) =>
    bucketOverlapsHalfOpen(bucket, event.key()),
  );
```

For every bucket, this scans the entire events array. With N events and B
buckets, that's O(N × B). Since events are sorted by time, a single pass or a
bisect-per-bucket would bring this to O(N + B) or O(B log N).

For 10k events across 100 buckets, this does 1M comparisons instead of ~10k.

### 2. `rolling()` (event-driven) — O(N²) full scan per event

`TimeSeries.ts:1277-1280`

```ts
const resultRows = this.events.map((event) => {
  const anchor = event.begin();
  const contributors = this.events.filter((candidate) =>
    anchorInWindow(candidate.begin(), anchor),
  );
```

Every event scans every other event. Since events are sorted, a sliding
two-pointer window would be O(N). This is the biggest performance issue in the
library — rolling windows are typically used on large series.

### 3. `rolling()` (sequence-driven) — same O(N × B) pattern

`TimeSeries.ts:1248`

Same `events.filter(...)` inside a `buckets.map(...)`. Same fix: bisect or
two-pointer.

### 4. `smooth('movingAverage')` — O(N²)

`TimeSeries.ts:1472-1478`

```ts
const values = sourceValues
  .filter((_, candidateIndex) =>
    anchorInWindow(anchors[candidateIndex]!, anchor),
  )
```

Same full-scan-per-event pattern. Should use a sliding window deque over sorted
anchors.

### 5. `smooth('loess')` — O(N² log N)

`TimeSeries.ts:1323-1340`

`loessAt()` is called per-event, and internally sorts all N points by distance
every time:

```ts
const sortedDistances = points
  .map((point) => ({ point, distance: Math.abs(point.x - x) }))
  .sort((left, right) => left.distance - right.distance);
```

N calls × (N map + N log N sort) = O(N² log N). The neighbor set shifts
incrementally between consecutive events, so a sliding window with insertion
sort or a KD-tree would help.

---

## Significant: re-validation on every derived series

### 6. Every transform re-validates through the constructor

`TimeSeries.ts:708-712`

`filter()`, `slice()`, `within()`, `before()`, `after()`, `select()`,
`rename()`, `map()`, `collapse()`, `asTime()`, `asTimeRange()`, `asInterval()`,
`trim()`, `overlapping()`, `containedBy()` — all of these create a
`new TimeSeries(...)` which calls `validateAndNormalize`, which:

- Type-checks every cell in every row
- Normalizes every key
- Checks sorted order

This is wasted work when the input was already validated. For a simple
`series.filter(...)` on a 10k-event series that keeps 5k events, you're
re-validating 5k events that were already validated on construction.

Fix: an internal constructor path (e.g., `TimeSeries.fromEvents(...)`) that
accepts pre-validated events directly and skips re-validation.

### 7. The `toRows()` → constructor round-trip

`TimeSeries.ts:209-222`

Most methods do this:

```
events → toRows() (decompose into row arrays) → new TimeSeries() → validateAndNormalize() (recompose into events)
```

This is a full serialize/deserialize cycle for data that's already in the right
form. The internal constructor path above would eliminate this entirely.

---

## Moderate: missed binary search opportunities

### 8. `includesKey()` — O(N) linear scan

`TimeSeries.ts:1557-1559`

```ts
includesKey(key: KeyLike): boolean {
  const normalizedKey = toKey(key);
  return this.events.some((event) => event.key().equals(normalizedKey));
}
```

`bisect` already exists and gives O(log N). This should use it:

```ts
const index = this.bisect(normalizedKey);
return index < this.events.length && this.events[index]!.key().equals(normalizedKey);
```

### 9. `#alignLinearAt()` — O(N) find before O(log N) bisects

`TimeSeries.ts:1874`

```ts
const exact = this.find((event) => event.begin() === t);
```

This scans the entire array looking for an exact match before falling back to
`atOrBefore`/`atOrAfter` which use bisect. Should just bisect once and check for
exact match at that index.

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

| # | Method(s) | Current | Should be | Impact |
|---|-----------|---------|-----------|--------|
| 1 | `aggregate` | O(N × B) | O(N + B) | **High** at scale |
| 2 | `rolling` (event) | O(N²) | O(N) | **High** at scale |
| 3 | `rolling` (sequence) | O(N × B) | O(N + B) | **High** at scale |
| 4 | `smooth('movingAverage')` | O(N²) | O(N) | **High** at scale |
| 5 | `smooth('loess')` | O(N² log N) | O(N²) or better | **High** at scale |
| 6 | All derived series | O(N) re-validation | O(1) | **Medium** cumulative |
| 7 | All derived series | rows→events round-trip | 0 passes | **Medium** cumulative |
| 8 | `includesKey` | O(N) | O(log N) | **Medium** |
| 9 | `#alignLinearAt` | O(N) + O(log N) | O(log N) | **Medium** |
| 10 | `Time`/`Interval` comparisons | 1 alloc/call | 0 | **Low–Medium** in loops |
| 11 | `Event` constructor | freeze × 2 | skip freeze | **Low–Medium** at volume |
| 12 | `rows` getter | O(N) per access | cached or method | **Low** (API surprise) |
| 13 | `aggregateValues` | 2 filter passes | 1 pass | **Low** |
| 14 | `compareEventKeys` | `localeCompare` | `<` compare | **Low** |
| 15 | `joinMany` | K pairwise joins | 1 N-way merge | **Low** unless K is large |
| 16 | `parseDurationInput` | duplicated | shared util | **Low** (maintenance) |

The O(N²) methods (#1–5) are by far the most important to fix — they're the
operations users call on their largest series. The re-validation/round-trip issue
(#6–7) is a multiplier that makes everything else slower than it needs to be. The
allocation issues are noise for small series but compound at scale.
