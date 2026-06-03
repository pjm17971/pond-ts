# Brief: collect-output columnar arc (§A pull/read — the baseline-path prize)

**Status:** design brief — the arc the measurements greenlit (user committed
2026-06-03). **Re-opens HELD [#175](https://github.com/pjm17971/pond-ts/pull/175).**
The build stays **gated after this brief** and per-piece (each piece is its own
reviewed PR; the public read surface and the #175 un-hold are human gates).
This is the execution plan for the biggest increment of the §A pull/read cut —
zero-copy for the dashboard's _baseline-band_ path, which increment 1 (0.19.0)
and increment 2's `windowColumn` spike deliberately left on the Event backing.

## Why now — the numbers are in

Two benches on `spike/structural-window` size the prize end to end:

- **`perf-baseline-memo-split.mjs`** — the dashboard baseline memo is
  **90–96% gather** (building typed arrays from the collected series), **<11%
  sigma** (the `avg ± σ·sd` band loop). The attackable share is almost all of it.
- **`perf-liveview-structural.mjs`** — `windowColumn` (read straight off chunk
  buffers, no `Event`) is a flat **~14×** over the increment-1 `Event.get()`
  gather on the realistic multi-chunk path, **effectively free** single-chunk.

So: ~90% of the memo, attacked ~14× / zero-copy. Decisive at the dashboard's
ceiling (the render-axis-at-scale bet); meaningful but not urgent at today's
cell (the absolute saving is ~2.5 ms/tick at 96k).

## The pipeline today — the backing audit (the crux)

The dashboard baseline is `live.partitionBy('host').rolling(W, …).collect()`,
then read as `collected.window('5m').partitionBy('host').toMap(gather)`. The
storage backing at each stage (verified on `main` + #175's branch):

| stage | what it holds | backing | zero-copy-able? |
| --- | --- | --- | --- |
| `partitionBy` sub-series | routed raw values per host | **chunked** (✓ #175, held) | yes (contiguous per host) |
| `rolling(W)` output | `avg`/`sd`/… per host | **`Event[]`** (`#outputEvents`) | **no — not chunked** |
| `collect()` unified | all hosts, time-interleaved | **`Event[]`** (per-event `_pushTrustedEvents` fan-in) | no |
| dashboard read | `window → partitionBy → gather` | re-partitions the unified series | **no — scatters** |

`windowColumn`'s win lands only on a **contiguous** row range. Two facts fall
out of the table:

1. **The dashboard re-partitions the unified series at read time.** Hosts are
   time-interleaved in `collect()`'s output, so re-partitioning scatters each
   host's rows — `windowColumn` cannot apply. Chunking `collect()`'s output
   alone does **not** give the dashboard zero-copy.
2. **The aggregates the dashboard actually charts (`avg`/`sd`) live in the
   rolling output, which is `Event[]`-backed.** #175 chunked the _routing_, not
   the _aggregates_.

## The decision — per-partition-direct read (not unify-then-re-partition)

Contiguity per host means reading **each partition's chunked rolling output
directly**, skipping the `collect()` unify-then-re-partition round-trip:

```
for each host H:  subSeriesForH.windowColumn('avg', start, end)  // zero-copy / 14×
                  subSeriesForH.windowColumn('sd',  start, end)
                  → band  (sigma, unchanged)
```

The dashboard already renders **per host** (one line + one band each), so
iterating partitions is no imposition — it removes a round-trip. This is the
clean zero-copy path, and it is what #175's per-partition chunked sub-series
were built to enable.

A partition-major `collect()` layout (all of host A, then host B) would also be
contiguous, but it breaks the unified series' time-sort that time-axis
rendering needs — rejected. Per-partition-direct it is.

## The arc — pieces, each a gated PR

- **P0 — un-hold + re-review #175.** Its coalescing staging tier (`stageRows` →
  flush one packed chunk per 256 rows) is the shared enabler for chunking _any_
  per-event fan-in. Re-review fresh (L2 + Codex) — it was reviewed-but-held on
  OOM grounds; the new justification is the render-axis/collect-output win.
  Decide: merge as-is, or rework for the arc.
- **P1 — chunk the rolling output per partition (the new core work).** Route the
  rolling reducer's `#outputEvents` fan-in through #175's coalescing tier so each
  partition's `avg`/`sd`/… land in a chunked sub-series. This is the piece
  beyond #175. **Input-shape, not just cadence** (#175 final review): #175's
  `stageRows` slices a _source store_ via `beginAt`/`valueAt`, but the rolling
  output is freshly-built `Event`s with no source store — so P1 adds a
  **tuple-level staging entry** (stage a built row-tuple directly) reusing the
  coalescing tier's flush/commit machinery but not its source-slice intake.
  Heap + per-partition-read bench gate.
- **P2 — per-partition `windowColumn` read surface (public; API gate).** A way
  to access each partition's chunked sub-series and `windowColumn` it. **Read
  contract must patch the pending tier** (#175 final review): the coalescing
  tier's pending rows (≤ `flushThreshold` per partition — the freshest data, the
  live tail a dashboard charts) are boxed row-tuples, not a typed-array column,
  so the windowed read concats `[zero-copy committed chunk slices] + [the
  pending tail materialized to a small typed array]`. The tail is ≤255 rows ×
  columns — negligible vs the N-row window, so the zero-copy win holds for the
  bulk; but the read is _wrong_ if it skips the tail. Widens the public live
  surface → human sign-off + L2 + Codex. Experimental (0.19.x).
- **P3 — wire the dashboard + A/B.** Shift the baseline read from
  unify-then-re-partition to per-partition-direct; dashboard agent runs the A/B
  on their real memo + harness. The binding realized-win number.
- **Pα (optional, independent) — chunk `collect()`'s unified output.** Route its
  per-event `_pushTrustedEvents` through the coalescing tier so the unified
  series is chunked. #175's coalescing enables this directly — but it serves
  only consumers that read the unified numeric columns _whole_ (no per-host
  split), **not** the dashboard's bands. Bank it only if a unified consumer
  needs it; it is not on the dashboard's critical path.

## Decisions to resolve during the build

1. **Read-surface shape (P2).** Per-partition accessor returning a small read
   object with `windowColumn(name, window)` / a key window; or extend the
   `partitionBy(...).toMap(...)` shape to hand each group a columnar read. Lead
   with the smallest contained surface; reuse increment-2's `Float64Array`
   return (the consumer's actual need).
2. **React invalidation across partitions.** One `useLiveVersion` bump for the
   partitioned source on any partition update, vs per-partition subscriptions.
   Start with one bump (simplest; matches the source granularity).
3. **Rolling-output staging mechanism (P1) — input shape, then cadence.**
   `stageRows` can't be reused verbatim: it slices a source store, and the
   rolling output has none. Add a tuple-level staging entry (stage a built
   row-tuple) on the coalescing tier. _Then_ consider cadence — rolling emits
   one output per window step, sparser than raw ingest, so the 256-row flush
   threshold may want a time/idle-based flush too, or the live tail sits in the
   pending tier longer (P2's read patch handles correctness regardless).
4. **Empty-window + warm-up contract.** `windowColumn` throws on empty today
   (spike). The rolling warm-up emits `NaN`/undefined before the first full
   window — decide the read contract for partial windows.
5. **Partition-key contiguity guarantee.** Confirm a partition's rolling output
   is appended strictly in time order (it is, per partition) so the chunked
   sub-series stays sorted and `windowColumn` is valid.

## Scope guards — what the arc does NOT touch

- **Chunked, strict, time-keyed only.** Other backings keep increment 1's
  allocation-skip.
- **The `Event[]` fan-in stays the default** for non-chunked / internal paths;
  the coalescing route is additive.
- **No §B** (reorder / corral). Append-only.
- **No partition-major `collect()`** (breaks time-sort). Per-partition-direct.
- **Sigma stays as-is** — it is <11% of the memo; not worth touching.

## Measurement gates (no piece merges without numbers)

- **P1:** retained-heap delta (chunked rolling output vs `Event[]`, process
  isolation) + a per-partition `windowColumn` read bench vs the increment-1
  per-partition gather, at the dashboard cells.
- **P3:** dashboard A/B on the real baseline memo — the binding number. The
  in-pond benches size it; the A/B confirms it in React with real data.

## API gate (do not skip)

P2 adds a public per-partition columnar read surface → human sign-off + Layer-2
+ a Codex pass, same as increment 1. Experimental; surface may change. P0's
#175 un-hold is a separate human gate (re-opening held work).

## The #175 un-hold / re-review plan

#175 is OPEN, `feat/columnar-partition-routing`, `_`-internal only (no public
surface), 2028 tests green, gRPC V8 cleared every gate (60× fewer
ColumnarStores, −99.4% Event retention, +24% throughput). Held only because the
partition-retention OOM wasn't a binding production need at the time. The arc
supplies a new, render-axis justification. Re-review fresh before building P1
on it — it has diverged from `main` (it predates 0.19.0 + the `windowColumn`
spike, and both touch `live-chunked-storage.ts`; expect a rebase).

## Cross-references

- `docs/briefs/liveview-structural-window-spike.md` — increment 2 + the
  `windowColumn` substrate this arc reads through (results: ~14× / zero-copy).
- `docs/briefs/column-on-liveview-spike.md` — increment 1 (allocation-skip),
  shipped 0.19.0 (#179).
- [#175](https://github.com/pjm17971/pond-ts/pull/175) — per-partition chunked
  routing + coalescing (HELD; the arc's P0).
- `docs/briefs/column-native-live-pipeline.md` — the chunked backing (Phase 1).
- `packages/core/scripts/perf-baseline-memo-split.mjs`,
  `perf-liveview-structural.mjs` — the two sizing benches.
- `packages/core/src/live/live-partitioned-series.ts` — `collect()` per-event
  fan-in (the `__backing: 'array'` + `_pushTrustedEvents` path Pα would reroute).
- `packages/core/src/live/live-rolling-aggregation.ts` — `#outputEvents` (the
  Event-backed rolling output P1 would chunk).
