# Column-native live pipeline (design brief)

**Status:** Draft, 2026-05-29. **Awaiting design review before implementation.**
**Author:** pond-ts library agent (Claude).
**Supersedes:** the Step 7 ring-buffer approach
([brief](step-7-live-series-ring-buffer.md), walked back — see its §13).
**Motivation:** the gRPC aggregator OOMs at high partition count.

## TL;DR

`LiveSeries` retains a window of `Event` objects. At high partition
count that retained set is the dominant heap consumer and the
aggregator dies with OOM. Replace the per-`LiveSeries` retained
storage with a **chunked columnar buffer** — each `pushMany` batch is
validated directly into typed-array columns (no per-row `Event`) and
appended as a chunk; retention drops/slices chunks off the front.

Measured (spike, `scripts/investigate-batch.mjs`, batched ingest
300k/1k-batch/50k-window):

|                                 | array-events (today) | chunked-columnar | win      |
| ------------------------------- | -------------------- | ---------------- | -------- |
| ingest                          | 48.2 ms              | 12.9 ms          | **3.7×** |
| consume (windowed reduce)       | 0.106 ms             | 0.052 ms         | **2×**   |
| **retained heap** (200k window) | **31.5 MB**          | **6.9 MB**       | **4.6×** |

The 4.6× retained-heap cut is the headline — it's the OOM fix. It
compounds across partitions: V5 measured 92k/s × 1k hosts at
**1857 MB**; a 4.6× cut targets ~400-600 MB.

## Why this wins where the Step 7 ring lost

The Step 7 ring buffer was **9× slower** on this exact workload. The
difference is one thing: **whether an `Event` is ever created.**

- **Step 7 ring:** `create Event` (for the `'event'` listener) →
  _decompose_ it into ring columns. Paid for Events AND columns.
- **This design:** rows → `validateAndNormalizeColumnar` → typed-array
  columns, **directly**. No `Event` created on the columnar path.
  Consumers that read columns never touch an `Event`.

The 9× loss was the Event tax stacked on the columnar tax. Remove the
`Event` from the path and columnar wins on all three axes. The
corollary: **the win only exists if the consume side is columnar
too** — see §3 (the load-bearing condition).

## 1. Where the retained heap goes today

`LiveSeries` (array backing) holds `EventForSchema<S>[]` — one `Event`
(+ `Time` key + frozen data dict) per retained row. For a window of W
rows that's ~31 MB at W=200k (measured), tenured (survives GC,
old-gen). With `partitionBy('host')` over P hosts, it's P × that. The
retained `Event[]` is what OOMs the aggregator, not transient churn.

A chunked columnar buffer holds the same W rows as typed-array columns
(~7 MB at W=200k) and retains **zero `Event` objects**. That's the
4.6× cut.

## 2. The design — chunked columnar live buffer

A new `LiveStorage` backing (slots into the PR-2a strategy layer that
already shipped, #168):

```
ChunkedColumnarLiveStorage
  #chunks: ColumnarStore<S>[]    // each = one validated pushMany batch
  #chunkOffsets: number[]         // prefix sum of chunk lengths (for at(i) bisect)
  #total: number                  // sum of chunk lengths
  #eventCache: Map<number, Event> // lazy at(i) materialization, remapped on evict
```

- **append (pushMany):** `validateAndNormalizeColumnar(batch)` →
  `ColumnarStore` → push as a chunk. No per-row `Event`. (Reuses the
  Step 2c intake path verbatim.)
- **retention:** drop whole chunks off the front while the oldest is
  fully out of window; slice the boundary chunk for exact `maxEvents`
  (see §4 Q1). O(chunks), no per-row work, no copy on whole-chunk
  drops.
- **at(i):** bisect `#chunkOffsets` → (chunk, localIndex) → read the
  row's columns → materialize + cache an `Event`. Same lazy-cache +
  evict-remap shape the Step 7 ring used (and the same care required —
  see §5 risks).
- **snapshot / toTimeSeries:** `concatSorted(#chunks)` (the chunks are
  temporally disjoint + sorted — exactly what `concatSorted` from step
  1g was built for) → wrap as `TimeSeries`. Or materialize. Likely
  much cheaper than today's row-rebuild.

## 3. The load-bearing condition: column-native consume

The retained-heap win (Phase 1) holds **even with Event-based
consumers**, because the consumer extracts values and discards the
`Event` — it's the _buffer_ that retains Events today, not the
consumer. So a chunked buffer cuts the retained set regardless of how
consumers read.

But the _throughput / GC-rate_ win (the V5 22% GC) requires the
consume side to go columnar too: the `'event'` listener fires per row
and forces a transient `Event` materialization per push. Those are
young-gen (collected fast) so they don't OOM — but they're the 22% GC
_rate_. Removing them needs the rolling reducer to consume columns
(= Step 3C). Hence the phasing:

## 4. Phasing

### Phase 1 — chunked columnar buffer + column-native intake (the OOM fix)

`ChunkedColumnarLiveStorage` + `pushMany` column-native intake.
Retained heap drops ~4.6×. The `'event'` listener still fires (Events
materialized transiently — young-gen, GC'd; the compat/slow path).
`at(i)` lazy-materializes + caches. **This is the targeted OOM fix and
is self-contained.**

- **Bench gate:** gRPC re-bench, 92k/s × 1k hosts. Target: 1857 MB →
  ≤ 800 MB (and ideally ~500). Plus the pond-side `perf-live-columnar.mjs`
  (ingest/consume/heap, the spike promoted to a real bench).
- **Public API:** zero new surface. Retention semantics preserved
  (see Q1). The `'event'` / `at(i)` / `toTimeSeries` contracts hold.

### Phase 2 — columnar rolling reducer (= Step 3C, the GC-rate win)

The rolling reducer gains an `ingestBatch(columnarChunk)` path that
walks the value column instead of per-event `Event`s. Removes the
transient per-row `Event` allocation on fan-out → cuts the 22% GC
rate → throughput win. Builds on Phase 1's chunked buffer. Bigger,
per-reducer work (Welford / monotonic-deque expressed columnar).

- **Bench gate:** gRPC V6 ceiling re-bench. Target: the
  `LivePartitionedFusedRolling.ingest` self-time + GC % both drop.

## 5. Hard design questions (need decisions before Phase 1 code)

### Q1 — retention exactness (the one public-behavior risk)

Batch-granular retention would make `live.length ≥ maxEvents` (window
= "last K full chunks"), a behavior change. **Recommendation: preserve
exact `maxEvents`** by slicing the boundary chunk — drop whole chunks
that are fully out, then replace the boundary chunk with
`store.sliceByRange(...)` (zero-copy subarray on Float64 columns).
`live.length === maxEvents` exactly, as today. `maxAge` is naturally
chunk-friendly (drop chunks whose newest row < cutoff; exact at the
row level via the same boundary slice). **No behavior change.**

### Q2 — the `'event'` listener compat path

`'event'` must keep firing per row (LiveAggregation/View/Reduce
subscribe to it). Phase 1 materializes a transient `Event` per pushed
row for the fan-out. Recommendation: accept the transient
materialization in Phase 1 (it's young-gen, doesn't OOM); Phase 2's
columnar consume removes it for substrate-aware consumers. **Optional
optimization:** skip materialization when `#onEvent.size === 0` (the
gRPC partition-router case uses `_pushTrustedEvents`, not `'event'`).

### Q3 — lazy `at(i)` cache + eviction remap

Same shape as the Step 7 ring (materialize on demand, cache, remap on
evict). This is the part that bit Step 7 (the LiveReduce identity
issue). **Mitigation:** LiveReduce already moved to FIFO-position
eviction on the ring branch — that fix is identity-independent and
should be carried forward (it's strictly more robust). Re-audit every
`'evict'` consumer for identity assumptions (only LiveReduce had one).

### Q4 — `reorder` ordering mode

A chunked append-only buffer can't sorted-insert mid-stream (same
constraint as the ring). **Recommendation:** `reorder` keeps the
`EventArrayLiveStorage` backing (the strategy layer already routes by
mode); chunked columnar serves `strict`/`drop`. The gRPC aggregator
is `strict`/`reorder` — wait, M4 uses `reorder` for late data. **Open:
does the OOM happen on the `reorder` (late-data) path or the `strict`
firehose path?** If OOM is on `reorder`, the chunked buffer doesn't
help it directly and we need the indexed-columnar approach (deferred
in the Step 7 brief §11). **This needs confirming against the actual
OOM workload before committing.**

### Q5 — interval-keyed series

Same as Step 7: chunked columnar handles time/timeRange; interval
keeps the array backing initially. Low priority.

## 6. Public API consequences

- **Phase 1:** zero new surface, retention/`event`/`at`/`toTimeSeries`
  contracts preserved (Q1 exactness). Internal storage swap behind the
  PR-2a strategy layer.
- **Phase 2:** the rolling reducer's `ingestBatch` is internal; the
  per-reducer columnar contract (`rollingColumn`-style) is the design
  question — internal hook vs public extension surface. Defer to
  Phase 2.

## 7. Estimated shape

- **Phase 1:** ~600-900 LOC (`ChunkedColumnarLiveStorage` + intake
  wiring + lazy cache + boundary-slice retention + conformance
  additions to the shared suite + invariant pins). One PR.
- **Phase 2:** larger, per-reducer columnar state. Multiple PRs.

Each step: Layer 2 + Codex review, **human merge approval** per the
wave standing rule. Bench in the commit message; gRPC re-bench is the
gate.

## 8. Sequencing note — measure the OOM workload first

The Step 7 lesson: don't build before confirming the measurement
targets the real problem. **Before Phase 1 code, confirm the OOM is
(a) on the `strict`/`drop` path the chunked buffer serves, and (b)
driven by retained `Event[]`, not a leak elsewhere.** The gRPC
experiment's heap profile at the OOM cell is the prerequisite — if it
confirms retained-Event-dominated heap on a chunked-eligible ordering
mode, Phase 1 proceeds with a clear target.

## 9. Status

**Awaiting design review.** Open decisions: Q1 (recommend exact via
boundary slice), Q4 (confirm OOM ordering mode — may need the
indexed-columnar path instead). Once resolved, Phase 1 is the first
implementation PR, gated on the gRPC heap re-bench.

## Cross-references

- [`step-7-live-series-ring-buffer.md`](step-7-live-series-ring-buffer.md)
  — the walked-back ring approach; §11 (storage strategy), §13 (NO-GO).
- [`PLAN.md` Phase 4.7 "Next wave"](../../PLAN.md#next-wave-grpc-re-bench--substrate-adoption-queued-2026-05-28).
- Spike benches: `scripts/investigate-ring.mjs`,
  `scripts/investigate-batch.mjs` (on branch `feat/step-7-ring-storage`).
- `validateAndNormalizeColumnar` (Step 2c), `concatSorted` +
  `ChunkedColumn` (step 1g) — the substrate this assembles.
