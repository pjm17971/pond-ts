# RFC: Trigger as a first-class concept in the live layer

**Status:** Approved 2026-05-01, shipped as `0.12.0` on branch `feat/triggers`.

**Drafted:** 2026-05-01.
**Authors:** the pond-ts library agent (Claude), in conversation with the user.

## Sign-off decisions

1. Option name: `trigger` ✓
2. Partitioned-with-clock output shape: per-partition rows of typed events ✓
3. `Trigger.event()` publicly callable (default behavior, useful for explicit-default documentation) ✓
4. No public class for the partitioned-sync emission path; just a typed `LiveSource<RowSchema>` ✓

## Motivating use cases

1. **Synchronised partitioned tick aggregation** — `pond-grpc-experiment`'s M3.5 dashboard wire (per-host 200ms ticks where every host's frame for tick T carries the same `ts`). Originally expressed via a hand-rolled `HostAggregator`; the experiment was shipping its own aggregator while the library had nothing to offer for synchronised partition emission.
2. **Sequence-sampled rolling** — frontend telemetry stats (collect latency events, report p95 to a backend every 30 s while the rolling state drives an in-app live display). v0.11.8 shipped `.sample(seq)` for this; the API was correct for the use case in isolation but specialised a general concept (trigger) into a single verb. Subsumed by the trigger primitive without loss.

## Why this RFC, why now

PR #11 (M3.5 step 1) and PR #13 (`bench:agg`) on the gRPC experiment showed that pond's lack of a synchronised-tick primitive is forcing the experiment to roll its own aggregation outside the library — turning the bench into a measurement of the experiment's `HostAggregator`, not of pond. As long as the gap is unfilled, every additional milestone is testing pond less and less.

The fix is small enough to scope as one PR and pre-1.0 enough to ship as `0.12.0` without lengthy stabilisation. Two real users (Codex on webapp telemetry + the Claude gRPC agent) will migrate as soon as it's available; the friction-from-real-use shapes any follow-up.

## The factoring

Pond's live layer today carries trigger semantics implicitly inside each accumulator type:

| Type                                       | Implicit trigger                                            |
| ------------------------------------------ | ----------------------------------------------------------- |
| `LiveRollingAggregation`                   | event-driven (emits per push)                               |
| `LiveAggregation`                          | bucket-close-driven                                         |
| `LiveSequenceRollingAggregation` (v0.11.8) | sequence-crossing-driven (this is what `.sample()` returns) |

Three accumulators, three implicit triggers, no recombination. Every new emission cadence we want forces a new accumulator type.

The sharper factoring is **Source × Trigger × Aggregation** — trigger as a first-class composable concept, orthogonal to the aggregation choice. After this RFC:

- An accumulator's emission cadence is a parameter, not a type identity.
- `LiveRollingAggregation` defaults to event-trigger (current behavior).
- Passing `Trigger.clock(seq)` switches it to sequence-triggered emission.
- A partitioned rolling with a clock trigger emits **synchronised across partitions** — one tick fires for everyone.

## API shape

### Trigger taxonomy

```ts
// src/triggers.ts (new file)

import { Sequence } from './Sequence.js';

export type Trigger = ClockTrigger | EventTrigger;
// CountTrigger and CustomTrigger reserved for future expansion;
// shipping only Clock + Event in 0.12.0.

export type ClockTrigger = Readonly<{ kind: 'clock'; sequence: Sequence }>;
export type EventTrigger = Readonly<{ kind: 'event' }>;

export const Trigger = Object.freeze({
  /**
   * Sequence-triggered emission. The accumulator emits one snapshot
   * each time a source event crosses an epoch-aligned boundary of
   * `sequence`. Emission timestamps are the boundary instants
   * (e.g. `Sequence.every('30s')` → 0, 30 000, 60 000 … ms).
   *
   * For partitioned accumulators, all partitions share this trigger
   * — when any partition's event crosses the boundary, every
   * partition's rolling-window snapshot fires together at the same
   * boundary timestamp. Synchronised by construction.
   *
   * Sequence must be fixed-step. Calendar sequences are rejected.
   */
  clock(sequence: Sequence): ClockTrigger {
    if (sequence.kind() !== 'fixed') {
      throw new TypeError(
        'Trigger.clock(sequence) requires a fixed-step Sequence',
      );
    }
    return Object.freeze({ kind: 'clock', sequence });
  },

  /**
   * Per-event emission. The accumulator emits one snapshot per
   * source event push. This is the default for accumulators that
   * don't specify a trigger; calling Trigger.event() explicitly is
   * useful for documentation but produces the same behavior as
   * omitting the option.
   */
  event(): EventTrigger {
    return Object.freeze({ kind: 'event' });
  },
});
```

### Accumulator integration: option, not chain step

After considering several shapes (chain step `.triggerOn()`, source-property, accumulator option), this RFC proposes the **option form**:

```ts
// On LiveRollingAggregation:
type LiveRollingOptions = {
  minSamples?: number;
  trigger?: Trigger; // NEW — defaults to Trigger.event()
};

// On the partitioned chain (LivePartitionedView.rolling sugar):
partitioned.rolling(window, mapping, { trigger?: Trigger; minSamples?: number });
```

**Rationale for option-not-chain:**

- Each accumulator declares its own emission cadence at the call site that introduces the accumulator. Locality.
- Different accumulators in the same chain can have different triggers. (Two rollings, two cadences. Two ways to handle it: two calls with different options.)
- No new wrapper types (`TriggeredLiveSource`) — keeps the type system simpler.
- A future `live.triggerOn(t)` chain step could still ship later as ergonomic sugar for "set the same trigger on every downstream accumulator," but starting with the option form is additive and minimal.

**Trade-offs accepted:**

- Slightly more verbose for the dashboard's case (`.partitionBy('host').rolling('1m', m, { trigger: Trigger.clock(seq) })` instead of `.triggerOn(t).partitionBy(...).rolling(...)`).
- The trigger appears as one option among many on the rolling call.

I'm willing to revise this in the implementation phase if the call sites end up reading worse than the chain form. The chain form is also future-extensible from this starting point.

### Output shape — non-partitioned

For non-partitioned `live.rolling(...)` with a clock trigger, the output is the same `LiveRollingAggregation<S, Out>` we have today, but emitting on boundary crossings instead of per source event. Output events are time-keyed at boundary instants. **This subsumes the v0.11.8 `LiveSequenceRollingAggregation`** — that class can be deleted (pre-1.0, no compat layer).

```ts
// v0.11.8:
const rolling = live.rolling('1m', { latency: 'p95' });
const sampled = rolling.sample(Sequence.every('30s'));
sampled.on('event', e => fetch('/api/telemetry', ...));

// v0.12.0:
const rolling = live.rolling('1m', { latency: 'p95' }, {
  trigger: Trigger.clock(Sequence.every('30s')),
});
rolling.on('event', e => fetch('/api/telemetry', ...));
```

The webapp telemetry agent migrates by adding the `trigger` option and removing the `.sample()` call. Single rolling object now serves both backend reporting (via `'event'`) and in-app display (via `.value()`).

### Output shape — partitioned with clock trigger

For `live.partitionBy(col).rolling(window, mapping, { trigger: Trigger.clock(seq) })`, emission is **synchronised across partitions**: when any partition's event crosses the boundary, every partition's rolling-window snapshot fires at the same instant.

The output is a `LiveSource<S'>` whose schema includes the partition column:

```ts
// Source schema: [time, cpu, host]
// Output schema: [time, host, cpu_avg, cpu_sd, cpu_n]
//   (one row per partition, all sharing the same boundary timestamp)

const ticks = live.partitionBy('host').rolling(
  '1m',
  {
    cpu: 'avg',
    cpu: 'stdev' /* shorthand collisions resolved via AggregateOutputMap, see deferred work */,
  },
  { trigger: Trigger.clock(Sequence.every('200ms')) },
);

ticks.on('event', (event) => {
  // event.begin() === <boundary timestamp>
  // event.get('host') === 'api-1' | 'api-2' | …
  // event.get('cpu_avg') === <rolling avg for that host>
});

// Consumer rebuckets by listening:
const latestByHost = new Map();
ticks.on('event', (e) => {
  if (e.begin() > lastTickTs) {
    // new tick boundary — flush
    flush(latestByHost);
    latestByHost.clear();
    lastTickTs = e.begin();
  }
  latestByHost.set(e.get('host'), e.data());
});
```

Emitting per-partition rows of typed events (rather than a Map-valued frame) keeps the output composable with downstream pond operators (filter, select, etc.). The dashboard consumer reconstructs per-tick frames by grouping on the boundary timestamp.

### Behaviour: synchronisation via shared trigger state

The synchronised-partition guarantee comes from the trigger maintaining a single `lastBucketIdx` shared across all partitions. When ANY partition's event advances the bucket index, the trigger fires for every partition, snapshotting their respective rolling windows at the boundary timestamp. Implementation detail; user observes only "all partitions get the same `ts`."

### What about `n_in_tick`?

The dashboard's `WIRE.md` calls for `cpu_n` as "samples that arrived since the last tick." This is computable app-side from consecutive snapshots:

```ts
const lastCount = new Map<string, number>();
ticks.on('event', (e) => {
  const host = e.get('host');
  const total = e.get('cpu_n_total'); // rolling-window count
  const delta = total - (lastCount.get(host) ?? 0);
  lastCount.set(host, total);
  // delta is n_in_tick
});
```

A library-shipped delta-reducer family (`countSince`, `sumSince`, etc.) is a separate sibling RFC, deferred. The MVP ships without it.

## Migration plan

| v0.11.8 form                                                      | v0.12.0 form                                                                                              |
| ----------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `live.rolling('1m', m).sample(Sequence.every('30s'))`             | `live.rolling('1m', m, { trigger: Trigger.clock(Sequence.every('30s')) })`                                |
| `LiveSequenceRollingAggregation` (class)                          | **Deleted.** All references go away; the same shape is achievable via `LiveRollingAggregation` + trigger. |
| `live.partitionBy(c).rolling(...).toMap()` per-partition rollings | Unchanged (no trigger → per-partition per-event emission, current behavior)                               |
| (no equivalent — needed `HostAggregator`)                         | `live.partitionBy(c).rolling(window, m, { trigger: Trigger.clock(seq) })` synchronised across partitions  |

**Breakage scope:** v0.11.8 was published 2026-04-30. The webapp telemetry agent (Codex) is the only known caller of `.sample()` and `LiveSequenceRollingAggregation`. Migration: ~5-line change in their code. Pre-1.0; no semver concerns.

Published as a minor bump to `0.12.0`. Pre-1.0 caret rules mean caret installs against `^0.11.0` keep getting `0.11.x`; consumers wanting the new shape upgrade their pin to `^0.12.0`.

## Implementation scope estimate

- **`src/triggers.ts`** — new file, ~40 lines (Trigger union, factory functions, validation).
- **`src/LiveRollingAggregation.ts`** — accept `trigger` in options; switch internal emission logic on trigger kind. ~80 lines added.
- **`src/LivePartitionedSeries.ts`** + **`src/LivePartitionedView.ts`** — when partitioned `rolling` is called with a clock trigger, lift to synchronised-emission mode. New emission path: ~120 lines.
- **`src/LiveSequenceRollingAggregation.ts`** — DELETED. ~262 lines removed.
- **`src/LiveSeries.ts`**, **`src/LiveView.ts`**, **`src/LiveAggregation.ts`** — remove `.sample()` references; thread `trigger` option through. ~60 lines net change.
- **`src/index.ts`** — export `Trigger` type and factory; remove `LiveSequenceRollingAggregation` export.
- **Tests** — new file `test/Triggers.test.ts` covering: clock trigger non-partitioned (replaces `LiveRollingAggregation.sample.test.ts`), clock trigger partitioned synchronisation, default event trigger preserves existing behavior, anchor support, calendar-sequence rejection, dispose semantics, multiple-listener correctness. ~300 lines.
- **Perf benchmark** — `scripts/perf-triggers.mjs` covering synchronised partitioned at the dashboard's 200ms cadence under realistic load. ~80 lines.
- **Docs** — `live-transforms.mdx` rewritten section on trigger-aware accumulators; removal of the `.sample()` page; telemetry recipe updated; new how-to section in PLAN's docs backlog list.

**Total:** roughly 700-900 lines net change across ~10 files. ~1 day of focused implementation + ~1 day of testing/docs/perf if no surprises.

## Open questions for sign-off

Three things to settle before I write code:

1. **Option name.** I've used `trigger` in this RFC. Alternatives: `emit`, `cadence`, `on`. None obviously better — `trigger` matches the conceptual vocabulary. **Default: `trigger`.**

2. **Partitioned-with-clock output shape.** I've proposed "one row per partition per tick, schema includes partition column." Alternative: a special `'tick'` event with a Map-valued frame. **Default: per-partition rows** — composable with downstream operators, consistent with how partitioned outputs work elsewhere. Confirm?

3. **Should `Trigger.event()` be explicitly callable, or only the implicit default?** If callable, users can pass `Trigger.event()` to make the default explicit (documentation value). If not, the only public Trigger factory is `Trigger.clock(seq)` and the default is "no `trigger` option specified." **Default: ship `Trigger.event()` as well** for symmetry and forward-compat.

4. **Naming the internal sync-partition emission path.** Does this need a public class name (`LiveTriggeredPartitionedRolling`) or is it just a typed return shape from the chain (`LiveSource<RowSchema>`)? **Default: just a `LiveSource<RowSchema>`** — no new public class.

## What this RFC does NOT cover

- **`Trigger.count(n)`** — count-triggered emission. Plausible but speculative; not in MVP.
- **Compound triggers** (`Trigger.any(t1, t2)`, `Trigger.all(t1, t2)`) — speculative.
- **Custom predicate triggers** (`Trigger.custom((events) => boolean)`) — speculative.
- **Delta-reducer family** (`countSince`, `sumSince`, etc.) — sibling RFC, separate.
- **A future `.triggerOn(t)` chain step** — additive; can ship later if call sites read awkwardly.
- **`LiveAggregation` integration** — the bucket-close-driven aggregation has its own trigger semantics; not folding it into `Trigger` taxonomy in MVP.

## Status & next steps

1. **Sign-off requested on the four open questions above.**
2. After sign-off, implementation in order: triggers.ts → option threading on `LiveRollingAggregation` → partitioned synchronisation path → tests → perf → docs.
3. Publish as `0.12.0` (minor bump from `0.11.8`). Both Codex (webapp) and the gRPC agent migrate to the trigger primitive; any further friction informs follow-up patch releases.

## Post-implementation (2026-05-01) — Layer 2 review findings

Adversarial review on PR #94 surfaced four real items that landed as fixes before publish, plus two deferred for stable `0.12.0`:

**Fixed pre-publish:**

- **Column-name collision now rejected at construction.** `partitionBy('cpu').rolling('1m', { cpu: 'avg' }, { trigger })` would have silently overwritten the partition tag with the reducer output. Constructor now throws with a clear error.
- **`dispose()` exposed on the synchronised partitioned source.** Disposers are registered both with the sync source (so `sync.dispose()` detaches per-partition listeners directly) and with the parent `LivePartitionedSeries` (so the parent dispose path also cleans up). Idempotent in either order.
- **Tests added** for column-collision rejection, dispose semantics, multi-partition multi-boundary jump, and late-spawn partition behavior. Test count: 25 (was 20).
- **Documentation** updated to flag late-spawn partition semantics, the column-collision rejection rule, and the npm peer-dep mixing warning when consumers cross dist-tags.

**Deferred for stable `0.12.0`:**

- **Typed return shape** for the synchronised-partitioned path. Currently `LiveSource<SeriesSchema>` — runtime schema is correct, but static types widen. Needs a `RowSchemaForSyncRolling<S, M, ByCol>` helper that combines the partition column type from the source schema with the rolling output columns. Real type-plumbing work; better done once the surface is settled.
- **Late-spawn semantics revisit.** Currently late partitions get no retroactive row in the current tick. If a real use case wants "every declared partition in every tick" or "partitions emit their first row for the tick they spawn in," the semantics can shift. Documented as an explicit limit for now.
