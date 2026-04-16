# Implementation Plan

This document turns the broader roadmap in [FUTURE.md](FUTURE.md) into a phased
execution plan.

It is intentionally biased toward shipping useful increments while protecting
the core design of Pond:

- performance fixes land before major new surface area
- batch analytics stays the stable foundation
- live/streaming grows as a separate layer
- React support comes after the live composition model is proven

---

## Current baseline

What already exists today:

- typed `TimeSeries` construction and JSON ingest
- `Time`, `TimeRange`, and `Interval` temporal keys
- immutable `Event` values
- temporal selection and slicing
- alignment, aggregation, joins, rolling windows, and smoothing
- calendar-aware `Sequence` and `BoundedSequence`
- npm packaging and automated release flow
- a Docusaurus docs site plus generated API reference

What is not yet stable enough to build on aggressively:

- performance characteristics of the current core
- edge-case coverage in several analytical paths
- a settled plan for live/stateful composition

---

## Phase 0: Core performance

Goal: make the current library scale better before adding major new APIs.

Scope:

- fix O(N²) and O(N × B) hot paths in:
  - `aggregate()`
  - `rolling()`
  - `smooth('movingAverage')`
  - `smooth('loess')`
- add an internal pre-validated constructor path for derived series
- remove `events -> rows -> validate -> events` round-trips where avoidable
- replace linear scans with bisect where possible
- reduce avoidable allocation in temporal comparisons and event construction

Primary references:

- [AUDIT.md](AUDIT.md)
- section 1 of [FUTURE.md](FUTURE.md)

Definition of done:

- existing public behavior is preserved
- targeted benchmarks show clear wins on large series
- the main hot paths no longer have quadratic behavior on sorted data
- derived transforms avoid unnecessary re-validation

Suggested deliverables:

- benchmark fixtures for large batch workloads
- a dedicated test/benchmark note in the repo documenting the before/after

---

## Phase 1: Batch hardening

Goal: make the existing batch surface trustworthy enough to extend.

Scope:

- edge-case tests for:
  - empty series
  - single-event series
  - empty aggregation buckets
  - rolling alignment edge cases
  - half-open interval semantics
- add `toJSON()`
- add `toRows()` and `toObjects()` as explicit export helpers
- add custom reducers for `aggregate()` and `rolling()`

Why this phase matters:

- serialization completes the current core story
- custom reducers unlock many user workflows without adding a lot of top-level API
- stronger tests make later performance refactors safer

Definition of done:

- round-trip `toJSON()` / `fromJSON()` works in tests
- custom reducer typing and runtime behavior are documented and covered
- edge-case coverage exists for every current analytical primitive

---

## Phase 2: Batch expansion

Goal: fill the most obvious product gaps in the batch analytics story.

Scope:

- `groupBy`
- `resample`
- `diff` / `rate`
- `fill` / `fillNull`

Nice-to-have in the same wave if the model is still clean:

- per-column alignment policies

Hold for later unless a concrete user need appears:

- `pivot` / `unpivot`

Why this order:

- `groupBy` is the biggest missing capability for real multi-entity analytics
- `resample`, `diff`, and `fill` are high-discoverability conveniences
- these features all sit on the already-strong batch foundation

Definition of done:

- each method has both API docs and worked examples
- type flow is preserved through all new methods
- batch examples cover realistic host/service metrics workflows

---

## Phase 3: Live core

Goal: introduce a minimal but principled live layer without collapsing the
immutable `TimeSeries` model.

Scope:

- `LiveSeries<S>`
- push/append APIs
- retention policies
- immutable snapshot via `toTimeSeries()`
- ordering modes and late-arrival policy
- subscriptions

Non-goals for this phase:

- live aggregation
- live rolling
- React hooks

Why this phase is separate:

- the core design decision is architectural, not additive:
  stateless transforms should be views, stateful transforms should own buffers

Definition of done:

- `LiveSeries` can ingest ordered data reliably
- retention and snapshot semantics are clearly documented
- subscriptions are predictable and synchronous
- the API is small enough to change if the composition model reveals flaws

---

## Phase 4: Live composition

Goal: validate the live composition model before building UI integrations on top
of it.

Scope:

- stateless live views:
  - `filter`
  - `map`
  - `select`
  - `rename`
  - `collapse`
  - `window`
- stateful live transforms:
  - `LiveAggregation`
  - `LiveRolling`
  - `LiveSmooth`

Key rule to preserve:

- if a transform needs memory between events, it becomes its own live object
- if it does not, it remains a lazy view over the source

Definition of done:

- stateless and stateful transforms compose cleanly
- filtered/live aggregation pipelines are demonstrated in examples
- snapshot vs closed/finalized semantics are explicit where relevant

---

## Phase 5: React integration

Goal: make Pond useful in actual frontend apps without forcing a framework-y
runtime model into the core package.

Scope:

- `pond-ts/react` entry point
- `useLiveSeries`
- `useSnapshot`
- `useTimeSeries`
- `useWindow`
- `useDerived`

Requirements before starting:

- live composition semantics from phases 3 and 4 should already feel stable

Definition of done:

- live data can flow from WebSocket-like sources into throttled React renders
- hooks have examples that mirror likely product use
- the docs explain when to use lazy views vs memoized derived data

---

## Phase 6: Ecosystem and adapters

Goal: make Pond easier to adopt in real products before committing to a full
first-party charting system.

Scope:

- `pond-ts/node` for stream adapters
- `pond-ts/adapters` for bridge helpers such as `toRecharts`
- improved docs and examples for integrating with existing chart libraries

Later, only after the previous phases are stable:

- `@pond-ts/charts`

Why this order:

- adapters create value quickly
- a first-party chart package is a product line of its own and should come from
  proven usage, not speculation

Definition of done:

- Node-specific APIs stay out of the browser-safe default entry point
- adapters solve common “how do I graph this?” questions in the docs
- a chart package remains an intentional future decision, not implied scope creep

---

## Cross-cutting work

These should happen throughout the phases rather than being deferred:

- keep the docs site aligned with shipped behavior
- add end-to-end examples whenever a major capability lands
- keep API reference generation working in CI
- expand tests alongside every new public API
- prefer benchmark-backed changes for performance-sensitive core refactors

---

## Recommended release grouping

One practical way to turn the phases into releases:

| Release band | Focus |
|--------------|-------|
| `0.1.x` | Performance fixes, hardening, serialization, custom reducers |
| `0.2.x` | `groupBy`, `resample`, `diff`/`rate`, `fill` |
| `0.3.x` | `LiveSeries` core and subscriptions |
| `0.4.x` | Live views and live stateful transforms |
| `0.5.x` | React hooks |
| `0.6.x` | Node adapters and third-party chart adapters |

This is only a planning guide, not a commitment to version numbers.

---

## Decision gates

Before moving from one major phase to the next, answer the relevant question:

- After Phase 0: is the core fast enough that new API surface will inherit good behavior?
- After Phase 1: is the batch layer complete and trustworthy enough to be the foundation?
- After Phase 3: is the `LiveSeries` shape correct, or are we still learning?
- After Phase 4: do live/stateful composition rules feel simple enough for users?
- After Phase 5: do common frontend use cases work without ad hoc glue?

If the answer is no, stay in the phase and tighten the model before expanding.
