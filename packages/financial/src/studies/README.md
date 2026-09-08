# Adding a study

A study is a pure function that **appends one or more columns** to a bar
`TimeSeries` and returns the widened series. Studies are thin assemblies over
the shared kernel — they add vocabulary, not new math. Follow this checklist so
every study lands consistent, composable, and **verified**.

The worked references are `moving-average.ts` (`sma`/`ema`), `bollinger.ts`
(multi-column family), `rolling-stat.ts` (shared-body single reducers),
`z-score.ts`, `envelope.ts`, and `percent-change.ts`.

## The checklist

### 0. Check it isn't already shipped under another name

The corpus names the same formula more than once. **ROC is `percentChange`**
— TA-Lib's `ROC` is `(price / prevPrice − 1) × 100`, and the oracle
cross-checks `percentChange` against it (exact agreement, identical warm-up).
Before writing a study, compute the reference against the existing surface
over the fixture's closes; if it agrees, the deliverable is the cross-check
and a doc note, not a second study that differs invisibly.

### 1. Write the study — `studies/<name>.ts`

- A pure `(series, options) => series` function, generic over the schema `S`
  and (for the type-precise appended column) the output name:
  `export function foo<S extends SeriesSchema, const Output extends string = 'foo'>(...)`.
- **Every study takes `column`** (the source field, **default `'close'`** via
  `DEFAULT_SOURCE`) **and `output`** (the appended column name; a multi-column
  study takes a `prefix` and appends `${prefix}Middle` / `Upper` / `Lower`).
  Never hard-code `'close'` — a study must run over any numeric column,
  including another study's output.
- **A study that compares two instruments takes a `benchmark` COLUMN**, not a
  second `TimeSeries`. The consumer aligns and joins first (`align` +
  `TimeSeries.joinMany`) and the study reads two columns of one row, as `atr`
  reads `high`/`low`/`close`. Guard the required `benchmark` name with
  `assertColumn` — it throws (missing or non-numeric), where a defaulted
  `column` reads all-missing exactly as it does in every other study.
- **Periods are bar counts**, not durations. Validate with `assertPeriod`.
- **Warm-up is length-preserving**: emit `undefined` for the first `period − 1`
  rows, keep the row count (so the study lines up on the source's time axis).
  You get this for free from the kernel (`minSamples: period`).
- **Compose on the kernel** (`kernels/rolling.ts`), don't hand-roll loops:
  - `rollingValues(series, column, reducer, period)` — one count-window reducer.
  - `rollingColumns(series, specs, period)` — several reducers in **one** pass
    (e.g. Bollinger's avg + stdev, z-score's mean + stdev).
  - `columnValues(series, column)` — a raw column as an array (for arithmetic
    like percent-change / the z-score numerator).
  - `emaValues(series, column, period)` — a span-EMA as an array.
  - `wilderValues(values, period, start)` (`kernels/wilder.ts`) — Wilder's
    SMA-seeded recursion over a raw array (RSI, ATR, the DMS' three smooths).
  - `trueRangeValues(high, low, close)` (`kernels/true-range.ts`) — the
    true-range array (ATR; Keltner, SuperTrend next).
  - `highestLowestValues(series, high, low, period)` and
    `percentOfRangeValues(close, hh, ll)` (`kernels/highest-lowest.ts`) —
    HH/LL from **one** scan, and `100·(c−LL)/(HH−LL)` (stochastics, %R,
    Donchian; Keltner-style channels next).
  - `barsSinceExtremeValues(values, period, 'max' | 'min')` (same file) — the
    **age** of the window's extreme rather than its value, via a monotonic
    deque (O(N), flat in `period`), ties to the newest bar, over a window of
    `period + 1` bars (Aroon).
  - `directionalMovementValues(high, low)`
    (`kernels/directional-movement.ts`) — Wilder's `+DM`/`−DM` split, with the
    vortex's crossing legs beside it (the DMS, Vortex).
  - `movingAverageValues(values, period, type)` (`kernels/moving-average.ts`)
    — the **K2 MA-type engine** over a raw array (`sma`/`ema`/`wma`/`smma`/
    `dema`/`tema`/`trima`/`hull`/`kama`/`zlema`), and `movingAverageColumn`
    for the series-column case. Any study exposing a "MA type" option takes
    `MaType` and calls this — do not add an eleventh private smoother.
  - `rollingMeanValues(values, period)` (`kernels/rolling-mean.ts`) — SMA of
    a **derived** array that waits for `period` finite _values_ (not rows —
    the scratch-column route warms up one bar early over a NaN head).
  - `rollingBivariateValues(x, y, period)` (`kernels/bivariate.ts`) — the
    rolling population **covariance** of two row-aligned arrays with each
    one's variance beside it, over a **strict** pair window (all `period`
    rows finite in BOTH), from which correlation and beta are closed forms
    (`correlation`, `beta`). Shifted-frame Welford with an aligned rebuild —
    never `Σxy − ΣxΣy/n`, which returns a negative variance at 1e12-scale
    prices.
  - `foldRows(inputs, outputCount, state, step)` (`kernels/fold.ts`) —
    kernel **K6**: a per-bar fold with **carried state** over several
    row-aligned columns, for the path-dependent studies no window can
    express (`parabolicSar`, `superTrend`, `atrTrailingStop`,
    `negativeVolumeIndex`/`positiveVolumeIndex`, `klinger`). The step sees
    `(state, i, run, inputs, outputs)` where `run` counts the consecutive
    complete rows ending at `i`, so `run === 1` is the seed bar; **a missing
    cell resets the machine**, and the kernel NaN-fills the outputs, so a
    step that writes nothing warms up.
  - `sessionIdValues(keys, sessions, stamped)` and
    `previousSessionHlcValues(ids, high, low, close)` (`kernels/session.ts`) —
    kernel **K11**: the merge walk that places each bar in its session
    (`O(N + sessions)`, and the same walk `TradingCalendar.tagSessions`
    appends as a column), and each session's aggregate high/low/close held
    across the **next** session. A study that resets or anchors on the
    trading day takes its sessions through `SessionAnchorOptions`
    (`contract/session-anchor.ts`) — `sessions` (a calendar or session list)
    or `session` (a session-id column) — and never derives session
    boundaries itself.
  - `anchoredVwapValues(typical, volume, anchors)`
    (`kernels/anchored-vwap.ts`) — `Σ tp·vol / Σ vol` accumulated per **anchor
    group**, where a change of id restarts both sums and a `NaN` id
    contributes nothing. One group is `anchoredVwap`; one id per session is
    `sessionVwap`.
  - `clvValues(high, low, close)` and
    `accumulationDistributionValues(high, low, close, volume)`
    (`kernels/close-location.ts`) — where the close sits in the bar's own
    range (`[−1, +1]`, missing on a flat bar) and the running sum of it
    weighted by volume (A/D, Chaikin oscillator, CMF).
- Guard the output name(s) with `assertNoColumn` before doing work.
- Append with `series.withColumn(output, values)` (it appends an **optional**
  number column — required for the `undefined` warm-up to survive a later
  strict-intake rebuild). Let the return type **infer** from `withColumn`
  (avoids TS2742); don't annotate it by hand.

### 2. Export it — `index.ts`

Export the function and its options type from the package barrel.

### 3. Add the fluent method — `fluent.ts`

Add the method to the `declare module 'pond-ts'` interface **and** mount it on
`TimeSeries.prototype` (delegating to the standalone function bound to `this`).
The declared return type must match the standalone function's (`AppendOpt<S,
Output>` for a single column, the triple-nested form for a `prefix` family).

### 3b. Describe it in the catalog — `catalog/<family>.ts`

Add a `defineStudy<XOptions<SeriesSchema, string>>({ … })` entry to the
family file under `src/catalog/` (`STUDY_FAMILIES` in `catalog/types.ts` is
the list; the file's array is the menu order). The compiler checks the
description covers every options key; `test/catalog.test.ts` runs it against
the study and **fails if any fluent method has no descriptor** — so this step
is not optional either. Field rules are on the types in `catalog/types.ts`;
the one that needs judgement is each output's `unit` (may it share the
source's axis?), which the `StudyUnit` doc comment settles.

### 4. Add a pandas oracle case — **required, not optional**

**A study does not merge without an oracle case.** This is the gate that lets us
trust the numbers (see `../../scripts/oracle/README.md`).

1. In `scripts/oracle/generate.py`, add a function computing the reference
   values with **pandas** (later: TA-Lib for named indicators), and a case in
   the `cases` list. **Match our conventions exactly** — the oracle only helps
   if it does: population stdev (`ddof=0`), `ewm(adjust=False)` for EMA, linear
   `quantile` for percentiles, `× 100` for percent — see the oracle README's
   table. The expected column **names must equal the study's default output**
   (a name mismatch reads all-null and the test passes _vacuously_).
2. Regenerate the fixture (a throwaway venv — see the oracle README).
3. Add a `case` for the study to the dispatch in `test/study-oracle.test.ts`
   (the `default` throws, so a missing dispatch fails loudly, never silently).

### 5. Add unit tests — `test/studies.test.ts`

The oracle pins the **values**; unit-test what it doesn't: validation throws,
default output / `prefix` naming, and edge handling (e.g. σ = 0 → `undefined`,
warm-up shape, `period > length`). Aim for a real assertion, not just
"doesn't throw."

### 5b. Cover the properties, not just the values —

`test/talib-properties.test.ts`

The oracle checks **values** on a clean, never-flat, gap-free input. The
property tests cover what that input is designed to avoid, and a named
indicator should join the ones that apply to it:

- **scale behaviour** — invariant (RSI) or linear (MACD)? Both are real
  assertions; pick the one your study should satisfy.
- **composition** — running over another study's output must preserve length
  and _compose_ the warm-up, not empty the column. `rsi(sma(...))` shipped in
  review returning nothing at all; TA-Lib has had a standing test for that
  shape for years and we did not.
- **all-missing input** → all-missing output: the study must neither throw
  nor invent a value. (Not "no NaN leak" — `withColumn` maps `NaN` to missing
  on its typed door, so a leaked `NaN` reaches any reader as `undefined`.
  That guarantee is core's and is tested there; a study test asserting it
  would pass vacuously.)

These are converted from TA-Lib's own suite (BSD 2-Clause, attributed in the
file header) precisely because they are _property_ tests — copying more of
its value tables would only duplicate the oracle.

### 6. CHANGELOG

Add the study under `## [Unreleased] → Added` in the root `CHANGELOG.md`.

## Performance: the kernel touches data, studies never do

Market-scale series are the normal case for this package — assume **1M bars**,
not 10k. The studies stay fast by construction, not by per-study tuning, and
the rules below are what keep that true. They exist because we shipped the
slow versions first: the original kernel read rolled results via
`rolled.events` and `event.data()`, which cost ~400 ms of pure Event/object
allocation per 1M rows — ~50× the entire rolling scan — and core's
`smooth('ema')` rebuilt every row through strict intake (PR #536 removed
both; `sma()` went 569 → 56 ms, `ema()` 603 → 2.5 ms).

- **Studies contain no data loop.** A study is options-validation + kernel
  calls + `withColumn`. If you are writing `for`, `.map`, or `.events` in a
  `studies/*.ts` file, stop — the loop belongs in `kernels/`, where it is
  shared, benchmarked, and written against the column API.
- **Kernels read columns, never events.** `series.events` materializes an
  `Event` plus a data object per row; at 1M rows that costs more than most
  whole-series scans. Bulk reads go through `series.column(name)` (see
  `readNumericColumn` in `kernels/rolling.ts`) — and per-cell arithmetic over
  those arrays is fine; it's the Event materialization that kills, not the
  loop.
- **Stay on core's columnar fast paths.** The kernel earns its speed by
  hitting them: count-window `rolling` runs its typed fast path when **every
  mapped reducer is a built-in name with a numeric output** (a single
  custom-function reducer sends the whole call to the generic sweep), and
  `smooth('ema')` runs its typed fast path on packed numeric sources. If a
  new study needs a shape those don't cover, the answer is a **new kernel
  helper**, never a bespoke event loop in the study — the per-bar stateful
  fold PSAR and SuperTrend need is `foldRows` ([PND-SFOLD]), which walks the
  columns once and costs 11.3 ms at 1M rows over a two-column no-op step
  against `ema()`'s 6.3 ms **on the same run** — 1.8× (the quieter
  reference run below reads `ema()` at 2.5 ms; compare ratios, not runs).
- **Touching a kernel means running the bench.** Compose-only studies inherit
  kernel performance and need no benchmark of their own. Any change to
  `kernels/*.ts` — or a new kernel helper — runs
  `scripts/perf-studies.mjs` before/after at the 1M scale and puts the table
  in the commit message (the root CLAUDE.md perf-check procedure applies to
  kernels even though studies themselves are exempt as pure delegators).
  Current reference points at 1M bars: `ema()` ≈ 2.5 ms, `sma()` ≈ 56 ms,
  `bollinger()` ≈ 162 ms, hand-rolled `Float64Array` floor ≈ 1.6–1.9 ms.
  A new kernel helper landing an order of magnitude off these numbers is a
  design smell, not a tuning task.

## Why the discipline

Studies are a **vocabulary package**: named, parameterized wrappers over a small
kernel. Keeping them thin (compose, don't re-implement), uniformly shaped
(`column`/`output`, bar-count periods, length-preserving warm-up), and
**oracle-verified** is what makes the corpus trustworthy and cheap to extend —
each new study is a short, testable assembly, and its numbers are checked
against an independent reference before anyone relies on them. The same
thinness is the performance model: because no study owns a data loop, a
kernel or core fast-path improvement speeds up every study at once (PR #536
needed zero study changes), and a slow study is by definition a kernel or
core bug — file it there, don't fork the study.
