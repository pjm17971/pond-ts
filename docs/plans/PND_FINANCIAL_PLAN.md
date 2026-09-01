# PND_FINANCIAL_PLAN — `@pond-ts/financial` studies + trading time

> Breakout plan for the **Financial** roadmap section in
> [PLAN.md](../../PLAN.md). Corpus analysis:
> [docs/notes/financial-indicators-assessment-2026-07.md](../notes/financial-indicators-assessment-2026-07.md)
> (124 ChartIQ studies → ~11 kernels, ~80% expressible on core primitives).
> RFCs: [trading-calendar.md](../rfcs/trading-calendar.md),
> [financial-charts.md](../rfcs/financial-charts.md). Study-authoring
> checklist: `packages/financial/src/studies/README.md`; oracle conventions:
> `packages/financial/scripts/oracle/README.md`. Shipped history (calendar
> engine, `scaleTradingTime`, tick ladder, first studies batch): the Tidal
> section of
> [docs/archive/experiments-2026.md](../archive/experiments-2026.md).

Shipped substrate: the trading calendar (Phases 1+2, released v0.42.0), core
G1 count-based `rolling({ count })`, `smooth('ema', { span, minSamples })`,
and the #449 first studies batch — sma, ema, bollinger, envelope,
rollingStdev/Min/Max/Percentile, zScore, percentChange — all fluent and
pandas-oracle-verified.

**Shipped 2026-07-23 — market-scale studies perf** (report: hand-rolled
Float64Array SMA/EMA at 1M bars was single-digit ms; the studies were
hundreds). Three behaviour-preserving cuts along one path: a
`smooth('ema')` columnar fast path (typed-buffer recurrence + trusted
construction, 530 → 4.4 ms at 1M), a `rolling({ count })` numeric fast
path (shared incremental reducer states fed straight off packed buffers
into typed result columns, 135 → 32 ms), and the financial kernel reading
study columns off the column API instead of materializing `series.events`
(~400 ms/1M of pure Event-allocation overhead). End-to-end at 1M bars:
`ema()` 603 → 2.5 ms, `sma()` 569 → 56 ms, `bollinger()` 748 → 162 ms.
Durable benches: `packages/core/scripts/perf-smooth-ema.mjs`,
`packages/financial/scripts/perf-studies.mjs`. **Considered and deferred:**
per-reducer fused kernels (running-sum SMA etc.) would close the remaining
~20× gap to the bespoke floor but duplicate reducer arithmetic outside the
shared states — take it up only if a consumer needs single-digit ms at 1M;
the boxed `(number | undefined)[]` hop between kernel and `withColumn`
(~12 ms/1M) is [PND-WCNAN]'s NaN-canonical typed intake, tracked in the
columnar plan.

## Tasks

### [PND-STUDY] — Studies Phase-1 breadth

Assessment §7.4: RSI, MACD, ATR (+bands), stochastics, %R, Donchian, OBV,
VWAP, Historical Volatility, momentum/ROC. These add **TA-Lib** alongside
pandas in the oracle harness (named-indicator convention deltas documented;
bar-for-bar vendor parity is a non-goal). The core substrate is complete —
each study is a vocabulary wrapper following the studies README checklist
(uniform `column`/`output` shape, bar-count periods, length-preserving
warm-up, fluent method, oracle case).

**Landed — stochastics / Williams %R / Donchian (one kernel).** All three
read the trailing highest-high / lowest-low, so they share
`kernels/highest-lowest.ts` (`highestLowestValues`: both extremes in ONE
`rolling` scan; `percentOfRangeValues`: `100·(c−LL)/(HH−LL)`, which _is_ fast
`%K`, with `%R = %K − 100`). Decisions, each measured against TA-Lib in the
oracle: (1) **one `stochastic` with `slowing`**, not `stochastic` +
`stochasticFast` — `slowing: 1` is `STOCHF` and is asserted as such. (2) **`%K`
emits from its own first valid bar** (15 at 14/3/3) where TA-Lib masks it to
`%D`'s (17) — the `macd` per-column precedent; values agree to `5.7e-14` on
every bar TA-Lib emits. (3) **Flat window → `undefined`** where TA-Lib
gives `0` — `0` is also "close at the bottom of a real range" for `%K` and
"at the top" for `%R`, so TA-Lib's convention contradicts itself across the
two studies on the same bar; the `rsi` precedent. (4) **The slow-K smoothing
is a raw-array kernel (`rollingMeanValues`) that waits for `slowing`
_values_**, not a scratch-column `sma`: core's count-window `avg` counts rows
and would have put "3-bar" `%K` on bar 13 with one value in it (TA-Lib and
pandas: bar 15). It reuses `rollingMeanSdInto` plus a NaN mask, so it is the
same SMA `sma()` gives (pinned bit-for-bit). (5) A **misnamed `high`/`low`
reads all-missing** in the kernel rather than throwing from core's `rolling`,
so a multi-input study has one rule for all its inputs (the `atr` precedent;
single-column `rollingMax` still throws — left alone). Considered and
rejected: clamping `%K`/`%R` to their bounds (a redirected `close` outside
its range should read outside, honestly); a Donchian `close`-based breakout
column (a rule, not a study).

### [PND-SFOLD] — K6 stateful-fold kernel (studies Phase 3)

A few Phase-3 studies (PSAR, SuperTrend, etc.) need the K6 stateful-fold
shim — a per-bar fold with carried state that doesn't fit the rolling
kernels. Design the kernel when Phase-1 breadth is done and a consumer pulls
on a Phase-3 study.

### [PND-TCAL] — Trading-time deferred items

Documented, none blocking:

- **`neighbourSpans` point-key slot widths on the discontinuous axis**
  (interval-keyed bars from `aggregate(barSequence)` — the primary path —
  are immune).
- **Exact exchange-tz tick grain** — the current grain buckets by
  runtime-local calendar.
- **Timezone control for the cursor readout** — the grain-aware default
  (#484 follow-up) sidesteps the daily-bar case, but true exchange-/display-tz
  handling is its own design conversation.
- Overnight sessions in `TradingCalendar.fromRules` (explicit-list only for
  now).
