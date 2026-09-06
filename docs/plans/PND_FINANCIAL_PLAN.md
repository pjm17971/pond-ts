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

**Status (2026-09-06): Phase-1 breadth landed** — every named study above
except **ATR bands** (open: Keltner-style `close ± k·ATR` on
`trueRangeValues`) and the **anchored / session VWAP** (deferred to the
session-anchored phase; needs a reset). The per-batch write-ups below are
the decision record. Package-wide questions the wave surfaced, none
blocking: `ema()`'s first-sample seed vs TA-Lib's SMA seed (MACD chose
internal consistency); the Wilder-vs-`ema` interior-gap asymmetry (decided
with the directional group below — status quo kept, reasoning recorded); `percentChange.periods` vs `period` naming; the website study
table has no rows for any of the new studies; a monotonic-deque fast path
for core's rolling min/max would lift stochastics/%R/Donchian ~2×.

#### Landed so far: RSI, MACD, ATR (2026-09-01)

Three of the ten, plus a property-test net. The decisions worth keeping,
because none of them are recoverable from the code alone:

**The seed question, answered both ways.** Wilder's recursion _is_ an EMA with
`α = 1/n`, so both RSI and MACD faced the same fork: match TA-Lib's seeding or
use pond's own. They landed on opposite sides, for measured reasons.

|                              | RSI                                      | MACD                    |
| ---------------------------- | ---------------------------------------- | ----------------------- |
| error on the non-TA-Lib seed | **7.03 points** on a bounded 0–100 scale | 3.80% of line magnitude |
| by bar 79                    | 0.15                                     | **0.089%**              |
| crosses a decision threshold | yes (70/30)                              | no                      |
| verdict                      | **dedicated `wilderValues` kernel**      | **pond's own EMA**      |

MACD's error largely cancels (it is a _difference_ of two EMAs) and decays far
faster, so the deciding argument there was **internal consistency**: seeding
TA-Lib's way would make `macd()` disagree with `ema(fast) − ema(slow)` inside
our own package. That property is pinned by a test — if the two drift, the
justification is gone.

**Left open, deliberately: `ema()`'s seed convention.** Making pond's MACD
TA-Lib-identical means changing it everywhere — breaking, oracle-pinned, and
inherited by every EMA-derived indicator after MACD. It is a package-wide
decision, not a per-study one, and was not taken here.

**Kernels added.** `wilderValues` (the seeded recursion; RSI and ATR) and
`trueRangeValues` (ATR, and ADX/NATR/Keltner/SuperTrend when they land).

**Recursive smoothers differ from `ema()` on interior gaps** — `ema()` skips
and recovers, Wilder propagates to the end. Both are defensible; Wilder's is
the conservative answer, since a bar with no close leaves the next bar's true
range unknown too. Documented on both studies; **not** reconciled. Worth a
deliberate decision before ADX adds a third consumer.

**Deliberate deltas from TA-Lib**, each documented on its study: RSI reports
`undefined` on a flat window where TA-Lib reports `0` (which is also its
answer for "every bar fell", so it cannot distinguish the two); MACD warms
each column up when it can, where TA-Lib masks all three back to the slowest.

**The oracle gained TA-Lib** alongside pandas, and the generator now _asserts_
agreement rather than merely computing alongside — including the null **mask**,
not just the values. Two assert-shape mistakes are recorded in the PR trail
and worth not repeating: `nanmax(|a−b|)` is blind to indices where only one
side is NaN (so it could not catch the warm-up off-by-one it existed for), and
"the delta must decay" is vacuous for a seed difference (it holds for every
wrong `α` too — only a tail bound discriminates).

**Property tests, converted from TA-Lib's own suite** (`talib-properties.test.ts`,
BSD 2-Clause). The oracle checks values on a clean, never-flat, gap-free input;
these cover what that input avoids — scale behaviour, composition over another
study's output, all-missing input. The composition one is not hypothetical:
`rsi(sma(...))` returned an entirely empty column in review. Studies README
step 5b makes it part of the checklist.

**Recurring failure mode, mine.** Every Layer-2 review of this wave found an
assertion that read as coverage and tested nothing: a monotonic fixture sending
every RSI value through one branch, a constant-range fixture passing under every
true-range mutation, unfireable `!isNaN` checks (`withColumn` maps NaN to
missing, so they can never fire). Mutation-test a new test before trusting it.

**Landed (returns fan-out): `momentum`, `historicalVolatility`; ROC resolved
as already shipped.** ROC was checked before being built: TA-Lib `ROC` and
`percentChange` agree exactly (`0.0`, identical masks) on the fixture, so the
deliverable was a TA-Lib cross-check in the generator plus doc notes, not a
duplicate study. Momentum matches TA-Lib `MOM` exactly. HV has no TA-Lib
reference, so its four conventions were decided and pinned in the pandas
oracle: population σ (package convention), log returns, `√annualize` with
`252` as an option not a constant, decimal output. Two shapes worth carrying
forward: (1) a study built on **differences** whose σ goes through the
rolling kernel must call `rollingMeanSdInto` on the returns array directly —
a scratch column through `rollingValues` counts _rows_, so the leading
missing return makes it emit one bar early over one return too few; (2) HV
steps over a leading run of missing returns (Wilder-kernel convention) so
the first window covers `period` real returns, matching pandas
`min_periods`, rather than the rolling family's "rows, not contributors"
contract that `sma∘sma` pins. Interior gaps still follow the rolling
contract (σ over the finite returns in the window).

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

**Landed — OBV / VWAP (volume fan-out).** The oracle fixture gains a
`volume` column (spikes of 5–9×, so a dropped weighting sits > 1.2 price
units from the plain mean — asserted). Decisions: (1) **`obv` is TA-Lib's
exactly** (seed `OBV[0] = volume[0]`, flat close adds nothing; mask then
values, delta 0) and is the first study with no `period` — it is read for
its shape, not its level. (2) **A missing cell in a running sum propagates
to the end**, the Wilder asymmetry for the same reason: a window recovers
once a gap leaves it, a cumulative quantity cannot. A leading gap in either
input shifts the seed to the first bar where both are present (what TA-Lib's
wrapper does by stripping leading NaNs). Deliberate delta from TA-Lib, whose
NaN close makes both comparisons false and silently carries on at a level
that is wrong forever (measured on seven bars: 400 out at the gap bar, 100
out thereafter). (3)
**`vwap` is ROLLING** — `Σ tp·v / Σ v` over `period` bars, `tp = (h+l+c)/3`,
`period` required (no conventional length). Anchored / session VWAP is NOT a
special case of it (a count window emits once it spans `period` rows) and
only makes sense with a reset, so it is deferred to the session phase; the
`cumulativeValues` kernel it needs already exists. No `price` option — a
knob with one conventional value; redirect `high`/`low` at `close` for a
close-weighted VWAP. A bar missing ANY input leaves BOTH sums, so the ratio
is never biased by a volume whose price went missing. (4) Four one-loop
kernels — `cumulativeValues` (A/D, PVT next), `signedVolumeValues`,
`typicalPriceValues` (CCI, MFI, Keltner next), `rollingWeightedMeanValues`
(VWMA next; two range-exact mean passes whose divisor cancels — a fused
single pass would be ~10× cheaper at the cost of range exactness, the same
trade already deferred for the fused SMA). Lesson from the mutation matrix:
the `Σw === 0 ? NaN` guard was dead code (non-negative weights make 0/0 NaN
already) and was removed rather than kept with a false "load-bearing" claim.

**Landed — the K2 moving-average engine.** `kernels/moving-average.ts` names
the MA-type menu once — `MaType` = `sma | ema | wma | smma | dema | tema |
trima | hull | kama | zlema` — with `movingAverageValues(values, period,
type)` over a **raw array** (so the derived-input callers can use it: Keltner
on typical price, Coppock's WMA of two ROCs, Disparity, the Price Oscillator)
and `movingAverageColumn` as the series-level door. Surfaced as the
`movingAverage({ period, type, column, output = 'ma' })` study plus fluent;
`envelope.maType` widened from `'sma' | 'ema'` to the whole menu. Decisions:
(1) **`sma` and `ema` are routed back to `rollingValues` / `emaValues`, not
reimplemented** — one definition rather than two that agree "to rounding", and
they keep the parallel accelerator and core's columnar EMA fast path; pinned
bit-for-bit, which is also what makes the envelope widening provably a no-op
on the two types it already had. (2) **The EMA seed question was not
reopened**: `ema`/`dema`/`tema` use pond's first-sample seed, so the engine
cannot disagree with `ema()` inside the package (the MACD precedent). Masks
are identical to TA-Lib's regardless — the seed does not move the lookback —
so the generator asserts the null mask exactly and bounds the values at the
tail; measured at period 21 the transient is 0.210% / 0.529% / 0.059% of scale
at the first shared bar and 0.0008% / 0.0155% / 0.0144% by bar 79. `sma`,
`wma`, `trima` and `kama` match TA-Lib `MA(matype=…)` outright (`kama` to
`0`). (3) **Interior gaps are stated per type rather than reconciled** — the
Wilder asymmetry the plan flagged before ADX, now written down: window types
recover, the `ema` family skips the gap bar and carries on, `smma` and `kama`
propagate to the end. On the **array door** every type, `sma` included,
steps over a leading gap (waits for `period` finite values — the studies
README's rule for derived inputs; the builder had kept `sma()`'s
rows-not-contributors window there and the Layer-2 review held the engine
to the rule). The **column door** routes `'sma'` to `rollingValues`, so
`movingAverage({ type: 'sma' })` over a column and `sma()` stay one SMA; the
doors differ only on a column with missing cells, pinned both ways. (4) **WMA got the O(N) running weighted sum**
(`W(i) = W(i−1) − S(i−1) + period·x(i)`, rebuilt on `i % period === 0` like
`ranged.ts` so the cancellation cannot accumulate): 57.0 → 30.7 ms at period
20 over 1M bars, 262.1 → 29.4 ms at period 100 — the point being that the
shipped form is flat in `period` where the definition's dot product is
linear in it. (5) **Deferred and named**: MAMA/FAMA, T3, VIDYA (wants the
K6 recursion, [PND-SFOLD]) and the time-series/regression forecast MA (that
is kernel **K7**, not a smoothing rate). Two mutation-matrix findings worth
keeping: TRIMA's two SMA lengths **commute**, so swapping them changes no
number (only dropping the even branch's `+ 1` does); and KAMA's `sum === 0 ?
1` flat-window guard is load-bearing for a reason the first comment got wrong
— it is not what propagates a gap (the recursion's own `NaN` does that), it is
what stops an unguarded `0/0` emptying the column when a series flattens while
KAMA is still far below it. Unblocks the §6.1 K2 family (Price Oscillator,
Disparity, DPO, MA Cross/Deviation, GMMA, Rainbow, Alligator/Gator, TRIX,
Coppock, KST) and the §6.2 MA-centred bands (Keltner, STARC, High-Low Bands).

**Landed — the K2 consumers (channels and smoothed rates).** The first five
studies built _on_ the moving-average engine rather than beside it:
`keltner`, `atrBands` (§6.2) and `qstick`, `trix`, `coppock` (§6.1/§6.3).
Nothing here added a smoother — every "MA type" option is `MaType` and every
average is a `movingAverageValues` call on a **derived array**, which is the
door the engine was built for (typical price, the candle body, a sum of two
ROCs). Decisions:

(1) **Two new kernel assemblies, both named to stop a definition forking.**
`atrValues(high, low, close, period)` (`kernels/true-range.ts`) is the
true-range-then-Wilder pair `atr` was doing inline; `atr`, `keltner` and
`atrBands` now all call it, which is what makes "`atrbUpper` is
`close + multiplier × atr()`" **bit-for-bit** rather than a coincidence two
tests keep true (pinned as an exact `toBe` against the shipped `atr` study —
the generator can only get to 7.1e-15 because `(c + w) − c` is not `w` in
IEEE754, so the exact form of the claim lives on the TypeScript side).
`percentChangeValues(values, periods)` (`kernels/rate-of-change.ts`, K4) is
the percent rate of change **and its two edge rules** — no predecessor yet, a
zero base → `undefined`; `percentChange` was refactored onto it and `trix`
(1 bar) and `coppock` (two look-backs) share it, so the zero-base guard is
one rule in one place rather than three.

(2) **Keltner's variant is pinned, and the delta is a doc note not an
option.** The **modern** form ships — EMA(20) of typical price ± 2 × ATR(10),
Chester Keltner via Linda Raschke, and ChartIQ / StockCharts / TradingView's
default — because it is what a caller's chart draws. Keltner's own 1960 form
(SMA(10) of typical price ± 1 × the SMA of the **plain** high−low range) is
reachable except for the half-width: it is always **true** range here. A
`range | trueRange` knob was considered and rejected — plain range is a
quantity the package computes nowhere else, and the knob would have exactly
one useful value. Warm-up is **per column** (`macd`): centre at the MA's own
first bar, bands at `max(centre, ATR)`, so at `{ period: 5, atrPeriod: 14 }`
the centre keeps ten real bars a study masking to the slower input would
discard.

(3) **`atrBands` appends TWO columns.** No `atrbMiddle`: the middle is
`column`, already on the series, and a third column would be a copy of the
caller's own input to keep in step. That is the line between this and
`keltner`/`bollinger`, whose centres are _computed_. It also needed `column`
(what the bands are drawn around) to be **separate from** `close` (what the
ATR is measured from) — bands on an `sma` with volatility off the raw bars is
the usual reason to reach for it.

(4) **TRIX is not TEMA, and the oracle is what proves it.** TRIX wants the
EMA **chain** (`EMA³`); the K2 menu's `tema` is Mulloy's de-lagged
`3·EMA − 3·EMA² + EMA³` built from the same three stages. Reaching for `tema`
would compile, run, and be a different indicator, so `trix` composes three
`movingAverageValues(…, 'ema')` passes and a test pins the two apart. The
generator follows the EMA-family pattern from #695: the **formula** is
asserted bit-exact on **TA-Lib's own SMA seed** (2.2e-14, identical masks),
which is what catches a dropped stage, a `tema` substitution _or_ a
log-vs-percent rate of change; pond's first-sample seed is then bounded as a
transient. That bound is **2% of scale, not the MA family's 0.5%**, and the
looser number is measured rather than fitted: TRIX's scale is a percent rate
of change (0.45 at `period 15`) rather than a price, and only 37 bars are
shared, so the correct rate sits at 0.85% where `2/(n+2)` and `2/n` sit at
8.5% and 9.8% — 2% is ~2.4× clear below and ~4× under the nearest wrong one.
The line is named `trix`, not `trixLine`: the study _is_ the line, and the
signal keeps the family suffix.

(5) **Coppock's monthly convention is stated, not enforced.** The 14/11/10
defaults are Coppock's **months** on a monthly index chart; the study is
bar-count like every other one here, so on daily bars it is a defensible
short-horizon oscillator and not the indicator he defined — said on the
docstring because the defaults look innocuous on any chart. The weighted
average is part of the definition, so no `maType` knob (an EMA-smoothed
variant is a different curve, and a knob would let one name mean two lines).
`longPeriod` / `shortPeriod` are **symmetric** — the two ROCs are added — so
no ordering is enforced, unlike `macd`, which subtracts.

(6) **`open` joined the bar contract's used columns.** `qstick` is the first
study to read it (`DEFAULT_OHLCV` already named it), so the oracle fixture
gained an `opens` column: the **previous close, pulled just inside the bar's
own range** when it does not fit. It has to be pulled in because the
fixture's half-widths are deliberately narrower than its close-to-close moves
(the property that makes true range's gap terms win) — 47 of the 80 opens are
the previous close exactly and 33 are gap bars, with 30 negative bodies and
80 distinct ones, all asserted, so a dropped sign or a constant body cannot
pass.

**Mutation matrix** (26 mutations; failing tests of the five study files
each): 24 killed. The first run had **four** survivors and every one was a
**default value** — `atrBands` period, `qstick` period, `trix` signalPeriod
all changed no test at all; a defaults-equality block was added and they now
kill 1–2 each. The lesson is worth carrying: a study's defaults are part of
its published definition and nothing else in the shape tests them, because
every other test passes explicit options. The two remaining survivors are
**observationally equivalent**, not missing tests: `percentChangeValues`'
`i < periods` early return (an out-of-range typed-array read is `undefined`
and every arithmetic on it is already `NaN`, so removing the branch changes
no output — it stays as a **bounds** guard, so `values[i - periods]!` is not
a lie, and is documented as such rather than claimed load-bearing) and
`atrValues`' `start = 1`, which `atr` already documented as redundant since
`wilderValues` steps over a leading `NaN` anyway.

**Perf** (1M bars, medians, `scripts/perf-studies.mjs`): `keltner` 84 ms
(`maType: 'sma'` 96 ms), `atrBands` 66 ms, `qstick` 46 ms, `trix` 67 ms,
`coppock` 47 ms — against `bollinger` 128 ms and `stochastic` 368 ms on the
same run, so all five sit in the cheap half of the corpus. Every one is
options-validation plus kernel calls, so these are the kernels' numbers: no
new data loop was written, and nothing here rescans a window.

**Landed — the K2 consumers (price-vs-moving-average oscillators).**
`priceOscillator`, `disparityIndex`, `detrendedPriceOscillator`, `elderRay`
and `awesomeOscillator` — the first five studies built _on_ the K2 engine
rather than beside it; all four with an "MA type" in the corpus take `maType`
and call `movingAverageColumn`, so no sixth smoother exists. Decisions:

(1) **`priceOscillator`'s `mode` defaults to `'percent'`, and the default
matters.** TA-Lib ships the two forms as two functions (`APO` absolute, `PPO`
percent), so the name settles nothing. What settled it was step 0 of the
studies README: `priceOscillator({ mode: 'absolute', maType: 'ema',
fastPeriod: 12, slowPeriod: 26 })` **is `macd()`'s line bar-for-bar** — a
test pins that identity — so defaulting to absolute would have made the
headline call of a new study a rename of a shipped column. Percent is also
the scale-invariant, cross-instrument form. Rejected: a `percent: boolean`
(reads worse at the call site, doesn't extend), and two exported functions
`apo`/`ppo` (one study with one knob is the `stochastic({ slowing: 1 })`
precedent). (2) **The EMA seed was not reopened**, per `macd`: on `'ema'`
pond keeps its first-sample seed, so the generator proves the **formula** on
TA-Lib's own SMA seed (`2.8e-14`) and bounds the **seed transient**
separately — 6.61% of scale at the first shared bar, 0.41% worst over the
last 20, where a wrong rate is 5.09% (`2/(n+2)`), 5.99% (`2/n`) or 44.4%
(`1/n`). On a seedless type the parity is outright: `{sma, absolute}` matches
`APO(matype=0)` to `7.1e-14`, masks identical. The generator also rebuilds
`APO`/`PPO` from `talib.MA` first and confirms they _are_ the MA difference
(`8.9e-16`) before using them as a reference. (3) **DPO's alignment is a real
fork and is named.** We ship `price[i] − MA[i − shift]` (TradingView's
non-centered default); StockCharts' `price[i − shift] − MA[i]` is a
**different series**, not a re-plotting of ours, and the reason to reject it
is that a study here appends a column aligned to the source's time axis — a
value on bar `i` that describes a price `shift` bars earlier silently
misaligns with everything else on that row. `shift = ⌊period/2⌋ + 1` **floors**
on odd periods (the only rule that keeps the study a pure re-indexing of the
average; the same call `zlema`'s lag makes), so the warm-up is
`period − 1 + shift` and an odd `period` lands a bar before the even one above
it. (4) **`elderRay` and `awesomeOscillator` deliberately take no `maType`.**
Elder names the 13-bar EMA and Williams names the 5/34 SMA pair; an option
whose other nine settings nobody publishes is a speculative knob, and it would
let `awesomeOscillator` return something nobody calls an Awesome Oscillator.
(5) **The percent forms report `undefined`, not `Infinity`, on a zero
denominator** — reachable only on a column that can be zero or negative (a
return series, another oscillator), and a `±Infinity` in a chart's y-domain is
worse than a gap. Pinned both ways: the absolute form keeps emitting there.
(6) **One new kernel helper: `medianPriceValues(high, low)`**, added beside
`typicalPriceValues` rather than in a file of its own. The alternative was a
`(h + l) / 2` loop inside `awesomeOscillator`, which the "studies contain no
data loop" rule forbids; one consumer ships today and three more are named
(Alligator, Gator, High-Low Bands). Not a public export. Measured 7.6 ms at
1M bars against `typicalPriceValues`' 10.8 ms, and `awesomeOscillator`'s
77.8 ms is exactly its parts (7.6 + 36.9 + 33.4). Perf at 1M bars:
`priceOscillator` 61.0 ms, `elderRay` 55.3 ms, `disparityIndex` 55.3 ms,
`detrendedPriceOscillator` 47.5 ms — all in the two-engine-calls-plus-a-pass
band, none of them near the range studies' 240–310 ms. (7) **Considered and
not built**: "moving average deviation" (`price − MA`, the absolute sibling of
Disparity) — it is arithmetic anyone can write and the comparable form is the
one worth naming; and a `centered` flag on DPO, which would be a second series
behind a boolean rather than a knob on one.

**Landed — the volume & money-flow group (corpus §6.6).**
`accumulationDistribution`, `chaikinOscillator`, `priceVolumeTrend`,
`chaikinMoneyFlow`, `moneyFlowIndex`, `forceIndex`, `easeOfMovement` and
`volumeOscillator`, over **one** new kernel. Decisions:

(1) **The close location value is a kernel, and a flat bar's is exactly
`0`.** `clvValues` / `accumulationDistributionValues`
(`kernels/close-location.ts`) are shared by three studies, which is what
earned the file. `CLV` is the stochastic's "position in a range" at a different
arity — `clv = 2·%K/100 − 1` over the bar's own high and low — and the builder
first carried the stochastic's flat-window rule across (`high === low` →
missing, on the "no answer beats a conventional zero" precedent), documenting
it as a deliberate delta from TA-Lib's `AD`. The Layer-2 review of #699 argued
the algebra and won: on a flat bar the numerator `(c − l) − (h − c)` is
_forced_ to zero — the close is at the high and at the low — so `0` is the
ratio's value in the limit and the only contribution the bar can make, where a
stochastic's flat window has a numerator that is not forced to anything. So a
flat bar contributes `0`: A/D matches TA-Lib bar-for-bar with no delta (measured
on twelve bars with bar 3 flattened, both give `[0, 100, 100, 100, 100, 400, …,
1900]`), the Chaikin oscillator carries on through a halt, and CMF keeps the
bar's volume in its denominator as every conventional CMF does. Only a
_missing_ price is a gap. The lesson is recorded for the next flat-window
decision: ask whether the numerator is forced to zero before reaching for
`undefined`.

(2) **The Chaikin oscillator is the one EMA-family study with no seed delta —
measured, not assumed.** Every other EMA study here carries the `macd`
precedent (pond seeds on the first sample, TA-Lib on the SMA of the first `n`),
and the brief expected the same treatment. It does not apply: TA-Lib's own
`ADOSC` seeds **both** EMAs with the first A/D value, which is pond's
convention, so the oracle asserts **exact** equality (delta `0`, identical
masks) at `{3,10}` and `{4,12}` instead of bounding a transient. The generator
also rebuilds the SMA-seeded version and asserts it is _far_ from `ADOSC`
(423.0 and 350.4 on the fixture), so the case pins which seed ships rather than
being blind to the difference.

(3) **`priceVolumeTrend` composes on the ROC kernel and divides by 100**, and
its **bar 0 is `undefined`, not `0`**. The kernel returns TA-Lib's percent; PVT's
published definition (ChartIQ's) is the fraction, so the constant appears in one
visible place rather than in a re-derived ratio — and the zero-base guard comes
with it. On the seed: unlike `obv`, whose `volume[0]` convention is TA-Lib's and
is matched for exactness, PVT has no vendor function to defer to, and the term
needs a previous close. Every level from bar 1 on is identical either way, so
declining to invent the seed costs nothing (a test pins that).

(4) **CMF is `rollingWeightedMeanValues`, MFI is two `rollingMeanValues`
passes, and `volumeOscillator` is `priceOscillator`.** No new window loops. CMF
is `Σ x·w / Σ w` with the close location for `x` — literally VWAP's kernel — so
it inherits the "a gap in either input leaves both sums" and "`Σ w = 0` →
missing" rules rather than growing a second set. MFI's ratio cancels the
`1/period`, so _means_ serve for its two sums. And `volumeOscillator` **is**
`priceOscillator`'s percent mode over the volume column; it delegates, a test
pins the identity, and what the wrapper adds is the name and the 5/10/sma
defaults (against 12/26/ema) — a thin alias being the honest answer when only
the vocabulary is new. Its `'absolute'` mode is deliberately not re-exposed: a
difference of two volume averages is a share count, which is what the percent
form exists to normalise away.

(5) **Volume Rate of Change is not a study.** `percentChange({ column:
'volume' })` _is_ the corpus definition, and `percentChange` is already
cross-checked against TA-Lib's `ROC`. What shipped is a recipe note in API.md
and a test that pins it — step 0 of the studies README, the `roc` precedent.

(6) **MFI's TA-Lib deltas, measured.** Values agree to `2.8e-14` with identical
masks, and the warm-up is `period` rows (not `period − 1`) on both sides. Two
divergences: a window with **zero total flow** (a flat typical price, or no
volume) is `undefined` here and `0` in TA-Lib — the `rsi` flat-window rule, and
`0` is MFI's most bearish reading for a window that showed no direction. More
sharply, **TA-Lib returns `0` for any window whose total flow is merely below
`1.0`** (a magnitude threshold in its C source, not a definition): measured on a
strictly rising 20-bar series at `1e-9` volume it returns `0` where the answer
is `100`. This study has no threshold.

(7) **`easeOfMovement` exposes `scale`, and is quadratic in price.** `100_000_000`
is StockCharts'/ChartIQ's constant and is a pure linear multiplier, so the knob
cannot change a sign or a crossing — it is exposed because the right constant
depends on the instrument's volume units (a crypto pair quoting fractional
volume reads as zeros at 1e8), and a caller forced to rescale the output column
by hand would be silently incomparable with the chart package beside them. The
alternative (fix it, tell callers to multiply) was considered and rejected on
that ground. Its **scale behaviour is the odd one in the package** and is pinned
because the plausible assumption is wrong: the distance moved _and_ the bar's
range both scale with price, so EOM scales with **k²**, is inversely
proportional to volume, and is linear in `scale`.

(8) **Lesson from the mutation matrix: a "redundant" guard that is
load-bearing on exactly one MA type.** Removing the `volume === 0` guard in
`easeOfMovement` killed **zero** tests — the window MA types mask a non-finite
cell, so the resulting `±Infinity` read back as missing anyway. It is not dead
code, though: `smma` is Wilder's recursion and _carries_ what it is given, so an
unguarded infinity would reach `withColumn`, which rejects an infinity loudly.
The fix was the missing test (`maType: 'smma'` over a zero-volume bar), not the
`rollingWeightedMeanValues` answer of deleting the guard — the difference from
that case is that this one has a reachable failure and that one had none.
Measured on the ten K2 types with an `Infinity` in the input: only `smma`
propagates it to the output.

**Perf at 1M bars** (all in their expected bands; nothing owns a data loop):
`accumulationDistribution` 53.4 ms, `chaikinOscillator` 70.1 ms,
`priceVolumeTrend` 33.0 ms, `chaikinMoneyFlow` 103.7 ms, `moneyFlowIndex`
110.6 ms, `forceIndex` 32.4 ms, `easeOfMovement` 74.8 ms, `volumeOscillator`
84.2 ms — against `obv()` 27.5 ms, `vwap(20)` 99.6 ms (CMF's own kernel) and
`sma(20)` 43.1 ms (volume oscillator is two of them).

**Landed — the momentum tail (§6.3).** `chandeMomentum`,
`ultimateOscillator`, `commodityChannelIndex`, `intradayMomentumIndex`,
`relativeVigorIndex` and `psychologicalLine` — six oscillators that are all
**ratios**, which is the property that shapes the batch: unlike the price-unit
family (`elderRay`, `atrBands`, `awesomeOscillator`) every one is invariant to
**both** a scale factor and a constant shift, and both halves are asserted,
because an implementation that dropped a normalisation keeps the shift
invariance and loses the scale one. Decisions:

(1) **CMO ships Chande's unsmoothed sums, and step 0 is why.** TA-Lib's `CMO`
Wilder-smooths the two legs, which makes it **exactly `2 · rsi − 100`** — the
generator asserts that (`2.8e-14` on the oracle input at both parameterisations)
rather than asserting our agreement with it. Shipping TA-Lib's definition would
have added a study that is an affine restatement of a shipped column, which is
the failure mode the studies README's step 0 exists to catch. The two are a
genuinely different series, not a warm-up transient: identical warm-ups
(both first emit on bar `period`) and **68.28 points apart at `period 14`**,
131.55 at `period 5`, on a scale spanning 200 — asserted as a minimum
separation so the fixture cannot stop telling them apart. Chande's form is also
the one the corpus names and the one VIDYA's adaptive constant is defined on,
so it is the form a future K2 `vidya` type will want. Rejected: a
`smoothing: 'wilder' | 'none'` option — two indicators behind a flag, and the
caller who wants TA-Lib's number already has `2 · rsi(...) − 100` exactly.

(2) **The Ultimate Oscillator takes three named periods, not a tuple.** The
4/2/1 weights are **positional**, so `periods: [28, 14, 7]` is a silently
different indicator while `longPeriod: 7` is obviously wrong at the call site;
the three are validated strictly increasing for the same reason. The weights
themselves are not an option (nobody publishes an alternative). Parity with
`ULTOSC` is exact — **7.1e-15**, identical masks, at both `(7,14,28)` and
`(3,5,9)` — and the generator additionally asserts our `trueRangeValues` is
`talib.TRANGE` **bit-for-bit** (`0.0`), so the ATR family and this study measure
range identically by construction rather than by coincidence. The warm-up is
`longPeriod` rows, not `longPeriod − 1`: both `BP` and `TR` read the previous
close.

(3) **CCI brought the package's first super-linear kernel, and it is stated
rather than hidden.** `rollingMeanAbsDevValues` is **O(N·period)**: mean
absolute deviation is not a moment, so it has no O(1) sliding update — one
removal and one addition can flip the sign of every remaining term. A
sub-linear form does exist (an order-statistic index over the window, prefix
sums either side of the mean, `O(N log period)`), and it is **written down in
the kernel docstring but not built**: measured at 1M bars, `period 20` costs
128 ms and `period 100` costs 229 ms, so the deviation walk is ~1.3 ms per unit
of period per 1M bars — ~25 ms of the 128 at the default, which is the same
band as the linear studies rather than a different one. The tree form is the
answer only if a caller wants CCI at a period in the thousands. The `period 20`
default is ChartIQ's and is **not universal** (TA-Lib's own default is 14),
which the docstring says so a vendor comparison checks the length first.

(4) **A zero-deviation guard was written, and mutation testing deleted it.**
CCI's `mad === 0` case is real (`undefined`, a deliberate delta from TA-Lib's
`0`, on the `rsi` flat-window precedent) but it needs no branch: the window ends
on the bar being reported, so a zero deviation forces a zero numerator and the
case _is_ `0/0`. The explicit guard survived every mutation because no input can
tell it is there — exactly the `rollingWeightedMeanValues` finding repeating,
and now recorded twice. The same reasoning is why `chandeMomentum` and
`intradayMomentumIndex` were written without one (non-negative legs: a zero
denominator forces a zero numerator). It does **not** apply to `ultimateOscillator` and
`relativeVigorIndex`, whose denominators can be zero over a non-zero numerator
on inconsistent bars (a close outside its own range, which a redirected `close`
option produces) — those guards kill mutations, and `±Infinity` reaching
`withColumn` is the alternative.

(5) **IMI is plain sums, not Wilder's.** The corpus maps it as "RSI form on
`(C − O)`", and _RSI form_ is the ambiguity: RSI's averages are smoothed, every
published statement of IMI's are not. The unsmoothed form ships, which is what
gives it the window family's recover-after-a-gap behaviour instead of RSI's
carry-to-the-end. Its warm-up is `period − 1`, one row shorter than `rsi` and
`chandeMomentum`, because a body needs no previous bar. Over identical legs
`imi = (cmo + 100)/2`, and a test pins that on a series whose opens are the
previous close — the two studies checking each other's arithmetic.

(6) **RVI is TradingView's, and the SWMA is a kernel.** The smoother is the
**symmetric `(1,2,2,1)/6`** 4-bar filter, not the K2 engine's linear `wma(4)`;
same width, same normalisation, completely different impulse response, so a
kernel test pins the response to a unit spike (`1, 2, 2, 1`, not `4, 3, 2, 1`).
The width is fixed rather than a `period` because "SWMA" names those weights and
there is no published rule to generalise them. The signal line is a fourth SWMA
of the index (TradingView's), the warm-up is per column (`period + 2` and
`period + 5`), and this is the first study in the package to read all four OHLC
columns.

(7) **Psychological Line pins the two choices it has.** An unchanged close is
**not** an up bar (strict `>`, so a flat window reads `0`, not `50` — some
vendors count it as half, which is a different study), and it is the one ratio
here with no `0/0` case at all because its denominator is `period`. A bar with
no close leaves **two** bars undirected (its own and its successor's), which the
array door then carries.

(8) **One kernel was extracted rather than added.** `upDownLegValues` — the
gain/loss split — was a private loop in `rsi`; `chandeMomentum` and
`intradayMomentumIndex` need the same one, so it moved to `kernels/up-down.ts`
and `rsi` now calls it (oracle unchanged). It earns a kernel because the obvious
one-liners are both wrong in one case each: `Math.max(d, 0)` maps a flat bar
correctly but `d > 0 ? d : 0` maps an **unknown** change to a flat one, and
"flat" and "unknown" are the two answers the whole family's `0/0`-versus-gap
behaviour depends on.

(9) **A fixture finding worth carrying forward.** The oracle's `opens` are the
previous close pulled inside the bar, so over a short window the candle bodies
and the close-to-close changes nearly coincide: IMI and the same form on closes
are **0.0 points apart at `n = 3`** and 0.07 at `n = 4`, reaching ~10 by
`n = 12`. The second IMI oracle case is therefore `period 8` (6.2 points apart),
not a shorter one — a fixture that cannot separate the two inputs would pass a
study that read the wrong one.

Perf at 1M bars (medians of 5, run twice, agreeing within 10%):
`psychologicalLine` 50 ms, `intradayMomentumIndex` 82 ms, `chandeMomentum`
90 ms, `commodityChannelIndex` 128 ms (229 at `period 100`),
`relativeVigorIndex` 149 ms, `ultimateOscillator` 225 ms — against `rsi` 59 ms,
`awesomeOscillator` 85 ms and the range studies' 250–330 ms. The two dearest are
the two that run the most kernel passes (UO computes six rolling means plus a
true range; RVI two SWMAs, two rolling means and a third SWMA), which is what
"a study is options-validation plus kernel calls" is supposed to cost.

**Considered and not built**: a `weights` option on the Ultimate Oscillator
(the definition is the weights); a `maType` on any of the six (none of the
published definitions has a smoother to choose — CMO and IMI sum, CCI averages
by definition, RVI's filter is named in its own definition); the
`O(N log period)` mean-absolute-deviation kernel (documented, gated on a caller
who wants a period in the thousands); and a half-credit rule for an unchanged
close in `psychologicalLine` (a second study behind a boolean).

**Landed — the Wilder directional group (§6.4).** `directionalMovement`,
`aroon` and `vortex`, plus the two kernels they needed —
`directionalMovementValues` (with the vortex's crossing legs beside it) and
`barsSinceExtremeValues`, the corpus's **G3 argmax gap**. The batch's shape is
the split between a _recursion_ and a _window_: two Wilder smooths stacked on
a third for `ADX`, against two studies that recover from a gap. Decisions:

(1) **The DMS ships Wilder's seed, and TA-Lib's own inconsistency is the
argument.** TA-Lib's `ADX` family accumulates `+DM`/`−DM`/`TR` over the first
`period − 1` bars and then takes one decayed step; Wilder's published
worksheet sums the first `period` and decays from there, which is exactly
`period ×` the mean-form recursion `wilderValues` runs — **and is what
TA-Lib's own `ATR` does**. Measured on the oracle input at `period 14`,
TA-Lib's `ATR` first value is `1.515450` while the true range its own `+DI` is
dividing by on the same bar is `1.411787`: a caller plotting TA-Lib's `+DI`
beside TA-Lib's `ATR` is reading two different ranges. Ours cannot diverge
that way — the `DI` denominator is literally the `atrValues` call, and a unit
test pins that a fixture with `+DM = 2`, bar range `2` and true range `3`
reads `+DI = 200/3` rather than `100`. The cost is a **decaying transient**
against TA-Lib rather than a wrong rate (the `macd`/`ema` precedent): `+DI`
0.117 points at the first shared bar, 0.513 at worst, 0.0092 by bar 79;
`ADX` 0.421 → 0.0046; at `period 5` it is larger at the start (2.58) and gone
(1e-6) by bar 79, decaying as `(1 − 1/period)^k`. The oracle splits the proof
the way `moving_average` splits the EMA family: **replay our own pipeline on
TA-Lib's seed and assert the FORMULA exactly** (`≤ 2.9e-14` on all five
columns, masks identical either way), then bound the pond-seed transient and
assert it decays. Rejected: a second Wilder door taking TA-Lib's seeding —
it would make `directionalMovement`'s range disagree with `atr()` inside our
own package, which is the same trade `macd` refused.

(2) **`ADXR` looks back `period − 1` bars.** Wilder's prose says "the `ADX`
`period` days ago" and several vendors read that literally; TA-Lib uses
`i − (period − 1)`, and so does this — it is what keeps the family
mask-identical to TA-Lib, and it is the package's own **bar-count**
convention, since a `period`-bar window spans `i − period + 1 … i` and this
averages its two ends. Measured separation between the two readings on the
oracle input: **2.64 points at `period 14`**, 8.19 at `period 5` — asserted in
the generator so the fixture cannot stop telling them apart. A caller wanting
the literal reading has `dmAdx` on the series and can shift it.

(3) **One study, five columns, five warm-ups** — the `macd` precedent, taken
further than any study so far. `ADXR` as a knob or a separate study was
rejected: it would re-run the entire pipeline to append one column that is two
reads of a column the study already holds. Warm-ups are `period` /
`2·period − 1` / `3·period − 2`, TA-Lib's own, and identical to its masks on
every column — the seed moves values, never lookback.

(4) **`+DI + −DI = 0` is `DX = 0`; a zero true range is `undefined`.** The
house test (is the numerator _forced_ to zero by the same condition?) splits
these two cleanly for once. Both `DM` legs are non-negative, so a zero sum
means both are zero and `|+DI − −DI|` is exactly zero with it — the
`clvValues` flat-bar case, and a real market state (a run of inside bars: range,
but no directional movement), which TA-Lib also reads as `0`. A zero **true
range** is not: there the `DI` ratio is a genuine `0/0`, and TA-Lib's `0`
would be indistinguishable from a real reading. `vortex` needed the opposite
answer for the same-looking case: `Σ TR = 0` does **not** force `Σ +VM = 0`,
because a flat window can still be preceded by a bar at another level, so its
guard is real (`±Infinity` is the alternative) and a unit test builds exactly
that series.

(5) **The G3 argmax kernel is a monotonic deque, and it is now the measured
case for the core fast path.** `barsSinceExtremeValues` keeps a ring buffer of
candidate indices, so it is O(N) and **flat in `period`**: measured at 1M bars,
26.7 ms at `period 25` and 26.5 ms at `period 200`, against a naive re-scan's
78 ms and 545 ms (both benchmarked, the naive form kept in `perf-studies.mjs`
as the control). The consequence worth carrying forward is the comparison with
the studies on core's reducers: `aroon` costs **102 ms** at 1M bars where
`donchian` costs **247 ms** and `stochastic` 313 ms. That is the first measured
number behind the standing note that a monotonic-deque fast path for core's
rolling min/max would lift the range studies ~2×.

(6) **Two window rules, deliberately different, in the same file.**
`barsSinceExtremeValues` is **strict** — every one of the `period + 1` cells
must be finite — where `highestLowestValues` skips gaps and reports the extreme
over what it has. The distinction is that an extreme over the cells you _do_
have is still an honest extreme, but its **age** is not: the hole could be
hiding the very bar being asked about. TA-Lib's own answer is the argument for
strictness — fed a `NaN` high it reports `aroonUp = 100` on every later bar,
because its running extreme silently never updates.

(7) **The window is `period + 1` bars** — the one place in the package where a
`period` is not its window's bar count. Aroon's `period` counts the oldest
_age_ reportable, and "`period` bars ago" is itself a reading. The rule lives
in the kernel rather than in `aroon` so a second consumer cannot get it wrong,
and the warm-up (`period` rows) is TA-Lib's.

(8) **Ties go to the most recent bar**, measured rather than assumed: on the
window `12, 11, 12, 10.5` at `period 4`, TA-Lib reports `aroonUp = 75` (the
newer bar's age), not `25`. The deque gets this from **non-strict** eviction
(`<=` for a max), and the generator asserts the measurement so the claim stays
reproducible.

(9) **The vortex's legs live in the DM kernel file, unexported.** `+VM` and
`−VM` are the same shape as Wilder's split — a two-legged per-bar derivation
off the previous bar's extremes — and the _contrast_ is the point: Wilder
compares each extreme with the previous bar's same extreme (at most one leg
non-zero, an inside bar reports nothing), the vortex crosses them (both legs
always positive). Keeping them together is what makes that a comment rather
than a coincidence. Only `directionalMovementValues` and
`barsSinceExtremeValues` are public, on the `movingAverageValues` precedent;
`vortexMovementValues` is module-scoped like `trueRangeValues`.

Perf at 1M bars (medians of 5, run twice, agreeing within 8%):
`aroon` 102 ms (110 at `period 200` — flat), `directionalMovement` 145 ms,
`vortex` 143 ms — against `rsi` 55 ms, `atrBands` 66 ms, `donchian` 247 ms and
`stochastic` 313 ms. The two Wilder-family studies cost what their kernel
passes cost (`directionalMovement` runs a DM split, a true range and four
Wilder smooths; `vortex` a movement split, a true range and three rolling
means).

Mutation matrix (failing tests per mutation, over the seven affected test
files): DM sign guard removed 1 / 1, `Math.max(up, 0)` 4, non-strict leg
comparison 1 / 1, NaN guard dropped 1, bar 0 zero-not-missing 1 / 1, `+VM`
absolute value dropped 1, `+VM` reads its own low 8; argmax tie rule 3, warm-up
one bar early 8, window one bar short 7, gap rule dropped 7, min mode 8, window
`period` not `period + 1` 7; DMS `DX` guard inverted 1, `DX` absolute value 3,
ADXR look-back `period` 6, ADXR without the look-back 5, ADX smoothed at the
wrong rate 7, DI denominator unsmoothed 23; aroon age not inverted 4, down leg
on the high column 5; vortex guard removed 1, legs swapped 4, plain-range
denominator 4. **One real survivor, now fixed:** dropping the `up > 0` sign
guard from the `+DM` leg killed nothing — the mirror case on `−DM` was covered
and this one was not (the shape is an inside bar whose high fell _less_ than
its low rose, where an unguarded leg reports a negative `+DM`); a kernel case
was added and it now kills a test. **One mutation survives by construction and
is left alone:** relaxing `up > 0` to `up >= 0` is semantically a no-op (at
`up === 0` both branches assign `0`), the third instance of the
`rollingWeightedMeanValues` / `commodityChannelIndex` finding — a branch no
input can distinguish. The `wilderValues(…, start = 1)` calls are the same kind
of documented no-op `atrValues` already carries.

**Considered and not built**: an `adxr` boolean or a second study for it (see
(3)); a `maType` on any of the three (Wilder's smoothing is the definition,
and the vortex's sums are not an average of anything); clamping `+DI`/`−DI`
into `0..100` (the bound is a property of consistent bars, not something to
enforce — an inconsistent redirected `close` should read honestly); and
promoting `barsSinceExtremeValues` to a core reducer, which is the right
follow-up but belongs with the rolling min/max fast path it shares a structure
with, not with a study batch.

**Decision record — the Wilder-vs-`ema` interior-gap asymmetry.** Deferred
since the Phase-1 write-up as "decide before ADX"; ADX is the study that makes
it compound, so here is the measured statement and a recommendation. **Nothing
was changed** — both kernels ship as they were.

_The two behaviours._ `wilderValues` propagates an interior `NaN` to the end
of the series: its recursion carries state forward, and there is no state to
carry across a hole. `emaValues` **skips** the gap bar and carries on from the
value before it, which is what lets `macd` and the K2 `ema` family run over
another study's output without emptying. Both kernels already step over a
_leading_ run of gaps, so the asymmetry is purely about interior ones.

_What the new studies do, measured_ (an 80-bar series, one missing cell at bar
40, `period 14`): a gap in **`high`** or **`low`** blanks `dmPlusDi`, `dmMinusDi`,
`dmDx`, `dmAdx` and `dmAdxr` from bar **40** to the end — the `DM` split needs
both bars, and the Wilder smooth then carries it. A gap in **`close`** blanks
the same five from bar **41**: the true range reads only the _previous_ close,
so it is the next bar that has no denominator (`dmAdxr` emits at bar 40, its
first possible bar, and nothing after). `aroon` and `vortex`, being windows,
lose a bounded run and recover — `aroon` `period + 1` bars, `vortex` up to
`period + 1`, with the two legs losing _different_ rows.

_What TA-Lib does with a `NaN`, measured._ Its behaviour is not a policy at
all; it is an artifact of C comparison semantics, and it goes both ways. A
`NaN` **high** or **low** propagates to the end of `ADX` exactly as ours does
(40 of the 40 remaining bars missing). A `NaN` **close** produces **no missing
value whatsoever**: every `max` comparison against `NaN` is false, so the true
range silently falls back to the bar's own range and the study carries on with
a wrong number. `AROON` fed a `NaN` high is worse — it reports `aroonUp = 100`
on every subsequent bar, forever, because its running extreme never updates.
So on this axis pond is already strictly better defined than the vendor, in
both directions.

_The options._

- **(a) Keep the asymmetry, document it per study** — the status quo.
  Blast radius: none. The two kernels differ because their _seeds_ differ in
  kind: `emaValues` is defined over a stream of samples (its seed is the first
  sample, so "skip" is just "do not consume that one"), while Wilder's is
  defined over a fixed _count_ of bars whose mean seeds it — skipping a hole
  silently redefines the time base, making a "14-bar RMA" span 20 calendar
  bars without saying so. It is also the conservative answer: a bar with no
  close leaves the _next_ bar's true range unknown too, so there is no honest
  value to resume from.
- **(b) A `gaps: 'propagate' | 'skip'` option on `wilderValues`.** Blast
  radius: none by default, but the option has to surface on every study that
  smooths — `rsi`, `atr`, `keltner`, `atrBands`, `directionalMovement`, and
  the K2 `smma` type wherever a `maType` is exposed (ten studies) — or it is a
  kernel knob no caller can reach. That is the `smoothing: 'wilder' | 'none'`
  shape the CMO write-up rejected: two indicators behind a flag.
- **(c) Make `wilderValues` skip, like `ema`.** Blast radius: every Wilder
  consumer changes on gapped input — `rsi`, `atr` and everything on
  `atrValues` (`keltner`, `atrBands`, the new `directionalMovement`), plus
  `movingAverage({ type: 'smma' })` and the ten `maType` studies that can
  select it. No **oracle** case moves (the fixture is gap-free), which is
  precisely why this would be a silent behaviour change: nothing in the value
  suite would flag it, only the missing-cell tests.

_Recommendation: **(a)**._ The asymmetry is not an inconsistency to reconcile
but a difference in what the two recursions are defined over, and every study
that hits it now says so in its docstring. Revisit only on a real friction
signal — an experiment whose bars genuinely have interior holes (a halted
session) and whose Wilder studies therefore read empty from the halt onward.
The answer today is "fill before smoothing", and if that turns out to be
unreasonable in practice, **(c)** is the honest fix rather than **(b)** — one
behaviour, not a flag — and it should land as a deliberate, changelogged
behaviour change with missing-cell tests updated in the same pass.

**Fan-out mechanics (how the three parallel study PRs were run).** One
builder agent per study group on `isolation: "worktree"` branches
(`fanout/returns`, `fanout/stoch`, `fanout/volume`), Opus models per Peter,
integrated one at a time onto `main` by the library agent. Lessons: (1) an
agent that backgrounds `npm run verify` yields before it commits — verify
must run in the foreground; (2) the scratchpad is shared across worktrees,
so every agent prefixes its scratch files; (3) `git merge-tree` previews
against merge-commit history say "clean" where the real merge onto a
squash-merged base conflicts in every shared file (index, fluent, API.md,
tests, oracle generator, fixture) — budget for a hand merge, regenerate the
oracle fixture rather than resolving it textually, and never run a shell
heredoc inside an `&&` chain during that merge (it breaks the chain and the
following steps run on a conflicted tree); (4) three ~230k-token builders in
parallel exhaust the session rate limit — two at a time. (5) A builder's "measured"
claim is re-measured against the oracle venv before it is repeated
anywhere: #688's OBV write-up quoted a TA-Lib gap output that was actually
the fill-with-previous-close answer, and it reached the docstring, a test
comment, the CHANGELOG, this plan and the PR body before the Layer-2
reviewer re-ran TA-Lib.

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
