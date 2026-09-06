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
internal consistency); Wilder-vs-`ema` interior-gap asymmetry (decide
before ADX); `percentChange.periods` vs `period` naming; the website study
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
