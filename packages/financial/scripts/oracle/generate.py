#!/usr/bin/env python3
"""Oracle for @pond-ts/financial studies.

Computes reference indicator values with **pandas** (the de-facto numerical
reference — the open-source stand-in for MATLAB here) and writes a golden JSON
that the vitest suite asserts our TypeScript studies against. Independent
implementation → agreement is a real cross-check.

Conventions are pinned to match @pond-ts/financial exactly (and TA-Lib where
noted) — an oracle only helps if its conventions line up:

  - SMA        close.rolling(n).mean()
  - EMA        close.ewm(span=n, adjust=False).mean()  (recursive, first-value
               seed — NOT pandas' adjust=True default), first n-1 rows masked to
               match our length-preserving `minSamples: n` warm-up.
  - Bollinger  middle = SMA(n); band = middle +/- k * rolling(n).std(ddof=0)
               (POPULATION stdev — TA-Lib's convention, not pandas' ddof=1
               default).

The input series is deterministic (sines + drift, no RNG) so regeneration is
reproducible. Realistic enough to exercise the studies; deliberately never flat,
so the sigma=0 band -> undefined case (separately unit-tested) doesn't arise.

Regenerate (CI never runs this — the JSON is committed):

    python3 -m venv .venv
    .venv/bin/pip install pandas
    .venv/bin/python packages/financial/scripts/oracle/generate.py

Phase-2 named indicators (RSI/MACD/ATR/stochastics) will add TA-Lib alongside
pandas here, documenting any definition delta (bar-for-bar vendor parity is a
non-goal).
"""

import datetime as dt
import json
import math
import pathlib
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

try:  # optional: the industry cross-check for the named indicators
    import talib
except ImportError:  # pragma: no cover - pandas-only venv
    talib = None

N = 80
# Deterministic close series — two sines + a slow drift.
closes = [
    round(100 + 0.15 * i + 6 * math.sin(i / 9) + 2 * math.sin(i / 3.3), 4)
    for i in range(N)
]
s = pd.Series(closes, dtype="float64")

# High/low for the bar studies (ATR, and later stochastics/Donchian).
#
# Built AROUND each close rather than displaced from it, so `low <= close <=
# high` holds on every bar. An earlier version injected four artificial gap
# bars by shifting high and low without moving close, which left the close
# outside its own range - harmless for ATR (true range never reads the
# current close) but wrong for any study that does, which is exactly what
# these arrays are earmarked for next.
#
# The half-widths VARY, so a constant-range bug cannot pass. They are also
# deliberately SMALLER than the close series' own bar-to-bar moves, which
# reach 1.41: that makes genuine gaps arise from the data instead of being
# painted on, so `|high - prevClose|` and `|low - prevClose|` win on real
# bars. The assert below is what holds that true if the series ever changes.
highs = [
    round(c + 0.35 + 0.5 * abs(math.sin(i / 4)), 4) for i, c in enumerate(closes)
]
lows = [
    round(c - 0.35 - 0.5 * abs(math.cos(i / 3)), 4) for i, c in enumerate(closes)
]
assert all(
    lo <= c <= hi for lo, c, hi in zip(lows, closes, highs)
), "oracle bars must satisfy low <= close <= high"

_wins = {"hl": 0, "hc": 0, "lc": 0}
for _i in range(1, N):
    _terms = {
        "hl": highs[_i] - lows[_i],
        "hc": abs(highs[_i] - closes[_i - 1]),
        "lc": abs(lows[_i] - closes[_i - 1]),
    }
    _wins[max(_terms, key=lambda k: _terms[k])] += 1
assert all(v > 0 for v in _wins.values()), (
    f"each true-range term must win on some bar, got {_wins} - the fixture "
    "would not distinguish an implementation that dropped one"
)

# Opens for the body studies (QStick; later IMI, Relative Vigor Index).
#
# Built as the PREVIOUS CLOSE, pulled just inside the bar's own range when it
# does not fit. That is what a real tape looks like: a bar that does not gap
# opens where the last one closed, and one that does opens at the extreme end
# of its range. It has to be pulled in at all because this fixture's bar
# half-widths are deliberately NARROWER than its close-to-close moves (see the
# true-range note above), so an unclamped previous close would sit outside its
# own bar on the gap bars - which is exactly the shape the high/low asserts
# were added to stop. Measured on this series: 46 of the 80 opens are the
# previous close exactly, and 33 are gap bars.
#
# The 0.05 inset keeps the open strictly INSIDE the range rather than on it,
# so no bar reads a degenerate zero-width body against its own extreme.
opens = [
    round(
        min(
            max(closes[i - 1] if i else round(closes[0] - 0.2, 4), lows[i] + 0.05),
            highs[i] - 0.05,
        ),
        4,
    )
    for i in range(N)
]
assert all(
    lo <= o <= hi for lo, o, hi in zip(lows, opens, highs)
), "oracle bars must satisfy low <= open <= high"
_bodies = [c - o for c, o in zip(closes, opens)]
assert (
    sum(b > 0 for b in _bodies) >= 10 and sum(b < 0 for b in _bodies) >= 10
), (
    "oracle bodies must change sign - a QStick on all-up bars would pass "
    "under a dropped sign"
)
assert len(set(round(b, 6) for b in _bodies)) > N // 2, (
    "oracle bodies must vary - a constant body makes every MA type agree"
)

h = pd.Series(highs, dtype="float64")
low_s = pd.Series(lows, dtype="float64")
o_s = pd.Series(opens, dtype="float64")

# Volume for the volume studies (OBV, VWAP; later MFI / A/D / CMF).
#
# Positive and VARYING - a constant volume makes VWAP identical to the plain
# mean of typical price, so a fixture on constant volume cannot tell a
# weighted study from an unweighted one, and OBV on it is just a count of
# up-bars minus down-bars times a constant. Two incommensurate periods keep it
# from repeating, a floor of ~500 keeps it positive, and every eleventh bar
# (offset 7) carries a SPIKE of five to nine times the typical volume, so a
# VWAP that drops the weighting is off by a visible amount, not a rounding
# one. The asserts below hold both properties if the series ever changes.
volumes = [
    round(
        1500
        + 600 * math.sin(i / 2.7)
        + 400 * math.cos(i / 5.1)
        + (8000 + 300 * (i % 5) if i % 11 == 7 else 0)
    )
    for i in range(N)
]
assert all(v > 0 for v in volumes), "oracle volume must be strictly positive"
assert len(set(volumes)) > N // 2, "oracle volume must vary, not repeat"
assert sum(v > 5000 for v in volumes) >= 5, (
    "oracle volume needs several spike bars - without them a VWAP that "
    "dropped the weighting is within rounding of the plain mean"
)

vol = pd.Series(volumes, dtype="float64")

# Benchmark ("index") closes for the two-series family: correlation, beta,
# price relative and performance index (assessment 6.7, kernel K8).
#
# Modelled INDEPENDENTLY of `closes` -- a different drift, different sine
# periods and different phases -- for two reasons the studies depend on.
# It must be correlated with the primary series over some windows and
# anti-correlated over others, so the fixture exercises the whole [-1, 1]
# range rather than one corner. And it must NOT be an affine transform of
# `closes`: an affine benchmark pins correlation at exactly 1 on every
# window and makes beta a near-constant, and a fixture like that cannot
# tell a correct implementation from one that dropped a term. The asserts
# in `correlation` and `beta` below hold both properties if it ever changes.
benchmarks = [
    round(
        120 + 0.06 * i + 7 * math.sin(i / 13 + 0.5) + 2.5 * math.sin(i / 3.7 + 1.9),
        4,
    )
    for i in range(N)
]
assert all(b > 0 for b in benchmarks), "oracle benchmark must be strictly positive"
assert len(set(benchmarks)) > N // 2, "oracle benchmark must vary, not repeat"

bench_s = pd.Series(benchmarks, dtype="float64")

# A LONGER close-only series, for the two studies whose warm-up does not fit
# in 80 bars.
#
# Pring's Special K reaches back 530 bars and smooths the result over 195, so
# its first value lands on bar 724 -- a case run on `closes` would be entirely
# null and would pass VACUOUSLY. The Trend Intensity Index at Pee's own
# periods (a 60-bar average with the last 30 deviations summed) first prints
# on bar 88, which is also past the end of the 80-bar fixture.
#
# It is a SEPARATE array rather than a longer `closes` on purpose: extending
# the primary input would recompute every one of the existing cases and turn
# a nine-study addition into a whole-fixture diff. Cases that need it carry
# `"input": "long"` and the vitest side builds a close-only series from it.
#
# Three incommensurate sines over a DELIBERATELY SLIGHT drift, so that (a)
# Pring's Special K crosses zero -- the reading it exists for -- which a
# steadily-rising series does not, because every one of its twelve rates of
# change is then positive at once (measured: at a 0.05/bar drift the line runs
# 151.5..379.3 and never turns); (b) deviations from a 60-bar average change
# sign often, or TII would sit pinned at 0 or 100 and could not tell the sum
# form from the count form; (c) the 7- and 65-bar averages RAVI reads cross
# each other repeatedly, or its absolute value would be untested; and (d) it
# stays strictly positive, which every percent rate of change needs.
LONG_N = 900
long_closes = [
    round(
        100
        + 0.01 * i
        + 22 * math.sin(i / 70)
        + 6 * math.sin(i / 21 + 0.7)
        + 1.5 * math.sin(i / 5.3 + 2.1),
        4,
    )
    for i in range(LONG_N)
]
assert all(c > 0 for c in long_closes), "the long oracle series must be positive"
assert len(set(long_closes)) > LONG_N // 2, "the long oracle series must vary"

long_s = pd.Series(long_closes, dtype="float64")
_long_dev = long_s - long_s.rolling(60).mean()
_long_fast = long_s.rolling(7).mean() - long_s.rolling(65).mean()
assert (_long_fast > 0).sum() > 100 and (_long_fast < 0).sum() > 100, (
    "the long series' 7- and 65-bar averages must cross repeatedly -- "
    "otherwise RAVI's absolute value never bites and the signed form would "
    "be indistinguishable"
)
assert (_long_dev > 0).sum() > 200 and (_long_dev < 0).sum() > 200, (
    "the long series' deviations from its 60-bar average must change sign "
    "often -- a one-sided series pins the Trend Intensity Index at 0 or 100 "
    "and cannot tell the sum form from the count form"
)

# A SESSION-KEYED clock for the same 80 bars, for the session-anchored studies
# (assessment 6.9, the G4 pair: sessionVwap and pivotPoints).
#
# Every other case keys bar `i` at `i` MILLISECONDS -- bar-indexed, which is all
# a window study needs and which no trading calendar can be laid over: 80ms is
# not a trading week. So these two cases carry their own timestamp array
# (`input.sessionTimes`, cases marked `"input": "session"`) and REUSE the
# existing OHLCV arrays, which is what keeps this from becoming a second
# fixture: the prices, ranges and volume spikes the other 200-odd cases are
# checked against are exactly the ones these are checked against.
#
# The grid is a real one: 30-minute bars stamped at their OPEN on a 09:30-16:00
# America/New_York session, six sessions, plus TWO bars that fall in NO session
# --
#   * bar 13 is stamped at Monday's CLOSE instant, which under the default
#     `[open, close)` stamping is closed time (the next session has not opened);
#   * bar 66 is a Saturday noon print, inside a gap widened by a holiday on
#     Monday 2024-01-15.
# Without them the fixture could not tell a study that reads `undefined` in
# closed time from one that carries the last session's value across the gap.
# 6 * 13 + 2 = 80, so the row count still matches every other input.
_ET = ZoneInfo("America/New_York")
_SESSION_DATES = [
    "2024-01-08",  # Mon
    "2024-01-09",
    "2024-01-10",
    "2024-01-11",
    "2024-01-12",  # Fri
    "2024-01-16",  # Tue -- 2024-01-15 is a holiday in the rules
]
BARS_PER_SESSION = 13  # 09:30 .. 15:30 on a 30-minute grid
BAR_MS = 30 * 60 * 1000


def _et_ms(date_str: str, hour: int, minute: int) -> int:
    """A local wall-clock instant in the exchange zone -> epoch milliseconds."""
    d = dt.date.fromisoformat(date_str)
    return int(
        dt.datetime(d.year, d.month, d.day, hour, minute, tzinfo=_ET).timestamp()
        * 1000
    )


sessions_table = [
    {"date": d, "open": _et_ms(d, 9, 30), "close": _et_ms(d, 16, 0)}
    for d in _SESSION_DATES
]
session_times: list[int] = []
for _si, _s in enumerate(sessions_table):
    session_times.extend(_s["open"] + k * BAR_MS for k in range(BARS_PER_SESSION))
    if _si == 0:
        session_times.append(_s["close"])  # exactly at the close -> closed time
    if _si == 4:
        session_times.append(_et_ms("2024-01-13", 12, 0))  # a Saturday print
assert len(session_times) == N, (
    f"the session grid must cover the same {N} bars as every other input; "
    f"got {len(session_times)}"
)
assert session_times == sorted(session_times), "bar times must ascend"


def _session_id(t: int):
    """The `[open, close)` session containing `t`, by its open instant."""
    for sess in sessions_table:
        if sess["open"] <= t < sess["close"]:
            return sess["open"]
    return math.nan


session_ids = [_session_id(t) for t in session_times]
assert sum(1 for x in session_ids if not math.isnan(x)) == N - 2, (
    "exactly two of the fixture's bars must fall outside every session"
)
assert len(set(x for x in session_ids if not math.isnan(x))) == len(_SESSION_DATES)

sid_s = pd.Series(session_ids, dtype="float64")


def col(series: pd.Series) -> list:
    """A pandas Series -> JSON list; NaN / non-finite (missing) -> null."""
    return [
        None if (pd.isna(x) or not math.isfinite(float(x))) else float(x)
        for x in series
    ]


def sma(n: int) -> dict:
    return {"sma": col(s.rolling(n).mean())}


def ema(n: int) -> dict:
    e = s.ewm(span=n, adjust=False).mean()
    e.iloc[: n - 1] = math.nan  # our length-preserving warm-up (minSamples: n)
    return {"ema": col(e)}


# --------------------------------------------------------------------------
# The K2 moving-average engine: ten types behind one `type` option.
#
# TA-Lib ships seven of them under MA(matype=...) and they are asserted
# against below. SMMA / Hull / ZLEMA have no TA-Lib function, so they are
# pandas replications of OUR definition with their analytic warm-up asserted,
# which is what stops a fixture from pinning the wrong one silently.
#
# THE SEED. `ema`, `dema` and `tema` run on POND's EMA (first-sample seed),
# not TA-Lib's (SMA of the first n) -- the `macd` precedent, and for the same
# reason: seeding TA-Lib's way here would make movingAverage(type='ema')
# disagree with ema() inside our own package. The seed does not move the
# lookback, so their null MASK is still asserted exactly; the VALUES get a
# tail bound (a seed difference decays, a wrong rate does not) and the
# first-bars delta is printed and recorded in the conventions block.
# --------------------------------------------------------------------------

def _round_half_up(x: float) -> int:
    """JS `Math.round`, which is not Python's `round` (banker's rounding).
    sqrt(n) is never exactly x.5 for integer n, so this only ever matters as
    a guard against the two languages drifting on some future input."""
    return math.floor(x + 0.5)


_MA_MENU = [
    "sma",
    "ema",
    "wma",
    "smma",
    "dema",
    "tema",
    "trima",
    "hull",
    "kama",
    "zlema",
]
_MA_TALIB = {"sma": 0, "ema": 1, "wma": 2, "dema": 3, "tema": 4, "trima": 5, "kama": 6}
_MA_SEED_DELTA = {"ema", "dema", "tema"}  # pond's seed, not TA-Lib's
# The three TA-Lib has no function for: assert the analytic first valid bar.
_MA_NO_TALIB_FIRST = {
    "smma": lambda n: n - 1,
    "hull": lambda n: n - 2 + _round_half_up(math.sqrt(n)),
    "zlema": lambda n: (n - 1) // 2 + n - 1,
}


def _ema_sma_seed(values, n: int) -> np.ndarray:
    """TA-Lib's span-EMA: seed = mean of the first n finite samples, placed at
    the n-th, then the recursion. Used to prove the DEMA/TEMA FORMULAS against
    TA-Lib exactly, separately from the seed convention (see moving_average)."""
    x = np.asarray(values, dtype=float)
    out = np.full(len(x), np.nan)
    alpha = 2.0 / (n + 1.0)
    finite = np.flatnonzero(~np.isnan(x))
    if len(finite) < n:
        return out
    first = int(finite[0])
    out[first + n - 1] = float(np.mean(x[first : first + n]))
    for i in range(first + n, len(x)):
        out[i] = alpha * x[i] + (1.0 - alpha) * out[i - 1]
    return out


def _ema_first_seed(values, n: int) -> pd.Series:
    """Pond's span-EMA over an ARRAY: first-sample seed, missing cells skipped,
    emitted once `n` samples have been consumed -- so a leading run of NaN
    (another stage's warm-up) steps the seed over rather than poisoning it."""
    x = np.asarray(values, dtype=float)
    out = np.full(len(x), np.nan)
    alpha = 2.0 / (n + 1.0)
    prev = None
    seen = 0
    for i, v in enumerate(x):
        if not np.isfinite(v):
            continue
        prev = v if prev is None else alpha * v + (1.0 - alpha) * prev
        seen += 1
        if seen >= n:
            out[i] = prev
    return pd.Series(out)


def _wma(values, n: int) -> pd.Series:
    """Linear weights 1..n, heaviest on the newest bar. pandas' rolling needs
    `n` non-NaN observations, which is our "the whole window must be finite"
    mask -- a positional weight cannot skip a cell without reweighting the
    rest."""
    w = np.arange(1.0, n + 1.0)
    return (
        pd.Series(np.asarray(values, dtype=float))
        .rolling(n)
        .apply(lambda x: float(np.dot(x, w) / w.sum()), raw=True)
    )


def _kama(values, n: int, fast: int = 2, slow: int = 30) -> pd.Series:
    """Kaufman adaptive MA with TA-Lib's constants; seeded on x[n-1], so the
    first value lands on bar n. ER = |n-bar change| / (sum of the n absolute
    steps); the triangle inequality bounds it by 1 with no clamp, and a flat
    window (0/0) takes TA-Lib's ER = 1."""
    x = np.asarray(values, dtype=float)
    out = np.full(len(x), np.nan)
    const_slow = 2.0 / (slow + 1.0)
    const_diff = 2.0 / (fast + 1.0) - const_slow
    prev = x[n - 1]
    for i in range(n, len(x)):
        change = abs(x[i] - x[i - n])
        path = float(np.sum(np.abs(np.diff(x[i - n : i + 1]))))
        er = 1.0 if path == 0 else change / path
        sc = (er * const_diff + const_slow) ** 2
        prev = prev + sc * (x[i] - prev)
        out[i] = prev
    return pd.Series(out)


def _wilder(values, n: int) -> pd.Series:
    """Wilder / RMA over an ARRAY: seed = mean of the first n finite values,
    then (prev*(n-1) + x)/n, with a leading run of NaN stepped over rather
    than poisoning the seed -- `wilderValues`, which is also the engine's
    `smma`."""
    x = np.asarray(values, dtype=float)
    out = np.full(len(x), np.nan)
    finite = np.flatnonzero(np.isfinite(x))
    if len(finite) < n:
        return pd.Series(out)
    first = int(finite[0])
    seed = first + n - 1
    if seed >= len(x):
        return pd.Series(out)
    out[seed] = float(np.mean(x[first : seed + 1]))
    for i in range(seed + 1, len(x)):
        out[i] = (out[i - 1] * (n - 1) + x[i]) / n
    return pd.Series(out)


def _ma_over(values, kind: str, n: int) -> pd.Series:
    """One of the ten K2 types over an arbitrary array -- the ARRAY door
    (`movingAverageValues`), where every type waits for `n` FINITE values.

    Parameterised on the input because the K2 consumers smooth DERIVED
    arrays: Keltner the typical price, QStick the candle body, Coppock the
    sum of two ROCs. `_ma_values` is this over the fixture's closes, where
    (the series being gap-free) the array and column doors agree.
    """
    x = pd.Series(np.asarray(values, dtype=float))
    if kind == "sma":
        # pandas' rolling requires `n` non-NaN observations, which IS the
        # array door's "finite values, not rows" rule.
        return x.rolling(n).mean()
    if kind == "ema":
        return _ema_first_seed(x, n)
    if kind == "wma":
        return _wma(x, n)
    if kind == "smma":
        return _wilder(x, n)
    if kind in ("dema", "tema"):
        e1 = _ema_first_seed(x, n)
        e2 = _ema_first_seed(e1, n)
        if kind == "dema":
            return 2 * e1 - e2
        e3 = _ema_first_seed(e2, n)
        return 3 * e1 - 3 * e2 + e3
    if kind == "trima":
        # TA-Lib's split: the two box lengths sum to n + 1, so the convolution
        # is n bars wide. Odd n gets a single peak, even n a two-bar plateau.
        p, q = ((n + 1) // 2, (n + 1) // 2) if n % 2 else (n // 2 + 1, n // 2)
        return x.rolling(p).mean().rolling(q).mean()
    if kind == "hull":
        half, root = max(1, n // 2), max(1, _round_half_up(math.sqrt(n)))
        return _wma(2 * _wma(x, half) - _wma(x, n), root)
    if kind == "kama":
        return _kama(x, n)
    if kind == "zlema":
        lag = (n - 1) // 2  # floors; see the kernel's note on even periods
        return _ema_first_seed(2 * x - x.shift(lag), n)
    raise AssertionError(f"unknown moving-average type {kind!r}")


def _ma_values(kind: str, n: int) -> pd.Series:
    return _ma_over(s, kind, n)


def moving_average(n: int, kind: str) -> dict:
    v = _ma_values(kind, n)
    label = f"movingAverage({n}, {kind})"

    if kind in _MA_NO_TALIB_FIRST:
        expected = _MA_NO_TALIB_FIRST[kind](n)
        assert v.first_valid_index() == expected, (
            f"{label} first valid at {v.first_valid_index()}, expected "
            f"{expected} -- the fixture would pin the wrong warm-up"
        )
        print(
            f"  {label}: pandas replication (TA-Lib has no {kind.upper()}); "
            f"first valid at {expected}"
        )
    elif talib is not None:
        matype = _MA_TALIB[kind]
        ref = pd.Series(
            talib.MA(np.asarray(closes, dtype=float), timeperiod=n, matype=matype)
        )
        # Mask first, always (see rsi): nanmax(|a-b|) is blind to an index
        # where only one side is NaN, so a warm-up off-by-one passes at 0.0.
        assert list(v.isna()) == list(ref.isna()), (
            f"{label} warm-up differs from TA-Lib MA(matype={matype}): ours "
            f"first valid {v.first_valid_index()}, TA-Lib {ref.first_valid_index()}"
        )
        both = (~np.asarray(v.isna())) & (~np.asarray(ref.isna()))
        d = np.abs(
            np.asarray(v, dtype=float)[both] - np.asarray(ref, dtype=float)[both]
        )
        if kind in _MA_SEED_DELTA:
            # Two separate questions, asserted separately (a Layer-2 review
            # of #695 showed the last-bar check alone lets a wrong DEMA
            # coefficient through, because every EMA-family formula converges
            # to the tail):
            #
            # (a) THE FORMULA, exactly: rebuild the same type on TA-Lib's own
            #     SMA seed and require bit-level agreement with TA-Lib. A
            #     swapped coefficient or a dropped stage fails here at 1e-9.
            e1 = _ema_sma_seed(closes, n)
            if kind == "ema":
                formula = e1
            elif kind == "dema":
                e2 = _ema_sma_seed(e1, n)
                formula = 2 * e1 - e2
            else:
                e2 = _ema_sma_seed(e1, n)
                e3 = _ema_sma_seed(e2, n)
                formula = 3 * e1 - 3 * e2 + e3
            refa = np.asarray(ref, dtype=float)
            assert (np.isnan(formula) == np.isnan(refa)).all(), (
                f"{label}: SMA-seeded replication's warm-up differs from TA-Lib"
            )
            fm = ~np.isnan(refa)
            fd = float(np.max(np.abs(formula[fm] - refa[fm])))
            assert fd < 1e-9, (
                f"{label}: SMA-seeded replication disagrees with TA-Lib by {fd} "
                "- the formula, not the seed, is wrong"
            )
            # (b) THE SEED, bounded: pond's first-sample seed differs from
            #     TA-Lib's SMA seed by a transient that must have decayed over
            #     the last 20 shared bars, not merely at the last one.
            scale = float(np.nanmax(np.abs(ref)))
            worst, tail = float(d.max()), float(d[-20:].max())
            assert tail / scale < 0.005, (
                f"{label} is {tail / scale:.3%} from TA-Lib over the last 20 "
                "shared bars - too far to be the seed transient"
            )
            print(
                f"  {label}: formula matches TA-Lib on its SMA seed to {fd:.2g}; "
                f"pond seed vs TA-Lib's SMA seed - {worst / scale:.3%} at the "
                f"first shared bar, {tail / scale:.4%} worst over the last 20 "
                "(warm-up masks identical)"
            )
        else:
            assert float(d.max()) < 1e-9, (
                f"{label} disagrees with TA-Lib MA(matype={matype}) by {d.max()}"
            )
            print(
                f"  {label}: matches TA-Lib MA(matype={matype}) to {d.max():.3g} "
                "(warm-ups identical)"
            )
    else:
        print(f"  {label}: pandas only - TA-Lib not installed, cross-check SKIPPED")

    return {"ma": col(v)}


# The engine's dispatch has to be DISCRIMINATING or a wrong-branch bug is
# invisible: ten types that all agreed on this fixture would let `type` be
# ignored entirely and every case still pass. Asserted pairwise, on the bars
# where both are defined.
for _n in (10, 21):
    _computed = {k: np.asarray(_ma_values(k, _n), dtype=float) for k in _MA_MENU}
    for _i, _a in enumerate(_MA_MENU):
        for _b in _MA_MENU[_i + 1 :]:
            _both = np.isfinite(_computed[_a]) & np.isfinite(_computed[_b])
            assert _both.any(), f"{_a} and {_b} never overlap at period {_n}"
            _sep = float(np.max(np.abs(_computed[_a][_both] - _computed[_b][_both])))
            assert _sep > 0.01, (
                f"movingAverage {_a} and {_b} at period {_n} agree to within "
                f"{_sep} on this fixture - a wrong-branch dispatch would pass"
            )


def bollinger(n: int, k: float) -> dict:
    mid = s.rolling(n).mean()
    sd = s.rolling(n).std(ddof=0)  # population — matches us + TA-Lib
    return {
        "bbMiddle": col(mid),
        "bbUpper": col(mid + k * sd),
        "bbLower": col(mid - k * sd),
    }


def rolling_stdev(n: int) -> dict:
    return {"stdev": col(s.rolling(n).std(ddof=0))}  # population


def rolling_min(n: int) -> dict:
    return {"min": col(s.rolling(n).min())}


def rolling_max(n: int) -> dict:
    return {"max": col(s.rolling(n).max())}


def rolling_percentile(n: int, q: float) -> dict:
    # linear interpolation (pandas default) == our p{q} reducer.
    return {f"p{q}": col(s.rolling(n).quantile(q / 100))}


def zscore(n: int) -> dict:
    m = s.rolling(n).mean()
    sd = s.rolling(n).std(ddof=0)
    return {"zscore": col((s - m) / sd)}


def envelope(n: int, percent: float) -> dict:
    mid = s.rolling(n).mean()
    f = percent / 100
    return {
        "envMiddle": col(mid),
        "envUpper": col(mid * (1 + f)),
        "envLower": col(mid * (1 - f)),
    }


def envelope_ema(n: int, percent: float) -> dict:
    # maType: 'ema' — centre line is a span-EMA (adjust=False), first n-1 masked
    # (our length-preserving warm-up). Validates the emaValues kernel helper.
    mid = s.ewm(span=n, adjust=False).mean()
    mid.iloc[: n - 1] = math.nan
    f = percent / 100
    return {
        "envMiddle": col(mid),
        "envUpper": col(mid * (1 + f)),
        "envLower": col(mid * (1 - f)),
    }


def percent_change(periods: int) -> dict:
    pc = s.pct_change(periods) * 100

    if talib is not None:
        # percentChange IS rate-of-change: TA-Lib's ROC is the same
        # `(price / prevPrice - 1) * 100`. Cross-checked here so the package
        # can say so with a number, rather than shipping a duplicate `roc`
        # study that differs invisibly. Mask first, then magnitude (see rsi).
        ref = pd.Series(talib.ROC(np.asarray(closes, dtype=float), timeperiod=periods))
        assert list(pc.isna()) == list(ref.isna()), (
            f"percentChange({periods}) warm-up differs from TA-Lib ROC: "
            f"ours first valid {pc.first_valid_index()}, "
            f"TA-Lib {ref.first_valid_index()}"
        )
        delta = float(np.nanmax(np.abs(pc - ref)))
        assert delta < 1e-9, f"percentChange({periods}) disagrees with TA-Lib ROC by {delta}"
        print(
            f"  percentChange({periods}): matches TA-Lib ROC to {delta:.3g} "
            "(warm-ups identical)"
        )

    return {"pctChange": col(pc)}


def _rsi_series(n: int) -> pd.Series:
    """Wilder's RSI over the fixture's closes, as a Series -- factored out of
    `rsi` so `stochasticRsi` reads the SAME RSI the TA-Lib-verified case
    pins, rather than a second replication that could drift from it.

    A flat window (no gains and no losses) is 0/0 and comes back NaN, which
    is pond's answer and a documented delta from TA-Lib's 0; an all-gains
    window divides by zero, giving +inf and therefore exactly 100, which is
    the limit of the formula and matches pond.
    """
    d = s.diff()
    up = d.clip(lower=0)
    dn = (-d).clip(lower=0)
    ag = pd.Series(math.nan, index=s.index, dtype="float64")
    al = pd.Series(math.nan, index=s.index, dtype="float64")
    ag.iloc[n] = up.iloc[1 : n + 1].mean()
    al.iloc[n] = dn.iloc[1 : n + 1].mean()
    for i in range(n + 1, len(s)):
        ag.iloc[i] = (ag.iloc[i - 1] * (n - 1) + up.iloc[i]) / n
        al.iloc[i] = (al.iloc[i - 1] * (n - 1) + dn.iloc[i]) / n
    return 100 - 100 / (1 + ag / al)


def rsi(n: int) -> dict:
    """Wilder's RSI, as TA-Lib defines it.

    The seed is the definition, not a warm-up detail. Wilder averages the
    FIRST n differences arithmetically and only then runs the recursion
    avg[i] = (avg[i-1]*(n-1) + x[i]) / n. A plain first-sample-seeded EMA of
    the same alpha (1/n) is a DIFFERENT indicator: on this very input it
    lands up to 7.03 RSI points away and is still 0.15 out at bar 79.

    Computed in pandas here (so the fixture regenerates without the C
    library) and asserted against TA-Lib below when it is installed.
    """
    r = _rsi_series(n)

    if talib is not None:
        ref = pd.Series(talib.RSI(np.asarray(closes, dtype=float), timeperiod=n))
        # Compare the WARM-UPS before the values. `nanmax(abs(a - b))` is blind
        # to any index where only one side is NaN — the difference there is
        # itself NaN and gets skipped — so a series emitting one bar earlier
        # than TA-Lib, with an arbitrarily wrong value in that slot, would pass
        # at 0.0. Which is precisely the off-by-one this assert exists to
        # catch, so it has to be checked as a mask, not a magnitude.
        assert list(r.isna()) == list(ref.isna()), (
            f"RSI({n}) warm-up differs from TA-Lib: "
            f"ours first valid {r.first_valid_index()}, "
            f"TA-Lib {ref.first_valid_index()}"
        )
        delta = float(np.nanmax(np.abs(r - ref)))
        assert delta < 1e-9, f"RSI({n}) disagrees with TA-Lib by {delta}"
        print(f"  rsi({n}): matches TA-Lib to {delta:.3g} (warm-ups identical)")
    else:
        print(f"  rsi({n}): pandas only - TA-Lib not installed, cross-check SKIPPED")

    return {"rsi": col(r)}


def macd(fast: int, slow: int, sig: int) -> dict:
    """MACD on POND'S EMA (first-sample seed), not TA-Lib's (SMA seed).

    Deliberate: seeding TA-Lib's way here would make macd() disagree with
    ema(fast) - ema(slow) inside our own package. The delta is measured and
    documented in studies/macd.ts; it is sub-percent and shrinking, unlike
    RSI's, because MACD is a DIFFERENCE of two EMAs so the seed error largely
    cancels. TA-Lib parity is therefore asserted only as a BOUND here, not as
    equality — which is the honest thing to check when the definitions differ
    on purpose.

    Each column warms up when it can, rather than all three waiting for the
    slowest (TA-Lib masks the line back to where the signal is valid).
    """
    fast_e = s.ewm(span=fast, adjust=False).mean()
    fast_e.iloc[: fast - 1] = math.nan
    slow_e = s.ewm(span=slow, adjust=False).mean()
    slow_e.iloc[: slow - 1] = math.nan
    line = fast_e - slow_e

    # The signal is an EMA of the line, whose own warm-up shifts past the
    # line's leading NaNs -- the same thing our kernel does.
    valid = line.dropna()
    sig_e = valid.ewm(span=sig, adjust=False).mean()
    sig_e.iloc[: sig - 1] = math.nan
    signal = pd.Series(math.nan, index=s.index, dtype="float64")
    signal.loc[valid.index] = sig_e
    hist = line - signal

    if talib is not None:
        mt, st, _ = talib.MACD(  # st is the signal - checked below too
            np.asarray(closes, dtype=float),
            fastperiod=fast,
            slowperiod=slow,
            signalperiod=sig,
        )
        scale = float(np.nanmax(np.abs(mt)))

        # The delta from TA-Lib here is a SEED difference, so what it must do
        # is DECAY - a differing seed washes out of a recursion, a differing
        # recursion does not. A fixed tolerance is therefore the wrong shape:
        # the transient is 3.8% at the first shared bar and 10.6% at short
        # spans. What discriminates is the TAIL.
        #
        # Only the tail. An earlier version also asserted `tail < worst` as
        # the "is it decaying" check; a review showed it is vacuous - it holds
        # for every wrong alpha, including 1/n, which is 43% out at the tail
        # and still decaying. Measured separation at (12,26,9), tail as a
        # fraction of scale:
        #
        #     correct 2/(n+1)   0.089%      <- passes
        #     wrong   2/(n+2)   2.304%
        #     wrong   2/n       2.818%
        #     wrong   1/n      42.850%
        #
        # so 0.5% sits ~25x clear of the correct value and ~5x below the
        # nearest wrong one. Both the line AND the signal are checked: the
        # signal is the novel piece here (an EMA of a derived array, whose
        # warm-up shifts past the line's own), so leaving it unchecked would
        # have exempted exactly the part worth checking.
        for label, ours, ref in (
            ("line", line, mt),
            ("signal", signal, st),
        ):
            both = (~np.isnan(ref)) & (~np.asarray(ours.isna()))
            d = np.abs(np.asarray(ours, dtype=float)[both] - ref[both])
            worst, tail = float(d.max()), float(d[-1])
            assert tail / scale < 0.005, (
                f"MACD({fast},{slow},{sig}) {label} is {tail / scale:.3%} from "
                "TA-Lib at the last bar - too far to be the seed transient, "
                "which points at the recursion rather than the seed"
            )
            print(
                f"  macd({fast},{slow},{sig}) {label}: seed transient "
                f"{worst / scale:.2%} at the first shared bar, "
                f"decayed to {tail / scale:.3%} by the last"
            )

    return {"macdLine": col(line), "macdSignal": col(signal), "macdHist": col(hist)}


def _true_range() -> pd.Series:
    """TR = max(high-low, |high-prevClose|, |low-prevClose|), undefined on bar
    0 (no previous close)."""
    prev = s.shift(1)
    tr = pd.concat(
        [(h - low_s), (h - prev).abs(), (low_s - prev).abs()], axis=1
    ).max(axis=1)
    tr.iloc[0] = math.nan
    return tr


def _atr_series(n: int) -> pd.Series:
    """Wilder's ATR: the recursion RSI uses over the true ranges, seeded on
    their mean over the first n, so the first value lands on bar n.

    Factored out because THREE studies now price on the same array -- `atr`
    itself, `keltner`'s band half-width and `atrBands` -- exactly as the
    TypeScript side shares one `atrValues` kernel call. A second replication
    here would be free to drift from the one TA-Lib is asserted against,
    which is the whole failure mode the shared kernel exists to stop.
    """
    tr = _true_range()
    a = pd.Series(math.nan, index=s.index, dtype="float64")
    a.iloc[n] = tr.iloc[1 : n + 1].mean()
    for i in range(n + 1, len(s)):
        a.iloc[i] = (a.iloc[i - 1] * (n - 1) + tr.iloc[i]) / n
    return a


def atr(n: int) -> dict:
    """Wilder's ATR, as TA-Lib defines it.

    TR = max(high-low, |high-prevClose|, |low-prevClose|), undefined on bar 0
    (no previous close); then the same Wilder recursion RSI uses, seeded on
    the mean of the first n true ranges, so the first value lands on bar n.

    Computed in pandas so the fixture regenerates without the C library, and
    asserted against TA-Lib -- both the values AND the warm-up mask -- when it
    is installed.
    """
    a = _atr_series(n)

    if talib is not None:
        ref = pd.Series(
            talib.ATR(
                np.asarray(highs, dtype=float),
                np.asarray(lows, dtype=float),
                np.asarray(closes, dtype=float),
                timeperiod=n,
            )
        )
        assert list(a.isna()) == list(ref.isna()), (
            f"ATR({n}) warm-up differs from TA-Lib: ours first valid "
            f"{a.first_valid_index()}, TA-Lib {ref.first_valid_index()}"
        )
        delta = float(np.nanmax(np.abs(a - ref)))
        assert delta < 1e-9, f"ATR({n}) disagrees with TA-Lib by {delta}"
        print(f"  atr({n}): matches TA-Lib to {delta:.3g} (warm-ups identical)")

    return {"atr": col(a)}


def momentum(n: int) -> dict:
    """Momentum: the absolute n-bar difference, `s[i] - s[i-n]`.

    The additive companion to percent_change's ratio. No smoothing, no seed,
    so TA-Lib's MOM must agree EXACTLY -- asserted below, mask first.
    """
    m = s.diff(n)

    if talib is not None:
        ref = pd.Series(talib.MOM(np.asarray(closes, dtype=float), timeperiod=n))
        assert list(m.isna()) == list(ref.isna()), (
            f"momentum({n}) warm-up differs from TA-Lib MOM: ours first valid "
            f"{m.first_valid_index()}, TA-Lib {ref.first_valid_index()}"
        )
        delta = float(np.nanmax(np.abs(m - ref)))
        assert delta < 1e-9, f"momentum({n}) disagrees with TA-Lib MOM by {delta}"
        print(f"  momentum({n}): matches TA-Lib MOM to {delta:.3g} (warm-ups identical)")

    return {"momentum": col(m)}


def historical_volatility(n: int, annualize: float) -> dict:
    """Historical volatility: population stdev of LOG returns, annualised.

        hv = ln(s).diff().rolling(n).std(ddof=0) * sqrt(annualize)

    Four conventions, each of which produces a different number if it goes
    the other way, and none of which TA-Lib can arbitrate (it has no HV):

      - POPULATION stdev (ddof=0) -- the package convention, shared with
        Bollinger / rollingStdev / zScore. ddof=1 is sqrt(n/(n-1)) larger.
      - LOG returns, not simple returns (symmetric, additive across bars,
        which is what makes sqrt-time annualisation legitimate).
      - annualised by sqrt(annualize); 252 for daily bars, 1 for per-bar.
      - a DECIMAL (0.18 = 18%), not a percent.

    The first return is undefined (bar 0 has no predecessor), so the first
    value lands on bar n -- `n` returns need `n + 1` prices. pandas' rolling
    requires n non-NaN contributors, which is exactly that warm-up.
    """
    r = np.log(s).diff()
    hv = r.rolling(n).std(ddof=0) * math.sqrt(annualize)
    assert hv.first_valid_index() == n, (
        f"historicalVolatility({n}) first valid at {hv.first_valid_index()}, "
        f"expected {n} -- the fixture would pin the wrong warm-up"
    )
    return {"hv": col(hv)}
# The range-position studies (stochastics, %R, Donchian) all read the highest
# high / lowest low over a window. Two things the fixture must NOT be for them
# to be checked honestly, both asserted rather than assumed:
#
#   - the window's extremes must not always sit at the window's EDGES, or an
#     implementation reading `high[i]` / `high[i-n+1]` instead of the max
#     would pass;
#   - the close must never sit exactly ON an extreme, or %K would be an exact
#     0 or 100 there and a swapped numerator (`HH - close` for `close - LL`)
#     would be invisible on that bar.
# Checked for EVERY window length a range-position case below uses, not just
# the default 14 -- a Layer-2 review of #687 found the 5-bar cases unguarded.
# "Not always at the edges" is made quantitative: at least ten windows must
# have an interior extreme (the fixture has 17 at 5 bars, more at 14), so an
# edge-reading bug differs from the oracle on at least ten bars per case.
for _n in (5, 14):
    _interior = 0
    for _i in range(_n - 1, N):
        _wh = highs[_i - _n + 1 : _i + 1]
        _wl = lows[_i - _n + 1 : _i + 1]
        if _wh.index(max(_wh)) not in (0, _n - 1) or _wl.index(min(_wl)) not in (
            0,
            _n - 1,
        ):
            _interior += 1
    _windows = N - _n + 1
    assert _interior >= 10, (
        f"only {_interior}/{_windows} {_n}-bar windows have an interior extreme; "
        "an edge-reading bug would be caught on too few bars to trust"
    )
    assert all(
        min(lows[_i - _n + 1 : _i + 1]) < closes[_i] < max(highs[_i - _n + 1 : _i + 1])
        for _i in range(_n - 1, N)
    ), f"a close sits exactly on a {_n}-bar extreme; %K would read an exact 0/100"


def _hh_ll(n: int):
    return h.rolling(n).max(), low_s.rolling(n).min()


def stochastic(k_period: int, slowing: int, d_period: int) -> dict:
    """Slow stochastic, as TA-Lib's STOCH (SMA smoothing) defines it.

    fast %K = 100 (close - LL) / (HH - LL) over k_period bars; %K = SMA of
    that over `slowing`; %D = SMA of %K over d_period. `slowing = 1` is the
    fast stochastic, which TA-Lib ships separately as STOCHF and is checked
    against too.

    ONE deliberate delta on the warm-up: TA-Lib masks %K back to %D's first
    valid bar, discarding d_period - 1 real values (bars 15 and 16 at the
    defaults). Ours emits %K where its definition makes it defined, as MACD
    does with its line. So the mask assert here is in two parts -- %D's mask
    must be IDENTICAL, and %K's must be identical from TA-Lib's first bar on
    and start exactly d_period - 1 bars earlier -- rather than one blanket
    equality that the documented delta would fail.
    """
    hh, ll = _hh_ll(k_period)
    fast_k = 100 * (s - ll) / (hh - ll)
    k = fast_k.rolling(slowing).mean()
    d = k.rolling(d_period).mean()

    if talib is not None:
        args = (
            np.asarray(highs, dtype=float),
            np.asarray(lows, dtype=float),
            np.asarray(closes, dtype=float),
        )
        ref_k, ref_d = talib.STOCH(
            *args,
            fastk_period=k_period,
            slowk_period=slowing,
            slowk_matype=0,
            slowd_period=d_period,
            slowd_matype=0,
        )
        ref_k, ref_d = pd.Series(ref_k), pd.Series(ref_d)
        label = f"stochastic({k_period},{slowing},{d_period})"
        assert list(d.isna()) == list(ref_d.isna()), (
            f"{label} %D warm-up differs from TA-Lib: ours first valid "
            f"{d.first_valid_index()}, TA-Lib {ref_d.first_valid_index()}"
        )
        ours_first, ref_first = k.first_valid_index(), ref_k.first_valid_index()
        assert ours_first == k_period + slowing - 2, (
            f"{label} %K first valid at {ours_first}, expected {k_period + slowing - 2}"
        )
        assert ref_first - ours_first == d_period - 1, (
            f"{label} %K starts {ref_first - ours_first} bars before TA-Lib's; "
            f"the documented delta is exactly {d_period - 1}"
        )
        assert list(k.isna())[ref_first:] == list(ref_k.isna())[ref_first:], (
            f"{label} %K mask differs from TA-Lib's after bar {ref_first}"
        )
        delta_k = float(np.nanmax(np.abs(k - ref_k)))
        delta_d = float(np.nanmax(np.abs(d - ref_d)))
        assert delta_k < 1e-9, f"{label} %K disagrees with TA-Lib by {delta_k}"
        assert delta_d < 1e-9, f"{label} %D disagrees with TA-Lib by {delta_d}"
        print(
            f"  {label}: %K matches TA-Lib to {delta_k:.3g} on every bar it emits "
            f"(ours starts {d_period - 1} bar(s) earlier, at {ours_first}); "
            f"%D to {delta_d:.3g} (warm-ups identical)"
        )
        if slowing == 1:
            # Fast stochastic: the same numbers must also be STOCHF's.
            fk, fd = talib.STOCHF(*args, fastk_period=k_period, fastd_period=d_period, fastd_matype=0)
            assert list(d.isna()) == list(pd.Series(fd).isna()), f"{label} %D mask != STOCHF"
            delta_fk = float(np.nanmax(np.abs(k - fk)))
            delta_fd = float(np.nanmax(np.abs(d - fd)))
            assert delta_fk < 1e-9 and delta_fd < 1e-9, f"{label} != STOCHF ({delta_fk}, {delta_fd})"
            print(f"  {label}: also matches STOCHF to {max(delta_fk, delta_fd):.3g}")

    return {"stochK": col(k), "stochD": col(d)}


def williams_r(n: int) -> dict:
    """Williams %R, as TA-Lib's WILLR: -100 (HH - close) / (HH - LL)."""
    hh, ll = _hh_ll(n)
    r = -100 * (hh - s) / (hh - ll)

    if talib is not None:
        ref = pd.Series(
            talib.WILLR(
                np.asarray(highs, dtype=float),
                np.asarray(lows, dtype=float),
                np.asarray(closes, dtype=float),
                timeperiod=n,
            )
        )
        assert list(r.isna()) == list(ref.isna()), (
            f"williamsR({n}) warm-up differs from TA-Lib: ours first valid "
            f"{r.first_valid_index()}, TA-Lib {ref.first_valid_index()}"
        )
        delta = float(np.nanmax(np.abs(r - ref)))
        assert delta < 1e-9, f"williamsR({n}) disagrees with TA-Lib by {delta}"
        print(f"  williamsR({n}): matches TA-Lib to {delta:.3g} (warm-ups identical)")

    return {"williamsR": col(r)}


def donchian(n: int) -> dict:
    """Donchian channel: rolling max of high, rolling min of low, midpoint.

    pandas only -- TA-Lib has no Donchian function.
    """
    upper, lower = _hh_ll(n)
    return {
        "dcUpper": col(upper),
        "dcLower": col(lower),
        "dcMiddle": col((upper + lower) / 2),
    }


def obv() -> dict:
    """On-Balance Volume, as TA-Lib defines it.

    OBV[i] = OBV[i-1] + sign(close[i] - close[i-1]) * volume[i], seeded with
    OBV[0] = volume[0] (TA-Lib's convention; some implementations seed at 0,
    which only offsets the whole line). An unchanged close adds nothing. No
    period, and no warm-up: the mask is empty on both sides.

    Computed in pandas here and asserted against TA-Lib - mask first, then
    values - when it is installed.
    """
    signed = np.sign(s.diff()) * vol
    signed.iloc[0] = vol.iloc[0]
    o = signed.cumsum()

    if talib is not None:
        ref = pd.Series(
            talib.OBV(
                np.asarray(closes, dtype=float), np.asarray(volumes, dtype=float)
            )
        )
        assert list(o.isna()) == list(ref.isna()), (
            f"OBV warm-up differs from TA-Lib: ours first valid "
            f"{o.first_valid_index()}, TA-Lib {ref.first_valid_index()}"
        )
        delta = float(np.nanmax(np.abs(o - ref)))
        assert delta < 1e-9, f"OBV disagrees with TA-Lib by {delta}"
        print(f"  obv(): matches TA-Lib to {delta:.3g} (warm-ups identical)")
    else:
        print("  obv(): pandas only - TA-Lib not installed, cross-check SKIPPED")

    return {"obv": col(o)}


def vwap(n: int) -> dict:
    """Rolling VWAP: sum(typicalPrice * volume) / sum(volume) over n bars,
    typicalPrice = (high + low + close) / 3.

    TA-Lib has no VWAP, and there is no single definition - the intraday
    desk's VWAP is ANCHORED (cumulative from the session open, no window).
    The rolling form is what ships (the package's bar-count-window shape);
    the anchored form is a session-reset study and is deferred to that phase.
    So this is a pandas replication of OUR definition, not a vendor check.

    The fixture must be able to tell the weighted mean from the plain one:
    asserted below as a minimum separation somewhere in the series.
    """
    tp = (h + low_s + s) / 3
    v = (tp * vol).rolling(n).sum() / vol.rolling(n).sum()

    plain = tp.rolling(n).mean()
    sep = float(np.nanmax(np.abs(v - plain)))
    assert sep > 0.25, (
        f"VWAP({n}) sits within {sep} of the unweighted mean of typical price "
        "- the fixture's volume is too flat to catch a dropped weighting"
    )
    print(f"  vwap({n}): pandas replication; {sep:.3f} from the plain mean at its widest")

    return {"vwap": col(v)}


def keltner(n: int, atr_n: int, mult: float, kind: str) -> dict:
    """Keltner Channel, the MODERN (Chester Keltner via Linda Raschke) form:
    an EMA of TYPICAL PRICE with bands at +/- mult * ATR.

        middle = MA((h+l+c)/3, n)
        upper  = middle + mult * ATR(atr_n)
        lower  = middle - mult * ATR(atr_n)

    TA-Lib has no Keltner, so this is a pandas replication of OUR definition
    -- but not an independent one all the way down: the ATR half-width is
    `_atr_series`, the same reference TA-Lib is asserted against above, and
    the typical price is `vwap`'s. What the case adds over those is the
    ASSEMBLY (which MA, over which price, times which multiplier) and the
    PER-COLUMN warm-up.

    The two warm-ups differ and the assert says so: the centre lands on the
    MA's own first bar and the bands on max(centre, ATR) -- the `macd`
    per-column rule, NOT all three masked back to the slower.

    The original 1960 Keltner (10-bar SMA of typical price, +/- 1x the SMA of
    the PLAIN high-low range) is a documented delta, not an option: the
    half-width here is always true range.
    """
    tp = (h + low_s + s) / 3
    mid = _ma_over(tp, kind, n)
    a = _atr_series(atr_n)
    upper, lower = mid + mult * a, mid - mult * a

    label = f"keltner({n},{atr_n},{mult},{kind})"
    # The analytic first valid bars, asserted rather than assumed: the sma /
    # ema / wma / smma / trima family lands at n-1 over a gap-free typical
    # price, and ATR at atr_n (true range costs bar 0).
    expected_mid = {"sma": n - 1, "ema": n - 1, "wma": n - 1}.get(kind)
    assert expected_mid is not None, f"{label}: no analytic warm-up for {kind}"
    assert mid.first_valid_index() == expected_mid, (
        f"{label} centre first valid at {mid.first_valid_index()}, expected "
        f"{expected_mid}"
    )
    assert a.first_valid_index() == atr_n, (
        f"{label} ATR first valid at {a.first_valid_index()}, expected {atr_n}"
    )
    assert upper.first_valid_index() == max(expected_mid, atr_n), (
        f"{label} bands first valid at {upper.first_valid_index()}, expected "
        f"max(centre, ATR) = {max(expected_mid, atr_n)} -- the per-column rule"
    )
    # The channel must be WIDE enough on this fixture that a dropped
    # multiplier (mult = 1) would be visible rather than a rounding away.
    width = float((upper - lower).dropna().min())
    assert width > 0.5, (
        f"{label} narrowest channel is {width} -- too tight for the fixture to "
        "catch a dropped multiplier"
    )
    print(
        f"  {label}: pandas replication on the TA-Lib-checked ATR; centre at "
        f"{expected_mid}, bands at {max(expected_mid, atr_n)}, narrowest "
        f"channel {width:.3f}"
    )
    return {
        "kcMiddle": col(mid),
        "kcUpper": col(upper),
        "kcLower": col(lower),
    }


def atr_bands(n: int, mult: float) -> dict:
    """ATR Bands: close +/- mult * ATR(n). TWO columns -- the middle is the
    field itself, which is already on the series.

    pandas replication on `_atr_series` (the reference TA-Lib is asserted
    against). The assert that earns its keep here is the IDENTITY: the upper
    band less the close must be exactly mult times that same ATR, which is
    what makes `atrBands` provably `atr()` plus arithmetic rather than a
    second ATR that happens to agree.
    """
    a = _atr_series(n)
    upper, lower = s + mult * a, s - mult * a

    label = f"atrBands({n},{mult})"
    assert upper.first_valid_index() == n, (
        f"{label} first valid at {upper.first_valid_index()}, expected {n}"
    )
    # The half-width IS mult * ATR. Read back by subtraction it is that to
    # within a double's last bits rather than exactly -- (c + w) - c is not w
    # in IEEE754 -- so the bit-exact form of this claim is asserted on the
    # TypeScript side, where `atrbUpper === close + mult * atr()` can be
    # checked as the same expression rather than as its difference.
    identity = float(np.nanmax(np.abs((upper - s) - mult * a)))
    assert identity < 1e-12, f"{label}: upper - close != mult * atr ({identity})"
    symmetry = float(np.nanmax(np.abs((upper - lower) - 2 * mult * a)))
    assert symmetry < 1e-12, (
        f"{label}: the two bands are not symmetric about the close ({symmetry})"
    )
    print(
        f"  {label}: pandas replication; upper - close == {mult} * atr to "
        f"{identity:.3g}, bands symmetric to {symmetry:.3g}"
    )
    return {"atrbUpper": col(upper), "atrbLower": col(lower)}


def qstick(n: int, kind: str) -> dict:
    """QStick (Tushar Chande): MA of the candle body, close - open.

    No TA-Lib function, so a pandas replication with the analytic first valid
    bar asserted. The fixture's bodies change sign 30/50 and are all
    distinct (asserted where `opens` is built), so a dropped sign or a
    constant-body bug cannot pass.
    """
    body = s - o_s
    v = _ma_over(body, kind, n)

    label = f"qstick({n},{kind})"
    expected = {"sma": n - 1, "ema": n - 1, "wma": n - 1}.get(kind)
    assert expected is not None, f"{label}: no analytic warm-up for {kind}"
    assert v.first_valid_index() == expected, (
        f"{label} first valid at {v.first_valid_index()}, expected {expected}"
    )
    # It must cross zero on this fixture, or the study's whole reading (the
    # zero line) is untested by the case.
    assert (v.dropna() > 0).any() and (v.dropna() < 0).any(), (
        f"{label} never crosses zero on this fixture"
    )
    print(
        f"  {label}: pandas replication (TA-Lib has no QSTICK); first valid at "
        f"{expected}, crosses zero"
    )
    return {"qstick": col(v)}


def trix(n: int, sig: int) -> dict:
    """TRIX: the 1-bar PERCENT rate of change of a triple-smoothed EMA, x100,
    plus a signal EMA of it.

        T      = EMA(EMA(EMA(close, n), n), n)
        trix   = 100 * (T[i] / T[i-1] - 1)
        signal = EMA(trix, sig)

    TA-Lib HAS this one (`TRIX`), but on ITS EMA seed (SMA of the first n)
    rather than pond's (first sample). So the check is the `moving_average`
    EMA-family pattern, in two parts:

      (a) THE FORMULA, exactly -- rebuild TRIX on TA-Lib's own SMA seed
          (`_ema_sma_seed` three times, then the percent ROC) and require
          bit-level agreement. This is what catches a dropped stage, a `tema`
          substituted for the EMA chain, or a LOG rate of change in place of
          the percent one; all three are within the tail bound below but none
          survives here.
      (b) THE SEED, bounded -- pond's transient must have decayed over the
          last 20 shared bars.

    The signal line has NO vendor reference (TA-Lib's TRIX returns the line
    alone), so it is a pandas replication with its analytic warm-up asserted.
    """
    t3 = _ema_first_seed(_ema_first_seed(_ema_first_seed(s, n), n), n)
    line = (t3 / t3.shift(1) - 1) * 100
    line[t3.shift(1) == 0] = math.nan
    signal = _ema_first_seed(line, sig)

    label = f"trix({n},{sig})"
    assert line.first_valid_index() == 3 * n - 2, (
        f"{label} first valid at {line.first_valid_index()}, expected "
        f"{3 * n - 2} (three EMAs then a 1-bar rate of change)"
    )
    assert signal.first_valid_index() == 3 * n - 2 + sig - 1, (
        f"{label} signal first valid at {signal.first_valid_index()}, expected "
        f"{3 * n - 2 + sig - 1}"
    )

    if talib is not None:
        ref = pd.Series(talib.TRIX(np.asarray(closes, dtype=float), timeperiod=n))
        assert list(line.isna()) == list(ref.isna()), (
            f"{label} warm-up differs from TA-Lib TRIX: ours first valid "
            f"{line.first_valid_index()}, TA-Lib {ref.first_valid_index()}"
        )
        e1 = _ema_sma_seed(closes, n)
        e2 = _ema_sma_seed(e1, n)
        e3 = _ema_sma_seed(e2, n)
        formula = np.full(len(e3), np.nan)
        formula[1:] = (e3[1:] / e3[:-1] - 1) * 100
        refa = np.asarray(ref, dtype=float)
        assert (np.isnan(formula) == np.isnan(refa)).all(), (
            f"{label}: SMA-seeded replication's warm-up differs from TA-Lib"
        )
        fm = ~np.isnan(refa)
        fd = float(np.max(np.abs(formula[fm] - refa[fm])))
        assert fd < 1e-9, (
            f"{label}: SMA-seeded replication disagrees with TA-Lib by {fd} - "
            "the formula, not the seed, is wrong"
        )
        scale = float(np.nanmax(np.abs(refa)))
        both = (~np.isnan(refa)) & (~np.asarray(line.isna()))
        d = np.abs(np.asarray(line, dtype=float)[both] - refa[both])
        worst, tail = float(d.max()), float(d[-20:].max())
        # 2% of scale, not the MA family's 0.5%, and the looser bound is
        # measured rather than fitted after the fact. TRIX divides a triple
        # EMA by its own predecessor, so `scale` here is the size of a percent
        # rate of change (0.45 at n=15) rather than of a price: a seed
        # difference that is a rounding error on the price is a visible
        # fraction of THIS number, and with only 37 shared bars at n=15 it has
        # 20 bars to decay in, not 60. Measured, tail over the last 20 bars as
        # a fraction of scale:
        #
        #                        n=15      n=5
        #     correct 2/(n+1)   0.847%   0.000%   <- passes
        #     wrong   2/(n+2)   8.540%  17.254%
        #     wrong   2/n       9.807%  20.939%
        #
        # so 2% sits ~2.4x clear of the correct value and ~4x below the
        # nearest wrong one. It is also the WEAKER of the two checks here:
        # part (a) above pins the formula, alpha included, bit-exactly on
        # TA-Lib's own seed, so every one of those wrong rates is already
        # dead before this line runs.
        assert tail / scale < 0.02, (
            f"{label} is {tail / scale:.3%} from TA-Lib over the last 20 shared "
            "bars - too far to be the seed transient"
        )
        print(
            f"  {label}: formula matches TA-Lib on its SMA seed to {fd:.2g}; "
            f"pond seed vs TA-Lib's - {worst / scale:.3%} at the first shared "
            f"bar, {tail / scale:.4%} worst over the last 20 (masks identical)"
        )
    else:
        print(f"  {label}: pandas only - TA-Lib not installed, cross-check SKIPPED")

    return {"trix": col(line), "trixSignal": col(signal)}


def coppock(long_n: int, short_n: int, wma_n: int) -> dict:
    """Coppock Curve: WMA(ROC(long) + ROC(short)), ROC in PERCENT.

    No TA-Lib function. A pandas replication built on the same `pct_change`
    the TA-Lib-verified `percentChange` case uses and the same `_wma` the
    TA-Lib-verified `MA(matype=2)` case uses, so the novel part -- the
    assembly and its composed warm-up -- is what the case pins. The WMA needs
    `wma_n` finite values (a positional weight cannot skip a cell), so the
    first value lands at max(long, short) + wma_n - 1.
    """
    total = s.pct_change(long_n) * 100 + s.pct_change(short_n) * 100
    v = _wma(total, wma_n)

    label = f"coppock({long_n},{short_n},{wma_n})"
    expected = max(long_n, short_n) + wma_n - 1
    assert v.first_valid_index() == expected, (
        f"{label} first valid at {v.first_valid_index()}, expected {expected}"
    )
    assert (v.dropna() > 0).any() and (v.dropna() < 0).any(), (
        f"{label} never crosses zero on this fixture - the only reading the "
        "curve has would be untested"
    )
    print(
        f"  {label}: pandas replication (TA-Lib has no Coppock); first valid at "
        f"{expected}, crosses zero"
    )
    return {"coppock": col(v)}


# --------------------------------------------------------------------------
# The K2 consumers: price-vs-moving-average oscillators.
#
# All five are assemblies over the moving-average engine above (or, for
# elderRay, over pond's EMA directly), so `_ma_values` is reused rather than
# a second smoother written here -- the same reason the engine exists.
#
# TA-Lib has APO/PPO (the price oscillator) and nothing for the other four,
# so those four are pandas replications of OUR definition with the analytic
# first-valid bar asserted, plus a DISCRIMINATING assert each: a fixture that
# cannot tell the study from the obvious wrong implementation pins nothing.
# --------------------------------------------------------------------------


def price_oscillator(fast: int, slow: int, kind: str, mode: str) -> dict:
    """Price Oscillator: MA(fast) - MA(slow), absolute or as a percent of the
    slow MA. TA-Lib's APO and PPO respectively, with matype.

    The seed question is MACD's, and is split the same way: for the EMA
    family the FORMULA is rebuilt on TA-Lib's own SMA seed and required to
    match exactly, and the pond-seed transient is bounded separately over the
    last 20 shared bars. For every other type there is no seed and equality
    is required outright.

    The 0.5% tail bound discriminates. Measured on (12,26,ema,percent), worst
    over the last 20 shared bars as a fraction of scale:

        correct 2/(n+1)    0.414%   <- passes
        wrong   2/(n+2)    5.094%
        wrong   2/n        5.990%
        wrong   1/n       44.405%

    so the correct rate sits just inside the bound and the nearest wrong one
    is 12x outside it. (The margin is tighter than MACD's because this is the
    worst of the last 20 bars, not the last bar alone -- 0.089% there.)
    """
    f = _ma_values(kind, fast)
    sl = _ma_values(kind, slow)
    v = (f - sl) if mode == "absolute" else 100 * (f - sl) / sl
    label = f"priceOscillator({fast},{slow},{kind},{mode})"

    # The two modes must not be within rounding of each other on this fixture,
    # or a study ignoring `mode` would pass both cases.
    other = (100 * (f - sl) / sl) if mode == "absolute" else (f - sl)
    sep = float(np.nanmax(np.abs(v - other)))
    assert sep > 0.1, (
        f"{label}: the two modes agree to within {sep} on this fixture - a "
        "study that ignored `mode` would pass"
    )

    if talib is None:
        print(f"  {label}: pandas only - TA-Lib not installed, cross-check SKIPPED")
        return {"priceOsc": col(v)}

    matype = _MA_TALIB[kind]
    fn = talib.APO if mode == "absolute" else talib.PPO
    ref = pd.Series(
        fn(
            np.asarray(closes, dtype=float),
            fastperiod=fast,
            slowperiod=slow,
            matype=matype,
        )
    )
    # Mask first, always (see rsi): nanmax(|a-b|) is blind to a one-sided NaN.
    assert list(v.isna()) == list(ref.isna()), (
        f"{label} warm-up differs from TA-Lib: ours first valid "
        f"{v.first_valid_index()}, TA-Lib {ref.first_valid_index()}"
    )
    both = (~np.asarray(v.isna())) & (~np.asarray(ref.isna()))
    d = np.abs(np.asarray(v, dtype=float)[both] - np.asarray(ref, dtype=float)[both])

    if kind in _MA_SEED_DELTA:
        # (a) THE FORMULA, exactly, on TA-Lib's SMA-seeded EMA.
        fe = _ema_sma_seed(closes, fast)
        se = _ema_sma_seed(closes, slow)
        if kind != "ema":
            raise AssertionError(
                f"{label}: the SMA-seeded formula proof is written for 'ema' "
                "only; add the dema/tema stages before using them here"
            )
        formula = (fe - se) if mode == "absolute" else 100 * (fe - se) / se
        refa = np.asarray(ref, dtype=float)
        assert (np.isnan(formula) == np.isnan(refa)).all(), (
            f"{label}: SMA-seeded replication's warm-up differs from TA-Lib"
        )
        fm = ~np.isnan(refa)
        fd = float(np.max(np.abs(formula[fm] - refa[fm])))
        assert fd < 1e-9, (
            f"{label}: SMA-seeded replication disagrees with TA-Lib by {fd} - "
            "the formula, not the seed, is wrong"
        )
        # (b) THE SEED, bounded over the last 20 shared bars.
        scale = float(np.nanmax(np.abs(ref)))
        worst, tail = float(d.max()), float(d[-20:].max())
        assert tail / scale < 0.005, (
            f"{label} is {tail / scale:.3%} from TA-Lib over the last 20 shared "
            "bars - too far to be the seed transient"
        )
        print(
            f"  {label}: formula matches TA-Lib on its SMA seed to {fd:.2g}; "
            f"pond seed - {worst / scale:.2%} at the first shared bar, "
            f"{tail / scale:.4%} worst over the last 20 (masks identical)"
        )
    else:
        assert float(d.max()) < 1e-9, (
            f"{label} disagrees with TA-Lib by {d.max()}"
        )
        print(
            f"  {label}: matches TA-Lib "
            f"{'APO' if mode == 'absolute' else 'PPO'}(matype={matype}) to "
            f"{d.max():.3g} (warm-ups identical)"
        )

    return {"priceOsc": col(v)}


def disparity_index(n: int, kind: str) -> dict:
    """Disparity Index: 100 * (close - MA) / MA. No TA-Lib function exists,
    so this is a pandas replication of our definition with the analytic first
    valid bar asserted."""
    ma = _ma_values(kind, n)
    v = 100 * (s - ma) / ma
    label = f"disparityIndex({n},{kind})"

    expected = n - 1  # sma / ema: the MA's own first valid bar
    assert v.first_valid_index() == expected, (
        f"{label} first valid at {v.first_valid_index()}, expected {expected} "
        "-- the fixture would pin the wrong warm-up"
    )
    # Dividing by the PRICE instead of the MA is the obvious wrong turn and is
    # numerically close; assert the fixture separates them.
    wrong = 100 * (s - ma) / s
    sep = float(np.nanmax(np.abs(v - wrong)))
    assert sep > 0.01, (
        f"{label} sits within {sep} of the divide-by-price version - the "
        "fixture cannot tell the denominator apart"
    )
    print(
        f"  {label}: pandas replication (TA-Lib has no Disparity Index); first "
        f"valid at {expected}, {sep:.3f} from the divide-by-price version"
    )
    return {"disparity": col(v)}


def dpo(n: int, kind: str) -> dict:
    """Detrended Price Oscillator: close - MA displaced floor(n/2)+1 bars back
    (TradingView's non-centered form; see studies/detrended-price-oscillator.ts
    for why that alignment and not StockCharts' centered one).

    No TA-Lib function; pandas replication with the analytic first valid bar
    (the MA's own, plus the displacement) asserted."""
    shift = n // 2 + 1
    ma = _ma_values(kind, n)
    v = s - ma.shift(shift)
    label = f"detrendedPriceOscillator({n},{kind})"

    expected = n - 1 + shift
    assert v.first_valid_index() == expected, (
        f"{label} first valid at {v.first_valid_index()}, expected {expected} "
        f"(MA's {n - 1} plus a {shift}-bar displacement)"
    )
    # Forgetting the displacement is THE bug this study can have; the fixture
    # has to separate the two by more than rounding.
    sep = float(np.nanmax(np.abs(v - (s - ma))))
    assert sep > 0.5, (
        f"{label} sits within {sep} of the undisplaced `close - MA` - the "
        "fixture cannot tell the displacement apart"
    )
    print(
        f"  {label}: pandas replication (TA-Lib has no DPO); shift {shift}, "
        f"first valid at {expected}, {sep:.3f} from the undisplaced form"
    )
    return {"dpo": col(v)}


def elder_ray(n: int) -> dict:
    """Elder Ray: bull = high - EMA(close, n), bear = low - EMA(close, n), on
    POND's EMA seed (first sample), which is what elderRay's kernel gives.

    No TA-Lib function; pandas replication with the EMA's first valid bar
    asserted."""
    e = s.ewm(span=n, adjust=False).mean()
    e.iloc[: n - 1] = math.nan  # our length-preserving warm-up
    bull, bear = h - e, low_s - e
    label = f"elderRay({n})"

    assert bull.first_valid_index() == n - 1 and bear.first_valid_index() == n - 1, (
        f"{label} first valid at {bull.first_valid_index()}/"
        f"{bear.first_valid_index()}, expected {n - 1} for both"
    )
    # The two legs must be far apart (a study emitting the same column twice
    # would otherwise pass) and neither may equal the close-based version.
    legs = float(np.nanmin(bull - bear))
    close_leg = float(np.nanmax(np.abs(bull - (s - e))))
    assert legs > 0.5, f"{label}: bull and bear are only {legs} apart at the closest"
    assert close_leg > 0.3, (
        f"{label}: bull power is within {close_leg} of the close-based version "
        "- the fixture cannot tell `high` from `close`"
    )
    print(
        f"  {label}: pandas replication (TA-Lib has no Elder Ray) on pond's EMA "
        f"seed; first valid at {n - 1}, legs >= {legs:.3f} apart"
    )
    return {"elderBull": col(bull), "elderBear": col(bear)}


def awesome_oscillator(fast: int, slow: int) -> dict:
    """Awesome Oscillator: SMA(fast) - SMA(slow) of the median price
    (high + low)/2. No TA-Lib function; pandas replication.

    pandas' rolling needs `fast`/`slow` non-NaN observations, which is exactly
    `rollingMeanValues`' "wait for that many finite VALUES" rule for a derived
    array."""
    med = (h + low_s) / 2
    v = med.rolling(fast).mean() - med.rolling(slow).mean()
    label = f"awesomeOscillator({fast},{slow})"

    assert v.first_valid_index() == slow - 1, (
        f"{label} first valid at {v.first_valid_index()}, expected {slow - 1}"
    )
    # Reading the CLOSE instead of the median price is the obvious wrong turn.
    wrong = s.rolling(fast).mean() - s.rolling(slow).mean()
    sep = float(np.nanmax(np.abs(v - wrong)))
    assert sep > 0.02, (
        f"{label} sits within {sep} of the close-based version - the fixture "
        "cannot tell the median price apart"
    )
    print(
        f"  {label}: pandas replication (TA-Lib has no AO); first valid at "
        f"{slow - 1}, {sep:.4f} from the close-based version"
    )
    return {"ao": col(v)}


def _clv() -> pd.Series:
    """Close Location Value: ((c - l) - (h - c)) / (h - l), in [-1, +1].

    The per-bar term of the A/D line and of Chaikin Money Flow. The fixture's
    bars always have a range, so the h == l case (which pond reports as
    missing and TA-Lib's AD folds in as a zero contribution) does not arise
    here - it is unit-tested TypeScript-side instead.
    """
    return ((s - low_s) - (h - s)) / (h - low_s)


def _ad_line() -> pd.Series:
    return (_clv() * vol).cumsum()


def accumulation_distribution() -> dict:
    """Accumulation/Distribution line: cumsum(CLV * volume), as TA-Lib's AD.

    No period and no warm-up - the term reads one bar, so the line is defined
    from bar 0. Cross-checked against TA-Lib (mask, then values).
    """
    ad = _ad_line()

    if talib is not None:
        ref = pd.Series(
            talib.AD(
                np.asarray(highs, dtype=float),
                np.asarray(lows, dtype=float),
                np.asarray(closes, dtype=float),
                np.asarray(volumes, dtype=float),
            )
        )
        assert list(ad.isna()) == list(ref.isna()), (
            f"AD warm-up differs from TA-Lib: ours first valid "
            f"{ad.first_valid_index()}, TA-Lib {ref.first_valid_index()}"
        )
        delta = float(np.nanmax(np.abs(ad - ref)))
        assert delta < 1e-9, f"AD disagrees with TA-Lib by {delta}"
        print(f"  accumulationDistribution(): matches TA-Lib to {delta:.3g}")
    else:
        print("  accumulationDistribution(): TA-Lib not installed - SKIPPED")

    # A study that read the SIGN of the close change (OBV's term) rather than
    # the close's LOCATION in the bar would be a different line entirely.
    signed = (np.sign(s.diff()) * vol).cumsum()
    sep = float(np.nanmax(np.abs(ad - signed)))
    assert sep > 1000, f"A/D sits within {sep} of the OBV shape on this fixture"

    return {"ad": col(ad)}


def chaikin_oscillator(fast: int, slow: int) -> dict:
    """Chaikin Oscillator: EMA(AD, fast) - EMA(AD, slow), TA-Lib's ADOSC.

    THE SEED AGREES HERE, which is the exception in this file. Every other
    EMA-family case keeps pond's first-sample seed against TA-Lib's SMA seed
    and bounds the transient - but TA-Lib's own ADOSC seeds both EMAs with
    the FIRST A/D value, which is pond's convention, so this one is asserted
    as an exact match. The SMA-seeded reconstruction is computed below too,
    and asserted to be DIFFERENT, so the case pins which seed we ship.
    """
    ad = _ad_line()
    v = _ema_first_seed(ad, fast) - _ema_first_seed(ad, slow)
    label = f"chaikinOscillator({fast},{slow})"

    assert v.first_valid_index() == slow - 1, (
        f"{label} first valid at {v.first_valid_index()}, expected {slow - 1}"
    )

    if talib is not None:
        ref = pd.Series(
            talib.ADOSC(
                np.asarray(highs, dtype=float),
                np.asarray(lows, dtype=float),
                np.asarray(closes, dtype=float),
                np.asarray(volumes, dtype=float),
                fastperiod=fast,
                slowperiod=slow,
            )
        )
        assert list(v.isna()) == list(ref.isna()), (
            f"{label} warm-up differs from TA-Lib: ours first valid "
            f"{v.first_valid_index()}, TA-Lib {ref.first_valid_index()}"
        )
        delta = float(np.nanmax(np.abs(v - ref)))
        assert delta < 1e-9, f"{label} disagrees with TA-Lib ADOSC by {delta}"
        sma_seed = pd.Series(
            _ema_sma_seed(ad.to_numpy(), fast) - _ema_sma_seed(ad.to_numpy(), slow)
        )
        seed_gap = float(np.nanmax(np.abs(sma_seed - ref)))
        assert seed_gap > 1.0, (
            f"{label}: the SMA-seeded reconstruction is only {seed_gap} from "
            "TA-Lib - this fixture cannot tell the two seeds apart"
        )
        print(
            f"  {label}: matches TA-Lib ADOSC to {delta:.3g} on POND's "
            f"first-sample EMA seed (an SMA seed would be {seed_gap:.1f} out)"
        )
    else:
        print(f"  {label}: TA-Lib not installed - cross-check SKIPPED")

    return {"chaikinOsc": col(v)}


def price_volume_trend() -> dict:
    """Price-Volume Trend: cumsum(fractional close change * volume).

    No TA-Lib function. PVT[0] is NULL, not 0: the term needs a previous
    close, and pond does not invent a seed where no vendor convention forces
    one (OBV's volume[0] seed is TA-Lib's and is matched; this has none).
    Every later level is the same either way.
    """
    pvt = (s.pct_change() * vol).cumsum()

    assert pvt.first_valid_index() == 1, (
        f"PVT first valid at {pvt.first_valid_index()}, expected 1"
    )
    # The plausible wrong turn is the ABSOLUTE change (a "price times volume"
    # sum) rather than the fractional one.
    absolute = (s.diff() * vol).cumsum()
    sep = float(np.nanmax(np.abs(pvt - absolute)))
    assert sep > 100, (
        f"PVT sits within {sep} of the absolute-change version - the fixture "
        "cannot tell the fractional form apart"
    )
    print(
        f"  priceVolumeTrend(): pandas replication (no TA-Lib PVT); first "
        f"valid at 1, {sep:.0f} from the absolute-change version"
    )
    return {"pvt": col(pvt)}


def chaikin_money_flow(n: int) -> dict:
    """Chaikin Money Flow: sum(CLV * volume) / sum(volume) over n bars.

    No TA-Lib function - a pandas replication of the same weighted-mean shape
    VWAP uses, with the volume weighting asserted to be visible on this
    fixture (the spike bars are what make it so).
    """
    clv = _clv()
    v = (clv * vol).rolling(n).sum() / vol.rolling(n).sum()
    label = f"chaikinMoneyFlow({n})"

    assert v.first_valid_index() == n - 1, (
        f"{label} first valid at {v.first_valid_index()}, expected {n - 1}"
    )
    assert float(np.nanmax(np.abs(v))) <= 1.0, f"{label} left [-1, +1]"
    plain = clv.rolling(n).mean()
    sep = float(np.nanmax(np.abs(v - plain)))
    assert sep > 0.02, (
        f"{label} sits within {sep} of the UNWEIGHTED mean of CLV - the "
        "fixture's volume is too flat to catch a dropped weighting"
    )
    print(
        f"  {label}: pandas replication (no TA-Lib CMF); first valid at "
        f"{n - 1}, {sep:.3f} from the unweighted mean of CLV"
    )
    return {"cmf": col(v)}


def money_flow_index(n: int) -> dict:
    """Money Flow Index: 100 * posFlow / (posFlow + negFlow) over n bars,
    flow = typical price * volume, direction from the typical price.

    Cross-checked against TA-Lib MFI (mask, then values). An unchanged
    typical price contributes to neither sum - TA-Lib's rule too. TA-Lib
    reports 0 where the window's total flow is zero (or merely below 1.0);
    pond reports NULL for a zero total, the rsi flat-window rule. That case
    does not arise on this fixture, so the values agree outright.
    """
    tp = (h + low_s + s) / 3
    flow = tp * vol
    d = tp.diff()
    pos = flow.where(d > 0, 0.0)
    neg = flow.where(d < 0, 0.0)
    pos.iloc[0] = math.nan  # no previous typical price
    neg.iloc[0] = math.nan
    up = pos.rolling(n).sum()
    down = neg.rolling(n).sum()
    v = 100 * up / (up + down)
    label = f"moneyFlowIndex({n})"

    assert v.first_valid_index() == n, (
        f"{label} first valid at {v.first_valid_index()}, expected {n} "
        "(the first bar has no previous typical price)"
    )

    if talib is not None:
        ref = pd.Series(
            talib.MFI(
                np.asarray(highs, dtype=float),
                np.asarray(lows, dtype=float),
                np.asarray(closes, dtype=float),
                np.asarray(volumes, dtype=float),
                timeperiod=n,
            )
        )
        assert list(v.isna()) == list(ref.isna()), (
            f"{label} warm-up differs from TA-Lib: ours first valid "
            f"{v.first_valid_index()}, TA-Lib {ref.first_valid_index()}"
        )
        delta = float(np.nanmax(np.abs(v - ref)))
        assert delta < 1e-9, f"{label} disagrees with TA-Lib MFI by {delta}"
        print(f"  {label}: matches TA-Lib MFI to {delta:.3g}")
    else:
        print(f"  {label}: TA-Lib not installed - cross-check SKIPPED")

    # An MFI that weighted by volume alone (dropping the typical price from
    # the flow) is the same shape with different numbers; separate it.
    alt_up = vol.where(d > 0, 0.0)
    alt_up.iloc[0] = math.nan
    alt_down = vol.where(d < 0, 0.0)
    alt_down.iloc[0] = math.nan
    alt = 100 * alt_up.rolling(n).sum() / (
        alt_up.rolling(n).sum() + alt_down.rolling(n).sum()
    )
    sep = float(np.nanmax(np.abs(v - alt)))
    assert sep > 0.05, f"{label} is within {sep} of the volume-only version"

    return {"mfi": col(v)}


def force_index(n: int) -> dict:
    """Elder's Force Index: EMA(close.diff() * volume, n) on POND's EMA seed.

    No TA-Lib function. First valid at n (not n-1): the raw force has no
    value on bar 0, and the EMA's array door waits for n FINITE values.
    """
    raw = s.diff() * vol
    v = _ema_first_seed(raw, n)
    label = f"forceIndex({n})"

    assert v.first_valid_index() == n, (
        f"{label} first valid at {v.first_valid_index()}, expected {n}"
    )
    # Dropping the volume factor is the mutation this separation catches.
    wrong = _ema_first_seed(s.diff(), n)
    sep = float(np.nanmax(np.abs(v - wrong)))
    assert sep > 100, f"{label} is within {sep} of the volume-less version"
    print(
        f"  {label}: pandas replication (no TA-Lib force index) on pond's EMA "
        f"seed; first valid at {n}"
    )
    return {"force": col(v)}


def ease_of_movement(n: int, kind: str, scale: float = 100_000_000.0) -> dict:
    """Arms' Ease of Movement: MA(distance / boxRatio, n), where
    distance = mid.diff(), boxRatio = (volume / scale) / (high - low), so the
    1-bar value is mid.diff() * (high - low) * scale / volume.

    No TA-Lib function. `scale` is StockCharts'/ChartIQ's 100,000,000 and is
    a pure linear multiplier. First valid at n (the 1-bar value needs a
    previous midpoint, and every MA type's array door waits for n finite
    values).
    """
    mid = (h + low_s) / 2
    raw = mid.diff() * (h - low_s) * scale / vol
    v = _ma_over(raw, kind, n)
    label = f"easeOfMovement({n},{kind})"

    assert v.first_valid_index() == n, (
        f"{label} first valid at {v.first_valid_index()}, expected {n}"
    )
    # Dropping the (high - low) factor from the box ratio is the mutation
    # this separation catches - it is the half of the definition that makes
    # EOM quadratic in price rather than linear.
    wrong = _ma_over(mid.diff() * scale / vol, kind, n)
    sep = float(np.nanmax(np.abs(v - wrong)))
    assert sep > 100, f"{label} is within {sep} of the range-less version"
    print(
        f"  {label}: pandas replication (no TA-Lib EOM); first valid at {n}, "
        f"{sep:.0f} from the version that drops the bar's range"
    )
    return {"eom": col(v)}


def volume_oscillator(fast: int, slow: int, kind: str) -> dict:
    """Volume Oscillator: 100 * (MA(volume, fast) - MA(volume, slow)) /
    MA(volume, slow) - the price oscillator's percent mode over VOLUME.

    No TA-Lib function. What this case pins that priceOscillator's own cases
    cannot is that the study reads the VOLUME column and applies the 5/10/sma
    defaults, so it is deliberately built from the same helpers.
    """
    fast_ma = _ma_over(vol, kind, fast)
    slow_ma = _ma_over(vol, kind, slow)
    v = 100 * (fast_ma - slow_ma) / slow_ma
    label = f"volumeOscillator({fast},{slow},{kind})"

    expected_first = slow - 1
    assert v.first_valid_index() == expected_first, (
        f"{label} first valid at {v.first_valid_index()}, expected "
        f"{expected_first}"
    )
    # Reading the CLOSE instead of the volume is the obvious wrong turn.
    wrong = 100 * (_ma_over(s, kind, fast) - _ma_over(s, kind, slow)) / _ma_over(
        s, kind, slow
    )
    sep = float(np.nanmax(np.abs(v - wrong)))
    assert sep > 5, f"{label} is within {sep} of the close-based version"
    print(
        f"  {label}: pandas replication (no TA-Lib volume oscillator); first "
        f"valid at {expected_first}, {sep:.1f} from the close-based version"
    )
    return {"volOsc": col(v)}


# --------------------------------------------------------------------------
# The momentum tail (assessment 6.3): CMO, Ultimate, CCI, IMI, RVI, PSY.
#
# Two of the six have a TA-Lib function and are asserted bar-for-bar against
# it (ULTOSC, CCI). One has a TA-Lib function we deliberately do NOT match
# (CMO -- TA-Lib's is Wilder-smoothed and is exactly 2*RSI-100, which this
# package already ships as `rsi`), so the generator asserts BOTH facts: that
# our Chande form is what we say it is, and that TA-Lib's is the rsi
# restatement, with the delta between them measured rather than asserted
# away. The remaining three (IMI, RVI, PSY) have no TA-Lib function and are
# pandas replications with the analytic first-valid bar asserted.
# --------------------------------------------------------------------------


def _up_down(delta: pd.Series):
    """The two non-negative legs of a change -- `upDownLegValues`."""
    return delta.clip(lower=0), (-delta).clip(lower=0)


def chande_momentum(n: int) -> dict:
    """Chande Momentum Oscillator, CHANDE's UNSMOOTHED window sums:

        100 * (sum(up) - sum(down)) / (sum(up) + sum(down))

    NOT TA-Lib's CMO, which Wilder-smooths the two legs and is therefore
    exactly 2*RSI-100 -- an affine restatement of a study we already ship.
    Both facts are asserted below, and the delta between the two definitions
    is measured (it is large: this is a different indicator, not a warm-up
    transient).
    """
    up, dn = _up_down(s.diff())
    su, sd = up.rolling(n).sum(), dn.rolling(n).sum()
    v = 100 * (su - sd) / (su + sd)
    label = f"chandeMomentum({n})"

    assert v.first_valid_index() == n, (
        f"{label} first valid at {v.first_valid_index()}, expected {n} "
        "(a period-bar sum of DIFFERENCES needs period+1 bars)"
    )
    # The band is exact in real arithmetic; an all-up window divides a sum by
    # itself and can land 1.4e-14 over, so the tolerance is float noise only.
    assert float(np.nanmax(np.abs(v))) <= 100.0 + 1e-9, (
        f"{label} left the -100..100 band by {float(np.nanmax(np.abs(v))) - 100}"
    )

    if talib is not None:
        ref = pd.Series(talib.CMO(np.asarray(closes, dtype=float), timeperiod=n))
        rsi_ref = pd.Series(talib.RSI(np.asarray(closes, dtype=float), timeperiod=n))
        # (a) TA-Lib's CMO IS 2*RSI-100 -- the reason we do not ship it.
        affine = float(np.nanmax(np.abs(ref - (2 * rsi_ref - 100))))
        assert affine < 1e-9, (
            f"talib.CMO is not 2*RSI-100 (max {affine}) -- the premise of our "
            "definition choice; re-check before trusting the docstring"
        )
        # (b) The warm-ups agree, so what is left is a DEFINITION delta.
        assert list(v.isna()) == list(ref.isna()), (
            f"{label} warm-up differs from TA-Lib: ours first valid "
            f"{v.first_valid_index()}, TA-Lib {ref.first_valid_index()}"
        )
        delta = float(np.nanmax(np.abs(v - ref)))
        assert delta > 10, (
            f"{label} sits within {delta} of TA-Lib's Wilder-smoothed CMO -- "
            "the fixture cannot tell the two definitions apart"
        )
        print(
            f"  {label}: Chande's unsmoothed sums, first valid at {n}. "
            f"talib.CMO == 2*RSI-100 to {affine:.3g} (why we don't ship it); "
            f"ours differs from talib.CMO by up to {delta:.2f} points, "
            "warm-ups identical"
        )
    else:
        print(f"  {label}: pandas only - TA-Lib not installed, cross-check SKIPPED")

    return {"cmo": col(v)}


def ultimate_oscillator(short_n: int, med_n: int, long_n: int) -> dict:
    """Williams' Ultimate Oscillator, as TA-Lib's ULTOSC:

        BP  = close - min(low, prevClose)
        TR  = max(high, prevClose) - min(low, prevClose)     [= TRANGE]
        A_n = sum(BP, n) / sum(TR, n)
        uo  = 100 * (4*A_short + 2*A_med + A_long) / 7
    """
    prev = s.shift(1)
    bp = s - np.minimum(low_s, prev)
    tr = np.maximum(h, prev) - np.minimum(low_s, prev)
    label = f"ultimateOscillator({short_n},{med_n},{long_n})"

    if talib is not None:
        # Our TR kernel and TA-Lib's TRANGE must be the same array, so the
        # ATR family and this study measure range identically by
        # construction rather than by coincidence.
        trange = pd.Series(
            talib.TRANGE(
                np.asarray(highs, dtype=float),
                np.asarray(lows, dtype=float),
                np.asarray(closes, dtype=float),
            )
        )
        tr_delta = float(np.nanmax(np.abs(tr - trange)))
        assert tr_delta == 0.0, f"{label}: our TR is not TRANGE ({tr_delta})"

    def leg(n: int) -> pd.Series:
        return bp.rolling(n).sum() / tr.rolling(n).sum()

    v = 100 * (4 * leg(short_n) + 2 * leg(med_n) + leg(long_n)) / 7

    assert v.first_valid_index() == long_n, (
        f"{label} first valid at {v.first_valid_index()}, expected {long_n}"
    )
    assert -1e-9 <= float(np.nanmin(v)) and float(np.nanmax(v)) <= 100.0 + 1e-9, (
        f"{label} left the 0..100 band ({np.nanmin(v)}..{np.nanmax(v)})"
    )

    if talib is not None:
        ref = pd.Series(
            talib.ULTOSC(
                np.asarray(highs, dtype=float),
                np.asarray(lows, dtype=float),
                np.asarray(closes, dtype=float),
                timeperiod1=short_n,
                timeperiod2=med_n,
                timeperiod3=long_n,
            )
        )
        assert list(v.isna()) == list(ref.isna()), (
            f"{label} warm-up differs from TA-Lib: ours first valid "
            f"{v.first_valid_index()}, TA-Lib {ref.first_valid_index()}"
        )
        delta = float(np.nanmax(np.abs(v - ref)))
        assert delta < 1e-9, f"{label} disagrees with TA-Lib by {delta}"
        print(
            f"  {label}: matches TA-Lib to {delta:.3g} (warm-ups identical; "
            "our TR is TRANGE exactly)"
        )
    else:
        print(f"  {label}: pandas only - TA-Lib not installed, cross-check SKIPPED")

    return {"uo": col(v)}


def commodity_channel_index(n: int) -> dict:
    """CCI, as TA-Lib's CCI: (tp - SMA(tp)) / (0.015 * meanAbsDev(tp)),
    tp = (high + low + close) / 3.

    The mean absolute deviation is taken about the WINDOW's OWN mean (not a
    median, and not the standard deviation) -- the convention TA-Lib uses
    and the one `rollingMeanAbsDevValues` implements.
    """
    tp = (h + low_s + s) / 3
    mean = tp.rolling(n).mean()
    mad = tp.rolling(n).apply(lambda x: float(np.mean(np.abs(x - x.mean()))), raw=True)
    v = (tp - mean) / (0.015 * mad)
    label = f"commodityChannelIndex({n})"

    assert v.first_valid_index() == n - 1, (
        f"{label} first valid at {v.first_valid_index()}, expected {n - 1}"
    )
    # A z-score on the same window is the obvious wrong turn (stdev instead
    # of mean absolute deviation), and it must not be within rounding.
    z = (tp - mean) / (0.015 * tp.rolling(n).std(ddof=0))
    sep = float(np.nanmax(np.abs(v - z)))
    assert sep > 5, (
        f"{label} sits within {sep} of the stdev version - the fixture cannot "
        "tell mean absolute deviation from standard deviation"
    )

    if talib is not None:
        ref = pd.Series(
            talib.CCI(
                np.asarray(highs, dtype=float),
                np.asarray(lows, dtype=float),
                np.asarray(closes, dtype=float),
                timeperiod=n,
            )
        )
        assert list(v.isna()) == list(ref.isna()), (
            f"{label} warm-up differs from TA-Lib: ours first valid "
            f"{v.first_valid_index()}, TA-Lib {ref.first_valid_index()}"
        )
        delta = float(np.nanmax(np.abs(v - ref)))
        assert delta < 1e-9, f"{label} disagrees with TA-Lib by {delta}"
        print(
            f"  {label}: matches TA-Lib to {delta:.3g} (warm-ups identical); "
            f"{sep:.1f} from the stdev version at its widest"
        )
    else:
        print(f"  {label}: pandas only - TA-Lib not installed, cross-check SKIPPED")

    return {"cci": col(v)}


def intraday_momentum_index(n: int) -> dict:
    """Chande's Intraday Momentum Index: RSI's form over the candle BODY,
    with PLAIN window sums (not Wilder smoothing):

        100 * sum(max(close-open, 0)) / (sum(max(close-open,0)) + sum(max(open-close,0)))

    No TA-Lib function; pandas replication with the analytic first valid bar
    asserted. A body needs no previous bar, so the warm-up is n-1 (one row
    shorter than RSI's or CMO's on the same n).
    """
    gains, losses = _up_down(s - o_s)
    sg, sl = gains.rolling(n).sum(), losses.rolling(n).sum()
    v = 100 * sg / (sg + sl)
    label = f"intradayMomentumIndex({n})"

    assert v.first_valid_index() == n - 1, (
        f"{label} first valid at {v.first_valid_index()}, expected {n - 1}"
    )
    assert -1e-9 <= float(np.nanmin(v)) and float(np.nanmax(v)) <= 100.0 + 1e-9, (
        f"{label} left the 0..100 band ({np.nanmin(v)}..{np.nanmax(v)})"
    )
    # The same form over CLOSE-TO-CLOSE changes is the obvious wrong turn.
    cu, cd = _up_down(s.diff())
    wrong = 100 * cu.rolling(n).sum() / (cu.rolling(n).sum() + cd.rolling(n).sum())
    sep = float(np.nanmax(np.abs(v - wrong)))
    assert sep > 5, (
        f"{label} sits within {sep} of the close-to-close version - the "
        "fixture cannot tell the candle body apart"
    )
    print(
        f"  {label}: pandas replication (TA-Lib has no IMI), plain sums not "
        f"Wilder; first valid at {n - 1}, {sep:.1f} points from the "
        "close-to-close form"
    )
    return {"imi": col(v)}


def _swma(x: pd.Series) -> pd.Series:
    """Ehlers' symmetric 4-bar (1,2,2,1)/6 filter -- TradingView's `swma`."""
    return (x + 2 * x.shift(1) + 2 * x.shift(2) + x.shift(3)) / 6


def relative_vigor_index(n: int) -> dict:
    """Relative Vigor Index, TradingView's definition:

        num = swma(close - open); den = swma(high - low)
        rvi = sum(num, n) / sum(den, n); signal = swma(rvi)

    No TA-Lib function; pandas replication, both columns' analytic first
    valid bars asserted (n+2 and n+5 -- the SWMA costs 3 rows, the summation
    n-1 more, and the signal 3 more again).
    """
    num, den = _swma(s - o_s), _swma(h - low_s)
    v = num.rolling(n).sum() / den.rolling(n).sum()
    sig = _swma(v)
    label = f"relativeVigorIndex({n})"

    assert v.first_valid_index() == n + 2, (
        f"{label} first valid at {v.first_valid_index()}, expected {n + 2}"
    )
    assert sig.first_valid_index() == n + 5, (
        f"{label} signal first valid at {sig.first_valid_index()}, expected {n + 5}"
    )
    # The linear WMA(4) (weights 1,2,3,4) is the obvious wrong smoother.
    def _wma4(x: pd.Series) -> pd.Series:
        return (4 * x + 3 * x.shift(1) + 2 * x.shift(2) + x.shift(3)) / 10

    wrong = _wma4(s - o_s).rolling(n).sum() / _wma4(h - low_s).rolling(n).sum()
    sep = float(np.nanmax(np.abs(v - wrong)))
    assert sep > 0.01, (
        f"{label} sits within {sep} of the linear-WMA version - the fixture "
        "cannot tell the symmetric weights apart"
    )
    assert float(np.nanmax(np.abs(v - sig))) > 0.02, (
        f"{label}: the signal is within rounding of the line - a study "
        "emitting the same column twice would pass"
    )
    print(
        f"  {label}: pandas replication (TA-Lib has no RVI); first valid at "
        f"{n + 2} / {n + 5}, {sep:.4f} from the linear-WMA(4) version"
    )
    return {"rvi": col(v), "rviSignal": col(sig)}


def psychological_line(n: int) -> dict:
    """Percent of the last n bars that closed UP (strictly), 100*count/n.

    No TA-Lib function; pandas replication. Bar 0 has no direction, so the
    warm-up is n rows and not n-1. An unchanged close counts as NOT up (this
    fixture has none, which the assert below records).
    """
    d = s.diff()
    up = pd.Series(np.where(d.isna(), np.nan, (d > 0).astype(float)))
    v = 100 * up.rolling(n).mean()
    label = f"psychologicalLine({n})"

    assert v.first_valid_index() == n, (
        f"{label} first valid at {v.first_valid_index()}, expected {n} "
        "(bar 0 has no direction)"
    )
    assert int((d == 0).sum()) == 0, (
        "the oracle input has an unchanged close - psychologicalLine's "
        "strict-`>` rule would then need its own fixture note"
    )
    levels = sorted(set(round(float(x), 9) for x in v.dropna()))
    assert len(levels) > 3, (
        f"{label} takes only {len(levels)} distinct values on this fixture - "
        "too flat to catch an off-by-one in the count"
    )
    print(
        f"  {label}: pandas replication (TA-Lib has no PSY); first valid at "
        f"{n}, {len(levels)} distinct levels, range "
        f"{levels[0]:.2f}..{levels[-1]:.2f}"
    )
    return {"psy": col(v)}



# --------------------------------------------------------------------------
# The Wilder directional group (corpus 6.4): the Directional Movement System,
# Aroon and the Vortex Indicator.
#
# THE SEED, again. TA-Lib's ADX family accumulates +DM / -DM / TR over the
# first `n-1` bars and then takes one decayed step, so its first smoothed
# value is NOT the mean of the first n. Wilder's own worksheet sums the first
# n and then decays, which is `n x` the mean-form recursion `wilderValues`
# runs -- and is what TA-Lib's own ATR uses. So ours seeds Wilder's way (its
# DI denominator IS `atrValues`), and the proof splits in two, exactly as
# `moving_average` splits the EMA family: replay OUR pipeline on TA-LIB's
# seed and assert the FORMULA exactly, then bound the pond-seed transient.
# --------------------------------------------------------------------------


def _dms():
    """+DM / -DM (Wilder): the part of this bar's move that lies outside the
    previous bar's range, on whichever side moved further. At most one leg is
    non-zero; a tie (including an inside bar's 0 == 0) makes both zero. Bar 0
    is undefined -- no previous bar."""
    up = h.diff()
    dn = low_s.shift(1) - low_s
    p = pd.Series(np.where((up > dn) & (up > 0), up, 0.0))
    m = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0))
    p.iloc[0] = math.nan
    m.iloc[0] = math.nan
    return p, m


def _talib_dm_seed(x, n: int) -> pd.Series:
    """TA-Lib's ADX-family accumulator, in SUM form: total the first n-1
    values (bars 1..n-1), then one decayed step `S - S/n + x` for bar n, and
    the same step onward. First value at bar n."""
    v = np.asarray(x, dtype=float)
    out = np.full(len(v), np.nan)
    acc = float(np.sum(v[1:n]))
    for i in range(n, len(v)):
        acc = acc - acc / n + v[i]
        out[i] = acc
    return pd.Series(out)


def _dx_from(pdi: pd.Series, mdi: pd.Series) -> pd.Series:
    """100*|+DI - -DI| / (+DI + -DI), with a zero sum reading 0.

    Both legs are non-negative, so a zero sum forces a zero numerator -- the
    clvValues flat-BAR case, not the percentOfRangeValues flat-WINDOW one.
    TA-Lib agrees (its ADX seed loop adds nothing to sumDX there)."""
    total = pdi + mdi
    out = pd.Series(np.full(len(pdi), math.nan))
    zero = total == 0
    live = (total != 0) & total.notna()
    out[zero] = 0.0
    out[live] = 100 * (pdi[live] - mdi[live]).abs() / total[live]
    return out


def _dm_pipeline(n: int, seed: str):
    """The five columns, on either seeding convention. `seed='wilder'` is
    ours (mean of the first n, i.e. `wilderValues` / `atrValues`);
    `seed='talib'` replays TA-Lib's (n-1)-value accumulator."""
    p, m = _dms()
    tr = _true_range()
    if seed == "wilder":
        sp, sm, st = _wilder(p, n), _wilder(m, n), _atr_series(n)
    else:
        sp, sm, st = (_talib_dm_seed(x, n) for x in (p, m, tr))
    pdi, mdi = 100 * sp / st, 100 * sm / st
    dx = _dx_from(pdi, mdi)
    adx = _wilder(dx, n)  # BOTH conventions seed ADX on the mean of n DXs
    adxr = (adx + adx.shift(n - 1)) / 2
    return pdi, mdi, dx, adx, adxr


def directional_movement(n: int) -> dict:
    """Wilder's DMS: +DI / -DI / DX / ADX / ADXR, on Wilder's seeding."""
    pdi, mdi, dx, adx, adxr = _dm_pipeline(n, "wilder")
    label = f"directionalMovement({n})"

    # Warm-ups are analytic and must hold with or without TA-Lib installed.
    for name, series_, want in (
        ("dmiPlusDi", pdi, n),
        ("dmiMinusDi", mdi, n),
        ("dmiDx", dx, n),
        ("dmiAdx", adx, 2 * n - 1),
        ("dmiAdxr", adxr, 3 * n - 2),
    ):
        assert series_.first_valid_index() == want, (
            f"{label} {name} first valid at {series_.first_valid_index()}, "
            f"expected {want}"
        )

    if talib is not None:
        args = (
            np.asarray(highs, dtype=float),
            np.asarray(lows, dtype=float),
            np.asarray(closes, dtype=float),
        )
        refs = {
            "dmiPlusDi": pd.Series(talib.PLUS_DI(*args, timeperiod=n)),
            "dmiMinusDi": pd.Series(talib.MINUS_DI(*args, timeperiod=n)),
            "dmiDx": pd.Series(talib.DX(*args, timeperiod=n)),
            "dmiAdx": pd.Series(talib.ADX(*args, timeperiod=n)),
            "dmiAdxr": pd.Series(talib.ADXR(*args, timeperiod=n)),
        }
        ours = dict(zip(refs, (pdi, mdi, dx, adx, adxr)))

        # (1) MASKS FIRST -- nanmax is blind to a one-sided NaN. Ours must be
        # TA-Lib's exactly on all five: the seed moves values, never lookback.
        for name, ref in refs.items():
            assert list(ours[name].isna()) == list(ref.isna()), (
                f"{label} {name} warm-up differs from TA-Lib: ours first valid "
                f"{ours[name].first_valid_index()}, TA-Lib {ref.first_valid_index()}"
            )

        # (2) THE FORMULA, proved exactly on TA-Lib's own seed. If the DM
        # split, the true range, the DX guard, the ADX smooth or the ADXR
        # offset were wrong, this fails -- which is what separates "we chose a
        # different seed" from "we got the indicator wrong".
        replay = dict(zip(refs, _dm_pipeline(n, "talib")))
        worst = 0.0
        for name, ref in refs.items():
            assert list(replay[name].isna()) == list(ref.isna()), (
                f"{label} {name}: TA-Lib-seed replay mask differs from TA-Lib's"
            )
            d = float(np.nanmax(np.abs(replay[name] - ref)))
            assert d < 1e-9, f"{label} {name} on TA-Lib's seed disagrees by {d}"
            worst = max(worst, d)

        # (3) The pond-seed transient, bounded rather than asserted away.
        report = []
        for name, ref in refs.items():
            d = (ours[name] - ref).abs()
            first = int(ours[name].first_valid_index())
            d_first, d_last, d_max = float(d[first]), float(d.iloc[-1]), float(np.nanmax(d))
            assert d_last <= d_max / 10, (
                f"{label} {name}: the seed delta is not decaying (max {d_max}, "
                f"last {d_last}) -- that would be a wrong RATE, not a seed"
            )
            report.append(f"{name} {d_first:.4f}->{d_last:.2e} (max {d_max:.4f})")
        print(
            f"  {label}: formula exact on TA-Lib's seed ({worst:.3g}, all five "
            f"masks identical); pond-seed transient " + ", ".join(report)
        )

        # (4) ADXR's look-back is period-1 (TA-Lib's, and the package's own
        # bar-count reading). Pin the separation from the literal `period`
        # reading so the fixture cannot stop telling the two apart.
        literal = (adx + adx.shift(n)) / 2
        gap = float(np.nanmax(np.abs(literal - adxr)))
        assert gap > 1.0, (
            f"{label} ADXR: the two look-back readings differ by only {gap} on "
            "this fixture - it cannot tell them apart"
        )
        print(f"  {label}: ADXR shift n-1 vs the literal n reading differ by {gap:.2f} points")

    return {
        "dmiPlusDi": col(pdi),
        "dmiMinusDi": col(mdi),
        "dmiDx": col(dx),
        "dmiAdx": col(adx),
        "dmiAdxr": col(adxr),
    }


def _bars_since_extreme(values, n: int, mode: str) -> pd.Series:
    """Bars since the extreme of the (n+1)-bar window ending at i, ties to the
    MOST RECENT bar. The naive O(N*n) reference the deque kernel must match."""
    x = np.asarray(values, dtype=float)
    out = np.full(len(x), np.nan)
    for i in range(n, len(x)):
        w = x[i - n : i + 1]
        best = np.max(w) if mode == "max" else np.min(w)
        out[i] = n - int(np.max(np.flatnonzero(w == best)))
    return pd.Series(out)


def aroon(n: int) -> dict:
    """Aroon up / down / oscillator, as TA-Lib's AROON and AROONOSC.

    100*(n - barsSinceExtreme)/n over a window of n+1 BARS -- `n` counts the
    oldest AGE the study can report, and "n bars ago" is itself a reading, so
    the warm-up is n rows.
    """
    up = 100 * (n - _bars_since_extreme(highs, n, "max")) / n
    down = 100 * (n - _bars_since_extreme(lows, n, "min")) / n
    osc = up - down
    label = f"aroon({n})"

    assert up.first_valid_index() == n, (
        f"{label} first valid at {up.first_valid_index()}, expected {n} "
        "(the window is n+1 bars)"
    )

    if talib is not None:
        ref_down, ref_up = talib.AROON(
            np.asarray(highs, dtype=float), np.asarray(lows, dtype=float), timeperiod=n
        )
        ref_osc = talib.AROONOSC(
            np.asarray(highs, dtype=float), np.asarray(lows, dtype=float), timeperiod=n
        )
        worst = 0.0
        for name, ours, ref in (
            ("aroonUp", up, pd.Series(ref_up)),
            ("aroonDown", down, pd.Series(ref_down)),
            ("aroonOsc", osc, pd.Series(ref_osc)),
        ):
            assert list(ours.isna()) == list(ref.isna()), (
                f"{label} {name} warm-up differs from TA-Lib: ours first valid "
                f"{ours.first_valid_index()}, TA-Lib {ref.first_valid_index()}"
            )
            d = float(np.nanmax(np.abs(ours - ref)))
            assert d < 1e-9, f"{label} {name} disagrees with TA-Lib by {d}"
            worst = max(worst, d)
        print(f"  {label}: matches TA-Lib AROON/AROONOSC to {worst:.3g} (warm-ups identical)")

        # The TIE rule, measured rather than assumed: a repeated high in the
        # window. TA-Lib's comparisons are non-strict, so the NEWEST bar wins.
        tie_h = np.array([10, 12, 11, 12, 10.5, 10.2, 10.1], dtype=float)
        tie_l = np.array([9, 9.5, 9.2, 9.5, 9.8, 9.9, 9.7], dtype=float)
        _, tie_up = talib.AROON(tie_h, tie_l, timeperiod=4)
        newest = 100 * (4 - _bars_since_extreme(tie_h, 4, "max")) / 4
        assert list(np.round(tie_up[4:], 9)) == list(np.round(newest[4:], 9)), (
            f"TA-Lib's AROON tie rule is not most-recent-wins: {tie_up[4:]}"
        )
        assert tie_up[4] == 75.0, (
            f"the tie fixture stopped exercising a tie (aroonUp[4] = {tie_up[4]})"
        )
        print("  aroon: ties go to the MOST RECENT bar (TA-Lib gives 75, not 25, on a repeated high)")

    return {"aroonUp": col(up), "aroonDown": col(down), "aroonOsc": col(osc)}


def vortex(n: int) -> dict:
    """Vortex Indicator (Botes & Siepman 2010): +VI = sum|H-prevL| / sum TR,
    -VI = sum|L-prevH| / sum TR, over n bars.

    pandas replication -- TA-Lib has no vortex function. Both legs read the
    previous bar, as does TR, so the first valid bar is n.
    """
    vp = (h - low_s.shift(1)).abs()
    vm = (low_s - h.shift(1)).abs()
    tr = _true_range()
    total = tr.rolling(n).sum()
    plus = vp.rolling(n).sum() / total
    minus = vm.rolling(n).sum() / total
    label = f"vortex({n})"

    assert plus.first_valid_index() == n, (
        f"{label} first valid at {plus.first_valid_index()}, expected {n}"
    )
    assert bool((plus.dropna() > 0).all()) and bool((minus.dropna() > 0).all()), (
        f"{label} both legs must be strictly positive - they are sums of "
        "absolute distances over a positive range"
    )
    # Separate the true-range denominator from the plain bar range: a study
    # that dropped the prevClose terms would otherwise pass this fixture.
    plain = vp.rolling(n).sum() / (h - low_s).rolling(n).sum()
    sep = float(np.nanmax(np.abs(plain - plus)))
    assert sep > 0.05, (
        f"{label} the plain-range denominator differs by only {sep} on this "
        "fixture - it cannot tell true range from bar range"
    )
    print(
        f"  {label}: pandas replication (no TA-Lib vortex); first valid at {n}, "
        f"+VI in {plus.min():.4f}..{plus.max():.4f}; the plain-range "
        f"denominator would differ by up to {sep:.4f}"
    )
    return {"viPlus": col(plus), "viMinus": col(minus)}


# --------------------------------------------------------------------------
# The volatility tail (assessment 6.5): Chaikin Volatility, Mass Index,
# Choppiness, Ulcer, VHF, GAPO and the Relative VOLATILITY Index.
#
# NONE of the seven has a TA-Lib function, so every case here is a pandas
# replication of the definition the study documents, and each one asserts
# (a) the analytic first valid bar and (b) a measured separation from the
# plausible WRONG turn -- the substitution a reader of the formula could
# make that would leave the shape of the curve intact while changing every
# value. Without (b) a fixture can pin the wrong indicator perfectly.
# --------------------------------------------------------------------------


def _bar_range() -> pd.Series:
    """high - low, the PLAIN range (`barRangeValues`). Chaikin's and Dorsey's
    studies take this; the Wilder family takes `_true_range()`."""
    return h - low_s


def chaikin_volatility(n: int, roc: int) -> dict:
    """Chaikin Volatility: the percent rate of change, over `roc` bars, of an
    EMA of the plain bar range.

    Pond's first-sample EMA seed through the ARRAY door (`_ema_first_seed`),
    which is what makes the warm-up n-1 and not n-1 plus a seed window.
    """
    e = _ema_first_seed(_bar_range(), n)
    v = (e / e.shift(roc) - 1) * 100
    label = f"chaikinVolatility({n}, {roc})"

    assert v.first_valid_index() == n - 1 + roc, (
        f"{label} first valid at {v.first_valid_index()}, expected {n - 1 + roc}"
    )
    assert float(np.nanmin(e)) > 0, (
        f"{label}: the fixture has a zero smoothed range - the zero-base rule "
        "would then need its own oracle note rather than a unit test"
    )
    # TRUE range instead of plain range is the substitution to catch: same
    # shape, every value different, and it is what `atr` uses two files away.
    wrong_e = _ema_first_seed(_true_range(), n)
    wrong = (wrong_e / wrong_e.shift(roc) - 1) * 100
    sep = float(np.nanmax(np.abs(v - wrong)))
    assert sep > 1, (
        f"{label} sits within {sep} of the TRUE-range version - the fixture "
        "cannot tell plain range from true range"
    )
    print(
        f"  {label}: pandas replication (TA-Lib has no Chaikin Volatility); "
        f"first valid at {n - 1 + roc}, {sep:.2f} points from the "
        "true-range version"
    )
    return {"chaikinVol": col(v)}


def mass_index(ema_n: int, sum_n: int) -> dict:
    """Dorsey's Mass Index: the SUM over `sum_n` bars of EMA(range)/EMA(EMA(range)).

    The EMA chain is the TRIX pattern one stage shallower: stage 2 steps over
    stage 1's warm-up (the array door), so the ratio starts at 2*ema_n - 2 and
    the summation sum_n - 1 bars after that.
    """
    e1 = _ema_first_seed(_bar_range(), ema_n)
    e2 = _ema_first_seed(e1, ema_n)
    ratio = e1 / e2
    v = ratio.rolling(sum_n).sum()
    label = f"massIndex({ema_n}, {sum_n})"

    expected_first = 2 * ema_n - 2 + sum_n - 1
    assert v.first_valid_index() == expected_first, (
        f"{label} first valid at {v.first_valid_index()}, expected {expected_first}"
    )
    assert float(np.nanmin(e2)) > 0, (
        f"{label}: the fixture has a zero double EMA - the 0/0 rule would then "
        "need its own oracle note rather than a unit test"
    )
    # The MEAN of the ratio rather than its sum is the wrong turn: identical
    # shape, sum_n times smaller, and every published threshold (27 / 26.5) is
    # on the sum.
    sep = float(np.nanmax(np.abs(v - ratio.rolling(sum_n).mean())))
    assert sep > 1, (
        f"{label} sits within {sep} of the MEAN version - the fixture cannot "
        "tell a sum from an average"
    )
    print(
        f"  {label}: pandas replication (TA-Lib has no Mass Index); first "
        f"valid at {expected_first}, range "
        f"{float(np.nanmin(v)):.3f}..{float(np.nanmax(v)):.3f} "
        f"(a steady market sits at ~{sum_n}), {sep:.2f} from the mean version"
    )
    return {"mass": col(v)}


def choppiness_index(n: int) -> dict:
    """Dreiss' Choppiness Index: 100*log10(sum(TR)/(HH-LL))/log10(n).

    TRUE range (the ATR family's, asserted equal to talib.TRANGE through the
    ultimateOscillator case), so the warm-up is n and not n-1: TR[0] does not
    exist.
    """
    tr = _true_range()
    total = tr.rolling(n).sum()
    hh, ll = _hh_ll(n)
    v = 100 * np.log10(total / (hh - ll)) / math.log10(n)
    label = f"choppinessIndex({n})"

    assert v.first_valid_index() == n, (
        f"{label} first valid at {v.first_valid_index()}, expected {n} "
        "(the true-range sum needs a previous close)"
    )
    lo_v, hi_v = float(np.nanmin(v)), float(np.nanmax(v))
    assert 0.0 <= lo_v and hi_v <= 100.0, (
        f"{label} left the 0..100 band ({lo_v}..{hi_v})"
    )
    # The log base cancels (log_b(x)/log_b(n) = log_n(x)), so log10 here is a
    # presentation choice and not a definition fork -- asserted rather than
    # asserted-against, because "which log?" is the first question the formula
    # raises. MIXING the bases would be a real bug and is not this number.
    same_base = float(
        np.nanmax(np.abs(v - 100 * np.log(total / (hh - ll)) / math.log(n)))
    )
    assert same_base < 1e-12, (
        f"{label}: log10/log10 and ln/ln disagree by {same_base} - they are "
        "the same number by change of base, so this is an arithmetic slip"
    )
    # PLAIN range in place of true range is the wrong turn -- the one
    # substitution that keeps the curve's shape and moves every value.
    plain = (h - low_s).rolling(n).sum()
    wrong = 100 * np.log10(plain / (hh - ll)) / math.log10(n)
    sep = float(np.nanmax(np.abs(v - wrong)))
    assert sep > 1, (
        f"{label} sits within {sep} of the plain-range version - the fixture "
        "cannot tell true range from high-minus-low"
    )
    print(
        f"  {label}: pandas replication (TA-Lib has no Choppiness Index); "
        f"first valid at {n}, range {lo_v:.2f}..{hi_v:.2f}, {sep:.2f} points "
        f"from the plain-range version; the log base cancels (to {same_base:.1e})"
    )
    return {"chop": col(v)}


def ulcer_index(n: int) -> dict:
    """Martin's Ulcer Index, StockCharts' ROLLING form: the root mean square
    of the percent drawdown from the window's OWN highest close.

    Two chained windows, so the first value lands at 2n-2: the drawdown needs
    n closes and the mean of squares needs n drawdowns.
    """
    peak = s.rolling(n).max()
    drawdown = 100 * (s - peak) / peak
    v = np.sqrt((drawdown**2).rolling(n).mean())
    label = f"ulcerIndex({n})"

    assert v.first_valid_index() == 2 * n - 2, (
        f"{label} first valid at {v.first_valid_index()}, expected {2 * n - 2}"
    )
    assert float(np.nanmin(v)) >= 0.0, f"{label} went negative"
    assert float(np.nanmin(peak.dropna())) > 0, (
        f"{label}: the fixture has a zero rolling peak - the guard would then "
        "need its own oracle note rather than a unit test"
    )
    # The MEAN ABSOLUTE drawdown (the "Pain Index") is the wrong turn: same
    # inputs, same warm-up, a different statistic that is always smaller.
    wrong = drawdown.abs().rolling(n).mean()
    sep = float(np.nanmax(np.abs(v - wrong)))
    assert sep > 0.1, (
        f"{label} sits within {sep} of the mean-absolute (Pain Index) form - "
        "the fixture cannot tell the squaring apart"
    )
    print(
        f"  {label}: pandas replication (TA-Lib has no Ulcer Index); first "
        f"valid at {2 * n - 2}, max {float(np.nanmax(v)):.3f}, {sep:.3f} from "
        "the mean-absolute (Pain Index) form"
    )
    return {"ulcer": col(v)}


def vertical_horizontal_filter(n: int) -> dict:
    """Adam White's VHF: (HH - LL) / sum(|close change|), both over n bars.

    The alignment is the point: n bars give n-1 changes, so an n-long SUM of
    changes needs n+1 closes and the study first lands on bar n -- one row
    later than the range's n-1.
    """
    hi, lo = s.rolling(n).max(), s.rolling(n).min()
    path = s.diff().abs().rolling(n).sum()
    v = (hi - lo) / path
    label = f"verticalHorizontalFilter({n})"

    assert v.first_valid_index() == n, (
        f"{label} first valid at {v.first_valid_index()}, expected {n} "
        "(n changes need n+1 closes)"
    )
    lo_v, hi_v = float(np.nanmin(v)), float(np.nanmax(v))
    assert 0.0 < lo_v and hi_v <= 1.0, f"{label} left (0, 1] ({lo_v}..{hi_v})"
    # The n-1-term path sum is the off-by-one this alignment invites: it
    # starts a bar earlier AND divides by less, so it is high everywhere.
    wrong = (hi - lo) / s.diff().abs().rolling(n - 1).sum()
    sep = float(np.nanmax(np.abs(v - wrong)))
    assert sep > 0.005, (
        f"{label} sits within {sep} of the (n-1)-term path version - the "
        "fixture cannot tell the window alignment apart"
    )
    print(
        f"  {label}: pandas replication (TA-Lib has no VHF); first valid at "
        f"{n}, range {lo_v:.4f}..{hi_v:.4f}, {sep:.4f} from the (n-1)-term "
        "path version"
    )
    return {"vhf": col(v)}


def gopalakrishnan_range_index(n: int) -> dict:
    """GAPO: ln(HH - LL) / ln(n) -- which IS log base n of the window's range.

    The base cancels (log_b(x)/log_b(n) = log_n(x) for every b), exactly as it
    does in the Choppiness Index, so writing it with the natural log (ChartIQ)
    or with log10 gives the same number -- asserted below rather than assumed,
    because "which log?" is the first question a reader of the formula asks.
    What does NOT cancel is MIXING the bases, which is the plausible slip, and
    that is what the separation assert pins.
    """
    hh, ll = _hh_ll(n)
    v = np.log(hh - ll) / math.log(n)
    label = f"gopalakrishnanRangeIndex({n})"

    assert v.first_valid_index() == n - 1, (
        f"{label} first valid at {v.first_valid_index()}, expected {n - 1}"
    )
    same_base = float(np.nanmax(np.abs(v - np.log10(hh - ll) / math.log10(n))))
    assert same_base < 1e-12, (
        f"{label}: ln/ln and log10/log10 disagree by {same_base} - they are "
        "the same number by change of base, so this is an arithmetic slip"
    )
    # Mixing the bases (log10 on top, ln underneath) is the real slip, and so
    # is dropping the ln(n) normalisation altogether.
    sep = min(
        float(np.nanmax(np.abs(v - np.log10(hh - ll) / math.log(n)))),
        float(np.nanmax(np.abs(v - np.log(hh - ll)))),
    )
    assert sep > 0.05, (
        f"{label} sits within {sep} of a mixed-base or un-normalised version "
        "- the fixture cannot tell the normalisation apart"
    )
    # The scale-ADDITIVE property, which is what makes GAPO different from
    # every other study in this batch: scaling the prices by k shifts the
    # reading by exactly ln(k)/ln(n) rather than leaving it alone. Note what
    # this does and does not pin: it re-uses the same hh/ll, so it is the
    # change-of-base identity ln(k*x) = ln(k) + ln(x) checked in floating
    # point, not the study end to end. The end-to-end claim (scale the bars,
    # run gopalakrishnanRangeIndex, compare) is the TS property test in
    # test/talib-properties.test.ts; this assert only documents the algebra
    # the separation above relies on.
    k = 1000.0
    scaled = np.log(k * hh - k * ll) / math.log(n)
    offset = float(np.nanmax(np.abs((scaled - v) - math.log(k) / math.log(n))))
    assert offset < 1e-12, (
        f"{label}: scaling by {k} did not shift the reading by exactly "
        f"ln(k)/ln(n) (off by {offset})"
    )
    print(
        f"  {label}: pandas replication (TA-Lib has no GAPO); first valid at "
        f"{n - 1}, range {float(np.nanmin(v)):.4f}..{float(np.nanmax(v)):.4f}; "
        f"the log base cancels (ln/ln == log10/log10 to {same_base:.1e}), "
        f"{sep:.4f} from a mixed-base or un-normalised version; scaling by "
        f"{k:.0f} shifts it by exactly ln(k)/ln(n) (to {offset:.1e})"
    )
    return {"gapo": col(v)}


def relative_volatility_index(n: int, sd_n: int) -> dict:
    """Dorsey's Relative VOLATILITY Index (as revised): RSI's form with the
    population standard deviation of the close in place of the price change,
    Wilder-smoothed over n.

    An unchanged close counts as a DOWN bar (Dorsey's `close > prevClose` is
    the up test, everything else is the other leg) -- deliberately unlike
    `upDownLegValues`, which gives a flat bar 0 on both legs.
    """
    sd = s.rolling(sd_n).std(ddof=0)
    d = s.diff()
    unknown = d.isna() | sd.isna()
    up = pd.Series(np.where(unknown, np.nan, np.where(d > 0, sd, 0.0)))
    down = pd.Series(np.where(unknown, np.nan, np.where(d > 0, 0.0, sd)))
    smoothed_up, smoothed_down = _wilder(up, n), _wilder(down, n)
    v = 100 * smoothed_up / (smoothed_up + smoothed_down)
    label = f"relativeVolatilityIndex({n}, {sd_n})"

    assert v.first_valid_index() == sd_n + n - 2, (
        f"{label} first valid at {v.first_valid_index()}, expected "
        f"{sd_n + n - 2}"
    )
    lo_v, hi_v = float(np.nanmin(v)), float(np.nanmax(v))
    assert -1e-9 <= lo_v and hi_v <= 100.0 + 1e-9, (
        f"{label} left the 0..100 band ({lo_v}..{hi_v})"
    )
    # The EMA-smoothed fork (TradingView's) is the definition delta worth
    # measuring: same legs, a different recursion and a different seed.
    ema_up = _ema_first_seed(up, n)
    ema_down = _ema_first_seed(down, n)
    fork = 100 * ema_up / (ema_up + ema_down)
    sep = float(np.nanmax(np.abs(v - fork)))
    assert sep > 1, (
        f"{label} sits within {sep} of the EMA-smoothed fork - the fixture "
        "cannot tell Wilder's smoothing from a span EMA"
    )
    # And the flat-bar rule: counting an unchanged close for NEITHER leg (the
    # `rsi` convention) is a different study. This fixture has no unchanged
    # close, which is what the assert records.
    assert int((d == 0).sum()) == 0, (
        "the oracle input has an unchanged close - relativeVolatilityIndex's "
        "count-it-as-down rule would then need its own fixture note"
    )
    print(
        f"  {label}: pandas replication (TA-Lib has no RVI of either kind); "
        f"first valid at {sd_n + n - 2}, range {lo_v:.2f}..{hi_v:.2f}, "
        f"{sep:.2f} points from the EMA-smoothed (TradingView) fork"
    )
    return {"relVol": col(v)}


# --------------------------------------------------------------------------
# K7: the rolling linear-regression family (corpus 6.7) plus the two studies
# in 6.3 that hang off it.
#
# x runs 0..n-1 over the window with x = 0 the OLDEST bar, which is TA-Lib's
# convention: LINEARREG_INTERCEPT is the fit at the window's FIRST bar and
# LINEARREG is the fit at its LAST. The four TA-Lib functions are asserted
# exactly (mask then values); R-squared has no TA-Lib equivalent (its CORREL
# is between two series, not against the bar index) and is a pandas/numpy
# replication.
# --------------------------------------------------------------------------


def _ols(values, n: int):
    """Rolling OLS of `values` against x = 0..n-1. Returns (slope, intercept
    at x=0, r2). The naive two-pass reference the O(N) kernel must match."""
    y = np.asarray(values, dtype=float)
    slope = np.full(len(y), np.nan)
    inter = np.full(len(y), np.nan)
    r2 = np.full(len(y), np.nan)
    x = np.arange(n, dtype=float)
    mx = x.mean()
    sxx = float(((x - mx) ** 2).sum())
    for i in range(n - 1, len(y)):
        w = y[i - n + 1 : i + 1]
        my = w.mean()
        dy = w - my
        sxy = float(((x - mx) * dy).sum())
        syy = float((dy * dy).sum())
        m = sxy / sxx
        slope[i] = m
        inter[i] = my - m * mx
        # A flat window is a genuine 0/0 for r2 and undefined; the slope's
        # numerator is forced to zero with it, so the slope is 0.
        r2[i] = np.nan if syy == 0 else (sxy * sxy) / (sxx * syy)
    return pd.Series(slope), pd.Series(inter), pd.Series(r2)


def linear_regression(n: int) -> dict:
    """LINEARREG / _SLOPE / _INTERCEPT / _ANGLE plus R-squared, one fit."""
    slope, inter, r2 = _ols(closes, n)
    value = inter + slope * (n - 1)
    angle = pd.Series(np.degrees(np.arctan(slope.to_numpy())))
    label = f"linearRegression({n})"

    for name, v in (("value", value), ("slope", slope), ("r2", r2)):
        assert v.first_valid_index() == n - 1, (
            f"{label} {name} first valid at {v.first_valid_index()}, "
            f"expected {n - 1}"
        )
    lo_r, hi_r = float(np.nanmin(r2)), float(np.nanmax(r2))
    assert -1e-12 <= lo_r and hi_r <= 1 + 1e-12, (
        f"{label} r2 left [0, 1] ({lo_r}..{hi_r})"
    )
    # The un-squared |correlation| is the plausible slip and lives in the same
    # band, so the fixture has to be able to tell them apart.
    sep_r = float(np.nanmax(np.abs(r2 - np.sqrt(r2))))
    assert sep_r > 0.1, (
        f"{label} r2 sits within {sep_r} of |corr| - the fixture cannot tell "
        "the squaring apart"
    )
    # The intercept is the window's FIRST bar, not its last: a reader who
    # takes it for "the line now" is off by slope*(n-1), and this is how far.
    gap = float(np.nanmax(np.abs(value - inter)))
    assert gap > 1.0, (
        f"{label}: value and intercept sit within {gap} - the fixture cannot "
        "tell the two ends of the window apart"
    )

    talib_note = "no TA-Lib cross-check (pandas only)"
    if talib is not None:
        c = np.asarray(closes, dtype=float)
        worst = 0.0
        for name, ours, ref in (
            ("linregValue", value, pd.Series(talib.LINEARREG(c, timeperiod=n))),
            ("linregSlope", slope, pd.Series(talib.LINEARREG_SLOPE(c, timeperiod=n))),
            (
                "linregIntercept",
                inter,
                pd.Series(talib.LINEARREG_INTERCEPT(c, timeperiod=n)),
            ),
            ("linregAngle", angle, pd.Series(talib.LINEARREG_ANGLE(c, timeperiod=n))),
        ):
            assert list(ours.isna()) == list(ref.isna()), (
                f"{label} {name} warm-up differs from TA-Lib: ours first valid "
                f"{ours.first_valid_index()}, TA-Lib {ref.first_valid_index()}"
            )
            d = float(np.nanmax(np.abs(ours - ref)))
            assert d < 1e-9, f"{label} {name} disagrees with TA-Lib by {d}"
            worst = max(worst, d)
        talib_note = f"matches TA-Lib LINEARREG/_SLOPE/_INTERCEPT/_ANGLE to {worst:.3g}"
    print(
        f"  {label}: {talib_note}; first valid at {n - 1}, r2 in "
        f"{lo_r:.4f}..{hi_r:.4f} ({sep_r:.3f} from |corr|), value and "
        f"intercept up to {gap:.2f} apart"
    )
    return {
        "linregValue": col(value),
        "linregSlope": col(slope),
        "linregIntercept": col(inter),
        "linregAngle": col(angle),
        "linregR2": col(r2),
    }


def time_series_forecast(n: int) -> dict:
    """TA-Lib TSF: the fit projected one bar PAST the window (x = n)."""
    slope, inter, _ = _ols(closes, n)
    v = inter + slope * n
    label = f"timeSeriesForecast({n})"

    assert v.first_valid_index() == n - 1, (
        f"{label} first valid at {v.first_valid_index()}, expected {n - 1}"
    )
    # The in-window endpoint (LINEARREG) is the off-by-one this study invites.
    endpoint = inter + slope * (n - 1)
    sep = float(np.nanmax(np.abs(v - endpoint)))
    assert sep > 0.05, (
        f"{label} sits within {sep} of the in-window endpoint (LINEARREG) - "
        "the fixture cannot tell the projection apart"
    )

    talib_note = "no TA-Lib cross-check (pandas only)"
    if talib is not None:
        ref = pd.Series(talib.TSF(np.asarray(closes, dtype=float), timeperiod=n))
        assert list(v.isna()) == list(ref.isna()), (
            f"{label} warm-up differs from TA-Lib: ours first valid "
            f"{v.first_valid_index()}, TA-Lib {ref.first_valid_index()}"
        )
        d = float(np.nanmax(np.abs(v - ref)))
        assert d < 1e-9, f"{label} disagrees with TA-Lib TSF by {d}"
        talib_note = f"matches TA-Lib TSF to {d:.3g} (warm-ups identical)"
    print(
        f"  {label}: {talib_note}; first valid at {n - 1}, {sep:.3f} from the "
        "in-window endpoint (LINEARREG)"
    )
    return {"tsf": col(v)}


def chande_forecast_oscillator(n: int) -> dict:
    """Chande's CFO: 100*(close - TSF)/close. No TA-Lib function."""
    slope, inter, _ = _ols(closes, n)
    tsf = inter + slope * n
    v = 100 * (s - tsf) / s
    label = f"chandeForecastOscillator({n})"

    assert v.first_valid_index() == n - 1, (
        f"{label} first valid at {v.first_valid_index()}, expected {n - 1}"
    )
    # Subtracting the IN-WINDOW endpoint instead of the one-bar-ahead forecast
    # is the slip: same shape, every value different.
    wrong = 100 * (s - (inter + slope * (n - 1))) / s
    sep = float(np.nanmax(np.abs(v - wrong)))
    assert sep > 0.05, (
        f"{label} sits within {sep} of the LINEARREG-based version - the "
        "fixture cannot tell the projection apart"
    )
    # Dividing by the FORECAST rather than the price is the other one.
    by_forecast = 100 * (s - tsf) / tsf
    sep2 = float(np.nanmax(np.abs(v - by_forecast)))
    assert sep2 > 1e-6, (
        f"{label} sits within {sep2} of the divide-by-forecast version"
    )
    assert float(np.nanmin(s)) > 0, (
        f"{label}: the fixture has a non-positive close - the zero-price "
        "guard would then need its own oracle note rather than a unit test"
    )
    print(
        f"  {label}: pandas replication (TA-Lib has no CFO) on the "
        f"TA-Lib-checked TSF; first valid at {n - 1}, range "
        f"{float(np.nanmin(v)):.4f}..{float(np.nanmax(v)):.4f}, {sep:.3f} "
        f"from the LINEARREG-based version, {sep2:.2e} from the "
        "divide-by-forecast version"
    )
    return {"cfo": col(v)}


def center_of_gravity(n: int) -> dict:
    """Ehlers' CG, TradingView's UNCENTRED convention: the newest bar carries
    weight 1 and the oldest carries n, so a flat window reads -(n+1)/2."""
    y = np.asarray(closes, dtype=float)
    out = np.full(len(y), np.nan)
    for i in range(n - 1, len(y)):
        w = y[i - n + 1 : i + 1]          # oldest .. newest
        weights = np.arange(n, 0, -1, dtype=float)  # n .. 1
        out[i] = -float((weights * w).sum()) / float(w.sum())
    v = pd.Series(out)
    label = f"centerOfGravity({n})"

    assert v.first_valid_index() == n - 1, (
        f"{label} first valid at {v.first_valid_index()}, expected {n - 1}"
    )
    lo_v, hi_v = float(np.nanmin(v)), float(np.nanmax(v))
    assert -n - 1e-9 <= lo_v and hi_v <= -1 + 1e-9, (
        f"{label} left [-{n}, -1] ({lo_v}..{hi_v})"
    )
    # The weighting DIRECTION is the slip that leaves the shape intact.
    asc = np.full(len(y), np.nan)
    for i in range(n - 1, len(y)):
        w = y[i - n + 1 : i + 1]
        asc[i] = -float((np.arange(1, n + 1, dtype=float) * w).sum()) / float(w.sum())
    sep = float(np.nanmax(np.abs(v - pd.Series(asc))))
    # Measured against the reading's OWN spread rather than a fixed number:
    # CG barely moves (it is a balance point, not a price), so an absolute
    # threshold would be arbitrary at one period and unreachable at another.
    # What matters is that the wrong direction is further away than the whole
    # range the right one covers -- which holds at every period on this
    # fixture, and by a margin that grows with it (0.049 vs a 0.045 spread at
    # n = 5, 0.173 vs 0.162 at n = 10, 0.515 vs 0.458 at n = 20).
    spread = hi_v - lo_v
    assert sep > spread, (
        f"{label} sits {sep} from the ascending-weight (oldest lightest) "
        f"reading, inside its own {spread} spread - the fixture cannot tell "
        "the direction apart"
    )
    # Ehlers' own EasyLanguage adds (n+1)/2 to re-centre on zero; we ship
    # TradingView's uncentred form, and the offset is exactly that constant.
    centred = v + (n + 1) / 2
    offset = float(np.nanmax(np.abs((centred - v) - (n + 1) / 2)))
    assert offset < 1e-12, f"{label}: the centring offset is not constant"
    print(
        f"  {label}: pandas replication (TA-Lib has no CG); first valid at "
        f"{n - 1}, range {lo_v:.4f}..{hi_v:.4f} (flat would read "
        f"{-(n + 1) / 2}), {sep:.3f} from the ascending-weight reading "
        f"against its own {spread:.3f} spread; "
        f"Ehlers' centred form is exactly {(n + 1) / 2} higher"
    )
    return {"cog": col(v)}



# --------------------------------------------------------------------------
# The two-series / comparison family (assessment 6.7, kernel K8, gap G7):
# correlation, beta, price relative and performance index.
#
# The comparison series is a COLUMN on the same (already joined) series, so
# every case here reads `closes` against `benchmarks`. TA-Lib has CORREL and
# BETA and both are asserted bar-for-bar; Price Relative and Performance
# Index have no TA-Lib function, so they are pandas replications with the
# analytic first valid bar and a measured separation from the wrong turn.
# --------------------------------------------------------------------------


def correlation(n: int) -> dict:
    """Pearson's r between the two PRICE columns over n bars -- TA-Lib CORREL.

    TA-Lib correlates the raw inputs (not their returns), which is what pond's
    `correlation` matches; the return correlation is the same study over two
    percent-change columns.
    """
    v = s.rolling(n).corr(bench_s)
    label = f"correlation({n})"

    assert v.first_valid_index() == n - 1, (
        f"{label} first valid at {v.first_valid_index()}, expected {n - 1}"
    )
    lo_v, hi_v = float(np.nanmin(v)), float(np.nanmax(v))
    assert -1.0 <= lo_v and hi_v <= 1.0, f"{label} left [-1, 1] ({lo_v}..{hi_v})"
    assert lo_v < -0.3 and hi_v > 0.3, (
        f"{label} spans only {lo_v}..{hi_v} - the fixture must exercise both "
        "signs or a dropped sign passes"
    )
    assert hi_v < 0.999, (
        f"{label} reaches {hi_v} - the benchmark is (near) an affine transform "
        "of the closes, which pins r at 1 and makes the case vacuous"
    )
    if talib is not None:
        ref = talib.CORREL(np.asarray(closes), np.asarray(benchmarks), timeperiod=n)
        mask_ours = np.asarray([x is None for x in col(v)])
        mask_talib = np.isnan(ref)
        assert np.array_equal(mask_ours, mask_talib), (
            f"{label} warm-up mask differs from talib.CORREL"
        )
        delta = float(np.nanmax(np.abs(np.asarray(v, dtype=float) - ref)))
        assert delta < 1e-9, f"{label} differs from talib.CORREL by {delta}"
        print(
            f"  {label}: == talib.CORREL to {delta:.2e}; range "
            f"{lo_v:.4f}..{hi_v:.4f}, first valid at {n - 1}"
        )
    return {"corr": col(v)}


def beta_study(n: int) -> dict:
    """Slope of the closes' one-bar returns on the benchmark's, over n returns
    -- TA-Lib BETA with the BENCHMARK FIRST.

    Measured: `talib.BETA(a, b, n)` returns cov(rA, rB)/var(rA), i.e. it
    regresses its SECOND input on its FIRST, so the call matching pond's
    `beta({ column: 'close', benchmark: 'bench' })` is BETA(benchmarks,
    closes). Passing them the other way round is a different number, not a
    different sign.
    """
    rx = s.pct_change()
    ry = bench_s.pct_change()
    v = rx.rolling(n).cov(ry, ddof=0) / ry.rolling(n).var(ddof=0)
    label = f"beta({n})"

    assert v.first_valid_index() == n, (
        f"{label} first valid at {v.first_valid_index()}, expected {n} "
        "(n returns need n+1 prices)"
    )
    lo_v, hi_v = float(np.nanmin(v)), float(np.nanmax(v))
    assert lo_v < 0 < hi_v, (
        f"{label} spans only {lo_v}..{hi_v} - the fixture must cross zero"
    )
    assert max(abs(lo_v - 1), abs(hi_v - 1)) > 0.5, (
        f"{label} sits near 1 throughout - a benchmark that is a scalar "
        "multiple of the closes would give exactly 1 and pin nothing"
    )
    # The INVERSE regression (closes as the denominator) is the wrong turn,
    # and it is the mistake TA-Lib's argument order invites.
    wrong = ry.rolling(n).cov(rx, ddof=0) / rx.rolling(n).var(ddof=0)
    sep = float(np.nanmax(np.abs(v - wrong)))
    assert sep > 1, (
        f"{label} sits within {sep} of the inverse regression - the fixture "
        "cannot tell which variance is the denominator"
    )
    if talib is not None:
        ref = talib.BETA(np.asarray(benchmarks), np.asarray(closes), timeperiod=n)
        mask_ours = np.asarray([x is None for x in col(v)])
        mask_talib = np.isnan(ref)
        assert np.array_equal(mask_ours, mask_talib), (
            f"{label} warm-up mask differs from talib.BETA"
        )
        delta = float(np.nanmax(np.abs(np.asarray(v, dtype=float) - ref)))
        assert delta < 1e-9, f"{label} differs from talib.BETA by {delta}"
        print(
            f"  {label}: == talib.BETA(benchmark, close) to {delta:.2e}; range "
            f"{lo_v:.4f}..{hi_v:.4f}, {sep:.2f} from the inverse regression, "
            f"first valid at {n}"
        )
    return {"beta": col(v)}


def price_relative() -> dict:
    """close / benchmark, bar by bar -- ChartIQ's "Price Relative", also
    published as "Relative Strength (comparative)". No period, no warm-up,
    and emphatically not Wilder's RSI."""
    v = s / bench_s
    label = "priceRelative()"

    assert v.first_valid_index() == 0, (
        f"{label} first valid at {v.first_valid_index()}, expected 0 (no window)"
    )
    # The INVERTED ratio is the wrong turn: same shape upside down, every
    # value different, and nothing in the output says which way round it is.
    sep = float(np.nanmax(np.abs(v - bench_s / s)))
    assert sep > 0.1, (
        f"{label} sits within {sep} of the inverted ratio - the fixture "
        "cannot tell which column is the denominator"
    )
    print(
        f"  {label}: pandas replication (TA-Lib has no Price Relative); range "
        f"{float(np.nanmin(v)):.4f}..{float(np.nanmax(v)):.4f}, {sep:.3f} from "
        "the inverted ratio"
    )
    return {"priceRel": col(v)}


def performance_index(n: int) -> dict:
    """(close[i]/close[i-n]) / (bench[i]/bench[i-n]) -- each side's own n-bar
    growth, divided. Oscillates around 1.

    The RATIO form ships (1 = parity), matching `priceRelative` beside it;
    the percent forms are one subtraction away. Asserted equal to
    percentChange(priceRelative, n) up to that subtraction, which is what
    makes this a normalisation of the ratio rather than new math.
    """
    v = (s / s.shift(n)) / (bench_s / bench_s.shift(n))
    label = f"performanceIndex({n})"

    assert v.first_valid_index() == n, (
        f"{label} first valid at {v.first_valid_index()}, expected {n}"
    )
    lo_v, hi_v = float(np.nanmin(v)), float(np.nanmax(v))
    assert lo_v < 1 < hi_v, (
        f"{label} spans only {lo_v}..{hi_v} - it must cross parity, or a "
        "study that always out- or under-performed would pin nothing"
    )
    # The identity that makes this the price relative's rate of change.
    rel = s / bench_s
    roc = (rel / rel.shift(n) - 1) * 100
    identity = float(np.nanmax(np.abs((v - 1) * 100 - roc)))
    assert identity < 1e-9, (
        f"{label}: (perf - 1)*100 differs from percentChange(priceRelative, "
        f"{n}) by {identity} - they are the same number"
    )
    # The DIFFERENCE of the two growths is the wrong turn: same shape, same
    # warm-up, a different statistic that is not a ratio at all.
    wrong = (s / s.shift(n)) - (bench_s / bench_s.shift(n))
    sep = float(np.nanmax(np.abs(v - wrong)))
    assert sep > 0.1, (
        f"{label} sits within {sep} of the difference form - the fixture "
        "cannot tell a ratio of growths from their difference"
    )
    print(
        f"  {label}: pandas replication (TA-Lib has no Performance Index); "
        f"range {lo_v:.4f}..{hi_v:.4f}, first valid at {n}, identity with "
        f"percentChange(priceRelative) to {identity:.1e}, {sep:.3f} from the "
        "difference form"
    )
    return {"perf": col(v)}


# --------------------------------------------------------------------------
# K6 -- the stateful row fold ([PND-SFOLD]).
#
# Five studies whose value on bar i depends on the bars before it through a
# carried STATE rather than a window: Parabolic SAR, SuperTrend, the ATR
# trailing stop, NVI/PVI and Klinger. Only the first has a TA-Lib function;
# the rest are pandas replications with the definition spelled out term by
# term and a SEPARATION assert against the nearby misreading (a fixture that
# cannot tell two readings apart pins neither).
# --------------------------------------------------------------------------


def _sar_values(high, low, accel: float, maximum: float, force_long=None):
    """Wilder's Parabolic SAR in TA-Lib's exact arrangement (ta_SAR.c).

    Returns (sar, trend). The value PRINTED on bar i was computed at the end
    of bar i-1; the clamp reads the last two bars; and the initial side comes
    from Wilder's -DM over the first one-bar move. All three are measured
    against talib.SAR below rather than recalled.
    """
    n = len(high)
    sar_out = np.full(n, np.nan)
    trend_out = np.full(n, np.nan)
    if n < 2:
        return sar_out, trend_out

    diff_p = high[1] - high[0]
    diff_m = low[0] - low[1]
    # `force_long` exists ONLY for the separation assert below - the shipped
    # reading is always the -DM rule.
    is_long = (
        force_long
        if force_long is not None
        else not (diff_m > 0 and diff_m > diff_p)
    )

    af = accel
    if is_long:
        ep = high[1]
        sar = low[0]
    else:
        ep = low[1]
        sar = high[0]
    # TA-Lib's "cheat": on the first printed bar, yesterday IS today.
    new_low = low[1]
    new_high = high[1]

    for today in range(1, n):
        prev_low, prev_high = new_low, new_high
        new_low, new_high = low[today], high[today]
        if is_long:
            if new_low <= sar:
                is_long = False
                sar = max(ep, prev_high, new_high)
                sar_out[today] = sar
                af = accel
                ep = new_low
                sar = max(sar + af * (ep - sar), prev_high, new_high)
            else:
                sar_out[today] = sar
                if new_high > ep:
                    ep = new_high
                    af = min(af + accel, maximum)
                sar = min(sar + af * (ep - sar), prev_low, new_low)
        else:
            if new_high >= sar:
                is_long = True
                sar = min(ep, prev_low, new_low)
                sar_out[today] = sar
                af = accel
                ep = new_high
                sar = min(sar + af * (ep - sar), prev_low, new_low)
            else:
                sar_out[today] = sar
                if new_low < ep:
                    ep = new_low
                    af = min(af + accel, maximum)
                sar = max(sar + af * (ep - sar), prev_high, new_high)
        trend_out[today] = 1.0 if is_long else -1.0
    return sar_out, trend_out


def parabolic_sar(accel: float = 0.02, maximum: float = 0.2) -> dict:
    high = np.asarray(highs, dtype=float)
    low = np.asarray(lows, dtype=float)
    sar_v, trend_v = _sar_values(high, low, accel, maximum)
    sar = pd.Series(sar_v)
    trend = pd.Series(trend_v)
    label = f"parabolicSar({accel}, {maximum})"

    assert sar.first_valid_index() == 1, (
        f"{label} first valid at {sar.first_valid_index()}, expected 1"
    )
    flips = int((trend.dropna().diff().fillna(0) != 0).sum())
    assert flips >= 2, (
        f"{label} flips only {flips} time(s) on this fixture - a SAR that "
        "never reverses does not exercise the reversal branch at all"
    )

    if talib is not None:
        ref = pd.Series(talib.SAR(high, low, acceleration=accel, maximum=maximum))
        assert list(sar.isna()) == list(ref.isna()), (
            f"{label} warm-up differs from TA-Lib: ours first valid "
            f"{sar.first_valid_index()}, TA-Lib {ref.first_valid_index()}"
        )
        d = float(np.nanmax(np.abs(sar - ref)))
        assert d == 0.0, f"{label} disagrees with TA-Lib SAR by {d}"
        print(f"  {label}: matches TA-Lib SAR EXACTLY ({d:.1f}), warm-ups identical, {flips} flips")

        # The -DM seed rule is load-bearing, so it is PROBED rather than
        # assumed: a 7-bar input whose first move is a falling low (-DM > 0)
        # opens SHORT, and a version forced long on the same data must
        # disagree with TA-Lib. Without this, "we match TA-Lib" could be an
        # accident of the fixture opening on the side we happened to pick.
        probe_h = np.array([10.0, 10.2, 10.3, 10.4, 10.1, 9.7, 9.9])
        probe_l = np.array([9.0, 8.0, 8.1, 8.2, 8.0, 7.6, 7.8])
        theirs = talib.SAR(probe_h, probe_l, accel, maximum)
        ours = _sar_values(probe_h, probe_l, accel, maximum)[0]
        assert np.allclose(ours[1:], theirs[1:]), (
            f"the -DM seed probe disagrees with TA-Lib: ours {ours[1:]}, "
            f"TA-Lib {theirs[1:]}"
        )
        forced = _sar_values(probe_h, probe_l, accel, maximum, force_long=True)[0]
        assert not np.allclose(forced[1:], theirs[1:]), (
            "the seed probe cannot tell a forced-long seed from TA-Lib's -DM "
            "rule, so it pins nothing"
        )
        print(
            "  parabolicSar: the initial side comes from Wilder's -DM on bar 1 "
            "(probed against TA-Lib; a forced-long seed disagrees)"
        )

    return {"psar": col(sar), "psarTrend": col(trend)}


def _super_trend_values(period: int, mult: float, ratchet_on_prev_close: bool = True,
                        flip_on_ratcheted: bool = True, ratchet: bool = True):
    """SuperTrend (Olivier Seban, as TradingView's ta.supertrend implements it).

    `ratchet_on_prev_close` / `flip_on_ratcheted` / `ratchet` exist ONLY to
    produce the separation probes - the shipped reading is all three True.
    """
    a = _atr_series(period).to_numpy()
    hi = np.asarray(highs, dtype=float)
    lo = np.asarray(lows, dtype=float)
    cl = np.asarray(closes, dtype=float)
    n = len(cl)
    line = np.full(n, np.nan)
    trend = np.full(n, np.nan)
    upper = lower = np.nan
    prev_upper = prev_lower = np.nan
    up = False
    prev_close = np.nan
    run = 0
    for i in range(n):
        if not all(np.isfinite(v) for v in (hi[i], lo[i], cl[i], a[i])):
            run = 0
            continue
        run += 1
        mid = (hi[i] + lo[i]) / 2.0
        half = mult * a[i]
        basic_upper = mid + half
        basic_lower = mid - half
        if run == 1:
            upper, lower, up = basic_upper, basic_lower, False
        else:
            ref_close = prev_close if ratchet_on_prev_close else cl[i]
            prev_upper, prev_lower = upper, lower
            if not ratchet:
                upper, lower = basic_upper, basic_lower
            else:
                if basic_upper < upper or ref_close > upper:
                    upper = basic_upper
                if basic_lower > lower or ref_close < lower:
                    lower = basic_lower
            test_upper = upper if flip_on_ratcheted else prev_upper
            test_lower = lower if flip_on_ratcheted else prev_lower
            up = (not (cl[i] < test_lower)) if up else (cl[i] > test_upper)
        line[i] = lower if up else upper
        trend[i] = 1.0 if up else -1.0
        prev_close = cl[i]
    return pd.Series(line), pd.Series(trend)


def super_trend(period: int = 10, mult: float = 3.0, min_flips: int = 1) -> dict:
    line, trend = _super_trend_values(period, mult)
    label = f"superTrend({period}, {mult})"
    assert line.first_valid_index() == period, (
        f"{label} first valid at {line.first_valid_index()}, expected {period} "
        "(the ATR's own first bar)"
    )
    flips = int((trend.dropna().diff().fillna(0) != 0).sum())
    # `min_flips` is per-case: a 3-ATR band on 80 gently-trending bars flips
    # once, which still exercises both sides and the reversal branch, but the
    # SECOND case is deliberately tuned tighter so the machine is seen turning
    # repeatedly rather than latching.
    assert flips >= min_flips, (
        f"{label} flips only {flips} time(s), expected at least {min_flips} "
        "- the fixture is degenerate for this parameter set"
    )
    assert set(trend.dropna().unique()) == {1.0, -1.0}, (
        f"{label} never takes both sides on this fixture"
    )

    # Separation. Both probes run at a FIXED tight parameter set rather than
    # this case's own: a 3-ATR band on 80 gently-trending bars turns once, and
    # a probe there would silently pin nothing.
    base, _ = _super_trend_values(7, 1.0)
    deltas = []
    for name, kwargs in (
        ("no-ratchet (basic bands)", {"ratchet": False}),
        ("ratchet-on-current-close", {"ratchet_on_prev_close": False}),
    ):
        other, _ = _super_trend_values(7, 1.0, **kwargs)
        d = float(np.nanmax(np.abs(base - other)))
        assert d > 1e-6, (
            f"superTrend cannot be told apart from the {name} reading at "
            f"(7, 1.0) (max |delta| = {d}) - the fixture pins neither"
        )
        deltas.append(f"{name} {d:.4g}")

    # The third candidate slip -- flipping on the PREVIOUS final band instead
    # of the just-ratcheted one -- is UNOBSERVABLE here, and structurally so
    # rather than by luck of the fixture. While the side is up the lower band
    # only ratchets UP, so the two readings differ only when
    # `prevLower <= close < basicLower`; and `close < basicLower` needs
    # `multiplier * ATR < mid - close <= (high - low)/2`, which the ATR bounds
    # out for any multiplier at or above 1 whenever the close sits inside its
    # own bar. That is a STRUCTURAL guarantee only for multiplier >= 1: below
    # 1 the two readings can and do separate on other data (a Layer-2
    # reviewer's random walks separated 55 of 168 period x multiplier sets,
    # all at 0.2 or 0.5, by up to 5.0 price units), and this fixture's 0.0
    # across (period 2..20) x (multiplier 0.2..4.0) — re-measured at
    # integration, 0 of 133 — is fixture luck below 1. So the assert pins the
    # regime the argument covers, (7, 1.0), as an EQUALITY: a future fixture
    # that separates them THERE fails loudly rather than leaving the claim
    # stale.
    flip_alt, _ = _super_trend_values(7, 1.0, flip_on_ratcheted=False)
    d_flip = float(np.nanmax(np.abs(base - flip_alt)))
    assert d_flip == 0.0, (
        "the flip-order reading now separates on this fixture "
        f"(max |delta| = {d_flip}) - promote it to an assert and update the note"
    )
    print(
        f"  {label}: first valid {period}, {flips} flips; separation probe at "
        f"(7, 1.0): " + ", ".join(deltas) + "; flip-order reading 0.0 (unobservable, see note)"
    )
    return {"st": col(line), "stTrend": col(trend)}


def _ats_values(period: int, mult: float):
    a = _atr_series(period).to_numpy()
    cl = np.asarray(closes, dtype=float)
    n = len(cl)
    stop = np.full(n, np.nan)
    trend = np.full(n, np.nan)
    cur = np.nan
    long = True
    prev_close = np.nan
    run = 0
    for i in range(n):
        if not (np.isfinite(cl[i]) and np.isfinite(a[i])):
            run = 0
            continue
        run += 1
        d = mult * a[i]
        c = cl[i]
        if run == 1:
            cur = c - d
            long = True
        else:
            prev = cur
            if c > prev and prev_close > prev:
                cur = max(prev, c - d)
            elif c < prev and prev_close < prev:
                cur = min(prev, c + d)
            elif c > prev:
                cur = c - d
                long = True
            else:
                cur = c + d
                long = False
        stop[i] = cur
        trend[i] = 1.0 if long else -1.0
        prev_close = c
    return pd.Series(stop), pd.Series(trend)


def atr_trailing_stop(period: int = 14, mult: float = 3.0) -> dict:
    stop, trend = _ats_values(period, mult)
    label = f"atrTrailingStop({period}, {mult})"
    assert stop.first_valid_index() == period, (
        f"{label} first valid at {stop.first_valid_index()}, expected {period}"
    )
    flips = int((trend.dropna().diff().fillna(0) != 0).sum())
    assert flips >= 2, f"{label} flips only {flips} time(s) - the fixture is degenerate"

    # The ratchet is the study: a version that re-anchors every bar (no max /
    # min against the previous stop) must differ.
    cl = np.asarray(closes, dtype=float)
    a = _atr_series(period).to_numpy()
    naive = pd.Series(
        np.where(trend.to_numpy() > 0, cl - mult * a, cl + mult * a)
    )
    d = float(np.nanmax(np.abs(stop - naive)))
    assert d > 1e-6, (
        f"{label} is indistinguishable from the un-ratcheted band "
        f"(max |delta| = {d})"
    )
    # The Chandelier anchor (rolling extreme instead of the close) is the
    # NAMED alternative, not this study; show it is a different number.
    hh = pd.Series(highs).rolling(period).max().to_numpy()
    ll = pd.Series(lows).rolling(period).min().to_numpy()
    chand = pd.Series(np.where(trend.to_numpy() > 0, hh - mult * a, ll + mult * a))
    dc = float(np.nanmax(np.abs(stop - chand)))
    assert dc > 1e-6, f"{label} is indistinguishable from the Chandelier anchor"
    print(f"  {label}: first valid {period}, {flips} flips, ratchet and close-anchor both separated")
    return {"ats": col(stop), "atsTrend": col(trend)}


def _volume_index_values(on_fall: bool, start: float) -> pd.Series:
    cl = np.asarray(closes, dtype=float)
    vo = np.asarray(volumes, dtype=float)
    n = len(cl)
    out = np.full(n, np.nan)
    value = start
    for i in range(n):
        if i == 0:
            value = start
        else:
            fell = vo[i] < vo[i - 1]
            if vo[i] != vo[i - 1] and fell == on_fall:
                value = value * (1.0 + (cl[i] - cl[i - 1]) / cl[i - 1])
        out[i] = value
    return pd.Series(out)


def volume_index(kind: str, start: float = 1000.0) -> dict:
    on_fall = kind == "nvi"
    v = _volume_index_values(on_fall, start)
    label = f"{kind}({start})"
    assert v.first_valid_index() == 0, f"{label} must start at bar 0"
    assert v.iloc[0] == start, f"{label} must open at {start}"
    moves = int((v.diff().fillna(0).abs() > 1e-12).sum())
    assert moves >= 10, (
        f"{label} moves on only {moves} bars - the fixture does not exercise "
        "the compounding branch"
    )
    holds = len(v) - 1 - moves
    assert holds >= 10, (
        f"{label} holds on only {holds} bars - the fixture does not exercise "
        "the hold branch"
    )
    # Separation from the WRONG side, and from the volume-weighted cousin.
    other = _volume_index_values(not on_fall, start)
    d = float(np.nanmax(np.abs(v - other)))
    assert d > 1e-6, f"{label} is indistinguishable from its twin"
    # Scale invariance in price and in volume, asserted here as well as in the
    # TypeScript property tests (the two references must agree on it).
    print(f"  {label}: opens at {start}, {moves} compounding bars / {holds} holds, twin separated")
    return {kind: col(v)}


def _klinger_force(alt_factor: bool = False, simplified: bool = False) -> pd.Series:
    hi = np.asarray(highs, dtype=float)
    lo = np.asarray(lows, dtype=float)
    cl = np.asarray(closes, dtype=float)
    vo = np.asarray(volumes, dtype=float)
    n = len(cl)
    out = np.full(n, np.nan)
    trend = 0
    cm = 0.0
    prev_hlc = np.nan
    prev_dm = np.nan
    for i in range(n):
        hlc = hi[i] + lo[i] + cl[i]
        dm = hi[i] - lo[i]
        if i == 0:
            trend, cm, prev_hlc, prev_dm = 0, 0.0, hlc, dm
            continue
        t = 1 if hlc > prev_hlc else -1
        cm = cm + dm if t == trend else prev_dm + dm
        trend = t
        # No `cm == 0` guard, matching the shipped TS: cm == 0 forces dm == 0
        # and the ratio is 0/0 -> NaN on its own (mutation testing found the
        # TS guard dead and deleted it; keeping one here would be an
        # edit-divergence trap).
        if simplified:
            out[i] = vo[i] * t * 100.0
        elif alt_factor:
            out[i] = vo[i] * abs(2.0 * (dm / cm) - 1.0) * t * 100.0
        else:
            out[i] = vo[i] * abs(2.0 * (dm / cm - 1.0)) * t * 100.0
        prev_hlc, prev_dm = hlc, dm
    return pd.Series(out)


def klinger(fast: int = 34, slow: int = 55, signal: int = 13) -> dict:
    vf = _klinger_force()
    line = _ema_first_seed(vf, fast) - _ema_first_seed(vf, slow)
    sig = _ema_first_seed(line, signal)
    label = f"klinger({fast}, {slow}, {signal})"

    assert vf.first_valid_index() == 1, (
        f"{label} volume force first valid at {vf.first_valid_index()}, expected 1"
    )
    assert line.first_valid_index() == slow, (
        f"{label} first valid at {line.first_valid_index()}, expected {slow} "
        "(the volume force starts at bar 1, so the slow EMA lands at slow)"
    )
    assert sig.first_valid_index() == slow + signal - 1, (
        f"{label} signal first valid at {sig.first_valid_index()}, expected "
        f"{slow + signal - 1}"
    )

    # F-AMBIG: the two readings of the |2 x (dm/cm - 1)| factor, and
    # TradingView's simplified (no dm/cm) form, must all be distinguishable.
    alt = _ema_first_seed(_klinger_force(alt_factor=True), fast) - _ema_first_seed(
        _klinger_force(alt_factor=True), slow
    )
    d_alt = float(np.nanmax(np.abs(line - alt)))
    assert d_alt > 1e-6, (
        f"{label} cannot be told apart from the 2*(dm/cm) - 1 reading"
    )
    simp_force = _klinger_force(simplified=True)
    simp = _ema_first_seed(simp_force, fast) - _ema_first_seed(simp_force, slow)
    d_simp = float(np.nanmax(np.abs(line - simp)))
    assert d_simp > 1e-6, (
        "Klinger's original force and TradingView's simplified form read the "
        f"same on this fixture (|delta| = {d_simp}) - the two definitions are "
        "not distinguishable here and the docstring's separation is stale"
    )
    ours_span = float(np.nanmax(line) - np.nanmin(line))
    simp_span = float(np.nanmax(simp) - np.nanmin(simp))
    print(
        f"  {label}: first valid {slow} / {slow + signal - 1}; "
        f"|delta| vs the 2*(dm/cm)-1 reading = {d_alt:.4g}; "
        f"vs TradingView's simplified form = {d_simp:.4g} "
        f"(spans {ours_span:.4g} vs {simp_span:.4g})"
    )
    return {"kvo": col(line), "kvoSignal": col(sig)}


# --------------------------------------------------------------------------
# The moving-average stacks (assessment 6.1) and the smoothed-momentum tail
# (6.3): guppy, rainbow / rainbowOscillator, kst, priceMomentumOscillator,
# stochasticRsi, trueStrengthIndex, movingAverageDeviation.
#
# The stacks are pure K2 assemblies, so the checks reuse `_ma_over` (the
# TA-Lib-verified engine replication) rather than a private smoother, and
# every EMA-family case is split the `moving_average` way: the FORMULA on
# TA-Lib's own SMA seed, then pond's seed transient bounded separately.
# --------------------------------------------------------------------------

GUPPY_SHORT = (3, 5, 8, 10, 12, 15)
GUPPY_LONG = (30, 35, 40, 45, 50, 60)


def _ema_seed_is_geometric(ours: pd.Series, ref, n: int, label: str) -> float:
    """The pond-seed transient check for a SHORT fixture.

    `moving_average` bounds pond's first-sample-seed transient at "under 0.5%
    of scale over the last 20 shared bars". That test needs the transient to
    have DECAYED, and at guppy's long periods it has not: at n = 60 only 21
    of the 80 bars are shared, and across the twelve periods the worst
    difference at the very LAST bar is still 0.367% of scale (measured).

    So assert the exact statement instead. Two EMAs over the same input with
    the same alpha and different seeds satisfy the same recursion, so their
    DIFFERENCE satisfies d[k] = d[0] * (1-alpha)^k exactly -- a pure geometric
    decay, at any period, however few bars are shared. Measured on this
    fixture the residue is 1e-14..1e-12 relative for the correct alpha and
    3.15 (n=15) / 0.459 (n=30) for a 2/n rate, so this discriminates a wrong
    rate by twelve orders of magnitude while making no claim about how far the
    transient has got.

    Returns the last shared bar's difference as a fraction of scale (printed,
    not asserted -- it is the number the 0.5% bound would have used).
    """
    o = np.asarray(ours, dtype=float)
    r = np.asarray(ref, dtype=float)
    m = ~np.isnan(r)
    d = o[m] - r[m]
    alpha = 2.0 / (n + 1.0)
    predicted = d[0] * (1.0 - alpha) ** np.arange(len(d))
    residue = float(np.max(np.abs(d - predicted)) / max(abs(float(d[0])), 1e-300))
    assert residue < 1e-9, (
        f"{label}: the pond-seed difference from TA-Lib is not a geometric "
        f"decay at (1 - 2/(n+1)) -- relative residue {residue}, so the RATE "
        "differs, not just the seed"
    )
    scale = float(np.nanmax(np.abs(r)))
    return float(abs(d[-1]) / scale)


def guppy(kind: str) -> dict:
    """Guppy's Multiple Moving Average: the fixed twelve, 3/5/8/10/12/15 and
    30/35/40/45/50/60, as gmmaS{n} and gmmaL{n}.

    No TA-Lib GMMA exists, but TA-Lib HAS a moving average at every one of the
    twelve periods, so each column is checked against `talib.MA` rather than
    only the assembly. For `sma` that is exact; for `ema` it is the split
    `moving_average` uses -- the formula on TA-Lib's SMA seed bit-exact, then
    the seed transient (see `_ema_seed_is_geometric`, which replaces the tail
    bound because 60 bars of warm-up leave too few shared bars for one).
    """
    out = {}
    computed = {}
    worst_tail = 0.0
    worst_formula = 0.0
    for half, periods in (("S", GUPPY_SHORT), ("L", GUPPY_LONG)):
        for n in periods:
            v = _ma_over(s, kind, n)
            name = f"gmma{half}{n}"
            label = f"guppy({kind}).{name}"
            assert v.first_valid_index() == n - 1, (
                f"{label} first valid at {v.first_valid_index()}, expected "
                f"{n - 1} -- the column is not the {n}-bar average"
            )
            if talib is not None:
                ref = pd.Series(
                    talib.MA(
                        np.asarray(closes, dtype=float),
                        timeperiod=n,
                        matype=_MA_TALIB[kind],
                    )
                )
                assert list(v.isna()) == list(ref.isna()), (
                    f"{label} warm-up differs from TA-Lib MA(matype="
                    f"{_MA_TALIB[kind]}, timeperiod={n})"
                )
                if kind == "ema":
                    formula = _ema_sma_seed(closes, n)
                    refa = np.asarray(ref, dtype=float)
                    fm = ~np.isnan(refa)
                    fd = float(np.max(np.abs(formula[fm] - refa[fm])))
                    assert fd < 1e-9, (
                        f"{label}: SMA-seeded replication disagrees with "
                        f"TA-Lib by {fd} - the formula, not the seed, is wrong"
                    )
                    worst_formula = max(worst_formula, fd)
                    worst_tail = max(
                        worst_tail, _ema_seed_is_geometric(v, ref, n, label)
                    )
                else:
                    both = (~np.asarray(v.isna())) & (~np.asarray(ref.isna()))
                    d = float(
                        np.max(
                            np.abs(
                                np.asarray(v, dtype=float)[both]
                                - np.asarray(ref, dtype=float)[both]
                            )
                        )
                    )
                    assert d < 1e-9, f"{label} disagrees with TA-Lib by {d}"
                    worst_formula = max(worst_formula, d)
            computed[name] = np.asarray(v, dtype=float)
            out[name] = col(v)

    # The twelve must be TWELVE, not one average copied: a study that read the
    # same period into every column, or that swapped the short and long
    # halves, would still produce twelve plausible ribbons.
    names = list(computed)
    for i, a in enumerate(names):
        for b in names[i + 1 :]:
            m = np.isfinite(computed[a]) & np.isfinite(computed[b])
            assert m.any(), f"guppy({kind}): {a} and {b} never overlap"
            sep = float(np.max(np.abs(computed[a][m] - computed[b][m])))
            assert sep > 0.01, (
                f"guppy({kind}): {a} and {b} agree to within {sep} on this "
                "fixture - a duplicated period would pass"
            )
    print(
        f"  guppy({kind}): twelve columns vs talib.MA(matype="
        f"{_MA_TALIB[kind]}) at each period, masks identical, "
        + (
            f"formula on TA-Lib's seed to {worst_formula:.2g}, pond seed "
            f"transient geometric (worst last-bar residue {worst_tail:.3%})"
            if kind == "ema"
            else f"values to {worst_formula:.2g}"
        )
    )
    return out


RAINBOW_DEPTH = 10


def _rainbow_stack(kind: str, period: int):
    """The ten RECURSIVE averages: each smooths the previous one, through the
    ARRAY door (`_ma_over`), so each stage waits for `period` finite values
    and steps over the previous stage's warm-up."""
    stack = []
    current = s
    for _ in range(RAINBOW_DEPTH):
        current = _ma_over(current, kind, period)
        stack.append(current)
    return stack


def rainbow(period: int, kind: str) -> dict:
    """Rainbow Moving Average (Mel Widner, TASC July 1997): ten recursive
    averages, rainbow1..rainbow10.

    No TA-Lib function. A pandas replication on the same `_ma_over` the
    TA-Lib-verified K2 engine cases run, so what this case pins is the
    RECURSION and its composed warm-up -- stage k first valid at
    k * (period - 1).

    The discriminating assert is against the OTHER thing published under
    "rainbow": ten averages of INCREASING LENGTH over the same source. A
    recursive 2-bar mean is a binomial filter, not a box, so it differs from
    the SMA covering the same support -- measured 0.83 for stage 10 at
    (period 2, sma) and 1.44 at (period 3, ema), against a fixture whose whole
    close range is 19.36.
    """
    stack = _rainbow_stack(kind, period)
    label = f"rainbow({period},{kind})"
    for i, v in enumerate(stack):
        expected = (i + 1) * (period - 1)
        assert v.first_valid_index() == expected, (
            f"{label} stage {i + 1} first valid at {v.first_valid_index()}, "
            f"expected {expected} -- the stages do not compose their warm-up"
        )
    # Each stage must differ from the next, or a stack that dropped the
    # recursion (ten copies of one average) would pass.
    for i in range(RAINBOW_DEPTH - 1):
        a = np.asarray(stack[i], dtype=float)
        b = np.asarray(stack[i + 1], dtype=float)
        m = np.isfinite(a) & np.isfinite(b)
        sep = float(np.max(np.abs(a[m] - b[m])))
        assert sep > 0.01, (
            f"{label} stages {i + 1} and {i + 2} agree to within {sep} -- a "
            "stack that dropped the recursion would pass"
        )
    support = RAINBOW_DEPTH * (period - 1) + 1
    alt = s.rolling(support).mean()
    a = np.asarray(stack[-1], dtype=float)
    b = np.asarray(alt, dtype=float)
    m = np.isfinite(a) & np.isfinite(b)
    widths = float(np.max(np.abs(a[m] - b[m])))
    assert widths > 0.5, (
        f"{label} stage 10 sits only {widths} from the plain SMA({support}) "
        "over the same support - the fixture cannot tell the recursive form "
        "from the increasing-length one"
    )
    print(
        f"  {label}: pandas replication (no TA-Lib Rainbow); stage k first "
        f"valid at k*(period-1), {widths:.4f} from the SMA({support}) that "
        "covers the same bars"
    )
    return {f"rainbow{i + 1}": col(v) for i, v in enumerate(stack)}


def rainbow_oscillator(period: int, lookback: int, kind: str) -> dict:
    """Rainbow Oscillator, ChartIQ's definition:

        rbo      = 100 * (close - mean(stack)) / (HH - LL over lookback)
        rboUpper = 100 * (max(stack) - min(stack)) / (HH - LL)
        rboLower = -rboUpper

    HH/LL are of the SOURCE COLUMN (close here), not of a bar's high and low.
    No TA-Lib function and the indicator is genuinely ambiguous across
    vendors, so the two nearest variants are measured and asserted far away:
    normalising by the PRICE instead of the range, and taking the numerator
    against the FIRST average instead of the stack's mean.
    """
    stack = _rainbow_stack(kind, period)
    m = np.vstack([np.asarray(v, dtype=float) for v in stack])
    mean10 = m.mean(axis=0)
    hh = s.rolling(lookback).max().to_numpy()
    ll = s.rolling(lookback).min().to_numpy()
    rng = hh - ll
    price = np.asarray(closes, dtype=float)

    line = 100.0 * (price - mean10) / rng
    band = 100.0 * (m.max(axis=0) - m.min(axis=0)) / rng
    # A flat lookback window reads null, not 0. Nothing forces the numerators
    # to zero -- the stack reaches back past the window -- so it is a real
    # number over zero, i.e. an infinity rather than a 0/0. (Not reachable on
    # this fixture; the study's unit tests pin it, measured at >10 points away
    # with lookback 3 and still 0.0044 away with lookback 10.)
    line = np.where(rng == 0, np.nan, line)
    band = np.where(rng == 0, np.nan, band)

    label = f"rainbowOscillator({period},{lookback},{kind})"
    expected = max(RAINBOW_DEPTH * (period - 1), lookback - 1)
    first = int(np.flatnonzero(np.isfinite(line))[0])
    assert first == expected, (
        f"{label} first valid at {first}, expected {expected} "
        "(the deepest average's warm-up or the range's, whichever is later)"
    )
    assert int(np.flatnonzero(np.isfinite(band))[0]) == expected, (
        f"{label} bands warm up on a different bar from the line"
    )
    assert np.nanmin(line) < 0 < np.nanmax(line), (
        f"{label} never crosses zero on this fixture"
    )
    assert np.nanmin(band) > 0, f"{label} band is not strictly positive"

    alt_price = 100.0 * (price - mean10) / price
    alt_first = 100.0 * (price - m[0]) / rng
    fm = np.isfinite(line)
    sep_price = float(np.max(np.abs(line[fm] - alt_price[fm])))
    sep_first = float(np.max(np.abs(line[fm] - alt_first[fm])))
    assert sep_price > 10 and sep_first > 10, (
        f"{label} sits {sep_price} from the divide-by-price variant and "
        f"{sep_first} from the first-average numerator - too close for the "
        "fixture to tell them apart"
    )
    print(
        f"  {label}: pandas replication of ChartIQ's definition; first valid "
        f"at {expected}, line {np.nanmin(line):.3f}..{np.nanmax(line):.3f}, "
        f"{sep_price:.2f} from the divide-by-price variant and {sep_first:.2f} "
        "from the first-average numerator"
    )
    return {
        "rbo": col(pd.Series(line)),
        "rboUpper": col(pd.Series(band)),
        "rboLower": col(pd.Series(-band)),
    }


KST_ROC = (10, 15, 20, 30)
KST_SMOOTHING = (10, 10, 10, 15)
KST_WEIGHTS = (1, 2, 3, 4)


def kst(signal_n: int) -> dict:
    """Pring's Know Sure Thing, the intermediate DAILY set:

        term_i = SMA(ROC(close, roc_i), smooth_i)      ROC in PERCENT
        kst    = 1*term1 + 2*term2 + 3*term3 + 4*term4
        signal = SMA(kst, signal_n)

    with roc = 10/15/20/30 and smooth = 10/10/10/15. No TA-Lib function, so
    this is a pandas replication on the same `pct_change` the TA-Lib-verified
    percentChange case uses. The terms are SMAs of a DERIVED array, so each
    waits for smooth_i finite VALUES (pandas' rolling does exactly that),
    which is what puts the line at roc_4 + smooth_4 - 1 = 44 rather than
    earlier.

    Two discriminating separations, because the shape survives both wrong
    turns: an EQUAL-weight sum, and a sum of UNSMOOTHED rates of change.
    """
    total = None
    for roc_n, smooth_n, weight in zip(KST_ROC, KST_SMOOTHING, KST_WEIGHTS):
        term = (s.pct_change(roc_n) * 100).rolling(smooth_n).mean() * weight
        total = term if total is None else total + term
    line = total
    signal = line.rolling(signal_n).mean()

    label = f"kst({signal_n})"
    expected = max(r + m for r, m in zip(KST_ROC, KST_SMOOTHING)) - 1
    assert line.first_valid_index() == expected, (
        f"{label} first valid at {line.first_valid_index()}, expected "
        f"{expected} (the slowest term's roc + smooth - 1)"
    )
    assert signal.first_valid_index() == expected + signal_n - 1, (
        f"{label} signal first valid at {signal.first_valid_index()}, "
        f"expected {expected + signal_n - 1}"
    )
    assert line.min() < 0 < line.max(), (
        f"{label} never crosses zero on this fixture - the reading the study "
        "exists for would be untested"
    )

    equal = None
    unsmoothed = None
    for roc_n, smooth_n, weight in zip(KST_ROC, KST_SMOOTHING, KST_WEIGHTS):
        e = (s.pct_change(roc_n) * 100).rolling(smooth_n).mean()
        u = s.pct_change(roc_n) * 100 * weight
        equal = e if equal is None else equal + e
        unsmoothed = u if unsmoothed is None else unsmoothed + u
    m = (~line.isna()) & (~equal.isna())
    sep_equal = float(np.max(np.abs(line[m] - equal[m])))
    m = (~line.isna()) & (~unsmoothed.isna())
    sep_raw = float(np.max(np.abs(line[m] - unsmoothed[m])))
    assert sep_equal > 10 and sep_raw > 10, (
        f"{label} sits {sep_equal} from the equal-weight sum and {sep_raw} "
        "from the unsmoothed one - the fixture cannot tell them apart"
    )
    print(
        f"  {label}: pandas replication (no TA-Lib KST); first valid at "
        f"{expected}, range {line.min():.4f}..{line.max():.4f}, "
        f"{sep_equal:.2f} from the equal-weight sum and {sep_raw:.2f} from "
        "the unsmoothed one"
    )
    return {"kst": col(line), "kstSignal": col(signal)}


def _alpha_ema(values, alpha: float, min_samples: int) -> pd.Series:
    """The exponential recursion at a caller-chosen ALPHA -- pond's
    `alphaEmaValues`. Same seed rule as `_ema_first_seed` (first finite
    sample, missing cells skipped, emitted once min_samples have been
    consumed); only the rate is free. Exists for DecisionPoint's "custom
    smoothing", which is 2/n rather than the span EMA's 2/(n+1)."""
    x = np.asarray(values, dtype=float)
    out = np.full(len(x), np.nan)
    prev = None
    seen = 0
    for i, v in enumerate(x):
        if not np.isfinite(v):
            continue
        prev = v if prev is None else alpha * v + (1.0 - alpha) * prev
        seen += 1
        if seen >= min_samples:
            out[i] = prev
    return pd.Series(out)


PMO_FIRST, PMO_SECOND, PMO_SIGNAL, PMO_SCALE = 35, 20, 10, 10


def price_momentum_oscillator() -> dict:
    """DecisionPoint's Price Momentum Oscillator:

        roc    = (close/close[-1] - 1) * 100
        stage1 = customEMA(roc, 35)              alpha = 2/35
        pmo    = customEMA(10 * stage1, 20)      alpha = 2/20
        signal = EMA(pmo, 10)                    alpha = 2/11  (SPAN)

    The two stages use DecisionPoint's "custom smoothing" (2/n), which is NOT
    a span EMA; the SIGNAL is a plain span EMA, which is DecisionPoint's own
    asymmetry. No TA-Lib function, so this is a pandas replication with the
    analytic first valid bars asserted and three separations measured:

      - both stages on the SPAN EMA instead of the custom rate
      - a custom-smoothed SIGNAL instead of the span one
      - the x10 moved to either end (must be IMMATERIAL - every stage is
        homogeneous, so this one asserts AGREEMENT, not separation)

    Note the study has no period options, so there is one case; the fixture's
    PMO does not cross zero (the series drifts up throughout), so the assert
    is on its spread rather than on a sign change.
    """
    roc = (s / s.shift(1) - 1) * 100
    stage1 = _alpha_ema(roc, 2.0 / PMO_FIRST, PMO_FIRST)
    line = _alpha_ema(stage1 * PMO_SCALE, 2.0 / PMO_SECOND, PMO_SECOND)
    signal = _ema_first_seed(line, PMO_SIGNAL)

    label = "priceMomentumOscillator()"
    expected = 1 + PMO_FIRST - 1 + PMO_SECOND - 1
    assert line.first_valid_index() == expected, (
        f"{label} first valid at {line.first_valid_index()}, expected "
        f"{expected} (a 1-bar ROC then two custom stages)"
    )
    assert signal.first_valid_index() == expected + PMO_SIGNAL - 1, (
        f"{label} signal first valid at {signal.first_valid_index()}, "
        f"expected {expected + PMO_SIGNAL - 1}"
    )

    # (a) the custom rate is not the span rate.
    span_stages = _ema_first_seed(
        _ema_first_seed(roc, PMO_FIRST) * PMO_SCALE, PMO_SECOND
    )
    m = (~line.isna()) & (~span_stages.isna())
    sep_span = float(np.max(np.abs(line[m] - span_stages[m])))
    scale = float(np.nanmax(np.abs(line)))
    assert sep_span / scale > 0.01, (
        f"{label} sits {sep_span} ({sep_span / scale:.2%} of scale) from the "
        "span-EMA build - too close for the fixture to tell 2/n from 2/(n+1)"
    )
    # (b) the signal is a SPAN ema, not a custom-smoothed one.
    custom_signal = _alpha_ema(line, 2.0 / PMO_SIGNAL, PMO_SIGNAL)
    m = (~signal.isna()) & (~custom_signal.isna())
    sep_signal = float(np.max(np.abs(signal[m] - custom_signal[m])))
    assert sep_signal / scale > 0.01, (
        f"{label} signal sits {sep_signal} from the custom-smoothed one - "
        "the fixture cannot tell which smoothing the signal uses"
    )
    # (c) the x10 placement is IMMATERIAL - every stage is homogeneous.
    early = _alpha_ema(
        _alpha_ema(roc * PMO_SCALE, 2.0 / PMO_FIRST, PMO_FIRST),
        2.0 / PMO_SECOND,
        PMO_SECOND,
    )
    late = (
        _alpha_ema(
            _alpha_ema(roc, 2.0 / PMO_FIRST, PMO_FIRST),
            2.0 / PMO_SECOND,
            PMO_SECOND,
        )
        * PMO_SCALE
    )
    m = ~line.isna()
    placement = max(
        float(np.max(np.abs(line[m] - early[m]))),
        float(np.max(np.abs(line[m] - late[m]))),
    )
    assert placement < 1e-12, (
        f"{label}: moving the x10 changed the answer by {placement} - the "
        "stages are not homogeneous after all"
    )
    spread = float(np.nanmax(line) - np.nanmin(line))
    assert spread > 1.0, f"{label} barely moves on this fixture ({spread})"

    print(
        f"  {label}: pandas replication (no TA-Lib PMO); first valid at "
        f"{expected}, signal at {expected + PMO_SIGNAL - 1}, range "
        f"{np.nanmin(line):.5f}..{np.nanmax(line):.5f}; {sep_span:.4f} "
        f"({sep_span / scale:.2%} of scale) from the span-EMA build, "
        f"{sep_signal:.4f} from a custom-smoothed signal, x10 placement "
        f"immaterial to {placement:.2g}"
    )
    return {"pmo": col(line), "pmoSignal": col(signal)}


def stochastic_rsi(rsi_n: int, stoch_n: int, k_n: int, d_n: int) -> dict:
    """Stochastic RSI (Chande & Kroll): the stochastic construction over the
    RSI rather than over price.

        r    = rsi(close, rsi_n)                    [the TA-Lib-verified rsi]
        raw  = 100 * (r - LL(r, stoch_n)) / (HH(r, stoch_n) - LL(...))
        K    = SMA(raw, k_n)
        D    = SMA(K, d_n)

    TA-Lib HAS this one, but STOCHRSI returns **fastk and fastd**, not a
    slowed %K and %D, so the correspondence is NOT the obvious one. Measured
    on this fixture at the defaults, and asserted below:

        our K        == talib fastd (fastk_period=stoch_n, fastd_period=k_n)
        the raw (unemitted) position == talib fastk
        our D        has NO TA-Lib counterpart

    The generator asserts BOTH directions - the match and the mismatch -
    because crossing the two is a 45-point error on this fixture rather than
    a rounding one, and a case that only checked "close to something TA-Lib
    returns" would pass on the wrong column.
    """
    r = _rsi_series(rsi_n)
    hh = r.rolling(stoch_n).max()
    ll = r.rolling(stoch_n).min()
    rng = hh - ll
    raw = 100.0 * (r - ll) / rng
    # A flat RSI window is 0/0 -> null here and 0.0 in TA-Lib; the shared
    # kernel rule (percentOfRangeValues). Not reachable on this fixture.
    raw = raw.where(rng != 0)
    k = raw.rolling(k_n).mean()
    d = k.rolling(d_n).mean()

    label = f"stochasticRsi({rsi_n},{stoch_n},{k_n},{d_n})"
    expected_k = rsi_n + stoch_n - 1 + k_n - 1
    assert k.first_valid_index() == expected_k, (
        f"{label} %K first valid at {k.first_valid_index()}, expected "
        f"{expected_k} (rsi_n + stoch_n - 1 + k_n - 1)"
    )
    assert d.first_valid_index() == expected_k + d_n - 1, (
        f"{label} %D first valid at {d.first_valid_index()}, expected "
        f"{expected_k + d_n - 1}"
    )
    lo_v, hi_v = float(k.min()), float(k.max())
    assert -1e-9 <= lo_v and hi_v <= 100 + 1e-9, (
        f"{label} %K left 0..100 ({lo_v}..{hi_v})"
    )

    if talib is not None:
        fastk, fastd = talib.STOCHRSI(
            np.asarray(closes, dtype=float),
            timeperiod=rsi_n,
            fastk_period=stoch_n,
            fastd_period=k_n,
            fastd_matype=0,
        )
        fastk = pd.Series(fastk)
        fastd = pd.Series(fastd)
        # (a) our %K IS TA-Lib's fastd, mask included.
        assert list(k.isna()) == list(fastd.isna()), (
            f"{label} %K warm-up differs from TA-Lib fastd: ours first valid "
            f"{k.first_valid_index()}, TA-Lib {fastd.first_valid_index()}"
        )
        m = ~fastd.isna()
        d_k = float(np.max(np.abs(np.asarray(k)[m] - np.asarray(fastd)[m])))
        assert d_k < 1e-9, f"{label} %K disagrees with TA-Lib fastd by {d_k}"
        # (b) the RAW position is TA-Lib's fastk (which TA-Lib masks back to
        #     fastd's first bar, as STOCH masks %K back to %D's).
        m = ~fastk.isna()
        d_raw = float(np.max(np.abs(np.asarray(raw)[m] - np.asarray(fastk)[m])))
        assert d_raw < 1e-9, (
            f"{label} the raw range position disagrees with TA-Lib fastk by "
            f"{d_raw}"
        )
        # (c) and the columns are NOT interchangeable - crossing them is a
        #     real error, so the fixture has to be able to see it.
        crossed = float(np.max(np.abs(np.asarray(k)[m] - np.asarray(fastk)[m])))
        if k_n > 1:
            assert crossed > 1.0, (
                f"{label} %K and TA-Lib's fastk agree to within {crossed} on "
                "this fixture - a study that emitted the unsmoothed position "
                "would pass"
            )
        else:
            # kPeriod 1 IS the fast form: %K is the raw position, so all
            # three (our %K, fastk, fastd) coincide by definition. Assert
            # that rather than a separation which cannot exist here.
            assert crossed < 1e-9, (
                f"{label} at kPeriod 1 should BE the raw position, but sits "
                f"{crossed} from TA-Lib's fastk"
            )
        print(
            f"  {label}: %K == talib.STOCHRSI fastd to {d_k:.2g} (masks "
            f"identical); the unemitted raw position == fastk to {d_raw:.2g}; "
            f"crossing them is {crossed:.1f} apart. %D has no TA-Lib "
            f"counterpart - pandas replication, first valid at "
            f"{expected_k + d_n - 1}"
        )
    else:
        print(f"  {label}: pandas only - TA-Lib not installed, cross-check SKIPPED")

    return {"stochRsiK": col(k), "stochRsiD": col(d)}


def true_strength_index(long_n: int, short_n: int, sig_n: int) -> dict:
    """William Blau's True Strength Index:

        d    = close.diff()
        tsi  = 100 * EMA(EMA(d, long_n), short_n)
                   / EMA(EMA(|d|, long_n), short_n)
        sig  = EMA(tsi, sig_n)

    No TA-Lib TSI. What the case CAN borrow from TA-Lib is the smoothing
    itself: an EMA stage rebuilt on TA-Lib's own SMA seed over the change
    array is required to match talib.EMA bit-exactly, so the stage
    arithmetic is vendor-checked even though the assembly is not. The EMAs
    in the fixture are POND's (first-sample seed), the macd precedent.

    The discriminating assert is the smoothing ORDER: long first, then
    short. Swapping them keeps the shape and moves the values.
    """
    d = s.diff()
    num = _ema_first_seed(_ema_first_seed(d, long_n), short_n)
    den = _ema_first_seed(_ema_first_seed(d.abs(), long_n), short_n)
    line = 100.0 * num / den
    line = line.where(den != 0)
    signal = _ema_first_seed(line, sig_n)

    label = f"trueStrengthIndex({long_n},{short_n},{sig_n})"
    expected = long_n + short_n - 1
    assert line.first_valid_index() == expected, (
        f"{label} first valid at {line.first_valid_index()}, expected "
        f"{expected} (a 1-bar difference then two EMA stages)"
    )
    assert signal.first_valid_index() == expected + sig_n - 1, (
        f"{label} signal first valid at {signal.first_valid_index()}, "
        f"expected {expected + sig_n - 1}"
    )
    lo_v, hi_v = float(line.min()), float(line.max())
    assert -100 - 1e-9 <= lo_v and hi_v <= 100 + 1e-9, (
        f"{label} left -100..100 ({lo_v}..{hi_v})"
    )
    assert lo_v < 0 < hi_v, (
        f"{label} never crosses zero on this fixture - the reading the study "
        "is used for would be untested"
    )

    swapped = 100.0 * _ema_first_seed(
        _ema_first_seed(d, short_n), long_n
    ) / _ema_first_seed(_ema_first_seed(d.abs(), short_n), long_n)
    m = (~line.isna()) & (~swapped.isna())
    sep = float(np.max(np.abs(line[m] - swapped[m])))
    # Measured against the reading's OWN spread rather than a fixed number.
    # Short spans saturate the ratio near +/-100, so the spread grows while
    # the swap's effect shrinks: an absolute threshold that discriminates at
    # (25, 13) is unreachable at short spans. Measured on this fixture, the
    # swap as a fraction of the line's own spread: 14.56% at (25, 13),
    # 3.99% at (20, 6), 1.64% at (12, 4), 0.40% at (8, 3) - so 2% sits ~2x
    # clear of the second case and the very short pairs are not used.
    spread = hi_v - lo_v
    assert sep > 0.02 * spread, (
        f"{label} sits {sep} from the SWAPPED smoothing order, inside "
        f"{0.005 * spread} of its own {spread} spread - the fixture cannot "
        "tell which span is applied first"
    )

    if talib is not None:
        # The stage arithmetic against TA-Lib, on TA-Lib's own seed: the
        # change array from bar 1 on is finite, so talib.EMA takes it
        # directly.
        deltas = np.asarray(d, dtype=float)[1:]
        ref = talib.EMA(deltas, timeperiod=long_n)
        mine = _ema_sma_seed(deltas, long_n)
        fm = ~np.isnan(ref)
        stage = float(np.max(np.abs(mine[fm] - ref[fm])))
        assert stage < 1e-9, (
            f"{label}: the EMA stage on TA-Lib's seed disagrees with "
            f"talib.EMA by {stage}"
        )
        print(
            f"  {label}: pandas replication (no TA-Lib TSI); the EMA stage "
            f"matches talib.EMA on its own seed to {stage:.2g}; first valid "
            f"at {expected}, range {lo_v:.4f}..{hi_v:.4f}, {sep:.2f} from the "
            f"swapped smoothing order ({sep / (hi_v - lo_v):.2%} of its own "
            "spread)"
        )
    else:
        print(f"  {label}: pandas only - TA-Lib not installed, cross-check SKIPPED")

    return {"tsi": col(line), "tsiSignal": col(signal)}


def moving_average_deviation(n: int, kind: str) -> dict:
    """Moving Average Deviation: close - MA(n), in PRICE UNITS.

    The corpus lists this study as "points or percent". The PERCENT form is
    already shipped as disparityIndex, and this case asserts that identity
    exactly -- 100 * maDev / MA == disparity, bit for bit -- which is why
    only the points form ships and there is no `mode` flag.

    The discriminating separation is from `momentum` (close - close[-n]),
    the wrong turn that leaves the shape intact: a lagged price rather than
    a smoothed one.
    """
    ma = _ma_values(kind, n)
    v = s - ma

    label = f"movingAverageDeviation({n},{kind})"
    expected = int(ma.first_valid_index())
    assert v.first_valid_index() == expected, (
        f"{label} first valid at {v.first_valid_index()}, expected the "
        f"average's own {expected}"
    )
    # The identity with disparityIndex, exact.
    disparity = 100 * (s - ma) / ma
    m = ~v.isna()
    identity = float(np.max(np.abs((100 * v[m] / ma[m]) - disparity[m])))
    assert identity == 0.0, (
        f"{label}: 100 * maDev / MA differs from disparityIndex by "
        f"{identity} - the percent form is supposed to BE that study"
    )
    # ... and the separation from `momentum`, which is close - close[-n].
    lagged = s - s.shift(n)
    m = (~v.isna()) & (~lagged.isna())
    sep = float(np.max(np.abs(v[m] - lagged[m])))
    assert sep > 1.0, (
        f"{label} sits {sep} from momentum({n}) (close - close[-n]) - the "
        "fixture cannot tell a smoothed reference from a lagged one"
    )
    print(
        f"  {label}: pandas replication (no TA-Lib function); first valid at "
        f"{expected}, range {v.min():.4f}..{v.max():.4f}; "
        f"100*maDev/MA == disparityIndex exactly ({identity}), {sep:.4f} from "
        f"momentum({n})"
    )
    return {"maDev": col(v)}


# --------------------------------------------------------------------------
# The momentum and trend leftovers (assessment 6.3 / 6.4 / 6.1): the
# Stochastic Momentum Index, the Fisher Transform, the Schaff Trend Cycle,
# the Pretty Good Oscillator, the swing index pair, the Random Walk Index,
# RAVI, the Trend Intensity Index and Pring's Special K.
#
# NONE of these has a TA-Lib function, so every case below is a pandas
# replication. Two of them (Fisher, Schaff) are state machines, and their
# "oracle" is honestly a TRANSCRIPTION of the same step the TypeScript runs
# rather than an independent derivation -- the review of #708 made that point
# and it holds here. What carries the weight for those two is (a) the
# analytic first-valid bar, asserted, and (b) the measured SEPARATION from
# the plausible wrong turn, also asserted: a transcription that agreed with a
# different definition would fail the separation even though it agreed with
# itself.
# --------------------------------------------------------------------------


def stochastic_momentum_index(q: int, r: int, u: int, sig: int) -> dict:
    """William Blau's SMI (TASC January 1993):

        M   = close - (HH(q) + LL(q))/2       distance from the midpoint
        H   = (HH(q) - LL(q))/2               half the range
        smi = 100 * EMA(EMA(M, r), u) / EMA(EMA(H, r), u)
        sig = EMA(smi, signalPeriod)

    Blau's own defaults are q=13, r=25, u=2, with a 3-bar signal. Bounded
    -100..100 because |M| <= H bar by bar and an EMA has non-negative
    weights. Both stages use pond's first-sample EMA seed over an ARRAY, so
    each steps over the previous stage's warm-up.

    Two separations, because the double-EMA-of-a-ratio shape survives both
    wrong turns: the SINGLE-smoothed form (only the r stage), and the short
    fork several charting packages ship (q=5, r=3, u=3).
    """
    hh = h.rolling(q).max()
    ll = low_s.rolling(q).min()
    distance = s - (hh + ll) / 2
    half_range = (hh - ll) / 2

    def double(x):
        return _ema_first_seed(_ema_first_seed(x, r), u)

    line = 100 * double(distance) / double(half_range)
    signal = _ema_first_seed(line, sig)

    label = f"stochasticMomentumIndex({q}, {r}, {u}, {sig})"
    expected = q + r + u - 3
    assert line.first_valid_index() == expected, (
        f"{label} first valid at {line.first_valid_index()}, expected "
        f"{expected} (the range at q-1, then each EMA stage)"
    )
    assert signal.first_valid_index() == expected + sig - 1, (
        f"{label} signal first valid at {signal.first_valid_index()}, "
        f"expected {expected + sig - 1}"
    )
    assert line.abs().max() <= 100 + 1e-9, (
        f"{label} breaks its own -100..100 bound at {line.abs().max()}"
    )
    assert line.min() < 0 < line.max(), (
        f"{label} never crosses zero on this fixture - the midpoint reading "
        "the study exists for would be untested"
    )

    # The UNSMOOTHED reading (100*M/H) is the strong probe: that is just a
    # rescaled stochastic, and it is what a build that dropped the smoothing
    # entirely would print.
    unsmoothed = 100 * distance / half_range
    m = (~line.isna()) & (~unsmoothed.isna())
    sep_raw = float(np.max(np.abs(line[m] - unsmoothed[m])))
    # Dropping only the SECOND stage is a much closer miss -- at Blau's u = 2
    # the finishing EMA is light -- so it gets its own, smaller, threshold.
    single = 100 * _ema_first_seed(distance, r) / _ema_first_seed(half_range, r)
    m = (~line.isna()) & (~single.isna())
    sep_single = float(np.max(np.abs(line[m] - single[m])))

    fork_hh = h.rolling(5).max()
    fork_ll = low_s.rolling(5).min()
    fork = 100 * _ema_first_seed(
        _ema_first_seed(s - (fork_hh + fork_ll) / 2, 3), 3
    ) / _ema_first_seed(_ema_first_seed((fork_hh - fork_ll) / 2, 3), 3)
    m = (~line.isna()) & (~fork.isna())
    sep_fork = float(np.max(np.abs(line[m] - fork[m])))
    assert sep_raw > 20 and sep_single > 1 and sep_fork > 20, (
        f"{label} sits {sep_raw} from the unsmoothed reading, {sep_single} "
        f"from the single-smoothed form and {sep_fork} from the (5, 3, 3) "
        "fork - the fixture cannot tell them apart"
    )
    print(
        f"  {label}: pandas replication (no TA-Lib SMI); first valid at "
        f"{expected}, range {line.min():.4f}..{line.max():.4f}, "
        f"{sep_raw:.4f} from the unsmoothed reading, {sep_single:.4f} from "
        f"the single-smoothed form and {sep_fork:.4f} from the (5, 3, 3) fork"
    )
    return {"smi": col(line), "smiSignal": col(signal)}


def _fisher_machine(x) -> tuple:
    """Ehlers' two recursions, transcribed (TASC November 2002):

        value = 0.33*x + 0.67*value[-1],  clamped: >0.99 -> 0.999, <-0.99 -> -0.999
        fish  = 0.5*ln((1+value)/(1-value)) + 0.5*fish[-1]

    A NaN input RESETS the machine (pond's foldRows rule), re-seeding both
    carried values at Ehlers' implicit zeros. The trigger is the PREVIOUS
    bar's fish and therefore exists only from the second bar of a run.
    """
    v = np.asarray(x, dtype=float)
    fish_out = np.full(len(v), np.nan)
    trig_out = np.full(len(v), np.nan)
    value = 0.0
    fish = 0.0
    run = 0
    for i, raw in enumerate(v):
        if not np.isfinite(raw):
            run = 0
            continue
        run += 1
        if run == 1:
            value = 0.0
            fish = 0.0
        value = 0.33 * raw + 0.67 * value
        if value > 0.99:
            value = 0.999
        elif value < -0.99:
            value = -0.999
        new_fish = 0.5 * math.log((1 + value) / (1 - value)) + 0.5 * fish
        fish_out[i] = new_fish
        if run >= 2:
            trig_out[i] = fish
        fish = new_fish
    return pd.Series(fish_out), pd.Series(trig_out)


def fisher_transform(n: int) -> dict:
    """Ehlers' Fisher Transform: the MEDIAN PRICE normalised over its OWN
    rolling extremes into (-1, 1), smoothed, clamped, pushed through
    0.5*ln((1+x)/(1-x)), and smoothed again.

    The range is over the median price series, not over high and low
    separately -- Ehlers' `Highest(Price, Len)` / `Lowest(Price, Len)`, where
    `Price = (H+L)/2`. Ports differ; the separation from that fork is
    measured below, and so is the separation from dropping the second
    (0.5/0.5) smoothing.
    """
    price = (h + low_s) / 2
    hh = price.rolling(n).max()
    ll = price.rolling(n).min()
    span = hh - ll
    # `pct/50 - 1` rather than `2*(p-LL)/span - 1`: the same number, in the
    # same floating-point ORDER the study computes it (percentOfRangeValues
    # then the rescale), so the transcription cannot drift by an ulp before
    # the recursion.
    x = pd.Series(np.where(span == 0, np.nan, 100 * (price - ll) / span) / 50 - 1)
    line, trigger = _fisher_machine(x)

    label = f"fisherTransform({n})"
    assert line.first_valid_index() == n - 1, (
        f"{label} first valid at {line.first_valid_index()}, expected {n - 1}"
    )
    assert trigger.first_valid_index() == n, (
        f"{label} trigger first valid at {trigger.first_valid_index()}, "
        f"expected {n} (the line delayed one bar)"
    )
    assert line.min() < 0 < line.max(), (
        f"{label} never crosses zero on this fixture"
    )

    bar_hh = h.rolling(n).max()
    bar_ll = low_s.rolling(n).min()
    bar_span = bar_hh - bar_ll
    port, _ = _fisher_machine(
        pd.Series(
            np.where(bar_span == 0, np.nan, 100 * (price - bar_ll) / bar_span)
            / 50
            - 1
        )
    )
    m = (~line.isna()) & (~port.isna())
    sep_port = float(np.max(np.abs(line[m] - port[m])))

    # The same machine with the SECOND smoothing dropped (fish = the raw
    # transform), which is the other common transcription slip.
    raw = np.full(len(x), np.nan)
    value = 0.0
    run = 0
    for i, v in enumerate(np.asarray(x, dtype=float)):
        if not np.isfinite(v):
            run = 0
            continue
        run += 1
        if run == 1:
            value = 0.0
        value = 0.33 * v + 0.67 * value
        if value > 0.99:
            value = 0.999
        elif value < -0.99:
            value = -0.999
        raw[i] = 0.5 * math.log((1 + value) / (1 - value))
    unsmoothed = pd.Series(raw)
    m = (~line.isna()) & (~unsmoothed.isna())
    sep_raw = float(np.max(np.abs(line[m] - unsmoothed[m])))
    assert sep_port > 0.1 and sep_raw > 0.3, (
        f"{label} sits {sep_port} from the high/low-extremes port and "
        f"{sep_raw} from the unsmoothed transform - the fixture cannot tell "
        "them apart"
    )
    print(
        f"  {label}: pandas TRANSCRIPTION of the same state machine (no "
        f"TA-Lib Fisher); first valid at {n - 1}, range "
        f"{line.min():.4f}..{line.max():.4f}, {sep_port:.4f} from the "
        f"high/low-extremes port and {sep_raw:.4f} from the unsmoothed form"
    )
    return {"fisher": col(line), "fisherSignal": col(trigger)}


def _half_smooth(x) -> pd.Series:
    """`x += 0.5*(raw - x)`, seeded on the first value of a run; a NaN RESETS
    it (pond's foldRows rule), where the common TradingView port instead
    holds the previous reading."""
    v = np.asarray(x, dtype=float)
    out = np.full(len(v), np.nan)
    prev = 0.0
    run = 0
    for i, raw in enumerate(v):
        if not np.isfinite(raw):
            run = 0
            continue
        run += 1
        prev = raw if run == 1 else prev + 0.5 * (raw - prev)
        out[i] = prev
    return pd.Series(out)


def _stoch_of(x, n: int) -> pd.Series:
    """100*(x - LL)/(HH - LL) over a STRICT n-bar window of a DERIVED series;
    a flat window is a 0/0 and reads null (percentOfRangeValues' rule)."""
    x = pd.Series(np.asarray(x, dtype=float))
    hh = x.rolling(n).max()
    ll = x.rolling(n).min()
    span = hh - ll
    return pd.Series(np.where(span == 0, np.nan, 100 * (x - ll) / span))


def schaff_trend_cycle(
    fast: int, slow: int, cycle: int, long_input: bool = False
) -> dict:
    """Doug Schaff's STC: a stochastic of the MACD, 0.5-smoothed, then a
    stochastic of THAT, 0.5-smoothed again. Defaults 23 / 50 / 10.

    Both EMAs are pond's first-sample seed, both stochastic passes take the
    STRICT window (a derived input), and both 0.5 recursions seed on their
    first value. The separation measured is from dropping BOTH recursions --
    the raw double stochastic, which is the shape a careless port produces.
    """
    src = long_s if long_input else s
    macd_line = _ema_first_seed(src, fast) - _ema_first_seed(src, slow)
    first_stage = _half_smooth(_stoch_of(macd_line, cycle))
    line = _half_smooth(_stoch_of(first_stage, cycle))

    label = f"schaffTrendCycle({fast}, {slow}, {cycle}, long={long_input})"
    # The EARLIEST the line can exist: the MACD at slow - 1, then two strict
    # cycle windows. It can legitimately start LATER, and on this fixture at
    # the defaults it does -- the MACD rises monotonically for sixteen bars,
    # which pins the first stochastic at 100, makes `d1` constant, and leaves
    # the SECOND window flat (a 0/0 -> null, the shared percentOfRangeValues
    # rule). So the assertion is the bound plus a proof that every null past
    # it is a flat second window rather than a lost bar.
    earliest = slow + 2 * cycle - 3
    first = line.first_valid_index()
    assert first >= earliest, (
        f"{label} first valid at {first}, EARLIER than the analytic bound "
        f"{earliest} (slow - 1, then two strict cycle windows)"
    )
    span2 = first_stage.rolling(cycle).max() - first_stage.rolling(cycle).min()
    for i in range(earliest, first):
        assert span2.iloc[i] == 0, (
            f"{label} is null at bar {i}, past the analytic bound "
            f"{earliest}, and the second stochastic window there is NOT flat "
            f"(span {span2.iloc[i]}) - that is a lost bar, not the "
            "flat-window rule"
        )
    if first > earliest:
        print(
            f"  {label}: the line starts at {first} rather than {earliest} - "
            f"{first - earliest} bars of flat SECOND window (the first "
            "stochastic pinned at 100 through a monotonic MACD leg)"
        )
    assert line.min() >= -1e-9 and line.max() <= 100 + 1e-9, (
        f"{label} leaves its 0..100 bound: {line.min()}..{line.max()}"
    )
    assert line.max() - line.min() > 50, (
        f"{label} spans only {line.max() - line.min()} on this fixture - a "
        "cycle oscillator that never swings would not test the recursions"
    )

    unsmoothed = _stoch_of(_stoch_of(macd_line, cycle), cycle)
    m = (~line.isna()) & (~unsmoothed.isna())
    sep_raw = float(np.max(np.abs(line[m] - unsmoothed[m])))
    assert sep_raw > 10, (
        f"{label} sits only {sep_raw} from the unsmoothed double stochastic"
    )
    print(
        f"  {label}: pandas TRANSCRIPTION of the same state machine (no "
        f"TA-Lib STC); first valid at {first} (bound {earliest}), range "
        f"{line.min():.4f}..{line.max():.4f}, {sep_raw:.4f} from the "
        "unsmoothed double stochastic"
    )
    return {"stc": col(line)}


def pretty_good_oscillator(n: int) -> dict:
    """Mark Johnson's PGO: (close - SMA(close, n)) / EMA(TR, n).

    F-AMBIG on the denominator: Johnson's is a SPAN EMA of true range, and
    the common port uses Wilder's ATR instead. The separation from that port
    is measured and asserted.
    """
    tr = _true_range()
    denominator = _ema_first_seed(tr, n)
    line = (s - s.rolling(n).mean()) / denominator

    label = f"prettyGoodOscillator({n})"
    assert line.first_valid_index() == n, (
        f"{label} first valid at {line.first_valid_index()}, expected {n} "
        "(TR[0] is undefined, so the EMA needs one extra bar)"
    )
    assert line.min() < 0 < line.max(), (
        f"{label} never crosses zero on this fixture"
    )

    wilder_port = (s - s.rolling(n).mean()) / _wilder(tr, n)
    m = (~line.isna()) & (~wilder_port.isna())
    sep_wilder = float(np.max(np.abs(line[m] - wilder_port[m])))
    assert sep_wilder > 0.05, (
        f"{label} sits only {sep_wilder} from the Wilder-ATR port"
    )
    print(
        f"  {label}: pandas replication (no TA-Lib PGO); first valid at {n}, "
        f"range {line.min():.4f}..{line.max():.4f}, {sep_wilder:.4f} from the "
        "Wilder-ATR denominator port"
    )
    return {"pgo": col(line)}


def _swing_index_series(limit: float) -> pd.Series:
    """Wilder's per-bar swing index (New Concepts, 1978), transcribed."""
    out = np.full(N, np.nan)
    for i in range(1, N):
        prev_close = closes[i - 1]
        prev_open = opens[i - 1]
        c, o, hi, lo = closes[i], opens[i], highs[i], lows[i]
        a = abs(hi - prev_close)
        b = abs(lo - prev_close)
        d = abs(hi - lo)
        yesterday = 0.25 * abs(prev_close - prev_open)
        if a >= b and a >= d:
            r = a - 0.5 * b + yesterday
        elif b >= a and b >= d:
            r = b - 0.5 * a + yesterday
        else:
            r = d + yesterday
        k = max(a, b)
        numerator = (c - prev_close) + 0.5 * (c - o) + 0.25 * (prev_close - prev_open)
        out[i] = math.nan if r == 0 else (50 * numerator / r) * (k / limit)
    return pd.Series(out)


def swing_index(limit: float, accumulative: bool) -> dict:
    """Wilder's Swing Index and its cumulative total.

    `limit` (Wilder's T, the instrument's limit move) is REQUIRED on the
    study -- there is no defensible default -- so the case names it. With a
    `limit` at least as large as the largest K on the fixture, the reading
    is bounded -100..100, which is the property the K/T factor exists for
    and is asserted here.

    Two separations: dropping the K/T factor entirely, and using the
    plain-range branch of R unconditionally (the two ways a transcription
    goes wrong while keeping the shape).
    """
    si = _swing_index_series(limit)
    label = f"swingIndex({limit}, accumulative={accumulative})"
    assert si.first_valid_index() == 1, (
        f"{label} first valid at {si.first_valid_index()}, expected 1"
    )
    max_k = max(
        max(abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        for i in range(1, N)
    )
    if limit >= max_k:
        assert si.abs().max() <= 100 + 1e-9, (
            f"{label} breaks the -100..100 bound at {si.abs().max()} even "
            f"though limit {limit} >= max K {max_k}"
        )
    assert si.min() < 0 < si.max(), f"{label} never changes sign"

    no_k = np.full(N, np.nan)
    plain_r = np.full(N, np.nan)
    for i in range(1, N):
        prev_close, prev_open = closes[i - 1], opens[i - 1]
        c, o, hi, lo = closes[i], opens[i], highs[i], lows[i]
        a, b, d = (
            abs(hi - prev_close),
            abs(lo - prev_close),
            abs(hi - lo),
        )
        yesterday = 0.25 * abs(prev_close - prev_open)
        r = (
            a - 0.5 * b + yesterday
            if (a >= b and a >= d)
            else (b - 0.5 * a + yesterday if (b >= a and b >= d) else d + yesterday)
        )
        numerator = (c - prev_close) + 0.5 * (c - o) + 0.25 * (prev_close - prev_open)
        if r != 0:
            no_k[i] = 50 * numerator / r
        r2 = d + yesterday
        if r2 != 0:
            plain_r[i] = (50 * numerator / r2) * (max(a, b) / limit)
    sep_no_k = float(np.nanmax(np.abs(si.to_numpy() - no_k)))
    sep_plain = float(np.nanmax(np.abs(si.to_numpy() - plain_r)))
    assert sep_no_k > 1 and sep_plain > 1, (
        f"{label} sits {sep_no_k} from the no-K/T form and {sep_plain} from "
        "the plain-range R form - the fixture cannot tell them apart"
    )

    if accumulative:
        asi = si.cumsum()
        print(
            f"  {label}: pandas replication (no TA-Lib ASI); si first valid "
            f"at 1, asi range {asi.min():.4f}..{asi.max():.4f}"
        )
        return {"asi": col(asi)}
    print(
        f"  {label}: pandas replication (no TA-Lib SI); first valid at 1, "
        f"range {si.min():.4f}..{si.max():.4f}, max K {max_k:.4f}, "
        f"{sep_no_k:.4f} from the no-K/T form and {sep_plain:.4f} from the "
        "plain-range R form"
    )
    return {"si": col(si)}


def _rwi_values(period: int, denominator: str = "mean") -> tuple:
    """Poulos' Random Walk Index: the max over horizons 2..period of
    (high - low[-n]) / (meanTR(n) * sqrt(n)), and its mirror."""
    tr = _true_range()
    hi = np.full(N, -np.inf)
    lo = np.full(N, -np.inf)
    for n in range(2, period + 1):
        base = (
            tr.rolling(n).mean().to_numpy()
            if denominator == "mean"
            else _wilder(tr, n).to_numpy()
        )
        den = base * math.sqrt(n)
        with np.errstate(invalid="ignore", divide="ignore"):
            th = np.where(
                den == 0,
                np.nan,
                (h.to_numpy() - low_s.shift(n).to_numpy()) / den,
            )
            tl = np.where(
                den == 0,
                np.nan,
                (h.shift(n).to_numpy() - low_s.to_numpy()) / den,
            )
        th[:n] = np.nan
        tl[:n] = np.nan
        hi = np.maximum(hi, th)
        lo = np.maximum(lo, tl)
    return pd.Series(hi), pd.Series(lo)


def random_walk_index(period: int) -> dict:
    """Poulos' RWI over horizons 2..period, with the n-bar MEAN true range in
    the denominator (not Wilder's ATR -- see the study's docstring).

    Two separations: the Wilder-denominator variant, and the SINGLE-horizon
    form that only tests n = period.
    """
    rwi_high, rwi_low = _rwi_values(period)
    label = f"randomWalkIndex({period})"
    assert rwi_high.first_valid_index() == period, (
        f"{label} rwiHigh first valid at {rwi_high.first_valid_index()}, "
        f"expected {period}"
    )
    assert rwi_low.first_valid_index() == period, (
        f"{label} rwiLow first valid at {rwi_low.first_valid_index()}, "
        f"expected {period}"
    )
    assert rwi_high.max() > 1 and rwi_high.min() < 1, (
        f"{label} never crosses the 1.0 random-walk line on this fixture - "
        "the only reading the study has would be untested"
    )

    wilder_high, _ = _rwi_values(period, denominator="wilder")
    m = (~rwi_high.isna()) & (~wilder_high.isna())
    sep_wilder = float(np.max(np.abs(rwi_high[m] - wilder_high[m])))

    tr = _true_range()
    den = tr.rolling(period).mean() * math.sqrt(period)
    single = pd.Series(
        np.where(
            den.to_numpy() == 0,
            np.nan,
            (h.to_numpy() - low_s.shift(period).to_numpy()) / den.to_numpy(),
        )
    )
    m = (~rwi_high.isna()) & (~single.isna())
    sep_single = float(np.max(np.abs(rwi_high[m] - single[m])))
    # The Wilder threshold is deliberately small: at long horizons Wilder's
    # recursion and the n-bar mean converge, so the separation SHRINKS with
    # `period` (0.19 at 14, 0.031 at 30 on this fixture). Both are many
    # orders of magnitude above the suite's 1e-9 tolerance, which is what
    # "the fixture can tell them apart" means.
    assert sep_wilder > 0.01 and sep_single > 0.1, (
        f"{label} sits {sep_wilder} from the Wilder-denominator variant and "
        f"{sep_single} from the single-horizon form"
    )
    print(
        f"  {label}: pandas replication (no TA-Lib RWI); first valid at "
        f"{period}, rwiHigh range {rwi_high.min():.4f}..{rwi_high.max():.4f}, "
        f"{sep_wilder:.4f} from the Wilder-denominator variant and "
        f"{sep_single:.4f} from the single-horizon form"
    )
    return {"rwiHigh": col(rwi_high), "rwiLow": col(rwi_low)}


def ravi_study(short_n: int, long_n: int, long_input: bool = False) -> dict:
    """Chande's RAVI: 100*|SMA(short) - SMA(long)| / SMA(long), 7 / 65."""
    src = long_s if long_input else s
    short_ma = src.rolling(short_n).mean()
    long_ma = src.rolling(long_n).mean()
    line = 100 * (short_ma - long_ma).abs() / long_ma

    label = f"ravi({short_n}, {long_n}, long={long_input})"
    expected = max(short_n, long_n) - 1
    assert line.first_valid_index() == expected, (
        f"{label} first valid at {line.first_valid_index()}, expected "
        f"{expected}"
    )
    assert line.min() >= 0, f"{label} must be non-negative"

    signed = 100 * (short_ma - long_ma) / long_ma
    m = (~line.isna()) & (~signed.isna())
    sep_signed = float(np.max(np.abs(line[m] - signed[m])))
    assert sep_signed > 0.5, (
        f"{label} sits only {sep_signed} from the SIGNED form - the fixture "
        "never puts the fast average below the slow one"
    )
    print(
        f"  {label}: pandas replication (no TA-Lib RAVI); first valid at "
        f"{expected}, range {line.min():.4f}..{line.max():.4f}, "
        f"{sep_signed:.4f} from the signed form"
    )
    return {"ravi": col(line)}


def trend_intensity_index(n: int, ma_n: int, long_input: bool) -> dict:
    """M. H. Pee's TII (TASC June 2002): 100 * SDpos / (SDpos + SDneg), where
    the deviations are close - SMA(close, ma_n) over the last n bars.

    F-AMBIG: the common simplification COUNTS the positive deviations instead
    of summing them. The separation from that count form is measured.
    """
    src = long_s if long_input else s
    deviation = src - src.rolling(ma_n).mean()
    up = deviation.clip(lower=0)
    down = (-deviation).clip(lower=0)
    up_sum = up.rolling(n).sum()
    down_sum = down.rolling(n).sum()
    total = up_sum + down_sum
    line = pd.Series(np.where(total == 0, np.nan, 100 * up_sum / total))

    label = f"trendIntensityIndex({n}, {ma_n}, long={long_input})"
    expected = ma_n + n - 2
    assert line.first_valid_index() == expected, (
        f"{label} first valid at {line.first_valid_index()}, expected "
        f"{expected} (the average at ma_n - 1, then n finite deviations)"
    )
    assert line.min() >= -1e-9 and line.max() <= 100 + 1e-9, (
        f"{label} leaves its 0..100 bound: {line.min()}..{line.max()}"
    )
    assert (line < 50).any() and (line > 50).any(), (
        f"{label} stays on one side of 50 - the reading would be untested"
    )

    count = 100 * (deviation > 0).rolling(n).mean()
    count[deviation.rolling(n).count() < n] = np.nan
    count[deviation.isna().rolling(n).sum() > 0] = np.nan
    m = (~line.isna()) & (~count.isna())
    sep_count = float(np.max(np.abs(line[m] - count[m])))
    assert sep_count > 10, (
        f"{label} sits only {sep_count} from the COUNT form"
    )
    print(
        f"  {label}: pandas replication (no TA-Lib TII); first valid at "
        f"{expected}, range {line.min():.4f}..{line.max():.4f}, "
        f"{sep_count:.4f} from the count form"
    )
    return {"tii": col(line)}


SPECIAL_K_ROC = [10, 15, 20, 30, 40, 65, 75, 100, 195, 265, 390, 530]
SPECIAL_K_SMOOTHING = [10, 10, 10, 15, 50, 65, 75, 100, 130, 130, 130, 195]
SPECIAL_K_WEIGHTS = [1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4]


def special_k() -> dict:
    """Pring's Special K -- the extended KST, twelve weighted terms across
    three groups. Computed on the LONG 900-bar input, because the slowest
    term (a 530-bar ROC smoothed over 195) first prints on bar 724.

    Two separations, the same two the kst case uses because the shape
    survives both: an EQUAL-weight sum, and a sum of UNSMOOTHED rates of
    change.
    """
    total = None
    equal = None
    unsmoothed = None
    for roc_n, smooth_n, weight in zip(
        SPECIAL_K_ROC, SPECIAL_K_SMOOTHING, SPECIAL_K_WEIGHTS
    ):
        term = (long_s.pct_change(roc_n) * 100).rolling(smooth_n).mean()
        total = term * weight if total is None else total + term * weight
        equal = term if equal is None else equal + term
        u = long_s.pct_change(roc_n) * 100 * weight
        unsmoothed = u if unsmoothed is None else unsmoothed + u
    line = total

    label = "specialK()"
    expected = max(
        r + m for r, m in zip(SPECIAL_K_ROC, SPECIAL_K_SMOOTHING)
    ) - 1
    assert expected == 724, f"the Special K table changed: warm-up {expected}"
    assert line.first_valid_index() == expected, (
        f"{label} first valid at {line.first_valid_index()}, expected "
        f"{expected} (the slowest term's roc + smooth - 1)"
    )
    assert line.notna().sum() > 100, (
        f"{label} has only {line.notna().sum()} values on the long input - "
        "the case would barely test anything"
    )
    assert line.min() < 0 < line.max(), (
        f"{label} never crosses zero on the long fixture"
    )

    m = (~line.isna()) & (~equal.isna())
    sep_equal = float(np.max(np.abs(line[m] - equal[m])))
    m = (~line.isna()) & (~unsmoothed.isna())
    sep_raw = float(np.max(np.abs(line[m] - unsmoothed[m])))
    assert sep_equal > 5 and sep_raw > 5, (
        f"{label} sits {sep_equal} from the equal-weight sum and {sep_raw} "
        "from the unsmoothed one"
    )
    print(
        f"  {label}: pandas replication (no TA-Lib Special K) on the LONG "
        f"input; first valid at {expected} of {LONG_N}, range "
        f"{line.min():.4f}..{line.max():.4f}, {sep_equal:.4f} from the "
        f"equal-weight sum and {sep_raw:.4f} from the unsmoothed one"
    )
    return {"specialK": col(line)}


# --------------------------------------------------------------------------
# The volume and miscellaneous leftovers (assessment 6.6 / 6.4 / 6.1).
#
# None of these has a TA-Lib function, so every one is a pandas replication
# with (a) the ANALYTIC first valid bar asserted and (b) a measured separation
# from the plausible wrong turn -- the discipline the phase-2 brief asks for
# where no vendor reference exists. Two of them (`tradeVolumeIndex`,
# `movingAverageCross`) are state machines, where the "independent
# implementation" claim is weaker: the loop below is a TRANSCRIPTION of the
# same recurrence the TypeScript runs, so it proves the transcription and the
# plumbing, and the SEPARATION probes are what carry the weight.
# --------------------------------------------------------------------------


def _true_bounds() -> tuple:
    """Twiggs' true high / true low: max(high, prevClose) / min(low, prevClose).

    Bar 0 has no previous close, so both are NaN there -- `pd.concat().max()`
    would skip the NaN and return the high, which is NOT the definition, so
    the mask is applied explicitly.
    """
    prev_c = s.shift(1)
    trh = pd.concat([h, prev_c], axis=1).max(axis=1)
    trl = pd.concat([low_s, prev_c], axis=1).min(axis=1)
    return trh.where(prev_c.notna()), trl.where(prev_c.notna())


def twiggs_money_flow(n: int = 21) -> dict:
    """Twiggs Money Flow: Wilder(flow, n) / Wilder(volume, n), where the flow
    is the close's location in the bar's TRUE range times volume.

    No TA-Lib function. Two separations are asserted: from the WINDOW-SUM form
    (the F-AMBIG fork -- Chaikin's averaging on Twiggs' range) and from
    `chaikinMoneyFlow` itself at the same period (Twiggs' range correction).
    The denominator is blanked wherever the numerator is, which is what the
    TypeScript does and what keeps the two smoothings on the same bars.
    """
    trh, trl = _true_bounds()
    flow = vol * ((s - trl) - (trh - s)) / (trh - trl)
    weight = vol.where(flow.notna())
    v = _wilder(flow, n) / _wilder(weight, n)
    label = f"twiggsMoneyFlow({n})"

    assert v.first_valid_index() == n, (
        f"{label} first valid at {v.first_valid_index()}, expected {n} "
        "(bar 0 has no true range, so the Wilder seed lands one bar late)"
    )
    assert float(np.nanmax(np.abs(v))) <= 1.0, f"{label} left [-1, +1]"
    assert v.min() < 0 < v.max(), (
        f"{label} does not change sign on this fixture - the accumulation / "
        "distribution reading would be one-sided"
    )

    window = flow.rolling(n).sum() / weight.rolling(n).sum()
    m = v.notna() & window.notna()
    sep_window = float(np.max(np.abs(v[m] - window[m])))
    cmf = (_clv() * vol).rolling(n).sum() / vol.rolling(n).sum()
    m = v.notna() & cmf.notna()
    sep_cmf = float(np.max(np.abs(v[m] - cmf[m])))
    assert sep_window > 0.01 and sep_cmf > 0.01, (
        f"{label} sits {sep_window} from the window-sum form and {sep_cmf} "
        "from Chaikin Money Flow - the fixture cannot tell them apart"
    )
    print(
        f"  {label}: pandas replication (no TA-Lib TMF); first valid at {n}, "
        f"range {v.min():.4f}..{v.max():.4f}, {sep_window:.4f} from the "
        f"window-sum form and {sep_cmf:.4f} from chaikinMoneyFlow({n})"
    )
    return {"tmf": col(v)}


def _tvi_values(min_tick: float, seed_direction: int = 0, persist: bool = True):
    """The Trade Volume Index recurrence -- a TRANSCRIPTION of the machine the
    TypeScript runs, so the separations below are what carry the weight.

    `seed_direction` is the vendor fork that assumes an initial UP tick;
    `persist=False` is the fork that treats an undecided bar as no trade at
    all (which is `obv` with a dead band, not this study).
    """
    c = np.asarray(closes, dtype=float)
    v = np.asarray(volumes, dtype=float)
    out = np.full(len(c), np.nan)
    level = 0.0
    direction = seed_direction
    undecided = 0
    for i in range(len(c)):
        if i > 0:
            move = c[i] - c[i - 1]
            if move > min_tick:
                direction = 1
            elif move < -min_tick:
                direction = -1
            else:
                undecided += 1
                if not persist:
                    direction = 0
            level += direction * v[i]
        out[i] = level
    return pd.Series(out), undecided


def trade_volume_index(min_tick: float, min_undecided: int = 5) -> dict:
    v, undecided = _tvi_values(min_tick)
    label = f"tradeVolumeIndex({min_tick})"

    assert v.first_valid_index() == 0, f"{label} must be defined from bar 0"
    assert v.iloc[0] == 0.0, f"{label} must open at 0"
    assert undecided >= min_undecided, (
        f"{label} has only {undecided} bars inside the dead band - the "
        "PERSISTENCE rule, which is the whole study, would be untested"
    )

    no_persist, _ = _tvi_values(min_tick, persist=False)
    sep_persist = float(np.max(np.abs(v - no_persist)))
    assert sep_persist > 1, (
        f"{label} sits {sep_persist} from the non-persisting fork - the "
        "fixture cannot tell them apart"
    )
    # The OTHER fork - seeding the direction UP instead of leaving it
    # undecided - is invisible on this fixture, and that is asserted rather
    # than assumed: bar 1's move (1.412) is the LARGEST in the series, so it
    # is decisive at every min_tick worth testing and the seed never gets a
    # bar to act on. It is pinned TypeScript-side instead, on a fixture whose
    # first bars sit inside the dead band (the negativeVolumeIndex precedent
    # for a rule the oracle input cannot exercise).
    seeded, _ = _tvi_values(min_tick, seed_direction=1)
    assert float(np.max(np.abs(v - seeded))) == 0.0, (
        f"{label}: bar 1 is no longer decisive, so the up-seeded fork IS "
        "visible here now - assert the separation instead of this"
    )
    print(
        f"  {label}: pandas TRANSCRIPTION of the same state machine (no "
        f"TA-Lib TVI); opens at 0, {undecided} dead-band bars, range "
        f"{v.min():.0f}..{v.max():.0f}, {sep_persist:.0f} from the "
        "non-persisting fork (the up-seeded fork is invisible here - see "
        "the note)"
    )
    return {"tvi": col(v)}


def shinohara_intensity_ratio(n: int = 26) -> dict:
    """Shinohara's A and B ratios: 100 * sum(up) / sum(down) over n bars, the
    A pair measured against the OPEN and the B pair against the PREVIOUS
    close.

    No TA-Lib function. Two separations are asserted: between the two
    columns themselves (a build that swapped the `strong` / `weak` labels -
    the F-AMBIG fork - would be wrong by most of the scale) and from the
    B-against-the-SAME-bar's-close misreading, which is the wrong turn that
    leaves the shape intact.
    """
    prev_c = s.shift(1)
    strong = 100 * (h - o_s).rolling(n).sum() / (o_s - low_s).rolling(n).sum()
    weak = 100 * (h - prev_c).rolling(n).sum() / (prev_c - low_s).rolling(n).sum()
    label = f"shinoharaIntensityRatio({n})"

    assert strong.first_valid_index() == n - 1, (
        f"{label} strong first valid at {strong.first_valid_index()}, "
        f"expected {n - 1}"
    )
    assert weak.first_valid_index() == n, (
        f"{label} weak first valid at {weak.first_valid_index()}, expected "
        f"{n} (bar 0 has no previous close, so the window starts one later)"
    )
    assert strong.min() < 100 < strong.max(), (
        f"{label} strong never crosses its neutral 100 on this fixture"
    )

    m = strong.notna() & weak.notna()
    sep_swap = float(np.max(np.abs(strong[m] - weak[m])))
    same_close = 100 * (h - s).rolling(n).sum() / (s - low_s).rolling(n).sum()
    m = weak.notna() & same_close.notna()
    sep_same = float(np.max(np.abs(weak[m] - same_close[m])))
    assert sep_swap > 10 and sep_same > 10, (
        f"{label}: the two columns sit {sep_swap} apart and the "
        f"same-bar-close misreading {sep_same} away - the fixture cannot "
        "tell the conventions apart"
    )
    print(
        f"  {label}: pandas replication (no TA-Lib Shinohara); strong first "
        f"valid at {n - 1} spanning {strong.min():.2f}..{strong.max():.2f}, "
        f"weak at {n} spanning {weak.min():.2f}..{weak.max():.2f}; the two "
        f"columns sit {sep_swap:.2f} apart and the same-bar-close "
        f"misreading {sep_same:.2f} away"
    )
    return {"sirStrong": col(strong), "sirWeak": col(weak)}


def elder_impulse(
    ema_n: int = 13, fast: int = 12, slow: int = 26, signal: int = 9
) -> dict:
    """Elder's Impulse System: +1 when EMA(ema_n) AND the MACD histogram both
    rose, -1 when both fell, 0 otherwise.

    No TA-Lib function. Every EMA is POND's first-sample seed (the macd
    precedent), so the histogram here is the same one `macd` produces. Two
    separations are asserted, and because the output is a three-valued column
    a max-abs distance says nothing useful - what is measured is the COUNT of
    bars on which each single-input build disagrees.
    """
    trend = _ema_first_seed(closes, ema_n)
    line = _ema_first_seed(closes, fast) - _ema_first_seed(closes, slow)
    sig = _ema_first_seed(line, signal)
    hist = line - sig

    def verdict(a: pd.Series, b: pd.Series) -> pd.Series:
        up = (a.diff() > 0) & (b.diff() > 0)
        down = (a.diff() < 0) & (b.diff() < 0)
        v = pd.Series(np.where(up, 1.0, np.where(down, -1.0, 0.0)))
        mask = a.notna() & a.shift(1).notna() & b.notna() & b.shift(1).notna()
        return v.where(mask)

    v = verdict(trend, hist)
    label = f"elderImpulse({ema_n}, {fast}, {slow}, {signal})"

    # Both inputs need TWO values: the EMA has them from bar `ema_n` (it
    # first prints at ema_n - 1) and the histogram from slow + signal - 1
    # (it first prints at slow + signal - 2).
    expected = max(ema_n, slow + signal - 1)
    assert v.first_valid_index() == expected, (
        f"{label} first valid at {v.first_valid_index()}, expected "
        f"{expected} (both inputs need TWO values)"
    )
    assert set(v.dropna().unique()) <= {-1.0, 0.0, 1.0}, (
        f"{label} emitted a value outside {{-1, 0, +1}}"
    )
    for want in (-1.0, 0.0, 1.0):
        assert (v == want).sum() > 0, (
            f"{label} never reads {want} on this fixture - the case would "
            "not exercise the whole rule"
        )

    # The two single-input builds: the EMA slope alone, and the histogram
    # slope alone. Each is a plausible misreading of "trend and momentum
    # agree", and each collapses the 0 band.
    ema_only = pd.Series(
        np.where(trend.diff() > 0, 1.0, np.where(trend.diff() < 0, -1.0, 0.0))
    ).where(v.notna())
    hist_only = pd.Series(
        np.where(hist.diff() > 0, 1.0, np.where(hist.diff() < 0, -1.0, 0.0))
    ).where(v.notna())
    m = v.notna()
    diff_ema = int((v[m] != ema_only[m]).sum())
    diff_hist = int((v[m] != hist_only[m]).sum())
    assert diff_ema >= 5 and diff_hist >= 5, (
        f"{label} differs from the EMA-only build on {diff_ema} bars and "
        f"from the histogram-only build on {diff_hist} - too few to tell "
        "them apart"
    )
    print(
        f"  {label}: pandas replication (no TA-Lib Elder Impulse); first "
        f"valid at {expected}, {int((v == 1).sum())} up / "
        f"{int((v == 0).sum())} neutral / {int((v == -1).sum())} down bars; "
        f"differs from the EMA-only build on {diff_ema} of {int(m.sum())} "
        f"bars and from the histogram-only build on {diff_hist}"
    )
    return {"impulse": col(v)}


def _cross_values(fast_ma, slow_ma, adjacent: bool = False, delay: int = 0):
    """The cross machine as a TRANSCRIPTION of the TypeScript step.

    `adjacent=True` is the naive rule (compare against the PREVIOUS sign
    rather than the last NON-ZERO one), which double-reports a crossing that
    passes through an exact tie and invents one on a touch-and-retreat.
    `delay=1` reports the event one bar late - the off-by-one that leaves the
    shape intact.
    """
    f = np.asarray(fast_ma, dtype=float)
    sl = np.asarray(slow_ma, dtype=float)
    out = np.full(len(f), np.nan)
    last = 0
    prev_sign = None
    run = 0
    for i in range(len(f)):
        if not (np.isfinite(f[i]) and np.isfinite(sl[i])):
            run = 0
            last = 0
            prev_sign = None
            continue
        run += 1
        d = f[i] - sl[i]
        sign = 1 if d > 0 else (-1 if d < 0 else 0)
        if run == 1:
            last = sign
            prev_sign = sign
            continue
        if adjacent:
            out[i] = sign if (sign != prev_sign) else 0
        elif sign == 0:
            out[i] = 0.0
        elif last == 0:
            last = sign
            out[i] = 0.0
        elif sign != last:
            last = sign
            out[i] = float(sign)
        else:
            out[i] = 0.0
        prev_sign = sign
    v = pd.Series(out)
    return v.shift(delay) if delay else v


def moving_average_cross(
    fast: int = 10, slow: int = 30, kind: str = "sma", long: bool = False
) -> dict:
    """Moving Average Cross: +1 on the bar the fast average crosses above the
    slow one, -1 below, 0 otherwise.

    No TA-Lib function. A TRANSCRIPTION of the same machine, so the weight is
    on the separations: from the REGIME column (`sign(fast - slow)`, which is
    a different reading entirely) and from the one-bar-late report. The tie
    rule cannot be separated on this fixture - two floating-point averages
    are never exactly equal on it - and that is asserted rather than assumed;
    it is pinned by the TypeScript unit tests on constructed input.
    """
    source = long_closes if long else closes
    f = _ma_over(source, kind, fast)
    sl = _ma_over(source, kind, slow)
    v = _cross_values(f, sl)
    label = f"movingAverageCross({fast}, {slow}, {kind}, long={long})"

    first = slow  # the slow average prints at slow - 1; the seed bar reports nothing
    assert v.first_valid_index() == first, (
        f"{label} first valid at {v.first_valid_index()}, expected {first} "
        "(the bar after both averages exist - the seed reports nothing)"
    )
    ups = int((v == 1).sum())
    downs = int((v == -1).sum())
    assert ups >= 1 and downs >= 1, (
        f"{label} has {ups} up-crosses and {downs} down-crosses on this "
        "fixture - it does not exercise both directions"
    )
    assert set(v.dropna().unique()) <= {-1.0, 0.0, 1.0}, (
        f"{label} emitted a value outside {{-1, 0, +1}}"
    )

    regime = pd.Series(np.sign(np.asarray(f, dtype=float) - np.asarray(sl, dtype=float))).where(v.notna())
    m = v.notna()
    diff_regime = int((v[m] != regime[m]).sum())
    late = _cross_values(f, sl, delay=1)
    diff_late = int((v[m] != late[m].fillna(0)).sum())
    assert diff_regime >= 20 and diff_late >= 2, (
        f"{label} differs from the regime column on {diff_regime} bars and "
        f"from the one-bar-late report on {diff_late} - too few"
    )

    # The tie rule: assert the naive adjacent-sign build AGREES here, so the
    # day the fixture grows an exact tie this stops being silently untested.
    adjacent = _cross_values(f, sl, adjacent=True)
    assert int((v[m] != adjacent[m]).sum()) == 0, (
        f"{label}: the fixture now contains an exact tie, so the naive "
        "adjacent-sign rule IS separable here - assert the separation"
    )
    print(
        f"  {label}: pandas TRANSCRIPTION of the same machine (no TA-Lib MA "
        f"cross); first valid at {first}, {ups} up / {downs} down crosses; "
        f"differs from the regime column on {diff_regime} of {int(m.sum())} "
        f"bars and from the one-bar-late report on {diff_late} (the tie rule "
        "is pinned TypeScript-side - no exact tie here)"
    )
    return {"maCross": col(v)}


def anchored_vwap(anchor: int) -> dict:
    """Anchored VWAP: cumsum(typicalPrice * volume) / cumsum(volume) from the
    first bar at or after `anchor`, with earlier bars null.

    The oracle series uses the ROW INDEX as its epoch-millisecond key, so an
    anchor of `n` is the bar at index `n`. No TA-Lib function; separated from
    the ROLLING vwap over the same span and from the unweighted cumulative
    mean of typical price.
    """
    tp = (h + low_s + s) / 3
    mask = pd.Series(range(len(closes))) >= anchor
    flow = (tp * vol).where(mask)
    weight = vol.where(mask)
    v = flow.cumsum() / weight.cumsum()
    label = f"anchoredVwap({anchor})"

    assert v.first_valid_index() == anchor, (
        f"{label} first valid at {v.first_valid_index()}, expected {anchor}"
    )
    assert abs(float(v.iloc[anchor]) - float(tp.iloc[anchor])) < 1e-9, (
        f"{label} must open at the anchor bar's own typical price"
    )

    # The rolling VWAP over the SAME number of bars is a different line at
    # every bar but the first, and the unweighted cumulative mean of typical
    # price is what a build that dropped the weighting would give.
    span = len(closes) - anchor
    rolling = (tp * vol).rolling(span).sum() / vol.rolling(span).sum()
    m = v.notna() & rolling.notna()
    sep_rolling = float(np.max(np.abs(v[m] - rolling[m])))
    unweighted = tp.where(mask).expanding().mean()
    m = v.notna() & unweighted.notna()
    sep_plain = float(np.max(np.abs(v[m] - unweighted[m])))
    assert sep_plain > 0.1, (
        f"{label} sits {sep_plain} from the unweighted cumulative mean - the "
        "fixture's volume is too flat to catch a dropped weighting"
    )
    if anchor > 0:
        assert sep_rolling > 0.1, (
            f"{label} sits {sep_rolling} from the rolling VWAP over the same "
            "span - the fixture cannot tell the two forms apart"
        )
    else:
        # Anchored at bar 0 the rolling window over the same span has exactly
        # ONE value, at the last bar, and the anchored line agrees with it
        # there by definition. Asserted rather than skipped: it is the one
        # place the two forms provably coincide, and vwap()'s docstring says
        # so ("period = length yields one value at the last bar").
        assert sep_rolling == 0.0, (
            f"{label} disagrees with the rolling VWAP at the one bar they "
            f"share ({sep_rolling})"
        )
    print(
        f"  {label}: pandas replication (no TA-Lib anchored VWAP); first "
        f"valid at {anchor}, range {v.min():.4f}..{v.max():.4f}, "
        f"{sep_rolling:.4f} from the rolling VWAP over the same span and "
        f"{sep_plain:.4f} from the unweighted cumulative mean"
    )
    return {"avwap": col(v)}


def _assert_talib_exact(ours: pd.Series, ref_values, label: str) -> float:
    """Mask FIRST, then values. `nanmax(|a-b|)` is blind to a one-sided NaN,
    so a study that warmed up a bar early would pass a values-only check."""
    ref = pd.Series(ref_values)
    assert list(ours.isna()) == list(ref.isna()), (
        f"{label} warm-up differs from TA-Lib: ours first valid "
        f"{ours.first_valid_index()}, TA-Lib {ref.first_valid_index()}"
    )
    delta = float(np.nanmax(np.abs(ours - ref)))
    assert delta == 0.0, f"{label} disagrees with TA-Lib by {delta} (want EXACT)"
    return delta


_H = np.asarray(highs, dtype=float)
_L = np.asarray(lows, dtype=float)
_C = np.asarray(closes, dtype=float)
_O = np.asarray(opens, dtype=float)


def typical_price() -> dict:
    """Typical price (h+l+c)/3 -- TA-Lib TYPPRICE, EXACT, no warm-up."""
    v = (h + low_s + s) / 3
    assert v.first_valid_index() == 0, "typicalPrice must be defined on bar 0"
    if talib is not None:
        d = _assert_talib_exact(v, talib.TYPPRICE(_H, _L, _C), "typicalPrice")
        print(f"  typicalPrice: matches TA-Lib TYPPRICE EXACTLY ({d}), no warm-up")
    return {"typicalPrice": col(v)}


def median_price() -> dict:
    """Median price (h+l)/2 -- TA-Lib MEDPRICE, EXACT, no warm-up."""
    v = (h + low_s) / 2
    assert v.first_valid_index() == 0, "medianPrice must be defined on bar 0"
    if talib is not None:
        d = _assert_talib_exact(v, talib.MEDPRICE(_H, _L), "medianPrice")
        print(f"  medianPrice: matches TA-Lib MEDPRICE EXACTLY ({d}), no warm-up")
    # It must NOT coincide with the typical price on this fixture, or the
    # two cases could not tell one transform from the other.
    sep = float(np.max(np.abs(v - (h + low_s + s) / 3)))
    assert sep > 0.05, (
        f"medianPrice sits only {sep} from typicalPrice on this fixture -- "
        "the two transforms would be indistinguishable"
    )
    return {"medianPrice": col(v)}


def weighted_close() -> dict:
    """Weighted close (h+l+2c)/4 -- TA-Lib WCLPRICE, EXACT, no warm-up.

    The SUMMATION ORDER is TA-Lib's, `h + l + c*2`, and so is ours: addition
    does not associate, so a rearrangement costs the exactness asserted here.
    """
    v = (h + low_s + s * 2) / 4
    assert v.first_valid_index() == 0, "weightedClose must be defined on bar 0"
    # The separation from typicalPrice is `(c - (h+l)/2) / 6`, and this
    # fixture's bars are deliberately NARROW (see the highs/lows note), so
    # it is small in absolute terms -- 0.0417 at its widest. That is still
    # ~8e7 times the 1e-9 tolerance the vitest oracle compares at, so the
    # case does tell the two transforms apart; the threshold below says so
    # explicitly rather than pretending the gap is large.
    sep = float(np.max(np.abs(v - (h + low_s + s) / 3)))
    assert sep > 1e-3, (
        f"weightedClose sits only {sep} from typicalPrice -- within reach of "
        "the oracle's 1e-9 comparison, so the fixture could not tell the "
        "close's weight apart"
    )
    if talib is not None:
        d = _assert_talib_exact(v, talib.WCLPRICE(_H, _L, _C), "weightedClose")
        print(
            f"  weightedClose: matches TA-Lib WCLPRICE EXACTLY ({d}), no "
            f"warm-up; {sep:.4f} from typicalPrice (narrow bars, but 1e7x the "
            "oracle tolerance)"
        )
    return {"weightedClose": col(v)}


def average_price() -> dict:
    """Average price (o+h+l+c)/4 -- TA-Lib AVGPRICE, EXACT, no warm-up.

    TA-Lib sums `high + low + close + open`, and so do we. Summing in OHLC
    order instead is a DIFFERENT floating-point result -- measured below and
    asserted to be non-zero, which is why the kernel's comment names the
    order as load-bearing rather than incidental.
    """
    v = (h + low_s + s + o_s) / 4
    assert v.first_valid_index() == 0, "averagePrice must be defined on bar 0"
    if talib is not None:
        d = _assert_talib_exact(v, talib.AVGPRICE(_O, _H, _L, _C), "averagePrice")
        # The OHLC-order sum, for the record: same value to a chart, a
        # different double.
        ohlc_order = (o_s + h + low_s + s) / 4
        order_gap = float(np.max(np.abs(v - ohlc_order)))
        assert order_gap > 0.0, (
            "the OHLC summation order happens to agree bit-for-bit on this "
            "fixture -- the kernel's 'order is load-bearing' note would be "
            "unsupported; pick a fixture where it is not"
        )
        print(
            f"  averagePrice: matches TA-Lib AVGPRICE EXACTLY ({d}), no "
            f"warm-up; the OHLC summation order differs by {order_gap:.3g}"
        )
    return {"averagePrice": col(v)}


def balance_of_power(n=None, kind: str = "sma") -> dict:
    """Balance of Power (Igor Livshin): (close - open) / (high - low).

    RAW is TA-Lib's BOP and is asserted EXACT against it. The optional
    smoothing (ChartIQ's form) has no TA-Lib function, so that case is a
    pandas replication over the same raw array with the analytic first valid
    bar asserted.

    TA-Lib returns 0.0 when `high - low < 1e-8`; we test `range == 0`
    exactly. The assert below holds the fixture clear of that gap, so the
    exactness claimed here is not quietly resting on a threshold.
    """
    rng = h - low_s
    assert float(rng.min()) > 1e-8, (
        f"the fixture has a bar with range {float(rng.min())} -- inside "
        "TA-Lib's 1e-8 flat threshold, so BOP exactness would be a coincidence"
    )
    raw = (s - o_s) / rng
    assert float(raw.abs().max()) <= 1.0, "BOP must stay within [-1, 1] on real bars"
    assert (raw > 0).any() and (raw < 0).any(), (
        "BOP never changes sign on this fixture -- a dropped sign would pass"
    )

    if talib is not None:
        d = _assert_talib_exact(raw, talib.BOP(_O, _H, _L, _C), "balanceOfPower")
        print(
            f"  balanceOfPower(raw): matches TA-Lib BOP EXACTLY ({d}); range "
            f"{raw.min():.4f}..{raw.max():.4f}, no warm-up"
        )

    if n is None:
        return {"bop": col(raw)}

    v = _ma_over(raw, kind, n)
    label = f"balanceOfPower({n},{kind})"
    expected = {"sma": n - 1, "ema": n - 1, "wma": n - 1}.get(kind)
    assert expected is not None, f"{label}: no analytic warm-up for {kind}"
    assert v.first_valid_index() == expected, (
        f"{label} first valid at {v.first_valid_index()}, expected {expected}"
    )
    # The smoothed line must be materially calmer than the raw one, or the
    # case would not distinguish "smoothed" from "forgot to smooth".
    sep = float(np.max(np.abs(v.dropna() - raw[v.notna()])))
    assert sep > 0.05, f"{label} sits only {sep} from the raw BOP"
    print(
        f"  {label}: pandas replication (TA-Lib BOP is raw only); first valid "
        f"at {expected}, {sep:.4f} from the raw line"
    )
    return {"bop": col(v)}


def starc_bands(n: int, atr_n: int, mult: float, kind: str) -> dict:
    """STARC Bands (Manning Stoller): MA(CLOSE) +/- mult * ATR(atr_n).

    Distinct from keltner (MA of TYPICAL PRICE) and from atrBands (bands
    around an EXISTING column, no middle). pandas replication on the same
    `_atr_series` TA-Lib's ATR is asserted against, so the numbers are
    TA-Lib's ATR with arithmetic on top; no TA-Lib STARC exists.

    The asserts that earn their keep: the per-column warm-up (centre at the
    MA's own bar, bands at max(centre, ATR)), and the SEPARATION from
    keltner at the same parameters -- if the two agreed on this fixture the
    case could not tell a close-centred channel from a typical-price one.
    """
    mid = _ma_over(s, kind, n)
    a = _atr_series(atr_n)
    upper, lower = mid + mult * a, mid - mult * a

    label = f"starcBands({n},{atr_n},{mult},{kind})"
    expected_mid = {"sma": n - 1, "ema": n - 1, "wma": n - 1}.get(kind)
    assert expected_mid is not None, f"{label}: no analytic warm-up for {kind}"
    assert mid.first_valid_index() == expected_mid, (
        f"{label} centre first valid at {mid.first_valid_index()}, expected "
        f"{expected_mid}"
    )
    assert upper.first_valid_index() == max(expected_mid, atr_n), (
        f"{label} bands first valid at {upper.first_valid_index()}, expected "
        f"max(centre, ATR) = {max(expected_mid, atr_n)} -- the per-column rule"
    )
    # Not keltner: the centre is the CLOSE's average, not the typical
    # price's, and the two must be distinguishable on this fixture.
    kc_mid = _ma_over((h + low_s + s) / 3, kind, n)
    sep = float(np.nanmax(np.abs(mid - kc_mid)))
    assert sep > 0.01, (
        f"{label} centre sits only {sep} from keltner's typical-price centre "
        "-- the fixture cannot tell the two channels apart"
    )
    print(
        f"  {label}: pandas replication on the TA-Lib-checked ATR; centre at "
        f"{expected_mid}, bands at {max(expected_mid, atr_n)}, {sep:.4f} from "
        f"keltner's centre"
    )
    return {
        "starcMiddle": col(mid),
        "starcUpper": col(upper),
        "starcLower": col(lower),
    }


def high_low_bands(n: int, percent: float, kind: str) -> dict:
    """High Low Bands: MA(median price) x (1 +/- percent%).

    Three columns. No TA-Lib function; a pandas replication of the corpus
    definition (assessment 6.2) with the analytic first valid bar asserted.

    The MEDIAN price is the centre's input, not the close -- asserted here
    by separation, because an envelope() over the close is the study this
    would silently become.
    """
    med = (h + low_s) / 2
    mid = _ma_over(med, kind, n)
    f = percent / 100.0
    upper, lower = mid * (1 + f), mid * (1 - f)

    label = f"highLowBands({n},{percent},{kind})"
    expected = {"sma": n - 1, "trima": n - 1, "wma": n - 1}.get(kind)
    assert expected is not None, f"{label}: no analytic warm-up for {kind}"
    assert mid.first_valid_index() == expected, (
        f"{label} centre first valid at {mid.first_valid_index()}, expected "
        f"{expected}"
    )
    close_centre = _ma_over(s, kind, n)
    sep = float(np.nanmax(np.abs(mid - close_centre)))
    assert sep > 0.01, (
        f"{label} centre sits only {sep} from the same average of the CLOSE "
        "-- the fixture cannot tell the median-price centre apart"
    )
    # The band spacing is MULTIPLICATIVE, so the width grows with the level.
    widths = (upper - lower).dropna()
    assert float(widths.max()) - float(widths.min()) > 1e-6, (
        f"{label} band width is constant -- a percent band must widen with "
        "the centre, and an additive bug would pass"
    )
    print(
        f"  {label}: pandas replication (no TA-Lib High Low Bands); first "
        f"valid at {expected}, {sep:.4f} from the close-centred average, "
        f"width {float(widths.min()):.4f}..{float(widths.max()):.4f}"
    )
    return {
        "hlbMiddle": col(mid),
        "hlbUpper": col(upper),
        "hlbLower": col(lower),
    }


def bollinger_bandwidth(n: int, k: float) -> dict:
    """Bollinger BandWidth: 100 * (upper - lower) / middle.

    The x100 (StockCharts / ChartIQ / TradingView) form, NOT Bollinger's
    bare ratio -- the generator records the factor so the choice is visible
    in the fixture rather than only in the docstring.
    """
    mid = s.rolling(n).mean()
    sd = s.rolling(n).std(ddof=0)
    v = 100 * (2 * k * sd) / mid

    label = f"bollingerBandwidth({n},{k})"
    assert v.first_valid_index() == n - 1, (
        f"{label} first valid at {v.first_valid_index()}, expected {n - 1}"
    )
    assert (v.dropna() > 0).all(), f"{label} must be positive on a rising fixture"
    # It has to VARY, or a squeeze reading would be meaningless.
    assert float(v.max()) / float(v.min()) > 1.5, (
        f"{label} spans only {float(v.min()):.4f}..{float(v.max()):.4f} -- too "
        "flat for the fixture to exercise a width reading"
    )
    print(
        f"  {label}: pandas replication (no TA-Lib BandWidth); first valid at "
        f"{n - 1}, range {float(v.min()):.4f}..{float(v.max()):.4f} (x100 form)"
    )
    return {"bbWidth": col(v)}


def bollinger_percent_b(n: int, k: float) -> dict:
    """Bollinger %B: (price - lower) / (upper - lower), the DECIMAL form.

    0 is the lower band, 1 the upper, and the fixture must put the close
    outside both at some point or the reading's whole use (above 1 / below
    0) goes untested.
    """
    mid = s.rolling(n).mean()
    sd = s.rolling(n).std(ddof=0)
    upper, lower = mid + k * sd, mid - k * sd
    v = (s - lower) / (upper - lower)

    label = f"bollingerPercentB({n},{k})"
    assert v.first_valid_index() == n - 1, (
        f"{label} first valid at {v.first_valid_index()}, expected {n - 1}"
    )
    assert (v.dropna() > 1).any() or (v.dropna() < 0).any(), (
        f"{label} never leaves [0, 1] on this fixture -- the out-of-band "
        "reading the study exists for is untested"
    )
    print(
        f"  {label}: pandas replication (no TA-Lib %B); first valid at "
        f"{n - 1}, range {float(v.min()):.4f}..{float(v.max()):.4f} (decimal, "
        "not x100)"
    )
    return {"percentB": col(v)}


def _sieve(limit: int) -> list:
    """Primes up to `limit` by a plain sieve of Eratosthenes.

    Deliberately NOT sympy: the oracle venv is pandas + TA-Lib and nothing
    else, and a study whose whole content is "which integers are prime"
    should not have its reference supplied by a library that might not be
    installed. Twenty lines of pure Python is an independent implementation
    of exactly the thing being checked.
    """
    flags = bytearray([1]) * (limit + 1)
    flags[0:2] = b"\x00\x00"
    p = 2
    while p * p <= limit:
        if flags[p]:
            flags[p * p :: p] = bytearray(len(flags[p * p :: p]))
        p += 1
    return [i for i, f in enumerate(flags) if f]


# The fixture's prices live around 100-115, so a sieve to 1000 covers every
# bracketing prime with room to spare. Asserted below rather than assumed.
_PRIMES = _sieve(1000)
_PRIME_SET = set(_PRIMES)


def _prime_at_or_above(x: float):
    if not math.isfinite(x) or x < 2:
        return None
    n = math.ceil(x)
    while n <= _PRIMES[-1]:
        if n in _PRIME_SET:
            return float(n)
        n += 1
    raise AssertionError(f"sieve limit {_PRIMES[-1]} too small for {x}")


def _prime_at_or_below(x: float):
    if not math.isfinite(x) or x < 2:
        return None
    n = math.floor(x)
    while n >= 2:
        if n in _PRIME_SET:
            return float(n)
        n -= 1
    return None


def _nearest_prime(x: float):
    """Nearest prime to x; a TIE goes to the LOWER prime (our convention)."""
    if not math.isfinite(x) or x < 2:
        return None
    below = _prime_at_or_below(x)
    above = _prime_at_or_above(x)
    if below is None:
        return above
    if above is None:
        return below
    return below if (x - below) <= (above - x) else above


def prime_number_bands() -> dict:
    """Prime Number Bands: the smallest prime >= high and the largest <= low.

    A step function of the PRICE LEVEL, not a statistic -- no window, no
    warm-up, no history. Replicated over the pure-Python sieve above.
    """
    upper = pd.Series([_prime_at_or_above(v) for v in highs], dtype="float64")
    lower = pd.Series([_prime_at_or_below(v) for v in lows], dtype="float64")

    assert upper.notna().all() and lower.notna().all(), (
        "the fixture's prices are all well above 2, so no band may be null"
    )
    assert (upper.values >= np.asarray(highs)).all(), "upper band below its high"
    assert (lower.values <= np.asarray(lows)).all(), "lower band above its low"
    # It must actually STEP on this fixture -- a fixture where the bands never
    # move would not distinguish the study from two constants.
    steps = int((upper.diff().fillna(0) != 0).sum())
    assert steps >= 3, f"upper band steps only {steps} times on this fixture"
    print(
        f"  primeNumberBands: pure-Python sieve replication (no TA-Lib); no "
        f"warm-up, upper steps {steps} times over "
        f"{float(upper.min()):.0f}..{float(upper.max()):.0f}"
    )
    return {"pnbUpper": col(upper), "pnbLower": col(lower)}


def prime_number_oscillator() -> dict:
    """Prime Number Oscillator: close - nearestPrime(close).

    Sign convention is OURS and is pinned here: `price - prime`, so positive
    means the price sits above the prime nearest it. Ties go to the LOWER
    prime.
    """
    v = pd.Series(
        [c - _nearest_prime(c) for c in closes],
        dtype="float64",
    )
    assert v.notna().all(), "no close on this fixture is outside the domain"
    # It must cross zero, or the sign convention is untested by the case.
    assert (v > 0).any() and (v < 0).any(), (
        "the oscillator never changes sign on this fixture"
    )
    # …and it must be bounded by HALF the largest prime gap the closes span,
    # computed from the sieve rather than guessed. (The fixture's closes run
    # to ~112 and the 113 -> 127 gap of 14 is the widest one they reach into,
    # so the bound is 7 -- a first draft asserting "< 5" failed at 5.0109,
    # which was the fixture telling the truth about that gap.)
    lo_c, hi_c = float(min(closes)), float(max(closes))
    span = [p for p in _PRIMES if lo_c - 20 <= p <= hi_c + 20]
    half_gap = max(
        (span[i + 1] - span[i]) / 2 for i in range(len(span) - 1)
    )
    assert float(v.abs().max()) <= half_gap, (
        f"|pno| reaches {float(v.abs().max())} but the widest half-gap over "
        f"the fixture's price range is {half_gap} -- the nearest prime is not "
        "being found"
    )
    print(
        f"  primeNumberOscillator: pure-Python sieve replication (no TA-Lib); "
        f"no warm-up, range {float(v.min()):.4f}..{float(v.max()):.4f}, "
        f"crosses zero, within the {half_gap:.1f} half-gap bound"
    )
    return {"pno": col(v)}


def market_facilitation_index() -> dict:
    """Bill Williams' MFI: (high - low) / volume, the RAW ratio (no scale).

    Output column is `bwmfi`, NOT `mfi` -- moneyFlowIndex (an entirely
    different indicator published under the same abbreviation) already owns
    that name.
    """
    v = (h - low_s) / vol

    assert v.first_valid_index() == 0, "bwmfi must be defined on bar 0"
    assert (v.dropna() > 0).all(), "bwmfi must be positive on real bars"
    # The volume spikes have to show: the index must vary by more than an
    # order of magnitude, or a version that dropped the division would pass.
    ratio = float(v.max()) / float(v.min())
    assert ratio > 5, f"bwmfi spans only {ratio:.2f}x -- the volume spikes do not show"
    print(
        f"  marketFacilitationIndex: pandas replication (no TA-Lib); no "
        f"warm-up, range {float(v.min()):.3g}..{float(v.max()):.3g} "
        f"({ratio:.1f}x)"
    )
    return {"bwmfi": col(v)}


# --------------------------------------------------------------------------
# The session-anchored pair (assessment 6.9 -- the G4 studies).
#
# Both run on the SESSION-KEYED clock above. Neither has a TA-Lib function; the
# references below are pandas GROUPBY replications -- `groupby(sid).cumsum()`
# for the VWAP and `groupby(sid).agg(...).shift(1)` reindexed onto the bars for
# the pivots -- which is a genuinely different formulation from our sequential
# reset loop, not a transcription of it. A bar in closed time carries a NaN
# session key, which pandas' groupby drops, so it reads null for free.
# --------------------------------------------------------------------------

_session_df = pd.DataFrame(
    {
        "sid": sid_s,
        "high": h,
        "low": low_s,
        "close": s,
        "volume": vol,
    }
)
_session_df["tp"] = (_session_df["high"] + _session_df["low"] + _session_df["close"]) / 3


def session_vwap() -> dict:
    """cumsum(tp*v) / cumsum(v), reset at each session, null in closed time."""
    df = _session_df
    g = df.groupby("sid", sort=False)
    num = (df["tp"] * df["volume"]).groupby(df["sid"], sort=False).cumsum()
    den = g["volume"].cumsum()
    v = num / den

    # The RESET is the whole study: each session's first bar must read that
    # bar's own typical price, or the line is running across the boundary.
    firsts = df.dropna(subset=["sid"]).groupby("sid", sort=False).head(1).index
    assert len(firsts) == len(_SESSION_DATES)
    for i in firsts:
        assert abs(float(v.iloc[i]) - float(df["tp"].iloc[i])) < 1e-9, (
            f"session VWAP must open at bar {i}'s own typical price"
        )
    assert int(v.notna().sum()) == N - 2, "closed-time bars must read null"

    # Separated from the two builds it must not be confused with: the
    # NON-resetting cumulative form (which is `anchoredVwap` from bar 0) and
    # the UNWEIGHTED per-session cumulative mean of typical price.
    flat = (df["tp"] * df["volume"]).cumsum() / df["volume"].cumsum()
    flat = flat.where(df["sid"].notna())
    unweighted = g["tp"].cumsum() / g.cumcount().add(1)
    d_flat = float((v - flat).abs().max())
    d_unw = float((v - unweighted).abs().max())
    assert d_flat > 0.5, f"the reset is invisible on this fixture ({d_flat:.4f})"
    assert d_unw > 0.1, f"the weighting is invisible on this fixture ({d_unw:.4f})"
    print(
        f"  sessionVwap: pandas groupby-cumsum (no TA-Lib); {int(v.notna().sum())} "
        f"of {N} bars in session, separated from the non-resetting form by "
        f"{d_flat:.4f} and from the unweighted session mean by {d_unw:.4f}"
    )
    return {"svwap": col(v)}


_PIVOT_ORDER = ["Pivot", "R1", "R2", "R3", "S1", "S2", "S3"]
_PIVOT_ORDER_CAMARILLA = ["Pivot", "R1", "R2", "R3", "R4", "S1", "S2", "S3", "S4"]


def _pivot_levels(method: str, hi, lo, cl) -> dict:
    """The four formula sets, over the previous session's aggregates."""
    rng = hi - lo
    p = (hi + lo + 2 * cl) / 4 if method == "woodie" else (hi + lo + cl) / 3
    out = {"Pivot": p}
    if method == "fibonacci":
        out["R1"], out["S1"] = p + 0.382 * rng, p - 0.382 * rng
        out["R2"], out["S2"] = p + 0.618 * rng, p - 0.618 * rng
        out["R3"], out["S3"] = p + rng, p - rng
    elif method == "camarilla":
        for level, k in ((1, 1.1 / 12), (2, 1.1 / 6), (3, 1.1 / 4), (4, 1.1 / 2)):
            out[f"R{level}"] = cl + rng * k
            out[f"S{level}"] = cl - rng * k
    else:  # 'standard' and 'woodie' share the ladder; only `p` differs
        out["R1"], out["S1"] = 2 * p - lo, 2 * p - hi
        out["R2"], out["S2"] = p + rng, p - rng
        out["R3"], out["S3"] = hi + 2 * (p - lo), lo - 2 * (hi - p)
    return out


def _previous_session_hlc():
    """Each session's (max high, min low, last close), SHIFTED one session and
    broadcast back onto its bars -- pandas' own hold-broadcast."""
    df = _session_df
    agg = df.dropna(subset=["sid"]).groupby("sid", sort=False).agg(
        H=("high", "max"), L=("low", "min"), C=("close", "last")
    )
    prev = agg.shift(1)
    held = prev.reindex(df["sid"]).reset_index(drop=True)
    return held["H"], held["L"], held["C"]


def pivot_points(method: str) -> dict:
    hi, lo, cl = _previous_session_hlc()
    levels = _pivot_levels(method, hi, lo, cl)
    names = _PIVOT_ORDER_CAMARILLA if method == "camarilla" else _PIVOT_ORDER
    assert set(names) == set(levels), f"{method}: column set mismatch"

    pivot = levels["Pivot"]
    # The first session has no previous one, and the two closed-time bars have
    # no session at all: 13 + 2 nulls, and every other bar defined.
    assert int(pivot.isna().sum()) == BARS_PER_SESSION + 2, (
        f"{method}: expected {BARS_PER_SESSION + 2} null bars, "
        f"got {int(pivot.isna().sum())}"
    )
    assert pivot.first_valid_index() == BARS_PER_SESSION + 1
    # Levels are FLAT within a session -- that is what makes them levels.
    for sid, group in pivot.groupby(_session_df["sid"], sort=False):
        assert group.nunique(dropna=True) <= 1, f"{method}: levels move inside {sid}"
    # The ladder must be ORDERED on every defined bar, or a swapped pair would
    # pass unnoticed. Camarilla's levels are centred on the previous CLOSE
    # rather than on the pivot, so its two halves are checked separately --
    # nothing places its pivot inside its own ladder.
    ladders = (
        [["S4", "S3", "S2", "S1"], ["R1", "R2", "R3", "R4"]]
        if method == "camarilla"
        else [["S3", "S2", "S1", "Pivot", "R1", "R2", "R3"]]
    )
    for ladder in ladders:
        for a, b in zip(ladder, ladder[1:]):
            assert (levels[a].dropna() <= levels[b].dropna() + 1e-9).all(), (
                f"{method}: {a} must not sit above {b}"
            )
    print(
        f"  pivotPoints({method}): pandas groupby-shift-reindex (no TA-Lib); "
        f"{len(names)} columns, first valid bar {BARS_PER_SESSION + 1}, "
        f"pivot range {float(pivot.min()):.4f}..{float(pivot.max()):.4f}"
    )
    return {f"pp{name}": col(levels[name]) for name in names}


# The four methods must be genuinely different lines, or the menu is
# decoration. Checked on R1, which every set defines.
_hi_p, _lo_p, _cl_p = _previous_session_hlc()
_pp_r1 = {
    m: _pivot_levels(m, _hi_p, _lo_p, _cl_p)["R1"]
    for m in ("standard", "fibonacci", "woodie", "camarilla")
}
_pp_seps = {}
for _a, _b in (
    ("standard", "fibonacci"),
    ("standard", "woodie"),
    ("standard", "camarilla"),
    ("fibonacci", "camarilla"),
):
    _sep = float((_pp_r1[_a] - _pp_r1[_b]).abs().max())
    _pp_seps[f"{_a}/{_b}"] = round(_sep, 4)
    assert _sep > 0.05, f"pivot methods {_a}/{_b} are only {_sep:.4f} apart"
print(f"  pivotPoints: method separations on R1 {_pp_seps}")


cases = [
    {"study": "sma", "params": {"period": 20}, "expected": sma(20)},
    {"study": "sma", "params": {"period": 5}, "expected": sma(5)},
    {"study": "ema", "params": {"period": 12}, "expected": ema(12)},
    {"study": "ema", "params": {"period": 26}, "expected": ema(26)},
    *[
        {
            "study": "movingAverage",
            "params": {"period": n, "type": kind},
            "expected": moving_average(n, kind),
        }
        # Two periods per type, one even and one odd -- TRIMA and ZLEMA both
        # split the period, and their even and odd branches differ.
        for kind in _MA_MENU
        for n in (10, 21)
    ],
    {
        "study": "bollinger",
        "params": {"period": 20, "stdDev": 2},
        "expected": bollinger(20, 2),
    },
    {"study": "rollingStdev", "params": {"period": 20}, "expected": rolling_stdev(20)},
    {"study": "rollingMin", "params": {"period": 20}, "expected": rolling_min(20)},
    {"study": "rollingMax", "params": {"period": 20}, "expected": rolling_max(20)},
    {
        "study": "rollingPercentile",
        "params": {"period": 20, "q": 90},
        "expected": rolling_percentile(20, 90),
    },
    {"study": "zScore", "params": {"period": 20}, "expected": zscore(20)},
    {
        "study": "envelope",
        "params": {"period": 20, "percent": 2.5},
        "expected": envelope(20, 2.5),
    },
    {
        "study": "envelope",
        "params": {"period": 20, "percent": 2.5, "maType": "ema"},
        "expected": envelope_ema(20, 2.5),
    },
    {
        "study": "percentChange",
        "params": {"periods": 1},
        "expected": percent_change(1),
    },
    {
        "study": "percentChange",
        "params": {"periods": 5},
        "expected": percent_change(5),
    },
    {"study": "rsi", "params": {"period": 14}, "expected": rsi(14)},
    {"study": "rsi", "params": {"period": 5}, "expected": rsi(5)},
    {
        "study": "macd",
        "params": {"fastPeriod": 12, "slowPeriod": 26, "signalPeriod": 9},
        "expected": macd(12, 26, 9),
    },
    {
        "study": "macd",
        "params": {"fastPeriod": 3, "slowPeriod": 7, "signalPeriod": 4},
        "expected": macd(3, 7, 4),
    },
    {"study": "atr", "params": {"period": 14}, "expected": atr(14)},
    {"study": "atr", "params": {"period": 3}, "expected": atr(3)},
    {"study": "momentum", "params": {"period": 10}, "expected": momentum(10)},
    {"study": "momentum", "params": {"period": 3}, "expected": momentum(3)},
    {
        "study": "historicalVolatility",
        "params": {"period": 20, "annualize": 252},
        "expected": historical_volatility(20, 252),
    },
    {
        "study": "historicalVolatility",
        "params": {"period": 10, "annualize": 1},
        "expected": historical_volatility(10, 1),
    },
    {
        "study": "stochastic",
        "params": {"kPeriod": 14, "slowing": 3, "dPeriod": 3},
        "expected": stochastic(14, 3, 3),
    },
    {
        "study": "stochastic",
        "params": {"kPeriod": 5, "slowing": 1, "dPeriod": 3},
        "expected": stochastic(5, 1, 3),
    },
    {"study": "williamsR", "params": {"period": 14}, "expected": williams_r(14)},
    {"study": "williamsR", "params": {"period": 5}, "expected": williams_r(5)},
    {"study": "donchian", "params": {"period": 20}, "expected": donchian(20)},
    {"study": "donchian", "params": {"period": 5}, "expected": donchian(5)},
    {"study": "obv", "params": {}, "expected": obv()},
    {"study": "vwap", "params": {"period": 14}, "expected": vwap(14)},
    {"study": "vwap", "params": {"period": 5}, "expected": vwap(5)},
    {
        # The defaults: percent (PPO) on pond's EMA seed.
        "study": "priceOscillator",
        "params": {
            "fastPeriod": 12,
            "slowPeriod": 26,
            "maType": "ema",
            "mode": "percent",
        },
        "expected": price_oscillator(12, 26, "ema", "percent"),
    },
    {
        # The other mode on a seedless type, where TA-Lib parity is exact.
        "study": "priceOscillator",
        "params": {
            "fastPeriod": 5,
            "slowPeriod": 13,
            "maType": "sma",
            "mode": "absolute",
        },
        "expected": price_oscillator(5, 13, "sma", "absolute"),
    },
    {
        "study": "disparityIndex",
        "params": {"period": 14, "maType": "sma"},
        "expected": disparity_index(14, "sma"),
    },
    {
        "study": "disparityIndex",
        "params": {"period": 20, "maType": "ema"},
        "expected": disparity_index(20, "ema"),
    },
    {
        "study": "detrendedPriceOscillator",
        "params": {"period": 20, "maType": "sma"},
        "expected": dpo(20, "sma"),
    },
    {
        # An ODD period, so the floor in `shift = floor(n/2) + 1` is exercised.
        "study": "detrendedPriceOscillator",
        "params": {"period": 15, "maType": "ema"},
        "expected": dpo(15, "ema"),
    },
    {"study": "elderRay", "params": {"period": 13}, "expected": elder_ray(13)},
    {"study": "elderRay", "params": {"period": 5}, "expected": elder_ray(5)},
    {
        "study": "awesomeOscillator",
        "params": {"fastPeriod": 5, "slowPeriod": 34},
        "expected": awesome_oscillator(5, 34),
    },
    {
        "study": "awesomeOscillator",
        "params": {"fastPeriod": 3, "slowPeriod": 8},
        "expected": awesome_oscillator(3, 8),
    },
    {
        "study": "keltner",
        "params": {"period": 20, "atrPeriod": 10, "multiplier": 2, "maType": "ema"},
        "expected": keltner(20, 10, 2, "ema"),
    },
    {
        # A shorter centre than the ATR, so the per-column warm-up differs the
        # OTHER way round (bands later than the centre) than at the defaults.
        "study": "keltner",
        "params": {"period": 5, "atrPeriod": 14, "multiplier": 1.5, "maType": "sma"},
        "expected": keltner(5, 14, 1.5, "sma"),
    },
    {
        "study": "atrBands",
        "params": {"period": 14, "multiplier": 2},
        "expected": atr_bands(14, 2),
    },
    {
        "study": "atrBands",
        "params": {"period": 5, "multiplier": 3},
        "expected": atr_bands(5, 3),
    },
    {"study": "qstick", "params": {"period": 8}, "expected": qstick(8, "sma")},
    {
        "study": "qstick",
        "params": {"period": 5, "maType": "ema"},
        "expected": qstick(5, "ema"),
    },
    {
        "study": "trix",
        "params": {"period": 15, "signalPeriod": 9},
        "expected": trix(15, 9),
    },
    {
        "study": "trix",
        "params": {"period": 5, "signalPeriod": 3},
        "expected": trix(5, 3),
    },
    {
        "study": "coppock",
        "params": {"longPeriod": 14, "shortPeriod": 11, "wmaPeriod": 10},
        "expected": coppock(14, 11, 10),
    },
    {
        "study": "coppock",
        "params": {"longPeriod": 6, "shortPeriod": 3, "wmaPeriod": 4},
        "expected": coppock(6, 3, 4),
    },
    {
        "study": "accumulationDistribution",
        "params": {},
        "expected": accumulation_distribution(),
    },
    {
        "study": "chaikinOscillator",
        "params": {"fastPeriod": 3, "slowPeriod": 10},
        "expected": chaikin_oscillator(3, 10),
    },
    {
        "study": "chaikinOscillator",
        "params": {"fastPeriod": 4, "slowPeriod": 12},
        "expected": chaikin_oscillator(4, 12),
    },
    {"study": "priceVolumeTrend", "params": {}, "expected": price_volume_trend()},
    {
        "study": "chaikinMoneyFlow",
        "params": {"period": 20},
        "expected": chaikin_money_flow(20),
    },
    {
        "study": "chaikinMoneyFlow",
        "params": {"period": 5},
        "expected": chaikin_money_flow(5),
    },
    {
        "study": "moneyFlowIndex",
        "params": {"period": 14},
        "expected": money_flow_index(14),
    },
    {
        "study": "moneyFlowIndex",
        "params": {"period": 5},
        "expected": money_flow_index(5),
    },
    {"study": "forceIndex", "params": {"period": 13}, "expected": force_index(13)},
    {"study": "forceIndex", "params": {"period": 2}, "expected": force_index(2)},
    {
        "study": "easeOfMovement",
        "params": {"period": 14, "maType": "sma"},
        "expected": ease_of_movement(14, "sma"),
    },
    {
        "study": "easeOfMovement",
        "params": {"period": 5, "maType": "ema"},
        "expected": ease_of_movement(5, "ema"),
    },
    {
        "study": "volumeOscillator",
        "params": {"fastPeriod": 5, "slowPeriod": 10, "maType": "sma"},
        "expected": volume_oscillator(5, 10, "sma"),
    },
    {
        "study": "volumeOscillator",
        "params": {"fastPeriod": 4, "slowPeriod": 12, "maType": "ema"},
        "expected": volume_oscillator(4, 12, "ema"),
    },
    {
        "study": "chandeMomentum",
        "params": {"period": 14},
        "expected": chande_momentum(14),
    },
    {
        "study": "chandeMomentum",
        "params": {"period": 5},
        "expected": chande_momentum(5),
    },
    {
        "study": "ultimateOscillator",
        "params": {"shortPeriod": 7, "mediumPeriod": 14, "longPeriod": 28},
        "expected": ultimate_oscillator(7, 14, 28),
    },
    {
        # A short set, so the three legs' warm-ups sit close together and a
        # study that used the wrong leg's mask would still be caught.
        "study": "ultimateOscillator",
        "params": {"shortPeriod": 3, "mediumPeriod": 5, "longPeriod": 9},
        "expected": ultimate_oscillator(3, 5, 9),
    },
    {
        "study": "commodityChannelIndex",
        "params": {"period": 20},
        "expected": commodity_channel_index(20),
    },
    {
        "study": "commodityChannelIndex",
        "params": {"period": 5},
        "expected": commodity_channel_index(5),
    },
    {
        "study": "intradayMomentumIndex",
        "params": {"period": 14},
        "expected": intraday_momentum_index(14),
    },
    {
        # 8, not 4: this fixture's opens are the PREVIOUS CLOSE pulled inside
        # the bar, so over a very short window the candle bodies and the
        # close-to-close changes nearly agree (measured: max separation 0.0
        # points at n=3, 0.07 at n=4, 6.2 at n=8, ~10 at n=12). A short case
        # is still worth having for its warm-up; it just has to be long
        # enough that the fixture can tell the two inputs apart.
        "study": "intradayMomentumIndex",
        "params": {"period": 8},
        "expected": intraday_momentum_index(8),
    },
    {
        "study": "relativeVigorIndex",
        "params": {"period": 10},
        "expected": relative_vigor_index(10),
    },
    {
        "study": "relativeVigorIndex",
        "params": {"period": 4},
        "expected": relative_vigor_index(4),
    },
    {
        "study": "psychologicalLine",
        "params": {"period": 12},
        "expected": psychological_line(12),
    },
    {
        "study": "psychologicalLine",
        "params": {"period": 5},
        "expected": psychological_line(5),
    },
    {
        "study": "chaikinVolatility",
        "params": {"period": 10, "rocPeriod": 10},
        "expected": chaikin_volatility(10, 10),
    },
    {
        # A shorter EMA than the look-back, so the two halves of the warm-up
        # are told apart (a study that used one period for both would land on
        # the same bar at the defaults).
        "study": "chaikinVolatility",
        "params": {"period": 5, "rocPeriod": 3},
        "expected": chaikin_volatility(5, 3),
    },
    {
        "study": "massIndex",
        "params": {"emaPeriod": 9, "sumPeriod": 25},
        "expected": mass_index(9, 25),
    },
    {
        "study": "massIndex",
        "params": {"emaPeriod": 4, "sumPeriod": 10},
        "expected": mass_index(4, 10),
    },
    {
        "study": "choppinessIndex",
        "params": {"period": 14},
        "expected": choppiness_index(14),
    },
    {
        "study": "choppinessIndex",
        "params": {"period": 5},
        "expected": choppiness_index(5),
    },
    {"study": "ulcerIndex", "params": {"period": 14}, "expected": ulcer_index(14)},
    {"study": "ulcerIndex", "params": {"period": 5}, "expected": ulcer_index(5)},
    {
        "study": "verticalHorizontalFilter",
        "params": {"period": 28},
        "expected": vertical_horizontal_filter(28),
    },
    {
        "study": "verticalHorizontalFilter",
        "params": {"period": 10},
        "expected": vertical_horizontal_filter(10),
    },
    {
        "study": "gopalakrishnanRangeIndex",
        "params": {"period": 10},
        "expected": gopalakrishnan_range_index(10),
    },
    {
        "study": "gopalakrishnanRangeIndex",
        "params": {"period": 5},
        "expected": gopalakrishnan_range_index(5),
    },
    {
        "study": "relativeVolatilityIndex",
        "params": {"period": 14, "stdevPeriod": 10},
        "expected": relative_volatility_index(14, 10),
    },
    {
        # Both periods short, so the Wilder seed and the sigma window sit
        # close together and a study that swapped them would still be caught.
        "study": "relativeVolatilityIndex",
        "params": {"period": 8, "stdevPeriod": 5},
        "expected": relative_volatility_index(8, 5),
    },
    {
        "study": "directionalMovement",
        "params": {"period": 14},
        "expected": directional_movement(14),
    },
    {
        "study": "directionalMovement",
        "params": {"period": 5},
        "expected": directional_movement(5),
    },
    {"study": "aroon", "params": {"period": 25}, "expected": aroon(25)},
    {"study": "aroon", "params": {"period": 5}, "expected": aroon(5)},
    {"study": "vortex", "params": {"period": 14}, "expected": vortex(14)},
    {
        "study": "correlation",
        "params": {"period": 30, "benchmark": "bench"},
        "expected": correlation(30),
    },
    {
        "study": "correlation",
        "params": {"period": 5, "benchmark": "bench"},
        "expected": correlation(5),
    },
    {
        "study": "beta",
        "params": {"period": 5, "benchmark": "bench"},
        "expected": beta_study(5),
    },
    {
        "study": "beta",
        "params": {"period": 20, "benchmark": "bench"},
        "expected": beta_study(20),
    },
    {
        "study": "priceRelative",
        "params": {"benchmark": "bench"},
        "expected": price_relative(),
    },
    {
        "study": "performanceIndex",
        "params": {"period": 20, "benchmark": "bench"},
        "expected": performance_index(20),
    },
    {
        "study": "performanceIndex",
        "params": {"period": 5, "benchmark": "bench"},
        "expected": performance_index(5),
    },
    {"study": "vortex", "params": {"period": 6}, "expected": vortex(6)},
    {
        "study": "linearRegression",
        "params": {"period": 14},
        "expected": linear_regression(14),
    },
    {
        "study": "linearRegression",
        "params": {"period": 5},
        "expected": linear_regression(5),
    },
    {
        "study": "timeSeriesForecast",
        "params": {"period": 14},
        "expected": time_series_forecast(14),
    },
    {
        "study": "timeSeriesForecast",
        "params": {"period": 5},
        "expected": time_series_forecast(5),
    },
    {
        "study": "chandeForecastOscillator",
        "params": {"period": 14},
        "expected": chande_forecast_oscillator(14),
    },
    {
        "study": "chandeForecastOscillator",
        "params": {"period": 5},
        "expected": chande_forecast_oscillator(5),
    },
    {
        "study": "centerOfGravity",
        "params": {"period": 10},
        "expected": center_of_gravity(10),
    },
    {
        "study": "centerOfGravity",
        "params": {"period": 5},
        "expected": center_of_gravity(5),
    },
    {
        "study": "parabolicSar",
        "params": {},
        "expected": parabolic_sar(),
    },
    {
        "study": "parabolicSar",
        "params": {"step": 0.05, "maxStep": 0.5},
        "expected": parabolic_sar(0.05, 0.5),
    },
    {
        "study": "superTrend",
        "params": {"period": 10, "multiplier": 3},
        "expected": super_trend(10, 3.0),
    },
    {
        "study": "superTrend",
        "params": {"period": 7, "multiplier": 1},
        "expected": super_trend(7, 1.0, min_flips=3),
    },
    {
        "study": "atrTrailingStop",
        "params": {"period": 14, "multiplier": 3},
        "expected": atr_trailing_stop(14, 3.0),
    },
    {
        "study": "atrTrailingStop",
        "params": {"period": 7, "multiplier": 1.5},
        "expected": atr_trailing_stop(7, 1.5),
    },
    {
        "study": "negativeVolumeIndex",
        "params": {},
        "expected": volume_index("nvi"),
    },
    {
        "study": "positiveVolumeIndex",
        "params": {},
        "expected": volume_index("pvi"),
    },
    {
        "study": "klinger",
        "params": {},
        "expected": klinger(),
    },
    {
        "study": "klinger",
        "params": {"fastPeriod": 5, "slowPeriod": 13, "signalPeriod": 4},
        "expected": klinger(5, 13, 4),
    },
    {"study": "guppy", "params": {"type": "ema"}, "expected": guppy("ema")},
    {
        # The seedless type, where TA-Lib parity is exact at all twelve
        # periods rather than split into formula + transient.
        "study": "guppy",
        "params": {"type": "sma"},
        "expected": guppy("sma"),
    },
    {"study": "rainbow", "params": {"period": 2}, "expected": rainbow(2, "sma")},
    {
        # A longer stage, so the composed warm-up (k*(period-1)) is told
        # apart from a flat k, and an EMA stage so the recursion is checked
        # on a type that carries state rather than a window.
        "study": "rainbow",
        "params": {"period": 3, "type": "ema"},
        "expected": rainbow(3, "ema"),
    },
    {
        "study": "rainbowOscillator",
        "params": {"period": 2, "lookback": 10},
        "expected": rainbow_oscillator(2, 10, "sma"),
    },
    {
        # The stack deeper than the range window at the defaults, so this
        # case puts the RANGE on the late side of the warm-up instead.
        "study": "rainbowOscillator",
        "params": {"period": 2, "lookback": 25},
        "expected": rainbow_oscillator(2, 25, "sma"),
    },
    {"study": "kst", "params": {}, "expected": kst(9)},
    {
        # A shorter signal, so the second column's own warm-up is told apart
        # from the line's rather than sitting a fixed 8 bars later.
        "study": "kst",
        "params": {"signalPeriod": 3},
        "expected": kst(3),
    },
    {
        # One case only: the study has no period options at all (see the
        # study's docstring for why), so there is no second shape to check.
        "study": "priceMomentumOscillator",
        "params": {},
        "expected": price_momentum_oscillator(),
    },
    {
        "study": "stochasticRsi",
        "params": {"rsiPeriod": 14, "stochPeriod": 14, "kPeriod": 3, "dPeriod": 3},
        "expected": stochastic_rsi(14, 14, 3, 3),
    },
    {
        # rsiPeriod != stochPeriod and kPeriod = 1 (the FAST form, %K left as
        # the raw range position) - the shape the defaults cannot tell apart.
        "study": "stochasticRsi",
        "params": {"rsiPeriod": 8, "stochPeriod": 5, "kPeriod": 1, "dPeriod": 4},
        "expected": stochastic_rsi(8, 5, 1, 4),
    },
    {
        "study": "trueStrengthIndex",
        "params": {"longPeriod": 25, "shortPeriod": 13, "signalPeriod": 7},
        "expected": true_strength_index(25, 13, 7),
    },
    {
        # Shorter spans, so the two stages' warm-ups sit close together and
        # a build that applied one span twice would still be caught. Not
        # SHORTER than this: at (8, 3) the ratio saturates near +/-100 and
        # the swapped order is only 0.40% of the line's own spread away, so
        # the fixture could no longer tell the two apart (measured).
        "study": "trueStrengthIndex",
        "params": {"longPeriod": 20, "shortPeriod": 6, "signalPeriod": 4},
        "expected": true_strength_index(20, 6, 4),
    },
    {
        "study": "movingAverageDeviation",
        "params": {"period": 20, "maType": "sma"},
        "expected": moving_average_deviation(20, "sma"),
    },
    {
        # The same two shapes disparityIndex is checked at, so the identity
        # between the two studies is asserted on both an SMA and an EMA
        # centre (the EMA also carries pond's first-sample seed).
        "study": "movingAverageDeviation",
        "params": {"period": 14, "maType": "ema"},
        "expected": moving_average_deviation(14, "ema"),
    },
    {
        "study": "stochasticMomentumIndex",
        "params": {},
        "expected": stochastic_momentum_index(13, 25, 2, 3),
    },
    {
        # Shorter and unequal smoothings, so a build that applied one span
        # twice (or swapped the two stages' order) is still caught, and the
        # signal's own warm-up is told apart from the line's.
        "study": "stochasticMomentumIndex",
        "params": {
            "period": 8,
            "longPeriod": 10,
            "shortPeriod": 4,
            "signalPeriod": 5,
        },
        "expected": stochastic_momentum_index(8, 10, 4, 5),
    },
    {"study": "fisherTransform", "params": {}, "expected": fisher_transform(10)},
    {
        # A short look-back, so the normalised price reaches the clamp more
        # often and the machine's seed path is exercised on a different bar
        # than the default's.
        "study": "fisherTransform",
        "params": {"period": 4},
        "expected": fisher_transform(4),
    },
    {
        # Schaff's own periods need far more than 80 bars to say anything:
        # on the short fixture the line has six values and every one of them
        # is 0. So the default case runs on the LONG input.
        "study": "schaffTrendCycle",
        "params": {},
        "input": "long",
        "expected": schaff_trend_cycle(23, 50, 10, True),
    },
    {
        # Much shorter, so the study has most of the fixture to itself and
        # the two 0.5 recursions are seen over many more bars than the
        # defaults' thirteen.
        "study": "schaffTrendCycle",
        "params": {"fastPeriod": 5, "slowPeriod": 12, "cyclePeriod": 4},
        "expected": schaff_trend_cycle(5, 12, 4),
    },
    {
        "study": "prettyGoodOscillator",
        "params": {},
        "expected": pretty_good_oscillator(14),
    },
    {
        # A short period, where the EMA of true range has much less memory
        # and the Wilder-denominator port would be closer - the harder case
        # for the separation the generator asserts.
        "study": "prettyGoodOscillator",
        "params": {"period": 5},
        "expected": pretty_good_oscillator(5),
    },
    {
        # `limit` is REQUIRED on this study, so both cases name it. 3.0 is
        # comfortably above the fixture's largest K, which is what makes the
        # -100..100 bound assertable.
        "study": "swingIndex",
        "params": {"limit": 3},
        "expected": swing_index(3, False),
    },
    {
        # A limit BELOW the largest K, where the reading legitimately leaves
        # the -100..100 band - the case that pins K/T as a real division
        # rather than a normalisation the study could drop.
        "study": "swingIndex",
        "params": {"limit": 0.5},
        "expected": swing_index(0.5, False),
    },
    {
        "study": "accumulativeSwingIndex",
        "params": {"limit": 3},
        "expected": swing_index(3, True),
    },
    {"study": "randomWalkIndex", "params": {}, "expected": random_walk_index(14)},
    {
        # A longer horizon list, which is the axis this study's cost and its
        # definition both live on - a build that only tested n = period would
        # agree at period 2 and diverge here.
        "study": "randomWalkIndex",
        "params": {"period": 30},
        "expected": random_walk_index(30),
    },
    {
        # Chande's 65-bar average leaves only sixteen readings on the short
        # fixture, and over those the 7-bar average never crosses below it -
        # so the ABSOLUTE VALUE, which is the whole point of the study, would
        # be untested. The default case runs on the LONG input, where the
        # fast average crosses the slow one repeatedly.
        "study": "ravi",
        "params": {},
        "input": "long",
        "expected": ravi_study(7, 65, True),
    },
    {
        # Shorter periods, so the reading exists over most of the fixture and
        # crosses Chande's 3% threshold in both directions.
        "study": "ravi",
        "params": {"shortPeriod": 4, "longPeriod": 20},
        "expected": ravi_study(4, 20),
    },
    {
        # Pee's own periods need 89 bars, so this case runs on the LONG
        # input; the short case below stays on the 80-bar fixture.
        "study": "trendIntensityIndex",
        "params": {},
        "input": "long",
        "expected": trend_intensity_index(30, 60, True),
    },
    {
        "study": "trendIntensityIndex",
        "params": {"period": 10, "maPeriod": 20},
        "expected": trend_intensity_index(10, 20, False),
    },
    {
        # One case only: the study has no period options at all (the
        # thirty-six constants ARE the study), and its 724-bar warm-up means
        # it can only run on the long input.
        "study": "specialK",
        "params": {},
        "input": "long",
        "expected": special_k(),
    },
    # The K3 price transforms (assessment 6.8). All four are exact against
    # TA-Lib and have no warm-up, so one case each is the whole story.
    {"study": "typicalPrice", "params": {}, "expected": typical_price()},
    {"study": "medianPrice", "params": {}, "expected": median_price()},
    {"study": "weightedClose", "params": {}, "expected": weighted_close()},
    {"study": "averagePrice", "params": {}, "expected": average_price()},
    # Balance of Power: the raw TA-Lib form (the default) and the smoothed
    # ChartIQ one, so both halves of the F-AMBIG fork are pinned.
    {"study": "balanceOfPower", "params": {}, "expected": balance_of_power()},
    {
        "study": "balanceOfPower",
        "params": {"period": 14, "maType": "sma"},
        "expected": balance_of_power(14, "sma"),
    },
    {
        "study": "balanceOfPower",
        "params": {"period": 5, "maType": "ema"},
        "expected": balance_of_power(5, "ema"),
    },
    # The bands and channels tail (assessment 6.2).
    {
        "study": "starcBands",
        "params": {"period": 20, "atrPeriod": 15, "multiplier": 2, "maType": "sma"},
        "expected": starc_bands(20, 15, 2, "sma"),
    },
    {
        # A shorter centre than the ATR, so the per-column warm-up differs
        # the OTHER way round (bands later than the centre).
        "study": "starcBands",
        "params": {"period": 5, "atrPeriod": 14, "multiplier": 1.5, "maType": "ema"},
        "expected": starc_bands(5, 14, 1.5, "ema"),
    },
    {
        "study": "highLowBands",
        "params": {"period": 10, "percent": 1, "maType": "trima"},
        "expected": high_low_bands(10, 1, "trima"),
    },
    {
        # A different average and a wider shift, so neither default is the
        # only thing the case pins.
        "study": "highLowBands",
        "params": {"period": 20, "percent": 3.5, "maType": "sma"},
        "expected": high_low_bands(20, 3.5, "sma"),
    },
    {
        "study": "bollingerBandwidth",
        "params": {"period": 20, "stdDev": 2},
        "expected": bollinger_bandwidth(20, 2),
    },
    {
        "study": "bollingerBandwidth",
        "params": {"period": 10, "stdDev": 1.5},
        "expected": bollinger_bandwidth(10, 1.5),
    },
    {
        "study": "bollingerPercentB",
        "params": {"period": 20, "stdDev": 2},
        "expected": bollinger_percent_b(20, 2),
    },
    {
        "study": "bollingerPercentB",
        "params": {"period": 10, "stdDev": 1.5},
        "expected": bollinger_percent_b(10, 1.5),
    },
    # The prime studies (6.2 / 6.3) and Bill Williams' MFI (6.6). All three
    # are per-bar and parameterless, so one case each is the whole story.
    {"study": "primeNumberBands", "params": {}, "expected": prime_number_bands()},
    {
        "study": "primeNumberOscillator",
        "params": {},
        "expected": prime_number_oscillator(),
    },
    {
        "study": "marketFacilitationIndex",
        "params": {},
        "expected": market_facilitation_index(),
    },
    {
        "study": "twiggsMoneyFlow",
        "params": {},
        "expected": twiggs_money_flow(21),
    },
    {
        # A short period, where the Wilder seed is a much smaller share of the
        # history and the reading swings across its whole range.
        "study": "twiggsMoneyFlow",
        "params": {"period": 5},
        "expected": twiggs_money_flow(5),
    },
    {
        # A dead band wide enough that several bars are undecided, which is
        # what exercises the direction-persists rule.
        "study": "tradeVolumeIndex",
        "params": {"minTick": 0.5},
        "expected": trade_volume_index(0.5),
    },
    {
        # A narrow band: almost every bar decides its own direction, so this
        # case pins the accumulation rather than the persistence.
        "study": "tradeVolumeIndex",
        "params": {"minTick": 0.05},
        "expected": trade_volume_index(0.05, min_undecided=3),
    },
    {
        "study": "shinoharaIntensityRatio",
        "params": {},
        "expected": shinohara_intensity_ratio(26),
    },
    {
        # A short window, where the two ratios swing much further and the
        # per-column warm-up difference is easier to see.
        "study": "shinoharaIntensityRatio",
        "params": {"period": 5},
        "expected": shinohara_intensity_ratio(5),
    },
    {
        # Elder's own periods. Warm-up 34 of the fixture's 80 bars, so the
        # case still has 46 verdicts to check.
        "study": "elderImpulse",
        "params": {},
        "expected": elder_impulse(13, 12, 26, 9),
    },
    {
        # Short periods, so the verdicts start at bar 8 and the whole fixture
        # is covered — and so the two knobs are shown to be knobs.
        "study": "elderImpulse",
        "params": {
            "emaPeriod": 5,
            "fastPeriod": 3,
            "slowPeriod": 7,
            "signalPeriod": 4,
        },
        "expected": elder_impulse(5, 3, 7, 4),
    },
    {
        "study": "movingAverageCross",
        "params": {},
        "expected": moving_average_cross(10, 30, "sma"),
    },
    {
        # A recursive type and shorter periods: the K2 engine's array door,
        # more crossings, and a warm-up that is not the window's.
        "study": "movingAverageCross",
        "params": {"fastPeriod": 5, "slowPeriod": 12, "maType": "ema"},
        "expected": moving_average_cross(5, 12, "ema"),
    },
    {
        # The 80-bar fixture only turns twice, so the default case above sees
        # one cross of each sign. The LONG input turns repeatedly, which is
        # what actually exercises a signal column.
        "study": "movingAverageCross",
        "params": {},
        "input": "long",
        "expected": moving_average_cross(10, 30, "sma", long=True),
    },
    {
        # Mid-series: the half of the fixture before the anchor must be null
        # and the half after must be the running average from it.
        "study": "anchoredVwap",
        "params": {"anchor": 40},
        "expected": anchored_vwap(40),
    },
    {
        # Anchored on bar 0, which is the cumulative-from-inception form and
        # the case where every bar contributes.
        "study": "anchoredVwap",
        "params": {"anchor": 0},
        "expected": anchored_vwap(0),
    },
    # The session-anchored pair. These are the only cases on the SESSION clock
    # (`"input": "session"`) -- same 80 bars of OHLCV, keyed onto a real
    # 09:30-16:00 America/New_York grid; the vitest side rebuilds the calendar
    # from the same rules, so a Temporal/zoneinfo disagreement about a session
    # boundary fails these cases rather than hiding.
    {"study": "sessionVwap", "params": {}, "input": "session", "expected": session_vwap()},
    *[
        {
            "study": "pivotPoints",
            "params": {"method": method},
            "input": "session",
            "expected": pivot_points(method),
        }
        for method in ("standard", "fibonacci", "woodie", "camarilla")
    ],
]

out = {
    "meta": {
        "generator": "packages/financial/scripts/oracle/generate.py",
        "oracle": (
            f"pandas {pd.__version__}"
            + (f" + TA-Lib {talib.__version__}" if talib is not None else "")
        ),
        "conventions": {
            "sma": "close.rolling(n).mean()",
            "ema": "close.ewm(span=n, adjust=False).mean(); first n-1 masked",
            "movingAverage": (
                "the K2 MA-type menu. Exact against TA-Lib MA(matype): sma 0, "
                "wma 2, trima 5 (SMA-of-SMA, lengths summing to n+1), kama 6 "
                "(fast 2 / slow 30, seeded on x[n-1]). ema 1 / dema 3 / tema 4 "
                "run on POND's first-sample EMA seed, not TA-Lib's SMA seed - "
                "identical null masks, values bounded at the tail (the seed "
                "transient decays: at n=21 it is 0.210% / 0.529% / 0.059% of "
                "scale at the first shared bar and 0.0008% / 0.0155% / 0.0144% "
                "by bar 79; at n=10 it is under 0.00005% by bar 79). "
                "smma = Wilder (the rsi/atr recursion), hull = "
                "WMA(2*WMA(n/2)-WMA(n), round(sqrt n)), zlema = "
                "EMA(2x - x[n-1 floor-halved back]) - pandas replications, no "
                "TA-Lib function exists for any of the three"
            ),
            "bollingerStd": "rolling(n).std(ddof=0) [population]",
            "atr": (
                "TR = max(h-l, |h-prevC|, |l-prevC|), TR[0] undefined; Wilder "
                "seed = mean of first n TRs; cross-checked against TA-Lib"
            ),
            "macd": (
                "fast/slow EMA on POND's ewm(adjust=False) first-sample seed, "
                "not TA-Lib's SMA seed; each column warms up when it can"
            ),
            "rsi": (
                "Wilder: seed = mean of first n diffs, then "
                "(prev*(n-1) + x)/n; cross-checked against TA-Lib"
            ),
            "percentChange": (
                "pct_change(n) * 100; IS rate-of-change, cross-checked "
                "against TA-Lib ROC"
            ),
            "momentum": "diff(n); cross-checked against TA-Lib MOM",
            "historicalVolatility": (
                "log(close).diff().rolling(n).std(ddof=0) * sqrt(annualize) "
                "[population, log returns, decimal]; no TA-Lib HV exists"
            ),
            "stochastic": (
                "fast %K = 100(c-LL)/(HH-LL); %K = SMA(fast, slowing); "
                "%D = SMA(%K, dPeriod); TA-Lib STOCH/STOCHF values, but %K "
                "emitted from its own first valid bar (TA-Lib masks it to %D's)"
            ),
            "williamsR": (
                "-100(HH-c)/(HH-LL) over n bars; cross-checked against TA-Lib"
            ),
            "donchian": (
                "high.rolling(n).max(), low.rolling(n).min(), midpoint; "
                "pandas only (TA-Lib has no Donchian)"
            ),
            "obv": (
                "cumsum(sign(close.diff()) * volume), OBV[0] = volume[0]; "
                "cross-checked against TA-Lib"
            ),
            "vwap": (
                "rolling: (tp*volume).rolling(n).sum() / volume.rolling(n).sum(), "
                "tp = (high+low+close)/3; pandas replication (no TA-Lib VWAP)"
            ),
            "keltner": (
                "the MODERN variant (Keltner via Raschke; ChartIQ's default): "
                "MA(typical price, n) +/- mult * ATR(atr_n), defaults 20 / 10 / "
                "2 / ema. pandas replication reusing the TA-Lib-checked ATR and "
                "the typical price; per-column warm-up - centre at the MA's own "
                "first bar, bands at max(centre, ATR). The 1960 original (SMA "
                "of typical price +/- 1x SMA of PLAIN range) is a documented "
                "delta: the half-width here is always TRUE range"
            ),
            "atrBands": (
                "column +/- mult * ATR(n), defaults 14 / 2; TWO columns (the "
                "middle is the field itself). pandas replication on the same "
                "TA-Lib-checked ATR, with `upper - close == mult * atr` "
                "asserted exactly"
            ),
            "qstick": (
                "MA(close - open, n), defaults 8 / sma; pandas replication (no "
                "TA-Lib QSTICK). The fixture's opens are the previous close "
                "pulled inside the bar, so 30 of 80 bodies are negative"
            ),
            "trix": (
                "100 * (T[i]/T[i-1] - 1) where T = EMA(EMA(EMA(x, n))), plus "
                "EMA(trix, sig); defaults 15 / 9. TA-Lib TRIX is the reference: "
                "the FORMULA is asserted bit-exact on TA-Lib's own SMA seed "
                "(which catches a log-vs-percent ROC, a dropped stage or a "
                "`tema` substitution), pond's first-sample seed bounded at the "
                "tail. The signal has no vendor reference (TA-Lib returns the "
                "line alone); 9 is ChartIQ's default"
            ),
            "coppock": (
                "WMA(pct_change(long)*100 + pct_change(short)*100, wma), "
                "defaults 14 / 11 / 10 - Coppock's MONTHLY lengths, applied as "
                "bar counts like every other study here; pandas replication (no "
                "TA-Lib Coppock)"
            ),
            "priceOscillator": (
                "percent = 100*(MA(fast)-MA(slow))/MA(slow) [TA-Lib PPO], "
                "absolute = MA(fast)-MA(slow) [TA-Lib APO], matype = our "
                "maType. The sma/absolute case matches TA-Lib APO(matype=0) "
                "exactly; the ema/percent case keeps POND's first-sample EMA "
                "seed (the macd precedent), so the formula is proven on "
                "TA-Lib's SMA seed and the seed transient bounded at the tail"
            ),
            "disparityIndex": (
                "100*(close-MA)/MA; pandas replication (no TA-Lib function), "
                "first valid bar asserted, and separated from the "
                "divide-by-price version on this fixture"
            ),
            "detrendedPriceOscillator": (
                "close - MA.shift(floor(n/2)+1) - the non-centered alignment "
                "(TradingView's default); pandas replication, first valid at "
                "n-1+shift, separated from the undisplaced close-MA"
            ),
            "elderRay": (
                "bull = high - EMA(close,n), bear = low - EMA(close,n) on "
                "POND's first-sample EMA seed; pandas replication (no TA-Lib "
                "Elder Ray), first valid at n-1 for both columns"
            ),
            "awesomeOscillator": (
                "SMA(fast) - SMA(slow) of the median price (high+low)/2; "
                "pandas replication (no TA-Lib AO), first valid at slow-1, "
                "separated from the close-based version"
            ),
            "accumulationDistribution": (
                "cumsum(CLV * volume), CLV = ((c-l)-(h-c))/(h-l); "
                "cross-checked against TA-Lib AD (mask and values). No period "
                "and no warm-up. A FLAT bar (h == l) contributes 0 on both "
                "sides (the CLV numerator is exactly zero), so the two agree "
                "everywhere - the fixture always has a range, so the flat "
                "bar is unit-tested TypeScript-side"
            ),
            "chaikinOscillator": (
                "EMA(AD, fast) - EMA(AD, slow), defaults 3 / 10; "
                "cross-checked against TA-Lib ADOSC EXACTLY - the one "
                "EMA-family study with no seed delta, because TA-Lib's own "
                "ADOSC seeds both EMAs on the FIRST A/D value, which is "
                "pond's convention. The SMA-seeded reconstruction is asserted "
                "to be visibly different, so the case pins the seed"
            ),
            "priceVolumeTrend": (
                "cumsum(close.pct_change() * volume) - the FRACTIONAL change, "
                "not the percent one; pandas replication (no TA-Lib PVT). "
                "PVT[0] is null (no previous close, and no vendor convention "
                "to seed from); first valid at 1, separated from the "
                "absolute-change version"
            ),
            "chaikinMoneyFlow": (
                "sum(CLV * volume) / sum(volume) over n, default 20; pandas "
                "replication (no TA-Lib CMF) on the same weighted-mean shape "
                "as VWAP. First valid at n-1, bounded by [-1, +1], and "
                "separated from the UNWEIGHTED mean of CLV"
            ),
            "moneyFlowIndex": (
                "100 * posFlow / (posFlow + negFlow) over n, flow = typical "
                "price * volume, direction from the typical price, default "
                "14; cross-checked against TA-Lib MFI. Unchanged typical "
                "price counts for neither side (TA-Lib's rule). Warm-up is n "
                "rows, not n-1. TA-Lib reports 0 for a window whose total "
                "flow is zero - or merely below 1.0 - where pond reports null "
                "(the rsi flat-window rule); neither case arises here"
            ),
            "forceIndex": (
                "EMA(close.diff() * volume, n) on POND's first-sample EMA "
                "seed, default 13 (Elder); pandas replication (no TA-Lib "
                "function). First valid at n, not n-1: the raw force has no "
                "value on bar 0 and the EMA array door waits for n finite "
                "VALUES. period 1 is the raw, unsmoothed force"
            ),
            "easeOfMovement": (
                "MA(mid.diff() * (h-l) * scale / volume, n), defaults 14 / "
                "sma / scale 100,000,000 (StockCharts, ChartIQ); pandas "
                "replication (no TA-Lib EOM). First valid at n; separated "
                "from the version that drops the bar's range, which is the "
                "half of the definition making EOM QUADRATIC in price"
            ),
            "volumeOscillator": (
                "100 * (MA(volume,fast) - MA(volume,slow)) / MA(volume,slow), "
                "defaults 5 / 10 / sma - the price oscillator's percent mode "
                "over volume, which is what the study delegates to. pandas "
                "replication (no TA-Lib function); first valid at slow-1, "
                "separated from the close-based version so the column is "
                "pinned"
            ),
            "chandeMomentum": (
                "100*(sum(up)-sum(down))/(sum(up)+sum(down)) over n bars, "
                "CHANDE's UNSMOOTHED sums, first valid at n. NOT TA-Lib's "
                "CMO, which Wilder-smooths the legs and is therefore exactly "
                "2*RSI-100 (asserted to 2.8e-14) - i.e. an affine "
                "restatement of the shipped `rsi`. The two definitions have "
                "identical warm-ups and differ by up to 68.28 points at n=14 "
                "and 131.55 at n=5, which is asserted as a separation"
            ),
            "ultimateOscillator": (
                "BP = close - min(low, prevClose), TR = TRANGE; "
                "100*(4*sumBP/sumTR(7) + 2*(14) + (28))/7; cross-checked "
                "against TA-Lib ULTOSC (7.1e-15, identical masks), with our "
                "TR asserted equal to talib.TRANGE exactly. First valid at "
                "the LONGEST period (both legs read prevClose)"
            ),
            "commodityChannelIndex": (
                "(tp - SMA(tp,n)) / (0.015 * meanAbsDev(tp,n)), tp = "
                "(h+l+c)/3, deviation taken about the window's own MEAN; "
                "cross-checked against TA-Lib CCI (3.6e-12 at n=20, 1.5e-11 "
                "at n=5 - float summation order), first valid at n-1, and "
                "separated from the standard-deviation (z-score) version. "
                "Default period 20 is ChartIQ's; TA-Lib's own default is 14"
            ),
            "intradayMomentumIndex": (
                "100*sum(gains)/(sum(gains)+sum(losses)) over the candle "
                "BODY (close-open), PLAIN sums not Wilder smoothing; pandas "
                "replication (no TA-Lib IMI), first valid at n-1 (a body "
                "needs no previous bar), separated from the close-to-close "
                "form"
            ),
            "relativeVigorIndex": (
                "num = swma(close-open), den = swma(high-low) with swma the "
                "SYMMETRIC (1,2,2,1)/6 4-bar filter; rvi = sum(num,n)/"
                "sum(den,n); signal = swma(rvi) - TradingView's definition. "
                "pandas replication (no TA-Lib RVI), first valid at n+2 and "
                "n+5, separated from the linear-WMA(4) version"
            ),
            "directionalMovement": (
                "Wilder's DMS. +DM/-DM = the part of the bar's move outside "
                "the previous bar's range on the side that moved further (at "
                "most one leg non-zero; a tie is zero for both); +DI = "
                "100*Wilder(+DM,n)/Wilder(TR,n) with the denominator the SAME "
                "array `atr` uses; DX = 100*|+DI - -DI|/(+DI + -DI) with a "
                "zero sum reading 0 (the numerator is forced to zero with "
                "it); ADX = Wilder(DX,n); ADXR = (ADX[i] + ADX[i-n+1])/2, "
                "TA-Lib's n-1 look-back and the package's own bar-count "
                "reading (the literal `n` reading differs by up to 2.64 "
                "points at n=14). SEED: Wilder's own - the mean of the first "
                "n values, i.e. `wilderValues`. TA-Lib instead seeds +DM/-DM/"
                "TR on the first n-1 and takes one decayed step, which its "
                "own ATR does not do; masks are identical either way, the "
                "FORMULA is asserted exactly by replaying our pipeline on "
                "TA-Lib's seed (<=2.9e-14 on all five columns), and the "
                "pond-seed transient is bounded and asserted to decay"
            ),
            "aroon": (
                "100*(n - barsSinceExtreme)/n over a window of n+1 BARS (n "
                "counts the oldest AGE reportable), ties to the MOST RECENT "
                "bar - both measured against TA-Lib AROON/AROONOSC, exact, "
                "identical masks, first valid at n"
            ),
            "vortex": (
                "+VI = sum|high - prevLow| / sum TR, -VI = sum|low - prevHigh| "
                "/ sum TR over n bars (Botes & Siepman 2010); pandas "
                "replication (no TA-Lib vortex), first valid at n, separated "
                "from the plain-bar-range denominator. sum TR = 0 -> undefined "
                "(unlike DX, the numerator is NOT forced to zero with it)"
            ),
            "psychologicalLine": (
                "100 * count(close > prevClose) / n, strictly greater (an "
                "unchanged close is NOT up); pandas replication (no TA-Lib "
                "PSY), first valid at n (bar 0 has no direction)"
            ),
            "chaikinVolatility": (
                "100 * (E[i]/E[i-roc] - 1) where E = EMA(high-low, period) on "
                "POND's first-sample seed, defaults 10 / 10; PLAIN range, not "
                "true range (Chaikin's). pandas replication - no TA-Lib "
                "function - first valid at period-1+roc, separated from the "
                "true-range version"
            ),
            "massIndex": (
                "sum over sumPeriod of EMA(range, emaPeriod) / "
                "EMA(EMA(range, emaPeriod), emaPeriod), defaults 9 / 25 "
                "(Dorsey's). A SUM, not a mean - the 27 / 26.5 reversal-bulge "
                "thresholds are on the sum. The EMA chain follows the TRIX "
                "rule (stage 2 starts at stage 1's first valid bar), so the "
                "first valid bar is 2*emaPeriod + sumPeriod - 3; pandas "
                "replication, separated from the mean version"
            ),
            "choppinessIndex": (
                "100 * log10(sum(TR, n) / (HH - LL over n)) / log10(n), "
                "default 14; TRUE range (the ATR family's). The log base "
                "cancels here (asserted: log10/log10 and ln/ln agree to "
                "1e-16), so log10 is a presentation choice. First valid at n, not "
                "n-1 (TR[0] does not exist); bounded 0..100 on this fixture; "
                "pandas replication, separated from the plain-range version. "
                "n must be >= 2 (log10(1) = 0)"
            ),
            "ulcerIndex": (
                "sqrt(mean over n of (100*(close - rollingMax(close,n))/"
                "rollingMax(close,n))^2), default 14 - Peter Martin's, in "
                "StockCharts' ROLLING form (Martin's own is cumulative over "
                "the whole history, a different deliverable). Two chained "
                "windows, so the first valid bar is 2n-2; pandas replication, "
                "separated from the mean-ABSOLUTE (Pain Index) form"
            ),
            "verticalHorizontalFilter": (
                "(HH - LL over n) / sum(|close change|, n), default 28 (Adam "
                "White's); a FRACTION, not a percent, and both halves read "
                "the same column. n changes need n+1 closes, so the first "
                "valid bar is n and not n-1; bounded (0, 1]; pandas "
                "replication, separated from the (n-1)-term path version"
            ),
            "gopalakrishnanRangeIndex": (
                "ln(HH - LL over n) / ln(n), default 10 - i.e. log base n of "
                "the window's range. The log base CANCELS (asserted: ln/ln "
                "and log10/log10 agree to 1e-16), so ChartIQ's natural log is "
                "a presentation choice; a MIXED base is the slip, and that is "
                "separated. First valid at n-1; pandas replication. "
                "Scale-ADDITIVE rather than scale-invariant: scaling every "
                "price by k shifts the reading by exactly ln(k)/ln(n), which "
                "the generator asserts. n must be >= 2"
            ),
            "linearRegression": (
                "one rolling OLS fit of the column against the bar index "
                "x = 0..n-1 with x = 0 the OLDEST bar (TA-Lib's convention), "
                "default 14, five columns off it: linregValue = the fit at "
                "x = n-1 (TA-Lib LINEARREG), linregSlope (LINEARREG_SLOPE), "
                "linregIntercept = the fit at x = 0, i.e. the window's FIRST "
                "bar (LINEARREG_INTERCEPT), linregAngle = degrees(atan(slope)) "
                "(LINEARREG_ANGLE - SCALE-DEPENDENT, TA-Lib applies no "
                "normalisation) and linregR2 = corr(x, y)^2, which TA-Lib has "
                "no function for. The four TA-Lib readings are asserted exact, "
                "mask and values; all five warm up together at n-1. A flat "
                "window: slope 0 (the numerator is forced to zero) but r2 null "
                "(a genuine 0/0)"
            ),
            "timeSeriesForecast": (
                "intercept + slope*period - the same fit projected one bar "
                "PAST the window (x = n), default 14; cross-checked against "
                "TA-Lib TSF, mask and values. Deliberately NOT a member of the "
                "K2 MaType menu: every type there is the identity at period 1 "
                "and a one-bar window has no slope"
            ),
            "chandeForecastOscillator": (
                "100 * (close - TSF)/close, default 14 (Tushar Chande); "
                "pandas replication on the TA-Lib-checked TSF - no TA-Lib CFO "
                "exists - first valid at n-1, separated from the version that "
                "subtracts the IN-WINDOW endpoint (LINEARREG) and from the "
                "divide-by-forecast version. A zero price reads null (the "
                "division is at the output, so the guard is live)"
            ),
            "centerOfGravity": (
                "-sum((k+1)*close[i-k], k = 0..n-1) / sum(close[i-k]), "
                "default 10 (John Ehlers, Stocks & Commodities May 2002). The "
                "NEWEST bar carries weight 1, so the reading is negative and a "
                "flat window balances at -(n+1)/2; this is TradingView's "
                "ta.cog convention, not Ehlers' own EasyLanguage, which adds "
                "(n+1)/2 to re-centre on zero (a constant offset, asserted). "
                "pandas replication - no TA-Lib CG - first valid at n-1, "
                "bounded [-n, -1], separated from the ascending-weight reading"
            ),
            "correlation": (
                "Pearson's r between `column` and `benchmark` over period "
                "bars, default 30; == talib.CORREL bar-for-bar on the RAW "
                "prices (TA-Lib correlates the inputs, not their returns). "
                "The comparison series is a COLUMN on the same joined series, "
                "never a second TimeSeries. First valid at period-1; the "
                "window is STRICT (all period rows of BOTH columns). A flat "
                "window is 0/0 -> undefined here and 0.0 in TA-Lib (measured) "
                "- the one deliberate delta"
            ),
            "beta": (
                "cov(r_column, r_benchmark)/var(r_benchmark) over period "
                "one-bar returns, default 5; == talib.BETA(benchmark, close) "
                "bar-for-bar -- TA-Lib regresses its SECOND input on its "
                "FIRST (measured), so the benchmark goes first. Returns are "
                "taken INSIDE the study (pass prices), as percentChange(v, 1). "
                "First valid at period (n returns need n+1 prices). A flat "
                "benchmark window is 0/0 -> undefined here and 0.0 in TA-Lib; "
                "a zero price is a MISSING return here and a 0 return in "
                "TA-Lib"
            ),
            "priceRelative": (
                "close / benchmark, bar by bar - ChartIQ's Price Relative, "
                "also published as Relative Strength (comparative); NOT "
                "Wilder's RSI. No period and no warm-up; a zero benchmark is "
                "undefined (a LIVE guard - the division is the output). "
                "pandas replication - no TA-Lib function - separated from the "
                "inverted ratio"
            ),
            "performanceIndex": (
                "(close[i]/close[i-n]) / (bench[i]/bench[i-n]), default 20 - "
                "each side's own n-bar growth, divided; the RATIO form, so 1 "
                "is parity (some vendors publish x100 or -1). Asserted "
                "identical to percentChange(priceRelative, n) after (x-1)*100. "
                "First valid at n; three LIVE zero guards. pandas replication "
                "- no TA-Lib function - separated from the DIFFERENCE of the "
                "two growths. Trading Technologies publishes a different "
                "formula under this name (a moving-average baseline rather "
                "than a lagged one); that variant is a composition of shipped "
                "primitives and is not this study"
            ),
            "relativeVolatilityIndex": (
                "100*U/(U+D) where U = Wilder(sigma on up bars, period), "
                "D = Wilder(sigma on the rest, period) and sigma = "
                "rolling(stdevPeriod).std(ddof=0) of the close; defaults 14 / "
                "10 (Dorsey as revised; his 1993 original used stdevPeriod 9). "
                "An unchanged close counts as a DOWN bar, unlike rsi's "
                "up/down split which gives a flat bar 0 on both legs. First "
                "valid at stdevPeriod + period - 2; bounded 0..100; pandas "
                "replication - no TA-Lib RVI of either kind - separated from "
                "the EMA-smoothed (TradingView) fork. Its output column is "
                "`relVol`, NOT `rvi`, which belongs to relativeVigorIndex"
            ),
            "parabolicSar": (
                "Wilder's Parabolic SAR == talib.SAR(high, low, acceleration, "
                "maximum) EXACTLY (0.0 max |delta| at (0.02,0.2), (0.05,0.5), "
                "(0.01,0.1)), identical warm-up (first valid at bar 1). Three "
                "prose-ambiguous details are pinned to TA-Lib's reading and "
                "probed rather than recalled: the initial side comes from "
                "Wilder's -DM over the first one-bar move (short only if the "
                "low fell further than the high rose; a tie is long), the "
                "first printed bar treats yesterday's extremes as today's, "
                "and the clamp against the last two bars applies to the "
                "reversal override as well as the ordinary advance. Two "
                "columns: psar (the stop) and psarTrend (+1 long / -1 short), "
                "because the clamp can print the stop exactly ON an extreme, "
                "so `psar < low` is not a safe derivation of the side. "
                "SAREXT (offset-on-reverse, signed short output) is out of "
                "scope. TA-Lib checks no NaN at all, so its answer on a gap "
                "is numeric garbage; ours resets the machine ([PND-SFOLD])"
            ),
            "superTrend": (
                "Olivier Seban's SuperTrend as TradingView's ta.supertrend "
                "implements it, defaults 10 / 3: basic bands (high+low)/2 +/- "
                "multiplier*ATR(period) on WILDER's ATR (the same _atr_series "
                "`atr` and `keltner` use); the final band ratchets towards "
                "price and is released when the PREVIOUS close closed through "
                "the PREVIOUS final band; the side then flips on THIS bar's "
                "close against the JUST-RATCHETED band. The generator "
                "separates the no-ratchet and ratchet-on-current-close "
                "readings; the flip-order reading (previous band instead of "
                "the just-ratcheted one) is UNOBSERVABLE whenever the close "
                "sits inside its own bar and multiplier >= 1 — a structural "
                "guarantee in that regime only (below 1 it separates on other "
                "data; this fixture reads 0.0 across period 2..20 x multiplier "
                "0.2..4.0 by luck) — and is asserted as an equality at "
                "multiplier 1 so a future fixture that separates it there "
                "fails loudly. Two columns: st "
                "(the live band) and stTrend (+1 = line below price). The "
                "bands themselves are NOT emitted - `st` already is whichever "
                "band is live. Seed: on the first finite-ATR bar both bands "
                "take their basic values and the side starts DOWN, mirroring "
                "ta.supertrend's `direction := 1` branch (Pine's sign is "
                "inverted from ours). First valid at `period`. pandas "
                "replication - no TA-Lib SuperTrend"
            ),
            "atrTrailingStop": (
                "The close-anchored ATR trailing stop (Sylvain Vervoort's, the "
                "form TradingView's ATR Trailing Stop / UT Bot scripts "
                "implement), defaults 14 / 3: d = multiplier*ATR(period) on "
                "Wilder's ATR; four cases - ratchet up while long "
                "(max(prev, close-d)), ratchet down while short "
                "(min(prev, close+d)), and a flip to close-d / close+d when "
                "the close crosses the stop. A close exactly ON the stop "
                "resolves SHORT (the fourth case is `otherwise`). Two columns: "
                "ats and atsTrend (+1 = stop below price). First valid at "
                "`period`. Separated from the un-ratcheted band AND from the "
                "CHANDELIER anchor (rolling extreme instead of the close), "
                "which is a different study, reachable as donchian + atr and "
                "deliberately not a knob here. pandas replication"
            ),
            "negativeVolumeIndex": (
                "Fosback/Dysart's NVI: index starts at `start` (1000) on bar "
                "0 and compounds the bar's simple close return ONLY when that "
                "bar's volume was LOWER than the previous bar's; otherwise it "
                "holds. A FLAT volume holds on both NVI and PVI (strict "
                "comparison each way), so the two do not partition the bars "
                "- the fixture has NO adjacent-equal volume, so that rule is "
                "pinned by the TypeScript unit tests, not here. "
                "No period and no warm-up. Scale-invariant in price and in "
                "volume. A zero previous close is undefined from there on "
                "(the division is the output). pandas replication - no TA-Lib "
                "NVI - separated from the wrong-side (PVI) reading"
            ),
            "positiveVolumeIndex": (
                "Fosback's PVI - negativeVolumeIndex's twin, compounding only "
                "when volume ROSE. Every convention there applies, including "
                "that a flat volume holds"
            ),
            "klinger": (
                "Klinger's ORIGINAL volume oscillator (1997, as StockCharts "
                "documents it), defaults 34 / 55 / 13: trend = +1 if "
                "(high+low+close) rose else -1; dm = high - low; cm "
                "accumulates dm while the trend holds and re-bases on "
                "dm[i-1]+dm[i] when it turns (which is also the seed); vf = "
                "volume * |2 * (dm/cm - 1)| * trend * 100; kvo = EMA(vf, "
                "fast) - EMA(vf, slow) and kvoSignal = EMA(kvo, signal), all "
                "on POND's first-sample EMA seed (the macd precedent). "
                "F-AMBIG: separated from the |2*(dm/cm) - 1| misreading and "
                "from TradingView's ta.kvo, which drops the dm/cm factor "
                "entirely (signed volume only) and is a DIFFERENT indicator, "
                "not an option. cm = 0 (a leg of zero-range bars) is a "
                "genuine 0/0 -> undefined. The volume force starts at bar 1, "
                "so kvo lands at `slow` and the signal at slow+signal-1. "
                "pandas replication - no TA-Lib Klinger"
            ),
            "guppy": (
                "Daryl Guppy's GMMA: the FIXED twelve moving averages, short "
                "3/5/8/10/12/15 as gmmaS{n} and long 30/35/40/45/50/60 as "
                "gmmaL{n}, default type ema. No TA-Lib GMMA, but each column "
                "is checked against talib.MA at its own period (exact for "
                "sma; for ema the formula on TA-Lib's SMA seed plus a "
                "GEOMETRIC-decay check on pond's first-sample seed, because "
                "at n = 60 only 21 of the 80 bars are shared and the "
                "transient is still 0.367% of scale at the last one). "
                "Per-column warm-up: n-1 each, so gmmaS3 starts at bar 2 and "
                "gmmaL60 at bar 59. All twelve asserted pairwise distinct"
            ),
            "rainbow": (
                "Mel Widner's Rainbow (TASC July 1997): TEN RECURSIVE "
                "averages, each smoothing the PREVIOUS one, rainbow1.."
                "rainbow10; defaults period 2 / sma. Through the ARRAY door, "
                "so stage k first valid at k*(period-1) - the composed "
                "warm-up is the case's main claim. pandas replication (no "
                "TA-Lib Rainbow), separated from the increasing-LENGTH "
                "variant: stage 10 is 0.83 (period 2, sma) / 1.44 (period 3, "
                "ema) from "
                "the plain SMA covering the same support, on a fixture whose "
                "close range is 19.36"
            ),
            "rainbowOscillator": (
                "ChartIQ's: 100*(close - mean(stack))/(HH - LL over "
                "lookback), with bands +/-100*(max(stack) - min(stack))/"
                "(HH - LL); defaults period 2 / lookback 10. HH/LL are of the "
                "SOURCE COLUMN, not a bar's high and low. All three columns "
                "warm up together at max(10*(period-1), lookback-1). A flat "
                "lookback window is null: nothing forces the numerators to "
                "zero (the stack reaches back past the window), so it is a "
                "real number over zero rather than a 0/0 - measured >10 "
                "points away at lookback 3 and still 0.0044 away at lookback "
                "10. Not reachable on this fixture; unit-tested instead. "
                "F-AMBIG, so the two nearest variants are measured: 67.27 "
                "from the divide-by-PRICE form and 49.16 from the "
                "first-average numerator, against a reading that spans "
                "-68.6..63.8"
            ),
            "kst": (
                "Martin Pring's Know Sure Thing, the intermediate DAILY set: "
                "1*SMA(ROC(10),10) + 2*SMA(ROC(15),10) + 3*SMA(ROC(20),10) + "
                "4*SMA(ROC(30),15), signal = SMA(kst, 9). ROC is PERCENT "
                "((x/x[-n] - 1)*100), matching percentChange / TA-Lib ROC - "
                "the ratio form would shift the line by 1000. The twelve "
                "numbers are NOT options (Pring published several KSTs; they "
                "are different studies), only signalPeriod is. Line first "
                "valid at 44, signal at 44 + signalPeriod - 1. pandas "
                "replication - no TA-Lib KST - separated from the "
                "equal-weight sum (80.61) and from the unsmoothed one "
                "(68.53), on a line spanning -56.91..125.44"
            ),
            "priceMomentumOscillator": (
                "DecisionPoint's PMO: customEMA(10*customEMA(1-bar percent "
                "ROC, 35), 20) with a SPAN EMA(10) signal. 'Custom smoothing' "
                "is alpha = 2/n, NOT the span EMA's 2/(n+1) - the one "
                "non-span exponential in the package, which is why the kernel "
                "has a raw-alpha door. Measured: the span-EMA build sits "
                "0.1060 away (2.71% of a 3.906 scale) and a custom-smoothed "
                "SIGNAL 0.0883 away, while moving the x10 to either end "
                "changes nothing (8.9e-16 - every stage is homogeneous). No "
                "period options; line first valid at 54, signal at 63. pandas "
                "replication - no TA-Lib PMO. The fixture's PMO does not "
                "cross zero (the series drifts up throughout), so the assert "
                "is on its spread"
            ),
            "stochasticRsi": (
                "Chande & Kroll's: the stochastic construction over the RSI. "
                "raw = 100*(r - LL)/(HH - LL) over stochPeriod bars of the "
                "rsi, K = SMA(raw, kPeriod), D = SMA(K, dPeriod); defaults 14 "
                "/ 14 / 3 / 3. TA-Lib STOCHRSI returns fastk and fastd, NOT a "
                "slowed %K and %D, so the mapping is measured rather than "
                "assumed: our K == talib fastd (fastk_period=stochPeriod, "
                "fastd_period=kPeriod) bar-for-bar with identical masks, the "
                "unemitted raw position == talib fastk, and our D has no "
                "TA-Lib counterpart. Crossing K with fastk is 45 points on "
                "this fixture, and the generator asserts that too. Option "
                "names are TradingView's: stochPeriod here is `stochastic`'s "
                "kPeriod, and kPeriod here is its `slowing`. A flat RSI "
                "window is null (TA-Lib says 0) - the shared "
                "percentOfRangeValues rule"
            ),
            "trueStrengthIndex": (
                "William Blau's TSI: 100 * EMA(EMA(diff, longPeriod), "
                "shortPeriod) / EMA(EMA(|diff|, longPeriod), shortPeriod), "
                "signal = EMA(tsi, signalPeriod); defaults 25 / 13 / 7. The "
                "ORDER is the definition - long first, then short - and the "
                "swap sits 15.93 away on a line spanning -28.70..80.69, which "
                "the generator asserts. No TA-Lib TSI, so a pandas "
                "replication; the EMA STAGE is nonetheless checked against "
                "talib.EMA on TA-Lib's own SMA seed (5.6e-16). Bounded "
                "-100..100 by |num| <= den; a flat column gives den = 0 with "
                "num forced to 0, so the division is a literal 0/0 -> null "
                "with NO guard in the study (one would be dead code - "
                "measured). First valid at "
                "longPeriod + shortPeriod - 1 = 37, signal at 43. Options are "
                "longPeriod / shortPeriod, not long / short - bare 'long' is "
                "position vocabulary in a financial package"
            ),
            "movingAverageDeviation": (
                "close - MA(n) in PRICE UNITS, defaults 20 / sma, MA-type. "
                "The corpus lists this study as 'points or percent'; the "
                "PERCENT form is already shipped as disparityIndex, and the "
                "case asserts 100*maDev/MA == disparity EXACTLY (0.0, bit for "
                "bit), which is why only the points form ships and there is "
                "no `mode` flag - two indicators behind an option is the "
                "keltner precedent this package avoids. Separated from "
                "momentum(n) = close - close[-n], the wrong turn that leaves "
                "the shape intact. First valid at the average's own bar; no "
                "division anywhere, so no zero-denominator case. Linear in "
                "price AND shift-INVARIANT (the constant cancels between the "
                "price and its own average), which is exactly what the "
                "percent form is not"
            ),
            "priceTransforms": (
                "The K3 per-bar price summaries (assessment 6.8), all four "
                "EXACT against TA-Lib with identical (empty) warm-up masks: "
                "typicalPrice (h+l+c)/3 = TYPPRICE, medianPrice (h+l)/2 = "
                "MEDPRICE, weightedClose (h+l+2c)/4 = WCLPRICE, averagePrice "
                "(o+h+l+c)/4 = AVGPRICE. The SUMMATION ORDER is TA-Lib's in "
                "both four-term cases - addition does not associate, and "
                "summing averagePrice in OHLC order instead differs in the "
                "last bits (asserted non-zero here), which is enough to lose "
                "the exactness. No warm-up at all: a bar's own prices are all "
                "any of them reads, so bar 0 is defined. Linear in price and "
                "shift-EQUIVARIANT (they are weighted means of prices)"
            ),
            "balanceOfPower": (
                "Igor Livshin's BOP = (close - open) / (high - low), the "
                "body over the range, bounded [-1, 1]. The RAW per-bar form "
                "is the default and is EXACT against TA-Lib BOP; the optional "
                "`period` (+ maType) is ChartIQ's smoothed form, a pandas "
                "replication with the analytic first valid bar asserted "
                "(F-AMBIG: both conventions are in circulation, so the "
                "TA-Lib-pinnable one ships as the default). A flat bar "
                "(high == low) is 0, not null - the numerator is forced to "
                "zero because a bar with no range traded at one price, the "
                "clvValues rule from #699 - and TA-Lib agrees, though it gets "
                "there via a 1e-8 threshold where we test range == 0 exactly; "
                "the generator asserts the fixture's narrowest bar is clear "
                "of that gap so the exactness is not resting on it. Scale- "
                "AND shift-INVARIANT: both the body and the range are "
                "differences of prices"
            ),
            "starcBands": (
                "Manning Stoller's STARC: MA(CLOSE) +/- mult * ATR, defaults "
                "20 / 15 / 2 / sma. Distinct from keltner (MA of TYPICAL "
                "PRICE) and from atrBands (bands around an existing column, "
                "no middle) -- the generator asserts the centre's separation "
                "from keltner's on this fixture. Per-column warm-up: centre "
                "at the MA's own bar, bands at max(centre, ATR). pandas "
                "replication on the same _atr_series TA-Lib's ATR is checked "
                "against; no TA-Lib STARC. Linear in price AND translation-"
                "equivariant (the width is an ATR, a difference)"
            ),
            "highLowBands": (
                "MA(median price) x (1 +/- shift%), defaults 10 / 1% / trima. "
                "The corpus (6.2) names the formula but not the average, so "
                "trima is pond's default and the whole MaType menu is "
                "available. The centre is the MEDIAN price, and the generator "
                "asserts its separation from the same average of the CLOSE "
                "(which is what envelope() computes). MULTIPLICATIVE bands, "
                "so the width grows with the level -- asserted, since an "
                "additive bug would otherwise pass. Linear in price but NOT "
                "shift-equivariant. Option spelled `shift` (this indicator's "
                "own label) where envelope spells the same quantity `percent`"
            ),
            "bollingerDerived": (
                "bollingerBandwidth = 100*(upper-lower)/middle (the "
                "StockCharts / ChartIQ / TradingView x100 form, NOT "
                "Bollinger's bare ratio) and bollingerPercentB = "
                "(price-lower)/(upper-lower) (the DECIMAL form, 0 = lower "
                "band and 1 = upper, NOT x100). Both run the same "
                "rollingColumns avg+stdev pass bollinger itself makes "
                "(population ddof=0), and both re-form the bands with "
                "bollinger's own expression so the identities hold BIT-for-"
                "bit rather than to rounding. A flat window splits them and "
                "that is the point: bandwidth is 0 (its numerator is "
                "2*k*sigma, forced to zero, over a non-zero centre) while %B "
                "is null (a genuine 0/0 -- the numerator is zero only because "
                "the denominator is), and bollinger's own bands are null "
                "there too. Bandwidth is scale-invariant but NOT shift-"
                "invariant; %B is both. First valid at period-1 for both. No "
                "TA-Lib function for either"
            ),
            "primeStudies": (
                "primeNumberBands = the smallest prime >= high and the "
                "largest <= low (a band that CONTAINS the bar); "
                "primeNumberOscillator = close - nearestPrime(close), our "
                "sign convention (positive = the price sits above the prime "
                "nearest it), with a TIE going to the LOWER prime. Both are "
                "step functions of the PRICE LEVEL rather than statistics: no "
                "window, no warm-up, no history, and NEITHER scale- nor "
                "shift-equivariant (the primes do not move with the data) -- "
                "the property tests assert that ABSENCE rather than skipping "
                "it. A value below 2 has no prime neighbourhood and reads "
                "null; so does anything past 2**53. Replicated over a "
                "pure-Python sieve of Eratosthenes rather than sympy, which "
                "the oracle venv does not carry -- and which would be a "
                "poor reference anyway for a study whose whole content is "
                "which integers are prime"
            ),
            "marketFacilitationIndex": (
                "Bill Williams' MFI = (high - low) / volume, the RAW ratio. "
                "Output is `bwmfi`, NOT `mfi`: moneyFlowIndex (Quong & "
                "Soudack's RSI-on-money-flow, a different indicator "
                "published under the same abbreviation) already owns that "
                "name, and two studies cannot share a default output. No "
                "`scale` option -- unlike easeOfMovement, whose 100_000_000 "
                "is a PUBLISHED constant, no vendor constant is standard "
                "here, so exposing one would be inventing a default. No "
                "warm-up; a zero-volume bar is null (the guard is live -- "
                "withColumn REJECTS an infinity rather than mapping it to a "
                "gap), a flat bar is 0 (a genuine zero over a real volume). "
                "Linear in price, INVERSELY proportional to volume, and "
                "shift-invariant in price. pandas replication, no TA-Lib"
            ),
            "twiggsMoneyFlow": (
                "Colin Twiggs' money flow, default 21: the close's location "
                "in the bar's TRUE range (max(high, prevClose) down to "
                "min(low, prevClose)) times volume, Wilder-smoothed against a "
                "matching Wilder smoothing of volume. F-AMBIG on the "
                "smoothing - Wilder's exponential form ships (Incredible "
                "Charts / Twiggs' own published algorithm) and the WINDOW-SUM "
                "fork sits 0.0706 away at 21; chaikinMoneyFlow(21) sits "
                "0.1494 away, which is wider than the reading's own range. "
                "The denominator is blanked wherever the numerator is, so the "
                "two smoothings consume the same bars. First valid at "
                "`period` (bar 0 has no true range). No TA-Lib TMF"
            ),
            "tradeVolumeIndex": (
                "Tick-direction accumulation: +1 above the dead band, -1 "
                "below it, and the PREVIOUS direction on an undecided bar "
                "(the band is closed on both sides). minTick is REQUIRED. "
                "Opens at 0, no warm-up; no first direction is invented. "
                "A pandas TRANSCRIPTION of the same machine, separated from "
                "the non-persisting fork (54584 at minTick 0.5). The "
                "UP-SEEDED fork is invisible on this fixture - bar 1 carries "
                "the largest close-to-close move, so it is decisive at any "
                "minTick worth testing - and the generator asserts that "
                "agreement is still exact; it is pinned TypeScript-side"
            ),
            "shinoharaIntensityRatio": (
                "The A and B ratios, 100 * sum(up) / sum(down) over 26 bars: "
                "A against each bar's OWN open (sirStrong), B against the "
                "PREVIOUS close (sirWeak). F-AMBIG is a NAMING fork - the "
                "arithmetic is the standard pair and the strong/weak labels "
                "come from the corpus' own list; the two columns sit 23312 "
                "apart, so the labels carry information. Separated from the "
                "same-bar-close misreading (22863). Per-column warm-up 25 / "
                "26. Neither is bounded and B INVERTS on a gappy tape "
                "(prevClose - low goes negative), which this fixture does: "
                "sirWeak spans -22761..7620. No TA-Lib Shinohara"
            ),
            "elderImpulse": (
                "+1 when EMA(13) AND the MACD histogram both rose, -1 when "
                "both fell, 0 otherwise; every EMA is POND's first-sample "
                "seed, so the histogram is the one macd() produces. A "
                "three-valued column, so the separations are COUNTS of "
                "disagreeing bars rather than distances: 20 of 46 against the "
                "EMA-only build and 20 against the histogram-only one. First "
                "valid at max(emaPeriod, slow + signal - 1) = 34. No TA-Lib "
                "Elder Impulse"
            ),
            "movingAverageCross": (
                "The cross EVENT: +1 on the bar the fast average crosses "
                "above the slow one, -1 below, 0 otherwise, carrying the last "
                "NON-ZERO sign so an exact tie is not a cross and a "
                "touch-and-retreat is not either. The seed bar reports "
                "nothing, so the head is slowPeriod. A TRANSCRIPTION of the "
                "same machine; separated from the REGIME column (852 of 870 "
                "bars on the long input) and from the one-bar-late report "
                "(35). The tie rule cannot be separated here - the fixture "
                "holds no exact tie, which the generator asserts - and is "
                "pinned TypeScript-side. No TA-Lib MA cross"
            ),
            "anchoredVwap": (
                "cumsum(typicalPrice * volume) / cumsum(volume) from the "
                "first bar at or after the anchor, earlier bars null. The "
                "oracle series keys each bar by its ROW INDEX in ms, so an "
                "anchor of n is the bar at index n. Separated from the "
                "unweighted cumulative mean (1.71 at anchor 40) and from the "
                "ROLLING vwap over the same span (5.05) - except at anchor 0, "
                "where the rolling window has exactly one value, at the last "
                "bar, and the two provably coincide (asserted at 0.0). No "
                "TA-Lib anchored VWAP"
            ),
            "sessionVwap": (
                "cumsum(typicalPrice * volume) / cumsum(volume) RESET at each "
                "session open, null in closed time. Run on the SESSION clock "
                "(input.sessionTimes): the same 80 bars keyed onto a "
                "09:30-16:00 America/New_York 30-minute grid, six sessions "
                "plus two bars in no session (one stamped exactly at a close, "
                "one on a Saturday inside a holiday-widened gap). A pandas "
                "groupby-cumsum, which is a different formulation from our "
                "sequential reset loop. Separated from the NON-resetting "
                "cumulative form and from the unweighted per-session mean of "
                "typical price; the generator prints both distances. No "
                "TA-Lib session VWAP"
            ),
            "pivotPoints": (
                "The previous session's (max high, min low, last close), "
                "shifted one session and held across the next -- a pandas "
                "groupby().agg().shift(1).reindex(sid). Four formula sets: "
                "standard/floor, Fibonacci (0.382 / 0.618 / 1.000 of the "
                "range), Woodie's ((H+L+2C)/4 centre, standard ladder) and "
                "Camarilla (1.1/12, 1.1/6, 1.1/4, 1.1/2 measured from the "
                "CLOSE, and the only set with a fourth pair, so it appends 9 "
                "columns where the others append 7). First valid bar is the "
                "second session's first (13 + 2 nulls: the first session has "
                "no predecessor, the two closed-time bars have no session). "
                "The generator asserts the levels are FLAT within a session, "
                "the ladder is ordered, and the four methods are separated on "
                "R1. No TA-Lib pivot points"
            ),
        },
    },
    "input": {
        "closes": closes,
        "longCloses": long_closes,
        "opens": opens,
        "highs": highs,
        "lows": lows,
        "volumes": volumes,
        "benchmarks": benchmarks,
        "sessionTimes": session_times,
    },
    "cases": cases,
}

path = (
    pathlib.Path(__file__).resolve().parents[2]
    / "test"
    / "fixtures"
    / "study-oracle.json"
)
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(out, indent=2) + "\n")
print(
    f"wrote {path.relative_to(pathlib.Path.cwd())} ({N} bars, plus a "
    f"{LONG_N}-bar close-only input, {len(cases)} cases)"
)
