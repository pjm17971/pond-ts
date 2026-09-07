// Benchmarks the financial studies' hot path at market-data scale: SMA /
// EMA / Bollinger on a 1M-bar series, against the hand-rolled Float64Array
// floor (the "what a bespoke loop costs" honesty reference — it skips
// missing-cell handling, minSamples, and TimeSeries construction, so treat
// it as a floor, not a target). The studies compose on core's count-window
// `rolling` columnar fast path and `smooth('ema')`'s columnar fast path;
// run from `packages/financial/` after `npm run build` at the repo root.
import { performance } from 'node:perf_hooks';
import { TimeSeries } from 'pond-ts';
import {
  MA_TYPES,
  accumulationDistribution,
  aroon,
  barsSinceExtremeValues,
  rollingBivariateValues,
  correlation,
  beta,
  priceRelative,
  performanceIndex,
  linearRegressionValues,
  linearRegression,
  timeSeriesForecast,
  chandeForecastOscillator,
  centerOfGravity,
  guppy,
  rainbow,
  rainbowOscillator,
  kst,
  priceMomentumOscillator,
  stochasticRsi,
  trueStrengthIndex,
  movingAverageDeviation,
  directionalMovement,
  vortex,
  atrBands,
  awesomeOscillator,
  chaikinMoneyFlow,
  chaikinOscillator,
  easeOfMovement,
  forceIndex,
  moneyFlowIndex,
  priceVolumeTrend,
  volumeOscillator,
  bollinger,
  chaikinVolatility,
  choppinessIndex,
  gopalakrishnanRangeIndex,
  massIndex,
  relativeVolatilityIndex,
  ulcerIndex,
  verticalHorizontalFilter,
  chandeMomentum,
  commodityChannelIndex,
  coppock,
  detrendedPriceOscillator,
  disparityIndex,
  donchian,
  elderRay,
  ema,
  intradayMomentumIndex,
  psychologicalLine,
  relativeVigorIndex,
  ultimateOscillator,
  keltner,
  macd,
  movingAverage,
  obv,
  priceOscillator,
  qstick,
  rsi,
  sma,
  stochastic,
  trix,
  vwap,
  williamsR,
  foldRows,
  parabolicSar,
  superTrend,
  atrTrailingStop,
  negativeVolumeIndex,
  positiveVolumeIndex,
  klinger,
  stochasticMomentumIndex,
  fisherTransform,
  schaffTrendCycle,
  prettyGoodOscillator,
  swingIndex,
  accumulativeSwingIndex,
  randomWalkIndex,
  ravi,
  trendIntensityIndex,
  specialK,
  twiggsMoneyFlow,
  tradeVolumeIndex,
  shinoharaIntensityRatio,
  elderImpulse,
  movingAverageCross,
  anchoredVwap,
  sessionVwap,
  pivotPoints,
  TradingCalendar,
  typicalPrice,
  medianPrice,
  weightedClose,
  averagePrice,
  balanceOfPower,
  starcBands,
  highLowBands,
  bollingerBandwidth,
  bollingerPercentB,
  primeNumberBands,
  primeNumberOscillator,
  marketFacilitationIndex,
} from '../dist/index.js';

const PERIOD = 20;

/** The two calendars the session-anchored studies are benchmarked against.
 *  `makeBars` stamps bar `i` at `1_700_000_000_000 + i * 60_000` — one-minute
 *  bars round the clock — so a 1M-bar run spans ~694 days.
 *
 *  - `equity` is a 09:30-16:00 America/New_York weekday schedule: ~520
 *    sessions, and only ~27% of the bars fall in one, so the closed-time path
 *    carries most of the walk.
 *  - `allDay` is one 24-hour session per calendar day: ~760 sessions and every
 *    bar in one.
 *
 *  Running both is the O(N + sessions) evidence: the session count changes by
 *  ~1.5x and the in-session fraction by ~3.7x while the cost stays flat. A
 *  per-bar search over the schedule would be 1e6 x 5e2 = 5e8 comparisons and
 *  would not be within an order of magnitude of these numbers. */
const CAL_FROM = '2023-11-01';
const CAL_TO = '2025-11-01';
const equityCalendar = TradingCalendar.fromRules(
  { timeZone: 'America/New_York', open: '09:30', close: '16:00' },
  { from: CAL_FROM, to: CAL_TO },
);
const allDayCalendar = TradingCalendar.fromRules(
  {
    timeZone: 'UTC',
    open: '00:00',
    close: '24:00',
    weekmask: [1, 2, 3, 4, 5, 6, 7],
  },
  { from: CAL_FROM, to: CAL_TO },
);

/** `priceScale` multiplies every price column and leaves volume alone. It
 *  exists for the two prime studies, whose per-bar cost is the only one in
 *  this package that grows with the MAGNITUDE of the data rather than its
 *  length — a 1x and a 1e5x run are what make that growth visible instead
 *  of assumed. Every other benchmark uses the default 1. */
function makeBars(length, priceScale = 1) {
  const time = new Float64Array(length);
  const open = new Float64Array(length);
  const high = new Float64Array(length);
  const low = new Float64Array(length);
  const close = new Float64Array(length);
  const volume = new Float64Array(length);
  // The comparison ("benchmark") column the two-series family reads — the
  // shape a consumer gets after `align` + `joinMany`. A SECOND random walk,
  // correlated with the first through a shared drift term but with its own
  // noise, so the rolling correlation moves rather than sitting at 1 (a
  // benchmark that is a multiple of `close` would make every window
  // degenerate and would not exercise the kernel's arithmetic).
  const benchmark = new Float64Array(length);
  let px = 100;
  let bx = 60;
  for (let i = 0; i < length; i += 1) {
    time[i] = 1_700_000_000_000 + i * 60_000;
    px += Math.sin(i * 0.001) * 0.3 + ((i * 2654435761) % 97) / 970 - 0.05;
    close[i] = px * priceScale;
    // The open leans off the close by a varying amount, so the candle body
    // QStick averages changes sign rather than being a constant offset.
    open[i] = (px - 0.25 * Math.cos(i / 3.1)) * priceScale;
    // Varying half-widths, so the range studies never see a flat window.
    high[i] = (px + 0.2 + 0.3 * Math.abs(Math.sin(i / 4))) * priceScale;
    low[i] = (px - 0.2 - 0.3 * Math.abs(Math.cos(i / 3))) * priceScale;
    volume[i] = 1_000 + ((i * 40_503) % 5_000);
    bx += Math.sin(i * 0.001) * 0.18 + ((i * 40_503) % 89) / 890 - 0.05;
    benchmark[i] = bx;
  }
  const series = TimeSeries.fromColumns({
    name: 'bars',
    schema: [
      { name: 'time', kind: 'time' },
      { name: 'open', kind: 'number' },
      { name: 'high', kind: 'number' },
      { name: 'low', kind: 'number' },
      { name: 'close', kind: 'number' },
      { name: 'volume', kind: 'number' },
      { name: 'benchmark', kind: 'number' },
    ],
    columns: { time, open, high, low, close, volume, benchmark },
  });
  return { series, close, benchmark };
}

function median(values) {
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[middle - 1] + sorted[middle]) / 2
    : sorted[middle];
}

function benchmark(label, fn, repeats = 5) {
  fn(); // warm-up
  const samples = [];
  for (let i = 0; i < repeats; i += 1) {
    const start = performance.now();
    fn();
    samples.push(performance.now() - start);
  }
  return {
    label,
    medianMs: Number(median(samples).toFixed(2)),
    minMs: Number(Math.min(...samples).toFixed(2)),
    maxMs: Number(Math.max(...samples).toFixed(2)),
  };
}

function handRolledSma(close) {
  const out = new Float64Array(close.length).fill(NaN);
  let sum = 0;
  for (let i = 0; i < close.length; i += 1) {
    sum += close[i];
    if (i >= PERIOD) sum -= close[i - PERIOD];
    if (i >= PERIOD - 1) out[i] = sum / PERIOD;
  }
  return out;
}

function handRolledEma(close) {
  const out = new Float64Array(close.length).fill(NaN);
  const alpha = 2 / (PERIOD + 1);
  let prev = close[0];
  for (let i = 1; i < close.length; i += 1) {
    prev = alpha * close[i] + (1 - alpha) * prev;
    if (i >= PERIOD - 1) out[i] = prev;
  }
  return out;
}

/** The O(N·period) argmax the `barsSinceExtremeValues` deque replaces —
 *  kept here, not in `src/`, purely as the benchmark's control. */
function naiveBarsSinceMax(values, period) {
  const out = new Float64Array(values.length).fill(NaN);
  for (let i = period; i < values.length; i += 1) {
    let best = -Infinity;
    let at = -1;
    let missing = false;
    for (let j = i - period; j <= i; j += 1) {
      const v = values[j];
      if (!Number.isFinite(v)) {
        missing = true;
        break;
      }
      if (v >= best) {
        best = v;
        at = j;
      }
    }
    if (!missing) out[i] = i - at;
  }
  return out;
}

function scaleResults(length) {
  // `benchmarkColumn`, not `benchmark` — the local `benchmark()` timing
  // helper below owns that name.
  const { series, close, benchmark: benchmarkColumn } = makeBars(length);
  // ~1e7 prices, for the two prime studies whose cost grows with magnitude.
  const pricesX1e5 = makeBars(length, 1e5).series;
  // Pre-tagged, for the session studies' column door — the shape a consumer
  // who already ran `partitionBy(session)` hands them.
  const tagged = equityCalendar.tagSessions(series);
  return {
    length,
    results: [
      benchmark('hand-rolled sma floor', () => handRolledSma(close)),
      benchmark('hand-rolled ema floor', () => handRolledEma(close)),
      benchmark('sma({ period: 20 })', () => sma(series, { period: PERIOD })),
      benchmark('ema({ period: 20 })', () => ema(series, { period: PERIOD })),
      // The K2 MA-type menu, one entry per type. Every one is O(N) and
      // independent of `period`; `sma` and `ema` here are the same calls the
      // two entries above make, so a gap between them would be dispatch
      // overhead and nothing else.
      ...MA_TYPES.map((type) =>
        benchmark(`movingAverage({ 20, '${type}' })`, () =>
          movingAverage(series, { period: PERIOD, type }),
        ),
      ),
      benchmark('rsi({ period: 14 })', () => rsi(series, { period: 14 })),
      benchmark('macd({ 12, 26, 9 })', () => macd(series)),
      benchmark('obv()', () => obv(series)),
      benchmark('vwap({ period: 20 })', () => vwap(series, { period: PERIOD })),
      benchmark('bollinger({ period: 20 })', () =>
        bollinger(series, { period: PERIOD }),
      ),
      benchmark('stochastic({ 14, 3, 3 })', () => stochastic(series)),
      benchmark('williamsR({ period: 14 })', () =>
        williamsR(series, { period: 14 }),
      ),
      benchmark('donchian({ period: 20 })', () =>
        donchian(series, { period: PERIOD }),
      ),
      // The K2 consumers. Every one is options-validation plus kernel calls,
      // so what these measure is the kernels underneath: `keltner` is a
      // typical price, an MA of it and an ATR; `atrBands` is the same ATR
      // plus two adds; `qstick` a subtraction and one MA; `trix` three EMA
      // passes plus a rate of change and a fourth EMA; `coppock` two rate-of-
      // change passes and a WMA.
      benchmark('keltner({ 20, 10, 2, ema })', () => keltner(series)),
      benchmark('keltner({ maType: sma })', () =>
        keltner(series, { maType: 'sma' }),
      ),
      benchmark('atrBands({ period: 14 })', () => atrBands(series)),
      benchmark('qstick({ period: 8 })', () => qstick(series)),
      benchmark('trix({ 15, 9 })', () => trix(series)),
      benchmark('coppock({ 14, 11, 10 })', () => coppock(series)),
      // The K2 consumers. Each is two engine calls (or one, plus a column
      // read) and a per-cell pass, so their cost should read as the sum of
      // their parts — a number far above that means the study grew a loop
      // of its own.
      benchmark("priceOscillator({ 12, 26, 'ema', percent })", () =>
        priceOscillator(series),
      ),
      benchmark('disparityIndex({ period: 14 })', () =>
        disparityIndex(series, { period: 14 }),
      ),
      benchmark('detrendedPriceOscillator({ period: 20 })', () =>
        detrendedPriceOscillator(series, { period: PERIOD }),
      ),
      benchmark('elderRay({ period: 13 })', () =>
        elderRay(series, { period: 13 }),
      ),
      benchmark('awesomeOscillator({ 5, 34 })', () =>
        awesomeOscillator(series),
      ),
      // The volume & money-flow group (corpus 6.6). Two are running sums
      // over a per-bar term (A/D, PVT), one is that line smoothed twice
      // (Chaikin oscillator), two are windows over a derived array (CMF on
      // the weighted-mean kernel, MFI on two rolling means), and two are an
      // MA over a per-bar derivation (force index, ease of movement). The
      // cumulative pair should sit near obv(); the window pair near vwap().
      benchmark('accumulationDistribution()', () =>
        accumulationDistribution(series),
      ),
      benchmark('chaikinOscillator({ 3, 10 })', () =>
        chaikinOscillator(series),
      ),
      benchmark('priceVolumeTrend()', () => priceVolumeTrend(series)),
      benchmark('chaikinMoneyFlow({ period: 20 })', () =>
        chaikinMoneyFlow(series, { period: PERIOD }),
      ),
      benchmark('moneyFlowIndex({ period: 14 })', () =>
        moneyFlowIndex(series, { period: 14 }),
      ),
      benchmark('forceIndex({ period: 13 })', () =>
        forceIndex(series, { period: 13 }),
      ),
      benchmark('easeOfMovement({ 14, sma })', () =>
        easeOfMovement(series, { period: 14 }),
      ),
      benchmark('volumeOscillator({ 5, 10, sma })', () =>
        volumeOscillator(series),
      ),
      // The momentum tail. Five of the six are options-validation plus
      // O(N) kernel calls, so they should read as the sum of their parts.
      // The exception is `commodityChannelIndex`, whose mean-absolute-
      // deviation kernel is O(N·period) — the two entries below are there
      // to show that growth explicitly rather than hide it in one number.
      benchmark('chandeMomentum({ period: 14 })', () =>
        chandeMomentum(series, { period: 14 }),
      ),
      benchmark('ultimateOscillator({ 7, 14, 28 })', () =>
        ultimateOscillator(series),
      ),
      benchmark('commodityChannelIndex({ period: 20 })', () =>
        commodityChannelIndex(series, { period: 20 }),
      ),
      benchmark('commodityChannelIndex({ period: 100 })', () =>
        commodityChannelIndex(series, { period: 100 }),
      ),
      benchmark('intradayMomentumIndex({ period: 14 })', () =>
        intradayMomentumIndex(series, { period: 14 }),
      ),
      benchmark('relativeVigorIndex({ period: 10 })', () =>
        relativeVigorIndex(series, { period: 10 }),
      ),
      benchmark('psychologicalLine({ period: 12 })', () =>
        psychologicalLine(series, { period: 12 }),
      ),
      // The Wilder directional group (corpus 6.4). `directionalMovement` is
      // three Wilder smooths over two kernel derivations plus a fourth over
      // DX, so it should read as a small multiple of `atr`/`rsi`; `vortex`
      // is three rolling means. `aroon` is the interesting one: its argmax
      // kernel is a monotonic deque, so its cost must be FLAT in `period` —
      // the two entries plus the naive control below are what show that.
      benchmark('directionalMovement({ period: 14 })', () =>
        directionalMovement(series, { period: 14 }),
      ),
      benchmark('aroon({ period: 25 })', () => aroon(series, { period: 25 })),
      benchmark('aroon({ period: 200 })', () => aroon(series, { period: 200 })),
      benchmark('vortex({ period: 14 })', () => vortex(series, { period: 14 })),
      benchmark('barsSinceExtremeValues(25) [deque]', () =>
        barsSinceExtremeValues(close, 25, 'max'),
      ),
      benchmark('barsSinceExtremeValues(200) [deque]', () =>
        barsSinceExtremeValues(close, 200, 'max'),
      ),
      benchmark('naive bars-since-max(25) [control]', () =>
        naiveBarsSinceMax(close, 25),
      ),
      benchmark('naive bars-since-max(200) [control]', () =>
        naiveBarsSinceMax(close, 200),
      ),
      // The volatility tail. Six of the seven are options-validation plus
      // O(N) kernel calls; `ulcerIndex` and `verticalHorizontalFilter` each
      // add a rolling min/max pass, which is the dearest substrate in the
      // package (see the range studies above), so they should read near
      // `donchian` rather than near `ema`.
      benchmark('chaikinVolatility({ 10, 10 })', () =>
        chaikinVolatility(series),
      ),
      benchmark('massIndex({ 9, 25 })', () => massIndex(series)),
      benchmark('choppinessIndex({ period: 14 })', () =>
        choppinessIndex(series, { period: 14 }),
      ),
      benchmark('ulcerIndex({ period: 14 })', () =>
        ulcerIndex(series, { period: 14 }),
      ),
      benchmark('verticalHorizontalFilter({ period: 28 })', () =>
        verticalHorizontalFilter(series, { period: 28 }),
      ),
      benchmark('gopalakrishnanRangeIndex({ period: 10 })', () =>
        gopalakrishnanRangeIndex(series, { period: 10 }),
      ),
      benchmark('relativeVolatilityIndex({ 14, 10 })', () =>
        relativeVolatilityIndex(series),
      ),
      // The K7 regression family. The kernel is one pass with three O(1)
      // accumulator updates and an amortised rebuild, so every entry here
      // must be FLAT in `period` — the 14 / 200 pair is what shows that —
      // and `linearRegression` should sit near `sma`, whose substrate is the
      // same shape (one rolling accumulator over one column).
      benchmark('linearRegressionValues(14) [kernel]', () =>
        linearRegressionValues(close, 14),
      ),
      benchmark('linearRegressionValues(200) [kernel]', () =>
        linearRegressionValues(close, 200),
      ),
      benchmark('linearRegression({ period: 14 })', () =>
        linearRegression(series, { period: 14 }),
      ),
      benchmark('linearRegression({ period: 200 })', () =>
        linearRegression(series, { period: 200 }),
      ),
      benchmark('timeSeriesForecast({ period: 14 })', () =>
        timeSeriesForecast(series, { period: 14 }),
      ),
      benchmark('timeSeriesForecast({ period: 200 })', () =>
        timeSeriesForecast(series, { period: 200 }),
      ),
      benchmark('chandeForecastOscillator({ period: 14 })', () =>
        chandeForecastOscillator(series, { period: 14 }),
      ),
      benchmark('centerOfGravity({ period: 10 })', () =>
        centerOfGravity(series, { period: 10 }),
      ),
      benchmark('centerOfGravity({ period: 200 })', () =>
        centerOfGravity(series, { period: 200 }),
      ),
      // The two-series family (corpus 6.7). `correlation` is one bivariate
      // kernel pass and a per-row division, so it should sit near
      // `bollinger` (the other two-moment window study) and be FLAT in
      // `period` — the kernel's rebuild costs one extra accumulation per row
      // at any period, so the two entries below are what show that. `beta`
      // adds two rate-of-change passes; `priceRelative` and
      // `performanceIndex` have no window at all and should read as bare
      // column reads.
      benchmark('correlation({ period: 30 })', () =>
        correlation(series, { benchmark: 'benchmark', period: 30 }),
      ),
      benchmark('correlation({ period: 200 })', () =>
        correlation(series, { benchmark: 'benchmark', period: 200 }),
      ),
      benchmark('beta({ period: 5 })', () =>
        beta(series, { benchmark: 'benchmark', period: 5 }),
      ),
      benchmark('priceRelative()', () =>
        priceRelative(series, { benchmark: 'benchmark' }),
      ),
      benchmark('performanceIndex({ period: 20 })', () =>
        performanceIndex(series, { benchmark: 'benchmark', period: PERIOD }),
      ),
      benchmark('rollingBivariateValues(30) [bare kernel]', () =>
        rollingBivariateValues(close, benchmarkColumn, 30),
      ),
      benchmark('rollingBivariateValues(200) [bare kernel]', () =>
        rollingBivariateValues(close, benchmarkColumn, 200),
      ),
      // The moving-average stacks (corpus 6.1). `guppy` is twelve K2 column
      // calls and nothing else, so it should read as ~12x `ema()` — a
      // materially higher number would mean the study grew work of its own.
      benchmark('guppy({ type: ema }) [12 EMAs]', () => guppy(series)),
      benchmark('guppy({ type: sma }) [12 SMAs]', () =>
        guppy(series, { type: 'sma' }),
      ),
      // `rainbow` is ten CHAINED array-door averages, so it pays ten passes
      // over a derived array rather than ten over the column; the oscillator
      // adds one HH/LL scan and a ten-wide per-row reduce.
      benchmark('rainbow({ period: 2 }) [10 recursive SMAs]', () =>
        rainbow(series),
      ),
      benchmark('rainbowOscillator({ 2, 10 })', () =>
        rainbowOscillator(series),
      ),
      // The smoothed-momentum tail (corpus 6.3). `kst` is four rate-of-change
      // passes and five array SMAs; nothing here rescans a window.
      benchmark('kst({ signalPeriod: 9 })', () => kst(series)),
      benchmark('priceMomentumOscillator()', () =>
        priceMomentumOscillator(series),
      ),
      // `stochasticRsi` is `rsi` plus the STRICT monotonic-deque extremes
      // kernel and two array SMAs. The deque is flat in `period`, so the two
      // entries below should read the same — that is what they are for.
      benchmark('stochasticRsi({ 14, 14, 3, 3 })', () => stochasticRsi(series)),
      benchmark('stochasticRsi({ stochPeriod: 200 })', () =>
        stochasticRsi(series, { stochPeriod: 200 }),
      ),
      benchmark('trueStrengthIndex({ 25, 13, 7 })', () =>
        trueStrengthIndex(series),
      ),
      benchmark('movingAverageDeviation({ 20, sma })', () =>
        movingAverageDeviation(series),
      ),
      benchmark('rolling({ count: 20 }, avg) [core substrate]', () =>
        series.rolling(
          { count: PERIOD },
          { value: { from: 'close', using: 'avg' } },
          { minSamples: PERIOD },
        ),
      ),
      // The K6 stateful fold ([PND-SFOLD]). The bare kernel is one finite
      // test per input cell plus one call per complete row, so the two-column
      // entry is the floor every state machine pays and should read near
      // `ema()` (~2.5 ms), not near `sma()`. `parabolicSar` is that floor plus
      // its own arithmetic and TWO output columns; `superTrend` and
      // `atrTrailingStop` add one `atrValues` pass each (true range + Wilder),
      // so they should read as fold + `atr()`. `klinger` pays the fold, two
      // EMA passes over the derived force and a third over the line.
      benchmark('foldRows(2 cols, no-op) [bare kernel]', () =>
        foldRows([close, benchmarkColumn], 1, null, () => {}),
      ),
      benchmark('foldRows(4 cols, no-op) [bare kernel]', () =>
        foldRows(
          [close, benchmarkColumn, close, benchmarkColumn],
          1,
          null,
          () => {},
        ),
      ),
      benchmark('parabolicSar()', () => parabolicSar(series)),
      benchmark('superTrend({ period: 10, multiplier: 3 })', () =>
        superTrend(series),
      ),
      benchmark('atrTrailingStop({ period: 14, multiplier: 3 })', () =>
        atrTrailingStop(series),
      ),
      benchmark('negativeVolumeIndex()', () => negativeVolumeIndex(series)),
      benchmark('positiveVolumeIndex()', () => positiveVolumeIndex(series)),
      benchmark('klinger({ 34, 55, 13 })', () => klinger(series)),
      // The K3 price transforms (corpus 6.8). Each is ONE per-bar pass over
      // two to four columns and a `withColumn`, with no window and no
      // recursion — the cheapest study shape in the package. They are the
      // floor every other bar study pays on top of, so they should read
      // close to the hand-rolled reference, not to `sma()`.
      benchmark('typicalPrice()', () => typicalPrice(series)),
      benchmark('medianPrice()', () => medianPrice(series)),
      benchmark('weightedClose()', () => weightedClose(series)),
      benchmark('averagePrice()', () => averagePrice(series)),
      // Balance of Power: the same shape plus a division and the flat-bar
      // branch (raw), then one K2 engine call on the derived array
      // (smoothed) — so the pair shows what the optional smoothing costs.
      benchmark('balanceOfPower() [raw]', () => balanceOfPower(series)),
      benchmark('balanceOfPower({ period: 14 })', () =>
        balanceOfPower(series, { period: 14 }),
      ),
      // The bands tail (corpus 6.2). `starcBands` is one MA plus one ATR
      // plus two adds, so it should read as `keltner` less the typical
      // price; `highLowBands` is a median price, one MA and two multiplies,
      // so it should read near `envelope`. The two Bollinger derivatives
      // run the SAME avg+stdev rolling pass `bollinger` makes and then one
      // per-row division, so each should sit near `bollinger()` — measured a
      // number materially higher would mean the σ was being computed twice.
      benchmark('starcBands({ 20, 15, 2, sma })', () => starcBands(series)),
      benchmark('highLowBands({ 10, 1%, trima })', () => highLowBands(series)),
      benchmark('bollingerBandwidth({ period: 20 })', () =>
        bollingerBandwidth(series),
      ),
      benchmark('bollingerPercentB({ period: 20 })', () =>
        bollingerPercentB(series),
      ),
      // Bill Williams' MFI is a range and a division — the same shape as the
      // price transforms, so it should read with them.
      benchmark('marketFacilitationIndex()', () =>
        marketFacilitationIndex(series),
      ),
      // The prime studies. These are the ONLY operators in the package whose
      // per-bar cost depends on the MAGNITUDE of the data: primality is
      // trial division to √n and the search walks the local prime gap, so
      // both grow roughly as √p / log p. The 1x / 1e5x pairs below are the
      // measurement of that, not a formality — `pricesX1e5` puts the series
      // at ~1e7, the top of the range the corpus assessment names.
      // The 1e7 entries run FEWER repeats on purpose: at 1M bars each pass
      // is seconds, and five of them would make this script's runtime about
      // the two of them. Three samples is enough to read an 80x gap.
      benchmark('primeNumberBands() [price ~1e2]', () =>
        primeNumberBands(series),
      ),
      benchmark(
        'primeNumberBands() [price ~1e7]',
        () => primeNumberBands(pricesX1e5),
        3,
      ),
      benchmark('primeNumberOscillator() [price ~1e2]', () =>
        primeNumberOscillator(series),
      ),
      benchmark(
        'primeNumberOscillator() [price ~1e7]',
        () => primeNumberOscillator(pricesX1e5),
        3,
      ),
      // The momentum and trend leftovers (assessment 6.3 / 6.4 / 6.1).
      // Everything here is a compose-only study except `randomWalkIndex`,
      // whose kernel is the one deliberately O(N·period) walk in the batch:
      // the two entries below sit side by side so the linearity in `period`
      // stays visible, and the `swingIndexValues` pair shows the kernel's
      // own cost against the study that appends it.
      benchmark('stochasticMomentumIndex({ 13, 25, 2, 3 })', () =>
        stochasticMomentumIndex(series),
      ),
      benchmark('fisherTransform({ period: 10 })', () =>
        fisherTransform(series),
      ),
      benchmark('schaffTrendCycle({ 23, 50, 10 })', () =>
        schaffTrendCycle(series),
      ),
      benchmark('prettyGoodOscillator({ period: 14 })', () =>
        prettyGoodOscillator(series),
      ),
      benchmark('swingIndex({ limit: 5 })', () =>
        swingIndex(series, { limit: 5 }),
      ),
      benchmark('accumulativeSwingIndex({ limit: 5 })', () =>
        accumulativeSwingIndex(series, { limit: 5 }),
      ),
      benchmark('randomWalkIndex({ period: 14 })', () =>
        randomWalkIndex(series),
      ),
      benchmark('randomWalkIndex({ period: 50 })', () =>
        randomWalkIndex(series, { period: 50 }),
      ),
      benchmark('ravi({ 7, 65 })', () => ravi(series)),
      benchmark('trendIntensityIndex({ 30, 60 })', () =>
        trendIntensityIndex(series),
      ),
      benchmark('specialK()', () => specialK(series)),
      // The volume and miscellaneous leftovers (assessment 6.6 / 6.4 / 6.1).
      // Every one is compose-only: `twiggsMoneyFlow` is two column reads, a
      // true-bounds pass and two Wilder recursions, so it should sit near
      // `atr()`; the two K6 machines near `foldRows(2 cols)`; `elderImpulse`
      // near `macd()` plus one EMA; `anchoredVwap` near two cumulative
      // passes. The only entry with a window is the Shinohara pair, which is
      // four `rollingMeanValues` scans and should be FLAT in `period` — the
      // 26 / 200 pair below is what shows that.
      benchmark('twiggsMoneyFlow({ period: 21 })', () =>
        twiggsMoneyFlow(series),
      ),
      benchmark('tradeVolumeIndex({ minTick: 0.01 })', () =>
        tradeVolumeIndex(series, { minTick: 0.01 }),
      ),
      benchmark('shinoharaIntensityRatio({ period: 26 })', () =>
        shinoharaIntensityRatio(series),
      ),
      benchmark('shinoharaIntensityRatio({ period: 200 })', () =>
        shinoharaIntensityRatio(series, { period: 200 }),
      ),
      benchmark('elderImpulse({ 13, 12, 26, 9 })', () => elderImpulse(series)),
      benchmark('movingAverageCross({ 10, 30, sma })', () =>
        movingAverageCross(series),
      ),
      benchmark('movingAverageCross({ 10, 30, ema })', () =>
        movingAverageCross(series, { maType: 'ema' }),
      ),
      benchmark('anchoredVwap({ anchor: midpoint })', () =>
        anchoredVwap(series, {
          anchor: 1_700_000_000_000 + Math.floor(length / 2) * 60_000,
        }),
      ),
      // The session-anchored pair (assessment 6.9 / G4). `sessionVwap` is
      // `anchoredVwap`'s kernel with a session id per bar, so the two should
      // sit within the id walk of each other; `pivotPoints` is one aggregate
      // pass plus one arithmetic pass per column, so `camarilla` (9 columns)
      // should sit above `standard` (7) by two column writes and nothing else.
      // The two calendars differ in session count and in-session fraction —
      // see the note on `equityCalendar` for what that is measuring.
      benchmark('tagSessions(equity ~520 sessions)', () =>
        equityCalendar.tagSessions(series),
      ),
      benchmark('sessionVwap({ sessions: equity ~520 })', () =>
        sessionVwap(series, { sessions: equityCalendar }),
      ),
      benchmark('sessionVwap({ sessions: allDay ~760 })', () =>
        sessionVwap(series, { sessions: allDayCalendar }),
      ),
      benchmark('sessionVwap({ session: column })', () =>
        sessionVwap(tagged, { session: 'session' }),
      ),
      benchmark('pivotPoints({ sessions: equity, standard })', () =>
        pivotPoints(series, { sessions: equityCalendar }),
      ),
      benchmark('pivotPoints({ sessions: equity, camarilla })', () =>
        pivotPoints(series, { sessions: equityCalendar, method: 'camarilla' }),
      ),
      benchmark('pivotPoints({ session: column, standard })', () =>
        pivotPoints(tagged, { session: 'session' }),
      ),
    ],
  };
}

const scales = [100_000, 1_000_000];
console.log(JSON.stringify(scales.map(scaleResults), null, 2));
