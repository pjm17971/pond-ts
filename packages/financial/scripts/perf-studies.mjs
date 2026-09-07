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
} from '../dist/index.js';

const PERIOD = 20;

function makeBars(length) {
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
    close[i] = px;
    // The open leans off the close by a varying amount, so the candle body
    // QStick averages changes sign rather than being a constant offset.
    open[i] = px - 0.25 * Math.cos(i / 3.1);
    // Varying half-widths, so the range studies never see a flat window.
    high[i] = px + 0.2 + 0.3 * Math.abs(Math.sin(i / 4));
    low[i] = px - 0.2 - 0.3 * Math.abs(Math.cos(i / 3));
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
    ],
  };
}

const scales = [100_000, 1_000_000];
console.log(JSON.stringify(scales.map(scaleResults), null, 2));
