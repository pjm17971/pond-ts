/**
 * **Opt-in fluent studies.** Importing this module for its side effect mounts
 * the studies as chainable methods on `TimeSeries`, so composition reads like
 * the core operators it sits beside:
 *
 * ```ts
 * import '@pond-ts/financial/fluent';
 *
 * const study = bars
 *   .sma({ period: 20 })
 *   .ema({ period: 12 })
 *   .bollinger({ period: 20, stdDev: 2 });
 * ```
 *
 * It is **opt-in by import** — the default entry (`import { sma } from
 * '@pond-ts/financial'`) leaves `TimeSeries` untouched, so a non-financial
 * project never sees `.sma()` on its series. The methods are exactly the
 * standalone {@link sma}/{@link ema}/{@link bollinger} functions bound to
 * `this`; this file adds no new behaviour, only the calling style. (Same
 * prototype-augmentation pattern core uses to mount the column methods.)
 *
 * **Caveats (prototype augmentation):**
 * - Import it for its **runtime** side effect, not only its types. The
 *   `declare module` merge is compilation-global (once any file imports this,
 *   `.sma()` type-checks everywhere), but the methods only exist at runtime in a
 *   program that actually loaded this module — so `import '@pond-ts/financial/fluent'`
 *   in your entry, not just where the types are handy.
 * - **ESM only.** The `./fluent` subpath ships no CJS build; `require()` can't
 *   opt in. Use the standalone functions from a CJS context.
 * - If core ever adds a `TimeSeries` method named `sma`/`ema`/`bollinger`, this
 *   would shadow it — a deliberate, reviewable collision, not a silent surprise.
 */
import { TimeSeries } from 'pond-ts';
import type {
  OptionalNumberColumn,
  SeriesSchema,
  SmoothAppendSchema,
  ValueColumnsForSchema,
} from 'pond-ts';
import {
  sma as smaStudy,
  ema as emaStudy,
  movingAverage as movingAverageStudy,
} from './studies/moving-average.js';
import type {
  MovingAverageOptions,
  MovingAverageTypeOptions,
} from './studies/moving-average.js';
import { bollinger as bollingerStudy } from './studies/bollinger.js';
import type { BollingerOptions } from './studies/bollinger.js';
import {
  rollingStdev as rollingStdevStudy,
  rollingMin as rollingMinStudy,
  rollingMax as rollingMaxStudy,
  rollingPercentile as rollingPercentileStudy,
} from './studies/rolling-stat.js';
import type {
  RollingStatOptions,
  RollingPercentileOptions,
} from './studies/rolling-stat.js';
import { zScore as zScoreStudy } from './studies/z-score.js';
import type { ZScoreOptions } from './studies/z-score.js';
import { envelope as envelopeStudy } from './studies/envelope.js';
import type { EnvelopeOptions } from './studies/envelope.js';
import { percentChange as percentChangeStudy } from './studies/percent-change.js';
import type { RsiOptions } from './studies/rsi.js';
import { rsi as rsiStudy } from './studies/rsi.js';
import type { MacdOptions } from './studies/macd.js';
import { macd as macdStudy } from './studies/macd.js';
import type { AtrOptions } from './studies/atr.js';
import { atr as atrStudy } from './studies/atr.js';
import type { PercentChangeOptions } from './studies/percent-change.js';
import type { MomentumOptions } from './studies/momentum.js';
import { momentum as momentumStudy } from './studies/momentum.js';
import type { HistoricalVolatilityOptions } from './studies/volatility.js';
import { historicalVolatility as historicalVolatilityStudy } from './studies/volatility.js';
import type { StochasticOptions } from './studies/stochastic.js';
import { stochastic as stochasticStudy } from './studies/stochastic.js';
import type { WilliamsROptions } from './studies/williams-r.js';
import { williamsR as williamsRStudy } from './studies/williams-r.js';
import type { DonchianOptions } from './studies/donchian.js';
import { donchian as donchianStudy } from './studies/donchian.js';
import type { ObvOptions } from './studies/obv.js';
import { obv as obvStudy } from './studies/obv.js';
import type { VwapOptions } from './studies/vwap.js';
import { vwap as vwapStudy } from './studies/vwap.js';
import type { KeltnerOptions } from './studies/keltner.js';
import { keltner as keltnerStudy } from './studies/keltner.js';
import type { AtrBandsOptions } from './studies/atr-bands.js';
import { atrBands as atrBandsStudy } from './studies/atr-bands.js';
import type { QstickOptions } from './studies/qstick.js';
import { qstick as qstickStudy } from './studies/qstick.js';
import type { TrixOptions } from './studies/trix.js';
import { trix as trixStudy } from './studies/trix.js';
import type { CoppockOptions } from './studies/coppock.js';
import { coppock as coppockStudy } from './studies/coppock.js';
import type { PriceOscillatorOptions } from './studies/price-oscillator.js';
import { priceOscillator as priceOscillatorStudy } from './studies/price-oscillator.js';
import type { DisparityIndexOptions } from './studies/disparity-index.js';
import { disparityIndex as disparityIndexStudy } from './studies/disparity-index.js';
import type { DetrendedPriceOscillatorOptions } from './studies/detrended-price-oscillator.js';
import { detrendedPriceOscillator as detrendedPriceOscillatorStudy } from './studies/detrended-price-oscillator.js';
import type { ElderRayOptions } from './studies/elder-ray.js';
import { elderRay as elderRayStudy } from './studies/elder-ray.js';
import type { AwesomeOscillatorOptions } from './studies/awesome-oscillator.js';
import { awesomeOscillator as awesomeOscillatorStudy } from './studies/awesome-oscillator.js';
import type { AccumulationDistributionOptions } from './studies/accumulation-distribution.js';
import { accumulationDistribution as accumulationDistributionStudy } from './studies/accumulation-distribution.js';
import type { ChaikinOscillatorOptions } from './studies/chaikin-oscillator.js';
import { chaikinOscillator as chaikinOscillatorStudy } from './studies/chaikin-oscillator.js';
import type { PriceVolumeTrendOptions } from './studies/price-volume-trend.js';
import { priceVolumeTrend as priceVolumeTrendStudy } from './studies/price-volume-trend.js';
import type { ChaikinMoneyFlowOptions } from './studies/chaikin-money-flow.js';
import { chaikinMoneyFlow as chaikinMoneyFlowStudy } from './studies/chaikin-money-flow.js';
import type { MoneyFlowIndexOptions } from './studies/money-flow-index.js';
import { moneyFlowIndex as moneyFlowIndexStudy } from './studies/money-flow-index.js';
import type { ForceIndexOptions } from './studies/force-index.js';
import { forceIndex as forceIndexStudy } from './studies/force-index.js';
import type { EaseOfMovementOptions } from './studies/ease-of-movement.js';
import { easeOfMovement as easeOfMovementStudy } from './studies/ease-of-movement.js';
import type { VolumeOscillatorOptions } from './studies/volume-oscillator.js';
import { volumeOscillator as volumeOscillatorStudy } from './studies/volume-oscillator.js';
import type { ChandeMomentumOptions } from './studies/chande-momentum.js';
import { chandeMomentum as chandeMomentumStudy } from './studies/chande-momentum.js';
import type { UltimateOscillatorOptions } from './studies/ultimate-oscillator.js';
import { ultimateOscillator as ultimateOscillatorStudy } from './studies/ultimate-oscillator.js';
import type { CommodityChannelIndexOptions } from './studies/commodity-channel-index.js';
import { commodityChannelIndex as commodityChannelIndexStudy } from './studies/commodity-channel-index.js';
import type { IntradayMomentumIndexOptions } from './studies/intraday-momentum-index.js';
import { intradayMomentumIndex as intradayMomentumIndexStudy } from './studies/intraday-momentum-index.js';
import type { RelativeVigorIndexOptions } from './studies/relative-vigor-index.js';
import { relativeVigorIndex as relativeVigorIndexStudy } from './studies/relative-vigor-index.js';
import type { PsychologicalLineOptions } from './studies/psychological-line.js';
import { psychologicalLine as psychologicalLineStudy } from './studies/psychological-line.js';
import type { ChaikinVolatilityOptions } from './studies/chaikin-volatility.js';
import { chaikinVolatility as chaikinVolatilityStudy } from './studies/chaikin-volatility.js';
import type { MassIndexOptions } from './studies/mass-index.js';
import { massIndex as massIndexStudy } from './studies/mass-index.js';
import type { ChoppinessIndexOptions } from './studies/choppiness-index.js';
import { choppinessIndex as choppinessIndexStudy } from './studies/choppiness-index.js';
import type { UlcerIndexOptions } from './studies/ulcer-index.js';
import { ulcerIndex as ulcerIndexStudy } from './studies/ulcer-index.js';
import type { CorrelationOptions } from './studies/correlation.js';
import { correlation as correlationStudy } from './studies/correlation.js';
import type { BetaOptions } from './studies/beta.js';
import { beta as betaStudy } from './studies/beta.js';
import type { PriceRelativeOptions } from './studies/price-relative.js';
import { priceRelative as priceRelativeStudy } from './studies/price-relative.js';
import type { PerformanceIndexOptions } from './studies/performance-index.js';
import { performanceIndex as performanceIndexStudy } from './studies/performance-index.js';
import type { VerticalHorizontalFilterOptions } from './studies/vertical-horizontal-filter.js';
import { verticalHorizontalFilter as verticalHorizontalFilterStudy } from './studies/vertical-horizontal-filter.js';
import type { GopalakrishnanRangeIndexOptions } from './studies/gopalakrishnan-range-index.js';
import { gopalakrishnanRangeIndex as gopalakrishnanRangeIndexStudy } from './studies/gopalakrishnan-range-index.js';
import type { RelativeVolatilityIndexOptions } from './studies/relative-volatility-index.js';
import { relativeVolatilityIndex as relativeVolatilityIndexStudy } from './studies/relative-volatility-index.js';
import type { DirectionalMovementOptions } from './studies/directional-movement.js';
import { directionalMovement as directionalMovementStudy } from './studies/directional-movement.js';
import type { AroonOptions } from './studies/aroon.js';
import { aroon as aroonStudy } from './studies/aroon.js';
import type { VortexOptions } from './studies/vortex.js';
import { vortex as vortexStudy } from './studies/vortex.js';
import type { LinearRegressionOptions } from './studies/linear-regression.js';
import { linearRegression as linearRegressionStudy } from './studies/linear-regression.js';
import type { TimeSeriesForecastOptions } from './studies/time-series-forecast.js';
import { timeSeriesForecast as timeSeriesForecastStudy } from './studies/time-series-forecast.js';
import type { ChandeForecastOscillatorOptions } from './studies/chande-forecast-oscillator.js';
import { chandeForecastOscillator as chandeForecastOscillatorStudy } from './studies/chande-forecast-oscillator.js';
import type { CenterOfGravityOptions } from './studies/center-of-gravity.js';
import { centerOfGravity as centerOfGravityStudy } from './studies/center-of-gravity.js';
import type { GuppyOptions } from './studies/guppy.js';
import { guppy as guppyStudy } from './studies/guppy.js';
import type {
  RainbowOptions,
  RainbowOscillatorOptions,
} from './studies/rainbow.js';
import {
  rainbow as rainbowStudy,
  rainbowOscillator as rainbowOscillatorStudy,
} from './studies/rainbow.js';
import type { KstOptions } from './studies/kst.js';
import { kst as kstStudy } from './studies/kst.js';
import type { PriceMomentumOscillatorOptions } from './studies/price-momentum-oscillator.js';
import { priceMomentumOscillator as priceMomentumOscillatorStudy } from './studies/price-momentum-oscillator.js';
import type { StochasticRsiOptions } from './studies/stochastic-rsi.js';
import { stochasticRsi as stochasticRsiStudy } from './studies/stochastic-rsi.js';
import type { TrueStrengthIndexOptions } from './studies/true-strength-index.js';
import { trueStrengthIndex as trueStrengthIndexStudy } from './studies/true-strength-index.js';
import type { MovingAverageDeviationOptions } from './studies/moving-average-deviation.js';
import { movingAverageDeviation as movingAverageDeviationStudy } from './studies/moving-average-deviation.js';
import type { ParabolicSarOptions } from './studies/parabolic-sar.js';
import { parabolicSar as parabolicSarStudy } from './studies/parabolic-sar.js';
import type { SuperTrendOptions } from './studies/super-trend.js';
import { superTrend as superTrendStudy } from './studies/super-trend.js';
import type { AtrTrailingStopOptions } from './studies/atr-trailing-stop.js';
import { atrTrailingStop as atrTrailingStopStudy } from './studies/atr-trailing-stop.js';
import type { VolumeIndexOptions } from './studies/volume-index.js';
import {
  negativeVolumeIndex as negativeVolumeIndexStudy,
  positiveVolumeIndex as positiveVolumeIndexStudy,
} from './studies/volume-index.js';
import type { KlingerOptions } from './studies/klinger.js';
import { klinger as klingerStudy } from './studies/klinger.js';
import type {
  PriceTransformOptions,
  AveragePriceOptions,
} from './studies/price-transform.js';
import {
  typicalPrice as typicalPriceStudy,
  medianPrice as medianPriceStudy,
  weightedClose as weightedCloseStudy,
  averagePrice as averagePriceStudy,
} from './studies/price-transform.js';
import type { BalanceOfPowerOptions } from './studies/balance-of-power.js';
import { balanceOfPower as balanceOfPowerStudy } from './studies/balance-of-power.js';
import type { StarcBandsOptions } from './studies/starc-bands.js';
import { starcBands as starcBandsStudy } from './studies/starc-bands.js';
import type { HighLowBandsOptions } from './studies/high-low-bands.js';
import { highLowBands as highLowBandsStudy } from './studies/high-low-bands.js';
import type { BollingerDerivedOptions } from './studies/bollinger-derived.js';
import {
  bollingerBandwidth as bollingerBandwidthStudy,
  bollingerPercentB as bollingerPercentBStudy,
} from './studies/bollinger-derived.js';
import type {
  PrimeNumberBandsOptions,
  PrimeNumberOscillatorOptions,
} from './studies/prime-number.js';
import {
  primeNumberBands as primeNumberBandsStudy,
  primeNumberOscillator as primeNumberOscillatorStudy,
} from './studies/prime-number.js';
import type { MarketFacilitationIndexOptions } from './studies/market-facilitation-index.js';
import { marketFacilitationIndex as marketFacilitationIndexStudy } from './studies/market-facilitation-index.js';
import type { StochasticMomentumIndexOptions } from './studies/stochastic-momentum-index.js';
import { stochasticMomentumIndex as stochasticMomentumIndexStudy } from './studies/stochastic-momentum-index.js';
import type { FisherTransformOptions } from './studies/fisher-transform.js';
import { fisherTransform as fisherTransformStudy } from './studies/fisher-transform.js';
import type { SchaffTrendCycleOptions } from './studies/schaff-trend-cycle.js';
import { schaffTrendCycle as schaffTrendCycleStudy } from './studies/schaff-trend-cycle.js';
import type { PrettyGoodOscillatorOptions } from './studies/pretty-good-oscillator.js';
import { prettyGoodOscillator as prettyGoodOscillatorStudy } from './studies/pretty-good-oscillator.js';
import type { SwingIndexOptions } from './studies/swing-index.js';
import {
  swingIndex as swingIndexStudy,
  accumulativeSwingIndex as accumulativeSwingIndexStudy,
} from './studies/swing-index.js';
import type { RandomWalkIndexOptions } from './studies/random-walk-index.js';
import { randomWalkIndex as randomWalkIndexStudy } from './studies/random-walk-index.js';
import type { RaviOptions } from './studies/ravi.js';
import { ravi as raviStudy } from './studies/ravi.js';
import type { TrendIntensityIndexOptions } from './studies/trend-intensity-index.js';
import { trendIntensityIndex as trendIntensityIndexStudy } from './studies/trend-intensity-index.js';
import type { SpecialKOptions } from './studies/special-k.js';
import { specialK as specialKStudy } from './studies/special-k.js';
import type { TwiggsMoneyFlowOptions } from './studies/twiggs-money-flow.js';
import { twiggsMoneyFlow as twiggsMoneyFlowStudy } from './studies/twiggs-money-flow.js';
import type { TradeVolumeIndexOptions } from './studies/trade-volume-index.js';
import { tradeVolumeIndex as tradeVolumeIndexStudy } from './studies/trade-volume-index.js';
import type { ShinoharaIntensityRatioOptions } from './studies/shinohara-intensity-ratio.js';
import { shinoharaIntensityRatio as shinoharaIntensityRatioStudy } from './studies/shinohara-intensity-ratio.js';
import type { ElderImpulseOptions } from './studies/elder-impulse.js';
import { elderImpulse as elderImpulseStudy } from './studies/elder-impulse.js';
import type { MovingAverageCrossOptions } from './studies/moving-average-cross.js';
import { movingAverageCross as movingAverageCrossStudy } from './studies/moving-average-cross.js';
import type { AnchoredVwapOptions } from './studies/anchored-vwap.js';
import { anchoredVwap as anchoredVwapStudy } from './studies/anchored-vwap.js';
import type { IchimokuOptions } from './studies/ichimoku.js';
import { ichimoku as ichimokuStudy } from './studies/ichimoku.js';
import type { ZigZagOptions } from './studies/zig-zag.js';
import { zigZag as zigZagStudy } from './studies/zig-zag.js';
import type { SessionVwapOptions } from './studies/session-vwap.js';
import { sessionVwap as sessionVwapStudy } from './studies/session-vwap.js';
import type { PivotMethod } from './kernels/pivot.js';
import type {
  PivotPointsOptions,
  PivotPointsResult,
} from './studies/pivot-points.js';
import { pivotPoints as pivotPointsStudy } from './studies/pivot-points.js';

/** A series schema with one optional number column appended — the shape
 *  `TimeSeries.withColumn` (and hence `sma`) yields. */
type AppendOpt<S extends SeriesSchema, Name extends string> = readonly [
  S[0],
  ...ValueColumnsForSchema<S>,
  OptionalNumberColumn<Name>,
];

/** {@link AppendOpt} folded over a list of names — the same left-to-right
 *  chain of `withColumn` calls a wide study makes, written once rather than
 *  nested by hand. `guppy` appends twelve columns; the hand-nested form is
 *  twelve levels deep and unreadable. */
type AppendOptAll<
  S extends SeriesSchema,
  Names extends readonly string[],
> = Names extends readonly [
  infer Head extends string,
  ...infer Rest extends readonly string[],
]
  ? AppendOptAll<AppendOpt<S, Head>, Rest>
  : S;

declare module 'pond-ts' {
  interface TimeSeries<S extends SeriesSchema> {
    /** Fluent {@link sma} — requires `import '@pond-ts/financial/fluent'`. */
    sma<const Output extends string = 'sma'>(
      options: MovingAverageOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent {@link ema} — requires `import '@pond-ts/financial/fluent'`. */
    ema<const Output extends string = 'ema'>(
      options: MovingAverageOptions<S, Output>,
    ): TimeSeries<SmoothAppendSchema<S, Output>>;
    /** Fluent {@link movingAverage} — the shared MA-type engine. */
    movingAverage<const Output extends string = 'ma'>(
      options: MovingAverageTypeOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent {@link bollinger} — requires `import '@pond-ts/financial/fluent'`. */
    bollinger<const Prefix extends string = 'bb'>(
      options: BollingerOptions<S, Prefix>,
    ): TimeSeries<
      AppendOpt<
        AppendOpt<AppendOpt<S, `${Prefix}Middle`>, `${Prefix}Upper`>,
        `${Prefix}Lower`
      >
    >;
    /** Fluent rolling standard deviation. */
    rollingStdev<const Output extends string = 'stdev'>(
      options: RollingStatOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent rolling minimum. */
    rollingMin<const Output extends string = 'min'>(
      options: RollingStatOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent rolling maximum. */
    rollingMax<const Output extends string = 'max'>(
      options: RollingStatOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent rolling percentile. */
    rollingPercentile<const Output extends string = string>(
      options: RollingPercentileOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent rolling z-score. */
    zScore<const Output extends string = 'zscore'>(
      options: ZScoreOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent moving-average envelope. */
    envelope<const Prefix extends string = 'env'>(
      options: EnvelopeOptions<S, Prefix>,
    ): TimeSeries<
      AppendOpt<
        AppendOpt<AppendOpt<S, `${Prefix}Middle`>, `${Prefix}Upper`>,
        `${Prefix}Lower`
      >
    >;
    /** Fluent percent change (rate of change). */
    percentChange<const Output extends string = 'pctChange'>(
      options?: PercentChangeOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Wilder RSI. */
    rsi<const Output extends string = 'rsi'>(
      options?: RsiOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Wilder ATR. */
    atr<const Output extends string = 'atr'>(
      options?: AtrOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent MACD. */
    macd<const Prefix extends string = 'macd'>(
      options?: MacdOptions<S, Prefix>,
    ): TimeSeries<
      AppendOpt<
        AppendOpt<AppendOpt<S, `${Prefix}Line`>, `${Prefix}Signal`>,
        `${Prefix}Hist`
      >
    >;
    /** Fluent momentum (absolute `period`-bar difference). */
    momentum<const Output extends string = 'momentum'>(
      options?: MomentumOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent historical volatility (annualised σ of log returns). */
    historicalVolatility<const Output extends string = 'hv'>(
      options?: HistoricalVolatilityOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent stochastic oscillator (`%K` / `%D`). */
    stochastic<const Prefix extends string = 'stoch'>(
      options?: StochasticOptions<S, Prefix>,
    ): TimeSeries<AppendOpt<AppendOpt<S, `${Prefix}K`>, `${Prefix}D`>>;
    /** Fluent Williams %R. */
    williamsR<const Output extends string = 'williamsR'>(
      options?: WilliamsROptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Donchian channel. */
    donchian<const Prefix extends string = 'dc'>(
      options?: DonchianOptions<S, Prefix>,
    ): TimeSeries<
      AppendOpt<
        AppendOpt<AppendOpt<S, `${Prefix}Upper`>, `${Prefix}Lower`>,
        `${Prefix}Middle`
      >
    >;
    /** Fluent On-Balance Volume. */
    obv<const Output extends string = 'obv'>(
      options?: ObvOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent rolling VWAP. */
    vwap<const Output extends string = 'vwap'>(
      options: VwapOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Keltner Channel. */
    keltner<const Prefix extends string = 'kc'>(
      options?: KeltnerOptions<S, Prefix>,
    ): TimeSeries<
      AppendOpt<
        AppendOpt<AppendOpt<S, `${Prefix}Middle`>, `${Prefix}Upper`>,
        `${Prefix}Lower`
      >
    >;
    /** Fluent ATR bands (two columns — the middle is the field itself). */
    atrBands<const Prefix extends string = 'atrb'>(
      options?: AtrBandsOptions<S, Prefix>,
    ): TimeSeries<AppendOpt<AppendOpt<S, `${Prefix}Upper`>, `${Prefix}Lower`>>;
    /** Fluent QStick (moving average of the candle body). */
    qstick<const Output extends string = 'qstick'>(
      options?: QstickOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent TRIX (line + signal). */
    trix<const Prefix extends string = 'trix'>(
      options?: TrixOptions<S, Prefix>,
    ): TimeSeries<AppendOpt<AppendOpt<S, Prefix>, `${Prefix}Signal`>>;
    /** Fluent Coppock Curve. */
    coppock<const Output extends string = 'coppock'>(
      options?: CoppockOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Price Oscillator (percent by default; `mode: 'absolute'` for
     *  the points form). */
    priceOscillator<const Output extends string = 'priceOsc'>(
      options?: PriceOscillatorOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Disparity Index. */
    disparityIndex<const Output extends string = 'disparity'>(
      options?: DisparityIndexOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Detrended Price Oscillator. */
    detrendedPriceOscillator<const Output extends string = 'dpo'>(
      options?: DetrendedPriceOscillatorOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Elder Ray Index (bull / bear power). */
    elderRay<const Prefix extends string = 'elder'>(
      options?: ElderRayOptions<S, Prefix>,
    ): TimeSeries<AppendOpt<AppendOpt<S, `${Prefix}Bull`>, `${Prefix}Bear`>>;
    /** Fluent Awesome Oscillator. */
    awesomeOscillator<const Output extends string = 'ao'>(
      options?: AwesomeOscillatorOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Accumulation/Distribution line. */
    accumulationDistribution<const Output extends string = 'ad'>(
      options?: AccumulationDistributionOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Chaikin Oscillator (EMA 3 − EMA 10 of the A/D line). */
    chaikinOscillator<const Output extends string = 'chaikinOsc'>(
      options?: ChaikinOscillatorOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Price-Volume Trend. */
    priceVolumeTrend<const Output extends string = 'pvt'>(
      options?: PriceVolumeTrendOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Chaikin Money Flow. */
    chaikinMoneyFlow<const Output extends string = 'cmf'>(
      options?: ChaikinMoneyFlowOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Money Flow Index. */
    moneyFlowIndex<const Output extends string = 'mfi'>(
      options?: MoneyFlowIndexOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Elder Force Index. */
    forceIndex<const Output extends string = 'force'>(
      options?: ForceIndexOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Ease of Movement. */
    easeOfMovement<const Output extends string = 'eom'>(
      options?: EaseOfMovementOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Volume Oscillator. */
    volumeOscillator<const Output extends string = 'volOsc'>(
      options?: VolumeOscillatorOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Chande Momentum Oscillator (unsmoothed up/down sums). */
    chandeMomentum<const Output extends string = 'cmo'>(
      options?: ChandeMomentumOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Ultimate Oscillator (three horizons, weighted 4/2/1). */
    ultimateOscillator<const Output extends string = 'uo'>(
      options?: UltimateOscillatorOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Commodity Channel Index. */
    commodityChannelIndex<const Output extends string = 'cci'>(
      options?: CommodityChannelIndexOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Intraday Momentum Index (RSI form on the candle body). */
    intradayMomentumIndex<const Output extends string = 'imi'>(
      options?: IntradayMomentumIndexOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Relative Vigor Index (index + signal). */
    relativeVigorIndex<const Prefix extends string = 'rvi'>(
      options?: RelativeVigorIndexOptions<S, Prefix>,
    ): TimeSeries<AppendOpt<AppendOpt<S, Prefix>, `${Prefix}Signal`>>;
    /** Fluent Psychological Line (percent of up closes). */
    psychologicalLine<const Output extends string = 'psy'>(
      options?: PsychologicalLineOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Directional Movement System (`+DI` / `−DI` / `DX` / `ADX` /
     *  `ADXR`). */
    directionalMovement<const Prefix extends string = 'dmi'>(
      options?: DirectionalMovementOptions<S, Prefix>,
    ): TimeSeries<
      AppendOpt<
        AppendOpt<
          AppendOpt<
            AppendOpt<AppendOpt<S, `${Prefix}PlusDi`>, `${Prefix}MinusDi`>,
            `${Prefix}Dx`
          >,
          `${Prefix}Adx`
        >,
        `${Prefix}Adxr`
      >
    >;
    /** Fluent Aroon (up / down / oscillator). */
    aroon<const Prefix extends string = 'aroon'>(
      options?: AroonOptions<S, Prefix>,
    ): TimeSeries<
      AppendOpt<
        AppendOpt<AppendOpt<S, `${Prefix}Up`>, `${Prefix}Down`>,
        `${Prefix}Osc`
      >
    >;
    /** Fluent Vortex Indicator (`+VI` / `−VI`). */
    vortex<const Prefix extends string = 'vi'>(
      options?: VortexOptions<S, Prefix>,
    ): TimeSeries<AppendOpt<AppendOpt<S, `${Prefix}Plus`>, `${Prefix}Minus`>>;
    /** Fluent Chaikin Volatility (percent ROC of an EMA of the bar range). */
    chaikinVolatility<const Output extends string = 'chaikinVol'>(
      options?: ChaikinVolatilityOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Mass Index (Σ of EMA(range)/EMA(EMA(range))). */
    massIndex<const Output extends string = 'mass'>(
      options?: MassIndexOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Choppiness Index (ΣTR against the window's own range). */
    choppinessIndex<const Output extends string = 'chop'>(
      options?: ChoppinessIndexOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Ulcer Index (RMS percentage drawdown from the rolling peak). */
    ulcerIndex<const Output extends string = 'ulcer'>(
      options?: UlcerIndexOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent rolling correlation against a joined `benchmark` column. */
    correlation<const Output extends string = 'corr'>(
      options: CorrelationOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent beta of `column`'s returns against a joined `benchmark`'s. */
    beta<const Output extends string = 'beta'>(
      options: BetaOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Price Relative (`column / benchmark`, no period). */
    priceRelative<const Output extends string = 'priceRel'>(
      options: PriceRelativeOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Performance Index (period-bar growth over the benchmark's). */
    performanceIndex<const Output extends string = 'perf'>(
      options: PerformanceIndexOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Vertical Horizontal Filter (net range over path length). */
    verticalHorizontalFilter<const Output extends string = 'vhf'>(
      options?: VerticalHorizontalFilterOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Gopalakrishnan Range Index (`ln(HH − LL)/ln(period)`). */
    gopalakrishnanRangeIndex<const Output extends string = 'gapo'>(
      options?: GopalakrishnanRangeIndexOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Relative Volatility Index (RSI's form on σ; column `relVol`). */
    relativeVolatilityIndex<const Output extends string = 'relVol'>(
      options?: RelativeVolatilityIndexOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent rolling linear regression (value / slope / intercept / angle / R²). */
    linearRegression<const Prefix extends string = 'linreg'>(
      options?: LinearRegressionOptions<S, Prefix>,
    ): TimeSeries<
      AppendOpt<
        AppendOpt<
          AppendOpt<
            AppendOpt<AppendOpt<S, `${Prefix}Value`>, `${Prefix}Slope`>,
            `${Prefix}Intercept`
          >,
          `${Prefix}Angle`
        >,
        `${Prefix}R2`
      >
    >;
    /** Fluent Time Series Forecast (the regression one bar past the window). */
    timeSeriesForecast<const Output extends string = 'tsf'>(
      options?: TimeSeriesForecastOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Chande Forecast Oscillator (`100·(price − TSF)/price`). */
    chandeForecastOscillator<const Output extends string = 'cfo'>(
      options?: ChandeForecastOscillatorOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Center of Gravity (Ehlers' position-weighted balance point). */
    centerOfGravity<const Output extends string = 'cog'>(
      options?: CenterOfGravityOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Guppy Multiple Moving Average — the fixed twelve. */
    guppy<const Prefix extends string = 'gmma'>(
      options?: GuppyOptions<S, Prefix>,
    ): TimeSeries<
      AppendOptAll<
        S,
        [
          `${Prefix}S3`,
          `${Prefix}S5`,
          `${Prefix}S8`,
          `${Prefix}S10`,
          `${Prefix}S12`,
          `${Prefix}S15`,
          `${Prefix}L30`,
          `${Prefix}L35`,
          `${Prefix}L40`,
          `${Prefix}L45`,
          `${Prefix}L50`,
          `${Prefix}L60`,
        ]
      >
    >;
    /** Fluent Rainbow Moving Average — ten recursive averages. */
    rainbow<const Prefix extends string = 'rainbow'>(
      options?: RainbowOptions<S, Prefix>,
    ): TimeSeries<
      AppendOptAll<
        S,
        [
          `${Prefix}1`,
          `${Prefix}2`,
          `${Prefix}3`,
          `${Prefix}4`,
          `${Prefix}5`,
          `${Prefix}6`,
          `${Prefix}7`,
          `${Prefix}8`,
          `${Prefix}9`,
          `${Prefix}10`,
        ]
      >
    >;
    /** Fluent Know Sure Thing (line + signal). */
    kst<const Prefix extends string = 'kst'>(
      options?: KstOptions<S, Prefix>,
    ): TimeSeries<AppendOpt<AppendOpt<S, Prefix>, `${Prefix}Signal`>>;
    /** Fluent Price Momentum Oscillator (DecisionPoint; line + signal). */
    priceMomentumOscillator<const Prefix extends string = 'pmo'>(
      options?: PriceMomentumOscillatorOptions<S, Prefix>,
    ): TimeSeries<AppendOpt<AppendOpt<S, Prefix>, `${Prefix}Signal`>>;
    /** Fluent Stochastic RSI (`%K` / `%D` over the RSI's own range). */
    stochasticRsi<const Prefix extends string = 'stochRsi'>(
      options?: StochasticRsiOptions<S, Prefix>,
    ): TimeSeries<AppendOpt<AppendOpt<S, `${Prefix}K`>, `${Prefix}D`>>;
    /** Fluent True Strength Index (Blau; line + signal). */
    trueStrengthIndex<const Prefix extends string = 'tsi'>(
      options?: TrueStrengthIndexOptions<S, Prefix>,
    ): TimeSeries<AppendOpt<AppendOpt<S, Prefix>, `${Prefix}Signal`>>;
    /** Fluent Moving Average Deviation (`price − MA`, in price units). */
    movingAverageDeviation<const Output extends string = 'maDev'>(
      options?: MovingAverageDeviationOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Rainbow Oscillator (line + mirrored bands). */
    rainbowOscillator<const Prefix extends string = 'rbo'>(
      options?: RainbowOscillatorOptions<S, Prefix>,
    ): TimeSeries<
      AppendOpt<
        AppendOpt<AppendOpt<S, Prefix>, `${Prefix}Upper`>,
        `${Prefix}Lower`
      >
    >;
    /** Fluent Parabolic SAR (`${prefix}` stop + `${prefix}Trend` side). */
    parabolicSar<const Prefix extends string = 'psar'>(
      options?: ParabolicSarOptions<S, Prefix>,
    ): TimeSeries<AppendOpt<AppendOpt<S, Prefix>, `${Prefix}Trend`>>;
    /** Fluent SuperTrend (`${prefix}` line + `${prefix}Trend` side). */
    superTrend<const Prefix extends string = 'st'>(
      options?: SuperTrendOptions<S, Prefix>,
    ): TimeSeries<AppendOpt<AppendOpt<S, Prefix>, `${Prefix}Trend`>>;
    /** Fluent ATR trailing stop (`${prefix}` stop + `${prefix}Trend` side). */
    atrTrailingStop<const Prefix extends string = 'ats'>(
      options?: AtrTrailingStopOptions<S, Prefix>,
    ): TimeSeries<AppendOpt<AppendOpt<S, Prefix>, `${Prefix}Trend`>>;
    /** Fluent Negative Volume Index. */
    negativeVolumeIndex<const Output extends string = 'nvi'>(
      options?: VolumeIndexOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Positive Volume Index. */
    positiveVolumeIndex<const Output extends string = 'pvi'>(
      options?: VolumeIndexOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Klinger Volume Oscillator (`${prefix}` + `${prefix}Signal`). */
    klinger<const Prefix extends string = 'kvo'>(
      options?: KlingerOptions<S, Prefix>,
    ): TimeSeries<AppendOpt<AppendOpt<S, Prefix>, `${Prefix}Signal`>>;
    /** Fluent Stochastic Momentum Index (Blau; line + signal). */
    stochasticMomentumIndex<const Prefix extends string = 'smi'>(
      options?: StochasticMomentumIndexOptions<S, Prefix>,
    ): TimeSeries<AppendOpt<AppendOpt<S, Prefix>, `${Prefix}Signal`>>;
    /** Fluent Ehlers Fisher Transform (line + one-bar-delayed trigger). */
    fisherTransform<const Prefix extends string = 'fisher'>(
      options?: FisherTransformOptions<S, Prefix>,
    ): TimeSeries<AppendOpt<AppendOpt<S, Prefix>, `${Prefix}Signal`>>;
    /** Fluent Schaff Trend Cycle. */
    schaffTrendCycle<const Output extends string = 'stc'>(
      options?: SchaffTrendCycleOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Pretty Good Oscillator. */
    prettyGoodOscillator<const Output extends string = 'pgo'>(
      options?: PrettyGoodOscillatorOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Swing Index (Wilder; `limit` is required). */
    swingIndex<const Output extends string = 'si'>(
      options: SwingIndexOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Accumulative Swing Index (Wilder; `limit` is required). */
    accumulativeSwingIndex<const Output extends string = 'asi'>(
      options: SwingIndexOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Random Walk Index (`${prefix}High` + `${prefix}Low`). */
    randomWalkIndex<const Prefix extends string = 'rwi'>(
      options?: RandomWalkIndexOptions<S, Prefix>,
    ): TimeSeries<AppendOpt<AppendOpt<S, `${Prefix}High`>, `${Prefix}Low`>>;
    /** Fluent RAVI (Chande's Range Action Verification Index). */
    ravi<const Output extends string = 'ravi'>(
      options?: RaviOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Trend Intensity Index. */
    trendIntensityIndex<const Output extends string = 'tii'>(
      options?: TrendIntensityIndexOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Pring's Special K. */
    specialK<const Output extends string = 'specialK'>(
      options?: SpecialKOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent typical price — `(high + low + close) / 3`. */
    typicalPrice<const Output extends string = 'typicalPrice'>(
      options?: PriceTransformOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent median price — `(high + low) / 2`. */
    medianPrice<const Output extends string = 'medianPrice'>(
      options?: PriceTransformOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent weighted close — `(high + low + 2·close) / 4`. */
    weightedClose<const Output extends string = 'weightedClose'>(
      options?: PriceTransformOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent average price — `(open + high + low + close) / 4`. */
    averagePrice<const Output extends string = 'averagePrice'>(
      options?: AveragePriceOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Balance of Power — `(close − open) / (high − low)`. */
    balanceOfPower<const Output extends string = 'bop'>(
      options?: BalanceOfPowerOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent STARC Bands — MA(close) ± multiplier·ATR. */
    starcBands<const Prefix extends string = 'starc'>(
      options?: StarcBandsOptions<S, Prefix>,
    ): TimeSeries<
      AppendOpt<
        AppendOpt<AppendOpt<S, `${Prefix}Middle`>, `${Prefix}Upper`>,
        `${Prefix}Lower`
      >
    >;
    /** Fluent High Low Bands — MA(median price) × (1 ± percent%). */
    highLowBands<const Prefix extends string = 'hlb'>(
      options?: HighLowBandsOptions<S, Prefix>,
    ): TimeSeries<
      AppendOpt<
        AppendOpt<AppendOpt<S, `${Prefix}Middle`>, `${Prefix}Upper`>,
        `${Prefix}Lower`
      >
    >;
    /** Fluent Bollinger BandWidth — `100·(upper − lower)/middle`. */
    bollingerBandwidth<const Output extends string = 'bbWidth'>(
      options?: BollingerDerivedOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Bollinger %B — `(price − lower)/(upper − lower)`. */
    bollingerPercentB<const Output extends string = 'percentB'>(
      options?: BollingerDerivedOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Prime Number Bands — the primes bracketing each bar. */
    primeNumberBands<const Prefix extends string = 'pnb'>(
      options?: PrimeNumberBandsOptions<S, Prefix>,
    ): TimeSeries<AppendOpt<AppendOpt<S, `${Prefix}Upper`>, `${Prefix}Lower`>>;
    /** Fluent Prime Number Oscillator — `price − nearestPrime(price)`. */
    primeNumberOscillator<const Output extends string = 'pno'>(
      options?: PrimeNumberOscillatorOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Market Facilitation Index — `(high − low)/volume`. */
    marketFacilitationIndex<const Output extends string = 'bwmfi'>(
      options?: MarketFacilitationIndexOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Twiggs Money Flow. */
    twiggsMoneyFlow<const Output extends string = 'tmf'>(
      options?: TwiggsMoneyFlowOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Trade Volume Index (`minTick` is required). */
    tradeVolumeIndex<const Output extends string = 'tvi'>(
      options: TradeVolumeIndexOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Shinohara Intensity Ratio (`${prefix}Strong` + `${prefix}Weak`). */
    shinoharaIntensityRatio<const Prefix extends string = 'sir'>(
      options?: ShinoharaIntensityRatioOptions<S, Prefix>,
    ): TimeSeries<AppendOpt<AppendOpt<S, `${Prefix}Strong`>, `${Prefix}Weak`>>;
    /** Fluent Elder Impulse System (+1 / 0 / −1). */
    elderImpulse<const Output extends string = 'impulse'>(
      options?: ElderImpulseOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Ichimoku Kinko Hyo — five lines, no displacement applied. */
    ichimoku<const Prefix extends string = 'ichi'>(
      options?: IchimokuOptions<S, Prefix>,
    ): TimeSeries<
      AppendOptAll<
        S,
        [
          `${Prefix}Tenkan`,
          `${Prefix}Kijun`,
          `${Prefix}SenkouA`,
          `${Prefix}SenkouB`,
          `${Prefix}Chikou`,
        ]
      >
    >;
    /** Fluent ZigZag (pivots, leg direction and the interpolated line). */
    zigZag<const Prefix extends string = 'zz'>(
      options?: ZigZagOptions<S, Prefix>,
    ): TimeSeries<
      AppendOptAll<S, [`${Prefix}Pivot`, `${Prefix}Direction`, `${Prefix}Line`]>
    >;
    /** Fluent Moving Average Cross (a signal column: +1 / 0 / −1). */
    movingAverageCross<const Output extends string = 'maCross'>(
      options?: MovingAverageCrossOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Anchored VWAP (`anchor` is required). */
    anchoredVwap<const Output extends string = 'avwap'>(
      options: AnchoredVwapOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Session VWAP — exactly one of `sessions` / `session`. */
    sessionVwap<const Output extends string = 'svwap'>(
      options: SessionVwapOptions<S, Output>,
    ): TimeSeries<AppendOpt<S, Output>>;
    /** Fluent Pivot Points — the column set follows `method`
     *  (`'camarilla'` adds `${Prefix}R4` / `${Prefix}S4`). */
    pivotPoints<
      const Prefix extends string = 'pp',
      const Method extends PivotMethod = 'standard',
    >(
      options: PivotPointsOptions<S, Prefix, Method>,
    ): PivotPointsResult<S, Prefix, Method>;
  }
}

// Runtime mount. Each method delegates to the standalone study bound to `this`;
// the declared signatures above carry the precise per-study return types.
const proto = TimeSeries.prototype as unknown as Record<string, unknown>;
proto.sma = function (
  this: TimeSeries<SeriesSchema>,
  options: MovingAverageOptions<SeriesSchema, string>,
) {
  return smaStudy(this, options);
};
proto.ema = function (
  this: TimeSeries<SeriesSchema>,
  options: MovingAverageOptions<SeriesSchema, string>,
) {
  return emaStudy(this, options);
};
proto.movingAverage = function (
  this: TimeSeries<SeriesSchema>,
  options: MovingAverageTypeOptions<SeriesSchema, string>,
) {
  return movingAverageStudy(this, options);
};
proto.bollinger = function (
  this: TimeSeries<SeriesSchema>,
  options: BollingerOptions<SeriesSchema, string>,
) {
  return bollingerStudy(this, options);
};
proto.rollingStdev = function (
  this: TimeSeries<SeriesSchema>,
  options: RollingStatOptions<SeriesSchema, string>,
) {
  return rollingStdevStudy(this, options);
};
proto.rollingMin = function (
  this: TimeSeries<SeriesSchema>,
  options: RollingStatOptions<SeriesSchema, string>,
) {
  return rollingMinStudy(this, options);
};
proto.rollingMax = function (
  this: TimeSeries<SeriesSchema>,
  options: RollingStatOptions<SeriesSchema, string>,
) {
  return rollingMaxStudy(this, options);
};
proto.rollingPercentile = function (
  this: TimeSeries<SeriesSchema>,
  options: RollingPercentileOptions<SeriesSchema, string>,
) {
  return rollingPercentileStudy(this, options);
};
proto.zScore = function (
  this: TimeSeries<SeriesSchema>,
  options: ZScoreOptions<SeriesSchema, string>,
) {
  return zScoreStudy(this, options);
};
proto.envelope = function (
  this: TimeSeries<SeriesSchema>,
  options: EnvelopeOptions<SeriesSchema, string>,
) {
  return envelopeStudy(this, options);
};
proto.percentChange = function (
  this: TimeSeries<SeriesSchema>,
  options?: PercentChangeOptions<SeriesSchema, string>,
) {
  return percentChangeStudy(this, options);
};
proto.rsi = function (
  this: TimeSeries<SeriesSchema>,
  options?: RsiOptions<SeriesSchema, string>,
) {
  return rsiStudy(this, options);
};
proto.atr = function (
  this: TimeSeries<SeriesSchema>,
  options?: AtrOptions<SeriesSchema, string>,
) {
  return atrStudy(this, options);
};
proto.macd = function (
  this: TimeSeries<SeriesSchema>,
  options?: MacdOptions<SeriesSchema, string>,
) {
  return macdStudy(this, options);
};
proto.momentum = function (
  this: TimeSeries<SeriesSchema>,
  options?: MomentumOptions<SeriesSchema, string>,
) {
  return momentumStudy(this, options);
};
proto.historicalVolatility = function (
  this: TimeSeries<SeriesSchema>,
  options?: HistoricalVolatilityOptions<SeriesSchema, string>,
) {
  return historicalVolatilityStudy(this, options);
};
proto.stochastic = function (
  this: TimeSeries<SeriesSchema>,
  options?: StochasticOptions<SeriesSchema, string>,
) {
  return stochasticStudy(this, options);
};
proto.williamsR = function (
  this: TimeSeries<SeriesSchema>,
  options?: WilliamsROptions<SeriesSchema, string>,
) {
  return williamsRStudy(this, options);
};
proto.donchian = function (
  this: TimeSeries<SeriesSchema>,
  options?: DonchianOptions<SeriesSchema, string>,
) {
  return donchianStudy(this, options);
};
proto.obv = function (
  this: TimeSeries<SeriesSchema>,
  options?: ObvOptions<SeriesSchema, string>,
) {
  return obvStudy(this, options);
};
proto.vwap = function (
  this: TimeSeries<SeriesSchema>,
  options: VwapOptions<SeriesSchema, string>,
) {
  return vwapStudy(this, options);
};
proto.keltner = function (
  this: TimeSeries<SeriesSchema>,
  options?: KeltnerOptions<SeriesSchema, string>,
) {
  return keltnerStudy(this, options);
};
proto.atrBands = function (
  this: TimeSeries<SeriesSchema>,
  options?: AtrBandsOptions<SeriesSchema, string>,
) {
  return atrBandsStudy(this, options);
};
proto.qstick = function (
  this: TimeSeries<SeriesSchema>,
  options?: QstickOptions<SeriesSchema, string>,
) {
  return qstickStudy(this, options);
};
proto.trix = function (
  this: TimeSeries<SeriesSchema>,
  options?: TrixOptions<SeriesSchema, string>,
) {
  return trixStudy(this, options);
};
proto.coppock = function (
  this: TimeSeries<SeriesSchema>,
  options?: CoppockOptions<SeriesSchema, string>,
) {
  return coppockStudy(this, options);
};
proto.priceOscillator = function (
  this: TimeSeries<SeriesSchema>,
  options?: PriceOscillatorOptions<SeriesSchema, string>,
) {
  return priceOscillatorStudy(this, options);
};
proto.disparityIndex = function (
  this: TimeSeries<SeriesSchema>,
  options?: DisparityIndexOptions<SeriesSchema, string>,
) {
  return disparityIndexStudy(this, options);
};
proto.detrendedPriceOscillator = function (
  this: TimeSeries<SeriesSchema>,
  options?: DetrendedPriceOscillatorOptions<SeriesSchema, string>,
) {
  return detrendedPriceOscillatorStudy(this, options);
};
proto.elderRay = function (
  this: TimeSeries<SeriesSchema>,
  options?: ElderRayOptions<SeriesSchema, string>,
) {
  return elderRayStudy(this, options);
};
proto.awesomeOscillator = function (
  this: TimeSeries<SeriesSchema>,
  options?: AwesomeOscillatorOptions<SeriesSchema, string>,
) {
  return awesomeOscillatorStudy(this, options);
};
proto.accumulationDistribution = function (
  this: TimeSeries<SeriesSchema>,
  options?: AccumulationDistributionOptions<SeriesSchema, string>,
) {
  return accumulationDistributionStudy(this, options);
};
proto.chaikinOscillator = function (
  this: TimeSeries<SeriesSchema>,
  options?: ChaikinOscillatorOptions<SeriesSchema, string>,
) {
  return chaikinOscillatorStudy(this, options);
};
proto.priceVolumeTrend = function (
  this: TimeSeries<SeriesSchema>,
  options?: PriceVolumeTrendOptions<SeriesSchema, string>,
) {
  return priceVolumeTrendStudy(this, options);
};
proto.chaikinMoneyFlow = function (
  this: TimeSeries<SeriesSchema>,
  options?: ChaikinMoneyFlowOptions<SeriesSchema, string>,
) {
  return chaikinMoneyFlowStudy(this, options);
};
proto.moneyFlowIndex = function (
  this: TimeSeries<SeriesSchema>,
  options?: MoneyFlowIndexOptions<SeriesSchema, string>,
) {
  return moneyFlowIndexStudy(this, options);
};
proto.forceIndex = function (
  this: TimeSeries<SeriesSchema>,
  options?: ForceIndexOptions<SeriesSchema, string>,
) {
  return forceIndexStudy(this, options);
};
proto.easeOfMovement = function (
  this: TimeSeries<SeriesSchema>,
  options?: EaseOfMovementOptions<SeriesSchema, string>,
) {
  return easeOfMovementStudy(this, options);
};
proto.volumeOscillator = function (
  this: TimeSeries<SeriesSchema>,
  options?: VolumeOscillatorOptions<SeriesSchema, string>,
) {
  return volumeOscillatorStudy(this, options);
};
proto.chandeMomentum = function (
  this: TimeSeries<SeriesSchema>,
  options?: ChandeMomentumOptions<SeriesSchema, string>,
) {
  return chandeMomentumStudy(this, options);
};
proto.ultimateOscillator = function (
  this: TimeSeries<SeriesSchema>,
  options?: UltimateOscillatorOptions<SeriesSchema, string>,
) {
  return ultimateOscillatorStudy(this, options);
};
proto.commodityChannelIndex = function (
  this: TimeSeries<SeriesSchema>,
  options?: CommodityChannelIndexOptions<SeriesSchema, string>,
) {
  return commodityChannelIndexStudy(this, options);
};
proto.intradayMomentumIndex = function (
  this: TimeSeries<SeriesSchema>,
  options?: IntradayMomentumIndexOptions<SeriesSchema, string>,
) {
  return intradayMomentumIndexStudy(this, options);
};
proto.relativeVigorIndex = function (
  this: TimeSeries<SeriesSchema>,
  options?: RelativeVigorIndexOptions<SeriesSchema, string>,
) {
  return relativeVigorIndexStudy(this, options);
};
proto.psychologicalLine = function (
  this: TimeSeries<SeriesSchema>,
  options?: PsychologicalLineOptions<SeriesSchema, string>,
) {
  return psychologicalLineStudy(this, options);
};
proto.directionalMovement = function (
  this: TimeSeries<SeriesSchema>,
  options?: DirectionalMovementOptions<SeriesSchema, string>,
) {
  return directionalMovementStudy(this, options);
};
proto.aroon = function (
  this: TimeSeries<SeriesSchema>,
  options?: AroonOptions<SeriesSchema, string>,
) {
  return aroonStudy(this, options);
};
proto.vortex = function (
  this: TimeSeries<SeriesSchema>,
  options?: VortexOptions<SeriesSchema, string>,
) {
  return vortexStudy(this, options);
};
proto.chaikinVolatility = function (
  this: TimeSeries<SeriesSchema>,
  options?: ChaikinVolatilityOptions<SeriesSchema, string>,
) {
  return chaikinVolatilityStudy(this, options);
};
proto.massIndex = function (
  this: TimeSeries<SeriesSchema>,
  options?: MassIndexOptions<SeriesSchema, string>,
) {
  return massIndexStudy(this, options);
};
proto.choppinessIndex = function (
  this: TimeSeries<SeriesSchema>,
  options?: ChoppinessIndexOptions<SeriesSchema, string>,
) {
  return choppinessIndexStudy(this, options);
};
proto.ulcerIndex = function (
  this: TimeSeries<SeriesSchema>,
  options?: UlcerIndexOptions<SeriesSchema, string>,
) {
  return ulcerIndexStudy(this, options);
};
proto.verticalHorizontalFilter = function (
  this: TimeSeries<SeriesSchema>,
  options?: VerticalHorizontalFilterOptions<SeriesSchema, string>,
) {
  return verticalHorizontalFilterStudy(this, options);
};
proto.gopalakrishnanRangeIndex = function (
  this: TimeSeries<SeriesSchema>,
  options?: GopalakrishnanRangeIndexOptions<SeriesSchema, string>,
) {
  return gopalakrishnanRangeIndexStudy(this, options);
};
proto.relativeVolatilityIndex = function (
  this: TimeSeries<SeriesSchema>,
  options?: RelativeVolatilityIndexOptions<SeriesSchema, string>,
) {
  return relativeVolatilityIndexStudy(this, options);
};
proto.linearRegression = function (
  this: TimeSeries<SeriesSchema>,
  options?: LinearRegressionOptions<SeriesSchema, string>,
) {
  return linearRegressionStudy(this, options);
};
proto.timeSeriesForecast = function (
  this: TimeSeries<SeriesSchema>,
  options?: TimeSeriesForecastOptions<SeriesSchema, string>,
) {
  return timeSeriesForecastStudy(this, options);
};
proto.chandeForecastOscillator = function (
  this: TimeSeries<SeriesSchema>,
  options?: ChandeForecastOscillatorOptions<SeriesSchema, string>,
) {
  return chandeForecastOscillatorStudy(this, options);
};
proto.centerOfGravity = function (
  this: TimeSeries<SeriesSchema>,
  options?: CenterOfGravityOptions<SeriesSchema, string>,
) {
  return centerOfGravityStudy(this, options);
};
proto.correlation = function (
  this: TimeSeries<SeriesSchema>,
  options: CorrelationOptions<SeriesSchema, string>,
) {
  return correlationStudy(this, options);
};
proto.beta = function (
  this: TimeSeries<SeriesSchema>,
  options: BetaOptions<SeriesSchema, string>,
) {
  return betaStudy(this, options);
};
proto.priceRelative = function (
  this: TimeSeries<SeriesSchema>,
  options: PriceRelativeOptions<SeriesSchema, string>,
) {
  return priceRelativeStudy(this, options);
};
proto.performanceIndex = function (
  this: TimeSeries<SeriesSchema>,
  options: PerformanceIndexOptions<SeriesSchema, string>,
) {
  return performanceIndexStudy(this, options);
};
proto.parabolicSar = function (
  this: TimeSeries<SeriesSchema>,
  options?: ParabolicSarOptions<SeriesSchema, string>,
) {
  return parabolicSarStudy(this, options);
};
proto.superTrend = function (
  this: TimeSeries<SeriesSchema>,
  options?: SuperTrendOptions<SeriesSchema, string>,
) {
  return superTrendStudy(this, options);
};
proto.atrTrailingStop = function (
  this: TimeSeries<SeriesSchema>,
  options?: AtrTrailingStopOptions<SeriesSchema, string>,
) {
  return atrTrailingStopStudy(this, options);
};
proto.negativeVolumeIndex = function (
  this: TimeSeries<SeriesSchema>,
  options?: VolumeIndexOptions<SeriesSchema, string>,
) {
  return negativeVolumeIndexStudy(this, options);
};
proto.positiveVolumeIndex = function (
  this: TimeSeries<SeriesSchema>,
  options?: VolumeIndexOptions<SeriesSchema, string>,
) {
  return positiveVolumeIndexStudy(this, options);
};
proto.klinger = function (
  this: TimeSeries<SeriesSchema>,
  options?: KlingerOptions<SeriesSchema, string>,
) {
  return klingerStudy(this, options);
};
proto.typicalPrice = function (
  this: TimeSeries<SeriesSchema>,
  options?: PriceTransformOptions<SeriesSchema, string>,
) {
  return typicalPriceStudy(this, options);
};
proto.medianPrice = function (
  this: TimeSeries<SeriesSchema>,
  options?: PriceTransformOptions<SeriesSchema, string>,
) {
  return medianPriceStudy(this, options);
};
proto.weightedClose = function (
  this: TimeSeries<SeriesSchema>,
  options?: PriceTransformOptions<SeriesSchema, string>,
) {
  return weightedCloseStudy(this, options);
};
proto.averagePrice = function (
  this: TimeSeries<SeriesSchema>,
  options?: AveragePriceOptions<SeriesSchema, string>,
) {
  return averagePriceStudy(this, options);
};
proto.balanceOfPower = function (
  this: TimeSeries<SeriesSchema>,
  options?: BalanceOfPowerOptions<SeriesSchema, string>,
) {
  return balanceOfPowerStudy(this, options);
};
proto.starcBands = function (
  this: TimeSeries<SeriesSchema>,
  options?: StarcBandsOptions<SeriesSchema, string>,
) {
  return starcBandsStudy(this, options);
};
proto.highLowBands = function (
  this: TimeSeries<SeriesSchema>,
  options?: HighLowBandsOptions<SeriesSchema, string>,
) {
  return highLowBandsStudy(this, options);
};
proto.bollingerBandwidth = function (
  this: TimeSeries<SeriesSchema>,
  options?: BollingerDerivedOptions<SeriesSchema, string>,
) {
  return bollingerBandwidthStudy(this, options);
};
proto.bollingerPercentB = function (
  this: TimeSeries<SeriesSchema>,
  options?: BollingerDerivedOptions<SeriesSchema, string>,
) {
  return bollingerPercentBStudy(this, options);
};
proto.primeNumberBands = function (
  this: TimeSeries<SeriesSchema>,
  options?: PrimeNumberBandsOptions<SeriesSchema, string>,
) {
  return primeNumberBandsStudy(this, options);
};
proto.primeNumberOscillator = function (
  this: TimeSeries<SeriesSchema>,
  options?: PrimeNumberOscillatorOptions<SeriesSchema, string>,
) {
  return primeNumberOscillatorStudy(this, options);
};
proto.marketFacilitationIndex = function (
  this: TimeSeries<SeriesSchema>,
  options?: MarketFacilitationIndexOptions<SeriesSchema, string>,
) {
  return marketFacilitationIndexStudy(this, options);
};
proto.guppy = function (
  this: TimeSeries<SeriesSchema>,
  options?: GuppyOptions<SeriesSchema, string>,
) {
  return guppyStudy(this, options);
};
proto.rainbow = function (
  this: TimeSeries<SeriesSchema>,
  options?: RainbowOptions<SeriesSchema, string>,
) {
  return rainbowStudy(this, options);
};
proto.rainbowOscillator = function (
  this: TimeSeries<SeriesSchema>,
  options?: RainbowOscillatorOptions<SeriesSchema, string>,
) {
  return rainbowOscillatorStudy(this, options);
};
proto.kst = function (
  this: TimeSeries<SeriesSchema>,
  options?: KstOptions<SeriesSchema, string>,
) {
  return kstStudy(this, options);
};
proto.priceMomentumOscillator = function (
  this: TimeSeries<SeriesSchema>,
  options?: PriceMomentumOscillatorOptions<SeriesSchema, string>,
) {
  return priceMomentumOscillatorStudy(this, options);
};
proto.stochasticRsi = function (
  this: TimeSeries<SeriesSchema>,
  options?: StochasticRsiOptions<SeriesSchema, string>,
) {
  return stochasticRsiStudy(this, options);
};
proto.trueStrengthIndex = function (
  this: TimeSeries<SeriesSchema>,
  options?: TrueStrengthIndexOptions<SeriesSchema, string>,
) {
  return trueStrengthIndexStudy(this, options);
};
proto.movingAverageDeviation = function (
  this: TimeSeries<SeriesSchema>,
  options?: MovingAverageDeviationOptions<SeriesSchema, string>,
) {
  return movingAverageDeviationStudy(this, options);
};
proto.stochasticMomentumIndex = function (
  this: TimeSeries<SeriesSchema>,
  options?: StochasticMomentumIndexOptions<SeriesSchema, string>,
) {
  return stochasticMomentumIndexStudy(this, options);
};
proto.fisherTransform = function (
  this: TimeSeries<SeriesSchema>,
  options?: FisherTransformOptions<SeriesSchema, string>,
) {
  return fisherTransformStudy(this, options);
};
proto.schaffTrendCycle = function (
  this: TimeSeries<SeriesSchema>,
  options?: SchaffTrendCycleOptions<SeriesSchema, string>,
) {
  return schaffTrendCycleStudy(this, options);
};
proto.prettyGoodOscillator = function (
  this: TimeSeries<SeriesSchema>,
  options?: PrettyGoodOscillatorOptions<SeriesSchema, string>,
) {
  return prettyGoodOscillatorStudy(this, options);
};
proto.swingIndex = function (
  this: TimeSeries<SeriesSchema>,
  options: SwingIndexOptions<SeriesSchema, string>,
) {
  return swingIndexStudy(this, options);
};
proto.accumulativeSwingIndex = function (
  this: TimeSeries<SeriesSchema>,
  options: SwingIndexOptions<SeriesSchema, string>,
) {
  return accumulativeSwingIndexStudy(this, options);
};
proto.randomWalkIndex = function (
  this: TimeSeries<SeriesSchema>,
  options?: RandomWalkIndexOptions<SeriesSchema, string>,
) {
  return randomWalkIndexStudy(this, options);
};
proto.ravi = function (
  this: TimeSeries<SeriesSchema>,
  options?: RaviOptions<SeriesSchema, string>,
) {
  return raviStudy(this, options);
};
proto.trendIntensityIndex = function (
  this: TimeSeries<SeriesSchema>,
  options?: TrendIntensityIndexOptions<SeriesSchema, string>,
) {
  return trendIntensityIndexStudy(this, options);
};
proto.specialK = function (
  this: TimeSeries<SeriesSchema>,
  options?: SpecialKOptions<SeriesSchema, string>,
) {
  return specialKStudy(this, options);
};
proto.twiggsMoneyFlow = function (
  this: TimeSeries<SeriesSchema>,
  options?: TwiggsMoneyFlowOptions<SeriesSchema, string>,
) {
  return twiggsMoneyFlowStudy(this, options);
};
proto.tradeVolumeIndex = function (
  this: TimeSeries<SeriesSchema>,
  options: TradeVolumeIndexOptions<SeriesSchema, string>,
) {
  return tradeVolumeIndexStudy(this, options);
};
proto.shinoharaIntensityRatio = function (
  this: TimeSeries<SeriesSchema>,
  options?: ShinoharaIntensityRatioOptions<SeriesSchema, string>,
) {
  return shinoharaIntensityRatioStudy(this, options);
};
proto.elderImpulse = function (
  this: TimeSeries<SeriesSchema>,
  options?: ElderImpulseOptions<SeriesSchema, string>,
) {
  return elderImpulseStudy(this, options);
};
proto.movingAverageCross = function (
  this: TimeSeries<SeriesSchema>,
  options?: MovingAverageCrossOptions<SeriesSchema, string>,
) {
  return movingAverageCrossStudy(this, options);
};
proto.anchoredVwap = function (
  this: TimeSeries<SeriesSchema>,
  options: AnchoredVwapOptions<SeriesSchema, string>,
) {
  return anchoredVwapStudy(this, options);
};
proto.sessionVwap = function (
  this: TimeSeries<SeriesSchema>,
  options: SessionVwapOptions<SeriesSchema, string>,
) {
  return sessionVwapStudy(this, options);
};
proto.pivotPoints = function (
  this: TimeSeries<SeriesSchema>,
  options: PivotPointsOptions<SeriesSchema, string, PivotMethod>,
) {
  return pivotPointsStudy(this, options);
};
proto.ichimoku = function (
  this: TimeSeries<SeriesSchema>,
  options?: IchimokuOptions<SeriesSchema, string>,
) {
  return ichimokuStudy(this, options);
};
proto.zigZag = function (
  this: TimeSeries<SeriesSchema>,
  options?: ZigZagOptions<SeriesSchema, string>,
) {
  return zigZagStudy(this, options);
};
