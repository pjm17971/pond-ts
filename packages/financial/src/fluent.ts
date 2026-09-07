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

/** A series schema with one optional number column appended — the shape
 *  `TimeSeries.withColumn` (and hence `sma`) yields. */
type AppendOpt<S extends SeriesSchema, Name extends string> = readonly [
  S[0],
  ...ValueColumnsForSchema<S>,
  OptionalNumberColumn<Name>,
];

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
