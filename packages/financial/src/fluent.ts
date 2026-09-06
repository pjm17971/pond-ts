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
