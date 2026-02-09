import type {
  CandlestickData,
  HistogramData,
  Time,
  WhitespaceData,
} from "lightweight-charts";
import { isGapCandle, toNonZero } from "../../../src/shared/chartLegend.js";
import type { Candle } from "./types.js";

export const VOLUME_POSITIVE = "#3bca6d";
export const VOLUME_NEGATIVE = "#d62828";
export const VOLUME_POSITIVE_DIM = "rgba(59, 202, 109, 0.45)";
export const VOLUME_NEGATIVE_DIM = "rgba(214, 40, 40, 0.45)";
export const LONG_LIQ_COLOR = "#ff8c00";
export const SHORT_LIQ_COLOR = "#b24dff";

export type PriceBar = CandlestickData<Time> | WhitespaceData<Time>;
export type HistBar = HistogramData<Time> | WhitespaceData<Time>;

export interface SeriesData {
  priceData: PriceBar[];
  totalVolumeData: HistBar[];
  deltaVolumeData: HistBar[];
  longLiqData: HistBar[];
  shortLiqData: HistBar[];
}

export interface SeriesUpdate {
  price: PriceBar;
  totalVolume: HistBar;
  deltaVolume: HistBar;
  longLiq: HistBar;
  shortLiq: HistBar;
}

export function toTime(timeMs: number): Time {
  return Math.floor(timeMs / 1000) as Time;
}

export function toTimeMs(time: Time | undefined): number | null {
  return typeof time === "number" && Number.isFinite(time)
    ? Math.floor(time) * 1000
    : null;
}

export function buildSeriesData(candles: readonly Candle[]): SeriesData {
  const len = candles.length;
  const priceData: PriceBar[] = new Array(len);
  const totalVolumeData: HistBar[] = new Array(len);
  const deltaVolumeData: HistBar[] = new Array(len);
  const longLiqData: HistBar[] = new Array(len);
  const shortLiqData: HistBar[] = new Array(len);

  for (let i = 0; i < len; i++) {
    const candle = candles[i];
    const update = buildSeriesUpdate(candle);
    priceData[i] = update.price;
    totalVolumeData[i] = update.totalVolume;
    deltaVolumeData[i] = update.deltaVolume;
    longLiqData[i] = update.longLiq;
    shortLiqData[i] = update.shortLiq;
  }

  return { priceData, totalVolumeData, deltaVolumeData, longLiqData, shortLiqData };
}

export function buildSeriesUpdate(candle: Candle): SeriesUpdate {
  const time = toTime(candle.time);
  const signedDelta = candle.buyVol - candle.sellVol;
  const positive = signedDelta >= 0;

  return {
    price: toPriceBar(candle, time),
    totalVolume: toHistogramBar(
      time,
      toNonZero(candle.buyVol + candle.sellVol),
      positive ? VOLUME_POSITIVE_DIM : VOLUME_NEGATIVE_DIM,
    ),
    deltaVolume: toHistogramBar(
      time,
      toNonZero(Math.abs(signedDelta)),
      positive ? VOLUME_POSITIVE : VOLUME_NEGATIVE,
    ),
    longLiq: toHistogramBar(time, toNonZero(-candle.liqSell)),
    shortLiq: toHistogramBar(time, toNonZero(candle.liqBuy)),
  };
}

export function hasHistogramValue(point: HistBar): point is HistogramData<Time> {
  return (point as HistogramData<Time>).value !== undefined;
}

function toPriceBar(candle: Candle, time: Time): PriceBar {
  if (isGapCandle(candle)) return { time };
  return {
    time,
    open: candle.open,
    high: candle.high,
    low: candle.low,
    close: candle.close,
  };
}

function toHistogramBar(time: Time, value: number | null, color?: string): HistBar {
  if (value === null) return { time };
  if (color === undefined) return { time, value };
  return { time, value, color };
}
