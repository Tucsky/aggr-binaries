import type { CandlestickData, HistogramData, Time, WhitespaceData } from "lightweight-charts";

export interface PreviewSeriesCandle {
  time: number;
  open?: number;
  high?: number;
  low?: number;
  close?: number;
  buyVol: number;
  sellVol: number;
  liqBuy: number;
  liqSell: number;
}

export interface PreviewSeriesPoint {
  time: Time;
  price: CandlestickData<Time> | WhitespaceData<Time>;
  totalVolume: HistogramData<Time>;
  volumeDelta: HistogramData<Time>;
  longLiquidation: HistogramData<Time>;
  shortLiquidation: HistogramData<Time>;
}

export const VOLUME_POSITIVE = "#3bca6d";
export const VOLUME_NEGATIVE = "#d62828";
export const VOLUME_POSITIVE_DIM = "rgba(59, 202, 109, 0.45)";
export const VOLUME_NEGATIVE_DIM = "rgba(214, 40, 40, 0.45)";

export function toPreviewSeriesPoint(candle: PreviewSeriesCandle): PreviewSeriesPoint {
  const time = Math.floor(Number(candle.time) / 1000) as Time;
  const totalVolume = candle.buyVol + candle.sellVol;
  const signedDelta = candle.buyVol - candle.sellVol;
  const deltaAbs = Math.abs(signedDelta);
  const positive = signedDelta >= 0;
  return {
    time,
    price: toPricePoint(candle, time),
    totalVolume: { time, value: totalVolume, color: positive ? VOLUME_POSITIVE_DIM : VOLUME_NEGATIVE_DIM },
    volumeDelta: { time, value: deltaAbs, color: positive ? VOLUME_POSITIVE : VOLUME_NEGATIVE },
    longLiquidation: { time, value: -candle.liqSell },
    shortLiquidation: { time, value: candle.liqBuy },
  };
}

export function mapPreviewSeries(candles: readonly PreviewSeriesCandle[]): PreviewSeriesPoint[] {
  const points: PreviewSeriesPoint[] = new Array(candles.length);
  for (let i = 0; i < candles.length; i += 1) {
    points[i] = toPreviewSeriesPoint(candles[i]);
  }
  return points;
}

function toPricePoint(
  candle: PreviewSeriesCandle,
  time: Time,
): CandlestickData<Time> | WhitespaceData<Time> {
  const { open, high, low, close } = candle;
  if (
    open === undefined ||
    high === undefined ||
    low === undefined ||
    close === undefined ||
    open === 0 ||
    high === 0 ||
    low === 0 ||
    close === 0
  ) {
    return { time };
  }
  return {
    time,
    open,
    high,
    low,
    close,
  };
}
