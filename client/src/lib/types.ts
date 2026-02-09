export type Status = "idle" | "connected" | "closed" | "error";

export interface Meta {
  startTs: number;
  endTs: number;
  timeframe: string;
  priceScale: number;
  volumeScale: number;
  timeframeMs: number;
  records: number;
  anchorIndex: number;
}

export interface Prefs {
  collector: string;
  exchange: string;
  symbol: string;
  timeframe: string;
  timeframes: string[];
  start: string;
}

export interface Candle {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  buyVol: number;
  sellVol: number;
  buyCount: number;
  sellCount: number;
  liqBuy: number;
  liqSell: number;
  index: number;
}

export interface Market {
  collector: string;
  exchange: string;
  symbol: string;
}
