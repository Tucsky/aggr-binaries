import type { CandlestickData } from "lightweight-charts";

export type Status = "idle" | "connected" | "closed" | "error";

export interface Meta {
  startTs: number;
  endTs: number;
  timeframe: string;
  priceScale: number;
  volumeScale: number;
  timeframeMs: number;
  sparse: boolean;
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

export type Candle = CandlestickData & {
  buyVol: number;
  sellVol: number;
  buyCount: number;
  sellCount: number;
  liqBuy: number;
  liqSell: number;
  index: number;
};

export interface Market {
  collector: string;
  exchange: string;
  symbol: string;
}
