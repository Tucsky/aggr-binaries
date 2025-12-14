export type Side = "buy" | "sell";

export interface Trade {
  ts: number;
  price: number;
  size: number;
  side: Side;
  liquidation: boolean;
  exchange: string;
  symbol: string;
}

export interface Candle {
  open: number;
  high: number;
  low: number;
  close: number;
  buyVol: bigint;
  sellVol: bigint;
  buyCount: number;
  sellCount: number;
  liqBuy: bigint;
  liqSell: bigint;
}

export const PRICE_SCALE = 1e4; // int32 safe for typical crypto prices
export const VOL_SCALE = 1e6; // quote volume micro units
export const CANDLE_BYTES = 56;

export function parseTradeLine(line: string, pathExchange?: string | null, pathSymbol?: string | null): Trade | null {
  const parts = line.trim().split(/\s+/);
  if (parts.length < 4) return null;
  const ts = Number(parts[0]);
  const price = Number(parts[1]);
  const size = Number(parts[2]);
  const side = parts[3] === "1" ? "buy" : "sell";
  const liquidation = parts[4] === "1";
  if (!pathExchange || !pathSymbol) return null;
  const exchange = pathExchange;
  const symbol = pathSymbol;
  if (!Number.isFinite(ts) || !Number.isFinite(price) || !Number.isFinite(size)) return null;
  return { ts, price, size, side, liquidation, exchange, symbol };
}

export function accumulate(
  acc: { buckets: Map<number, Candle>; minMinute: number; maxMinute: number },
  t: Trade,
  timeframeMs: number,
): boolean {
  const slot = Math.floor(t.ts / timeframeMs) * timeframeMs;
  if (slot < acc.minMinute) acc.minMinute = slot;
  if (slot > acc.maxMinute) acc.maxMinute = slot;
  const priceInt = Math.round(t.price * PRICE_SCALE);
  const quoteVol = BigInt(Math.round(t.price * t.size * VOL_SCALE));
  const existing = acc.buckets.get(slot);
  const isNew = existing === undefined;
  const bucket =
    existing ??
    {
      open: priceInt,
      high: priceInt,
      low: priceInt,
      close: priceInt,
      buyVol: 0n,
      sellVol: 0n,
      buyCount: 0,
      sellCount: 0,
      liqBuy: 0n,
      liqSell: 0n,
    };
  bucket.high = Math.max(bucket.high, priceInt);
  bucket.low = Math.min(bucket.low, priceInt);
  bucket.close = priceInt;
  if (t.side === "buy") {
    bucket.buyVol += quoteVol;
    bucket.buyCount += 1;
    if (t.liquidation) bucket.liqBuy += quoteVol;
  } else {
    bucket.sellVol += quoteVol;
    bucket.sellCount += 1;
    if (t.liquidation) bucket.liqSell += quoteVol;
  }
  acc.buckets.set(slot, bucket);
  return isNew;
}
