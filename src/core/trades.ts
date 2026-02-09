import { EventType, type ParseRejectReason } from "./events.js";
export type { ParseRejectReason } from "./events.js";

export type Side = "buy" | "sell";

export interface Trade {
  ts: number;
  price: number;
  size: number;
  side: Side;
  liquidation: boolean;
  exchange?: string;
  symbol?: string;
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
// Timestamp bounds keep us in a sane ms-since-epoch range without heavy validation.
const MIN_TS_MS = 1e11; // ~1973
const MAX_TS_MS = 1e13; // ~2286
const MAX_NOTIONAL = 1e9;

export interface ParseReject {
  reason?: ParseRejectReason;
}

export function parseTradeLine(line: string, reject?: ParseReject): Trade | null {
  let i = 0;
  let start = 0;
  let end = 0;
  const len = line.length;

  const readNext = (): boolean => {
    while (i < len && line.charCodeAt(i) <= 32) i += 1;
    start = i;
    while (i < len && line.charCodeAt(i) > 32) i += 1;
    end = i;
    return end > start;
  };

  if (!readNext()) {
    if (reject) reject.reason = EventType.PartsShort;
    return null;
  }
  const ts = Number(line.slice(start, end));

  if (!readNext()) {
    if (reject) reject.reason = EventType.PartsShort;
    return null;
  }
  const price = Number(line.slice(start, end));

  if (!readNext()) {
    if (reject) reject.reason = EventType.PartsShort;
    return null;
  }
  const size = Number(line.slice(start, end));

  if (!readNext()) {
    if (reject) reject.reason = EventType.PartsShort;
    return null;
  }
  const side: Side = end - start === 1 && line.charCodeAt(start) === 49 ? "buy" : "sell";

  const hasLiquidation = readNext();
  const liquidation = hasLiquidation && end - start === 1 && line.charCodeAt(start) === 49;

  if (!Number.isFinite(ts) || !Number.isFinite(price) || !Number.isFinite(size)) {
    if (reject) reject.reason = EventType.NonFinite;
    return null;
  }

  if (ts <= MIN_TS_MS || ts >= MAX_TS_MS) {
    if (reject) reject.reason = EventType.InvalidTsRange;
    return null;
  }

  const notional = price * size;
  if (!Number.isFinite(notional) || notional > MAX_NOTIONAL) {
    if (reject) reject.reason = EventType.NotionalTooLarge;
    return null;
  }

  return { ts, price, size, side, liquidation };
}

export function accumulate(
  acc: { buckets: Map<number, Candle>; minMinute: number; maxMinute: number },
  t: Trade,
  timeframeMs: number,
): boolean {
  const slot = Math.floor(t.ts / timeframeMs) * timeframeMs;
  if (slot < acc.minMinute) acc.minMinute = slot;
  if (slot > acc.maxMinute) acc.maxMinute = slot;
  const quoteVol = BigInt(Math.round(t.price * t.size * VOL_SCALE));
  const existing = acc.buckets.get(slot);
  const isNew = existing === undefined;
  const bucket =
    existing ??
    {
      open: 0,
      high: 0,
      low: 0,
      close: 0,
      buyVol: 0n,
      sellVol: 0n,
      buyCount: 0,
      sellCount: 0,
      liqBuy: 0n,
      liqSell: 0n,
    };

  if (t.liquidation) {
    if (t.side === "buy") bucket.liqBuy += quoteVol;
    else bucket.liqSell += quoteVol;
  } else {
    const priceInt = Math.round(t.price * PRICE_SCALE);
    if (bucket.buyCount === 0 && bucket.sellCount === 0) {
      bucket.open = priceInt;
      bucket.high = priceInt;
      bucket.low = priceInt;
      bucket.close = priceInt;
    } else {
      bucket.high = Math.max(bucket.high, priceInt);
      bucket.low = Math.min(bucket.low, priceInt);
      bucket.close = priceInt;
    }

    if (t.side === "buy") {
      bucket.buyVol += quoteVol;
      bucket.buyCount += 1;
    } else {
      bucket.sellVol += quoteVol;
      bucket.sellCount += 1;
    }
  }
  acc.buckets.set(slot, bucket);
  return isNew;
}
