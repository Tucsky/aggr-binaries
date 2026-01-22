import fs from "node:fs/promises";
import path from "node:path";
import type { Db } from "../core/db.js";
import type { NormalizedCompanionMetadata } from "../core/model.js";
import { ensurePreviewTimeframe } from "./resample.js";

export interface PreviewContext {
  db: Db;
  outputRoot: string;
}

export type Companion = NormalizedCompanionMetadata;

export interface CandleMsg {
  type: "candles";
  fromIndex: number;
  toIndex: number;
  candles: Array<{
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
  }>;
}

export async function loadCompanion(
  ctx: PreviewContext,
  collector: string,
  exchange: string,
  symbol: string,
  timeframe: string,
): Promise<Companion> {
  return ensurePreviewTimeframe(ctx, collector, exchange, symbol, timeframe);
}

export async function readCandles(
  ctx: PreviewContext,
  collector: string,
  exchange: string,
  symbol: string,
  timeframe: string,
  fromIndex: number,
  toIndex: number,
  companion: Companion,
): Promise<CandleMsg> {
  const binPath = path.join(ctx.outputRoot, collector, exchange, symbol, `${timeframe}.bin`);
  const fh = await fs.open(binPath, "r");
  const tf = companion.timeframeMs ?? 60_000;
  const firstIdx = Math.max(0, Math.min(companion.records - 1, fromIndex));
  const lastIdx = Math.max(firstIdx, Math.min(companion.records - 1, toIndex));
  const count = Math.max(0, lastIdx - firstIdx + 1);
  const buf = Buffer.allocUnsafe(count * 56);
  await fh.read(buf, 0, buf.length, firstIdx * 56);
  await fh.close();

  const candles: CandleMsg["candles"] = [];
  for (let i = 0; i < count; i++) {
    const base = i * 56;
    const open = buf.readInt32LE(base) / companion.priceScale;
    const high = buf.readInt32LE(base + 4) / companion.priceScale;
    const low = buf.readInt32LE(base + 8) / companion.priceScale;
    const close = buf.readInt32LE(base + 12) / companion.priceScale;
    const buyVol = Number(buf.readBigInt64LE(base + 16)) / companion.volumeScale;
    const sellVol = Number(buf.readBigInt64LE(base + 24)) / companion.volumeScale;
    const buyCount = buf.readUInt32LE(base + 32);
    const sellCount = buf.readUInt32LE(base + 36);
    const liqBuy = Number(buf.readBigInt64LE(base + 40)) / companion.volumeScale;
    const liqSell = Number(buf.readBigInt64LE(base + 48)) / companion.volumeScale;
    candles.push({
      time: companion.startTs + (firstIdx + i) * tf,
      open,
      high,
      low,
      close,
      buyVol,
      sellVol,
      buyCount,
      sellCount,
      liqBuy,
      liqSell,
    });
  }

  return { type: "candles", fromIndex: firstIdx, toIndex: lastIdx, candles };
}

export async function computeAnchorIndex(
  ctx: PreviewContext,
  collector: string,
  exchange: string,
  symbol: string,
  timeframe: string,
  companion: Companion,
  startMs: number | null,
): Promise<number> {
  const records = companion.records;
  if (!records || records <= 0) return 0;
  if (!startMs || Number.isNaN(startMs)) {
    return records - 1;
  }

  const tf = companion.timeframeMs ?? 60_000;
  const idx = Math.floor((startMs - companion.startTs) / tf);
  return Math.max(0, Math.min(records - 1, idx));
}
