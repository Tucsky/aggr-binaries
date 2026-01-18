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
  if (companion.sparse) {
    return readCandlesSparse(ctx, collector, exchange, symbol, timeframe, fromIndex, toIndex, companion);
  }
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

async function readCandlesSparse(
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
  const buf = await fs.readFile(binPath);
  const recordSize = 8 + 56;
  const total = Math.floor(buf.length / recordSize);
  const firstIdx = Math.max(0, Math.min(total - 1, fromIndex));
  const lastIdx = Math.max(firstIdx, Math.min(total - 1, toIndex));
  const candles: CandleMsg["candles"] = [];
  for (let i = firstIdx; i <= lastIdx; i++) {
    const base = i * recordSize;
    const ts = Number(buf.readBigInt64LE(base));
    const open = buf.readInt32LE(base + 8) / companion.priceScale;
    const high = buf.readInt32LE(base + 12) / companion.priceScale;
    const low = buf.readInt32LE(base + 16) / companion.priceScale;
    const close = buf.readInt32LE(base + 20) / companion.priceScale;
    const buyVol = Number(buf.readBigInt64LE(base + 24)) / companion.volumeScale;
    const sellVol = Number(buf.readBigInt64LE(base + 32)) / companion.volumeScale;
    const buyCount = buf.readUInt32LE(base + 40);
    const sellCount = buf.readUInt32LE(base + 44);
    const liqBuy = Number(buf.readBigInt64LE(base + 48)) / companion.volumeScale;
    const liqSell = Number(buf.readBigInt64LE(base + 56)) / companion.volumeScale;
    candles.push({
      time: ts,
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
  return { type: "candles", fromIndex: firstIdx, toIndex: firstIdx + candles.length - 1, candles };
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

  if (!companion.sparse) {
    const tf = companion.timeframeMs ?? 60_000;
    const idx = Math.floor((startMs - companion.startTs) / tf);
    return Math.max(0, Math.min(records - 1, idx));
  }

  const binPath = path.join(ctx.outputRoot, collector, exchange, symbol, `${timeframe}.bin`);
  const recordSize = 8 + 56;
  let lo = 0;
  let hi = records - 1;
  while (lo < hi) {
    const mid = Math.floor((lo + hi) / 2);
    const ts = await readSparseTimestamp(binPath, mid, recordSize);
    if (ts >= startMs) {
      hi = mid;
    } else {
      lo = mid + 1;
    }
  }
  return lo;
}

async function readSparseTimestamp(binPath: string, index: number, recordSize: number): Promise<number> {
  const fh = await fs.open(binPath, "r");
  const buf = Buffer.allocUnsafe(8);
  await fh.read(buf, 0, 8, index * recordSize);
  await fh.close();
  return Number(buf.readBigInt64LE(0));
}
