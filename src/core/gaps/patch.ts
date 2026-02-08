import fs from "node:fs/promises";
import path from "node:path";
import readline from "node:readline";
import type { Config } from "../config.js";
import type { Db } from "../db.js";
import { openTradeReadStream } from "./io.js";
import type { RecoveredTrade } from "./adapters/index.js";
import type { CompanionMetadata } from "../model.js";
import { normalizeCompanionRange } from "../model.js";
import { parseTimeframeMs } from "../config.js";
import { CANDLE_BYTES, accumulate, parseTradeLine, type Candle } from "../trades.js";

interface PatchTarget {
  timeframe: string;
  timeframeMs: number;
  startTs: number;
  endTs: number;
  binPath: string;
  fromSlot: number;
  toSlot: number;
  buckets: Map<number, Candle>;
}

export interface PatchBinariesResult {
  patchedTimeframes: number;
}

export async function patchBinariesForRecoveredTrades(
  config: Config,
  db: Db,
  market: { collector: string; exchange: string; symbol: string },
  filePath: string,
  insertedTrades: RecoveredTrade[],
): Promise<PatchBinariesResult> {
  if (!insertedTrades.length) return { patchedTimeframes: 0 };

  const minTs = insertedTrades.reduce((min, t) => (t.ts < min ? t.ts : min), Number.POSITIVE_INFINITY);
  const maxTs = insertedTrades.reduce((max, t) => (t.ts > max ? t.ts : max), Number.NEGATIVE_INFINITY);

  const targets = await loadPatchTargets(config, market, minTs, maxTs);
  if (!targets.length) {
    throw new Error(`No patchable binaries for ${market.collector}/${market.exchange}/${market.symbol}`);
  }

  let globalMinTs = Number.POSITIVE_INFINITY;
  let globalMaxTsExclusive = Number.NEGATIVE_INFINITY;
  for (const target of targets) {
    const min = target.fromSlot;
    const maxExclusive = target.toSlot + target.timeframeMs;
    if (min < globalMinTs) globalMinTs = min;
    if (maxExclusive > globalMaxTsExclusive) globalMaxTsExclusive = maxExclusive;
  }

  const stream = await openTradeReadStream(filePath);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  const accumulators = targets.map((target) => ({
    target,
    acc: {
      buckets: target.buckets,
      minMinute: Number.POSITIVE_INFINITY,
      maxMinute: Number.NEGATIVE_INFINITY,
    },
  }));

  for await (const line of rl) {
    const trade = parseTradeLine(line);
    if (!trade) continue;
    if (trade.ts < globalMinTs || trade.ts >= globalMaxTsExclusive) continue;

    for (const item of accumulators) {
      const slot = Math.floor(trade.ts / item.target.timeframeMs) * item.target.timeframeMs;
      if (slot < item.target.fromSlot || slot > item.target.toSlot) continue;
      accumulate(item.acc, trade, item.target.timeframeMs);
    }
  }
  rl.close();

  for (const target of targets) {
    await writePatchedRange(target);
    db.upsertRegistry({
      collector: market.collector,
      exchange: market.exchange,
      symbol: market.symbol,
      timeframe: target.timeframe,
      startTs: target.startTs,
      endTs: target.endTs,
    });
  }

  return { patchedTimeframes: targets.length };
}

async function loadPatchTargets(
  config: Config,
  market: { collector: string; exchange: string; symbol: string },
  minTs: number,
  maxTs: number,
): Promise<PatchTarget[]> {
  const symbolDir = path.join(config.outDir, market.collector, market.exchange, market.symbol);
  const entries = await fs.readdir(symbolDir, { withFileTypes: true }).catch(() => []);
  const targets: PatchTarget[] = [];

  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith(".json")) continue;
    const timeframe = entry.name.slice(0, -5);
    const companionPath = path.join(symbolDir, entry.name);
    const binPath = path.join(symbolDir, `${timeframe}.bin`);

    const hasBinary = await fs.stat(binPath).then(() => true).catch(() => false);
    if (!hasBinary) continue;

    const raw = await fs.readFile(companionPath, "utf8");
    const parsed = JSON.parse(raw) as CompanionMetadata;
    const normalized = normalizeCompanionRange({
      ...parsed,
      timeframe: parsed.timeframe ?? timeframe,
      exchange: parsed.exchange ?? market.exchange,
      symbol: parsed.symbol ?? market.symbol,
    });

    const timeframeMs =
      parsed.timeframeMs ?? parseTimeframeMs(normalized.timeframe) ?? parseTimeframeMs(timeframe);
    if (!timeframeMs || !Number.isFinite(timeframeMs)) continue;

    const maxSlotInCompanion = normalized.endTs - timeframeMs;
    if (maxSlotInCompanion < normalized.startTs) continue;

    const fromSlot = Math.max(
      normalized.startTs,
      Math.floor(minTs / timeframeMs) * timeframeMs,
    );
    const toSlot = Math.min(
      maxSlotInCompanion,
      Math.floor(maxTs / timeframeMs) * timeframeMs,
    );
    if (toSlot < fromSlot) continue;

    targets.push({
      timeframe,
      timeframeMs,
      startTs: normalized.startTs,
      endTs: normalized.endTs,
      binPath,
      fromSlot,
      toSlot,
      buckets: new Map<number, Candle>(),
    });
  }

  return targets;
}

async function writePatchedRange(target: PatchTarget): Promise<void> {
  const delta = target.fromSlot - target.startTs;
  if (delta < 0 || delta % target.timeframeMs !== 0) {
    throw new Error(
      `Invalid patch alignment for ${target.timeframe}: start=${target.startTs} from=${target.fromSlot}`,
    );
  }

  const offsetBytes = (delta / target.timeframeMs) * CANDLE_BYTES;
  const fh = await fs.open(target.binPath, "r+");
  const empty: Candle = {
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

  const chunk = 4096;
  const buf = Buffer.allocUnsafe(chunk * CANDLE_BYTES);
  let slot = target.fromSlot;
  let fileOffset = offsetBytes;

  while (slot <= target.toSlot) {
    let count = 0;
    for (; count < chunk && slot <= target.toSlot; count += 1, slot += target.timeframeMs) {
      const candle = target.buckets.get(slot) ?? empty;
      const base = count * CANDLE_BYTES;
      buf.writeInt32LE(candle.open, base);
      buf.writeInt32LE(candle.high, base + 4);
      buf.writeInt32LE(candle.low, base + 8);
      buf.writeInt32LE(candle.close, base + 12);
      buf.writeBigInt64LE(candle.buyVol, base + 16);
      buf.writeBigInt64LE(candle.sellVol, base + 24);
      buf.writeUint32LE(candle.buyCount >>> 0, base + 32);
      buf.writeUint32LE(candle.sellCount >>> 0, base + 36);
      buf.writeBigInt64LE(candle.liqBuy, base + 40);
      buf.writeBigInt64LE(candle.liqSell, base + 48);
    }
    const bytes = count * CANDLE_BYTES;
    await fh.write(buf, 0, bytes, fileOffset);
    fileOffset += bytes;
  }

  await fh.close();
}
