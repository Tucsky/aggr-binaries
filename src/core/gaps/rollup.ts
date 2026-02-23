import fs from "node:fs/promises";
import path from "node:path";
import type { Config } from "../config.js";
import { parseTimeframeMs } from "../config.js";
import type { Db } from "../db.js";
import type { CompanionMetadata, NormalizedCompanionMetadata } from "../model.js";
import { normalizeCompanionRange } from "../model.js";
import { CANDLE_BYTES, type Candle } from "../trades.js";

export interface DirtyMarketRange {
  collector: string;
  exchange: string;
  symbol: string;
  minTs: number;
  maxTs: number;
}

interface CompanionWithMs extends NormalizedCompanionMetadata {
  timeframe: string;
  timeframeMs: number;
}

interface AggCandle extends Candle {
  hasPrice: boolean;
}

interface RollupTarget {
  timeframe: string;
  timeframeMs: number;
  startTs: number;
  endTs: number;
  fromSlot: number;
  toSlot: number;
  binPath: string;
  buckets: Map<number, AggCandle>;
}

export interface RollupHigherTimeframesResult {
  patchedTimeframes: number;
}

export function mergeDirtyMarketRange(
  dirtyByMarket: Map<string, DirtyMarketRange>,
  next: DirtyMarketRange,
): void {
  const key = `${next.collector}|${next.exchange}|${next.symbol}`;
  const prev = dirtyByMarket.get(key);
  if (!prev) {
    dirtyByMarket.set(key, next);
    return;
  }
  if (next.minTs < prev.minTs) prev.minTs = next.minTs;
  if (next.maxTs > prev.maxTs) prev.maxTs = next.maxTs;
}

export async function rollupHigherTimeframesFromBase(
  config: Config,
  db: Db,
  dirty: DirtyMarketRange,
): Promise<RollupHigherTimeframesResult> {
  const symbolDir = path.join(config.outDir, dirty.collector, dirty.exchange, dirty.symbol);
  const entries = await fs.readdir(symbolDir, { withFileTypes: true }).catch(() => []);
  if (!entries.length) {
    return { patchedTimeframes: 0 };
  }

  const companions = new Map<string, CompanionWithMs>();
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith(".json")) continue;
    const timeframe = entry.name.slice(0, -5);
    const companion = await readCompanionWithMs(
      path.join(symbolDir, entry.name),
      timeframe,
      dirty.exchange,
      dirty.symbol,
    );
    if (!companion) continue;
    companions.set(timeframe, companion);
  }

  const root = companions.get(config.timeframe);
  if (!root) {
    return { patchedTimeframes: 0 };
  }
  const rootBinPath = path.join(symbolDir, `${config.timeframe}.bin`);
  if (!(await fileExists(rootBinPath))) {
    return { patchedTimeframes: 0 };
  }

  const targets: RollupTarget[] = [];
  for (const [timeframe, companion] of companions) {
    if (timeframe === config.timeframe) continue;
    if (companion.timeframeMs <= root.timeframeMs) continue;
    if (companion.timeframeMs % root.timeframeMs !== 0) continue;
    const binPath = path.join(symbolDir, `${timeframe}.bin`);
    if (!(await fileExists(binPath))) continue;

    const maxSlotInCompanion = companion.endTs - companion.timeframeMs;
    if (maxSlotInCompanion < companion.startTs) continue;

    const fromSlot = Math.max(companion.startTs, Math.floor(dirty.minTs / companion.timeframeMs) * companion.timeframeMs);
    const toSlot = Math.min(maxSlotInCompanion, Math.floor(dirty.maxTs / companion.timeframeMs) * companion.timeframeMs);
    if (toSlot < fromSlot) continue;

    targets.push({
      timeframe,
      timeframeMs: companion.timeframeMs,
      startTs: companion.startTs,
      endTs: companion.endTs,
      fromSlot,
      toSlot,
      binPath,
      buckets: new Map<number, AggCandle>(),
    });
  }

  if (!targets.length) {
    return { patchedTimeframes: 0 };
  }

  targets.sort((a, b) => (a.timeframeMs - b.timeframeMs) || a.timeframe.localeCompare(b.timeframe));

  let sourceFromTs = Number.POSITIVE_INFINITY;
  let sourceToTsExclusive = Number.NEGATIVE_INFINITY;
  for (const target of targets) {
    if (target.fromSlot < sourceFromTs) sourceFromTs = target.fromSlot;
    const endExclusive = target.toSlot + target.timeframeMs;
    if (endExclusive > sourceToTsExclusive) sourceToTsExclusive = endExclusive;
  }

  if (sourceFromTs < root.startTs) sourceFromTs = root.startTs;
  if (sourceToTsExclusive > root.endTs) sourceToTsExclusive = root.endTs;
  if (sourceToTsExclusive <= sourceFromTs) {
    return { patchedTimeframes: 0 };
  }

  const rootRecords = Math.max(0, Math.floor((root.endTs - root.startTs) / root.timeframeMs));
  const sourceFromIndex = Math.max(0, Math.floor((sourceFromTs - root.startTs) / root.timeframeMs));
  const sourceToIndexExclusive = Math.max(
    sourceFromIndex,
    Math.min(rootRecords, Math.ceil((sourceToTsExclusive - root.startTs) / root.timeframeMs)),
  );
  if (sourceToIndexExclusive <= sourceFromIndex) {
    return { patchedTimeframes: 0 };
  }

  await accumulateTargetsFromRoot(rootBinPath, root, targets, sourceFromIndex, sourceToIndexExclusive);

  for (const target of targets) {
    await writeTargetRange(target);
    db.upsertRegistry({
      collector: dirty.collector,
      exchange: dirty.exchange,
      symbol: dirty.symbol,
      timeframe: target.timeframe,
      startTs: target.startTs,
      endTs: target.endTs,
    });
  }

  return { patchedTimeframes: targets.length };
}

async function accumulateTargetsFromRoot(
  rootBinPath: string,
  root: CompanionWithMs,
  targets: RollupTarget[],
  fromIndex: number,
  toIndexExclusive: number,
): Promise<void> {
  const fh = await fs.open(rootBinPath, "r");
  const chunkCandles = 4096;
  const buf = Buffer.allocUnsafe(chunkCandles * CANDLE_BYTES);

  let cursor = fromIndex;
  while (cursor < toIndexExclusive) {
    const batch = Math.min(chunkCandles, toIndexExclusive - cursor);
    const bytes = batch * CANDLE_BYTES;
    await fh.read(buf, 0, bytes, cursor * CANDLE_BYTES);

    for (let i = 0; i < batch; i += 1) {
      const ts = root.startTs + (cursor + i) * root.timeframeMs;
      const candle = readCandle(buf, i * CANDLE_BYTES);
      for (const target of targets) {
        const slot = Math.floor(ts / target.timeframeMs) * target.timeframeMs;
        if (slot < target.fromSlot || slot > target.toSlot) continue;
        let agg = target.buckets.get(slot);
        if (!agg) {
          agg = createEmptyAggCandle();
          target.buckets.set(slot, agg);
        }
        mergeIntoAgg(agg, candle);
      }
    }

    cursor += batch;
  }

  await fh.close();
}

async function writeTargetRange(target: RollupTarget): Promise<void> {
  const delta = target.fromSlot - target.startTs;
  if (delta < 0 || delta % target.timeframeMs !== 0) {
    throw new Error(
      `Invalid rollup alignment for ${target.timeframe}: start=${target.startTs} from=${target.fromSlot}`,
    );
  }

  const offsetBytes = (delta / target.timeframeMs) * CANDLE_BYTES;
  const fh = await fs.open(target.binPath, "r+");
  const empty = createEmptyCandle();

  const chunk = 4096;
  const buf = Buffer.allocUnsafe(chunk * CANDLE_BYTES);
  let slot = target.fromSlot;
  let fileOffset = offsetBytes;

  while (slot <= target.toSlot) {
    let count = 0;
    for (; count < chunk && slot <= target.toSlot; count += 1, slot += target.timeframeMs) {
      const agg = target.buckets.get(slot);
      const candle = agg ? asCandle(agg) : empty;
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

async function readCompanionWithMs(
  companionPath: string,
  timeframe: string,
  exchange: string,
  symbol: string,
): Promise<CompanionWithMs | null> {
  try {
    const raw = await fs.readFile(companionPath, "utf8");
    const parsed = JSON.parse(raw) as CompanionMetadata;
    const normalized = normalizeCompanionRange({
      ...parsed,
      timeframe: parsed.timeframe ?? timeframe,
      exchange: parsed.exchange ?? exchange,
      symbol: parsed.symbol ?? symbol,
    });
    const timeframeMs =
      parsed.timeframeMs ?? parseTimeframeMs(normalized.timeframe) ?? parseTimeframeMs(timeframe);
    if (!timeframeMs || !Number.isFinite(timeframeMs)) return null;
    return { ...normalized, timeframe: normalized.timeframe, timeframeMs };
  } catch {
    return null;
  }
}

function mergeIntoAgg(agg: AggCandle, candle: Candle): void {
  const isGap = candle.open === 0 && candle.high === 0 && candle.low === 0 && candle.close === 0;
  if (!isGap) {
    if (!agg.hasPrice) {
      agg.open = candle.open;
      agg.high = candle.high;
      agg.low = candle.low;
      agg.close = candle.close;
      agg.hasPrice = true;
    } else {
      if (candle.high > agg.high) agg.high = candle.high;
      if (candle.low < agg.low) agg.low = candle.low;
      agg.close = candle.close;
    }
  }
  agg.buyVol += candle.buyVol;
  agg.sellVol += candle.sellVol;
  agg.buyCount += candle.buyCount;
  agg.sellCount += candle.sellCount;
  agg.liqBuy += candle.liqBuy;
  agg.liqSell += candle.liqSell;
}

function asCandle(agg: AggCandle): Candle {
  return {
    open: agg.open,
    high: agg.high,
    low: agg.low,
    close: agg.close,
    buyVol: agg.buyVol,
    sellVol: agg.sellVol,
    buyCount: agg.buyCount,
    sellCount: agg.sellCount,
    liqBuy: agg.liqBuy,
    liqSell: agg.liqSell,
  };
}

function createEmptyCandle(): Candle {
  return {
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
}

function createEmptyAggCandle(): AggCandle {
  return {
    ...createEmptyCandle(),
    hasPrice: false,
  };
}

function readCandle(buf: Buffer, base: number): Candle {
  return {
    open: buf.readInt32LE(base),
    high: buf.readInt32LE(base + 4),
    low: buf.readInt32LE(base + 8),
    close: buf.readInt32LE(base + 12),
    buyVol: buf.readBigInt64LE(base + 16),
    sellVol: buf.readBigInt64LE(base + 24),
    buyCount: buf.readUInt32LE(base + 32),
    sellCount: buf.readUInt32LE(base + 36),
    liqBuy: buf.readBigInt64LE(base + 40),
    liqSell: buf.readBigInt64LE(base + 48),
  };
}

async function fileExists(pathStr: string): Promise<boolean> {
  try {
    await fs.access(pathStr);
    return true;
  } catch {
    return false;
  }
}
