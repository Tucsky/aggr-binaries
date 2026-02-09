import fs, { type FileHandle } from "node:fs/promises";
import path from "node:path";
import type { Db } from "../core/db.js";
import type { CompanionMetadata, NormalizedCompanionMetadata, RegistryEntry, RegistryKey } from "../core/model.js";
import { normalizeCompanionRange } from "../core/model.js";
import { CANDLE_BYTES, PRICE_SCALE, VOL_SCALE, type Candle } from "../core/trades.js";
import { parseTimeframeMs } from "../shared/timeframes.js";

export interface ResampleContext {
  db: Db;
  outputRoot: string;
}

type Companion = NormalizedCompanionMetadata & { timeframe: string; timeframeMs: number };

type AggBucket = Candle & { hasPrice: boolean };

interface MarketEntry extends RegistryEntry {
  timeframeMs: number;
}

interface Candidate {
  entry: MarketEntry;
  companion: Companion;
  fresh: boolean;
}

interface ResampleRangeParams {
  ctx: ResampleContext;
  collector: string;
  exchange: string;
  symbol: string;
  src: Companion;
  dstTimeframe: string;
  dstTimeframeMs: number;
  dstStart: number;
  from: number;
  to: number;
  existingRecords: number;
}

export async function ensurePreviewTimeframe(
  ctx: ResampleContext,
  collector: string,
  exchange: string,
  symbol: string,
  timeframe: string,
): Promise<Companion> {
  const target = timeframe.trim();
  const targetMs = parseTimeframeMs(target);
  if (!targetMs) {
    throw new Error(`Invalid timeframe: ${target}`);
  }
  const dstBinPath = getBinPath(ctx.outputRoot, collector, exchange, symbol, target);
  const dstCompanionPath = getCompanionPath(ctx.outputRoot, collector, exchange, symbol, target);

  const marketEntries = loadMarketEntries(ctx.db, collector, exchange, symbol);
  if (!marketEntries.length) {
    throw new Error(`No registry entries for ${collector}/${exchange}/${symbol}`);
  }

  const rootEntry = marketEntries.reduce((min, entry) => (entry.timeframeMs < min.timeframeMs ? entry : min));
  const rootCompanion = await readCompanion(
    ctx,
    collector,
    exchange,
    symbol,
    rootEntry.timeframe,
    rootEntry,
    { requireDense: true },
  );
  if (!rootCompanion) {
    throw new Error(`Missing root companion for ${collector}/${exchange}/${symbol}/${rootEntry.timeframe}`);
  }

  const maxEndFor = (tfMs: number): number => alignEnd(rootCompanion.endTs, tfMs);
  const targetMaxEnd = maxEndFor(targetMs);

  let existingCompanion = await readCompanion(
    ctx,
    collector,
    exchange,
    symbol,
    target,
    marketEntries.find((e) => e.timeframe === target),
  );
  if (existingCompanion) {
    const hasFiles = (await fileExists(dstBinPath)) && (await fileExists(dstCompanionPath));
    if (!hasFiles) {
      console.warn(
        "[resample] missing target files, purging registry",
        `${collector}/${exchange}/${symbol}/${target}`,
      );
      deleteRegistryEntry(ctx.db, { collector, exchange, symbol, timeframe: target });
      existingCompanion = null;
    }
  }
  if (existingCompanion && existingCompanion.endTs === targetMaxEnd) {
    const normalized = normalizeCompanion(existingCompanion, target, targetMs);
    upsertRegistryFromCompanion(ctx.db, collector, normalized);
    console.log(
      "[resample] fresh",
      `${collector}/${exchange}/${symbol}/${target}`,
      new Date(normalized.startTs).toISOString(),
      "->",
      new Date(normalized.endTs).toISOString(),
    );
    return normalized;
  }

  const source = await pickSource({
    ctx,
    collector,
    exchange,
    symbol,
    entries: marketEntries,
    targetMs,
    rootCompanion,
    maxEndFor,
  });
  if (!source) {
    throw new Error(`No usable source timeframe for ${collector}/${exchange}/${symbol}/${target}`);
  }
  console.log(
    "[resample] selected source",
    `${collector}/${exchange}/${symbol}`,
    `dst=${target}`,
    `src=${source.timeframe}`,
    `srcEnd=${new Date(source.endTs).toISOString()}`,
  );

  const dstStart = existingCompanion?.startTs ?? alignToBucket(rootCompanion.startTs, targetMs);
  const from = existingCompanion ? existingCompanion.endTs : dstStart;
  const to = targetMaxEnd;
  const existingRecords = existingCompanion?.records ?? Math.max(0, Math.floor((from - dstStart) / targetMs));

  if (to <= from) {
    console.log("[resample] up-to-date", `${collector}/${exchange}/${symbol}/${target}`);
    const normalized = normalizeCompanion(
      existingCompanion ?? {
        exchange,
        symbol,
        timeframe: target,
        timeframeMs: targetMs,
        startTs: dstStart,
        endTs: to,
        priceScale: source.priceScale ?? PRICE_SCALE,
        volumeScale: source.volumeScale ?? VOL_SCALE,
        records: existingRecords,
      },
      target,
      targetMs,
    );
    upsertRegistryFromCompanion(ctx.db, collector, normalized);
    return normalized;
  }

  await resampleRange({
    ctx,
    collector,
    exchange,
    symbol,
    src: source,
    dstTimeframe: target,
    dstTimeframeMs: targetMs,
    dstStart,
    from,
    to,
    existingRecords,
  });

  const updatedCompanion = await readCompanion(ctx, collector, exchange, symbol, target, {
    collector,
    exchange,
    symbol,
    timeframe: target,
    startTs: dstStart,
    endTs: to,
  });
  if (!updatedCompanion) {
    throw new Error(`Failed to read updated companion for ${collector}/${exchange}/${symbol}/${target}`);
  }
  const normalized = normalizeCompanion(updatedCompanion, target, targetMs);
  upsertRegistryFromCompanion(ctx.db, collector, normalized);
  return normalized;
}

async function resampleRange(params: ResampleRangeParams): Promise<void> {
  const {
    ctx,
    collector,
    exchange,
    symbol,
    src,
    dstTimeframe,
    dstTimeframeMs,
    dstStart,
    from,
    to,
    existingRecords,
  } = params;

  console.log('Resampling', `${collector}/${exchange}/${symbol}`, `from ${src.timeframe} to ${dstTimeframe}`, `for range ${new Date(from).toISOString()} - ${new Date(to).toISOString()}`);

  const srcMs = src.timeframeMs;
  if (!Number.isFinite(srcMs) || srcMs <= 0) {
    throw new Error(`Invalid source timeframe for resampling: ${src.timeframe}`);
  }
  const srcBinPath = getBinPath(ctx.outputRoot, collector, exchange, symbol, src.timeframe);
  const dstBinPath = getBinPath(ctx.outputRoot, collector, exchange, symbol, dstTimeframe);

  const span = to - from;
  if (span < 0) {
    throw new Error(`Invalid resample window for ${dstTimeframe}: to < from`);
  }
  if (span % dstTimeframeMs !== 0) {
    throw new Error(`Unaligned resample window for ${dstTimeframe}: ${(span % dstTimeframeMs).toString()}ms skew`);
  }

  const srcFromIndex = Math.max(0, Math.floor((from - src.startTs) / srcMs));
  const srcToIndexExclusive = Math.min(src.records, Math.ceil((to - src.startTs) / srcMs));
  if (srcToIndexExclusive <= srcFromIndex) {
    // Nothing to read, but still need to extend the destination with empty buckets.
    await writeDestinationBuckets(dstBinPath, dstTimeframeMs, from, to, existingRecords, new Map<number, AggBucket>());
    await writeCompanion(ctx.outputRoot, collector, exchange, symbol, dstTimeframe, {
      exchange,
      symbol,
      timeframe: dstTimeframe,
      timeframeMs: dstTimeframeMs,
      startTs: dstStart,
      endTs: to,
      priceScale: src.priceScale ?? PRICE_SCALE,
      volumeScale: src.volumeScale ?? VOL_SCALE,
      records: existingRecords + Math.max(0, Math.floor((to - from) / dstTimeframeMs)),
    });
    return;
  }

  console.log(
    "[resample] updating",
    `${collector}/${exchange}/${symbol}`,
    `dst=${dstTimeframe}`,
    `src=${src.timeframe}`,
    `append=${new Date(from).toISOString()} -> ${new Date(to).toISOString()}`,
  );

  const buckets = new Map<number, AggBucket>();
  const fh = await fs.open(srcBinPath, "r");
  const chunkCandles = 4096;
  let cursor = srcFromIndex;
  while (cursor < srcToIndexExclusive) {
    const batch = Math.min(chunkCandles, srcToIndexExclusive - cursor);
    const buf = Buffer.allocUnsafe(batch * CANDLE_BYTES);
    await fh.read(buf, 0, buf.length, cursor * CANDLE_BYTES);
    for (let i = 0; i < batch; i++) {
      const ts = src.startTs + (cursor + i) * srcMs;
      if (ts < from || ts >= to) continue;
      const base = i * CANDLE_BYTES;
      const candle = readCandle(buf, base);
      const bucketTs = alignToBucket(ts, dstTimeframeMs);
      const aggregated = buckets.get(bucketTs) ?? createEmptyAgg();
      const isGap = candle.open === 0 && candle.high === 0 && candle.low === 0 && candle.close === 0;
      if (!isGap) {
        if (!aggregated.hasPrice) {
          aggregated.open = candle.open;
          aggregated.high = candle.high;
          aggregated.low = candle.low;
          aggregated.close = candle.close;
          aggregated.hasPrice = true;
        } else {
          aggregated.high = Math.max(aggregated.high, candle.high);
          aggregated.low = Math.min(aggregated.low, candle.low);
          aggregated.close = candle.close;
        }
      }
      aggregated.buyVol += candle.buyVol;
      aggregated.sellVol += candle.sellVol;
      aggregated.buyCount += candle.buyCount;
      aggregated.sellCount += candle.sellCount;
      aggregated.liqBuy += candle.liqBuy;
      aggregated.liqSell += candle.liqSell;
      buckets.set(bucketTs, aggregated);
    }
    cursor += batch;
  }
  await fh.close();

  await fs.mkdir(path.dirname(dstBinPath), { recursive: true });
  await writeDestinationBuckets(dstBinPath, dstTimeframeMs, from, to, existingRecords, buckets);

  const appendedRecords = Math.max(0, Math.floor((to - from) / dstTimeframeMs));
  const companion: CompanionMetadata = {
    exchange,
    symbol,
    timeframe: dstTimeframe,
    timeframeMs: dstTimeframeMs,
    startTs: dstStart,
    endTs: to,
    priceScale: src.priceScale ?? PRICE_SCALE,
    volumeScale: src.volumeScale ?? VOL_SCALE,
    records: existingRecords + appendedRecords,
  };
  await writeCompanion(ctx.outputRoot, collector, exchange, symbol, dstTimeframe, companion);
  console.log(
    "[resample] wrote",
    `${collector}/${exchange}/${symbol}/${dstTimeframe}`,
    `appended=${appendedRecords}`,
    `records=${companion.records}`,
    `range=${new Date(companion.startTs!).toISOString()} -> ${new Date(companion.endTs!).toISOString()}`,
  );
}

async function writeDestinationBuckets(
  dstBinPath: string,
  dstMs: number,
  from: number,
  to: number,
  existingRecords: number,
  buckets: Map<number, AggBucket>,
): Promise<void> {
  const totalBuckets = Math.max(0, Math.floor((to - from) / dstMs));
  if (totalBuckets === 0) return;

  await fs.mkdir(path.dirname(dstBinPath), { recursive: true });

  let fileOffset = existingRecords * CANDLE_BYTES;
  let fh: FileHandle;
  try {
    fh = await fs.open(dstBinPath, existingRecords ? "r+" : "w");
  } catch {
    fh = await fs.open(dstBinPath, "w");
    fileOffset = 0;
  }

  const empty: AggBucket = createEmptyAgg();

  const chunk = 4096;
  const buf = Buffer.allocUnsafe(chunk * CANDLE_BYTES);
  let written = 0;
  while (written < totalBuckets) {
    const batch = Math.min(chunk, totalBuckets - written);
    for (let i = 0; i < batch; i++) {
      const ts = from + (written + i) * dstMs;
      const c = buckets.get(ts) ?? empty;
      const base = i * CANDLE_BYTES;
      buf.writeInt32LE(c.open, base);
      buf.writeInt32LE(c.high, base + 4);
      buf.writeInt32LE(c.low, base + 8);
      buf.writeInt32LE(c.close, base + 12);
      buf.writeBigInt64LE(c.buyVol, base + 16);
      buf.writeBigInt64LE(c.sellVol, base + 24);
      buf.writeUint32LE(c.buyCount >>> 0, base + 32);
      buf.writeUint32LE(c.sellCount >>> 0, base + 36);
      buf.writeBigInt64LE(c.liqBuy, base + 40);
      buf.writeBigInt64LE(c.liqSell, base + 48);
    }
    const bytes = batch * CANDLE_BYTES;
    await fh.write(buf, 0, bytes, fileOffset);
    fileOffset += bytes;
    written += batch;
  }
  await fh.close();
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

function createEmptyAgg(): AggBucket {
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
    hasPrice: false,
  };
}

async function pickSource(opts: {
  ctx: ResampleContext;
  collector: string;
  exchange: string;
  symbol: string;
  entries: MarketEntry[];
  targetMs: number;
  rootCompanion: Companion;
  maxEndFor: (tfMs: number) => number;
}): Promise<Companion | null> {
  const { ctx, collector, exchange, symbol, entries, targetMs, rootCompanion, maxEndFor } = opts;
  const candidates: Candidate[] = [];
  for (const entry of entries) {
    if (entry.timeframeMs > targetMs) continue;
    if (targetMs % entry.timeframeMs !== 0) continue;
    let companion: Companion | null;
    if (entry.timeframe === rootCompanion.timeframe) {
      companion = rootCompanion;
    } else {
      companion = await readCompanion(ctx, collector, exchange, symbol, entry.timeframe, entry);
    }
    if (!companion) continue;
    const binPath = getBinPath(ctx.outputRoot, collector, exchange, symbol, entry.timeframe);
    if (!(await fileExists(binPath))) {
      console.warn(
        "[resample] skipping missing source",
        `${collector}/${exchange}/${symbol}/${entry.timeframe}`,
      );
      deleteRegistryEntry(ctx.db, {
        collector,
        exchange,
        symbol,
        timeframe: entry.timeframe,
      });
      continue;
    }
    const fresh = companion.endTs === maxEndFor(entry.timeframeMs);
    candidates.push({ entry, companion, fresh });
  }

  if (!candidates.length) return null;

  const freshCandidates = candidates.filter((c) => c.fresh);
  if (freshCandidates.length) {
    return freshCandidates.reduce((best, cur) => (cur.entry.timeframeMs > best.entry.timeframeMs ? cur : best)).companion;
  }

  const rootCandidate = candidates.find((c) => c.entry.timeframe === rootCompanion.timeframe);
  return rootCandidate?.companion ?? null;
}

function loadMarketEntries(db: Db, collector: string, exchange: string, symbol: string): MarketEntry[] {
  const rows =
    (db.db
      .prepare(
        `SELECT timeframe, start_ts, end_ts
         FROM registry
         WHERE collector = :collector AND exchange = :exchange AND symbol = :symbol;`,
      )
      .all({ collector, exchange, symbol }) as Array<{ timeframe: string; start_ts: number; end_ts: number }>) ??
    [];

  return rows
    .map((row) => {
      const timeframeMs = parseTimeframeMs(row.timeframe);
      if (!timeframeMs) return null;
      return {
        collector,
        exchange,
        symbol,
        timeframe: row.timeframe,
        startTs: (row as any).startTs ?? row.start_ts,
        endTs: (row as any).endTs ?? row.end_ts,
        timeframeMs,
      };
    })
    .filter((e): e is MarketEntry => e !== null);
}

async function readCompanion(
  ctx: ResampleContext,
  collector: string,
  exchange: string,
  symbol: string,
  timeframe: string,
  fallback?: Partial<RegistryEntry>,
  opts?: { requireDense?: boolean },
): Promise<Companion | null> {
  const companionPath = getCompanionPath(ctx.outputRoot, collector, exchange, symbol, timeframe);
  let parsed: CompanionMetadata | null = null;
  try {
    const raw = await fs.readFile(companionPath, "utf8");
    const rawParsed = JSON.parse(raw) as CompanionMetadata;
    if ((rawParsed as { sparse?: boolean }).sparse) {
      if (opts?.requireDense) {
        throw new Error(`Sparse companions are no longer supported: ${companionPath}`);
      }
      return null;
    }
    // Normalize segmented vs monolithic format
    parsed = normalizeCompanionRange(rawParsed);
  } catch (err) {
    const code = (err as { code?: string }).code;
    if (code && code !== "ENOENT") {
      throw new Error(`Failed to read companion ${companionPath}: ${String(err)}`);
    }
  }

  if (!parsed && !fallback) return null;
  const merged: Partial<CompanionMetadata> = {
    ...(parsed ?? {}),
    exchange: parsed?.exchange ?? exchange,
    symbol: parsed?.symbol ?? symbol,
    timeframe: parsed?.timeframe ?? timeframe,
    startTs: parsed?.startTs ?? (fallback as any)?.startTs ?? (fallback as any)?.start_ts,
    endTs: parsed?.endTs ?? (fallback as any)?.endTs ?? (fallback as any)?.end_ts,
  };

  if (!Number.isFinite(merged.startTs) || !Number.isFinite(merged.endTs)) {
    return null;
  }

  const { sparse: _ignored, ...rest } = merged as Partial<CompanionMetadata> & { sparse?: boolean };
  const resolved = normalizeCompanion(rest, timeframe);
  return resolved;
}

function normalizeCompanion(meta: Partial<CompanionMetadata>, timeframe: string, tfMs?: number | null): Companion {
  const timeframeMs = tfMs ?? meta.timeframeMs ?? parseTimeframeMs(meta.timeframe ?? timeframe);
  if (!Number.isFinite(timeframeMs) || !timeframeMs) {
    throw new Error(`Cannot resolve timeframeMs for ${meta.timeframe ?? timeframe}`);
  }

  if (!Number.isFinite(meta.startTs) || !Number.isFinite(meta.endTs)) {
    throw new Error(`Companion is missing range for ${meta.timeframe ?? timeframe}`);
  }
  const exchange = meta.exchange ?? "";
  const symbol = meta.symbol ?? "";
  if (!exchange || !symbol) {
    throw new Error(`Companion is missing market information for ${meta.timeframe ?? timeframe}`);
  }

  const records = Math.max(
    0,
    Math.floor(((meta.endTs as number) - (meta.startTs as number)) / timeframeMs),
  );

  return {
    ...meta,
    exchange,
    symbol,
    startTs: meta.startTs as number,
    endTs: meta.endTs as number,
    timeframe: meta.timeframe ?? timeframe,
    timeframeMs,
    priceScale: meta.priceScale ?? PRICE_SCALE,
    volumeScale: meta.volumeScale ?? VOL_SCALE,
    records,
  };
}

async function writeCompanion(
  outputRoot: string,
  collector: string,
  exchange: string,
  symbol: string,
  timeframe: string,
  meta: CompanionMetadata,
): Promise<void> {
  const companionPath = getCompanionPath(outputRoot, collector, exchange, symbol, timeframe);
  await fs.mkdir(path.dirname(companionPath), { recursive: true });
  await fs.writeFile(companionPath, JSON.stringify(meta, null, 2));
}

function upsertRegistryFromCompanion(db: Db, collector: string, companion: Companion): void {
  db.upsertRegistry({
    collector,
    exchange: companion.exchange,
    symbol: companion.symbol,
    timeframe: companion.timeframe,
    startTs: companion.startTs,
    endTs: companion.endTs,
  });
}

function getBinPath(outputRoot: string, collector: string, exchange: string, symbol: string, timeframe: string): string {
  return path.join(outputRoot, collector, exchange, symbol, `${timeframe}.bin`);
}

function getCompanionPath(
  outputRoot: string,
  collector: string,
  exchange: string,
  symbol: string,
  timeframe: string,
): string {
  return path.join(outputRoot, collector, exchange, symbol, `${timeframe}.json`);
}

function alignToBucket(ts: number, tfMs: number): number {
  return Math.floor(ts / tfMs) * tfMs;
}

function alignEnd(rootEndTs: number, tfMs: number): number {
  return Math.floor(rootEndTs / tfMs) * tfMs;
}

async function fileExists(p: string): Promise<boolean> {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
}

function deleteRegistryEntry(db: Db, key: RegistryKey): void {
  try {
    db.db
      .prepare(
        `DELETE FROM registry WHERE collector = :collector AND exchange = :exchange AND symbol = :symbol AND timeframe = :timeframe;`,
      )
      .run({
        collector: key.collector,
        exchange: key.exchange,
        symbol: key.symbol,
        timeframe: key.timeframe,
      });
  } catch (err) {
    console.warn("[resample] failed to delete registry entry", key, err);
  }
}
