import fs from "node:fs/promises";
import path from "node:path";
import readline from "node:readline";
import { finished } from "node:stream/promises";
import zlib from "node:zlib";
import type { Config } from "./config.js";
import type { Db } from "./db.js";
import type { CompanionMetadata, FileRow } from "./model.js";
import {
  CANDLE_BYTES,
  PRICE_SCALE,
  VOL_SCALE,
  accumulate,
  parseTradeLine,
  type Candle,
  type ParseRejectReason,
} from "./trades.js";

interface Accumulator {
  collector: string;
  exchange: string;
  symbol: string;
  buckets: Map<number, Candle>;
  bucketCount: number;
  minMinute: number;
  maxMinute: number;
  maxInputStartTs: number;
  companion?: CompanionMetadata;
}

interface ProcessStats {
  totalLines: number;
  totalTradesKept: number;
  processedFiles: number;
  processedMarkets: number;
  maxBuckets: number;
}

interface StreamResult {
  linesRead: number;
  tradesKept: number;
  newBuckets: number;
}

interface MarketRef {
  collector: string;
  exchange: string;
  symbol: string;
}

interface MarketFlushState {
  outBase: string;
  binaryPath: string;
  startBase?: number;
  resumeSlot?: number;
  nextWriteFrom?: number;
  lastFlushedEndTs?: number;
  needsResumeRewrite: boolean;
  hasFlushed: boolean;
}

enum BinaryState {
  Exists = "exists",
  Missing = "missing",
}

export async function runProcess(config: Config, db: Db): Promise<void> {
  const collectors = await resolveCollectors(config, db);
  if (!collectors.length) {
    throw new Error("No collectors found; specify --collector or ensure index has rows.");
  }

  const allowExchange = config.exchange?.toUpperCase();
  const allowSymbol = config.symbol;
  const totalCandidates = countCandidateFiles(db, collectors, allowExchange, allowSymbol);
  const totalMarkets = countMarkets(db, collectors, allowExchange, allowSymbol);
  if (!totalCandidates) {
    console.log("No files to process for selected collectors.");
    return;
  }

  const timeframe = config.timeframe;
  const markets = iterateMarkets(db, collectors, allowExchange, allowSymbol);

  console.log(
    `[process] collectors=${collectors.join(",")} markets=${totalMarkets} files=${totalCandidates} timeframe=${timeframe} filters=${allowExchange || "ALL"}/${allowSymbol || "ALL"} (market-first)`,
  );

  const startAll = Date.now();
  const stats = await processByMarket({
    markets,
    totalCandidates,
    totalMarkets,
    db,
    config,
    startAll,
  });

  const totalElapsed = (Date.now() - startAll) / 1000;
  console.log(
    `[process] complete markets=${stats.processedMarkets} files=${stats.processedFiles}/${totalCandidates} lines=${stats.totalLines} kept=${stats.totalTradesKept} maxBuckets=${stats.maxBuckets} elapsed=${totalElapsed.toFixed(
      2,
    )}s`,
  );
}

async function resolveCollectors(config: Config, db: Db): Promise<string[]> {
  if (config.collector) return [config.collector.toUpperCase()];
  const rows = db.db.prepare("SELECT DISTINCT collector FROM files;").all() as Array<{ collector: string }>;
  return rows.map((r) => r.collector.toUpperCase());
}

function countCandidateFiles(db: Db, collectors: string[], allowExchange?: string, allowSymbol?: string): number {
  const stmt = db.db.prepare(
    `SELECT COUNT(*) as count FROM files
     WHERE collector IN (${collectors.map((_, i) => `:c${i}`).join(",")})
       AND (:allowExchange IS NULL OR exchange = :allowExchange)
       AND (:allowSymbol IS NULL OR symbol = :allowSymbol);`,
  );
  const params: Record<string, string | null> = {
    allowExchange: allowExchange ?? null,
    allowSymbol: allowSymbol ?? null,
  };
  collectors.forEach((c, i) => (params[`c${i}`] = c));
  const row = stmt.get(params) as { count?: number } | undefined;
  return row?.count ? Number(row.count) : 0;
}

function iterateMarkets(db: Db, collectors: string[], allowExchange?: string, allowSymbol?: string): Iterable<MarketRef> {
  const stmt = db.db.prepare(
    `SELECT DISTINCT collector, exchange, symbol FROM files
     WHERE collector IN (${collectors.map((_, i) => `:c${i}`).join(",")})
       AND (:allowExchange IS NULL OR exchange = :allowExchange)
       AND (:allowSymbol IS NULL OR symbol = :allowSymbol)
     ORDER BY collector, exchange, symbol;`,
  );
  const params: Record<string, string | null> = {
    allowExchange: allowExchange ?? null,
    allowSymbol: allowSymbol ?? null,
  };
  collectors.forEach((c, i) => (params[`c${i}`] = c));
  return stmt.iterate(params) as Iterable<MarketRef>;
}

function countMarkets(db: Db, collectors: string[], allowExchange?: string, allowSymbol?: string): number {
  const stmt = db.db.prepare(
    `SELECT COUNT(DISTINCT collector || '::' || exchange || '::' || symbol) as count FROM files
     WHERE collector IN (${collectors.map((_, i) => `:c${i}`).join(",")})
       AND (:allowExchange IS NULL OR exchange = :allowExchange)
       AND (:allowSymbol IS NULL OR symbol = :allowSymbol);`,
  );
  const params: Record<string, string | null> = {
    allowExchange: allowExchange ?? null,
    allowSymbol: allowSymbol ?? null,
  };
  collectors.forEach((c, i) => (params[`c${i}`] = c));
  const row = stmt.get(params) as { count?: number } | undefined;
  return row?.count ? Number(row.count) : 0;
}

function makeMarketKey(collector: string, exchange: string, symbol: string): string {
  return `${collector}::${exchange}::${symbol}`;
}

function iterateFilesForMarket(
  db: Db,
  market: MarketRef,
  minStartTs?: number,
): Iterable<FileRow> {
  const stmt = db.db.prepare(
    `SELECT * FROM files
     WHERE collector = :collector AND exchange = :exchange AND symbol = :symbol
       AND (:minStartTs IS NULL OR start_ts >= :minStartTs)
     ORDER BY start_ts, relative_path;`,
  );
  return stmt.iterate({
    collector: market.collector,
    exchange: market.exchange,
    symbol: market.symbol,
    minStartTs: minStartTs ?? null,
  }) as Iterable<FileRow>;
}

async function startAccumulatorForMarket(
  config: Config,
  collector: string,
  exchange: string,
  symbol: string,
): Promise<Accumulator> {
  const companionPath = path.join(config.outDir, collector, exchange, symbol, `${config.timeframe}.json`);
  const companion = await readCompanion(companionPath);
  console.log(
    `[${collector}/${exchange}/${symbol}] init accumulator companion=${companion ? "present" : "missing"} path=${companionPath}`,
  );
  return {
    collector,
    exchange,
    symbol,
    buckets: new Map(),
    bucketCount: 0,
    minMinute: Number.POSITIVE_INFINITY,
    maxMinute: Number.NEGATIVE_INFINITY,
    maxInputStartTs: companion?.lastInputStartTs ?? Number.NEGATIVE_INFINITY,
    companion: companion ?? undefined,
  };
}

async function processByMarket(opts: {
  markets: Iterable<MarketRef>;
  totalCandidates: number;
  totalMarkets: number;
  db: Db;
  config: Config;
  startAll: number;
}): Promise<ProcessStats> {
  const {
    markets,
    totalCandidates,
    totalMarkets,
    db,
    config,
    startAll,
  } = opts;

  const timeframe = config.timeframe;
  const timeframeMs = config.timeframeMs;
  const flushIntervalMs = Math.max(1_000, Math.floor(config.flushIntervalSeconds * 1000));

  let acc: Accumulator | null = null;
  let totalLines = 0;
  let totalTradesKept = 0;
  let processedFiles = 0;
  let processedMarkets = 0;
  let maxBuckets = 0;
  const logHeartbeat = () => {
    const elapsed = ((Date.now() - startAll) / 1000).toFixed(1);
    console.log(
      `[process] heartbeat markets=${processedMarkets}/${totalMarkets} files=${processedFiles}/${totalCandidates} buckets=${maxBuckets} current=${acc ? makeMarketKey(acc.collector, acc.exchange, acc.symbol) : "idle"} elapsed=${elapsed}s`,
    );
  };

  for (const market of markets) {
    acc = await startAccumulatorForMarket(config, market.collector, market.exchange, market.symbol);
    const outBase = path.join(config.outDir, market.collector, market.exchange, market.symbol, timeframe);

    if (!config.force && acc.companion) {
      const binaryState = await getBinaryState(outBase + ".bin");
      if (binaryState === BinaryState.Missing) {
        console.log(
          `[${acc.collector}/${acc.exchange}/${acc.symbol}/${timeframe}] companion present but binary missing; rebuilding full output`,
        );
        acc.companion = undefined;
        acc.maxInputStartTs = Number.NEGATIVE_INFINITY;
      }
    }

    const minStartTs =
      !config.force && acc.companion?.lastInputStartTs !== undefined ? acc.companion.lastInputStartTs : undefined;
    const resumeSlot =
      !config.force && acc.companion && acc.companion.endTs !== undefined
        ? acc.companion.endTs - timeframeMs
        : undefined;
    console.log(
      `[${acc.collector}/${acc.exchange}/${acc.symbol}/${timeframe}] start market companion=${Boolean(acc.companion)} minStartTs=${minStartTs} resumeSlot=${resumeSlot}`,
    );
    const files = iterateFilesForMarket(db, market, minStartTs);
    const flushState: MarketFlushState = {
      outBase,
      binaryPath: outBase + ".bin",
      startBase: !config.force ? acc.companion?.startTs : undefined,
      resumeSlot,
      nextWriteFrom: resumeSlot ?? (!config.force ? acc.companion?.startTs : undefined),
      lastFlushedEndTs: !config.force ? acc.companion?.endTs : undefined,
      needsResumeRewrite: Boolean(resumeSlot && acc.companion),
      hasFlushed: false,
    };
    let lastFlushAt = Date.now();

    for (const file of files) {
      /*console.log(
        `[${acc.collector}/${acc.exchange}/${acc.symbol}/${timeframe}] stream file=${file.relative_path} start_ts=${file.start_ts} skipBefore=${resumeSlot}`,
      );*/
      const { linesRead, tradesKept, newBuckets } = await streamFile({
        file,
        acc,
        root: config.root,
        timeframeMs,
        skipBeforeTs: resumeSlot,
      });
      /*console.log(
        `[${acc.collector}/${acc.exchange}/${acc.symbol}/${timeframe}] done file=${file.relative_path} lines=${linesRead} kept=${tradesKept} newBuckets=${newBuckets} bucketCount=${acc.bucketCount}`,
      );*/

      processedFiles += 1;
      totalLines += linesRead;
      totalTradesKept += tradesKept;
      if (newBuckets) {
        const currentBuckets = acc.buckets.size;
        if (currentBuckets > maxBuckets) {
          maxBuckets = currentBuckets;
        }
      }

      const now = Date.now();
      if (now - lastFlushAt >= flushIntervalMs) {
        /* console.log(
          `[${acc.collector}/${acc.exchange}/${acc.symbol}/${timeframe}] triggering interval flush elapsedMs=${now - lastFlushAt}`,
        ); */
        const flushed = await flushMarketOutput(acc, config, db, flushState, { final: false });
        if (flushed) {
          lastFlushAt = now;
          logHeartbeat();
        }
      }
    }

    const finalFlushed = await flushMarketOutput(acc, config, db, flushState, { final: true });
    if (finalFlushed) {
      logHeartbeat();
    }
    processedMarkets += 1;
    acc = null;
  }

  return { totalLines, totalTradesKept, processedFiles, processedMarkets, maxBuckets };
}

async function streamFile(opts: {
  file: FileRow;
  acc: Accumulator;
  root: string;
  timeframeMs: number;
  skipBeforeTs?: number;
}): Promise<StreamResult> {
  const { file, acc, root, timeframeMs, skipBeforeTs } = opts;

  const fullPath = path.join(root, file.relative_path);
  const fileStartTs = file.start_ts;

  const stream = await makeStream(fullPath);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  const reject: { reason?: ParseRejectReason } = {};
  const rejectCounts: Record<ParseRejectReason, number> = {
    parts_short: 0,
    non_finite: 0,
    invalid_ts_range: 0,
    notional_too_large: 0,
  };

  let linesRead = 0;
  let tradesKept = 0;
  let newBuckets = 0;
  let rejectTotal = 0;

  for await (const line of rl) {
    linesRead += 1;
    reject.reason = undefined;

    const trade = parseTradeLine(line, reject);
    if (!trade) {
      const reason = reject.reason;
      if (reason !== undefined) {
        switch (reason) {
          case "parts_short":
            rejectCounts.parts_short += 1;
            break;
          case "non_finite":
            rejectCounts.non_finite += 1;
            break;
          case "invalid_ts_range":
            rejectCounts.invalid_ts_range += 1;
            break;
          case "notional_too_large":
            rejectCounts.notional_too_large += 1;
            break;
        }
        rejectTotal += 1;
      }
      continue;
    }

    if (skipBeforeTs !== undefined && trade.ts < skipBeforeTs) continue;

    const created = accumulate(acc, trade, timeframeMs);
    if (created) {
      acc.bucketCount += 1;
      newBuckets += 1;
    }
    tradesKept += 1;
  }
  rl.close();

  if (tradesKept > 0 && fileStartTs > acc.maxInputStartTs) {
    acc.maxInputStartTs = fileStartTs;
  }

  if (rejectTotal > 0) {
    const summary: string[] = [];
    if (rejectCounts.parts_short) summary.push(`parts_short=${rejectCounts.parts_short}`);
    if (rejectCounts.non_finite) summary.push(`non_finite=${rejectCounts.non_finite}`);
    if (rejectCounts.invalid_ts_range) summary.push(`invalid_ts_range=${rejectCounts.invalid_ts_range}`);
    if (rejectCounts.notional_too_large) summary.push(`notional_too_large=${rejectCounts.notional_too_large}`);
    console.warn(`[parse-skip] path=${fullPath} rejects=${rejectTotal} ${summary.join(" ")}`);
  }

  return { linesRead, tradesKept, newBuckets };
}

async function makeStream(filePath: string) {
  const file = await fs.open(filePath, "r");
  const stream = file.createReadStream();
  const out = filePath.endsWith(".gz") ? stream.pipe(zlib.createGunzip()) : stream;
  void finished(out).finally(() => {
    return file.close().catch(() => {});
  });
  return out;
}

function computeResumeOffsetBytes(startTs: number, resumeSlot: number, timeframeMs: number): number {
  const delta = resumeSlot - startTs;
  if (delta < 0 || delta % timeframeMs !== 0) {
    throw new Error(
      `Invalid resume alignment: startTs=${startTs} resumeSlot=${resumeSlot} timeframeMs=${timeframeMs}`,
    );
  }
  return (delta / timeframeMs) * CANDLE_BYTES;
}

async function flushMarketOutput(
  acc: Accumulator,
  config: Config,
  db: Db,
  state: MarketFlushState,
  opts: { final: boolean },
): Promise<boolean> {
  const timeframe = config.timeframe;
  const timeframeMs = config.timeframeMs;

  if (!acc.bucketCount || !isFinite(acc.minMinute) || !isFinite(acc.maxMinute)) {
    if (opts.final) {
      console.log(`[${acc.collector}/${acc.exchange}/${acc.symbol}] no trades; skipping output`);
    }
    return false;
  }

  if (state.startBase === undefined) {
    state.startBase = acc.companion?.startTs !== undefined ? acc.companion.startTs : acc.minMinute;
  }
  /* console.log(
    `[${acc.collector}/${acc.exchange}/${acc.symbol}/${timeframe}] flush start final=${opts.final} startBase=${state.startBase} resumeSlot=${state.resumeSlot} nextWriteFrom=${state.nextWriteFrom} lastFlushed=${state.lastFlushedEndTs}`,
  ); */

  const startBase = state.startBase;
  if (!Number.isFinite(startBase)) {
    return false;
  }
  if (startBase % timeframeMs !== 0) {
    throw new Error(
      `startBase misaligned with timeframe: startBase=${startBase} timeframeMs=${timeframeMs}`,
    );
  }

  const lastClosedSlot = opts.final ? acc.maxMinute : acc.maxMinute - timeframeMs;
  if (!Number.isFinite(lastClosedSlot)) {
    return false;
  }

  if (state.nextWriteFrom === undefined) {
    state.nextWriteFrom = state.resumeSlot ?? startBase;
  }

  const writeFrom = state.nextWriteFrom;
  if (lastClosedSlot < writeFrom) {
    return false;
  }

  const flushEndExclusive = opts.final ? acc.maxMinute + timeframeMs : lastClosedSlot + timeframeMs;
  const usingResumeRewrite = state.needsResumeRewrite && !state.hasFlushed;
  if (state.lastFlushedEndTs !== undefined && flushEndExclusive <= state.lastFlushedEndTs && !usingResumeRewrite) {
    if (opts.final && state.hasFlushed) {
      console.log(
        `[${acc.collector}/${acc.exchange}/${acc.symbol}/${timeframe}] final flush already persisted through ${new Date(
          state.lastFlushedEndTs,
        ).toISOString()}`,
      );
    }
    return false;
  }

  if (flushEndExclusive <= writeFrom) {
    return false;
  }

  const writeMaxSlot = flushEndExclusive - timeframeMs;
  const offsetBytes = computeResumeOffsetBytes(startBase, writeFrom, timeframeMs);
  const firstWrite = !state.hasFlushed && !usingResumeRewrite && writeFrom === startBase;
  const flag = firstWrite ? "w" : "r+";

  /* console.log(
    `[${acc.collector}/${acc.exchange}/${acc.symbol}/${timeframe}] flush compute writeFrom=${writeFrom} flushEndExclusive=${flushEndExclusive} flag=${flag} offsetBytes=${offsetBytes} rewrite=${usingResumeRewrite}`,
  ); */

  await fs.mkdir(path.dirname(state.binaryPath), { recursive: true });

  /* if (usingResumeRewrite) {
    const offsetCandles = offsetBytes / CANDLE_BYTES;
    console.log(
      `[${acc.collector}/${acc.exchange}/${acc.symbol}/${timeframe}] resume slot=${new Date(writeFrom).toISOString()} offsetCandles=${offsetCandles} offsetBytes=${offsetBytes}`,
    );
  } */

  if (flag !== "w") {
    let existingSize: number | undefined;
    try {
      existingSize = (await fs.stat(state.binaryPath)).size;
    } catch (err) {
      if ((err as { code?: string }).code !== "ENOENT") throw err;
    }
    if (usingResumeRewrite || (existingSize !== undefined && offsetBytes < existingSize)) {
      await truncateBinaryTo(state.binaryPath, offsetBytes);
    }
  }

  const newCandles = Math.max(0, Math.floor((flushEndExclusive - writeFrom) / timeframeMs));
  const totalCandles = Math.max(0, Math.floor((flushEndExclusive - startBase) / timeframeMs));
  const estimatedMb = ((totalCandles * CANDLE_BYTES) / (1024 * 1024)).toFixed(2);
  const modeLabel = usingResumeRewrite ? "resume" : state.hasFlushed ? "append" : "fresh";
  const verb = opts.final ? "final" : "flush";

  console.log(
    `[${acc.collector}/${acc.exchange}/${acc.symbol}/${timeframe}] ${verb} +${newCandles} candles (total=${totalCandles}, ~${estimatedMb} MB) range ${new Date(
      writeFrom,
    ).toISOString()} -> ${new Date(flushEndExclusive).toISOString()} (${modeLabel})`,
  );

  await writeBinaryRange(state.binaryPath, acc.buckets, writeFrom, writeMaxSlot, timeframeMs, {
    offsetBytes,
    flag,
  });

  const lastInputStartTs = acc.maxInputStartTs === Number.NEGATIVE_INFINITY ? undefined : acc.maxInputStartTs;

  const metadata: CompanionMetadata = {
    exchange: acc.exchange,
    symbol: acc.symbol,
    timeframe,
    timeframeMs,
    startTs: startBase,
    endTs: flushEndExclusive,
    priceScale: PRICE_SCALE,
    volumeScale: VOL_SCALE,
    records: totalCandles,
    lastInputStartTs,
  };

  await writeCompanion(state.outBase + ".json", metadata);
  db.upsertRegistry({
    collector: acc.collector,
    exchange: acc.exchange,
    symbol: acc.symbol,
    timeframe,
    startTs: metadata.startTs!,
    endTs: metadata.endTs!,
  });

  if (opts.final) {
    console.log(
      `[${acc.collector}/${acc.exchange}/${acc.symbol}/${timeframe}] processed ${acc.bucketCount} populated candles into ${totalCandles} slots -> ${state.outBase}.bin`,
    );
  }

  state.hasFlushed = true;
  state.needsResumeRewrite = false;
  state.lastFlushedEndTs = flushEndExclusive;
  state.nextWriteFrom = flushEndExclusive;
  acc.companion = metadata;

  pruneBucketsBefore(acc, flushEndExclusive - timeframeMs);

  return true;
}

function pruneBucketsBefore(acc: Accumulator, cutoffTs: number): void {
  let removed = 0;
  for (const ts of acc.buckets.keys()) {
    if (ts < cutoffTs) {
      acc.buckets.delete(ts);
      removed += 1;
    }
  }
  /* console.log(
    `[${acc.collector}/${acc.exchange}/${acc.symbol}/${acc.companion?.timeframe ?? "tf"}] prune buckets cutoff=${cutoffTs} removed=${removed} remaining=${acc.buckets.size}`,
  ); */
}

async function getBinaryState(outPath: string): Promise<BinaryState> {
  try {
    await fs.stat(outPath);
    return BinaryState.Exists;
  } catch (err) {
    if ((err as { code?: string }).code === "ENOENT") return BinaryState.Missing;
    throw err;
  }
}

async function truncateBinaryTo(outPath: string, offsetBytes: number): Promise<void> {
  await fs.truncate(outPath, offsetBytes);
}

async function writeBinaryRange(
  outPath: string,
  buckets: Map<number, Candle>,
  minSlot: number,
  maxSlot: number,
  timeframeMs: number,
  options?: { offsetBytes?: number; flag?: string },
): Promise<void> {
  const { offsetBytes = 0, flag = "w" } = options ?? {};
  const fh = await fs.open(outPath, flag);

  const empty = {
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

  // Write in chunks to avoid millions of small syscalls when the range spans years.
  const chunkCandles = 4096;
  const buf = Buffer.allocUnsafe(chunkCandles * CANDLE_BYTES);
  let ts = minSlot;
  let fileOffset = offsetBytes;

  while (ts <= maxSlot) {
    let count = 0;
    for (; count < chunkCandles && ts <= maxSlot; count += 1, ts += timeframeMs) {
      const c = buckets.get(ts) ?? empty;
      const base = count * CANDLE_BYTES;
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
    const bytes = count * CANDLE_BYTES;
    await fh.write(buf, 0, bytes, fileOffset);
    fileOffset += bytes;
  }

  await fh.close();
}

async function writeCompanion(outPath: string, metadata: CompanionMetadata) {
  await fs.writeFile(outPath, JSON.stringify(metadata, null, 2));
}

async function readCompanion(pathStr: string): Promise<CompanionMetadata | undefined> {
  try {
    const raw = await fs.readFile(pathStr, "utf8");
    return JSON.parse(raw) as CompanionMetadata;
  } catch {
    return undefined;
  }
}
