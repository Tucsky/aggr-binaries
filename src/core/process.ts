import fs from "node:fs/promises";
import path from "node:path";
import readline from "node:readline";
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
  type Candle
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

  const timeframeMs = config.timeframeMs;

  let acc: Accumulator | null = null;
  let totalLines = 0;
  let totalTradesKept = 0;
  let processedFiles = 0;
  let processedMarkets = 0;
  let maxBuckets = 0;

  const heartbeatTimer = setInterval(() => {
    const elapsed = ((Date.now() - startAll) / 1000).toFixed(1);
    console.log(
      `[process] heartbeat markets=${processedMarkets}/${totalMarkets} files=${processedFiles}/${totalCandidates} buckets=${maxBuckets} current=${acc ? makeMarketKey(acc.collector, acc.exchange, acc.symbol) : "idle"} elapsed=${elapsed}s`,
    );
  }, 10_000);

  for (const market of markets) {
    acc = await startAccumulatorForMarket(config, market.collector, market.exchange, market.symbol);

    const minStartTs =
      !config.force && acc.companion?.lastInputStartTs !== undefined ? acc.companion.lastInputStartTs : undefined;
    const files = iterateFilesForMarket(db, market, minStartTs);
    const resumeCutoff = !config.force && acc.companion ? acc.companion.endTs : undefined;

    for (const file of files) {
      const { linesRead, tradesKept, newBuckets } = await streamFile({
        file,
        acc,
        root: config.root,
        timeframeMs,
        resumeCutoff,
      });

      processedFiles += 1;
      totalLines += linesRead;
      totalTradesKept += tradesKept;
      if (newBuckets && acc.bucketCount > maxBuckets) {
        maxBuckets = acc.bucketCount;
      }
    }

    await writeMarketOutput(acc, config, db);
    processedMarkets += 1;
    acc = null;
  }

  clearInterval(heartbeatTimer);

  return { totalLines, totalTradesKept, processedFiles, processedMarkets, maxBuckets };
}

async function streamFile(opts: {
  file: FileRow;
  acc: Accumulator;
  root: string;
  timeframeMs: number;
  resumeCutoff?: number;
}): Promise<StreamResult> {
  const { file, acc, root, timeframeMs, resumeCutoff } = opts;

  const fullPath = path.join(root, file.relative_path);
  const fileStartTs = file.start_ts;

  const stream = await makeStream(fullPath);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let linesRead = 0;
  let tradesKept = 0;
  let newBuckets = 0;

  for await (const line of rl) {
    linesRead += 1;

    const trade = parseTradeLine(line, acc.exchange, acc.symbol);
    if (!trade) continue;

    if (resumeCutoff !== undefined && trade.ts < resumeCutoff) continue;

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

  return { linesRead, tradesKept, newBuckets };
}

async function makeStream(filePath: string) {
  const file = await fs.open(filePath, "r");
  const stream = file.createReadStream();
  if (filePath.endsWith(".gz")) {
    return stream.pipe(zlib.createGunzip());
  }
  return stream;
}

async function writeMarketOutput(
  acc: Accumulator,
  config: Config,
  db: Db,
) {
  const timeframe = config.timeframe;
  const timeframeMs = config.timeframeMs;
  const sparse = Boolean(config.sparseOutput);

  if (!acc.bucketCount || !isFinite(acc.minMinute) || !isFinite(acc.maxMinute)) {
    console.log(`[${acc.collector}/${acc.exchange}/${acc.symbol}] no trades; skipping output`);
    return;
  }

  const startBase =
    acc.companion?.startTs !== undefined ? Math.min(acc.companion.startTs, acc.minMinute) : acc.minMinute;
  const endBase =
    acc.companion?.endTs !== undefined ? Math.max(acc.companion.endTs, acc.maxMinute + timeframeMs) : acc.maxMinute + timeframeMs;
  const totalCandles = sparse ? acc.bucketCount : Math.max(0, Math.floor((endBase - startBase) / timeframeMs));
  const estimatedMb = ((totalCandles * CANDLE_BYTES) / (1024 * 1024)).toFixed(2);

  const outBase = path.join(config.outDir, acc.collector, acc.exchange, acc.symbol, timeframe);
  await fs.mkdir(path.dirname(outBase), { recursive: true });

  console.log(
    `[${acc.collector}/${acc.exchange}/${acc.symbol}/${timeframe}] writing ${totalCandles} candles (~${estimatedMb} MB) range ${new Date(
      startBase,
    ).toISOString()} -> ${new Date(endBase).toISOString()} sparse=${sparse}`,
  );

  await writeBinary(outBase + ".bin", acc.buckets, startBase, endBase - timeframeMs, timeframeMs, sparse);

  const lastInputStartTs = acc.maxInputStartTs === Number.NEGATIVE_INFINITY ? undefined : acc.maxInputStartTs;

  const metadata: CompanionMetadata = {
    exchange: acc.exchange,
    symbol: acc.symbol,
    timeframe,
    timeframeMs,
    startTs: startBase,
    endTs: endBase,
    priceScale: PRICE_SCALE,
    volumeScale: VOL_SCALE,
    records: totalCandles,
    sparse,
    lastInputStartTs,
  };

  await writeCompanion(outBase + ".json", metadata);
  db.upsertRegistry({
    collector: acc.collector,
    exchange: acc.exchange,
    symbol: acc.symbol,
    timeframe,
    startTs: metadata.startTs!,
    endTs: metadata.endTs!,
    sparse: metadata.sparse ?? false,
  });

  console.log(
    `[${acc.collector}/${acc.exchange}/${acc.symbol}/${timeframe}] processed ${acc.bucketCount} populated candles into ${totalCandles} slots -> ${outBase}.bin`,
  );
}

async function writeBinary(
  outPath: string,
  buckets: Map<number, Candle>,
  minSlot: number,
  maxSlot: number,
  timeframeMs: number,
  sparse: boolean,
): Promise<void> {
  const fh = await fs.open(outPath, "w");
  if (sparse) {
    const sorted = Array.from(buckets.entries()).sort((a, b) => a[0] - b[0]);
    const recordSize = 8 + CANDLE_BYTES; // ts + candle
    const buf = Buffer.allocUnsafe(recordSize * sorted.length);
    let idx = 0;
    for (const [ts, c] of sorted) {
      const base = idx * recordSize;
      buf.writeBigInt64LE(BigInt(ts), base);
      buf.writeInt32LE(c.open, base + 8);
      buf.writeInt32LE(c.high, base + 12);
      buf.writeInt32LE(c.low, base + 16);
      buf.writeInt32LE(c.close, base + 20);
      buf.writeBigInt64LE(c.buyVol, base + 24);
      buf.writeBigInt64LE(c.sellVol, base + 32);
      buf.writeUint32LE(c.buyCount >>> 0, base + 40);
      buf.writeUint32LE(c.sellCount >>> 0, base + 44);
      buf.writeBigInt64LE(c.liqBuy, base + 48);
      buf.writeBigInt64LE(c.liqSell, base + 56);
      idx += 1;
    }
    await fh.write(buf, 0, buf.length, 0);
    await fh.close();
    return;
  }

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
  let fileOffset = 0;

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
