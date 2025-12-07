import fs from "node:fs/promises";
import path from "node:path";
import readline from "node:readline";
import zlib from "node:zlib";
import type { Config } from "./config.js";
import type { Db } from "./db.js";
import {
  PRICE_SCALE,
  VOL_SCALE,
  CANDLE_BYTES,
  applyCorrections,
  parseLegacyLine,
  parseLogicalLine,
  accumulate,
  type Candle,
} from "./trades.js";
import { Collector, Era, type FileRow } from "./model.js";
import { normalizeSymbol } from "./normalize.js";

interface Accumulator {
  collector: string;
  exchange: string;
  symbol: string;
  buckets: Map<number, Candle>;
  minMinute: number;
  maxMinute: number;
  maxInputStartTs: number;
  companion?: {
    startTs: number;
    endTs: number;
    lastInputStartTs?: number;
    priceScale: number;
    volumeScale: number;
    records: number;
  };
}

export async function runProcess(config: Config, db: Db): Promise<void> {
  const collectors = await resolveCollectors(config, db);
  if (!collectors.length) {
    throw new Error("No collectors found; specify --collector or ensure index has rows.");
  }

  const files = loadFiles(db, collectors);
  if (!files.length) {
    console.log("No files to process for selected collectors.");
    return;
  }

  console.log(
    `[process] collectors=${collectors.join(",")} files=${files.length} filters=${
      config.exchange || "ALL"
    }/${config.symbol || "ALL"}`,
  );

  const outRoot = path.resolve(config.outDir ?? "output");
  const allowExchange = config.exchange?.toUpperCase();
  const allowSymbol = config.symbol;

  const startAll = Date.now();
  const { accMap, totalLines, totalTradesKept, heartbeatTimer } = await processFiles({
    files,
    allowExchange,
    allowSymbol,
    config,
    outRoot,
    startAll,
  });

  await writeOutputs(accMap, outRoot);

  const totalElapsed = (Date.now() - startAll) / 1000;
  console.log(
    `[process] complete files=${files.length} lines=${totalLines} kept=${totalTradesKept} elapsed=${totalElapsed.toFixed(
      2,
    )}s`,
  );

  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
  }
}

async function resolveCollectors(config: Config, db: Db): Promise<string[]> {
  if (config.collector) return [config.collector.toUpperCase()];
  const rows = db.db.prepare("SELECT DISTINCT collector FROM files;").all() as Array<{ collector: string }>;
  return rows.map((r) => r.collector.toUpperCase());
}

function loadFiles(db: Db, collectors: string[]): FileRow[] {
  const stmt = db.db.prepare(
    `SELECT * FROM files
     WHERE collector IN (${collectors.map((_, i) => `:c${i}`).join(",")})
     ORDER BY collector, COALESCE(start_ts, 0), relative_path;`,
  );
  const params: Record<string, string> = {};
  collectors.forEach((c, i) => (params[`c${i}`] = c));
  return stmt.all(params) as unknown as FileRow[];
}

async function ensureAccumulator(
  accMap: Map<string, Accumulator>,
  outRoot: string,
  collector: Collector | string,
  exchange: string,
  symbol: string,
): Promise<Accumulator | null> {
  const key = `${collector}::${exchange}::${symbol}`;
  const existing = accMap.get(key);
  if (existing) return existing;

  const companionPath = path.join(outRoot, collector, exchange, `${symbol}.json`);
  const companion = await readCompanion(companionPath);

  const acc: Accumulator = {
    collector,
    exchange,
    symbol,
    buckets: new Map(),
    minMinute: Number.POSITIVE_INFINITY,
    maxMinute: Number.NEGATIVE_INFINITY,
    maxInputStartTs: Number.NEGATIVE_INFINITY,
    companion: companion
      ? {
          startTs: companion.startTs,
          endTs: companion.endTs,
          lastInputStartTs: companion.lastInputStartTs,
          priceScale: companion.priceScale,
          volumeScale: companion.volumeScale,
          records: companion.records,
        }
      : undefined,
  };
  accMap.set(key, acc);
  return acc;
}

async function makeStream(filePath: string) {
  const file = await fs.open(filePath, "r");
  const stream = file.createReadStream();
  if (filePath.endsWith(".gz")) {
    return stream.pipe(zlib.createGunzip());
  }
  return stream;
}

interface ProcessResult {
  accMap: Map<string, Accumulator>;
  totalLines: number;
  totalTradesKept: number;
  heartbeatTimer: NodeJS.Timeout | null;
}

async function processFiles(opts: {
  files: FileRow[];
  allowExchange?: string;
  allowSymbol?: string;
  config: Config;
  outRoot: string;
  startAll: number;
}): Promise<ProcessResult> {
  const { files, allowExchange, allowSymbol, config, outRoot, startAll } = opts;

  const filteredFiles = files.filter((f) => {
    if (f.era === Era.Legacy) return true;
    if (allowExchange && f.exchange && f.exchange !== allowExchange) return false;
    if (allowSymbol && f.symbol && f.symbol !== allowSymbol) return false;
    return true;
  });

  const accMap = new Map<string, Accumulator>();
  let totalLines = 0;
  let totalTradesKept = 0;
  let maxBuckets = 0;
  let processedFiles = 0;
  const totalFiles = filteredFiles.length;

  const heartbeatTimer = setInterval(() => {
    console.log(
      `[process] heartbeat files=${processedFiles}/${totalFiles} accumulators=${accMap.size} maxBuckets=${maxBuckets} elapsed=${(
        (Date.now() - startAll) /
        1000
      ).toFixed(1)}s`,
    );
  }, 10_000);

  for (const file of filteredFiles) {
    processedFiles += 1;
    const { linesRead, tradesKept } = await processSingleFile({
      file,
      allowExchange,
      allowSymbol,
      config,
      outRoot,
      accMap,
    });

    totalLines += linesRead;
    totalTradesKept += tradesKept;
    for (const acc of accMap.values()) {
      if (acc.buckets.size > maxBuckets) maxBuckets = acc.buckets.size;
    }
  }

  return { accMap, totalLines, totalTradesKept, heartbeatTimer };
}

async function processSingleFile(opts: {
  file: FileRow;
  allowExchange?: string;
  allowSymbol?: string;
  config: Config;
  outRoot: string;
  accMap: Map<string, Accumulator>;
}): Promise<{ linesRead: number; tradesKept: number }> {
  const { file, allowExchange, allowSymbol, config, outRoot, accMap } = opts;

  const fullPath = path.join(config.root, file.relative_path);
  const isLegacy = file.era === Era.Legacy;
  const pathExchange: string | null = file.exchange ?? null;
  const pathSymbol: string | null = file.symbol ?? null;
  const fileStartTs: number | null = typeof file.start_ts === "number" ? file.start_ts : null;

  let linesRead = 0;
  let tradesKept = 0;
  let skippedWholeFile = false;

  const stream = await makeStream(fullPath);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  for await (const line of rl) {
    linesRead += 1;
    const trade = isLegacy ? parseLegacyLine(line) : parseLogicalLine(line, pathExchange, pathSymbol);
    if (!trade) continue;

    if (allowExchange && trade.exchange !== allowExchange) continue;
    const normSym = normalizeSymbol(trade.exchange, trade.symbol, trade.ts) ?? trade.symbol;
    if (allowSymbol && normSym !== allowSymbol) continue;

    const acc = await ensureAccumulator(accMap, outRoot, file.collector, trade.exchange, normSym);
    if (!acc) continue;

    if (!config.force && acc.companion?.lastInputStartTs !== undefined && fileStartTs !== null) {
      if (fileStartTs < acc.companion.lastInputStartTs) {
        skippedWholeFile = true;
        continue;
      }
    }
    if (!config.force && acc.companion && trade.ts < acc.companion.endTs) continue;

    const corrected = applyCorrections({ ...trade, symbol: normSym });
    if (!corrected) continue;

    accumulate(acc, corrected);
    if (fileStartTs !== null && fileStartTs > acc.maxInputStartTs) acc.maxInputStartTs = fileStartTs;
    tradesKept += 1;
  }
  rl.close();
  if (skippedWholeFile) {
    // resume logic already applied
  }

  return { linesRead, tradesKept };
}

async function writeOutputs(accMap: Map<string, Accumulator>, outRoot: string) {
  let written = 0;
  const accList = Array.from(accMap.values());
  for (const acc of accList) {
    if (!acc.buckets.size || !isFinite(acc.minMinute) || !isFinite(acc.maxMinute)) {
      console.log(`[${acc.collector}/${acc.exchange}/${acc.symbol}] no trades; skipping output`);
      continue;
    }

    const startBase =
      acc.companion?.startTs !== undefined ? Math.min(acc.companion.startTs, acc.minMinute) : acc.minMinute;
    const endBase =
      acc.companion?.endTs !== undefined ? Math.max(acc.companion.endTs, acc.maxMinute + 60000) : acc.maxMinute + 60000;
    const totalCandles = Math.max(0, Math.floor((endBase - startBase) / 60000));
    const estimatedMb = ((totalCandles * CANDLE_BYTES) / (1024 * 1024)).toFixed(2);

    const outDir = path.join(outRoot, acc.collector, acc.exchange);
    await fs.mkdir(outDir, { recursive: true });
    const outBase = path.join(outDir, acc.symbol);

    console.log(
      `[${acc.collector}/${acc.exchange}/${acc.symbol}] writing ${totalCandles} candles (~${estimatedMb} MB) range ${new Date(
        startBase,
      ).toISOString()} -> ${new Date(endBase).toISOString()}`,
    );

    await writeBinary(outBase + ".bin", acc.buckets, startBase, endBase - 60000);

    const lastInputStartTs =
      acc.maxInputStartTs > Number.NEGATIVE_INFINITY || acc.companion?.lastInputStartTs !== undefined
        ? Math.max(acc.maxInputStartTs, acc.companion?.lastInputStartTs ?? Number.NEGATIVE_INFINITY)
        : undefined;

    await writeCompanion(
      outBase + ".json",
      acc.exchange,
      acc.symbol,
      totalCandles,
      startBase,
      endBase,
      lastInputStartTs === Number.NEGATIVE_INFINITY ? undefined : lastInputStartTs,
    );

    console.log(
      `[${acc.collector}/${acc.exchange}/${acc.symbol}] processed ${acc.buckets.size} populated candles into ${totalCandles} slots -> ${outBase}.bin`,
    );
    written += 1;
    if (written % 25 === 0) {
      console.log(`[process] output progress ${written}/${accList.length}`);
    }
  }
}

async function writeBinary(outPath: string, buckets: Map<number, Candle>, minMinute: number, maxMinute: number): Promise<void> {
  const fh = await fs.open(outPath, "w");
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
  let ts = minMinute;
  let fileOffset = 0;

  while (ts <= maxMinute) {
    let count = 0;
    for (; count < chunkCandles && ts <= maxMinute; count += 1, ts += 60000) {
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

async function writeCompanion(
  outPath: string,
  exchange: string,
  symbol: string,
  totalCandles: number,
  minTs: number,
  maxTs: number,
  lastInputStartTs?: number,
) {
  const data = {
    exchange,
    symbol,
    timeframe: "1m",
    startTs: minTs,
    endTs: maxTs,
    priceScale: PRICE_SCALE,
    volumeScale: VOL_SCALE,
    records: totalCandles,
    lastInputStartTs,
  };
  await fs.writeFile(outPath, JSON.stringify(data, null, 2));
}

async function readCompanion(pathStr: string): Promise<
  | {
      exchange: string;
      symbol: string;
      timeframe: string;
      startTs: number;
      endTs: number;
      priceScale: number;
      volumeScale: number;
      records: number;
      lastInputStartTs?: number;
    }
  | undefined
> {
  try {
    const raw = await fs.readFile(pathStr, "utf8");
    return JSON.parse(raw);
  } catch {
    return undefined;
  }
}
