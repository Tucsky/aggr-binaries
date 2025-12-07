import fs from "node:fs/promises";
import path from "node:path";
import readline from "node:readline";
import zlib from "node:zlib";
import type { Config } from "./config.js";
import type { Db } from "./db.js";
import { normalizeExchange, normalizeSymbol } from "./normalize.js";

type Side = "buy" | "sell";

interface Trade {
  ts: number;
  price: number;
  size: number;
  side: Side;
  liquidation: boolean;
  exchange: string;
  symbol: string;
}

interface Candle {
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

const PRICE_SCALE = 1e4; // int32 safe for typical crypto prices
const VOL_SCALE = 1e6; // quote volume micro units
const CANDLE_BYTES = 56;

const LEGACY_MAP: Record<string, [string, string]> = {
  bitfinex: ["BITFINEX", "BTCUSD"],
  binance: ["BINANCE", "btcusdt"],
  okex: ["OKEX", "BTC-USDT"],
  kraken: ["KRAKEN", "XBT-USD"],
  gdax: ["COINBASE", "BTC-USD"],
  poloniex: ["POLONIEX", "BTC_USDT"],
  huobi: ["HUOBI", "btcusdt"],
  bitstamp: ["BITSTAMP", "btcusd"],
  bitmex: ["BITMEX", "XBTUSD"],
  binance_futures: ["BINANCE_FUTURES", "btcusdt"],
  deribit: ["DERIBIT", "BTC-PERPETUAL"],
  ftx: ["FTX", "BTC-PERP"],
  bybit: ["BYBIT", "BTCUSD"],
  hitbtc: ["HITBTC", "BTCUSD"],
};

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

  // Reduce the workload when filters are provided: keep all legacy files (they may contain the target exchange),
  // but drop logical files whose exchange/symbol metadata already excludes them.
  const filteredFiles = files.filter((f) => {
    if (f.era === "legacy") return true;
    if (allowExchange && f.exchange && f.exchange !== allowExchange) return false;
    if (allowSymbol && f.symbol && f.symbol !== allowSymbol) return false;
    return true;
  });

  const accMap = new Map<string, Accumulator>();
  let totalLines = 0;
  let totalTradesKept = 0;
  const startAll = Date.now();
  let maxBuckets = 0;
  let heartbeatTimer: NodeJS.Timeout | null = null;

  let processedFiles = 0;
  const totalFiles = filteredFiles.length;
  heartbeatTimer = setInterval(() => {
    console.log(
      `[process] heartbeat files=${processedFiles}/${totalFiles} accumulators=${accMap.size} maxBuckets=${maxBuckets} elapsed=${(
        (Date.now() - startAll) /
        1000
      ).toFixed(1)}s`,
    );
  }, 10_000);

  for (const file of filteredFiles) {
    processedFiles += 1;
    /*console.log(
      `[process] file ${processedFiles}/${totalFiles} ${file.collector} ${file.relative_path} (${file.era})`,
    );*/

    const fullPath = path.join(config.root, file.relative_path);
    const isLegacy = file.era === "legacy";
    const pathExchange: string | null = file.exchange ?? null;
    const pathSymbol: string | null = file.symbol ?? null;
    const fileStartTs: number | null = typeof file.start_ts === "number" ? file.start_ts : null;
    const fileSizeMb = file.size ? (file.size / (1024 * 1024)).toFixed(2) : "n/a";
    const fileStartMs = Date.now();
    let linesRead = 0;
    let tradesKept = 0;
    const touchedMarkets = new Set<string>();

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
      touchedMarkets.add(`${acc.exchange}/${acc.symbol}`);
    }
    rl.close();
    if (skippedWholeFile) {
      // nothing to do; resume logic already applied
    }

    totalLines += linesRead;
    totalTradesKept += tradesKept;
    for (const acc of accMap.values()) {
      if (acc.buckets.size > maxBuckets) maxBuckets = acc.buckets.size;
    }
    const elapsed = (Date.now() - fileStartMs) / 1000;
    /*console.log(
      `[process] file done ${file.collector} ${file.relative_path} lines=${linesRead} kept=${tradesKept} markets=${touchedMarkets.size} sizeMB=${fileSizeMb} elapsed=${elapsed.toFixed(
        2,
      )}s`,
    );*/

    /*if (processedFiles % 200 === 0) {
      console.log(
        `[process] progress ${processedFiles}/${files.length} accumulators=${accMap.size} maxBuckets=${maxBuckets}`,
      );
    }*/
  }

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

  const totalElapsed = (Date.now() - startAll) / 1000;
  console.log(
    `[process] complete files=${files.length} lines=${totalLines} kept=${totalTradesKept} elapsed=${totalElapsed.toFixed(
      2,
    )}s`,
  );

  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

async function resolveCollectors(config: Config, db: Db): Promise<string[]> {
  if (config.collector) return [config.collector.toUpperCase()];
  const rows = db.db.prepare("SELECT DISTINCT collector FROM files;").all() as Array<{ collector: string }>;
  return rows.map((r) => r.collector.toUpperCase());
}

function loadFiles(db: Db, collectors: string[]) {
  const stmt = db.db.prepare(
    `SELECT * FROM files
     WHERE collector IN (${collectors.map((_, i) => `:c${i}`).join(",")})
     ORDER BY collector, COALESCE(start_ts, 0), relative_path;`,
  );
  const params: Record<string, string> = {};
  collectors.forEach((c, i) => (params[`c${i}`] = c));
  return stmt.all(params) as any[];
}

async function ensureAccumulator(
  accMap: Map<string, Accumulator>,
  outRoot: string,
  collector: string,
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

function parseLegacyLine(line: string): Trade | null {
  const parts = line.trim().split(/\s+/);
  if (parts.length < 5) return null;
  const rawEx = parts[0];
  const ts = Number(parts[1]);
  const price = Number(parts[2]);
  const size = Number(parts[3]);
  const side = parts[4] === "1" ? "buy" : "sell";
  const liquidation = parts[5] === "1";
  const mapped = LEGACY_MAP[rawEx];
  if (!mapped) return null;
  const [exchange, rawSymbol] = mapped;
  const symbol = normalizeSymbol(exchange, rawSymbol, ts) ?? rawSymbol;
  if (!Number.isFinite(ts) || !Number.isFinite(price) || !Number.isFinite(size)) return null;
  return { ts, price, size, side, liquidation, exchange, symbol };
}

function parseLogicalLine(
  line: string,
  pathExchange?: string | null,
  pathSymbol?: string | null,
): Trade | null {
  const parts = line.trim().split(/\s+/);
  if (parts.length < 4) return null;
  const ts = Number(parts[0]);
  const price = Number(parts[1]);
  const size = Number(parts[2]);
  const side = parts[3] === "1" ? "buy" : "sell";
  const liquidation = parts[4] === "1";
  if (!pathExchange || !pathSymbol) return null;
  const exchange = normalizeExchange(pathExchange) ?? pathExchange;
  const symbol = normalizeSymbol(exchange, pathSymbol, ts) ?? pathSymbol;
  if (!Number.isFinite(ts) || !Number.isFinite(price) || !Number.isFinite(size)) return null;
  return { ts, price, size, side, liquidation, exchange, symbol };
}

function applyCorrections(trade: Trade): Trade | null {
  let t = { ...trade };
  // bitfinex liquidations flip side
  if (t.exchange === "BITFINEX" && t.liquidation) {
    t = { ...t, side: t.side === "buy" ? "sell" : "buy" };
  }
  // okex liquidation size divide by 500 for specific range
  if (
    t.exchange === "OKEX" &&
    t.liquidation &&
    t.ts >= 1572940388059 &&
    t.ts < 1572964319495
  ) {
    t = { ...t, size: t.size / 500 };
  }
  // randomize side for non-liq in given range (deterministic hash)
  if (
    !t.liquidation &&
    t.ts >= 1574193600000 &&
    t.ts <= 1575489600000
  ) {
    const rnd = (t.ts * 9301 + 49297) % 233280;
    t = { ...t, side: rnd / 233280 >= 0.5 ? "buy" : "sell" };
  }
  return t;
}

function accumulate(acc: Accumulator, t: Trade) {
  const minute = Math.floor(t.ts / 60000) * 60000;
  if (minute < acc.minMinute) acc.minMinute = minute;
  if (minute > acc.maxMinute) acc.maxMinute = minute;
  const priceInt = Math.round(t.price * PRICE_SCALE);
  const quoteVol = BigInt(Math.round(t.price * t.size * VOL_SCALE));
  const existing = acc.buckets.get(minute);
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
  acc.buckets.set(minute, bucket);
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
