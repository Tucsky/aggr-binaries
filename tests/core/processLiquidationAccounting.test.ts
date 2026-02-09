import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import type { Config } from "../../src/core/config.js";
import type { Db } from "../../src/core/db.js";
import { openDatabase } from "../../src/core/db.js";
import { classifyPath } from "../../src/core/normalize.js";
import { runProcess } from "../../src/core/process.js";
import { CANDLE_BYTES, PRICE_SCALE, VOL_SCALE } from "../../src/core/trades.js";
import { parseTimeframeMs } from "../../src/shared/timeframes.js";

const TIMEFRAME = "1m";
const TIMEFRAME_MS: number = (() => {
  const ms = parseTimeframeMs(TIMEFRAME);
  if (ms === undefined) throw new Error("Failed to parse timeframe");
  return ms;
})();

const MARKET = { collector: "RAM", bucket: "bucketA", exchange: "BYBIT", symbol: "BTCUSDT" };

function buildConfig(root: string, outDir: string, dbPath: string): Config {
  return {
    root,
    dbPath,
    batchSize: 10,
    flushIntervalSeconds: 1,
    outDir,
    collector: MARKET.collector,
    exchange: MARKET.exchange,
    symbol: MARKET.symbol,
    timeframe: TIMEFRAME,
    timeframeMs: TIMEFRAME_MS,
  };
}

function insertFixtureFile(db: Db, root: string, relativePath: string): void {
  const rootId = db.ensureRoot(root);
  const row = classifyPath(rootId, relativePath);
  if (!row) throw new Error(`Failed to classify fixture path ${relativePath}`);
  db.insertFiles([row]);
}

function quote(price: number, size: number): bigint {
  return BigInt(Math.round(price * size * VOL_SCALE));
}

function readCandle(bin: Buffer, index: number) {
  const base = index * CANDLE_BYTES;
  return {
    open: bin.readInt32LE(base),
    high: bin.readInt32LE(base + 4),
    low: bin.readInt32LE(base + 8),
    close: bin.readInt32LE(base + 12),
    buyVol: bin.readBigInt64LE(base + 16),
    sellVol: bin.readBigInt64LE(base + 24),
    buyCount: bin.readUInt32LE(base + 32),
    sellCount: bin.readUInt32LE(base + 36),
    liqBuy: bin.readBigInt64LE(base + 40),
    liqSell: bin.readBigInt64LE(base + 48),
  };
}

test("process keeps liquidation rows out of OHLC and trade stats", async () => {
  const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "aggr-process-liq-accounting-"));
  const root = path.join(baseDir, "input");
  const outDir = path.join(baseDir, "out");
  const marketDir = path.join(root, MARKET.collector, MARKET.bucket, MARKET.exchange, MARKET.symbol);
  await fs.mkdir(marketDir, { recursive: true });

  const baseTs = Math.floor(1_720_123_456_789 / TIMEFRAME_MS) * TIMEFRAME_MS;
  const lines = [
    `${baseTs + 1_000} 100 1 1 1`,
    `${baseTs + 5_000} 101 2 0 0`,
    `${baseTs + 20_000} 999 1 0 1`,
    `${baseTs + 30_000} 103 1 1 0`,
    `${baseTs + TIMEFRAME_MS + 1_000} 120 0.5 1 1`,
    `${baseTs + TIMEFRAME_MS + 2_000} 80 2 0 1`,
  ];

  const fileName = "2024-04-04-00";
  await fs.writeFile(path.join(marketDir, fileName), lines.join("\n"));

  const relativePath = path.posix.join(
    MARKET.collector,
    MARKET.bucket,
    MARKET.exchange,
    MARKET.symbol,
    fileName,
  );
  const dbPath = path.join(baseDir, "liq-accounting.sqlite");
  const db = openDatabase(dbPath);
  const config = buildConfig(root, outDir, dbPath);

  try {
    insertFixtureFile(db, root, relativePath);
    await runProcess(config, db);

    const baseOut = path.join(outDir, MARKET.collector, MARKET.exchange, MARKET.symbol, TIMEFRAME);
    const bin = await fs.readFile(`${baseOut}.bin`);
    assert.strictEqual(bin.length, CANDLE_BYTES * 2);

    const first = readCandle(bin, 0);
    const second = readCandle(bin, 1);

    assert.deepStrictEqual(first, {
      open: Math.round(101 * PRICE_SCALE),
      high: Math.round(103 * PRICE_SCALE),
      low: Math.round(101 * PRICE_SCALE),
      close: Math.round(103 * PRICE_SCALE),
      buyVol: quote(103, 1),
      sellVol: quote(101, 2),
      buyCount: 1,
      sellCount: 1,
      liqBuy: quote(100, 1),
      liqSell: quote(999, 1),
    });

    assert.deepStrictEqual(second, {
      open: 0,
      high: 0,
      low: 0,
      close: 0,
      buyVol: 0n,
      sellVol: 0n,
      buyCount: 0,
      sellCount: 0,
      liqBuy: quote(120, 0.5),
      liqSell: quote(80, 2),
    });
  } finally {
    db.close();
  }
});
