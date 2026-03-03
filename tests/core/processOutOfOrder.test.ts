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
import { CANDLE_BYTES, PRICE_SCALE } from "../../src/core/trades.js";
import { parseTimeframeMs } from "../../src/shared/timeframes.js";

const TIMEFRAME = "1m";
const TIMEFRAME_MS: number = (() => {
  const ms = parseTimeframeMs(TIMEFRAME);
  if (ms === undefined) {
    throw new Error("Failed to parse timeframe");
  }
  return ms;
})();

const MARKET = { collector: "PI", bucket: "bucketA", exchange: "KRAKEN", symbol: "XBT-USD" };

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

function readCandleAtSlot(bin: Buffer, startTs: number, slotTs: number): { high: number; close: number } {
  const candleIndex = Math.floor((slotTs - startTs) / TIMEFRAME_MS);
  const base = candleIndex * CANDLE_BYTES;
  if (candleIndex < 0 || base + CANDLE_BYTES > bin.length) {
    throw new Error(`Requested candle out of range: slotTs=${slotTs} startTs=${startTs}`);
  }
  return {
    high: bin.readInt32LE(base + 4),
    close: bin.readInt32LE(base + 12),
  };
}

test("process ignores late out-of-order rows for gap detection and candle close", async () => {
  const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "aggr-process-out-of-order-"));
  const root = path.join(baseDir, "input");
  const outDir = path.join(baseDir, "out");
  const marketDir = path.join(root, MARKET.collector, MARKET.bucket, MARKET.exchange, MARKET.symbol);
  await fs.mkdir(marketDir, { recursive: true });

  const alignedMinuteTs = Math.floor(1_710_500_000_000 / TIMEFRAME_MS) * TIMEFRAME_MS;
  const firstTradeTs = alignedMinuteTs + 2_000;
  let currentTs = firstTradeTs;
  const lines: string[] = [];

  for (let i = 0; i < 2_400; i += 1) {
    const side = i % 2 === 0 ? 1 : 0;
    lines.push(`${currentTs} 100 1 ${side} 0`);
    currentTs += 300;
  }

  const lateBackfillTs = firstTradeTs + 25_000;
  lines.push(`${lateBackfillTs} 30000 0.1 1 0`);
  currentTs += 300;
  lines.push(`${currentTs} 101 1 1 0`);

  const fileName = "2024-04-04-00";
  await fs.writeFile(path.join(marketDir, fileName), lines.join("\n"));
  const relativePath = path.posix.join(
    MARKET.collector,
    MARKET.bucket,
    MARKET.exchange,
    MARKET.symbol,
    fileName,
  );

  const dbPath = path.join(baseDir, "out-of-order.sqlite");
  const db = openDatabase(dbPath);
  const config = buildConfig(root, outDir, dbPath);

  try {
    insertFixtureFile(db, root, relativePath);
    await runProcess(config, db);

    const gapCountRow = db.db.prepare("SELECT COUNT(*) AS cnt FROM gaps;").get() as { cnt: number };
    assert.strictEqual(gapCountRow.cnt, 0);

    const outBase = path.join(outDir, MARKET.collector, MARKET.exchange, MARKET.symbol, TIMEFRAME);
    const bin = await fs.readFile(`${outBase}.bin`);
    const companionRaw = await fs.readFile(`${outBase}.json`, "utf8");
    const companion = JSON.parse(companionRaw) as { startTs?: number };
    if (!Number.isFinite(companion.startTs)) {
      throw new Error("Companion startTs missing");
    }

    const backfillSlotTs = Math.floor(lateBackfillTs / TIMEFRAME_MS) * TIMEFRAME_MS;
    const candle = readCandleAtSlot(bin, companion.startTs as number, backfillSlotTs);
    assert.strictEqual(candle.high, Math.round(30000 * PRICE_SCALE));
    assert.strictEqual(candle.close, Math.round(100 * PRICE_SCALE));
  } finally {
    db.close();
  }
});
