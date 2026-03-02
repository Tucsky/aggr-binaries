import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import type { Config } from "../../src/core/config.js";
import { parseTimeframeMs } from "../../src/core/config.js";
import type { Db } from "../../src/core/db.js";
import { openDatabase } from "../../src/core/db.js";
import { GapFixStatus } from "../../src/core/model.js";
import { createAdapterRegistry } from "../../src/core/gaps/adapters/index.js";
import { runFixGaps } from "../../src/core/gaps/index.js";
import { classifyPath } from "../../src/core/normalize.js";
import { runProcess } from "../../src/core/process.js";

const MARKET = {
  collector: "RAM",
  bucket: "bucketA",
  exchange: "KRAKEN",
  symbol: "XBT-USD",
};

const SLOT_4H = Date.UTC(2024, 0, 1, 20, 0, 0);
const TRADE_A0 = SLOT_4H;
const TRADE_MISSING = SLOT_4H + (5 * 60_000);
const TRADE_A1 = SLOT_4H + (60 * 60_000);
const TRADE_A2 = SLOT_4H + (65 * 60_000);
const TRADE_B1 = SLOT_4H + (120 * 60_000);
const TRADE_B2 = SLOT_4H + (180 * 60_000);

interface Fixture {
  baseDir: string;
  root: string;
  outDir: string;
  dbPath: string;
  relativePathA: string;
  relativePathB: string;
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

interface CompanionLike {
  startTs: number;
  timeframeMs: number;
}

async function createFixture(): Promise<Fixture> {
  const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "aggr-fixgaps-overlap-"));
  const root = path.join(baseDir, "input");
  const outDir = path.join(baseDir, "out");
  const dbPath = path.join(baseDir, "index.sqlite");
  const dir = path.join(root, MARKET.collector, MARKET.bucket, MARKET.exchange, MARKET.symbol);
  await fs.mkdir(dir, { recursive: true });

  const fileA = "2024-01-01-20";
  const fileB = "2024-01-02-00";

  const pathA = path.join(dir, fileA);
  const pathB = path.join(dir, fileB);
  const relativePathA = path.posix.join(MARKET.collector, MARKET.bucket, MARKET.exchange, MARKET.symbol, fileA);
  const relativePathB = path.posix.join(MARKET.collector, MARKET.bucket, MARKET.exchange, MARKET.symbol, fileB);

  // fileA has a missing early trade that fixgaps will recover.
  await fs.writeFile(pathA, [`${TRADE_A0} 150 1 1 0`, `${TRADE_A1} 200 1 1 0`, `${TRADE_A2} 201 1 0 0`].join("\n"));
  // fileB is named for 00:00 but carries earlier trades in the same 20:00-00:00 4h slot.
  await fs.writeFile(pathB, [`${TRADE_B1} 300 1 1 0`, `${TRADE_B2} 400 2 0 0`].join("\n"));

  return { baseDir, root, outDir, dbPath, relativePathA, relativePathB };
}

function buildConfig(root: string, outDir: string, dbPath: string, timeframe: string): Config {
  const timeframeMs = parseTimeframeMs(timeframe);
  if (!timeframeMs) throw new Error(`Invalid timeframe ${timeframe}`);
  return {
    root,
    dbPath,
    outDir,
    batchSize: 100,
    flushIntervalSeconds: 1,
    collector: MARKET.collector,
    exchange: MARKET.exchange,
    symbol: MARKET.symbol,
    timeframe,
    timeframeMs,
  };
}

function insertIndexedFile(db: Db, root: string, relativePath: string): number {
  const rootId = db.ensureRoot(root);
  const row = classifyPath(rootId, relativePath);
  if (!row) {
    throw new Error(`Failed to classify ${relativePath}`);
  }
  db.insertFiles([row]);
  return rootId;
}

function insertGapEvent(
  db: Db,
  payload: {
    rootId: number;
    relativePath: string;
    startLine?: number;
    endLine?: number;
    gapMs?: number;
    gapEndTs?: number;
  },
): number {
  const result = db.db
    .prepare(
      `INSERT INTO gaps
        (root_id, relative_path, collector, exchange, symbol, gap_ms, gap_miss, gap_end_ts, gap_fix_status, gap_score)
       VALUES
        (:rootId, :relativePath, :collector, :exchange, :symbol, :gapMs, :gapMiss, :gapEndTs, NULL, NULL);`,
    )
    .run({
      rootId: payload.rootId,
      relativePath: payload.relativePath,
      collector: MARKET.collector,
      exchange: MARKET.exchange,
      symbol: MARKET.symbol,
      gapMs: payload.gapMs ?? (TRADE_A1 - (TRADE_MISSING - 1)),
      gapMiss: 1,
      gapEndTs: payload.gapEndTs ?? TRADE_A1,
    });
  return Number(result.lastInsertRowid);
}

function decodeCandle(buf: Buffer, base: number): Candle {
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

async function readCompanion(outDir: string, timeframe: string): Promise<CompanionLike> {
  const p = path.join(outDir, MARKET.collector, MARKET.exchange, MARKET.symbol, `${timeframe}.json`);
  const raw = await fs.readFile(p, "utf8");
  const parsed = JSON.parse(raw) as { startTs: number; timeframeMs: number };
  return { startTs: parsed.startTs, timeframeMs: parsed.timeframeMs };
}

async function readCandleAtSlot(outDir: string, timeframe: string, slotTs: number): Promise<Candle> {
  const companion = await readCompanion(outDir, timeframe);
  const idx = Math.floor((slotTs - companion.startTs) / companion.timeframeMs);
  if (!Number.isFinite(idx) || idx < 0) {
    throw new Error(`Invalid index for ${timeframe} slot ${slotTs}`);
  }
  const p = path.join(outDir, MARKET.collector, MARKET.exchange, MARKET.symbol, `${timeframe}.bin`);
  const fh = await fs.open(p, "r");
  const buf = Buffer.allocUnsafe(56);
  await fh.read(buf, 0, 56, idx * 56);
  await fh.close();
  return decodeCandle(buf, 0);
}

function encodeCandle(buf: Buffer, candle: Candle): void {
  buf.writeInt32LE(candle.open, 0);
  buf.writeInt32LE(candle.high, 4);
  buf.writeInt32LE(candle.low, 8);
  buf.writeInt32LE(candle.close, 12);
  buf.writeBigInt64LE(candle.buyVol, 16);
  buf.writeBigInt64LE(candle.sellVol, 24);
  buf.writeUInt32LE(candle.buyCount >>> 0, 32);
  buf.writeUInt32LE(candle.sellCount >>> 0, 36);
  buf.writeBigInt64LE(candle.liqBuy, 40);
  buf.writeBigInt64LE(candle.liqSell, 48);
}

async function writeCandleAtSlot(outDir: string, timeframe: string, slotTs: number, candle: Candle): Promise<void> {
  const companion = await readCompanion(outDir, timeframe);
  const idx = Math.floor((slotTs - companion.startTs) / companion.timeframeMs);
  if (!Number.isFinite(idx) || idx < 0) {
    throw new Error(`Invalid index for ${timeframe} slot ${slotTs}`);
  }
  const p = path.join(outDir, MARKET.collector, MARKET.exchange, MARKET.symbol, `${timeframe}.bin`);
  const fh = await fs.open(p, "r+");
  const buf = Buffer.allocUnsafe(56);
  encodeCandle(buf, candle);
  await fh.write(buf, 0, 56, idx * 56);
  await fh.close();
}

async function aggregate1mIntoSlot(outDir: string, slotTs: number, slotTfMs: number): Promise<Candle> {
  const companion = await readCompanion(outDir, "1m");
  const oneMinute = companion.timeframeMs;
  if (slotTfMs % oneMinute !== 0) {
    throw new Error(`Cannot aggregate 1m into ${slotTfMs}`);
  }

  const fromIdx = Math.floor((slotTs - companion.startTs) / oneMinute);
  const count = Math.floor(slotTfMs / oneMinute);
  const p = path.join(outDir, MARKET.collector, MARKET.exchange, MARKET.symbol, "1m.bin");
  const fh = await fs.open(p, "r");
  const buf = Buffer.alloc(count * 56);
  await fh.read(buf, 0, buf.length, fromIdx * 56);
  await fh.close();

  const out: Candle = {
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
  let hasPrice = false;

  for (let i = 0; i < count; i += 1) {
    const candle = decodeCandle(buf, i * 56);
    const isGap = candle.open === 0 && candle.high === 0 && candle.low === 0 && candle.close === 0;
    if (!isGap) {
      if (!hasPrice) {
        out.open = candle.open;
        out.high = candle.high;
        out.low = candle.low;
        out.close = candle.close;
        hasPrice = true;
      } else {
        if (candle.high > out.high) out.high = candle.high;
        if (candle.low < out.low) out.low = candle.low;
        out.close = candle.close;
      }
    }
    out.buyVol += candle.buyVol;
    out.sellVol += candle.sellVol;
    out.buyCount += candle.buyCount;
    out.sellCount += candle.sellCount;
    out.liqBuy += candle.liqBuy;
    out.liqSell += candle.liqSell;
  }

  return out;
}

function zeroCandle(): Candle {
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

test("fixgaps patches 4h slot with overlapping market files, not just current file", async () => {
  const fixture = await createFixture();
  const db = openDatabase(fixture.dbPath);

  try {
    const rootId = insertIndexedFile(db, fixture.root, fixture.relativePathA);
    insertIndexedFile(db, fixture.root, fixture.relativePathB);

    await runProcess(buildConfig(fixture.root, fixture.outDir, fixture.dbPath, "1m"), db);
    await runProcess(buildConfig(fixture.root, fixture.outDir, fixture.dbPath, "4h"), db);
    db.db.exec("DELETE FROM gaps;");

    insertGapEvent(db, { rootId, relativePath: fixture.relativePathA });

    const before = await readCandleAtSlot(fixture.outDir, "4h", SLOT_4H);
    assert.strictEqual(before.close, 4_000_000);

    const stats = await runFixGaps(buildConfig(fixture.root, fixture.outDir, fixture.dbPath, "1m"), db, {
      adapterRegistry: createAdapterRegistry({
        KRAKEN: {
          name: "kraken-test",
          async recover() {
            return [{ ts: TRADE_MISSING, price: 100, size: 1, side: "buy", priceText: "100", sizeText: "1" }];
          },
        },
      }),
    });

    assert.strictEqual(stats.fixedEvents, 1);
    assert.strictEqual(stats.recoveredTrades, 1);
    assert.ok(stats.binariesPatched >= 2);

    const after4h = await readCandleAtSlot(fixture.outDir, "4h", SLOT_4H);
    const expectedFrom1m = await aggregate1mIntoSlot(fixture.outDir, SLOT_4H, 14_400_000);
    assert.deepStrictEqual(after4h, expectedFrom1m);
    assert.strictEqual(after4h.open, 1_500_000);
    assert.strictEqual(after4h.close, 4_000_000);

    const eventRow = db.db
      .prepare("SELECT gap_fix_status, gap_fix_recovered FROM gaps WHERE root_id = :rootId AND relative_path = :relativePath;")
      .get({ rootId, relativePath: fixture.relativePathA }) as { gap_fix_status: string | null; gap_fix_recovered: number | null };
    assert.strictEqual(eventRow.gap_fix_status, GapFixStatus.Fixed);
    assert.strictEqual(eventRow.gap_fix_recovered, 1);
  } finally {
    db.close();
  }
});
