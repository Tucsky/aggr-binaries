import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import type { Config } from "../../src/core/config.js";
import { parseTimeframeMs } from "../../src/core/config.js";
import type { Db } from "../../src/core/db.js";
import { openDatabase } from "../../src/core/db.js";
import { GapFixStatus } from "../../src/core/events.js";
import { createAdapterRegistry, type TradeRecoveryAdapter } from "../../src/core/gaps/adapters/index.js";
import { runFixGaps } from "../../src/core/gaps/index.js";
import { classifyPath } from "../../src/core/normalize.js";
import { runProcess } from "../../src/core/process.js";

const DAY_MS = 86_400_000;
const MARKET = {
  collector: "RAM",
  bucket: "bucketA",
  exchange: "BITFINEX",
  symbol: "BTCUSD",
};

const TS0 = 1_704_067_200_000;
const TS1 = TS0 + 120_000;

interface Fixture {
  baseDir: string;
  root: string;
  outDir: string;
  dbPath: string;
  relativePath: string;
  fullPath: string;
}

async function createFixture(exchange = MARKET.exchange): Promise<Fixture> {
  const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "aggr-fixgaps-guard-"));
  const root = path.join(baseDir, "input");
  const outDir = path.join(baseDir, "out");
  const dbPath = path.join(baseDir, "index.sqlite");
  const dir = path.join(root, MARKET.collector, MARKET.bucket, exchange, MARKET.symbol);
  await fs.mkdir(dir, { recursive: true });

  const fileName = "2024-01-01-00";
  const fullPath = path.join(dir, fileName);
  const relativePath = path.posix.join(MARKET.collector, MARKET.bucket, exchange, MARKET.symbol, fileName);
  await fs.writeFile(fullPath, `${TS0} 100 1 1 0\n${TS1} 102 1 0 0\n`);

  return { baseDir, root, outDir, dbPath, relativePath, fullPath };
}

function buildConfig(root: string, outDir: string, dbPath: string): Config {
  const timeframe = "1m";
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

function insertIndexedFile(db: Db, root: string, relativePath: string): { rootId: number } {
  const rootId = db.ensureRoot(root);
  const row = classifyPath(rootId, relativePath);
  if (!row) throw new Error(`Failed to classify ${relativePath}`);
  db.insertFiles([row]);
  return { rootId };
}

function insertGapEvent(
  db: Db,
  payload: {
    rootId: number;
    relativePath: string;
    gapMs: number;
    gapEndTs?: number;
    startLine?: number;
    endLine?: number;
  },
): void {
  db.db
    .prepare(
      `INSERT INTO events
        (root_id, relative_path, collector, exchange, symbol, event_type, start_line, end_line, gap_ms, gap_miss, gap_end_ts, gap_fix_status)
       VALUES
        (:rootId, :relativePath, :collector, :exchange, :symbol, 'gap', :startLine, :endLine, :gapMs, 1, :gapEndTs, NULL);`,
    )
    .run({
      rootId: payload.rootId,
      relativePath: payload.relativePath,
      collector: MARKET.collector,
      exchange: MARKET.exchange,
      symbol: MARKET.symbol,
      startLine: payload.startLine ?? 9_999,
      endLine: payload.endLine ?? 9_999,
      gapMs: payload.gapMs,
      gapEndTs: payload.gapEndTs ?? TS1,
    });
}

function readSingleEvent(db: Db): { status: string | null; error: string | null; recovered: number | null } {
  return db.db
    .prepare("SELECT gap_fix_status AS status, gap_fix_error AS error, gap_fix_recovered AS recovered FROM events LIMIT 1;")
    .get() as { status: string | null; error: string | null; recovered: number | null };
}

function createCountingAdapter(apiOnly = false): { adapter: TradeRecoveryAdapter; getCalls: () => number } {
  let calls = 0;
  return {
    adapter: {
      name: apiOnly ? "counting-api-only" : "counting",
      apiOnly,
      async recover() {
        calls += 1;
        return [];
      },
    },
    getCalls: () => calls,
  };
}

test("fixgaps skips recovery for gaps over 60 days", async () => {
  const fixture = await createFixture();
  const db = openDatabase(fixture.dbPath);
  try {
    const { rootId } = insertIndexedFile(db, fixture.root, fixture.relativePath);
    insertGapEvent(db, {
      rootId,
      relativePath: fixture.relativePath,
      gapMs: (60 * DAY_MS) + 1,
      gapEndTs: TS1,
    });

    const counting = createCountingAdapter();
    const stats = await runFixGaps(buildConfig(fixture.root, fixture.outDir, fixture.dbPath), db, {
      adapterRegistry: createAdapterRegistry({
        BITFINEX: counting.adapter,
      }),
    });

    assert.strictEqual(counting.getCalls(), 0);
    assert.strictEqual(stats.selectedEvents, 1);
    assert.strictEqual(stats.fixedEvents, 0);
    assert.strictEqual(stats.adapterError, 0);
    const event = readSingleEvent(db);
    assert.strictEqual(event.status, GapFixStatus.SkippedLargeGap);
    assert.strictEqual(event.error, null);
    assert.strictEqual(event.recovered, 0);
  } finally {
    db.close();
  }
});

test("fixgaps processes api-only gaps over 7 days instead of skipping them", async () => {
  const fixture = await createFixture();
  const db = openDatabase(fixture.dbPath);
  try {
    const { rootId } = insertIndexedFile(db, fixture.root, fixture.relativePath);
    insertGapEvent(db, {
      rootId,
      relativePath: fixture.relativePath,
      gapMs: (7 * DAY_MS) + 1,
      gapEndTs: TS1,
    });

    const counting = createCountingAdapter(true);
    const stats = await runFixGaps(buildConfig(fixture.root, fixture.outDir, fixture.dbPath), db, {
      adapterRegistry: createAdapterRegistry({
        BITFINEX: counting.adapter,
      }),
    });

    assert.ok(counting.getCalls() > 0);
    assert.strictEqual(stats.selectedEvents, 1);
    assert.strictEqual(stats.fixedEvents, 1);
    assert.strictEqual(stats.adapterError, 0);
    const event = readSingleEvent(db);
    assert.strictEqual(event.status, GapFixStatus.Fixed);
    assert.strictEqual(event.error, null);
    assert.strictEqual(event.recovered, 0);
  } finally {
    db.close();
  }
});

test("fixgaps uses the batch last trade timestamp to resolve merge target file path", async () => {
  const fixture = await createFixture();
  const db = openDatabase(fixture.dbPath);
  const tradeA = TS0 + DAY_MS - 30_000;
  const tradeB = TS0 + DAY_MS + 30_000;
  const tailTs = TS0 + DAY_MS + 120_000;
  try {
    await fs.writeFile(fixture.fullPath, `${TS0} 100 1 1 0\n${tailTs} 102 1 0 0\n`);
    const { rootId } = insertIndexedFile(db, fixture.root, fixture.relativePath);
    await runProcess(buildConfig(fixture.root, fixture.outDir, fixture.dbPath), db);
    db.db.exec("DELETE FROM events;");
    insertGapEvent(db, {
      rootId,
      relativePath: fixture.relativePath,
      gapMs: 2 * DAY_MS,
      gapEndTs: tailTs,
    });

    const stats = await runFixGaps(buildConfig(fixture.root, fixture.outDir, fixture.dbPath), db, {
      adapterRegistry: createAdapterRegistry({
        BITFINEX: {
          name: "batch-last-trade-path",
          async recover() {
            return [
              { ts: tradeA, price: 98, size: 0.25, side: "buy", priceText: "98", sizeText: "0.25" },
              { ts: tradeB, price: 99, size: 0.5, side: "sell", priceText: "99", sizeText: "0.5" },
            ];
          },
        },
      }),
    });

    const targetRelative = path.posix.join(
      MARKET.collector,
      MARKET.bucket,
      MARKET.exchange,
      MARKET.symbol,
      "2024-01-02-00",
    );
    const targetFullPath = path.join(fixture.root, targetRelative);
    const targetRaw = await fs.readFile(targetFullPath, "utf8");
    assert.match(targetRaw, new RegExp(`^${tradeA} 98 0\\.25 1`, "m"));
    assert.match(targetRaw, new RegExp(`^${tradeB} 99 0\\.5 0`, "m"));

    const indexed = db.db
      .prepare("SELECT relative_path FROM files WHERE root_id = :rootId AND relative_path = :relativePath;")
      .get({ rootId, relativePath: targetRelative }) as { relative_path: string } | undefined;
    assert.strictEqual(indexed?.relative_path, targetRelative);

    assert.strictEqual(stats.selectedEvents, 1);
    assert.strictEqual(stats.fixedEvents, 1);
    assert.strictEqual(stats.recoveredTrades, 2);
    assert.strictEqual(stats.adapterError, 0);
    const event = readSingleEvent(db);
    assert.strictEqual(event.status, GapFixStatus.Fixed);
    assert.strictEqual(event.recovered, 2);
  } finally {
    db.close();
  }
});

test("fixgaps consumes adapter streamed batches without relying on a monolithic return array", async () => {
  const fixture = await createFixture();
  const db = openDatabase(fixture.dbPath);
  const recoveredA = TS0 + 30_000;
  const recoveredB = TS0 + 90_000;
  try {
    const { rootId } = insertIndexedFile(db, fixture.root, fixture.relativePath);
    await runProcess(buildConfig(fixture.root, fixture.outDir, fixture.dbPath), db);
    db.db.exec("DELETE FROM events;");
    insertGapEvent(db, {
      rootId,
      relativePath: fixture.relativePath,
      gapMs: TS1 - TS0,
      gapEndTs: TS1,
    });

    const stats = await runFixGaps(buildConfig(fixture.root, fixture.outDir, fixture.dbPath), db, {
      adapterRegistry: createAdapterRegistry({
        BITFINEX: {
          name: "streaming-batches",
          async recover(req) {
            if (req.onRecoveredBatch) {
              await req.onRecoveredBatch([
                { ts: recoveredA, price: 101, size: 0.3, side: "buy", priceText: "101", sizeText: "0.3" },
              ]);
              await req.onRecoveredBatch([
                { ts: recoveredB, price: 101.5, size: 0.2, side: "sell", priceText: "101.5", sizeText: "0.2" },
              ]);
            }
            return [];
          },
        },
      }),
    });

    const raw = await fs.readFile(fixture.fullPath, "utf8");
    assert.match(raw, new RegExp(`^${recoveredA} 101 0\\.3 1`, "m"));
    assert.match(raw, new RegExp(`^${recoveredB} 101\\.5 0\\.2 0`, "m"));

    assert.strictEqual(stats.selectedEvents, 1);
    assert.strictEqual(stats.fixedEvents, 1);
    assert.strictEqual(stats.recoveredTrades, 2);
    assert.strictEqual(stats.adapterError, 0);
    const event = readSingleEvent(db);
    assert.strictEqual(event.status, GapFixStatus.Fixed);
    assert.strictEqual(event.recovered, 2);
  } finally {
    db.close();
  }
});
