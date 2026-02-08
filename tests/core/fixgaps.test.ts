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
import { createAdapterRegistry, type TradeRecoveryAdapter, type RecoveredTrade } from "../../src/core/gaps/adapters/index.js";
import { runFixGaps } from "../../src/core/gaps/index.js";
import { classifyPath } from "../../src/core/normalize.js";
import { runProcess } from "../../src/core/process.js";

const MARKET = {
  collector: "RAM",
  bucket: "bucketA",
  exchange: "BITFINEX",
  symbol: "BTCUSD",
};

const TS0 = 1_704_067_200_000;
const TS1 = TS0 + 60_000;
const TS2 = TS0 + 120_000;

interface Fixture {
  baseDir: string;
  root: string;
  outDir: string;
  dbPath: string;
  relativePath: string;
  fullPath: string;
}

async function createFixture(exchange = MARKET.exchange): Promise<Fixture> {
  const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "aggr-fixgaps-"));
  const root = path.join(baseDir, "input");
  const outDir = path.join(baseDir, "out");
  const dbPath = path.join(baseDir, "index.sqlite");
  const dir = path.join(root, MARKET.collector, MARKET.bucket, exchange, MARKET.symbol);
  await fs.mkdir(dir, { recursive: true });

  const fileName = "2024-01-01-00";
  const fullPath = path.join(dir, fileName);
  const relativePath = path.posix.join(MARKET.collector, MARKET.bucket, exchange, MARKET.symbol, fileName);

  const lines = [
    `${TS0} 100 1 1 0`,
    `${TS2} 102 1 0 0`,
  ];
  await fs.writeFile(fullPath, lines.join("\n"));

  return { baseDir, root, outDir, dbPath, relativePath, fullPath };
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

function insertIndexedFile(db: Db, root: string, relativePath: string): { rootId: number } {
  const rootId = db.ensureRoot(root);
  const row = classifyPath(rootId, relativePath);
  if (!row) {
    throw new Error(`Failed to classify ${relativePath}`);
  }
  db.insertFiles([row]);
  return { rootId };
}

function insertGapEvent(
  db: Db,
  payload: {
    rootId: number;
    relativePath: string;
    exchange?: string;
    startLine?: number;
    endLine?: number;
    gapMs?: number;
    gapEndTs?: number;
    status?: string | null;
  },
): number {
  const result = db.db
    .prepare(
      `INSERT INTO events
        (root_id, relative_path, collector, exchange, symbol, event_type, start_line, end_line, gap_ms, gap_miss, gap_end_ts, gap_fix_status)
       VALUES
        (:rootId, :relativePath, :collector, :exchange, :symbol, 'gap', :startLine, :endLine, :gapMs, :gapMiss, :gapEndTs, :status);`,
    )
    .run({
      rootId: payload.rootId,
      relativePath: payload.relativePath,
      collector: MARKET.collector,
      exchange: payload.exchange ?? MARKET.exchange,
      symbol: MARKET.symbol,
      startLine: payload.startLine ?? 2,
      endLine: payload.endLine ?? 2,
      gapMs: payload.gapMs ?? TS2 - TS0,
      gapMiss: 1,
      gapEndTs: payload.gapEndTs ?? TS2,
      status: payload.status ?? null,
    });
  return Number(result.lastInsertRowid);
}

function readEventRows(db: Db): Array<{ id: number; status: string | null; error: string | null }> {
  return db.db
    .prepare("SELECT id, gap_fix_status AS status, gap_fix_error AS error FROM events ORDER BY id;")
    .all() as Array<{ id: number; status: string | null; error: string | null }>;
}

function readBin(outDir: string, timeframe: string): Promise<Buffer> {
  const p = path.join(outDir, MARKET.collector, MARKET.exchange, MARKET.symbol, `${timeframe}.bin`);
  return fs.readFile(p);
}

function oneTradeAdapter(trade: RecoveredTrade): TradeRecoveryAdapter {
  return {
    name: "test-adapter",
    async recover() {
      return [trade];
    },
  };
}

test("fixgaps merges recovered trades, patches all timeframes, and is idempotent on second run", async () => {
  const fixture = await createFixture();
  const db = openDatabase(fixture.dbPath);

  try {
    const { rootId } = insertIndexedFile(db, fixture.root, fixture.relativePath);
    await runProcess(buildConfig(fixture.root, fixture.outDir, fixture.dbPath, "1m"), db);
    await runProcess(buildConfig(fixture.root, fixture.outDir, fixture.dbPath, "5m"), db);
    db.db.exec("DELETE FROM events;");

    insertGapEvent(db, { rootId, relativePath: fixture.relativePath });

    const before1m = await readBin(fixture.outDir, "1m");
    const before5m = await readBin(fixture.outDir, "5m");

    const adapterRegistry = createAdapterRegistry({
      BITFINEX: oneTradeAdapter({ ts: TS1, price: 101, size: 1, side: "buy", priceText: "101", sizeText: "1" }),
    });

    const stats = await runFixGaps(buildConfig(fixture.root, fixture.outDir, fixture.dbPath, "1m"), db, {
      adapterRegistry,
    });

    assert.strictEqual(stats.deletedEvents, 1);
    assert.strictEqual(stats.recoveredTrades, 1);
    assert.ok(stats.binariesPatched >= 2);

    const lines = (await fs.readFile(fixture.fullPath, "utf8")).trim().split("\n");
    assert.deepStrictEqual(lines, [`${TS0} 100 1 1 0`, `${TS1} 101 1 1 0`, `${TS2} 102 1 0 0`]);

    const after1m = await readBin(fixture.outDir, "1m");
    const after5m = await readBin(fixture.outDir, "5m");
    assert.notStrictEqual(Buffer.compare(before1m, after1m), 0);
    assert.notStrictEqual(Buffer.compare(before5m, after5m), 0);
    assert.strictEqual(readEventRows(db).length, 0);

    const second = await runFixGaps(buildConfig(fixture.root, fixture.outDir, fixture.dbPath, "1m"), db, {
      adapterRegistry,
    });
    assert.strictEqual(second.selectedEvents, 0);
    assert.strictEqual(Buffer.compare(after1m, await readBin(fixture.outDir, "1m")), 0);
    assert.strictEqual(Buffer.compare(after5m, await readBin(fixture.outDir, "5m")), 0);
  } finally {
    db.close();
  }
});

test("fixgaps deletes event when adapter succeeds with zero recovered trades", async () => {
  const fixture = await createFixture();
  const db = openDatabase(fixture.dbPath);

  try {
    const { rootId } = insertIndexedFile(db, fixture.root, fixture.relativePath);
    await runProcess(buildConfig(fixture.root, fixture.outDir, fixture.dbPath, "1m"), db);
    db.db.exec("DELETE FROM events;");
    insertGapEvent(db, { rootId, relativePath: fixture.relativePath });

    const beforeFile = await fs.readFile(fixture.fullPath, "utf8");
    const beforeBin = await readBin(fixture.outDir, "1m");

    const adapterRegistry = createAdapterRegistry({
      BITFINEX: {
        name: "empty",
        async recover() {
          return [];
        },
      },
    });

    const stats = await runFixGaps(buildConfig(fixture.root, fixture.outDir, fixture.dbPath, "1m"), db, {
      adapterRegistry,
    });

    assert.strictEqual(stats.deletedEvents, 1);
    assert.strictEqual(stats.recoveredTrades, 0);
    assert.strictEqual(stats.binariesPatched, 0);
    assert.strictEqual(await fs.readFile(fixture.fullPath, "utf8"), beforeFile);
    assert.strictEqual(Buffer.compare(await readBin(fixture.outDir, "1m"), beforeBin), 0);
    assert.strictEqual(readEventRows(db).length, 0);
  } finally {
    db.close();
  }
});

test("fixgaps marks unsupported exchanges as missing_adapter", async () => {
  const fixture = await createFixture("UNSUPPORTEDX");
  const db = openDatabase(fixture.dbPath);

  try {
    const { rootId } = insertIndexedFile(db, fixture.root, fixture.relativePath);
    insertGapEvent(db, {
      rootId,
      relativePath: fixture.relativePath,
      exchange: "UNSUPPORTEDX",
    });

    const stats = await runFixGaps(
      {
        ...buildConfig(fixture.root, fixture.outDir, fixture.dbPath, "1m"),
        exchange: "UNSUPPORTEDX",
      },
      db,
    );

    assert.strictEqual(stats.deletedEvents, 0);
    assert.strictEqual(stats.missingAdapter, 1);
    const rows = readEventRows(db);
    assert.strictEqual(rows.length, 1);
    assert.strictEqual(rows[0].status, GapFixStatus.MissingAdapter);
  } finally {
    db.close();
  }
});

test("fixgaps supports retry-status filtering and fallback windows for shifted lines", async () => {
  const fixture = await createFixture();
  const db = openDatabase(fixture.dbPath);

  try {
    const { rootId } = insertIndexedFile(db, fixture.root, fixture.relativePath);
    await runProcess(buildConfig(fixture.root, fixture.outDir, fixture.dbPath, "1m"), db);
    db.db.exec("DELETE FROM events;");

    insertGapEvent(db, {
      rootId,
      relativePath: fixture.relativePath,
      startLine: 99,
      endLine: 99,
      gapMs: TS2 - TS1,
      gapEndTs: TS2,
      status: GapFixStatus.AdapterError,
    });

    const defaultRun = await runFixGaps(buildConfig(fixture.root, fixture.outDir, fixture.dbPath, "1m"), db, {
      adapterRegistry: createAdapterRegistry({
        BITFINEX: {
          name: "unused",
          async recover() {
            return [];
          },
        },
      }),
    });
    assert.strictEqual(defaultRun.selectedEvents, 0);
    assert.strictEqual(readEventRows(db).length, 1);

    let seenWindows = 0;
    const retryRun = await runFixGaps(buildConfig(fixture.root, fixture.outDir, fixture.dbPath, "1m"), db, {
      retryStatuses: [GapFixStatus.AdapterError],
      adapterRegistry: createAdapterRegistry({
        BITFINEX: {
          name: "retry",
          async recover(req) {
            seenWindows = req.windows.length;
            assert.strictEqual(req.windows[0]?.fromTs, TS1);
            assert.strictEqual(req.windows[0]?.toTs, TS2);
            return [];
          },
        },
      }),
    });

    assert.strictEqual(seenWindows, 1);
    assert.strictEqual(retryRun.deletedEvents, 1);
    assert.strictEqual(readEventRows(db).length, 0);
  } finally {
    db.close();
  }
});

test("fixgaps uses fallback window on first run when line mapping cannot be resolved", async () => {
  const fixture = await createFixture();
  const db = openDatabase(fixture.dbPath);

  try {
    const { rootId } = insertIndexedFile(db, fixture.root, fixture.relativePath);
    await runProcess(buildConfig(fixture.root, fixture.outDir, fixture.dbPath, "1m"), db);
    db.db.exec("DELETE FROM events;");

    insertGapEvent(db, {
      rootId,
      relativePath: fixture.relativePath,
      startLine: 9_999,
      endLine: 9_999,
      gapMs: TS2 - TS1,
      gapEndTs: TS2,
      status: null,
    });

    let seenWindows = 0;
    await runFixGaps(buildConfig(fixture.root, fixture.outDir, fixture.dbPath, "1m"), db, {
      adapterRegistry: createAdapterRegistry({
        BITFINEX: {
          name: "fallback-first-run",
          async recover(req) {
            seenWindows = req.windows.length;
            assert.strictEqual(req.windows[0]?.fromTs, TS1);
            assert.strictEqual(req.windows[0]?.toTs, TS2);
            return [];
          },
        },
      }),
    });

    assert.strictEqual(seenWindows, 1);
    assert.strictEqual(readEventRows(db).length, 0);
  } finally {
    db.close();
  }
});

test("fixgaps marks adapter runtime failures and keeps events", async () => {
  const fixture = await createFixture();
  const db = openDatabase(fixture.dbPath);

  try {
    const { rootId } = insertIndexedFile(db, fixture.root, fixture.relativePath);
    insertGapEvent(db, { rootId, relativePath: fixture.relativePath });

    const stats = await runFixGaps(buildConfig(fixture.root, fixture.outDir, fixture.dbPath, "1m"), db, {
      adapterRegistry: createAdapterRegistry({
        BITFINEX: {
          name: "broken",
          async recover() {
            throw new Error("simulated adapter failure");
          },
        },
      }),
    });

    assert.strictEqual(stats.deletedEvents, 0);
    assert.strictEqual(stats.adapterError, 1);
    const rows = readEventRows(db);
    assert.strictEqual(rows.length, 1);
    assert.strictEqual(rows[0].status, GapFixStatus.AdapterError);
    assert.match(rows[0].error ?? "", /simulated adapter failure/);
  } finally {
    db.close();
  }
});
