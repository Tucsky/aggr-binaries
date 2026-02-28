import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import type { Config } from "../../src/core/config.js";
import { openDatabase } from "../../src/core/db.js";
import { runIndex } from "../../src/core/indexer.js";

const FIXTURE_MARKET = {
  collector: "RAM",
  bucket: "bucketA",
  exchange: "BINANCE",
  symbol: "BTCUSDT",
};

async function createIndexFixture(prefix: string): Promise<{ baseDir: string; root: string; dbPath: string; marketDir: string }> {
  const baseDir = await fs.mkdtemp(path.join(os.tmpdir(), prefix));
  const root = path.join(baseDir, "input");
  const dbPath = path.join(baseDir, "index.sqlite");
  const marketDir = path.join(
    root,
    FIXTURE_MARKET.collector,
    FIXTURE_MARKET.bucket,
    FIXTURE_MARKET.exchange,
    FIXTURE_MARKET.symbol,
  );
  await fs.mkdir(marketDir, { recursive: true });
  return { baseDir, root, dbPath, marketDir };
}

function buildConfig(root: string, dbPath: string, force = false): Config {
  return {
    root,
    dbPath,
    batchSize: 8,
    flushIntervalSeconds: 5,
    outDir: path.join(root, "out"),
    force,
    timeframe: "1m",
    timeframeMs: 60_000,
  };
}

async function writeTradeFile(marketDir: string, fileName: string): Promise<void> {
  await fs.writeFile(path.join(marketDir, fileName), "1700000000000 100 1 1 0\n");
}

async function bumpDirectoryMtime(dirPath: string): Promise<void> {
  const bump = new Date(Date.now() + 5_000);
  await fs.utimes(dirPath, bump, bump);
}

function countIndexedFiles(db: ReturnType<typeof openDatabase>): number {
  const row = db.db.prepare("SELECT COUNT(*) AS count FROM files;").get() as { count?: number } | undefined;
  return Number(row?.count ?? 0);
}

test("runIndex incrementally admits only high-watermark and newer files", async () => {
  const fixture = await createIndexFixture("aggr-indexer-incremental-");
  const db = openDatabase(fixture.dbPath);
  const config = buildConfig(fixture.root, fixture.dbPath);

  try {
    await writeTradeFile(fixture.marketDir, "2026-01-01-00.gz");
    await writeTradeFile(fixture.marketDir, "2026-01-01-01.gz");

    const first = await runIndex(config, db);
    assert.deepStrictEqual(first, {
      seen: 2,
      inserted: 2,
      existing: 0,
      conflicts: 0,
      skipped: 0,
    });

    const unchanged = await runIndex(config, db);
    assert.deepStrictEqual(unchanged, {
      seen: 0,
      inserted: 0,
      existing: 0,
      conflicts: 0,
      skipped: 0,
    });

    await writeTradeFile(fixture.marketDir, "2026-01-01-02.gz");
    await bumpDirectoryMtime(fixture.marketDir);
    const changed = await runIndex(config, db);
    assert.deepStrictEqual(changed, {
      seen: 3,
      inserted: 1,
      existing: 1,
      conflicts: 0,
      skipped: 0,
    });
    assert.strictEqual(countIndexedFiles(db), 3);
  } finally {
    db.close();
    await fs.rm(fixture.baseDir, { recursive: true, force: true });
  }
});

test("runIndex --force performs full reconciliation and captures historical backfills", async () => {
  const fixture = await createIndexFixture("aggr-indexer-force-");
  const db = openDatabase(fixture.dbPath);
  const incrementalConfig = buildConfig(fixture.root, fixture.dbPath);
  const forceConfig = buildConfig(fixture.root, fixture.dbPath, true);

  try {
    await writeTradeFile(fixture.marketDir, "2026-01-01-00.gz");
    await writeTradeFile(fixture.marketDir, "2026-01-01-01.gz");
    await runIndex(incrementalConfig, db);

    await writeTradeFile(fixture.marketDir, "2025-12-31-23.gz");
    await bumpDirectoryMtime(fixture.marketDir);
    const incremental = await runIndex(incrementalConfig, db);
    assert.deepStrictEqual(incremental, {
      seen: 3,
      inserted: 0,
      existing: 1,
      conflicts: 0,
      skipped: 0,
    });
    assert.strictEqual(countIndexedFiles(db), 2);

    const forced = await runIndex(forceConfig, db);
    assert.deepStrictEqual(forced, {
      seen: 3,
      inserted: 1,
      existing: 2,
      conflicts: 0,
      skipped: 0,
    });
    assert.strictEqual(countIndexedFiles(db), 3);
  } finally {
    db.close();
    await fs.rm(fixture.baseDir, { recursive: true, force: true });
  }
});

test("runIndex incremental discovers symbol directories without DB baseline", async () => {
  const fixture = await createIndexFixture("aggr-indexer-unknown-dir-");
  const db = openDatabase(fixture.dbPath);
  const config = buildConfig(fixture.root, fixture.dbPath);
  const unknownDir = path.join(
    fixture.root,
    FIXTURE_MARKET.collector,
    FIXTURE_MARKET.bucket,
    FIXTURE_MARKET.exchange,
    "NEWMARKET",
  );

  try {
    await writeTradeFile(fixture.marketDir, "2026-01-01-00.gz");
    const first = await runIndex(config, db);
    assert.deepStrictEqual(first, {
      seen: 1,
      inserted: 1,
      existing: 0,
      conflicts: 0,
      skipped: 0,
    });

    await fs.mkdir(unknownDir, { recursive: true });
    await writeTradeFile(unknownDir, "2026-01-01-00.gz");
    await bumpDirectoryMtime(path.dirname(unknownDir));
    await bumpDirectoryMtime(unknownDir);

    const incremental = await runIndex(config, db);
    assert.strictEqual(incremental.inserted, 1);
    assert.strictEqual(incremental.existing, 0);
    assert.strictEqual(incremental.conflicts, 0);
    assert.strictEqual(incremental.skipped, 0);
    assert.strictEqual(countIndexedFiles(db), 2);
  } finally {
    db.close();
    await fs.rm(fixture.baseDir, { recursive: true, force: true });
  }
});
