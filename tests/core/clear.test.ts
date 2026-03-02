import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import type { Config } from "../../src/core/config.js";
import { openDatabase } from "../../src/core/db.js";
import { Collector } from "../../src/core/model.js";
import { runClear } from "../../src/core/clear.js";

test("runClear removes selected market state then reindexes only that market", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "aggr-clear-"));
  const outDir = path.join(root, "out");
  const dbPath = path.join(root, "index.sqlite");
  const marketDir = path.join(root, "PI", "2025", "BYBIT", "BTCUSDT");
  const otherMarketDir = path.join(root, "PI", "2025", "BYBIT", "ETHUSDT");
  await fs.mkdir(marketDir, { recursive: true });
  await fs.mkdir(otherMarketDir, { recursive: true });
  await fs.writeFile(path.join(marketDir, "2025-01-01-00.gz"), "btc");
  await fs.writeFile(path.join(otherMarketDir, "2025-01-01-00.gz"), "eth");
  await fs.mkdir(path.join(outDir, "PI", "BYBIT", "BTCUSDT"), { recursive: true });
  await fs.mkdir(path.join(outDir, "PI", "BYBIT", "ETHUSDT"), { recursive: true });
  await fs.writeFile(path.join(outDir, "PI", "BYBIT", "BTCUSDT", "1m.bin"), "btc-out");
  await fs.writeFile(path.join(outDir, "PI", "BYBIT", "ETHUSDT", "1m.bin"), "eth-out");

  const db = openDatabase(dbPath);
  const config: Config = {
    root,
    dbPath,
    batchSize: 1000,
    flushIntervalSeconds: 10,
    outDir,
    collector: "PI",
    exchange: "BYBIT",
    symbol: "BTCUSDT",
    timeframe: "1m",
    timeframeMs: 60_000,
  };

  try {
    const rootId = db.ensureRoot(root);
    db.insertFiles([
      {
        rootId,
        relativePath: "PI/2025/BYBIT/BTCUSDT/2025-01-01-00.gz",
        collector: Collector.PI,
        exchange: "BYBIT",
        symbol: "BTCUSDT",
        startTs: 1_735_689_600_000,
        ext: ".gz",
      },
      {
        rootId,
        relativePath: "PI/2025/BYBIT/ETHUSDT/2025-01-01-00.gz",
        collector: Collector.PI,
        exchange: "BYBIT",
        symbol: "ETHUSDT",
        startTs: 1_735_689_600_000,
        ext: ".gz",
      },
    ]);
    db.insertGaps(
      {
        rootId,
        collector: "PI",
        exchange: "BYBIT",
        symbol: "BTCUSDT",
      },
      [{
        gapMs: 90_000,
        gapMiss: 1,
        startTs: 1_735_689_600_000,
        endTs: 1_735_689_690_000,
        startRelativePath: "PI/2025/BYBIT/BTCUSDT/2025-01-01-00.gz",
        endRelativePath: "PI/2025/BYBIT/BTCUSDT/2025-01-01-00.gz",
      }],
    );
    db.insertGaps(
      {
        rootId,
        collector: "PI",
        exchange: "BYBIT",
        symbol: "ETHUSDT",
      },
      [{
        gapMs: 90_000,
        gapMiss: 1,
        startTs: 1_735_689_600_000,
        endTs: 1_735_689_690_000,
        startRelativePath: "PI/2025/BYBIT/ETHUSDT/2025-01-01-00.gz",
        endRelativePath: "PI/2025/BYBIT/ETHUSDT/2025-01-01-00.gz",
      }],
    );
    db.upsertRegistry({
      collector: "PI",
      exchange: "BYBIT",
      symbol: "BTCUSDT",
      timeframe: "1m",
      startTs: 1_735_689_600_000,
      endTs: 1_735_689_660_000,
    });
    db.upsertRegistry({
      collector: "PI",
      exchange: "BYBIT",
      symbol: "ETHUSDT",
      timeframe: "1m",
      startTs: 1_735_689_600_000,
      endTs: 1_735_689_660_000,
    });

    const stats = await runClear(config, db);
    assert.deepStrictEqual(stats, {
      outputsDeleted: 1,
      eventsDeleted: 1,
      filesDeleted: 0,
      registryDeleted: 1,
      seen: 1,
      inserted: 0,
      existing: 1,
      conflicts: 0,
      skipped: 0,
    });

    assert.strictEqual(countRows(db, "files", "BTCUSDT"), 1);
    assert.strictEqual(countRows(db, "gaps", "BTCUSDT"), 0);
    assert.strictEqual(countRows(db, "registry", "BTCUSDT"), 0);
    assert.strictEqual(countRows(db, "files", "ETHUSDT"), 1);
    assert.strictEqual(countRows(db, "gaps", "ETHUSDT"), 1);
    assert.strictEqual(countRows(db, "registry", "ETHUSDT"), 1);
    await assert.rejects(fs.stat(path.join(outDir, "PI", "BYBIT", "BTCUSDT")));
    await assert.doesNotReject(fs.stat(path.join(outDir, "PI", "BYBIT", "ETHUSDT", "1m.bin")));
  } finally {
    db.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

function countRows(db: ReturnType<typeof openDatabase>, table: "files" | "gaps" | "registry", symbol: string): number {
  const row = db.db
    .prepare(`SELECT COUNT(*) AS count FROM ${table} WHERE collector = 'PI' AND exchange = 'BYBIT' AND symbol = :symbol;`)
    .get({ symbol }) as { count?: number } | undefined;
  return Number(row?.count ?? 0);
}
