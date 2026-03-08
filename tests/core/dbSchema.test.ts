import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { openDatabase } from "../../src/core/db.js";

async function createTempDbPath(prefix: string): Promise<string> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), prefix));
  return path.join(dir, "index.sqlite");
}

test("openDatabase rejects legacy registry metadata schema", async () => {
  const dbPath = await createTempDbPath("aggr-db-schema-legacy-");
  const seedDb = new DatabaseSync(dbPath);
  try {
    seedDb.exec(`
      CREATE TABLE registry (
        collector TEXT NOT NULL,
        exchange TEXT NOT NULL,
        symbol TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        metadata TEXT NOT NULL,
        created_at INTEGER,
        updated_at INTEGER,
        PRIMARY KEY (collector, exchange, symbol, timeframe)
      );
    `);
  } finally {
    seedDb.close();
  }

  assert.throws(
    () => openDatabase(dbPath),
    /Incompatible schema for table 'registry'/,
  );
});

test("openDatabase rejects files data when indexed ranges are missing", async () => {
  const dbPath = await createTempDbPath("aggr-db-schema-ranges-");
  const db = openDatabase(dbPath);
  db.close();

  const seedDb = new DatabaseSync(dbPath);
  try {
    seedDb.exec(`
      INSERT INTO files(relative_path, collector, exchange, symbol, start_ts, ext)
      VALUES('RAM/BITMEX/SOLUSD/2026-02-22-23.gz', 'RAM', 'BITMEX', 'SOLUSD', 1700000000000, 'gz');
    `);
  } finally {
    seedDb.close();
  }

  assert.throws(
    () => openDatabase(dbPath),
    /indexed_market_ranges is empty while files has data/,
  );
});

test("root_id migration rewrites legacy files and gaps schema", async () => {
  const dbPath = await createTempDbPath("aggr-db-schema-migrate-root-id-");
  const seedDb = new DatabaseSync(dbPath);
  try {
    seedDb.exec(`
      CREATE TABLE roots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        path TEXT NOT NULL UNIQUE
      );
      CREATE TABLE files (
        root_id INTEGER NOT NULL REFERENCES roots(id) ON DELETE CASCADE,
        relative_path TEXT NOT NULL,
        collector TEXT NOT NULL,
        exchange TEXT NOT NULL,
        symbol TEXT NOT NULL,
        start_ts INTEGER NOT NULL,
        ext TEXT,
        created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000),
        PRIMARY KEY (root_id, relative_path)
      );
      CREATE TABLE gaps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        root_id INTEGER NOT NULL REFERENCES roots(id) ON DELETE CASCADE,
        start_relative_path TEXT NOT NULL,
        end_relative_path TEXT NOT NULL,
        collector TEXT NOT NULL,
        exchange TEXT NOT NULL,
        symbol TEXT NOT NULL,
        gap_ms INTEGER,
        gap_miss INTEGER,
        start_ts INTEGER NOT NULL,
        end_ts INTEGER NOT NULL,
        gap_score FLOAT,
        gap_fix_status TEXT,
        gap_fix_error TEXT,
        gap_fix_recovered INTEGER,
        gap_fix_updated_at INTEGER,
        created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000)
      );
      CREATE TABLE registry (
        collector TEXT NOT NULL,
        exchange TEXT NOT NULL,
        symbol TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        start_ts INTEGER NOT NULL,
        end_ts INTEGER NOT NULL,
        created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000),
        updated_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000),
        PRIMARY KEY (collector, exchange, symbol, timeframe)
      );
      CREATE TABLE indexed_market_ranges (
        collector TEXT NOT NULL,
        exchange TEXT NOT NULL,
        symbol TEXT NOT NULL,
        start_ts INTEGER NOT NULL,
        end_ts INTEGER NOT NULL,
        created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000),
        updated_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000),
        PRIMARY KEY (collector, exchange, symbol)
      );
      CREATE INDEX idx_registry_exchange_symbol ON registry(exchange, symbol);
      CREATE INDEX idx_files_exchange_symbol ON files(exchange, symbol);
      CREATE INDEX idx_files_start_ts ON files(start_ts);
      CREATE INDEX idx_files_collector ON files(collector);
      CREATE INDEX idx_gaps_end_file ON gaps(root_id, end_relative_path);
      CREATE INDEX idx_gaps_market ON gaps(collector, exchange, symbol);
      CREATE INDEX idx_gaps_market_end_ts ON gaps(collector, exchange, symbol, end_ts, id);
      CREATE INDEX idx_gaps_fix_queue ON gaps(gap_fix_status, collector, exchange, symbol, root_id, end_relative_path, id);
      CREATE INDEX idx_indexed_market_ranges_exchange_symbol ON indexed_market_ranges(exchange, symbol);
    `);
    seedDb.exec("INSERT INTO roots(path) VALUES('/tmp/source');");
    seedDb.exec(`
      INSERT INTO files(root_id, relative_path, collector, exchange, symbol, start_ts, ext)
      VALUES(1, 'RAM/BITMEX/SOLUSD/2026-02-22-23.gz', 'RAM', 'BITMEX', 'SOLUSD', 1700000000000, '.gz');
    `);
    seedDb.exec(`
      INSERT INTO gaps(root_id, start_relative_path, end_relative_path, collector, exchange, symbol, gap_ms, gap_miss, start_ts, end_ts, gap_score)
      VALUES(1, 'RAM/BITMEX/SOLUSD/2026-02-22-23.gz', 'RAM/BITMEX/SOLUSD/2026-02-22-23.gz', 'RAM', 'BITMEX', 'SOLUSD', 60000, 1, 1700000000000, 1700000060000, NULL);
    `);
    seedDb.exec(`
      INSERT INTO indexed_market_ranges(collector, exchange, symbol, start_ts, end_ts)
      VALUES('RAM', 'BITMEX', 'SOLUSD', 1700000000000, 1700000000000);
    `);
  } finally {
    seedDb.close();
  }

  const migrationScriptPath = path.resolve("scripts/migrate_remove_root_id.mjs");
  const migration = spawnSync(process.execPath, [migrationScriptPath, "--db", dbPath], {
    encoding: "utf8",
  });
  assert.strictEqual(
    migration.status,
    0,
    `Migration script failed.\nstdout:\n${migration.stdout}\nstderr:\n${migration.stderr}`,
  );

  const db = openDatabase(dbPath);
  try {
    const filesColumns = db.db.prepare("PRAGMA table_info(files);").all() as Array<{ name: string }>;
    assert.ok(!filesColumns.some((column) => column.name === "root_id"));
    const gapsColumns = db.db.prepare("PRAGMA table_info(gaps);").all() as Array<{ name: string }>;
    assert.ok(!gapsColumns.some((column) => column.name === "root_id"));
    const rootsTable = db.db
      .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'roots';")
      .get() as { name?: string } | undefined;
    assert.strictEqual(rootsTable, undefined);

    const filesRow = db.db.prepare("SELECT COUNT(*) AS cnt FROM files;").get() as { cnt: number };
    assert.strictEqual(filesRow.cnt, 1);
    const gapsRow = db.db.prepare("SELECT COUNT(*) AS cnt FROM gaps;").get() as { cnt: number };
    assert.strictEqual(gapsRow.cnt, 1);
  } finally {
    db.close();
  }
});
