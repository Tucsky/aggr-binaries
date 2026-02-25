import assert from "node:assert/strict";
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
    seedDb.exec("INSERT INTO roots(path) VALUES('/tmp/root');");
    seedDb.exec(`
      INSERT INTO files(root_id, relative_path, collector, exchange, symbol, start_ts, ext)
      VALUES(1, 'RAM/BITMEX/SOLUSD/2026-02-22-23.gz', 'RAM', 'BITMEX', 'SOLUSD', 1700000000000, 'gz');
    `);
  } finally {
    seedDb.close();
  }

  assert.throws(
    () => openDatabase(dbPath),
    /indexed_market_ranges is empty while files has data/,
  );
});
