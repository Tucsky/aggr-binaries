#!/usr/bin/env node
import { DatabaseSync } from "node:sqlite";
import path from "node:path";

const DEFAULT_DB_PATH = "index.sqlite";

function parseArgs(argv) {
  let dbPath = DEFAULT_DB_PATH;
  for (let i = 2; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--db") {
      const next = argv[i + 1];
      if (!next) {
        throw new Error("Missing value for --db");
      }
      dbPath = next;
      i += 1;
      continue;
    }
    if (arg === "--help" || arg === "-h") {
      printHelp();
      process.exit(0);
    }
    throw new Error(`Unknown argument: ${arg}`);
  }
  return { dbPath: path.resolve(dbPath) };
}

function printHelp() {
  console.log("Usage: node scripts/migrate_remove_root_id.mjs [--db <path>]");
  console.log("");
  console.log("One-time migration:");
  console.log("- removes roots table");
  console.log("- removes root_id columns from files/gaps");
  console.log("- preserves existing rows and ids");
}

function tableExists(db, tableName) {
  const row = db
    .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = :name;")
    .get({ name: tableName });
  return Boolean(row);
}

function tableHasColumn(db, tableName, columnName) {
  if (!tableExists(db, tableName)) return false;
  const rows = db.prepare(`PRAGMA table_info(${tableName});`).all();
  for (const row of rows) {
    if (row.name === columnName) return true;
  }
  return false;
}

function assertNoDuplicateRelativePaths(db) {
  const duplicate = db
    .prepare(
      `SELECT relative_path, COUNT(*) AS duplicate_count
         FROM files
        GROUP BY relative_path
       HAVING COUNT(*) > 1
        LIMIT 1;`,
    )
    .get();
  if (!duplicate) return;
  throw new Error(
    `Cannot migrate: duplicate relative_path '${duplicate.relative_path}' appears ${duplicate.duplicate_count} times in files.`,
  );
}

function createModernTables(db) {
  db.exec(`
    CREATE TABLE files_next (
      relative_path TEXT PRIMARY KEY,
      collector TEXT NOT NULL,
      exchange TEXT NOT NULL,
      symbol TEXT NOT NULL,
      start_ts INTEGER NOT NULL,
      ext TEXT,
      created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000)
    );
  `);
  db.exec(`
    CREATE TABLE gaps_next (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
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
  `);
}

function copyData(db) {
  db.exec(`
    INSERT INTO files_next(relative_path, collector, exchange, symbol, start_ts, ext, created_at)
    SELECT relative_path, collector, exchange, symbol, start_ts, ext, COALESCE(created_at, (unixepoch('subsec') * 1000))
      FROM files;
  `);
  db.exec(`
    INSERT INTO gaps_next(
      id,
      start_relative_path,
      end_relative_path,
      collector,
      exchange,
      symbol,
      gap_ms,
      gap_miss,
      start_ts,
      end_ts,
      gap_score,
      gap_fix_status,
      gap_fix_error,
      gap_fix_recovered,
      gap_fix_updated_at,
      created_at
    )
    SELECT
      id,
      start_relative_path,
      end_relative_path,
      collector,
      exchange,
      symbol,
      gap_ms,
      gap_miss,
      start_ts,
      end_ts,
      gap_score,
      gap_fix_status,
      gap_fix_error,
      gap_fix_recovered,
      gap_fix_updated_at,
      COALESCE(created_at, (unixepoch('subsec') * 1000))
    FROM gaps;
  `);
}

function installModernTables(db) {
  db.exec("ALTER TABLE files RENAME TO files_legacy_root;");
  db.exec("ALTER TABLE gaps RENAME TO gaps_legacy_root;");
  db.exec("ALTER TABLE files_next RENAME TO files;");
  db.exec("ALTER TABLE gaps_next RENAME TO gaps;");
  db.exec("DROP TABLE files_legacy_root;");
  db.exec("DROP TABLE gaps_legacy_root;");
  if (tableExists(db, "roots")) {
    db.exec("DROP TABLE roots;");
  }
}

function createModernIndexes(db) {
  db.exec("CREATE INDEX IF NOT EXISTS idx_files_exchange_symbol ON files(exchange, symbol);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_files_start_ts ON files(start_ts);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_files_collector ON files(collector);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_registry_exchange_symbol ON registry(exchange, symbol);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_gaps_end_file ON gaps(end_relative_path);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_gaps_market ON gaps(collector, exchange, symbol);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_gaps_market_end_ts ON gaps(collector, exchange, symbol, end_ts, id);");
  db.exec(
    "CREATE INDEX IF NOT EXISTS idx_gaps_fix_queue ON gaps(gap_fix_status, collector, exchange, symbol, end_relative_path, id);",
  );
  db.exec("CREATE INDEX IF NOT EXISTS idx_indexed_market_ranges_exchange_symbol ON indexed_market_ranges(exchange, symbol);");
}

function syncGapSequence(db) {
  if (!tableExists(db, "sqlite_sequence")) return;
  db.exec("DELETE FROM sqlite_sequence WHERE name = 'gaps';");
  db.exec("INSERT INTO sqlite_sequence(name, seq) SELECT 'gaps', COALESCE(MAX(id), 0) FROM gaps;");
}

function main() {
  const { dbPath } = parseArgs(process.argv);
  const db = new DatabaseSync(dbPath);
  try {
    const filesHasRootId = tableHasColumn(db, "files", "root_id");
    const gapsHasRootId = tableHasColumn(db, "gaps", "root_id");
    const hasRootsTable = tableExists(db, "roots");

    if (!filesHasRootId && !gapsHasRootId && !hasRootsTable) {
      console.log(`[migrate_remove_root_id] already migrated: ${dbPath}`);
      return;
    }

    if (!filesHasRootId || !gapsHasRootId || !hasRootsTable) {
      throw new Error(
        "Unexpected partial root_id schema. Expected files.root_id + gaps.root_id + roots table together.",
      );
    }

    assertNoDuplicateRelativePaths(db);

    db.exec("BEGIN IMMEDIATE;");
    try {
      createModernTables(db);
      copyData(db);
      installModernTables(db);
      createModernIndexes(db);
      syncGapSequence(db);
      db.exec("COMMIT;");
    } catch (err) {
      db.exec("ROLLBACK;");
      throw err;
    }

    if (tableHasColumn(db, "files", "root_id") || tableHasColumn(db, "gaps", "root_id") || tableExists(db, "roots")) {
      throw new Error("Migration verification failed: root_id or roots table still present.");
    }

    console.log(`[migrate_remove_root_id] migration complete: ${dbPath}`);
  } finally {
    db.close();
  }
}

main();
