import { DatabaseSync, StatementSync } from "node:sqlite";
import type { IndexedFile } from "./model.js";

export interface Db {
  db: DatabaseSync;
  ensureRoot(path: string): number;
  insertFiles(rows: IndexedFile[]): { inserted: number; existing: number };
  close(): void;
}

export function openDatabase(dbPath: string): Db {
  const db = new DatabaseSync(dbPath);
  db.exec("PRAGMA journal_mode=WAL;");
  db.exec("PRAGMA synchronous=NORMAL;");
  db.exec("PRAGMA temp_store=MEMORY;");
  db.exec("PRAGMA wal_autocheckpoint=10000;");

  migrate(db);

  const insertStmt = db.prepare(
    `INSERT OR IGNORE INTO files
      (root_id, relative_path, collector, era, exchange, symbol, start_ts, ext)
     VALUES
      (:rootId, :relativePath, :collector, :era, :exchange, :symbol, :startTs, :ext);`,
  );

  const ensureRootStmt = db.prepare("INSERT OR IGNORE INTO roots(path) VALUES(:path);");
  const getRootIdStmt = db.prepare("SELECT id FROM roots WHERE path = :path;");

  const api: Db = {
    db,
    ensureRoot: (path: string): number => {
      ensureRootStmt.run({ path });
      const row = getRootIdStmt.get({ path }) as { id: number } | undefined;
      if (!row) {
        throw new Error(`Failed to resolve root id for ${path}`);
      }
      return row.id;
    },
    insertFiles: (rows: IndexedFile[]) => insertMany(db, insertStmt, rows),
    close: () => db.close(),
  };

  return api;
}

function migrate(db: DatabaseSync): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS roots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      path TEXT NOT NULL UNIQUE
    );
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS files (
      root_id INTEGER NOT NULL REFERENCES roots(id) ON DELETE CASCADE,
      relative_path TEXT NOT NULL,
      collector TEXT NOT NULL,
      era TEXT NOT NULL,
      exchange TEXT,
      symbol TEXT,
      start_ts INTEGER,
      ext TEXT,
      created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000),
      PRIMARY KEY (root_id, relative_path)
    );
  `);

  db.exec("CREATE INDEX IF NOT EXISTS idx_files_exchange_symbol ON files(exchange, symbol);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_files_start_ts ON files(start_ts);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_files_collector ON files(collector);");
}

function insertMany(
  db: DatabaseSync,
  stmt: StatementSync,
  rows: IndexedFile[],
): { inserted: number; existing: number } {
  let inserted = 0;
  let existing = 0;
  db.exec("BEGIN");
  try {
    for (const row of rows) {
      const res = stmt.run({
        rootId: row.rootId,
        relativePath: row.relativePath,
        collector: row.collector,
        era: row.era,
        exchange: row.exchange ?? null,
        symbol: row.symbol ?? null,
        startTs: row.startTs ?? null,
        ext: row.ext ?? null,
      });
      const delta = Number(res.changes ?? 0);
      inserted += delta;
      existing += delta === 0 ? 1 : 0;
    }
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
  return { inserted, existing };
}
