import { DatabaseSync, StatementSync } from "node:sqlite";
import type { IndexedFile, RegistryEntry, RegistryFilter, RegistryKey } from "./model.js";

export interface Db {
  db: DatabaseSync;
  ensureRoot(path: string): number;
  insertFiles(rows: IndexedFile[]): { inserted: number; existing: number };
  upsertRegistry(entry: RegistryEntry): void;
  replaceRegistry(entries: RegistryEntry[], filter?: RegistryFilter): { upserted: number; deleted: number };
  getRegistryEntry(key: RegistryKey): RegistryEntry | null;
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
      (root_id, relative_path, collector, exchange, symbol, start_ts, ext)
     VALUES
      (:rootId, :relativePath, :collector, :exchange, :symbol, :startTs, :ext);`,
  );

  const ensureRootStmt = db.prepare("INSERT OR IGNORE INTO roots(path) VALUES(:path);");
  const getRootIdStmt = db.prepare("SELECT id FROM roots WHERE path = :path;");
  const upsertRegistryStmt = db.prepare(
    `INSERT INTO registry
      (collector, exchange, symbol, timeframe, start_ts, end_ts, sparse)
     VALUES
      (:collector, :exchange, :symbol, :timeframe, :startTs, :endTs, :sparse)
     ON CONFLICT(collector, exchange, symbol, timeframe) DO UPDATE SET
      start_ts = excluded.start_ts,
      end_ts = excluded.end_ts,
      sparse = excluded.sparse,
      updated_at = (unixepoch('subsec') * 1000);`,
  );
  const deleteRegistryStmt = db.prepare(
    `DELETE FROM registry
     WHERE (:collector IS NULL OR collector = :collector)
       AND (:exchange IS NULL OR exchange = :exchange)
       AND (:symbol IS NULL OR symbol = :symbol)
       AND (:timeframe IS NULL OR timeframe = :timeframe);`,
  );
  const getRegistryStmt = db.prepare(
    `SELECT * FROM registry
     WHERE collector = :collector AND exchange = :exchange AND symbol = :symbol AND timeframe = :timeframe;`,
  );

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
    upsertRegistry: (entry: RegistryEntry) => upsertRegistry(upsertRegistryStmt, entry),
    replaceRegistry: (entries: RegistryEntry[], filter?: RegistryFilter) =>
      replaceRegistry(db, upsertRegistryStmt, deleteRegistryStmt, entries, filter),
    getRegistryEntry: (key: RegistryKey) => getRegistry(getRegistryStmt, key),
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
      exchange TEXT NOT NULL,
      symbol TEXT NOT NULL,
      start_ts INTEGER NOT NULL,
      ext TEXT,
      created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000),
      PRIMARY KEY (root_id, relative_path)
    );
  `);

  db.exec("CREATE INDEX IF NOT EXISTS idx_files_exchange_symbol ON files(exchange, symbol);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_files_start_ts ON files(start_ts);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_files_collector ON files(collector);");

  migrateRegistry(db);

  const columns = db.prepare("PRAGMA table_info(files);").all() as Array<{ name: string; notnull: number }>;
  const missingNotNull = ["exchange", "symbol", "start_ts"].filter(
    (col) => !columns.some((c) => c.name === col && c.notnull === 1),
  );
  if (missingNotNull.length) {
    throw new Error(
      `files table missing NOT NULL constraints for: ${missingNotNull.join(
        ", ",
      )}. Please rebuild the index (drop index.sqlite and rerun index).`,
    );
  }

  const nullCountRow = db
    .prepare("SELECT COUNT(*) as cnt FROM files WHERE exchange IS NULL OR symbol IS NULL OR start_ts IS NULL;")
    .get() as { cnt?: number };
  if (nullCountRow?.cnt && nullCountRow.cnt > 0) {
    throw new Error(
      `files table contains ${nullCountRow.cnt} rows with NULL exchange/symbol/start_ts. Please rebuild the index.`,
    );
  }
}

function migrateRegistry(db: DatabaseSync): void {
  const info = db.prepare("PRAGMA table_info(registry);").all() as Array<{ name: string }>;
  if (!info.length) {
    createRegistry(db);
    return;
  }

  const hasMetadata = info.some((c) => c.name === "metadata");
  const hasStart = info.some((c) => c.name === "start_ts");
  const hasEnd = info.some((c) => c.name === "end_ts");
  const hasSparse = info.some((c) => c.name === "sparse");

  if (!hasMetadata && hasStart && hasEnd && hasSparse) {
    db.exec("CREATE INDEX IF NOT EXISTS idx_registry_exchange_symbol ON registry(exchange, symbol);");
    return;
  }

  if (hasMetadata) {
    migrateRegistryFromMetadata(db);
    return;
  }

  db.exec("DROP TABLE IF EXISTS registry;");
  createRegistry(db);
}

function createRegistry(db: DatabaseSync, tableName = "registry"): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS ${tableName} (
      collector TEXT NOT NULL,
      exchange TEXT NOT NULL,
      symbol TEXT NOT NULL,
      timeframe TEXT NOT NULL,
      sparse INTEGER NOT NULL DEFAULT 0,
      start_ts INTEGER NOT NULL,
      end_ts INTEGER NOT NULL,
      created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000),
      updated_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000),
      PRIMARY KEY (collector, exchange, symbol, timeframe)
    );
  `);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_registry_exchange_symbol ON ${tableName}(exchange, symbol);`);
}

function migrateRegistryFromMetadata(db: DatabaseSync): void {
  createRegistry(db, "registry_new");
  const rows = db
    .prepare("SELECT collector, exchange, symbol, timeframe, metadata, created_at, updated_at FROM registry;")
    .all() as Array<{ collector: string; exchange: string; symbol: string; timeframe: string; metadata: string; created_at?: number; updated_at?: number }>;
  const insert = db.prepare(
    `INSERT INTO registry_new
      (collector, exchange, symbol, timeframe, start_ts, end_ts, sparse, created_at, updated_at)
     VALUES
      (:collector, :exchange, :symbol, :timeframe, :startTs, :endTs, :sparse, :created_at, :updated_at);`,
  );

  db.exec("BEGIN");
  try {
    for (const row of rows) {
      try {
        const parsed = JSON.parse(row.metadata) as Partial<RegistryEntry> &
          Partial<{ start_ts: number; end_ts: number }>;
        const startTs = (parsed as any).startTs ?? (parsed as any).start_ts;
        const endTs = (parsed as any).endTs ?? (parsed as any).end_ts;
        const sparse = Boolean((parsed as any).sparse);
        if (!Number.isFinite(startTs) || !Number.isFinite(endTs)) {
          continue;
        }
        insert.run({
          collector: row.collector,
          exchange: row.exchange,
          symbol: row.symbol,
          timeframe: row.timeframe,
          startTs,
          endTs,
          sparse: sparse ? 1 : 0,
          created_at: row.created_at ?? Date.now(),
          updated_at: row.updated_at ?? row.created_at ?? Date.now(),
        });
      } catch {
        // skip bad rows
      }
    }
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }

  db.exec("DROP TABLE registry;");
  db.exec("ALTER TABLE registry_new RENAME TO registry;");
  db.exec("CREATE INDEX IF NOT EXISTS idx_registry_exchange_symbol ON registry(exchange, symbol);");
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
      if (!row.exchange || !row.symbol || !Number.isFinite(row.startTs)) {
        throw new Error(`Invalid indexed row ${row.relativePath}: missing exchange/symbol/startTs`);
      }
      const res = stmt.run({
        rootId: row.rootId,
        relativePath: row.relativePath,
        collector: row.collector,
        exchange: row.exchange,
        symbol: row.symbol,
        startTs: row.startTs,
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

function upsertRegistry(stmt: StatementSync, entry: RegistryEntry): void {
  const payload = {
    collector: entry.collector,
    exchange: entry.exchange,
    symbol: entry.symbol,
    timeframe: entry.timeframe,
    startTs: entry.startTs,
    endTs: entry.endTs,
    sparse: entry.sparse ? 1 : 0,
  };
  stmt.run(payload);
}

function replaceRegistry(
  db: DatabaseSync,
  upsertStmt: StatementSync,
  deleteStmt: StatementSync,
  entries: RegistryEntry[],
  filter?: RegistryFilter,
): { upserted: number; deleted: number } {
  let deleted = 0;
  let upserted = 0;
  db.exec("BEGIN");
  try {
    const delRes = deleteStmt.run({
      collector: filter?.collector ?? null,
      exchange: filter?.exchange ?? null,
      symbol: filter?.symbol ?? null,
      timeframe: filter?.timeframe ?? null,
    });
    deleted = Number(delRes.changes ?? 0);

    for (const entry of entries) {
      upsertRegistry(upsertStmt, entry);
      upserted += 1;
    }

    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
  return { upserted, deleted };
}

function getRegistry(stmt: StatementSync, key: RegistryKey): RegistryEntry | null {
  const row = stmt.get({
    collector: key.collector,
    exchange: key.exchange,
    symbol: key.symbol,
    timeframe: key.timeframe,
  }) as
    | {
        collector: string;
        exchange: string;
        symbol: string;
        timeframe: string;
        start_ts: number;
        end_ts: number;
        sparse: number;
        created_at?: number;
        updated_at?: number;
      }
    | undefined;
  if (!row) return null;
  return {
    collector: row.collector,
    exchange: row.exchange,
    symbol: row.symbol,
    timeframe: row.timeframe,
    startTs: (row as any).startTs ?? (row as any).start_ts ?? row.start_ts,
    endTs: (row as any).endTs ?? (row as any).end_ts ?? row.end_ts,
    sparse: Boolean((row as any).sparse),
    created_at: row.created_at,
    updated_at: row.updated_at,
  };
}
