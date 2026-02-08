import { DatabaseSync, StatementSync } from "node:sqlite";
import type { EventRange, GapFixStatus } from "./events.js";
import type { IndexedFile, RegistryEntry, RegistryFilter, RegistryKey } from "./model.js";

export interface Db {
  db: DatabaseSync;
  ensureRoot(path: string): number;
  insertFiles(rows: IndexedFile[]): { inserted: number; existing: number };
  upsertRegistry(entry: RegistryEntry): void;
  replaceRegistry(entries: RegistryEntry[], filter?: RegistryFilter): { upserted: number; deleted: number };
  getRegistryEntry(key: RegistryKey): RegistryEntry | null;
  insertEvents(rows: EventRange[]): void;
  deleteEventsForFile(rootId: number, relativePath: string): void;
  iterateGapEventsForFix(opts: GapFixQueueFilter): Iterable<GapFixQueueRow>;
  updateGapFixStatus(rows: Array<{ id: number; status: GapFixStatus; error?: string }>): void;
  deleteEventsByIds(ids: number[]): void;
  close(): void;
}

export interface GapFixQueueFilter {
  collector?: string;
  exchange?: string;
  symbol?: string;
  retryStatuses?: string[];
  limit?: number;
  id?: number;
}

export interface GapFixQueueRow {
  id: number;
  root_id: number;
  root_path: string;
  relative_path: string;
  collector: string;
  exchange: string;
  symbol: string;
  start_line: number;
  end_line: number;
  gap_ms: number | null;
  gap_miss: number | null;
  gap_end_ts: number | null;
  gap_fix_status: string | null;
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
      (collector, exchange, symbol, timeframe, start_ts, end_ts)
     VALUES
      (:collector, :exchange, :symbol, :timeframe, :startTs, :endTs)
     ON CONFLICT(collector, exchange, symbol, timeframe) DO UPDATE SET
      start_ts = excluded.start_ts,
      end_ts = excluded.end_ts,
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
  const insertEventStmt = db.prepare(
    `INSERT INTO events
      (root_id, relative_path, collector, exchange, symbol, event_type, start_line, end_line, gap_ms, gap_miss, gap_end_ts)
     VALUES
      (:rootId, :relativePath, :collector, :exchange, :symbol, :eventType, :startLine, :endLine, :gapMs, :gapMiss, :gapEndTs);`,
  );
  const deleteEventsForFileStmt = db.prepare(
    `DELETE FROM events WHERE root_id = :rootId AND relative_path = :relativePath;`,
  );
  const updateGapFixStatusStmt = db.prepare(
    `UPDATE events
        SET gap_fix_status = :status,
            gap_fix_error = :error,
            gap_fix_updated_at = (unixepoch('subsec') * 1000)
      WHERE id = :id;`,
  );
  const deleteEventByIdStmt = db.prepare("DELETE FROM events WHERE id = :id;");

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
    insertEvents: (rows: EventRange[]) => insertEvents(db, insertEventStmt, rows),
    deleteEventsForFile: (rootId: number, relativePath: string) =>
      deleteEventsForFileStmt.run({ rootId, relativePath }),
    iterateGapEventsForFix: (opts: GapFixQueueFilter): Iterable<GapFixQueueRow> =>
      iterateGapEventsForFix(db, opts),
    updateGapFixStatus: (rows: Array<{ id: number; status: GapFixStatus; error?: string }>) =>
      updateGapFixStatus(db, updateGapFixStatusStmt, rows),
    deleteEventsByIds: (ids: number[]) => deleteEventsByIds(db, deleteEventByIdStmt, ids),
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
  migrateEvents(db);

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

  if (!hasMetadata && hasStart && hasEnd && !hasSparse) {
    db.exec("CREATE INDEX IF NOT EXISTS idx_registry_exchange_symbol ON registry(exchange, symbol);");
    return;
  }

  if (hasMetadata) {
    migrateRegistryFromMetadata(db);
    return;
  }

  if (hasSparse) {
    migrateRegistryDropSparse(db);
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
      (collector, exchange, symbol, timeframe, start_ts, end_ts, created_at, updated_at)
     VALUES
      (:collector, :exchange, :symbol, :timeframe, :startTs, :endTs, :created_at, :updated_at);`,
  );

  db.exec("BEGIN");
  try {
    for (const row of rows) {
      try {
        const parsed = JSON.parse(row.metadata) as Partial<RegistryEntry> &
          Partial<{ start_ts: number; end_ts: number }>;
        const startTs = (parsed as any).startTs ?? (parsed as any).start_ts;
        const endTs = (parsed as any).endTs ?? (parsed as any).end_ts;
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

function migrateRegistryDropSparse(db: DatabaseSync): void {
  createRegistry(db, "registry_new");
  const rows = db
    .prepare("SELECT collector, exchange, symbol, timeframe, start_ts, end_ts, created_at, updated_at FROM registry;")
    .all() as Array<{
      collector: string;
      exchange: string;
      symbol: string;
      timeframe: string;
      start_ts: number;
      end_ts: number;
      created_at?: number;
      updated_at?: number;
    }>;
  const insert = db.prepare(
    `INSERT INTO registry_new
      (collector, exchange, symbol, timeframe, start_ts, end_ts, created_at, updated_at)
     VALUES
      (:collector, :exchange, :symbol, :timeframe, :startTs, :endTs, :created_at, :updated_at);`,
  );

  db.exec("BEGIN");
  try {
    for (const row of rows) {
      insert.run({
        collector: row.collector,
        exchange: row.exchange,
        symbol: row.symbol,
        timeframe: row.timeframe,
        startTs: row.start_ts,
        endTs: row.end_ts,
        created_at: row.created_at ?? Date.now(),
        updated_at: row.updated_at ?? row.created_at ?? Date.now(),
      });
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

function migrateEvents(db: DatabaseSync): void {
  const info = db.prepare("PRAGMA table_info(events);").all() as Array<{ name: string }>;
  const hasGapMiss = info.some((c) => c.name === "gap_miss");
  const hasGapEndTs = info.some((c) => c.name === "gap_end_ts");
  const hasFixStatus = info.some((c) => c.name === "gap_fix_status");
  const hasFixError = info.some((c) => c.name === "gap_fix_error");
  const hasFixUpdatedAt = info.some((c) => c.name === "gap_fix_updated_at");

  if (!info.length) {
    db.exec(`
      CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        root_id INTEGER NOT NULL REFERENCES roots(id) ON DELETE CASCADE,
        relative_path TEXT NOT NULL,
        collector TEXT NOT NULL,
        exchange TEXT NOT NULL,
        symbol TEXT NOT NULL,
        event_type TEXT NOT NULL,
        start_line INTEGER NOT NULL,
        end_line INTEGER NOT NULL,
        gap_ms INTEGER,
        gap_miss INTEGER,
        gap_end_ts INTEGER,
        gap_fix_status TEXT,
        gap_fix_error TEXT,
        gap_fix_updated_at INTEGER,
        created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000)
      );
    `);
  } else {
    if (!hasGapMiss) {
      db.exec("ALTER TABLE events ADD COLUMN gap_miss INTEGER;");
    }
    if (!hasGapEndTs) {
      db.exec("ALTER TABLE events ADD COLUMN gap_end_ts INTEGER;");
    }
    if (!hasFixStatus) {
      db.exec("ALTER TABLE events ADD COLUMN gap_fix_status TEXT;");
    }
    if (!hasFixError) {
      db.exec("ALTER TABLE events ADD COLUMN gap_fix_error TEXT;");
    }
    if (!hasFixUpdatedAt) {
      db.exec("ALTER TABLE events ADD COLUMN gap_fix_updated_at INTEGER;");
    }
  }
  db.exec("CREATE INDEX IF NOT EXISTS idx_events_file ON events(root_id, relative_path);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_events_market ON events(collector, exchange, symbol);");
  db.exec(
    "CREATE INDEX IF NOT EXISTS idx_events_fix_queue ON events(event_type, gap_fix_status, collector, exchange, symbol, root_id, relative_path, id);",
  );
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

function insertEvents(db: DatabaseSync, stmt: StatementSync, rows: EventRange[]): void {
  if (!rows.length) return;
  db.exec("BEGIN");
  try {
    for (const row of rows) {
      stmt.run({
        rootId: row.rootId,
        relativePath: row.relativePath,
        collector: row.collector,
        exchange: row.exchange,
        symbol: row.symbol,
        eventType: row.type,
        startLine: row.startLine,
        endLine: row.endLine,
        gapMs: row.gapMs ?? null,
        gapMiss: row.gapMiss ?? null,
        gapEndTs: row.gapEndTs ?? null,
      });
    }
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
}

function updateGapFixStatus(
  db: DatabaseSync,
  stmt: StatementSync,
  rows: Array<{ id: number; status: GapFixStatus; error?: string }>,
): void {
  if (!rows.length) return;
  db.exec("BEGIN");
  try {
    for (const row of rows) {
      stmt.run({
        id: row.id,
        status: row.status,
        error: row.error ?? null,
      });
    }
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
}

function deleteEventsByIds(db: DatabaseSync, stmt: StatementSync, ids: number[]): void {
  if (!ids.length) return;
  db.exec("BEGIN");
  try {
    for (const id of ids) {
      stmt.run({ id });
    }
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
}

function upsertRegistry(stmt: StatementSync, entry: RegistryEntry): void {
  const payload = {
    collector: entry.collector,
    exchange: entry.exchange,
    symbol: entry.symbol,
    timeframe: entry.timeframe,
    startTs: entry.startTs,
    endTs: entry.endTs,
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
    created_at: row.created_at,
    updated_at: row.updated_at,
  };
}

function iterateGapEventsForFix(db: DatabaseSync, opts: GapFixQueueFilter): Iterable<GapFixQueueRow> {
  const where: string[] = ["e.event_type = 'gap'"];
  const params: Record<string, string | number | null> = {};

  const selectedId = Number.isFinite(opts.id) && (opts.id as number) > 0 ? Math.floor(opts.id as number) : undefined;
  if (selectedId !== undefined) {
    where.push("e.id = :id");
    params.id = selectedId;
  } else {
    const retryStatuses = (opts.retryStatuses ?? []).map((s) => s.trim()).filter(Boolean);
    if (retryStatuses.length) {
      const placeholders: string[] = [];
      for (let i = 0; i < retryStatuses.length; i += 1) {
        const key = `retry${i}`;
        placeholders.push(`:${key}`);
        params[key] = retryStatuses[i];
      }
      where.push(`(e.gap_fix_status IS NULL OR e.gap_fix_status IN (${placeholders.join(",")}))`);
    } else {
      where.push("e.gap_fix_status IS NULL");
    }
  }

  if (opts.collector) {
    where.push("e.collector = :collector");
    params.collector = opts.collector.toUpperCase();
  }
  if (opts.exchange) {
    where.push("e.exchange = :exchange");
    params.exchange = opts.exchange.toUpperCase();
  }
  if (opts.symbol) {
    where.push("e.symbol = :symbol");
    params.symbol = opts.symbol;
  }

  const limit = Number.isFinite(opts.limit) && (opts.limit as number) > 0 ? Math.floor(opts.limit as number) : undefined;
  if (limit !== undefined) {
    params.limit = limit;
  }

  const sql =
    `SELECT e.id, e.root_id, r.path AS root_path, e.relative_path,
            e.collector, e.exchange, e.symbol,
            e.start_line, e.end_line, e.gap_ms, e.gap_miss, e.gap_end_ts, e.gap_fix_status
       FROM events e
       JOIN roots r ON r.id = e.root_id
      WHERE ${where.join(" AND ")}
      ORDER BY e.root_id, e.relative_path, e.start_line, e.id` +
    (limit !== undefined ? " LIMIT :limit" : "") +
    ";";
  const stmt = db.prepare(sql);
  return stmt.iterate(params) as Iterable<GapFixQueueRow>;
}
