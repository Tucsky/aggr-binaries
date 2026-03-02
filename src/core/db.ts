import { DatabaseSync, StatementSync } from "node:sqlite";
import type { Gap } from "./gapTracker.js";
import type { GapCtx, GapFixStatus, IndexedFile, RegistryEntry, RegistryFilter, RegistryKey } from "./model.js";
import { configureSqliteWriteContention, runSqliteWrite, runSqliteWriteTransaction } from "./sqliteWrite.js";

// This file intentionally stays centralized despite its size: migrations and prepared write paths are tightly coupled.
export interface Db {
  db: DatabaseSync;
  ensureRoot(path: string): number;
  insertFiles(rows: IndexedFile[]): { inserted: number; existing: number };
  listIndexedMarketRanges(): IndexedMarketRangeRow[];
  upsertRegistry(entry: RegistryEntry): void;
  replaceRegistry(entries: RegistryEntry[], filter?: RegistryFilter): { upserted: number; deleted: number };
  getRegistryEntry(key: RegistryKey): RegistryEntry | null;
  insertGaps(ctx: GapCtx, gaps: Gap[]): void;
  deleteGapsForFile(rootId: number, relativePath: string): void;
  iterateGapsForFix(opts: GapFixQueueFilter): Iterable<GapFixQueueRow>;
  updateGapFixStatus(rows: Array<{ id: number; status: GapFixStatus; error?: string | null; recovered?: number | null }>): void;
  deleteGapsByIds(ids: number[]): void;
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
  gap_ms: number | null;
  gap_miss: number | null;
  gap_end_ts: number | null;
  gap_fix_status: string | null;
  gap_score: number | null;
}

export interface IndexedMarketRangeRow {
  collector: string;
  exchange: string;
  symbol: string;
  startTs: number;
  endTs: number;
  updatedAt: number;
}

export function openDatabase(dbPath: string): Db {
  const db = new DatabaseSync(dbPath);
  db.exec("PRAGMA journal_mode=WAL;");
  db.exec("PRAGMA synchronous=NORMAL;");
  db.exec("PRAGMA temp_store=MEMORY;");
  db.exec("PRAGMA wal_autocheckpoint=10000;");
  configureSqliteWriteContention(db);

  migrate(db);

  const insertStmt = db.prepare(
    `INSERT OR IGNORE INTO files
      (root_id, relative_path, collector, exchange, symbol, start_ts, ext)
     VALUES
      (:rootId, :relativePath, :collector, :exchange, :symbol, :startTs, :ext);`,
  );
  const upsertIndexedMarketRangeStmt = db.prepare(
    `INSERT INTO indexed_market_ranges
      (collector, exchange, symbol, start_ts, end_ts)
     VALUES
      (:collector, :exchange, :symbol, :startTs, :endTs)
     ON CONFLICT(collector, exchange, symbol) DO UPDATE SET
      start_ts = MIN(indexed_market_ranges.start_ts, excluded.start_ts),
      end_ts = MAX(indexed_market_ranges.end_ts, excluded.end_ts),
      updated_at = (unixepoch('subsec') * 1000);`,
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
  const insertGapStmt = db.prepare(
    `INSERT INTO gaps
      (root_id, relative_path, collector, exchange, symbol, gap_ms, gap_miss, gap_score, gap_end_ts, gap_score)
     VALUES
      (:rootId, :relativePath, :collector, :exchange, :symbol, :gapMs, :gapMiss, :gapScore, :gapEndTs, :gapScore);`,
  );
  const deleteGapsForFileStmt = db.prepare(
    `DELETE FROM gaps WHERE root_id = :rootId AND relative_path = :relativePath;`,
  );
  const updateGapFixStatusStmt = db.prepare(
    `UPDATE gaps
        SET gap_fix_status = :status,
            gap_fix_error = :error,
            gap_fix_recovered = :recovered,
            gap_fix_updated_at = (unixepoch('subsec') * 1000)
      WHERE id = :id;`,
  );
  const deleteGapByIdStmt = db.prepare("DELETE FROM gaps WHERE id = :id;");
  const listIndexedMarketRangesStmt = db.prepare(
    `SELECT collector, exchange, symbol, start_ts, end_ts, updated_at
       FROM indexed_market_ranges;`,
  );

  const api: Db = {
    db,
    ensureRoot: (path: string): number => {
      return runSqliteWrite(() => {
        ensureRootStmt.run({ path });
        const row = getRootIdStmt.get({ path }) as { id: number } | undefined;
        if (!row) {
          throw new Error(`Failed to resolve root id for ${path}`);
        }
        return row.id;
      });
    },
    insertFiles: (rows: IndexedFile[]) => insertMany(db, insertStmt, upsertIndexedMarketRangeStmt, rows),
    listIndexedMarketRanges: () => listIndexedMarketRanges(listIndexedMarketRangesStmt),
    upsertRegistry: (entry: RegistryEntry) => upsertRegistry(upsertRegistryStmt, entry),
    replaceRegistry: (entries: RegistryEntry[], filter?: RegistryFilter) =>
      replaceRegistry(db, upsertRegistryStmt, deleteRegistryStmt, entries, filter),
    getRegistryEntry: (key: RegistryKey) => getRegistry(getRegistryStmt, key),
    insertGaps: (ctx: GapCtx, rows: Gap[]) => insertGaps(db, insertGapStmt, ctx, rows),
    deleteGapsForFile: (rootId: number, relativePath: string) =>
      runSqliteWrite(() => {
        deleteGapsForFileStmt.run({ rootId, relativePath });
      }),
    iterateGapsForFix: (opts: GapFixQueueFilter): Iterable<GapFixQueueRow> =>
      iterateGapsForFix(db, opts),
    updateGapFixStatus: (rows: Array<{ id: number; status: GapFixStatus; error?: string | null; recovered?: number | null }>) =>
      updateGapFixStatus(db, updateGapFixStatusStmt, rows),
    deleteGapsByIds: (ids: number[]) => deleteGapsByIds(db, deleteGapByIdStmt, ids),
    close: () => db.close(),
  };

  return api;
}

function migrate(db: DatabaseSync): void {
  createTables(db);
  createIndexes(db);
  assertSchema(db);
}

function createTables(db: DatabaseSync): void {
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

  db.exec(`
    CREATE TABLE IF NOT EXISTS registry (
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

  db.exec(`
    CREATE TABLE IF NOT EXISTS gaps (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      root_id INTEGER NOT NULL REFERENCES roots(id) ON DELETE CASCADE,
      relative_path TEXT NOT NULL,
      collector TEXT NOT NULL,
      exchange TEXT NOT NULL,
      symbol TEXT NOT NULL,
      gap_ms INTEGER,
      gap_miss INTEGER,
      gap_end_ts INTEGER,
      gap_score FLOAT,
      gap_fix_status TEXT,
      gap_fix_error TEXT,
      gap_fix_recovered INTEGER,
      gap_fix_updated_at INTEGER,
      created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000)
    );
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS indexed_market_ranges (
      collector TEXT NOT NULL,
      exchange TEXT NOT NULL,
      symbol TEXT NOT NULL,
      start_ts INTEGER NOT NULL,
      end_ts INTEGER NOT NULL,
      created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000),
      updated_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000),
      PRIMARY KEY (collector, exchange, symbol)
    );
  `);
}

function createIndexes(db: DatabaseSync): void {
  db.exec("CREATE INDEX IF NOT EXISTS idx_files_exchange_symbol ON files(exchange, symbol);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_files_start_ts ON files(start_ts);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_files_collector ON files(collector);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_registry_exchange_symbol ON registry(exchange, symbol);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_gaps_file ON gaps(root_id, relative_path);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_gaps_market ON gaps(collector, exchange, symbol);");
  db.exec("CREATE INDEX IF NOT EXISTS idx_gaps_market_gap_end_ts ON gaps(collector, exchange, symbol, gap_end_ts, id);");
  db.exec(
    "CREATE INDEX IF NOT EXISTS idx_gaps_fix_queue ON gaps(gap_fix_status, collector, exchange, symbol, root_id, relative_path, id);",
  );
  db.exec("CREATE INDEX IF NOT EXISTS idx_indexed_market_ranges_exchange_symbol ON indexed_market_ranges(exchange, symbol);");
}

interface TableInfo {
  name: string;
  notnull: number;
}

function assertSchema(db: DatabaseSync): void {
  assertExactColumns(db, "roots", ["id", "path"]);
  assertExactColumns(db, "files", [
    "root_id",
    "relative_path",
    "collector",
    "exchange",
    "symbol",
    "start_ts",
    "ext",
    "created_at",
  ]);
  assertExactColumns(db, "registry", [
    "collector",
    "exchange",
    "symbol",
    "timeframe",
    "start_ts",
    "end_ts",
    "created_at",
    "updated_at",
  ]);
  assertExactColumns(db, "gaps", [
    "id",
    "root_id",
    "relative_path",
    "collector",
    "exchange",
    "symbol",
    "gap_ms",
    "gap_miss",
    "gap_score",
    "gap_end_ts",
    "gap_fix_status",
    "gap_fix_error",
    "gap_fix_recovered",
    "gap_fix_updated_at",
    "created_at",
  ]);
  assertExactColumns(db, "indexed_market_ranges", [
    "collector",
    "exchange",
    "symbol",
    "start_ts",
    "end_ts",
    "created_at",
    "updated_at",
  ]);

  assertColumnsNotNull(db, "files", ["exchange", "symbol", "start_ts"]);

  const nullCountRow = db
    .prepare("SELECT COUNT(*) as cnt FROM files WHERE exchange IS NULL OR symbol IS NULL OR start_ts IS NULL;")
    .get() as { cnt?: number };
  if (nullCountRow?.cnt && nullCountRow.cnt > 0) {
    throw new Error(
      `files table contains ${nullCountRow.cnt} rows with NULL exchange/symbol/start_ts. ${rebuildDbHint()}`,
    );
  }

  const fileCount = db.prepare("SELECT COUNT(*) AS cnt FROM files;").get() as { cnt?: number } | undefined;
  const rangeCount = db
    .prepare("SELECT COUNT(*) AS cnt FROM indexed_market_ranges;")
    .get() as { cnt?: number } | undefined;
  if ((fileCount?.cnt ?? 0) > 0 && (rangeCount?.cnt ?? 0) === 0) {
    throw new Error(
      `indexed_market_ranges is empty while files has data. ${rebuildDbHint()}`,
    );
  }
}

function assertExactColumns(db: DatabaseSync, table: string, expected: string[]): void {
  const info = db.prepare(`PRAGMA table_info(${table});`).all() as unknown as TableInfo[];
  const actual = info.map((c) => c.name);
  const missing = expected.filter((name) => !actual.includes(name));
  const unexpected = actual.filter((name) => !expected.includes(name));
  if (!missing.length && !unexpected.length) return;

  const details: string[] = [];
  if (missing.length) details.push(`missing: ${missing.join(", ")}`);
  if (unexpected.length) details.push(`unexpected: ${unexpected.join(", ")}`);
  throw new Error(`Incompatible schema for table '${table}' (${details.join("; ")}). ${rebuildDbHint()}`);
}

function assertColumnsNotNull(db: DatabaseSync, table: string, required: string[]): void {
  const info = db.prepare(`PRAGMA table_info(${table});`).all() as unknown as TableInfo[];
  const missingNotNull = required.filter((name) => !info.some((c) => c.name === name && c.notnull === 1));
  if (!missingNotNull.length) return;
  throw new Error(
    `Table '${table}' missing NOT NULL constraints for: ${missingNotNull.join(", ")}. ${rebuildDbHint()}`,
  );
}

function rebuildDbHint(): string {
  return "Delete index.sqlite and run index again.";
}

function insertMany(
  db: DatabaseSync,
  stmt: StatementSync,
  upsertIndexedMarketRangeStmt: StatementSync,
  rows: IndexedFile[],
): { inserted: number; existing: number } {
  return runSqliteWriteTransaction(db, () => {
    let inserted = 0;
    let existing = 0;
    const pendingRanges = new Map<string, PendingIndexedMarketRange>();
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
      if (delta > 0) {
        accumulateIndexedMarketRange(pendingRanges, row.collector, row.exchange, row.symbol, row.startTs);
      }
    }

    // This second loop is intentional: batch by market key to avoid one indexed-range upsert per inserted file.
    for (const range of pendingRanges.values()) {
      upsertIndexedMarketRangeStmt.run({
        collector: range.collector,
        exchange: range.exchange,
        symbol: range.symbol,
        startTs: range.startTs,
        endTs: range.endTs,
      });
    }
    return { inserted, existing };
  });
}

interface PendingIndexedMarketRange {
  collector: string;
  exchange: string;
  symbol: string;
  startTs: number;
  endTs: number;
}

function accumulateIndexedMarketRange(
  ranges: Map<string, PendingIndexedMarketRange>,
  collector: string,
  exchange: string,
  symbol: string,
  startTs: number,
): void {
  const key = `${collector}\u0000${exchange}\u0000${symbol}`;
  const existing = ranges.get(key);
  if (!existing) {
    ranges.set(key, { collector, exchange, symbol, startTs, endTs: startTs });
    return;
  }
  if (startTs < existing.startTs) existing.startTs = startTs;
  if (startTs > existing.endTs) existing.endTs = startTs;
}

function insertGaps(db: DatabaseSync, stmt: StatementSync, ctx: GapCtx, rows: Gap[]): void {
  if (!rows.length) return;
  runSqliteWriteTransaction(db, () => {
    for (const row of rows) {
      stmt.run({
        rootId: ctx.rootId,
        relativePath: ctx.relativePath,
        collector: ctx.collector,
        exchange: ctx.exchange,
        symbol: ctx.symbol,
        gapMs: row.gapMs ?? null,
        gapScore: row.gapScore ?? null,
        gapMiss: row.gapMiss ?? null,
        gapEndTs: row.gapEndTs ?? null,
      });
    }
  });
}

function updateGapFixStatus(
  db: DatabaseSync,
  stmt: StatementSync,
  rows: Array<{ id: number; status: GapFixStatus; error?: string | null; recovered?: number | null }>,
): void {
  if (!rows.length) return;
  runSqliteWriteTransaction(db, () => {
    for (const row of rows) {
      stmt.run({
        id: row.id,
        status: row.status,
        error: row.error ?? null,
        recovered: row.recovered ?? null,
      });
    }
  });
}

function deleteGapsByIds(db: DatabaseSync, stmt: StatementSync, ids: number[]): void {
  if (!ids.length) return;
  runSqliteWriteTransaction(db, () => {
    for (const id of ids) {
      stmt.run({ id });
    }
  });
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
  runSqliteWrite(() => {
    stmt.run(payload);
  });
}

function replaceRegistry(
  db: DatabaseSync,
  upsertStmt: StatementSync,
  deleteStmt: StatementSync,
  entries: RegistryEntry[],
  filter?: RegistryFilter,
): { upserted: number; deleted: number } {
  return runSqliteWriteTransaction(db, () => {
    const delRes = deleteStmt.run({
      collector: filter?.collector ?? null,
      exchange: filter?.exchange ?? null,
      symbol: filter?.symbol ?? null,
      timeframe: filter?.timeframe ?? null,
    });
    const deleted = Number(delRes.changes ?? 0);
    let upserted = 0;

    for (const entry of entries) {
      upsertStmt.run({
        collector: entry.collector,
        exchange: entry.exchange,
        symbol: entry.symbol,
        timeframe: entry.timeframe,
        startTs: entry.startTs,
        endTs: entry.endTs,
      });
      upserted += 1;
    }

    return { upserted, deleted };
  });
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

function listIndexedMarketRanges(stmt: StatementSync): IndexedMarketRangeRow[] {
  const rows = stmt.all() as Array<{
    collector: string;
    exchange: string;
    symbol: string;
    start_ts: number;
    end_ts: number;
    updated_at: number;
  }>;
  if (!rows.length) return [];

  const ranges = new Array<IndexedMarketRangeRow>(rows.length);
  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i];
    ranges[i] = {
      collector: row.collector,
      exchange: row.exchange,
      symbol: row.symbol,
      startTs: row.start_ts,
      endTs: row.end_ts,
      updatedAt: row.updated_at,
    };
  }
  return ranges;
}

const GAP_FIX_QUEUE_PAGE_SIZE = 1024;

interface GapFixQueueCursor {
  collector: string;
  exchange: string;
  symbol: string;
  rootId: number;
  relativePath: string;
  id: number;
}

function iterateGapsForFix(db: DatabaseSync, opts: GapFixQueueFilter): Iterable<GapFixQueueRow> {
  const where: string[] = [];
  const baseParams: Record<string, string | number | null> = {};

  const selectedId = Number.isFinite(opts.id) && (opts.id as number) > 0 ? Math.floor(opts.id as number) : undefined;
  if (selectedId !== undefined) {
    where.push("e.id = :id");
    baseParams.id = selectedId;
  } else {
    const retryStatuses = (opts.retryStatuses ?? []).map((s) => s.trim()).filter(Boolean);
    if (retryStatuses.length) {
      const placeholders: string[] = [];
      for (let i = 0; i < retryStatuses.length; i += 1) {
        const key = `retry${i}`;
        placeholders.push(`:${key}`);
        baseParams[key] = retryStatuses[i];
      }
      where.push(`(e.gap_fix_status IS NULL OR e.gap_fix_status IN (${placeholders.join(",")}))`);
    } else {
      where.push("e.gap_fix_status IS NULL");
    }
  }

  if (opts.collector) {
    where.push("e.collector = :collector");
    baseParams.collector = opts.collector.toUpperCase();
  }
  if (opts.exchange) {
    where.push("e.exchange = :exchange");
    baseParams.exchange = opts.exchange.toUpperCase();
  }
  if (opts.symbol) {
    where.push("e.symbol = :symbol");
    baseParams.symbol = opts.symbol;
  }

  const limit = Number.isFinite(opts.limit) && (opts.limit as number) > 0 ? Math.floor(opts.limit as number) : undefined;
  const baseSql =
    `SELECT e.id, e.root_id, r.path AS root_path, e.relative_path,
            e.collector, e.exchange, e.symbol,
            e.gap_ms, e.gap_miss, e.gap_end_ts, e.gap_fix_status
       FROM gaps e
       JOIN roots r ON r.id = e.root_id`;
  const whereSql = where.join(" AND ");
  // Keep queue deterministic and market-local so one symbol can be drained before moving to the next.
  const orderBySql = " ORDER BY e.collector, e.exchange, e.symbol, e.root_id, e.relative_path, e.id";

  if (selectedId !== undefined) {
    const params: Record<string, string | number | null> = limit !== undefined ? { ...baseParams, limit } : baseParams;
    const sql = `${baseSql} WHERE ${whereSql}${orderBySql}${limit !== undefined ? " LIMIT :limit" : ""};`;
    return db.prepare(sql).all(params) as unknown as GapFixQueueRow[];
  }

  const pageSize = limit !== undefined ? Math.min(limit, GAP_FIX_QUEUE_PAGE_SIZE) : GAP_FIX_QUEUE_PAGE_SIZE;
  // Page by keyset so each select uses a fresh read snapshot and does not hold a long-lived cursor while writes run.
  return {
    *[Symbol.iterator](): Iterator<GapFixQueueRow> {
      let remaining = limit ?? Number.POSITIVE_INFINITY;
      let cursor: GapFixQueueCursor | undefined;

      while (remaining > 0) {
        const pageLimit = Number.isFinite(remaining) ? Math.min(pageSize, remaining) : pageSize;
        const rows = selectGapFixQueuePage(db, baseSql, whereSql, orderBySql, baseParams, pageLimit, cursor);
        if (!rows.length) return;

        for (let i = 0; i < rows.length; i += 1) {
          yield rows[i];
        }

        remaining -= rows.length;
        const last = rows[rows.length - 1];
        cursor = {
          collector: last.collector,
          exchange: last.exchange,
          symbol: last.symbol,
          rootId: last.root_id,
          relativePath: last.relative_path,
          id: last.id,
        };
      }
    },
  };
}

function selectGapFixQueuePage(
  db: DatabaseSync,
  baseSql: string,
  whereSql: string,
  orderBySql: string,
  baseParams: Record<string, string | number | null>,
  pageLimit: number,
  cursor?: GapFixQueueCursor,
): GapFixQueueRow[] {
  const whereParts: string[] = [whereSql];
  const params: Record<string, string | number | null> = {
    ...baseParams,
    pageLimit,
  };
  
  if (cursor) {
    whereParts.push(
      `(e.collector > :cursorCollector
         OR (e.collector = :cursorCollector AND (
              e.exchange > :cursorExchange
              OR (e.exchange = :cursorExchange AND (
                   e.symbol > :cursorSymbol
                   OR (e.symbol = :cursorSymbol AND (
                        e.root_id > :cursorRootId
                        OR (e.root_id = :cursorRootId AND (
                             e.relative_path > :cursorRelativePath
                             OR (e.relative_path = :cursorRelativePath AND (
                                  e.id > :cursorId
                                ))
                           ))
                      ))
                 ))
            )))`,
    );

    params.cursorCollector = cursor.collector;
    params.cursorExchange = cursor.exchange;
    params.cursorSymbol = cursor.symbol;
    params.cursorRootId = cursor.rootId;
    params.cursorRelativePath = cursor.relativePath;
    params.cursorId = cursor.id;
  }

  const sql = `${baseSql} WHERE ${whereParts.join(" AND ")}${orderBySql} LIMIT :pageLimit;`;

  return (db.prepare(sql).all(params) as unknown as GapFixQueueRow[]) ?? [];
}