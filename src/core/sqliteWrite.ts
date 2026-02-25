import type { DatabaseSync } from "node:sqlite";

const SQLITE_BUSY_PRIMARY = 5;
const SQLITE_LOCKED_PRIMARY = 6;

const DEFAULT_BUSY_TIMEOUT_MS = 1_000;
const DEFAULT_MAX_RETRIES = 300;
const DEFAULT_BASE_DELAY_MS = 10;
const DEFAULT_MAX_DELAY_MS = 250;

const waitArray = new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT));

interface SqliteErrorShape {
  code?: string;
  errcode?: number;
  errstr?: string;
  message?: string;
}

export interface SqliteWriteRetryOptions {
  maxRetries?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  sleep?: (delayMs: number) => void;
}

export function configureSqliteWriteContention(db: DatabaseSync): void {
  db.exec(`PRAGMA busy_timeout=${DEFAULT_BUSY_TIMEOUT_MS};`);
}

export function runSqliteWrite<T>(op: () => T, options?: SqliteWriteRetryOptions): T {
  const maxRetries = options?.maxRetries ?? DEFAULT_MAX_RETRIES;
  const baseDelayMs = options?.baseDelayMs ?? DEFAULT_BASE_DELAY_MS;
  const maxDelayMs = options?.maxDelayMs ?? DEFAULT_MAX_DELAY_MS;
  const sleep = options?.sleep ?? sleepBlockingMs;

  for (let attempt = 0; ; attempt += 1) {
    try {
      return op();
    } catch (err) {
      if (!isSqliteWriteContentionError(err) || attempt >= maxRetries) {
        throw err;
      }
      sleep(computeBackoffMs(attempt, baseDelayMs, maxDelayMs));
    }
  }
}

export function runSqliteWriteTransaction<T>(
  db: DatabaseSync,
  op: () => T,
  options?: SqliteWriteRetryOptions,
): T {
  return runSqliteWrite(() => {
    let began = false;
    db.exec("BEGIN");
    began = true;
    try {
      const value = op();
      db.exec("COMMIT");
      began = false;
      return value;
    } catch (err) {
      if (began) {
        try {
          db.exec("ROLLBACK");
        } catch {
          // best-effort rollback: do not shadow the original failure
        }
      }
      throw err;
    }
  }, options);
}

export function isSqliteWriteContentionError(err: unknown): boolean {
  if (!err || typeof err !== "object") return false;
  const sqliteErr = err as SqliteErrorShape;

  if (Number.isFinite(sqliteErr.errcode)) {
    const primary = Math.abs(sqliteErr.errcode!) & 0xff;
    if (primary === SQLITE_BUSY_PRIMARY || primary === SQLITE_LOCKED_PRIMARY) {
      return true;
    }
  }

  if (sqliteErr.code !== "ERR_SQLITE_ERROR") return false;
  const detail = `${sqliteErr.errstr ?? ""} ${sqliteErr.message ?? ""}`.toLowerCase();
  return detail.includes("database is locked") || detail.includes("database is busy");
}

function computeBackoffMs(attempt: number, baseDelayMs: number, maxDelayMs: number): number {
  if (attempt <= 0) return Math.max(0, Math.min(baseDelayMs, maxDelayMs));
  const scaled = baseDelayMs * 2 ** Math.min(attempt, 12);
  return Math.max(0, Math.min(scaled, maxDelayMs));
}

function sleepBlockingMs(delayMs: number): void {
  if (delayMs <= 0) return;
  try {
    Atomics.wait(waitArray, 0, 0, delayMs);
  } catch {
    // Ignore environments where blocking waits are unavailable.
  }
}
