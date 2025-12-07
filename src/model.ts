export enum Collector {
  RAM = "RAM",
  PI = "PI",
}

export enum Era {
  Legacy = "legacy",
  Logical = "logical",
}

export enum QuoteCurrency {
  USDT = "USDT",
  USDC = "USDC",
  USD = "USD",
  EUR = "EUR",
}

// Database schema reference:
// CREATE TABLE roots (
//   id INTEGER PRIMARY KEY AUTOINCREMENT,
//   path TEXT NOT NULL UNIQUE
// );
//
// CREATE TABLE files (
//   root_id INTEGER NOT NULL REFERENCES roots(id) ON DELETE CASCADE,
//   relative_path TEXT NOT NULL,
//   size INTEGER NOT NULL,
//   mtime_ms INTEGER NOT NULL,
//   collector TEXT NOT NULL,   -- RAM | PI
//   era TEXT NOT NULL,         -- legacy | logical
//   exchange TEXT,
//   symbol TEXT,
//   start_ts INTEGER,
//   ext TEXT,
//   created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec') * 1000),
//   PRIMARY KEY (root_id, relative_path)
// );
// CREATE INDEX idx_files_exchange_symbol ON files(exchange, symbol);
// CREATE INDEX idx_files_start_ts ON files(start_ts);
// CREATE INDEX idx_files_collector ON files(collector);

export interface IndexedFile {
  rootId: number;
  relativePath: string; // POSIX separators
  size: number;
  mtimeMs: number;
  collector: Collector;
  era: Era;
  exchange?: string;
  symbol?: string;
  startTs?: number;
  ext?: string;
}

export interface FileSystemEntry {
  rootId: number;
  rootPath: string;
  relativePath: string; // POSIX separators
  fullPath: string;
  size: number;
  mtimeMs: number;
  ext?: string;
}

export interface IndexStats {
  seen: number;
  inserted: number;
  existing: number;
  conflicts: number;
  skipped: number;
}
