export enum Collector {
  RAM = "RAM",
  PI = "PI",
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
//   collector TEXT NOT NULL,   -- RAM | PI
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
  collector: Collector;
  exchange: string;
  symbol: string;
  startTs: number;
  ext?: string;
}

export interface FileSystemEntry {
  rootId: number;
  rootPath: string;
  relativePath: string; // POSIX separators
  fullPath: string;
  ext?: string;
}

export interface IndexStats {
  seen: number;
  inserted: number;
  existing: number;
  conflicts: number;
  skipped: number;
}

// Row as stored in SQLite (snake_case, matches schema)
export interface FileRow {
  root_id: number;
  relative_path: string;
  collector: Collector;
  exchange: string;
  symbol: string;
  start_ts: number;
  ext?: string | null;
}

export interface CompanionMetadata {
  exchange: string;
  symbol: string;
  timeframe: string;
  timeframeMs?: number;
  // Monolithic format (aggr-binaries)
  startTs?: number;
  endTs?: number;
  // Segmented format (aggr-server)
  segmentStartTs?: number;
  segmentEndTs?: number;
  segmentSpanMs?: number;
  segmentRecords?: number;
  priceScale: number;
  volumeScale: number;
  records: number;
  sparse?: boolean;
  lastInputStartTs?: number;
}

/**
 * Normalized companion metadata with guaranteed startTs/endTs.
 * This is what the rest of the codebase should use after reading a companion.
 */
export interface NormalizedCompanionMetadata extends Omit<CompanionMetadata, 'startTs' | 'endTs' | 'segmentStartTs' | 'segmentEndTs'> {
  startTs: number;
  endTs: number;
}

/**
 * Normalize companion metadata from either monolithic or segmented format.
 * Ensures startTs and endTs are always present.
 */
export function normalizeCompanionRange(meta: CompanionMetadata): NormalizedCompanionMetadata {
  const startTs = meta.startTs ?? meta.segmentStartTs;
  const endTs = meta.endTs ?? meta.segmentEndTs;
  
  if (!Number.isFinite(startTs) || !Number.isFinite(endTs)) {
    throw new Error('Companion missing valid range fields (startTs/endTs or segmentStartTs/segmentEndTs)');
  }
  
  return {
    ...meta,
    startTs: startTs!,
    endTs: endTs!,
  };
}

export interface RegistryEntry {
  collector: string;
  exchange: string;
  symbol: string;
  timeframe: string;
  startTs: number;
  endTs: number;
  sparse: boolean;
  created_at?: number;
  updated_at?: number;
}

export interface RegistryKey {
  collector: string;
  exchange: string;
  symbol: string;
  timeframe: string;
}

export interface RegistryFilter {
  collector?: string;
  exchange?: string;
  symbol?: string;
  timeframe?: string;
}
