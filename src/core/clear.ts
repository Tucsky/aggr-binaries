import fs from "node:fs/promises";
import path from "node:path";
import type { Config } from "./config.js";
import type { Db } from "./db.js";
import { runIndex } from "./indexer.js";
import type { IndexStats } from "./model.js";
import { runSqliteWriteTransaction } from "./sqliteWrite.js";

export interface ClearMarket {
  collector: string;
  exchange: string;
  symbol: string;
}

export interface ClearStats {
  outputsDeleted: number;
  eventsDeleted: number;
  filesDeleted: number;
  registryDeleted: number;
}

export interface ClearRunStats extends ClearStats, IndexStats {}

interface RunClearDeps {
  runIndex?: typeof runIndex;
}

/**
 * Clear one market's outputs + DB state, then reindex only that market subtree.
 */
export async function runClear(config: Config, db: Db, deps?: RunClearDeps): Promise<ClearRunStats> {
  const market = resolveMarketFromConfig(config);
  const clearStats = await clearMarket(db, config.outDir, market);
  const includePaths = await resolveIndexIncludePaths(config.root, market.collector, market.exchange, market.symbol);
  const indexFn = deps?.runIndex ?? runIndex;
  const indexStats = await indexFn(
    {
      ...config,
      collector: market.collector,
      exchange: market.exchange,
      symbol: market.symbol,
      force: true,
      includePaths,
    },
    db,
  );
  return {
    outputsDeleted: clearStats.outputsDeleted,
    eventsDeleted: clearStats.eventsDeleted,
    filesDeleted: clearStats.filesDeleted,
    registryDeleted: clearStats.registryDeleted,
    seen: indexStats.seen,
    inserted: indexStats.inserted,
    existing: indexStats.existing,
    conflicts: indexStats.conflicts,
    skipped: indexStats.skipped,
  };
}

export async function clearMarket(db: Db, outDir: string, market: ClearMarket): Promise<ClearStats> {
  const outputsDeleted = await deleteMarketOutputs(outDir, market);
  const dbStats = await deleteMarketRows(db, market);
  return {
    outputsDeleted,
    eventsDeleted: dbStats.eventsDeleted,
    filesDeleted: dbStats.filesDeleted,
    registryDeleted: dbStats.registryDeleted,
  };
}

/**
 * Resolve include paths for one market under either direct collector layout or bucketed layout.
 */
export async function resolveIndexIncludePaths(
  root: string,
  collector: string,
  exchange: string,
  symbol: string,
): Promise<string[]> {
  const out = new Set<string>();

  const collectorRoot = path.join(root, collector);
  const baseRoot = (await isDirectory(collectorRoot)) ? collectorRoot : root;
  const directPath = path.join(baseRoot, exchange, symbol);
  if (await isDirectory(directPath)) {
    out.add(path.relative(root, directPath));
  }

  const bucketDirs = await readSubdirs(baseRoot);
  for (const bucket of bucketDirs) {
    const candidate = path.join(baseRoot, bucket, exchange, symbol);
    if (await isDirectory(candidate)) {
      out.add(path.relative(root, candidate));
    }
  }

  if (!out.size) {
    if (baseRoot === collectorRoot) {
      out.add(path.join(collector, exchange, symbol));
    }
    out.add(path.join(exchange, symbol));
  }

  return [...out].sort();
}

function resolveMarketFromConfig(config: Config): ClearMarket {
  if (!config.collector || !config.exchange || !config.symbol) {
    throw new Error("clear requires --collector, --exchange, and --symbol");
  }
  return {
    collector: config.collector,
    exchange: config.exchange,
    symbol: config.symbol,
  };
}

async function deleteMarketOutputs(outDir: string, market: ClearMarket): Promise<number> {
  const marketOutDir = path.join(outDir, market.collector, market.exchange, market.symbol);
  try {
    await fs.rm(marketOutDir, { recursive: true });
    return 1;
  } catch (err) {
    if ((err as { code?: string }).code === "ENOENT") {
      return 0;
    }
    throw err;
  }
}

async function deleteMarketRows(
  db: Db,
  market: ClearMarket,
): Promise<{ eventsDeleted: number; filesDeleted: number; registryDeleted: number }> {
  const params = {
    collector: market.collector,
    exchange: market.exchange,
    symbol: market.symbol,
  };
  const deleteEventsStmt = db.db.prepare(
    `DELETE FROM gaps
     WHERE collector = :collector AND exchange = :exchange AND symbol = :symbol;`,
  );
  const deleteFilesStmt = db.db.prepare(
    `DELETE FROM files
     WHERE collector = :collector AND exchange = :exchange AND symbol = :symbol;`,
  );
  const deleteRegistryStmt = db.db.prepare(
    `DELETE FROM registry
     WHERE collector = :collector AND exchange = :exchange AND symbol = :symbol;`,
  );

  return runSqliteWriteTransaction(db.db, () => {
    const eventsDeleted = Number(deleteEventsStmt.run(params).changes ?? 0);
    const filesDeleted = Number(deleteFilesStmt.run(params).changes ?? 0);
    const registryDeleted = Number(deleteRegistryStmt.run(params).changes ?? 0);
    return { eventsDeleted, filesDeleted, registryDeleted };
  });
}

async function readSubdirs(root: string): Promise<string[]> {
  try {
    const entries = await fs.readdir(root, { withFileTypes: true });
    return entries.filter((entry) => entry.isDirectory()).map((entry) => entry.name);
  } catch {
    return [];
  }
}

async function isDirectory(absPath: string): Promise<boolean> {
  try {
    const stat = await fs.stat(absPath);
    return stat.isDirectory();
  } catch {
    return false;
  }
}
