import fs from "node:fs/promises";
import type { Db } from "./db.js";
import type { Config } from "./config.js";
import { classifyPath, normalizeSymbol } from "./normalize.js";
import type { IndexStats, IndexedFile } from "./model.js";
import { createProgressReporter } from "./progress.js";
import type { DirectoryEntry } from "./walker.js";
import { walkFiles } from "./walker.js";

const indexProgress = createProgressReporter({
  envVar: "AGGR_INDEX_PROGRESS",
  prefix: "[index]",
});

interface MarketIndexState {
  endTs: number;
  updatedAt: number;
}

export async function runIndex(config: Config, db: Db): Promise<IndexStats> {
  const rootId = db.ensureRoot(config.root);
  const stats: IndexStats = { seen: 0, inserted: 0, existing: 0, conflicts: 0, skipped: 0 };
  const marketIndexState = config.force ? null : buildMarketIndexState(db);
  const shouldDescendDir = buildDirectoryPruner(marketIndexState);

  let batch: IndexedFile[] = [];
  let skipLogged = 0;
  for await (const entry of walkFiles(config.root, rootId, {
    includePaths: config.includePaths,
    shouldDescendDir,
  })) {
    stats.seen += 1;

    const row = classifyPath(entry.rootId, entry.relativePath);
    if (!row) {
      stats.skipped += 1;
      if (skipLogged < 50) {
        indexProgress.log(`[skip] ${entry.relativePath}`);
        skipLogged++;
      }
      continue;
    }

    if (!shouldQueueForInsert(row, marketIndexState)) {
      continue;
    }

    batch.push(row);
    if (batch.length >= config.batchSize) {
      const res = db.insertFiles(batch);
      stats.inserted += res.inserted;
      stats.existing += res.existing;
      batch = [];
    }

    if (stats.seen % 10_000 === 0) {
      logProgress(stats, batch.length);
    }
  }

  if (batch.length) {
    const res = db.insertFiles(batch);
    stats.inserted += res.inserted;
    stats.existing += res.existing;
  }

  indexProgress.clear();
  return stats;
}

function buildMarketIndexState(db: Db): Map<string, MarketIndexState> | null {
  const indexedRanges = db.listIndexedMarketRanges();
  if (!indexedRanges.length) return null;
  const state = new Map<string, MarketIndexState>();
  for (const row of indexedRanges) {
    state.set(marketKey(row.collector, row.exchange, row.symbol), {
      endTs: row.endTs,
      updatedAt: row.updatedAt,
    });
  }
  return state;
}

function buildDirectoryPruner(
  marketIndexState: Map<string, MarketIndexState> | null,
): ((entry: DirectoryEntry) => Promise<boolean>) | undefined {
  if (!marketIndexState) return undefined;

  return async (entry: DirectoryEntry): Promise<boolean> => {
    const marketDir = parseMarketDirectory(entry.relativePath);
    if (!marketDir) return true;
    const state = marketIndexState.get(marketKey(marketDir.collector, marketDir.exchange, marketDir.symbol));
    if (!state) return true;

    try {
      const stat = await fs.stat(entry.fullPath);
      return stat.mtimeMs > state.updatedAt;
    } catch {
      return true;
    }
  };
}

function shouldQueueForInsert(row: IndexedFile, marketIndexState: Map<string, MarketIndexState> | null): boolean {
  if (!marketIndexState) return true;
  const state = marketIndexState.get(marketKey(row.collector, row.exchange, row.symbol));
  if (!state) return true;
  // Keep the max-start slot eligible; this preserves same-timestamp additions while still skipping historical rows.
  return row.startTs >= state.endTs;
}

function parseMarketDirectory(
  relativePath: string,
): { collector: string; exchange: string; symbol: string } | null {
  const segments = relativePath.split("/");
  if (segments.length !== 4) return null;
  const collector = segments[0];
  const exchange = segments[2].toUpperCase();
  let symbol = normalizeSymbol(exchange, segments[3]);
  if (!symbol) {
    return null;
  }

  return { collector, exchange, symbol };
}

function marketKey(collector: string, exchange: string, symbol: string): string {
  return `${collector}\u0000${exchange}\u0000${symbol}`;
}

function logProgress(stats: IndexStats, pendingBatch: number): void {
  indexProgress.update(
    `[index] progress seen=${stats.seen} inserted=${stats.inserted} existing=${stats.existing} conflicts=${stats.conflicts} skipped=${stats.skipped} pendingBatch=${pendingBatch}`,
  );
}
