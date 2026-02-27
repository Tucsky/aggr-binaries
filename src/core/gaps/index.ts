import type { Config } from "../config.js";
import type { Db } from "../db.js";
import { createDefaultAdapterRegistry, type AdapterRegistry } from "./adapters/index.js";
import { clearFixgapsProgress, logFixgapsLine, setFixgapsProgress } from "./progress.js";
import { iterateGapFixEvents, type GapFixEventRow } from "./queue.js";
import { processFileGapBatch } from "./processFileGapBatch.js";
import { mergeDirtyMarketRange, rollupHigherTimeframesFromBase, type DirtyMarketRange } from "./rollup.js";

export interface FixGapsOptions {
  limit?: number;
  retryStatuses?: string[];
  adapterRegistry?: AdapterRegistry;
  dryRun?: boolean;
  id?: number;
}

export interface FixGapsStats {
  selectedEvents: number;
  processedFiles: number;
  recoveredTrades: number;
  deletedEvents: number;
  fixedEvents: number;
  missingAdapter: number;
  adapterError: number;
  binariesPatched: number;
}

// Fixgaps processes one file at a time because a single file can contain many gap events.
// Queue rows are grouped by (root_id, relative_path), then each file-group is handled once.
export async function runFixGaps(config: Config, db: Db, options: FixGapsOptions = {}): Promise<FixGapsStats> {
  const start = Date.now();
  const stats: FixGapsStats = {
    selectedEvents: 0,
    processedFiles: 0,
    recoveredTrades: 0,
    deletedEvents: 0,
    fixedEvents: 0,
    missingAdapter: 0,
    adapterError: 0,
    binariesPatched: 0,
  };

  const adapterRegistry = options.adapterRegistry ?? createDefaultAdapterRegistry();
  logFixgapsLine(
    `[fixgaps] start filters=${config.collector ?? "ALL"}/${config.exchange ?? "ALL"}/${config.symbol ?? "ALL"} id=${options.id ?? "ALL"} limit=${options.limit ?? "ALL"} retry=${options.retryStatuses?.join(",") ?? "NONE"} dry_run=${options.dryRun ? "1" : "0"}`,
  );

  const queue = iterateGapFixEvents(db, {
    collector: config.collector,
    exchange: config.exchange,
    symbol: config.symbol,
    limit: options.limit,
    retryStatuses: options.retryStatuses,
    id: options.id,
  });
  // "dirty market range" = timestamp bounds changed by recovered trades.
  // We accumulate these bounds per market so higher-timeframe rollups run once per market.
  const dirtyRangesByMarket = new Map<string, DirtyMarketRange>();
  const dryRun = options.dryRun === true;
  let pendingFileEvents: GapFixEventRow[] = [];
  let pendingFileKey = "";

  async function flushPendingFileEvents(): Promise<void> {
    if (!pendingFileEvents.length) return;
    const dirtyRange = await processFileGapBatch(pendingFileEvents, config, db, adapterRegistry, stats, dryRun);
    if (dirtyRange) mergeDirtyMarketRange(dirtyRangesByMarket, dirtyRange);
    pendingFileEvents = [];
    pendingFileKey = "";
  }

  for (const row of queue) {
    stats.selectedEvents += 1;
    const fileKey = `${row.root_id}|${row.relative_path}`;
    if (!pendingFileEvents.length) {
      pendingFileEvents = [row];
      pendingFileKey = fileKey;
      continue;
    }
    if (fileKey === pendingFileKey) {
      pendingFileEvents.push(row);
      continue;
    }
    await flushPendingFileEvents();
    pendingFileEvents = [row];
    pendingFileKey = fileKey;
  }

  // Flush the last file-group after the loop (there is no key-change after the final row).
  await flushPendingFileEvents();

  if (!dryRun && dirtyRangesByMarket.size) {
    for (const dirtyRange of dirtyRangesByMarket.values()) {
      setFixgapsProgress(
        `[fixgaps] rolling up ${dirtyRange.collector}/${dirtyRange.exchange}/${dirtyRange.symbol} higher timeframes ...`,
      );
      const rollup = await rollupHigherTimeframesFromBase(config, db, dirtyRange);
      stats.binariesPatched += rollup.patchedTimeframes;
    }
  }

  const elapsed = ((Date.now() - start) / 1000).toFixed(2);
  logFixgapsLine(
    `[fixgaps] complete selected=${stats.selectedEvents} files=${stats.processedFiles} recovered=${stats.recoveredTrades} fixed=${stats.fixedEvents} deleted=${stats.deletedEvents} missing_adapter=${stats.missingAdapter} adapter_error=${stats.adapterError} patched_timeframes=${stats.binariesPatched} elapsed=${elapsed}s`,
  );
  clearFixgapsProgress();

  return stats;
}
