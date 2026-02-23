import path from "node:path";
import type { Config } from "../config.js";
import type { Db } from "../db.js";
import { GapFixStatus } from "../events.js";
import { formatElapsedDhms } from "../../shared/elapsed.js";
import { createDefaultAdapterRegistry, type AdapterRegistry } from "./adapters/index.js";
import { extractGapWindows } from "./extract.js";
import { mergeRecoveredTradesIntoFile } from "./merge.js";
import { patchBinariesForRecoveredTrades } from "./patch.js";
import { clearFixgapsProgress, logFixgapsLine, setFixgapsProgress, setFixgapsProgressContext } from "./progress.js";
import { iterateGapFixEvents, type GapFixEventRow } from "./queue.js";
import { mergeDirtyMarketRange, rollupHigherTimeframesFromBase, type DirtyMarketRange } from "./rollup.js";

const DEBUG_FIXGAPS = process.env.AGGR_FIXGAPS_DEBUG === "1";

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
  // Track per-market dirty ranges so higher-timeframe rollups run once per market.
  const dirtyByMarket = new Map<string, DirtyMarketRange>();

  let currentGroup: GapFixEventRow[] = [];
  let currentKey = "";

  for (const row of queue) {
    stats.selectedEvents += 1;
    const key = `${row.root_id}|${row.relative_path}`;
    if (!currentGroup.length) {
      currentGroup.push(row);
      currentKey = key;
      continue;
    }
    if (key === currentKey) {
      currentGroup.push(row);
      continue;
    }

    const dirty = await processGroup(currentGroup, config, db, adapterRegistry, stats, options.dryRun === true);
    if (dirty) mergeDirtyMarketRange(dirtyByMarket, dirty);
    currentGroup = [row];
    currentKey = key;
  }

  if (currentGroup.length) {
    const dirty = await processGroup(currentGroup, config, db, adapterRegistry, stats, options.dryRun === true);
    if (dirty) mergeDirtyMarketRange(dirtyByMarket, dirty);
  }

  if (!options.dryRun && dirtyByMarket.size) {
    for (const dirty of dirtyByMarket.values()) {
      setFixgapsProgress(`[fixgaps] rolling up ${dirty.collector}/${dirty.exchange}/${dirty.symbol} higher timeframes ...`);
      const rollup = await rollupHigherTimeframesFromBase(config, db, dirty);
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

async function processGroup(
  rows: GapFixEventRow[],
  config: Config,
  db: Db,
  adapterRegistry: AdapterRegistry,
  stats: FixGapsStats,
  dryRun: boolean,
): Promise<DirtyMarketRange | undefined> {
  if (!rows.length) return undefined;
  stats.processedFiles += 1;
  const rowsById = new Map<number, GapFixEventRow>(rows.map((row) => [row.id, row]));

  const first = rows[0];
  setFixgapsProgressContext(formatGapContext(first));
  try {
    if (DEBUG_FIXGAPS) {
      logFixgapsLine(
        `[fixgaps/debug] file_start exchange=${first.exchange} symbol=${first.symbol} path=${first.relative_path} events=${rows.length}`,
      );
    }

    const adapter = adapterRegistry.getAdapter(first.exchange);
    if (!adapter) {
      const reason = `No adapter for exchange ${first.exchange}`;
      if (!dryRun) {
        db.updateGapFixStatus(
          rows.map((row) => ({
            id: row.id,
            status: GapFixStatus.MissingAdapter,
            error: reason,
          })),
        );
      }
      for (const row of rows) {
        logGapError(row, reason);
      }
      stats.missingAdapter += rows.length;
      return undefined;
    }

    const filePath = path.join(first.root_path, first.relative_path);
    const fileLabel = formatFileProgressLabel(first);
    let extraction;
    try {
      setFixgapsProgress(`[fixgaps] scanning ${fileLabel} ...`);
      extraction = await extractGapWindows(filePath, rows);
    } catch (err) {
      if (!dryRun) {
        db.updateGapFixStatus(
          rows.map((row) => ({
            id: row.id,
            status: GapFixStatus.AdapterError,
            error: toErrorMessage(err, "Failed to extract gap windows"),
          })),
        );
      }
      stats.adapterError += rows.length;
      return undefined;
    }
    if (DEBUG_FIXGAPS) {
      logFixgapsLine(
        `[fixgaps/debug] windows path=${first.relative_path} resolvable=${extraction.windows.length} unresolved=${extraction.unresolvedEventIds.length}`,
      );
    }

    if (extraction.unresolvedEventIds.length) {
      const reason = "Unable to resolve event lines into gap windows";
      if (!dryRun) {
        db.updateGapFixStatus(
          extraction.unresolvedEventIds.map((id) => ({
            id,
            status: GapFixStatus.AdapterError,
            error: reason,
          })),
        );
      }
      for (const id of extraction.unresolvedEventIds) {
        const row = rowsById.get(id);
        if (row) logGapError(row, reason);
      }
      stats.adapterError += extraction.unresolvedEventIds.length;
    }

    const selectedWindows = extraction.windows;
    const resolvableEventIds = new Set<number>(selectedWindows.map((w) => w.eventId));
    if (!resolvableEventIds.size) {
      return undefined;
    }

    let recovered;
    try {
      setFixgapsProgress(`[fixgaps] recovering ${fileLabel} via ${adapter.name} (${selectedWindows.length} windows) ...`);
      recovered = await adapter.recover({
        exchange: first.exchange,
        symbol: first.symbol,
        windows: selectedWindows,
      });
    } catch (err) {
      const ids = [...resolvableEventIds];
      const reason = toErrorMessage(err, `Adapter ${adapter.name} failed`);
      if (!dryRun) {
        db.updateGapFixStatus(
          ids.map((id) => ({
            id,
            status: GapFixStatus.AdapterError,
            error: reason,
          })),
        );
      }
      for (const id of ids) {
        const row = rowsById.get(id);
        if (row) {
          logGapError(row, reason);
        }
      }
      stats.adapterError += ids.length;
      return undefined;
    }
    if (DEBUG_FIXGAPS) {
      logFixgapsLine(
        `[fixgaps/debug] adapter_done adapter=${adapter.name} path=${first.relative_path} recovered=${recovered.length}`,
      );
    }

    if (recovered.length && dryRun) stats.recoveredTrades += recovered.length;
    let recoveredMinTs = Number.POSITIVE_INFINITY;
    let recoveredMaxTs = Number.NEGATIVE_INFINITY;
    for (const trade of recovered) {
      if (trade.ts < recoveredMinTs) recoveredMinTs = trade.ts;
      if (trade.ts > recoveredMaxTs) recoveredMaxTs = trade.ts;
    }

    let dirtyRange: DirtyMarketRange | undefined;
    if (recovered.length && !dryRun) {
      try {
        setFixgapsProgress(`[fixgaps] merging ${fileLabel} (${recovered.length} recovered trades) ...`);
        const mergeResult = await mergeRecoveredTradesIntoFile(filePath, recovered);
        if (DEBUG_FIXGAPS) {
          logFixgapsLine(
            `[fixgaps/debug] merge_done path=${first.relative_path} inserted=${mergeResult.inserted} deduped=${recovered.length - mergeResult.inserted}`,
          );
        }
        setFixgapsProgress(
          `[fixgaps] patching ${fileLabel} ${config.timeframe} (${mergeResult.inserted} inserted / ${recovered.length} recovered) ...`,
        );
        const patchResult = await patchBinariesForRecoveredTrades(
          config,
          db,
          {
            collector: first.collector,
            exchange: first.exchange,
            symbol: first.symbol,
          },
          filePath,
          recovered,
          { timeframes: [config.timeframe] },
        );
        stats.binariesPatched += patchResult.patchedTimeframes;
        stats.recoveredTrades += mergeResult.inserted;
        if (Number.isFinite(recoveredMinTs) && Number.isFinite(recoveredMaxTs)) {
          dirtyRange = {
            collector: first.collector,
            exchange: first.exchange,
            symbol: first.symbol,
            minTs: recoveredMinTs,
            maxTs: recoveredMaxTs,
          };
        }
        if (DEBUG_FIXGAPS) {
          logFixgapsLine(
            `[fixgaps/debug] patch_done path=${first.relative_path} patched_timeframes=${patchResult.patchedTimeframes}`,
          );
        }
      } catch (err) {
        const ids = [...resolvableEventIds];
        const reason = toErrorMessage(err, "Failed to merge or patch recovered trades");
        if (!dryRun) {
          db.updateGapFixStatus(
            ids.map((id) => ({
              id,
              status: GapFixStatus.AdapterError,
              error: reason,
            })),
          );
        }
        for (const id of ids) {
          const row = rowsById.get(id);
          if (row) logGapError(row, reason);
        }
        stats.adapterError += ids.length;
        return undefined;
      }
    }

    const resolvedIds = [...resolvableEventIds];
    const recoveredByEvent = countRecoveredByEvent(selectedWindows, recovered);
    for (const id of resolvedIds) {
      const row = rowsById.get(id);
      if (!row) continue;
      logGapRecovered(row, recoveredByEvent.get(id) ?? 0);
    }
    if (!dryRun) {
      db.updateGapFixStatus(
        resolvedIds.map((id) => ({
          id,
          status: GapFixStatus.Fixed,
          error: null,
          recovered: recoveredByEvent.get(id) ?? 0,
        })),
      );
      stats.fixedEvents += resolvedIds.length;
    }
    if (DEBUG_FIXGAPS && !dryRun) {
      logFixgapsLine(`[fixgaps/debug] file_done path=${first.relative_path} fixed=${resolvedIds.length}`);
    }
    return dirtyRange;
  } finally {
    setFixgapsProgressContext();
  }
}

function toErrorMessage(err: unknown, fallback: string): string {
  return err instanceof Error && err.message ? err.message : fallback;
}

function countRecoveredByEvent(
  windows: Array<{ eventId: number; fromTs: number; toTs: number }>,
  recovered: Array<{ ts: number }>,
): Map<number, number> {
  const counts = new Map<number, number>();
  const sortedWindows = [...windows].sort((a, b) => (a.fromTs - b.fromTs) || (a.toTs - b.toTs) || (a.eventId - b.eventId));
  for (const window of sortedWindows) if (!counts.has(window.eventId)) counts.set(window.eventId, 0);
  if (!recovered.length || !sortedWindows.length) return counts;

  for (const trade of recovered) {
    for (const window of sortedWindows) {
      if (trade.ts <= window.fromTs) break;
      if (trade.ts >= window.toTs) continue;
      counts.set(window.eventId, (counts.get(window.eventId) ?? 0) + 1);
      break;
    }
  }
  return counts;
}

function logGapRecovered(row: GapFixEventRow, recovered: number): void {
  const miss = row.gap_miss === null ? "?" : String(row.gap_miss);
  logFixgapsLine(`[fixgaps] ${formatGapContext(row)} : recovered ${recovered} / ${miss}`);
}

function logGapError(row: GapFixEventRow, reason: string): void {
  const sanitized = reason.replaceAll("\n", " ").replaceAll("\r", " ");
  logFixgapsLine(`[fixgaps] ${formatGapContext(row)} : error (${sanitized})`);
}

function formatGapContext(row: GapFixEventRow): string {
  const startTs = gapStartTs(row);
  const dayTime = formatGapDayTimeUtc(startTs);
  const date = dayTime?.date ?? "unknown";
  const time = dayTime?.time ?? "unknown";
  const gapElapsed = row.gap_ms === null || !Number.isFinite(row.gap_ms) ? "?" : formatElapsedDhms(row.gap_ms);
  return `[${row.exchange}/${row.symbol}/${date}] ${gapElapsed} gap @ ${time}`;
}

function gapStartTs(row: GapFixEventRow): number | null {
  if (row.gap_end_ts === null || !Number.isFinite(row.gap_end_ts)) return null;
  if (row.gap_ms === null || !Number.isFinite(row.gap_ms)) return row.gap_end_ts;
  const start = row.gap_end_ts - row.gap_ms;
  return Number.isFinite(start) ? start : row.gap_end_ts;
}

function formatGapDayTimeUtc(ts: number | null): { date: string; time: string } | undefined {
  if (ts === null || !Number.isFinite(ts)) return undefined;
  const d = new Date(ts);
  const year = d.getUTCFullYear();
  const month = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  const hour = String(d.getUTCHours()).padStart(2, "0");
  const minute = String(d.getUTCMinutes()).padStart(2, "0");
  return { date: `${year}-${month}-${day}`, time: `${hour}:${minute}` };
}

function formatFileProgressLabel(row: GapFixEventRow): string {
  return path.posix.basename(row.relative_path);
}
