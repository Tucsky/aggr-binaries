import type { Config } from "../config.js";
import type { Db } from "../db.js";
import { GapFixStatus } from "../events.js";
import type { GapWindow, RecoveredTrade, TradeRecoveryAdapter } from "./adapters/index.js";
import { ensureFlushTargetFile, mergeDirtyRange, resolveFlushTargetFile } from "./flushBatchTarget.js";
import {
  countRecoveredByEvent,
  logGapError,
  logGapRecovered,
} from "./groupHelpers.js";
import { mergeRecoveredTradesIntoFile } from "./merge.js";
import { patchBinariesForRecoveredTrades } from "./patch.js";
import { logFixgapsLine, setFixgapsProgress } from "./progress.js";
import type { GapFixEventRow } from "./queue.js";
import type { DirtyMarketRange } from "./rollup.js";
import type { FixGapsStats } from "./index.js";

const DEBUG_FIXGAPS = process.env.AGGR_FIXGAPS_DEBUG === "1";
const FLUSH_TRADE_LIMIT = 1_000_000;

interface GapWindowResolutionResult {
  windows: GapWindow[];
  unresolvedEventIds: number[];
}

/**
 * Mark all events in this file as missing_adapter and account them in stats.
 */
export function markMissingAdapter(
  rows: GapFixEventRow[],
  exchange: string,
  db: Db,
  stats: FixGapsStats,
  dryRun: boolean,
): void {
  const reason = `No adapter for exchange ${exchange}`;
  if (!dryRun) {
    db.updateGapFixStatus(
      rows.map((row) => ({
        id: row.id,
        status: GapFixStatus.MissingAdapter,
        error: reason,
      })),
    );
  }
  for (const row of rows) logGapError(row, reason);
  stats.missingAdapter += rows.length;
}

/**
 * Build adapter windows directly from persisted gap event payloads.
 */
export function extractResolvableWindows(
  fileLabel: string,
  rows: GapFixEventRow[],
): GapWindowResolutionResult {
  setFixgapsProgress(`[fixgaps] resolving windows ${fileLabel} ...`);
  const windows: GapWindow[] = [];
  const unresolvedEventIds: number[] = [];

  for (const row of rows) {
    const window = buildWindowFromEvent(row);
    if (window) {
      windows.push(window);
      continue;
    }
    unresolvedEventIds.push(row.id);
  }

  windows.sort((a, b) => (a.fromTs - b.fromTs) || (a.toTs - b.toTs) || (a.eventId - b.eventId));
  return { windows, unresolvedEventIds };
}

/**
 * Mark rows that could not produce a valid adapter window from event payloads.
 */
export function markUnresolvedWindowEvents(
  extraction: GapWindowResolutionResult,
  rowsById: Map<number, GapFixEventRow>,
  db: Db,
  stats: FixGapsStats,
  dryRun: boolean,
): void {
  if (!extraction.unresolvedEventIds.length) return;

  const reason = "Unable to resolve gap window from event payload";
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

/**
 * Call the exchange adapter for resolvable windows and map failures back to event ids.
 */
export async function recoverTradesForWindows(
  row: GapFixEventRow,
  fileLabel: string,
  windows: Array<{ eventId: number; fromTs: number; toTs: number }>,
  resolvableEventIds: Set<number>,
  rowsById: Map<number, GapFixEventRow>,
  adapter: TradeRecoveryAdapter,
  db: Db,
  stats: FixGapsStats,
  dryRun: boolean,
): Promise<RecoveredTrade[] | undefined> {
  try {
    setFixgapsProgress(`[fixgaps] recovering ${fileLabel} via ${adapter.name} (${windows.length} windows) ...`);
    const recovered = await adapter.recover({
      exchange: row.exchange,
      symbol: row.symbol,
      windows,
    });
    if (DEBUG_FIXGAPS) {
      logFixgapsLine(`[fixgaps/debug] adapter_done adapter=${adapter.name} path=${row.relative_path} recovered=${recovered.length}`);
    }
    return recovered;
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
      const failedRow = rowsById.get(id);
      if (failedRow) logGapError(failedRow, reason);
    }
    stats.adapterError += ids.length;
    return undefined;
  }
}

/**
 * Apply the same merge+patch flow in deterministic fixed-size recovery batches.
 * Batch target file is selected from the last trade timestamp in each batch.
 */
export async function mergeAndPatchRecoveredTradesInFlushBatches(
  recovered: RecoveredTrade[],
  config: Config,
  db: Db,
  row: GapFixEventRow,
  resolvableEventIds: Set<number>,
  rowsById: Map<number, GapFixEventRow>,
  stats: FixGapsStats,
  dryRun: boolean,
): Promise<DirtyMarketRange | undefined | null> {
  if (!recovered.length || dryRun) return undefined;

  let dirtyRange: DirtyMarketRange | undefined;
  for (let start = 0; start < recovered.length; start += FLUSH_TRADE_LIMIT) {
    const batch = recovered.slice(start, start + FLUSH_TRADE_LIMIT);
    if (!batch.length) continue;
    const lastTrade = batch[batch.length - 1];
    const target = resolveFlushTargetFile(row, lastTrade.ts);
    await ensureFlushTargetFile(row, db, target.relativePath, target.absolutePath);
    const targetRow: GapFixEventRow =
      target.relativePath === row.relative_path ? row : { ...row, relative_path: target.relativePath };
    const merged = await mergeAndPatchRecoveredTrades(
      batch,
      config,
      db,
      targetRow,
      target.absolutePath,
      target.label,
      resolvableEventIds,
      rowsById,
      stats,
      dryRun,
    );
    if (merged === null) return null;
    if (merged) dirtyRange = mergeDirtyRange(dirtyRange, merged);
  }
  return dirtyRange;
}

/**
 * Merge recovered trades into the raw file and patch base timeframe binaries.
 * Returns:
 * - `DirtyMarketRange` when patched trades produced known ts bounds
 * - `undefined` when no patch was needed
 * - `null` when merge/patch failed (errors already recorded)
 */
export async function mergeAndPatchRecoveredTrades(
  recovered: RecoveredTrade[],
  config: Config,
  db: Db,
  row: GapFixEventRow,
  filePath: string,
  fileLabel: string,
  resolvableEventIds: Set<number>,
  rowsById: Map<number, GapFixEventRow>,
  stats: FixGapsStats,
  dryRun: boolean,
): Promise<DirtyMarketRange | undefined | null> {
  if (!recovered.length || dryRun) return undefined;

  const tsBounds = recoveredTsBounds(recovered);
  try {
    setFixgapsProgress(`[fixgaps] merging ${fileLabel} (${recovered.length} recovered trades) ...`);
    const mergeResult = await mergeRecoveredTradesIntoFile(filePath, recovered);
    if (DEBUG_FIXGAPS) {
      logFixgapsLine(
        `[fixgaps/debug] merge_done path=${row.relative_path} input=${recovered.length} inserted=${mergeResult.inserted}`,
      );
    }

    setFixgapsProgress(
      `[fixgaps] patching ${fileLabel} ${config.timeframe} (${mergeResult.inserted} inserted / ${recovered.length} recovered) ...`,
    );
    const patchResult = await patchBinariesForRecoveredTrades(
      config,
      db,
      {
        collector: row.collector,
        exchange: row.exchange,
        symbol: row.symbol,
      },
      filePath,
      recovered,
      { timeframes: [config.timeframe] },
    );
    stats.binariesPatched += patchResult.patchedTimeframes;
    stats.recoveredTrades += mergeResult.inserted;
    if (DEBUG_FIXGAPS) {
      logFixgapsLine(`[fixgaps/debug] patch_done path=${row.relative_path} patched_timeframes=${patchResult.patchedTimeframes}`);
    }

    if (!tsBounds) return undefined;
    return {
      collector: row.collector,
      exchange: row.exchange,
      symbol: row.symbol,
      minTs: tsBounds.minTs,
      maxTs: tsBounds.maxTs,
    };
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
      const failedRow = rowsById.get(id);
      if (failedRow) logGapError(failedRow, reason);
    }
    stats.adapterError += ids.length;
    return null;
  }
}

/**
 * Finalize success path: per-event recovered counts + fixed status updates.
 */
export function markResolvedWindowEvents(
  windows: Array<{ eventId: number; fromTs: number; toTs: number }>,
  recovered: RecoveredTrade[],
  resolvableEventIds: Set<number>,
  rowsById: Map<number, GapFixEventRow>,
  db: Db,
  stats: FixGapsStats,
  dryRun: boolean,
): void {
  const resolvedIds = [...resolvableEventIds];
  const recoveredByEvent = countRecoveredByEvent(windows, recovered);
  for (const id of resolvedIds) {
    const row = rowsById.get(id);
    if (row) logGapRecovered(row, recoveredByEvent.get(id) ?? 0);
  }
  if (dryRun) return;

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

/**
 * Compute min/max timestamp bounds for recovered trades.
 */
function recoveredTsBounds(recovered: RecoveredTrade[]): { minTs: number; maxTs: number } | undefined {
  let minTs = Number.POSITIVE_INFINITY;
  let maxTs = Number.NEGATIVE_INFINITY;
  for (const trade of recovered) {
    if (trade.ts < minTs) minTs = trade.ts;
    if (trade.ts > maxTs) maxTs = trade.ts;
  }
  if (!Number.isFinite(minTs) || !Number.isFinite(maxTs)) return undefined;
  return { minTs, maxTs };
}

/**
 * Normalize unknown errors into deterministic status messages.
 */
function toErrorMessage(err: unknown, fallback: string): string {
  return err instanceof Error && err.message ? err.message : fallback;
}

function buildWindowFromEvent(row: GapFixEventRow): GapWindow | undefined {
  if (row.gap_end_ts === null || row.gap_ms === null || row.gap_ms <= 0) return undefined;
  if (!Number.isFinite(row.gap_end_ts) || !Number.isFinite(row.gap_ms)) return undefined;
  const fromTs = row.gap_end_ts - row.gap_ms;
  const toTs = row.gap_end_ts;
  if (!Number.isFinite(fromTs) || toTs <= fromTs) return undefined;
  return { eventId: row.id, fromTs, toTs };
}
