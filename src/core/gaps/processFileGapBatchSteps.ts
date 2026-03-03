import type { Config } from "../config.js";
import type { Db } from "../db.js";
import type { GapWindow, RecoveredBatchHandler, RecoveredTrade, TradeRecoveryAdapter } from "./adapters/index.js";
import { ensureFlushTargetFile, resolveFlushTargetFile } from "./flushBatchTarget.js";
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
import { GapFixStatus } from "../model.js";

const DEBUG_FIXGAPS = process.env.AGGR_FIXGAPS_DEBUG === "1";
const DAY_MS = 86_400_000;
export const MAX_RECOVER_GAP_DAYS = 60;
export const MAX_RECOVER_GAP_MS = MAX_RECOVER_GAP_DAYS * DAY_MS;
export const FLUSH_TRADE_LIMIT = 1_000_000;
export const STREAMING_ABORT_ERROR = "fixgaps_streaming_abort";

interface GapWindowResolutionResult {
  windows: GapWindow[];
  skippedLargeGapEventIds: number[];
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
  const skippedLargeGapEventIds: number[] = [];
  const unresolvedEventIds: number[] = [];

  for (const row of rows) {
    if (isGapTooLargeForRecovery(row.gap_ms)) {
      skippedLargeGapEventIds.push(row.id);
      continue;
    }
    const window = buildWindowFromEvent(row);
    if (window) {
      windows.push(window);
      continue;
    }
    unresolvedEventIds.push(row.id);
  }

  windows.sort((a, b) => (a.fromTs - b.fromTs) || (a.toTs - b.toTs) || (a.eventId - b.eventId));
  return { windows, skippedLargeGapEventIds, unresolvedEventIds };
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
 * Mark rows skipped by long-gap guard with a dedicated skip status.
 */
export function markSkippedLargeGapWindowEvents(
  extraction: GapWindowResolutionResult,
  rowsById: Map<number, GapFixEventRow>,
  db: Db,
  stats: FixGapsStats,
  dryRun: boolean,
): void {
  const skippedIds = extraction.skippedLargeGapEventIds;
  if (!skippedIds.length) return;

  for (const id of skippedIds) {
    const row = rowsById.get(id);
    if (row) logGapRecovered(row, 0);
  }
  if (dryRun) return;

  db.updateGapFixStatus(
    skippedIds.map((id) => ({
      id,
      status: GapFixStatus.SkippedLargeGap,
      error: null,
      recovered: 0,
    })),
  );
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
  onRecoveredBatch?: RecoveredBatchHandler,
): Promise<RecoveredTrade[] | undefined> {
  try {
    setFixgapsProgress(`[fixgaps] recovering ${fileLabel} via ${adapter.name} (${windows.length} windows) ...`);
    const recovered = await adapter.recover({
      exchange: row.exchange,
      symbol: row.symbol,
      windows,
      onRecoveredBatch,
    });
    if (DEBUG_FIXGAPS) {
      logFixgapsLine(
        `[fixgaps/debug] adapter_done adapter=${adapter.name} path=${row.end_relative_path} recovered=${recovered.length}`,
      );
    }
    return recovered;
  } catch (err) {
    if (err instanceof Error && err.message === STREAMING_ABORT_ERROR) {
      return undefined;
    }
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
 * Merge one deterministic flush batch by routing target from chunk bounds and persisted gap boundaries.
 */
export async function mergeAndPatchRecoveredTradesFlushBatch(
  batch: RecoveredTrade[],
  config: Config,
  db: Db,
  row: GapFixEventRow,
  selectedWindows: Array<{ eventId: number; fromTs: number; toTs: number }>,
  resolvableEventIds: Set<number>,
  rowsById: Map<number, GapFixEventRow>,
  stats: FixGapsStats,
  dryRun: boolean,
): Promise<DirtyMarketRange | undefined | null> {
  if (!batch.length || dryRun) return undefined;
  const chunkTsBounds = recoveredTsBounds(batch);
  if (!chunkTsBounds) return undefined;
  const routingRow = selectBatchRoutingRow(chunkTsBounds, selectedWindows, rowsById, row);
  const target = resolveFlushTargetFile(routingRow, chunkTsBounds.minTs, chunkTsBounds.maxTs);
  await ensureFlushTargetFile(row, db, target.relativePath, target.absolutePath);
  return mergeAndPatchRecoveredTrades(
    batch,
    config,
    db,
    row,
    target.relativePath,
    target.absolutePath,
    target.label,
    resolvableEventIds,
    rowsById,
    stats,
    dryRun,
  );
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
  fileRelativePath: string,
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
        `[fixgaps/debug] merge_done path=${fileRelativePath} input=${recovered.length} inserted=${mergeResult.inserted}`,
      );
    }

    setFixgapsProgress(
      `[fixgaps] patching ${fileLabel} ${config.timeframe} (${mergeResult.inserted} inserted) ...`,
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
      logFixgapsLine(
        `[fixgaps/debug] patch_done path=${fileRelativePath} patched_timeframes=${patchResult.patchedTimeframes}`,
      );
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
  recovered: RecoveredTrade[] | Map<number, number>,
  resolvableEventIds: Set<number>,
  rowsById: Map<number, GapFixEventRow>,
  db: Db,
  stats: FixGapsStats,
  dryRun: boolean,
): void {
  const resolvedIds = [...resolvableEventIds];
  const recoveredByEvent = recovered instanceof Map ? recovered : countRecoveredByEvent(windows, recovered);
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
 * Pick the gap row whose window overlaps this chunk so routing uses the correct boundary files.
 */
function selectBatchRoutingRow(
  chunk: { minTs: number; maxTs: number },
  windows: Array<{ eventId: number; fromTs: number; toTs: number }>,
  rowsById: Map<number, GapFixEventRow>,
  fallback: GapFixEventRow,
): GapFixEventRow {
  for (const window of windows) {
    if (chunk.maxTs < window.fromTs) break;
    if (chunk.minTs > window.toTs) continue;
    const row = rowsById.get(window.eventId);
    if (row) return row;
  }
  return fallback;
}

/**
 * Normalize unknown errors into deterministic status messages.
 */
function toErrorMessage(err: unknown, fallback: string): string {
  return err instanceof Error && err.message ? err.message : fallback;
}

function buildWindowFromEvent(row: GapFixEventRow): GapWindow | undefined {
  if (!Number.isFinite(row.start_ts) || !Number.isFinite(row.end_ts)) return undefined;
  const fromTs = row.start_ts;
  const toTs = row.end_ts;
  if (!Number.isFinite(fromTs) || toTs <= fromTs) return undefined;
  return { eventId: row.id, fromTs, toTs };
}

function isGapTooLargeForRecovery(gapMs: number | null): boolean {
  return gapMs !== null && Number.isFinite(gapMs) && gapMs > MAX_RECOVER_GAP_MS;
}
