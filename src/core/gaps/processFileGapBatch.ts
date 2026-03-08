import type { Config } from "../config.js";
import type { Db } from "../db.js";
import type { AdapterRegistry } from "./adapters/index.js";
import { formatFileProgressLabel, formatGapContext } from "./groupHelpers.js";
import { logFixgapsLine, setFixgapsProgressContext } from "./progress.js";
import type { GapFixEventRow } from "./queue.js";
import type { DirtyMarketRange } from "./rollup.js";
import type { FixGapsStats } from "./index.js";
import {
  extractResolvableWindows,
  markMissingAdapter,
  markResolvedWindowEvents,
  markSkippedLargeGapWindowEvents,
  markUnresolvedWindowEvents,
  recoverTradesForWindows,
} from "./processFileGapBatchSteps.js";
import {
  createStreamingRecoveredBatchHandler,
  createStreamingRecoveryAccumulator,
  finalizeStreamingRecoveredBatches,
} from "./streamingRecovery.js";

const DEBUG_FIXGAPS = process.env.AGGR_FIXGAPS_DEBUG === "1";

/**
 * Process all gap events that belong to a single raw input file.
 * "Batch" here means grouped by market + end_relative_path.
 */
export async function processFileGapBatch(
  fileGapEvents: GapFixEventRow[],
  config: Config,
  db: Db,
  adapterRegistry: AdapterRegistry,
  stats: FixGapsStats,
  dryRun: boolean,
): Promise<DirtyMarketRange | undefined> {
  if (!fileGapEvents.length) return undefined;
  stats.processedFiles += 1;
  const rowsById = new Map<number, GapFixEventRow>(fileGapEvents.map((row) => [row.id, row]));
  const fileRow = fileGapEvents[0];
  const fileLabel = formatFileProgressLabel(fileRow);

  setFixgapsProgressContext(formatGapContext(fileRow));
  try {
    if (DEBUG_FIXGAPS) {
      logFixgapsLine(
        `[fixgaps/debug] file_start exchange=${fileRow.exchange} symbol=${fileRow.symbol} path=${fileRow.end_relative_path} events=${fileGapEvents.length}`,
      );
    }

    const adapter = adapterRegistry.getAdapter(fileRow.exchange);
    if (!adapter) {
      markMissingAdapter(fileGapEvents, fileRow.exchange, db, stats, dryRun);
      return undefined;
    }

    const extraction = extractResolvableWindows(fileLabel, fileGapEvents);
    if (DEBUG_FIXGAPS) {
      logFixgapsLine(
        `[fixgaps/debug] windows path=${fileRow.end_relative_path} resolvable=${extraction.windows.length} skipped_large=${extraction.skippedLargeGapEventIds.length} unresolved=${extraction.unresolvedEventIds.length}`,
      );
    }

    markUnresolvedWindowEvents(extraction, rowsById, db, stats, dryRun);
    markSkippedLargeGapWindowEvents(extraction, rowsById, db, stats, dryRun);
    const selectedWindows = extraction.windows;
    const resolvableEventIds = new Set<number>(selectedWindows.map((window) => window.eventId));
    if (!resolvableEventIds.size) return undefined;

    const streamingAccumulator = createStreamingRecoveryAccumulator();
    const streamingContext = {
      config,
      db,
      fileRow,
      selectedWindows,
      resolvableEventIds,
      rowsById,
      stats,
      dryRun,
    };
    const onRecoveredBatch = createStreamingRecoveredBatchHandler(streamingAccumulator, streamingContext);

    const recovered = await recoverTradesForWindows(
      fileRow,
      fileLabel,
      selectedWindows,
      resolvableEventIds,
      rowsById,
      adapter,
      db,
      stats,
      dryRun,
      onRecoveredBatch,
    );
    if (!recovered) return undefined;
    if (!await finalizeStreamingRecoveredBatches(streamingAccumulator, streamingContext, recovered)) return undefined;
    if (dryRun && streamingAccumulator.recoveredTotal) stats.recoveredTrades += streamingAccumulator.recoveredTotal;

    markResolvedWindowEvents(
      selectedWindows,
      streamingAccumulator.recoveredByEvent,
      resolvableEventIds,
      rowsById,
      db,
      stats,
      dryRun,
    );
    if (DEBUG_FIXGAPS && !dryRun) {
      logFixgapsLine(`[fixgaps/debug] file_done path=${fileRow.end_relative_path} fixed=${resolvableEventIds.size}`);
    }
    return streamingAccumulator.dirtyRange;
  } finally {
    setFixgapsProgressContext();
  }
}
