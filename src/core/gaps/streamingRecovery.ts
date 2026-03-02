import type { Config } from "../config.js";
import type { Db } from "../db.js";
import type { RecoveredTrade } from "./adapters/index.js";
import { mergeDirtyRange } from "./flushBatchTarget.js";
import { countRecoveredByEvent } from "./groupHelpers.js";
import type { FixGapsStats } from "./index.js";
import { FLUSH_TRADE_LIMIT, mergeAndPatchRecoveredTradesFlushBatch, STREAMING_ABORT_ERROR } from "./processFileGapBatchSteps.js";
import type { GapFixEventRow } from "./queue.js";
import type { DirtyMarketRange } from "./rollup.js";

export interface StreamingRecoveryContext {
  config: Config;
  db: Db;
  fileRow: GapFixEventRow;
  selectedWindows: Array<{ eventId: number; fromTs: number; toTs: number }>;
  resolvableEventIds: Set<number>;
  rowsById: Map<number, GapFixEventRow>;
  stats: FixGapsStats;
  dryRun: boolean;
}

/**
 * Mutable streaming accumulator for one (root_id, end_relative_path) file-group.
 * Centralized here so processFileGapBatch stays a thin orchestrator.
 */
export interface StreamingRecoveryAccumulator {
  pendingFlushBatch: RecoveredTrade[];
  recoveredByEvent: Map<number, number>;
  recoveredTotal: number;
  dirtyRange: DirtyMarketRange | undefined;
}

export function createStreamingRecoveryAccumulator(): StreamingRecoveryAccumulator {
  return {
    pendingFlushBatch: [],
    recoveredByEvent: new Map<number, number>(),
    recoveredTotal: 0,
    dirtyRange: undefined,
  };
}

/**
 * Callback wired into adapter.recover(). It increments counters and flushes deterministic 1M chunks.
 */
export function createStreamingRecoveredBatchHandler(
  accumulator: StreamingRecoveryAccumulator,
  context: StreamingRecoveryContext,
): (batch: RecoveredTrade[]) => Promise<void> {
  return async function onRecoveredBatch(batch: RecoveredTrade[]): Promise<void> {
    const ok = await ingestStreamingRecoveredBatch(accumulator, context, batch);
    if (!ok) {
      throw new Error(STREAMING_ABORT_ERROR);
    }
  };
}

/**
 * Ingest one streamed adapter batch. This may flush one or more full 1M chunks.
 */
export async function ingestStreamingRecoveredBatch(
  accumulator: StreamingRecoveryAccumulator,
  context: StreamingRecoveryContext,
  batch: RecoveredTrade[],
): Promise<boolean> {
  if (!batch.length) return true;
  accumulator.recoveredTotal += batch.length;
  mergeRecoveredByEvent(accumulator.recoveredByEvent, countRecoveredByEvent(context.selectedWindows, batch));
  if (context.dryRun) return true;

  let cursor = 0;
  if (accumulator.pendingFlushBatch.length) {
    const needed = FLUSH_TRADE_LIMIT - accumulator.pendingFlushBatch.length;
    const take = Math.min(needed, batch.length);
    for (; cursor < take; cursor += 1) {
      accumulator.pendingFlushBatch.push(batch[cursor] as RecoveredTrade);
    }
    if (accumulator.pendingFlushBatch.length === FLUSH_TRADE_LIMIT) {
      if (!await flushStreamingChunk(accumulator, context, accumulator.pendingFlushBatch)) return false;
      accumulator.pendingFlushBatch.length = 0;
    }
  }

  while (cursor + FLUSH_TRADE_LIMIT <= batch.length) {
    if (!await flushStreamingChunk(accumulator, context, batch.slice(cursor, cursor + FLUSH_TRADE_LIMIT))) return false;
    cursor += FLUSH_TRADE_LIMIT;
  }

  for (; cursor < batch.length; cursor += 1) {
    accumulator.pendingFlushBatch.push(batch[cursor] as RecoveredTrade);
  }
  return true;
}

/**
 * Finalize streaming mode by ingesting adapter tail + flushing remainder.
 */
export async function finalizeStreamingRecoveredBatches(
  accumulator: StreamingRecoveryAccumulator,
  context: StreamingRecoveryContext,
  recoveredTail: RecoveredTrade[],
): Promise<boolean> {
  if (recoveredTail.length) {
    if (!await ingestStreamingRecoveredBatch(accumulator, context, recoveredTail)) return false;
  }
  if (!context.dryRun && accumulator.pendingFlushBatch.length) {
    if (!await flushStreamingChunk(accumulator, context, accumulator.pendingFlushBatch)) return false;
    accumulator.pendingFlushBatch.length = 0;
  }
  return true;
}

async function flushStreamingChunk(
  accumulator: StreamingRecoveryAccumulator,
  context: StreamingRecoveryContext,
  batch: RecoveredTrade[],
): Promise<boolean> {
  const merged = await mergeAndPatchRecoveredTradesFlushBatch(
    batch,
    context.config,
    context.db,
    context.fileRow,
    context.selectedWindows,
    context.resolvableEventIds,
    context.rowsById,
    context.stats,
    context.dryRun,
  );
  if (merged === null) return false;
  if (merged) accumulator.dirtyRange = mergeDirtyRange(accumulator.dirtyRange, merged);
  return true;
}

function mergeRecoveredByEvent(target: Map<number, number>, delta: Map<number, number>): void {
  for (const [id, recovered] of delta) {
    target.set(id, (target.get(id) ?? 0) + recovered);
  }
}
