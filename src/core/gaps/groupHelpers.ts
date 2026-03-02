import path from "node:path";
import { formatElapsedDhms } from "../../shared/elapsed.js";
import { logFixgapsLine } from "./progress.js";
import type { GapFixEventRow } from "./queue.js";

/**
 * Count recovered trades per event window for final per-event recovered stats.
 */
export function countRecoveredByEvent(
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

/**
 * Log a successful gap-recovery line with contextual metadata.
 */
export function logGapRecovered(row: GapFixEventRow, recovered: number): void {
  const miss = row.gap_miss === null ? "?" : String(row.gap_miss);
  logFixgapsLine(`[fixgaps] ${formatGapContext(row)} : recovered ${recovered} / ${miss}`);
}

/**
 * Log a normalized per-gap error line.
 */
export function logGapError(row: GapFixEventRow, reason: string): void {
  const sanitized = reason.replaceAll("\n", " ").replaceAll("\r", " ");
  logFixgapsLine(`[fixgaps] ${formatGapContext(row)} : error (${sanitized})`);
}

/**
 * Build human-readable gap context used in progress and log lines.
 */
export function formatGapContext(row: GapFixEventRow): string {
  const startTs = gapStartTs(row);
  const dayTime = formatGapDayTimeUtc(startTs);
  const date = dayTime?.date ?? "unknown";
  const time = dayTime?.time ?? "unknown";
  const gapElapsed = row.gap_ms === null || !Number.isFinite(row.gap_ms) ? "?" : formatElapsedDhms(row.gap_ms);
  return `[${row.exchange}/${row.symbol}/${row.id}] ${gapElapsed} gap @ ${date}T${time}`;
}

/**
 * Build compact progress label for a file path.
 */
export function formatFileProgressLabel(row: GapFixEventRow): string {
  return path.posix.basename(row.end_relative_path);
}

function gapStartTs(row: GapFixEventRow): number | null {
  if (!Number.isFinite(row.start_ts)) return null;
  return row.start_ts;
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
