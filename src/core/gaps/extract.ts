import readline from "node:readline";
import { openTradeReadStream } from "./io.js";
import { type GapWindow } from "./adapters/index.js";
import type { GapFixEventRow } from "./queue.js";
import { parseTradeLine } from "../trades.js";

export interface GapWindowExtractionResult {
  windows: GapWindow[];
  unresolvedEventIds: number[];
}

export async function extractGapWindows(filePath: string, rows: GapFixEventRow[]): Promise<GapWindowExtractionResult> {
  const sortedRows = [...rows].sort((a, b) => (a.start_line - b.start_line) || (a.id - b.id));
  const windows: GapWindow[] = [];
  const hasWindow = new Set<number>();
  let cursor = 0;
  let lineNumber = 0;
  let lastValidTradeTs: number | undefined;

  const stream = await openTradeReadStream(filePath);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  for await (const line of rl) {
    lineNumber += 1;
    while (cursor < sortedRows.length && sortedRows[cursor].end_line < lineNumber) {
      cursor += 1;
    }

    const trade = parseTradeLine(line);
    if (!trade) continue;
    if (trade.liquidation) continue;

    let idx = cursor;
    while (idx < sortedRows.length && sortedRows[idx].start_line <= lineNumber) {
      const row = sortedRows[idx];
      if (
        lineNumber <= row.end_line &&
        lastValidTradeTs !== undefined &&
        lastValidTradeTs < trade.ts
      ) {
        windows.push({ eventId: row.id, fromTs: lastValidTradeTs, toTs: trade.ts });
        hasWindow.add(row.id);
      }
      idx += 1;
    }

    lastValidTradeTs = trade.ts;
  }

  rl.close();

  const unresolvedEventIds: number[] = [];
  for (const row of sortedRows) {
    if (hasWindow.has(row.id)) continue;
    const fallback = buildFallbackWindow(row);
    if (fallback) {
      windows.push(fallback);
      hasWindow.add(row.id);
      continue;
    }
    unresolvedEventIds.push(row.id);
  }

  windows.sort((a, b) => (a.fromTs - b.fromTs) || (a.toTs - b.toTs) || (a.eventId - b.eventId));
  return { windows, unresolvedEventIds };
}

function buildFallbackWindow(row: GapFixEventRow): GapWindow | undefined {
  if (row.gap_end_ts === null || row.gap_ms === null || row.gap_ms <= 0) return undefined;
  const fromTs = row.gap_end_ts - row.gap_ms;
  const toTs = row.gap_end_ts;
  if (!Number.isFinite(fromTs) || !Number.isFinite(toTs) || toTs <= fromTs) {
    return undefined;
  }
  return { eventId: row.id, fromTs, toTs };
}
