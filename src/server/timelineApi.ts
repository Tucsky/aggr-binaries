import type http from "node:http";
import type { Db } from "../core/db.js";
import { createTimelineActionsApiHandler, type TimelineActionsApiOptions } from "./timelineActionsApi.js";

export interface TimelineMarket {
  collector: string;
  exchange: string;
  symbol: string;
  timeframe: string;
  startTs: number;
  endTs: number;
  indexedStartTs?: number | null;
  indexedEndTs?: number | null;
  processedStartTs?: number | null;
  processedEndTs?: number | null;
}

export interface TimelineEvent {
  id: number;
  collector: string;
  exchange: string;
  symbol: string;
  relativePath: string;
  eventType: string;
  gapFixStatus: string | null;
  gapFixRecovered: number | null;
  ts: number;
  startLine: number;
  endLine: number;
  gapMs: number | null;
  gapMiss: number | null;
}

export interface TimelineEventFilter {
  collector?: string;
  exchange?: string;
  symbol?: string;
  startTs: number;
  endTs: number;
}

export interface TimelineMarketsResult {
  markets: TimelineMarket[];
  timeframes: string[];
}

interface TimelineIndexedMarketRow {
  collector: string;
  exchange: string;
  symbol: string;
  start_ts: number;
  end_ts: number;
}

interface TimelineRegistryMarketRow {
  collector: string;
  exchange: string;
  symbol: string;
  timeframe: string;
  start_ts: number;
  end_ts: number;
}

export type HttpApiHandler = (
  req: http.IncomingMessage,
  res: http.ServerResponse,
  url: URL,
) => Promise<boolean> | boolean;

export function listTimelineMarkets(db: Db, timeframe?: string): TimelineMarketsResult {
  const timeframes = listTimelineTimeframes(db);
  const selected = timeframe?.trim();
  const indexedRows = listTimelineIndexedMarketRanges(db);
  const registryRows = selected
    ? listTimelineRegistryRangesByTimeframe(db, selected)
    : listTimelineRegistryRangesAllTimeframes(db);
  const fallbackTimeframe = selected || "ALL";
  const markets = mergeTimelineMarketRanges(indexedRows, registryRows, fallbackTimeframe);
  return { markets, timeframes };
}

export function listTimelineEvents(db: Db, filter: TimelineEventFilter): TimelineEvent[] {
  if (!Number.isFinite(filter.startTs) || !Number.isFinite(filter.endTs)) {
    throw new Error("startTs and endTs are required");
  }
  if (filter.endTs < filter.startTs) {
    throw new Error("endTs must be >= startTs");
  }

  const where: string[] = [
    "COALESCE(e.gap_end_ts, f.start_ts) IS NOT NULL",
    "COALESCE(e.gap_end_ts, f.start_ts) >= :startTs",
    "COALESCE(e.gap_end_ts, f.start_ts) <= :endTs",
  ];
  const params: Record<string, string | number> = {
    startTs: Math.floor(filter.startTs),
    endTs: Math.floor(filter.endTs),
  };

  if (filter.collector) {
    where.push("e.collector = :collector");
    params.collector = filter.collector.trim().toUpperCase();
  }
  if (filter.exchange) {
    where.push("e.exchange = :exchange");
    params.exchange = filter.exchange.trim().toUpperCase();
  }
  if (filter.symbol) {
    where.push("LOWER(e.symbol) LIKE :symbolLike");
    params.symbolLike = `%${filter.symbol.trim().toLowerCase()}%`;
  }

  const rows =
    (db.db
      .prepare(
        `SELECT e.id, e.collector, e.exchange, e.symbol, e.relative_path, e.event_type, e.gap_fix_status, e.gap_fix_recovered,
                COALESCE(e.gap_end_ts, f.start_ts) AS ts,
                e.start_line, e.end_line, e.gap_ms, e.gap_miss
         FROM events e
         LEFT JOIN files f ON f.root_id = e.root_id AND f.relative_path = e.relative_path
         WHERE ${where.join(" AND ")}
         ORDER BY e.collector, e.exchange, e.symbol, ts, e.id;`,
      )
      .all(params) as Array<{
      id: number;
      collector: string;
      exchange: string;
      symbol: string;
      relative_path: string;
      event_type: string;
      gap_fix_status: string | null;
      gap_fix_recovered: number | null;
      ts: number;
      start_line: number;
      end_line: number;
      gap_ms: number | null;
      gap_miss: number | null;
    }>) ?? [];

  return rows.map((row) => ({
    id: row.id,
    collector: row.collector.toUpperCase(),
    exchange: row.exchange.toUpperCase(),
    symbol: row.symbol,
    relativePath: row.relative_path,
    eventType: row.event_type,
    gapFixStatus: row.gap_fix_status,
    gapFixRecovered: row.gap_fix_recovered,
    ts: row.ts,
    startLine: row.start_line,
    endLine: row.end_line,
    gapMs: row.gap_ms,
    gapMiss: row.gap_miss,
  }));
}

export function createTimelineApiHandler(db: Db, actionOptions: TimelineActionsApiOptions): HttpApiHandler {
  const timelineActionsHandler = createTimelineActionsApiHandler(db, actionOptions);
  return async (req, res, url) => {
    if (!url.pathname.startsWith("/api/timeline/")) {
      return false;
    }
    if (url.pathname === "/api/timeline/actions") {
      return timelineActionsHandler(req, res, url);
    }
    if (req.method !== "GET") {
      writeJson(res, 405, { error: "Method not allowed" });
      return true;
    }

    if (url.pathname === "/api/timeline/markets") {
      const timeframe = emptyToUndefined(url.searchParams.get("timeframe"));
      writeJson(res, 200, listTimelineMarkets(db, timeframe));
      return true;
    }
    if (url.pathname === "/api/timeline/events") {
      const startTs = parseNumberParam(url.searchParams.get("startTs"));
      const endTs = parseNumberParam(url.searchParams.get("endTs"));
      if (startTs === null || endTs === null) {
        writeJson(res, 400, { error: "startTs and endTs are required numeric query params" });
        return true;
      }
      try {
        const events = listTimelineEvents(db, {
          collector: emptyToUndefined(url.searchParams.get("collector")),
          exchange: emptyToUndefined(url.searchParams.get("exchange")),
          symbol: emptyToUndefined(url.searchParams.get("symbol")),
          startTs,
          endTs,
        });
        writeJson(res, 200, { events });
      } catch (err) {
        writeJson(res, 400, { error: err instanceof Error ? err.message : "Invalid timeline query" });
      }
      return true;
    }

    writeJson(res, 404, { error: "Not found" });
    return true;
  };
}

function writeJson(res: http.ServerResponse, status: number, payload: unknown): void {
  res.writeHead(status, {
    "Content-Type": "application/json",
  });
  res.end(JSON.stringify(payload));
}

function parseNumberParam(raw: string | null): number | null {
  if (raw === null) return null;
  const value = Number(raw);
  if (!Number.isFinite(value)) return null;
  return Math.floor(value);
}

function emptyToUndefined(raw: string | null): string | undefined {
  if (raw === null) return undefined;
  const trimmed = raw.trim();
  return trimmed ? trimmed : undefined;
}

function listTimelineTimeframes(db: Db): string[] {
  const rows =
    (db.db
      .prepare("SELECT DISTINCT timeframe FROM registry ORDER BY timeframe;")
      .all() as Array<{ timeframe: string }>) ?? [];
  return rows.map((row) => row.timeframe);
}

function listTimelineIndexedMarketRanges(db: Db): TimelineIndexedMarketRow[] {
  return (
    (db.db
      .prepare(
        // files only stores logical start_ts, so indexed-only rows use min/max start_ts as their visible bounds.
        `SELECT collector, exchange, symbol, MIN(start_ts) AS start_ts, MAX(start_ts) AS end_ts
         FROM files
         GROUP BY collector, exchange, symbol
         ORDER BY collector, exchange, symbol;`,
      )
      .all() as unknown as TimelineIndexedMarketRow[]) ?? []
  );
}

function listTimelineRegistryRangesByTimeframe(db: Db, timeframe: string): TimelineRegistryMarketRow[] {
  return (
    (db.db
      .prepare(
        `SELECT collector, exchange, symbol, timeframe, start_ts, end_ts
         FROM registry
         WHERE timeframe = :timeframe
         ORDER BY collector, exchange, symbol;`,
      )
      .all({ timeframe }) as unknown as TimelineRegistryMarketRow[]) ?? []
  );
}

function listTimelineRegistryRangesAllTimeframes(db: Db): TimelineRegistryMarketRow[] {
  return (
    (db.db
      .prepare(
        `SELECT collector, exchange, symbol, 'ALL' AS timeframe, MIN(start_ts) AS start_ts, MAX(end_ts) AS end_ts
         FROM registry
         GROUP BY collector, exchange, symbol
         ORDER BY collector, exchange, symbol;`,
      )
      .all() as unknown as TimelineRegistryMarketRow[]) ?? []
  );
}

function mergeTimelineMarketRanges(
  indexedRows: TimelineIndexedMarketRow[],
  registryRows: TimelineRegistryMarketRow[],
  indexedFallbackTimeframe: string,
): TimelineMarket[] {
  // Both inputs are pre-sorted by market identity. For shared keys, registry carries processed range and
  // indexed carries raw source extent, so the merged row can render split coverage.
  const merged: TimelineMarket[] = [];
  let indexedIdx = 0;
  let registryIdx = 0;

  while (indexedIdx < indexedRows.length || registryIdx < registryRows.length) {
    if (indexedIdx >= indexedRows.length) {
      merged.push(mapRegistryRow(registryRows[registryIdx]));
      registryIdx += 1;
      continue;
    }
    if (registryIdx >= registryRows.length) {
      merged.push(mapIndexedRow(indexedRows[indexedIdx], indexedFallbackTimeframe));
      indexedIdx += 1;
      continue;
    }

    const indexedRow = indexedRows[indexedIdx];
    const registryRow = registryRows[registryIdx];
    const cmp = compareMarketIdentity(indexedRow, registryRow);
    if (cmp === 0) {
      merged.push(mapMergedRow(indexedRow, registryRow));
      indexedIdx += 1;
      registryIdx += 1;
      continue;
    }
    if (cmp < 0) {
      merged.push(mapIndexedRow(indexedRow, indexedFallbackTimeframe));
      indexedIdx += 1;
      continue;
    }
    merged.push(mapRegistryRow(registryRow));
    registryIdx += 1;
  }

  return merged;
}

function mapIndexedRow(row: TimelineIndexedMarketRow, timeframe: string): TimelineMarket {
  return {
    collector: row.collector.toUpperCase(),
    exchange: row.exchange.toUpperCase(),
    symbol: row.symbol,
    timeframe,
    startTs: row.start_ts,
    endTs: row.end_ts,
    indexedStartTs: row.start_ts,
    indexedEndTs: row.end_ts,
    processedStartTs: null,
    processedEndTs: null,
  };
}

function mapRegistryRow(row: TimelineRegistryMarketRow): TimelineMarket {
  return {
    collector: row.collector.toUpperCase(),
    exchange: row.exchange.toUpperCase(),
    symbol: row.symbol,
    timeframe: row.timeframe,
    startTs: row.start_ts,
    endTs: row.end_ts,
    indexedStartTs: null,
    indexedEndTs: null,
    processedStartTs: row.start_ts,
    processedEndTs: row.end_ts,
  };
}

function mapMergedRow(indexedRow: TimelineIndexedMarketRow, registryRow: TimelineRegistryMarketRow): TimelineMarket {
  const startTs = indexedRow.start_ts < registryRow.start_ts ? indexedRow.start_ts : registryRow.start_ts;
  const endTs = indexedRow.end_ts > registryRow.end_ts ? indexedRow.end_ts : registryRow.end_ts;
  return {
    collector: registryRow.collector.toUpperCase(),
    exchange: registryRow.exchange.toUpperCase(),
    symbol: registryRow.symbol,
    timeframe: registryRow.timeframe,
    startTs,
    endTs,
    indexedStartTs: indexedRow.start_ts,
    indexedEndTs: indexedRow.end_ts,
    processedStartTs: registryRow.start_ts,
    processedEndTs: registryRow.end_ts,
  };
}

function compareMarketIdentity(
  a: Pick<TimelineIndexedMarketRow, "collector" | "exchange" | "symbol">,
  b: Pick<TimelineIndexedMarketRow, "collector" | "exchange" | "symbol">,
): number {
  if (a.collector < b.collector) return -1;
  if (a.collector > b.collector) return 1;
  if (a.exchange < b.exchange) return -1;
  if (a.exchange > b.exchange) return 1;
  if (a.symbol < b.symbol) return -1;
  if (a.symbol > b.symbol) return 1;
  return 0;
}
