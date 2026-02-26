import { TimelineMarketAction, type TimelineEvent, type TimelineMarket } from "./timelineTypes.js";

export interface TimelineMarketsResponse {
  markets: TimelineMarket[];
  timeframes: string[];
}

export interface TimelineMarketsQuery {
  timeframe?: string;
  collector?: string;
  exchange?: string;
  symbol?: string;
  signal?: AbortSignal;
}

export const TIMELINE_SYMBOL_MODE = {
  Contains: "contains",
  Exact: "exact",
} as const;

export type TimelineSymbolMode = (typeof TIMELINE_SYMBOL_MODE)[keyof typeof TIMELINE_SYMBOL_MODE];

const TIMELINE_API_ORIGIN = "http://localhost";

export interface TimelineEventsQuery {
  collector?: string;
  exchange?: string;
  symbol?: string;
  symbolMode?: TimelineSymbolMode;
  startTs: number;
  endTs: number;
}

export interface TimelineEventRowFilter {
  collector: string;
  exchange: string;
  symbol: string;
}

export interface TimelineEventsRowsQuery {
  startTs: number;
  endTs: number;
  rows: TimelineEventRowFilter[];
}

export interface TimelineEventsRowsPayload {
  startTs: number;
  endTs: number;
  rows: TimelineEventRowFilter[];
}

export interface TimelineRunActionQuery {
  action: TimelineMarketAction;
  collector: string;
  exchange: string;
  symbol: string;
  timeframe?: string;
  signal?: AbortSignal;
}

export interface TimelineRunActionResponse {
  action: TimelineMarketAction;
  market: {
    collector: string;
    exchange: string;
    symbol: string;
    timeframe?: string;
  };
  durationMs: number;
  details: Record<string, number>;
}

export async function fetchTimelineMarkets(
  opts: TimelineMarketsQuery = {},
): Promise<TimelineMarketsResponse> {
  const response = await fetch(buildTimelineMarketsPath(opts), {
    method: "GET",
    cache: "no-store",
    signal: opts.signal,
  });
  const payload = (await parseJsonResponse(
    response,
    "Failed to load markets",
  )) as TimelineMarketsResponse;
  return {
    markets: payload.markets ?? [],
    timeframes: payload.timeframes ?? [],
  };
}

export async function fetchTimelineEvents(query: TimelineEventsQuery, signal?: AbortSignal): Promise<TimelineEvent[]> {
  const response = await fetch(buildTimelineEventsPath(query), {
    method: "GET",
    cache: "no-store",
    signal,
  });
  const payload = (await parseJsonResponse(
    response,
    "Failed to load events",
  )) as { events?: TimelineEvent[] };
  return payload.events ?? [];
}

export async function fetchTimelineEventsByRows(
  query: TimelineEventsRowsQuery,
  signal?: AbortSignal,
): Promise<TimelineEvent[]> {
  const response = await fetch("/api/timeline/events/query", {
    method: "POST",
    cache: "no-store",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(buildTimelineEventsRowsPayload(query)),
    signal,
  });
  const payload = (await parseJsonResponse(
    response,
    "Failed to load events",
  )) as { events?: TimelineEvent[] };
  return payload.events ?? [];
}

export function buildTimelineMarketsPath(query: TimelineMarketsQuery = {}): string {
  const url = new URL("/api/timeline/markets", TIMELINE_API_ORIGIN);
  if (query.timeframe) url.searchParams.set("timeframe", query.timeframe);
  if (query.collector) url.searchParams.set("collector", query.collector);
  if (query.exchange) url.searchParams.set("exchange", query.exchange);
  if (query.symbol) url.searchParams.set("symbol", query.symbol);
  return url.pathname + url.search;
}

export function buildTimelineEventsPath(query: TimelineEventsQuery): string {
  const url = new URL("/api/timeline/events", TIMELINE_API_ORIGIN);
  if (query.collector) url.searchParams.set("collector", query.collector);
  if (query.exchange) url.searchParams.set("exchange", query.exchange);
  if (query.symbol) url.searchParams.set("symbol", query.symbol);
  if (query.symbolMode) url.searchParams.set("symbolMode", query.symbolMode);
  url.searchParams.set("startTs", String(Math.floor(query.startTs)));
  url.searchParams.set("endTs", String(Math.floor(query.endTs)));
  return url.pathname + url.search;
}

export function buildTimelineEventsRowsPayload(
  query: TimelineEventsRowsQuery,
): TimelineEventsRowsPayload {
  const rows: TimelineEventRowFilter[] = [];
  for (const row of query.rows) {
    const collector = row.collector.trim().toUpperCase();
    const exchange = row.exchange.trim().toUpperCase();
    const symbol = row.symbol.trim();
    if (!collector || !exchange || !symbol) continue;
    rows.push({ collector, exchange, symbol });
  }
  return {
    startTs: Math.floor(query.startTs),
    endTs: Math.floor(query.endTs),
    rows,
  };
}

export async function runTimelineMarketAction(
  query: TimelineRunActionQuery,
): Promise<TimelineRunActionResponse> {
  const url = new URL("/api/timeline/actions", window.location.origin);
  const response = await fetch(url.pathname + url.search, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      action: query.action,
      collector: query.collector,
      exchange: query.exchange,
      symbol: query.symbol,
      timeframe: query.timeframe,
    }),
    signal: query.signal,
  });
  return (await parseJsonResponse(response, "Failed to run market action")) as TimelineRunActionResponse;
}

async function parseJsonResponse(
  response: Response,
  context: string,
): Promise<unknown> {
  const raw = await response.text();
  const parsed = parseJson(raw, context);
  const errorMessage = extractApiError(parsed);
  if (!response.ok) throw new Error(errorMessage ? `${context} (${response.status}): ${errorMessage}` : `${context} (${response.status})`);
  return parsed;
}

function parseJson(raw: string, context: string): unknown {
  try {
    return JSON.parse(raw) as unknown;
  } catch {
    const preview = raw.slice(0, 30).trim();
    throw new Error(
      `${context}: expected JSON response but received "${preview}". If running dev client, start backend on http://localhost:3000 or set DEV_API_TARGET.`,
    );
  }
}

function extractApiError(payload: unknown): string | null {
  if (!payload || typeof payload !== "object") return null;
  const value = (payload as { error?: unknown }).error;
  return typeof value === "string" && value.trim() ? value : null;
}
