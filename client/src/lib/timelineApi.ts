import type { TimelineEvent, TimelineMarket } from "./timelineTypes.js";

export interface TimelineMarketsResponse {
  markets: TimelineMarket[];
  timeframes: string[];
}

export interface TimelineEventsQuery {
  collector?: string;
  exchange?: string;
  symbol?: string;
  startTs: number;
  endTs: number;
}

export async function fetchTimelineMarkets(
  opts: { timeframe?: string; signal?: AbortSignal } = {},
): Promise<TimelineMarketsResponse> {
  const url = new URL("/api/timeline/markets", window.location.origin);
  if (opts.timeframe) {
    url.searchParams.set("timeframe", opts.timeframe);
  }
  const response = await fetch(url.pathname + url.search, {
    method: "GET",
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
  const url = new URL("/api/timeline/events", window.location.origin);
  if (query.collector) url.searchParams.set("collector", query.collector);
  if (query.exchange) url.searchParams.set("exchange", query.exchange);
  if (query.symbol) url.searchParams.set("symbol", query.symbol);
  url.searchParams.set("startTs", String(Math.floor(query.startTs)));
  url.searchParams.set("endTs", String(Math.floor(query.endTs)));

  const response = await fetch(url.pathname + url.search, {
    method: "GET",
    signal,
  });
  const payload = (await parseJsonResponse(
    response,
    "Failed to load events",
  )) as { events?: TimelineEvent[] };
  return payload.events ?? [];
}

async function parseJsonResponse(
  response: Response,
  context: string,
): Promise<unknown> {
  const raw = await response.text();
  if (!response.ok) {
    throw new Error(`${context} (${response.status})`);
  }
  try {
    return JSON.parse(raw) as unknown;
  } catch {
    const preview = raw.slice(0, 30).trim();
    throw new Error(
      `${context}: expected JSON response but received "${preview}". If running dev client, start backend on http://localhost:3000 or set DEV_API_TARGET.`,
    );
  }
}
