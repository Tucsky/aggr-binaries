import type { TimelineMarketsResponse } from "./timelineApi.js";
import type { TimelineMarket } from "./timelineTypes.js";
import { resolveTimelineTimeframe } from "./timelineViewport.js";
import type { TimelineRange } from "./timelineUtils.js";

export function unique(values: string[]): string[] {
  return Array.from(new Set(values)).sort();
}

export function normalizeMarketRows(markets: TimelineMarket[], fallbackTimeframe: string): TimelineMarket[] {
  return markets.map((market) => ({
    ...market,
    collector: market.collector.toUpperCase(),
    exchange: market.exchange.toUpperCase(),
    timeframe: market.timeframe || fallbackTimeframe,
  }));
}

export function normalizeMarketsResponse(
  response: TimelineMarketsResponse,
  requestedTimeframe: string,
): { markets: TimelineMarket[]; timeframes: string[]; selectedTimeframe: string } {
  const inferred = unique(
    response.markets
      .map((market) => market.timeframe)
      .filter((value): value is string => typeof value === "string" && value.length > 0),
  );
  const fallback = requestedTimeframe || inferred[0] || "1m";
  const timeframes = response.timeframes.length ? response.timeframes : inferred.length ? inferred : [fallback];
  const selectedTimeframe = resolveTimelineTimeframe(requestedTimeframe, timeframes) || fallback;
  return { markets: normalizeMarketRows(response.markets, selectedTimeframe), timeframes, selectedTimeframe };
}

export function restorePersistedViewRange(
  selected: TimelineRange,
  persistedViewStartTs: number | null,
  persistedViewEndTs: number | null,
  panOverscrollRatio: number,
): TimelineRange | null {
  if (persistedViewStartTs === null || persistedViewEndTs === null) {
    // No persisted view range to restore
    return null;
  }
  if (persistedViewEndTs <= persistedViewStartTs) {
    // Invalid persisted view range
    return null
  }
  const candidateSpan = persistedViewEndTs - persistedViewStartTs;
  const fullSpan = selected.endTs - selected.startTs;
  if (candidateSpan >= fullSpan) return { ...selected };
  const overscrollMs = Math.round(candidateSpan * panOverscrollRatio);
  const minStartTs = selected.startTs - overscrollMs;
  const maxEndTs = selected.endTs + overscrollMs;
  let nextStart = persistedViewStartTs;
  let nextEnd = persistedViewEndTs;
  if (nextStart < minStartTs) {
    const delta = minStartTs - nextStart;
    nextStart += delta;
    nextEnd += delta;
  }
  if (nextEnd > maxEndTs) {
    const delta = nextEnd - maxEndTs;
    nextStart -= delta;
    nextEnd -= delta;
  }

  console.log(nextEnd > nextStart ? `restored range ${new Date(nextStart).toISOString()} - ${new Date(nextEnd).toISOString()}` : `restored range null`)

  return nextEnd > nextStart ? { startTs: nextStart, endTs: nextEnd } : null;
}

export function buildEventsQueryKey(
  range: TimelineRange,
  collectorFilter: string,
  exchangeFilter: string,
  symbolFilter: string,
): string {
  return `${collectorFilter}|${exchangeFilter}|${symbolFilter.toLowerCase()}|${range.startTs}|${range.endTs}`;
}
