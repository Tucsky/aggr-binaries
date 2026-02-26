import type { TimelineMarket } from "./timelineTypes.js";
import type { TimelineEventRowFilter } from "./timelineApi.js";
import type { TimelineRange } from "./timelineUtils.js";
import {
  buildEventsQueryKey,
  buildViewportEventsQueryKey,
  clampRangeToBounds,
  expandRangeWithinBounds,
  isRangeCoveredBy,
} from "./timelinePageHelpers.js";
import { marketKey } from "./timelineUtils.js";
export {
  createTimelineViewportEventCacheState,
  readTimelineViewportEventCache,
  resolveTimelineViewportMissingRows,
  writeTimelineViewportEventCache,
  type TimelineViewportEventCacheEntry,
  type TimelineViewportEventCacheReadResult,
  type TimelineViewportEventCacheSegment,
  type TimelineViewportEventCacheState,
  type TimelineViewportEventCacheWriteInput,
} from "./timelineEventViewportCache.js";

export interface TimelineViewportEventSelection {
  rowKeys: string[];
  rows: TimelineEventRowFilter[];
}

export interface TimelineViewportEventRequest {
  scopeKey: string;
  queryKey: string;
  requestRange: TimelineRange;
  rowKeys: string[];
  rows: TimelineEventRowFilter[];
}

interface TimelineViewportRequestInput {
  scopeKey: string;
  selectedRange: TimelineRange;
  viewRange: TimelineRange;
  selection: TimelineViewportEventSelection;
  loadedRange: TimelineRange | null;
  loadedRowKeys: Set<string>;
  rangeOverscanRatio: number;
  forceReload: boolean;
}

export function buildTimelineEventsScopeKey(
  timeframe: string,
  range: TimelineRange,
  collectorFilter: string,
  exchangeFilter: string,
  symbolFilter: string,
): string {
  return `${timeframe}|${buildEventsQueryKey(range, collectorFilter, exchangeFilter, symbolFilter)}`;
}

export function selectTimelineViewportEventRows(
  markets: TimelineMarket[],
  startIndex: number,
  endIndex: number,
  rowOverscan: number,
  maxRows: number,
): TimelineViewportEventSelection {
  const safeOverscan = Math.max(0, Math.floor(rowOverscan));
  const safeMaxRows = Math.max(1, Math.floor(maxRows));
  // Overscan smooths nearby scroll/pan moves; maxRows prevents oversized viewport payloads.
  const start = Math.max(0, Math.floor(startIndex) - safeOverscan);
  const end = Math.min(markets.length, Math.ceil(endIndex) + safeOverscan);
  const cappedEnd = Math.min(end, start + safeMaxRows);
  const rowKeys: string[] = [];
  const rows: TimelineEventRowFilter[] = [];
  for (let i = start; i < cappedEnd; i += 1) {
    const market = markets[i];
    rowKeys.push(marketKey(market));
    rows.push({
      collector: market.collector,
      exchange: market.exchange,
      symbol: market.symbol,
    });
  }
  return { rowKeys, rows };
}

export function resolveTimelineViewportEventRequest(
  input: TimelineViewportRequestInput,
): TimelineViewportEventRequest | null {
  if (!input.selection.rows.length) return null;
  const visibleRange = clampRangeToBounds(input.viewRange, input.selectedRange);
  if (
    !input.forceReload &&
    isRangeCoveredBy(input.loadedRange, visibleRange) &&
    areAllRowKeysCovered(input.loadedRowKeys, input.selection.rowKeys)
  ) {
    return null;
  }
  const requestRange = expandRangeWithinBounds(
    visibleRange,
    input.selectedRange,
    input.rangeOverscanRatio,
  );
  return {
    scopeKey: input.scopeKey,
    queryKey: buildViewportEventsQueryKey(input.scopeKey, requestRange, input.selection.rowKeys),
    requestRange,
    rowKeys: input.selection.rowKeys,
    rows: input.selection.rows,
  };
}

function areAllRowKeysCovered(loaded: Set<string>, rowKeys: string[]): boolean {
  for (let i = 0; i < rowKeys.length; i += 1) {
    if (loaded.has(rowKeys[i])) continue;
    return false;
  }
  return true;
}
