import type { TimelineEvent, TimelineEventKind, TimelineMarket } from "./timelineTypes.js";

export interface TimelineRange {
  startTs: number;
  endTs: number;
}

export const TIMELINE_EDGE_PADDING_PX = 8;
export interface TimelineEdgePadding {
  leftPx: number;
  rightPx: number;
}

export function marketKey(input: Pick<TimelineMarket, "collector" | "exchange" | "symbol">): string {
  return `${input.collector.toUpperCase()}:${input.exchange.toUpperCase()}:${input.symbol}`;
}

export function eventKind(event: TimelineEvent): TimelineEventKind {
  if (event.gapFixStatus === "adapter_error") return "adapter_error";
  if (event.gapFixStatus === "missing_adapter") return "missing_adapter";
  if (event.eventType === "gap") return "gap";
  if (
    event.eventType === "parts_short" ||
    event.eventType === "non_finite" ||
    event.eventType === "invalid_ts_range" ||
    event.eventType === "notional_too_large"
  ) {
    return "parse_error";
  }
  return "unknown";
}

export function clampTs(value: number, minTs: number, maxTs: number): number {
  if (value < minTs) return minTs;
  if (value > maxTs) return maxTs;
  return value;
}

export function toTimelineX(
  ts: number,
  range: TimelineRange,
  width: number,
  edgePadding: number | TimelineEdgePadding = 0,
): number {
  const padded = resolveTimelinePadding(width, edgePadding);
  const span = Math.max(1, range.endTs - range.startTs);
  const ratio = (ts - range.startTs) / span;
  const x = padded.minX + ratio * padded.usableWidth;
  if (x < padded.minX) return padded.minX;
  if (x > padded.maxX) return padded.maxX;
  return x;
}

export function toTimelineTs(
  x: number,
  range: TimelineRange,
  width: number,
  edgePadding: number | TimelineEdgePadding = 0,
): number {
  const padded = resolveTimelinePadding(width, edgePadding);
  const clampedX = clampTs(x, padded.minX, padded.maxX);
  const ratio = (clampedX - padded.minX) / padded.usableWidth;
  const ts = range.startTs + ratio * (range.endTs - range.startTs);
  return Math.floor(clampTs(ts, range.startTs, range.endTs));
}

export function groupEventsByMarket(events: TimelineEvent[]): Map<string, TimelineEvent[]> {
  const grouped = new Map<string, TimelineEvent[]>();
  for (const event of events) {
    const key = marketKey(event);
    const bucket = grouped.get(key);
    if (bucket) {
      bucket.push(event);
    } else {
      grouped.set(key, [event]);
    }
  }
  for (const bucket of grouped.values()) {
    bucket.sort((a, b) => a.ts - b.ts || a.id - b.id);
  }
  return grouped;
}

export function computeGlobalRange(markets: TimelineMarket[]): TimelineRange | null {
  if (!markets.length) return null;
  let minTs = Number.POSITIVE_INFINITY;
  let maxTs = Number.NEGATIVE_INFINITY;
  for (const market of markets) {
    if (market.startTs < minTs) minTs = market.startTs;
    if (market.endTs > maxTs) maxTs = market.endTs;
  }
  if (!Number.isFinite(minTs) || !Number.isFinite(maxTs) || maxTs < minTs) {
    return null;
  }
  return { startTs: minTs, endTs: maxTs };
}

export function clampMarketToRange(market: TimelineMarket, range: TimelineRange): TimelineRange | null {
  const startTs = clampTs(market.startTs, range.startTs, range.endTs);
  const endTs = clampTs(market.endTs, range.startTs, range.endTs);
  if (endTs < startTs) return null;
  return { startTs, endTs };
}

function resolveTimelinePadding(
  width: number,
  edgePaddingPx: number | TimelineEdgePadding,
): { minX: number; maxX: number; usableWidth: number } {
  const safeWidth = Math.max(1, width);
  const padding = normalizePadding(edgePaddingPx);
  const safeLeftPadding = Math.max(0, Math.min(padding.leftPx, safeWidth / 2));
  const safeRightPadding = Math.max(0, Math.min(padding.rightPx, safeWidth / 2));
  const minX = safeLeftPadding;
  const maxX = safeWidth - safeRightPadding;
  const usableWidth = Math.max(1, maxX - minX);
  return { minX, maxX, usableWidth };
}

function normalizePadding(edgePadding: number | TimelineEdgePadding): TimelineEdgePadding {
  if (typeof edgePadding === "number") {
    return { leftPx: edgePadding, rightPx: edgePadding };
  }
  return edgePadding;
}
