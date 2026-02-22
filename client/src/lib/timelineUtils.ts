import type { TimelineEvent, TimelineEventKind, TimelineMarket } from "./timelineTypes.js";

export interface TimelineRange {
  startTs: number;
  endTs: number;
}

export interface TimelineEventWindow {
  startIndex: number;
  endIndex: number;
}

export function marketKey(input: Pick<TimelineMarket, "collector" | "exchange" | "symbol">): string {
  return `${input.collector.toUpperCase()}:${input.exchange.toUpperCase()}:${input.symbol}`;
}

export function eventKind(event: TimelineEvent): TimelineEventKind {
  if (event.gapFixStatus === "fixed") return "gap_fixed";
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
): number {
  const safeWidth = Math.max(1, width);
  const span = Math.max(1, range.endTs - range.startTs);
  const ratio = (ts - range.startTs) / span;
  const x = ratio * safeWidth;
  if (x < 0) return 0;
  if (x > safeWidth) return safeWidth;
  return x;
}

export function toTimelineTs(
  x: number,
  range: TimelineRange,
  width: number,
): number {
  const safeWidth = Math.max(1, width);
  const clampedX = clampTs(x, 0, safeWidth);
  const ratio = clampedX / safeWidth;
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

export function findTimelineEventWindow(
  events: TimelineEvent[],
  startTs: number,
  endTs: number,
): TimelineEventWindow {
  if (!events.length || endTs < startTs) {
    return { startIndex: 0, endIndex: 0 };
  }
  const startIndex = lowerBoundEventTs(events, startTs);
  const endIndex = upperBoundEventTs(events, endTs);
  return { startIndex, endIndex };
}

function lowerBoundEventTs(events: TimelineEvent[], minTs: number): number {
  let lo = 0;
  let hi = events.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (events[mid].ts < minTs) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

function upperBoundEventTs(events: TimelineEvent[], maxTs: number): number {
  let lo = 0;
  let hi = events.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (events[mid].ts <= maxTs) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}
