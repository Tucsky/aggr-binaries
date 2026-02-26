import type { TimelineRange } from "./timelineUtils.js";
import type {
  TimelineViewportEventCacheEntry,
  TimelineViewportEventCacheSegment,
  TimelineViewportEventCacheState,
} from "./timelineEventViewportCache.js";

interface TimelineRowEvictionCandidate {
  fullKey: string;
  distanceToActiveRange: number;
  lastAccessSeq: number;
  eventCount: number;
}

interface TimelineSegmentEvictionCandidate {
  fullKey: string;
  segmentIdx: number;
  distanceToActiveRange: number;
  accessSeq: number;
  eventCount: number;
}

export function pruneTimelineViewportEventCache(
  cache: TimelineViewportEventCacheState,
  maxRows: number,
  maxEvents: number,
  activeRange: TimelineRange | null,
): void {
  const safeMaxRows = Math.max(1, Math.floor(maxRows));
  const safeMaxEvents = Math.max(1, Math.floor(maxEvents));
  while (cache.byRow.size > safeMaxRows) {
    const rowCandidate = selectTimelineRowEvictionCandidate(cache, activeRange);
    if (!rowCandidate) break;
    cache.totalEvents -= rowCandidate.eventCount;
    cache.byRow.delete(rowCandidate.fullKey);
  }

  while (cache.totalEvents > safeMaxEvents) {
    const segmentCandidate = selectTimelineSegmentEvictionCandidate(cache, activeRange);
    if (!segmentCandidate) break;
    const entry = cache.byRow.get(segmentCandidate.fullKey);
    if (!entry || !entry.segments[segmentCandidate.segmentIdx]) continue;
    entry.segments.splice(segmentCandidate.segmentIdx, 1);
    cache.totalEvents -= segmentCandidate.eventCount;
    if (!entry.segments.length) {
      cache.byRow.delete(segmentCandidate.fullKey);
    }
  }

  if (cache.totalEvents < 0) cache.totalEvents = 0;
}

export function distanceTimelineRangeToAnchor(
  range: TimelineRange,
  anchor: TimelineRange | null,
): number {
  if (!anchor) return 0;
  if (range.startTs <= anchor.endTs && anchor.startTs <= range.endTs) return 0;
  if (range.endTs < anchor.startTs) return anchor.startTs - range.endTs;
  return range.startTs - anchor.endTs;
}

export function findTimelineEventWindow(
  events: Array<{ ts: number }>,
  startTs: number,
  endTs: number,
): { startIndex: number; endIndex: number } {
  if (!events.length || endTs < startTs) return { startIndex: 0, endIndex: 0 };
  const startIndex = lowerBoundTimelineEventTs(events, startTs);
  const endIndex = upperBoundTimelineEventTs(events, endTs);
  return { startIndex, endIndex };
}

function selectTimelineRowEvictionCandidate(
  cache: TimelineViewportEventCacheState,
  activeRange: TimelineRange | null,
): TimelineRowEvictionCandidate | null {
  let candidate: TimelineRowEvictionCandidate | null = null;
  for (const [fullKey, entry] of cache.byRow.entries()) {
    const eventCount = countEntryEvents(entry);
    const distanceToActiveRange = minTimelineSegmentDistance(entry.segments, activeRange);
    const lastAccessSeq = maxTimelineSegmentAccessSeq(entry.segments);
    const next: TimelineRowEvictionCandidate = {
      fullKey,
      distanceToActiveRange,
      lastAccessSeq,
      eventCount,
    };
    if (!candidate || shouldReplaceRowEvictionCandidate(candidate, next)) {
      candidate = next;
    }
  }
  return candidate;
}

function selectTimelineSegmentEvictionCandidate(
  cache: TimelineViewportEventCacheState,
  activeRange: TimelineRange | null,
): TimelineSegmentEvictionCandidate | null {
  let candidate: TimelineSegmentEvictionCandidate | null = null;
  for (const [fullKey, entry] of cache.byRow.entries()) {
    for (let i = 0; i < entry.segments.length; i += 1) {
      const segment = entry.segments[i];
      const next: TimelineSegmentEvictionCandidate = {
        fullKey,
        segmentIdx: i,
        distanceToActiveRange: distanceTimelineRangeToAnchor(segment.range, activeRange),
        accessSeq: segment.accessSeq,
        eventCount: segment.events.length,
      };
      if (!candidate || shouldReplaceSegmentEvictionCandidate(candidate, next)) {
        candidate = next;
      }
    }
  }
  return candidate;
}

function shouldReplaceRowEvictionCandidate(
  current: TimelineRowEvictionCandidate,
  next: TimelineRowEvictionCandidate,
): boolean {
  if (next.distanceToActiveRange !== current.distanceToActiveRange) {
    return next.distanceToActiveRange > current.distanceToActiveRange;
  }
  if (next.lastAccessSeq !== current.lastAccessSeq) {
    return next.lastAccessSeq < current.lastAccessSeq;
  }
  return next.eventCount > current.eventCount;
}

function shouldReplaceSegmentEvictionCandidate(
  current: TimelineSegmentEvictionCandidate,
  next: TimelineSegmentEvictionCandidate,
): boolean {
  if (next.distanceToActiveRange !== current.distanceToActiveRange) {
    return next.distanceToActiveRange > current.distanceToActiveRange;
  }
  if (next.accessSeq !== current.accessSeq) {
    return next.accessSeq < current.accessSeq;
  }
  return next.eventCount > current.eventCount;
}

function minTimelineSegmentDistance(
  segments: TimelineViewportEventCacheSegment[],
  activeRange: TimelineRange | null,
): number {
  if (!segments.length) return Number.POSITIVE_INFINITY;
  let minDistance = Number.POSITIVE_INFINITY;
  for (let i = 0; i < segments.length; i += 1) {
    const distance = distanceTimelineRangeToAnchor(segments[i].range, activeRange);
    if (distance < minDistance) minDistance = distance;
  }
  return minDistance;
}

function maxTimelineSegmentAccessSeq(segments: TimelineViewportEventCacheSegment[]): number {
  let maxAccess = 0;
  for (let i = 0; i < segments.length; i += 1) {
    if (segments[i].accessSeq > maxAccess) maxAccess = segments[i].accessSeq;
  }
  return maxAccess;
}

function countEntryEvents(entry: TimelineViewportEventCacheEntry | undefined): number {
  if (!entry) return 0;
  let total = 0;
  for (let i = 0; i < entry.segments.length; i += 1) {
    total += entry.segments[i].events.length;
  }
  return total;
}

function lowerBoundTimelineEventTs(events: Array<{ ts: number }>, minTs: number): number {
  let lo = 0;
  let hi = events.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (events[mid].ts < minTs) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

function upperBoundTimelineEventTs(events: Array<{ ts: number }>, maxTs: number): number {
  let lo = 0;
  let hi = events.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (events[mid].ts <= maxTs) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}
