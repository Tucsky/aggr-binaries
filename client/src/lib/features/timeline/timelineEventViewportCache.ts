import type { TimelineEvent } from "./timelineTypes.js";
import type { TimelineEventRowFilter } from "./timelineApi.js";
import type { TimelineRange } from "./timelineUtils.js";
import { marketKey } from "./timelineUtils.js";
import { isRangeCoveredBy } from "./timelinePageHelpers.js";
import type { TimelineViewportEventSelection } from "./timelineEventViewportLoader.js";
import {
  distanceTimelineRangeToAnchor,
  findTimelineEventWindow,
  pruneTimelineViewportEventCache,
} from "./timelineEventViewportCacheHelpers.js";

export interface TimelineViewportEventCacheEntry {
  request: {
    scopeKey: string;
    queryKey: string;
    requestRange: TimelineRange;
    rowKeys: string[];
    rows: TimelineEventRowFilter[];
  };
  segments: TimelineViewportEventCacheSegment[];
}

export interface TimelineViewportEventCacheSegment {
  range: TimelineRange;
  events: TimelineEvent[];
  accessSeq: number;
}

export interface TimelineViewportEventCacheState {
  byRow: Map<string, TimelineViewportEventCacheEntry>;
  totalEvents: number;
  accessSeq: number;
}

export interface TimelineViewportEventCacheReadResult {
  events: TimelineEvent[];
  coveredRowKeys: Set<string>;
}

export interface TimelineViewportEventCacheWriteInput {
  scopeKey: string;
  requestRange: TimelineRange;
  rowKeys: string[];
  events: TimelineEvent[];
}

const MAX_SEGMENTS_PER_ROW = 3;
const MAX_EVENTS_PER_SEGMENT = 20_000;
const EMPTY_EVENTS: TimelineEvent[] = [];

export function createTimelineViewportEventCacheState(): TimelineViewportEventCacheState {
  return {
    byRow: new Map<string, TimelineViewportEventCacheEntry>(),
    totalEvents: 0,
    accessSeq: 0,
  };
}

export function writeTimelineViewportEventCache(
  cache: TimelineViewportEventCacheState,
  maxRows: number,
  maxEvents: number,
  input: TimelineViewportEventCacheWriteInput,
): void {
  const groupedEvents = groupEventsByRowKey(input.events);
  for (let i = 0; i < input.rowKeys.length; i += 1) {
    const rowKey = input.rowKeys[i];
    const fullKey = buildRowCacheKey(input.scopeKey, rowKey);
    const existing = cache.byRow.get(fullKey);
    const previousCount = countEntryEvents(existing);
    const nextAccess = nextTimelineViewportEventAccessSeq(cache);
    const nextSegment: TimelineViewportEventCacheSegment = {
      range: input.requestRange,
      events: groupedEvents.get(rowKey) ?? EMPTY_EVENTS,
      accessSeq: nextAccess,
    };
    const nextSegments = upsertTimelineViewportSegments(
      existing?.segments ?? [],
      nextSegment,
      nextAccess,
      input.requestRange,
    );
    const nextEntry: TimelineViewportEventCacheEntry = {
      request: {
        scopeKey: input.scopeKey,
        queryKey: fullKey,
        requestRange: input.requestRange,
        rowKeys: [rowKey],
        rows: [],
      },
      segments: nextSegments,
    };
    const nextCount = countEntryEvents(nextEntry);
    cache.totalEvents += nextCount - previousCount;
    cache.byRow.set(fullKey, nextEntry);
  }
  pruneTimelineViewportEventCache(cache, maxRows, maxEvents, input.requestRange);
}

export function readTimelineViewportEventCache(
  cache: TimelineViewportEventCacheState,
  scopeKey: string,
  requestRange: TimelineRange,
  selection: TimelineViewportEventSelection,
): TimelineViewportEventCacheReadResult {
  const coveredRowKeys = new Set<string>();
  const events: TimelineEvent[] = [];
  for (let i = 0; i < selection.rowKeys.length; i += 1) {
    const rowKey = selection.rowKeys[i];
    const fullKey = buildRowCacheKey(scopeKey, rowKey);
    const entry = cache.byRow.get(fullKey);
    if (!entry) continue;
    const segment = findTimelineViewportCoveringSegment(entry.segments, requestRange);
    if (!segment) continue;
    segment.accessSeq = nextTimelineViewportEventAccessSeq(cache);
    coveredRowKeys.add(rowKey);
    const window = findTimelineEventWindow(
      segment.events,
      requestRange.startTs,
      requestRange.endTs,
    );
    for (let eventIdx = window.startIndex; eventIdx < window.endIndex; eventIdx += 1) {
      events.push(segment.events[eventIdx]);
    }
  }
  return { events, coveredRowKeys };
}

export function resolveTimelineViewportMissingRows(
  selection: TimelineViewportEventSelection,
  coveredRowKeys: Set<string>,
): TimelineViewportEventSelection {
  if (coveredRowKeys.size >= selection.rowKeys.length) return { rowKeys: [], rows: [] };
  const rowKeys: string[] = [];
  const rows: TimelineEventRowFilter[] = [];
  for (let i = 0; i < selection.rowKeys.length; i += 1) {
    const rowKey = selection.rowKeys[i];
    if (coveredRowKeys.has(rowKey)) continue;
    rowKeys.push(rowKey);
    rows.push(selection.rows[i]);
  }
  return { rowKeys, rows };
}

function upsertTimelineViewportSegments(
  segments: TimelineViewportEventCacheSegment[],
  nextSegment: TimelineViewportEventCacheSegment,
  accessSeq: number,
  activeRange: TimelineRange | null,
): TimelineViewportEventCacheSegment[] {
  const next = segments.map((segment) => ({
    range: segment.range,
    events: segment.events,
    accessSeq: segment.accessSeq,
  }));
  next.push(nextSegment);
  next.sort((a, b) => {
    if (a.range.startTs !== b.range.startTs) return a.range.startTs - b.range.startTs;
    return a.range.endTs - b.range.endTs;
  });

  const merged: TimelineViewportEventCacheSegment[] = [];
  for (let i = 0; i < next.length; i += 1) {
    const segment = next[i];
    const previous = merged[merged.length - 1];
    if (!previous || !doRangesOverlapOrTouch(previous.range, segment.range)) {
      merged.push(segment);
      continue;
    }
    const mergedEvents = mergeTimelineEvents(
      previous.events,
      previous.accessSeq,
      segment.events,
      segment.accessSeq,
    );
    if (mergedEvents.length > MAX_EVENTS_PER_SEGMENT) {
      merged.push(segment);
      continue;
    }
    previous.range = {
      startTs: Math.min(previous.range.startTs, segment.range.startTs),
      endTs: Math.max(previous.range.endTs, segment.range.endTs),
    };
    previous.events = mergedEvents;
    previous.accessSeq = previous.accessSeq > segment.accessSeq ? previous.accessSeq : segment.accessSeq;
  }

  while (merged.length > MAX_SEGMENTS_PER_ROW) {
    const evictionIdx = selectSegmentEvictionIndex(merged, activeRange);
    merged.splice(evictionIdx, 1);
  }

  if (merged.length) merged[merged.length - 1].accessSeq = accessSeq;
  return merged;
}

function selectSegmentEvictionIndex(
  segments: TimelineViewportEventCacheSegment[],
  activeRange: TimelineRange | null,
): number {
  let index = 0;
  let bestDistance = distanceTimelineRangeToAnchor(segments[0].range, activeRange);
  let bestAccess = segments[0].accessSeq;
  for (let i = 1; i < segments.length; i += 1) {
    const distance = distanceTimelineRangeToAnchor(segments[i].range, activeRange);
    if (distance > bestDistance) {
      index = i;
      bestDistance = distance;
      bestAccess = segments[i].accessSeq;
      continue;
    }
    if (distance === bestDistance && segments[i].accessSeq < bestAccess) {
      index = i;
      bestAccess = segments[i].accessSeq;
    }
  }
  return index;
}

function mergeTimelineEvents(
  a: TimelineEvent[],
  aAccessSeq: number,
  b: TimelineEvent[],
  bAccessSeq: number,
): TimelineEvent[] {
  if (!a.length) return b;
  if (!b.length) return a;
  const merged: TimelineEvent[] = [];
  const idToIndex = new Map<number, number>();
  const idToAccessSeq = new Map<number, number>();
  let ai = 0;
  let bi = 0;
  while (ai < a.length || bi < b.length) {
    const takeA = bi >= b.length || (ai < a.length && compareTimelineEventsOrder(a[ai], b[bi]) <= 0);
    const next = takeA ? a[ai++] : b[bi++];
    const nextAccessSeq = takeA ? aAccessSeq : bAccessSeq;
    const existingIdx = idToIndex.get(next.id);
    if (existingIdx === undefined) {
      idToIndex.set(next.id, merged.length);
      idToAccessSeq.set(next.id, nextAccessSeq);
      merged.push(next);
      continue;
    }
    const previousAccessSeq = idToAccessSeq.get(next.id) ?? 0;
    if (nextAccessSeq < previousAccessSeq) {
      continue;
    }
    merged[existingIdx] = next;
    idToAccessSeq.set(next.id, nextAccessSeq);
  }
  return merged;
}

function compareTimelineEventsOrder(a: TimelineEvent, b: TimelineEvent): number {
  if (a.ts !== b.ts) return a.ts - b.ts;
  if (a.id !== b.id) return a.id - b.id;
  return 0;
}

function findTimelineViewportCoveringSegment(
  segments: TimelineViewportEventCacheSegment[],
  range: TimelineRange,
): TimelineViewportEventCacheSegment | null {
  for (let i = 0; i < segments.length; i += 1) {
    const segment = segments[i];
    if (!isRangeCoveredBy(segment.range, range)) continue;
    return segment;
  }
  return null;
}

function countEntryEvents(entry: TimelineViewportEventCacheEntry | undefined): number {
  if (!entry) return 0;
  let total = 0;
  for (let i = 0; i < entry.segments.length; i += 1) {
    total += entry.segments[i].events.length;
  }
  return total;
}

function groupEventsByRowKey(events: TimelineEvent[]): Map<string, TimelineEvent[]> {
  const grouped = new Map<string, TimelineEvent[]>();
  for (let i = 0; i < events.length; i += 1) {
    const event = events[i];
    const key = marketKey(event);
    const bucket = grouped.get(key);
    if (bucket) {
      bucket.push(event);
      continue;
    }
    grouped.set(key, [event]);
  }
  return grouped;
}

function buildRowCacheKey(scopeKey: string, rowKey: string): string {
  return `${scopeKey}|${rowKey}`;
}

function doRangesOverlapOrTouch(a: TimelineRange, b: TimelineRange): boolean {
  return a.startTs <= b.endTs && b.startTs <= a.endTs;
}

function nextTimelineViewportEventAccessSeq(cache: TimelineViewportEventCacheState): number {
  cache.accessSeq += 1;
  return cache.accessSeq;
}
