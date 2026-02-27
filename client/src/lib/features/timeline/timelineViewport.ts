import type { TimelineRange } from "./timelineUtils.js";

export const DEFAULT_MIN_VIEW_SPAN_MS = 60_000;
export const DEFAULT_ZOOM_SENSITIVITY = 0.008;
export const DEFAULT_PAN_OVERSCROLL_RATIO = 0;
const MAX_TIMELINE_OVERSCROLL_RATIO = 0.49;

export function resolveTimelineTimeframe(current: string, options: string[]): string {
  if (current && options.includes(current)) return current;
  if (options.includes("1m")) return "1m";
  return options[0] ?? "";
}

export function buildInitialViewRange(selectedRange: TimelineRange, yearMs: number): TimelineRange {
  const span = selectedRange.endTs - selectedRange.startTs;
  if (span <= yearMs) {
    return { ...selectedRange };
  }
  return {
    startTs: selectedRange.endTs - yearMs,
    endTs: selectedRange.endTs,
  };
}

export function zoomTimelineRange(
  selectedRange: TimelineRange,
  viewRange: TimelineRange,
  centerTs: number,
  deltaY: number,
  minViewSpanMs = DEFAULT_MIN_VIEW_SPAN_MS,
  zoomSensitivity = DEFAULT_ZOOM_SENSITIVITY,
  overscrollRatio = DEFAULT_PAN_OVERSCROLL_RATIO,
): TimelineRange {
  const clampedOverscrollRatio = clampOverscrollRatio(overscrollRatio);
  const currentSpan = Math.max(1, viewRange.endTs - viewRange.startTs);
  const fullSpan = Math.max(1, selectedRange.endTs - selectedRange.startTs);
  const maxSpan = resolveMaxViewSpan(fullSpan, clampedOverscrollRatio);
  const factor = Math.exp(deltaY * zoomSensitivity);
  let nextSpan = Math.round(currentSpan * factor);
  if (nextSpan < minViewSpanMs) nextSpan = minViewSpanMs;
  if (nextSpan > maxSpan) nextSpan = maxSpan;
  if (nextSpan >= fullSpan) {
    return alignViewSpanAroundSelectedRange(selectedRange, nextSpan, maxSpan);
  }

  const ratioRaw = (centerTs - viewRange.startTs) / currentSpan;
  const ratio = Math.min(1, Math.max(0, ratioRaw));
  let nextStart = Math.floor(centerTs - ratio * nextSpan);
  let nextEnd = nextStart + nextSpan;
  const overscrollMs = Math.round(nextSpan * clampedOverscrollRatio);
  const minStartTs = selectedRange.startTs - overscrollMs;
  const maxEndTs = selectedRange.endTs + overscrollMs;
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
  return {
    startTs: nextStart,
    endTs: nextEnd,
  };
}

export function panTimelineRange(
  selectedRange: TimelineRange,
  viewRange: TimelineRange,
  deltaMs: number,
  overscrollRatio = DEFAULT_PAN_OVERSCROLL_RATIO,
): TimelineRange {
  const clampedOverscrollRatio = clampOverscrollRatio(overscrollRatio);
  const selectedSpan = Math.max(1, selectedRange.endTs - selectedRange.startTs);
  const span = Math.max(1, viewRange.endTs - viewRange.startTs);
  const maxSpan = resolveMaxViewSpan(selectedSpan, clampedOverscrollRatio);
  const clampedSpan = span > maxSpan ? maxSpan : span;
  if (clampedSpan >= selectedSpan) {
    // Once the viewport reaches the data span, keep it centered and deterministic.
    return alignViewSpanAroundSelectedRange(selectedRange, clampedSpan, maxSpan);
  }
  if (deltaMs === 0) return viewRange;
  const overscrollMs = Math.round(clampedSpan * clampedOverscrollRatio);
  const minStartTs = selectedRange.startTs - overscrollMs;
  const maxEndTs = selectedRange.endTs + overscrollMs;
  let nextStart = viewRange.startTs + deltaMs;
  let nextEnd = nextStart + clampedSpan;
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
  return {
    startTs: nextStart,
    endTs: nextEnd,
  };
}

export function buildTimelineFullViewRange(
  selectedRange: TimelineRange,
  overscrollRatio = DEFAULT_PAN_OVERSCROLL_RATIO,
): TimelineRange {
  const selectedSpan = Math.max(1, selectedRange.endTs - selectedRange.startTs);
  const clampedOverscrollRatio = clampOverscrollRatio(overscrollRatio);
  const maxSpan = resolveMaxViewSpan(selectedSpan, clampedOverscrollRatio);
  return alignViewSpanAroundSelectedRange(selectedRange, maxSpan, maxSpan);
}

function clampOverscrollRatio(overscrollRatio: number): number {
  if (!Number.isFinite(overscrollRatio) || overscrollRatio <= 0) return 0;
  if (overscrollRatio >= MAX_TIMELINE_OVERSCROLL_RATIO) return MAX_TIMELINE_OVERSCROLL_RATIO;
  return overscrollRatio;
}

function resolveMaxViewSpan(selectedSpan: number, overscrollRatio: number): number {
  if (overscrollRatio <= 0) return selectedSpan;
  const rawMaxSpan = Math.round(selectedSpan / (1 - overscrollRatio * 2));
  return rawMaxSpan >= selectedSpan ? rawMaxSpan : selectedSpan;
}

function alignViewSpanAroundSelectedRange(
  selectedRange: TimelineRange,
  requestedSpan: number,
  maxSpan: number,
): TimelineRange {
  const selectedSpan = Math.max(1, selectedRange.endTs - selectedRange.startTs);
  const targetSpan = Math.max(selectedSpan, Math.min(maxSpan, Math.round(requestedSpan)));
  const extraSpan = targetSpan - selectedSpan;
  const leftPad = Math.floor(extraSpan / 2);
  const rightPad = extraSpan - leftPad;
  return {
    startTs: selectedRange.startTs - leftPad,
    endTs: selectedRange.endTs + rightPad,
  };
}

export function shiftViewRangeIntoRangeIfDisjoint(
  range: TimelineRange,
  viewRange: TimelineRange,
): TimelineRange {
  if (viewRange.endTs >= range.startTs && viewRange.startTs <= range.endTs) {
    return viewRange;
  }
  const viewSpan = Math.max(1, viewRange.endTs - viewRange.startTs);
  const rangeSpan = Math.max(0, range.endTs - range.startTs);
  if (viewSpan >= rangeSpan) return { ...range };
  if (viewRange.endTs < range.startTs) {
    const startTs = range.startTs;
    const endTs = startTs + viewSpan;
    if (endTs <= range.endTs) return { startTs, endTs };
    return { startTs: range.endTs - viewSpan, endTs: range.endTs };
  }
  const endTs = range.endTs;
  const startTs = endTs - viewSpan;
  if (startTs >= range.startTs) return { startTs, endTs };
  return { ...range };
}

export function formatTimelineTsLabel(ts: number | null): string {
  if (ts === null) return "";
  const date = new Date(ts);
  if (!Number.isFinite(date.getTime())) return "";
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  const hours = String(date.getUTCHours()).padStart(2, "0");
  const minutes = String(date.getUTCMinutes()).padStart(2, "0");
  const seconds = String(date.getUTCSeconds()).padStart(2, "0");
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds} UTC`;
}
