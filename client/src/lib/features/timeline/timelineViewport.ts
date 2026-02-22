import type { TimelineRange } from "./timelineUtils.js";

export const DEFAULT_MIN_VIEW_SPAN_MS = 60_000;
export const DEFAULT_ZOOM_SENSITIVITY = 0.008;
export const DEFAULT_PAN_OVERSCROLL_RATIO = 0;

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
): TimelineRange {
  const currentSpan = Math.max(1, viewRange.endTs - viewRange.startTs);
  const fullSpan = Math.max(1, selectedRange.endTs - selectedRange.startTs);
  const factor = Math.exp(deltaY * zoomSensitivity);
  let nextSpan = Math.round(currentSpan * factor);
  if (nextSpan < minViewSpanMs) nextSpan = minViewSpanMs;
  if (nextSpan > fullSpan) nextSpan = fullSpan;

  const ratioRaw = (centerTs - viewRange.startTs) / currentSpan;
  const ratio = Math.min(1, Math.max(0, ratioRaw));
  let nextStart = Math.floor(centerTs - ratio * nextSpan);
  let nextEnd = nextStart + nextSpan;
  if (nextStart < selectedRange.startTs) {
    const delta = selectedRange.startTs - nextStart;
    nextStart += delta;
    nextEnd += delta;
  }
  if (nextEnd > selectedRange.endTs) {
    const delta = nextEnd - selectedRange.endTs;
    nextStart -= delta;
    nextEnd -= delta;
  }
  return {
    startTs: Math.max(selectedRange.startTs, nextStart),
    endTs: Math.min(selectedRange.endTs, nextEnd),
  };
}

export function panTimelineRange(
  selectedRange: TimelineRange,
  viewRange: TimelineRange,
  deltaMs: number,
  overscrollRatio = DEFAULT_PAN_OVERSCROLL_RATIO,
): TimelineRange {
  if (deltaMs === 0) return viewRange;
  const span = Math.max(1, viewRange.endTs - viewRange.startTs);
  const clampedOverscrollRatio = Math.max(0, overscrollRatio);
  const overscrollMs = Math.round(span * clampedOverscrollRatio);
  const minStartTs = selectedRange.startTs - overscrollMs;
  const maxEndTs = selectedRange.endTs + overscrollMs;
  let nextStart = viewRange.startTs + deltaMs;
  let nextEnd = viewRange.endTs + deltaMs;
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
