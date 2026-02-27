import { toTimelineTs, type TimelineRange } from "./timelineUtils.js";

export interface TimelineSurfaceCoordinates {
  x: number;
  ts: number;
}

export function resolveTimelineSurfaceX(
  clientX: number,
  surfaceLeft: number,
  titleWidth: number,
  timelineWidth: number,
): number | null {
  if (!Number.isFinite(clientX) || !Number.isFinite(surfaceLeft) || !Number.isFinite(titleWidth)) {
    return null;
  }
  const width = normalizeTimelineWidth(timelineWidth);
  const x = clientX - surfaceLeft - titleWidth;
  if (x < 0 || x > width) return null;
  return x;
}

export function resolveTimelineSurfaceCoordinates(
  clientX: number,
  surfaceLeft: number,
  titleWidth: number,
  timelineWidth: number,
  viewRange: TimelineRange,
): TimelineSurfaceCoordinates | null {
  const x = resolveTimelineSurfaceX(clientX, surfaceLeft, titleWidth, timelineWidth);
  if (x === null) return null;
  const width = normalizeTimelineWidth(timelineWidth);
  return { x, ts: toTimelineTs(x, viewRange, width) };
}

export function computeTimelinePanDeltaMsFromPointer(
  pointerDeltaX: number,
  viewRange: TimelineRange,
  timelineWidth: number,
): number {
  if (!Number.isFinite(pointerDeltaX) || pointerDeltaX === 0) return 0;
  const msPerPx = resolveTimelineMsPerPx(viewRange, timelineWidth);
  return Math.round(pointerDeltaX * msPerPx) * -1;
}

export function computeTimelinePanDeltaMsFromWheel(
  horizontalWheelDelta: number,
  viewRange: TimelineRange,
  timelineWidth: number,
): number {
  if (!Number.isFinite(horizontalWheelDelta) || horizontalWheelDelta === 0) return 0;
  const msPerPx = resolveTimelineMsPerPx(viewRange, timelineWidth);
  return Math.round(horizontalWheelDelta * msPerPx);
}

function resolveTimelineMsPerPx(viewRange: TimelineRange, timelineWidth: number): number {
  const span = Math.max(1, viewRange.endTs - viewRange.startTs);
  return span / normalizeTimelineWidth(timelineWidth);
}

function normalizeTimelineWidth(width: number): number {
  return Math.max(1, Math.floor(width));
}
