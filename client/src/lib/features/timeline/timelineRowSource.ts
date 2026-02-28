import { roundedRectPath } from "./timelineRowCanvas.js";
import { clampTs, toTimelineX, type TimelineRange } from "./timelineUtils.js";

export interface TimelineSourceStyle {
  fill: string;
  stroke: string;
}

export interface TimelineSourceRect {
  x1: number;
  x2: number;
  y: number;
  height: number;
  roundLeft: boolean;
  roundRight: boolean;
}

export const INDEXED_SOURCE_STYLE: TimelineSourceStyle = {
  fill: "rgba(100,116,139,0.42)",
  stroke: "rgba(148,163,184,0.76)",
};

export const PROCESSED_SOURCE_STYLE: TimelineSourceStyle = {
  fill: "rgba(46,95,171,0.50)",
  stroke: "rgba(125,177,255,0.72)",
};

export function resolveTimelineSourceRange(
  startTs: number | null | undefined,
  endTs: number | null | undefined,
  range: TimelineRange,
): TimelineRange | null {
  if (!Number.isFinite(startTs) || !Number.isFinite(endTs)) return null;
  const clampedStart = clampTs(startTs as number, range.startTs, range.endTs);
  const clampedEnd = clampTs(endTs as number, range.startTs, range.endTs);
  if (clampedEnd < clampedStart) return null;
  return { startTs: clampedStart, endTs: clampedEnd };
}

export function drawTimelineSourceRect(
  ctx: CanvasRenderingContext2D,
  sourceStartTs: number,
  sourceEndTs: number,
  viewRange: TimelineRange,
  width: number,
  height: number,
  style: TimelineSourceStyle,
): TimelineSourceRect | null {
  const rect = resolveTimelineSourceRect(
    sourceStartTs,
    sourceEndTs,
    viewRange,
    width,
    height,
  );
  if (!rect) return null;

  const radius = 4;
  roundedRectPath(ctx, rect.x1, rect.y, rect.x2 - rect.x1, rect.height, {
    topLeft: rect.roundLeft ? radius : 0,
    bottomLeft: rect.roundLeft ? radius : 0,
    topRight: rect.roundRight ? radius : 0,
    bottomRight: rect.roundRight ? radius : 0,
  });
  ctx.fillStyle = style.fill;
  ctx.fill();
  ctx.strokeStyle = style.stroke;
  ctx.lineWidth = 1;
  ctx.stroke();
  return rect;
}

export function resolveTimelineSourceRect(
  sourceStartTs: number,
  sourceEndTs: number,
  viewRange: TimelineRange,
  width: number,
  height: number,
): TimelineSourceRect | null {
  if (sourceEndTs < viewRange.startTs || sourceStartTs > viewRange.endTs) return null;

  const startXRaw = toTimelineX(sourceStartTs, viewRange, width);
  const endXRaw = toTimelineX(sourceEndTs, viewRange, width);
  const x1 = Math.max(0, Math.min(width, startXRaw));
  const x2 = Math.max(0, Math.min(width, endXRaw));
  if (x2 <= x1) return null;

  const barHeight = 20;
  const y = Math.floor((height - barHeight) / 2);
  return {
    x1,
    x2,
    y,
    height: barHeight,
    roundLeft: sourceStartTs >= viewRange.startTs,
    roundRight: sourceEndTs <= viewRange.endTs,
  };
}
