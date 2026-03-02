import { TimelineEventKind } from "./timelineTypes.js";
import { toTimelineX, type TimelineRange } from "./timelineUtils.js";

export interface EventPaintStyle {
  fill: string;
  stroke: string;
}

export interface TimelineHighlightStyle {
  fill: string;
  stroke: string;
}

export interface RectCorners {
  topLeft: number;
  topRight: number;
  bottomLeft: number;
  bottomRight: number;
}

export interface TimelineHighlightWindow {
  x1: number;
  x2: number;
}

export const VISIBLE_RANGE_HIGHLIGHT_STYLE: TimelineHighlightStyle = {
  fill: "rgba(125, 177, 255, 0.12)",
  stroke: "rgba(125, 177, 255, 0.85)",
};

export function eventPaintStyle(kind: TimelineEventKind): EventPaintStyle {
  if (kind === "gap_fixed") return { fill: "rgba(46, 204, 113, 0.25)", stroke: "rgba(67, 238, 143, 0.25)" };
  if (kind === "skipped_large_gap") return { fill: "rgba(245, 158, 11, 0.35)", stroke: "rgba(251, 191, 36, 0.8)" };
  if (kind === "gap") return { fill: "rgba(201, 48, 48, 0.9)", stroke: "rgba(246, 72, 72, 0.9)" };
  return { fill: "#e65100", stroke: "#f57c00" };
}

export function roundedRectPath(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  w: number,
  h: number,
  radius: RectCorners,
): void {
  const max = Math.min(w / 2, h / 2);
  const tl = Math.min(radius.topLeft, max);
  const tr = Math.min(radius.topRight, max);
  const br = Math.min(radius.bottomRight, max);
  const bl = Math.min(radius.bottomLeft, max);

  ctx.beginPath();
  ctx.moveTo(x + tl, y);
  ctx.lineTo(x + w - tr, y);
  if (tr > 0) ctx.arcTo(x + w, y, x + w, y + tr, tr);
  else ctx.lineTo(x + w, y);
  ctx.lineTo(x + w, y + h - br);
  if (br > 0) ctx.arcTo(x + w, y + h, x + w - br, y + h, br);
  else ctx.lineTo(x + w, y + h);
  ctx.lineTo(x + bl, y + h);
  if (bl > 0) ctx.arcTo(x, y + h, x, y + h - bl, bl);
  else ctx.lineTo(x, y + h);
  ctx.lineTo(x, y + tl);
  if (tl > 0) ctx.arcTo(x, y, x + tl, y, tl);
  else ctx.lineTo(x, y);
  ctx.closePath();
}

export function resolveTimelineHighlightWindow(
  highlightRange: TimelineRange,
  viewRange: TimelineRange,
  width: number,
): TimelineHighlightWindow | null {
  if (highlightRange.endTs < viewRange.startTs || highlightRange.startTs > viewRange.endTs) return null;

  const startTs = highlightRange.startTs < viewRange.startTs ? viewRange.startTs : highlightRange.startTs;
  const endTs = highlightRange.endTs > viewRange.endTs ? viewRange.endTs : highlightRange.endTs;
  if (endTs <= startTs) return null;

  const safeWidth = Math.max(1, width);
  const x1 = toTimelineX(startTs, viewRange, safeWidth);
  const x2 = toTimelineX(endTs, viewRange, safeWidth);
  if (x2 <= x1) return null;
  return { x1, x2 };
}

export function drawTimelineVisibleRangeHighlight(
  ctx: CanvasRenderingContext2D,
  highlightRange: TimelineRange,
  viewRange: TimelineRange,
  width: number,
  height: number,
  style: TimelineHighlightStyle = VISIBLE_RANGE_HIGHLIGHT_STYLE,
): void {
  const window = resolveTimelineHighlightWindow(highlightRange, viewRange, width);
  if (!window) return;

  const insetY = 1;
  const boxHeight = Math.max(1, height - insetY * 2);
  roundedRectPath(ctx, window.x1, insetY, window.x2 - window.x1, boxHeight, {
    topLeft: 2,
    topRight: 2,
    bottomLeft: 2,
    bottomRight: 2,
  });
  ctx.fillStyle = style.fill;
  ctx.fill();
  ctx.strokeStyle = style.stroke;
  ctx.lineWidth = 1;
  ctx.stroke();
}
