import { formatTimelineTsLabel } from "./timelineViewport.js";

export interface TimelineDebugState {
  selectedStart: number | null;
  selectedEnd: number | null;
  viewStart: number | null;
  viewEnd: number | null;
  selectedSpan: number | null;
  viewSpan: number | null;
  zoomRatio: number | null;
  atLeft: boolean;
  atRight: boolean;
  crosshairTs: number | null;
  crosshairPx: number | null;
  timelineWidth: number;
  lastZoomDelta: number;
  lastPanDeltaMs: number;
}

export function buildTimelineDebugText(state: TimelineDebugState): string {
  return [
    "timeline-debug",
    `selected.start: ${fmtTs(state.selectedStart)}`,
    `selected.end:   ${fmtTs(state.selectedEnd)}`,
    `view.start:     ${fmtTs(state.viewStart)}`,
    `view.end:       ${fmtTs(state.viewEnd)}`,
    `selected.span:  ${fmtNum(state.selectedSpan)}`,
    `view.span:      ${fmtNum(state.viewSpan)}`,
    `zoom.ratio:     ${fmtNum(state.zoomRatio)}`,
    `at.left:        ${state.atLeft}`,
    `at.right:       ${state.atRight}`,
    `crosshair.ts:   ${fmtTs(state.crosshairTs)}`,
    `crosshair.px:   ${fmtNum(state.crosshairPx)}`,
    `timeline.width: ${state.timelineWidth}`,
    `last.zoom.dy:   ${state.lastZoomDelta}`,
    `last.pan.ms:    ${state.lastPanDeltaMs}`,
  ].join("\n");
}

function fmtTs(ts: number | null): string {
  if (ts === null) return "null";
  const label = formatTimelineTsLabel(ts);
  return `${ts}\n\t\t\t\t(${label})`;
}

function fmtNum(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "null";
  return String(Math.round(value));
}
