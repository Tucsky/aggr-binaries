export const TIMELINE_ROW_HEIGHT = 33;
export const MIN_TIMELINE_VIEWPORT_WIDTH = 320;
export const DEFAULT_TIMELINE_TITLE_WIDTH = 180;
export const MIN_TIMELINE_TITLE_WIDTH = 120;

export function normalizeTimelineViewportWidth(width: number): number {
  const safeWidth = Number.isFinite(width) ? Math.floor(width) : 0;
  if (safeWidth >= MIN_TIMELINE_VIEWPORT_WIDTH) return safeWidth;
  return MIN_TIMELINE_VIEWPORT_WIDTH;
}

export function resolveTimelineMaxTitleWidth(containerWidth: number): number {
  const safeContainerWidth = Number.isFinite(containerWidth) ? Math.floor(containerWidth) : 0;
  const maxTitleWidth = safeContainerWidth - MIN_TIMELINE_VIEWPORT_WIDTH;
  if (maxTitleWidth >= MIN_TIMELINE_TITLE_WIDTH) return maxTitleWidth;
  return MIN_TIMELINE_TITLE_WIDTH;
}

export function clampTimelineTitleWidth(
  requestedWidth: number,
  containerWidth: number,
): number {
  const safeRequested = Number.isFinite(requestedWidth)
    ? Math.floor(requestedWidth)
    : DEFAULT_TIMELINE_TITLE_WIDTH;
  const maxTitleWidth = resolveTimelineMaxTitleWidth(containerWidth);
  if (safeRequested <= MIN_TIMELINE_TITLE_WIDTH) return MIN_TIMELINE_TITLE_WIDTH;
  if (safeRequested >= maxTitleWidth) return maxTitleWidth;
  return safeRequested;
}

export function computeTimelineViewportWidth(
  containerWidth: number,
  titleWidth: number,
): number {
  const safeContainerWidth = Number.isFinite(containerWidth) ? Math.floor(containerWidth) : 0;
  const clampedTitleWidth = clampTimelineTitleWidth(titleWidth, safeContainerWidth);
  return normalizeTimelineViewportWidth(safeContainerWidth - clampedTitleWidth);
}
