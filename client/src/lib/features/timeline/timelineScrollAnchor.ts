export interface TimelineScrollAnchor {
  scrollTop: number;
  topRowKey: string | null;
  topRowOffset: number;
}

export function captureTimelineScrollAnchor<T>(
  rows: readonly T[],
  scrollTop: number,
  rowHeight: number,
  rowKey: (row: T) => string,
): TimelineScrollAnchor {
  const normalizedRowHeight = normalizeRowHeight(rowHeight);
  const normalizedScrollTop = normalizeNonNegative(scrollTop);
  const topIndex = Math.floor(normalizedScrollTop / normalizedRowHeight);
  const topRow = rows[topIndex];
  return {
    scrollTop: normalizedScrollTop,
    topRowKey: topRow ? rowKey(topRow) : null,
    topRowOffset: normalizedScrollTop - topIndex * normalizedRowHeight,
  };
}

export function resolveTimelineRestoredScrollTop<T>(
  rows: readonly T[],
  anchor: TimelineScrollAnchor,
  rowHeight: number,
  maxScrollTop: number,
  rowKey: (row: T) => string,
): number {
  const normalizedRowHeight = normalizeRowHeight(rowHeight);
  const normalizedMaxScrollTop = normalizeNonNegative(maxScrollTop);
  const fallback = clampScrollTop(anchor.scrollTop, normalizedMaxScrollTop);
  if (!anchor.topRowKey) return fallback;
  for (let i = 0; i < rows.length; i += 1) {
    if (rowKey(rows[i]) !== anchor.topRowKey) continue;
    const anchoredScrollTop = i * normalizedRowHeight + normalizeNonNegative(anchor.topRowOffset);
    return clampScrollTop(anchoredScrollTop, normalizedMaxScrollTop);
  }
  return fallback;
}

function normalizeRowHeight(rowHeight: number): number {
  return Number.isFinite(rowHeight) && rowHeight > 0 ? rowHeight : 1;
}

function normalizeNonNegative(value: number): number {
  if (!Number.isFinite(value) || value <= 0) return 0;
  return value;
}

function clampScrollTop(scrollTop: number, maxScrollTop: number): number {
  if (scrollTop >= maxScrollTop) return maxScrollTop;
  if (scrollTop <= 0) return 0;
  return scrollTop;
}
