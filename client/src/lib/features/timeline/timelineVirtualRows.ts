export interface TimelineVirtualWindow {
  startIndex: number;
  endIndex: number;
  topPadding: number;
  bottomPadding: number;
}

export function computeTimelineVirtualWindow(
  totalRows: number,
  scrollTop: number,
  viewportHeight: number,
  rowHeight: number,
  overscan: number,
): TimelineVirtualWindow {
  const safeTotalRows = Math.max(0, Math.floor(totalRows));
  const safeScrollTop = Math.max(0, scrollTop);
  const safeViewportHeight = Math.max(0, viewportHeight);
  const safeRowHeight = Math.max(1, Math.floor(rowHeight));
  const safeOverscan = Math.max(0, Math.floor(overscan));

  const startIndex = Math.max(0, Math.floor(safeScrollTop / safeRowHeight) - safeOverscan);
  const endIndex = Math.min(
    safeTotalRows,
    Math.ceil((safeScrollTop + safeViewportHeight) / safeRowHeight) + safeOverscan,
  );
  const topPadding = startIndex * safeRowHeight;
  const bottomPadding = Math.max(0, (safeTotalRows - endIndex) * safeRowHeight);

  return {
    startIndex,
    endIndex,
    topPadding,
    bottomPadding,
  };
}
