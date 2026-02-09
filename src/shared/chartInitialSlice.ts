export interface ChartInitialSlice {
  fromIndex: number;
  toIndex: number;
}

export interface ChartVisibleRange {
  from: number;
  to: number;
}

const DEFAULT_WINDOW = 500;
const DEFAULT_VISIBLE_SPAN = 120;

export function computeChartInitialSlice(
  anchorIndex: number,
  records: number,
  windowSize: number = DEFAULT_WINDOW,
): ChartInitialSlice | null {
  if (!Number.isFinite(anchorIndex) || !Number.isFinite(records) || records <= 0) return null;

  const lastIndex = Math.max(0, Math.trunc(records) - 1);
  const anchor = clamp(Math.trunc(anchorIndex), 0, lastIndex);
  const window = Math.max(1, Math.trunc(windowSize));

  if (anchor < lastIndex) {
    return {
      fromIndex: anchor,
      toIndex: Math.min(lastIndex, anchor + window),
    };
  }

  return {
    fromIndex: Math.max(0, anchor - window),
    toIndex: anchor,
  };
}

function clamp(value: number, min: number, max: number): number {
  if (value < min) return min;
  if (value > max) return max;
  return value;
}

export function computeAnchoredVisibleRange(
  pointCount: number,
  currentSpan: number | null,
): ChartVisibleRange | null {
  if (!Number.isFinite(pointCount) || pointCount <= 0) return null;
  const maxTo = Math.max(0, Math.trunc(pointCount) - 1);
  const span =
    currentSpan !== null && Number.isFinite(currentSpan)
      ? Math.max(10, Math.round(currentSpan))
      : DEFAULT_VISIBLE_SPAN;
  return { from: 0, to: Math.min(maxTo, span) };
}
