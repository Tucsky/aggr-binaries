export interface TimelinePopoverPlacementInput {
  pointerClientX: number;
  pointerClientY: number;
  markerLeftClientX: number;
  markerRightClientX: number;
  markerTopClientY: number;
  markerBottomClientY: number;
  popoverWidth: number;
  popoverHeight: number;
  viewportWidth: number;
  viewportHeight: number;
  offsetPx?: number;
  marginPx?: number;
}

export interface TimelinePopoverPlacement {
  left: number;
  top: number;
  placeLeft: boolean;
  placeAbove: boolean;
}

const DEFAULT_OFFSET_PX = 14;
const DEFAULT_MARGIN_PX = 10;

export function computeTimelinePopoverPlacement(
  input: TimelinePopoverPlacementInput,
): TimelinePopoverPlacement {
  const width = Math.max(1, Math.floor(input.popoverWidth));
  const height = Math.max(1, Math.floor(input.popoverHeight));
  const viewportWidth = Math.max(1, Math.floor(input.viewportWidth));
  const viewportHeight = Math.max(1, Math.floor(input.viewportHeight));
  const offsetPx = Math.max(0, Math.floor(input.offsetPx ?? DEFAULT_OFFSET_PX));
  const marginPx = Math.max(0, Math.floor(input.marginPx ?? DEFAULT_MARGIN_PX));

  const maxLeft = Math.max(marginPx, viewportWidth - width - marginPx);
  const maxTop = Math.max(marginPx, viewportHeight - height - marginPx);

  const rightCandidate = input.pointerClientX + offsetPx;
  const leftCandidate = input.pointerClientX - offsetPx - width;
  let left = rightCandidate;
  let placeLeft = false;
  if (rightCandidate > maxLeft) {
    left = leftCandidate;
    placeLeft = true;
  }
  left = clamp(left, marginPx, maxLeft);
  if (!placeLeft) placeLeft = left < input.pointerClientX;

  const belowCandidate = input.pointerClientY + offsetPx;
  const aboveCandidate = input.pointerClientY - offsetPx - height;
  let top = belowCandidate;
  let placeAbove = false;
  if (belowCandidate > maxTop) {
    top = aboveCandidate;
    placeAbove = true;
  }
  top = clamp(top, marginPx, maxTop);
  if (!placeAbove) placeAbove = top < input.pointerClientY;

  return {
    left: Math.round(left),
    top: Math.round(top),
    placeLeft,
    placeAbove,
  };
}

function clamp(value: number, min: number, max: number): number {
  if (value < min) return min;
  if (value > max) return max;
  return value;
}
