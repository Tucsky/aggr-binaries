import type { TimelineEvent } from "./timelineTypes.js";
import { clampTs, type TimelineRange } from "./timelineUtils.js";

export function resolveOpenTsFromClick(
  clickedTs: number,
  marketRange: TimelineRange,
  markerEvent: TimelineEvent | null,
): number {
  const boundedTs = clampTs(clickedTs, marketRange.startTs, marketRange.endTs);
  if (!markerEvent) return boundedTs;
  const markerGapMs =
    Number.isFinite(markerEvent.gapMs) && (markerEvent.gapMs as number) > 0
      ? Math.floor(markerEvent.gapMs as number)
      : 0;
  const markerStartTs = clampTs(
    markerEvent.ts - markerGapMs,
    marketRange.startTs,
    marketRange.endTs,
  );
  const markerEndTs = clampTs(
    markerEvent.ts,
    marketRange.startTs,
    marketRange.endTs,
  );
  const minTs = Math.min(markerStartTs, markerEndTs);
  const maxTs = Math.max(markerStartTs, markerEndTs);
  return clampTs(boundedTs, minTs, maxTs);
}
