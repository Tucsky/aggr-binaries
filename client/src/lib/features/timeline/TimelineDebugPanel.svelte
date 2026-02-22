<script lang="ts">
  import {
    buildTimelineDebugText,
    type TimelineDebugState,
  } from "./timelineDebug.js";
  import type { TimelineRange } from "./timelineUtils.js";

  export let selectedRange: TimelineRange | null = null;
  export let viewRange: TimelineRange | null = null;
  export let crosshairTs: number | null = null;
  export let crosshairPx: number | null = null;
  export let timelineWidth = 0;
  export let lastZoomDelta = 0;
  export let lastPanDeltaMs = 0;

  $: selectedSpan = selectedRange
    ? selectedRange.endTs - selectedRange.startTs
    : null;
  $: viewSpan = viewRange ? viewRange.endTs - viewRange.startTs : null;
  $: zoomRatio =
    selectedSpan !== null && viewSpan !== null && viewSpan > 0
      ? selectedSpan / viewSpan
      : null;
  $: atLeft =
    Boolean(selectedRange && viewRange) &&
    selectedRange!.startTs === viewRange!.startTs;
  $: atRight =
    Boolean(selectedRange && viewRange) &&
    selectedRange!.endTs === viewRange!.endTs;

  $: debugState = {
    selectedStart: selectedRange?.startTs ?? null,
    selectedEnd: selectedRange?.endTs ?? null,
    viewStart: viewRange?.startTs ?? null,
    viewEnd: viewRange?.endTs ?? null,
    selectedSpan,
    viewSpan,
    zoomRatio,
    atLeft,
    atRight,
    crosshairTs,
    crosshairPx,
    timelineWidth,
    lastZoomDelta,
    lastPanDeltaMs,
  } satisfies TimelineDebugState;

  $: debugText = buildTimelineDebugText(debugState);
</script>

<div class="hidden pointer-events-none absolute bottom-2 left-2 z-40 rounded border border-slate-700 bg-slate-900/95 px-2 py-1 opacity-50">
  <pre class="m-0 text-[10px] leading-4 text-slate-300">{debugText}</pre>
</div>
