<script lang="ts">
  import { formatTimelineTsLabel } from "./timelineViewport.js";
  import type { TimelineRange } from "./timelineUtils.js";

  export let titleWidth: number;
  export let timelineWidth: number;
  export let rowHeight: number;
  export let marketCount = 0;
  export let selectedRange: TimelineRange | null = null;
  export let viewRange: TimelineRange | null = null;
  export let loadingEvents = false;
  export let eventsError = "";

  $: safeMarketCount = Number.isFinite(marketCount) && marketCount > 0 ? Math.floor(marketCount) : 0;
  $: selectedSpan = selectedRange ? Math.max(0, selectedRange.endTs - selectedRange.startTs) : 0;
  $: viewSpan = viewRange ? Math.max(0, viewRange.endTs - viewRange.startTs) : 0;
  $: coverageRatio = selectedSpan > 0 ? Math.min(100, Math.max(0, (viewSpan / selectedSpan) * 100)) : 0;
</script>

<div
  class="grid min-w-max items-center border-t border-slate-800 bg-slate-900/90 text-[11px] text-slate-300"
  style={`grid-template-columns: ${titleWidth}px ${timelineWidth}px; height: ${rowHeight}px;`}
>
  <div class="sticky left-0 z-20 h-full border-r border-slate-800 bg-slate-900/90 px-2 text-slate-400">
    <div class="flex h-full items-center font-mono uppercase tracking-[0.04em]">{safeMarketCount} markets</div>
  </div>
  <div class="flex h-full items-center gap-3 px-2 font-mono">
    {#if eventsError}
      <span class="truncate text-red-300">{eventsError}</span>
    {:else if loadingEvents}
      <span class="truncate text-slate-300">Loading events...</span>
    {:else}
      <span class="truncate text-slate-400">Viewport {coverageRatio.toFixed(1)}%</span>
    {/if}
    {#if viewRange}
      <span class="ml-auto truncate text-slate-400">
        {formatTimelineTsLabel(viewRange.startTs)} - {formatTimelineTsLabel(viewRange.endTs)}
      </span>
    {/if}
  </div>
</div>
