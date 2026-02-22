<script lang="ts">
  import { onDestroy, tick } from "svelte";
  import { formatElapsedDhms, formatEstimatedMiss, formatRecoveredCount } from "./timelineEventPopoverFormat.js";
  import { computeTimelinePopoverPlacement } from "./timelineEventPopoverPosition.js";
  import type { TimelineHoverEvent } from "./timelineTypes.js";
  import { formatTimelineTsLabel } from "./timelineViewport.js";

  const DEFAULT_POPOVER_WIDTH = 260;
  const DEFAULT_POPOVER_HEIGHT = 140;
  const HIDE_DELAY_MS = 120;
  const POSITION_EASE = 0.28;
  const POSITION_SNAP_PX = 0.45;

  export let hoveredEvent: TimelineHoverEvent | null = null;

  let popoverEl: HTMLDivElement | null = null;
  let mounted = false;
  let visible = false;
  let renderData: TimelineHoverEvent | null = null;
  let desiredLeft = 0;
  let desiredTop = 0;
  let renderedLeft = 0;
  let renderedTop = 0;
  let hideTimer: ReturnType<typeof setTimeout> | null = null;
  let rafId = 0;

  $: if (hoveredEvent) {
    showPopover(hoveredEvent);
  } else {
    hidePopoverWithDelay();
  }

  function showPopover(next: TimelineHoverEvent): void {
    clearHideTimer();
    renderData = next;
    visible = true;
    if (mounted && popoverEl) {
      updateDesiredPosition();
      ensurePositionAnimation();
      return;
    }
    const wasMounted = mounted;
    if (!mounted) mounted = true;
    void tick().then(() => {
      if (!mounted || !renderData) return;
      updateDesiredPosition();
      if (!wasMounted) {
        renderedLeft = desiredLeft;
        renderedTop = desiredTop;
        return;
      }
      ensurePositionAnimation();
    });
  }

  function hidePopoverWithDelay(): void {
    if (!mounted) return;
    visible = false;
    clearHideTimer();
    hideTimer = setTimeout(() => {
      if (hoveredEvent) return;
      mounted = false;
      renderData = null;
      stopPositionAnimation();
    }, HIDE_DELAY_MS);
  }

  function clearHideTimer(): void {
    if (!hideTimer) return;
    clearTimeout(hideTimer);
    hideTimer = null;
  }

  function updateDesiredPosition(): void {
    if (!renderData) return;
    const popoverWidth = Math.max(1, popoverEl?.offsetWidth ?? DEFAULT_POPOVER_WIDTH);
    const popoverHeight = Math.max(1, popoverEl?.offsetHeight ?? DEFAULT_POPOVER_HEIGHT);
    const placement = computeTimelinePopoverPlacement({
      pointerClientX: renderData.pointerClientX,
      pointerClientY: renderData.pointerClientY,
      markerLeftClientX: renderData.markerLeftClientX,
      markerRightClientX: renderData.markerRightClientX,
      markerTopClientY: renderData.markerTopClientY,
      markerBottomClientY: renderData.markerBottomClientY,
      popoverWidth,
      popoverHeight,
      viewportWidth: Math.max(1, window.innerWidth || 1),
      viewportHeight: Math.max(1, window.innerHeight || 1),
    });
    desiredLeft = placement.left;
    desiredTop = placement.top;
  }

  function ensurePositionAnimation(): void {
    if (rafId !== 0) return;
    rafId = requestAnimationFrame(stepPosition);
  }

  function stepPosition(): void {
    rafId = 0;
    const dx = desiredLeft - renderedLeft;
    const dy = desiredTop - renderedTop;
    if (Math.abs(dx) <= POSITION_SNAP_PX && Math.abs(dy) <= POSITION_SNAP_PX) {
      renderedLeft = desiredLeft;
      renderedTop = desiredTop;
      return;
    }
    renderedLeft += dx * POSITION_EASE;
    renderedTop += dy * POSITION_EASE;
    rafId = requestAnimationFrame(stepPosition);
  }

  function stopPositionAnimation(): void {
    if (rafId === 0) return;
    cancelAnimationFrame(rafId);
    rafId = 0;
  }

  function handleWindowResize(): void {
    if (!mounted || !renderData) return;
    updateDesiredPosition();
    if (visible) ensurePositionAnimation();
  }

  function basename(relativePath: string): string {
    const slash = relativePath.lastIndexOf("/");
    const backslash = relativePath.lastIndexOf("\\");
    const split = slash > backslash ? slash : backslash;
    return split >= 0 ? relativePath.slice(split + 1) : relativePath;
  }

  function formatLineRange(startLine: number, endLine: number): string {
    const start = Math.max(1, Math.floor(startLine));
    const end = Math.max(start, Math.floor(endLine));
    if (start === end) return `L${start}`;
    return `L${start}-${end}`;
  }

  function formatEventType(eventType: string): string {
    return eventType.split("_").join(" ");
  }

  function resolveGapStartTs(data: TimelineHoverEvent | null): number | null {
    if (!data || data.event.eventType !== "gap") return null;
    const gapMs = data.event.gapMs;
    if (gapMs === null || !Number.isFinite(gapMs) || gapMs <= 0) return null;
    return data.event.ts - Math.floor(gapMs);
  }

  function formatGapFixStatus(status: string | null | undefined): string {
    if (!status) return "pending";
    return status.split("_").join(" ");
  }

  $: gapStartTs = resolveGapStartTs(renderData);

  if (typeof window !== "undefined") {
    window.addEventListener("resize", handleWindowResize, { passive: true });
  }

  onDestroy(() => {
    clearHideTimer();
    stopPositionAnimation();
    if (typeof window !== "undefined") {
      window.removeEventListener("resize", handleWindowResize);
    }
  });
</script>

{#if mounted && renderData}
  <div
    bind:this={popoverEl}
    class="pointer-events-none fixed left-0 top-0 z-30 max-w-[340px] rounded-md border border-slate-700/95 bg-slate-950/95 px-2.5 py-2 shadow-2xl backdrop-blur-sm transition-opacity duration-100"
    style={`opacity: ${visible ? 1 : 0}; transform: translate3d(${renderedLeft}px, ${renderedTop}px, 0);`}
  >
    <div class="flex items-center justify-between gap-3 text-[10px] uppercase tracking-[0.08em] text-slate-400">
      <span>{formatEventType(renderData.event.eventType)}</span>
      <span>{renderData.market.collector}:{renderData.market.exchange}</span>
    </div>
    <div class="mt-1 text-xs text-slate-200">
      {basename(renderData.event.relativePath)}:{formatLineRange(renderData.event.startLine, renderData.event.endLine)}
    </div>
    {#if renderData.event.eventType === "gap"}
      <div class="mt-2 grid grid-cols-[72px_1fr] gap-x-2 gap-y-1 text-[11px] leading-tight">
        <span class="text-slate-500">Start time</span>
        <span class="text-slate-200">{gapStartTs === null ? "n/a" : formatTimelineTsLabel(gapStartTs)}</span>
        <span class="text-slate-500">Elapsed</span>
        <span class="text-slate-200">{formatElapsedDhms(renderData.event.gapMs)}</span>
        <span class="text-slate-500">Miss</span>
        <span class="text-slate-200">{formatEstimatedMiss(renderData.event.gapMiss)}</span>
        <span class="text-slate-500">Status</span>
        <span class="text-slate-200">{formatGapFixStatus(renderData.event.gapFixStatus)}</span>
        <span class="text-slate-500">Recovered</span>
        <span class="text-slate-200">{formatRecoveredCount(renderData.event.gapFixRecovered)}</span>
      </div>
    {/if}
  </div>
{/if}
