<script lang="ts">
  import { createEventDispatcher, onMount } from "svelte";
  import Ellipsis from "lucide-svelte/icons/ellipsis";
  import {
    clampTs,
    eventKind,
    findTimelineEventWindow,
    toTimelineTs,
    toTimelineX,
    type TimelineRange,
  } from "./timelineUtils.js";
  import { eventPaintStyle, roundedRectPath } from "./timelineRowCanvas.js";
  import {
    drawTimelineSourceRect,
    INDEXED_SOURCE_STYLE,
    PROCESSED_SOURCE_STYLE,
    resolveTimelineSourceRange,
  } from "./timelineRowSource.js";
  import type { TimelineEvent, TimelineHoverEvent, TimelineMarket } from "./timelineTypes.js";

  interface OpenDetail {
    market: TimelineMarket;
    ts: number;
  }

  interface HoverDetail {
    ts: number | null;
    x: number | null;
    hoveredEvent: TimelineHoverEvent | null;
  }

  interface ZoomDetail {
    centerTs: number;
    deltaY: number;
  }

  interface PanDetail {
    deltaMs: number;
  }

  interface ActionsDetail {
    market: TimelineMarket;
    anchorEl: HTMLButtonElement | null;
  }

  interface MarkerHit {
    x1: number;
    x2: number;
    y1: number;
    y2: number;
    event: TimelineEvent;
  }

  export let market: TimelineMarket;
  export let events: TimelineEvent[] = [];
  export let range: TimelineRange;
  export let viewRange: TimelineRange;
  export let timelineWidth = 1200;
  export let rowHeight = 33;
  export let titleWidth = 310;

  const dispatch = createEventDispatcher<{
    open: OpenDetail;
    hover: HoverDetail;
    zoom: ZoomDetail;
    pan: PanDetail;
    actions: ActionsDetail;
  }>();

  let canvasEl: HTMLCanvasElement | null = null;
  let markerHits: MarkerHit[] = [];
  let actionsButton: HTMLButtonElement | null = null;

  let pointerActive = false;
  let dragMoved = false;
  let lastPointerX = 0;
  let dragDistance = 0;

  $: timelineWidth = Math.max(1, Math.floor(timelineWidth));
  $: if (canvasEl) {
    // Keep canvas in lockstep with view/market/event updates.
    market;
    events;
    range;
    viewRange;
    rowHeight;
    timelineWidth;
    drawCanvas();
  }

  onMount(() => {
    drawCanvas();
  });

  function drawCanvas(): void {
    if (!canvasEl) return;
    const ctx = canvasEl.getContext("2d");
    if (!ctx) return;

    const dpr = Math.max(1, window.devicePixelRatio || 1);
    const cssWidth = timelineWidth;
    const cssHeight = rowHeight;
    const actualWidth = Math.max(1, Math.floor(cssWidth * dpr));
    const actualHeight = Math.max(1, Math.floor(cssHeight * dpr));
    if (canvasEl.width !== actualWidth) canvasEl.width = actualWidth;
    if (canvasEl.height !== actualHeight) canvasEl.height = actualHeight;
    canvasEl.style.width = `${cssWidth}px`;
    canvasEl.style.height = `${cssHeight}px`;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, cssWidth, cssHeight);

    markerHits = [];

    const indexedSource = resolveTimelineSourceRange(
      market.indexedStartTs,
      market.indexedEndTs,
      range,
    );
    const processedSource = resolveTimelineSourceRange(
      market.processedStartTs,
      market.processedEndTs,
      range,
    );

    if (indexedSource) {
      drawTimelineSourceRect(
        ctx,
        indexedSource.startTs,
        indexedSource.endTs,
        viewRange,
        cssWidth,
        cssHeight,
        INDEXED_SOURCE_STYLE,
      );
    }
    if (processedSource) {
      drawTimelineSourceRect(
        ctx,
        processedSource.startTs,
        processedSource.endTs,
        viewRange,
        cssWidth,
        cssHeight,
        PROCESSED_SOURCE_STYLE,
      );
    }

    const visibleWindow = findTimelineEventWindow(
      events,
      viewRange.startTs,
      viewRange.endTs,
    );

    for (let i = visibleWindow.startIndex; i < visibleWindow.endIndex; i += 1) {
      drawEvent(ctx, events[i], cssWidth, cssHeight);
    }
  }

  function drawEvent(
    ctx: CanvasRenderingContext2D,
    event: TimelineEvent,
    width: number,
    height: number,
  ): void {
    const gapMs =
      Number.isFinite(event.gapMs) && (event.gapMs as number) > 0
        ? (event.gapMs as number)
        : 0;
    const rawStartTs = event.ts - gapMs;
    const rawEndTs = event.ts;
    const eventStartTs = Math.max(rawStartTs, range.startTs);
    const eventEndTs = Math.min(rawEndTs, range.endTs);
    if (eventEndTs < viewRange.startTs || eventStartTs > viewRange.endTs)
      return;

    const x1 = toTimelineX(eventStartTs, viewRange, width);
    const x2 = toTimelineX(eventEndTs, viewRange, width);
    const left = Math.min(x1, x2);
    const right = Math.max(x1, x2);
    const pixelWidth = right - left;
    const style = eventPaintStyle(eventKind(event));

    const barHeight = 16;
    const y = Math.floor((height - barHeight) / 2);

    if (pixelWidth >= 3) {
      const drawWidth = Math.max(3, pixelWidth);
      roundedRectPath(ctx, left, y, drawWidth, barHeight, {
        topLeft: 2,
        topRight: 2,
        bottomLeft: 2,
        bottomRight: 2,
      });
      ctx.fillStyle = style.fill;
      ctx.fill();
      ctx.strokeStyle = style.stroke;
      ctx.lineWidth = 1;
      ctx.stroke();
      markerHits.push({ x1: left, x2: left + drawWidth, y1: y, y2: y + barHeight, event });
      return;
    }

    const lineX = Math.min(width - 0.5, Math.max(0.5, Math.floor(right) + 0.5));
    ctx.strokeStyle = style.stroke;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(lineX, y);
    ctx.lineTo(lineX, y + barHeight);
    ctx.stroke();
    markerHits.push({ x1: lineX - 2, x2: lineX + 2, y1: y, y2: y + barHeight, event });
  }

  function handleCanvasClick(event: MouseEvent): void {
    if (dragMoved || !canvasEl) {
      dragMoved = false;
      return;
    }
    const rect = canvasEl.getBoundingClientRect();
    const x = clampTs(event.clientX - rect.left, 0, timelineWidth);
    const y = clampTs(event.clientY - rect.top, 0, rowHeight);
    const marker = findMarkerAtPoint(x, y);
    if (marker) {
      dispatch("open", { market, ts: marker.event.ts });
      return;
    }
    const clickedTs = toTimelineTs(x, viewRange, timelineWidth);
    const boundedTs = clampTs(clickedTs, market.startTs, market.endTs);
    dispatch("open", { market, ts: boundedTs });
  }

  function handleWheel(event: WheelEvent): void {
    if (!canvasEl) return;
    const rect = canvasEl.getBoundingClientRect();
    const x = clampTs(event.clientX - rect.left, 0, timelineWidth);

    if (event.ctrlKey || event.metaKey) {
      event.preventDefault();
      const centerTs = toTimelineTs(x, viewRange, timelineWidth);
      dispatch("zoom", { centerTs, deltaY: event.deltaY });
      return;
    }

    const horizontalDelta =
      Math.abs(event.deltaX) >= Math.abs(event.deltaY) ? event.deltaX : 0;
    if (horizontalDelta !== 0) {
      event.preventDefault();
      const span = Math.max(1, viewRange.endTs - viewRange.startTs);
      const msPerPx = span / Math.max(1, timelineWidth);
      dispatch("pan", { deltaMs: Math.round(-horizontalDelta * msPerPx) * -1 });
    }
  }

  function handlePointerDown(event: PointerEvent): void {
    if (!canvasEl) return;
    if (event.pointerType === "touch") return;
    pointerActive = true;
    dragMoved = false;
    dragDistance = 0;
    lastPointerX = event.clientX;
    canvasEl.setPointerCapture(event.pointerId);
    emitHoverTs(event.clientX, event.clientY);
  }

  function handlePointerMove(event: PointerEvent): void {
    if (event.pointerType === "touch") return;
    emitHoverTs(event.clientX, event.clientY);
    if (!pointerActive) return;

    const dx = event.clientX - lastPointerX;
    lastPointerX = event.clientX;
    dragDistance += Math.abs(dx);
    if (dragDistance > 2) {
      dragMoved = true;
    }
    if (!dragMoved || dx === 0) return;

    const span = Math.max(1, viewRange.endTs - viewRange.startTs);
    const msPerPx = span / Math.max(1, timelineWidth);
    dispatch("pan", { deltaMs: Math.round(dx * msPerPx) });
  }

  function handlePointerUp(event: PointerEvent): void {
    if (!canvasEl) return;
    if (event.pointerType === "touch") return;
    if (pointerActive) {
      try {
        canvasEl.releasePointerCapture(event.pointerId);
      } catch {
        // ignore
      }
    }
    pointerActive = false;
  }

  function handlePointerLeave(): void {
    if (!pointerActive) {
      dispatch("hover", { ts: null, x: null, hoveredEvent: null });
    }
  }

  function emitHoverTs(clientX: number, clientY: number): void {
    if (!canvasEl) return;
    const rect = canvasEl.getBoundingClientRect();
    const x = clampTs(clientX - rect.left, 0, timelineWidth);
    const y = clampTs(clientY - rect.top, 0, rowHeight);
    const ts = toTimelineTs(x, viewRange, timelineWidth);
    const marker = findMarkerAtPoint(x, y);
    const hoveredEvent: TimelineHoverEvent | null = marker
      ? {
          event: marker.event,
          market,
          pointerClientX: clientX,
          pointerClientY: clientY,
          markerLeftClientX: rect.left + marker.x1,
          markerRightClientX: rect.left + marker.x2,
          markerTopClientY: rect.top + marker.y1,
          markerBottomClientY: rect.top + marker.y2,
        }
      : null;
    dispatch("hover", { ts, x, hoveredEvent });
  }

  function findMarkerAtPoint(x: number, y: number): MarkerHit | null {
    for (let i = markerHits.length - 1; i >= 0; i -= 1) {
      const hit = markerHits[i];
      if (x >= hit.x1 && x <= hit.x2 && y >= hit.y1 && y <= hit.y2) return hit;
    }
    return null;
  }

  function handleActionsClick(): void {
    dispatch("actions", { market, anchorEl: actionsButton });
  }
</script>

<div
  class="grid min-w-max items-center hover:bg-slate-900/50"
  style={`grid-template-columns: ${titleWidth}px ${timelineWidth}px; height: ${rowHeight}px;`}
>
  <div
    class="sticky left-0 z-20 h-full border-r border-slate-800 bg-slate-900/50 px-2 text-slate-200"
  >
    <div class="flex h-full items-center gap-2 text-[13px] tracking-[0.02em]">
      <div class="flex flex-col text-xs font-mono leading-none">
        <div class="opacity-50"><span>{market.collector}</span>:<span>{market.exchange}</span></div>
        <strong>{market.symbol}</strong>
      </div>
      <button
        bind:this={actionsButton}
        class="ml-auto flex h-6 w-6 items-center justify-center rounded-md border-none py-1 text-slate-300 hover:bg-slate-800/50 hover:text-slate-100"
        type="button"
        aria-label="Row actions"
        on:click={handleActionsClick}
      >
        <Ellipsis class="h-4 w-4" aria-hidden="true" strokeWidth={1.9} />
      </button>
    </div>
  </div>

  <div class="h-full">
    <canvas
      bind:this={canvasEl}
      class="h-full w-full cursor-crosshair"
      on:click={handleCanvasClick}
      on:wheel={handleWheel}
      on:pointerdown={handlePointerDown}
      on:pointermove={handlePointerMove}
      on:pointerup={handlePointerUp}
      on:pointercancel={handlePointerUp}
      on:pointerleave={handlePointerLeave}
    ></canvas>
  </div>
</div>
