<script lang="ts">
  import { createEventDispatcher, onMount } from "svelte";
  import {
    clampTs,
    clampMarketToRange,
    eventKind,
    toTimelineTs,
    toTimelineX,
    type TimelineRange,
  } from "./timelineUtils.js";
  import { eventPaintStyle, roundedRectPath } from "./timelineRowCanvas.js";
  import type { TimelineEvent, TimelineMarket } from "./timelineTypes.js";

  interface OpenDetail {
    market: TimelineMarket;
    ts: number;
  }

  interface HoverDetail {
    ts: number | null;
    x: number | null;
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
    event: TimelineEvent;
  }

  export let market: TimelineMarket;
  export let events: TimelineEvent[] = [];
  export let range: TimelineRange;
  export let viewRange: TimelineRange;
  export let contentWidth = 1200;
  export let rowHeight = 33;
  export let leftWidth = 310;
  export let rightWidth = 88;

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

  $: timelineWidth = Math.max(1, Math.floor(contentWidth));
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

    const source = clampMarketToRange(market, range);
    if (source) {
      drawSourceRect(ctx, source.startTs, source.endTs, cssWidth, cssHeight);
    }

    for (const event of events) {
      drawEvent(ctx, event, cssWidth, cssHeight);
    }
  }

  function drawSourceRect(
    ctx: CanvasRenderingContext2D,
    sourceStartTs: number,
    sourceEndTs: number,
    width: number,
    height: number,
  ): void {
    if (sourceEndTs < viewRange.startTs || sourceStartTs > viewRange.endTs) return;

    const startXRaw = toTimelineX(sourceStartTs, viewRange, width);
    const endXRaw = toTimelineX(sourceEndTs, viewRange, width);
    const x1 = Math.max(0, Math.min(width, startXRaw));
    const x2 = Math.max(0, Math.min(width, endXRaw));
    if (x2 <= x1) return;

    const barHeight = 20;
    const y = Math.floor((height - barHeight) / 2);
    const radius = 4;
    const roundLeft = sourceStartTs >= viewRange.startTs;
    const roundRight = sourceEndTs <= viewRange.endTs;
    const fillColor = "rgba(46,95,171,0.50)";
    const strokeColor = "rgba(125,177,255,0.72)";

    roundedRectPath(ctx, x1, y, x2 - x1, barHeight, {
      topLeft: roundLeft ? radius : 0,
      bottomLeft: roundLeft ? radius : 0,
      topRight: roundRight ? radius : 0,
      bottomRight: roundRight ? radius : 0,
    });
    ctx.fillStyle = fillColor;
    ctx.fill();
    ctx.strokeStyle = strokeColor;
    ctx.lineWidth = 1;
    ctx.stroke();

    // Hide side borders for overflowing edges while keeping rounded corners when visible.
    if (!roundLeft) {
      ctx.fillStyle = fillColor;
      ctx.fillRect(x1 - 1, y + 1, 2, Math.max(1, barHeight - 2));
    }
    if (!roundRight) {
      ctx.fillStyle = fillColor;
      ctx.fillRect(x2 - 1, y + 1, 2, Math.max(1, barHeight - 2));
    }

    if (!roundLeft || !roundRight) {
      // Repaint top/bottom only when masking a clipped side border.
      ctx.strokeStyle = strokeColor;
      ctx.lineWidth = 1;
      const top = y + 0.5;
      const bottom = y + barHeight - 0.5;
      ctx.beginPath();
      ctx.moveTo(x1 + (roundLeft ? 0.5 : 0), top);
      ctx.lineTo(x2 - (roundRight ? 0.5 : 0), top);
      ctx.moveTo(x1 + (roundLeft ? 0.5 : 0), bottom);
      ctx.lineTo(x2 - (roundRight ? 0.5 : 0), bottom);
      ctx.stroke();
    }
  }

  function drawEvent(ctx: CanvasRenderingContext2D, event: TimelineEvent, width: number, height: number): void {
    const gapMs = Number.isFinite(event.gapMs) && (event.gapMs as number) > 0 ? (event.gapMs as number) : 0;
    const rawStartTs = event.ts - gapMs;
    const rawEndTs = event.ts;
    const eventStartTs = Math.max(rawStartTs, range.startTs);
    const eventEndTs = Math.min(rawEndTs, range.endTs);
    if (eventEndTs < viewRange.startTs || eventStartTs > viewRange.endTs) return;

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
      markerHits.push({ x1: left, x2: left + drawWidth, event });
      return;
    }

    const lineX = Math.min(width - 0.5, Math.max(0.5, Math.floor(right) + 0.5));
    ctx.strokeStyle = style.stroke;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(lineX, y);
    ctx.lineTo(lineX, y + barHeight);
    ctx.stroke();
    markerHits.push({ x1: lineX - 2, x2: lineX + 2, event });
  }

  function handleCanvasClick(event: MouseEvent): void {
    if (dragMoved || !canvasEl) {
      dragMoved = false;
      return;
    }
    const rect = canvasEl.getBoundingClientRect();
    const x = clampTs(event.clientX - rect.left, 0, timelineWidth);
    const marker = markerHits.find((hit) => x >= hit.x1 && x <= hit.x2);
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

    const horizontalDelta = Math.abs(event.deltaX) >= Math.abs(event.deltaY) ? event.deltaX : 0;
    if (horizontalDelta !== 0) {
      event.preventDefault();
      const span = Math.max(1, viewRange.endTs - viewRange.startTs);
      const msPerPx = span / Math.max(1, timelineWidth);
      dispatch("pan", { deltaMs: Math.round(-horizontalDelta * msPerPx) });
    }
  }

  function handlePointerDown(event: PointerEvent): void {
    if (!canvasEl) return;
    pointerActive = true;
    dragMoved = false;
    dragDistance = 0;
    lastPointerX = event.clientX;
    canvasEl.setPointerCapture(event.pointerId);
    emitHoverTs(event.clientX);
  }

  function handlePointerMove(event: PointerEvent): void {
    emitHoverTs(event.clientX);
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
      dispatch("hover", { ts: null, x: null });
    }
  }

  function emitHoverTs(clientX: number): void {
    if (!canvasEl) return;
    const rect = canvasEl.getBoundingClientRect();
    const x = clampTs(clientX - rect.left, 0, timelineWidth);
    const ts = toTimelineTs(x, viewRange, timelineWidth);
    dispatch("hover", { ts, x });
  }

  function handleActionsClick(): void {
    dispatch("actions", { market, anchorEl: actionsButton });
  }
</script>

<div
  class="grid min-w-max items-center hover:bg-slate-900/50"
  style={`grid-template-columns: ${leftWidth}px ${contentWidth}px ${rightWidth}px; height: ${rowHeight}px;`}
>
  <div class="sticky left-0 z-20 h-full border-r border-slate-800 bg-slate-900/50 px-2 text-slate-200">
    <div class="flex h-full items-center gap-2 text-[13px] tracking-[0.02em]">
      <span class="font-medium">{market.collector}:{market.exchange}:{market.symbol}</span>
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

  <div class="sticky right-0 z-20 h-full border-l border-slate-800 bg-slate-900/50">
    <div class="flex h-full items-center justify-center">
      <button
        bind:this={actionsButton}
        class="rounded-md border-none py-1 text-slate-300 w-full h-full"
        type="button"
        on:click={handleActionsClick}
      >
        ...
      </button>
    </div>
  </div>
</div>
