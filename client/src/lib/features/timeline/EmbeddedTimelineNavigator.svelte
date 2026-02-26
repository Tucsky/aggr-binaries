<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount } from "svelte";
  import TimelineEventPopover from "./TimelineEventPopover.svelte";
  import TimelineRow from "./TimelineRow.svelte";
  import {
    fetchTimelineEvents,
    fetchTimelineMarkets,
    TIMELINE_SYMBOL_MODE,
  } from "./timelineApi.js";
  import {
    MIN_TIMELINE_VIEWPORT_WIDTH,
    TIMELINE_ROW_HEIGHT,
  } from "./timelineLayout.js";
  import { panTimelineRange, zoomTimelineRange } from "./timelineViewport.js";
  import type {
    TimelineEvent,
    TimelineHoverEvent,
    TimelineMarket,
  } from "./timelineTypes.js";
  import type { TimelineRange } from "./timelineUtils.js";

  interface JumpDetail {
    market: TimelineMarket;
    ts: number;
  }

  const PAN_OVERSCROLL_RATIO = 0.01;

  export let collector = "";
  export let exchange = "";
  export let symbol = "";
  export let timeframe = "1m";
  export let chartVisibleRange: TimelineRange | null = null;

  const dispatch = createEventDispatcher<{
    jump: JumpDetail;
  }>();

  let hostEl: HTMLDivElement | null = null;
  let resizeObserver: ResizeObserver | null = null;
  let timelineWidth = MIN_TIMELINE_VIEWPORT_WIDTH;

  let loading = false;
  let error = "";
  let market: TimelineMarket | null = null;
  let events: TimelineEvent[] = [];
  let viewRange: TimelineRange | null = null;
  let hoveredEvent: TimelineHoverEvent | null = null;

  let eventsAbort: AbortController | null = null;
  let activeRequestId = 0;
  let lastLoadKey = "";

  $: normalizedCollector = collector.trim().toUpperCase();
  $: normalizedExchange = exchange.trim().toUpperCase();
  $: normalizedSymbol = symbol.trim();
  $: normalizedTimeframe = timeframe.trim() || "1m";
  $: loadKey =
    normalizedCollector && normalizedExchange && normalizedSymbol
      ? `${normalizedCollector}:${normalizedExchange}:${normalizedSymbol}:${normalizedTimeframe}`
      : "";
  $: hasMoreLeft = Boolean(market && viewRange && viewRange.startTs > market.startTs);
  $: hasMoreRight = Boolean(market && viewRange && viewRange.endTs < market.endTs);

  $: if (loadKey && loadKey !== lastLoadKey) {
    lastLoadKey = loadKey;
    void loadNavigatorData(loadKey);
  }
  $: if (!loadKey && lastLoadKey) {
    resetData();
  }

  onMount(() => {
    updateTimelineWidth();
    resizeObserver = new ResizeObserver(() => updateTimelineWidth());
    if (hostEl) resizeObserver.observe(hostEl);
  });

  onDestroy(() => {
    resizeObserver?.disconnect();
    eventsAbort?.abort();
  });

  async function loadNavigatorData(expectedKey: string): Promise<void> {
    const requestId = ++activeRequestId;
    eventsAbort?.abort();
    eventsAbort = new AbortController();
    loading = true;
    error = "";
    hoveredEvent = null;

    try {
      const response = await fetchTimelineMarkets({
        timeframe: normalizedTimeframe,
        collector: normalizedCollector,
        exchange: normalizedExchange,
        symbol: normalizedSymbol,
      });
      if (!isCurrentRequest(requestId, expectedKey)) return;

      const nextMarket = findMarketRow(
        response.markets,
        normalizedCollector,
        normalizedExchange,
        normalizedSymbol,
        normalizedTimeframe,
      );
      if (!nextMarket) {
        market = null;
        events = [];
        viewRange = null;
        error = "No timeline range for selected market.";
        return;
      }

      market = nextMarket;
      viewRange = { startTs: nextMarket.startTs, endTs: nextMarket.endTs };
      events = await fetchTimelineEvents(
        {
          collector: nextMarket.collector,
          exchange: nextMarket.exchange,
          symbol: nextMarket.symbol,
          symbolMode: TIMELINE_SYMBOL_MODE.Exact,
          startTs: nextMarket.startTs,
          endTs: nextMarket.endTs,
        },
        eventsAbort.signal,
      );
      if (!isCurrentRequest(requestId, expectedKey)) return;
    } catch (err) {
      if ((err as { name?: string }).name === "AbortError") return;
      if (!isCurrentRequest(requestId, expectedKey)) return;
      error = err instanceof Error ? err.message : "Failed to load embedded timeline";
      market = null;
      events = [];
      viewRange = null;
    } finally {
      if (isCurrentRequest(requestId, expectedKey)) loading = false;
    }
  }

  function resetData(): void {
    lastLoadKey = "";
    activeRequestId += 1;
    eventsAbort?.abort();
    eventsAbort = null;
    loading = false;
    error = "";
    market = null;
    events = [];
    viewRange = null;
    hoveredEvent = null;
  }

  function isCurrentRequest(requestId: number, expectedKey: string): boolean {
    return requestId === activeRequestId && expectedKey === loadKey;
  }

  function findMarketRow(
    rows: TimelineMarket[],
    expectedCollector: string,
    expectedExchange: string,
    expectedSymbol: string,
    expectedTimeframe: string,
  ): TimelineMarket | null {
    const symbolNeedle = expectedSymbol.toLowerCase();
    for (const row of rows) {
      if (row.collector !== expectedCollector) continue;
      if (row.exchange !== expectedExchange) continue;
      if (row.symbol.toLowerCase() !== symbolNeedle) continue;
      if (expectedTimeframe && row.timeframe !== expectedTimeframe) continue;
      return row;
    }
    return null;
  }

  function selectedRange(): TimelineRange | null {
    if (!market) return null;
    return { startTs: market.startTs, endTs: market.endTs };
  }

  function handleOpen(event: CustomEvent<{ market: TimelineMarket; ts: number }>): void {
    dispatch("jump", { market: event.detail.market, ts: event.detail.ts });
  }

  function handleHover(
    event: CustomEvent<{
      ts: number | null;
      x: number | null;
      hoveredEvent: TimelineHoverEvent | null;
    }>,
  ): void {
    hoveredEvent = event.detail.hoveredEvent;
  }

  function handleZoom(event: CustomEvent<{ centerTs: number; deltaY: number }>): void {
    const range = selectedRange();
    if (!range || !viewRange) return;
    viewRange = zoomTimelineRange(
      range,
      viewRange,
      event.detail.centerTs,
      event.detail.deltaY,
    );
  }

  function handlePan(event: CustomEvent<{ deltaMs: number }>): void {
    const range = selectedRange();
    if (!range || !viewRange || event.detail.deltaMs === 0) return;
    viewRange = panTimelineRange(
      range,
      viewRange,
      event.detail.deltaMs,
      PAN_OVERSCROLL_RATIO,
    );
  }

  function updateTimelineWidth(): void {
    if (!hostEl) return;
    timelineWidth = Math.max(
      MIN_TIMELINE_VIEWPORT_WIDTH,
      Math.floor(hostEl.clientWidth),
    );
  }
</script>

<section class="border-b border-slate-800 bg-slate-900/70 py-1">
  <div class="relative">
    <div bind:this={hostEl} class="relative overflow-x-auto overflow-y-hidden">
      {#if loading}
        <div class="px-2 py-1 text-xs text-slate-400">Loading timeline...</div>
      {:else if error}
        <div class="px-2 py-1 text-xs text-red-300">{error}</div>
      {:else if market && viewRange}
        <TimelineRow
          {market}
          {events}
          range={{ startTs: market.startTs, endTs: market.endTs }}
          {viewRange}
          {timelineWidth}
          rowHeight={TIMELINE_ROW_HEIGHT}
          showLabel={false}
          showActions={false}
          highlightRange={chartVisibleRange}
          on:open={handleOpen}
          on:hover={handleHover}
          on:zoom={handleZoom}
          on:pan={handlePan}
        />
      {:else}
        <div class="px-2 py-1 text-xs text-slate-500">No market selected.</div>
      {/if}
    </div>
    {#if hasMoreLeft}
      <div class="pointer-events-none absolute inset-y-0 left-0 z-10 w-5 bg-gradient-to-r from-slate-950/90 to-transparent"></div>
    {/if}
    {#if hasMoreRight}
      <div class="pointer-events-none absolute inset-y-0 right-0 z-10 w-5 bg-gradient-to-l from-slate-950/90 to-transparent"></div>
    {/if}
  </div>
  <TimelineEventPopover hoveredEvent={hoveredEvent} />
</section>
