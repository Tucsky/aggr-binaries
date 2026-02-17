<script lang="ts">
  import { get } from "svelte/store";
  import { onDestroy, onMount } from "svelte";
  import TimelineDebugPanel from "../lib/TimelineDebugPanel.svelte";
  import TimelineControls from "../lib/TimelineControls.svelte";
  import TimelineRowActionsMenu from "../lib/TimelineRowActionsMenu.svelte";
  import TimelineRow from "../lib/TimelineRow.svelte";
  import { navigate } from "../lib/routeStore.js";
  import { addToast } from "../lib/toastStore.js";
  import { fetchTimelineEvents, fetchTimelineMarkets, type TimelineMarketsResponse } from "../lib/timelineApi.js";
  import type { TimelineEvent, TimelineMarket } from "../lib/timelineTypes.js";
  import { prefs } from "../lib/viewerStore.js";
  import { buildInitialViewRange, formatTimelineTsLabel, panTimelineRange, resolveTimelineTimeframe, zoomTimelineRange } from "../lib/timelineViewport.js";
  import { computeGlobalRange, groupEventsByMarket, marketKey, toTimelineTs, toTimelineX, type TimelineRange } from "../lib/timelineUtils.js";
  const YEAR_MS = 365 * 24 * 60 * 60 * 1000;
  const ROW_HEIGHT = 33;
  const OVERSCAN = 8;
  const LEFT_WIDTH = 180;
  const RIGHT_WIDTH = 33;
  const TIMELINE_STATE_STORAGE_KEY = "aggr.timeline.state.v1";
  const PAN_OVERSCROLL_RATIO = 0.01;
  let loadingMarkets = false;
  let loadingEvents = false;
  let marketsError = "";
  let eventsError = "";
  let allMarkets: TimelineMarket[] = [];
  let allEvents: TimelineEvent[] = [];
  let groupedEvents = new Map<string, TimelineEvent[]>();
  let timeframeOptions: string[] = [];
  let timeframeFilter = "1m";
  let collectorFilter = "";
  let exchangeFilter = "";
  let symbolFilter = "";
  let selectedRange: TimelineRange | null = null;
  let overallRange: TimelineRange | null = null;
  let viewRange: TimelineRange | null = null;
  let persistedViewStartTs: number | null = null;
  let persistedViewEndTs: number | null = null;
  let crosshairTs: number | null = null;
  let crosshairPx: number | null = null;
  let lastZoomDelta = 0;
  let lastPanDeltaMs = 0;
  let actionsOpen = false;
  let actionsAnchorEl: HTMLElement | null = null;
  let actionsMarket: TimelineMarket | null = null;
  let scrollEl: HTMLDivElement | null = null;
  let resizeObserver: ResizeObserver | null = null;
  let scrollTop = 0;
  let viewportHeight = 0;
  let timelineViewportWidth = 900;
  let eventAbort: AbortController | null = null;
  $: collectorOptions = unique(allMarkets.map((market) => market.collector));
  $: if (collectorFilter && !collectorOptions.includes(collectorFilter)) collectorFilter = "";
  $: exchangeOptions = unique(allMarkets.filter((market) => !collectorFilter || market.collector === collectorFilter).map((market) => market.exchange));
  $: if (exchangeFilter && !exchangeOptions.includes(exchangeFilter)) exchangeFilter = "";
  $: filteredMarkets = allMarkets.filter((market) => {
    if (collectorFilter && market.collector !== collectorFilter) return false;
    if (exchangeFilter && market.exchange !== exchangeFilter) return false;
    if (symbolFilter && !market.symbol.toLowerCase().includes(symbolFilter.toLowerCase())) return false;
    return true;
  });
  $: groupedEvents = groupEventsByMarket(allEvents);
  $: timelineWidth = Math.max(320, Math.floor(timelineViewportWidth));
  $: totalGridWidth = LEFT_WIDTH + timelineWidth + RIGHT_WIDTH;
  $: startIndex = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN);
  $: endIndex = Math.min(filteredMarkets.length, Math.ceil((scrollTop + viewportHeight) / ROW_HEIGHT) + OVERSCAN);
  $: topPadding = startIndex * ROW_HEIGHT;
  $: bottomPadding = Math.max(0, (filteredMarkets.length - endIndex) * ROW_HEIGHT);
  $: visibleMarkets = filteredMarkets.slice(startIndex, endIndex);
  $: crosshairX = crosshairPx !== null ? crosshairPx : crosshairTs !== null && viewRange ? toTimelineX(crosshairTs, viewRange, timelineWidth) : null;
  $: crosshairLeft = crosshairX === null ? null : LEFT_WIDTH + crosshairX;
  $: timelineStartPx = LEFT_WIDTH;
  $: timelineEndPx = LEFT_WIDTH + timelineWidth;
  $: hasMoreLeft = Boolean(selectedRange && viewRange && viewRange.startTs > selectedRange.startTs);
  $: hasMoreRight = Boolean(selectedRange && viewRange && viewRange.endTs < selectedRange.endTs);
  onMount(() => {
    restoreTimelineState();
    void initTimeline();
    if (!scrollEl) return;
    viewportHeight = scrollEl.clientHeight;
    timelineViewportWidth = Math.max(320, scrollEl.clientWidth - LEFT_WIDTH - RIGHT_WIDTH);
    resizeObserver = new ResizeObserver(() => {
      if (!scrollEl) return;
      viewportHeight = scrollEl.clientHeight;
      timelineViewportWidth = Math.max(320, scrollEl.clientWidth - LEFT_WIDTH - RIGHT_WIDTH);
    });
    resizeObserver.observe(scrollEl);
  });
  onDestroy(() => {
    persistTimelineState();
    resizeObserver?.disconnect();
    eventAbort?.abort();
  });
  async function initTimeline(): Promise<void> {
    await loadOverallRange();
    await loadMarkets();
  }
  async function loadOverallRange(): Promise<void> {
    try {
      const response = await fetchTimelineMarkets();
      overallRange = computeGlobalRange(normalizeMarketRows(response.markets, timeframeFilter));
    } catch {
      overallRange = null;
    }
  }
  async function loadMarkets(): Promise<void> {
    loadingMarkets = true;
    marketsError = "";
    try {
      let requested = timeframeFilter.trim() || "1m";
      let response = await fetchTimelineMarkets({ timeframe: requested });
      let normalized = normalizeMarketsResponse(response, requested);
      timeframeOptions = normalized.timeframes;
      if (normalized.selectedTimeframe !== requested) {
        requested = normalized.selectedTimeframe;
        response = await fetchTimelineMarkets({ timeframe: requested });
        normalized = normalizeMarketsResponse(response, requested);
        timeframeOptions = normalized.timeframes;
      }
      timeframeFilter = normalized.selectedTimeframe;
      allMarkets = normalized.markets;
      const nextCollectorOptions = unique(allMarkets.map((market) => market.collector));
      if (collectorFilter && !nextCollectorOptions.includes(collectorFilter)) collectorFilter = "";
      const nextExchangeOptions = unique(
        allMarkets
          .filter((market) => !collectorFilter || market.collector === collectorFilter)
          .map((market) => market.exchange),
      );
      if (exchangeFilter && !nextExchangeOptions.includes(exchangeFilter)) exchangeFilter = "";
      const globalRange = overallRange ?? computeGlobalRange(allMarkets);
      if (globalRange) {
        selectedRange = globalRange;
        viewRange = restorePersistedViewRange(globalRange) ?? buildInitialViewRange(globalRange, YEAR_MS);
        await loadEvents();
      } else {
        selectedRange = null;
        viewRange = null;
        allEvents = [];
      }
      crosshairTs = null;
      crosshairPx = null;
      persistTimelineState();
    } catch (err) {
      marketsError = err instanceof Error ? err.message : "Failed to load timeline markets";
    } finally {
      loadingMarkets = false;
    }
  }
  async function loadEvents(): Promise<void> {
    if (!selectedRange) {
      allEvents = [];
      return;
    }
    eventAbort?.abort();
    eventAbort = new AbortController();
    loadingEvents = true;
    eventsError = "";
    try {
      allEvents = await fetchTimelineEvents(
        { collector: collectorFilter || undefined, exchange: exchangeFilter || undefined, symbol: symbolFilter || undefined, startTs: selectedRange.startTs, endTs: selectedRange.endTs },
        eventAbort.signal,
      );
    } catch (err) {
      if ((err as { name?: string }).name !== "AbortError") eventsError = err instanceof Error ? err.message : "Failed to load timeline events";
    } finally {
      loadingEvents = false;
    }
  }
  function normalizeMarketsResponse(response: TimelineMarketsResponse, requestedTimeframe: string): { markets: TimelineMarket[]; timeframes: string[]; selectedTimeframe: string } {
    const inferred = unique(response.markets.map((market) => market.timeframe).filter((value): value is string => typeof value === "string" && value.length > 0));
    const fallback = requestedTimeframe || inferred[0] || "1m";
    const timeframes = response.timeframes.length ? response.timeframes : inferred.length ? inferred : [fallback];
    const selectedTimeframe = resolveTimelineTimeframe(requestedTimeframe, timeframes) || fallback;
    return { markets: normalizeMarketRows(response.markets, selectedTimeframe), timeframes, selectedTimeframe };
  }
  function normalizeMarketRows(markets: TimelineMarket[], fallbackTimeframe: string): TimelineMarket[] {
    return markets.map((market) => ({ ...market, collector: market.collector.toUpperCase(), exchange: market.exchange.toUpperCase(), timeframe: market.timeframe || fallbackTimeframe }));
  }
  function unique(values: string[]): string[] {
    return Array.from(new Set(values)).sort();
  }
  function zoomAround(centerTs: number, deltaY: number): void {
    if (!selectedRange || !viewRange) return;
    viewRange = zoomTimelineRange(selectedRange, viewRange, centerTs, deltaY);
  }

  function panView(deltaMs: number): void {
    if (!selectedRange || !viewRange || deltaMs === 0) return;
    viewRange = panTimelineRange(selectedRange, viewRange, deltaMs, PAN_OVERSCROLL_RATIO);
  }
  function handleScroll(): void {
    if (!scrollEl) return;
    scrollTop = scrollEl.scrollTop;
    viewportHeight = scrollEl.clientHeight;
    actionsOpen = false;
  }

  function handleCollectorChange(event: CustomEvent<string>): void {
    collectorFilter = event.detail;
    persistTimelineState();
    void loadEvents();
  }
  function handleExchangeChange(event: CustomEvent<string>): void {
    exchangeFilter = event.detail;
    persistTimelineState();
    void loadEvents();
  }

  function handleSymbolInput(event: CustomEvent<string>): void {
    symbolFilter = event.detail.trim();
    persistTimelineState();
    void loadEvents();
  }
  function handleTimeframeChange(event: CustomEvent<string>): void {
    timeframeFilter = event.detail;
    persistTimelineState();
    void loadMarkets();
  }

  function handleOpen(event: CustomEvent<{ market: TimelineMarket; ts: number }>): void {
    const currentPrefs = get(prefs);
    const detail = event.detail;
    persistTimelineState();
    navigate({ kind: "chart", market: { collector: detail.market.collector, exchange: detail.market.exchange, symbol: detail.market.symbol }, timeframe: currentPrefs.timeframe || "1m", startTs: detail.ts }, { replace: false });
  }
  function syncCrosshairTsFromPx(): void {
    if (crosshairPx === null || !viewRange) return;
    const clampedX = Math.max(0, Math.min(timelineWidth, crosshairPx));
    crosshairTs = toTimelineTs(clampedX, viewRange, timelineWidth);
  }

  function handleHover(event: CustomEvent<{ ts: number | null; x: number | null }>): void {
    crosshairTs = event.detail.ts;
    crosshairPx = event.detail.x;
  }
  function handleZoom(event: CustomEvent<{ centerTs: number; deltaY: number }>): void {
    lastZoomDelta = event.detail.deltaY;
    zoomAround(event.detail.centerTs, event.detail.deltaY);
    syncCrosshairTsFromPx();
    persistTimelineState();
  }

  function handlePan(event: CustomEvent<{ deltaMs: number }>): void {
    lastPanDeltaMs = event.detail.deltaMs;
    panView(event.detail.deltaMs);
    syncCrosshairTsFromPx();
    persistTimelineState();
  }
  function handleRowActions(event: CustomEvent<{ market: TimelineMarket; anchorEl: HTMLButtonElement | null }>): void {
    const anchorEl = event.detail.anchorEl;
    if (!anchorEl) return;
    const sameAnchor = actionsOpen && actionsAnchorEl === anchorEl;
    actionsAnchorEl = anchorEl;
    actionsMarket = event.detail.market;
    actionsOpen = !sameAnchor;
  }
  function closeActionsMenu(): void {
    actionsOpen = false;
  }
  function openMarketFromMenu(event: CustomEvent<TimelineMarket>): void {
    closeActionsMenu();
    const market = event.detail;
    const currentPrefs = get(prefs);
    persistTimelineState();
    navigate({ kind: "chart", market: { collector: market.collector, exchange: market.exchange, symbol: market.symbol }, timeframe: currentPrefs.timeframe || market.timeframe || "1m", startTs: market.endTs }, { replace: false });
  }
  async function copyMarketFromMenu(event: CustomEvent<TimelineMarket>): Promise<void> {
    closeActionsMenu();
    const market = event.detail;
    const key = market.timeframe ? `${market.collector}:${market.exchange}:${market.symbol}:${market.timeframe}` : `${market.collector}:${market.exchange}:${market.symbol}`;
    try {
      await navigator.clipboard.writeText(key);
      addToast(`Copied ${key}`, "success", 1200);
    } catch {
      addToast("Clipboard unavailable", "error", 1800);
    }
  }
  function rowEvents(market: TimelineMarket): TimelineEvent[] {
    return groupedEvents.get(marketKey(market)) ?? [];
  }

  function rowIdentity(market: TimelineMarket): string {
    return `${market.collector}:${market.exchange}:${market.symbol}:${market.timeframe}`;
  }
  function persistTimelineState(): void {
    if (typeof window === "undefined") return;
    const payload = {
      collectorFilter,
      exchangeFilter,
      symbolFilter,
      timeframeFilter,
      viewStartTs: viewRange?.startTs ?? null,
      viewEndTs: viewRange?.endTs ?? null,
    };
    try {
      window.localStorage.setItem(TIMELINE_STATE_STORAGE_KEY, JSON.stringify(payload));
    } catch {
      // ignore storage failures
    }
  }

  function restoreTimelineState(): void {
    if (typeof window === "undefined") return;
    try {
      const raw = window.localStorage.getItem(TIMELINE_STATE_STORAGE_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw) as Partial<{
        collectorFilter: string;
        exchangeFilter: string;
        symbolFilter: string;
        timeframeFilter: string;
        viewStartTs: number | null;
        viewEndTs: number | null;
      }>;
      collectorFilter = typeof parsed.collectorFilter === "string" ? parsed.collectorFilter : "";
      exchangeFilter = typeof parsed.exchangeFilter === "string" ? parsed.exchangeFilter : "";
      symbolFilter = typeof parsed.symbolFilter === "string" ? parsed.symbolFilter : "";
      timeframeFilter = typeof parsed.timeframeFilter === "string" && parsed.timeframeFilter.length ? parsed.timeframeFilter : timeframeFilter;
      persistedViewStartTs = Number.isFinite(parsed.viewStartTs) ? Number(parsed.viewStartTs) : null;
      persistedViewEndTs = Number.isFinite(parsed.viewEndTs) ? Number(parsed.viewEndTs) : null;
    } catch {
      // ignore malformed state
    }
  }

  function restorePersistedViewRange(selected: TimelineRange): TimelineRange | null {
    if (persistedViewStartTs === null || persistedViewEndTs === null) return null;
    if (persistedViewEndTs <= persistedViewStartTs) return null;
    const candidateSpan = persistedViewEndTs - persistedViewStartTs;
    const fullSpan = selected.endTs - selected.startTs;
    if (candidateSpan >= fullSpan) return { ...selected };
    const overscrollMs = Math.round(candidateSpan * PAN_OVERSCROLL_RATIO);
    const minStartTs = selected.startTs - overscrollMs;
    const maxEndTs = selected.endTs + overscrollMs;
    let nextStart = persistedViewStartTs;
    let nextEnd = persistedViewEndTs;
    if (nextStart < minStartTs) {
      const delta = minStartTs - nextStart;
      nextStart += delta;
      nextEnd += delta;
    }
    if (nextEnd > maxEndTs) {
      const delta = nextEnd - maxEndTs;
      nextStart -= delta;
      nextEnd -= delta;
    }
    return nextEnd > nextStart ? { startTs: nextStart, endTs: nextEnd } : null;
  }
</script>
<div class="flex h-full min-h-0 flex-col bg-slate-950 text-slate-100">
  <TimelineControls {collectorFilter} {exchangeFilter} {symbolFilter} {timeframeFilter} {collectorOptions} {exchangeOptions} {timeframeOptions} on:collectorChange={handleCollectorChange} on:exchangeChange={handleExchangeChange} on:symbolInput={handleSymbolInput} on:timeframeChange={handleTimeframeChange} />

  <div class="relative flex-1 min-h-0">
    <div bind:this={scrollEl} class="relative h-full overflow-y-auto overflow-x-hidden" on:scroll={handleScroll}>
      <div class="pointer-events-none absolute inset-y-0 left-0 z-0 border-r border-slate-800 bg-slate-900/50" style={`width:${LEFT_WIDTH}px;`}></div>
      <div class="pointer-events-none absolute inset-y-0 z-0 border-l border-slate-800 bg-slate-900/50" style={`left:${timelineEndPx}px;width:${RIGHT_WIDTH}px;`}></div>
      {#if loadingMarkets}
        <div class="relative z-10 p-3 text-sm text-slate-300">Loading markets...</div>
      {:else if marketsError}
        <div class="relative z-10 p-3 text-sm text-red-300">{marketsError}</div>
      {:else if !selectedRange || !viewRange}
        <div class="relative z-10 p-3 text-sm text-slate-400">No markets in registry for selected timeframe.</div>
      {:else}
        <div class="relative z-10 min-w-max" style={`width: ${totalGridWidth}px;`}>
          <div style={`height: ${topPadding}px;`}></div>
          {#each visibleMarkets as market (rowIdentity(market))}
            <TimelineRow {market} events={rowEvents(market)} range={selectedRange} {viewRange} contentWidth={timelineWidth} rowHeight={ROW_HEIGHT} leftWidth={LEFT_WIDTH} rightWidth={RIGHT_WIDTH} on:open={handleOpen} on:hover={handleHover} on:zoom={handleZoom} on:pan={handlePan} on:actions={handleRowActions} />
          {/each}
          <div style={`height: ${bottomPadding}px;`}></div>
        </div>
      {/if}
    </div>
    {#if crosshairLeft !== null && crosshairTs !== null && viewRange}
      <div class="pointer-events-none absolute inset-0 overflow-hidden z-11">
        <div class="absolute bottom-0 top-0 w-px bg-white/30" style={`left: ${crosshairLeft}px;`}></div>
        <div class="absolute bottom-2 -translate-x-1/2 rounded border border-slate-600 bg-slate-900/95 px-2 py-0.5 text-[10px] text-slate-100" style={`left: ${crosshairLeft}px;`}>{formatTimelineTsLabel(crosshairTs)}</div>
      </div>
    {/if}
    {#if hasMoreLeft}
      <div class="z-10 pointer-events-none absolute bottom-0 top-0 w-5 bg-gradient-to-r from-slate-950/90 to-transparent" style={`left:${timelineStartPx}px;`}></div>
    {/if}
    {#if hasMoreRight}
      <div class="z-10 pointer-events-none absolute bottom-0 top-0 w-5 bg-gradient-to-l from-slate-950/90 to-transparent" style={`left:${timelineEndPx - 20}px;`}></div>
    {/if}
    <TimelineDebugPanel {selectedRange} {viewRange} {crosshairTs} crosshairPx={crosshairX} {timelineWidth} {lastZoomDelta} {lastPanDeltaMs} />
    <TimelineRowActionsMenu open={actionsOpen} anchorEl={actionsAnchorEl} market={actionsMarket} on:close={closeActionsMenu} on:openMarket={openMarketFromMenu} on:copyMarket={copyMarketFromMenu} />
  </div>
  {#if loadingEvents}
    <div class="border-t border-slate-800 bg-slate-900 px-3 py-1 text-[11px] text-slate-300">Loading events...</div>
  {:else if eventsError}
    <div class="border-t border-slate-800 bg-slate-900 px-3 py-1 text-[11px] text-red-300">{eventsError}</div>
  {/if}
</div>
