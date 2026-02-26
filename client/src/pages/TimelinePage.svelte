<script lang="ts">
  import { get } from "svelte/store";
  import { onDestroy, onMount, tick } from "svelte";
  import TimelineDebugPanel from "../lib/features/timeline/TimelineDebugPanel.svelte";
  import TimelineEventPopover from "../lib/features/timeline/TimelineEventPopover.svelte";
  import TimelineControls from "../lib/features/timeline/TimelineControls.svelte";
  import TimelineRowActionsMenu from "../lib/features/timeline/TimelineRowActionsMenu.svelte";
  import TimelineRow from "../lib/features/timeline/TimelineRow.svelte";
  import { navigate } from "../lib/framework/routing/routeStore.js";
  import { addToast } from "../lib/framework/toast/toastStore.js";
  import { fetchTimelineEventsByRows, fetchTimelineMarkets } from "../lib/features/timeline/timelineApi.js";
  import {
    filterMarketsWithRange,
    normalizeMarketRows,
    normalizeMarketsResponse,
    restorePersistedViewRange,
    shouldKeepFilterSelection,
    unique,
  } from "../lib/features/timeline/timelinePageHelpers.js";
  import { type TimelineMarketAction, type TimelineEvent, type TimelineHoverEvent, type TimelineMarket } from "../lib/features/timeline/timelineTypes.js";
  import { prefs, savePrefs } from "../lib/features/viewer/viewerStore.js";
  import {
    mergeSharedControlsIntoPrefs,
    persistTimelineLocalState,
    readSharedControlsFromPrefs,
    restoreTimelineLocalState,
    type TimelineSharedControls,
  } from "../lib/features/timeline/timelineControlsPersistence.js";
  import { computeTimelineVirtualWindow } from "../lib/features/timeline/timelineVirtualRows.js";
  import {
    createTimelineViewportEventCacheState,
    buildTimelineEventsScopeKey,
    readTimelineViewportEventCache,
    resolveTimelineViewportMissingRows,
    resolveTimelineViewportEventRequest,
    selectTimelineViewportEventRows,
    writeTimelineViewportEventCache,
    type TimelineViewportEventCacheState,
  } from "../lib/features/timeline/timelineEventViewportLoader.js";
  import { buildInitialViewRange, formatTimelineTsLabel, panTimelineRange, resolveTimelineTimeframe, shiftViewRangeIntoRangeIfDisjoint, zoomTimelineRange } from "../lib/features/timeline/timelineViewport.js";
  import {
    captureTimelineScrollAnchor,
    resolveTimelineRestoredScrollTop,
    type TimelineScrollAnchor,
  } from "../lib/features/timeline/timelineScrollAnchor.js";
  import { computeGlobalRange, groupEventsByMarket, marketKey, toTimelineTs, toTimelineX, type TimelineRange } from "../lib/features/timeline/timelineUtils.js";
  const YEAR_MS = 365 * 24 * 60 * 60 * 1000;
  const ROW_HEIGHT = 33;
  const OVERSCAN = 8;
  const LEFT_WIDTH = 180;
  // Intentionally kept in one page: timeline viewport math, row virtualization, and pointer interactions are tightly coupled.
  const PAN_OVERSCROLL_RATIO = 0.01;
  const SYMBOL_INPUT_DEBOUNCE_MS = 180;
  const VIEWPORT_EVENT_RELOAD_DEBOUNCE_MS = 48;
  const EVENT_ROW_FETCH_OVERSCAN = 2;
  const EVENT_RANGE_OVERSCAN_RATIO = 0.5;
  const MAX_EVENT_ROWS_PER_REQUEST = 24;
  const EVENT_CACHE_ROW_LIMIT = 128;
  const EVENT_CACHE_EVENTS_LIMIT = 60_000;
  let loadingMarkets = false;
  let loadingEvents = false;
  let marketsError = "";
  let eventsError = "";
  let allMarkets: TimelineMarket[] = [];
  let filteredMarkets: TimelineMarket[] = [];
  let filteredRange: TimelineRange | null = null;
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
  let hoveredEvent: TimelineHoverEvent | null = null;
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
  let startIndex = 0;
  let endIndex = 0;
  let topPadding = 0;
  let bottomPadding = 0;
  let eventAbort: AbortController | null = null;
  let eventsReloadTimer: ReturnType<typeof setTimeout> | null = null;
  let lastEventsQueryKey = "";
  let eventsScopeKey = "";
  let loadedEventsRange: TimelineRange | null = null;
  let loadedEventRowKeys = new Set<string>();
  let pendingEventsReload = false;
  let pendingEventsForceReload = false;
  let inFlightEventsQueryKey = "";
  let eventsCache: TimelineViewportEventCacheState = createTimelineViewportEventCacheState();
  $: collectorOptions = unique(allMarkets.map((market) => market.collector));
  $: if (!shouldKeepFilterSelection(collectorFilter, collectorOptions)) collectorFilter = "";
  $: exchangeOptions = unique(allMarkets.filter((market) => !collectorFilter || market.collector === collectorFilter).map((market) => market.exchange));
  $: if (!shouldKeepFilterSelection(exchangeFilter, exchangeOptions)) exchangeFilter = "";
  $: ({ markets: filteredMarkets, range: filteredRange } = filterMarketsWithRange(allMarkets, collectorFilter, exchangeFilter, symbolFilter));
  $: selectedRange = filteredRange;
  $: if (viewRange && filteredRange) {
    const adjusted = shiftViewRangeIntoRangeIfDisjoint(filteredRange, viewRange);
    if (adjusted.startTs !== viewRange.startTs || adjusted.endTs !== viewRange.endTs) {
      viewRange = adjusted;
      syncCrosshairTsFromPx();
      persistTimelineState();
    }
  }
  $: groupedEvents = groupEventsByMarket(allEvents);
  $: timelineWidth = Math.max(320, Math.floor(timelineViewportWidth));
  $: totalGridWidth = LEFT_WIDTH + timelineWidth;
  $: ({ startIndex, endIndex, topPadding, bottomPadding } = computeTimelineVirtualWindow(
    filteredMarkets.length,
    scrollTop,
    viewportHeight,
    ROW_HEIGHT,
    OVERSCAN,
  ));
  $: visibleMarkets = filteredMarkets.slice(startIndex, endIndex);
  $: crosshairX = crosshairPx !== null ? crosshairPx : crosshairTs !== null && viewRange ? toTimelineX(crosshairTs, viewRange, timelineWidth) : null;
  $: crosshairLeft = crosshairX === null ? null : LEFT_WIDTH + crosshairX;
  $: timelineStartPx = LEFT_WIDTH;
  $: timelineEndPx = LEFT_WIDTH + timelineWidth;
  $: hasMoreLeft = Boolean(selectedRange && viewRange && viewRange.startTs > selectedRange.startTs);
  $: hasMoreRight = Boolean(selectedRange && viewRange && viewRange.endTs < selectedRange.endTs);
  onMount(() => {
    restoreSharedControlPrefs();
    restoreTimelineState();
    void initTimeline();
    if (!scrollEl) return;
    viewportHeight = scrollEl.clientHeight;
    timelineViewportWidth = Math.max(320, scrollEl.clientWidth - LEFT_WIDTH);
    resizeObserver = new ResizeObserver(() => {
      if (!scrollEl) return;
      viewportHeight = scrollEl.clientHeight;
      timelineViewportWidth = Math.max(320, scrollEl.clientWidth - LEFT_WIDTH);
      scheduleEventsReload(0);
    });
    resizeObserver.observe(scrollEl);
  });
  onDestroy(() => {
    persistTimelineState();
    resizeObserver?.disconnect();
    eventAbort?.abort();
    clearEventsReloadTimer();
  });
  async function initTimeline(): Promise<void> {
    await loadOverallRange();
    await loadMarkets();
  }
  async function loadOverallRange(): Promise<void> {
    try {
      const response = await fetchTimelineMarkets();
      const normalizedMarkets = normalizeMarketRows(response.markets, timeframeFilter);
      overallRange = computeGlobalRange(normalizedMarkets);
      const inferred = unique(
        (response.timeframes.length ? response.timeframes : normalizedMarkets.map((market) => market.timeframe)).filter(
          (value): value is string => typeof value === "string" && value.length > 0,
        ),
      );
      if (inferred.length) {
        timeframeOptions = inferred;
        const resolved = resolveTimelineTimeframe(timeframeFilter.trim() || "1m", inferred);
        if (resolved) timeframeFilter = resolved;
      }
    } catch {
      overallRange = null;
    }
  }
  async function loadMarkets(forceLoadEvents = false): Promise<void> {
    const scrollAnchor = captureCurrentScrollAnchor();
    loadingMarkets = true;
    marketsError = "";
    try {
      let requested = timeframeFilter.trim() || "1m";
      if (timeframeOptions.length) {
        requested = resolveTimelineTimeframe(requested, timeframeOptions) || requested;
      }
      let response = await fetchTimelineMarkets({ timeframe: requested });
      let normalized = normalizeMarketsResponse(response, requested);
      timeframeOptions = normalized.timeframes;
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
        if (!viewRange) {
          viewRange =
            restorePersistedViewRange(globalRange, persistedViewStartTs, persistedViewEndTs, PAN_OVERSCROLL_RATIO) ??
            buildInitialViewRange(globalRange, YEAR_MS);
        }
      } else {
        selectedRange = null;
        viewRange = null;
        resetLoadedEventsState(true, true);
      }
      crosshairTs = null;
      crosshairPx = null;
      persistSharedControlPrefs();
      persistTimelineState();
    } catch (err) {
      marketsError = err instanceof Error ? err.message : "Failed to load timeline markets";
    } finally {
      loadingMarkets = false;
      await restoreScrollAnchor(scrollAnchor);
      if (!marketsError && selectedRange && viewRange) {
        scheduleEventsReload(0, forceLoadEvents);
      }
    }
  }
  async function loadEvents(forceReload = false): Promise<void> {
    if (!selectedRange || !viewRange || !filteredMarkets.length) {
      eventAbort?.abort();
      eventAbort = null;
      inFlightEventsQueryKey = "";
      loadingEvents = false;
      pendingEventsReload = false;
      pendingEventsForceReload = false;
      resetLoadedEventsState(!selectedRange, !selectedRange || !viewRange);
      return;
    }

    // Scope key changes mean previously loaded viewport windows no longer match current filters/timeframe/range.
    const scopeKey = buildTimelineEventsScopeKey(
      timeframeFilter,
      selectedRange,
      collectorFilter,
      exchangeFilter,
      symbolFilter,
    );
    if (scopeKey !== eventsScopeKey) {
      eventsScopeKey = scopeKey;
      resetLoadedEventsState(true, false);
    }

    const selection = selectTimelineViewportEventRows(
      filteredMarkets,
      startIndex,
      endIndex,
      EVENT_ROW_FETCH_OVERSCAN,
      MAX_EVENT_ROWS_PER_REQUEST,
    );
    if (!selection.rows.length) {
      eventAbort?.abort();
      eventAbort = null;
      inFlightEventsQueryKey = "";
      pendingEventsReload = false;
      pendingEventsForceReload = false;
      resetLoadedEventsState(false, false);
      loadingEvents = false;
      return;
    }
    // Fetch only when loaded rows/range do not fully cover the current viewport intent.
    const request = resolveTimelineViewportEventRequest({
      scopeKey,
      selectedRange,
      viewRange,
      selection,
      loadedRange: loadedEventsRange,
      loadedRowKeys: loadedEventRowKeys,
      rangeOverscanRatio: EVENT_RANGE_OVERSCAN_RATIO,
      forceReload,
    });
    if (!request) return;
    if (!forceReload && request.queryKey === lastEventsQueryKey) return;
    if (loadingEvents) {
      if (!forceReload && request.queryKey === inFlightEventsQueryKey) return;
      queuePendingEventsReload(forceReload);
      return;
    }

    const cached = readTimelineViewportEventCache(
      eventsCache,
      scopeKey,
      request.requestRange,
      selection,
    );
    if (!forceReload && cached.coveredRowKeys.size >= selection.rowKeys.length) {
      allEvents = cached.events;
      loadedEventsRange = request.requestRange;
      loadedEventRowKeys = new Set(selection.rowKeys);
      lastEventsQueryKey = request.queryKey;
      eventsError = "";
      loadingEvents = false;
      return;
    }

    const rowsToFetch = forceReload
      ? selection
      : resolveTimelineViewportMissingRows(selection, cached.coveredRowKeys);
    if (!rowsToFetch.rows.length) {
      allEvents = cached.events;
      loadedEventsRange = request.requestRange;
      loadedEventRowKeys = new Set(selection.rowKeys);
      lastEventsQueryKey = request.queryKey;
      eventsError = "";
      loadingEvents = false;
      return;
    }

    if (cached.events.length) {
      allEvents = cached.events;
    }
    eventAbort?.abort();
    eventAbort = new AbortController();
    loadingEvents = true;
    inFlightEventsQueryKey = request.queryKey;
    eventsError = "";
    try {
      const fetched = await fetchTimelineEventsByRows(
        {
          startTs: request.requestRange.startTs,
          endTs: request.requestRange.endTs,
          rows: rowsToFetch.rows,
        },
        eventAbort.signal,
      );
      writeTimelineViewportEventCache(
        eventsCache,
        EVENT_CACHE_ROW_LIMIT,
        EVENT_CACHE_EVENTS_LIMIT,
        {
          scopeKey,
          requestRange: request.requestRange,
          rowKeys: rowsToFetch.rowKeys,
          events: fetched,
        },
      );
      const refreshed = readTimelineViewportEventCache(
        eventsCache,
        scopeKey,
        request.requestRange,
        selection,
      );
      allEvents = refreshed.events;
      loadedEventsRange = request.requestRange;
      loadedEventRowKeys = new Set(selection.rowKeys);
      lastEventsQueryKey = request.queryKey;
      eventsError = "";
    } catch (err) {
      if ((err as { name?: string }).name !== "AbortError") {
        eventsError = err instanceof Error ? err.message : "Failed to load timeline events";
      }
    } finally {
      loadingEvents = false;
      inFlightEventsQueryKey = "";
      flushPendingEventsReload();
    }
  }
  function resetLoadedEventsState(clearCache: boolean, clearScope: boolean): void {
    allEvents = [];
    loadedEventsRange = null;
    loadedEventRowKeys = new Set<string>();
    lastEventsQueryKey = "";
    if (clearScope) eventsScopeKey = "";
    if (!clearCache) return;
    eventsCache = createTimelineViewportEventCacheState();
  }
  function queuePendingEventsReload(forceReload: boolean): void {
    pendingEventsReload = true;
    pendingEventsForceReload = pendingEventsForceReload || forceReload;
  }
  function flushPendingEventsReload(): void {
    if (!pendingEventsReload) return;
    const forceReload = pendingEventsForceReload;
    pendingEventsReload = false;
    pendingEventsForceReload = false;
    scheduleEventsReload(0, forceReload);
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
    scheduleEventsReload(VIEWPORT_EVENT_RELOAD_DEBOUNCE_MS);
  }
  function handleCollectorChange(event: CustomEvent<string>): void {
    collectorFilter = event.detail;
    if (exchangeFilter && !allMarkets.some((market) => (!collectorFilter || market.collector === collectorFilter) && market.exchange === exchangeFilter)) {
      exchangeFilter = "";
    }
    persistSharedControlPrefs();
    persistTimelineState();
    scheduleEventsReload(0);
  }
  function handleExchangeChange(event: CustomEvent<string>): void {
    exchangeFilter = event.detail;
    persistSharedControlPrefs();
    persistTimelineState();
    scheduleEventsReload(0);
  }

  function handleSymbolInput(event: CustomEvent<string>): void {
    symbolFilter = event.detail.trim();
    persistTimelineState();
    scheduleEventsReload(SYMBOL_INPUT_DEBOUNCE_MS);
  }
  function handleTimeframeChange(event: CustomEvent<string>): void {
    timeframeFilter = event.detail;
    persistSharedControlPrefs();
    persistTimelineState();
    clearEventsReloadTimer();
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

  function handleHover(event: CustomEvent<{ ts: number | null; x: number | null; hoveredEvent: TimelineHoverEvent | null }>): void {
    crosshairTs = event.detail.ts;
    crosshairPx = event.detail.x;
    hoveredEvent = event.detail.hoveredEvent;
  }
  function handleZoom(event: CustomEvent<{ centerTs: number; deltaY: number }>): void {
    lastZoomDelta = event.detail.deltaY;
    zoomAround(event.detail.centerTs, event.detail.deltaY);
    syncCrosshairTsFromPx();
    persistTimelineState();
    scheduleEventsReload(VIEWPORT_EVENT_RELOAD_DEBOUNCE_MS);
  }

  function handlePan(event: CustomEvent<{ deltaMs: number }>): void {
    lastPanDeltaMs = event.detail.deltaMs;
    panView(event.detail.deltaMs);
    syncCrosshairTsFromPx();
    persistTimelineState();
    scheduleEventsReload(VIEWPORT_EVENT_RELOAD_DEBOUNCE_MS);
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
  function rowIdentity(market: TimelineMarket): string {
    return `${market.collector}:${market.exchange}:${market.symbol}:${market.timeframe}`;
  }

  function captureCurrentScrollAnchor(): TimelineScrollAnchor | null {
    if (!scrollEl) return null;
    return captureTimelineScrollAnchor(filteredMarkets, scrollEl.scrollTop, ROW_HEIGHT, rowIdentity);
  }

  async function restoreScrollAnchor(anchor: TimelineScrollAnchor | null): Promise<void> {
    if (!scrollEl || !anchor) return;
    await tick();
    if (!scrollEl) return;
    const maxScrollTop = Math.max(0, scrollEl.scrollHeight - scrollEl.clientHeight);
    const restored = resolveTimelineRestoredScrollTop(filteredMarkets, anchor, ROW_HEIGHT, maxScrollTop, rowIdentity);
    scrollEl.scrollTop = restored;
    scrollTop = restored;
  }

  async function handleActionCompleted(event: CustomEvent<{ action: TimelineMarketAction; market: TimelineMarket }>): Promise<void> {
    await loadOverallRange();
    await loadMarkets(true);
    actionsMarket = event.detail.market;
  }

  function persistSharedControlPrefs(): void {
    const current = get(prefs);
    const controls: TimelineSharedControls = { collectorFilter, exchangeFilter, timeframeFilter };
    const next = mergeSharedControlsIntoPrefs(current, controls);
    if (!next) return;
    savePrefs(next);
  }

  function restoreSharedControlPrefs(): void {
    const restored = readSharedControlsFromPrefs(get(prefs), timeframeFilter);
    collectorFilter = restored.collectorFilter;
    exchangeFilter = restored.exchangeFilter;
    timeframeFilter = restored.timeframeFilter;
  }

  function persistTimelineState(): void {
    if (typeof window === "undefined") return;
    persistTimelineLocalState(window.localStorage, {
      symbolFilter,
      viewStartTs: viewRange?.startTs ?? null,
      viewEndTs: viewRange?.endTs ?? null,
    });
  }

  function restoreTimelineState(): void {
    if (typeof window === "undefined") return;
    const restored = restoreTimelineLocalState(window.localStorage);
    symbolFilter = restored.symbolFilter;
    persistedViewStartTs = restored.viewStartTs;
    persistedViewEndTs = restored.viewEndTs;
    if (restored.legacySharedControls) {
      if (typeof restored.legacySharedControls.collectorFilter === "string") {
        collectorFilter = restored.legacySharedControls.collectorFilter;
      }
      if (typeof restored.legacySharedControls.exchangeFilter === "string") {
        exchangeFilter = restored.legacySharedControls.exchangeFilter;
      }
      if (typeof restored.legacySharedControls.timeframeFilter === "string") {
        timeframeFilter = restored.legacySharedControls.timeframeFilter;
      }
      persistSharedControlPrefs();
    }
  }

  function clearEventsReloadTimer(): void {
    if (!eventsReloadTimer) return;
    clearTimeout(eventsReloadTimer);
    eventsReloadTimer = null;
  }
  function scheduleEventsReload(delayMs: number, forceReload = false): void {
    clearEventsReloadTimer();
    const waitMs = delayMs <= 0 ? 0 : delayMs;
    eventsReloadTimer = setTimeout(() => {
      eventsReloadTimer = null;
      void loadEvents(forceReload);
    }, waitMs);
  }
</script>
<div class="flex h-full min-h-0 flex-col bg-slate-950 text-slate-100">
  <TimelineControls {collectorFilter} {exchangeFilter} {symbolFilter} {timeframeFilter} {collectorOptions} {exchangeOptions} {timeframeOptions} on:collectorChange={handleCollectorChange} on:exchangeChange={handleExchangeChange} on:symbolInput={handleSymbolInput} on:timeframeChange={handleTimeframeChange} />

  <div class="relative flex-1 min-h-0">
    <div bind:this={scrollEl} class="relative h-full overflow-y-auto overflow-x-hidden" on:scroll={handleScroll}>
      <div class="pointer-events-none absolute inset-y-0 left-0 z-0 border-r border-slate-800 bg-slate-900/50" style={`width:${LEFT_WIDTH}px;`}></div>
      {#if marketsError}
        <div class="relative z-10 p-3 text-sm text-red-300">{marketsError}</div>
      {:else if !selectedRange || !viewRange}
        {#if loadingMarkets}
          <div class="relative z-10 p-3 text-sm text-slate-300">Loading markets...</div>
        {:else}
          <div class="relative z-10 p-3 text-sm text-slate-400">No markets in registry for selected timeframe.</div>
        {/if}
      {:else}
        <div class="relative z-10 min-w-max" style={`width: ${totalGridWidth}px;`}>
          <div style={`height: ${topPadding}px;`}></div>
          {#each visibleMarkets as market (rowIdentity(market))}
            <TimelineRow {market} events={groupedEvents.get(marketKey(market)) ?? []} range={selectedRange} {viewRange} timelineWidth={timelineWidth} rowHeight={ROW_HEIGHT} titleWidth={LEFT_WIDTH} on:open={handleOpen} on:hover={handleHover} on:zoom={handleZoom} on:pan={handlePan} on:actions={handleRowActions} />
          {/each}
          <div style={`height: ${bottomPadding}px;`}></div>
        </div>
      {/if}
    </div>
    {#if loadingMarkets && selectedRange && viewRange}
      <div class="pointer-events-none absolute right-2 top-2 z-20 rounded border border-slate-700/70 bg-slate-900/85 px-2 py-1 text-[11px] text-slate-300">
        Refreshing markets...
      </div>
    {/if}
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
    <TimelineEventPopover hoveredEvent={hoveredEvent} />
    <TimelineRowActionsMenu
      open={actionsOpen}
      anchorEl={actionsAnchorEl}
      market={actionsMarket}
      on:close={closeActionsMenu}
      on:openMarket={openMarketFromMenu}
      on:copyMarket={copyMarketFromMenu}
      on:actionCompleted={handleActionCompleted}
    />
  </div>
  {#if loadingEvents}
    <div class="border-t border-slate-800 bg-slate-900 px-3 py-1 text-[11px] text-slate-300">Loading events...</div>
  {:else if eventsError}
    <div class="border-t border-slate-800 bg-slate-900 px-3 py-1 text-[11px] text-red-300">{eventsError}</div>
  {/if}
</div>
