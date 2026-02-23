<script lang="ts">
  import { onDestroy, onMount } from "svelte";
  import ArrowLeft from "lucide-svelte/icons/arrow-left";
  import RefreshCcw from "lucide-svelte/icons/refresh-ccw";
  import { buildAppRouteUrl, resolveRouteMarket, type ChartRoute } from "../../framework/routing/routes.js";
  import { navigate } from "../../framework/routing/routeStore.js";
  import Autocomplete from "../../framework/ui/Autocomplete.svelte";
  import StartDateInput from "../../framework/ui/StartDateInput.svelte";
  import { formatStartInputUtc } from "../../../../../src/shared/startInput.js";
  import TimeframeDropdown from "./TimeframeDropdown.svelte";
  import type { Market } from "./types.js";
  import { buildViewerMarketKey, parseViewerMarketKey } from "./viewerMarketKey.js";
  import { markets, status } from "./viewerStore.js";
  import { connect, reconnect, setStart, setTarget } from "./viewerWs.js";

  export let route: ChartRoute | null = null;

  let currentMarkets: Market[] = [];
  let marketOptions: string[] = [];
  let selectedMarket = "";
  let selectedTimeframe = "1m";
  let selectedStartMs: number | null = null;
  let selectedStartValue = "";
  let initialSyncDone = false;
  let routeSignature = "none";

  const unsubMarkets = markets.subscribe((values) => {
    currentMarkets = values ?? [];
    syncFromRouteAndMarkets();
  });

  onMount(() => {
    connect();
  });

  onDestroy(() => {
    unsubMarkets();
  });

  $: routeSignature = buildRouteSignature(route);
  $: routeSignature, syncFromRouteAndMarkets();
  $: isConnected = $status === "connected";

  function uniq(list: string[]): string[] {
    return Array.from(new Set(list)).sort();
  }

  function pick(current: string, options: string[]): string {
    if (current && options.includes(current)) return current;
    return options[0] ?? "";
  }

  function buildRouteSignature(nextRoute: ChartRoute | null): string {
    if (!nextRoute || !nextRoute.market) {
      return `none:${nextRoute?.timeframe ?? ""}:${nextRoute?.startTs ?? ""}`;
    }
    return `${nextRoute.market.collector}:${nextRoute.market.exchange}:${nextRoute.market.symbol}:${nextRoute.timeframe ?? ""}:${nextRoute.startTs ?? ""}`;
  }

  function readRouteTimeframe(nextRoute: ChartRoute | null): string {
    return nextRoute?.timeframe?.trim() || "1m";
  }

  function readRouteStartMs(nextRoute: ChartRoute | null): number | null {
    if (!nextRoute || !Number.isFinite(nextRoute.startTs)) return null;
    return Math.floor(nextRoute.startTs as number);
  }

  function syncFromRouteAndMarkets(): void {
    selectedTimeframe = readRouteTimeframe(route);
    selectedStartMs = readRouteStartMs(route);
    selectedStartValue = selectedStartMs === null ? "" : formatStartInputUtc(selectedStartMs);
    selectedMarket = route?.market ? buildViewerMarketKey(route.market) : selectedMarket;

    if (!currentMarkets.length) {
      marketOptions = [];
      return;
    }

    const routeMarket = route?.market;
    const found = routeMarket ? resolveRouteMarket(currentMarkets, routeMarket) : null;
    marketOptions = uniq(currentMarkets.map((market) => buildViewerMarketKey(market)));
    const requestedMarket = routeMarket ? buildViewerMarketKey(routeMarket) : "";
    const market = found
      ? buildViewerMarketKey(found)
      : pick(requestedMarket, marketOptions);
    const parsed = parseViewerMarketKey(market);
    if (!parsed) return;

    selectedMarket = market;

    const desiredRoute: ChartRoute = {
      kind: "chart",
      market: {
        collector: parsed.collector,
        exchange: parsed.exchange,
        symbol: parsed.symbol,
      },
      timeframe: selectedTimeframe || undefined,
      startTs: selectedStartMs === null ? undefined : selectedStartMs,
    };
    const desiredUrl = buildAppRouteUrl(desiredRoute);
    const currentUrl = buildAppRouteUrl(route ?? { kind: "chart" });
    if (desiredUrl !== currentUrl) {
      navigate(desiredRoute, { replace: true });
    }

    sendActiveSelection(!initialSyncDone);
    initialSyncDone = true;
  }

  function sendActiveSelection(force = false): void {
    const parsed = parseViewerMarketKey(selectedMarket);
    if (!parsed) return;
    const exists = currentMarkets.some(
      (market) =>
        market.collector === parsed.collector &&
        market.exchange === parsed.exchange &&
        market.symbol === parsed.symbol,
    );
    if (!exists) return;
    setTarget(
      {
        collector: parsed.collector,
        exchange: parsed.exchange,
        symbol: parsed.symbol,
      },
      {
        force,
        clearMeta: true,
        timeframe: selectedTimeframe,
        startMs: selectedStartMs,
      },
    );
  }

  function handleMarketChange(value: string): void {
    const parsed = parseViewerMarketKey(value);
    if (!parsed) return;
    navigate(
      {
        kind: "chart",
        market: {
          collector: parsed.collector,
          exchange: parsed.exchange,
          symbol: parsed.symbol,
        },
        timeframe: selectedTimeframe || undefined,
        startTs: selectedStartMs === null ? undefined : selectedStartMs,
      },
      { replace: true },
    );
  }

  function handleTimeframeSelect(event: CustomEvent<string>): void {
    const timeframe = event.detail.trim();
    if (!timeframe) return;
    const parsed = parseViewerMarketKey(selectedMarket);
    if (!parsed) return;
    navigate(
      {
        kind: "chart",
        market: {
          collector: parsed.collector,
          exchange: parsed.exchange,
          symbol: parsed.symbol,
        },
        timeframe,
        startTs: selectedStartMs === null ? undefined : selectedStartMs,
      },
      { replace: true },
    );
  }

  function handleStartChange(event: CustomEvent<{ value: string; ms: number | null }>): void {
    const nextMs = event.detail.ms;
    const parsed = parseViewerMarketKey(selectedMarket);
    if (!parsed) return;
    selectedStartValue = event.detail.value;
    selectedStartMs = nextMs;
    setStart(nextMs, { force: true });
    navigate(
      {
        kind: "chart",
        market: {
          collector: parsed.collector,
          exchange: parsed.exchange,
          symbol: parsed.symbol,
        },
        timeframe: selectedTimeframe || undefined,
        startTs: nextMs === null ? undefined : nextMs,
      },
      { replace: true },
    );
  }

  function handleReconnect(): void {
    reconnect();
    sendActiveSelection(true);
  }
</script>

<header class="border-b border-slate-800 bg-slate-900/80">
  <div class="flex flex-wrap items-center overflow-hidden text-xs">
    <button
      class="border-r border-slate-800 bg-transparent px-2 py-1.5 text-slate-100 outline-none hover:bg-slate-800/30"
      type="button"
      aria-label="Back to timeline"
      on:click={() => navigate({ kind: "timeline" })}
    >
      <ArrowLeft class="h-4 w-4 text-slate-100" aria-hidden="true" strokeWidth={1.9} />
    </button>

    <Autocomplete
      options={marketOptions}
      value={selectedMarket}
      placeholder="COLLECTOR:EXCHANGE:SYMBOL"
      on:change={(event) => handleMarketChange(event.detail)}
    />

    <TimeframeDropdown
      currentValue={selectedTimeframe}
      on:select={handleTimeframeSelect}
    />

    <StartDateInput
      className="w-[150px] border-l border-slate-800 bg-transparent py-1.5 pl-2 pr-8 text-xs text-slate-100 outline-none placeholder:text-slate-500"
      value={selectedStartValue}
      on:change={handleStartChange}
    />

    <div class="ml-auto flex items-center gap-2 border-l border-slate-800 px-3 py-1.5 text-xs">
      <span class={`h-1.5 w-1.5 rounded-full ${isConnected ? "bg-emerald-400" : "bg-rose-400"}`}></span>
      <span class={isConnected ? "text-emerald-300" : "text-rose-300"}>
        {isConnected ? "Connected" : "Disconnected"}
      </span>
      {#if !isConnected}
        <button
          type="button"
          class="inline-flex items-center justify-center rounded p-1 text-slate-500 hover:bg-slate-800/80 hover:text-slate-200 focus-visible:outline focus-visible:outline-2 focus-visible:outline-slate-600 -my-1 -mr-1"
          aria-label="Reconnect websocket"
          on:click={handleReconnect}
        >
          <RefreshCcw class="h-3.5 w-3.5" aria-hidden="true" strokeWidth={1.9} />
        </button>
      {/if}
    </div>
  </div>
</header>
