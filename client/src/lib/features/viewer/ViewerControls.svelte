<script lang="ts">
  import { onDestroy, onMount } from "svelte";
  import ArrowLeft from "lucide-svelte/icons/arrow-left";
  import Settings2 from "lucide-svelte/icons/settings-2";
  import Autocomplete from "../../framework/ui/Autocomplete.svelte";
  import Dropdown from "../../framework/ui/Dropdown.svelte";
  import StartDateInput from "../../framework/ui/StartDateInput.svelte";
  import TimeframeDropdown from "./TimeframeDropdown.svelte";
  import type { Market } from "./types.js";
  import { buildAppRouteUrl, resolveRouteMarket, type ChartRoute } from "../../framework/routing/routes.js";
  import {
      markets,
      meta,
      status,
  } from "./viewerStore.js";
  import {
      connect,
      reconnect,
      requestMarkets,
      setStart,
      setTarget,
  } from "./viewerWs.js";
  import { formatStartInputUtc } from "../../../../../src/shared/startInput.js";
  import { navigate } from "../../framework/routing/routeStore.js";

  export let route: ChartRoute | null = null;

  let currentMarkets: Market[] = [];
  let collectorOptions: string[] = [];
  let marketOptions: string[] = [];
  let selectedCollector = "";
  let selectedMarket = "";
  let selectedTimeframe = "1m";
  let selectedStartMs: number | null = null;
  let selectedStartValue = "";
  let initialSyncDone = false;
  let routeSignature = "none";
  let settingsDropdownOpened = false;
  let settingsDropdownButton: HTMLButtonElement | null = null;

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

  function uniq(list: string[]): string[] {
    return Array.from(new Set(list)).sort();
  }

  function pick(current: string, options: string[]): string {
    if (current && options.includes(current)) return current;
    return options[0] ?? "";
  }

  function combineMarket(exchange: string, symbol: string): string {
    if (!exchange || !symbol) return "";
    return `${exchange}:${symbol}`;
  }

  function parseMarket(
    value: string,
  ): { exchange: string; symbol: string } | null {
    const [exchange, ...rest] = value.split(":");
    const symbol = rest.join(":");
    const ex = exchange?.trim().toUpperCase();
    const sym = symbol?.trim();
    if (!ex || !sym) return null;
    return { exchange: ex, symbol: sym };
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

    if (!currentMarkets.length) {
      collectorOptions = [];
      marketOptions = [];
      return;
    }

    const routeMarket = route?.market;
    const found = routeMarket ? resolveRouteMarket(currentMarkets, routeMarket) : null;
    collectorOptions = uniq(currentMarkets.map((m) => m.collector));
    const requestedCollector = routeMarket?.collector?.toUpperCase() ?? "";
    const collector = found?.collector
      ?? (collectorOptions.includes(requestedCollector) ? requestedCollector : pick("", collectorOptions));

    marketOptions = uniq(
      currentMarkets
        .filter((m) => m.collector === collector)
        .map((m) => combineMarket(m.exchange, m.symbol)),
    );
    const requestedMarket = routeMarket
      ? combineMarket(routeMarket.exchange, routeMarket.symbol)
      : "";
    const market = found
      ? combineMarket(found.exchange, found.symbol)
      : pick(requestedMarket, marketOptions);
    const parsed = parseMarket(market);
    if (!collector || !parsed) return;

    selectedCollector = collector;
    selectedMarket = market;

    const desiredRoute: ChartRoute = {
      kind: "chart",
      market: { collector, exchange: parsed.exchange, symbol: parsed.symbol },
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
    const parsed = parseMarket(selectedMarket);
    if (!selectedCollector || !parsed) return;
    const exists = currentMarkets.some(
      (m) =>
        m.collector === selectedCollector &&
        m.exchange === parsed.exchange &&
        m.symbol === parsed.symbol,
    );
    if (!exists) return;
    setTarget(
      {
        collector: selectedCollector,
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

  function handleCollectorChange(event: Event): void {
    const collector = (event.target as HTMLSelectElement).value;
    const nextOptions = uniq(
      currentMarkets
        .filter((m) => m.collector === collector)
        .map((m) => combineMarket(m.exchange, m.symbol)),
    );
    const nextMarket = pick("", nextOptions);
    const parsed = parseMarket(nextMarket);
    if (!collector || !parsed) return;
    navigate(
      {
        kind: "chart",
        market: { collector, exchange: parsed.exchange, symbol: parsed.symbol },
        timeframe: selectedTimeframe || undefined,
        startTs: selectedStartMs === null ? undefined : selectedStartMs,
      },
      { replace: true },
    );
  }

  function handleMarketChange(value: string): void {
    const parsed = parseMarket(value);
    if (!parsed || !selectedCollector) return;
    navigate(
      {
        kind: "chart",
        market: {
          collector: selectedCollector,
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
    const parsed = parseMarket(selectedMarket);
    if (!selectedCollector || !parsed) return;
    navigate(
      {
        kind: "chart",
        market: {
          collector: selectedCollector,
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
    const parsed = parseMarket(selectedMarket);
    if (!selectedCollector || !parsed) return;
    selectedStartValue = event.detail.value;
    selectedStartMs = nextMs;
    setStart(nextMs, { force: true });
    navigate(
      {
        kind: "chart",
        market: {
          collector: selectedCollector,
          exchange: parsed.exchange,
          symbol: parsed.symbol,
        },
        timeframe: selectedTimeframe || undefined,
        startTs: nextMs === null ? undefined : nextMs,
      },
      { replace: true },
    );
  }

  function handleReconnect() {
    reconnect();
    sendActiveSelection(true);
  }

  function refreshMarkets() {
    requestMarkets();
  }

  function toggleSettings() {
    settingsDropdownOpened = !settingsDropdownOpened;
  }
</script>

<header class="border-b border-slate-800 bg-slate-900">
  <div class="flex flex-wrap items-stretch justify-between text-sm">
    <button
      class="border-none px-2 py-1.5 outline-none bg-slate-900 text-slate-100 hover:bg-slate-900/60"
      type="button"
      aria-label="Back to timeline"
      on:click={() => navigate({ kind: "timeline" })}
    >
      <ArrowLeft class="h-4 w-4 text-slate-100" aria-hidden="true" strokeWidth={1.9} />
    </button>

    <Autocomplete
      options={marketOptions}
      value={selectedMarket}
      placeholder="EXCHANGE:SYMBOL"
      on:change={(e) => handleMarketChange(e.detail)}
    />

    <TimeframeDropdown
      currentValue={selectedTimeframe}
      on:select={handleTimeframeSelect}
    />

    <StartDateInput
      className="border-none bg-slate-900 outline-none px-2 py-1.5 text-slate-100 placeholder:text-slate-500"
      value={selectedStartValue}
      on:change={handleStartChange}
    />

    <button
      class="ml-auto flex items-center gap-2 px-2 py-1.5 text-slate-100 hover:bg-slate-900/60 focus-visible:outline focus-visible:outline-2 focus-visible:outline-slate-600"
      on:click={toggleSettings}
      type="button"
      bind:this={settingsDropdownButton}
    >
      <Settings2 class="h-4 w-4 text-slate-100" aria-hidden="true" strokeWidth={1.85} />
      <span>Settings</span>
    </button>
  </div>
</header>

<Dropdown
  open={settingsDropdownOpened}
  anchorEl={settingsDropdownButton}
  on:close={() => (settingsDropdownOpened = false)}
  margin={10}
>
  <div class="w-72 space-y-2 p-4 text-sm">
    <div class="space-y-2">
      <div class="text-[11px] uppercase tracking-[0.08em] text-slate-400">
        Collector
      </div>
      <select
        class="w-full rounded-md border border-slate-800 bg-slate-900/80 px-3 py-2 text-slate-100"
        on:change={handleCollectorChange}
        value={selectedCollector}
      >
        {#if collectorOptions.length === 0}
          <option disabled selected>Loading...</option>
        {:else}
          {#each collectorOptions as option}
            <option value={option} selected={option === selectedCollector}
              >{option}</option
            >
          {/each}
        {/if}
      </select>
    </div>

    <div class="flex items-center gap-2">
      <button
        class="flex-1 rounded-md bg-emerald-600 px-3 py-2 font-semibold text-white transition hover:bg-emerald-500"
        on:click={handleReconnect}
      >
        Reconnect
      </button>
      <button
        class="flex-1 rounded-md border border-slate-700 bg-slate-800 px-3 py-2 text-slate-100 transition hover:bg-slate-700"
        on:click={refreshMarkets}
      >
        Fetch
      </button>
    </div>

    <div
      class="space-y-2 rounded-lg border border-slate-800 bg-slate-900/70 p-3 text-xs"
    >
      <div>
        Status:
        {#if $status === "connected"}
          <span class="text-emerald-400">connected</span>
        {:else if $status === "error"}
          <span class="text-red-400">error</span>
        {:else if $status === "closed"}
          <span class="text-yellow-300">closed</span>
        {:else}
          <span class="text-slate-400">idle</span>
        {/if}
      </div>

      {#if $status === "connected" && $meta}
        <div class="space-y-1 text-slate-200">
          <div>Collector: {selectedCollector}</div>
          <div>
            Timeframe: {$meta.timeframe ??
              `${($meta.timeframeMs ?? 0) / 1000}s`}
          </div>
          <div>Records: {$meta.records ?? "?"}</div>
        </div>
      {/if}
    </div>
  </div>
</Dropdown>
