<script lang="ts">
  import { onDestroy, onMount } from "svelte";
  import { get } from "svelte/store";
  import ArrowLeft from "lucide-svelte/icons/arrow-left";
  import Settings2 from "lucide-svelte/icons/settings-2";
  import Autocomplete from "./Autocomplete.svelte";
  import Dropdown from "./Dropdown.svelte";
  import StartDateInput from "./StartDateInput.svelte";
  import TimeframeDropdown from "./TimeframeDropdown.svelte";
  import type { Market } from "./types.js";
  import { resolveRouteMarket, type ChartRoute } from "./routes.js";
  import {
      markets,
      meta,
      prefs,
      savePrefs,
      status,
  } from "./viewerStore.js";
  import {
      connect,
      reconnect,
      requestMarkets,
      setStart,
      setTarget,
  } from "./viewerWs.js";
  import { parseStartInputUtcMs } from "../../../src/shared/startInput.js";
  import { navigate } from "./routeStore.js";

  export let route: ChartRoute | null = null;

  let local = get(prefs);
  let currentMarkets: Market[] = [];
  let collectorOptions: string[] = [];
  let marketOptions: string[] = [];
  let localMarket = combineMarket(local.exchange, local.symbol);
  let initialSyncDone = false;
  let manualRouteOverride = false;
  let routeInvalid = false;
  let routeSignature = "";
  let settingsDropdownOpened = false;
  let settingsDropdownButton: HTMLButtonElement | null = null;

  const unsubPrefs = prefs.subscribe((v) => (local = v));
  const unsubMarkets = markets.subscribe((values) =>
    syncFromMarkets(values ?? []),
  );

  onMount(() => {
    connect(local);
  });

  onDestroy(() => {
    unsubPrefs();
    unsubMarkets();
  });

  $: routeSignature = route?.market
    ? `${route.market.collector}:${route.market.exchange}:${route.market.symbol}:${route.timeframe ?? ""}:${route.startTs ?? ""}`
    : "";
  $: if (routeSignature) {
    manualRouteOverride = false;
  }

  $: prefs.set(local);
  $: if (!local.timeframe) local.timeframe = "1m";

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

  function syncFromMarkets(marketsList: Market[], force = false) {
    currentMarkets = marketsList;
    routeInvalid = false;
    if (!marketsList.length) {
      collectorOptions = [];
      marketOptions = [];
      return;
    }

    const routeMarket = route?.market;
    if (routeMarket && !manualRouteOverride) {
      const found = resolveRouteMarket(marketsList, routeMarket);
      if (!found) {
        routeInvalid = true;
        meta.set(null);
        collectorOptions = uniq(marketsList.map((m) => m.collector));
        const collector = collectorOptions.includes(routeMarket.collector.toUpperCase())
          ? routeMarket.collector.toUpperCase()
          : local.collector;
        marketOptions = uniq(
          marketsList
            .filter((m) => m.collector === collector)
            .map((m) => combineMarket(m.exchange, m.symbol)),
        );
        local = {
          ...local,
          collector: routeMarket.collector.toUpperCase(),
          exchange: routeMarket.exchange.toUpperCase(),
          symbol: routeMarket.symbol,
        };
        localMarket = combineMarket(local.exchange, local.symbol);
        savePrefs(local);
        return;
      }
      local = {
        ...local,
        collector: found.collector,
        exchange: found.exchange,
        symbol: found.symbol,
      };
      localMarket = combineMarket(found.exchange, found.symbol);
    }

    collectorOptions = uniq(marketsList.map((m) => m.collector));
    const collector = collectorOptions.includes(local.collector)
      ? local.collector
      : pick(local.collector, collectorOptions);

    marketOptions = uniq(
      marketsList
        .filter((m) => m.collector === collector)
        .map((m) => combineMarket(m.exchange, m.symbol)),
    );

    const desiredMarket =
      localMarket || combineMarket(local.exchange, local.symbol);
    const market = pick(desiredMarket, marketOptions);
    const parsed = parseMarket(market);
    local = {
      ...local,
      collector,
      exchange: parsed?.exchange ?? "",
      symbol: parsed?.symbol ?? "",
    };
    localMarket = market;
    savePrefs(local);
    sendSelections(force || !initialSyncDone);
    initialSyncDone = true;
  }

  function sendSelections(force = false) {
    const parsed = parseMarket(
      localMarket || combineMarket(local.exchange, local.symbol),
    );
    if (!local.collector || !parsed) return;
    const exists = currentMarkets.some(
      (m) =>
        m.collector === local.collector &&
        m.exchange === parsed.exchange &&
        m.symbol === parsed.symbol,
    );
    if (!exists) return;
    const startMs = parseStart(local.start);
    setTarget(
      {
        collector: local.collector,
        exchange: parsed.exchange,
        symbol: parsed.symbol,
      },
      {
        force,
        clearMeta: true,
        timeframe: local.timeframe,
        startMs,
      },
    );
  }

  function handleCollectorChange(event: Event) {
    manualRouteOverride = true;
    const value = (event.target as HTMLSelectElement).value;
    local = { ...local, collector: value, exchange: "", symbol: "" };
    localMarket = "";
    savePrefs(local);
    syncFromMarkets(currentMarkets, true);
  }

  function handleMarketChange(value: string) {
    manualRouteOverride = true;
    localMarket = value;
    const parsed = parseMarket(value);
    if (!parsed) return;
    local = { ...local, exchange: parsed.exchange, symbol: parsed.symbol };
    savePrefs(local);
    sendSelections(true);
  }

  function handleStartChange(event: CustomEvent<{ value: string; ms: number | null }>) {
    const nextValue = event.detail.value;
    const nextMs = event.detail.ms;
    const currentMs = parseStart(local.start);
    if (nextValue !== local.start) {
      local = { ...local, start: nextValue };
      savePrefs(local);
    }
    if (nextMs !== currentMs) {
      setStart(nextMs, { force: true });
    }
  }

  function parseStart(value: string): number | null | undefined {
    if (!value) return null;
    const ms = parseStartInputUtcMs(value);
    return ms === null ? undefined : ms;
  }

  function handleReconnect() {
    reconnect();
    sendSelections(true);
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
      value={localMarket}
      placeholder="EXCHANGE:SYMBOL"
      on:change={(e) => handleMarketChange(e.detail)}
    />

    <TimeframeDropdown />

    <StartDateInput
      className="border-none bg-slate-900 outline-none px-2 py-1.5 text-slate-100 placeholder:text-slate-500"
      value={local.start}
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
  {#if routeInvalid}
    <div class="px-2 pb-1 text-[11px] text-amber-300">
      Route market was not found in registry. Select a market to continue.
    </div>
  {/if}
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
        value={local.collector}
      >
        {#if collectorOptions.length === 0}
          <option disabled selected>Loading...</option>
        {:else}
          {#each collectorOptions as option}
            <option value={option} selected={option === local.collector}
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
          <div>Collector: {local.collector}</div>
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
