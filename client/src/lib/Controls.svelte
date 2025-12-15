<script lang="ts">
  import { onDestroy, onMount } from "svelte";
  import { get } from "svelte/store";
  import Autocomplete from "./Autocomplete.svelte";
  import type { Market } from "./types.js";
  import { collapsed, markets, meta, prefs, savePrefs, status, timeframes } from "./viewerStore.js";
  import { connect, reconnect, requestMarkets, setStart, setTarget, setTimeframe } from "./viewerWs.js";

  let local = get(prefs);
  let currentMarkets: Market[] = [];
  let collectorOptions: string[] = [];
  let marketOptions: string[] = [];
  let timeframeOptions: string[] = [];
  const marketInputId = "market-input";
  let localMarket = combineMarket(local.exchange, local.symbol);
  let initialSyncDone = false;

  const unsubPrefs = prefs.subscribe((v) => (local = v));
  const unsubMarkets = markets.subscribe((values) => syncFromMarkets(values ?? []));
  const unsubTimeframes = timeframes.subscribe((values) => syncTimeframes(values ?? []));
  const unsubStatus = status.subscribe(() => {});

  onMount(() => {
    connect(local);
  });

  onDestroy(() => {
    unsubPrefs();
    unsubMarkets();
    unsubTimeframes();
    unsubStatus();
  });

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

  function parseMarket(value: string): { exchange: string; symbol: string } | null {
    const [exchange, ...rest] = value.split(":");
    const symbol = rest.join(":");
    const ex = exchange?.trim().toUpperCase();
    const sym = symbol?.trim();
    if (!ex || !sym) return null;
    return { exchange: ex, symbol: sym };
  }

  function syncFromMarkets(marketsList: Market[], force = false) {
    currentMarkets = marketsList;
    collectorOptions = uniq(marketsList.map((m) => m.collector));
    const collector = pick(local.collector, collectorOptions);

    marketOptions = uniq(
      marketsList.filter((m) => m.collector === collector).map((m) => combineMarket(m.exchange, m.symbol)),
    );

    const desiredMarket = localMarket || combineMarket(local.exchange, local.symbol);
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

  function syncTimeframes(options: string[], force = false) {
    timeframeOptions = options.slice();
    if (!timeframeOptions.length) return;
    const timeframe = pick(local.timeframe, timeframeOptions);
    if (timeframe !== local.timeframe) {
      local = { ...local, timeframe };
      savePrefs(local);
    }
    sendSelections(force || !initialSyncDone);
  }

  function sendSelections(force = false) {
    const parsed = parseMarket(localMarket || combineMarket(local.exchange, local.symbol));
    if (!local.collector || !parsed) return;
    const exists = currentMarkets.some(
      (m) => m.collector === local.collector && m.exchange === parsed.exchange && m.symbol === parsed.symbol,
    );
    if (!exists) return;
    setTarget(
      { collector: local.collector, exchange: parsed.exchange, symbol: parsed.symbol },
      { force, clearMeta: true },
    );
    if (timeframeOptions.length) {
      setTimeframe(local.timeframe, { force });
    }
    if (local.start) {
      const ts = Date.parse(local.start);
      if (!Number.isNaN(ts)) {
        setStart(ts, { force });
      }
    }
  }

  function handleCollectorChange(event: Event) {
    const value = (event.target as HTMLSelectElement).value;
    local = { ...local, collector: value, exchange: "", symbol: "" };
    localMarket = "";
    savePrefs(local);
    syncFromMarkets(currentMarkets, true);
  }

  function handleMarketChange(value: string) {
    localMarket = value;
    const parsed = parseMarket(value);
    if (!parsed) return;
    local = { ...local, exchange: parsed.exchange, symbol: parsed.symbol };
    savePrefs(local);
    sendSelections(true);
  }

  function handleTimeframeChange(event: Event) {
    const value = (event.target as HTMLSelectElement).value;
    local = { ...local, timeframe: value };
    savePrefs(local);
    setTimeframe(local.timeframe, { force: true });
  }

  function handleStartChange(event: Event) {
    const value = (event.target as HTMLInputElement).value;
    local = { ...local, start: value };
    savePrefs(local);
    if (!value) {
      setStart(null, { force: true });
      return;
    }
    const ts = Date.parse(value);
    if (!Number.isNaN(ts)) {
      setStart(ts, { force: true });
    }
  }

  function handleReconnect() {
    reconnect();
    sendSelections(true);
  }

  function toggleControls() {
    collapsed.update((v) => !v);
  }

  function refreshMarkets() {
    requestMarkets();
  }
</script>

<div class="absolute top-2 left-2 z-20 flex flex-col gap-2">
  <button
    class="bg-slate-800/90 border border-slate-700 rounded px-3 py-1 text-xs w-fit"
    on:click={toggleControls}
  >
    {$collapsed ? "Show controls" : "Hide controls"}
  </button>

  <div
    class={`bg-slate-900/90 border border-slate-700 rounded-md p-3 flex flex-col gap-3 text-xs ${
      $collapsed ? "hidden" : "flex"
    }`}
  >
    <div class="flex flex-col gap-2">
      <label class="flex items-center gap-2">
        <span class="w-24 text-slate-300">Collector</span>
        <select
          class="bg-slate-900 border border-slate-700 rounded px-2 py-1 text-slate-100"
          on:change={handleCollectorChange}
          value={local.collector}
        >
          {#if collectorOptions.length === 0}
            <option disabled selected>Loading...</option>
          {:else}
            {#each collectorOptions as option}
              <option value={option} selected={option === local.collector}>{option}</option>
            {/each}
          {/if}
        </select>
      </label>

      <label class="flex items-center gap-2" for={marketInputId}>
        <span class="w-24 text-slate-300">Market</span>
        <div class="flex-1">
          <Autocomplete
            id={marketInputId}
            options={marketOptions}
            value={localMarket}
            placeholder="EXCHANGE:SYMBOL"
            on:change={(e) => handleMarketChange(e.detail)}
          />
        </div>
      </label>

      <label class="flex items-center gap-2">
        <span class="w-24 text-slate-300">Timeframe</span>
        <select
          class="bg-slate-900 border border-slate-700 rounded px-2 py-1 text-slate-100"
          on:change={handleTimeframeChange}
          value={local.timeframe}
          disabled={timeframeOptions.length === 0}
        >
          {#if timeframeOptions.length === 0}
            <option disabled selected>Load timeframes</option>
          {:else}
            {#each timeframeOptions as option}
              <option value={option} selected={option === local.timeframe}>{option}</option>
            {/each}
          {/if}
        </select>
      </label>

      <label class="flex items-center gap-2">
        <span class="w-24 text-slate-300">Start (UTC)</span>
        <input
          class="bg-slate-900 border border-slate-700 rounded px-2 py-1 text-slate-100 w-full"
          type="datetime-local"
          value={local.start}
          on:change={handleStartChange}
        />
      </label>
    </div>

    <div class="flex gap-2">
      <button
        class="bg-emerald-600 hover:bg-emerald-500 text-white font-semibold px-3 py-2 rounded"
        on:click={handleReconnect}
      >
        Reconnect
      </button>
      <button
        class="bg-slate-800 hover:bg-slate-700 text-slate-100 border border-slate-700 px-3 py-2 rounded"
        on:click={refreshMarkets}
      >
        Reload markets
      </button>
    </div>

    <div class="text-xs space-y-1">
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
        <div class="text-slate-300 space-y-1">
          <div>Timeframe: {$meta.timeframe ?? `${($meta.timeframeMs ?? 0) / 1000}s`}</div>
          <div>Records: {$meta.records ?? "?"}</div>
        </div>
      {/if}
    </div>
  </div>
</div>
