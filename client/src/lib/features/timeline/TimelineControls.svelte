<script lang="ts">
  import { createEventDispatcher } from "svelte";
  import Building2 from "lucide-svelte/icons/building-2";
  import Clock3 from "lucide-svelte/icons/clock-3";
  import Database from "lucide-svelte/icons/database";
  import Search from "lucide-svelte/icons/search";

  export let collectorFilter = "";
  export let exchangeFilter = "";
  export let symbolFilter = "";
  export let timeframeFilter = "";
  export let collectorOptions: string[] = [];
  export let exchangeOptions: string[] = [];
  export let timeframeOptions: string[] = [];

  const dispatch = createEventDispatcher<{
    collectorChange: string;
    exchangeChange: string;
    symbolInput: string;
    timeframeChange: string;
  }>();

  function handleCollectorChange(event: Event): void {
    dispatch("collectorChange", (event.target as HTMLSelectElement).value);
  }

  function handleExchangeChange(event: Event): void {
    dispatch("exchangeChange", (event.target as HTMLSelectElement).value);
  }

  function handleSymbolInput(event: Event): void {
    dispatch("symbolInput", (event.target as HTMLInputElement).value);
  }

  function handleTimeframeChange(event: Event): void {
    dispatch("timeframeChange", (event.target as HTMLSelectElement).value);
  }
</script>

<header class="flex flex-wrap items-center overflow-hidden border-b border-slate-800 bg-slate-900/80">
  <div class="relative mr-2">
    <Database
      class="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-slate-500"
      aria-hidden="true"
      strokeWidth={2}
    />
    <select
      class="border-none bg-transparent py-1.5 pl-7 pr-2 text-xs text-slate-100 outline-none"
      value={collectorFilter}
      on:change={handleCollectorChange}
    >
      <option value="">All Collectors</option>
      {#each collectorOptions as collector}
        <option value={collector}>{collector}</option>
      {/each}
    </select>
  </div>
  <div class="relative mr-2">
    <Building2
      class="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-slate-500"
      aria-hidden="true"
      strokeWidth={2}
    />
    <select
      class="border-l border-slate-800 bg-transparent py-1.5 pl-7 pr-2 text-xs text-slate-100 outline-none"
      value={exchangeFilter}
      on:change={handleExchangeChange}
    >
      <option value="">All Exchanges</option>
      {#each exchangeOptions as exchange}
        <option value={exchange}>{exchange}</option>
      {/each}
    </select>
  </div>
  <div class="relative mr-2">
    <Search
      class="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-slate-500"
      aria-hidden="true"
      strokeWidth={2}
    />
    <input
      class="border-l border-slate-800 bg-transparent py-1.5 pl-7 pr-2 text-xs text-slate-100 placeholder:text-slate-500 outline-none"
      placeholder="Search market symbol..."
      value={symbolFilter}
      on:input={handleSymbolInput}
    />
  </div>
  <div class="relative mr-2">
    <Clock3
      class="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-slate-500"
      aria-hidden="true"
      strokeWidth={2}
    />
    <select
      class="border-l border-slate-800 bg-transparent py-1.5 pl-7 pr-2 text-xs text-slate-100 outline-none"
      value={timeframeFilter}
      on:change={handleTimeframeChange}
    >
      {#each timeframeOptions as timeframe}
        <option value={timeframe}>{timeframe}</option>
      {/each}
    </select>
  </div>
</header>
