<script lang="ts">
  import { createEventDispatcher } from "svelte";

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

<header class="flex flex-wrap items-center overflow-hidden rounded border border-slate-800 bg-slate-900/80">
  <select
    class="border-none bg-transparent px-2 py-1.5 text-xs text-slate-100 outline-none mr-2"
    value={collectorFilter}
    on:change={handleCollectorChange}
  >
    <option value="">All Collectors</option>
    {#each collectorOptions as collector}
      <option value={collector}>{collector}</option>
    {/each}
  </select>
  <select
    class="border-l border-slate-800 bg-transparent px-2 py-1.5 text-xs text-slate-100 outline-none mr-2"
    value={exchangeFilter}
    on:change={handleExchangeChange}
  >
    <option value="">All Exchanges</option>
    {#each exchangeOptions as exchange}
      <option value={exchange}>{exchange}</option>
    {/each}
  </select>
  <input
    class="border-l border-slate-800 bg-transparent px-2 py-1.5 text-xs text-slate-100 placeholder:text-slate-500 outline-none mr-2"
    placeholder="Search market symbol..."
    value={symbolFilter}
    on:input={handleSymbolInput}
  />
  <select
    class="border-l border-slate-800 bg-transparent px-2 py-1.5 text-xs text-slate-100 outline-none mr-2"
    value={timeframeFilter}
    on:change={handleTimeframeChange}
  >
    {#each timeframeOptions as timeframe}
      <option value={timeframe}>{timeframe}</option>
    {/each}
  </select>
</header>
