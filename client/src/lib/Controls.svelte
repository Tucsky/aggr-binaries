<script lang="ts">
  import { onDestroy } from "svelte";
  import { get } from "svelte/store";
  import { collapsed, meta, prefs, savePrefs, status } from "./viewerStore.js";
  import { connect, disconnect } from "./viewerWs.js";

  let local = get(prefs);

  const unsubscribe = prefs.subscribe((v) => (local = v));
  $: prefs.set(local);

  function toggleControls() {
    collapsed.update((v) => !v);
  }

  function handleConnect() {
    savePrefs(local);
    connect(local);
  }

  function handleDisconnect() {
    disconnect();
  }

  $: if (!local.collector) local.collector = "PI";
  $: if (!local.exchange) local.exchange = "BITFINEX";
  $: if (!local.symbol) local.symbol = "BTCUSD";

  // Cleanup
  onDestroy(unsubscribe);
</script>

<div class="absolute top-2 left-2 z-20 flex flex-col gap-2">
  <button
    class="bg-slate-800/90 border border-slate-700 rounded px-3 py-1 text-xs w-fit"
    on:click={toggleControls}
  >
    {$collapsed ? "Show controls" : "Hide controls"}
  </button>

  <div
    class={`bg-slate-900/90 border border-slate-700 rounded-md p-3 flex flex-col gap-2 text-xs ${
      $collapsed ? "hidden" : "flex"
    }`}
  >
    <label class="flex items-center gap-2">
      <span class="w-20 text-slate-300">Collector</span>
      <input
        class="bg-slate-900 border border-slate-700 rounded px-2 py-1 text-slate-100"
        bind:value={local.collector}
        size="8"
      />
    </label>
    <label class="flex items-center gap-2">
      <span class="w-20 text-slate-300">Exchange</span>
      <input
        class="bg-slate-900 border border-slate-700 rounded px-2 py-1 text-slate-100"
        bind:value={local.exchange}
        size="12"
      />
    </label>
    <label class="flex items-center gap-2">
      <span class="w-20 text-slate-300">Symbol</span>
      <input
        class="bg-slate-900 border border-slate-700 rounded px-2 py-1 text-slate-100"
        bind:value={local.symbol}
        size="12"
      />
    </label>
    <label class="flex items-center gap-2">
      <span class="w-20 text-slate-300">Start (UTC)</span>
      <input
        class="bg-slate-900 border border-slate-700 rounded px-2 py-1 text-slate-100"
        type="datetime-local"
        bind:value={local.start}
      />
    </label>
    <button
      class="mt-1 bg-emerald-600 hover:bg-emerald-500 text-white font-semibold px-3 py-2 rounded"
      on:click={$status === "connected" ? handleDisconnect : handleConnect}
    >
      {$status === "connected" ? "Disconnect" : "Connect"}
    </button>
    <div class="text-xs">
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
    {#if $status === "connected"}
      <div class="text-xs text-slate-300 space-y-1">
        <div>Records: {$meta?.records ?? "?"}</div>
      </div>
    {/if}
  </div>
</div>
