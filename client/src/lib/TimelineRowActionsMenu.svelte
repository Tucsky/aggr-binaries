<script lang="ts">
  import { createEventDispatcher } from "svelte";
  import Dropdown from "./Dropdown.svelte";
  import type { TimelineMarket } from "./timelineTypes.js";

  export let open = false;
  export let anchorEl: HTMLElement | null = null;
  export let market: TimelineMarket | null = null;

  const dispatch = createEventDispatcher<{
    close: void;
    openMarket: TimelineMarket;
    copyMarket: TimelineMarket;
  }>();

  const futureLabels = ["Index", "Process", "Fix gaps", "Rebuild", "Export timeframe", "Delete outputs"];

  function closeMenu(): void {
    dispatch("close");
  }

  function emitOpenMarket(): void {
    if (!market) return;
    dispatch("openMarket", market);
    closeMenu();
  }

  function emitCopyMarket(): void {
    if (!market) return;
    dispatch("copyMarket", market);
    closeMenu();
  }
</script>

<Dropdown {open} {anchorEl} on:close={closeMenu} margin={8}>
  <div class="w-44 p-1">
    <button
      class="block w-full rounded px-3 py-1.5 text-left text-xs text-slate-200 hover:bg-slate-800/80"
      type="button"
      on:click={emitOpenMarket}
      disabled={!market}
    >
      Open in Viewer
    </button>
    <button
      class="block w-full rounded px-3 py-1.5 text-left text-xs text-slate-200 hover:bg-slate-800/80"
      type="button"
      on:click={emitCopyMarket}
      disabled={!market}
    >
      Copy market key
    </button>
    {#each futureLabels as label}
      <button
        class="block w-full cursor-not-allowed rounded px-3 py-1.5 text-left text-xs text-slate-500"
        type="button"
        disabled
      >
        {label}
      </button>
    {/each}
  </div>
</Dropdown>
