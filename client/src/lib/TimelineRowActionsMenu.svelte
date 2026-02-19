<script lang="ts">
  import ArrowUpRight from "lucide-svelte/icons/arrow-up-right";
  import Binary from "lucide-svelte/icons/binary";
  import Copy from "lucide-svelte/icons/copy";
  import Download from "lucide-svelte/icons/download";
  import RefreshCcw from "lucide-svelte/icons/refresh-ccw";
  import Trash2 from "lucide-svelte/icons/trash-2";
  import Workflow from "lucide-svelte/icons/workflow";
  import Wrench from "lucide-svelte/icons/wrench";
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

  const futureActions = [
    { label: "Index", icon: Binary },
    { label: "Process", icon: Workflow },
    { label: "Fix gaps", icon: Wrench },
    { label: "Rebuild", icon: RefreshCcw },
    { label: "Export timeframe", icon: Download },
  ];

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
      class="flex w-full items-center gap-2 rounded px-3 py-1.5 text-left text-xs text-slate-200 hover:bg-slate-800/80"
      type="button"
      on:click={emitOpenMarket}
      disabled={!market}
    >
      <ArrowUpRight class="h-3.5 w-3.5 text-slate-400" aria-hidden="true" strokeWidth={2} />
      <span>Open in Viewer</span>
    </button>
    <button
      class="flex w-full items-center gap-2 rounded px-3 py-1.5 text-left text-xs text-slate-200 hover:bg-slate-800/80"
      type="button"
      on:click={emitCopyMarket}
      disabled={!market}
    >
      <Copy class="h-3.5 w-3.5 text-slate-400" aria-hidden="true" strokeWidth={2} />
      <span>Copy market key</span>
    </button>
    {#each futureActions as action}
      <button
        class="flex w-full cursor-not-allowed items-center gap-2 rounded px-3 py-1.5 text-left text-xs text-slate-500"
        type="button"
        disabled
      >
        <svelte:component
          this={action.icon}
          class="h-3.5 w-3.5 text-slate-600"
          aria-hidden="true"
          strokeWidth={2}
        />
        <span>{action.label}</span>
      </button>
    {/each}
    <button
      class="flex w-full cursor-not-allowed items-center gap-2 rounded px-3 py-1.5 text-left text-xs text-slate-500"
      type="button"
      disabled
    >
      <Trash2 class="h-3.5 w-3.5 text-slate-600" aria-hidden="true" strokeWidth={2} />
      <span>Delete outputs</span>
    </button>
  </div>
</Dropdown>
