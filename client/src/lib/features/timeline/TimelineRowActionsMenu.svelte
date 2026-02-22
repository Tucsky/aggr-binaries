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
  import { addToast } from "../../framework/toast/toastStore.js";
  import { runTimelineMarketAction } from "./timelineApi.js";
  import Dropdown from "../../framework/ui/Dropdown.svelte";
  import { TimelineMarketAction, type TimelineMarket } from "./timelineTypes.js";

  export let open = false;
  export let anchorEl: HTMLElement | null = null;
  export let market: TimelineMarket | null = null;

  const dispatch = createEventDispatcher<{
    close: void;
    openMarket: TimelineMarket;
    copyMarket: TimelineMarket;
    actionCompleted: { action: TimelineMarketAction; market: TimelineMarket };
  }>();

  const rowActions = [
    { label: "Index", icon: Binary, action: TimelineMarketAction.Index },
    { label: "Process", icon: Workflow, action: TimelineMarketAction.Process },
    { label: "Fix gaps", icon: Wrench, action: TimelineMarketAction.FixGaps },
    { label: "Rebuild", icon: RefreshCcw, action: TimelineMarketAction.Registry },
    { label: "Clear", icon: Trash2, action: TimelineMarketAction.Clear },
  ];
  let actionInFlight = false;

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

  async function runAction(action: TimelineMarketAction): Promise<void> {
    if (!market || actionInFlight) return;
    const target = market;
    const marketLabel = `${target.collector}/${target.exchange}/${target.symbol}`;
    actionInFlight = true;
    closeMenu();

    addToast(`Running ${formatActionLabel(action)} for ${marketLabel}...`, "info", 1600);
    try {
      const result = await runTimelineMarketAction({
        action,
        collector: target.collector,
        exchange: target.exchange,
        symbol: target.symbol,
        timeframe: target.timeframe,
      });
      const durationSeconds = (result.durationMs / 1000).toFixed(1);
      addToast(`${formatActionLabel(action)} completed for ${marketLabel} (${durationSeconds}s)`, "success", 2200);
      dispatch("actionCompleted", { action, market: target });
    } catch (err) {
      const message = err instanceof Error ? err.message : `Failed to run ${formatActionLabel(action)}`;
      addToast(message, "error", 3200);
    } finally {
      actionInFlight = false;
    }
  }

  function formatActionLabel(action: TimelineMarketAction): string {
    if (action === TimelineMarketAction.FixGaps) return "Fix gaps";
    if (action === TimelineMarketAction.Registry) return "Rebuild";
    if (action === TimelineMarketAction.Clear) return "Clear";
    if (action === TimelineMarketAction.Index) return "Index";
    return "Process";
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
    {#each rowActions as action}
      <button
        class="flex w-full items-center gap-2 rounded px-3 py-1.5 text-left text-xs disabled:cursor-not-allowed disabled:text-slate-500 enabled:text-slate-200 enabled:hover:bg-slate-800/80"
        type="button"
        disabled={!market || actionInFlight}
        on:click={() => void runAction(action.action)}
      >
        <svelte:component
          this={action.icon}
          class={`h-3.5 w-3.5 ${!market || actionInFlight ? "text-slate-600" : "text-slate-400"}`}
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
      <Download class="h-3.5 w-3.5 text-slate-600" aria-hidden="true" strokeWidth={2} />
      <span>Export timeframe</span>
    </button>
  </div>
</Dropdown>
