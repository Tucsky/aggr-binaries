<script lang="ts">
  import { createEventDispatcher, tick } from "svelte";
  import { get } from "svelte/store";
  import ChevronDown from "lucide-svelte/icons/chevron-down";
  import Clock3 from "lucide-svelte/icons/clock-3";
  import Pencil from "lucide-svelte/icons/pencil";
  import Trash2 from "lucide-svelte/icons/trash-2";
  import X from "lucide-svelte/icons/x";
  import { parseTimeframeMs } from "../../../../../src/shared/timeframes.js";
  import Dropdown from "../../framework/ui/Dropdown.svelte";
  import {
    filterTimeframesByQuery,
    type TimeframeEntry,
  } from "./timeframeDropdownUtils.js";
  import {
      addTimeframe,
      removeTimeframe,
      serverTimeframes,
      timeframes,
  } from "./viewerStore.js";

  export let currentValue = "";

  const dispatch = createEventDispatcher<{
    select: string;
  }>();

  let open = false;
  let anchorEl: HTMLElement | null = null;
  let inputEl: HTMLInputElement | null = null;

  let input = "";
  let editing = false;

  const groups = [
    { title: "Seconds", weight: 0, min: 0, max: 60_000 },
    { title: "Minutes", weight: 1, min: 60_000, max: 3_600_000 },
    { title: "Hours", weight: 2, min: 3_600_000, max: 86_400_000 },
    {
      title: "Days",
      weight: 3,
      min: 86_400_000,
      max: Number.POSITIVE_INFINITY,
    },
  ];

  $: normalized = normalizeTimeframes($timeframes);
  $: filtered = filterTimeframesByQuery(normalized, input);
  $: grouped = buildGroups(filtered);
  $: inputMs = parseTimeframeMs(input.trim());
  $: removable = new Set($timeframes);
  $: serverSet = new Set($serverTimeframes);
  $: if (open) {
    void focusInputOnOpen();
  }

  function toggle() {
    open = !open;
  }

  function close() {
    open = false;
    editing = false;
  }

  function normalizeTimeframes(list: string[]): TimeframeEntry[] {
    return Array.from(new Set(list))
      .map((tf) => ({ value: tf, ms: parseTimeframeMs(tf) }))
      .filter((t): t is { value: string; ms: number } => Boolean(t.ms))
      .sort((a, b) => a.ms - b.ms || a.value.localeCompare(b.value));
  }

  function buildGroups(list: TimeframeEntry[]) {
    return groups
      .map((g) => ({
        ...g,
        items: list.filter((t) => t.ms >= g.min && t.ms < g.max),
      }))
      .filter((g) => g.items.length > 0);
  }

  async function focusInputOnOpen() {
    await tick();
    if (!open || !inputEl) return;
    inputEl.focus();
  }

  function select(tf: string) {
    const trimmed = tf.trim();
    if (!trimmed) return;
    dispatch("select", trimmed);
    close();
  }

  function handleSubmit() {
    const tf = input.trim();
    if (!parseTimeframeMs(tf)) return;
    addTimeframe(tf);
    select(tf);
    input = "";
  }

  function toggleEdit() {
    editing = !editing;
  }

  function remove(tf: string) {
    if (!removable.has(tf)) return;
    removeTimeframe(tf);
    if (tf === currentValue) {
      const next = get(timeframes)[0] ?? "";
      if (next) dispatch("select", next);
    }
  }
</script>

<button
  class="relative flex items-center gap-1.5 border-l border-slate-800 bg-transparent py-1.5 pl-7 pr-2 text-xs text-slate-100 hover:bg-slate-800/20 focus-visible:outline focus-visible:outline-2 focus-visible:outline-slate-600"
  on:click={toggle}
  type="button"
  bind:this={anchorEl}
>
  <Clock3
    class="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-slate-500"
    aria-hidden="true"
    strokeWidth={1.9}
  />
  <span>{currentValue || "Select TF"}</span>
  <ChevronDown class="h-3.5 w-3.5 text-slate-500" aria-hidden="true" strokeWidth={2} />
</button>

<Dropdown {open} {anchorEl} on:close={close} margin={8}>
  <div class="w-32">
    <div class="sticky top-0">
      <div
        class="backdrop-blur-sm flex items-center gap-2 bg-slate-900/80 px-1 py-1"
      >
        <input
          class="min-w-px flex-1 bg-transparent text-xs text-slate-100 outline-none placeholder:text-slate-500"
          placeholder="Search"
          bind:this={inputEl}
          bind:value={input}
          on:keydown={(e) => e.key === "Enter" && handleSubmit()}
        />
        {#if inputMs}
          <button
            class="text-[11px] text-emerald-300 hover:text-emerald-200"
            on:click={handleSubmit}
            type="button"
          >
            Add
          </button>
        {:else}
          <button
            class="text-slate-400 hover:text-slate-200"
            type="button"
            on:click={toggleEdit}
            aria-label="Toggle edit"
          >
            {#if editing}
              <X class="h-3.5 w-3.5" aria-hidden="true" strokeWidth={2.2} />
            {:else}
              <Pencil class="h-3.5 w-3.5" aria-hidden="true" strokeWidth={2} />
            {/if}
          </button>
        {/if}
      </div>
    </div>

    <div class="px-1 pb-1">
      {#each grouped as group}
        <div
          class="mb-1 mt-2 flex items-center justify-between px-3 text-[10px] font-mono uppercase tracking-[0.08em] text-slate-400"
        >
          <span>{group.title}</span>
        </div>
        {#each group.items as tf}
          <button
            type="button"
            class={`flex w-full items-center justify-between rounded px-3 py-1.5 text-left text-xs  ${
              tf.value === currentValue
                ? "font-bold underline decoration-amber-300"
                : "text-slate-200 hover:bg-slate-800/80"
            }`}
            on:click={() => select(tf.value)}
          >
            <span
              class={`font-mono ${serverSet.has(tf.value) ? "text-amber-300" : ""}`}
              >{tf.value}</span
            >
            {#if editing && removable.has(tf.value)}
              <button
                type="button"
                class="text-xs text-rose-300 hover:text-rose-200"
                on:click|stopPropagation={() => remove(tf.value)}
                aria-label="Remove timeframe"
              >
                <Trash2 class="h-3.5 w-3.5" aria-hidden="true" strokeWidth={2} />
              </button>
            {/if}
          </button>
        {/each}
      {/each}
    </div>
  </div>
</Dropdown>
