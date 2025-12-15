<script lang="ts">
  import { createEventDispatcher } from "svelte";
  import { parseTimeframeMs } from "../../../src/shared/timeframes.js";
  import Dropdown from "./Dropdown.svelte";

  export let open = false;
  export let anchorEl: HTMLElement | null = null;
  export let timeframes: string[] = [];
  export let value = "";
  export let removable: Set<string> = new Set();
  export let serverSet: Set<string> = new Set();

  const dispatch = createEventDispatcher<{
    select: string;
    close: void;
    remove: string;
    add: string;
  }>();

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

  $: normalized = normalizeTimeframes(timeframes);
  $: grouped = buildGroups(normalized);
  $: inputMs = parseTimeframeMs(input.trim());

  function normalizeTimeframes(list: string[]) {
    return Array.from(new Set(list))
      .map((tf) => ({ value: tf, ms: parseTimeframeMs(tf) }))
      .filter((t): t is { value: string; ms: number } => Boolean(t.ms))
      .sort((a, b) => a.ms - b.ms || a.value.localeCompare(b.value));
  }

  function buildGroups(list: { value: string; ms: number }[]) {
    return groups
      .map((g) => ({
        ...g,
        items: list.filter((t) => t.ms >= g.min && t.ms < g.max),
      }))
      .filter((g) => g.items.length > 0);
  }

  function select(tf: string) {
    dispatch("select", tf);
    dispatch("close");
  }

  function handleSubmit() {
    const tf = input.trim();
    if (!parseTimeframeMs(tf)) return;
    dispatch("add", tf);
    select(tf);
    input = "";
  }

  function toggleEdit() {
    editing = !editing;
  }

  function remove(tf: string) {
    if (!removable.has(tf)) return;
    dispatch("remove", tf);
  }
</script>

<Dropdown {open} {anchorEl} on:close={() => dispatch("close")} margin={0}>
  <div class="w-32">
    <div class="p-2 sticky top-0">
      <div
        class="backdrop-blur-sm flex items-center gap-2 bg-slate-800/80 border border-slate-700 rounded px-2 py-1"
      >
        <input
          class="flex-1 min-w-px bg-transparent outline-none text-slate-100 placeholder:text-slate-500 text-sm"
          placeholder="ex 1m"
          bind:value={input}
          on:keydown={(e) => e.key === "Enter" && handleSubmit()}
        />
        {#if inputMs}
          <button
            class="text-xs text-emerald-300 hover:text-emerald-200"
            on:click={handleSubmit}
            type="button"
          >
            Add
          </button>
        {:else}
          <button
            class="text-xs text-slate-400 hover:text-slate-200"
            type="button"
            on:click={toggleEdit}
            aria-label="Toggle edit"
          >
            {#if editing}
              ✕
            {:else}
              ✎
            {/if}
          </button>
        {/if}
      </div>
    </div>

    <div class="p-2 pt-0">
      {#each grouped as group}
        <div
          class="flex items-center justify-between text-[10px] uppercase tracking-[0.08em] text-slate-400 font-semibold mt-2 mb-1"
        >
          <span>{group.title}</span>
        </div>
        <div class="flex flex-col gap-px">
          {#each group.items as tf}
            <button
              type="button"
              class={`text-left px-3 py-2 rounded hover:bg-slate-800/80 text-sm flex items-center justify-between ${
                tf.value === value
                  ? "bg-slate-800/80 text-slate-100"
                  : "text-slate-200"
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
                  🗑
                </button>
              {/if}
            </button>
          {/each}
        </div>
      {/each}
    </div>
  </div>
</Dropdown>
