<script lang="ts">
  import { createEventDispatcher } from "svelte";
  import Search from "lucide-svelte/icons/search";

  export let value = "";
  export let options: string[] = [];
  export let placeholder = "Type to search...";
  export let disabled = false;
  export let id = "";

  const dispatch = createEventDispatcher<{ change: string }>();
  const inputId = id || `auto-${Math.random().toString(36).slice(2)}`;
  const listId = `${inputId}-list`;

  function handleInput(event: Event) {
    const val = (event.target as HTMLInputElement).value;
    dispatch("change", val);
  }
</script>

<div class="relative bg-slate-900">
  <Search
    class="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-slate-500"
    aria-hidden="true"
    strokeWidth={2}
  />
  <input
    class="border-none bg-slate-900 py-1.5 pl-7 pr-2 outline-none text-sm text-slate-100"
    type="text"
    {id}
    {placeholder}
    list={listId}
    {disabled}
    {value}
    on:input={handleInput}
  />
</div>
<datalist id={listId}>
  {#each options as option}
    <option value={option} />
  {/each}
</datalist>
