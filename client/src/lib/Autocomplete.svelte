<script lang="ts">
  import { createEventDispatcher } from "svelte";

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

<input
  class="border-none px-2 py-1.5 outline-none bg-slate-900 text-sm text-slate-100"
  type="text"
  {id}
  {placeholder}
  list={listId}
  {disabled}
  {value}
  on:input={handleInput}
/>
<datalist id={listId}>
  {#each options as option}
    <option value={option} />
  {/each}
</datalist>
