<script lang="ts">
  import toasts from "./toastStore.js";
</script>

<div class="fixed bottom-4 left-4 z-30 flex flex-col gap-2">
  {#each $toasts as toast (toast.id)}
    <div
      class={`px-3 py-2 rounded shadow text-sm border
        ${toast.level === "error" ? "bg-red-900/90 border-red-600 text-red-100" : ""}
        ${toast.level === "success" ? "bg-emerald-900/90 border-emerald-600 text-emerald-100" : ""}
        ${toast.level === "info" ? "bg-slate-800/90 border-slate-600 text-slate-100" : ""}`}
    >
      {toast.message}
    </div>
  {/each}
</div>
