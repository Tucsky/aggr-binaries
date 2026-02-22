<script lang="ts">
  import { onMount } from "svelte";
  import Toasts from "./lib/framework/toast/Toasts.svelte";
  import { currentRoute, initRouteStore } from "./lib/framework/routing/routeStore.js";
  import ViewerPage from "./pages/ViewerPage.svelte";
  import TimelinePage from "./pages/TimelinePage.svelte";

  onMount(() => {
    initRouteStore();
  });
</script>

<main
  class="flex h-screen min-h-screen flex-col bg-slate-950 text-sm text-slate-100 overflow-hidden"
>
  <div class="flex-1 min-h-0">
    {#if $currentRoute.kind === "timeline"}
      <TimelinePage />
    {:else}
      <ViewerPage />
    {/if}
  </div>
  <Toasts />
</main>
