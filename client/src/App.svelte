<script lang="ts">
  import CandleChart from "./lib/CandleChart.svelte";
  import Controls from "./lib/Controls.svelte";
  import Toasts from "./lib/Toasts.svelte";
  import { loadPrefs } from "./lib/viewerStore.js";

  loadPrefs();
</script>

<main
  class="flex h-screen min-h-screen flex-col bg-slate-950 text-sm text-slate-100 overflow-hidden"
>
  <Controls />
  <div class="flex-1 min-h-0">
    <CandleChart />
  </div>
  <Toasts />
</main>
