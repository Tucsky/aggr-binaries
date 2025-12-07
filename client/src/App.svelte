<script lang="ts">
  import CandleChart from "./lib/CandleChart.svelte";
  import Controls from "./lib/Controls.svelte";
  import Toasts from "./lib/Toasts.svelte";
  import { loadPrefs } from "./lib/viewerStore.js";

  loadPrefs();
</script>

<main class="w-full h-screen relative text-sm text-slate-100">
  <Controls />
  <CandleChart />
  <Toasts />
</main>
