<script lang="ts">
import CandleChart from "./lib/CandleChart.svelte";
import Controls from "./lib/Controls.svelte";
import type { Prefs } from "./lib/types.js";
import { meta, prefs, savePrefs, status } from "./lib/viewerStore.js";
import { createViewerWs } from "./lib/viewerWs.js";

const ws = createViewerWs({
  setStatus: (s) => status.set(s),
  setMeta: (m) => meta.set(m),
});

$: prefsVal = $prefs as Prefs;

function handleConnect(event: CustomEvent<{ start?: string }>) {
  if (event.detail?.start !== undefined) {
    savePrefs({ ...prefsVal, start: event.detail.start });
  } else {
    savePrefs(prefsVal);
  }
  ws.connect(prefsVal);
}

function handleDisconnect() {
  ws.disconnect();
}
</script>

<main class="w-full h-screen relative text-sm text-slate-100">
  <Controls on:connect={handleConnect} on:disconnect={handleDisconnect} />
  <CandleChart {ws} />
</main>
