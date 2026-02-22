<script lang="ts">
  import { get } from "svelte/store";
  import CandleChart from "../lib/features/viewer/CandleChart.svelte";
  import ViewerControls from "../lib/features/viewer/ViewerControls.svelte";
  import { currentRoute, navigate } from "../lib/framework/routing/routeStore.js";
  import { applyChartRouteToPrefs, buildAppRouteUrl, chartRouteFromPrefs, isChartRoute, type ChartRoute } from "../lib/framework/routing/routes.js";
  import { prefs, savePrefs } from "../lib/features/viewer/viewerStore.js";
  import type { Prefs } from "../lib/features/viewer/types.js";
  import { onDestroy } from "svelte";

  let route: ChartRoute = { kind: "chart" };
  let syncingFromRoute = false;

  const unsubRoute = currentRoute.subscribe((next) => {
    if (!isChartRoute(next)) return;
    route = next;
    const current = get(prefs);
    const merged = applyChartRouteToPrefs(current, next);
    if (prefsEqual(current, merged)) return;
    syncingFromRoute = true;
    savePrefs(merged);
    syncingFromRoute = false;
  });

  const unsubPrefs = prefs.subscribe((nextPrefs) => {
    if (syncingFromRoute) return;
    const desired = chartRouteFromPrefs(nextPrefs);
    const desiredUrl = buildAppRouteUrl(desired);
    const currentUrl = buildAppRouteUrl(route);
    if (desiredUrl === currentUrl) return;
    navigate(desired, { replace: true });
  });

  onDestroy(() => {
    unsubRoute();
    unsubPrefs();
  });

  function prefsEqual(a: Prefs, b: Prefs): boolean {
    return (
      a.collector === b.collector &&
      a.exchange === b.exchange &&
      a.symbol === b.symbol &&
      a.timeframe === b.timeframe &&
      a.start === b.start
    );
  }
</script>

<div class="flex h-full min-h-0 flex-col">
  <ViewerControls route={route} />
  <div class="min-h-0 flex-1">
    <CandleChart />
  </div>
</div>
