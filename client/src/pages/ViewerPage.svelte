<script lang="ts">
  import EmbeddedTimelineNavigator from "../lib/features/timeline/EmbeddedTimelineNavigator.svelte";
  import type { TimelineRange } from "../lib/features/timeline/timelineUtils.js";
  import CandleChart from "../lib/features/viewer/CandleChart.svelte";
  import ViewerControls from "../lib/features/viewer/ViewerControls.svelte";
  import { currentRoute, navigate } from "../lib/framework/routing/routeStore.js";
  import { isChartRoute, type ChartRoute } from "../lib/framework/routing/routes.js";
  import { setStart } from "../lib/features/viewer/viewerWs.js";
  import { onDestroy } from "svelte";

  let route: ChartRoute = { kind: "chart" };
  let chartVisibleRange: TimelineRange | null = null;

  const unsubRoute = currentRoute.subscribe((next) => {
    if (!isChartRoute(next)) return;
    route = next;
  });

  onDestroy(() => {
    unsubRoute();
  });

  function handleChartVisibleRangeChange(
    event: CustomEvent<TimelineRange | null>,
  ): void {
    chartVisibleRange = event.detail;
  }

  function handleNavigatorJump(event: CustomEvent<{ ts: number }>): void {
    setStart(event.detail.ts, { force: true });
    const nextRoute: ChartRoute = {
      kind: "chart",
      market: route.market,
      timeframe: route.timeframe,
      startTs: event.detail.ts,
    };
    navigate(nextRoute, { replace: true });
  }
</script>

<div class="flex h-full min-h-0 flex-col">
  <ViewerControls route={route} />
  <EmbeddedTimelineNavigator
    collector={route.market?.collector ?? ""}
    exchange={route.market?.exchange ?? ""}
    symbol={route.market?.symbol ?? ""}
    timeframe={route.timeframe ?? "1m"}
    {chartVisibleRange}
    on:jump={handleNavigatorJump}
  />
  <div class="min-h-0 flex-1">
    <CandleChart on:visibleRangeChange={handleChartVisibleRangeChange} />
  </div>
</div>
