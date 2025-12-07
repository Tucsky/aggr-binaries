<script lang="ts">
  import { createChart, type IChartApi, type ISeriesApi } from "lightweight-charts";
  import { onDestroy, onMount } from "svelte";
  import type { Candle, Meta } from "./types.js";
  import { meta as metaStore, status as statusStore } from "./viewerStore.js";
  import type { ViewerWs } from "./viewerWs.js";

  export let ws: ViewerWs;

  let chartEl: HTMLDivElement;
  let chart: IChartApi | null = null;
  let series: ISeriesApi<"Candlestick"> | null = null;
  let cache = new Map<number, Candle>();
  let loadedIndex = { min: null as number | null, max: null as number | null };
  let suppressRangeEvent = false;
  let currentMeta: Meta | null = null;
  let unsubCandles: (() => void) | null = null;
  let unsubMeta: (() => void) | null = null;
  let unsubStatus: (() => void) | null = null;

  onMount(() => {
    setupChart();
    unsubCandles = ws.onCandles((fromIndex, candles) => ingest(fromIndex, candles));
    unsubMeta = metaStore.subscribe((m) => {
      currentMeta = m;
      reset();
      if (m) {
        const fromIdx = Math.max(0, m.anchorIndex - 500);
        ws.requestSlice(fromIdx, m.anchorIndex);
      }
    });
    unsubStatus = statusStore.subscribe((s) => {
      if (s !== "connected") reset();
    });
  });

  onDestroy(() => {
    unsubCandles?.();
    unsubMeta?.();
    unsubStatus?.();
    chart?.remove();
  });

  function setupChart() {
    chart = createChart(chartEl, {
      layout: { background: { color: "#0d1117" }, textColor: "#e6edf3" },
      grid: { vertLines: { color: "#161b22" }, horzLines: { color: "#161b22" } },
      timeScale: { rightOffset: 1, barSpacing: 6, timeVisible: true, secondsVisible: false },
      crosshair: { mode: 1 },
      priceScale: { mode: 2 },
    });
    series = chart.addCandlestickSeries({
      upColor: "#16a34a",
      downColor: "#ef4444",
      wickUpColor: "#16a34a",
      wickDownColor: "#ef4444",
      borderVisible: false,
    });
    chart.timeScale().subscribeVisibleTimeRangeChange((range) => {
      if (suppressRangeEvent || !range || !range.from || !range.to || !currentMeta) return;
      if (loadedIndex.min === null || loadedIndex.max === null || cache.size === 0) return;
      const sorted = Array.from(cache.values()).sort((a, b) => a.time - b.time);
      const minTime = sorted[0].time * 1000;
      const maxTime = sorted[sorted.length - 1].time * 1000;
      const fromMs = Number(range.from) * 1000;
      const toMs = Number(range.to) * 1000;
      if (fromMs < minTime + currentMeta.timeframeMs) {
        const nextFrom = Math.max(0, loadedIndex.min - 500);
        ws.requestSlice(nextFrom, loadedIndex.min - 1);
      }
      if (toMs > maxTime - currentMeta.timeframeMs) {
        const nextTo = Math.min(currentMeta.records - 1, loadedIndex.max + 500);
        ws.requestSlice(loadedIndex.max + 1, nextTo);
      }
    });
  }

  function reset() {
    cache.clear();
    loadedIndex = { min: null, max: null };
    suppressRangeEvent = false;
    series?.setData([]);
  }

  function ingest(fromIndex: number, candles: Candle[]) {
    if (!currentMeta) return;
    let newMin = loadedIndex.min;
    let newMax = loadedIndex.max;
    candles.forEach((c, i) => {
      const idx = fromIndex + i;
      cache.set(idx, { ...c, index: idx });
      if (newMin === null || idx < newMin) newMin = idx;
      if (newMax === null || idx > newMax) newMax = idx;
    });
    loadedIndex = { min: newMin, max: newMax };
    const sorted = Array.from(cache.entries())
      .sort((a, b) => a[0] - b[0])
      .map(([, c]) => c);
    suppressRangeEvent = true;
    series?.setData(sorted.map((c) => ({ time: c.time, open: c.open, high: c.high, low: c.low, close: c.close })));
    suppressRangeEvent = false;
    if (sorted.length) {
      const last = sorted[sorted.length - 1];
      const first = sorted[Math.max(0, sorted.length - 500)];
      chart?.timeScale().setVisibleRange({ from: first.time, to: last.time });
    }
  }
</script>

<div bind:this={chartEl} class="absolute inset-0"></div>
