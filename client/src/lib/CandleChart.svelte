<script lang="ts">
  import {
      createChart,
      CrosshairMode,
      PriceScaleMode,
      type CandlestickData,
      type IChartApi,
      type ISeriesApi,
      type Time,
      type WhitespaceData,
  } from "lightweight-charts";
  import { onDestroy, onMount } from "svelte";
  import type { Candle, Meta } from "./types.js";
  import { meta as metaStore, status as statusStore } from "./viewerStore.js";
  import { onCandles, requestSlice } from "./viewerWs.js";

  let chartEl: HTMLDivElement;
  let chart: IChartApi | null = null;
  let series: ISeriesApi<"Candlestick"> | null = null;
  let baseIndex: number | null = null;
  let bars: Bar[] = [];
  let suppressRangeEvent = false;
  let currentMeta: Meta | null = null;
  let unsubCandles: (() => void) | null = null;
  let unsubMeta: (() => void) | null = null;
  let unsubStatus: (() => void) | null = null;
  let resizeObserver: ResizeObserver | null = null;

  type Bar = CandlestickData<Time> | WhitespaceData<Time>;

  onMount(() => {
    setupChart();
    unsubCandles = onCandles((fromIndex, candles) =>
      ingest(fromIndex, candles),
    );
    unsubMeta = metaStore.subscribe((m) => {
      currentMeta = m;
      reset();
      if (m) {
        const fromIdx = Math.max(0, m.anchorIndex - 500);
        requestSlice(fromIdx, m.anchorIndex, "initial load");
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
    resizeObserver?.disconnect();
    chart?.remove();
  });

  function setupChart() {
    chart = createChart(chartEl, {
      layout: { background: { color: "#0d1117" }, textColor: "#e6edf3" },
      grid: {
        vertLines: { color: "#161b22", visible: false },
        horzLines: { color: "#161b22", visible: false },
      },
      timeScale: {
        rightOffset: 1,
        barSpacing: 6,
        timeVisible: true,
        secondsVisible: false,
      },
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: { mode: PriceScaleMode.Logarithmic },
    });
    series = chart.addCandlestickSeries({
      upColor: "#3bca6d",
      downColor: "#d62828",
      wickUpColor: "#41f07b",
      wickDownColor: "#ff5253",
      borderVisible: false,
    });
    startResizeObserver();
    chart.timeScale().subscribeVisibleTimeRangeChange((range) => {
      if (
        suppressRangeEvent ||
        !range ||
        range.from == null ||
        range.to == null ||
        !currentMeta
      )
        return;
      if (bars.length === 0 || baseIndex === null) return;
      const minTime = Number(bars[0].time) * 1000;
      const maxTime = Number(bars[bars.length - 1].time) * 1000;
      const fromMs = Number(range.from) * 1000;
      const toMs = Number(range.to) * 1000;
      const margin = currentMeta.timeframeMs * 2;
      const minIdx = loadedMin();
      const maxIdx = loadedMax();
      if (fromMs < minTime + margin && minIdx !== null && minIdx > 0) {
        const nextFrom = Math.max(0, minIdx - 500);
        requestSlice(nextFrom, minIdx - 1, "scroll left");
      }
      if (
        toMs > maxTime - margin &&
        maxIdx !== null &&
        maxIdx < currentMeta.records - 1
      ) {
        const nextTo = Math.min(currentMeta.records - 1, maxIdx + 500);
        requestSlice(maxIdx + 1, nextTo, "scroll right");
        suppressRangeEvent = true;
      }
    });
  }

  function reset() {
    baseIndex = null;
    bars = [];
    suppressRangeEvent = false;
    series?.setData([]);
  }

  function ingest(fromIndex: number, candles: Candle[]) {
    if (!currentMeta) return;
    if (!candles.length) return;
    const slice = candles.map(toBar);
    const sliceLen = slice.length;
    const sliceMax = fromIndex + sliceLen - 1;

    if (baseIndex === null || bars.length === 0) {
      baseIndex = fromIndex;
      bars = slice;
      suppressRangeEvent = true;
      series?.setData(bars);
      suppressRangeEvent = false;
      return;
    }

    const min = loadedMin()!;
    const max = loadedMax()!;
    const isAppendRight = fromIndex === max + 1;
    const isPrependLeft = sliceMax === min - 1;

    if (isAppendRight) {
      const ts = chart?.timeScale();
      const prevPos = ts?.scrollPosition() ?? 0;

      for (const b of slice) {
        bars.push(b);
        series?.update(b);
      }

      if (prevPos > 0) {
        ts?.scrollToPosition(prevPos - sliceLen, false);
      }

      setTimeout(() => {
        suppressRangeEvent = false;
      }, 100)
      return;
    }

    if (isPrependLeft) {
      baseIndex = fromIndex;
      bars = slice.concat(bars);
      suppressRangeEvent = true;
      series?.setData(bars);
      suppressRangeEvent = false;
      return;
    }

    const newMin = Math.min(min, fromIndex);
    const newMax = Math.max(max, sliceMax);
    const merged: Array<Bar | undefined> = Array(newMax - newMin + 1);

    for (let i = 0; i < bars.length; i++) {
      merged[min - newMin + i] = bars[i];
    }
    for (let i = 0; i < sliceLen; i++) {
      merged[fromIndex - newMin + i] = slice[i];
    }

    let first = merged.findIndex(Boolean);
    let last = merged.length - 1;
    while (last >= 0 && !merged[last]) last -= 1;
    if (first === -1 || last < first) return;

    baseIndex = newMin + first;
    bars = merged.slice(first, last + 1).filter(Boolean) as Bar[];
    suppressRangeEvent = true;
    series?.setData(bars);
    suppressRangeEvent = false;
  }

  function toBar(c: Candle): Bar {
    const timeSec: Time = Math.floor(Number(c.time) / 1000) as Time;
    const gap =
      c.open === undefined ||
      c.high === undefined ||
      c.low === undefined ||
      c.close === undefined ||
      c.open === 0 ||
      c.high === 0 ||
      c.low === 0 ||
      c.close === 0;
    if (gap) return { time: timeSec };
    return {
      time: timeSec,
      open: c.open,
      high: c.high,
      low: c.low,
      close: c.close,
    };
  }

  function loadedMin(): number | null {
    return baseIndex;
  }

  function loadedMax(): number | null {
    if (baseIndex === null) return null;
    return baseIndex + bars.length - 1;
  }

  function startResizeObserver() {
    resizeObserver?.disconnect();
    resizeObserver = new ResizeObserver(() => {
      if (!chart) return;
      const { clientWidth, clientHeight } = chartEl;
      chart.resize(clientWidth, clientHeight);
    });
    resizeObserver.observe(chartEl);
    const { clientWidth, clientHeight } = chartEl;
    chart?.resize(clientWidth, clientHeight);
  }
</script>

<div bind:this={chartEl} class="h-full w-full"></div>
