<script lang="ts">
  import {
    CrosshairMode,
    PriceScaleMode,
    createChart,
    type IChartApi,
    type ISeriesApi,
    type MouseEventParams,
    type Time,
  } from "lightweight-charts";
  import { onDestroy, onMount } from "svelte";
  import {
    LONG_LIQ_COLOR,
    SHORT_LIQ_COLOR,
    buildSeriesData,
    buildSeriesUpdate,
    hasHistogramValue,
    toTimeMs,
    VOLUME_POSITIVE,
    VOLUME_POSITIVE_DIM,
  } from "./candleChartSeries.js";
  import { computeChartScaleMargins } from "../../../src/shared/chartScaleMargins.js";
  import {
    computeAnchoredVisibleRange,
    computeChartInitialSlice,
  } from "../../../src/shared/chartInitialSlice.js";
  import {
    findCandleAtOrBefore,
    formatLiquidationLegend,
    formatPriceLegend,
    formatVolumeLegend,
  } from "../../../src/shared/chartLegend.js";
  import type { Candle, Meta } from "./types.js";
  import { meta as metaStore, status as statusStore } from "./viewerStore.js";
  import { onCandles, requestSlice } from "./viewerWs.js";

  let chartEl: HTMLDivElement;
  let chart: IChartApi | null = null;
  let priceSeries: ISeriesApi<"Candlestick"> | null = null;
  let totalVolumeSeries: ISeriesApi<"Histogram"> | null = null;
  let deltaVolumeSeries: ISeriesApi<"Histogram"> | null = null;
  let longLiqSeries: ISeriesApi<"Histogram"> | null = null;
  let shortLiqSeries: ISeriesApi<"Histogram"> | null = null;

  let liqValueEl: HTMLSpanElement;
  let priceValueEl: HTMLSpanElement;
  let volumeValueEl: HTMLSpanElement;

  let baseIndex: number | null = null;
  let points: Candle[] = [];
  let showPriceSeries = true;
  let showLiquidationSeries = true;
  let showVolumeSeries = true;
  let suppressRangeEvent = false;
  let hoverTimeMs: number | null = null;
  let currentMeta: Meta | null = null;
  let alignInitialRangeToAnchor = false;

  let unsubCandles: (() => void) | null = null;
  let unsubMeta: (() => void) | null = null;
  let unsubStatus: (() => void) | null = null;
  let resizeObserver: ResizeObserver | null = null;

  const legendCache: Record<"price" | "liq" | "volume", string> = {
    price: "",
    liq: "",
    volume: "",
  };

  onMount(() => {
    setupChart();
    resetLegendValues();

    unsubCandles = onCandles((fromIndex, candles) => {
      ingest(fromIndex, candles);
    });

    unsubMeta = metaStore.subscribe((meta) => {
      currentMeta = meta;
      reset();
      if (!meta) return;
      alignInitialRangeToAnchor = meta.anchorIndex < meta.records - 1;
      const slice = computeChartInitialSlice(meta.anchorIndex, meta.records);
      if (!slice) return;
      requestSlice(slice.fromIndex, slice.toIndex);
    });

    unsubStatus = statusStore.subscribe((status) => {
      if (status !== "connected") reset();
    });
  });

  onDestroy(() => {
    unsubCandles?.();
    unsubMeta?.();
    unsubStatus?.();
    resizeObserver?.disconnect();
    chart?.unsubscribeCrosshairMove(handleCrosshairMove);
    chart?.remove();
  });

  function setupChart(): void {
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

    priceSeries = chart.addCandlestickSeries({
      upColor: "#3bca6d",
      downColor: "#d62828",
      wickUpColor: "#41f07b",
      wickDownColor: "#ff5253",
      borderVisible: false,
      priceLineVisible: false,
    });

    totalVolumeSeries = chart.addHistogramSeries({
      priceScaleId: "volume",
      color: VOLUME_POSITIVE_DIM,
      priceLineVisible: false,
      priceFormat: { type: "volume" },
      base: 0,
    });

    deltaVolumeSeries = chart.addHistogramSeries({
      priceScaleId: "volume",
      color: VOLUME_POSITIVE,
      priceLineVisible: false,
      priceFormat: { type: "volume" },
      base: 0,
    });

    longLiqSeries = chart.addHistogramSeries({
      priceScaleId: "liq",
      color: LONG_LIQ_COLOR,
      priceLineVisible: false,
      priceFormat: { type: "volume" },
      base: 0,
    });

    shortLiqSeries = chart.addHistogramSeries({
      priceScaleId: "liq",
      color: SHORT_LIQ_COLOR,
      priceLineVisible: false,
      priceFormat: { type: "volume" },
      base: 0,
    });

    applySeriesVisibility();
    applyScaleMargins();
    chart.subscribeCrosshairMove(handleCrosshairMove);
    chart.timeScale().subscribeVisibleTimeRangeChange(handleVisibleRangeChange);
    startResizeObserver();
  }

  function handleVisibleRangeChange(range: { from: Time; to: Time } | null): void {
    if (suppressRangeEvent || !range || range.from == null || range.to == null || !currentMeta) return;
    if (points.length === 0 || baseIndex === null) return;

    const minTime = points[0].time;
    const maxTime = points[points.length - 1].time;
    const fromMs = Number(range.from) * 1000;
    const toMs = Number(range.to) * 1000;
    const margin = currentMeta.timeframeMs * 2;
    const minIdx = loadedMin();
    const maxIdx = loadedMax();

    if (fromMs < minTime + margin && minIdx !== null && minIdx > 0) {
      requestSlice(Math.max(0, minIdx - 500), minIdx - 1);
    }

    if (toMs > maxTime - margin && maxIdx !== null && maxIdx < currentMeta.records - 1) {
      requestSlice(maxIdx + 1, Math.min(currentMeta.records - 1, maxIdx + 500));
      suppressRangeEvent = true;
    }
  }

  function reset(): void {
    baseIndex = null;
    points = [];
    hoverTimeMs = null;
    suppressRangeEvent = false;
    alignInitialRangeToAnchor = false;
    priceSeries?.setData([]);
    totalVolumeSeries?.setData([]);
    deltaVolumeSeries?.setData([]);
    longLiqSeries?.setData([]);
    shortLiqSeries?.setData([]);
    resetLegendValues();
  }

  function ingest(fromIndex: number, candles: Candle[]): void {
    if (!currentMeta || candles.length === 0) return;

    const sliceLen = candles.length;
    const sliceMax = fromIndex + sliceLen - 1;

    if (baseIndex === null || points.length === 0) {
      baseIndex = fromIndex;
      points = candles.slice();
      suppressRangeEvent = true;
      setSeriesData(points);
      if (alignInitialRangeToAnchor) alignVisibleRangeToStart();
      suppressRangeEvent = false;
      alignInitialRangeToAnchor = false;
      return;
    }

    const min = loadedMin()!;
    const max = loadedMax()!;

    if (fromIndex === max + 1) {
      const ts = chart?.timeScale();
      const prevPos = ts?.scrollPosition() ?? 0;

      for (let i = 0; i < sliceLen; i++) {
        const candle = candles[i];
        points.push(candle);
        updateSeries(candle);
      }

      refreshLegendFromCurrentPoint();
      if (prevPos > 0) ts?.scrollToPosition(prevPos - sliceLen, false);

      setTimeout(() => {
        suppressRangeEvent = false;
      }, 100);
      return;
    }

    if (sliceMax === min - 1) {
      baseIndex = fromIndex;
      points = candles.concat(points);
      suppressRangeEvent = true;
      setSeriesData(points);
      suppressRangeEvent = false;
      return;
    }

    const newMin = Math.min(min, fromIndex);
    const newMax = Math.max(max, sliceMax);
    const merged: Array<Candle | undefined> = Array(newMax - newMin + 1);

    for (let i = 0; i < points.length; i++) merged[min - newMin + i] = points[i];
    for (let i = 0; i < sliceLen; i++) merged[fromIndex - newMin + i] = candles[i];

    const first = merged.findIndex(Boolean);
    let last = merged.length - 1;
    while (last >= 0 && !merged[last]) last -= 1;
    if (first === -1 || last < first) return;

    baseIndex = newMin + first;
    points = merged.slice(first, last + 1).filter(Boolean) as Candle[];

    suppressRangeEvent = true;
    setSeriesData(points);
    suppressRangeEvent = false;
  }

  function setSeriesData(data: readonly Candle[]): void {
    const { priceData, totalVolumeData, deltaVolumeData, longLiqData, shortLiqData } =
      buildSeriesData(data);
    priceSeries?.setData(priceData);
    totalVolumeSeries?.setData(totalVolumeData);
    deltaVolumeSeries?.setData(deltaVolumeData);
    longLiqSeries?.setData(longLiqData);
    shortLiqSeries?.setData(shortLiqData);

    refreshLegendFromCurrentPoint();
  }

  function alignVisibleRangeToStart(): void {
    const ts = chart?.timeScale();
    if (!ts || points.length === 0) return;
    const logical = ts.getVisibleLogicalRange();
    const span =
      logical && Number.isFinite(logical.to) && Number.isFinite(logical.from)
        ? logical.to - logical.from
        : null;
    const range = computeAnchoredVisibleRange(points.length, span);
    if (!range) return;
    ts.setVisibleLogicalRange(range);
  }

  function updateSeries(candle: Candle): void {
    const { price, totalVolume, deltaVolume, longLiq, shortLiq } = buildSeriesUpdate(candle);
    priceSeries?.update(price);
    totalVolumeSeries?.update(totalVolume);
    deltaVolumeSeries?.update(deltaVolume);
    if (hasHistogramValue(longLiq)) longLiqSeries?.update(longLiq);
    if (hasHistogramValue(shortLiq)) shortLiqSeries?.update(shortLiq);
  }

  function togglePriceSeries(): void {
    showPriceSeries = !showPriceSeries;
    applySeriesVisibility();
    refreshLegendFromCurrentPoint();
  }

  function toggleLiquidationSeries(): void {
    showLiquidationSeries = !showLiquidationSeries;
    applySeriesVisibility();
    refreshLegendFromCurrentPoint();
  }

  function toggleVolumeSeries(): void {
    showVolumeSeries = !showVolumeSeries;
    applySeriesVisibility();
    refreshLegendFromCurrentPoint();
  }

  function applySeriesVisibility(): void {
    priceSeries?.applyOptions({ visible: showPriceSeries });
    totalVolumeSeries?.applyOptions({ visible: showVolumeSeries });
    deltaVolumeSeries?.applyOptions({ visible: showVolumeSeries });
    longLiqSeries?.applyOptions({ visible: showLiquidationSeries });
    shortLiqSeries?.applyOptions({ visible: showLiquidationSeries });
    applyScaleMargins();
  }

  function handleCrosshairMove(param: MouseEventParams<Time>): void {
    hoverTimeMs = toTimeMs(param.time);
    refreshLegendFromCurrentPoint();
  }

  function refreshLegendFromCurrentPoint(): void {
    const candle =
      hoverTimeMs === null
        ? points.length > 0
          ? points[points.length - 1]
          : null
        : findCandleAtOrBefore(points, hoverTimeMs);

    setLegendText("liq", showLiquidationSeries ? formatLiquidationLegend(candle) : "off", liqValueEl);
    setLegendText("price", showPriceSeries ? formatPriceLegend(candle) : "off", priceValueEl);
    setLegendText("volume", showVolumeSeries ? formatVolumeLegend(candle) : "off", volumeValueEl);
  }

  function resetLegendValues(): void {
    setLegendText("liq", "na", liqValueEl);
    setLegendText("price", "na", priceValueEl);
    setLegendText("volume", "na", volumeValueEl);
  }

  function setLegendText(
    key: "price" | "liq" | "volume",
    next: string,
    target?: HTMLSpanElement,
  ): void {
    if (!target || legendCache[key] === next) return;
    legendCache[key] = next;
    target.textContent = next;
  }

  function loadedMin(): number | null {
    return baseIndex;
  }

  function loadedMax(): number | null {
    if (baseIndex === null) return null;
    return baseIndex + points.length - 1;
  }

  function startResizeObserver(): void {
    resizeObserver?.disconnect();
    resizeObserver = new ResizeObserver(() => {
      if (!chart) return;
      chart.resize(chartEl.clientWidth, chartEl.clientHeight);
    });
    resizeObserver.observe(chartEl);
    chart?.resize(chartEl.clientWidth, chartEl.clientHeight);
  }

  function applyScaleMargins(): void {
    if (!chart) return;
    const margins = computeChartScaleMargins({
      price: showPriceSeries,
      liq: showLiquidationSeries,
      volume: showVolumeSeries,
    });
    chart.priceScale("right").applyOptions({ scaleMargins: margins.right });
    chart.priceScale("liq").applyOptions({ scaleMargins: margins.liq });
    chart.priceScale("volume").applyOptions({ scaleMargins: margins.volume });
  }
</script>

<div class="relative h-full w-full">
  <div bind:this={chartEl} class="h-full w-full"></div>

  <div class="pointer-events-none absolute left-2 top-2 z-10 flex flex-col gap-1 text-xs leading-none">
    <button class="pointer-events-auto flex items-center gap-1 border-0 text-slate-200" class:opacity-45={!showLiquidationSeries} on:click={toggleLiquidationSeries} type="button">
      <span class="min-w-[75px] text-left py-1">Liquidations</span>
      <span bind:this={liqValueEl} class="text-left bg-slate-950/80 hover:bg-slate-950/95 px-2 py-1 font-mono text-emerald-300">na</span>
    </button>

    <button class="pointer-events-auto flex items-center gap-1 border-0 text-slate-200" class:opacity-45={!showPriceSeries} on:click={togglePriceSeries} type="button">
      <span class="min-w-[75px] text-left py-1">Price</span>
      <span bind:this={priceValueEl} class="text-left bg-slate-950/80 hover:bg-slate-950/95 px-2 py-1 font-mono text-emerald-300">na</span>
    </button>

    <button class="pointer-events-auto flex items-center gap-1 border-0 text-slate-200" class:opacity-45={!showVolumeSeries} on:click={toggleVolumeSeries} type="button">
      <span class="min-w-[75px] text-left py-1">Volume</span>
      <span bind:this={volumeValueEl} class="text-left bg-slate-950/80 hover:bg-slate-950/95 px-2 py-1 font-mono text-emerald-300">na</span>
    </button>
  </div>
</div>
