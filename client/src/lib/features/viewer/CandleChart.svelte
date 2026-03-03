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
  import { createEventDispatcher, onDestroy, onMount } from "svelte";
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
  import { computeChartScaleMargins } from "../../../../../src/shared/chartScaleMargins.js";
  import {
    computeAnchoredVisibleRange,
    computeChartInitialSlice,
  } from "../../../../../src/shared/chartInitialSlice.js";
  import {
    findCandleAtOrBefore,
    formatLiquidationLegend,
    formatPriceLegend,
    formatVolumeLegend,
  } from "../../../../../src/shared/chartLegend.js";
  import { resolveChartCrosshairTarget } from "./candleChartCrosshair.js";
  import type { Candle, Meta } from "./types.js";
  import { meta as metaStore, status as statusStore } from "./viewerStore.js";
  import { onCandles, requestSlice } from "./viewerWs.js";

  // Intentionally kept in one file: chart subscriptions, incremental merges, and range backfill share mutable state.
  interface VisibleRange {
    startTs: number;
    endTs: number;
  }

  const dispatch = createEventDispatcher<{
    visibleRangeChange: VisibleRange | null;
    crosshairChange: number | null;
  }>();

  interface ChartCrosshairApi {
    setCrosshairPosition?: (
      price: number,
      time: Time,
      series: ISeriesApi<"Candlestick">,
    ) => void;
    clearCrosshairPosition?: () => void;
  }

  export let externalCrosshairTs: number | null = null;

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
  let emittedVisibleRange: VisibleRange | null = null;
  let emittedCrosshairTs: number | null = null;
  let lastAppliedExternalCrosshairTs: number | null | undefined = undefined;

  let unsubCandles: (() => void) | null = null;
  let unsubMeta: (() => void) | null = null;
  let unsubStatus: (() => void) | null = null;
  let resizeObserver: ResizeObserver | null = null;

  const legendCache: Record<"price" | "liq" | "volume", string> = {
    price: "",
    liq: "",
    volume: "",
  };

  $: if (externalCrosshairTs !== lastAppliedExternalCrosshairTs) {
    lastAppliedExternalCrosshairTs = externalCrosshairTs;
    syncExternalCrosshair(externalCrosshairTs);
  }
  $: if (chart && priceSeries && externalCrosshairTs !== null) {
    syncExternalCrosshair(externalCrosshairTs);
  }

  onMount(() => {
    setupChart();
    resetLegendValues();

    unsubCandles = onCandles((fromIndex, candles) => {
      ingest(fromIndex, candles);
    });

    unsubMeta = metaStore.subscribe((meta) => {
      currentMeta = meta;
      if (!meta) return;
      const logical = chart?.timeScale().getVisibleLogicalRange();
      const preservedSpan =
        logical && Number.isFinite(logical.to) && Number.isFinite(logical.from)
          ? logical.to - logical.from
          : null;
      reset();
      alignInitialRangeToAnchor = meta.anchorIndex < meta.records - 1;
      const initialWindow =
        preservedSpan !== null && Number.isFinite(preservedSpan)
          ? Math.max(500, Math.max(1, Math.round(preservedSpan)) + 1)
          : 500;
      const slice = computeChartInitialSlice(meta.anchorIndex, meta.records, initialWindow);
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
    chart?.timeScale().unsubscribeVisibleTimeRangeChange(handleVisibleRangeChange);
    emitCrosshairChange(null);
    emitVisibleRangeChange(null);
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
    emitVisibleRangeFromTimeRange(range);
    if (suppressRangeEvent || !range || range.from == null || range.to == null || !currentMeta) return;
    if (points.length === 0 || baseIndex === null) return;

    const minTime = points[0].time;
    const maxTime = points[points.length - 1].time;
    const fromMs = toVisibleTimeMs(range.from);
    const toMs = toVisibleTimeMs(range.to);
    if (fromMs === null || toMs === null) return;
    const margin = currentMeta.timeframeMs * 2;
    const minIdx = loadedMin();
    const maxIdx = loadedMax();

    if (fromMs < minTime + margin && minIdx !== null && minIdx > 0) {
      const requestFromIndex = Math.max(0, minIdx - 500);
      const requestToIndex = minIdx - 1;
      requestSlice(requestFromIndex, requestToIndex);
    }

    if (toMs > maxTime - margin && maxIdx !== null && maxIdx < currentMeta.records - 1) {
      const requestFromIndex = maxIdx + 1;
      const requestToIndex = Math.min(currentMeta.records - 1, maxIdx + 500);
      requestSlice(requestFromIndex, requestToIndex);
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
    emitCrosshairChange(null);
    emitVisibleRangeChange(null);
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
      if (alignInitialRangeToAnchor) {
        alignVisibleRangeToStart();
      }
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
      if (Number.isFinite(prevPos) && sliceLen > 0) {
        const nextPos = prevPos - sliceLen;
        ts?.scrollToPosition(nextPos, false);
      }

      setTimeout(() => {
        suppressRangeEvent = false;
      }, 100);
      return;
    }

    if (sliceMax === min - 1) {
      baseIndex = fromIndex;
      points = candles.concat(points);
      const wasSuppressed = suppressRangeEvent;
      suppressRangeEvent = true;
      setSeriesData(points);
      suppressRangeEvent = wasSuppressed;
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

    const wasSuppressed = suppressRangeEvent;
    suppressRangeEvent = true;
    setSeriesData(points);
    suppressRangeEvent = wasSuppressed;
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
    emitCrosshairChange(hoverTimeMs);
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

  function emitVisibleRangeFromTimeRange(range: { from: Time; to: Time } | null): void {
    if (!range || range.from == null || range.to == null) {
      emitVisibleRangeChange(null);
      return;
    }
    const fromMs = toVisibleTimeMs(range.from);
    const toMs = toVisibleTimeMs(range.to);
    if (fromMs === null || toMs === null) {
      emitVisibleRangeChange(null);
      return;
    }
    const startTs = Math.min(fromMs, toMs);
    const endTs = Math.max(fromMs, toMs);
    emitVisibleRangeChange({ startTs, endTs });
  }

  function emitVisibleRangeChange(next: VisibleRange | null): void {
    if (sameVisibleRange(emittedVisibleRange, next)) return;
    emittedVisibleRange = next;
    dispatch("visibleRangeChange", next);
  }

  function sameVisibleRange(a: VisibleRange | null, b: VisibleRange | null): boolean {
    if (a === b) return true;
    if (!a || !b) return false;
    return a.startTs === b.startTs && a.endTs === b.endTs;
  }

  function emitCrosshairChange(next: number | null): void {
    if (emittedCrosshairTs === next) return;
    emittedCrosshairTs = next;
    dispatch("crosshairChange", next);
  }

  function syncExternalCrosshair(nextTs: number | null): void {
    const chartApi = chart as (IChartApi & ChartCrosshairApi) | null;
    if (!chartApi || !priceSeries) return;
    if (nextTs === null) {
      chartApi.clearCrosshairPosition?.();
      return;
    }
    const target = resolveChartCrosshairTarget(points, nextTs);
    if (!target) {
      chartApi.clearCrosshairPosition?.();
      return;
    }
    hoverTimeMs = target.snappedTs;

    refreshLegendFromCurrentPoint();
    chartApi.setCrosshairPosition?.(
      target.price,
      target.timeSec as Time,
      priceSeries,
    );
  }

  function toVisibleTimeMs(value: Time): number | null {
    if (typeof value === "number" && Number.isFinite(value)) {
      return Math.floor(value * 1000);
    }
    if (typeof value !== "object" || value === null) return null;
    const rawYear = (value as { year?: unknown }).year;
    const rawMonth = (value as { month?: unknown }).month;
    const rawDay = (value as { day?: unknown }).day;
    if (!Number.isFinite(rawYear) || !Number.isFinite(rawMonth) || !Number.isFinite(rawDay)) return null;
    return Date.UTC(Number(rawYear), Number(rawMonth) - 1, Number(rawDay));
  }
</script>

<div class="relative h-full w-full">
  <div bind:this={chartEl} class="h-full w-full"></div>

  <div class="pointer-events-none absolute left-2 top-2 z-10 flex flex-col gap-1 text-xs leading-none pr-1">
    <button class="pointer-events-auto flex items-center gap-1 border-0 text-slate-200" class:opacity-45={!showLiquidationSeries} on:click={toggleLiquidationSeries} type="button">
      <span class="text-left py-1">Liquidations</span>
      <span bind:this={liqValueEl} class="absolute left-full text-nowrap pointer-events-none text-left bg-slate-950/80 hover:bg-slate-950/95 px-2 py-1 font-mono text-emerald-300">na</span>
    </button>

    <button class="pointer-events-auto flex items-center gap-1 border-0 text-slate-200" class:opacity-45={!showPriceSeries} on:click={togglePriceSeries} type="button">
      <span class="text-left py-1">Price</span>
      <span bind:this={priceValueEl} class="absolute left-full text-nowrap pointer-events-none text-left bg-slate-950/80 hover:bg-slate-950/95 px-2 py-1 font-mono text-emerald-300">na</span>
    </button>

    <button class="pointer-events-auto flex items-center gap-1 border-0 text-slate-200" class:opacity-45={!showVolumeSeries} on:click={toggleVolumeSeries} type="button">
      <span class="text-left py-1">Volume</span>
      <span bind:this={volumeValueEl} class="absolute left-full text-nowrap pointer-events-none text-left bg-slate-950/80 hover:bg-slate-950/95 px-2 py-1 font-mono text-emerald-300">na</span>
    </button>
  </div>
</div>
