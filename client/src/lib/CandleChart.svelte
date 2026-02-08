<script lang="ts">
  import {
    CandlestickSeries,
    CrosshairMode,
    HistogramSeries,
    PriceScaleMode,
    createChart,
    type CandlestickData,
    type HistogramData,
    type IChartApi,
    type ISeriesApi,
    type Time,
    type WhitespaceData
  } from "lightweight-charts";
  import { onDestroy, onMount } from "svelte";
  import {
    VOLUME_POSITIVE_DIM,
    mapPreviewSeries,
    type PreviewSeriesPoint,
  } from "../../../src/shared/previewSeries.js";
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
  let baseIndex: number | null = null;
  let points: PreviewSeriesPoint[] = [];
  let hasLiquidationData = false;
  let suppressRangeEvent = false;
  let currentMeta: Meta | null = null;
  let unsubCandles: (() => void) | null = null;
  let unsubMeta: (() => void) | null = null;
  let unsubStatus: (() => void) | null = null;
  let resizeObserver: ResizeObserver | null = null;

  const PRICE_PANE_INDEX = 0;
  const VOLUME_PANE_INDEX = 1;
  const LIQUIDATION_PANE_INDEX = 2;
  const PRICE_PANE_WEIGHT = 65;
  const VOLUME_PANE_WEIGHT = 20;
  const LIQUIDATION_PANE_WEIGHT = 15;
  const HIDDEN_PANE_WEIGHT = 0.001;
  const LONG_LIQ_COLOR = "#ff8c00";
  const SHORT_LIQ_COLOR = "#b24dff";

  type PricePoint = CandlestickData<Time> | WhitespaceData<Time>;

  onMount(() => {
    setupChart();
    unsubCandles = onCandles((fromIndex, candles) =>
      ingest(fromIndex, candles),
    );
    unsubMeta = metaStore.subscribe((m) => {
      currentMeta = m;
      hasLiquidationData = m?.hasLiquidations ?? false;
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
      layout: {
        background: { color: "#0d1117" },
        textColor: "#e6edf3",
        panes: {
          separatorColor: "rgba(255, 255, 255, 0.10)",
          separatorHoverColor: "rgba(255, 255, 255, 0.12)",
        },
      },
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
    priceSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#3bca6d",
      downColor: "#d62828",
      wickUpColor: "#41f07b",
      wickDownColor: "#ff5253",
      borderVisible: false,
      priceLineVisible: false,
    }, PRICE_PANE_INDEX);
    totalVolumeSeries = chart.addSeries(HistogramSeries, {
      priceScaleId: "volume",
      color: VOLUME_POSITIVE_DIM,
      priceLineVisible: false,
      priceFormat: {
        type: 'volume',
      },
      base: 0,
    }, VOLUME_PANE_INDEX);
    deltaVolumeSeries = chart.addSeries(HistogramSeries, {
      priceScaleId: "volume",
      color: "#3bca6d",
      priceLineVisible: false,
      priceFormat: {
        type: 'volume',
      },
      base: 0,
    }, VOLUME_PANE_INDEX);
    longLiqSeries = chart.addSeries(HistogramSeries, {
      priceScaleId: "liq",
      color: LONG_LIQ_COLOR,
      priceLineVisible: false,
      priceFormat: {
        type: 'volume',
      },
      base: 0,
    }, LIQUIDATION_PANE_INDEX);
    shortLiqSeries = chart.addSeries(HistogramSeries, {
      priceScaleId: "liq",
      color: SHORT_LIQ_COLOR,
      priceLineVisible: false,
      priceFormat: {
        type: 'volume',
      },
      base: 0,
    }, LIQUIDATION_PANE_INDEX);
    chart.priceScale("volume", VOLUME_PANE_INDEX).applyOptions({
      scaleMargins: { top: 0.06, bottom: 0.04 },
    });
    chart.priceScale("liq", LIQUIDATION_PANE_INDEX).applyOptions({
      scaleMargins: { top: 0.1, bottom: 0.08 },
    });
    applyPaneLayout();
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
      if (points.length === 0 || baseIndex === null) return;
      const minTime = Number(points[0].time) * 1000;
      const maxTime = Number(points[points.length - 1].time) * 1000;
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
    points = [];
    suppressRangeEvent = false;
    setSeriesData([]);
    applyPaneLayout();
  }

  function ingest(fromIndex: number, candles: Candle[]) {
    if (!currentMeta) return;
    if (!candles.length) return;
    const slice = mapPreviewSeries(candles);
    const sliceLen = slice.length;
    const sliceMax = fromIndex + sliceLen - 1;

    if (baseIndex === null || points.length === 0) {
      baseIndex = fromIndex;
      points = slice;
      suppressRangeEvent = true;
      setSeriesData(points);
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

      for (const point of slice) {
        points.push(point);
        priceSeries?.update(point.price);
        totalVolumeSeries?.update(point.totalVolume);
        deltaVolumeSeries?.update(point.volumeDelta);
        longLiqSeries?.update(point.longLiquidation);
        shortLiqSeries?.update(point.shortLiquidation);
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
      points = slice.concat(points);
      suppressRangeEvent = true;
      setSeriesData(points);
      suppressRangeEvent = false;
      return;
    }

    const newMin = Math.min(min, fromIndex);
    const newMax = Math.max(max, sliceMax);
    const merged: Array<PreviewSeriesPoint | undefined> = Array(newMax - newMin + 1);

    for (let i = 0; i < points.length; i++) {
      merged[min - newMin + i] = points[i];
    }
    for (let i = 0; i < sliceLen; i++) {
      merged[fromIndex - newMin + i] = slice[i];
    }

    let first = merged.findIndex(Boolean);
    let last = merged.length - 1;
    while (last >= 0 && !merged[last]) last -= 1;
    if (first === -1 || last < first) return;

    baseIndex = newMin + first;
    points = merged.slice(first, last + 1).filter(Boolean) as PreviewSeriesPoint[];
    suppressRangeEvent = true;
    setSeriesData(points);
    suppressRangeEvent = false;
  }

  function setSeriesData(data: readonly PreviewSeriesPoint[]) {
    const len = data.length;
    const priceData: PricePoint[] = new Array(len);
    const totalVolumeData: HistogramData<Time>[] = new Array(len);
    const deltaVolumeData: HistogramData<Time>[] = new Array(len);
    const longLiqData: HistogramData<Time>[] = new Array(len);
    const shortLiqData: HistogramData<Time>[] = new Array(len);

    for (let i = 0; i < len; i += 1) {
      const point = data[i];
      priceData[i] = point.price;
      totalVolumeData[i] = point.totalVolume;
      deltaVolumeData[i] = point.volumeDelta;
      longLiqData[i] = point.longLiquidation;
      shortLiqData[i] = point.shortLiquidation;
    }

    priceSeries?.setData(priceData);
    totalVolumeSeries?.setData(totalVolumeData);
    deltaVolumeSeries?.setData(deltaVolumeData);
    longLiqSeries?.setData(longLiqData);
    shortLiqSeries?.setData(shortLiqData);
    applyPaneLayout();
  }

  function loadedMin(): number | null {
    return baseIndex;
  }

  function loadedMax(): number | null {
    if (baseIndex === null) return null;
    return baseIndex + points.length - 1;
  }

  function applyPaneLayout() {
    if (!chart) return;
    const panes = chart.panes();
    if (panes.length < 3) return;
    panes[PRICE_PANE_INDEX].setStretchFactor(PRICE_PANE_WEIGHT);
    panes[VOLUME_PANE_INDEX].setStretchFactor(VOLUME_PANE_WEIGHT);
    panes[LIQUIDATION_PANE_INDEX].setStretchFactor(
      hasLiquidationData ? LIQUIDATION_PANE_WEIGHT : HIDDEN_PANE_WEIGHT,
    );
    longLiqSeries?.applyOptions({ visible: hasLiquidationData });
    shortLiqSeries?.applyOptions({ visible: hasLiquidationData });
  }

  function startResizeObserver() {
    resizeObserver?.disconnect();
    resizeObserver = new ResizeObserver(() => {
      if (!chart) return;
      const { clientWidth, clientHeight } = chartEl;
      chart.resize(clientWidth, clientHeight);
      applyPaneLayout();
    });
    resizeObserver.observe(chartEl);
    const { clientWidth, clientHeight } = chartEl;
    chart?.resize(clientWidth, clientHeight);
    applyPaneLayout();
  }
</script>

<div bind:this={chartEl} class="h-full w-full"></div>
