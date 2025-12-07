<script lang="ts">
  import { createChart, UTCTimestamp, type CandlestickData, type IChartApi, type ISeriesApi } from "lightweight-charts";
  import { onMount } from "svelte";

  type Status = "idle" | "connected" | "closed" | "error";
  type Meta = {
    startTs: number;
    endTs: number;
    priceScale: number;
    volumeScale: number;
    timeframeMs: number;
    sparse: boolean;
    records: number;
    anchorIndex: number;
  };
  type Candle = CandlestickData & {
    buyVol: number;
    sellVol: number;
    buyCount: number;
    sellCount: number;
    liqBuy: number;
    liqSell: number;
    index: number;
  };

  const PREF_KEY = "aggr-viewer-prefs";

  let collector = "PI";
  let exchange = "BITFINEX";
  let symbol = "BTCUSD";
  let start = "";
  let status: Status = "idle";
  let ws: WebSocket | null = null;
  let meta: Meta | null = null;
  let timeframe = 60_000;
  let sparse = false;
  let records = 0;
  let anchorIndex: number | null = null;
  let lastRequested = { fromIndex: 0, toIndex: 0 };
  let loadedIndex = { min: null as number | null, max: null as number | null };
  let chart: IChartApi | null = null;
  let series: ISeriesApi<"Candlestick"> | null = null;
  let chartEl: HTMLDivElement;
  let cache = new Map<number, Candle>();
  let suppressRangeEvent = false;
  let initialViewSet = false;
  let collapsed = false;

  onMount(() => {
    loadPrefs();
    setupChart();
    return () => {
      teardownWs();
      chart?.remove();
    };
  });

  function loadPrefs() {
    try {
      const raw = localStorage.getItem(PREF_KEY);
      if (!raw) return;
      const prefs = JSON.parse(raw);
      collector = prefs.collector ?? collector;
      exchange = prefs.exchange ?? exchange;
      symbol = prefs.symbol ?? symbol;
      start = prefs.start ?? start;
    } catch {
      // ignore
    }
  }

  function savePrefs() {
    try {
      localStorage.setItem(
        PREF_KEY,
        JSON.stringify({
          collector,
          exchange,
          symbol,
          start,
        }),
      );
    } catch {
      // ignore
    }
  }

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
      if (suppressRangeEvent || !range || !range.from || !range.to) return;
      if (loadedIndex.min === null || loadedIndex.max === null || cache.size === 0) return;
      const sorted = Array.from(cache.values()).sort((a, b) => a.time - b.time);
      const minTime = sorted[0].time * 1000;
      const maxTime = sorted[sorted.length - 1].time * 1000;
      const fromMs = Number(range.from) * 1000;
      const toMs = Number(range.to) * 1000;
      if (fromMs < minTime + timeframe) {
        const nextFrom = Math.max(0, loadedIndex.min - 500);
        requestSlice(nextFrom, loadedIndex.min - 1);
      }
      if (toMs > maxTime - timeframe) {
        const nextTo = Math.min(records - 1, loadedIndex.max + 500);
        requestSlice(loadedIndex.max + 1, nextTo);
      }
    });
  }

  function setStatus(next: Status) {
    status = next;
  }

  function teardownWs() {
    if (ws) {
      ws.onopen = ws.onclose = ws.onerror = ws.onmessage = null;
      ws.close();
    }
    ws = null;
  }

  function toggleControls() {
    collapsed = !collapsed;
  }

  function connect() {
    if (ws && ws.readyState === WebSocket.OPEN) {
      teardownWs();
      resetState();
      setStatus("closed");
      return;
    }
    savePrefs();
    resetState();

    const startMs = start ? Date.parse(start) : null;
    const url = `ws://localhost:3000/ws?collector=${collector.trim().toUpperCase()}&exchange=${exchange
      .trim()
      .toUpperCase()}&symbol=${symbol.trim()}${startMs ? `&start=${startMs}` : ""}`;
    ws = new WebSocket(url);
    ws.onopen = () => setStatus("connected");
    ws.onclose = () => setStatus("closed");
    ws.onerror = () => setStatus("error");
    ws.onmessage = (ev) => {
      const msg = JSON.parse(ev.data);
      if (msg.type === "meta") {
        meta = {
          startTs: msg.startTs,
          endTs: msg.endTs,
          priceScale: msg.priceScale,
          volumeScale: msg.volumeScale,
          timeframeMs: msg.timeframeMs ?? 60_000,
          sparse: Boolean(msg.sparse),
          records: msg.records ?? 0,
          anchorIndex: msg.anchorIndex ?? (msg.records > 0 ? msg.records - 1 : 0),
        };
        timeframe = meta.timeframeMs;
        sparse = meta.sparse;
        records = meta.records;
        anchorIndex = meta.anchorIndex;
        initialViewSet = false;
        const fromIdx = Math.max(0, anchorIndex - 500);
        requestSlice(fromIdx, anchorIndex);
      } else if (msg.type === "candles") {
        ingest(msg.fromIndex, msg.candles);
      }
    };
  }

  function resetState() {
    cache = new Map();
    loadedIndex = { min: null, max: null };
    lastRequested = { fromIndex: 0, toIndex: 0 };
    meta = null;
    timeframe = 60_000;
    sparse = false;
    records = 0;
    anchorIndex = null;
    initialViewSet = false;
    suppressRangeEvent = false;
    series?.setData([]);
  }

  function requestSlice(fromIndex: number, toIndex: number) {
    if (!ws || ws.readyState !== WebSocket.OPEN || !meta) return;
    const from = Math.max(0, Math.min(records - 1, fromIndex));
    const to = Math.max(0, Math.min(records - 1, toIndex));
    if (from > to) return;
    if (lastRequested.fromIndex === from && lastRequested.toIndex === to) return;
    lastRequested = { fromIndex: from, toIndex: to };
    ws.send(JSON.stringify({ type: "slice", fromIndex: from, toIndex: to }));
  }

  function ingest(fromIndex: number, candles: any[]) {
    if (!meta) return;
    let newMin = loadedIndex.min;
    let newMax = loadedIndex.max;
    candles.forEach((c, i) => {
      const idx = fromIndex + i;
      const candle: Candle = {
        time: Math.floor(c.time / 1000) as UTCTimestamp,
        open: c.open,
        high: c.high,
        low: c.low,
        close: c.close,
        buyVol: c.buyVol,
        sellVol: c.sellVol,
        buyCount: c.buyCount,
        sellCount: c.sellCount,
        liqBuy: c.liqBuy,
        liqSell: c.liqSell,
        index: idx,
      };
      cache.set(idx, candle);
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
    if (!initialViewSet && sorted.length) {
      initialViewSet = true;
      const last = sorted[sorted.length - 1];
      const first = sorted[Math.max(0, sorted.length - 500)];
      chart?.timeScale().setVisibleRange({ from: first.time, to: last.time });
    }
  }
</script>

<main class="w-full h-screen relative text-sm text-slate-100">
  <button
    id="controls-toggle"
    class="absolute top-2 left-2 z-20 bg-slate-800/90 border border-slate-700 rounded px-3 py-1 text-xs"
    on:click={toggleControls}
  >
    {collapsed ? "Show controls" : "Hide controls"}
  </button>

  <div
    id="controls"
    class={`absolute top-2 left-2 z-10 bg-slate-900/90 border border-slate-700 rounded-md p-3 flex flex-col gap-2 text-xs ${
      collapsed ? "hidden" : "flex"
    }`}
  >
    <label class="flex items-center gap-2">
      <span class="w-20 text-slate-300">Collector</span>
      <input class="bg-slate-900 border border-slate-700 rounded px-2 py-1 text-slate-100" bind:value={collector} size="8" />
    </label>
    <label class="flex items-center gap-2">
      <span class="w-20 text-slate-300">Exchange</span>
      <input class="bg-slate-900 border border-slate-700 rounded px-2 py-1 text-slate-100" bind:value={exchange} size="12" />
    </label>
    <label class="flex items-center gap-2">
      <span class="w-20 text-slate-300">Symbol</span>
      <input class="bg-slate-900 border border-slate-700 rounded px-2 py-1 text-slate-100" bind:value={symbol} size="12" />
    </label>
    <label class="flex items-center gap-2">
      <span class="w-20 text-slate-300">Start (UTC)</span>
      <input class="bg-slate-900 border border-slate-700 rounded px-2 py-1 text-slate-100" type="datetime-local" bind:value={start} />
    </label>
    <button
      class="mt-1 bg-emerald-600 hover:bg-emerald-500 text-white font-semibold px-3 py-2 rounded"
      on:click={connect}
    >
      {status === "connected" ? "Disconnect" : "Connect"}
    </button>
    <div class="text-xs">
      Status:
      {#if status === "connected"}
        <span class="text-emerald-400">connected</span>
      {:else if status === "error"}
        <span class="text-red-400">error</span>
      {:else if status === "closed"}
        <span class="text-yellow-300">closed</span>
      {:else}
        <span class="text-slate-400">idle</span>
      {/if}
    </div>
    {#if meta}
      <div class="text-xs text-slate-300 space-y-1">
        <div>Timeframe: {timeframe / 1000}s {sparse ? "(sparse)" : "(dense)"}</div>
        <div>Records: {records}</div>
        <div>Start: {new Date(meta.startTs).toISOString()}</div>
        <div>End: {new Date(meta.endTs).toISOString()}</div>
      </div>
    {/if}
  </div>

  <div bind:this={chartEl} class="absolute inset-0"></div>
</main>
