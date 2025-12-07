import type { Candle, Meta, Prefs, Status } from "./types";

type CandlesHandler = (fromIndex: number, candles: Candle[]) => void;

export interface ViewerWs {
  connect: (prefs: Prefs) => void;
  disconnect: () => void;
  requestSlice: (fromIndex: number, toIndex: number) => void;
  onCandles: (cb: CandlesHandler) => () => void;
}

export function createViewerWs(opts: {
  setStatus: (s: Status) => void;
  setMeta: (m: Meta | null) => void;
}): ViewerWs {
  const { setStatus, setMeta } = opts;
  let ws: WebSocket | null = null;
  let currentMeta: Meta | null = null;
  let lastRequested = { fromIndex: -1, toIndex: -1 };
  const subs = new Set<CandlesHandler>();

  function disconnect() {
    if (ws) {
      ws.onopen = ws.onclose = ws.onerror = ws.onmessage = null;
      ws.close();
    }
    ws = null;
    currentMeta = null;
    lastRequested = { fromIndex: -1, toIndex: -1 };
    setStatus("closed");
    setMeta(null);
  }

  function connect(prefs: Prefs) {
    disconnect();
    const startMs = prefs.start ? Date.parse(prefs.start) : null;
    const url = `ws://localhost:3000/ws?collector=${prefs.collector.trim().toUpperCase()}&exchange=${prefs.exchange
      .trim()
      .toUpperCase()}&symbol=${prefs.symbol.trim()}${startMs ? `&start=${startMs}` : ""}`;
    ws = new WebSocket(url);
    ws.onopen = () => setStatus("connected");
    ws.onclose = () => setStatus("closed");
    ws.onerror = () => setStatus("error");
    ws.onmessage = (ev) => {
      const msg = JSON.parse(ev.data);
      if (msg.type === "meta") {
        currentMeta = {
          startTs: msg.startTs,
          endTs: msg.endTs,
          priceScale: msg.priceScale,
          volumeScale: msg.volumeScale,
          timeframeMs: msg.timeframeMs ?? 60_000,
          sparse: Boolean(msg.sparse),
          records: msg.records ?? 0,
          anchorIndex: msg.anchorIndex ?? (msg.records > 0 ? msg.records - 1 : 0),
        };
        setMeta(currentMeta);
      } else if (msg.type === "candles") {
        subs.forEach((cb) => cb(msg.fromIndex, msg.candles as Candle[]));
      }
    };
  }

  function requestSlice(fromIndex: number, toIndex: number) {
    if (!ws || ws.readyState !== WebSocket.OPEN || !currentMeta) return;
    const records = currentMeta.records ?? 0;
    if (!records) return;
    const from = Math.max(0, Math.min(records - 1, fromIndex));
    const to = Math.max(0, Math.min(records - 1, toIndex));
    if (from > to) return;
    if (lastRequested.fromIndex === from && lastRequested.toIndex === to) return;
    lastRequested = { fromIndex: from, toIndex: to };
    ws.send(JSON.stringify({ type: "slice", fromIndex: from, toIndex: to }));
  }

  function onCandles(cb: CandlesHandler) {
    subs.add(cb);
    return () => subs.delete(cb);
  }

  return { connect, disconnect, requestSlice, onCandles };
}
