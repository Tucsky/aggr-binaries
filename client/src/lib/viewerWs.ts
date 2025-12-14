import type { ToastLevel } from "./toastStore.js";
import { addToast } from "./toastStore.js";
import type { Candle, Meta, Prefs } from "./types.js";
import { meta, status } from "./viewerStore.js";

type CandlesHandler = (fromIndex: number, candles: Candle[]) => void;

let ws: WebSocket | null = null;
let currentMeta: Meta | null = null;
let lastRequested = { fromIndex: -1, toIndex: -1 };
const subs = new Set<CandlesHandler>();
const pending = new Set<string>();

function notify(msg: string, level: ToastLevel = "info", durationMs = 1500) {
  addToast(msg, level, durationMs);
}

export function onCandles(cb: CandlesHandler): () => void {
  subs.add(cb);
  return () => subs.delete(cb);
}

export function disconnect(): void {
  currentMeta = null;
  lastRequested = { fromIndex: -1, toIndex: -1 };
  pending.clear();
  if (ws) {
    ws.onopen = ws.onclose = ws.onerror = ws.onmessage = null;
    ws.close();
    ws = null;
  }
  status.set("closed");
  meta.set(null);
  notify("Disconnected", "info", 1500);
}

export function connect(prefs: Prefs): void {
  disconnect();
  const startMs = prefs.start ? Date.parse(prefs.start) : null;
  const timeframe = prefs.timeframe?.trim() || "1m";
  const url = `ws://localhost:3000/ws?collector=${prefs.collector.trim().toUpperCase()}&exchange=${prefs.exchange
    .trim()
    .toUpperCase()}&symbol=${prefs.symbol.trim()}&timeframe=${timeframe}${startMs ? `&start=${startMs}` : ""}`;

  ws = new WebSocket(url);
  ws.onopen = () => {
    status.set("connected");
    notify(`Connected to ${prefs.collector}/${prefs.exchange}/${prefs.symbol} @ ${timeframe}`, "success", 2000);
  };
  ws.onclose = () => {
    status.set("closed");
    notify("Connection closed", "info", 1500);
  };
  ws.onerror = () => {
    status.set("error");
    notify("Connection error", "error", 2500);
  };
  ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    if (msg.type === "meta") {
      currentMeta = {
        startTs: msg.startTs,
        endTs: msg.endTs,
        timeframe: msg.timeframe ?? timeframe,
        priceScale: msg.priceScale,
        volumeScale: msg.volumeScale,
        timeframeMs: msg.timeframeMs ?? 60_000,
        sparse: Boolean(msg.sparse),
        records: msg.records ?? 0,
        anchorIndex: msg.anchorIndex ?? (msg.records > 0 ? msg.records - 1 : 0),
      };
      meta.set(currentMeta);
      notify(
        `Meta: ${currentMeta.records} records, ${currentMeta.sparse ? "sparse" : "dense"} @ ${currentMeta.timeframeMs / 1000}s`,
        "info",
        2500,
      );
    } else if (msg.type === "candles") {
      const key = `${msg.fromIndex}-${msg.toIndex}`;
      pending.delete(key);
      subs.forEach((cb) => cb(msg.fromIndex, msg.candles as Candle[]));
    }
  };
}

export function requestSlice(fromIndex: number, toIndex: number, reason?: string): void {
  if (!ws || ws.readyState !== WebSocket.OPEN || !currentMeta) return;
  const records = currentMeta.records ?? 0;
  if (!records) return;
  const from = Math.max(0, Math.min(records - 1, fromIndex));
  const to = Math.max(0, Math.min(records - 1, toIndex));
  if (from > to) return;
  const key = `${from}-${to}`;
  if (pending.has(key)) return;
  if (lastRequested.fromIndex === from && lastRequested.toIndex === to) return;
  pending.add(key);
  lastRequested = { fromIndex: from, toIndex: to };
  if (reason) notify(`Requesting slice ${from}-${to} (${reason})`, "info", 800);
  ws.send(JSON.stringify({ type: "slice", fromIndex: from, toIndex: to }));
}
