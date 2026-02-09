import type { ToastLevel } from "./toastStore.js";
import { addToast } from "./toastStore.js";
import type { Candle, Market, Meta, Prefs } from "./types.js";
import { markets as marketsStore, meta, status, setServerTimeframes } from "./viewerStore.js";

type CandlesHandler = (fromIndex: number, candles: Candle[]) => void;

let ws: WebSocket | null = null;
let currentMeta: Meta | null = null;
let currentTarget: Market | null = null;
let currentTimeframe = "1m";
let currentStart: number | null = null;
let lastRequested = { fromIndex: -1, toIndex: -1 };
let lastTimeframeRequestKey = "";
const subs = new Set<CandlesHandler>();
const pending = new Set<string>();
let queuedMessages: string[] = [];

function notify(msg: string, level: ToastLevel = "info", durationMs = 1500) {
  addToast(msg, level, durationMs);
}

function normalizeTarget(target: Market | Prefs): Market {
  return {
    collector: target.collector.trim().toUpperCase(),
    exchange: target.exchange.trim().toUpperCase(),
    symbol: target.symbol.trim(),
  };
}

function targetKey(target: Market | null): string {
  if (!target) return "";
  return `${target.collector}::${target.exchange}::${target.symbol}`;
}

function resetSlices(clearMeta = true): void {
  lastRequested = { fromIndex: -1, toIndex: -1 };
  pending.clear();
  if (clearMeta) {
    currentMeta = null;
    meta.set(null);
  }
}

function sendMessage(payload: unknown): void {
  const raw = JSON.stringify(payload);
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    queuedMessages.push(raw);
    return;
  }
  ws.send(raw);
}

function flushQueue(): void {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  while (queuedMessages.length) {
    const next = queuedMessages.shift();
    if (next) ws.send(next);
  }
}

export function onCandles(cb: CandlesHandler): () => void {
  subs.add(cb);
  return () => subs.delete(cb);
}

export function connect(initial?: Prefs): void {
  if (initial) {
    currentTarget = normalizeTarget(initial);
    currentTimeframe = initial.timeframe?.trim() || "1m";
    currentStart = initial.start ? Date.parse(initial.start) : null;
  }
  if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;

  ws = new WebSocket("ws://localhost:3000/ws");
  ws.onopen = () => {
    status.set("connected");
    notify("Connected to preview server", "success", 1800);
    flushQueue();
    requestMarkets();
  };
  ws.onclose = () => {
    status.set("closed");
    meta.set(null);
    currentMeta = null;
    pending.clear();
    queuedMessages = [];
    notify("Connection closed", "info", 1500);
    ws = null;
  };
  ws.onerror = () => {
    status.set("error");
    notify("Connection error", "error", 2500);
  };
  ws.onmessage = handleMessage;
}

export function reconnect(): void {
  disconnect(false);
  connect();
}

export function disconnect(showToast = true): void {
  resetSlices();
  queuedMessages = [];
  lastTimeframeRequestKey = "";
  if (ws) {
    ws.onopen = ws.onclose = ws.onerror = ws.onmessage = null;
    ws.close();
    ws = null;
  }
  status.set("closed");
  if (showToast) notify("Disconnected", "info", 1500);
}

export function setTarget(
  target: Market,
  options: { clearMeta?: boolean; force?: boolean; timeframe?: string; startMs?: number | null } = {},
): void {
  const { clearMeta = true, force = false, timeframe, startMs } = options;
  const normalized = normalizeTarget(target);
  if (!force && targetKey(normalized) === targetKey(currentTarget)) {
    const nextTf = timeframe?.trim();
    const hasTfChange = nextTf && nextTf !== currentTimeframe;
    const hasStartChange = startMs !== undefined && startMs !== currentStart;
    if (!hasTfChange && !hasStartChange) return;
  }
  currentTarget = normalized;
  if (timeframe?.trim()) {
    currentTimeframe = timeframe.trim();
  }
  if (startMs !== undefined) {
    currentStart = startMs;
  }
  if (clearMeta) {
    resetSlices();
    setServerTimeframes([]);
  }
  sendMessage({
    type: "setTarget",
    ...currentTarget,
    timeframe: timeframe?.trim(),
    startTs: startMs,
  });
  requestTimeframes(currentTarget);
}

export function setTimeframe(timeframe: string, options: { clearMeta?: boolean; force?: boolean } = {}): void {
  const { clearMeta = true, force = false } = options;
  const tf = timeframe.trim();
  if (!tf) return;
  if (!force && tf === currentTimeframe) return;
  currentTimeframe = tf;
  if (clearMeta) {
    resetSlices();
  }
  sendMessage({ type: "setTimeframe", timeframe: tf });
}

export function setStart(startMs: number | null, options: { clearMeta?: boolean; force?: boolean } = {}): void {
  const { clearMeta = true, force = false } = options;
  if (!force && startMs === currentStart) return;
  currentStart = startMs;
  if (clearMeta) {
    resetSlices();
  }
  sendMessage({ type: "setStart", startTs: startMs });
}

export function requestMarkets(): void {
  sendMessage({ type: "listMarkets" });
}

export function requestTimeframes(target?: Market | null): void {
  const tgt = target ?? currentTarget;
  if (!tgt) return;
  const key = targetKey(tgt);
  lastTimeframeRequestKey = key;
  setServerTimeframes([]);
  sendMessage({ type: "listTimeframes", ...tgt });
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
  sendMessage({ type: "slice", fromIndex: from, toIndex: to });
}

function handleMessage(ev: MessageEvent) {
  let msg: any;
  try {
    msg = JSON.parse(ev.data);
  } catch {
    return;
  }
  switch (msg.type) {
    case "meta":
      handleMeta(msg);
      break;
    case "candles":
      handleCandles(msg);
      break;
    case "markets":
      marketsStore.set((msg.markets as Market[]) ?? []);
      break;
    case "timeframes":
      handleTimeframes(msg);
      break;
    case "error":
      notify(msg.message ?? "Server error", "error", 2500);
      break;
    default:
      break;
  }
}

function handleMeta(msg: any): void {
  const newMeta: Meta = {
    startTs: msg.startTs,
    endTs: msg.endTs,
    timeframe: msg.timeframe ?? currentTimeframe,
    priceScale: msg.priceScale,
    volumeScale: msg.volumeScale,
    timeframeMs: msg.timeframeMs ?? 60_000,
    records: msg.records ?? 0,
    anchorIndex: msg.anchorIndex ?? (msg.records > 0 ? msg.records - 1 : 0),
  };
  const unchanged =
    currentMeta &&
    newMeta.startTs === currentMeta.startTs &&
    newMeta.endTs === currentMeta.endTs &&
    newMeta.timeframe === currentMeta.timeframe &&
    newMeta.records === currentMeta.records &&
    newMeta.anchorIndex === currentMeta.anchorIndex &&
    newMeta.timeframeMs === currentMeta.timeframeMs &&
    newMeta.priceScale === currentMeta.priceScale &&
    newMeta.volumeScale === currentMeta.volumeScale;
  if (unchanged) return;

  resetSlices(false);
  currentTimeframe = newMeta.timeframe;
  currentMeta = newMeta;
  meta.set(currentMeta);
  notify(
    `Meta: ${currentMeta.records} records @ ${currentMeta.timeframeMs / 1000}s`,
    "info",
    2500,
  );
}

function handleCandles(msg: any): void {
  const key = `${msg.fromIndex}-${msg.toIndex}`;
  pending.delete(key);
  subs.forEach((cb) => cb(msg.fromIndex, msg.candles as Candle[]));
}

function handleTimeframes(msg: any): void {
  const msgTarget: Market = {
    collector: (msg.collector ?? "").toString().toUpperCase(),
    exchange: (msg.exchange ?? "").toString().toUpperCase(),
    symbol: (msg.symbol ?? "").toString(),
  };
  const msgKey = targetKey(msgTarget);
  if (currentTarget && msgKey && msgKey !== targetKey(currentTarget)) return;
  if (lastTimeframeRequestKey && msgKey && msgKey !== lastTimeframeRequestKey) return;
  const received: string[] = Array.isArray(msg.timeframes) ? msg.timeframes : [];
  setServerTimeframes(received);
}
