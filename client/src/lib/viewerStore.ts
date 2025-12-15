import { get, writable, type Writable } from "svelte/store";
import { COMMON_TIMEFRAMES, sortTimeframes } from "../../../src/shared/timeframes.js";
import type { Market, Meta, Prefs, Status } from "./types.js";

const PREF_KEY = "aggr-viewer-prefs";

const defaultPrefs: Prefs = {
  collector: "PI",
  exchange: "BITFINEX",
  symbol: "BTCUSD",
  timeframe: "1m",
  timeframes: [...COMMON_TIMEFRAMES],
  start: "",
};

const initialPrefs = loadPrefs();

export const prefs: Writable<Prefs> = writable(initialPrefs);
export const status: Writable<Status> = writable("idle");
export const meta: Writable<Meta | null> = writable(null);
export const collapsed = writable(false);
export const markets: Writable<Market[]> = writable([]);
export const timeframes: Writable<string[]> = writable([...initialPrefs.timeframes]);
export const serverTimeframes: Writable<string[]> = writable([]);

export function savePrefs(value: Prefs): void {
  const sanitized = sanitizePrefs(value);
  prefs.set(sanitized);
  timeframes.set([...sanitized.timeframes]);
  try {
    localStorage.setItem(PREF_KEY, JSON.stringify(sanitized));
  } catch {
    // ignore
  }
}

export function loadPrefs(): Prefs {
  try {
    const raw = localStorage.getItem(PREF_KEY);
    if (!raw) return defaultPrefs;
    const parsed = JSON.parse(raw);
    return sanitizePrefs(parsed);
  } catch {
    return defaultPrefs;
  }
}

export function addTimeframe(tf: string): void {
  if (!tf) return;
  updatePrefs((p) => {
    const list = sortTimeframes([...p.timeframes, tf]);
    const safeTimeframe = list.includes(p.timeframe) ? p.timeframe : list[0] ?? p.timeframe;
    return { ...p, timeframes: list, timeframe: safeTimeframe };
  });
}

export function removeTimeframe(tf: string): void {
  updatePrefs((p) => {
    const filtered = p.timeframes.filter((v) => v !== tf);
    const list = filtered.length ? filtered : [...COMMON_TIMEFRAMES];
    const safeTimeframe = list.includes(p.timeframe) ? p.timeframe : list[0] ?? p.timeframe;
    return { ...p, timeframes: list, timeframe: safeTimeframe };
  });
}

export function setServerTimeframes(values: string[]): void {
  serverTimeframes.set(values ?? []);
}

function updatePrefs(fn: (p: Prefs) => Prefs): void {
  const current = get(prefs);
  const next = sanitizePrefs(fn(current));
  savePrefs(next);
}

function sanitizePrefs(input: Partial<Prefs>): Prefs {
  const tfList =
    Array.isArray(input.timeframes) && input.timeframes.length
      ? sortTimeframes(input.timeframes.filter((v): v is string => typeof v === "string"))
      : [...COMMON_TIMEFRAMES];
  const timeframe = (input.timeframe ?? tfList[0] ?? defaultPrefs.timeframe).toString();
  return {
    collector: input.collector ?? defaultPrefs.collector,
    exchange: input.exchange ?? defaultPrefs.exchange,
    symbol: input.symbol ?? defaultPrefs.symbol,
    timeframe,
    timeframes: tfList,
    start: input.start ?? defaultPrefs.start,
  };
}
