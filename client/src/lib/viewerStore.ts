import { writable, type Writable } from "svelte/store";
import type { Meta, Prefs, Status } from "./types.js";

const PREF_KEY = "aggr-viewer-prefs";

const defaultPrefs: Prefs = {
  collector: "PI",
  exchange: "BITFINEX",
  symbol: "BTCUSD",
  timeframe: "1m",
  start: "",
};

export const prefs: Writable<Prefs> = writable(loadPrefs());
export const status: Writable<Status> = writable("idle");
export const meta: Writable<Meta | null> = writable(null);
export const collapsed = writable(false);

export function savePrefs(value: Prefs): void {
  prefs.set(value);
  try {
    localStorage.setItem(PREF_KEY, JSON.stringify(value));
  } catch {
    // ignore
  }
}

export function loadPrefs(): Prefs {
  try {
    const raw = localStorage.getItem(PREF_KEY);
    if (!raw) return defaultPrefs;
    const parsed = JSON.parse(raw);
    return {
      collector: parsed.collector ?? defaultPrefs.collector,
      exchange: parsed.exchange ?? defaultPrefs.exchange,
      symbol: parsed.symbol ?? defaultPrefs.symbol,
      timeframe: parsed.timeframe ?? defaultPrefs.timeframe,
      start: parsed.start ?? defaultPrefs.start,
    };
  } catch {
    return defaultPrefs;
  }
}
