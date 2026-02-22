import type { Prefs } from "../viewer/types.js";

const TIMELINE_STATE_STORAGE_KEY = "aggr.timeline.state.v1";

export interface TimelineSharedControls {
  collectorFilter: string;
  exchangeFilter: string;
  timeframeFilter: string;
}

export interface TimelineLocalState {
  symbolFilter: string;
  viewStartTs: number | null;
  viewEndTs: number | null;
}

export interface RestoredTimelineState extends TimelineLocalState {
  legacySharedControls: Partial<TimelineSharedControls> | null;
}

export function readSharedControlsFromPrefs(prefs: Prefs, fallbackTimeframe: string): TimelineSharedControls {
  const timeframe = prefs.timeframe.trim();
  return {
    collectorFilter: prefs.collector.trim().toUpperCase(),
    exchangeFilter: prefs.exchange.trim().toUpperCase(),
    timeframeFilter: timeframe || fallbackTimeframe,
  };
}

export function mergeSharedControlsIntoPrefs(current: Prefs, controls: TimelineSharedControls): Prefs | null {
  const collector = controls.collectorFilter.trim().toUpperCase();
  const exchange = controls.exchangeFilter.trim().toUpperCase();
  const timeframe = controls.timeframeFilter.trim() || current.timeframe || "1m";
  if (current.collector === collector && current.exchange === exchange && current.timeframe === timeframe) return null;
  return {
    ...current,
    collector,
    exchange,
    timeframe,
  };
}

export function restoreTimelineLocalState(storage: Storage): RestoredTimelineState {
  let symbolFilter = "";
  let viewStartTs: number | null = null;
  let viewEndTs: number | null = null;
  let legacySharedControls: Partial<TimelineSharedControls> | null = null;
  try {
    const raw = storage.getItem(TIMELINE_STATE_STORAGE_KEY);
    if (!raw) return { symbolFilter, viewStartTs, viewEndTs, legacySharedControls };
    const parsed = JSON.parse(raw) as Partial<{
      collectorFilter: string;
      exchangeFilter: string;
      timeframeFilter: string;
      symbolFilter: string;
      viewStartTs: number | null;
      viewEndTs: number | null;
    }>;
    if (typeof parsed.collectorFilter === "string") {
      legacySharedControls = legacySharedControls ?? {};
      legacySharedControls.collectorFilter = parsed.collectorFilter.trim().toUpperCase();
    }
    if (typeof parsed.exchangeFilter === "string") {
      legacySharedControls = legacySharedControls ?? {};
      legacySharedControls.exchangeFilter = parsed.exchangeFilter.trim().toUpperCase();
    }
    if (typeof parsed.timeframeFilter === "string" && parsed.timeframeFilter.length) {
      legacySharedControls = legacySharedControls ?? {};
      legacySharedControls.timeframeFilter = parsed.timeframeFilter;
    }
    symbolFilter = typeof parsed.symbolFilter === "string" ? parsed.symbolFilter : "";
    viewStartTs = Number.isFinite(parsed.viewStartTs) ? Number(parsed.viewStartTs) : null;
    viewEndTs = Number.isFinite(parsed.viewEndTs) ? Number(parsed.viewEndTs) : null;
  } catch {
    // ignore malformed state
  }
  return { symbolFilter, viewStartTs, viewEndTs, legacySharedControls };
}

export function persistTimelineLocalState(storage: Storage, state: TimelineLocalState): void {
  try {
    storage.setItem(TIMELINE_STATE_STORAGE_KEY, JSON.stringify(state));
  } catch {
    // ignore storage failures
  }
}
