import assert from "node:assert/strict";
import { test } from "node:test";
import {
  mergeSharedControlsIntoPrefs,
  persistTimelineLocalState,
  readSharedControlsFromPrefs,
  restoreTimelineLocalState,
} from "../../client/src/lib/features/timeline/timelineControlsPersistence.js";
import type { Prefs } from "../../client/src/lib/features/viewer/types.js";

function createMemoryStorage(seed: Record<string, string> = {}): Storage {
  const map = new Map(Object.entries(seed));
  return {
    get length() {
      return map.size;
    },
    clear(): void {
      map.clear();
    },
    getItem(key: string): string | null {
      return map.has(key) ? map.get(key) ?? null : null;
    },
    key(index: number): string | null {
      return Array.from(map.keys())[index] ?? null;
    },
    removeItem(key: string): void {
      map.delete(key);
    },
    setItem(key: string, value: string): void {
      map.set(key, value);
    },
  };
}

test("readSharedControlsFromPrefs normalizes collector/exchange and keeps timeframe fallback", () => {
  const prefs: Prefs = {
    collector: "pi",
    exchange: "bitfinex",
    symbol: "BTCUSD",
    timeframe: "",
    timeframes: ["1m"],
    start: "",
  };
  assert.deepStrictEqual(readSharedControlsFromPrefs(prefs, "1m"), {
    collectorFilter: "PI",
    exchangeFilter: "BITFINEX",
    timeframeFilter: "1m",
  });
});

test("mergeSharedControlsIntoPrefs returns null when no persisted values change", () => {
  const prefs: Prefs = {
    collector: "PI",
    exchange: "BITFINEX",
    symbol: "BTCUSD",
    timeframe: "1m",
    timeframes: ["1m"],
    start: "",
  };
  const merged = mergeSharedControlsIntoPrefs(prefs, {
    collectorFilter: "pi",
    exchangeFilter: "bitfinex",
    timeframeFilter: "1m",
  });
  assert.strictEqual(merged, null);
});

test("restoreTimelineLocalState restores timeline-local values and legacy shared controls", () => {
  const storage = createMemoryStorage({
    "aggr.timeline.state.v1": JSON.stringify({
      collectorFilter: "pi",
      exchangeFilter: "bitfinex",
      timeframeFilter: "5m",
      symbolFilter: "btc",
      viewStartTs: 100,
      viewEndTs: 200,
    }),
  });
  const restored = restoreTimelineLocalState(storage);
  assert.strictEqual(restored.symbolFilter, "btc");
  assert.strictEqual(restored.viewStartTs, 100);
  assert.strictEqual(restored.viewEndTs, 200);
  assert.deepStrictEqual(restored.legacySharedControls, {
    collectorFilter: "PI",
    exchangeFilter: "BITFINEX",
    timeframeFilter: "5m",
  });
});

test("persistTimelineLocalState only writes timeline-local payload", () => {
  const storage = createMemoryStorage();
  persistTimelineLocalState(storage, {
    symbolFilter: "eth",
    viewStartTs: 123,
    viewEndTs: 456,
  });
  assert.strictEqual(
    storage.getItem("aggr.timeline.state.v1"),
    JSON.stringify({
      symbolFilter: "eth",
      viewStartTs: 123,
      viewEndTs: 456,
    }),
  );
});
