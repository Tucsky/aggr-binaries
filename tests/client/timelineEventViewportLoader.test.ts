import assert from "node:assert/strict";
import { test } from "node:test";
import {
  buildTimelineEventsScopeKey,
  createTimelineViewportEventCacheState,
  readTimelineViewportEventCache,
  resolveTimelineViewportMissingRows,
  resolveTimelineViewportEventRequest,
  selectTimelineViewportEventRows,
  writeTimelineViewportEventCache,
} from "../../client/src/lib/features/timeline/timelineEventViewportLoader.js";
import type { TimelineEvent, TimelineMarket } from "../../client/src/lib/features/timeline/timelineTypes.js";

test("selectTimelineViewportEventRows applies row overscan and max row cap deterministically", () => {
  const markets: TimelineMarket[] = [
    { collector: "PI", exchange: "BYBIT", symbol: "BTCUSDT", timeframe: "1m", startTs: 1, endTs: 2 },
    { collector: "PI", exchange: "BYBIT", symbol: "ETHUSDT", timeframe: "1m", startTs: 1, endTs: 2 },
    { collector: "PI", exchange: "BYBIT", symbol: "XRPUSDT", timeframe: "1m", startTs: 1, endTs: 2 },
    { collector: "PI", exchange: "BYBIT", symbol: "SOLUSDT", timeframe: "1m", startTs: 1, endTs: 2 },
    { collector: "PI", exchange: "BYBIT", symbol: "ADAUSDT", timeframe: "1m", startTs: 1, endTs: 2 },
  ];

  const selected = selectTimelineViewportEventRows(markets, 2, 4, 1, 3);
  assert.deepStrictEqual(selected.rowKeys, [
    "PI:BYBIT:ETHUSDT",
    "PI:BYBIT:XRPUSDT",
    "PI:BYBIT:SOLUSDT",
  ]);
  assert.deepStrictEqual(selected.rows.map((row) => row.symbol), [
    "ETHUSDT",
    "XRPUSDT",
    "SOLUSDT",
  ]);
});

test("fetch row selection from visible window keeps all visible rows under row cap", () => {
  const markets: TimelineMarket[] = [];
  for (let i = 0; i < 200; i += 1) {
    markets.push({
      collector: "PI",
      exchange: "BYBIT",
      symbol: `SYM${i}`,
      timeframe: "1m",
      startTs: 1,
      endTs: 2,
    });
  }
  const visibleStartIndex = Math.max(0, Math.floor((33 * 100) / 33));
  const visibleEndIndex = Math.min(
    markets.length,
    Math.ceil(((33 * 100) + (33 * 22)) / 33),
  );
  assert.strictEqual(visibleStartIndex, 100);
  assert.strictEqual(visibleEndIndex, 122);
  const selected = selectTimelineViewportEventRows(
    markets,
    visibleStartIndex,
    visibleEndIndex,
    2,
    24,
  );
  assert.strictEqual(selected.rows.length, 24);
  const selectedSymbols = new Set(selected.rows.map((row) => row.symbol));
  for (let i = visibleStartIndex; i < visibleEndIndex; i += 1) {
    assert.strictEqual(selectedSymbols.has(`SYM${i}`), true);
  }
});

test("resolveTimelineViewportEventRequest skips fetch when loaded window covers visible rows/range", () => {
  const scopeKey = buildTimelineEventsScopeKey(
    "1m",
    { startTs: 100, endTs: 500 },
    "PI",
    "BYBIT",
    "BTC",
  );
  const selection = {
    rowKeys: ["PI:BYBIT:BTCUSDT", "PI:BYBIT:ETHUSDT"],
    rows: [
      { collector: "PI", exchange: "BYBIT", symbol: "BTCUSDT" },
      { collector: "PI", exchange: "BYBIT", symbol: "ETHUSDT" },
    ],
  };
  const covered = resolveTimelineViewportEventRequest({
    scopeKey,
    selectedRange: { startTs: 100, endTs: 500 },
    viewRange: { startTs: 200, endTs: 300 },
    selection,
    loadedRange: { startTs: 150, endTs: 320 },
    loadedRowKeys: new Set(selection.rowKeys),
    rangeOverscanRatio: 0.5,
    forceReload: false,
  });
  assert.strictEqual(covered, null);

  const uncovered = resolveTimelineViewportEventRequest({
    scopeKey,
    selectedRange: { startTs: 100, endTs: 500 },
    viewRange: { startTs: 280, endTs: 360 },
    selection,
    loadedRange: { startTs: 150, endTs: 320 },
    loadedRowKeys: new Set(selection.rowKeys),
    rangeOverscanRatio: 0.5,
    forceReload: false,
  });
  assert.deepStrictEqual(uncovered?.requestRange, { startTs: 240, endTs: 400 });
});

function makeEvent(id: number, symbol: string, ts: number): TimelineEvent {
  return {
    id,
    collector: "PI",
    exchange: "BYBIT",
    symbol,
    relativePath: "x",
    gapFixStatus: null,
    gapFixRecovered: null,
    ts,
    gapMs: 1,
    gapMiss: 1,
    gapScore: 1,
  };
}

test("timeline viewport row cache reuses covered rows and computes missing rows", () => {
  const cache = createTimelineViewportEventCacheState();
  const scopeKey = "1m|PI|BYBIT||100|400";
  writeTimelineViewportEventCache(cache, 8, 1000, {
    scopeKey,
    requestRange: { startTs: 140, endTs: 260 },
    rowKeys: ["PI:BYBIT:BTCUSDT"],
    events: [
      makeEvent(1, "BTCUSDT", 160),
      makeEvent(2, "BTCUSDT", 220),
    ],
  });
  const selection = {
    rowKeys: ["PI:BYBIT:BTCUSDT", "PI:BYBIT:ETHUSDT"],
    rows: [
      { collector: "PI", exchange: "BYBIT", symbol: "BTCUSDT" },
      { collector: "PI", exchange: "BYBIT", symbol: "ETHUSDT" },
    ],
  };
  const cached = readTimelineViewportEventCache(cache, scopeKey, { startTs: 150, endTs: 230 }, selection);
  assert.deepStrictEqual([...cached.coveredRowKeys], ["PI:BYBIT:BTCUSDT"]);
  assert.deepStrictEqual(cached.events.map((event) => event.id), [1, 2]);
  const missing = resolveTimelineViewportMissingRows(selection, cached.coveredRowKeys);
  assert.deepStrictEqual(missing.rowKeys, ["PI:BYBIT:ETHUSDT"]);
});

test("timeline viewport row cache evicts least-recently-used rows", () => {
  const cache = createTimelineViewportEventCacheState();
  const range = { startTs: 100, endTs: 200 };
  const singleEvent = (id: number, symbol: string) => makeEvent(id, symbol, 150);
  writeTimelineViewportEventCache(cache, 2, 1000, {
    scopeKey: "scope",
    requestRange: range,
    rowKeys: ["PI:BYBIT:BTCUSDT"],
    events: [singleEvent(1, "BTCUSDT")],
  });
  writeTimelineViewportEventCache(cache, 2, 1000, {
    scopeKey: "scope",
    requestRange: range,
    rowKeys: ["PI:BYBIT:ETHUSDT"],
    events: [singleEvent(2, "ETHUSDT")],
  });
  readTimelineViewportEventCache(cache, "scope", range, {
    rowKeys: ["PI:BYBIT:BTCUSDT"],
    rows: [{ collector: "PI", exchange: "BYBIT", symbol: "BTCUSDT" }],
  });
  writeTimelineViewportEventCache(cache, 2, 1000, {
    scopeKey: "scope",
    requestRange: range,
    rowKeys: ["PI:BYBIT:XRPUSDT"],
    events: [singleEvent(3, "XRPUSDT")],
  });
  assert.strictEqual(cache.byRow.has("scope|PI:BYBIT:ETHUSDT"), false);
  assert.strictEqual(cache.byRow.has("scope|PI:BYBIT:BTCUSDT"), true);
  assert.strictEqual(cache.byRow.has("scope|PI:BYBIT:XRPUSDT"), true);
});

test("timeline viewport row cache merges overlapping row segments", () => {
  const cache = createTimelineViewportEventCacheState();
  const event = (id: number, ts: number) => makeEvent(id, "BTCUSDT", ts);
  writeTimelineViewportEventCache(cache, 2, 1000, {
    scopeKey: "scope",
    requestRange: { startTs: 100, endTs: 220 },
    rowKeys: ["PI:BYBIT:BTCUSDT"],
    events: [event(1, 120), event(2, 200)],
  });
  writeTimelineViewportEventCache(cache, 2, 1000, {
    scopeKey: "scope",
    requestRange: { startTs: 180, endTs: 280 },
    rowKeys: ["PI:BYBIT:BTCUSDT"],
    events: [event(2, 200), event(3, 260)],
  });
  const covered = readTimelineViewportEventCache(cache, "scope", { startTs: 190, endTs: 210 }, {
    rowKeys: ["PI:BYBIT:BTCUSDT"],
    rows: [{ collector: "PI", exchange: "BYBIT", symbol: "BTCUSDT" }],
  });
  assert.deepStrictEqual([...covered.coveredRowKeys], ["PI:BYBIT:BTCUSDT"]);
  assert.deepStrictEqual(covered.events.map((item) => item.id), [2]);
});

test("timeline viewport cache prefers most recently fetched event payload for duplicate ids", () => {
  const cache = createTimelineViewportEventCacheState();
  const stale = { ...makeEvent(7, "BTCUSDT", 200), gapFixStatus: null };
  const fixed = { ...makeEvent(7, "BTCUSDT", 200), gapFixStatus: "fixed" };
  writeTimelineViewportEventCache(cache, 2, 1000, {
    scopeKey: "scope",
    requestRange: { startTs: 100, endTs: 260 },
    rowKeys: ["PI:BYBIT:BTCUSDT"],
    events: [stale],
  });
  writeTimelineViewportEventCache(cache, 2, 1000, {
    scopeKey: "scope",
    requestRange: { startTs: 180, endTs: 280 },
    rowKeys: ["PI:BYBIT:BTCUSDT"],
    events: [fixed],
  });
  const covered = readTimelineViewportEventCache(cache, "scope", { startTs: 190, endTs: 220 }, {
    rowKeys: ["PI:BYBIT:BTCUSDT"],
    rows: [{ collector: "PI", exchange: "BYBIT", symbol: "BTCUSDT" }],
  });
  assert.deepStrictEqual(covered.events.map((item) => item.id), [7]);
  assert.strictEqual(covered.events[0]?.gapFixStatus, "fixed");
});

test("timeline viewport cache keeps newest payload even when newer overlap sorts earlier by range", () => {
  const cache = createTimelineViewportEventCacheState();
  const stale = { ...makeEvent(11, "BTCUSDT", 180), gapFixStatus: null };
  const fixed = { ...makeEvent(11, "BTCUSDT", 180), gapFixStatus: "fixed" };
  writeTimelineViewportEventCache(cache, 2, 1000, {
    scopeKey: "scope",
    requestRange: { startTs: 140, endTs: 260 },
    rowKeys: ["PI:BYBIT:BTCUSDT"],
    events: [stale],
  });
  writeTimelineViewportEventCache(cache, 2, 1000, {
    scopeKey: "scope",
    requestRange: { startTs: 100, endTs: 220 },
    rowKeys: ["PI:BYBIT:BTCUSDT"],
    events: [fixed],
  });
  const covered = readTimelineViewportEventCache(cache, "scope", { startTs: 170, endTs: 200 }, {
    rowKeys: ["PI:BYBIT:BTCUSDT"],
    rows: [{ collector: "PI", exchange: "BYBIT", symbol: "BTCUSDT" }],
  });
  assert.deepStrictEqual(covered.events.map((item) => item.id), [11]);
  assert.strictEqual(covered.events[0]?.gapFixStatus, "fixed");
});

test("timeline viewport cache evicts segments farthest from active range first", () => {
  const cache = createTimelineViewportEventCacheState();
  const event = (id: number, symbol: string, ts: number) => makeEvent(id, symbol, ts);
  writeTimelineViewportEventCache(
    cache,
    4,
    1,
    {
      scopeKey: "scope",
      requestRange: { startTs: 0, endTs: 100 },
      rowKeys: ["PI:BYBIT:BTCUSDT"],
      events: [event(1, "BTCUSDT", 50)],
    },
  );
  writeTimelineViewportEventCache(
    cache,
    4,
    1,
    {
      scopeKey: "scope",
      requestRange: { startTs: 1_000, endTs: 1_100 },
      rowKeys: ["PI:BYBIT:ETHUSDT"],
      events: [event(2, "ETHUSDT", 1_050)],
    },
  );
  assert.strictEqual(cache.byRow.has("scope|PI:BYBIT:BTCUSDT"), false);
  assert.strictEqual(cache.byRow.has("scope|PI:BYBIT:ETHUSDT"), true);
});
